package broker

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/auth"
	"github.com/michael-emmanuel/eventstream/internal/fault"
	"github.com/michael-emmanuel/eventstream/internal/observability"
	"github.com/michael-emmanuel/eventstream/internal/partition"
	"github.com/michael-emmanuel/eventstream/internal/protocol"
	"github.com/michael-emmanuel/eventstream/internal/ratelimit"
	"github.com/michael-emmanuel/eventstream/internal/record"
	"github.com/michael-emmanuel/eventstream/internal/transport"
)

func (b *Broker) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		b.cfg.Metrics.WritePrometheus(w)
	})
	mux.HandleFunc("POST /v1/topics/{topic}/partitions/{partition}/events", b.authed(b.handleProduce))
	mux.HandleFunc("GET /v1/topics/{topic}/partitions/{partition}/events", b.authed(b.handleFetch))
	mux.HandleFunc("GET /v1/topics/{topic}/partitions/{partition}/offsets", b.authed(b.handleOffsets))
	mux.HandleFunc("GET /v1/metadata", b.authed(b.handleMetadata))
	mux.HandleFunc("POST /v1/producer-ids", b.authed(b.handleProducerID))
	mux.HandleFunc("GET /internal/v1/replica/fetch", b.authed(b.handleReplicaFetch))

	mux.HandleFunc("GET /v1/admin/partitions", b.admin(b.handlePartitions))
	mux.HandleFunc("GET /v1/admin/faults", b.admin(b.handleGetFaults))
	mux.HandleFunc("POST /v1/admin/faults", b.admin(b.handleSetFaults))
	mux.HandleFunc("GET /v1/admin/limits", b.admin(b.handleGetLimits))
	mux.HandleFunc("POST /v1/admin/limits", b.admin(b.handleSetLimits))
	mux.HandleFunc("POST /v1/admin/maintenance", b.admin(b.handleMaintenance))

	return transport.Middleware(mux, b.cfg.MaxConcurrentRequests, b.log, b.m.observeRequest)
}

type handler func(w http.ResponseWriter, r *http.Request, p auth.Principal)

func (b *Broker) authed(h handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b.faults.DelayRequest()
		p, err := b.cfg.Auth.Authenticate(r)
		if err != nil {
			protocol.WriteError(w, protocol.Errorf(401, protocol.CodeUnauthenticated, "%v", err))
			return
		}
		h(w, r, p)
	}
}

func (b *Broker) admin(h handler) http.HandlerFunc {
	return b.authed(func(w http.ResponseWriter, r *http.Request, p auth.Principal) {
		if b.cfg.Auth.Enabled && !p.Superuser {
			protocol.WriteError(w, protocol.Errorf(403, protocol.CodeForbidden, "admin endpoint"))
			return
		}
		b.log.Info("admin request", "audit", true, "principal", p.Name, "path", r.URL.Path)
		h(w, r, p)
	})
}

func (b *Broker) topicTenant(topic string) string {
	if md := b.md.Load(); md != nil {
		if t := md.Topic(topic); t != nil {
			return t.Config.Tenant
		}
	}
	return ""
}

func (b *Broker) authorize(w http.ResponseWriter, p auth.Principal, op auth.Operation, topic string) bool {
	if err := b.cfg.Auth.Authorize(p, op, topic, b.topicTenant(topic)); err != nil {
		b.log.Warn("authorization denied", "audit", true, "principal", p.Name, "op", op, "topic", topic)
		protocol.WriteError(w, protocol.Errorf(403, protocol.CodeForbidden, "%v", err))
		return false
	}
	return true
}

func (b *Broker) lookup(w http.ResponseWriter, r *http.Request) (tpKey, *partition.Partition, bool) {
	topic := r.PathValue("topic")
	pn, err := strconv.Atoi(r.PathValue("partition"))
	if err != nil {
		protocol.WriteError(w, protocol.Errorf(400, protocol.CodeInvalidRequest, "invalid partition"))
		return tpKey{}, nil, false
	}
	k := tpKey{topic, int32(pn)}
	p := b.partition(topic, int32(pn))
	if p == nil {
		e := protocol.Errorf(404, protocol.CodeUnknownTopic, "broker %d hosts no replica of %s", b.cfg.ID, k)
		if md := b.md.Load(); md != nil {
			if u, id, ok := md.LeaderURL(topic, int32(pn)); ok {
				e = protocol.Errorf(421, protocol.CodeNotLeader, "broker %d is not the leader for %s", b.cfg.ID, k)
				e.LeaderID, e.LeaderURL = &id, u
			}
		}
		protocol.WriteError(w, e)
		return k, nil, false
	}
	return k, p, true
}

// handleProduce is the write path:
//
//	auth -> size limit -> memory budget (backpressure) -> rate limits ->
//	decode -> partition.Produce (dedup, append, wait for acks) -> respond
//
// Every stage that can refuse work does so before allocating for it.
func (b *Broker) handleProduce(w http.ResponseWriter, r *http.Request, pr auth.Principal) {
	start := time.Now()
	k, p, ok := b.lookup(w, r)
	if !ok || !b.authorize(w, pr, auth.OpProduce, k.topic) {
		return
	}
	// Backpressure: bound the bytes of produce requests held in memory.
	size := r.ContentLength
	if size <= 0 {
		size = 4096
	}
	if size > b.cfg.MaxRequestBytes {
		protocol.WriteError(w, protocol.Errorf(413, protocol.CodeInvalidRequest, "request of %d bytes exceeds %d", size, b.cfg.MaxRequestBytes))
		return
	}
	actx, cancel := context.WithTimeout(r.Context(), 100*time.Millisecond)
	admitted := b.inflight.acquire(actx, size)
	cancel()
	if !admitted {
		b.m.rejected.With("memory").Inc()
		protocol.WriteError(w, &protocol.Error{Status: 503, Code: protocol.CodeOverloaded,
			Message: "broker produce buffer full; back off and retry", RetryAfterMs: 200})
		return
	}
	defer b.inflight.release(size)

	var req protocol.ProduceRequest
	if e := transport.ReadJSON(w, r, b.cfg.MaxRequestBytes, &req); e != nil {
		protocol.WriteError(w, e)
		return
	}
	if len(req.Records) == 0 {
		protocol.WriteError(w, protocol.Errorf(400, protocol.CodeInvalidRequest, "no records"))
		return
	}
	tenant := pr.Tenant
	if tenant == "" {
		tenant = b.topicTenant(k.topic)
	}
	if d := b.limiter.Allow(tenant, k.topic, req.ProducerID, int(size)); !d.Allowed {
		b.m.throttled.With(d.Scope).Inc()
		protocol.WriteError(w, &protocol.Error{Status: 429, Code: protocol.CodeThrottled,
			Message: "rate limit exceeded at " + d.Scope + " scope", RetryAfterMs: max(d.RetryAfter.Milliseconds(), 1)})
		return
	}
	acks, err := partition.ParseAcks(req.Acks)
	if err != nil {
		protocol.WriteError(w, protocol.Errorf(400, protocol.CodeInvalidRequest, "%v", err))
		return
	}

	parent, _ := observability.ParseTraceparent(r.Header.Get(observability.TraceparentHeader))
	_, span := b.tracer.Start(r.Context(), "broker.produce", parent)
	span.SetAttr("partition", k.String())
	span.SetAttr("records", len(req.Records))

	recs := make([]*record.Record, len(req.Records))
	var bytesIn int
	for i, jr := range req.Records {
		recs[i] = toRecord(jr)
		bytesIn += recs[i].EncodedSize()
	}
	timeout := time.Duration(req.TimeoutMs) * time.Millisecond
	if timeout <= 0 || timeout > 30*time.Second {
		timeout = b.cfg.DefaultProduceTimeout
	}
	pctx, pcancel := context.WithTimeout(r.Context(), timeout)
	res, err := p.Produce(pctx, partition.ProduceRequest{
		ProducerID: req.ProducerID, BaseSequence: req.BaseSequence, Records: recs, Acks: acks,
	})
	pcancel()
	span.End(err)
	if err != nil {
		b.m.produceErrors.With(k.topic, errCode(err)).Inc()
		b.writePartitionError(w, k, p, err)
		return
	}
	if b.faults.CrashOnProduce() {
		b.log.Error("injected crash after append, before response")
		b.cfg.Exit(137)
	}
	if res.Duplicate {
		b.m.duplicates.With(k.topic).Inc()
	} else {
		b.m.produceRecords.With(k.topic, strconv.Itoa(int(k.p))).Add(float64(len(recs)))
		b.m.produceBytes.With(k.topic, strconv.Itoa(int(k.p))).Add(float64(bytesIn))
	}
	b.m.produceLatency.With(acks.String()).ObserveSince(start)
	b.m.batchSize.With().Observe(float64(len(recs)))
	if b.faults.DropProduceResponse() {
		// Simulate a lost response: the append happened, the client never
		// hears about it, and its retry must be deduplicated.
		b.log.Warn("injected: dropping produce response", "partition", k.String(), "base_offset", res.BaseOffset)
		if conn, _, err := http.NewResponseController(w).Hijack(); err == nil {
			conn.Close()
			return
		}
		protocol.WriteError(w, protocol.Errorf(503, protocol.CodeUnavailable, "injected: response dropped"))
		return
	}
	protocol.WriteJSON(w, 200, protocol.ProduceResponse{
		Topic: k.topic, Partition: k.p, BaseOffset: res.BaseOffset, LastOffset: res.LastOffset,
		Duplicate: res.Duplicate, LogAppendTime: res.LogAppendTime,
	})
}

func errCode(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errorIs(err, partition.ErrNotLeader), errorIs(err, partition.ErrLeaseExpired):
		return "not_leader"
	case errorIs(err, partition.ErrNotEnoughReplicas):
		return "not_enough_replicas"
	case errorIs(err, partition.ErrReplicationTimeout):
		return "timeout"
	case errorIs(err, partition.ErrOutOfOrderSequence):
		return "out_of_order"
	case errorIs(err, partition.ErrInvalidRecord):
		return "invalid"
	case errorIs(err, partition.ErrStorage):
		return "storage"
	}
	return "other"
}

func toRecord(jr protocol.Record) *record.Record {
	r := &record.Record{}
	if jr.Key != nil {
		r.Key = []byte(*jr.Key)
	}
	if jr.Value == nil {
		r.Tombstone = true
	} else {
		r.Value = []byte(*jr.Value)
	}
	if len(jr.Headers) > 0 {
		keys := make([]string, 0, len(jr.Headers))
		for k := range jr.Headers {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			r.Headers = append(r.Headers, record.Header{Key: k, Value: []byte(jr.Headers[k])})
		}
	}
	return r
}

// ToConsumerRecord converts a stored record to its JSON form.
func ToConsumerRecord(topic string, p int32, r record.Record) protocol.ConsumerRecord {
	cr := protocol.ConsumerRecord{Topic: topic, Partition: p, Offset: r.Offset, Timestamp: r.Timestamp,
		LeaderEpoch: r.LeaderEpoch, ProducerID: r.ProducerID, Sequence: r.Sequence}
	if r.Key != nil {
		k := string(r.Key)
		cr.Key = &k
	}
	if !r.Tombstone {
		v := string(r.Value)
		cr.Value = &v
	}
	if len(r.Headers) > 0 {
		cr.Headers = make(map[string]string, len(r.Headers))
		for _, h := range r.Headers {
			cr.Headers[h.Key] = string(h.Value)
		}
	}
	return cr
}

// handleFetch serves consumers with batch reads and long polling.
//
// Batching: one request returns up to max_events records / max_bytes, so
// per-request costs (HTTP, auth, index lookup, syscalls) are amortized.
// Long polling: if nothing is available at offset, the request waits up to
// wait_ms for the high-water mark to move. Waiters are bounded; when the
// pool is exhausted the request returns immediately (empty) instead of
// tying up another goroutine.
func (b *Broker) handleFetch(w http.ResponseWriter, r *http.Request, pr auth.Principal) {
	start := time.Now()
	k, p, ok := b.lookup(w, r)
	if !ok || !b.authorize(w, pr, auth.OpConsume, k.topic) {
		return
	}
	offset, err := transport.QueryInt(r, "offset", -1)
	if err != nil {
		protocol.WriteError(w, protocol.Errorf(400, protocol.CodeInvalidRequest, "%v", err))
		return
	}
	if offset < 0 {
		offset = p.LogStartOffset()
	}
	maxEvents, _ := transport.QueryInt(r, "max_events", 500)
	maxBytes, _ := transport.QueryInt(r, "max_bytes", 1<<20)
	waitMs, _ := transport.QueryInt(r, "wait_ms", 0)
	if maxEvents <= 0 || maxEvents > 10000 {
		maxEvents = 10000
	}
	if maxBytes <= 0 || maxBytes > int64(b.cfg.MaxFetchBytes) {
		maxBytes = int64(b.cfg.MaxFetchBytes)
	}
	wait := time.Duration(waitMs) * time.Millisecond
	if wait > b.cfg.MaxLongPollWait {
		wait = b.cfg.MaxLongPollWait
	}
	var timer *time.Timer
	for {
		ch := p.WaitCh()
		res, err := p.Read(offset, int(maxBytes), int(maxEvents))
		if err != nil {
			b.writePartitionError(w, k, p, err)
			return
		}
		if len(res.Records) > 0 || wait <= 0 {
			b.writeFetch(w, k, offset, res, start)
			return
		}
		if timer == nil {
			select {
			case b.pollers <- struct{}{}:
				defer func() { <-b.pollers }()
			default:
				b.m.rejected.With("long_poll_capacity").Inc()
				b.writeFetch(w, k, offset, res, start)
				return
			}
			timer = time.NewTimer(wait)
			defer timer.Stop()
		}
		select {
		case <-ch:
		case <-timer.C:
			wait = 0
		case <-r.Context().Done():
			return
		}
	}
}

func (b *Broker) writeFetch(w http.ResponseWriter, k tpKey, offset int64, res partition.ReadResult, start time.Time) {
	out := protocol.FetchResponse{Topic: k.topic, Partition: k.p, Records: make([]protocol.ConsumerRecord, 0, len(res.Records)),
		HighWatermark: res.HighWatermark, LogStartOffset: res.LogStartOffset, LogEndOffset: res.LogEndOffset, NextOffset: offset}
	var bytesOut int
	for _, rec := range res.Records {
		out.Records = append(out.Records, ToConsumerRecord(k.topic, k.p, rec))
		out.NextOffset = rec.Offset + 1
		bytesOut += len(rec.Value) + len(rec.Key)
	}
	b.m.fetchRecords.With(k.topic).Add(float64(len(res.Records)))
	b.m.fetchBytes.With(k.topic).Add(float64(bytesOut))
	b.m.fetchLatency.With().ObserveSince(start)
	protocol.WriteJSON(w, 200, out)
}

func (b *Broker) handleOffsets(w http.ResponseWriter, r *http.Request, pr auth.Principal) {
	k, p, ok := b.lookup(w, r)
	if !ok || !b.authorize(w, pr, auth.OpDescribe, k.topic) {
		return
	}
	if role, _, _ := p.Role(); role != partition.RoleLeader {
		b.writePartitionError(w, k, p, partition.ErrNotLeader)
		return
	}
	out := protocol.OffsetsResponse{Topic: k.topic, Partition: k.p, LogStartOffset: p.LogStartOffset(),
		LogEndOffset: p.LogEndOffset(), HighWatermark: p.HighWatermark()}
	if ts := r.URL.Query().Get("timestamp"); ts != "" {
		v, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			protocol.WriteError(w, protocol.Errorf(400, protocol.CodeInvalidRequest, "timestamp must be unix ms"))
			return
		}
		off, err := p.OffsetForTimestamp(v)
		if err != nil {
			b.writePartitionError(w, k, p, err)
			return
		}
		out.OffsetForTimestamp = off
	}
	protocol.WriteJSON(w, 200, out)
}

func (b *Broker) handleMetadata(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	md := b.md.Load()
	if md == nil {
		protocol.WriteError(w, protocol.Errorf(503, protocol.CodeUnavailable, "broker has no metadata yet"))
		return
	}
	protocol.WriteJSON(w, 200, md)
}

func (b *Broker) handleProducerID(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	var out protocol.ProducerIDResponse
	if err := b.controller(r.Context(), "POST", "/v1/producer-ids", nil, &out); err != nil {
		protocol.WriteError(w, protocol.Errorf(503, protocol.CodeUnavailable, "controller: %v", err))
		return
	}
	protocol.WriteJSON(w, 200, out)
}

// --- admin -----------------------------------------------------------------

func (b *Broker) handlePartitions(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	var out []partition.State
	for _, p := range b.partitions() {
		out = append(out, p.State())
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	protocol.WriteJSON(w, 200, map[string]any{
		"broker_id": b.cfg.ID, "lease_valid": b.leaseValid(), "partitions": out,
		"inflight_produce_bytes": b.inflight.inUse(),
	})
}

func (b *Broker) handleGetFaults(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	protocol.WriteJSON(w, 200, b.faults.Get())
}

func (b *Broker) handleSetFaults(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	var c fault.Config
	if e := transport.ReadJSON(w, r, 1<<16, &c); e != nil {
		protocol.WriteError(w, e)
		return
	}
	b.faults.Set(c)
	b.log.Warn("fault injection updated", "faults", c)
	protocol.WriteJSON(w, 200, c)
}

func (b *Broker) handleGetLimits(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	protocol.WriteJSON(w, 200, b.limiter.Limits())
}

func (b *Broker) handleSetLimits(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	var l ratelimit.Limits
	if e := transport.ReadJSON(w, r, 1<<16, &l); e != nil {
		protocol.WriteError(w, e)
		return
	}
	b.limiter.SetLimits(l)
	protocol.WriteJSON(w, 200, b.limiter.Limits())
}

func (b *Broker) handleMaintenance(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	compact := r.URL.Query().Get("compact") != "false"
	protocol.WriteJSON(w, 200, b.RunMaintenance(compact))
}
