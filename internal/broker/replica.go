package broker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/auth"
	"github.com/michael-emmanuel/eventstream/internal/partition"
	"github.com/michael-emmanuel/eventstream/internal/protocol"
)

// Replication is pull-based: each follower runs a fetcher that repeatedly
// asks the leader for records after its log end offset. Pull keeps the
// leader simple (no per-follower send queues or flow control), lets a slow
// follower fall behind without slowing the leader, and makes the fetch
// offset double as the follower's acknowledgement.
//
// The payload is raw record frames, byte-identical to the leader's log:
// no re-encoding, and every frame's checksum is verified by the follower
// before it is appended.

const (
	hdrHWM          = "X-High-Watermark"
	hdrLogStart     = "X-Log-Start-Offset"
	hdrLogEnd       = "X-Log-End-Offset"
	hdrLeaderEpoch  = "X-Leader-Epoch"
	hdrRecordCount  = "X-Record-Count"
	hdrDivergeEpoch = "X-Diverging-Epoch"
	hdrDivergeEnd   = "X-Diverging-End-Offset"
)

func (b *Broker) runFetcher(ctx context.Context, k tpKey, p *partition.Partition) {
	backoff := 50 * time.Millisecond
	fail := func(reason string, err error) {
		b.m.replicaFetchErrors.With(reason).Inc()
		b.log.Debug("replica fetch failed", "partition", k.String(), "reason", reason, "error", err)
		sleepCtx(ctx, backoff)
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
	for ctx.Err() == nil {
		pos, ok := p.FollowerPosition()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-p.WaitCh():
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		md := b.md.Load()
		leader, ok := md.Broker(pos.LeaderID)
		if !ok {
			fail("unknown_leader", nil)
			continue
		}
		if err := b.faults.BeforeReplicaFetch(); err != nil {
			fail("injected", err)
			continue
		}
		res, err := b.fetchFromLeader(ctx, leader.URL, k, pos)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			var pe *protocol.Error
			if errors.As(err, &pe) && pe.Code == protocol.CodeOffsetOutOfRange && pe.LogStartOffset != nil {
				if pos.FetchOffset < *pe.LogStartOffset {
					b.log.Warn("follower is behind the leader's log start; resetting replica",
						"partition", k.String(), "fetch_offset", pos.FetchOffset, "leader_log_start", *pe.LogStartOffset)
					_ = p.ResetTo(*pe.LogStartOffset)
				} else if pe.HighWatermark != nil {
					_, _ = p.TruncateForDivergence(pos.LastFetchedEpoch, *pe.HighWatermark)
				}
				continue
			}
			fail("request", err)
			continue
		}
		backoff = 50 * time.Millisecond
		if res.diverging {
			target, err := p.TruncateForDivergence(res.divergingEpoch, res.divergingEnd)
			b.m.truncations.With().Inc()
			b.log.Warn("follower log diverged from leader; truncated", "partition", k.String(),
				"from", pos.FetchOffset, "to", target, "leader_epoch", pos.LeaderEpoch, "error", err)
			continue
		}
		n, err := p.AppendAsFollower(pos.LeaderEpoch, res.frames, res.hwm)
		if err != nil {
			if !errors.Is(err, partition.ErrNotFollower) {
				fail("append", err)
			}
			continue
		}
		b.m.replicaBytes.With(k.topic).Add(float64(len(res.frames)))
		b.m.replicaRecords.With(k.topic).Add(float64(n))
	}
}

type fetchResult struct {
	frames         []byte
	hwm            int64
	diverging      bool
	divergingEpoch int32
	divergingEnd   int64
}

func (b *Broker) fetchFromLeader(ctx context.Context, leaderURL string, k tpKey, pos partition.FollowerPosition) (fetchResult, error) {
	q := url.Values{}
	q.Set("topic", k.topic)
	q.Set("partition", strconv.Itoa(int(k.p)))
	q.Set("offset", strconv.FormatInt(pos.FetchOffset, 10))
	q.Set("last_epoch", strconv.Itoa(int(pos.LastFetchedEpoch)))
	q.Set("leader_epoch", strconv.Itoa(int(pos.LeaderEpoch)))
	q.Set("replica_id", strconv.Itoa(int(b.cfg.ID)))
	q.Set("max_bytes", strconv.Itoa(b.cfg.ReplicaFetchMaxBytes))
	q.Set("wait_ms", strconv.FormatInt(b.cfg.ReplicaFetchWait.Milliseconds(), 10))
	rctx, cancel := context.WithTimeout(ctx, b.cfg.ReplicaFetchWait+5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, "GET", leaderURL+"/internal/v1/replica/fetch?"+q.Encode(), nil)
	if err != nil {
		return fetchResult{}, err
	}
	resp, err := b.cfg.HTTPClient.Do(req)
	if err != nil {
		return fetchResult{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(b.cfg.ReplicaFetchMaxBytes)+(16<<20)))
	if err != nil {
		return fetchResult{}, err
	}
	if resp.StatusCode != 200 {
		return fetchResult{}, protocol.DecodeError(resp.StatusCode, body)
	}
	res := fetchResult{frames: body}
	res.hwm, _ = strconv.ParseInt(resp.Header.Get(hdrHWM), 10, 64)
	if e := resp.Header.Get(hdrDivergeEpoch); e != "" {
		ep, _ := strconv.Atoi(e)
		end, _ := strconv.ParseInt(resp.Header.Get(hdrDivergeEnd), 10, 64)
		res.diverging, res.divergingEpoch, res.divergingEnd = true, int32(ep), end
	}
	return res, nil
}

// handleReplicaFetch serves followers. It long-polls: if the follower is
// fully caught up, the request is held until new data arrives or wait_ms
// passes, so idle partitions cost almost nothing and new appends reach
// followers within one round trip.
func (b *Broker) handleReplicaFetch(w http.ResponseWriter, r *http.Request, pr auth.Principal) {
	if b.cfg.Auth.Enabled && !pr.Superuser {
		protocol.WriteError(w, protocol.Errorf(403, protocol.CodeForbidden, "replication requires an inter-broker credential"))
		return
	}
	if b.faults.RejectInboundReplication() {
		protocol.WriteError(w, protocol.Errorf(503, protocol.CodeUnavailable, "injected: isolated from brokers"))
		return
	}
	q := r.URL.Query()
	pn, _ := strconv.Atoi(q.Get("partition"))
	k := tpKey{q.Get("topic"), int32(pn)}
	p := b.partition(k.topic, k.p)
	if p == nil {
		protocol.WriteError(w, protocol.Errorf(404, protocol.CodeUnknownTopic, "no replica of %s here", k))
		return
	}
	atoi64 := func(name string) int64 { v, _ := strconv.ParseInt(q.Get(name), 10, 64); return v }
	req := partition.ReplicaFetchRequest{
		ReplicaID:        int32(atoi64("replica_id")),
		FetchOffset:      atoi64("offset"),
		LastFetchedEpoch: int32(atoi64("last_epoch")),
		LeaderEpoch:      int32(atoi64("leader_epoch")),
		MaxBytes:         int(atoi64("max_bytes")),
	}
	if req.MaxBytes <= 0 || req.MaxBytes > b.cfg.MaxFetchBytes {
		req.MaxBytes = b.cfg.MaxFetchBytes
	}
	wait := time.Duration(atoi64("wait_ms")) * time.Millisecond
	if wait > 5*time.Second {
		wait = 5 * time.Second
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		ch := p.WaitCh()
		resp, err := p.HandleReplicaFetch(req)
		if err != nil {
			b.writePartitionError(w, k, p, err)
			return
		}
		if resp.Diverging || resp.Count > 0 || wait <= 0 {
			if b.faults.RejectInboundReplication() {
				// A partition that starts while a long poll is parked must
				// also drop the response, or data "leaks" across it.
				protocol.WriteError(w, protocol.Errorf(503, protocol.CodeUnavailable, "injected: isolated from brokers"))
				return
			}
			h := w.Header()
			h.Set("Content-Type", "application/octet-stream")
			h.Set(hdrHWM, strconv.FormatInt(resp.HighWatermark, 10))
			h.Set(hdrLogStart, strconv.FormatInt(resp.LogStartOffset, 10))
			h.Set(hdrLogEnd, strconv.FormatInt(resp.LogEndOffset, 10))
			h.Set(hdrLeaderEpoch, strconv.Itoa(int(resp.LeaderEpoch)))
			h.Set(hdrRecordCount, strconv.Itoa(resp.Count))
			if resp.Diverging {
				h.Set(hdrDivergeEpoch, strconv.Itoa(int(resp.DivergingEpoch)))
				h.Set(hdrDivergeEnd, strconv.FormatInt(resp.DivergingEndOffset, 10))
			}
			w.WriteHeader(200)
			_, _ = w.Write(resp.Frames)
			return
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

// writePartitionError maps partition errors to wire errors, attaching the
// current leader so clients can redirect without a metadata round trip.
func (b *Broker) writePartitionError(w http.ResponseWriter, k tpKey, p *partition.Partition, err error) {
	var e *protocol.Error
	switch {
	case errors.Is(err, partition.ErrNotLeader), errors.Is(err, partition.ErrNotFollower):
		e = protocol.Errorf(421, protocol.CodeNotLeader, "broker %d is not the leader for %s", b.cfg.ID, k)
		if md := b.md.Load(); md != nil {
			if u, id, ok := md.LeaderURL(k.topic, k.p); ok {
				e.LeaderID, e.LeaderURL = &id, u
			}
		}
	case errors.Is(err, partition.ErrLeaseExpired):
		e = protocol.Errorf(421, protocol.CodeLeaseExpired, "%v", err)
	case errors.Is(err, partition.ErrFencedLeaderEpoch):
		e = protocol.Errorf(409, protocol.CodeFencedEpoch, "%v", err)
	case errors.Is(err, partition.ErrUnknownLeaderEpoch):
		e = protocol.Errorf(409, protocol.CodeUnknownEpoch, "%v", err)
	case errors.Is(err, partition.ErrUnknownReplica):
		e = protocol.Errorf(409, protocol.CodeUnknownReplica, "%v", err)
	case errors.Is(err, partition.ErrNotEnoughReplicas):
		e = protocol.Errorf(503, protocol.CodeNotEnoughReplicas, "%v", err)
	case errors.Is(err, partition.ErrReplicationTimeout):
		e = protocol.Errorf(504, protocol.CodeReplicationTimeout, "%v", err)
	case errors.Is(err, partition.ErrOutOfOrderSequence):
		e = protocol.Errorf(409, protocol.CodeOutOfOrderSequence, "%v", err)
	case errors.Is(err, partition.ErrInvalidRecord):
		e = protocol.Errorf(400, protocol.CodeInvalidRecord, "%v", err)
	case errors.Is(err, partition.ErrOffsetOutOfRange):
		e = protocol.Errorf(416, protocol.CodeOffsetOutOfRange, "%v", err)
		ls, hwm := p.LogStartOffset(), p.HighWatermark()
		e.LogStartOffset, e.HighWatermark = &ls, &hwm
	case errors.Is(err, partition.ErrStorage):
		e = protocol.Errorf(503, protocol.CodeStorage, "%v", err)
	default:
		e = protocol.Errorf(500, protocol.CodeInternal, "%v", err)
	}
	protocol.WriteError(w, e)
}
