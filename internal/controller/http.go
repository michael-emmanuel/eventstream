package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/auth"
	"github.com/michael-emmanuel/eventstream/internal/metadata"
	"github.com/michael-emmanuel/eventstream/internal/protocol"
	"github.com/michael-emmanuel/eventstream/internal/transport"
)

const maxBody = 1 << 20

// Handler returns the controller's HTTP API (including consumer groups).
func (c *Controller) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		c.cfg.Metrics.WritePrometheus(w)
	})

	// Broker-facing (internal).
	mux.HandleFunc("POST /v1/brokers/heartbeat", c.internal(c.handleHeartbeat))
	mux.HandleFunc("POST /v1/partitions/isr", c.internal(c.handleAlterISR))
	mux.HandleFunc("GET /v1/metadata", c.authed(c.handleMetadata))
	mux.HandleFunc("POST /v1/producer-ids", c.authed(c.handleProducerID))

	// Admin.
	mux.HandleFunc("GET /v1/cluster", c.authed(c.handleCluster))
	mux.HandleFunc("GET /v1/topics", c.authed(c.handleListTopics))
	mux.HandleFunc("POST /v1/topics", c.authed(c.handleCreateTopic))
	mux.HandleFunc("GET /v1/topics/{topic}", c.authed(c.handleDescribeTopic))
	mux.HandleFunc("DELETE /v1/topics/{topic}", c.authed(c.handleDeleteTopic))
	mux.HandleFunc("PATCH /v1/topics/{topic}/config", c.authed(c.handleAlterConfig))
	mux.HandleFunc("POST /v1/topics/{topic}/partitions", c.authed(c.handleAddPartitions))
	mux.HandleFunc("POST /v1/admin/preferred-election", c.internal(c.handlePreferredElection))

	// Consumer groups.
	mux.HandleFunc("GET /v1/groups", c.authed(c.handleListGroups))
	mux.HandleFunc("GET /v1/groups/{group}", c.authed(c.handleDescribeGroup))
	mux.HandleFunc("POST /v1/groups/{group}/join", c.authed(c.handleJoin))
	mux.HandleFunc("POST /v1/groups/{group}/heartbeat", c.authed(c.handleGroupHeartbeat))
	mux.HandleFunc("POST /v1/groups/{group}/leave", c.authed(c.handleLeave))
	mux.HandleFunc("POST /v1/groups/{group}/offsets", c.authed(c.handleCommit))
	mux.HandleFunc("GET /v1/groups/{group}/offsets", c.authed(c.handleFetchOffsets))
	mux.HandleFunc("GET /v1/groups/{group}/lag", c.authed(c.handleLag))
	mux.HandleFunc("POST /v1/groups/{group}/offsets/reset", c.authed(c.handleReset))

	reqs := c.cfg.Metrics.Counter("controller_requests_total", "Controller HTTP requests.", "route", "code")
	lat := c.cfg.Metrics.Histogram("controller_request_duration_seconds", "Controller request latency.", nil, "route")
	return transport.Middleware(mux, 1024, c.log, func(route string, status int, d time.Duration, _, _ int64) {
		reqs.With(route, strconv.Itoa(status)).Inc()
		lat.With(route).Observe(d.Seconds())
	})
}

type handler func(w http.ResponseWriter, r *http.Request, p auth.Principal)

func (c *Controller) authed(h handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, err := c.cfg.Auth.Authenticate(r)
		if err != nil {
			protocol.WriteError(w, protocol.Errorf(401, protocol.CodeUnauthenticated, "%v", err))
			return
		}
		h(w, r, p)
	}
}

// internal endpoints are for brokers and operators only.
func (c *Controller) internal(h handler) http.HandlerFunc {
	return c.authed(func(w http.ResponseWriter, r *http.Request, p auth.Principal) {
		if c.cfg.Auth.Enabled && !p.Superuser {
			protocol.WriteError(w, protocol.Errorf(403, protocol.CodeForbidden, "internal endpoint"))
			return
		}
		h(w, r, p)
	})
}

func (c *Controller) authorize(w http.ResponseWriter, p auth.Principal, op auth.Operation, topic string) bool {
	tenant := ""
	if t, ok := c.Topic(topic); ok {
		tenant = t.Config.Tenant
	}
	if err := c.cfg.Auth.Authorize(p, op, topic, tenant); err != nil {
		c.log.Warn("authorization denied", "audit", true, "principal", p.Name, "op", op, "topic", topic)
		protocol.WriteError(w, protocol.Errorf(403, protocol.CodeForbidden, "%v", err))
		return false
	}
	if op == auth.OpAdmin {
		c.log.Info("admin operation", "audit", true, "principal", p.Name, "topic", topic)
	}
	return true
}

func writeErr(w http.ResponseWriter, err error) {
	var pe *protocol.Error
	if errors.As(err, &pe) {
		protocol.WriteError(w, pe)
		return
	}
	protocol.WriteError(w, protocol.Errorf(500, protocol.CodeInternal, "%v", err))
}

func (c *Controller) handleHeartbeat(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	var req protocol.BrokerHeartbeatRequest
	if e := transport.ReadJSON(w, r, maxBody, &req); e != nil {
		protocol.WriteError(w, e)
		return
	}
	resp, err := c.Heartbeat(req)
	if err != nil {
		writeErr(w, err)
		return
	}
	protocol.WriteJSON(w, 200, resp)
}

func (c *Controller) handleAlterISR(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	var req protocol.AlterISRRequest
	if e := transport.ReadJSON(w, r, maxBody, &req); e != nil {
		protocol.WriteError(w, e)
		return
	}
	ps, err := c.AlterISR(req)
	if err != nil {
		writeErr(w, err)
		return
	}
	protocol.WriteJSON(w, 200, ps)
}

func (c *Controller) handleMetadata(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	minV, err := transport.QueryInt(r, "min_version", 0)
	if err != nil {
		protocol.WriteError(w, protocol.Errorf(400, protocol.CodeInvalidRequest, "%v", err))
		return
	}
	wait, _ := transport.QueryInt(r, "wait_ms", 0)
	if wait > 30_000 {
		wait = 30_000
	}
	protocol.WriteJSON(w, 200, c.Metadata(r.Context(), minV, time.Duration(wait)*time.Millisecond))
}

func (c *Controller) handleProducerID(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	protocol.WriteJSON(w, 200, protocol.ProducerIDResponse{ProducerID: c.AllocateProducerID()})
}

func (c *Controller) handleCluster(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	md := c.Metadata(r.Context(), 0, 0)
	under, offline := md.UnderReplicated()
	leaders := map[int32]int{}
	for _, t := range md.Topics {
		for _, p := range t.Partitions {
			if p.Leader >= 0 {
				leaders[p.Leader]++
			}
		}
	}
	type brokerSummary struct {
		metadata.Broker
		Leaders int `json:"leaders"`
	}
	var bs []brokerSummary
	for _, b := range md.Brokers {
		bs = append(bs, brokerSummary{b, leaders[b.ID]})
	}
	protocol.WriteJSON(w, 200, map[string]any{
		"metadata_version":       md.Version,
		"controller_url":         md.ControllerURL,
		"brokers":                bs,
		"topics":                 len(md.Topics),
		"under_replicated":       under,
		"offline_partitions":     offline,
		"controller_consensus":   "none (single controller, file-backed; see docs/distributed-consistency.md)",
		"broker_session_timeout": c.cfg.SessionTimeout.String(),
		"broker_lease_duration":  c.cfg.LeaseDuration.String(),
	})
}

func (c *Controller) handleListTopics(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	md := c.Metadata(r.Context(), 0, 0)
	type summary struct {
		Name       string `json:"name"`
		Partitions int    `json:"partitions"`
		RF         int    `json:"replication_factor"`
		Tenant     string `json:"tenant,omitempty"`
		Policy     string `json:"cleanup_policy"`
	}
	out := []summary{}
	for _, t := range md.Topics {
		out = append(out, summary{t.Name, len(t.Partitions), len(t.Partitions[0].Replicas), t.Config.Tenant, t.Config.CleanupPolicy})
	}
	protocol.WriteJSON(w, 200, out)
}

func (c *Controller) handleCreateTopic(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	var req protocol.CreateTopicRequest
	if e := transport.ReadJSON(w, r, maxBody, &req); e != nil {
		protocol.WriteError(w, e)
		return
	}
	if !c.authorize(w, p, auth.OpAdmin, req.Name) {
		return
	}
	t, err := c.CreateTopic(req, p)
	if err != nil {
		writeErr(w, err)
		return
	}
	protocol.WriteJSON(w, 201, t)
}

func (c *Controller) handleDescribeTopic(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	name := r.PathValue("topic")
	if !c.authorize(w, p, auth.OpDescribe, name) {
		return
	}
	t, ok := c.Topic(name)
	if !ok {
		protocol.WriteError(w, protocol.Errorf(404, protocol.CodeUnknownTopic, "unknown topic %q", name))
		return
	}
	protocol.WriteJSON(w, 200, t)
}

func (c *Controller) handleDeleteTopic(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	name := r.PathValue("topic")
	if !c.authorize(w, p, auth.OpAdmin, name) {
		return
	}
	if err := c.DeleteTopic(name); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(204)
}

func (c *Controller) handleAlterConfig(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	name := r.PathValue("topic")
	if !c.authorize(w, p, auth.OpAdmin, name) {
		return
	}
	var patch json.RawMessage
	if e := transport.ReadJSON(w, r, maxBody, &patch); e != nil {
		protocol.WriteError(w, e)
		return
	}
	t, err := c.AlterConfig(name, patch)
	if err != nil {
		writeErr(w, err)
		return
	}
	protocol.WriteJSON(w, 200, t)
}

func (c *Controller) handleAddPartitions(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	name := r.PathValue("topic")
	if !c.authorize(w, p, auth.OpAdmin, name) {
		return
	}
	var req protocol.AddPartitionsRequest
	if e := transport.ReadJSON(w, r, maxBody, &req); e != nil {
		protocol.WriteError(w, e)
		return
	}
	t, err := c.AddPartitions(name, req.TotalPartitions)
	if err != nil {
		writeErr(w, err)
		return
	}
	protocol.WriteJSON(w, 200, t)
}

func (c *Controller) handlePreferredElection(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	protocol.WriteJSON(w, 200, map[string]int{"leaders_moved": c.PreferredElection()})
}

// --- groups ----------------------------------------------------------------

func (c *Controller) authorizeTopics(w http.ResponseWriter, p auth.Principal, topics []string) bool {
	for _, t := range topics {
		if !c.authorize(w, p, auth.OpConsume, t) {
			return false
		}
	}
	return true
}

func (c *Controller) handleListGroups(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	protocol.WriteJSON(w, 200, c.coord.Groups())
}

func (c *Controller) handleDescribeGroup(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	d, ok := c.coord.Describe(r.PathValue("group"))
	if !ok {
		protocol.WriteError(w, protocol.Errorf(404, protocol.CodeUnknownMember, "unknown group"))
		return
	}
	protocol.WriteJSON(w, 200, d)
}

func (c *Controller) handleJoin(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	var req protocol.JoinGroupRequest
	if e := transport.ReadJSON(w, r, maxBody, &req); e != nil {
		protocol.WriteError(w, e)
		return
	}
	if !c.authorizeTopics(w, p, req.Topics) {
		return
	}
	for _, t := range req.Topics {
		if _, ok := c.Topic(t); !ok {
			protocol.WriteError(w, protocol.Errorf(404, protocol.CodeUnknownTopic, "unknown topic %q", t))
			return
		}
	}
	resp, err := c.coord.Join(r.Context(), r.PathValue("group"), req)
	if err != nil {
		writeErr(w, err)
		return
	}
	protocol.WriteJSON(w, 200, resp)
}

func (c *Controller) handleGroupHeartbeat(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	var req protocol.GroupHeartbeatRequest
	if e := transport.ReadJSON(w, r, maxBody, &req); e != nil {
		protocol.WriteError(w, e)
		return
	}
	resp, err := c.coord.Heartbeat(r.PathValue("group"), req)
	if err != nil {
		writeErr(w, err)
		return
	}
	protocol.WriteJSON(w, 200, resp)
}

func (c *Controller) handleLeave(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	var req protocol.LeaveGroupRequest
	if e := transport.ReadJSON(w, r, maxBody, &req); e != nil {
		protocol.WriteError(w, e)
		return
	}
	if err := c.coord.Leave(r.PathValue("group"), req.MemberID); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(204)
}

func (c *Controller) handleCommit(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	var req protocol.CommitOffsetsRequest
	if e := transport.ReadJSON(w, r, maxBody, &req); e != nil {
		protocol.WriteError(w, e)
		return
	}
	seen := map[string]bool{}
	var topics []string
	for _, o := range req.Offsets {
		if !seen[o.Topic] {
			seen[o.Topic] = true
			topics = append(topics, o.Topic)
		}
	}
	if !c.authorizeTopics(w, p, topics) {
		return
	}
	if err := c.coord.Commit(r.PathValue("group"), req); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(204)
}

func (c *Controller) handleFetchOffsets(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	g := r.PathValue("group")
	protocol.WriteJSON(w, 200, protocol.GroupOffsetsResponse{Group: g, Offsets: c.coord.Offsets(g)})
}

// PartitionOffsets asks the current leader for a partition's offsets.
func (c *Controller) PartitionOffsets(ctx context.Context, topic string, p int32, ts int64) (protocol.OffsetsResponse, error) {
	md := c.Metadata(ctx, 0, 0)
	u, _, ok := md.LeaderURL(topic, p)
	if !ok {
		return protocol.OffsetsResponse{}, fmt.Errorf("%s-%d has no leader", topic, p)
	}
	q := ""
	if ts >= 0 {
		q = "?timestamp=" + strconv.FormatInt(ts, 10)
	}
	var out protocol.OffsetsResponse
	err := transport.DoJSON(ctx, c.cfg.HTTPClient, "GET",
		fmt.Sprintf("%s/v1/topics/%s/partitions/%d/offsets%s", u, url.PathEscape(topic), p, q), nil, &out)
	return out, err
}

// Lag computes consumer lag for a group: high-water mark minus committed
// offset, per partition. Lag is measured against the HWM (not the LEO)
// because consumers can only ever read up to the HWM.
func (c *Controller) Lag(ctx context.Context, group string) protocol.GroupLagResponse {
	committed := map[tp]int64{}
	for _, o := range c.coord.Offsets(group) {
		committed[tp{o.Topic, o.Partition}] = o.Offset
	}
	owners := c.coord.Owners(group)
	set := map[tp]bool{}
	for k := range committed {
		set[k] = true
	}
	for k := range owners {
		set[tp{k.Topic, k.Partition}] = true
	}
	ownerOf := map[tp]string{}
	for k, m := range owners {
		ownerOf[tp{k.Topic, k.Partition}] = m
	}
	resp := protocol.GroupLagResponse{Group: group}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for k := range set {
		wg.Add(1)
		sem <- struct{}{}
		go func(k tp) {
			defer wg.Done()
			defer func() { <-sem }()
			pl := protocol.PartitionLag{Topic: k.topic, Partition: k.p, Committed: -1, Owner: ownerOf[k]}
			if off, ok := committed[k]; ok {
				pl.Committed = off
			}
			o, err := c.PartitionOffsets(ctx, k.topic, k.p, -1)
			if err != nil {
				pl.Error = err.Error()
			} else {
				pl.HighWatermark = o.HighWatermark
				base := pl.Committed
				if base < 0 {
					base = o.LogStartOffset
				}
				if base < o.LogStartOffset {
					base = o.LogStartOffset // committed offset was deleted by retention
				}
				pl.Lag = o.HighWatermark - base
				if pl.Lag < 0 {
					pl.Lag = 0
				}
			}
			mu.Lock()
			resp.Partitions = append(resp.Partitions, pl)
			resp.TotalLag += pl.Lag
			mu.Unlock()
		}(k)
	}
	wg.Wait()
	sortLag(resp.Partitions)
	return resp
}

type tp struct {
	topic string
	p     int32
}

func (c *Controller) handleLag(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	protocol.WriteJSON(w, 200, c.Lag(ctx, r.PathValue("group")))
}

// handleReset rewinds or fast-forwards a group's committed offsets. This is
// replay: "reprocess everything since 2026-09-01" becomes a reset to the
// first offset at or after that timestamp in every partition.
func (c *Controller) handleReset(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	group := r.PathValue("group")
	var req protocol.ResetOffsetsRequest
	if e := transport.ReadJSON(w, r, maxBody, &req); e != nil {
		protocol.WriteError(w, e)
		return
	}
	if !c.authorize(w, p, auth.OpConsume, req.Topic) {
		return
	}
	t, ok := c.Topic(req.Topic)
	if !ok {
		protocol.WriteError(w, protocol.Errorf(404, protocol.CodeUnknownTopic, "unknown topic %q", req.Topic))
		return
	}
	if !c.coord.IsEmpty(group) {
		protocol.WriteError(w, protocol.Errorf(409, protocol.CodeGroupNotEmpty,
			"group %s has active members; stop consumers before resetting offsets", group))
		return
	}
	ts := int64(-1)
	if req.To == "timestamp" {
		v, err := parseTimestamp(req.Timestamp)
		if err != nil {
			protocol.WriteError(w, protocol.Errorf(400, protocol.CodeInvalidRequest, "%v", err))
			return
		}
		ts = v
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	var offs []protocol.OffsetCommit
	for i := range t.Partitions {
		o, err := c.PartitionOffsets(ctx, req.Topic, int32(i), ts)
		if err != nil {
			protocol.WriteError(w, protocol.Errorf(503, protocol.CodeUnavailable, "partition %d: %v", i, err))
			return
		}
		var target int64
		switch req.To {
		case "earliest":
			target = o.LogStartOffset
		case "latest":
			target = o.HighWatermark
		case "offset":
			target = min(max(req.Offset, o.LogStartOffset), o.HighWatermark)
		case "timestamp":
			target = o.OffsetForTimestamp
		default:
			protocol.WriteError(w, protocol.Errorf(400, protocol.CodeInvalidRequest, "to must be earliest|latest|offset|timestamp"))
			return
		}
		offs = append(offs, protocol.OffsetCommit{Topic: req.Topic, Partition: int32(i), Offset: target,
			Metadata: "reset:" + req.To})
	}
	if !req.DryRun {
		if err := c.coord.Commit(group, protocol.CommitOffsetsRequest{Generation: -1, Offsets: offs}); err != nil {
			writeErr(w, err)
			return
		}
		c.log.Info("consumer group offsets reset", "audit", true, "principal", p.Name, "group", group, "topic", req.Topic, "to", req.To)
	}
	protocol.WriteJSON(w, 200, protocol.GroupOffsetsResponse{Group: group, Offsets: offs})
}

func parseTimestamp(s string) (int64, error) {
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return v, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0, fmt.Errorf("timestamp must be RFC 3339 or unix milliseconds: %q", s)
	}
	return t.UnixMilli(), nil
}

func sortLag(s []protocol.PartitionLag) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0; j-- {
			a, b := s[j-1], s[j]
			if a.Topic < b.Topic || (a.Topic == b.Topic && a.Partition <= b.Partition) {
				break
			}
			s[j-1], s[j] = b, a
		}
	}
}
