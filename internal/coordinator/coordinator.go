// Package coordinator implements consumer groups: membership, heartbeats,
// partition assignment, generation-based fencing, and committed offsets.
//
// Rebalance protocol (eager, simplified):
//
//	Stable(gen N) --member joins/leaves/expires/changes subscription-->
//	PreparingRebalance: every heartbeat answers REBALANCE_IN_PROGRESS.
//	  Members stop fetching, commit what they processed, and call Join.
//	  Join blocks until all members rejoined (or the rebalance timeout
//	  kicks the stragglers), and at least InitialRebalanceDelay passed
//	  (coalescing a burst of joins into one rebalance).
//	--> Stable(gen N+1) with a new assignment returned from every Join.
//
// This is "stop the world": no member of the group consumes during the
// rebalance. Cooperative rebalancing (only revoke partitions that actually
// move) is discussed in docs/tradeoffs.md; the sticky assignor here
// minimizes movement, which is the prerequisite for it.
//
// Fencing: commits carry the generation. A member that missed a rebalance
// (GC pause, network blip) holds an old generation and its commits are
// rejected, so a "zombie" cannot overwrite the progress of the partition's
// new owner.
package coordinator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/observability"
	"github.com/michael-emmanuel/eventstream/internal/protocol"
)

// Group states.
const (
	StateEmpty     = "Empty"
	StatePreparing = "PreparingRebalance"
	StateStable    = "Stable"
)

// Config configures the coordinator.
type Config struct {
	DataDir                 string
	DefaultSessionTimeout   time.Duration
	DefaultRebalanceTimeout time.Duration
	InitialRebalanceDelay   time.Duration
	MaxMembersPerGroup      int
	SyncOffsetWrites        bool
	TickInterval            time.Duration
	Now                     func() time.Time
	Logger                  *slog.Logger
	Metrics                 *observability.Registry
}

func (c *Config) defaults() {
	if c.DefaultSessionTimeout == 0 {
		c.DefaultSessionTimeout = 10 * time.Second
	}
	if c.DefaultRebalanceTimeout == 0 {
		c.DefaultRebalanceTimeout = 30 * time.Second
	}
	if c.InitialRebalanceDelay == 0 {
		c.InitialRebalanceDelay = 300 * time.Millisecond
	}
	if c.MaxMembersPerGroup == 0 {
		c.MaxMembersPerGroup = 1000
	}
	if c.TickInterval == 0 {
		c.TickInterval = 100 * time.Millisecond
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Logger == nil {
		c.Logger = observability.DiscardLogger()
	}
	if c.Metrics == nil {
		c.Metrics = observability.NewRegistry()
	}
}

type member struct {
	id               string
	clientID         string
	topics           []string
	sessionTimeout   time.Duration
	rebalanceTimeout time.Duration
	lastSeen         time.Time
	joined           bool
}

type group struct {
	name              string
	state             string
	generation        int32
	assignor          string
	members           map[string]*member
	assignment        map[string][]tp
	stableCh          chan struct{}
	rebalanceStart    time.Time
	rebalanceDeadline time.Time
	reason            string
	rebalances        int64
	lastRebalance     time.Duration
}

// Coordinator manages all consumer groups.
type Coordinator struct {
	cfg            Config
	store          *OffsetStore
	partitionCount func(topic string) int

	mu     sync.Mutex
	groups map[string]*group

	cancel context.CancelFunc
	wg     sync.WaitGroup

	mRebalances  *observability.CounterVec
	mRebalanceMs *observability.HistogramVec
	mCommits     *observability.CounterVec
	mExpired     *observability.CounterVec
}

// New creates a coordinator. partitionCount returns the number of
// partitions of a topic (0 if unknown).
func New(cfg Config, partitionCount func(topic string) int) (*Coordinator, error) {
	cfg.defaults()
	store, err := OpenOffsetStore(cfg.DataDir, cfg.SyncOffsetWrites)
	if err != nil {
		return nil, err
	}
	c := &Coordinator{cfg: cfg, store: store, partitionCount: partitionCount, groups: map[string]*group{}}
	r := cfg.Metrics
	c.mRebalances = r.Counter("group_rebalances_total", "Completed consumer group rebalances.", "group")
	c.mRebalanceMs = r.Histogram("group_rebalance_duration_seconds", "Time from rebalance start to new stable generation.", nil, "group")
	c.mCommits = r.Counter("group_offset_commits_total", "Offset commit requests.", "group", "result")
	c.mExpired = r.Counter("group_members_expired_total", "Members removed after missing heartbeats.", "group")
	r.RegisterCollector(func(e *observability.Emitter) {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, g := range c.groups {
			e.Gauge("group_members", "Members per consumer group.", float64(len(g.members)), "group", g.name)
			e.Gauge("group_generation", "Current generation per group.", float64(g.generation), "group", g.name)
			v := 0.0
			if g.state == StatePreparing {
				v = 1
			}
			e.Gauge("group_rebalancing", "1 while the group is rebalancing.", v, "group", g.name)
		}
	})
	return c, nil
}

// Start runs the session-expiry loop until Stop.
func (c *Coordinator) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		t := time.NewTicker(c.cfg.TickInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				c.tick()
			}
		}
	}()
}

// Stop stops background work and closes the offset store.
func (c *Coordinator) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	c.mu.Lock()
	for _, g := range c.groups {
		if g.state == StatePreparing {
			close(g.stableCh)
			g.state = StateEmpty
		}
	}
	c.mu.Unlock()
	c.store.Close()
}

// Store exposes the offset store (read-only use by the controller).
func (c *Coordinator) Store() *OffsetStore { return c.store }

func errf(status int, code, format string, args ...any) *protocol.Error {
	return protocol.Errorf(status, code, format, args...)
}

func (c *Coordinator) groupLocked(name string) *group {
	g := c.groups[name]
	if g == nil {
		g = &group{name: name, state: StateEmpty, assignor: AssignorRange,
			members: map[string]*member{}, assignment: map[string][]tp{}}
		c.groups[name] = g
	}
	return g
}

func newMemberID(clientID string) string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	if clientID == "" {
		clientID = "consumer"
	}
	return clientID + "-" + hex.EncodeToString(b[:])
}

// Join adds or re-admits a member and blocks until the group is Stable,
// returning the member's assignment for the new generation.
func (c *Coordinator) Join(ctx context.Context, name string, req protocol.JoinGroupRequest) (protocol.JoinGroupResponse, error) {
	if name == "" || len(req.Topics) == 0 {
		return protocol.JoinGroupResponse{}, errf(400, protocol.CodeInvalidRequest, "group and topics are required")
	}
	if req.Assignor != "" && !validAssignor(req.Assignor) {
		return protocol.JoinGroupResponse{}, errf(400, protocol.CodeInvalidRequest, "unknown assignor %q", req.Assignor)
	}
	now := c.cfg.Now()
	c.mu.Lock()
	g := c.groupLocked(name)
	var m *member
	if req.MemberID == "" {
		if len(g.members) >= c.cfg.MaxMembersPerGroup {
			c.mu.Unlock()
			return protocol.JoinGroupResponse{}, errf(429, protocol.CodeQuotaExceeded, "group %s has %d members", name, len(g.members))
		}
		if len(g.members) == 0 && req.Assignor != "" {
			g.assignor = req.Assignor
		}
		m = &member{id: newMemberID(req.ClientID), clientID: req.ClientID}
		g.members[m.id] = m
		c.prepareLocked(g, now, "member joined: "+m.id)
	} else {
		m = g.members[req.MemberID]
		if m == nil {
			c.mu.Unlock()
			return protocol.JoinGroupResponse{}, errf(409, protocol.CodeUnknownMember, "member %s is not in group %s (expired or removed); rejoin without a member ID", req.MemberID, name)
		}
		if !sameStrings(m.topics, req.Topics) {
			c.prepareLocked(g, now, "subscription changed: "+m.id)
		}
	}
	m.topics = append([]string(nil), req.Topics...)
	sort.Strings(m.topics)
	m.sessionTimeout = durOr(req.SessionTimeoutMs, c.cfg.DefaultSessionTimeout)
	m.rebalanceTimeout = durOr(req.RebalanceTimeoutMs, c.cfg.DefaultRebalanceTimeout)
	m.lastSeen = now
	m.joined = true
	if g.state == StateStable {
		resp := c.responseLocked(g, m)
		c.mu.Unlock()
		return resp, nil
	}
	c.maybeCompleteLocked(g, now)
	ch := g.stableCh
	c.mu.Unlock()

	if ch != nil {
		wait, cancel := context.WithTimeout(ctx, m.rebalanceTimeout+time.Second)
		defer cancel()
		select {
		case <-ch:
		case <-wait.Done():
			return protocol.JoinGroupResponse{}, errf(503, protocol.CodeRebalance, "rebalance did not complete in time")
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if g.members[m.id] == nil {
		return protocol.JoinGroupResponse{}, errf(409, protocol.CodeUnknownMember, "member %s was removed during the rebalance", m.id)
	}
	if g.state != StateStable {
		return protocol.JoinGroupResponse{}, errf(409, protocol.CodeRebalance, "another rebalance started; rejoin")
	}
	return c.responseLocked(g, m), nil
}

func (c *Coordinator) responseLocked(g *group, m *member) protocol.JoinGroupResponse {
	return protocol.JoinGroupResponse{
		MemberID:   m.id,
		Generation: g.generation,
		Assignment: append([]tp(nil), g.assignment[m.id]...),
		Members:    len(g.members),
	}
}

func durOr(ms int64, def time.Duration) time.Duration {
	if ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}
	return def
}

// prepareLocked moves the group into PreparingRebalance.
func (c *Coordinator) prepareLocked(g *group, now time.Time, reason string) {
	if g.state == StatePreparing {
		return
	}
	g.state = StatePreparing
	g.stableCh = make(chan struct{})
	g.rebalanceStart = now
	g.reason = reason
	timeout := c.cfg.DefaultRebalanceTimeout
	for _, m := range g.members {
		m.joined = false
		if m.rebalanceTimeout > timeout {
			timeout = m.rebalanceTimeout
		}
	}
	g.rebalanceDeadline = now.Add(timeout)
	c.cfg.Logger.Info("rebalance started", "group", g.name, "generation", g.generation, "reason", reason, "members", len(g.members))
}

// maybeCompleteLocked finishes the rebalance once everyone has rejoined (and
// the coalescing delay passed), or kicks stragglers at the deadline.
func (c *Coordinator) maybeCompleteLocked(g *group, now time.Time) {
	if g.state != StatePreparing {
		return
	}
	all := true
	for _, m := range g.members {
		if !m.joined {
			all = false
			break
		}
	}
	if all && now.Sub(g.rebalanceStart) >= c.cfg.InitialRebalanceDelay {
		c.completeLocked(g, now)
		return
	}
	if now.After(g.rebalanceDeadline) {
		for id, m := range g.members {
			if !m.joined {
				c.cfg.Logger.Warn("member failed to rejoin before rebalance timeout", "group", g.name, "member", id)
				delete(g.members, id)
			}
		}
		c.completeLocked(g, now)
	}
}

func (c *Coordinator) completeLocked(g *group, now time.Time) {
	prev := g.assignment
	g.generation++
	if len(g.members) == 0 {
		g.state = StateEmpty
		g.assignment = map[string][]tp{}
	} else {
		in := assignInput{subs: map[string][]string{}, partitions: map[string]int{}, prev: prev}
		for id, m := range g.members {
			in.members = append(in.members, id)
			in.subs[id] = m.topics
			for _, t := range m.topics {
				in.partitions[t] = c.partitionCount(t)
			}
		}
		sort.Strings(in.members)
		g.assignment = assign(g.assignor, in)
		g.state = StateStable
	}
	g.rebalances++
	g.lastRebalance = now.Sub(g.rebalanceStart)
	c.mRebalances.With(g.name).Inc()
	c.mRebalanceMs.With(g.name).Observe(g.lastRebalance.Seconds())
	if g.stableCh != nil {
		close(g.stableCh)
		g.stableCh = nil
	}
	c.cfg.Logger.Info("rebalance completed", "group", g.name, "generation", g.generation,
		"members", len(g.members), "partitions_moved", moved(prev, g.assignment),
		"duration_ms", g.lastRebalance.Milliseconds(), "reason", g.reason)
}

// Heartbeat refreshes a member's session and tells it whether to rejoin.
func (c *Coordinator) Heartbeat(name string, req protocol.GroupHeartbeatRequest) (protocol.GroupHeartbeatResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[name]
	if g == nil || g.members[req.MemberID] == nil {
		return protocol.GroupHeartbeatResponse{}, errf(409, protocol.CodeUnknownMember, "unknown member %s", req.MemberID)
	}
	g.members[req.MemberID].lastSeen = c.cfg.Now()
	resp := protocol.GroupHeartbeatResponse{Generation: g.generation, State: g.state}
	if g.state == StatePreparing {
		return resp, errf(409, protocol.CodeRebalance, "group is rebalancing; rejoin")
	}
	if req.Generation != g.generation {
		return resp, errf(409, protocol.CodeIllegalGeneration, "generation %d is stale (current %d)", req.Generation, g.generation)
	}
	return resp, nil
}

// Leave removes a member immediately (graceful shutdown), which triggers a
// rebalance without waiting for the session timeout.
func (c *Coordinator) Leave(name, memberID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[name]
	if g == nil || g.members[memberID] == nil {
		return errf(409, protocol.CodeUnknownMember, "unknown member %s", memberID)
	}
	now := c.cfg.Now()
	delete(g.members, memberID)
	c.prepareLocked(g, now, "member left: "+memberID)
	c.maybeCompleteLocked(g, now)
	return nil
}

func (c *Coordinator) tick() {
	now := c.cfg.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, g := range c.groups {
		for id, m := range g.members {
			// A member blocked in Join during a rebalance is alive by
			// definition; it is bounded by the rebalance timeout instead.
			if g.state == StatePreparing && m.joined {
				continue
			}
			if now.Sub(m.lastSeen) > m.sessionTimeout {
				c.cfg.Logger.Warn("member session expired", "group", g.name, "member", id,
					"silent_ms", now.Sub(m.lastSeen).Milliseconds())
				c.mExpired.With(g.name).Inc()
				delete(g.members, id)
				c.prepareLocked(g, now, "member expired: "+id)
			}
		}
		c.maybeCompleteLocked(g, now)
	}
}

// Commit stores offsets after validating the member's generation and
// ownership. It holds the coordinator lock while writing so a fenced
// commit can never land after a newer owner's commit.
func (c *Coordinator) Commit(name string, req protocol.CommitOffsetsRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[name]
	if req.MemberID == "" && req.Generation < 0 {
		if g != nil && len(g.members) > 0 {
			c.mCommits.With(name, "rejected").Inc()
			return errf(409, protocol.CodeGroupNotEmpty, "group %s has %d active members; stop them before an administrative commit", name, len(g.members))
		}
	} else {
		if g == nil || g.members[req.MemberID] == nil {
			c.mCommits.With(name, "rejected").Inc()
			return errf(409, protocol.CodeUnknownMember, "unknown member %s", req.MemberID)
		}
		if req.Generation != g.generation {
			c.mCommits.With(name, "fenced").Inc()
			return errf(409, protocol.CodeIllegalGeneration, "commit from generation %d rejected (current %d)", req.Generation, g.generation)
		}
		owned := map[tp]bool{}
		for _, x := range g.assignment[req.MemberID] {
			owned[x] = true
		}
		for _, o := range req.Offsets {
			if !owned[tp{Topic: o.Topic, Partition: o.Partition}] {
				c.mCommits.With(name, "rejected").Inc()
				return errf(409, protocol.CodeNotOwner, "member %s does not own %s-%d", req.MemberID, o.Topic, o.Partition)
			}
		}
	}
	for _, o := range req.Offsets {
		if o.Offset < 0 {
			return errf(400, protocol.CodeInvalidRequest, "negative offset")
		}
	}
	if err := c.store.Commit(name, req.Offsets); err != nil {
		c.mCommits.With(name, "error").Inc()
		return errf(http.StatusInternalServerError, protocol.CodeStorage, "%v", err)
	}
	c.mCommits.With(name, "ok").Inc()
	return nil
}

// Offsets returns committed offsets.
func (c *Coordinator) Offsets(name string) []protocol.OffsetCommit { return c.store.Get(name) }

// Describe returns a group description.
func (c *Coordinator) Describe(name string) (protocol.GroupDescription, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[name]
	if g == nil {
		if len(c.store.Get(name)) > 0 {
			return protocol.GroupDescription{Group: name, State: StateEmpty}, true
		}
		return protocol.GroupDescription{}, false
	}
	d := protocol.GroupDescription{Group: name, State: g.state, Generation: g.generation, Assignor: g.assignor,
		Rebalances: g.rebalances, LastRebalanceMs: g.lastRebalance.Milliseconds()}
	ids := make([]string, 0, len(g.members))
	for id := range g.members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		m := g.members[id]
		d.Members = append(d.Members, protocol.GroupMember{MemberID: id, ClientID: m.clientID, Topics: m.topics,
			Assignment: append([]tp(nil), g.assignment[id]...)})
	}
	return d, true
}

// Owners maps each assigned partition to its member ID.
func (c *Coordinator) Owners(name string) map[tp]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[tp]string{}
	if g := c.groups[name]; g != nil {
		for m, ps := range g.assignment {
			for _, x := range ps {
				out[x] = m
			}
		}
	}
	return out
}

// Groups lists known groups (active or with committed offsets).
func (c *Coordinator) Groups() []string {
	set := map[string]bool{}
	for _, g := range c.store.Groups() {
		set[g] = true
	}
	c.mu.Lock()
	for g := range c.groups {
		set[g] = true
	}
	c.mu.Unlock()
	out := make([]string, 0, len(set))
	for g := range set {
		out = append(out, g)
	}
	sort.Strings(out)
	return out
}

// IsEmpty reports whether a group has no active members.
func (c *Coordinator) IsEmpty(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[name]
	return g == nil || len(g.members) == 0
}

// ErrUnknownGroup is returned for unknown groups.
var ErrUnknownGroup = errors.New("unknown group")

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	x := append([]string(nil), a...)
	y := append([]string(nil), b...)
	sort.Strings(x)
	sort.Strings(y)
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}
