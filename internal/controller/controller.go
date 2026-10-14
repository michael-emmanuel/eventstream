// Package controller is the cluster's metadata authority: broker
// membership and liveness, topic and partition assignment, leader election,
// in-sync replica sets, and producer ID allocation. It also hosts the
// consumer group coordinator.
//
// What this is, honestly: a single process that persists metadata to a
// local file with atomic rename. It is NOT replicated and does NOT run Raft
// or any other consensus protocol. If it is down, the data plane keeps
// serving existing leaders until their leases expire, but no failover,
// topic change or consumer rebalance can happen. Production systems put
// this state in a consensus-backed store (Kafka KRaft, ZooKeeper, etcd);
// docs/distributed-consistency.md explains what that buys and what this
// design gives up.
//
// What it does guarantee: every leadership change increments the
// partition's leader epoch, the epoch is the fencing token checked by every
// replica, and a broker loses its lease (and stops accepting writes) before
// the controller considers it dead and elects a replacement.
package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/auth"
	"github.com/michael-emmanuel/eventstream/internal/coordinator"
	"github.com/michael-emmanuel/eventstream/internal/metadata"
	"github.com/michael-emmanuel/eventstream/internal/observability"
	"github.com/michael-emmanuel/eventstream/internal/protocol"
)

// Config configures the controller.
type Config struct {
	DataDir      string
	AdvertiseURL string
	// SessionTimeout: a broker silent for this long is declared dead.
	SessionTimeout time.Duration
	// LeaseDuration is granted to brokers on each heartbeat; it must be
	// shorter than SessionTimeout so an isolated leader stops accepting
	// writes before a replacement is elected.
	LeaseDuration          time.Duration
	MaxPartitionsPerTenant int
	Coordinator            coordinator.Config
	HTTPClient             *http.Client
	Auth                   *auth.Config
	Logger                 *slog.Logger
	Metrics                *observability.Registry
	Now                    func() time.Time
}

func (c *Config) defaults() {
	if c.SessionTimeout == 0 {
		c.SessionTimeout = 10 * time.Second
	}
	if c.LeaseDuration == 0 || c.LeaseDuration >= c.SessionTimeout {
		c.LeaseDuration = c.SessionTimeout * 6 / 10
	}
	if c.Logger == nil {
		c.Logger = observability.DiscardLogger()
	}
	if c.Metrics == nil {
		c.Metrics = observability.NewRegistry()
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}
	if c.Auth == nil {
		c.Auth = &auth.Config{}
	}
	if c.Coordinator.DataDir == "" {
		c.Coordinator.DataDir = filepath.Join(c.DataDir, "groups")
	}
	c.Coordinator.Logger = c.Logger
	c.Coordinator.Metrics = c.Metrics
}

// state is what the controller persists.
type state struct {
	Version        int64                      `json:"version"`
	NextProducerID int64                      `json:"next_producer_id"`
	Brokers        map[int32]*metadata.Broker `json:"brokers"`
	Topics         map[string]*metadata.Topic `json:"topics"`
}

type tpKey struct {
	topic string
	p     int32
}

// Controller is the metadata authority.
type Controller struct {
	cfg   Config
	log   *slog.Logger
	coord *coordinator.Coordinator

	mu      sync.Mutex
	st      state
	lastHB  map[int32]time.Time
	offline map[int32]map[tpKey]bool
	changed chan struct{}

	cancel context.CancelFunc
	wg     sync.WaitGroup

	mElections *observability.CounterVec
	mHB        *observability.CounterVec
}

// New loads persisted state (if any) and creates the controller.
func New(cfg Config) (*Controller, error) {
	cfg.defaults()
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	c := &Controller{
		cfg: cfg, log: cfg.Logger,
		st:      state{NextProducerID: 1, Brokers: map[int32]*metadata.Broker{}, Topics: map[string]*metadata.Topic{}},
		lastHB:  map[int32]time.Time{},
		offline: map[int32]map[tpKey]bool{},
		changed: make(chan struct{}),
	}
	if b, err := os.ReadFile(c.statePath()); err == nil {
		if err := json.Unmarshal(b, &c.st); err != nil {
			return nil, fmt.Errorf("load controller state: %w", err)
		}
		if c.st.Brokers == nil {
			c.st.Brokers = map[int32]*metadata.Broker{}
		}
		if c.st.Topics == nil {
			c.st.Topics = map[string]*metadata.Topic{}
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	// After a controller restart we do not know who is alive. Give every
	// known broker a full session timeout to check in before declaring it
	// dead, rather than triggering a mass failover.
	now := cfg.Now()
	for id := range c.st.Brokers {
		c.lastHB[id] = now
	}
	coord, err := coordinator.New(cfg.Coordinator, c.partitionCount)
	if err != nil {
		return nil, err
	}
	c.coord = coord
	c.registerMetrics()
	return c, nil
}

func (c *Controller) statePath() string { return filepath.Join(c.cfg.DataDir, "cluster.json") }

// Coordinator returns the consumer group coordinator.
func (c *Controller) Coordinator() *coordinator.Coordinator { return c.coord }

// Start runs background loops (broker session expiry, group expiry).
func (c *Controller) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.coord.Start()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		t := time.NewTicker(c.cfg.SessionTimeout / 10)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				c.expireBrokers()
			}
		}
	}()
}

// Stop stops background loops.
func (c *Controller) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	c.coord.Stop()
}

func (c *Controller) registerMetrics() {
	r := c.cfg.Metrics
	c.mElections = r.Counter("controller_leader_elections_total", "Partition leader changes.", "reason")
	c.mHB = r.Counter("controller_broker_heartbeats_total", "Broker heartbeats received.", "broker")
	r.RegisterCollector(func(e *observability.Emitter) {
		c.mu.Lock()
		defer c.mu.Unlock()
		alive := 0
		for _, b := range c.st.Brokers {
			v := 0.0
			if b.Alive {
				alive++
				v = 1
			}
			e.Gauge("controller_broker_alive", "1 if the broker holds a live session.", v, "broker", fmt.Sprint(b.ID))
		}
		e.Gauge("controller_brokers_alive", "Brokers with live sessions.", float64(alive))
		md := c.snapshotLocked()
		under, offline := md.UnderReplicated()
		e.Gauge("controller_under_replicated_partitions", "Partitions whose ISR is smaller than the replica set.", float64(under))
		e.Gauge("controller_offline_partitions", "Partitions without a leader.", float64(offline))
		e.Gauge("controller_metadata_version", "Metadata version.", float64(c.st.Version))
		n := 0
		for _, t := range c.st.Topics {
			n += len(t.Partitions)
		}
		e.Gauge("controller_partitions", "Total partitions.", float64(n))
	})
}

// --- persistence and change notification -----------------------------------

// commitLocked bumps the version, persists state and wakes metadata
// watchers. Persistence uses write-to-temp + fsync + rename so a crash
// leaves either the old or the new state, never a mix.
func (c *Controller) commitLocked() {
	c.st.Version++
	b, err := json.MarshalIndent(c.st, "", "  ")
	if err == nil {
		err = writeFileAtomic(c.statePath(), b)
	}
	if err != nil {
		// Continuing with in-memory state that is not durable would let a
		// controller restart roll back leader epochs, which breaks fencing.
		c.log.Error("FATAL: cannot persist controller state", "error", err)
		panic(fmt.Sprintf("controller state not durable: %v", err))
	}
	close(c.changed)
	c.changed = make(chan struct{})
}

func writeFileAtomic(path string, b []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Metadata returns the current snapshot, waiting up to wait for a version
// greater than or equal to minVersion (long polling). Brokers use this to
// learn about leadership changes within milliseconds instead of polling.
func (c *Controller) Metadata(ctx context.Context, minVersion int64, wait time.Duration) *metadata.Cluster {
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	for {
		c.mu.Lock()
		if c.st.Version >= minVersion || wait <= 0 {
			md := c.snapshotLocked()
			c.mu.Unlock()
			return md
		}
		ch := c.changed
		c.mu.Unlock()
		select {
		case <-ch:
		case <-deadline.C:
			wait = 0
		case <-ctx.Done():
			wait = 0
		}
	}
}

func (c *Controller) snapshotLocked() *metadata.Cluster {
	md := &metadata.Cluster{Version: c.st.Version, ControllerURL: c.cfg.AdvertiseURL, CoordinatorURL: c.cfg.AdvertiseURL}
	for _, b := range c.st.Brokers {
		md.Brokers = append(md.Brokers, *b)
	}
	sort.Slice(md.Brokers, func(i, j int) bool { return md.Brokers[i].ID < md.Brokers[j].ID })
	for _, t := range c.st.Topics {
		ct := *t
		ct.Partitions = make([]metadata.PartitionState, len(t.Partitions))
		for i, p := range t.Partitions {
			p.Replicas = append([]int32(nil), p.Replicas...)
			p.ISR = append([]int32(nil), p.ISR...)
			ct.Partitions[i] = p
		}
		md.Topics = append(md.Topics, ct)
	}
	sort.Slice(md.Topics, func(i, j int) bool { return md.Topics[i].Name < md.Topics[j].Name })
	return md
}

func (c *Controller) partitionCount(topic string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t := c.st.Topics[topic]; t != nil {
		return len(t.Partitions)
	}
	return 0
}

// --- brokers, liveness and elections ---------------------------------------

// Heartbeat registers a broker or renews its session, and returns a lease.
func (c *Controller) Heartbeat(req protocol.BrokerHeartbeatRequest) (protocol.BrokerHeartbeatResponse, error) {
	if req.BrokerID < 0 || req.URL == "" || req.Incarnation == "" {
		return protocol.BrokerHeartbeatResponse{}, protocol.Errorf(400, protocol.CodeInvalidRequest, "broker_id, url and incarnation are required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.cfg.Now()
	c.mHB.With(fmt.Sprint(req.BrokerID)).Inc()
	b, known := c.st.Brokers[req.BrokerID]
	changed := false
	reported := map[tpKey]bool{}
	for _, tp := range req.OfflinePartitions {
		reported[tpKey{tp.Topic, tp.Partition}] = true
	}
	switch {
	case !known || !b.Alive || b.Incarnation != req.Incarnation:
		if known && b.Alive && b.Incarnation != req.Incarnation {
			// The broker restarted faster than the session timeout. It may
			// have lost unflushed writes, so treat the restart as a failure:
			// move leadership away and make it rejoin the ISR by catching up.
			c.log.Warn("broker restarted with a new incarnation", "broker", req.BrokerID)
			c.brokerFailedLocked(req.BrokerID, "restart")
		}
		c.st.Brokers[req.BrokerID] = &metadata.Broker{ID: req.BrokerID, URL: req.URL, Alive: true, Incarnation: req.Incarnation}
		c.offline[req.BrokerID] = reported
		c.log.Info("broker registered", "broker", req.BrokerID, "url", req.URL, "incarnation", req.Incarnation)
		c.electOfflineLocked("broker_registered")
		changed = true
	default:
		if b.URL != req.URL {
			b.URL = req.URL
			changed = true
		}
		prev := c.offline[req.BrokerID]
		c.offline[req.BrokerID] = reported // must be set before re-electing
		for k := range reported {
			if !prev[k] {
				c.log.Warn("broker reports partition offline (storage failure)", "broker", req.BrokerID, "topic", k.topic, "partition", k.p)
				c.replicaFailedLocked(req.BrokerID, k, "storage_failure")
				changed = true
			}
		}
	}
	c.lastHB[req.BrokerID] = now
	if changed {
		c.commitLocked()
	}
	return protocol.BrokerHeartbeatResponse{LeaseMs: c.cfg.LeaseDuration.Milliseconds(), MetadataVersion: c.st.Version}, nil
}

func (c *Controller) expireBrokers() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.cfg.Now()
	changed := false
	for id, b := range c.st.Brokers {
		if b.Alive && now.Sub(c.lastHB[id]) > c.cfg.SessionTimeout {
			c.log.Warn("broker session expired", "broker", id, "silent_ms", now.Sub(c.lastHB[id]).Milliseconds())
			c.brokerFailedLocked(id, "session_expired")
			changed = true
		}
	}
	if changed {
		c.commitLocked()
	}
}

func (c *Controller) eligibleLocked(id int32, k tpKey) bool {
	b := c.st.Brokers[id]
	return b != nil && b.Alive && !c.offline[id][k]
}

// brokerFailedLocked marks a broker dead, re-elects leaders it held, and
// removes it from ISRs it was part of.
func (c *Controller) brokerFailedLocked(id int32, reason string) {
	if b := c.st.Brokers[id]; b != nil {
		b.Alive = false
	}
	for _, t := range c.st.Topics {
		for i := range t.Partitions {
			k := tpKey{t.Name, int32(i)}
			if contains(t.Partitions[i].Replicas, id) {
				c.replicaFailedLocked(id, k, reason)
			}
		}
	}
}

// replicaFailedLocked handles the loss of one replica of one partition.
func (c *Controller) replicaFailedLocked(id int32, k tpKey, reason string) {
	t := c.st.Topics[k.topic]
	if t == nil || int(k.p) >= len(t.Partitions) {
		return
	}
	ps := &t.Partitions[k.p]
	if ps.Leader == id {
		c.electLocked(t, ps, k, reason)
		return
	}
	// A dead follower can safely leave the ISR: the HWM then advances
	// without it. It rejoins once it catches up.
	if contains(ps.ISR, id) && len(ps.ISR) > 1 {
		ps.ISR = remove(ps.ISR, id)
	}
}

// electLocked chooses a new leader from the ISR (clean election). If none
// is eligible and the topic allows it, it falls back to any live replica
// (unclean election: acknowledged data may be lost). Otherwise the partition
// goes offline: unavailable, but nothing acknowledged is lost.
func (c *Controller) electLocked(t *metadata.Topic, ps *metadata.PartitionState, k tpKey, reason string) {
	leader := int32(-1)
	for _, r := range ps.Replicas { // replica order = preference order
		if contains(ps.ISR, r) && c.eligibleLocked(r, k) {
			leader = r
			break
		}
	}
	unclean := false
	if leader < 0 && t.Config.UncleanLeaderElection {
		for _, r := range ps.Replicas {
			if c.eligibleLocked(r, k) {
				leader, unclean = r, true
				break
			}
		}
	}
	old := ps.Leader
	ps.LeaderEpoch++
	ps.Leader = leader
	switch {
	case unclean:
		ps.ISR = []int32{leader}
		c.log.Error("UNCLEAN leader election: acknowledged records may be lost",
			"topic", k.topic, "partition", k.p, "leader", leader, "epoch", ps.LeaderEpoch)
		c.mElections.With("unclean").Inc()
	case leader >= 0:
		var isr []int32
		for _, r := range ps.ISR {
			if c.eligibleLocked(r, k) {
				isr = append(isr, r)
			}
		}
		ps.ISR = isr
		c.mElections.With(reason).Inc()
	default:
		// Keep the last ISR so that when one of its members returns, a
		// clean election is possible.
		c.log.Error("partition offline: no in-sync replica available",
			"topic", k.topic, "partition", k.p, "isr", ps.ISR, "epoch", ps.LeaderEpoch)
		c.mElections.With("offline").Inc()
	}
	c.log.Info("leader changed", "topic", k.topic, "partition", k.p, "old", old, "new", leader,
		"epoch", ps.LeaderEpoch, "isr", ps.ISR, "reason", reason)
}

func (c *Controller) electOfflineLocked(reason string) {
	for _, t := range c.st.Topics {
		for i := range t.Partitions {
			if t.Partitions[i].Leader < 0 {
				k := tpKey{t.Name, int32(i)}
				before := t.Partitions[i].LeaderEpoch
				c.electLocked(t, &t.Partitions[i], k, reason)
				if t.Partitions[i].Leader < 0 {
					// Still offline: undo the epoch bump so repeated attempts
					// do not churn epochs.
					t.Partitions[i].LeaderEpoch = before
				}
			}
		}
	}
}

// AlterISR applies an ISR change requested by a partition leader. The
// request is accepted only from the current leader in the current epoch:
// a deposed leader cannot shrink the ISR to exclude the new leader.
func (c *Controller) AlterISR(req protocol.AlterISRRequest) (metadata.PartitionState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.st.Topics[req.Topic]
	if t == nil || req.Partition < 0 || int(req.Partition) >= len(t.Partitions) {
		return metadata.PartitionState{}, protocol.Errorf(404, protocol.CodeUnknownTopic, "unknown partition %s-%d", req.Topic, req.Partition)
	}
	ps := &t.Partitions[req.Partition]
	if ps.Leader != req.LeaderID || ps.LeaderEpoch != req.LeaderEpoch {
		return *ps, protocol.Errorf(409, protocol.CodeStaleISRUpdate,
			"leader %d epoch %d is not current (leader %d epoch %d)", req.LeaderID, req.LeaderEpoch, ps.Leader, ps.LeaderEpoch)
	}
	if !contains(req.ISR, req.LeaderID) {
		return *ps, protocol.Errorf(400, protocol.CodeInvalidRequest, "ISR must contain the leader")
	}
	k := tpKey{req.Topic, req.Partition}
	var isr []int32
	for _, r := range req.ISR {
		if !contains(ps.Replicas, r) {
			return *ps, protocol.Errorf(400, protocol.CodeInvalidRequest, "broker %d is not a replica", r)
		}
		if c.eligibleLocked(r, k) {
			isr = append(isr, r)
		}
	}
	sort.Slice(isr, func(i, j int) bool { return isr[i] < isr[j] })
	if !equal(isr, ps.ISR) {
		c.log.Info("ISR changed", "topic", req.Topic, "partition", req.Partition, "old", ps.ISR, "new", isr)
		ps.ISR = isr
		c.commitLocked()
	}
	return *ps, nil
}

// PreferredElection moves leadership back to each partition's first
// replica when it is in sync. Without this, leadership piles up on the
// brokers that survived the last failure.
func (c *Controller) PreferredElection() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.st.Topics {
		for i := range t.Partitions {
			ps := &t.Partitions[i]
			pref := ps.Replicas[0]
			if ps.Leader != pref && contains(ps.ISR, pref) && c.eligibleLocked(pref, tpKey{t.Name, int32(i)}) {
				ps.Leader = pref
				ps.LeaderEpoch++
				c.mElections.With("preferred").Inc()
				n++
			}
		}
	}
	if n > 0 {
		c.commitLocked()
	}
	return n
}

// AllocateProducerID hands out a unique producer ID. IDs are persisted so
// they are never reused after a controller restart (reuse would let a new
// producer's sequence numbers collide with an old one's dedup state).
func (c *Controller) AllocateProducerID() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.st.NextProducerID
	c.st.NextProducerID++
	c.commitLocked()
	return id
}

// --- topics ----------------------------------------------------------------

var topicName = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,200}$`)

// CreateTopic creates a topic and assigns replicas round-robin across live
// brokers, rotating the starting broker per partition so leadership and
// follower load spread evenly.
func (c *Controller) CreateTopic(req protocol.CreateTopicRequest, p auth.Principal) (*metadata.Topic, error) {
	if !topicName.MatchString(req.Name) {
		return nil, protocol.Errorf(400, protocol.CodeInvalidRequest, "invalid topic name %q", req.Name)
	}
	if req.Partitions < 1 || req.Partitions > 10000 {
		return nil, protocol.Errorf(400, protocol.CodeInvalidRequest, "partitions must be in [1, 10000]")
	}
	if req.ReplicationFactor == 0 {
		req.ReplicationFactor = 1
	}
	cfg := metadata.DefaultTopicConfig()
	if len(req.Config) > 0 {
		if err := json.Unmarshal(req.Config, &cfg); err != nil {
			return nil, protocol.Errorf(400, protocol.CodeInvalidRequest, "invalid config: %v", err)
		}
	}
	if c.cfg.Auth.Enabled && !p.Superuser {
		cfg.Tenant = p.Tenant
	}
	if err := cfg.Validate(req.ReplicationFactor); err != nil {
		return nil, protocol.Errorf(400, protocol.CodeInvalidRequest, "%v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.st.Topics[req.Name]; ok {
		return nil, protocol.Errorf(409, protocol.CodeTopicExists, "topic %q exists", req.Name)
	}
	if q := c.cfg.MaxPartitionsPerTenant; q > 0 && cfg.Tenant != "" {
		used := 0
		for _, t := range c.st.Topics {
			if t.Config.Tenant == cfg.Tenant {
				used += len(t.Partitions)
			}
		}
		if used+req.Partitions > q {
			return nil, protocol.Errorf(429, protocol.CodeQuotaExceeded, "tenant %s would have %d partitions (quota %d)", cfg.Tenant, used+req.Partitions, q)
		}
	}
	alive := c.aliveLocked()
	if len(alive) < req.ReplicationFactor {
		return nil, protocol.Errorf(503, protocol.CodeInsufficientBroker,
			"replication factor %d needs %d live brokers, have %d", req.ReplicationFactor, req.ReplicationFactor, len(alive))
	}
	t := &metadata.Topic{Name: req.Name, ID: randomID(), Config: cfg}
	start := randInt(len(alive))
	for i := 0; i < req.Partitions; i++ {
		t.Partitions = append(t.Partitions, c.newPartition(int32(i), alive, start+i, req.ReplicationFactor))
	}
	c.st.Topics[req.Name] = t
	c.commitLocked()
	c.log.Info("topic created", "topic", req.Name, "partitions", req.Partitions, "rf", req.ReplicationFactor, "tenant", cfg.Tenant)
	return t, nil
}

func (c *Controller) newPartition(p int32, brokers []int32, start, rf int) metadata.PartitionState {
	reps := make([]int32, rf)
	for j := 0; j < rf; j++ {
		reps[j] = brokers[(start+j)%len(brokers)]
	}
	return metadata.PartitionState{Partition: p, Leader: reps[0], LeaderEpoch: 0,
		Replicas: reps, ISR: append([]int32(nil), reps...)}
}

func (c *Controller) aliveLocked() []int32 {
	var out []int32
	for id, b := range c.st.Brokers {
		if b.Alive {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// AddPartitions grows a topic. Existing keys will map to different
// partitions afterwards (hash mod n changes), so per-key ordering across the
// change is not preserved. That is why partition counts should be sized
// generously up front.
func (c *Controller) AddPartitions(name string, total int) (*metadata.Topic, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.st.Topics[name]
	if t == nil {
		return nil, protocol.Errorf(404, protocol.CodeUnknownTopic, "unknown topic %q", name)
	}
	if total <= len(t.Partitions) || total > 10000 {
		return nil, protocol.Errorf(400, protocol.CodeInvalidRequest, "partition count can only grow (current %d)", len(t.Partitions))
	}
	alive := c.aliveLocked()
	rf := len(t.Partitions[0].Replicas)
	if len(alive) < rf {
		return nil, protocol.Errorf(503, protocol.CodeInsufficientBroker, "not enough live brokers")
	}
	start := randInt(len(alive))
	for i := len(t.Partitions); i < total; i++ {
		t.Partitions = append(t.Partitions, c.newPartition(int32(i), alive, start+i, rf))
	}
	c.commitLocked()
	return t, nil
}

// AlterConfig merges a partial JSON config into the topic's config.
func (c *Controller) AlterConfig(name string, patch json.RawMessage) (*metadata.Topic, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.st.Topics[name]
	if t == nil {
		return nil, protocol.Errorf(404, protocol.CodeUnknownTopic, "unknown topic %q", name)
	}
	cfg := t.Config
	tenant := cfg.Tenant
	if err := json.Unmarshal(patch, &cfg); err != nil {
		return nil, protocol.Errorf(400, protocol.CodeInvalidRequest, "invalid config: %v", err)
	}
	cfg.Tenant = tenant // ownership cannot be changed by a config patch
	if err := cfg.Validate(len(t.Partitions[0].Replicas)); err != nil {
		return nil, protocol.Errorf(400, protocol.CodeInvalidRequest, "%v", err)
	}
	t.Config = cfg
	c.commitLocked()
	return t, nil
}

// DeleteTopic removes a topic. Brokers delete local data when they see it
// disappear from metadata. Topic IDs protect against a recreated topic of
// the same name picking up the old data.
func (c *Controller) DeleteTopic(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.st.Topics[name] == nil {
		return protocol.Errorf(404, protocol.CodeUnknownTopic, "unknown topic %q", name)
	}
	delete(c.st.Topics, name)
	c.commitLocked()
	c.log.Info("topic deleted", "topic", name)
	return nil
}

// Topic returns a copy of a topic.
func (c *Controller) Topic(name string) (metadata.Topic, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	md := c.snapshotLocked()
	t := md.Topic(name)
	if t == nil {
		return metadata.Topic{}, false
	}
	return *t, true
}

// --- helpers ---------------------------------------------------------------

func contains(s []int32, v int32) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func remove(s []int32, v int32) []int32 {
	var out []int32
	for _, x := range s {
		if x != v {
			out = append(out, x)
		}
	}
	return out
}

func equal(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func randomID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func randInt(n int) int {
	var b [2]byte
	_, _ = rand.Read(b[:])
	return (int(b[0])<<8 | int(b[1])) % n
}
