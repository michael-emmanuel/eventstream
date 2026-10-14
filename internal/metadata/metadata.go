// Package metadata defines cluster metadata (brokers, topics, partition
// leadership) and a client-side cache for it.
//
// The controller owns the authoritative copy. Brokers and clients hold
// cached copies that may be stale; staleness is tolerated because every
// leadership-sensitive operation is checked again on the broker against the
// leader epoch, and a NOT_LEADER response triggers a refresh.
package metadata

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Cleanup policies.
const (
	CleanupDelete  = "delete"
	CleanupCompact = "compact"
)

// TopicConfig is per-topic configuration. Topic configuration is a scaling
// boundary: partition count caps consumer parallelism, replication factor
// multiplies storage and network, and retention determines disk. Getting
// it wrong is expensive to fix later because partition count changes remap
// keys and retention changes move terabytes.
type TopicConfig struct {
	RetentionMs           int64  `json:"retention_ms"`
	RetentionBytes        int64  `json:"retention_bytes"`
	SegmentBytes          int64  `json:"segment_bytes"`
	SegmentMs             int64  `json:"segment_ms"`
	CleanupPolicy         string `json:"cleanup_policy"`
	MinInsyncReplicas     int    `json:"min_insync_replicas"`
	MaxEventBytes         int    `json:"max_event_bytes"`
	Compression           string `json:"compression"`
	UncleanLeaderElection bool   `json:"unclean_leader_election"`
	TombstoneRetentionMs  int64  `json:"tombstone_retention_ms"`
	ReplicaLagMaxMs       int64  `json:"replica_lag_max_ms"`
	Tenant                string `json:"tenant,omitempty"`
}

// DefaultTopicConfig returns defaults.
func DefaultTopicConfig() TopicConfig {
	return TopicConfig{
		RetentionMs:          7 * 24 * 3600 * 1000,
		SegmentBytes:         64 << 20,
		SegmentMs:            24 * 3600 * 1000,
		CleanupPolicy:        CleanupDelete,
		MinInsyncReplicas:    1,
		MaxEventBytes:        1 << 20,
		Compression:          "none",
		TombstoneRetentionMs: 24 * 3600 * 1000,
		ReplicaLagMaxMs:      10_000,
	}
}

// Validate checks a config against a replication factor.
func (c TopicConfig) Validate(rf int) error {
	if c.MinInsyncReplicas < 1 || c.MinInsyncReplicas > rf {
		return fmt.Errorf("min_insync_replicas must be in [1, %d]", rf)
	}
	if c.SegmentBytes < 1024 || c.SegmentBytes > 1<<31-1 {
		return errors.New("segment_bytes must be in [1KiB, 2GiB)")
	}
	if c.MaxEventBytes < 64 || int64(c.MaxEventBytes) > c.SegmentBytes {
		return errors.New("max_event_bytes must be >= 64 and <= segment_bytes")
	}
	for _, p := range strings.Split(c.CleanupPolicy, ",") {
		if p != CleanupDelete && p != CleanupCompact {
			return fmt.Errorf("unknown cleanup policy %q", p)
		}
	}
	if c.Compression != "" && c.Compression != "none" {
		return fmt.Errorf("compression %q is not implemented (see docs/tradeoffs.md#26-compression)", c.Compression)
	}
	return nil
}

// HasPolicy reports whether the cleanup policy includes p.
func (c TopicConfig) HasPolicy(p string) bool {
	for _, x := range strings.Split(c.CleanupPolicy, ",") {
		if x == p {
			return true
		}
	}
	return false
}

// Broker describes a broker.
type Broker struct {
	ID          int32  `json:"id"`
	URL         string `json:"url"`
	Alive       bool   `json:"alive"`
	Incarnation string `json:"incarnation,omitempty"`
}

// PartitionState is the controller's view of one partition.
//
// LeaderEpoch increases on every leadership change. It is the fencing token:
// every replica rejects requests that carry an older epoch, which is what
// stops a deposed leader from acting on stale beliefs.
type PartitionState struct {
	Partition   int32   `json:"partition"`
	Leader      int32   `json:"leader"` // -1 = offline
	LeaderEpoch int32   `json:"leader_epoch"`
	Replicas    []int32 `json:"replicas"`
	ISR         []int32 `json:"isr"`
}

// Topic describes a topic.
type Topic struct {
	Name       string           `json:"name"`
	ID         string           `json:"id"`
	Config     TopicConfig      `json:"config"`
	Partitions []PartitionState `json:"partitions"`
}

// Cluster is a versioned metadata snapshot.
type Cluster struct {
	Version        int64    `json:"version"`
	ControllerURL  string   `json:"controller_url"`
	CoordinatorURL string   `json:"coordinator_url"`
	Brokers        []Broker `json:"brokers"`
	Topics         []Topic  `json:"topics"`
}

// Topic returns the named topic or nil.
func (c *Cluster) Topic(name string) *Topic {
	if c == nil {
		return nil
	}
	for i := range c.Topics {
		if c.Topics[i].Name == name {
			return &c.Topics[i]
		}
	}
	return nil
}

// Broker returns the broker with id.
func (c *Cluster) Broker(id int32) (Broker, bool) {
	if c == nil {
		return Broker{}, false
	}
	for _, b := range c.Brokers {
		if b.ID == id {
			return b, true
		}
	}
	return Broker{}, false
}

// Partition returns the state of topic/partition.
func (c *Cluster) Partition(topic string, p int32) (PartitionState, bool) {
	t := c.Topic(topic)
	if t == nil || p < 0 || int(p) >= len(t.Partitions) {
		return PartitionState{}, false
	}
	return t.Partitions[p], true
}

// LeaderURL returns the URL of the current leader of topic/partition.
func (c *Cluster) LeaderURL(topic string, p int32) (string, int32, bool) {
	ps, ok := c.Partition(topic, p)
	if !ok || ps.Leader < 0 {
		return "", -1, false
	}
	b, ok := c.Broker(ps.Leader)
	if !ok {
		return "", ps.Leader, false
	}
	return b.URL, ps.Leader, true
}

// UnderReplicated counts partitions whose ISR is smaller than the replica set.
func (c *Cluster) UnderReplicated() (under, offline int) {
	for _, t := range c.Topics {
		for _, p := range t.Partitions {
			if p.Leader < 0 {
				offline++
			}
			if len(p.ISR) < len(p.Replicas) {
				under++
			}
		}
	}
	return
}

// Cache is a TTL cache of cluster metadata for clients. Metadata is read on
// every produce and fetch, but changes rarely, so caching it removes a
// round trip from the hot path. Correctness does not depend on freshness:
// brokers reject requests sent to the wrong leader, and callers Invalidate
// on NOT_LEADER, so the cost of staleness is one extra round trip.
type Cache struct {
	ttl   time.Duration
	fetch func(ctx context.Context) (*Cluster, error)

	mu      sync.Mutex
	cur     *Cluster
	fetched time.Time
	refresh chan struct{} // non-nil while a refresh is in flight
}

// NewCache creates a cache.
func NewCache(ttl time.Duration, fetch func(ctx context.Context) (*Cluster, error)) *Cache {
	return &Cache{ttl: ttl, fetch: fetch}
}

// Get returns cached metadata, refreshing it if older than the TTL.
// Concurrent callers share a single refresh (no thundering herd).
func (c *Cache) Get(ctx context.Context) (*Cluster, error) {
	for {
		c.mu.Lock()
		if c.cur != nil && time.Since(c.fetched) < c.ttl {
			cur := c.cur
			c.mu.Unlock()
			return cur, nil
		}
		if c.refresh != nil {
			ch := c.refresh
			c.mu.Unlock()
			select {
			case <-ch:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		ch := make(chan struct{})
		c.refresh = ch
		c.mu.Unlock()

		md, err := c.fetch(ctx)
		c.mu.Lock()
		c.refresh = nil
		close(ch)
		if err != nil {
			stale := c.cur
			c.mu.Unlock()
			if stale != nil {
				// Serving stale metadata beats failing: brokers will
				// correct us if it is wrong.
				return stale, nil
			}
			return nil, err
		}
		if c.cur == nil || md.Version >= c.cur.Version {
			c.cur = md
		}
		c.fetched = time.Now()
		cur := c.cur
		c.mu.Unlock()
		return cur, nil
	}
}

// Peek returns the cached snapshot without refreshing (may be nil).
func (c *Cache) Peek() *Cluster {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cur
}

// Invalidate forces the next Get to refresh.
func (c *Cache) Invalidate() {
	c.mu.Lock()
	c.fetched = time.Time{}
	c.mu.Unlock()
}
