package controller

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/auth"
	"github.com/michael-emmanuel/eventstream/internal/protocol"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func newController(t *testing.T, dir string, clk *clock) *Controller {
	t.Helper()
	c, err := New(Config{DataDir: dir, SessionTimeout: 10 * time.Second, Now: clk.Now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	return c
}

func hb(t *testing.T, c *Controller, id int32, inc string) {
	t.Helper()
	if _, err := c.Heartbeat(protocol.BrokerHeartbeatRequest{BrokerID: id, URL: "http://b", Incarnation: inc}); err != nil {
		t.Fatal(err)
	}
}

func setup(t *testing.T, unclean bool) (*Controller, *clock) {
	clk := &clock{t: time.Unix(1000, 0)}
	c := newController(t, t.TempDir(), clk)
	for id := int32(1); id <= 3; id++ {
		hb(t, c, id, "x")
	}
	cfg, _ := json.Marshal(map[string]any{"unclean_leader_election": unclean, "min_insync_replicas": 2})
	if _, err := c.CreateTopic(protocol.CreateTopicRequest{Name: "orders", Partitions: 6, ReplicationFactor: 3, Config: cfg}, auth.Anonymous); err != nil {
		t.Fatal(err)
	}
	return c, clk
}

func TestCreateTopicSpreadsLeaders(t *testing.T) {
	c, _ := setup(t, false)
	topic, _ := c.Topic("orders")
	leaders := map[int32]int{}
	for _, p := range topic.Partitions {
		if len(p.Replicas) != 3 || len(p.ISR) != 3 {
			t.Fatalf("%+v", p)
		}
		leaders[p.Leader]++
	}
	for id, n := range leaders {
		if n != 2 {
			t.Fatalf("broker %d leads %d partitions: %v", id, n, leaders)
		}
	}
	if _, err := c.CreateTopic(protocol.CreateTopicRequest{Name: "big", Partitions: 1, ReplicationFactor: 4}, auth.Anonymous); err == nil {
		t.Fatal("RF > live brokers accepted")
	}
}

func TestLeaderFailoverOnSessionExpiry(t *testing.T) {
	c, clk := setup(t, false)
	before, _ := c.Topic("orders")
	// Brokers 2 and 3 keep heartbeating; broker 1 goes silent.
	clk.Advance(6 * time.Second)
	hb(t, c, 2, "x")
	hb(t, c, 3, "x")
	clk.Advance(5 * time.Second)
	c.expireBrokers()
	after, _ := c.Topic("orders")
	for i, p := range after.Partitions {
		if p.Leader == 1 {
			t.Fatalf("partition %d still led by dead broker", i)
		}
		for _, r := range p.ISR {
			if r == 1 {
				t.Fatalf("dead broker still in ISR of %d", i)
			}
		}
		if before.Partitions[i].Leader == 1 && p.LeaderEpoch != before.Partitions[i].LeaderEpoch+1 {
			t.Fatalf("epoch not bumped on failover")
		}
	}
}

func TestStaleLeaderCannotAlterISR(t *testing.T) {
	c, clk := setup(t, false)
	topic, _ := c.Topic("orders")
	p0 := topic.Partitions[0]
	// Correct leader and epoch: shrink accepted.
	others := []int32{}
	for _, r := range p0.Replicas {
		if r != p0.Leader {
			others = append(others, r)
		}
	}
	ps, err := c.AlterISR(protocol.AlterISRRequest{Topic: "orders", Partition: 0, LeaderID: p0.Leader,
		LeaderEpoch: p0.LeaderEpoch, ISR: []int32{p0.Leader, others[0]}})
	if err != nil || len(ps.ISR) != 2 {
		t.Fatal(ps, err)
	}
	// Leader dies; a new leader is elected in a new epoch.
	clk.Advance(11 * time.Second)
	for _, id := range others {
		hb(t, c, id, "x")
	}
	c.expireBrokers()
	// The deposed leader tries to update the ISR with its old epoch.
	_, err = c.AlterISR(protocol.AlterISRRequest{Topic: "orders", Partition: 0, LeaderID: p0.Leader,
		LeaderEpoch: p0.LeaderEpoch, ISR: []int32{p0.Leader}})
	var pe *protocol.Error
	if !errors.As(err, &pe) || pe.Code != protocol.CodeStaleISRUpdate {
		t.Fatalf("stale ISR update accepted: %v", err)
	}
}

func TestOfflinePartitionAndCleanRecovery(t *testing.T) {
	c, clk := setup(t, false)
	topic, _ := c.Topic("orders")
	p0 := topic.Partitions[0]
	// Shrink ISR to just the leader, then kill the leader.
	c.AlterISR(protocol.AlterISRRequest{Topic: "orders", Partition: 0, LeaderID: p0.Leader, LeaderEpoch: p0.LeaderEpoch, ISR: []int32{p0.Leader}})
	clk.Advance(11 * time.Second)
	for _, r := range p0.Replicas {
		if r != p0.Leader {
			hb(t, c, r, "x")
		}
	}
	c.expireBrokers()
	got, _ := c.Topic("orders")
	if got.Partitions[0].Leader != -1 {
		t.Fatalf("out-of-sync replica elected without unclean election: %+v", got.Partitions[0])
	}
	// The last in-sync replica returns: clean election.
	hb(t, c, p0.Leader, "y")
	got, _ = c.Topic("orders")
	if got.Partitions[0].Leader != p0.Leader {
		t.Fatalf("partition not recovered: %+v", got.Partitions[0])
	}
}

func TestUncleanElectionWhenAllowed(t *testing.T) {
	c, clk := setup(t, true)
	topic, _ := c.Topic("orders")
	p0 := topic.Partitions[0]
	c.AlterISR(protocol.AlterISRRequest{Topic: "orders", Partition: 0, LeaderID: p0.Leader, LeaderEpoch: p0.LeaderEpoch, ISR: []int32{p0.Leader}})
	clk.Advance(11 * time.Second)
	for _, r := range p0.Replicas {
		if r != p0.Leader {
			hb(t, c, r, "x")
		}
	}
	c.expireBrokers()
	got, _ := c.Topic("orders")
	if got.Partitions[0].Leader < 0 || got.Partitions[0].Leader == p0.Leader {
		t.Fatalf("unclean election did not happen: %+v", got.Partitions[0])
	}
}

func TestRestartedBrokerLosesLeadership(t *testing.T) {
	c, _ := setup(t, false)
	before, _ := c.Topic("orders")
	hb(t, c, 1, "new-incarnation")
	after, _ := c.Topic("orders")
	for i, p := range after.Partitions {
		if before.Partitions[i].Leader == 1 && p.Leader == 1 {
			t.Fatalf("restarted broker kept leadership of %d", i)
		}
	}
}

func TestStorageFailureReportedByBroker(t *testing.T) {
	c, _ := setup(t, false)
	topic, _ := c.Topic("orders")
	p0 := topic.Partitions[0]
	_, err := c.Heartbeat(protocol.BrokerHeartbeatRequest{BrokerID: p0.Leader, URL: "http://b", Incarnation: "x",
		OfflinePartitions: []protocol.TopicPartition{{Topic: "orders", Partition: 0}}})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := c.Topic("orders")
	if got.Partitions[0].Leader == p0.Leader {
		t.Fatal("leader with failed disk kept leadership")
	}
}

func TestStatePersistsAcrossControllerRestart(t *testing.T) {
	dir := t.TempDir()
	clk := &clock{t: time.Unix(1000, 0)}
	c := newController(t, dir, clk)
	hb(t, c, 1, "x")
	c.CreateTopic(protocol.CreateTopicRequest{Name: "t", Partitions: 3, ReplicationFactor: 1}, auth.Anonymous)
	pid := c.AllocateProducerID()
	c.Stop()
	c2 := newController(t, dir, clk)
	if _, ok := c2.Topic("t"); !ok {
		t.Fatal("topic lost")
	}
	if c2.AllocateProducerID() <= pid {
		t.Fatal("producer IDs reused after restart")
	}
}

func TestPreferredElection(t *testing.T) {
	c, clk := setup(t, false)
	clk.Advance(11 * time.Second)
	hb(t, c, 2, "x")
	hb(t, c, 3, "x")
	c.expireBrokers()
	hb(t, c, 1, "x2") // broker 1 back, but not in any ISR yet
	if n := c.PreferredElection(); n != 0 {
		t.Fatalf("moved leadership to out-of-sync replica (%d)", n)
	}
}
