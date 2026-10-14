package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/fault"
	"github.com/michael-emmanuel/eventstream/internal/partition"
	"github.com/michael-emmanuel/eventstream/internal/protocol"
	"github.com/michael-emmanuel/eventstream/pkg/client"
)

func produceDirect(t *testing.T, cl *client.Client, brokerURL, topic string, p int32, value, acks string) error {
	_, err := cl.BrokerJSON(ctx(t, 5*time.Second), brokerURL, "POST",
		fmt.Sprintf("/v1/topics/%s/partitions/%d/events", topic, p),
		protocol.ProduceRequest{Acks: acks, TimeoutMs: 500, Records: []protocol.Record{{Value: &value}}})
	return err
}

func code(err error) string {
	var pe *protocol.Error
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// Network partition: the leader is cut off from the controller and the other
// brokers, while clients can still reach it.
//
//  1. During its remaining lease it still accepts acks=leader writes (the
//     split-brain window; these writes are at risk).
//  2. acks=all writes cannot complete: no follower can replicate them.
//  3. Once the lease expires it refuses writes (LEASE_EXPIRED). Because
//     the lease is shorter than the controller's session timeout, this
//     happens before a new leader is elected: there is never a moment when
//     two brokers both accept writes for the partition after the window.
//  4. The controller elects a new leader in a higher epoch.
//  5. When the partition heals, the old leader rejoins as a follower,
//     detects divergence via leader epochs, and truncates the unreplicated
//     acks=leader write. Replicas converge on the new leader's history.
func TestNetworkPartitionFencesStaleLeader(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client()
	mustCreate(t, cl, "ledger", 1, 3, map[string]any{"min_insync_replicas": 2})
	c.waitTopicReady(cl, "ledger")
	pr := cl.NewProducer(client.ProducerConfig{})
	produceN(t, pr, "ledger", 10, func(i int) string { return "acct" })
	pr.Close(context.Background())

	old := c.partition("ledger", 0)
	isolated := c.broker(old.Leader)
	isolated.Faults().Set(fault.Config{IsolateFromController: true, IsolateFromBrokers: true})

	// 1. Split-brain window: the isolated leader still holds its lease.
	if err := produceDirect(t, cl, isolated.URL(), "ledger", 0, "written-during-partition", "leader"); err != nil {
		t.Fatalf("write within lease should be accepted by the isolated leader: %v", err)
	}
	// 2. acks=all cannot be satisfied without followers.
	if err := produceDirect(t, cl, isolated.URL(), "ledger", 0, "needs-quorum", "all"); err == nil {
		t.Fatal("acks=all succeeded without any reachable follower")
	}
	// 3. After the lease expires, the isolated leader fences itself.
	c.waitFor("isolated leader to lose its lease", 5*time.Second, func() bool {
		return code(produceDirect(t, cl, isolated.URL(), "ledger", 0, "late", "leader")) == protocol.CodeLeaseExpired
	})
	// 4. New leader, higher epoch.
	c.waitFor("controller elects a new leader", 5*time.Second, func() bool {
		ps := c.partition("ledger", 0)
		return ps.Leader >= 0 && ps.Leader != old.Leader && ps.LeaderEpoch > old.LeaderEpoch
	})
	newLeader := c.partition("ledger", 0).Leader
	pr = cl.NewProducer(client.ProducerConfig{})
	produceN(t, pr, "ledger", 5, func(i int) string { return "acct" })
	pr.Close(context.Background())

	// 5. Heal and converge.
	isolated.Faults().Set(fault.Config{})
	np := c.broker(newLeader).Partition("ledger", 0)
	op := isolated.Partition("ledger", 0)
	c.waitFor("old leader converges as follower", 10*time.Second, func() bool {
		role, _, _ := op.Role()
		return role == partition.RoleFollower && op.LogEndOffset() == np.LogEndOffset()
	})
	recs, err := op.Log().Read(0, op.LogEndOffset(), 1<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 15 {
		for _, r := range recs {
			t.Logf("offset %d epoch %d value %q", r.Offset, r.LeaderEpoch, r.Value)
		}
		t.Fatalf("old leader has %d records after healing, want 15", len(recs))
	}
	for _, r := range recs {
		if v := string(r.Value); v == "written-during-partition" || v == "needs-quorum" || v == "late" {
			t.Fatalf("unreplicated write %q survived on the deposed leader", v)
		}
	}
	ref, _ := np.Log().Read(0, np.LogEndOffset(), 1<<20, 0)
	for i := range recs {
		if string(recs[i].Value) != string(ref[i].Value) {
			t.Fatalf("replicas disagree at offset %d", i)
		}
	}
}

// A slow follower is visible as replica lag, is removed from the ISR after
// replica_lag_max_ms, and is re-admitted once it catches up.
func TestSlowReplicaLeavesAndRejoinsISR(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client()
	mustCreate(t, cl, "telemetry", 1, 3, map[string]any{"replica_lag_max_ms": 500, "min_insync_replicas": 1})
	c.waitTopicReady(cl, "telemetry")
	ps := c.partition("telemetry", 0)
	var slow int32 = -1
	for _, r := range ps.Replicas {
		if r != ps.Leader {
			slow = r
			break
		}
	}
	c.broker(slow).Faults().Set(fault.Config{ReplicaFetchDelayMs: 2000})
	pr := cl.NewProducer(client.ProducerConfig{Acks: "leader"})
	defer pr.Close(context.Background())
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				pr.Send(context.Background(), client.Message{Topic: "telemetry", Value: []byte("tick")})
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()
	leaderPart := c.broker(ps.Leader).Partition("telemetry", 0)
	c.waitFor("slow replica removed from ISR", 10*time.Second, func() bool {
		for _, r := range c.partition("telemetry", 0).ISR {
			if r == slow {
				return false
			}
		}
		return true
	})
	st := leaderPart.State()
	for _, f := range st.Followers {
		if f.BrokerID == slow && f.InSync {
			t.Fatal("leader still counts slow follower as in sync")
		}
	}
	if !st.UnderReplicated {
		t.Fatal("partition should be reported under-replicated")
	}
	close(stop)
	c.broker(slow).Faults().Set(fault.Config{})
	c.waitFor("slow replica re-admitted to ISR", 10*time.Second, func() bool {
		return len(c.partition("telemetry", 0).ISR) == 3
	})
}

// Without the controller, leaders keep serving reads, but stop accepting
// writes once their lease expires: this design prefers refusing writes to
// risking two leaders. (Production systems avoid the single point of
// failure by replicating the controller with consensus.)
func TestControllerOutageStopsWritesAfterLease(t *testing.T) {
	c := newCluster(t, 1)
	cl := c.client()
	mustCreate(t, cl, "orders", 1, 1, nil)
	c.waitTopicReady(cl, "orders")
	b := c.broker(1)
	if err := produceDirect(t, cl, b.URL(), "orders", 0, "before", "all"); err != nil {
		t.Fatal(err)
	}
	b.Faults().Set(fault.Config{IsolateFromController: true})
	c.waitFor("lease expiry", 5*time.Second, func() bool {
		return code(produceDirect(t, cl, b.URL(), "orders", 0, "during", "all")) == protocol.CodeLeaseExpired
	})
	r, err := cl.Fetch(ctx(t, 5*time.Second), "orders", 0, 0, client.FetchOptions{})
	if err != nil || len(r.Records) == 0 || *r.Records[0].Value != "before" {
		t.Fatalf("reads should continue during controller outage: %v", err)
	}
	b.Faults().Set(fault.Config{})
	c.waitFor("writes resume", 5*time.Second, func() bool {
		return produceDirect(t, cl, b.URL(), "orders", 0, "after", "all") == nil
	})
}

// A disk failure on the leader takes that replica offline; the broker
// reports it to the controller, which moves leadership.
func TestDiskFailureMovesLeadership(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client()
	mustCreate(t, cl, "invoices", 1, 3, map[string]any{"min_insync_replicas": 2})
	c.waitTopicReady(cl, "invoices")
	ps := c.partition("invoices", 0)
	c.broker(ps.Leader).Faults().Set(fault.Config{DiskWriteErrorRate: 1})
	err := produceDirect(t, cl, c.broker(ps.Leader).URL(), "invoices", 0, "x", "all")
	if code(err) != protocol.CodeStorage {
		t.Fatalf("err=%v", err)
	}
	c.waitFor("leadership moved off failed disk", 5*time.Second, func() bool {
		p := c.partition("invoices", 0)
		return p.Leader >= 0 && p.Leader != ps.Leader
	})
	c.broker(ps.Leader).Faults().Set(fault.Config{})
	pr := cl.NewProducer(client.ProducerConfig{})
	defer pr.Close(context.Background())
	produceN(t, pr, "invoices", 5, func(i int) string { return "k" })
}

// A bounded produce buffer pushes back on the application instead of
// growing without limit when the cluster cannot keep up.
func TestProducerBufferBackpressure(t *testing.T) {
	c := newCluster(t, 1)
	cl := c.client()
	mustCreate(t, cl, "firehose", 1, 1, nil)
	c.waitTopicReady(cl, "firehose")
	c.broker(1).Faults().Set(fault.Config{DiskWriteDelayMs: 300}) // slow disk
	pr := cl.NewProducer(client.ProducerConfig{MaxBufferedBytes: 8 << 10, MaxBlock: 100 * time.Millisecond, BatchMaxRecords: 5})
	defer func() {
		c.broker(1).Faults().Set(fault.Config{})
		pr.Close(context.Background())
	}()
	payload := make([]byte, 1024)
	var full error
	for i := 0; i < 100 && full == nil; i++ {
		_, full = pr.Send(context.Background(), client.Message{Topic: "firehose", Value: payload})
	}
	if !errors.Is(full, client.ErrBufferFull) {
		t.Fatalf("expected ErrBufferFull, got %v", full)
	}
	if b := pr.Buffered(); b > 9<<10 {
		t.Fatalf("buffer exceeded its bound: %d", b)
	}
}
