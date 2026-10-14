package partition

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	plog "github.com/michael-emmanuel/eventstream/internal/log"
	"github.com/michael-emmanuel/eventstream/internal/record"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func newClock() *clock { return &clock{t: time.Unix(1_700_000_000, 0)} }

func openReplica(t *testing.T, dir string, broker int32, clk *clock) *Partition {
	t.Helper()
	cfg := DefaultTopicConfig()
	cfg.ReplicaLagMaxTime = 5 * time.Second
	p, _, err := Open(Options{
		Topic: "orders", Partition: 0, BrokerID: broker,
		Dir:    filepath.Join(dir, fmt.Sprintf("b%d", broker)),
		Log:    plog.Config{SegmentBytes: 4096},
		Config: cfg, Now: clk.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func recs(vals ...string) []*record.Record {
	out := make([]*record.Record, len(vals))
	for i, v := range vals {
		out[i] = &record.Record{Key: []byte("k"), Value: []byte(v)}
	}
	return out
}

func produce(t *testing.T, p *Partition, acks Acks, vals ...string) ProduceResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, err := p.Produce(ctx, ProduceRequest{Records: recs(vals...), Acks: acks})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// replicate runs one follower fetch round trip.
func replicate(t *testing.T, leader, follower *Partition) ReplicaFetchResponse {
	t.Helper()
	pos, ok := follower.FollowerPosition()
	if !ok {
		t.Fatal("not a follower")
	}
	resp, err := leader.HandleReplicaFetch(ReplicaFetchRequest{
		ReplicaID: follower.self, FetchOffset: pos.FetchOffset,
		LastFetchedEpoch: pos.LastFetchedEpoch, LeaderEpoch: pos.LeaderEpoch, MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Diverging {
		if _, err := follower.TruncateForDivergence(resp.DivergingEpoch, resp.DivergingEndOffset); err != nil {
			t.Fatal(err)
		}
		return resp
	}
	if _, err := follower.AppendAsFollower(pos.LeaderEpoch, resp.Frames, resp.HighWatermark); err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestSingleReplicaProduceAndRead(t *testing.T) {
	clk := newClock()
	p := openReplica(t, t.TempDir(), 1, clk)
	p.BecomeLeader(0, []int32{1}, []int32{1})
	res := produce(t, p, AckAll, "a", "b", "c")
	if res.BaseOffset != 0 || res.LastOffset != 2 {
		t.Fatalf("%+v", res)
	}
	if p.HighWatermark() != 3 {
		t.Fatalf("hwm %d", p.HighWatermark())
	}
	rr, err := p.Read(1, 1<<20, 10)
	if err != nil || len(rr.Records) != 2 || string(rr.Records[0].Value) != "b" {
		t.Fatalf("%+v %v", rr, err)
	}
	if _, err := p.Read(4, 100, 1); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("read past HWM: %v", err)
	}
}

func TestIdempotentProducerDeduplicatesRetries(t *testing.T) {
	p := openReplica(t, t.TempDir(), 1, newClock())
	p.BecomeLeader(0, []int32{1}, []int32{1})
	ctx := context.Background()
	send := func(seq int64, vals ...string) (ProduceResult, error) {
		return p.Produce(ctx, ProduceRequest{ProducerID: 99, BaseSequence: seq, Records: recs(vals...), Acks: AckAll})
	}
	r1, err := send(0, "a", "b")
	if err != nil {
		t.Fatal(err)
	}
	// The response was "lost" and the producer retries the same batch.
	r2, err := send(0, "a", "b")
	if err != nil || !r2.Duplicate || r2.BaseOffset != r1.BaseOffset {
		t.Fatalf("retry not deduplicated: %+v %v", r2, err)
	}
	if p.LogEndOffset() != 2 {
		t.Fatalf("duplicate appended: LEO %d", p.LogEndOffset())
	}
	// Interleave another producer so offsets and sequences diverge.
	produce(t, p, AckAll, "other")
	r3, _ := send(2, "c")
	if r3.BaseOffset != 3 {
		t.Fatalf("%+v", r3)
	}
	// A gap in sequences means a batch was lost in flight: reject.
	if _, err := send(5, "x"); !errors.Is(err, ErrOutOfOrderSequence) {
		t.Fatalf("gap accepted: %v", err)
	}
	// A brand-new producer must start at sequence 0.
	_, err = p.Produce(ctx, ProduceRequest{ProducerID: 100, BaseSequence: 7, Records: recs("z"), Acks: AckAll})
	if !errors.Is(err, ErrOutOfOrderSequence) {
		t.Fatalf("unknown producer mid-sequence accepted: %v", err)
	}
}

func TestProducerStateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	p := openReplica(t, dir, 1, clk)
	p.BecomeLeader(0, []int32{1}, []int32{1})
	ctx := context.Background()
	req := ProduceRequest{ProducerID: 5, BaseSequence: 0, Records: recs("a", "b", "c"), Acks: AckAll}
	if _, err := p.Produce(ctx, req); err != nil {
		t.Fatal(err)
	}
	p.Close()
	p2 := openReplica(t, dir, 1, clk)
	p2.BecomeLeader(1, []int32{1}, []int32{1})
	if p2.HighWatermark() != 3 {
		t.Fatalf("hwm checkpoint lost: %d", p2.HighWatermark())
	}
	req.Records = recs("a", "b", "c")
	res, err := p2.Produce(ctx, req)
	if err != nil || !res.Duplicate || res.BaseOffset != 0 {
		t.Fatalf("retry after restart: %+v %v", res, err)
	}
}

func TestAckAllWaitsForFollowerAndHWMGatesConsumers(t *testing.T) {
	clk := newClock()
	dir := t.TempDir()
	leader := openReplica(t, dir, 1, clk)
	follower := openReplica(t, dir, 2, clk)
	leader.BecomeLeader(1, []int32{1, 2}, []int32{1, 2})
	follower.BecomeFollower(1, 1, []int32{1, 2}, []int32{1, 2})

	// acks=leader returns immediately; the record is not yet visible.
	produce(t, leader, AckLeader, "x")
	if leader.HighWatermark() != 0 {
		t.Fatal("HWM advanced without follower")
	}
	if rr, _ := leader.Read(0, 100, 10); len(rr.Records) != 0 {
		t.Fatal("consumer saw uncommitted record")
	}

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := leader.Produce(ctx, ProduceRequest{Records: recs("y"), Acks: AckAll})
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("acks=all returned before replication")
	default:
	}
	replicate(t, leader, follower) // follower copies x, y
	replicate(t, leader, follower) // fetch offset 2 acknowledges them
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if leader.HighWatermark() != 2 {
		t.Fatalf("hwm %d", leader.HighWatermark())
	}
	replicate(t, leader, follower)
	if follower.HighWatermark() != 2 {
		t.Fatalf("follower hwm %d", follower.HighWatermark())
	}
}

func TestReplicationTimeoutStillAppends(t *testing.T) {
	clk := newClock()
	dir := t.TempDir()
	leader := openReplica(t, dir, 1, clk)
	leader.BecomeLeader(1, []int32{1, 2}, []int32{1, 2})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	res, err := leader.Produce(ctx, ProduceRequest{Records: recs("a"), Acks: AckAll})
	if !errors.Is(err, ErrReplicationTimeout) || res.BaseOffset != 0 {
		t.Fatalf("%+v %v", res, err)
	}
	if leader.LogEndOffset() != 1 {
		t.Fatal("timed-out batch should remain in the leader log")
	}
}

func TestMinISREnforced(t *testing.T) {
	p := openReplica(t, t.TempDir(), 1, newClock())
	cfg := p.Config()
	cfg.MinISR = 2
	p.SetConfig(cfg)
	p.BecomeLeader(0, []int32{1, 2}, []int32{1})
	_, err := p.Produce(context.Background(), ProduceRequest{Records: recs("a"), Acks: AckAll})
	if !errors.Is(err, ErrNotEnoughReplicas) {
		t.Fatalf("err=%v", err)
	}
	// acks=leader trades durability for availability and is still accepted.
	if _, err := p.Produce(context.Background(), ProduceRequest{Records: recs("a"), Acks: AckLeader}); err != nil {
		t.Fatal(err)
	}
}

func TestQuorumAcks(t *testing.T) {
	clk := newClock()
	dir := t.TempDir()
	leader := openReplica(t, dir, 1, clk)
	f2 := openReplica(t, dir, 2, clk)
	leader.BecomeLeader(1, []int32{1, 2, 3}, []int32{1, 2, 3})
	f2.BecomeFollower(1, 1, []int32{1, 2, 3}, []int32{1, 2, 3})
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := leader.Produce(ctx, ProduceRequest{Records: recs("q"), Acks: AckQuorum})
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	replicate(t, leader, f2)
	replicate(t, leader, f2)
	// 2 of 3 replicas have the record: quorum reached even though replica 3
	// (still in the ISR) has nothing, so the HWM has not moved.
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if leader.HighWatermark() != 0 {
		t.Fatalf("hwm %d: replica 3 never fetched", leader.HighWatermark())
	}
}

func TestLeaseExpiredRejectsWrites(t *testing.T) {
	clk := newClock()
	valid := true
	p, _, err := Open(Options{Topic: "t", BrokerID: 1, Dir: t.TempDir(), Config: DefaultTopicConfig(),
		LeaseValid: func() bool { return valid }, Now: clk.Now})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.BecomeLeader(0, []int32{1}, []int32{1})
	produce(t, p, AckAll, "ok")
	valid = false
	if _, err := p.Produce(context.Background(), ProduceRequest{Records: recs("no"), Acks: AckLeader}); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("err=%v", err)
	}
}

// The classic divergence scenario: the old leader appended records that
// were acknowledged with acks=leader but never replicated. After failover
// the new leader writes different records at the same offsets. When the old
// leader rejoins as a follower it must discard exactly its unreplicated
// suffix, using the leader-epoch history, and converge on the new leader.
func TestDivergentFollowerTruncatesUsingLeaderEpochs(t *testing.T) {
	clk := newClock()
	dir := t.TempDir()
	a := openReplica(t, dir, 1, clk)
	b := openReplica(t, dir, 2, clk)
	a.BecomeLeader(1, []int32{1, 2}, []int32{1, 2})
	b.BecomeFollower(1, 1, []int32{1, 2}, []int32{1, 2})
	for i := 0; i < 10; i++ {
		produce(t, a, AckLeader, fmt.Sprintf("committed-%d", i))
	}
	replicate(t, a, b)
	replicate(t, a, b)
	if a.HighWatermark() != 10 {
		t.Fatalf("hwm %d", a.HighWatermark())
	}
	// A accepts 5 more records nobody copies, then is partitioned away.
	for i := 0; i < 5; i++ {
		produce(t, a, AckLeader, fmt.Sprintf("lost-%d", i))
	}
	// Controller elects B in epoch 2; B writes new records at 10..12.
	b.BecomeLeader(2, []int32{1, 2}, []int32{2})
	for i := 0; i < 3; i++ {
		produce(t, b, AckAll, fmt.Sprintf("new-%d", i))
	}
	// A learns about epoch 2 and rejoins as follower.
	a.BecomeFollower(2, 2, []int32{1, 2}, []int32{2})
	resp := replicate(t, b, a)
	if !resp.Diverging || resp.DivergingEndOffset != 10 {
		t.Fatalf("expected divergence at 10, got %+v", resp)
	}
	if a.LogEndOffset() != 10 {
		t.Fatalf("A LEO after truncation %d", a.LogEndOffset())
	}
	replicate(t, b, a)
	replicate(t, b, a)
	ra, _ := a.Log().Read(0, a.LogEndOffset(), 1<<20, 0)
	rb, _ := b.Log().Read(0, b.LogEndOffset(), 1<<20, 0)
	if len(ra) != len(rb) || len(ra) != 13 {
		t.Fatalf("logs did not converge: %d vs %d", len(ra), len(rb))
	}
	for i := range ra {
		if string(ra[i].Value) != string(rb[i].Value) {
			t.Fatalf("offset %d: %q vs %q", i, ra[i].Value, rb[i].Value)
		}
	}
	if string(ra[10].Value) != "new-0" {
		t.Fatal("unreplicated acks=leader data should be gone")
	}
}

// A retry that reaches the new leader after failover must be recognized as a
// duplicate of a batch the old leader accepted and replicated.
func TestDedupStateFollowsLeadership(t *testing.T) {
	clk := newClock()
	dir := t.TempDir()
	a := openReplica(t, dir, 1, clk)
	b := openReplica(t, dir, 2, clk)
	a.BecomeLeader(1, []int32{1, 2}, []int32{1, 2})
	b.BecomeFollower(1, 1, []int32{1, 2}, []int32{1, 2})
	req := ProduceRequest{ProducerID: 42, BaseSequence: 0, Records: recs("p1", "p2"), Acks: AckLeader}
	if _, err := a.Produce(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	replicate(t, a, b)
	b.BecomeLeader(2, []int32{1, 2}, []int32{2})
	req.Records = recs("p1", "p2")
	res, err := b.Produce(context.Background(), req)
	if err != nil || !res.Duplicate || res.BaseOffset != 0 {
		t.Fatalf("new leader did not dedupe: %+v %v", res, err)
	}
}

func TestStaleEpochsAreFenced(t *testing.T) {
	clk := newClock()
	dir := t.TempDir()
	a := openReplica(t, dir, 1, clk)
	a.BecomeLeader(3, []int32{1, 2}, []int32{1, 2})
	if a.BecomeLeader(2, []int32{1, 2}, []int32{1, 2}) {
		t.Fatal("accepted older epoch")
	}
	_, err := a.HandleReplicaFetch(ReplicaFetchRequest{ReplicaID: 2, LeaderEpoch: 2, LastFetchedEpoch: -1})
	if !errors.Is(err, ErrFencedLeaderEpoch) {
		t.Fatalf("err=%v", err)
	}
	_, err = a.HandleReplicaFetch(ReplicaFetchRequest{ReplicaID: 2, LeaderEpoch: 4, LastFetchedEpoch: -1})
	if !errors.Is(err, ErrUnknownLeaderEpoch) {
		t.Fatalf("err=%v", err)
	}
	_, err = a.HandleReplicaFetch(ReplicaFetchRequest{ReplicaID: 9, LeaderEpoch: 3, LastFetchedEpoch: -1})
	if !errors.Is(err, ErrUnknownReplica) {
		t.Fatalf("err=%v", err)
	}
}

func TestISRShrinkAndExpand(t *testing.T) {
	clk := newClock()
	dir := t.TempDir()
	leader := openReplica(t, dir, 1, clk)
	f := openReplica(t, dir, 2, clk)
	leader.BecomeLeader(1, []int32{1, 2}, []int32{1, 2})
	f.BecomeFollower(1, 1, []int32{1, 2}, []int32{1, 2})
	produce(t, leader, AckLeader, "a")
	replicate(t, leader, f)
	replicate(t, leader, f)
	if _, _, changed := leader.ISRProposal(); changed {
		t.Fatal("healthy follower proposed for removal")
	}
	// The follower stops fetching while the leader keeps receiving writes.
	produce(t, leader, AckLeader, "b")
	clk.Advance(6 * time.Second)
	epoch, isr, changed := leader.ISRProposal()
	if !changed || len(isr) != 1 {
		t.Fatalf("expected shrink to [1], got %v", isr)
	}
	// Until the controller accepts the shrink, the HWM waits for broker 2.
	if leader.HighWatermark() != 1 {
		t.Fatal("hwm moved before shrink was committed")
	}
	leader.CommitISR(epoch, isr)
	if leader.HighWatermark() != 2 {
		t.Fatalf("hwm after shrink %d", leader.HighWatermark())
	}
	// The follower catches up and is proposed for re-entry.
	replicate(t, leader, f)
	replicate(t, leader, f)
	_, isr, changed = leader.ISRProposal()
	if !changed || len(isr) != 2 {
		t.Fatalf("expected expand to [1 2], got %v", isr)
	}
}

func TestFollowerBehindLogStartResets(t *testing.T) {
	clk := newClock()
	dir := t.TempDir()
	leader := openReplica(t, dir, 1, clk)
	leader.BecomeLeader(1, []int32{1}, []int32{1})
	for i := 0; i < 200; i++ {
		produce(t, leader, AckAll, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
	}
	leader.Log().ApplyRetention(0, 1, clk.Now(), leader.HighWatermark())
	start := leader.LogStartOffset()
	if start == 0 {
		t.Fatal("retention did nothing")
	}
	leader.BecomeLeader(1, []int32{1, 2}, []int32{1})
	f := openReplica(t, dir, 2, clk)
	f.BecomeFollower(1, 1, []int32{1, 2}, []int32{1})
	pos, _ := f.FollowerPosition()
	_, err := leader.HandleReplicaFetch(ReplicaFetchRequest{ReplicaID: 2, FetchOffset: pos.FetchOffset, LastFetchedEpoch: -1, LeaderEpoch: 1, MaxBytes: 1 << 20})
	if !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("err=%v", err)
	}
	if err := f.ResetTo(start); err != nil {
		t.Fatal(err)
	}
	replicate(t, leader, f)
	if f.LogEndOffset() != leader.LogEndOffset() {
		t.Fatalf("follower %d leader %d", f.LogEndOffset(), leader.LogEndOffset())
	}
}

func TestWaitChWakesLongPoll(t *testing.T) {
	p := openReplica(t, t.TempDir(), 1, newClock())
	p.BecomeLeader(0, []int32{1}, []int32{1})
	ch := p.WaitCh()
	go func() {
		time.Sleep(10 * time.Millisecond)
		p.Produce(context.Background(), ProduceRequest{Records: recs("wake"), Acks: AckAll})
	}()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("long poll not woken")
	}
}

func TestCompactedTopicRequiresKey(t *testing.T) {
	p := openReplica(t, t.TempDir(), 1, newClock())
	cfg := p.Config()
	cfg.CleanupCompact = true
	p.SetConfig(cfg)
	p.BecomeLeader(0, []int32{1}, []int32{1})
	_, err := p.Produce(context.Background(), ProduceRequest{Records: []*record.Record{{Value: []byte("v")}}, Acks: AckAll})
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("err=%v", err)
	}
}

func TestEpochCache(t *testing.T) {
	var c epochCache
	c.assign(1, 0)
	c.assign(3, 10)
	c.assign(5, 20)
	c.assign(4, 25) // older epoch ignored
	cases := []struct {
		ask     int32
		wantE   int32
		wantEnd int64
	}{{1, 1, 10}, {2, 1, 10}, {3, 3, 20}, {5, 5, 30}, {9, 5, 30}, {0, 0, 0}}
	for _, tc := range cases {
		e, end := c.endOffsetFor(tc.ask, 30)
		if e != tc.wantE || end != tc.wantEnd {
			t.Errorf("endOffsetFor(%d)=(%d,%d) want (%d,%d)", tc.ask, e, end, tc.wantE, tc.wantEnd)
		}
	}
	if c.epochAt(15) != 3 || c.epochAt(30) != 5 || c.epochAt(0) != -1 {
		t.Fatal("epochAt")
	}
	c.truncateFrom(20)
	if c.latest() != 3 {
		t.Fatal("truncateFrom")
	}
}
