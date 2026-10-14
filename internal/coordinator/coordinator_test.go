package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/protocol"
)

func newCoord(t *testing.T, dir string) *Coordinator {
	t.Helper()
	c, err := New(Config{
		DataDir:               dir,
		DefaultSessionTimeout: 300 * time.Millisecond,
		InitialRebalanceDelay: 20 * time.Millisecond,
		TickInterval:          10 * time.Millisecond,
	}, func(topic string) int { return 6 })
	if err != nil {
		t.Fatal(err)
	}
	c.Start()
	t.Cleanup(c.Stop)
	return c
}

func join(t *testing.T, c *Coordinator, memberID, client string) protocol.JoinGroupResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := c.Join(ctx, "fraud", protocol.JoinGroupRequest{MemberID: memberID, ClientID: client, Topics: []string{"orders"}})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func code(err error) string {
	var pe *protocol.Error
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

func TestSingleMemberGetsAllPartitions(t *testing.T) {
	c := newCoord(t, t.TempDir())
	r := join(t, c, "", "fraud-1")
	if len(r.Assignment) != 6 || r.Generation != 1 {
		t.Fatalf("%+v", r)
	}
}

// Second member joins: the first sees REBALANCE_IN_PROGRESS on heartbeat,
// rejoins, and both end up with half the partitions in generation 2.
func TestRebalanceOnJoinSplitsPartitions(t *testing.T) {
	c := newCoord(t, t.TempDir())
	a := join(t, c, "", "fraud-1")
	var b protocol.JoinGroupResponse
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); b = join(t, c, "", "fraud-2") }()
	// Member A learns about the rebalance through its heartbeat.
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := c.Heartbeat("fraud", protocol.GroupHeartbeatRequest{MemberID: a.MemberID, Generation: a.Generation})
		if code(err) == protocol.CodeRebalance {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no rebalance signal")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Commits from the old generation are still accepted while preparing,
	// so A can save progress before giving up its partitions.
	err := c.Commit("fraud", protocol.CommitOffsetsRequest{MemberID: a.MemberID, Generation: a.Generation,
		Offsets: []protocol.OffsetCommit{{Topic: "orders", Partition: 5, Offset: 10}}})
	if err != nil {
		t.Fatal(err)
	}
	a2 := join(t, c, a.MemberID, "fraud-1")
	wg.Wait()
	if a2.Generation != 2 || b.Generation != 2 {
		t.Fatalf("generations %d %d", a2.Generation, b.Generation)
	}
	if len(a2.Assignment)+len(b.Assignment) != 6 || len(a2.Assignment) != 3 {
		t.Fatalf("assignments %v / %v", a2.Assignment, b.Assignment)
	}
	// The old generation is now fenced.
	err = c.Commit("fraud", protocol.CommitOffsetsRequest{MemberID: a.MemberID, Generation: a.Generation,
		Offsets: []protocol.OffsetCommit{{Topic: "orders", Partition: 0, Offset: 11}}})
	if code(err) != protocol.CodeIllegalGeneration {
		t.Fatalf("zombie commit accepted: %v", err)
	}
}

func TestExpiredMemberTriggersRebalance(t *testing.T) {
	c := newCoord(t, t.TempDir())
	a := join(t, c, "", "a")
	stop := make(chan struct{})
	var b protocol.JoinGroupResponse
	done := make(chan struct{})
	go func() {
		b = join(t, c, "", "b")
		close(done)
	}()
	// A rejoins once it sees the rebalance, then keeps heartbeating.
	for code(func() error {
		_, err := c.Heartbeat("fraud", protocol.GroupHeartbeatRequest{MemberID: a.MemberID, Generation: a.Generation})
		return err
	}()) != protocol.CodeRebalance {
		time.Sleep(5 * time.Millisecond)
	}
	a = join(t, c, a.MemberID, "a")
	<-done
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
				c.Heartbeat("fraud", protocol.GroupHeartbeatRequest{MemberID: a.MemberID, Generation: a.Generation})
			}
		}
	}()
	defer close(stop)
	// B crashes (stops heartbeating). After the session timeout it is
	// removed; A must rejoin and receive all six partitions.
	_ = b
	deadline := time.Now().Add(3 * time.Second)
	for {
		d, _ := c.Describe("fraud")
		if len(d.Members) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expired member not removed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	r := join(t, c, a.MemberID, "a")
	if len(r.Assignment) != 6 {
		t.Fatalf("survivor got %d partitions", len(r.Assignment))
	}
}

func TestAdminCommitRequiresEmptyGroup(t *testing.T) {
	c := newCoord(t, t.TempDir())
	join(t, c, "", "a")
	err := c.Commit("fraud", protocol.CommitOffsetsRequest{Generation: -1,
		Offsets: []protocol.OffsetCommit{{Topic: "orders", Partition: 0, Offset: 0}}})
	if code(err) != protocol.CodeGroupNotEmpty {
		t.Fatalf("err=%v", err)
	}
	if err := c.Commit("idle", protocol.CommitOffsetsRequest{Generation: -1,
		Offsets: []protocol.OffsetCommit{{Topic: "orders", Partition: 0, Offset: 42}}}); err != nil {
		t.Fatal(err)
	}
}

func TestNonOwnerCommitRejected(t *testing.T) {
	c := newCoord(t, t.TempDir())
	ctx := context.Background()
	r, err := c.Join(ctx, "g", protocol.JoinGroupRequest{Topics: []string{"orders"}})
	if err != nil {
		t.Fatal(err)
	}
	err = c.Commit("g", protocol.CommitOffsetsRequest{MemberID: r.MemberID, Generation: r.Generation,
		Offsets: []protocol.OffsetCommit{{Topic: "payments", Partition: 0, Offset: 1}}})
	if code(err) != protocol.CodeNotOwner {
		t.Fatalf("err=%v", err)
	}
}

func TestOffsetsPersistAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenOffsetStore(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2000; i++ {
		s.Commit("g", []protocol.OffsetCommit{{Topic: "t", Partition: int32(i % 3), Offset: int64(i)}})
	}
	s.Close()
	s2, err := OpenOffsetStore(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got := s2.Get("g")
	if len(got) != 3 || got[0].Offset != 1998 || got[1].Offset != 1999 || got[2].Offset != 1997 {
		t.Fatalf("%+v", got)
	}
	if s2.lines > 4*3+1000 {
		t.Fatalf("store was not compacted: %d lines", s2.lines)
	}
}

func TestAssignors(t *testing.T) {
	in := assignInput{
		members:    []string{"a", "b", "c"},
		subs:       map[string][]string{"a": {"t"}, "b": {"t"}, "c": {"t"}},
		partitions: map[string]int{"t": 7},
	}
	for _, name := range []string{AssignorRange, AssignorRoundRobin, AssignorSticky} {
		out := assign(name, in)
		seen := map[tp]bool{}
		for _, ps := range out {
			if len(ps) < 2 || len(ps) > 3 {
				t.Fatalf("%s unbalanced: %v", name, out)
			}
			for _, x := range ps {
				if seen[x] {
					t.Fatalf("%s assigned %v twice", name, x)
				}
				seen[x] = true
			}
		}
		if len(seen) != 7 {
			t.Fatalf("%s left partitions unassigned", name)
		}
	}
}

// When one of four consumers leaves, the sticky assignor moves only the
// departed member's partitions; range reshuffles many more.
func TestStickyMinimizesMovement(t *testing.T) {
	mk := func(ms ...string) assignInput {
		in := assignInput{subs: map[string][]string{}, partitions: map[string]int{"t": 24}}
		for _, m := range ms {
			in.members = append(in.members, m)
			in.subs[m] = []string{"t"}
		}
		return in
	}
	for _, name := range []string{AssignorRange, AssignorSticky} {
		before := assign(name, mk("a", "b", "c", "d"))
		in := mk("a", "c", "d")
		in.prev = before
		after := assign(name, in)
		delete(before, "b")
		n := moved(before, after)
		t.Logf("%s: %d partitions changed owner (excluding the departed member's)", name, n)
		if name == AssignorSticky && n != 0 {
			t.Fatalf("sticky moved %d surviving partitions", n)
		}
	}
}

func TestRebalanceStormCoalescing(t *testing.T) {
	c := newCoord(t, t.TempDir())
	var wg sync.WaitGroup
	gens := make([]int32, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := join(t, c, "", fmt.Sprintf("c%d", i))
			gens[i] = r.Generation
		}(i)
	}
	wg.Wait()
	d, _ := c.Describe("fraud")
	// Five near-simultaneous joins are absorbed into very few rebalances
	// thanks to the initial rebalance delay.
	if d.Rebalances > 2 {
		t.Fatalf("%d rebalances for one burst of joins", d.Rebalances)
	}
}
