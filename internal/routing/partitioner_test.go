package routing

import (
	"fmt"
	"testing"
)

func TestKeyedRoutingIsDeterministic(t *testing.T) {
	var d Default
	p1, _ := d.Partition([]byte("user-123"), nil, 8)
	for i := 0; i < 100; i++ {
		p, _ := d.Partition([]byte("user-123"), nil, 8)
		if p != p1 {
			t.Fatal("same key routed to different partitions")
		}
	}
}

func TestExplicitAndRoundRobin(t *testing.T) {
	var d Default
	two := int32(2)
	if p, _ := d.Partition([]byte("k"), &two, 4); p != 2 {
		t.Fatal("explicit partition ignored")
	}
	bad := int32(9)
	if _, err := d.Partition(nil, &bad, 4); err == nil {
		t.Fatal("out of range explicit partition accepted")
	}
	seen := map[int32]int{}
	for i := 0; i < 400; i++ {
		p, _ := d.Partition(nil, nil, 4)
		seen[p]++
	}
	for p := int32(0); p < 4; p++ {
		if seen[p] != 100 {
			t.Fatalf("round robin uneven: %v", seen)
		}
	}
}

// One customer producing 70% of events overloads a single partition under
// hash(customer_id), no matter how many partitions exist.
func TestHotKeySkew(t *testing.T) {
	var d Default
	const n, total = 8, 10000
	counts := make([]int, n)
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("customer-%d", i%1000)
		if i%10 < 7 {
			key = "customer-whale"
		}
		p, _ := d.Partition([]byte(key), nil, n)
		counts[p]++
	}
	max := 0
	for _, c := range counts {
		if c > max {
			max = c
		}
	}
	if float64(max)/total < 0.7 {
		t.Fatalf("expected a hot partition with >=70%% of traffic, got %v", counts)
	}

	// Salting the hot key spreads it, at the cost of its ordering.
	s := &Salted{HotKeys: map[string]bool{"customer-whale": true}, Spread: n}
	counts = make([]int, n)
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("customer-%d", i%1000)
		if i%10 < 7 {
			key = "customer-whale"
		}
		p, _ := s.Partition([]byte(key), nil, n)
		counts[p]++
	}
	max = 0
	for _, c := range counts {
		if c > max {
			max = c
		}
	}
	if float64(max)/total > 0.3 {
		t.Fatalf("salting did not spread load: %v", counts)
	}
}

// Changing the partition count with modulo hashing moves most keys; a
// consistent-hash ring moves roughly 1/(n+1) of them. Either way, any moved
// key loses its ordering guarantee across the change.
func TestRemapOnPartitionGrowth(t *testing.T) {
	const keys = 20000
	movedMod, movedRing := 0, 0
	r8, r9 := NewRing(8, 128), NewRing(9, 128)
	for i := 0; i < keys; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		if HashPartition(k, 8) != HashPartition(k, 9) {
			movedMod++
		}
		if r8.Locate(k) != r9.Locate(k) {
			movedRing++
		}
	}
	fm, fr := float64(movedMod)/keys, float64(movedRing)/keys
	t.Logf("8->9 partitions: modulo moved %.1f%% of keys, consistent hashing %.1f%%", fm*100, fr*100)
	if fm < 0.8 || fr > 0.2 {
		t.Fatalf("unexpected movement: modulo=%.2f ring=%.2f", fm, fr)
	}
}

func BenchmarkHashPartition(b *testing.B) {
	k := []byte("user-1234567")
	for i := 0; i < b.N; i++ {
		HashPartition(k, 64)
	}
}
