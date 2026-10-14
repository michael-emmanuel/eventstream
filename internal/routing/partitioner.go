// Package routing decides which partition a record goes to.
//
// Strategies:
//
//	hash(key)      default when a key is present. Same key -> same partition
//	               while the partition count is unchanged, which is what gives
//	               per-key ordering. Skewed keys create hot partitions.
//	round robin    default for keyless records. Even load, no ordering
//	               relationship between records.
//	explicit       the caller names the partition (e.g. a dedicated
//	               partition for a large tenant).
//	salted hash    hash(key + "#" + salt) with salt in [0, n): spreads one
//	               hot key over n partitions, giving up ordering for that key.
//
// Consistent hashing is implemented separately (Ring) to demonstrate how much
// key movement each scheme causes when the partition count changes; the
// broker does not use it for partitions because a partition's identity is
// its log, and remapping keys between logs breaks per-key ordering no matter
// how few keys move.
package routing

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync/atomic"
)

// Hash returns a stable 32-bit FNV-1a hash of key. The hash function is part
// of the contract: changing it remaps every key, exactly like changing the
// partition count.
func Hash(key []byte) uint32 {
	h := fnv.New32a()
	h.Write(key)
	return h.Sum32()
}

// HashPartition maps key to a partition in [0, n).
func HashPartition(key []byte, n int) int32 {
	return int32(Hash(key) % uint32(n))
}

// Partitioner chooses a partition for a record.
type Partitioner interface {
	Partition(key []byte, explicit *int32, n int) (int32, error)
}

// Default is key-aware: explicit partition if given, hash(key) if a key is
// present, otherwise round robin.
type Default struct {
	rr atomic.Uint32
}

// Partition implements Partitioner.
func (d *Default) Partition(key []byte, explicit *int32, n int) (int32, error) {
	if n <= 0 {
		return 0, fmt.Errorf("topic has no partitions")
	}
	if explicit != nil {
		if *explicit < 0 || int(*explicit) >= n {
			return 0, fmt.Errorf("partition %d out of range [0, %d)", *explicit, n)
		}
		return *explicit, nil
	}
	if key != nil {
		return HashPartition(key, n), nil
	}
	return int32(d.rr.Add(1) % uint32(n)), nil
}

// Salted spreads selected hot keys over Spread partitions by appending a
// rotating salt. Ordering for those keys is lost; everything else behaves
// like Default. This is the producer-side mitigation for a known hot key.
type Salted struct {
	Default
	HotKeys map[string]bool
	Spread  int
	n       atomic.Uint32
}

// Partition implements Partitioner.
func (s *Salted) Partition(key []byte, explicit *int32, n int) (int32, error) {
	if explicit == nil && key != nil && s.HotKeys[string(key)] && s.Spread > 1 {
		salt := s.n.Add(1) % uint32(s.Spread)
		return HashPartition([]byte(string(key)+"#"+strconv.Itoa(int(salt))), n), nil
	}
	return s.Default.Partition(key, explicit, n)
}

// Ring is a consistent-hash ring with virtual nodes.
type Ring struct {
	points []uint32
	owners map[uint32]int
}

// NewRing builds a ring over n buckets with vnodes virtual nodes each.
func NewRing(n, vnodes int) *Ring {
	r := &Ring{owners: map[uint32]int{}}
	for b := 0; b < n; b++ {
		for v := 0; v < vnodes; v++ {
			h := Hash([]byte(fmt.Sprintf("bucket-%d-vnode-%d", b, v)))
			if _, dup := r.owners[h]; dup {
				continue
			}
			r.owners[h] = b
			r.points = append(r.points, h)
		}
	}
	sort.Slice(r.points, func(i, j int) bool { return r.points[i] < r.points[j] })
	return r
}

// Locate returns the bucket owning key.
func (r *Ring) Locate(key []byte) int {
	h := Hash(key)
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= h })
	if i == len(r.points) {
		i = 0
	}
	return r.owners[r.points[i]]
}
