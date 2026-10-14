package partition

import (
	"time"
)

// recentBatches is how many recent batches per producer are remembered for
// duplicate detection. The client keeps one in-flight batch per partition,
// so a retry is always for one of the last few batches.
const recentBatches = 5

// batchEntry maps a contiguous run of sequences to contiguous offsets.
type batchEntry struct {
	FirstSeq, LastSeq int64
	BaseOffset        int64
}

func (b batchEntry) contains(first, last int64) bool {
	return first >= b.FirstSeq && last <= b.LastSeq
}

// producerEntry is the idempotency state for one producer on one partition.
type producerEntry struct {
	lastSeq    int64
	batches    []batchEntry // oldest first, at most recentBatches
	lastUpdate time.Time
}

// producerTable implements idempotent-producer duplicate suppression.
//
// Each idempotent producer has an ID (allocated by the controller) and,
// per partition, a sequence number that increases by one per record. The
// leader accepts a batch only if its first sequence is exactly lastSeq+1.
// A batch whose sequences were already accepted is a retry: the leader
// returns the original offsets instead of appending again.
//
// The table is rebuilt from the sequence numbers stored in the log at
// startup, and followers maintain it from replicated records, so a newly
// elected leader can still recognize retries of batches the old leader
// accepted.
//
// It is not safe for concurrent use; Partition guards it with appendMu.
type producerTable struct {
	m map[int64]*producerEntry
}

func newProducerTable() *producerTable { return &producerTable{m: map[int64]*producerEntry{}} }

// checkResult is the outcome of validating a batch.
type checkResult int

const (
	seqOK checkResult = iota
	seqDuplicate
	seqDuplicateUnknownOffsets
	seqOutOfOrder
)

// check validates the sequence range [first, first+n-1] for producer pid.
// For duplicates it returns the base offset of the original append.
func (t *producerTable) check(pid, first int64, n int) (checkResult, int64) {
	last := first + int64(n) - 1
	e, ok := t.m[pid]
	if !ok {
		// A producer we do not know (new, or expired) must start a fresh
		// sequence space at 0. Anything else could be a retry of a batch
		// whose state we lost, so we refuse rather than risk a duplicate.
		if first == 0 {
			return seqOK, 0
		}
		return seqOutOfOrder, 0
	}
	if first == e.lastSeq+1 {
		return seqOK, 0
	}
	if last <= e.lastSeq {
		for _, b := range e.batches {
			if b.contains(first, last) {
				return seqDuplicate, b.BaseOffset + (first - b.FirstSeq)
			}
		}
		return seqDuplicateUnknownOffsets, -1
	}
	return seqOutOfOrder, 0
}

// record notes that sequences [first, first+n-1] were appended at baseOffset.
func (t *producerTable) record(pid, first int64, n int, baseOffset int64, now time.Time) {
	e, ok := t.m[pid]
	if !ok {
		e = &producerEntry{lastSeq: -1}
		t.m[pid] = e
	}
	last := first + int64(n) - 1
	if last <= e.lastSeq {
		return
	}
	// Merge with the previous batch when sequences and offsets are both
	// contiguous: this happens when a follower rebuilds state record by
	// record, and the merged range still maps sequences to offsets exactly.
	if k := len(e.batches); k > 0 {
		p := &e.batches[k-1]
		if p.LastSeq+1 == first && p.BaseOffset+(p.LastSeq-p.FirstSeq)+1 == baseOffset {
			p.LastSeq = last
			e.lastSeq = last
			e.lastUpdate = now
			return
		}
	}
	e.batches = append(e.batches, batchEntry{first, last, baseOffset})
	if len(e.batches) > recentBatches {
		e.batches = e.batches[len(e.batches)-recentBatches:]
	}
	e.lastSeq = last
	e.lastUpdate = now
}

// expire removes producers idle for longer than ttl. Expiration bounds
// memory, at the cost that a producer silent for longer than ttl must start
// a new sequence space (it receives an out-of-order error and re-registers).
func (t *producerTable) expire(now time.Time, ttl time.Duration) int {
	n := 0
	for pid, e := range t.m {
		if now.Sub(e.lastUpdate) > ttl {
			delete(t.m, pid)
			n++
		}
	}
	return n
}

func (t *producerTable) len() int { return len(t.m) }
