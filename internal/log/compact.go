package log

import (
	"fmt"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/record"
	"github.com/michael-emmanuel/eventstream/internal/segment"
)

// CompactionResult reports what a compaction pass did.
type CompactionResult struct {
	SegmentsRewritten int
	RecordsKept       int
	RecordsRemoved    int
	TombstonesRemoved int
	BytesBefore       int64
	BytesAfter        int64
	DistinctKeys      int
	Duration          time.Duration
}

// Compact performs key-based log compaction on sealed segments.
//
// Algorithm (deliberately simple and obviously correct, not fast):
//
//  1. Scan the whole log (sealed + active, up to the current end) and build
//     a map key -> latest offset. Memory is O(distinct keys).
//  2. For each sealed segment, write a copy containing only records that are
//     the latest for their key. Tombstones are kept until they are older
//     than tombstoneRetention, so consumers that are behind still observe
//     the delete before it disappears.
//  3. Atomically rename each cleaned copy over the original.
//
// Offsets are preserved, so compaction creates gaps; readers skip them. The
// active segment is never compacted, so the newest records are always
// readable in their original form. Records without a key are kept: a
// compacted topic rejects them at produce time, so they are not expected.
//
// Crash safety: until the rename, the original segment is intact and the
// ".cleaned" file is discarded by recovery. After the rename the cleaned
// segment is complete because it was fsynced first.
//
// Limitations versus production compactors: no dirty/clean ratio tracking
// (every pass rescans everything), no bounded-memory offset map, no merging
// of small segments, and compaction I/O is not throttled.
func (l *Log) Compact(now time.Time, tombstoneRetention time.Duration) (CompactionResult, error) {
	start := time.Now()
	l.maintMu.Lock()
	defer l.maintMu.Unlock()

	var res CompactionResult
	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return res, ErrClosed
	}
	sealed := append([]*segment.Segment(nil), l.segments[:len(l.segments)-1]...)
	l.mu.RUnlock()
	if len(sealed) == 0 {
		return res, nil
	}

	latest := map[string]int64{}
	from := l.StartOffset()
	end := l.EndOffset()
	for from < end {
		r, err := l.ReadFrames(from, end, 1<<20, 0)
		if err != nil {
			return res, err
		}
		if r.Count == 0 {
			break
		}
		b := r.Frames
		for len(b) > 0 {
			rec, n, err := record.Decode(b, l.cfg.MaxRecordSize)
			if err != nil {
				return res, err
			}
			if rec.Key != nil {
				latest[string(rec.Key)] = rec.Offset
			}
			b = b[n:]
		}
		from = r.LastOffset + 1
	}
	res.DistinctKeys = len(latest)
	tombCutoff := now.Add(-tombstoneRetention).UnixMilli()

	for _, s := range sealed {
		res.BytesBefore += s.Size()
		tombstonesDropped := 0
		keep := func(frame []byte) (bool, error) {
			rec, _, err := record.Decode(frame, l.cfg.MaxRecordSize)
			if err != nil {
				return false, err
			}
			if rec.Key == nil {
				return true, nil
			}
			if latest[string(rec.Key)] != rec.Offset {
				return false, nil
			}
			if rec.Tombstone && rec.Timestamp < tombCutoff {
				tombstonesDropped++
				return false, nil
			}
			return true, nil
		}
		kept, dropped, keptBytes, err := s.Filter(keep)
		if err != nil {
			return res, fmt.Errorf("compact segment %d: %w", s.Base(), err)
		}
		res.RecordsKept += kept
		res.RecordsRemoved += dropped
		res.TombstonesRemoved += tombstonesDropped
		if dropped == 0 {
			s.DiscardCleaned()
			res.BytesAfter += s.Size()
			continue
		}
		l.mu.Lock()
		ns, err := s.ReplaceWithCleaned()
		if err == nil {
			for i := range l.segments {
				if l.segments[i] == s {
					l.segments[i] = ns
				}
			}
		}
		l.mu.Unlock()
		if err != nil {
			return res, fmt.Errorf("swap compacted segment %d: %w", s.Base(), err)
		}
		res.SegmentsRewritten++
		res.BytesAfter += keptBytes
	}
	res.Duration = time.Since(start)
	return res, nil
}
