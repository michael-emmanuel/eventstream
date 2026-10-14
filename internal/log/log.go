// Package log implements a partition's durable, append-only, segmented log.
//
// A Log is a sequence of segments ordered by base offset. Only the last
// segment (the "active" segment) is written to; all others are sealed. Offsets
// are partition-local and strictly increasing. After compaction, offsets may
// have gaps; readers handle that by returning the next record at or after the
// requested offset.
//
// Locking:
//
//	appendMu  serializes writers (append, truncate, reset). Readers never take it.
//	maintMu   serializes maintenance (retention, compaction, truncation) so they
//	          never operate on the same segment concurrently.
//	mu        RWMutex over the segment slice. Readers hold it shared for the
//	          duration of a read, which guarantees a segment cannot be deleted or
//	          swapped out underneath them. Appends to the active segment do not
//	          take it; only rolling a new segment does.
//
// Lock order: appendMu -> maintMu -> mu.
package log

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/record"
	"github.com/michael-emmanuel/eventstream/internal/segment"
)

// Errors.
var (
	ErrOffsetOutOfRange = errors.New("log: offset out of range")
	ErrClosed           = errors.New("log: closed")
	ErrInvalidOffsets   = errors.New("log: replicated offsets are not increasing")
)

// Hooks let tests and the fault injector intercept disk operations.
type Hooks struct {
	BeforeWrite func() error
	BeforeSync  func() error
}

// Config controls a Log.
type Config struct {
	SegmentBytes       int64
	SegmentMaxAge      time.Duration // roll the active segment once it is this old (0 = never)
	IndexIntervalBytes int
	MaxRecordSize      int
	// SyncEveryAppend fsyncs after every append. When false, durability of
	// recent appends relies on an external flusher calling Flush and on
	// replication.
	SyncEveryAppend bool
	// VerifyOnRecovery fully verifies sealed segments at startup.
	VerifyOnRecovery bool
	Now              func() time.Time
	Hooks            Hooks
}

func (c Config) withDefaults() Config {
	if c.SegmentBytes <= 0 {
		c.SegmentBytes = 64 << 20
	}
	if c.SegmentBytes > segment.MaxSegmentBytes {
		c.SegmentBytes = segment.MaxSegmentBytes
	}
	if c.MaxRecordSize <= 0 {
		c.MaxRecordSize = record.DefaultMaxRecordSize
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

func (c Config) segCfg() segment.Config {
	return segment.Config{IndexIntervalBytes: c.IndexIntervalBytes, MaxRecordSize: c.MaxRecordSize}
}

// Log is a partition log.
type Log struct {
	dir string
	cfg Config

	appendMu sync.Mutex
	maintMu  sync.Mutex

	mu       sync.RWMutex
	segments []*segment.Segment
	closed   bool

	lastTs  int64 // guarded by appendMu
	scratch []byte
}

// RecoveryReport describes what Open did. It is surfaced in logs and
// metrics so operators can see when a broker truncated data.
type RecoveryReport struct {
	Dir             string
	Segments        int
	TruncatedBytes  int64
	TruncationCause string
	RemovedFiles    []string
	IndexesRebuilt  int
	LogStartOffset  int64
	LogEndOffset    int64
	Duration        time.Duration
}

// Open opens or creates the log in dir and runs crash recovery:
//
//  1. remove leftovers of interrupted operations (*.cleaned, *.tmp, orphan indexes)
//  2. discover segments by file name and sort by base offset
//  3. sealed segments: validate the persisted index and verify the tail after it
//     (or everything, with VerifyOnRecovery)
//  4. active segment: verify every frame, truncate a torn or corrupt tail
//  5. rebuild indexes that were missing or inconsistent
//  6. check that segments do not overlap
func Open(dir string, cfg Config) (*Log, RecoveryReport, error) {
	start := time.Now()
	cfg = cfg.withDefaults()
	rep := RecoveryReport{Dir: dir}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, rep, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, rep, err
	}
	var bases []int64
	logs := map[int64]bool{}
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, segment.CleanedSuffix), strings.HasSuffix(name, ".tmp"):
			// An interrupted compaction or index write. The original file
			// is intact, so the leftover is simply discarded.
			_ = os.Remove(filepath.Join(dir, name))
			rep.RemovedFiles = append(rep.RemovedFiles, name)
		case strings.HasSuffix(name, segment.LogSuffix):
			b, err := strconv.ParseInt(strings.TrimSuffix(name, segment.LogSuffix), 10, 64)
			if err != nil {
				return nil, rep, fmt.Errorf("unexpected file %s: %w", name, err)
			}
			bases = append(bases, b)
			logs[b] = true
		}
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, segment.IndexSuffix) {
			b, err := strconv.ParseInt(strings.TrimSuffix(name, segment.IndexSuffix), 10, 64)
			if err == nil && !logs[b] {
				_ = os.Remove(filepath.Join(dir, name))
				rep.RemovedFiles = append(rep.RemovedFiles, name)
			}
		}
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })

	l := &Log{dir: dir, cfg: cfg}
	for i, b := range bases {
		mode := segment.RecoverTail
		if cfg.VerifyOnRecovery {
			mode = segment.VerifyFull
		}
		if i == len(bases)-1 {
			mode = segment.RecoverFull
		}
		s, res, err := segment.Open(dir, b, cfg.segCfg(), mode)
		if err != nil {
			l.closeSegments()
			return nil, rep, err
		}
		if res.IndexRebuilt {
			rep.IndexesRebuilt++
		}
		if res.TruncatedBytes > 0 {
			rep.TruncatedBytes += res.TruncatedBytes
			rep.TruncationCause = res.Cause.Error()
		}
		if n := len(l.segments); n > 0 && l.segments[n-1].NextOffset() > b {
			l.closeSegments()
			s.Close()
			return nil, rep, fmt.Errorf("log %s: segment %d overlaps previous segment ending at %d",
				dir, b, l.segments[n-1].NextOffset())
		}
		l.segments = append(l.segments, s)
	}
	if len(l.segments) == 0 {
		s, err := segment.Create(dir, 0, cfg.segCfg(), cfg.Now())
		if err != nil {
			return nil, rep, err
		}
		l.segments = append(l.segments, s)
	}
	l.lastTs = l.active().LastTimestamp()
	if l.lastTs < 0 {
		for i := len(l.segments) - 1; i >= 0; i-- {
			if ts := l.segments[i].LastTimestamp(); ts >= 0 {
				l.lastTs = ts
				break
			}
		}
	}
	rep.Segments = len(l.segments)
	rep.LogStartOffset = l.StartOffset()
	rep.LogEndOffset = l.EndOffset()
	rep.Duration = time.Since(start)
	return l, rep, nil
}

func (l *Log) active() *segment.Segment { return l.segments[len(l.segments)-1] }

// cur returns the active segment for writers, which hold appendMu but not mu.
// Retention may concurrently replace the slice header, so the read needs mu.
func (l *Log) cur() *segment.Segment {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.active()
}

// Dir returns the directory of the log.
func (l *Log) Dir() string { return l.dir }

// StartOffset is the first offset still retained.
func (l *Log) StartOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].Base()
}

// EndOffset (log end offset, LEO) is the offset the next record will get.
func (l *Log) EndOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.active().NextOffset()
}

// Size returns the total bytes across segments.
func (l *Log) Size() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var n int64
	for _, s := range l.segments {
		n += s.Size()
	}
	return n
}

// SegmentCount returns the number of segments.
func (l *Log) SegmentCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.segments)
}

// SegmentInfo describes a segment for admin endpoints and tests.
type SegmentInfo struct {
	Base, Next      int64
	Size            int64
	FirstTs, LastTs int64
	IndexEntries    int
}

// Segments describes all segments.
func (l *Log) Segments() []SegmentInfo {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]SegmentInfo, len(l.segments))
	for i, s := range l.segments {
		out[i] = SegmentInfo{s.Base(), s.NextOffset(), s.Size(), s.FirstTimestamp(), s.LastTimestamp(), s.IndexEntries()}
	}
	return out
}

// Append assigns offsets and timestamps to recs, encodes them and writes them
// as a single batch (one write syscall). Timestamps are the broker's
// log-append time, clamped so they never go backwards within a partition;
// that keeps timestamp lookups binary-searchable even if the wall clock
// steps back. Producer-side event time belongs in a header. Returns the
// base offset.
func (l *Log) Append(recs []*record.Record) (int64, error) {
	if len(recs) == 0 {
		return l.EndOffset(), nil
	}
	l.appendMu.Lock()
	defer l.appendMu.Unlock()
	if l.isClosed() {
		return 0, ErrClosed
	}
	now := l.cfg.Now().UnixMilli()
	if now < l.lastTs {
		now = l.lastTs
	}
	base := l.cur().NextOffset()
	buf := l.scratch[:0]
	metas := make([]segment.FrameMeta, len(recs))
	for i, r := range recs {
		if err := r.Validate(l.cfg.MaxRecordSize); err != nil {
			return 0, err
		}
		r.Offset = base + int64(i)
		r.Timestamp = now
		before := len(buf)
		buf = record.AppendEncode(buf, r)
		metas[i] = segment.FrameMeta{Offset: r.Offset, Timestamp: r.Timestamp, Size: len(buf) - before}
	}
	if err := l.writeLocked(buf, metas); err != nil {
		return 0, err
	}
	l.lastTs = recs[len(recs)-1].Timestamp
	if cap(buf) <= 4<<20 {
		l.scratch = buf
	}
	return base, nil
}

// AppendRaw appends frames copied from the leader, preserving their offsets
// and timestamps. Every frame is checksum-verified first: replication must
// never propagate corruption from one disk to another.
func (l *Log) AppendRaw(frames []byte) ([]record.Meta, error) {
	metas, err := record.SplitFrames(frames, l.cfg.MaxRecordSize)
	if err != nil {
		return nil, err
	}
	if len(metas) == 0 {
		return nil, nil
	}
	l.appendMu.Lock()
	defer l.appendMu.Unlock()
	if l.isClosed() {
		return nil, ErrClosed
	}
	prev := l.cur().NextOffset() - 1
	fm := make([]segment.FrameMeta, len(metas))
	for i, m := range metas {
		if m.Offset <= prev {
			return nil, fmt.Errorf("%w: %d after %d", ErrInvalidOffsets, m.Offset, prev)
		}
		prev = m.Offset
		fm[i] = segment.FrameMeta{Offset: m.Offset, Timestamp: m.Timestamp, Size: m.Size}
	}
	if err := l.writeLocked(frames, fm); err != nil {
		return nil, err
	}
	if ts := metas[len(metas)-1].Timestamp; ts > l.lastTs {
		l.lastTs = ts
	}
	return metas, nil
}

func (l *Log) writeLocked(buf []byte, metas []segment.FrameMeta) error {
	if err := l.maybeRollLocked(int64(len(buf)), metas[0].Offset); err != nil {
		return err
	}
	if h := l.cfg.Hooks.BeforeWrite; h != nil {
		if err := h(); err != nil {
			return err
		}
	}
	if err := l.cur().Append(buf, metas); err != nil {
		return err
	}
	if l.cfg.SyncEveryAppend {
		return l.syncActive()
	}
	return nil
}

// maybeRollLocked seals the active segment and starts a new one when the
// incoming batch would overflow it or it is older than SegmentMaxAge.
func (l *Log) maybeRollLocked(incoming int64, nextOffset int64) error {
	a := l.cur()
	if a.Size() == 0 {
		return nil
	}
	full := a.Size()+incoming > l.cfg.SegmentBytes
	old := l.cfg.SegmentMaxAge > 0 && l.cfg.Now().Sub(a.Created()) >= l.cfg.SegmentMaxAge
	if !full && !old {
		return nil
	}
	return l.rollLocked(nextOffset)
}

func (l *Log) rollLocked(base int64) error {
	a := l.cur()
	if err := a.Seal(); err != nil {
		return fmt.Errorf("seal segment: %w", err)
	}
	if base < a.NextOffset() {
		base = a.NextOffset()
	}
	s, err := segment.Create(l.dir, base, l.cfg.segCfg(), l.cfg.Now())
	if err != nil {
		return err
	}
	if err := segment.SyncDir(l.dir); err != nil {
		return err
	}
	l.mu.Lock()
	l.segments = append(l.segments, s)
	l.mu.Unlock()
	return nil
}

// MaybeRollByAge rolls a non-empty active segment that exceeded
// SegmentMaxAge, so time-based retention can eventually delete its data on a
// low-volume partition.
func (l *Log) MaybeRollByAge() (bool, error) {
	l.appendMu.Lock()
	defer l.appendMu.Unlock()
	if l.isClosed() || l.cfg.SegmentMaxAge <= 0 {
		return false, nil
	}
	a := l.cur()
	if a.Size() == 0 || l.cfg.Now().Sub(a.Created()) < l.cfg.SegmentMaxAge {
		return false, nil
	}
	return true, l.rollLocked(a.NextOffset())
}

func (l *Log) syncActive() error {
	if h := l.cfg.Hooks.BeforeSync; h != nil {
		if err := h(); err != nil {
			return err
		}
	}
	l.mu.RLock()
	a := l.active()
	l.mu.RUnlock()
	return a.Sync()
}

// Flush fsyncs the active segment. Sealed segments were fsynced on roll.
func (l *Log) Flush() error {
	if l.isClosed() {
		return nil
	}
	return l.syncActive()
}

// ReadFrames returns raw frames for offsets in [from, maxOffset). It reads
// across segment boundaries until limits are reached. It returns
// ErrOffsetOutOfRange if from is below the log start or above the end.
func (l *Log) ReadFrames(from, maxOffset int64, maxBytes, maxRecords int) (segment.ReadResult, error) {
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	if maxRecords <= 0 {
		maxRecords = 1 << 30
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return segment.ReadResult{}, ErrClosed
	}
	start, end := l.segments[0].Base(), l.active().NextOffset()
	if from < start || from > end {
		return segment.ReadResult{}, fmt.Errorf("%w: %d not in [%d, %d]", ErrOffsetOutOfRange, from, start, end)
	}
	out := segment.ReadResult{LastOffset: -1}
	i := sort.Search(len(l.segments), func(i int) bool { return l.segments[i].Base() > from }) - 1
	if i < 0 {
		i = 0
	}
	for ; i < len(l.segments); i++ {
		remBytes := maxBytes - len(out.Frames)
		remRecs := maxRecords - out.Count
		if out.Count > 0 && (remBytes <= 0 || remRecs <= 0) {
			break
		}
		if remBytes <= 0 {
			remBytes = 1
		}
		r, err := l.segments[i].ReadFrames(from, maxOffset, remBytes, remRecs, out.Frames)
		if err != nil {
			return out, err
		}
		out.Frames = r.Frames
		if r.Count > 0 {
			out.Count += r.Count
			out.LastOffset = r.LastOffset
			from = r.LastOffset + 1
		}
		// Only continue into the next segment if this one ran out of data;
		// otherwise a limit or maxOffset stopped the read.
		if !r.Exhausted {
			break
		}
	}
	return out, nil
}

// Read decodes records in [from, maxOffset).
func (l *Log) Read(from, maxOffset int64, maxBytes, maxRecords int) ([]record.Record, error) {
	r, err := l.ReadFrames(from, maxOffset, maxBytes, maxRecords)
	if err != nil {
		return nil, err
	}
	return record.DecodeAll(r.Frames, l.cfg.MaxRecordSize)
}

// ReadOne reads the record at offset (or the next one after a compaction gap).
func (l *Log) ReadOne(offset int64) (record.Record, error) {
	recs, err := l.Read(offset, l.EndOffset(), 1, 1)
	if err != nil {
		return record.Record{}, err
	}
	if len(recs) == 0 {
		return record.Record{}, fmt.Errorf("%w: no record at or after %d", ErrOffsetOutOfRange, offset)
	}
	return recs[0], nil
}

// Scan calls fn for the metadata of every record from the start of the log.
// Used at startup to rebuild producer and leader-epoch state.
func (l *Log) Scan(fn func(m record.Meta) error) error {
	from := l.StartOffset()
	for {
		end := l.EndOffset()
		if from >= end {
			return nil
		}
		r, err := l.ReadFrames(from, end, 1<<20, 0)
		if err != nil {
			return err
		}
		if r.Count == 0 {
			return nil
		}
		b := r.Frames
		for len(b) > 0 {
			m, err := record.ParseMeta(b, l.cfg.MaxRecordSize)
			if err != nil {
				return err
			}
			if err := fn(m); err != nil {
				return err
			}
			b = b[m.Size:]
		}
		from = r.LastOffset + 1
	}
}

// OffsetForTimestamp returns the first offset whose timestamp is >= ts, or
// the log end offset if every record is older.
func (l *Log) OffsetForTimestamp(ts int64) (int64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, s := range l.segments {
		if s.LastTimestamp() < ts {
			continue
		}
		off, ok, err := s.FindTimestamp(ts)
		if err != nil {
			return 0, err
		}
		if ok {
			return off, nil
		}
	}
	return l.active().NextOffset(), nil
}

// Truncate removes every record with offset >= offset. Used by followers
// whose log diverged from the new leader's.
func (l *Log) Truncate(offset int64) error {
	l.appendMu.Lock()
	defer l.appendMu.Unlock()
	l.maintMu.Lock()
	defer l.maintMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if offset >= l.active().NextOffset() {
		return nil
	}
	if offset <= l.segments[0].Base() {
		return l.resetLocked(offset)
	}
	keep := l.segments[:0]
	var drop []*segment.Segment
	for _, s := range l.segments {
		if s.Base() >= offset {
			drop = append(drop, s)
		} else {
			keep = append(keep, s)
		}
	}
	for _, s := range drop {
		if err := s.Remove(); err != nil {
			return err
		}
	}
	l.segments = keep
	if err := l.active().TruncateTo(offset); err != nil {
		return err
	}
	l.lastTs = l.active().LastTimestamp()
	return segment.SyncDir(l.dir)
}

// ResetTo discards the entire log and restarts it at offset. Used when a
// follower is so far behind that the leader already deleted the data it
// needs.
func (l *Log) ResetTo(offset int64) error {
	l.appendMu.Lock()
	defer l.appendMu.Unlock()
	l.maintMu.Lock()
	defer l.maintMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	return l.resetLocked(offset)
}

func (l *Log) resetLocked(offset int64) error {
	for _, s := range l.segments {
		if err := s.Remove(); err != nil {
			return err
		}
	}
	s, err := segment.Create(l.dir, offset, l.cfg.segCfg(), l.cfg.Now())
	if err != nil {
		return err
	}
	l.segments = []*segment.Segment{s}
	l.lastTs = -1
	return segment.SyncDir(l.dir)
}

// RetentionResult reports what retention deleted.
type RetentionResult struct {
	DeletedSegments []int64
	DeletedBytes    int64
	NewStartOffset  int64
}

// ApplyRetention deletes whole sealed segments, oldest first, while either
// policy says they are expired:
//
//	time:  the newest record in the segment is older than now-retention
//	bytes: the log would still be >= retentionBytes without the segment
//
// The active segment is never deleted, and only a prefix of the log is ever
// removed so offsets stay contiguous. Segments containing offsets at or
// beyond upTo (normally the high-water mark) are kept, so retention can
// never delete records that are not yet committed. Zero disables a policy.
func (l *Log) ApplyRetention(retention time.Duration, retentionBytes int64, now time.Time, upTo int64) (RetentionResult, error) {
	l.maintMu.Lock()
	defer l.maintMu.Unlock()

	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return RetentionResult{}, ErrClosed
	}
	var total int64
	for _, s := range l.segments {
		total += s.Size()
	}
	cutoff := now.Add(-retention).UnixMilli()
	n := 0
	for _, s := range l.segments[:len(l.segments)-1] {
		byTime := retention > 0 && s.LastTimestamp() >= 0 && s.LastTimestamp() < cutoff
		byBytes := retentionBytes > 0 && total-s.Size() >= retentionBytes
		if (!byTime && !byBytes) || s.NextOffset() > upTo {
			break
		}
		total -= s.Size()
		n++
	}
	l.mu.RUnlock()
	if n == 0 {
		return RetentionResult{NewStartOffset: l.StartOffset()}, nil
	}

	l.mu.Lock()
	victims := append([]*segment.Segment(nil), l.segments[:n]...)
	l.segments = append([]*segment.Segment(nil), l.segments[n:]...)
	start := l.segments[0].Base()
	l.mu.Unlock()

	res := RetentionResult{NewStartOffset: start}
	for _, s := range victims {
		res.DeletedSegments = append(res.DeletedSegments, s.Base())
		res.DeletedBytes += s.Size()
		if err := s.Remove(); err != nil {
			return res, err
		}
	}
	return res, segment.SyncDir(l.dir)
}

// Close persists indexes and closes all files. A cleanly closed log still
// goes through recovery on open, but finds nothing to repair.
func (l *Log) Close() error {
	l.appendMu.Lock()
	defer l.appendMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	var first error
	if err := l.active().Sync(); err != nil {
		first = err
	}
	for _, s := range l.segments {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Delete closes the log and removes its directory.
func (l *Log) Delete() error {
	_ = l.Close()
	return os.RemoveAll(l.dir)
}

func (l *Log) closeSegments() {
	for _, s := range l.segments {
		s.Close()
	}
}

func (l *Log) isClosed() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.closed
}
