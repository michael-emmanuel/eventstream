// Package segment implements one bounded, append-only file of a partition log
// together with its sparse offset index.
//
// Concurrency model: a segment has exactly one writer at a time (the owning
// log serializes appends) and any number of concurrent readers. Readers use
// ReadAt (pread) on the shared *os.File, so they need no lock against the
// writer; they only read up to the published size, which is stored
// atomically after the bytes are written. The in-memory index has its own
// small RWMutex because the writer appends entries while readers search it.
package segment

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/record"
)

const (
	// LogSuffix and IndexSuffix are the on-disk file extensions.
	LogSuffix     = ".log"
	IndexSuffix   = ".index"
	CleanedSuffix = ".cleaned"

	indexEntrySize = 8 // relative offset uint32 + position uint32

	// MaxSegmentBytes keeps positions representable in the 32-bit index.
	MaxSegmentBytes = 1<<31 - 1
)

// ErrCorrupt is returned when a sealed segment fails verification. Sealed
// segments are never silently truncated: data in the middle of a log cannot
// be dropped without breaking offset continuity, so this needs an operator
// or a re-fetch from a healthy replica.
var ErrCorrupt = errors.New("segment: corrupt segment")

// Config controls segment behaviour.
type Config struct {
	// IndexIntervalBytes is how many bytes of records are written between
	// sparse index entries. Smaller = bigger index, faster lookups.
	IndexIntervalBytes int
	// MaxRecordSize bounds a single frame when scanning.
	MaxRecordSize int
}

func (c Config) withDefaults() Config {
	if c.IndexIntervalBytes <= 0 {
		c.IndexIntervalBytes = 4096
	}
	if c.MaxRecordSize <= 0 {
		c.MaxRecordSize = record.DefaultMaxRecordSize
	}
	return c
}

// IndexEntry maps an offset to the byte position of its frame.
type IndexEntry struct {
	Offset int64
	Pos    int64
}

// FrameMeta describes one frame inside a buffer handed to Append.
type FrameMeta struct {
	Offset    int64
	Timestamp int64
	Size      int
}

// Segment is a single log file plus its sparse index.
type Segment struct {
	dir  string
	base int64
	cfg  Config
	f    *os.File

	size    atomic.Int64 // bytes of valid data, published after writes
	next    atomic.Int64 // offset after the last record (base if empty)
	firstTs atomic.Int64 // timestamp of first record, -1 if empty
	lastTs  atomic.Int64 // timestamp of last record, -1 if empty
	count   atomic.Int64 // number of records
	created time.Time

	idxMu           sync.RWMutex
	index           []IndexEntry
	bytesSinceIndex int // writer-only
}

// LogPath returns the path of the segment log file for base.
func LogPath(dir string, base int64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d%s", base, LogSuffix))
}

// IndexPath returns the path of the index file for base.
func IndexPath(dir string, base int64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d%s", base, IndexSuffix))
}

// Create makes a new empty segment starting at base.
func Create(dir string, base int64, cfg Config, now time.Time) (*Segment, error) {
	f, err := os.OpenFile(LogPath(dir, base), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("segment create: %w", err)
	}
	s := &Segment{dir: dir, base: base, cfg: cfg.withDefaults(), f: f, created: now}
	s.next.Store(base)
	s.firstTs.Store(-1)
	s.lastTs.Store(-1)
	return s, nil
}

// RecoveryMode selects how much of a segment is verified on open.
type RecoveryMode int

const (
	// RecoverTail trusts the persisted index and verifies only the bytes
	// after the last index entry. Used for sealed segments, which were
	// fsynced when they were rolled.
	RecoverTail RecoveryMode = iota
	// RecoverFull verifies every frame and truncates an invalid tail.
	// Used for the active segment, which may contain a torn write.
	RecoverFull
	// VerifyFull verifies every frame but refuses to truncate: corruption
	// is reported as ErrCorrupt. Used for sealed segments when deep
	// verification is requested.
	VerifyFull
)

// RecoveryResult reports what Open found.
type RecoveryResult struct {
	Base           int64
	Records        int64
	ValidBytes     int64
	TruncatedBytes int64
	IndexRebuilt   bool
	Cause          error // why the tail was truncated, if it was
}

// Open opens an existing segment and recovers its in-memory state.
func Open(dir string, base int64, cfg Config, mode RecoveryMode) (*Segment, RecoveryResult, error) {
	cfg = cfg.withDefaults()
	f, err := os.OpenFile(LogPath(dir, base), os.O_RDWR, 0o644)
	if err != nil {
		return nil, RecoveryResult{}, fmt.Errorf("segment open: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, RecoveryResult{}, err
	}
	s := &Segment{dir: dir, base: base, cfg: cfg, f: f, created: st.ModTime()}
	s.next.Store(base)
	s.firstTs.Store(-1)
	s.lastTs.Store(-1)
	res, err := s.recover(st.Size(), mode)
	if err != nil {
		f.Close()
		return nil, res, err
	}
	return s, res, nil
}

func (s *Segment) recover(fileSize int64, mode RecoveryMode) (RecoveryResult, error) {
	res := RecoveryResult{Base: s.base}
	startPos := int64(0)
	if mode == RecoverTail {
		if idx, ok := s.loadIndex(fileSize); ok && len(idx) > 0 {
			s.index = idx
			startPos = idx[len(idx)-1].Pos
			// The first record's timestamp is needed for time-based lookups.
			if ts, ok := s.readFirstTimestamp(fileSize); ok {
				s.firstTs.Store(ts)
			}
		}
	}
	if startPos == 0 {
		s.index = nil
		res.IndexRebuilt = true
	}

	sr := io.NewSectionReader(s.f, startPos, fileSize-startPos)
	br := bufio.NewReaderSize(sr, 64<<10)
	pos := startPos
	prev := int64(-1)
	var scratch []byte
	var records int64
	var scanErr error
	for {
		frame, err := record.ReadFrame(br, scratch, s.cfg.MaxRecordSize)
		if err == io.EOF {
			break
		}
		if err != nil {
			scanErr = err
			break
		}
		scratch = frame
		m, err := record.ParseMeta(frame, s.cfg.MaxRecordSize)
		if err != nil {
			scanErr = err
			break
		}
		if m.Offset < s.base || m.Offset <= prev {
			scanErr = fmt.Errorf("%w: non-monotonic offset %d after %d", record.ErrMalformed, m.Offset, prev)
			break
		}
		if res.IndexRebuilt || pos > startPos {
			s.maybeIndex(m.Offset, pos, len(frame))
		} else {
			// The first frame at startPos is already indexed.
			s.bytesSinceIndex = len(frame)
		}
		if s.firstTs.Load() < 0 {
			s.firstTs.Store(m.Timestamp)
		}
		s.lastTs.Store(m.Timestamp)
		s.next.Store(m.Offset + 1)
		prev = m.Offset
		pos += int64(len(frame))
		records++
	}
	res.ValidBytes = pos
	if scanErr != nil {
		switch mode {
		case RecoverFull:
			if err := s.f.Truncate(pos); err != nil {
				return res, fmt.Errorf("truncate corrupt tail: %w", err)
			}
			if err := s.f.Sync(); err != nil {
				return res, err
			}
			res.TruncatedBytes = fileSize - pos
			res.Cause = scanErr
		default:
			return res, fmt.Errorf("%w %s at byte %d: %v", ErrCorrupt, LogPath(s.dir, s.base), pos, scanErr)
		}
	}
	if mode == RecoverTail && !res.IndexRebuilt {
		// Count only covers the tail; a full count would need a full scan.
		records = -1
	}
	res.Records = records
	s.count.Store(records)
	s.size.Store(pos)
	return res, nil
}

func (s *Segment) readFirstTimestamp(fileSize int64) (int64, bool) {
	br := bufio.NewReader(io.NewSectionReader(s.f, 0, fileSize))
	frame, err := record.ReadFrame(br, nil, s.cfg.MaxRecordSize)
	if err != nil {
		return 0, false
	}
	m, err := record.ParseMeta(frame, s.cfg.MaxRecordSize)
	if err != nil {
		return 0, false
	}
	return m.Timestamp, true
}

// loadIndex reads and validates the persisted index. Any inconsistency
// means the index is discarded and rebuilt from the log.
func (s *Segment) loadIndex(fileSize int64) ([]IndexEntry, bool) {
	b, err := os.ReadFile(IndexPath(s.dir, s.base))
	if err != nil || len(b)%indexEntrySize != 0 {
		return nil, false
	}
	idx := make([]IndexEntry, 0, len(b)/indexEntrySize)
	for i := 0; i < len(b); i += indexEntrySize {
		e := IndexEntry{
			Offset: s.base + int64(binary.BigEndian.Uint32(b[i:])),
			Pos:    int64(binary.BigEndian.Uint32(b[i+4:])),
		}
		if e.Pos >= fileSize {
			return nil, false
		}
		if n := len(idx); n > 0 && (e.Offset <= idx[n-1].Offset || e.Pos <= idx[n-1].Pos) {
			return nil, false
		}
		idx = append(idx, e)
	}
	return idx, true
}

// maybeIndex adds a sparse index entry when enough bytes were written since
// the previous one. The first frame of a segment is always indexed.
func (s *Segment) maybeIndex(offset, pos int64, size int) {
	if len(s.index) == 0 || s.bytesSinceIndex >= s.cfg.IndexIntervalBytes {
		s.idxMu.Lock()
		s.index = append(s.index, IndexEntry{Offset: offset, Pos: pos})
		s.idxMu.Unlock()
		s.bytesSinceIndex = 0
	}
	s.bytesSinceIndex += size
}

// Append writes a buffer of already-encoded frames. metas must describe
// every frame in buf in order. Append is not safe for concurrent writers.
func (s *Segment) Append(buf []byte, metas []FrameMeta) error {
	pos := s.size.Load()
	if _, err := s.f.WriteAt(buf, pos); err != nil {
		// A failed write may have left a partial frame on disk. Truncate
		// back so later appends do not land behind garbage.
		_ = s.f.Truncate(pos)
		return fmt.Errorf("segment write: %w", err)
	}
	p := pos
	for _, m := range metas {
		s.maybeIndex(m.Offset, p, m.Size)
		p += int64(m.Size)
	}
	if len(metas) > 0 {
		if s.firstTs.Load() < 0 {
			s.firstTs.Store(metas[0].Timestamp)
		}
		last := metas[len(metas)-1]
		s.lastTs.Store(last.Timestamp)
		s.next.Store(last.Offset + 1)
		s.count.Add(int64(len(metas)))
	}
	// Publish the new size last: readers never see unwritten bytes.
	s.size.Store(pos + int64(len(buf)))
	return nil
}

// lookup returns the byte position from which to scan for offset.
func (s *Segment) lookup(offset int64) int64 {
	s.idxMu.RLock()
	defer s.idxMu.RUnlock()
	i := sort.Search(len(s.index), func(i int) bool { return s.index[i].Offset > offset })
	if i == 0 {
		return 0
	}
	return s.index[i-1].Pos
}

// ReadResult is the outcome of ReadFrames.
type ReadResult struct {
	Frames     []byte
	Count      int
	LastOffset int64
	// Exhausted is true when the read stopped because the segment had no
	// more data, rather than because a limit or maxOffset was reached.
	Exhausted bool
}

// ReadFrames returns raw frames for records with from <= offset < maxOffset.
// It stops once adding a frame would exceed maxBytes or maxRecords, but
// always returns at least one frame if one is available, so a single large
// record can never wedge a reader.
func (s *Segment) ReadFrames(from, maxOffset int64, maxBytes, maxRecords int, dst []byte) (ReadResult, error) {
	res := ReadResult{Frames: dst, LastOffset: -1}
	if from >= maxOffset {
		return res, nil
	}
	if from >= s.next.Load() {
		res.Exhausted = true
		return res, nil
	}
	end := s.size.Load()
	pos := s.lookup(from)
	if pos >= end {
		res.Exhausted = true
		return res, nil
	}
	br := bufio.NewReaderSize(io.NewSectionReader(s.f, pos, end-pos), 32<<10)
	var scratch []byte
	for {
		frame, err := record.ReadFrame(br, scratch, s.cfg.MaxRecordSize)
		if err == io.EOF {
			res.Exhausted = true
			break
		}
		if err != nil {
			return res, fmt.Errorf("segment %d read at %d: %w", s.base, pos, err)
		}
		scratch = frame
		pos += int64(len(frame))
		off := record.FrameOffset(frame)
		if off < from {
			continue
		}
		if off >= maxOffset {
			break
		}
		if res.Count > 0 && len(res.Frames)-len(dst)+len(frame) > maxBytes {
			break
		}
		res.Frames = append(res.Frames, frame...)
		res.Count++
		res.LastOffset = off
		if res.Count >= maxRecords || len(res.Frames)-len(dst) >= maxBytes {
			break
		}
	}
	return res, nil
}

// FindTimestamp returns the offset of the first record with timestamp >= ts.
func (s *Segment) FindTimestamp(ts int64) (int64, bool, error) {
	if s.lastTs.Load() < ts {
		return 0, false, nil
	}
	end := s.size.Load()
	br := bufio.NewReaderSize(io.NewSectionReader(s.f, 0, end), 32<<10)
	var scratch []byte
	for {
		frame, err := record.ReadFrame(br, scratch, s.cfg.MaxRecordSize)
		if err == io.EOF {
			return 0, false, nil
		}
		if err != nil {
			return 0, false, err
		}
		scratch = frame
		if record.FrameTimestamp(frame) >= ts {
			return record.FrameOffset(frame), true, nil
		}
	}
}

// PositionOf returns the byte position of the first record with offset >= target.
func (s *Segment) PositionOf(target int64) (int64, error) {
	end := s.size.Load()
	pos := s.lookup(target)
	br := bufio.NewReaderSize(io.NewSectionReader(s.f, pos, end-pos), 32<<10)
	var scratch []byte
	for {
		frame, err := record.ReadFrame(br, scratch, s.cfg.MaxRecordSize)
		if err == io.EOF {
			return end, nil
		}
		if err != nil {
			return 0, err
		}
		scratch = frame
		if record.FrameOffset(frame) >= target {
			return pos, nil
		}
		pos += int64(len(frame))
	}
}

// TruncateTo removes every record with offset >= target and recomputes the
// in-memory state by rescanning. Truncation is rare (follower divergence),
// so the rescan cost is acceptable and keeps the logic obviously correct.
func (s *Segment) TruncateTo(target int64) error {
	pos, err := s.PositionOf(target)
	if err != nil {
		return err
	}
	if err := s.f.Truncate(pos); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	s.idxMu.Lock()
	s.index = nil
	s.idxMu.Unlock()
	s.bytesSinceIndex = 0
	s.next.Store(s.base)
	s.firstTs.Store(-1)
	s.lastTs.Store(-1)
	s.count.Store(0)
	_, err = s.recover(pos, RecoverFull)
	_ = os.Remove(IndexPath(s.dir, s.base))
	return err
}

// Filter writes the frames for which keep returns true into a ".cleaned"
// sibling file and returns how many records and bytes were kept. The
// original file is untouched; Log swaps the files with an atomic rename.
func (s *Segment) Filter(keep func(frame []byte) (bool, error)) (kept, dropped int, keptBytes int64, err error) {
	path := LogPath(s.dir, s.base) + CleanedSuffix
	out, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() {
		if err != nil {
			out.Close()
			os.Remove(path)
		}
	}()
	bw := bufio.NewWriterSize(out, 64<<10)
	end := s.size.Load()
	br := bufio.NewReaderSize(io.NewSectionReader(s.f, 0, end), 64<<10)
	var scratch []byte
	for {
		frame, rerr := record.ReadFrame(br, scratch, s.cfg.MaxRecordSize)
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return 0, 0, 0, rerr
		}
		scratch = frame
		ok, kerr := keep(frame)
		if kerr != nil {
			return 0, 0, 0, kerr
		}
		if !ok {
			dropped++
			continue
		}
		if _, err = bw.Write(frame); err != nil {
			return 0, 0, 0, err
		}
		kept++
		keptBytes += int64(len(frame))
	}
	if err = bw.Flush(); err != nil {
		return 0, 0, 0, err
	}
	if err = out.Sync(); err != nil {
		return 0, 0, 0, err
	}
	if err = out.Close(); err != nil {
		return 0, 0, 0, err
	}
	return kept, dropped, keptBytes, nil
}

// ReplaceWithCleaned atomically swaps the ".cleaned" file in place of this
// segment and returns the reopened segment. The receiver must not be used
// afterwards. Callers must hold exclusive access to the segment.
func (s *Segment) ReplaceWithCleaned() (*Segment, error) {
	if err := s.f.Close(); err != nil {
		return nil, err
	}
	logPath := LogPath(s.dir, s.base)
	if err := os.Rename(logPath+CleanedSuffix, logPath); err != nil {
		return nil, err
	}
	if err := syncDir(s.dir); err != nil {
		return nil, err
	}
	_ = os.Remove(IndexPath(s.dir, s.base))
	ns, _, err := Open(s.dir, s.base, s.cfg, VerifyFull)
	if err != nil {
		return nil, err
	}
	if err := ns.WriteIndex(); err != nil {
		return nil, err
	}
	return ns, nil
}

// DiscardCleaned removes a ".cleaned" file produced by Filter that will not
// be used.
func (s *Segment) DiscardCleaned() {
	_ = os.Remove(LogPath(s.dir, s.base) + CleanedSuffix)
}

// WriteIndex persists the sparse index with a write-then-rename so a crash
// never leaves a half-written index in place.
func (s *Segment) WriteIndex() error {
	s.idxMu.RLock()
	b := make([]byte, 0, len(s.index)*indexEntrySize)
	for _, e := range s.index {
		b = binary.BigEndian.AppendUint32(b, uint32(e.Offset-s.base))
		b = binary.BigEndian.AppendUint32(b, uint32(e.Pos))
	}
	s.idxMu.RUnlock()
	path := IndexPath(s.dir, s.base)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Sync flushes the segment file to stable storage.
func (s *Segment) Sync() error { return s.f.Sync() }

// Seal fsyncs the segment and persists its index. A sealed segment is
// immutable until retention deletes it or compaction rewrites it.
func (s *Segment) Seal() error {
	if err := s.f.Sync(); err != nil {
		return err
	}
	return s.WriteIndex()
}

// Close persists the index and closes the file.
func (s *Segment) Close() error {
	ierr := s.WriteIndex()
	cerr := s.f.Close()
	if ierr != nil {
		return ierr
	}
	return cerr
}

// Remove closes and deletes the segment files.
func (s *Segment) Remove() error {
	s.f.Close()
	err1 := os.Remove(LogPath(s.dir, s.base))
	err2 := os.Remove(IndexPath(s.dir, s.base))
	if err1 != nil && !os.IsNotExist(err1) {
		return err1
	}
	if err2 != nil && !os.IsNotExist(err2) {
		return err2
	}
	return nil
}

// Accessors.
func (s *Segment) Base() int64           { return s.base }
func (s *Segment) NextOffset() int64     { return s.next.Load() }
func (s *Segment) Size() int64           { return s.size.Load() }
func (s *Segment) FirstTimestamp() int64 { return s.firstTs.Load() }
func (s *Segment) LastTimestamp() int64  { return s.lastTs.Load() }
func (s *Segment) Created() time.Time    { return s.created }
func (s *Segment) IndexEntries() int {
	s.idxMu.RLock()
	defer s.idxMu.RUnlock()
	return len(s.index)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// SyncDir fsyncs a directory so that file creations, renames and deletions
// inside it are durable.
func SyncDir(dir string) error { return syncDir(dir) }
