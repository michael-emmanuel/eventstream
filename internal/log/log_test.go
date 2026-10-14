package log

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/record"
	"github.com/michael-emmanuel/eventstream/internal/segment"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func smallCfg(clk *fakeClock) Config {
	return Config{SegmentBytes: 1024, IndexIntervalBytes: 128, Now: clk.Now}
}

func mustOpen(t *testing.T, dir string, cfg Config) *Log {
	t.Helper()
	l, _, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func appendN(t *testing.T, l *Log, n int, keyf func(i int) string) {
	t.Helper()
	for i := 0; i < n; i++ {
		r := &record.Record{Value: []byte(fmt.Sprintf("value-%04d", i)), Sequence: -1}
		if keyf != nil {
			r.Key = []byte(keyf(i))
		}
		if _, err := l.Append([]*record.Record{r}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAppendReadOrderingAndRolling(t *testing.T) {
	clk := newClock()
	l := mustOpen(t, t.TempDir(), smallCfg(clk))
	defer l.Close()
	appendN(t, l, 200, nil)
	if got := l.EndOffset(); got != 200 {
		t.Fatalf("LEO=%d", got)
	}
	if l.SegmentCount() < 5 {
		t.Fatalf("expected rolling, got %d segments", l.SegmentCount())
	}
	// Read everything in small batches and check strict ordering.
	next := int64(0)
	for next < 200 {
		recs, err := l.Read(next, 200, 300, 7)
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) == 0 {
			t.Fatalf("no progress at %d", next)
		}
		for _, r := range recs {
			if r.Offset != next {
				t.Fatalf("offset %d, want %d", r.Offset, next)
			}
			if want := fmt.Sprintf("value-%04d", next); string(r.Value) != want {
				t.Fatalf("value %q want %q", r.Value, want)
			}
			next++
		}
	}
	// maxOffset (the high-water mark for consumers) is respected.
	recs, _ := l.Read(10, 15, 1<<20, 100)
	if len(recs) != 5 || recs[4].Offset != 14 {
		t.Fatalf("bounded read returned %d", len(recs))
	}
	// Reads past the end are empty, below the start are out of range.
	if recs, _ := l.Read(200, 200, 100, 100); len(recs) != 0 {
		t.Fatal("expected empty read at LEO")
	}
	if _, err := l.Read(201, 300, 100, 100); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("err=%v", err)
	}
}

func TestBatchAppendAssignsContiguousOffsets(t *testing.T) {
	l := mustOpen(t, t.TempDir(), smallCfg(newClock()))
	defer l.Close()
	batch := make([]*record.Record, 10)
	for i := range batch {
		batch[i] = &record.Record{Value: []byte("x")}
	}
	base, err := l.Append(batch)
	if err != nil || base != 0 {
		t.Fatal(base, err)
	}
	base, _ = l.Append(batch[:3])
	if base != 10 || l.EndOffset() != 13 {
		t.Fatalf("base %d LEO %d", base, l.EndOffset())
	}
}

func TestLargeRecordAlwaysReturned(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Config{SegmentBytes: 1 << 20})
	defer l.Close()
	big := make([]byte, 64<<10)
	l.Append([]*record.Record{{Value: big}})
	recs, err := l.Read(0, 1, 100, 10)
	if err != nil || len(recs) != 1 {
		t.Fatalf("reader wedged on large record: %d %v", len(recs), err)
	}
}

func TestRecoveryAfterCleanClose(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	l := mustOpen(t, dir, smallCfg(clk))
	appendN(t, l, 100, nil)
	l.Close()
	l2, rep, err := Open(dir, smallCfg(clk))
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if rep.TruncatedBytes != 0 || l2.EndOffset() != 100 {
		t.Fatalf("rep=%+v LEO=%d", rep, l2.EndOffset())
	}
	r, err := l2.ReadOne(57)
	if err != nil || string(r.Value) != "value-0057" {
		t.Fatalf("%v %v", r, err)
	}
	appendN(t, l2, 1, nil)
	if l2.EndOffset() != 101 {
		t.Fatal("append after recovery")
	}
}

func lastSegmentPath(t *testing.T, l *Log) string {
	segs := l.Segments()
	return segment.LogPath(l.Dir(), segs[len(segs)-1].Base)
}

// A crash in the middle of a write leaves a partial frame. Recovery must
// truncate it and keep every complete record before it.
func TestRecoveryTruncatesTornWrite(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	l := mustOpen(t, dir, smallCfg(clk))
	appendN(t, l, 50, nil)
	path := lastSegmentPath(t, l)
	leo := l.EndOffset()
	l.Close()

	frame := record.Encode(&record.Record{Offset: leo, Value: []byte("half-written")})
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	f.Write(frame[:len(frame)-5])
	f.Close()

	l2, rep, err := Open(dir, smallCfg(clk))
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if rep.TruncatedBytes != int64(len(frame)-5) {
		t.Fatalf("truncated %d, want %d (%s)", rep.TruncatedBytes, len(frame)-5, rep.TruncationCause)
	}
	if l2.EndOffset() != leo {
		t.Fatalf("LEO %d want %d", l2.EndOffset(), leo)
	}
	appendN(t, l2, 1, nil)
	r, err := l2.ReadOne(leo)
	if err != nil || r.Offset != leo {
		t.Fatalf("post-recovery append unreadable: %v", err)
	}
}

// A flipped bit in the last record of the active segment is detected by the
// checksum and the record is dropped.
func TestRecoveryDetectsCorruptTail(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	l := mustOpen(t, dir, smallCfg(clk))
	appendN(t, l, 20, nil)
	path := lastSegmentPath(t, l)
	l.Close()
	b, _ := os.ReadFile(path)
	b[len(b)-3] ^= 0xFF
	os.WriteFile(path, b, 0o644)
	l2, rep, err := Open(dir, smallCfg(clk))
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if rep.TruncatedBytes == 0 || l2.EndOffset() != 19 {
		t.Fatalf("rep %+v LEO %d", rep, l2.EndOffset())
	}
}

// Corruption inside a sealed segment cannot be fixed by truncation without
// losing every later segment, so deep verification refuses to open.
func TestVerifyDetectsSealedCorruption(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	l := mustOpen(t, dir, smallCfg(clk))
	appendN(t, l, 100, nil)
	first := segment.LogPath(dir, 0)
	l.Close()
	b, _ := os.ReadFile(first)
	b[40] ^= 0x01
	os.WriteFile(first, b, 0o644)
	cfg := smallCfg(clk)
	cfg.VerifyOnRecovery = true
	if _, _, err := Open(dir, cfg); !errors.Is(err, segment.ErrCorrupt) {
		t.Fatalf("err=%v, want ErrCorrupt", err)
	}
	// Without deep verification the broker starts, and the corrupt record
	// is caught by its checksum when read.
	l2, _, err := Open(dir, smallCfg(clk))
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if _, err := l2.Read(0, 10, 1<<20, 10); !errors.Is(err, record.ErrChecksum) {
		t.Fatalf("read err=%v", err)
	}
}

func TestRecoveryRebuildsMissingIndexAndRemovesLeftovers(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	l := mustOpen(t, dir, smallCfg(clk))
	appendN(t, l, 100, nil)
	l.Close()
	os.Remove(segment.IndexPath(dir, 0))
	os.WriteFile(segment.LogPath(dir, 0)+segment.CleanedSuffix, []byte("junk"), 0o644)
	os.WriteFile(segment.IndexPath(dir, 999999), []byte("orphan"), 0o644)
	l2, rep, err := Open(dir, smallCfg(clk))
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if rep.IndexesRebuilt == 0 || len(rep.RemovedFiles) != 2 {
		t.Fatalf("rep=%+v", rep)
	}
	r, err := l2.ReadOne(3)
	if err != nil || r.Offset != 3 {
		t.Fatal(err)
	}
}

func TestSparseIndexIsSparse(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Config{SegmentBytes: 1 << 20, IndexIntervalBytes: 1024})
	defer l.Close()
	appendN(t, l, 1000, nil)
	s := l.Segments()[0]
	// ~60-byte records, one entry per 1 KiB => roughly 1 entry per 16 records.
	if s.IndexEntries < 30 || s.IndexEntries > 120 {
		t.Fatalf("index entries %d for 1000 records", s.IndexEntries)
	}
	for _, off := range []int64{0, 1, 499, 998, 999} {
		r, err := l.ReadOne(off)
		if err != nil || r.Offset != off {
			t.Fatalf("lookup %d: %v", off, err)
		}
	}
}

func TestRetentionByTime(t *testing.T) {
	clk := newClock()
	l := mustOpen(t, t.TempDir(), smallCfg(clk))
	defer l.Close()
	appendN(t, l, 100, nil)
	clk.Advance(2 * time.Hour)
	appendN(t, l, 100, nil)
	before := l.SegmentCount()
	res, err := l.ApplyRetention(time.Hour, 0, clk.Now(), l.EndOffset())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.DeletedSegments) == 0 || l.SegmentCount() >= before {
		t.Fatalf("nothing deleted: %+v", res)
	}
	if l.StartOffset() > 100 {
		t.Fatalf("deleted fresh data: start=%d", l.StartOffset())
	}
	if _, err := l.Read(0, 200, 100, 1); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("expected out of range below log start, got %v", err)
	}
}

func TestRetentionByBytesNeverDeletesActive(t *testing.T) {
	clk := newClock()
	l := mustOpen(t, t.TempDir(), smallCfg(clk))
	defer l.Close()
	appendN(t, l, 300, nil)
	_, err := l.ApplyRetention(0, 1, clk.Now(), l.EndOffset())
	if err != nil {
		t.Fatal(err)
	}
	if l.SegmentCount() != 1 {
		t.Fatalf("segments=%d, want only the active segment", l.SegmentCount())
	}
	// Retention never deletes beyond the high-water mark.
	appendN(t, l, 300, nil)
	hwm := l.StartOffset() + 1
	l.ApplyRetention(0, 1, clk.Now(), hwm)
	if l.StartOffset() >= hwm {
		t.Fatal("retention deleted uncommitted data")
	}
	if l.EndOffset() != 600 {
		t.Fatal("LEO changed")
	}
}

func TestRollByAge(t *testing.T) {
	clk := newClock()
	cfg := smallCfg(clk)
	cfg.SegmentBytes = 1 << 20
	cfg.SegmentMaxAge = time.Minute
	l := mustOpen(t, t.TempDir(), cfg)
	defer l.Close()
	appendN(t, l, 3, nil)
	clk.Advance(2 * time.Minute)
	rolled, err := l.MaybeRollByAge()
	if err != nil || !rolled || l.SegmentCount() != 2 {
		t.Fatalf("rolled=%v err=%v segs=%d", rolled, err, l.SegmentCount())
	}
}

func TestCompactionKeepsLatestPerKey(t *testing.T) {
	clk := newClock()
	l := mustOpen(t, t.TempDir(), smallCfg(clk))
	defer l.Close()
	// 5 keys, 40 updates each.
	appendN(t, l, 200, func(i int) string { return fmt.Sprintf("user-%d", i%5) })
	// Delete user-0 with a tombstone, then roll so everything is sealed.
	l.Append([]*record.Record{{Key: []byte("user-0"), Tombstone: true}})
	appendN(t, l, 30, func(i int) string { return "other" })
	leo := l.EndOffset()
	before := l.Size()

	res, err := l.Compact(clk.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if res.RecordsRemoved == 0 || l.Size() >= before {
		t.Fatalf("nothing compacted: %+v", res)
	}
	if l.EndOffset() != leo {
		t.Fatal("compaction changed LEO")
	}
	latest := map[string]record.Record{}
	prev := int64(-1)
	recs, err := l.Read(l.StartOffset(), leo, 1<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if r.Offset <= prev {
			t.Fatal("ordering broken by compaction")
		}
		prev = r.Offset
		latest[string(r.Key)] = r
	}
	for k := 1; k < 5; k++ {
		key := fmt.Sprintf("user-%d", k)
		want := fmt.Sprintf("value-%04d", 195+k)
		if string(latest[key].Value) != want {
			t.Fatalf("%s=%q want %q", key, latest[key].Value, want)
		}
	}
	if !latest["user-0"].Tombstone {
		t.Fatal("tombstone must survive until tombstone retention expires")
	}
	// Reading from an offset that was compacted away returns the next record.
	r, err := l.ReadOne(0)
	if err != nil || r.Offset == 0 {
		t.Fatalf("gap handling: %v %v", r.Offset, err)
	}

	// After tombstone retention, the delete marker itself is removed.
	clk.Advance(2 * time.Hour)
	appendN(t, l, 30, func(i int) string { return "other" })
	res, err = l.Compact(clk.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if res.TombstonesRemoved != 1 {
		t.Fatalf("tombstones removed %d", res.TombstonesRemoved)
	}
}

func TestCompactionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	l := mustOpen(t, dir, smallCfg(clk))
	appendN(t, l, 200, func(i int) string { return fmt.Sprintf("k%d", i%3) })
	appendN(t, l, 30, func(i int) string { return "z" })
	if _, err := l.Compact(clk.Now(), time.Hour); err != nil {
		t.Fatal(err)
	}
	leo := l.EndOffset()
	l.Close()
	cfg := smallCfg(clk)
	cfg.VerifyOnRecovery = true
	l2, _, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if l2.EndOffset() != leo {
		t.Fatal("LEO changed across restart")
	}
}

func TestTruncate(t *testing.T) {
	clk := newClock()
	l := mustOpen(t, t.TempDir(), smallCfg(clk))
	defer l.Close()
	appendN(t, l, 100, nil)
	if err := l.Truncate(37); err != nil {
		t.Fatal(err)
	}
	if l.EndOffset() != 37 {
		t.Fatalf("LEO %d", l.EndOffset())
	}
	appendN(t, l, 1, nil)
	r, _ := l.ReadOne(37)
	if r.Offset != 37 || string(r.Value) != "value-0000" {
		t.Fatalf("got %d %q", r.Offset, r.Value)
	}
	if err := l.Truncate(0); err != nil || l.EndOffset() != 0 {
		t.Fatal("truncate to zero")
	}
}

func TestAppendRawPreservesOffsetsAndRejectsCorruption(t *testing.T) {
	l := mustOpen(t, t.TempDir(), smallCfg(newClock()))
	defer l.Close()
	var buf []byte
	for i := 0; i < 5; i++ {
		buf = record.AppendEncode(buf, &record.Record{Offset: int64(i * 2), Timestamp: 5, Value: []byte("r")})
	}
	if _, err := l.AppendRaw(buf); err != nil {
		t.Fatal(err)
	}
	if l.EndOffset() != 9 {
		t.Fatalf("LEO %d", l.EndOffset())
	}
	bad := record.Encode(&record.Record{Offset: 20, Value: []byte("r")})
	bad[len(bad)-1] ^= 1
	if _, err := l.AppendRaw(bad); !errors.Is(err, record.ErrChecksum) {
		t.Fatalf("corrupt replication payload accepted: %v", err)
	}
	stale := record.Encode(&record.Record{Offset: 3, Value: []byte("r")})
	if _, err := l.AppendRaw(stale); !errors.Is(err, ErrInvalidOffsets) {
		t.Fatalf("stale offsets accepted: %v", err)
	}
}

func TestOffsetForTimestamp(t *testing.T) {
	clk := newClock()
	l := mustOpen(t, t.TempDir(), smallCfg(clk))
	defer l.Close()
	appendN(t, l, 50, nil)
	clk.Advance(time.Minute)
	mid := clk.Now().UnixMilli()
	appendN(t, l, 50, nil)
	off, err := l.OffsetForTimestamp(mid)
	if err != nil || off != 50 {
		t.Fatalf("off=%d err=%v", off, err)
	}
	off, _ = l.OffsetForTimestamp(mid + 10_000_000)
	if off != 100 {
		t.Fatalf("future timestamp should map to LEO, got %d", off)
	}
}

func TestTimestampsNeverGoBackwards(t *testing.T) {
	clk := newClock()
	l := mustOpen(t, t.TempDir(), smallCfg(clk))
	defer l.Close()
	appendN(t, l, 5, nil)
	clk.Advance(-time.Hour) // wall clock steps back
	appendN(t, l, 5, nil)
	recs, _ := l.Read(0, 10, 1<<20, 0)
	for i := 1; i < len(recs); i++ {
		if recs[i].Timestamp < recs[i-1].Timestamp {
			t.Fatal("timestamp went backwards")
		}
	}
}

// Concurrent appenders and readers. Run with -race.
func TestConcurrentAppendAndRead(t *testing.T) {
	clk := newClock()
	cfg := smallCfg(clk)
	cfg.SegmentBytes = 4096
	l := mustOpen(t, t.TempDir(), cfg)
	defer l.Close()
	const writers, per = 4, 250
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if _, err := l.Append([]*record.Record{{Value: []byte("concurrent")}}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	stop := make(chan struct{})
	var rwg sync.WaitGroup
	for r := 0; r < 3; r++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			next := int64(0)
			for {
				select {
				case <-stop:
					return
				default:
				}
				recs, err := l.Read(next, l.EndOffset(), 4096, 50)
				if err != nil {
					t.Error(err)
					return
				}
				for _, r := range recs {
					if r.Offset != next {
						t.Errorf("reader saw %d want %d", r.Offset, next)
						return
					}
					next++
				}
			}
		}()
	}
	// Retention runs concurrently with appends and reads.
	rwg.Add(1)
	go func() {
		defer rwg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				l.ApplyRetention(0, 1<<40, clk.Now(), l.EndOffset())
				time.Sleep(time.Millisecond)
			}
		}
	}()
	wg.Wait()
	close(stop)
	rwg.Wait()
	if l.EndOffset() != writers*per {
		t.Fatalf("LEO %d", l.EndOffset())
	}
}

func TestSyncEveryAppendAndHooks(t *testing.T) {
	boom := errors.New("disk on fire")
	fail := false
	cfg := Config{SyncEveryAppend: true, Hooks: Hooks{BeforeWrite: func() error {
		if fail {
			return boom
		}
		return nil
	}}}
	l := mustOpen(t, filepath.Join(t.TempDir(), "p"), cfg)
	defer l.Close()
	if _, err := l.Append([]*record.Record{{Value: []byte("a")}}); err != nil {
		t.Fatal(err)
	}
	fail = true
	if _, err := l.Append([]*record.Record{{Value: []byte("b")}}); !errors.Is(err, boom) {
		t.Fatalf("err=%v", err)
	}
	if l.EndOffset() != 1 {
		t.Fatal("failed write advanced LEO")
	}
}

func BenchmarkAppend1KB(b *testing.B) {
	l, _, _ := Open(b.TempDir(), Config{})
	defer l.Close()
	v := make([]byte, 1024)
	b.SetBytes(1024)
	for i := 0; i < b.N; i++ {
		l.Append([]*record.Record{{Value: v}})
	}
}

func BenchmarkAppendBatch100x1KB(b *testing.B) {
	l, _, _ := Open(b.TempDir(), Config{})
	defer l.Close()
	batch := make([]*record.Record, 100)
	for i := range batch {
		batch[i] = &record.Record{Value: make([]byte, 1024)}
	}
	b.SetBytes(100 * 1024)
	for i := 0; i < b.N; i++ {
		l.Append(batch)
	}
}

func BenchmarkAppendSync1KB(b *testing.B) {
	l, _, _ := Open(b.TempDir(), Config{SyncEveryAppend: true})
	defer l.Close()
	v := make([]byte, 1024)
	b.SetBytes(1024)
	for i := 0; i < b.N; i++ {
		l.Append([]*record.Record{{Value: v}})
	}
}

func BenchmarkRead(b *testing.B) {
	l, _, _ := Open(b.TempDir(), Config{})
	defer l.Close()
	v := make([]byte, 1024)
	for i := 0; i < 10000; i++ {
		l.Append([]*record.Record{{Value: v}})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := int64(i*37) % 9900
		if _, err := l.ReadFrames(off, 10000, 64<<10, 64); err != nil {
			b.Fatal(err)
		}
	}
}
