package segment

import (
	"os"
	"testing"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/record"
)

func write(t *testing.T, s *Segment, from, n int) {
	var buf []byte
	var metas []FrameMeta
	for i := from; i < from+n; i++ {
		before := len(buf)
		buf = record.AppendEncode(buf, &record.Record{Offset: int64(100 + i), Timestamp: int64(i), Value: make([]byte, 100)})
		metas = append(metas, FrameMeta{Offset: int64(100 + i), Timestamp: int64(i), Size: len(buf) - before})
	}
	if err := s.Append(buf, metas); err != nil {
		t.Fatal(err)
	}
}

func TestPersistedIndexIsReusedOnTailRecovery(t *testing.T) {
	dir := t.TempDir()
	s, err := Create(dir, 100, Config{IndexIntervalBytes: 512}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	write(t, s, 0, 200)
	entries := s.IndexEntries()
	if err := s.Seal(); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s2, res, err := Open(dir, 100, Config{IndexIntervalBytes: 512}, RecoverTail)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if res.IndexRebuilt || s2.IndexEntries() != entries || s2.NextOffset() != 300 {
		t.Fatalf("res=%+v entries=%d/%d next=%d", res, s2.IndexEntries(), entries, s2.NextOffset())
	}
	if s2.FirstTimestamp() != 0 || s2.LastTimestamp() != 199 {
		t.Fatalf("timestamps %d %d", s2.FirstTimestamp(), s2.LastTimestamp())
	}
	r, err := s2.ReadFrames(250, 300, 1<<20, 1, nil)
	if err != nil || r.Count != 1 || record.FrameOffset(r.Frames) != 250 {
		t.Fatalf("indexed read: %+v %v", r, err)
	}
}

func TestCorruptIndexIsRebuilt(t *testing.T) {
	dir := t.TempDir()
	s, _ := Create(dir, 0, Config{IndexIntervalBytes: 512}, time.Now())
	write(t, s, 0, 50)
	s.Close()
	os.WriteFile(IndexPath(dir, 0), []byte{1, 2, 3}, 0o644) // not a multiple of 8 bytes
	s2, res, err := Open(dir, 0, Config{IndexIntervalBytes: 512}, RecoverTail)
	if err != nil || !res.IndexRebuilt {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	s2.Close()
}
