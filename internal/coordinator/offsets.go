package coordinator

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/protocol"
)

// OffsetStore persists committed consumer offsets as an append-only log of
// JSON lines, replayed on startup (last write wins), and compacted when the
// file grows well beyond the number of live entries.
//
// This mirrors how the event log itself works: consumer progress is just
// another stream of events ("group G is at offset N"), and the current
// state is a materialized view over it. Kafka stores this stream in an
// internal compacted topic; here it is a local file on the coordinator.
type OffsetStore struct {
	mu    sync.Mutex
	path  string
	f     *os.File
	data  map[string]map[tp]protocol.OffsetCommit
	lines int
	sync  bool
}

type offsetLine struct {
	G  string `json:"g"`
	T  string `json:"t"`
	P  int32  `json:"p"`
	O  int64  `json:"o"`
	M  string `json:"m,omitempty"`
	TS int64  `json:"ts"`
	D  bool   `json:"d,omitempty"` // group deleted
}

// OpenOffsetStore opens (or creates) the store in dir. A torn final line
// (crash mid-write) is detected and truncated.
func OpenOffsetStore(dir string, syncWrites bool) (*OffsetStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &OffsetStore{path: filepath.Join(dir, "consumer-offsets.log"), data: map[string]map[tp]protocol.OffsetCommit{}, sync: syncWrites}
	b, err := os.ReadFile(s.path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	valid := 0
	for len(b) > 0 {
		i := bytes.IndexByte(b, '\n')
		if i < 0 {
			break // torn final line
		}
		var l offsetLine
		if err := json.Unmarshal(b[:i], &l); err != nil {
			break
		}
		s.apply(l)
		s.lines++
		valid += i + 1
		b = b[i+1:]
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(int64(valid)); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(int64(valid), 0); err != nil {
		f.Close()
		return nil, err
	}
	s.f = f
	if s.lines > 4*s.entries()+1000 {
		if err := s.compactLocked(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *OffsetStore) apply(l offsetLine) {
	if l.D {
		delete(s.data, l.G)
		return
	}
	g := s.data[l.G]
	if g == nil {
		g = map[tp]protocol.OffsetCommit{}
		s.data[l.G] = g
	}
	g[tp{Topic: l.T, Partition: l.P}] = protocol.OffsetCommit{Topic: l.T, Partition: l.P, Offset: l.O, Metadata: l.M, CommitMs: l.TS}
}

func (s *OffsetStore) entries() int {
	n := 0
	for _, g := range s.data {
		n += len(g)
	}
	return n
}

// Commit durably records offsets for group. One write and one fsync per
// call, however many partitions it covers.
func (s *OffsetStore) Commit(group string, offs []protocol.OffsetCommit) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixMilli()
	var buf bytes.Buffer
	lines := make([]offsetLine, len(offs))
	for i, o := range offs {
		lines[i] = offsetLine{G: group, T: o.Topic, P: o.Partition, O: o.Offset, M: o.Metadata, TS: now}
		b, _ := json.Marshal(lines[i])
		buf.Write(b)
		buf.WriteByte('\n')
	}
	if _, err := s.f.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("offset store write: %w", err)
	}
	if s.sync {
		if err := s.f.Sync(); err != nil {
			return fmt.Errorf("offset store fsync: %w", err)
		}
	}
	for _, l := range lines {
		s.apply(l)
	}
	s.lines += len(lines)
	if s.lines > 4*s.entries()+1000 {
		return s.compactLocked()
	}
	return nil
}

// DeleteGroup forgets all offsets of a group.
func (s *OffsetStore) DeleteGroup(group string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, _ := json.Marshal(offsetLine{G: group, D: true})
	if _, err := s.f.Write(append(b, '\n')); err != nil {
		return err
	}
	s.apply(offsetLine{G: group, D: true})
	s.lines++
	return nil
}

// Get returns a group's committed offsets.
func (s *OffsetStore) Get(group string) []protocol.OffsetCommit {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]protocol.OffsetCommit, 0, len(s.data[group]))
	for _, o := range s.data[group] {
		out = append(out, o)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out
}

// Groups lists groups that have committed offsets.
func (s *OffsetStore) Groups() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.data))
	for g := range s.data {
		out = append(out, g)
	}
	sort.Strings(out)
	return out
}

// compactLocked rewrites the file with one line per live entry.
func (s *OffsetStore) compactLocked() error {
	tmp := s.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	n := 0
	for g, m := range s.data {
		for k, o := range m {
			b, _ := json.Marshal(offsetLine{G: g, T: k.Topic, P: k.Partition, O: o.Offset, M: o.Metadata, TS: o.CommitMs})
			w.Write(b)
			w.WriteByte('\n')
			n++
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	s.f.Close()
	s.f, err = os.OpenFile(s.path, os.O_RDWR|os.O_APPEND, 0o644)
	s.lines = n
	return err
}

// Close closes the file.
func (s *OffsetStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}
