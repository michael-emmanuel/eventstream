// Package partition implements one replica of a topic partition: the local
// log plus the replication state machine around it (leader/follower role,
// leader epoch, in-sync replica set, high-water mark and idempotent-producer
// state).
//
// Three offsets matter and must not be confused:
//
//	log start offset   first offset still on disk (advanced by retention)
//	high-water mark    first offset NOT known to be replicated to every
//	                   in-sync replica; consumers only see offsets below it
//	log end offset     offset the next append will get (LEO)
//
//	  logStart              HWM                 LEO
//	     |                   |                   |
//	     v                   v                   v
//	     [ committed, visible ][ appended, not yet ]
//	                            [ replicated to ISR ]
//
// Consumer offsets are a separate concept owned by consumer groups.
//
// Locking: appendMu serializes everything that changes the log contents
// (produce, follower append, truncation, role change) and guards the
// producer table. mu guards replication state and is never held across
// disk I/O or waiting. Lock order: appendMu -> mu.
package partition

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	plog "github.com/michael-emmanuel/eventstream/internal/log"
	"github.com/michael-emmanuel/eventstream/internal/record"
)

// Role of this replica.
type Role int

const (
	RoleNone Role = iota
	RoleLeader
	RoleFollower
)

func (r Role) String() string {
	switch r {
	case RoleLeader:
		return "leader"
	case RoleFollower:
		return "follower"
	}
	return "none"
}

// Acks is the producer acknowledgement mode.
type Acks int

const (
	// AckLeader acknowledges once the leader appended (not fsynced) the
	// batch. Lowest latency; acknowledged data is lost if the leader dies
	// before a follower copies it.
	AckLeader Acks = 1
	// AckAll acknowledges once every in-sync replica has the batch (the
	// high-water mark passed it) and requires at least MinISR in-sync
	// replicas. Survives the loss of any ISR-1 replicas.
	AckAll Acks = -1
	// AckQuorum acknowledges once a majority of all assigned replicas has
	// the batch. See docs/consistency.md: because leader election here is
	// ISR-based rather than quorum-based, this mode is weaker than it
	// sounds and exists to demonstrate why ack rules and election rules
	// must be designed together.
	AckQuorum Acks = 2
)

func (a Acks) String() string {
	switch a {
	case AckLeader:
		return "leader"
	case AckAll:
		return "all"
	case AckQuorum:
		return "quorum"
	}
	return strconv.Itoa(int(a))
}

// ParseAcks parses "leader"/"1", "all"/"-1" or "quorum".
func ParseAcks(s string) (Acks, error) {
	switch strings.ToLower(s) {
	case "", "all", "-1":
		return AckAll, nil
	case "leader", "1":
		return AckLeader, nil
	case "quorum":
		return AckQuorum, nil
	}
	return 0, fmt.Errorf("unknown acks %q", s)
}

// Errors. Callers map these to wire error codes.
var (
	ErrNotLeader          = errors.New("not the leader for this partition")
	ErrLeaseExpired       = errors.New("leader lease expired: cannot reach controller")
	ErrNotFollower        = errors.New("not a follower for this partition")
	ErrFencedLeaderEpoch  = errors.New("request carries an older leader epoch")
	ErrUnknownLeaderEpoch = errors.New("request carries a newer leader epoch than this replica knows")
	ErrUnknownReplica     = errors.New("broker is not a replica of this partition")
	ErrNotEnoughReplicas  = errors.New("in-sync replicas below min.insync.replicas")
	ErrReplicationTimeout = errors.New("timed out waiting for replication; the batch may or may not be committed")
	ErrOutOfOrderSequence = errors.New("producer sequence out of order")
	ErrInvalidRecord      = errors.New("invalid record")
	ErrStorage            = errors.New("storage failure: partition offline on this broker")
	ErrOffsetOutOfRange   = plog.ErrOffsetOutOfRange
)

// TopicConfig is the subset of topic configuration a replica needs.
type TopicConfig struct {
	MinISR             int
	MaxRecordSize      int
	ReplicaLagMaxTime  time.Duration
	ProducerStateTTL   time.Duration
	CleanupDelete      bool
	CleanupCompact     bool
	Retention          time.Duration
	RetentionBytes     int64
	TombstoneRetention time.Duration
}

// DefaultTopicConfig returns sane defaults.
func DefaultTopicConfig() TopicConfig {
	return TopicConfig{
		MinISR:             1,
		MaxRecordSize:      record.DefaultMaxRecordSize,
		ReplicaLagMaxTime:  10 * time.Second,
		ProducerStateTTL:   15 * time.Minute,
		CleanupDelete:      true,
		Retention:          7 * 24 * time.Hour,
		TombstoneRetention: 24 * time.Hour,
	}
}

// Options configure Open.
type Options struct {
	Topic     string
	Partition int32
	BrokerID  int32
	Dir       string
	Log       plog.Config
	Config    TopicConfig
	// LeaseValid reports whether this broker currently holds a valid lease
	// from the controller. A leader without a lease refuses writes, which
	// bounds how long an isolated stale leader can accept data.
	LeaseValid func() bool
	Now        func() time.Time
}

type followerState struct {
	leo            int64 // -1 until the first fetch after becoming leader
	lastFetch      time.Time
	lastCaughtUp   time.Time
	leoAtLastFetch int64
}

// Partition is one local replica.
type Partition struct {
	topic string
	id    int32
	self  int32
	dir   string
	log   *plog.Log
	cfg   atomic.Pointer[TopicConfig]
	lease func() bool
	now   func() time.Time

	appendMu  sync.Mutex
	producers *producerTable

	mu          sync.Mutex
	role        Role
	leaderEpoch int32
	leaderID    int32
	replicas    []int32
	isr         []int32
	expanding   map[int32]bool
	followers   map[int32]*followerState
	epochs      epochCache
	hwm         int64
	notify      chan struct{}
	failed      error
}

const hwmFile = "high-watermark"

// Open opens the replica's log, runs crash recovery, and rebuilds
// producer and leader-epoch state from the records on disk.
func Open(o Options) (*Partition, plog.RecoveryReport, error) {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Log.Now == nil {
		o.Log.Now = o.Now
	}
	if o.Config.MaxRecordSize > 0 {
		o.Log.MaxRecordSize = o.Config.MaxRecordSize
	}
	l, rep, err := plog.Open(o.Dir, o.Log)
	if err != nil {
		return nil, rep, err
	}
	p := &Partition{
		topic:       o.Topic,
		id:          o.Partition,
		self:        o.BrokerID,
		dir:         o.Dir,
		log:         l,
		lease:       o.LeaseValid,
		now:         o.Now,
		producers:   newProducerTable(),
		leaderEpoch: -1,
		leaderID:    -1,
		notify:      make(chan struct{}),
	}
	cfg := o.Config
	p.cfg.Store(&cfg)
	if err := p.rebuildState(); err != nil {
		l.Close()
		return nil, rep, err
	}
	p.hwm = p.readCheckpoint()
	return p, rep, nil
}

// rebuildState scans the log to reconstruct producer sequences and the
// leader-epoch history. Cost is proportional to log size; production systems
// avoid this with periodic snapshots (see docs/tradeoffs.md).
func (p *Partition) rebuildState() error {
	pt := newProducerTable()
	var ec epochCache
	err := p.log.Scan(func(m record.Meta) error {
		if m.ProducerID > 0 && m.Sequence >= 0 {
			pt.record(m.ProducerID, m.Sequence, 1, m.Offset, time.UnixMilli(m.Timestamp))
		}
		ec.assign(m.LeaderEpoch, m.Offset)
		return nil
	})
	if err != nil {
		return fmt.Errorf("rebuild partition state: %w", err)
	}
	p.producers = pt
	p.mu.Lock()
	p.epochs = ec
	p.mu.Unlock()
	return nil
}

func (p *Partition) readCheckpoint() int64 {
	start, end := p.log.StartOffset(), p.log.EndOffset()
	b, err := os.ReadFile(filepath.Join(p.dir, hwmFile))
	if err != nil {
		return start
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return start
	}
	if v > end {
		v = end
	}
	if v < start {
		v = start
	}
	return v
}

// SaveCheckpoint persists the high-water mark so a restarted broker does not
// have to assume nothing is committed.
func (p *Partition) SaveCheckpoint() error {
	hwm := p.HighWatermark()
	path := filepath.Join(p.dir, hwmFile)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatInt(hwm, 10)), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// --- accessors -------------------------------------------------------------

func (p *Partition) Topic() string       { return p.topic }
func (p *Partition) ID() int32           { return p.id }
func (p *Partition) Dir() string         { return p.dir }
func (p *Partition) Log() *plog.Log      { return p.log }
func (p *Partition) Config() TopicConfig { return *p.cfg.Load() }

// SetConfig updates topic configuration (e.g. after an ALTER).
func (p *Partition) SetConfig(c TopicConfig) { p.cfg.Store(&c) }

func (p *Partition) HighWatermark() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hwm
}
func (p *Partition) LogEndOffset() int64   { return p.log.EndOffset() }
func (p *Partition) LogStartOffset() int64 { return p.log.StartOffset() }

// Role returns the role, leader epoch and leader ID.
func (p *Partition) Role() (Role, int32, int32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.role, p.leaderEpoch, p.leaderID
}

// Failed returns the storage error that took this replica offline, if any.
func (p *Partition) Failed() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.failed
}

// WaitCh returns a channel closed on the next state change (append, HWM
// advance, role change, follower progress). It is the building block for
// long polling: check a condition, and if false, wait on the channel.
func (p *Partition) WaitCh() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.notify
}

func (p *Partition) signalLocked() {
	close(p.notify)
	p.notify = make(chan struct{})
}

func (p *Partition) markFailed(err error) {
	p.mu.Lock()
	if p.failed == nil {
		p.failed = err
	}
	p.signalLocked()
	p.mu.Unlock()
}

// --- produce path (leader) -------------------------------------------------

// ProduceRequest is one batch for this partition.
type ProduceRequest struct {
	ProducerID   int64 // 0 = not idempotent
	BaseSequence int64
	Records      []*record.Record
	Acks         Acks
}

// ProduceResult describes where a batch landed.
type ProduceResult struct {
	BaseOffset    int64
	LastOffset    int64
	Duplicate     bool
	LogAppendTime int64
}

// Produce appends a batch as leader and waits for the requested
// acknowledgement. If waiting times out, the result still carries the
// offsets: the batch is in the leader's log and may yet become committed,
// which is exactly why producers retry with the same sequence numbers.
func (p *Partition) Produce(ctx context.Context, req ProduceRequest) (ProduceResult, error) {
	if len(req.Records) == 0 {
		return ProduceResult{}, fmt.Errorf("%w: empty batch", ErrInvalidRecord)
	}
	cfg := p.cfg.Load()
	for _, r := range req.Records {
		if cfg.CleanupCompact && r.Key == nil {
			return ProduceResult{}, fmt.Errorf("%w: compacted topics require a key", ErrInvalidRecord)
		}
		if err := r.Validate(cfg.MaxRecordSize); err != nil {
			return ProduceResult{}, fmt.Errorf("%w: %v", ErrInvalidRecord, err)
		}
	}
	if req.ProducerID > 0 && req.BaseSequence < 0 {
		return ProduceResult{}, fmt.Errorf("%w: idempotent producer without sequence", ErrInvalidRecord)
	}

	p.appendMu.Lock()
	res, epoch, err := p.appendAsLeaderLocked(req, cfg)
	p.appendMu.Unlock()
	if err != nil || res.Duplicate && res.BaseOffset < 0 {
		return res, err
	}
	if err := p.waitForAcks(ctx, res.LastOffset, req.Acks, epoch); err != nil {
		return res, err
	}
	return res, nil
}

func (p *Partition) appendAsLeaderLocked(req ProduceRequest, cfg *TopicConfig) (ProduceResult, int32, error) {
	p.mu.Lock()
	role, epoch, failed, isrLen := p.role, p.leaderEpoch, p.failed, len(p.isr)
	p.mu.Unlock()
	switch {
	case failed != nil:
		return ProduceResult{}, 0, fmt.Errorf("%w: %v", ErrStorage, failed)
	case role != RoleLeader:
		return ProduceResult{}, 0, ErrNotLeader
	case p.lease != nil && !p.lease():
		return ProduceResult{}, 0, ErrLeaseExpired
	case req.Acks == AckAll && isrLen < cfg.MinISR:
		return ProduceResult{}, 0, fmt.Errorf("%w: isr=%d min=%d", ErrNotEnoughReplicas, isrLen, cfg.MinISR)
	}
	n := len(req.Records)
	if req.ProducerID > 0 {
		switch res, off := p.producers.check(req.ProducerID, req.BaseSequence, n); res {
		case seqDuplicate:
			return ProduceResult{BaseOffset: off, LastOffset: off + int64(n) - 1, Duplicate: true}, epoch, nil
		case seqDuplicateUnknownOffsets:
			return ProduceResult{BaseOffset: -1, LastOffset: -1, Duplicate: true}, epoch, nil
		case seqOutOfOrder:
			return ProduceResult{}, 0, fmt.Errorf("%w: producer %d batch starting at %d",
				ErrOutOfOrderSequence, req.ProducerID, req.BaseSequence)
		}
	}
	for i, r := range req.Records {
		r.LeaderEpoch = epoch
		r.ProducerID = req.ProducerID
		r.Sequence = -1
		if req.ProducerID > 0 {
			r.Sequence = req.BaseSequence + int64(i)
		}
	}
	base, err := p.log.Append(req.Records)
	if err != nil {
		p.markFailed(err)
		return ProduceResult{}, 0, fmt.Errorf("%w: %v", ErrStorage, err)
	}
	if req.ProducerID > 0 {
		p.producers.record(req.ProducerID, req.BaseSequence, n, base, p.now())
	}
	p.mu.Lock()
	p.maybeAdvanceHWMLocked()
	p.signalLocked()
	p.mu.Unlock()
	return ProduceResult{
		BaseOffset:    base,
		LastOffset:    base + int64(n) - 1,
		LogAppendTime: req.Records[0].Timestamp,
	}, epoch, nil
}

func (p *Partition) waitForAcks(ctx context.Context, last int64, acks Acks, epoch int32) error {
	if acks == AckLeader {
		return nil
	}
	for {
		p.mu.Lock()
		if p.role != RoleLeader || p.leaderEpoch != epoch {
			p.mu.Unlock()
			return fmt.Errorf("%w: leadership changed while waiting for replication", ErrNotLeader)
		}
		var done bool
		if acks == AckQuorum {
			done = p.quorumLocked(last)
		} else {
			done = p.hwm > last
		}
		ch := p.notify
		p.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ErrReplicationTimeout
		}
	}
}

func (p *Partition) quorumLocked(last int64) bool {
	have := 1 // the leader
	for _, fs := range p.followers {
		if fs.leo > last {
			have++
		}
	}
	return have > len(p.replicas)/2
}

func (p *Partition) effectiveISRLocked() []int32 {
	out := append([]int32(nil), p.isr...)
	for id := range p.expanding {
		if !contains(out, id) {
			out = append(out, id)
		}
	}
	return out
}

// maybeAdvanceHWMLocked moves the HWM to the minimum LEO across the leader
// and every member of the (maximal) ISR. It never moves backwards.
func (p *Partition) maybeAdvanceHWMLocked() {
	if p.role != RoleLeader {
		return
	}
	min := p.log.EndOffset()
	for _, id := range p.effectiveISRLocked() {
		if id == p.self {
			continue
		}
		fs := p.followers[id]
		if fs == nil || fs.leo < 0 {
			return
		}
		if fs.leo < min {
			min = fs.leo
		}
	}
	if min > p.hwm {
		p.hwm = min
	}
}

// --- consumer reads (leader) -----------------------------------------------

// ReadResult is what a consumer fetch returns.
type ReadResult struct {
	Records        []record.Record
	HighWatermark  int64
	LogStartOffset int64
	LogEndOffset   int64
}

// Read returns committed records starting at offset. Consumers never see
// records at or above the high-water mark: those could still be truncated
// by a leader change, and showing them would let a consumer act on data
// that later "un-happens".
func (p *Partition) Read(offset int64, maxBytes, maxRecords int) (ReadResult, error) {
	p.mu.Lock()
	role, hwm, failed := p.role, p.hwm, p.failed
	p.mu.Unlock()
	if failed != nil {
		return ReadResult{}, fmt.Errorf("%w: %v", ErrStorage, failed)
	}
	if role != RoleLeader {
		return ReadResult{}, ErrNotLeader
	}
	res := ReadResult{HighWatermark: hwm, LogStartOffset: p.log.StartOffset(), LogEndOffset: p.log.EndOffset()}
	if offset < res.LogStartOffset || offset > hwm {
		return res, fmt.Errorf("%w: offset %d, valid range [%d, %d]", ErrOffsetOutOfRange, offset, res.LogStartOffset, hwm)
	}
	if offset == hwm {
		return res, nil
	}
	recs, err := p.log.Read(offset, hwm, maxBytes, maxRecords)
	if err != nil {
		return res, err
	}
	res.Records = recs
	return res, nil
}

// OffsetForTimestamp maps a timestamp to the first offset at or after it.
func (p *Partition) OffsetForTimestamp(ts int64) (int64, error) {
	off, err := p.log.OffsetForTimestamp(ts)
	if err != nil {
		return 0, err
	}
	if hwm := p.HighWatermark(); off > hwm {
		off = hwm
	}
	return off, nil
}

// --- replication: leader side ----------------------------------------------

// ReplicaFetchRequest is sent by a follower.
type ReplicaFetchRequest struct {
	ReplicaID        int32
	FetchOffset      int64 // the follower's LEO
	LastFetchedEpoch int32 // epoch of the follower's last record, -1 if none
	LeaderEpoch      int32 // the epoch the follower believes is current
	MaxBytes         int
}

// ReplicaFetchResponse is returned to a follower.
type ReplicaFetchResponse struct {
	Frames         []byte
	Count          int
	HighWatermark  int64
	LogStartOffset int64
	LogEndOffset   int64
	LeaderEpoch    int32
	// Diverging is set when the follower's log disagrees with the leader's
	// after DivergingEndOffset; the follower must truncate and retry.
	Diverging          bool
	DivergingEpoch     int32
	DivergingEndOffset int64
}

// HandleReplicaFetch serves a follower fetch. The fetch offset doubles as
// an acknowledgement: a follower asking for offset N has everything below N,
// which is how the leader learns follower progress and advances the HWM.
func (p *Partition) HandleReplicaFetch(req ReplicaFetchRequest) (ReplicaFetchResponse, error) {
	p.mu.Lock()
	if p.role != RoleLeader {
		p.mu.Unlock()
		return ReplicaFetchResponse{}, ErrNotLeader
	}
	if req.LeaderEpoch < p.leaderEpoch {
		p.mu.Unlock()
		return ReplicaFetchResponse{}, ErrFencedLeaderEpoch
	}
	if req.LeaderEpoch > p.leaderEpoch {
		p.mu.Unlock()
		return ReplicaFetchResponse{}, ErrUnknownLeaderEpoch
	}
	fs := p.followers[req.ReplicaID]
	if fs == nil {
		p.mu.Unlock()
		return ReplicaFetchResponse{}, ErrUnknownReplica
	}
	leo := p.log.EndOffset()
	resp := ReplicaFetchResponse{LeaderEpoch: p.leaderEpoch, LogEndOffset: leo, LogStartOffset: p.log.StartOffset()}
	if req.LastFetchedEpoch >= 0 && req.FetchOffset > resp.LogStartOffset {
		e, end := p.epochs.endOffsetFor(req.LastFetchedEpoch, leo)
		if e != req.LastFetchedEpoch || end < req.FetchOffset {
			resp.Diverging, resp.DivergingEpoch, resp.DivergingEndOffset = true, e, end
			resp.HighWatermark = p.hwm
			p.mu.Unlock()
			return resp, nil
		}
	}
	if req.FetchOffset < resp.LogStartOffset || req.FetchOffset > leo {
		resp.HighWatermark = p.hwm
		p.mu.Unlock()
		return resp, fmt.Errorf("%w: follower offset %d, leader range [%d, %d]",
			ErrOffsetOutOfRange, req.FetchOffset, resp.LogStartOffset, leo)
	}

	now := p.now()
	if req.FetchOffset >= leo {
		fs.lastCaughtUp = now
	} else if req.FetchOffset >= fs.leoAtLastFetch {
		fs.lastCaughtUp = fs.lastFetch
	}
	fs.leoAtLastFetch = leo
	fs.lastFetch = now
	changed := fs.leo != req.FetchOffset
	fs.leo = req.FetchOffset
	if !contains(p.isr, req.ReplicaID) && !p.expanding[req.ReplicaID] &&
		fs.leo >= p.hwm && fs.leo >= p.epochStartLocked() {
		// Maximal ISR: count the follower immediately for the HWM (more
		// conservative), while the change is confirmed with the controller.
		p.expanding[req.ReplicaID] = true
		changed = true
	}
	old := p.hwm
	p.maybeAdvanceHWMLocked()
	if changed || p.hwm != old {
		p.signalLocked()
	}
	resp.HighWatermark = p.hwm
	p.mu.Unlock()

	r, err := p.log.ReadFrames(req.FetchOffset, leo, req.MaxBytes, 0)
	if err != nil {
		return resp, err
	}
	resp.Frames, resp.Count = r.Frames, r.Count
	return resp, nil
}

func (p *Partition) epochStartLocked() int64 {
	for _, e := range p.epochs.entries {
		if e.Epoch == p.leaderEpoch {
			return e.StartOffset
		}
	}
	return 0
}

// ISRProposal computes the in-sync replica set the leader wants: itself plus
// every ISR member (or catching-up candidate) that was fully caught up
// within ReplicaLagMaxTime. Returns changed=false if it equals the current
// committed ISR.
func (p *Partition) ISRProposal() (epoch int32, isr []int32, changed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.role != RoleLeader {
		return 0, nil, false
	}
	maxLag := p.cfg.Load().ReplicaLagMaxTime
	now := p.now()
	isr = []int32{p.self}
	for _, id := range p.replicas {
		if id == p.self {
			continue
		}
		fs := p.followers[id]
		if fs == nil {
			continue
		}
		member := contains(p.isr, id) || p.expanding[id]
		if member && now.Sub(fs.lastCaughtUp) <= maxLag {
			isr = append(isr, id)
		}
	}
	sortIDs(isr)
	return p.leaderEpoch, isr, !sameSet(isr, p.isr)
}

// CommitISR applies an ISR change that the controller accepted. Shrinks only
// take effect here, after the controller durably recorded them: if the
// leader shrank first and then died, the controller could elect a replica
// the leader had already stopped waiting for, losing committed data.
func (p *Partition) CommitISR(epoch int32, isr []int32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.role != RoleLeader || epoch != p.leaderEpoch {
		return
	}
	p.isr = append([]int32(nil), isr...)
	p.expanding = map[int32]bool{}
	p.maybeAdvanceHWMLocked()
	p.signalLocked()
}

// --- replication: follower side --------------------------------------------

// FollowerPosition is what a follower sends in its next fetch.
type FollowerPosition struct {
	LeaderID         int32
	LeaderEpoch      int32
	FetchOffset      int64
	LastFetchedEpoch int32
}

// FollowerPosition returns the follower's fetch position, or false if this
// replica is not currently a follower.
func (p *Partition) FollowerPosition() (FollowerPosition, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.role != RoleFollower || p.failed != nil {
		return FollowerPosition{}, false
	}
	leo := p.log.EndOffset()
	return FollowerPosition{
		LeaderID:         p.leaderID,
		LeaderEpoch:      p.leaderEpoch,
		FetchOffset:      leo,
		LastFetchedEpoch: p.epochs.epochAt(leo),
	}, true
}

// AppendAsFollower appends frames fetched from the leader, verifying every
// checksum, and adopts the leader's high-water mark (capped at our LEO).
func (p *Partition) AppendAsFollower(epoch int32, frames []byte, leaderHWM int64) (int, error) {
	p.appendMu.Lock()
	defer p.appendMu.Unlock()
	p.mu.Lock()
	if p.role != RoleFollower || epoch != p.leaderEpoch {
		p.mu.Unlock()
		return 0, ErrNotFollower
	}
	if p.failed != nil {
		p.mu.Unlock()
		return 0, ErrStorage
	}
	p.mu.Unlock()

	var metas []record.Meta
	if len(frames) > 0 {
		var err error
		metas, err = p.log.AppendRaw(frames)
		if err != nil {
			if errors.Is(err, plog.ErrInvalidOffsets) || errors.Is(err, record.ErrChecksum) ||
				errors.Is(err, record.ErrMalformed) || errors.Is(err, record.ErrTruncated) {
				return 0, err
			}
			p.markFailed(err)
			return 0, fmt.Errorf("%w: %v", ErrStorage, err)
		}
		for _, m := range metas {
			if m.ProducerID > 0 && m.Sequence >= 0 {
				p.producers.record(m.ProducerID, m.Sequence, 1, m.Offset, p.now())
			}
		}
	}
	p.mu.Lock()
	for _, m := range metas {
		p.epochs.assign(m.LeaderEpoch, m.Offset)
	}
	h := leaderHWM
	if leo := p.log.EndOffset(); h > leo {
		h = leo
	}
	if h > p.hwm {
		p.hwm = h
	}
	p.signalLocked()
	p.mu.Unlock()
	return len(metas), nil
}

// TruncateForDivergence truncates the follower's log to the point where it
// agrees with the leader, as reported by the leader's epoch history.
func (p *Partition) TruncateForDivergence(leaderEpochForOffset int32, leaderEndOffset int64) (int64, error) {
	p.appendMu.Lock()
	defer p.appendMu.Unlock()
	p.mu.Lock()
	if p.role != RoleFollower {
		p.mu.Unlock()
		return 0, ErrNotFollower
	}
	leo := p.log.EndOffset()
	_, myEnd := p.epochs.endOffsetFor(leaderEpochForOffset, leo)
	target := leaderEndOffset
	if myEnd < target {
		target = myEnd
	}
	if leo < target {
		target = leo
	}
	p.mu.Unlock()

	if err := p.log.Truncate(target); err != nil {
		p.markFailed(err)
		return 0, err
	}
	p.mu.Lock()
	p.epochs.truncateFrom(target)
	if p.hwm > target {
		p.hwm = target
	}
	p.mu.Unlock()
	if err := p.rebuildProducersLocked(); err != nil {
		return target, err
	}
	return target, nil
}

func (p *Partition) rebuildProducersLocked() error {
	pt := newProducerTable()
	err := p.log.Scan(func(m record.Meta) error {
		if m.ProducerID > 0 && m.Sequence >= 0 {
			pt.record(m.ProducerID, m.Sequence, 1, m.Offset, time.UnixMilli(m.Timestamp))
		}
		return nil
	})
	if err != nil {
		return err
	}
	p.producers = pt
	return nil
}

// ResetTo discards the local log and restarts it at offset. Used when the
// follower is behind the leader's log start offset.
func (p *Partition) ResetTo(offset int64) error {
	p.appendMu.Lock()
	defer p.appendMu.Unlock()
	if err := p.log.ResetTo(offset); err != nil {
		p.markFailed(err)
		return err
	}
	p.producers = newProducerTable()
	p.mu.Lock()
	p.epochs.reset()
	p.hwm = offset
	p.signalLocked()
	p.mu.Unlock()
	return nil
}

// --- role transitions ------------------------------------------------------

// BecomeLeader makes this replica the leader for epoch. Stale epochs are
// ignored: metadata can arrive out of order, and acting on an old epoch
// would resurrect a deposed leader. Returns true if the role changed.
func (p *Partition) BecomeLeader(epoch int32, replicas, isr []int32) bool {
	p.appendMu.Lock()
	defer p.appendMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if epoch < p.leaderEpoch {
		return false
	}
	if p.role == RoleLeader && epoch == p.leaderEpoch {
		p.replicas = append([]int32(nil), replicas...)
		p.isr = append([]int32(nil), isr...)
		now := p.now()
		for _, r := range replicas {
			if r != p.self && p.followers[r] == nil {
				p.followers[r] = &followerState{leo: -1, lastFetch: now, lastCaughtUp: now}
			}
		}
		p.maybeAdvanceHWMLocked()
		p.signalLocked()
		return false
	}
	now := p.now()
	p.role = RoleLeader
	p.leaderEpoch = epoch
	p.leaderID = p.self
	p.replicas = append([]int32(nil), replicas...)
	p.isr = append([]int32(nil), isr...)
	p.expanding = map[int32]bool{}
	p.followers = map[int32]*followerState{}
	for _, r := range replicas {
		if r != p.self {
			p.followers[r] = &followerState{leo: -1, lastFetch: now, lastCaughtUp: now}
		}
	}
	p.epochs.assign(epoch, p.log.EndOffset())
	p.maybeAdvanceHWMLocked()
	p.signalLocked()
	return true
}

// BecomeFollower makes this replica follow leaderID in epoch. leaderID < 0
// means the partition is offline (no eligible leader).
func (p *Partition) BecomeFollower(epoch int32, leaderID int32, replicas, isr []int32) bool {
	p.appendMu.Lock()
	defer p.appendMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if epoch < p.leaderEpoch {
		return false
	}
	role := RoleFollower
	if leaderID < 0 {
		role = RoleNone
	}
	p.replicas = append([]int32(nil), replicas...)
	p.isr = append([]int32(nil), isr...)
	if p.role == role && epoch == p.leaderEpoch && leaderID == p.leaderID {
		return false
	}
	p.role = role
	p.leaderEpoch = epoch
	p.leaderID = leaderID
	p.followers = nil
	p.expanding = nil
	p.signalLocked()
	return true
}

// --- maintenance -----------------------------------------------------------

// Flush fsyncs the log. A failed fsync takes the replica offline: after an
// fsync error the kernel may have dropped dirty pages, so the on-disk state
// is unknown and continuing to serve would risk silent data loss.
func (p *Partition) Flush() error {
	if err := p.log.Flush(); err != nil {
		p.markFailed(err)
		return err
	}
	return nil
}

// MaintenanceResult summarizes one maintenance pass.
type MaintenanceResult struct {
	Retention        plog.RetentionResult
	Compaction       plog.CompactionResult
	Rolled           bool
	ExpiredProducers int
}

// RunMaintenance applies retention, compaction, age-based rolling and
// producer-state expiry according to the topic config.
func (p *Partition) RunMaintenance(now time.Time, doCompaction bool) (MaintenanceResult, error) {
	var res MaintenanceResult
	if p.Failed() != nil {
		return res, nil
	}
	cfg := p.cfg.Load()
	rolled, err := p.log.MaybeRollByAge()
	if err != nil {
		return res, err
	}
	res.Rolled = rolled
	if cfg.CleanupDelete && (cfg.Retention > 0 || cfg.RetentionBytes > 0) {
		// Never delete past the committed point this replica knows about.
		upTo := p.HighWatermark()
		res.Retention, err = p.log.ApplyRetention(cfg.Retention, cfg.RetentionBytes, now, upTo)
		if err != nil {
			return res, err
		}
	}
	if cfg.CleanupCompact && doCompaction {
		res.Compaction, err = p.log.Compact(now, cfg.TombstoneRetention)
		if err != nil {
			return res, err
		}
	}
	p.appendMu.Lock()
	res.ExpiredProducers = p.producers.expire(now, cfg.ProducerStateTTL)
	p.appendMu.Unlock()
	return res, nil
}

// Close checkpoints the HWM and closes the log.
func (p *Partition) Close() error {
	p.mu.Lock()
	p.role = RoleNone
	p.signalLocked()
	p.mu.Unlock()
	cerr := p.SaveCheckpoint()
	if err := p.log.Close(); err != nil {
		return err
	}
	return cerr
}

// Delete closes the replica and removes its data.
func (p *Partition) Delete() error {
	p.mu.Lock()
	p.role = RoleNone
	p.signalLocked()
	p.mu.Unlock()
	return p.log.Delete()
}

// --- introspection ---------------------------------------------------------

// FollowerInfo is the leader's view of one follower.
type FollowerInfo struct {
	BrokerID          int32 `json:"broker_id"`
	LogEndOffset      int64 `json:"log_end_offset"`
	LagRecords        int64 `json:"lag_records"`
	LastCaughtUpAgoMs int64 `json:"last_caught_up_ago_ms"`
	LastFetchAgoMs    int64 `json:"last_fetch_ago_ms"`
	InSync            bool  `json:"in_sync"`
}

// State is a snapshot for admin endpoints and metrics.
type State struct {
	Topic           string         `json:"topic"`
	Partition       int32          `json:"partition"`
	Role            string         `json:"role"`
	LeaderID        int32          `json:"leader_id"`
	LeaderEpoch     int32          `json:"leader_epoch"`
	Replicas        []int32        `json:"replicas"`
	ISR             []int32        `json:"isr"`
	LogStartOffset  int64          `json:"log_start_offset"`
	HighWatermark   int64          `json:"high_watermark"`
	LogEndOffset    int64          `json:"log_end_offset"`
	SizeBytes       int64          `json:"size_bytes"`
	Segments        int            `json:"segments"`
	Producers       int            `json:"producers"`
	UnderReplicated bool           `json:"under_replicated"`
	Followers       []FollowerInfo `json:"followers,omitempty"`
	Failed          string         `json:"failed,omitempty"`
}

// State returns a snapshot of the replica.
func (p *Partition) State() State {
	p.appendMu.Lock()
	producers := p.producers.len()
	p.appendMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	leo := p.log.EndOffset()
	s := State{
		Topic: p.topic, Partition: p.id, Role: p.role.String(), LeaderID: p.leaderID,
		LeaderEpoch: p.leaderEpoch, Replicas: p.replicas, ISR: p.isr,
		LogStartOffset: p.log.StartOffset(), HighWatermark: p.hwm, LogEndOffset: leo,
		SizeBytes: p.log.Size(), Segments: p.log.SegmentCount(), Producers: producers,
		UnderReplicated: p.role == RoleLeader && len(p.isr) < len(p.replicas),
	}
	if p.failed != nil {
		s.Failed = p.failed.Error()
	}
	ids := make([]int32, 0, len(p.followers))
	for id := range p.followers {
		ids = append(ids, id)
	}
	sortIDs(ids)
	for _, id := range ids {
		fs := p.followers[id]
		lag := leo - fs.leo
		if fs.leo < 0 {
			lag = leo
		}
		s.Followers = append(s.Followers, FollowerInfo{
			BrokerID: id, LogEndOffset: fs.leo, LagRecords: lag,
			LastCaughtUpAgoMs: now.Sub(fs.lastCaughtUp).Milliseconds(),
			LastFetchAgoMs:    now.Sub(fs.lastFetch).Milliseconds(),
			InSync:            contains(p.isr, id),
		})
	}
	return s
}

// --- helpers ---------------------------------------------------------------

func contains(s []int32, v int32) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func sortIDs(s []int32) { sort.Slice(s, func(i, j int) bool { return s[i] < s[j] }) }

func sameSet(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		if !contains(b, x) {
			return false
		}
	}
	return true
}
