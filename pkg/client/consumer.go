package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/observability"
	"github.com/michael-emmanuel/eventstream/internal/protocol"
)

// Record is a consumed record.
type Record struct {
	Topic       string
	Partition   int32
	Offset      int64
	Timestamp   time.Time
	Key         []byte // nil if the record had no key
	Value       []byte // nil for a tombstone
	Headers     map[string]string
	LeaderEpoch int32
}

func fromWire(cr protocol.ConsumerRecord) Record {
	r := Record{Topic: cr.Topic, Partition: cr.Partition, Offset: cr.Offset,
		Timestamp: time.UnixMilli(cr.Timestamp), Headers: cr.Headers, LeaderEpoch: cr.LeaderEpoch}
	if cr.Key != nil {
		r.Key = []byte(*cr.Key)
	}
	if cr.Value != nil {
		r.Value = []byte(*cr.Value)
	}
	return r
}

// ConsumerConfig configures a group consumer.
type ConsumerConfig struct {
	Group    string
	Topics   []string
	ClientID string
	Assignor string // range (default), roundrobin, sticky

	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	RebalanceTimeout  time.Duration
	// MaxPollInterval: if the application does not call Poll for this
	// long, the consumer leaves the group so its partitions are reassigned
	// instead of being held by a stuck process.
	MaxPollInterval time.Duration

	// AutoOffsetReset applies when the group has no committed offset (or it
	// was deleted by retention): "earliest" or "latest".
	AutoOffsetReset string

	// Flow control: each partition fetcher holds at most one batch of up to
	// FetchMaxEvents/FetchMaxBytes while it waits for Poll, and the shared
	// queue holds at most QueuedBatches. Memory is bounded no matter how
	// slow the application is.
	FetchMaxEvents int
	FetchMaxBytes  int
	FetchWait      time.Duration
	QueuedBatches  int

	// AutoCommit commits the positions of records already returned by Poll
	// every AutoCommitInterval, at the start of the next Poll. Because it
	// only commits records returned by an earlier Poll, it is at-least-once
	// provided the application finishes processing a batch before polling
	// again.
	AutoCommit         bool
	AutoCommitInterval time.Duration

	OnAssign func(parts []protocol.TopicPartition)
	OnRevoke func(parts []protocol.TopicPartition)

	Logger *slog.Logger
	Tracer *observability.Tracer
}

func (c *ConsumerConfig) defaults() {
	d := func(v *time.Duration, def time.Duration) {
		if *v == 0 {
			*v = def
		}
	}
	d(&c.SessionTimeout, 10*time.Second)
	d(&c.HeartbeatInterval, c.SessionTimeout/3)
	d(&c.RebalanceTimeout, 30*time.Second)
	d(&c.MaxPollInterval, 5*time.Minute)
	d(&c.FetchWait, 2*time.Second)
	d(&c.AutoCommitInterval, 5*time.Second)
	if c.AutoOffsetReset == "" {
		c.AutoOffsetReset = "earliest"
	}
	if c.FetchMaxEvents == 0 {
		c.FetchMaxEvents = 500
	}
	if c.FetchMaxBytes == 0 {
		c.FetchMaxBytes = 1 << 20
	}
	if c.QueuedBatches == 0 {
		c.QueuedBatches = 8
	}
	if c.Logger == nil {
		c.Logger = observability.DiscardLogger()
	}
}

type batch struct {
	tp      protocol.TopicPartition
	gen     int64
	records []Record
	hwm     int64
}

// ConsumerStats are cumulative counters and gauges.
type ConsumerStats struct {
	Records    int64
	Rebalances int64
	Commits    int64
	Generation int32
	Assigned   int
	MemberID   string
}

// Consumer is a consumer-group member. Poll, Commit and Close must be called
// from one goroutine (the application's processing loop); heartbeats and
// fetches run in background goroutines owned by the consumer.
type Consumer struct {
	c   *Client
	cfg ConsumerConfig
	log *slog.Logger

	mu         sync.Mutex
	memberID   string
	generation int32
	assigned   []protocol.TopicPartition
	positions  map[protocol.TopicPartition]int64 // next offset to hand to the app
	needRejoin bool

	fetchGen  atomic.Int64
	queue     chan batch
	fetchStop context.CancelFunc
	fetchWG   sync.WaitGroup

	lastPoll   atomic.Int64
	lastCommit time.Time
	hbStop     context.CancelFunc
	hbWG       sync.WaitGroup
	closed     bool

	records    atomic.Int64
	rebalances atomic.Int64
	commits    atomic.Int64
}

// NewConsumer creates a group consumer. It joins the group on first Poll.
func (c *Client) NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if cfg.Group == "" || len(cfg.Topics) == 0 {
		return nil, errors.New("consumer: group and topics are required")
	}
	cfg.defaults()
	cs := &Consumer{c: c, cfg: cfg, log: cfg.Logger.With("group", cfg.Group), needRejoin: true,
		positions: map[protocol.TopicPartition]int64{}, queue: make(chan batch, cfg.QueuedBatches)}
	cs.lastPoll.Store(time.Now().UnixNano())
	ctx, cancel := context.WithCancel(context.Background())
	cs.hbStop = cancel
	cs.hbWG.Add(1)
	go func() {
		defer cs.hbWG.Done()
		cs.heartbeatLoop(ctx)
	}()
	return cs, nil
}

func (cs *Consumer) groupPath(suffix string) string {
	return "/v1/groups/" + url.PathEscape(cs.cfg.Group) + suffix
}

// Stats returns counters.
func (cs *Consumer) Stats() ConsumerStats {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return ConsumerStats{Records: cs.records.Load(), Rebalances: cs.rebalances.Load(), Commits: cs.commits.Load(),
		Generation: cs.generation, Assigned: len(cs.assigned), MemberID: cs.memberID}
}

// Assignment returns the currently assigned partitions.
func (cs *Consumer) Assignment() []protocol.TopicPartition {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return append([]protocol.TopicPartition(nil), cs.assigned...)
}

// Poll returns the next batch of records (possibly empty after timeout).
// It also performs rebalances and auto-commits, so it must be called
// regularly (at least every MaxPollInterval).
func (cs *Consumer) Poll(ctx context.Context, timeout time.Duration) ([]Record, error) {
	cs.lastPoll.Store(time.Now().UnixNano())
	cs.mu.Lock()
	rejoin, closed := cs.needRejoin, cs.closed
	cs.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	if cs.cfg.AutoCommit && time.Since(cs.lastCommit) >= cs.cfg.AutoCommitInterval && !rejoin {
		if err := cs.Commit(ctx); err != nil {
			cs.log.Warn("auto-commit failed", "error", err)
		}
	}
	if rejoin {
		if err := cs.rebalance(ctx); err != nil {
			return nil, err
		}
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	for {
		select {
		case b := <-cs.queue:
			if b.gen != cs.fetchGen.Load() {
				continue // from before the last rebalance
			}
			cs.mu.Lock()
			pos, owned := cs.positions[b.tp]
			if !owned || len(b.records) == 0 || b.records[len(b.records)-1].Offset < pos {
				cs.mu.Unlock()
				continue
			}
			cs.positions[b.tp] = b.records[len(b.records)-1].Offset + 1
			cs.mu.Unlock()
			cs.records.Add(int64(len(b.records)))
			return b.records, nil
		case <-t.C:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// rebalance stops fetching, gives up the old assignment, rejoins the group
// and starts fetching the new assignment from committed offsets.
func (cs *Consumer) rebalance(ctx context.Context) error {
	start := time.Now()
	cs.stopFetchers()
	cs.mu.Lock()
	old := cs.assigned
	member, gen := cs.memberID, cs.generation
	cs.mu.Unlock()
	if len(old) > 0 {
		if cs.cfg.AutoCommit {
			_ = cs.Commit(ctx) // still allowed with the old generation while the group is preparing
		}
		if cs.cfg.OnRevoke != nil {
			cs.cfg.OnRevoke(old)
		}
	}
	var resp protocol.JoinGroupResponse
	jctx, cancel := context.WithTimeout(ctx, cs.cfg.RebalanceTimeout+5*time.Second)
	err := cs.c.controllerDo(jctx, "POST", cs.groupPath("/join"), protocol.JoinGroupRequest{
		MemberID: member, ClientID: cs.cfg.ClientID, Topics: cs.cfg.Topics,
		SessionTimeoutMs: cs.cfg.SessionTimeout.Milliseconds(), RebalanceTimeoutMs: cs.cfg.RebalanceTimeout.Milliseconds(),
		Assignor: cs.cfg.Assignor,
	}, &resp)
	cancel()
	if err != nil {
		if errCode(err) == protocol.CodeUnknownMember {
			cs.mu.Lock()
			cs.memberID = ""
			cs.mu.Unlock()
		}
		cs.mu.Lock()
		cs.assigned = nil
		cs.positions = map[protocol.TopicPartition]int64{}
		cs.mu.Unlock()
		return fmt.Errorf("join group: %w", err)
	}
	positions, err := cs.startPositions(ctx, resp.Assignment)
	if err != nil {
		return err
	}
	cs.mu.Lock()
	cs.memberID, cs.generation = resp.MemberID, resp.Generation
	cs.assigned = resp.Assignment
	cs.positions = positions
	cs.needRejoin = false
	cs.mu.Unlock()
	cs.rebalances.Add(1)
	cs.lastCommit = time.Now()
	cs.log.Info("joined group", "member", resp.MemberID, "generation", resp.Generation,
		"assignment", tpsString(resp.Assignment), "previous_generation", gen, "duration_ms", time.Since(start).Milliseconds())
	cs.startFetchers(positions)
	if cs.cfg.OnAssign != nil {
		cs.cfg.OnAssign(resp.Assignment)
	}
	return nil
}

// startPositions resolves where to start each assigned partition: the
// committed offset, or the reset policy if none (or if it was deleted).
func (cs *Consumer) startPositions(ctx context.Context, parts []protocol.TopicPartition) (map[protocol.TopicPartition]int64, error) {
	var committed protocol.GroupOffsetsResponse
	if err := cs.c.controllerDo(ctx, "GET", cs.groupPath("/offsets"), nil, &committed); err != nil {
		return nil, fmt.Errorf("fetch committed offsets: %w", err)
	}
	have := map[protocol.TopicPartition]int64{}
	for _, o := range committed.Offsets {
		have[protocol.TopicPartition{Topic: o.Topic, Partition: o.Partition}] = o.Offset
	}
	out := map[protocol.TopicPartition]int64{}
	for _, tp := range parts {
		if off, ok := have[tp]; ok {
			out[tp] = off
			continue
		}
		off, err := cs.resetOffset(ctx, tp)
		if err != nil {
			return nil, err
		}
		out[tp] = off
	}
	return out, nil
}

func (cs *Consumer) resetOffset(ctx context.Context, tp protocol.TopicPartition) (int64, error) {
	o, err := cs.c.Offsets(ctx, tp.Topic, tp.Partition, -1)
	if err != nil {
		return 0, fmt.Errorf("offsets for %s: %w", tp, err)
	}
	if cs.cfg.AutoOffsetReset == "latest" {
		return o.HighWatermark, nil
	}
	return o.LogStartOffset, nil
}

func (cs *Consumer) startFetchers(positions map[protocol.TopicPartition]int64) {
	gen := cs.fetchGen.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	cs.fetchStop = cancel
	for tp, off := range positions {
		cs.fetchWG.Add(1)
		go func(tp protocol.TopicPartition, off int64) {
			defer cs.fetchWG.Done()
			cs.fetchLoop(ctx, gen, tp, off)
		}(tp, off)
	}
}

func (cs *Consumer) stopFetchers() {
	if cs.fetchStop != nil {
		cs.fetchStop()
		cs.fetchStop = nil
	}
	// Drain while waiting so fetchers blocked on a full queue can exit.
	done := make(chan struct{})
	go func() { cs.fetchWG.Wait(); close(done) }()
	for {
		select {
		case <-cs.queue:
		case <-done:
			for {
				select {
				case <-cs.queue:
				default:
					return
				}
			}
		}
	}
}

// fetchLoop long-polls one partition and hands batches to Poll. Sending on
// the bounded queue blocks when the application is slow: that is the
// backpressure that keeps consumer memory bounded.
func (cs *Consumer) fetchLoop(ctx context.Context, gen int64, tp protocol.TopicPartition, offset int64) {
	attempt := 0
	for ctx.Err() == nil {
		resp, err := cs.c.Fetch(ctx, tp.Topic, tp.Partition, offset, FetchOptions{
			MaxEvents: cs.cfg.FetchMaxEvents, MaxBytes: cs.cfg.FetchMaxBytes, Wait: cs.cfg.FetchWait})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errCode(err) == protocol.CodeOffsetOutOfRange {
				// Our position was deleted by retention (or is beyond the
				// end after an unclean failover). Records were skipped;
				// make it visible rather than silently jumping.
				newOff, rerr := cs.resetOffset(ctx, tp)
				if rerr == nil {
					cs.log.Warn("offset out of range; applying reset policy", "partition", tp.String(),
						"from", offset, "to", newOff, "policy", cs.cfg.AutoOffsetReset)
					offset = newOff
					continue
				}
			}
			if c := errCode(err); c == protocol.CodeNotLeader || c == "" {
				cs.c.InvalidateMetadata()
			}
			_ = sleep(ctx, backoff(100*time.Millisecond, attempt, 2*time.Second))
			attempt++
			continue
		}
		attempt = 0
		if len(resp.Records) == 0 {
			continue
		}
		recs := make([]Record, len(resp.Records))
		for i, r := range resp.Records {
			recs[i] = fromWire(r)
		}
		select {
		case cs.queue <- batch{tp: tp, gen: gen, records: recs, hwm: resp.HighWatermark}:
			offset = resp.NextOffset
		case <-ctx.Done():
			return
		}
	}
}

// Commit commits the positions of all records returned by Poll so far
// (i.e. "everything I have processed"). Call it after processing.
func (cs *Consumer) Commit(ctx context.Context) error {
	cs.mu.Lock()
	offs := make([]protocol.OffsetCommit, 0, len(cs.positions))
	for tp, off := range cs.positions {
		offs = append(offs, protocol.OffsetCommit{Topic: tp.Topic, Partition: tp.Partition, Offset: off})
	}
	member, gen := cs.memberID, cs.generation
	cs.mu.Unlock()
	return cs.commit(ctx, member, gen, offs)
}

// CommitRecords commits offset+1 for the highest record per partition.
func (cs *Consumer) CommitRecords(ctx context.Context, recs []Record) error {
	hi := map[protocol.TopicPartition]int64{}
	for _, r := range recs {
		tp := protocol.TopicPartition{Topic: r.Topic, Partition: r.Partition}
		if r.Offset+1 > hi[tp] {
			hi[tp] = r.Offset + 1
		}
	}
	offs := make([]protocol.OffsetCommit, 0, len(hi))
	for tp, off := range hi {
		offs = append(offs, protocol.OffsetCommit{Topic: tp.Topic, Partition: tp.Partition, Offset: off})
	}
	cs.mu.Lock()
	member, gen := cs.memberID, cs.generation
	cs.mu.Unlock()
	return cs.commit(ctx, member, gen, offs)
}

func (cs *Consumer) commit(ctx context.Context, member string, gen int32, offs []protocol.OffsetCommit) error {
	if len(offs) == 0 || member == "" {
		return nil
	}
	sort.Slice(offs, func(i, j int) bool { return offs[i].Partition < offs[j].Partition })
	err := cs.c.controllerDo(ctx, "POST", cs.groupPath("/offsets"),
		protocol.CommitOffsetsRequest{MemberID: member, Generation: gen, Offsets: offs}, nil)
	if err != nil {
		if code := errCode(err); code == protocol.CodeIllegalGeneration || code == protocol.CodeUnknownMember || code == protocol.CodeNotOwner {
			// We were fenced: another member owns these partitions now.
			// Our uncommitted work will be redone by the new owner.
			cs.mu.Lock()
			cs.needRejoin = true
			cs.mu.Unlock()
		}
		return err
	}
	cs.commits.Add(1)
	cs.lastCommit = time.Now()
	return nil
}

func (cs *Consumer) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(cs.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		cs.mu.Lock()
		member, gen, rejoin := cs.memberID, cs.generation, cs.needRejoin
		cs.mu.Unlock()
		if member == "" || rejoin {
			continue
		}
		if since := time.Since(time.Unix(0, cs.lastPoll.Load())); since > cs.cfg.MaxPollInterval {
			cs.log.Warn("poll interval exceeded; leaving group so partitions can be reassigned",
				"since_last_poll", since.String())
			_ = cs.c.controllerDo(ctx, "POST", cs.groupPath("/leave"), protocol.LeaveGroupRequest{MemberID: member}, nil)
			cs.mu.Lock()
			cs.memberID, cs.needRejoin = "", true
			cs.mu.Unlock()
			continue
		}
		hctx, cancel := context.WithTimeout(ctx, cs.cfg.HeartbeatInterval)
		var resp protocol.GroupHeartbeatResponse
		err := cs.c.controllerDo(hctx, "POST", cs.groupPath("/heartbeat"),
			protocol.GroupHeartbeatRequest{MemberID: member, Generation: gen}, &resp)
		cancel()
		switch errCode(err) {
		case "":
		case protocol.CodeRebalance, protocol.CodeIllegalGeneration:
			cs.mu.Lock()
			cs.needRejoin = true
			cs.mu.Unlock()
		case protocol.CodeUnknownMember:
			cs.mu.Lock()
			cs.memberID, cs.needRejoin = "", true
			cs.mu.Unlock()
		}
	}
}

// Close commits (if auto-commit), leaves the group and stops goroutines.
// Leaving explicitly triggers an immediate rebalance instead of making the
// group wait for the session timeout.
func (cs *Consumer) Close(ctx context.Context) error {
	cs.mu.Lock()
	if cs.closed {
		cs.mu.Unlock()
		return nil
	}
	cs.closed = true
	member := cs.memberID
	cs.mu.Unlock()
	cs.stopFetchers()
	var err error
	if cs.cfg.AutoCommit {
		err = cs.Commit(ctx)
	}
	cs.hbStop()
	cs.hbWG.Wait()
	if member != "" {
		_ = cs.c.controllerDo(ctx, "POST", cs.groupPath("/leave"), protocol.LeaveGroupRequest{MemberID: member}, nil)
	}
	return err
}

func tpsString(tps []protocol.TopicPartition) string {
	s := make([]string, len(tps))
	for i, tp := range tps {
		s[i] = tp.String()
	}
	sort.Strings(s)
	return fmt.Sprint(s)
}
