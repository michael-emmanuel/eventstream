package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/observability"
	"github.com/michael-emmanuel/eventstream/internal/protocol"
	"github.com/michael-emmanuel/eventstream/internal/routing"
)

// ErrBufferFull is returned when the producer's memory budget is exhausted
// for longer than MaxBlock: the application is producing faster than the
// cluster accepts, and must slow down or shed load.
var ErrBufferFull = errors.New("producer buffer full")

// ErrClosed is returned after Close.
var ErrClosed = errors.New("producer closed")

// ProducerConfig configures a Producer.
type ProducerConfig struct {
	Acks string // "all" (default), "leader", "quorum"
	// DisableIdempotence turns off producer IDs and sequence numbers.
	// Retries can then create duplicates.
	DisableIdempotence bool
	// Batching: a batch is sent when it reaches BatchMaxRecords or
	// BatchMaxBytes, or Linger after its first record arrived. Larger and
	// longer batches raise throughput and latency together.
	BatchMaxRecords int
	BatchMaxBytes   int
	Linger          time.Duration
	// MaxBufferedBytes bounds memory held by unsent records; Send blocks up
	// to MaxBlock when it is exhausted (backpressure into the application).
	MaxBufferedBytes int64
	MaxBlock         time.Duration
	// DeliveryTimeout bounds the total time (including retries) a record
	// may take before it fails.
	DeliveryTimeout time.Duration
	RequestTimeout  time.Duration
	RetryBackoff    time.Duration
	Partitioner     routing.Partitioner
	Tracer          *observability.Tracer
	Logger          *slog.Logger
}

func (c *ProducerConfig) defaults() {
	if c.Acks == "" {
		c.Acks = "all"
	}
	if c.BatchMaxRecords == 0 {
		c.BatchMaxRecords = 500
	}
	if c.BatchMaxBytes == 0 {
		c.BatchMaxBytes = 512 << 10
	}
	if c.Linger == 0 {
		c.Linger = 5 * time.Millisecond
	}
	if c.MaxBufferedBytes == 0 {
		c.MaxBufferedBytes = 32 << 20
	}
	if c.MaxBlock == 0 {
		c.MaxBlock = 5 * time.Second
	}
	if c.DeliveryTimeout == 0 {
		c.DeliveryTimeout = 30 * time.Second
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 10 * time.Second
	}
	if c.RetryBackoff == 0 {
		c.RetryBackoff = 50 * time.Millisecond
	}
	if c.Partitioner == nil {
		c.Partitioner = &routing.Default{}
	}
	if c.Logger == nil {
		c.Logger = observability.DiscardLogger()
	}
}

// Message is a record to produce. A nil Value is a tombstone.
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Partition *int32 // explicit partition (optional)
}

// Result is the outcome of producing one message.
type Result struct {
	Topic     string
	Partition int32
	Offset    int64 // -1 if unknown (e.g. very old duplicate)
	Duplicate bool
	Err       error
	Latency   time.Duration
}

// Future resolves to a Result.
type Future struct {
	done chan struct{}
	res  Result
}

// Done is closed when the result is ready.
func (f *Future) Done() <-chan struct{} { return f.done }

// Wait blocks for the result.
func (f *Future) Wait(ctx context.Context) Result {
	select {
	case <-f.done:
		return f.res
	case <-ctx.Done():
		return Result{Err: ctx.Err(), Offset: -1}
	}
}

type pending struct {
	rec      protocol.Record
	size     int64
	fut      *Future
	enqueued time.Time
	span     *observability.Span
}

// ProducerStats are cumulative counters.
type ProducerStats struct {
	Records    int64
	Batches    int64
	Retries    int64
	Errors     int64
	Duplicates int64
	PIDResets  int64
}

// Producer batches messages per partition and sends them with retries.
//
// Ordering and duplicates: each partition has exactly one batch in flight.
// Combined with per-partition producer sequence numbers, a retried batch
// is either recognized by the leader as a duplicate or appended in its
// original position, so retries neither reorder nor duplicate records.
// This is idempotent production; it is not exactly-once processing (a
// consumer can still process a record twice).
type Producer struct {
	c   *Client
	cfg ProducerConfig

	mu       sync.Mutex
	queues   map[protocol.TopicPartition]*partQueue
	buffered int64
	space    chan struct{} // closed when buffer space is released
	closed   bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	stats  struct{ records, batches, retries, errors, duplicates, resets atomic.Int64 }
}

// partQueue is one partition's accumulator and its sender goroutine's state.
type partQueue struct {
	tp      protocol.TopicPartition
	mu      sync.Mutex
	pending []*pending
	wake    chan struct{} // capacity 1
	// Idempotence state is per partition: a fresh producer ID can be taken
	// for one partition (after an unrecoverable batch failure) without
	// disturbing the others.
	pid     int64
	nextSeq int64
}

// NewProducer creates a producer.
func (c *Client) NewProducer(cfg ProducerConfig) *Producer {
	cfg.defaults()
	ctx, cancel := context.WithCancel(context.Background())
	return &Producer{c: c, cfg: cfg, queues: map[protocol.TopicPartition]*partQueue{},
		space: make(chan struct{}), ctx: ctx, cancel: cancel}
}

// Stats returns cumulative counters.
func (p *Producer) Stats() ProducerStats {
	return ProducerStats{Records: p.stats.records.Load(), Batches: p.stats.batches.Load(), Retries: p.stats.retries.Load(),
		Errors: p.stats.errors.Load(), Duplicates: p.stats.duplicates.Load(), PIDResets: p.stats.resets.Load()}
}

// Send enqueues a message and returns a Future. It blocks (up to MaxBlock)
// when the producer's memory budget is exhausted.
func (p *Producer) Send(ctx context.Context, m Message) (*Future, error) {
	n, err := p.c.Partitions(ctx, m.Topic)
	if err != nil {
		return nil, err
	}
	part, err := p.cfg.Partitioner.Partition(m.Key, m.Partition, n)
	if err != nil {
		return nil, err
	}
	pd := &pending{fut: &Future{done: make(chan struct{})}, enqueued: time.Now()}
	if m.Key != nil {
		k := string(m.Key)
		pd.rec.Key = &k
	}
	if m.Value != nil {
		v := string(m.Value)
		pd.rec.Value = &v
	}
	if len(m.Headers) > 0 || p.cfg.Tracer != nil {
		pd.rec.Headers = make(map[string]string, len(m.Headers)+1)
		for k, v := range m.Headers {
			pd.rec.Headers[k] = v
		}
	}
	if p.cfg.Tracer != nil {
		parent, _ := observability.ParseTraceparent(m.Headers[observability.TraceparentHeader])
		_, span := p.cfg.Tracer.Start(ctx, "producer.send", parent)
		span.SetAttr("topic", m.Topic)
		span.SetAttr("partition", part)
		if span.Context().Sampled {
			pd.rec.Headers[observability.TraceparentHeader] = span.Context().String()
		}
		pd.span = span
	}
	pd.size = int64(len(m.Key)+len(m.Value)) + 64
	for k, v := range pd.rec.Headers {
		pd.size += int64(len(k) + len(v))
	}
	if err := p.reserve(ctx, pd.size); err != nil {
		return nil, err
	}
	tp := protocol.TopicPartition{Topic: m.Topic, Partition: part}
	q, err := p.queue(tp)
	if err != nil {
		p.release(pd.size)
		return nil, err
	}
	q.mu.Lock()
	q.pending = append(q.pending, pd)
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return pd.fut, nil
}

// SendSync sends one message and waits for its result.
func (p *Producer) SendSync(ctx context.Context, m Message) Result {
	f, err := p.Send(ctx, m)
	if err != nil {
		return Result{Topic: m.Topic, Offset: -1, Err: err}
	}
	return f.Wait(ctx)
}

func (p *Producer) reserve(ctx context.Context, n int64) error {
	deadline := time.NewTimer(p.cfg.MaxBlock)
	defer deadline.Stop()
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return ErrClosed
		}
		if p.buffered+n <= p.cfg.MaxBufferedBytes || p.buffered == 0 {
			p.buffered += n
			p.mu.Unlock()
			return nil
		}
		ch := p.space
		p.mu.Unlock()
		select {
		case <-ch:
		case <-deadline.C:
			return fmt.Errorf("%w: %d bytes buffered", ErrBufferFull, p.cfg.MaxBufferedBytes)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *Producer) release(n int64) {
	p.mu.Lock()
	p.buffered -= n
	close(p.space)
	p.space = make(chan struct{})
	p.mu.Unlock()
}

// Buffered returns the bytes currently held by the producer.
func (p *Producer) Buffered() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.buffered
}

func (p *Producer) queue(tp protocol.TopicPartition) (*partQueue, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosed
	}
	q := p.queues[tp]
	if q == nil {
		q = &partQueue{tp: tp, wake: make(chan struct{}, 1)}
		p.queues[tp] = q
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.sender(q)
		}()
	}
	return q, nil
}

// sender owns one partition: it waits for records, lingers to fill a
// batch, sends it (with retries), and completes the futures.
func (p *Producer) sender(q *partQueue) {
	for {
		q.mu.Lock()
		empty := len(q.pending) == 0
		q.mu.Unlock()
		if empty {
			select {
			case <-q.wake:
				continue
			case <-p.ctx.Done():
				return
			}
		}
		// Linger: give the batch a chance to fill.
		q.mu.Lock()
		first := q.pending[0].enqueued
		full := len(q.pending) >= p.cfg.BatchMaxRecords
		q.mu.Unlock()
		if wait := p.cfg.Linger - time.Since(first); !full && wait > 0 {
			t := time.NewTimer(wait)
			select {
			case <-t.C:
			case <-p.ctx.Done():
				t.Stop()
			}
		}
		batch := p.takeBatch(q)
		if len(batch) > 0 {
			p.sendBatch(q, batch)
		}
	}
}

func (p *Producer) takeBatch(q *partQueue) []*pending {
	q.mu.Lock()
	defer q.mu.Unlock()
	n, bytes := 0, int64(0)
	for n < len(q.pending) && n < p.cfg.BatchMaxRecords {
		if n > 0 && bytes+q.pending[n].size > int64(p.cfg.BatchMaxBytes) {
			break
		}
		bytes += q.pending[n].size
		n++
	}
	batch := q.pending[:n:n]
	q.pending = append([]*pending(nil), q.pending[n:]...)
	return batch
}

func (p *Producer) sendBatch(q *partQueue, batch []*pending) {
	ctx := p.ctx
	if ctx.Err() != nil {
		// Closing: still deliver what was accepted, bounded by delivery timeout.
		ctx = context.Background()
	}
	deadline := batch[0].enqueued.Add(p.cfg.DeliveryTimeout)
	req := protocol.ProduceRequest{Acks: p.cfg.Acks, TimeoutMs: int(p.cfg.RequestTimeout.Milliseconds()),
		Records: make([]protocol.Record, len(batch))}
	for i, pd := range batch {
		req.Records[i] = pd.rec
	}
	if !p.cfg.DisableIdempotence {
		if q.pid == 0 {
			pid, err := p.c.AllocateProducerID(ctx)
			if err != nil {
				p.complete(batch, protocol.ProduceResponse{}, fmt.Errorf("producer id: %w", err))
				return
			}
			q.pid, q.nextSeq = pid, 0
		}
		req.ProducerID, req.BaseSequence = q.pid, q.nextSeq
	}
	p.stats.batches.Add(1)
	var resp protocol.ProduceResponse
	var err error
	for attempt := 0; ; attempt++ {
		rctx, cancel := context.WithTimeout(ctx, p.cfg.RequestTimeout)
		resp, err = p.c.Produce(rctx, q.tp.Topic, q.tp.Partition, req)
		cancel()
		if err == nil || !retryable(err) || time.Now().After(deadline) {
			break
		}
		switch errCode(err) {
		case protocol.CodeNotLeader, protocol.CodeLeaseExpired, protocol.CodeUnknownTopic, protocol.CodeFencedEpoch, "":
			// "" is a transport error (connection refused, reset, timeout):
			// the leader may have died, so cached leadership is suspect.
			p.c.InvalidateMetadata()
		}
		p.stats.retries.Add(1)
		p.cfg.Logger.Debug("retrying batch", "partition", q.tp.String(), "attempt", attempt+1,
			"records", len(batch), "error", err)
		wait := retryAfter(err)
		if wait == 0 {
			wait = backoff(p.cfg.RetryBackoff, attempt, time.Second)
		}
		if time.Now().Add(wait).After(deadline) {
			wait = time.Until(deadline)
		}
		if sleep(ctx, wait) != nil {
			break
		}
	}
	if err != nil {
		p.cfg.Logger.Warn("batch failed", "partition", q.tp.String(), "records", len(batch),
			"attempts", "see retries", "error", err, "age", time.Since(batch[0].enqueued).String())
	}
	if err == nil {
		q.nextSeq += int64(len(batch))
	} else if !p.cfg.DisableIdempotence {
		// The batch may or may not have been appended, so the next
		// sequence number is unknown. Start a fresh sequence space for this
		// partition with a new producer ID. Ordering relative to the failed
		// batch is not guaranteed across this boundary.
		q.pid = 0
		p.stats.resets.Add(1)
	}
	p.complete(batch, resp, err)
}

func (p *Producer) complete(batch []*pending, resp protocol.ProduceResponse, err error) {
	var freed int64
	for i, pd := range batch {
		r := Result{Topic: resp.Topic, Partition: resp.Partition, Err: err, Latency: time.Since(pd.enqueued), Offset: -1}
		if err == nil {
			if resp.BaseOffset >= 0 {
				r.Offset = resp.BaseOffset + int64(i)
			}
			r.Duplicate = resp.Duplicate
		}
		pd.fut.res = r
		if pd.span != nil {
			pd.span.SetAttr("offset", r.Offset)
			pd.span.End(err)
		}
		close(pd.fut.done)
		freed += pd.size
	}
	if err != nil {
		p.stats.errors.Add(int64(len(batch)))
	} else {
		p.stats.records.Add(int64(len(batch)))
		if resp.Duplicate {
			p.stats.duplicates.Add(int64(len(batch)))
		}
	}
	p.release(freed)
}

// Flush waits until every buffered message has been sent (or failed).
func (p *Producer) Flush(ctx context.Context) error {
	for {
		p.mu.Lock()
		if p.buffered == 0 {
			p.mu.Unlock()
			return nil
		}
		ch := p.space
		p.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Close flushes and stops the producer.
func (p *Producer) Close(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()
	err := p.Flush(ctx)
	p.cancel()
	p.wg.Wait()
	return err
}

// Queues returns the partitions the producer has sent to, for diagnostics.
func (p *Producer) Queues() []protocol.TopicPartition {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]protocol.TopicPartition, 0, len(p.queues))
	for tp := range p.queues {
		out = append(out, tp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}
