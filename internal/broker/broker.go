// Package broker implements a broker process: it hosts partition replicas,
// serves produce and fetch requests for partitions it leads, replicates
// partitions it follows, and keeps a session and lease with the controller.
//
// Goroutines owned by a Broker (all started in Start, all stopped by Stop,
// all tracked by one WaitGroup):
//
//	heartbeat loop    renews the controller session and the write lease
//	metadata loop     long-polls the controller and applies role changes
//	isr loop          proposes ISR shrink/expand for partitions it leads
//	flush loop        fsyncs logs on an interval (unless fsync-per-append)
//	checkpoint loop   persists high-water marks
//	maintenance loop  retention, compaction, producer-state expiry
//	fetchers          one per followed partition, started/stopped by metadata
//	http server       request goroutines are bounded by MaxConcurrentRequests
package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/auth"
	"github.com/michael-emmanuel/eventstream/internal/fault"
	plog "github.com/michael-emmanuel/eventstream/internal/log"
	"github.com/michael-emmanuel/eventstream/internal/metadata"
	"github.com/michael-emmanuel/eventstream/internal/observability"
	"github.com/michael-emmanuel/eventstream/internal/partition"
	"github.com/michael-emmanuel/eventstream/internal/protocol"
	"github.com/michael-emmanuel/eventstream/internal/ratelimit"
	"github.com/michael-emmanuel/eventstream/internal/transport"
)

// Config configures a broker.
type Config struct {
	ID            int32
	ListenAddr    string // host:port to bind; ":0" picks a free port
	AdvertiseURL  string // URL others use to reach this broker; derived from the listener if empty
	DataDir       string
	ControllerURL string

	HeartbeatInterval   time.Duration
	MetadataWait        time.Duration
	ISRInterval         time.Duration
	FlushInterval       time.Duration
	FsyncEveryAppend    bool
	CheckpointInterval  time.Duration
	MaintenanceInterval time.Duration
	VerifyOnRecovery    bool

	ReplicaFetchMaxBytes int
	ReplicaFetchWait     time.Duration

	MaxRequestBytes         int64
	MaxInflightProduceBytes int64
	MaxFetchBytes           int
	MaxLongPollWaiters      int
	MaxLongPollWait         time.Duration
	MaxConcurrentRequests   int
	DefaultProduceTimeout   time.Duration

	Limits     ratelimit.Limits
	Auth       *auth.Config
	HTTPClient *http.Client // for controller and replication calls
	TLSCert    string
	TLSKey     string

	TraceSampleRate float64
	Logger          *slog.Logger
	Metrics         *observability.Registry
	Faults          *fault.Injector
	Now             func() time.Time
	Exit            func(code int) // for crash fault injection; defaults to os.Exit
}

func (c *Config) defaults() {
	d := func(v *time.Duration, def time.Duration) {
		if *v == 0 {
			*v = def
		}
	}
	d(&c.HeartbeatInterval, time.Second)
	d(&c.MetadataWait, 2*time.Second)
	d(&c.ISRInterval, 500*time.Millisecond)
	d(&c.FlushInterval, 200*time.Millisecond)
	d(&c.CheckpointInterval, 2*time.Second)
	d(&c.MaintenanceInterval, 30*time.Second)
	d(&c.ReplicaFetchWait, 500*time.Millisecond)
	d(&c.MaxLongPollWait, 30*time.Second)
	d(&c.DefaultProduceTimeout, 5*time.Second)
	if c.ReplicaFetchMaxBytes == 0 {
		c.ReplicaFetchMaxBytes = 1 << 20
	}
	if c.MaxRequestBytes == 0 {
		c.MaxRequestBytes = 8 << 20
	}
	if c.MaxInflightProduceBytes == 0 {
		c.MaxInflightProduceBytes = 64 << 20
	}
	if c.MaxFetchBytes == 0 {
		c.MaxFetchBytes = 4 << 20
	}
	if c.MaxLongPollWaiters == 0 {
		c.MaxLongPollWaiters = 1000
	}
	if c.MaxConcurrentRequests == 0 {
		c.MaxConcurrentRequests = 2000
	}
	if c.Auth == nil {
		c.Auth = &auth.Config{}
	}
	if c.HTTPClient == nil {
		c.HTTPClient, _ = transport.NewHTTPClient(transport.ClientOptions{})
	}
	if c.Logger == nil {
		c.Logger = observability.DiscardLogger()
	}
	if c.Metrics == nil {
		c.Metrics = observability.NewRegistry()
	}
	if c.Faults == nil {
		c.Faults = fault.New()
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Exit == nil {
		c.Exit = os.Exit
	}
}

type tpKey struct {
	topic string
	p     int32
}

func (k tpKey) String() string { return fmt.Sprintf("%s-%d", k.topic, k.p) }

type fetcher struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Broker is one broker process.
type Broker struct {
	cfg         Config
	log         *slog.Logger
	m           *brokerMetrics
	faults      *fault.Injector
	limiter     *ratelimit.Limiter
	tracer      *observability.Tracer
	incarnation string

	start      time.Time
	leaseUntil atomic.Int64 // monotonic nanoseconds since start
	md         atomic.Pointer[metadata.Cluster]

	mu         sync.RWMutex
	parts      map[tpKey]*partition.Partition
	fetchers   map[tpKey]*fetcher
	seenTopics map[string]string // topic name -> ID seen in this process

	inflight *byteBudget
	pollers  chan struct{}

	srv     *http.Server
	ln      net.Listener
	url     string
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	maintMu sync.Mutex
	stopped atomic.Bool
}

// New opens existing partition data (running crash recovery for each) and
// prepares the broker. Call Start to join the cluster.
func New(cfg Config) (*Broker, error) {
	cfg.defaults()
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	var inc [8]byte
	_, _ = rand.Read(inc[:])
	b := &Broker{
		cfg:         cfg,
		log:         cfg.Logger.With("broker", cfg.ID),
		faults:      cfg.Faults,
		limiter:     ratelimit.New(cfg.Limits, cfg.Now),
		tracer:      observability.NewTracer(cfg.Logger, cfg.TraceSampleRate),
		incarnation: hex.EncodeToString(inc[:]),
		start:       time.Now(),
		parts:       map[tpKey]*partition.Partition{},
		fetchers:    map[tpKey]*fetcher{},
		seenTopics:  map[string]string{},
		inflight:    newByteBudget(cfg.MaxInflightProduceBytes),
		pollers:     make(chan struct{}, cfg.MaxLongPollWaiters),
	}
	b.m = newBrokerMetrics(cfg.Metrics, b)
	if err := b.recoverLocalPartitions(); err != nil {
		return nil, err
	}
	return b, nil
}

// recoverLocalPartitions opens every partition directory found on disk.
// Each open runs log recovery (truncate torn tails, rebuild indexes) and
// rebuilds producer/epoch state. Roles are assigned later by metadata.
func (b *Broker) recoverLocalPartitions() error {
	entries, err := os.ReadDir(b.cfg.DataDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(b.cfg.DataDir, e.Name())
		if _, err := os.Stat(filepath.Join(dir, topicIDFile)); err != nil {
			continue
		}
		i := strings.LastIndex(e.Name(), "-")
		if i <= 0 {
			continue
		}
		pn, err := strconv.Atoi(e.Name()[i+1:])
		if err != nil {
			continue
		}
		k := tpKey{e.Name()[:i], int32(pn)}
		p, rep, err := b.openPartition(k, dir, partition.DefaultTopicConfig(), metadata.DefaultTopicConfig())
		if err != nil {
			// A corrupt replica is not fatal for the broker: the partition
			// stays offline here and the controller elects elsewhere.
			b.log.Error("partition recovery failed", "partition", k.String(), "error", err)
			continue
		}
		b.parts[k] = p
		b.logRecovery(k, rep)
	}
	return nil
}

func (b *Broker) logRecovery(k tpKey, rep plog.RecoveryReport) {
	attrs := []any{"partition", k.String(), "segments", rep.Segments, "log_start", rep.LogStartOffset,
		"log_end", rep.LogEndOffset, "indexes_rebuilt", rep.IndexesRebuilt, "duration_ms", rep.Duration.Milliseconds()}
	if rep.TruncatedBytes > 0 {
		b.m.recoveryTruncated.With().Add(float64(rep.TruncatedBytes))
		b.log.Warn("recovery truncated incomplete or corrupt log tail",
			append(attrs, "truncated_bytes", rep.TruncatedBytes, "cause", rep.TruncationCause)...)
		return
	}
	b.log.Info("partition recovered", attrs...)
}

const topicIDFile = "topic-id"

func (b *Broker) partitionDir(k tpKey) string {
	return filepath.Join(b.cfg.DataDir, fmt.Sprintf("%s-%d", k.topic, k.p))
}

func (b *Broker) openPartition(k tpKey, dir string, pc partition.TopicConfig, tc metadata.TopicConfig) (*partition.Partition, plog.RecoveryReport, error) {
	return partition.Open(partition.Options{
		Topic: k.topic, Partition: k.p, BrokerID: b.cfg.ID, Dir: dir,
		Log: plog.Config{
			SegmentBytes:       tc.SegmentBytes,
			SegmentMaxAge:      time.Duration(tc.SegmentMs) * time.Millisecond,
			IndexIntervalBytes: 4096,
			MaxRecordSize:      tc.MaxEventBytes + 1024,
			SyncEveryAppend:    b.cfg.FsyncEveryAppend,
			VerifyOnRecovery:   b.cfg.VerifyOnRecovery,
			Hooks:              plog.Hooks{BeforeWrite: b.faults.BeforeDiskWrite, BeforeSync: b.faults.BeforeDiskSync},
		},
		Config:     pc,
		LeaseValid: b.leaseValid,
		Now:        b.cfg.Now,
	})
}

func partitionConfig(tc metadata.TopicConfig) partition.TopicConfig {
	return partition.TopicConfig{
		MinISR:             tc.MinInsyncReplicas,
		MaxRecordSize:      tc.MaxEventBytes + 1024,
		ReplicaLagMaxTime:  time.Duration(tc.ReplicaLagMaxMs) * time.Millisecond,
		ProducerStateTTL:   15 * time.Minute,
		CleanupDelete:      tc.HasPolicy(metadata.CleanupDelete),
		CleanupCompact:     tc.HasPolicy(metadata.CleanupCompact),
		Retention:          time.Duration(tc.RetentionMs) * time.Millisecond,
		RetentionBytes:     tc.RetentionBytes,
		TombstoneRetention: time.Duration(tc.TombstoneRetentionMs) * time.Millisecond,
	}
}

// Partition returns the local replica of topic/partition, or nil. Exposed for
// tests and tooling; request handling goes through the HTTP API.
func (b *Broker) Partition(topic string, p int32) *partition.Partition { return b.partition(topic, p) }

// URL returns the advertised URL (valid after Start).
func (b *Broker) URL() string { return b.url }

// ID returns the broker ID.
func (b *Broker) ID() int32 { return b.cfg.ID }

// Faults returns the fault injector.
func (b *Broker) Faults() *fault.Injector { return b.faults }

// Limiter returns the rate limiter.
func (b *Broker) Limiter() *ratelimit.Limiter { return b.limiter }

// Start binds the listener, serves HTTP and starts background loops.
func (b *Broker) Start() error {
	ln, err := net.Listen("tcp", b.cfg.ListenAddr)
	if err != nil {
		return err
	}
	b.ln = ln
	b.url = b.cfg.AdvertiseURL
	if b.url == "" {
		scheme := "http"
		if b.cfg.TLSCert != "" {
			scheme = "https"
		}
		b.url = fmt.Sprintf("%s://%s", scheme, ln.Addr().String())
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())
	b.srv = &http.Server{
		Handler:           b.handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		BaseContext:       func(net.Listener) context.Context { return b.ctx },
	}
	b.goLoop("http", func(ctx context.Context) {
		var err error
		if b.cfg.TLSCert != "" {
			err = b.srv.ServeTLS(ln, b.cfg.TLSCert, b.cfg.TLSKey)
		} else {
			err = b.srv.Serve(ln)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			b.log.Error("http server failed", "error", err)
		}
	})
	b.goLoop("heartbeat", b.heartbeatLoop)
	b.goLoop("metadata", b.metadataLoop)
	b.goLoop("isr", b.isrLoop)
	if !b.cfg.FsyncEveryAppend {
		b.goLoop("flush", b.flushLoop)
	}
	b.goLoop("checkpoint", b.checkpointLoop)
	b.goLoop("maintenance", b.maintenanceLoop)
	b.log.Info("broker started", "url", b.url, "data_dir", b.cfg.DataDir, "incarnation", b.incarnation,
		"fsync_every_append", b.cfg.FsyncEveryAppend, "flush_interval", b.cfg.FlushInterval.String())
	return nil
}

func (b *Broker) goLoop(name string, fn func(ctx context.Context)) {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		fn(b.ctx)
	}()
}

// Stop shuts down gracefully: stop accepting requests, stop loops and
// fetchers, flush and close every log (writing indexes and HWM checkpoints).
func (b *Broker) Stop() error {
	if !b.stopped.CompareAndSwap(false, true) {
		return nil
	}
	b.cancel()
	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = b.srv.Shutdown(shCtx)
	b.mu.Lock()
	fs := b.fetchers
	b.fetchers = map[tpKey]*fetcher{}
	b.mu.Unlock()
	for _, f := range fs {
		f.cancel()
	}
	b.wg.Wait()
	b.mu.Lock()
	defer b.mu.Unlock()
	var first error
	for k, p := range b.parts {
		if err := p.Close(); err != nil && first == nil {
			first = fmt.Errorf("close %s: %w", k, err)
		}
	}
	b.log.Info("broker stopped")
	return first
}

// Kill simulates a crash for tests: the listener closes and loops stop, but
// logs are not flushed or closed cleanly. (A real process crash also loses
// nothing that reached the page cache; a machine crash would.)
func (b *Broker) Kill() {
	if !b.stopped.CompareAndSwap(false, true) {
		return
	}
	b.cancel()
	_ = b.srv.Close()
	b.mu.Lock()
	for _, f := range b.fetchers {
		f.cancel()
	}
	b.mu.Unlock()
	b.wg.Wait()
	b.mu.Lock()
	for _, p := range b.parts {
		// Release file handles without writing indexes or checkpoints.
		_ = p.Log().Close()
	}
	b.mu.Unlock()
}

// --- lease -----------------------------------------------------------------

// leaseValid reports whether the controller lease is current. Measured with
// the monotonic clock, so wall-clock jumps cannot extend it.
func (b *Broker) leaseValid() bool {
	return int64(time.Since(b.start)) < b.leaseUntil.Load()
}

// --- partitions ------------------------------------------------------------

func (b *Broker) partition(topic string, p int32) *partition.Partition {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.parts[tpKey{topic, p}]
}

func (b *Broker) partitions() map[tpKey]*partition.Partition {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make(map[tpKey]*partition.Partition, len(b.parts))
	for k, p := range b.parts {
		out[k] = p
	}
	return out
}

// ensurePartition returns the local replica, creating it if needed. A
// directory belonging to a different topic ID (deleted and recreated topic)
// is wiped first.
func (b *Broker) ensurePartition(t *metadata.Topic, pn int32) (*partition.Partition, error) {
	k := tpKey{t.Name, pn}
	b.mu.RLock()
	p := b.parts[k]
	b.mu.RUnlock()
	dir := b.partitionDir(k)
	idPath := filepath.Join(dir, topicIDFile)
	if p != nil {
		if id, _ := os.ReadFile(idPath); string(id) == t.ID {
			return p, nil
		}
		b.log.Warn("local replica belongs to an older topic with the same name; deleting", "partition", k.String())
		_ = p.Delete()
		b.mu.Lock()
		delete(b.parts, k)
		b.mu.Unlock()
	} else if id, err := os.ReadFile(idPath); err == nil && string(id) != t.ID {
		_ = os.RemoveAll(dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if err := os.WriteFile(idPath, []byte(t.ID), 0o644); err != nil {
		return nil, err
	}
	p, rep, err := b.openPartition(k, dir, partitionConfig(t.Config), t.Config)
	if err != nil {
		return nil, err
	}
	b.logRecovery(k, rep)
	b.mu.Lock()
	b.parts[k] = p
	b.mu.Unlock()
	return p, nil
}

// applyMetadata reconciles local replicas with a metadata snapshot.
func (b *Broker) applyMetadata(md *metadata.Cluster) {
	prev := b.md.Load()
	if prev != nil && md.Version < prev.Version {
		return
	}
	b.md.Store(md)
	want := map[tpKey]bool{}
	for ti := range md.Topics {
		t := &md.Topics[ti]
		b.mu.Lock()
		b.seenTopics[t.Name] = t.ID
		b.mu.Unlock()
		for _, ps := range t.Partitions {
			if !containsID(ps.Replicas, b.cfg.ID) {
				continue
			}
			k := tpKey{t.Name, ps.Partition}
			want[k] = true
			p, err := b.ensurePartition(t, ps.Partition)
			if err != nil {
				b.log.Error("cannot open replica", "partition", k.String(), "error", err)
				continue
			}
			p.SetConfig(partitionConfig(t.Config))
			if ps.Leader == b.cfg.ID {
				b.stopFetcher(k)
				if p.BecomeLeader(ps.LeaderEpoch, ps.Replicas, ps.ISR) {
					b.m.roleChanges.With("leader").Inc()
					b.log.Info("became leader", "partition", k.String(), "epoch", ps.LeaderEpoch,
						"isr", ps.ISR, "log_end", p.LogEndOffset(), "hwm", p.HighWatermark())
				}
				continue
			}
			if p.BecomeFollower(ps.LeaderEpoch, ps.Leader, ps.Replicas, ps.ISR) {
				b.m.roleChanges.With("follower").Inc()
				b.log.Info("became follower", "partition", k.String(), "leader", ps.Leader, "epoch", ps.LeaderEpoch)
			}
			if ps.Leader >= 0 {
				b.ensureFetcher(k, p)
			} else {
				b.stopFetcher(k)
			}
		}
	}
	// Remove replicas this broker no longer hosts. Only delete data for
	// topics this process has seen in metadata before (deleted or moved
	// since); a directory for a topic never seen is reported, not deleted,
	// so a controller that lost its state cannot wipe the cluster's data.
	for k, p := range b.partitions() {
		if want[k] {
			continue
		}
		b.stopFetcher(k)
		t := md.Topic(k.topic)
		b.mu.RLock()
		_, seen := b.seenTopics[k.topic]
		b.mu.RUnlock()
		if t == nil && !seen {
			b.log.Warn("orphaned partition directory (topic unknown to controller); leaving data in place", "partition", k.String())
			continue
		}
		b.log.Info("removing replica", "partition", k.String())
		b.mu.Lock()
		delete(b.parts, k)
		b.mu.Unlock()
		_ = p.Delete()
		b.m.forgetPartition(k)
	}
	b.mu.Lock()
	for name := range b.seenTopics {
		if md.Topic(name) == nil {
			b.seenTopics[name] = "" // keep "seen" so a late directory is still cleaned
		}
	}
	b.mu.Unlock()
}

func (b *Broker) ensureFetcher(k tpKey, p *partition.Partition) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.fetchers[k]; ok || b.stopped.Load() {
		return
	}
	ctx, cancel := context.WithCancel(b.ctx)
	f := &fetcher{cancel: cancel, done: make(chan struct{})}
	b.fetchers[k] = f
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		defer close(f.done)
		b.runFetcher(ctx, k, p)
	}()
}

func (b *Broker) stopFetcher(k tpKey) {
	b.mu.Lock()
	f := b.fetchers[k]
	delete(b.fetchers, k)
	b.mu.Unlock()
	if f != nil {
		f.cancel()
		<-f.done
	}
}

// --- background loops ------------------------------------------------------

func (b *Broker) controller(ctx context.Context, method, path string, in, out any) error {
	if b.faults.ControllerUnreachable() {
		return fmt.Errorf("%w: controller unreachable", fault.ErrInjected)
	}
	return transport.DoJSON(ctx, b.cfg.HTTPClient, method, b.cfg.ControllerURL+path, in, out)
}

func (b *Broker) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(b.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		b.heartbeatOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (b *Broker) heartbeatOnce(ctx context.Context) {
	req := protocol.BrokerHeartbeatRequest{BrokerID: b.cfg.ID, URL: b.url, Incarnation: b.incarnation}
	for k, p := range b.partitions() {
		if p.Failed() != nil {
			req.OfflinePartitions = append(req.OfflinePartitions, protocol.TopicPartition{Topic: k.topic, Partition: k.p})
		}
	}
	// The lease starts when the request is sent, not when the response
	// arrives: the controller measures the session from receipt, which is
	// later, so the broker's lease always expires first.
	sent := time.Since(b.start)
	cctx, cancel := context.WithTimeout(ctx, b.cfg.HeartbeatInterval*2)
	defer cancel()
	var resp protocol.BrokerHeartbeatResponse
	if err := b.controller(cctx, "POST", "/v1/brokers/heartbeat", req, &resp); err != nil {
		if ctx.Err() == nil {
			b.m.heartbeatErrors.With().Inc()
			b.log.Warn("controller heartbeat failed", "error", err, "lease_valid", b.leaseValid())
		}
		return
	}
	b.leaseUntil.Store(int64(sent + time.Duration(resp.LeaseMs)*time.Millisecond))
}

func (b *Broker) metadataLoop(ctx context.Context) {
	for ctx.Err() == nil {
		var v int64
		if md := b.md.Load(); md != nil {
			v = md.Version + 1
		}
		var md metadata.Cluster
		cctx, cancel := context.WithTimeout(ctx, b.cfg.MetadataWait+3*time.Second)
		err := b.controller(cctx, "GET", fmt.Sprintf("/v1/metadata?min_version=%d&wait_ms=%d", v, b.cfg.MetadataWait.Milliseconds()), nil, &md)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				sleepCtx(ctx, 300*time.Millisecond)
			}
			continue
		}
		if cur := b.md.Load(); cur == nil || md.Version > cur.Version {
			b.applyMetadata(&md)
		}
	}
}

func (b *Broker) isrLoop(ctx context.Context) {
	t := time.NewTicker(b.cfg.ISRInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		for k, p := range b.partitions() {
			epoch, isr, changed := p.ISRProposal()
			if !changed {
				continue
			}
			var ps metadata.PartitionState
			err := b.controller(ctx, "POST", "/v1/partitions/isr", protocol.AlterISRRequest{
				Topic: k.topic, Partition: k.p, LeaderID: b.cfg.ID, LeaderEpoch: epoch, ISR: isr}, &ps)
			if err != nil {
				b.log.Debug("ISR change rejected", "partition", k.String(), "proposed", isr, "error", err)
				continue
			}
			p.CommitISR(epoch, ps.ISR)
			b.m.isrChanges.With().Inc()
			b.log.Info("ISR updated", "partition", k.String(), "isr", ps.ISR)
		}
	}
}

func (b *Broker) flushLoop(ctx context.Context) {
	t := time.NewTicker(b.cfg.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		for k, p := range b.partitions() {
			start := time.Now()
			if err := p.Flush(); err != nil {
				b.log.Error("fsync failed; replica taken offline", "partition", k.String(), "error", err)
				continue
			}
			b.m.fsync.With().ObserveSince(start)
		}
	}
}

func (b *Broker) checkpointLoop(ctx context.Context) {
	t := time.NewTicker(b.cfg.CheckpointInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		for _, p := range b.partitions() {
			_ = p.SaveCheckpoint()
		}
	}
}

func (b *Broker) maintenanceLoop(ctx context.Context) {
	t := time.NewTicker(b.cfg.MaintenanceInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		b.RunMaintenance(true)
		b.limiter.Cleanup()
	}
}

// MaintenanceReport is the per-partition outcome of RunMaintenance.
type MaintenanceReport struct {
	Partition         string  `json:"partition"`
	DeletedSegments   []int64 `json:"deleted_segments,omitempty"`
	DeletedBytes      int64   `json:"deleted_bytes,omitempty"`
	LogStartOffset    int64   `json:"log_start_offset"`
	CompactedSegments int     `json:"compacted_segments,omitempty"`
	RecordsRemoved    int     `json:"records_removed,omitempty"`
	TombstonesRemoved int     `json:"tombstones_removed,omitempty"`
	Rolled            bool    `json:"rolled,omitempty"`
	ExpiredProducers  int     `json:"expired_producers,omitempty"`
	Error             string  `json:"error,omitempty"`
}

// RunMaintenance applies retention and compaction to every local replica.
func (b *Broker) RunMaintenance(compact bool) []MaintenanceReport {
	b.maintMu.Lock()
	defer b.maintMu.Unlock()
	var out []MaintenanceReport
	now := b.cfg.Now()
	for k, p := range b.partitions() {
		res, err := p.RunMaintenance(now, compact)
		r := MaintenanceReport{Partition: k.String(), DeletedSegments: res.Retention.DeletedSegments,
			DeletedBytes: res.Retention.DeletedBytes, LogStartOffset: p.LogStartOffset(),
			CompactedSegments: res.Compaction.SegmentsRewritten, RecordsRemoved: res.Compaction.RecordsRemoved,
			TombstonesRemoved: res.Compaction.TombstonesRemoved, Rolled: res.Rolled, ExpiredProducers: res.ExpiredProducers}
		if err != nil {
			r.Error = err.Error()
			b.log.Error("maintenance failed", "partition", k.String(), "error", err)
		}
		if len(r.DeletedSegments) > 0 {
			b.m.retentionBytes.With(k.topic).Add(float64(r.DeletedBytes))
			b.log.Info("retention deleted segments", "partition", k.String(), "segments", len(r.DeletedSegments),
				"bytes", r.DeletedBytes, "new_log_start", r.LogStartOffset)
		}
		if r.CompactedSegments > 0 {
			b.m.compactionRemoved.With(k.topic).Add(float64(r.RecordsRemoved))
			b.log.Info("compaction rewrote segments", "partition", k.String(), "segments", r.CompactedSegments,
				"records_removed", r.RecordsRemoved, "tombstones_removed", r.TombstonesRemoved)
		}
		out = append(out, r)
	}
	return out
}

// --- helpers ---------------------------------------------------------------

func containsID(s []int32, v int32) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// byteBudget bounds the bytes of produce requests held in memory at once.
// When it is exhausted, new requests wait briefly and are then rejected
// with BROKER_OVERLOADED: the producer slows down instead of the broker
// running out of memory. A single request larger than the budget is
// admitted only when nothing else is in flight.
type byteBudget struct {
	mu      sync.Mutex
	used    int64
	max     int64
	changed chan struct{}
}

func newByteBudget(max int64) *byteBudget {
	return &byteBudget{max: max, changed: make(chan struct{})}
}

func (b *byteBudget) acquire(ctx context.Context, n int64) bool {
	for {
		b.mu.Lock()
		if b.used+n <= b.max || b.used == 0 {
			b.used += n
			b.mu.Unlock()
			return true
		}
		ch := b.changed
		b.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return false
		}
	}
}

func (b *byteBudget) release(n int64) {
	b.mu.Lock()
	b.used -= n
	close(b.changed)
	b.changed = make(chan struct{})
	b.mu.Unlock()
}

func (b *byteBudget) inUse() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}
