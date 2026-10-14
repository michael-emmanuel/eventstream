// Package fault provides runtime-configurable fault injection for brokers.
//
// Faults are set through the broker admin API (POST /v1/admin/faults) or
// streamctl, so failure scenarios can be demonstrated against a running
// cluster rather than only in unit tests. Every fault is off by default and
// the injector costs one atomic load per check when nothing is enabled.
package fault

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Config is the set of active faults. Durations are milliseconds and rates
// are probabilities in [0, 1] so the struct round-trips through JSON.
type Config struct {
	// Storage.
	DiskWriteDelayMs   int     `json:"disk_write_delay_ms,omitempty"`   // slow disk
	DiskWriteErrorRate float64 `json:"disk_write_error_rate,omitempty"` // failing disk
	DiskSyncErrorRate  float64 `json:"disk_sync_error_rate,omitempty"`

	// Replication (applied on the follower's fetch path).
	ReplicaFetchDelayMs  int     `json:"replica_fetch_delay_ms,omitempty"`  // lagging replica
	ReplicaFetchDropRate float64 `json:"replica_fetch_drop_rate,omitempty"` // packet loss / timeouts

	// Network.
	IsolateFromController bool    `json:"isolate_from_controller,omitempty"` // heartbeats and metadata fail
	IsolateFromBrokers    bool    `json:"isolate_from_brokers,omitempty"`    // replication in and out fails
	RequestDelayMs        int     `json:"request_delay_ms,omitempty"`        // network latency on client API
	DropProduceResponse   float64 `json:"drop_produce_response,omitempty"`   // append, then kill the connection

	// Process.
	CrashOnNextProduce bool `json:"crash_on_next_produce,omitempty"` // os.Exit after append, before response
}

// ErrInjected is returned by injected failures.
var ErrInjected = errors.New("injected fault")

// Injector holds the active configuration.
type Injector struct {
	cfg     atomic.Pointer[Config]
	mu      sync.Mutex
	rnd     *rand.Rand
	counter atomic.Int64
}

// New returns an injector with no faults.
func New() *Injector {
	in := &Injector{rnd: rand.New(rand.NewSource(time.Now().UnixNano()))}
	in.cfg.Store(&Config{})
	return in
}

// Set replaces the configuration.
func (in *Injector) Set(c Config) { in.cfg.Store(&c) }

// Get returns the configuration.
func (in *Injector) Get() Config { return *in.cfg.Load() }

// Injected returns how many faults fired.
func (in *Injector) Injected() int64 { return in.counter.Load() }

func (in *Injector) roll(p float64) bool {
	if p <= 0 {
		return false
	}
	in.mu.Lock()
	v := in.rnd.Float64()
	in.mu.Unlock()
	if v < p {
		in.counter.Add(1)
		return true
	}
	return false
}

func sleepMs(ms int) {
	if ms > 0 {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// BeforeDiskWrite is installed as the log's write hook.
func (in *Injector) BeforeDiskWrite() error {
	c := in.cfg.Load()
	sleepMs(c.DiskWriteDelayMs)
	if in.roll(c.DiskWriteErrorRate) {
		return errors.Join(ErrInjected, errors.New("disk write failed"))
	}
	return nil
}

// BeforeDiskSync is installed as the log's fsync hook.
func (in *Injector) BeforeDiskSync() error {
	if in.roll(in.cfg.Load().DiskSyncErrorRate) {
		return errors.Join(ErrInjected, errors.New("fsync failed"))
	}
	return nil
}

// BeforeReplicaFetch delays or fails a follower fetch.
func (in *Injector) BeforeReplicaFetch() error {
	c := in.cfg.Load()
	if c.IsolateFromBrokers {
		in.counter.Add(1)
		return errors.Join(ErrInjected, errors.New("isolated from brokers"))
	}
	sleepMs(c.ReplicaFetchDelayMs)
	if in.roll(c.ReplicaFetchDropRate) {
		return errors.Join(ErrInjected, errors.New("replica fetch dropped"))
	}
	return nil
}

// RejectInboundReplication reports whether to refuse fetches from followers.
func (in *Injector) RejectInboundReplication() bool {
	return in.cfg.Load().IsolateFromBrokers
}

// ControllerUnreachable reports whether controller calls should fail.
func (in *Injector) ControllerUnreachable() bool {
	return in.cfg.Load().IsolateFromController
}

// DelayRequest applies network latency to client requests.
func (in *Injector) DelayRequest() { sleepMs(in.cfg.Load().RequestDelayMs) }

// DropProduceResponse reports whether to drop this produce response.
func (in *Injector) DropProduceResponse() bool {
	return in.roll(in.cfg.Load().DropProduceResponse)
}

// CrashOnProduce reports (once) whether to crash after the next append.
func (in *Injector) CrashOnProduce() bool {
	for {
		c := in.cfg.Load()
		if !c.CrashOnNextProduce {
			return false
		}
		n := *c
		n.CrashOnNextProduce = false
		if in.cfg.CompareAndSwap(c, &n) {
			return true
		}
	}
}
