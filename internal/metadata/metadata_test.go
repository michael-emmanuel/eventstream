package metadata

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheSingleFlightAndStaleOnError(t *testing.T) {
	var calls atomic.Int32
	fail := atomic.Bool{}
	c := NewCache(time.Hour, func(ctx context.Context) (*Cluster, error) {
		calls.Add(1)
		time.Sleep(10 * time.Millisecond)
		if fail.Load() {
			return nil, errors.New("down")
		}
		return &Cluster{Version: int64(calls.Load())}, nil
	})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.Get(context.Background()) }()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("thundering herd: %d fetches", calls.Load())
	}
	fail.Store(true)
	c.Invalidate()
	md, err := c.Get(context.Background())
	if err != nil || md == nil {
		t.Fatal("stale metadata should be served when refresh fails")
	}
}

func TestValidateConfig(t *testing.T) {
	c := DefaultTopicConfig()
	if err := c.Validate(3); err != nil {
		t.Fatal(err)
	}
	c.MinInsyncReplicas = 4
	if c.Validate(3) == nil {
		t.Fatal("min ISR above RF accepted")
	}
	c = DefaultTopicConfig()
	c.CleanupPolicy = "compact,delete"
	if c.Validate(1) != nil || !c.HasPolicy(CleanupCompact) || !c.HasPolicy(CleanupDelete) {
		t.Fatal("combined policy")
	}
	c.Compression = "zstd"
	if c.Validate(1) == nil {
		t.Fatal("unimplemented compression accepted")
	}
}
