package ratelimit

import (
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func TestBucketRefillAndBurst(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	l := New(Limits{ProducerBytesPerSec: 1000, BurstSeconds: 1}, c.now)
	if d := l.Allow("", "", 1, 1000); !d.Allowed {
		t.Fatal("full bucket should admit a burst")
	}
	d := l.Allow("", "", 1, 500)
	if d.Allowed || d.Scope != ScopeProducer || d.RetryAfter != 500*time.Millisecond {
		t.Fatalf("decision %+v", d)
	}
	c.t = c.t.Add(500 * time.Millisecond)
	if d := l.Allow("", "", 1, 500); !d.Allowed {
		t.Fatalf("refill failed: %+v", d)
	}
	// Other producers have independent buckets.
	if d := l.Allow("", "", 2, 1000); !d.Allowed {
		t.Fatal("producer isolation broken")
	}
}

func TestAllOrNothingAcrossScopes(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	l := New(Limits{BrokerBytesPerSec: 10_000, TenantBytesPerSec: 1000}, c.now)
	if d := l.Allow("acme", "orders", 0, 1000); !d.Allowed {
		t.Fatal(d)
	}
	d := l.Allow("acme", "orders", 0, 1000)
	if d.Allowed || d.Scope != ScopeTenant {
		t.Fatalf("tenant limit not applied: %+v", d)
	}
	// The rejected request must not have consumed broker tokens: another
	// tenant can still use the remaining 9000 bytes.
	for i := 0; i < 9; i++ {
		if d := l.Allow("other-"+string(rune('a'+i)), "x", 0, 1000); !d.Allowed {
			t.Fatalf("broker tokens leaked on rejection at %d: %+v", i, d)
		}
	}
	if d := l.Allow("z", "x", 0, 1000); d.Allowed || d.Scope != ScopeBroker {
		t.Fatalf("broker limit: %+v", d)
	}
}

func TestOversizedRequestIsSlowedNotStarved(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	l := New(Limits{TopicBytesPerSec: 100}, c.now)
	if d := l.Allow("", "t", 0, 1000); !d.Allowed {
		t.Fatal("oversized request should pass on a full bucket")
	}
	d := l.Allow("", "t", 0, 10)
	if d.Allowed || d.RetryAfter < 9*time.Second {
		t.Fatalf("debt not enforced: %+v", d)
	}
}

func TestCleanupDropsIdleBuckets(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	l := New(Limits{ProducerBytesPerSec: 100}, c.now)
	for i := int64(1); i <= 50; i++ {
		l.Allow("", "", i, 50)
	}
	c.t = c.t.Add(time.Minute)
	if n := l.Cleanup(); n != 50 {
		t.Fatalf("cleaned %d", n)
	}
}
