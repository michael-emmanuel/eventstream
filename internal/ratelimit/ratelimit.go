// Package ratelimit implements token-bucket rate limiting at several scopes
// (broker, tenant, topic, producer). It is the token-bucket limiter from the
// earlier portfolio project, extended with hierarchical scopes and
// all-or-nothing admission.
//
// Why the broker needs its own limits even if clients are well behaved:
// client-side limits are advisory (a buggy or malicious client ignores
// them), they do not compose (a thousand compliant clients can still
// overwhelm a broker), and only the broker sees the aggregate. Broker
// limits are the only ones the system can rely on.
package ratelimit

import (
	"sync"
	"time"
)

// Bucket is a token bucket. Tokens accrue at Rate per second up to Burst.
// It is not safe for concurrent use on its own; Limiter serializes access.
type Bucket struct {
	Rate   float64
	Burst  float64
	tokens float64
	last   time.Time
}

// NewBucket returns a full bucket.
func NewBucket(rate, burst float64, now time.Time) *Bucket {
	if burst < 1 {
		burst = 1
	}
	return &Bucket{Rate: rate, Burst: burst, tokens: burst, last: now}
}

func (b *Bucket) refill(now time.Time) {
	if now.After(b.last) {
		b.tokens += now.Sub(b.last).Seconds() * b.Rate
		if b.tokens > b.Burst {
			b.tokens = b.Burst
		}
		b.last = now
	}
}

// wait returns how long until n tokens are available (0 = now). A request
// larger than the burst is admitted when the bucket is full and drives the
// balance negative, so oversized requests are slowed down rather than
// rejected forever.
func (b *Bucket) wait(n float64, now time.Time) time.Duration {
	b.refill(now)
	need := n
	if need > b.Burst {
		need = b.Burst
	}
	if b.tokens >= need {
		return 0
	}
	return time.Duration((need - b.tokens) / b.Rate * float64(time.Second))
}

func (b *Bucket) take(n float64) { b.tokens -= n }

// Tokens returns the current balance (for tests and metrics).
func (b *Bucket) Tokens(now time.Time) float64 {
	b.refill(now)
	return b.tokens
}

// Limits configures byte rates per scope. Zero means unlimited.
type Limits struct {
	BrokerBytesPerSec   float64 `json:"broker_bytes_per_sec"`
	TenantBytesPerSec   float64 `json:"tenant_bytes_per_sec"`
	TopicBytesPerSec    float64 `json:"topic_bytes_per_sec"`
	ProducerBytesPerSec float64 `json:"producer_bytes_per_sec"`
	// BurstSeconds sets the bucket size as a multiple of the rate.
	BurstSeconds float64 `json:"burst_seconds"`
}

// Scope names returned when a request is throttled.
const (
	ScopeBroker   = "broker"
	ScopeTenant   = "tenant"
	ScopeTopic    = "topic"
	ScopeProducer = "producer"
)

// Decision is the outcome of Allow.
type Decision struct {
	Allowed    bool
	RetryAfter time.Duration
	Scope      string // which limit rejected the request
}

// Limiter applies all configured scopes to a request.
type Limiter struct {
	now func() time.Time

	mu        sync.Mutex
	limits    Limits
	broker    *Bucket
	tenants   map[string]*Bucket
	topics    map[string]*Bucket
	producers map[int64]*Bucket
}

// New creates a limiter.
func New(l Limits, now func() time.Time) *Limiter {
	if now == nil {
		now = time.Now
	}
	lim := &Limiter{now: now}
	lim.SetLimits(l)
	return lim
}

// SetLimits replaces the configuration and resets all buckets.
func (l *Limiter) SetLimits(lim Limits) {
	if lim.BurstSeconds <= 0 {
		lim.BurstSeconds = 1
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.limits = lim
	l.broker = nil
	if lim.BrokerBytesPerSec > 0 {
		l.broker = NewBucket(lim.BrokerBytesPerSec, lim.BrokerBytesPerSec*lim.BurstSeconds, l.now())
	}
	l.tenants = map[string]*Bucket{}
	l.topics = map[string]*Bucket{}
	l.producers = map[int64]*Bucket{}
}

// Limits returns the current configuration.
func (l *Limiter) Limits() Limits {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limits
}

// Allow admits a request of n bytes only if every applicable scope has
// capacity, and only then consumes tokens from all of them. Checking all
// scopes before taking from any avoids charging a tenant for a request
// that the broker-wide limit then rejects.
func (l *Limiter) Allow(tenant, topic string, producer int64, n int) Decision {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	type scoped struct {
		b     *Bucket
		scope string
	}
	var bs [4]scoped
	k := 0
	if l.broker != nil {
		bs[k] = scoped{l.broker, ScopeBroker}
		k++
	}
	if r := l.limits.TenantBytesPerSec; r > 0 && tenant != "" {
		bs[k] = scoped{bucketFor(l.tenants, tenant, r, l.limits.BurstSeconds, now), ScopeTenant}
		k++
	}
	if r := l.limits.TopicBytesPerSec; r > 0 && topic != "" {
		bs[k] = scoped{bucketFor(l.topics, topic, r, l.limits.BurstSeconds, now), ScopeTopic}
		k++
	}
	if r := l.limits.ProducerBytesPerSec; r > 0 && producer > 0 {
		bs[k] = scoped{bucketFor(l.producers, producer, r, l.limits.BurstSeconds, now), ScopeProducer}
		k++
	}
	for _, s := range bs[:k] {
		if w := s.b.wait(float64(n), now); w > 0 {
			return Decision{Allowed: false, RetryAfter: w, Scope: s.scope}
		}
	}
	for _, s := range bs[:k] {
		s.b.take(float64(n))
	}
	return Decision{Allowed: true}
}

func bucketFor[K comparable](m map[K]*Bucket, k K, rate, burstSec float64, now time.Time) *Bucket {
	b, ok := m[k]
	if !ok {
		b = NewBucket(rate, rate*burstSec, now)
		m[k] = b
	}
	return b
}

// Cleanup drops buckets that have been idle long enough to refill, which
// bounds memory when producer IDs churn. A full bucket carries no state, so
// recreating it later is equivalent.
func (l *Limiter) Cleanup() int {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	n := cleanup(l.tenants, now) + cleanup(l.topics, now) + cleanup(l.producers, now)
	return n
}

func cleanup[K comparable](m map[K]*Bucket, now time.Time) int {
	n := 0
	for k, b := range m {
		if b.Tokens(now) >= b.Burst {
			delete(m, k)
			n++
		}
	}
	return n
}
