// Package observability provides metrics in Prometheus text format,
// structured logging helpers, and lightweight W3C trace-context tracing.
//
// It is implemented on the standard library only. The metric model follows
// Prometheus conventions (counters, gauges, histograms with labels) so the
// /metrics endpoint can be scraped by a real Prometheus server; it is not a
// full client library (no summaries, no exemplars, no native histograms).
package observability

import (
	"fmt"
	"io"
	"math"
	"runtime"
	"runtime/metrics"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Registry holds metric families and collectors.
type Registry struct {
	mu         sync.Mutex
	families   map[string]*family
	order      []string
	collectors []func(e *Emitter)
	start      time.Time
}

// NewRegistry creates a registry that also exports Go runtime metrics.
func NewRegistry() *Registry {
	r := &Registry{families: map[string]*family{}, start: time.Now()}
	r.RegisterCollector(runtimeCollector(r.start))
	return r
}

type kind int

const (
	kindCounter kind = iota
	kindGauge
	kindHistogram
)

type family struct {
	name, help string
	kind       kind
	labels     []string
	buckets    []float64

	mu     sync.RWMutex
	series map[string]any // label-values key -> *Counter|*Gauge|*Histogram
	keys   map[string][]string
}

func (r *Registry) family(name, help string, k kind, labels []string, buckets []float64) *family {
	r.mu.Lock()
	defer r.mu.Unlock()
	if f, ok := r.families[name]; ok {
		return f
	}
	f := &family{name: name, help: help, kind: k, labels: labels, buckets: buckets,
		series: map[string]any{}, keys: map[string][]string{}}
	r.families[name] = f
	r.order = append(r.order, name)
	return f
}

func (f *family) get(values []string, mk func() any) any {
	if len(values) != len(f.labels) {
		panic(fmt.Sprintf("metric %s: got %d label values, want %d", f.name, len(values), len(f.labels)))
	}
	key := strings.Join(values, "\xff")
	f.mu.RLock()
	s, ok := f.series[key]
	f.mu.RUnlock()
	if ok {
		return s
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.series[key]; ok {
		return s
	}
	s = mk()
	f.series[key] = s
	f.keys[key] = append([]string(nil), values...)
	return s
}

// Counter is a monotonically increasing value.
type Counter struct{ bits atomic.Uint64 }

// Add adds v (must be >= 0).
func (c *Counter) Add(v float64) {
	for {
		old := c.bits.Load()
		if c.bits.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
			return
		}
	}
}

// Inc adds 1.
func (c *Counter) Inc() { c.Add(1) }

// Value returns the current value.
func (c *Counter) Value() float64 { return math.Float64frombits(c.bits.Load()) }

// Gauge is a value that goes up and down.
type Gauge struct{ bits atomic.Uint64 }

// Set sets the value.
func (g *Gauge) Set(v float64) { g.bits.Store(math.Float64bits(v)) }

// Add adds v.
func (g *Gauge) Add(v float64) {
	for {
		old := g.bits.Load()
		if g.bits.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
			return
		}
	}
}

// Value returns the current value.
func (g *Gauge) Value() float64 { return math.Float64frombits(g.bits.Load()) }

// Histogram counts observations into cumulative buckets.
type Histogram struct {
	buckets []float64
	counts  []atomic.Uint64
	sum     Counter
	count   atomic.Uint64
}

// Observe records v.
func (h *Histogram) Observe(v float64) {
	i := sort.SearchFloat64s(h.buckets, v)
	if i < len(h.counts) {
		h.counts[i].Add(1)
	}
	h.sum.Add(v)
	h.count.Add(1)
}

// ObserveSince records the seconds elapsed since t.
func (h *Histogram) ObserveSince(t time.Time) { h.Observe(time.Since(t).Seconds()) }

// Count returns the number of observations.
func (h *Histogram) Count() uint64 { return h.count.Load() }

// CounterVec is a labeled counter family.
type CounterVec struct{ f *family }

// GaugeVec is a labeled gauge family.
type GaugeVec struct{ f *family }

// HistogramVec is a labeled histogram family.
type HistogramVec struct{ f *family }

// Counter registers (or returns) a counter family.
func (r *Registry) Counter(name, help string, labels ...string) *CounterVec {
	return &CounterVec{r.family(name, help, kindCounter, labels, nil)}
}

// Gauge registers (or returns) a gauge family.
func (r *Registry) Gauge(name, help string, labels ...string) *GaugeVec {
	return &GaugeVec{r.family(name, help, kindGauge, labels, nil)}
}

// Histogram registers (or returns) a histogram family.
func (r *Registry) Histogram(name, help string, buckets []float64, labels ...string) *HistogramVec {
	if buckets == nil {
		buckets = LatencyBuckets
	}
	return &HistogramVec{r.family(name, help, kindHistogram, labels, buckets)}
}

// With returns the series for the label values.
func (v *CounterVec) With(values ...string) *Counter {
	return v.f.get(values, func() any { return &Counter{} }).(*Counter)
}

// With returns the series for the label values.
func (v *GaugeVec) With(values ...string) *Gauge {
	return v.f.get(values, func() any { return &Gauge{} }).(*Gauge)
}

// Delete removes a series (e.g. when a partition moves away).
func (v *GaugeVec) Delete(values ...string) {
	key := strings.Join(values, "\xff")
	v.f.mu.Lock()
	delete(v.f.series, key)
	delete(v.f.keys, key)
	v.f.mu.Unlock()
}

// With returns the series for the label values.
func (v *HistogramVec) With(values ...string) *Histogram {
	return v.f.get(values, func() any {
		return &Histogram{buckets: v.f.buckets, counts: make([]atomic.Uint64, len(v.f.buckets))}
	}).(*Histogram)
}

// LatencyBuckets are default latency buckets in seconds (100µs .. 10s).
var LatencyBuckets = []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// SizeBuckets are byte-size buckets (64B .. 16MiB).
var SizeBuckets = []float64{64, 256, 1024, 4096, 16384, 65536, 262144, 1 << 20, 4 << 20, 16 << 20}

// CountBuckets are batch-size buckets.
var CountBuckets = []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000}

// Emitter receives samples from collectors at scrape time.
type Emitter struct {
	w     io.Writer
	typed map[string]bool
}

// Gauge emits a gauge sample.
func (e *Emitter) Gauge(name, help string, value float64, labels ...string) {
	e.sample(name, help, "gauge", value, labels...)
}

// Counter emits a counter sample.
func (e *Emitter) Counter(name, help string, value float64, labels ...string) {
	e.sample(name, help, "counter", value, labels...)
}

func (e *Emitter) sample(name, help, typ string, value float64, labels ...string) {
	if !e.typed[name] {
		fmt.Fprintf(e.w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
		e.typed[name] = true
	}
	fmt.Fprintf(e.w, "%s%s %s\n", name, labelPairs(labels), formatFloat(value))
}

// RegisterCollector adds a function called on every scrape. Collectors are
// used for values that are cheaper to compute on demand than to maintain
// (partition offsets, lag, runtime stats).
func (r *Registry) RegisterCollector(fn func(e *Emitter)) {
	r.mu.Lock()
	r.collectors = append(r.collectors, fn)
	r.mu.Unlock()
}

// WritePrometheus writes all metrics in Prometheus text exposition format.
func (r *Registry) WritePrometheus(w io.Writer) {
	r.mu.Lock()
	names := append([]string(nil), r.order...)
	fams := make([]*family, len(names))
	for i, n := range names {
		fams[i] = r.families[n]
	}
	collectors := append([]func(*Emitter){}, r.collectors...)
	r.mu.Unlock()

	for _, f := range fams {
		typ := map[kind]string{kindCounter: "counter", kindGauge: "gauge", kindHistogram: "histogram"}[f.kind]
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, typ)
		f.mu.RLock()
		keys := make([]string, 0, len(f.series))
		for k := range f.series {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			pairs := pairsFor(f.labels, f.keys[k])
			switch s := f.series[k].(type) {
			case *Counter:
				fmt.Fprintf(w, "%s%s %s\n", f.name, labelPairs(pairs), formatFloat(s.Value()))
			case *Gauge:
				fmt.Fprintf(w, "%s%s %s\n", f.name, labelPairs(pairs), formatFloat(s.Value()))
			case *Histogram:
				var cum uint64
				for i, b := range s.buckets {
					cum += s.counts[i].Load()
					fmt.Fprintf(w, "%s_bucket%s %d\n", f.name, labelPairs(append(pairs, "le", formatFloat(b))), cum)
				}
				fmt.Fprintf(w, "%s_bucket%s %d\n", f.name, labelPairs(append(pairs, "le", "+Inf")), s.count.Load())
				fmt.Fprintf(w, "%s_sum%s %s\n", f.name, labelPairs(pairs), formatFloat(s.sum.Value()))
				fmt.Fprintf(w, "%s_count%s %d\n", f.name, labelPairs(pairs), s.count.Load())
			}
		}
		f.mu.RUnlock()
	}
	e := &Emitter{w: w, typed: map[string]bool{}}
	for _, c := range collectors {
		c(e)
	}
}

func pairsFor(names, values []string) []string {
	out := make([]string, 0, 2*len(names))
	for i := range names {
		out = append(out, names[i], values[i])
	}
	return out
}

func labelPairs(kv []string) string {
	if len(kv) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s=%q", kv[i], kv[i+1])
	}
	b.WriteByte('}')
	return b.String()
}

func formatFloat(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	if v == math.Trunc(v) && math.Abs(v) < 1e15 {
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%g", v)
}

// runtimeCollector exports Go runtime health: goroutines, heap, GC and CPU.
func runtimeCollector(start time.Time) func(e *Emitter) {
	samples := []metrics.Sample{
		{Name: "/gc/cycles/total:gc-cycles"},
	}
	var mu sync.Mutex
	return func(e *Emitter) {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		e.Gauge("go_goroutines", "Number of goroutines.", float64(runtime.NumGoroutine()))
		e.Gauge("go_memstats_heap_alloc_bytes", "Bytes of allocated heap objects.", float64(ms.HeapAlloc))
		e.Gauge("go_memstats_heap_inuse_bytes", "Bytes in in-use heap spans.", float64(ms.HeapInuse))
		e.Gauge("go_memstats_sys_bytes", "Bytes obtained from the OS.", float64(ms.Sys))
		e.Counter("go_gc_pause_seconds_total", "Total GC stop-the-world pause time.", float64(ms.PauseTotalNs)/1e9)
		mu.Lock()
		metrics.Read(samples)
		mu.Unlock()
		e.Counter("process_cpu_seconds_total", "User+system CPU seconds (getrusage).", ProcessCPUSeconds())
		if samples[0].Value.Kind() == metrics.KindUint64 {
			e.Counter("go_gc_cycles_total", "Completed GC cycles.", float64(samples[0].Value.Uint64()))
		}
		e.Gauge("process_uptime_seconds", "Seconds since start.", time.Since(start).Seconds())
	}
}

// Quantile estimates a quantile from a set of samples (used by the load
// generator, which keeps raw samples so its percentiles are exact).
func Quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(q*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
