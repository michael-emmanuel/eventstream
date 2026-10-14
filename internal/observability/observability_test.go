package observability

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestPrometheusExposition(t *testing.T) {
	r := NewRegistry()
	r.Counter("requests_total", "Requests.", "route").With("produce").Add(3)
	r.Gauge("lag", "Lag.", "group").With("g1").Set(42)
	h := r.Histogram("latency_seconds", "Latency.", []float64{0.1, 1}, "op")
	h.With("x").Observe(0.05)
	h.With("x").Observe(0.5)
	h.With("x").Observe(5)
	var buf bytes.Buffer
	r.WritePrometheus(&buf)
	out := buf.String()
	for _, want := range []string{
		`requests_total{route="produce"} 3`,
		`lag{group="g1"} 42`,
		`latency_seconds_bucket{op="x",le="0.1"} 1`,
		`latency_seconds_bucket{op="x",le="1"} 2`,
		`latency_seconds_bucket{op="x",le="+Inf"} 3`,
		`latency_seconds_count{op="x"} 3`,
		"go_goroutines",
		"process_cpu_seconds_total",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q", want)
		}
	}
}

func TestTraceparentRoundTrip(t *testing.T) {
	tr := NewTracer(DiscardLogger(), 1)
	_, s := tr.Start(context.Background(), "root", SpanContext{})
	sc, ok := ParseTraceparent(s.Context().String())
	if !ok || sc.TraceID != s.Context().TraceID || !sc.Sampled {
		t.Fatalf("round trip failed: %s", s.Context())
	}
	_, child := tr.Start(context.Background(), "child", sc)
	if child.Context().TraceID != sc.TraceID || child.Context().SpanID == sc.SpanID {
		t.Fatal("child must share trace ID and have its own span ID")
	}
	if _, ok := ParseTraceparent("garbage"); ok {
		t.Fatal("parsed garbage")
	}
}

func TestQuantile(t *testing.T) {
	v := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if Quantile(v, 0.5) != 5 || Quantile(v, 0.99) != 10 || Quantile(v, 0) != 1 {
		t.Fatal("quantile")
	}
}
