package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"
)

// TraceparentHeader is the W3C trace context header (and record header key).
const TraceparentHeader = "traceparent"

// SpanContext identifies a span (W3C trace context, version 00).
type SpanContext struct {
	TraceID [16]byte
	SpanID  [8]byte
	Sampled bool
}

// Valid reports whether the context has a trace ID.
func (sc SpanContext) Valid() bool { return sc.TraceID != [16]byte{} }

// String formats the context as a traceparent value.
func (sc SpanContext) String() string {
	flags := "00"
	if sc.Sampled {
		flags = "01"
	}
	return fmt.Sprintf("00-%s-%s-%s", hex.EncodeToString(sc.TraceID[:]), hex.EncodeToString(sc.SpanID[:]), flags)
}

// TraceIDString returns the trace ID in hex.
func (sc SpanContext) TraceIDString() string { return hex.EncodeToString(sc.TraceID[:]) }

// ParseTraceparent parses a traceparent header value.
func ParseTraceparent(s string) (SpanContext, bool) {
	parts := strings.Split(strings.TrimSpace(s), "-")
	if len(parts) != 4 || parts[0] != "00" || len(parts[1]) != 32 || len(parts[2]) != 16 {
		return SpanContext{}, false
	}
	var sc SpanContext
	if _, err := hex.Decode(sc.TraceID[:], []byte(parts[1])); err != nil {
		return SpanContext{}, false
	}
	if _, err := hex.Decode(sc.SpanID[:], []byte(parts[2])); err != nil {
		return SpanContext{}, false
	}
	sc.Sampled = parts[3] == "01"
	return sc, sc.Valid()
}

// Span is an in-flight operation. Spans are exported as structured log
// lines (msg="span"), which keeps the project dependency-free while
// preserving the data model of OpenTelemetry: trace ID, span ID, parent,
// links, attributes and duration. Swapping in an OTLP exporter would only
// touch this file.
type Span struct {
	tracer *Tracer
	name   string
	ctx    SpanContext
	parent SpanContext
	links  []SpanContext
	start  time.Time
	attrs  []any
}

// Context returns the span's context for propagation.
func (s *Span) Context() SpanContext { return s.ctx }

// SetAttr adds an attribute.
func (s *Span) SetAttr(k string, v any) { s.attrs = append(s.attrs, k, v) }

// AddLink links this span to another trace. Consumers use links rather than
// parent/child: one consumer batch may contain records from many producer
// traces, and a record may be processed many times (replay), so a strict
// tree does not describe asynchronous messaging.
func (s *Span) AddLink(sc SpanContext) {
	if sc.Valid() {
		s.links = append(s.links, sc)
	}
}

// End finishes the span and exports it.
func (s *Span) End(err error) {
	if s.tracer == nil || !s.ctx.Sampled {
		return
	}
	args := []any{
		"trace_id", s.ctx.TraceIDString(),
		"span_id", hex.EncodeToString(s.ctx.SpanID[:]),
		"name", s.name,
		"duration_ms", float64(time.Since(s.start).Microseconds()) / 1000,
	}
	if s.parent.Valid() {
		args = append(args, "parent_span_id", hex.EncodeToString(s.parent.SpanID[:]))
	}
	for i, l := range s.links {
		args = append(args, fmt.Sprintf("link_%d", i), l.String())
	}
	if err != nil {
		args = append(args, "error", err.Error())
	}
	args = append(args, s.attrs...)
	s.tracer.logger.Info("span", args...)
}

// Tracer creates spans.
type Tracer struct {
	logger *slog.Logger
	// SampleRate is the fraction of new root traces that are sampled.
	SampleRate float64
}

// NewTracer creates a tracer exporting to logger. A nil logger disables export.
func NewTracer(logger *slog.Logger, sampleRate float64) *Tracer {
	return &Tracer{logger: logger, SampleRate: sampleRate}
}

type spanKey struct{}

// Start begins a span. If parent is valid the span joins its trace;
// otherwise a new trace is started (sampled at SampleRate).
func (t *Tracer) Start(ctx context.Context, name string, parent SpanContext) (context.Context, *Span) {
	s := &Span{tracer: t, name: name, parent: parent, start: time.Now()}
	if parent.Valid() {
		s.ctx.TraceID = parent.TraceID
		s.ctx.Sampled = parent.Sampled
	} else {
		_, _ = rand.Read(s.ctx.TraceID[:])
		var b [1]byte
		_, _ = rand.Read(b[:])
		s.ctx.Sampled = t != nil && t.logger != nil && float64(b[0])/256 < t.SampleRate
	}
	_, _ = rand.Read(s.ctx.SpanID[:])
	if t == nil || t.logger == nil {
		s.ctx.Sampled = false
	}
	return context.WithValue(ctx, spanKey{}, s), s
}

// SpanFrom returns the span in ctx, if any.
func SpanFrom(ctx context.Context) (*Span, bool) {
	s, ok := ctx.Value(spanKey{}).(*Span)
	return s, ok
}

// NewLogger builds a JSON slog logger with a component attribute.
func NewLogger(component string, level slog.Level, w io.Writer) *slog.Logger {
	if w == nil {
		w = os.Stderr
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(h).With("component", component)
}

// DiscardLogger returns a logger that drops everything.
func DiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ParseLevel converts a level name to a slog level.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	}
	return slog.LevelInfo
}
