// Package transport holds shared HTTP plumbing: client construction with
// optional TLS, JSON request helpers, and server middleware.
package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/protocol"
)

// ClientOptions configures NewHTTPClient.
type ClientOptions struct {
	CAFile  string        // trust this CA for TLS (optional)
	Token   string        // bearer token sent on every request (optional)
	Timeout time.Duration // overall request timeout (0 = none; use contexts)
	// MaxConnsPerHost bounds connections to one broker so a burst of
	// requests queues on the client instead of opening unbounded sockets.
	MaxConnsPerHost int
}

type tokenTransport struct {
	base  http.RoundTripper
	token string
}

func (t *tokenTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if t.token != "" && r.Header.Get("Authorization") == "" {
		r = r.Clone(r.Context())
		r.Header.Set("Authorization", "Bearer "+t.token)
	}
	return t.base.RoundTrip(r)
}

// NewHTTPClient returns a client with keep-alive pooling and optional TLS.
func NewHTTPClient(o ClientOptions) (*http.Client, error) {
	if o.MaxConnsPerHost == 0 {
		o.MaxConnsPerHost = 64
	}
	tr := &http.Transport{
		Proxy:               nil,
		DialContext:         (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: o.MaxConnsPerHost,
		MaxConnsPerHost:     o.MaxConnsPerHost,
		IdleConnTimeout:     90 * time.Second,
	}
	if o.CAFile != "" {
		pem, err := os.ReadFile(o.CAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates in %s", o.CAFile)
		}
		tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	return &http.Client{Transport: &tokenTransport{base: tr, token: o.Token}, Timeout: o.Timeout}, nil
}

// DoJSON sends a JSON request and decodes a JSON response into out (if not
// nil). Non-2xx responses are returned as *protocol.Error.
func DoJSON(ctx context.Context, c *http.Client, method, url string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return protocol.DecodeError(resp.StatusCode, b)
	}
	if out != nil && len(b) > 0 {
		if err := json.Unmarshal(b, out); err != nil {
			return fmt.Errorf("decode %s %s: %w", method, url, err)
		}
	}
	return nil
}

// statusRecorder captures the response status for metrics and logs.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = 200
	}
	n, err := s.ResponseWriter.Write(b)
	s.bytes += n
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer
// (needed for Hijack in fault injection).
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// RequestObserver is notified after each request.
type RequestObserver func(route string, status int, dur time.Duration, bytesIn, bytesOut int64)

// Middleware wraps a handler with: a concurrency limit (load shedding with
// 503 instead of unbounded queuing), panic recovery, metrics and debug logs.
func Middleware(next http.Handler, maxConcurrent int, logger *slog.Logger, observe RequestObserver) http.Handler {
	var sem chan struct{}
	if maxConcurrent > 0 {
		sem = make(chan struct{}, maxConcurrent)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		if sem != nil {
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			default:
				protocol.WriteError(w, &protocol.Error{Status: http.StatusServiceUnavailable,
					Code: protocol.CodeOverloaded, Message: "too many concurrent requests", RetryAfterMs: 100})
				if observe != nil {
					observe("overloaded", http.StatusServiceUnavailable, time.Since(start), r.ContentLength, 0)
				}
				return
			}
		}
		rec := &statusRecorder{ResponseWriter: w}
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				logger.Error("panic in handler", "path", r.URL.Path, "panic", fmt.Sprint(v))
				if rec.status == 0 {
					protocol.WriteError(rec, protocol.Errorf(500, protocol.CodeInternal, "internal error"))
				}
			}
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			if observe != nil {
				observe(route, rec.status, time.Since(start), r.ContentLength, int64(rec.bytes))
			}
			logger.Debug("request", "method", r.Method, "route", route, "status", rec.status,
				"duration_ms", time.Since(start).Milliseconds())
		}()
		next.ServeHTTP(rec, r)
	})
}

// QueryInt parses an integer query parameter with a default.
func QueryInt(r *http.Request, name string, def int64) (int64, error) {
	s := r.URL.Query().Get(name)
	if s == "" {
		return def, nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %q", name, s)
	}
	return v, nil
}

// ReadJSON decodes a size-limited JSON body.
func ReadJSON(w http.ResponseWriter, r *http.Request, maxBytes int64, v any) *protocol.Error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return protocol.Errorf(http.StatusRequestEntityTooLarge, protocol.CodeInvalidRequest, "request body exceeds %d bytes", maxBytes)
		}
		return protocol.Errorf(http.StatusBadRequest, protocol.CodeInvalidRequest, "invalid JSON: %v", err)
	}
	return nil
}
