// Package client is the Go client for the event streaming platform:
// partition-aware routing with a metadata cache, an idempotent batching
// producer, a consumer-group consumer, and admin helpers.
//
// Routing: the client sends every request directly to the broker that
// leads the target partition, found in cached metadata. It never sprays
// requests across brokers; a broker that is not the leader answers
// NOT_LEADER (with a hint), and the client refreshes metadata and retries.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/metadata"
	"github.com/michael-emmanuel/eventstream/internal/protocol"
	"github.com/michael-emmanuel/eventstream/internal/transport"
)

// Options configure a Client.
type Options struct {
	// Brokers are bootstrap URLs; any reachable broker serves metadata.
	Brokers []string
	// ControllerURL overrides the controller address advertised in metadata
	// (admin and consumer-group calls go to the controller).
	ControllerURL string
	Token         string
	CAFile        string
	MetadataTTL   time.Duration
	HTTPClient    *http.Client
}

// Client is safe for concurrent use.
type Client struct {
	opts  Options
	http  *http.Client
	cache *metadata.Cache

	mu   sync.Mutex
	next int
}

// New creates a client.
func New(opts Options) (*Client, error) {
	if len(opts.Brokers) == 0 && opts.ControllerURL == "" {
		return nil, errors.New("client: at least one broker or a controller URL is required")
	}
	if opts.MetadataTTL == 0 {
		opts.MetadataTTL = 10 * time.Second
	}
	hc := opts.HTTPClient
	if hc == nil {
		var err error
		hc, err = transport.NewHTTPClient(transport.ClientOptions{CAFile: opts.CAFile, Token: opts.Token})
		if err != nil {
			return nil, err
		}
	}
	c := &Client{opts: opts, http: hc}
	c.cache = metadata.NewCache(opts.MetadataTTL, c.fetchMetadata)
	return c, nil
}

// fetchMetadata asks known brokers in turn: every broker learned from the
// last metadata snapshot, then the bootstrap list, then the controller. The
// bootstrap list is only needed to find the cluster once; relying on it
// afterwards would make a single dead bootstrap broker blind the client to
// every subsequent leader change.
func (c *Client) fetchMetadata(ctx context.Context) (*metadata.Cluster, error) {
	var urls []string
	seen := map[string]bool{}
	add := func(u string) {
		if u != "" && !seen[u] {
			seen[u] = true
			urls = append(urls, u)
		}
	}
	if c.cache != nil {
		if cur := c.cache.Peek(); cur != nil {
			for _, b := range cur.Brokers {
				if b.Alive {
					add(b.URL)
				}
			}
		}
	}
	for _, u := range c.opts.Brokers {
		add(u)
	}
	add(c.opts.ControllerURL)
	c.mu.Lock()
	start := c.next
	c.next++
	c.mu.Unlock()
	var last error
	for i := range urls {
		u := urls[(start+i)%len(urls)]
		var md metadata.Cluster
		rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := transport.DoJSON(rctx, c.http, "GET", u+"/v1/metadata", nil, &md)
		cancel()
		if err == nil {
			return &md, nil
		}
		last = err
	}
	return nil, fmt.Errorf("metadata unavailable from %d endpoints: %w", len(urls), last)
}

// Metadata returns cached cluster metadata.
func (c *Client) Metadata(ctx context.Context) (*metadata.Cluster, error) { return c.cache.Get(ctx) }

// InvalidateMetadata forces a refresh on next use.
func (c *Client) InvalidateMetadata() { c.cache.Invalidate() }

// Partitions returns a topic's partition count.
func (c *Client) Partitions(ctx context.Context, topic string) (int, error) {
	md, err := c.Metadata(ctx)
	if err != nil {
		return 0, err
	}
	t := md.Topic(topic)
	if t == nil {
		c.InvalidateMetadata()
		md, err = c.Metadata(ctx)
		if err != nil {
			return 0, err
		}
		if t = md.Topic(topic); t == nil {
			return 0, &protocol.Error{Code: protocol.CodeUnknownTopic, Message: "unknown topic " + topic}
		}
	}
	return len(t.Partitions), nil
}

func (c *Client) leaderURL(ctx context.Context, topic string, p int32) (string, error) {
	md, err := c.Metadata(ctx)
	if err != nil {
		return "", err
	}
	u, _, ok := md.LeaderURL(topic, p)
	if !ok {
		c.InvalidateMetadata()
		return "", &protocol.Error{Code: protocol.CodeNotLeader, Retriable: true,
			Message: fmt.Sprintf("no known leader for %s-%d", topic, p)}
	}
	return u, nil
}

func (c *Client) controllerURL(ctx context.Context) (string, error) {
	if c.opts.ControllerURL != "" {
		return c.opts.ControllerURL, nil
	}
	md, err := c.Metadata(ctx)
	if err != nil {
		return "", err
	}
	if md.ControllerURL == "" {
		return "", errors.New("metadata has no controller URL")
	}
	return md.ControllerURL, nil
}

// leaderCall runs fn against the partition leader, refreshing metadata and
// retrying once if the broker says it is not the leader.
func (c *Client) leaderCall(ctx context.Context, topic string, p int32, fn func(url string) error) error {
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		var u string
		u, err = c.leaderURL(ctx, topic, p)
		if err != nil {
			return err
		}
		err = fn(u)
		var pe *protocol.Error
		if errors.As(err, &pe) && (pe.Code == protocol.CodeNotLeader || pe.Code == protocol.CodeUnknownTopic) {
			c.InvalidateMetadata()
			continue
		}
		return err
	}
	return err
}

// Produce sends one batch to one partition (no batching, no retries beyond
// a leader redirect). The Producer type builds on this.
func (c *Client) Produce(ctx context.Context, topic string, p int32, req protocol.ProduceRequest) (protocol.ProduceResponse, error) {
	var out protocol.ProduceResponse
	err := c.leaderCall(ctx, topic, p, func(u string) error {
		return transport.DoJSON(ctx, c.http, "POST",
			fmt.Sprintf("%s/v1/topics/%s/partitions/%d/events", u, url.PathEscape(topic), p), req, &out)
	})
	return out, err
}

// FetchOptions control a fetch.
type FetchOptions struct {
	MaxEvents int
	MaxBytes  int
	Wait      time.Duration // long-poll wait when no data is available
}

// Fetch reads committed records from a partition starting at offset.
func (c *Client) Fetch(ctx context.Context, topic string, p int32, offset int64, o FetchOptions) (protocol.FetchResponse, error) {
	q := url.Values{}
	q.Set("offset", strconv.FormatInt(offset, 10))
	if o.MaxEvents > 0 {
		q.Set("max_events", strconv.Itoa(o.MaxEvents))
	}
	if o.MaxBytes > 0 {
		q.Set("max_bytes", strconv.Itoa(o.MaxBytes))
	}
	if o.Wait > 0 {
		q.Set("wait_ms", strconv.FormatInt(o.Wait.Milliseconds(), 10))
	}
	var out protocol.FetchResponse
	err := c.leaderCall(ctx, topic, p, func(u string) error {
		return transport.DoJSON(ctx, c.http, "GET",
			fmt.Sprintf("%s/v1/topics/%s/partitions/%d/events?%s", u, url.PathEscape(topic), p, q.Encode()), nil, &out)
	})
	return out, err
}

// Offsets returns a partition's log start, end and high-water mark, and
// optionally the first offset at or after a timestamp (ts >= 0).
func (c *Client) Offsets(ctx context.Context, topic string, p int32, ts int64) (protocol.OffsetsResponse, error) {
	q := ""
	if ts >= 0 {
		q = "?timestamp=" + strconv.FormatInt(ts, 10)
	}
	var out protocol.OffsetsResponse
	err := c.leaderCall(ctx, topic, p, func(u string) error {
		return transport.DoJSON(ctx, c.http, "GET",
			fmt.Sprintf("%s/v1/topics/%s/partitions/%d/offsets%s", u, url.PathEscape(topic), p, q), nil, &out)
	})
	return out, err
}

// AllocateProducerID obtains a producer ID through any broker.
func (c *Client) AllocateProducerID(ctx context.Context) (int64, error) {
	urls := append([]string(nil), c.opts.Brokers...)
	if cu, err := c.controllerURL(ctx); err == nil {
		urls = append(urls, cu)
	}
	var last error
	for _, u := range urls {
		var out protocol.ProducerIDResponse
		if err := transport.DoJSON(ctx, c.http, "POST", u+"/v1/producer-ids", nil, &out); err != nil {
			last = err
			continue
		}
		return out.ProducerID, nil
	}
	return 0, fmt.Errorf("allocate producer id: %w", last)
}

// --- admin -----------------------------------------------------------------

func (c *Client) controllerDo(ctx context.Context, method, path string, in, out any) error {
	u, err := c.controllerURL(ctx)
	if err != nil {
		return err
	}
	return transport.DoJSON(ctx, c.http, method, u+path, in, out)
}

// CreateTopic creates a topic. cfg may be nil or a partial
// metadata.TopicConfig-shaped map.
func (c *Client) CreateTopic(ctx context.Context, name string, partitions, rf int, cfg map[string]any) (*metadata.Topic, error) {
	req := protocol.CreateTopicRequest{Name: name, Partitions: partitions, ReplicationFactor: rf}
	if cfg != nil {
		b, err := json.Marshal(cfg)
		if err != nil {
			return nil, err
		}
		req.Config = b
	}
	var t metadata.Topic
	if err := c.controllerDo(ctx, "POST", "/v1/topics", req, &t); err != nil {
		return nil, err
	}
	c.InvalidateMetadata()
	return &t, nil
}

// DeleteTopic deletes a topic.
func (c *Client) DeleteTopic(ctx context.Context, name string) error {
	defer c.InvalidateMetadata()
	return c.controllerDo(ctx, "DELETE", "/v1/topics/"+url.PathEscape(name), nil, nil)
}

// DescribeTopic returns a topic's metadata from the controller.
func (c *Client) DescribeTopic(ctx context.Context, name string) (*metadata.Topic, error) {
	var t metadata.Topic
	return &t, c.controllerDo(ctx, "GET", "/v1/topics/"+url.PathEscape(name), nil, &t)
}

// AlterTopicConfig patches topic configuration.
func (c *Client) AlterTopicConfig(ctx context.Context, name string, patch map[string]any) (*metadata.Topic, error) {
	var t metadata.Topic
	return &t, c.controllerDo(ctx, "PATCH", "/v1/topics/"+url.PathEscape(name)+"/config", patch, &t)
}

// AdminJSON performs an arbitrary admin call against the controller and
// returns the raw JSON (used by streamctl).
func (c *Client) AdminJSON(ctx context.Context, method, path string, in any) (json.RawMessage, error) {
	var out json.RawMessage
	err := c.controllerDo(ctx, method, path, in, &out)
	return out, err
}

// BrokerJSON performs a call against a specific broker URL.
func (c *Client) BrokerJSON(ctx context.Context, brokerURL, method, path string, in any) (json.RawMessage, error) {
	var out json.RawMessage
	err := transport.DoJSON(ctx, c.http, method, brokerURL+path, in, &out)
	return out, err
}

// Lag returns consumer lag for a group.
func (c *Client) Lag(ctx context.Context, group string) (protocol.GroupLagResponse, error) {
	var out protocol.GroupLagResponse
	return out, c.controllerDo(ctx, "GET", "/v1/groups/"+url.PathEscape(group)+"/lag", nil, &out)
}

// ResetOffsets rewinds/fast-forwards a stopped group (replay).
func (c *Client) ResetOffsets(ctx context.Context, group string, req protocol.ResetOffsetsRequest) (protocol.GroupOffsetsResponse, error) {
	var out protocol.GroupOffsetsResponse
	return out, c.controllerDo(ctx, "POST", "/v1/groups/"+url.PathEscape(group)+"/offsets/reset", req, &out)
}

// --- retry helpers ---------------------------------------------------------

// retryable classifies an error from a produce or fetch.
func retryable(err error) bool {
	if err == nil {
		return false
	}
	var pe *protocol.Error
	if errors.As(err, &pe) {
		return pe.Retriable || protocol.IsRetriable(pe.Code)
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	// Network errors (connection refused/reset, EOF, timeouts) are
	// ambiguous for writes: the request may or may not have been applied.
	// They are safe to retry only because the producer is idempotent.
	return true
}

func retryAfter(err error) time.Duration {
	var pe *protocol.Error
	if errors.As(err, &pe) && pe.RetryAfterMs > 0 {
		return time.Duration(pe.RetryAfterMs) * time.Millisecond
	}
	return 0
}

func errCode(err error) string {
	var pe *protocol.Error
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// backoff returns exponential backoff with full jitter.
func backoff(base time.Duration, attempt int, max time.Duration) time.Duration {
	d := base << uint(min(attempt, 10))
	if d > max {
		d = max
	}
	return time.Duration(rand.Int63n(int64(d)) + 1)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
