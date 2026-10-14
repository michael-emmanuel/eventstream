// Package tests contains multi-broker integration and failure tests. Each
// test starts a real controller and real brokers in-process, talking HTTP
// over loopback, with timeouts shortened so failovers take about a second.
package tests

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/broker"
	"github.com/michael-emmanuel/eventstream/internal/controller"
	"github.com/michael-emmanuel/eventstream/internal/coordinator"
	"github.com/michael-emmanuel/eventstream/internal/metadata"
	"github.com/michael-emmanuel/eventstream/internal/observability"
	"github.com/michael-emmanuel/eventstream/pkg/client"
)

const (
	sessionTimeout = 1200 * time.Millisecond
	leaseDuration  = 700 * time.Millisecond
)

type cluster struct {
	t       *testing.T
	dir     string
	ctrl    *controller.Controller
	srv     *http.Server
	ctrlURL string

	mu      sync.Mutex
	brokers map[int32]*broker.Broker
	logger  *slog.Logger
}

func testLogger() *slog.Logger {
	if os.Getenv("CLUSTER_LOGS") != "" {
		return observability.NewLogger("test", slog.LevelInfo, os.Stderr)
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	c := &cluster{t: t, dir: t.TempDir(), brokers: map[int32]*broker.Broker{}, logger: testLogger()}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c.ctrlURL = "http://" + ln.Addr().String()
	c.ctrl, err = controller.New(controller.Config{
		DataDir: filepath.Join(c.dir, "controller"), AdvertiseURL: c.ctrlURL,
		SessionTimeout: sessionTimeout, LeaseDuration: leaseDuration,
		Coordinator: coordinator.Config{DefaultSessionTimeout: 2 * time.Second, InitialRebalanceDelay: 50 * time.Millisecond,
			DefaultRebalanceTimeout: 5 * time.Second, TickInterval: 20 * time.Millisecond},
		Logger: c.logger.With("component", "controller"),
	})
	if err != nil {
		t.Fatal(err)
	}
	c.ctrl.Start()
	c.srv = &http.Server{Handler: c.ctrl.Handler()}
	go c.srv.Serve(ln)
	for id := int32(1); id <= int32(n); id++ {
		c.startBroker(id)
	}
	c.waitFor("brokers registered", 5*time.Second, func() bool {
		md := c.ctrl.Metadata(context.Background(), 0, 0)
		alive := 0
		for _, b := range md.Brokers {
			if b.Alive {
				alive++
			}
		}
		return alive == n
	})
	t.Cleanup(c.close)
	return c
}

func (c *cluster) startBroker(id int32) *broker.Broker {
	c.t.Helper()
	b, err := broker.New(broker.Config{
		ID: id, ListenAddr: "127.0.0.1:0", DataDir: filepath.Join(c.dir, fmt.Sprintf("broker-%d", id)),
		ControllerURL: c.ctrlURL, HeartbeatInterval: 100 * time.Millisecond, MetadataWait: 300 * time.Millisecond,
		ISRInterval: 100 * time.Millisecond, FlushInterval: 50 * time.Millisecond, CheckpointInterval: 200 * time.Millisecond,
		MaintenanceInterval: time.Hour, ReplicaFetchWait: 200 * time.Millisecond, DefaultProduceTimeout: 3 * time.Second,
		Logger: c.logger.With("component", "broker"),
		Exit:   func(int) { c.t.Error("unexpected injected crash") },
	})
	if err != nil {
		c.t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	c.brokers[id] = b
	c.mu.Unlock()
	return b
}

func (c *cluster) broker(id int32) *broker.Broker {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.brokers[id]
}

// kill simulates a broker crash (no clean shutdown).
func (c *cluster) kill(id int32) {
	c.mu.Lock()
	b := c.brokers[id]
	delete(c.brokers, id)
	c.mu.Unlock()
	if b != nil {
		b.Kill()
	}
}

func (c *cluster) close() {
	c.mu.Lock()
	bs := c.brokers
	c.brokers = map[int32]*broker.Broker{}
	c.mu.Unlock()
	for _, b := range bs {
		b.Stop()
	}
	c.srv.Close()
	c.ctrl.Stop()
}

func (c *cluster) brokerURLs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, b := range c.brokers {
		out = append(out, b.URL())
	}
	sort.Strings(out)
	return out
}

func (c *cluster) client() *client.Client {
	c.t.Helper()
	cl, err := client.New(client.Options{Brokers: c.brokerURLs(), ControllerURL: c.ctrlURL, MetadataTTL: 200 * time.Millisecond})
	if err != nil {
		c.t.Fatal(err)
	}
	return cl
}

func (c *cluster) waitFor(what string, timeout time.Duration, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (c *cluster) partition(topic string, p int32) metadata.PartitionState {
	ps, _ := c.ctrl.Metadata(context.Background(), 0, 0).Partition(topic, p)
	return ps
}

// waitLeadersReady waits until every partition's leader has applied
// leadership and its ISR is full.
func (c *cluster) waitTopicReady(cl *client.Client, topic string) {
	c.t.Helper()
	c.waitFor("topic "+topic+" ready", 10*time.Second, func() bool {
		md := c.ctrl.Metadata(context.Background(), 0, 0)
		t := md.Topic(topic)
		if t == nil {
			return false
		}
		for _, ps := range t.Partitions {
			if ps.Leader < 0 {
				return false
			}
			if _, err := cl.Offsets(context.Background(), topic, ps.Partition, -1); err != nil {
				cl.InvalidateMetadata()
				return false
			}
		}
		return true
	})
}

func ctx(t *testing.T, d time.Duration) context.Context {
	c, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return c
}
