// Command broker runs a broker.
package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/auth"
	"github.com/michael-emmanuel/eventstream/internal/broker"
	"github.com/michael-emmanuel/eventstream/internal/observability"
	"github.com/michael-emmanuel/eventstream/internal/ratelimit"
	"github.com/michael-emmanuel/eventstream/internal/transport"
)

func main() {
	var (
		id          = flag.Int("id", 1, "broker ID (unique, stable across restarts)")
		addr        = flag.String("addr", ":9091", "listen address")
		advertise   = flag.String("advertise", "", "URL other brokers and clients use (default http://localhost<addr>)")
		dataDir     = flag.String("data", "", "data directory (default ./data/broker-<id>)")
		ctrl        = flag.String("controller", "http://localhost:9000", "controller URL")
		fsync       = flag.String("fsync", "interval", "always (fsync every append) | interval")
		flushEvery  = flag.Duration("flush-interval", 200*time.Millisecond, "fsync interval when -fsync=interval")
		hb          = flag.Duration("heartbeat-interval", time.Second, "controller heartbeat interval")
		maint       = flag.Duration("maintenance-interval", 30*time.Second, "retention/compaction interval")
		verify      = flag.Bool("verify-on-recovery", false, "fully verify sealed segments at startup")
		maxInflight = flag.Int64("max-inflight-produce-bytes", 64<<20, "produce memory budget (backpressure)")
		maxConc     = flag.Int("max-concurrent-requests", 2000, "concurrent request limit (load shedding)")
		maxReq      = flag.Int64("max-request-bytes", 8<<20, "maximum produce request body")
		brokerRate  = flag.Float64("broker-bytes-per-sec", 0, "broker-wide produce rate limit (0 = off)")
		tenantRate  = flag.Float64("tenant-bytes-per-sec", 0, "per-tenant produce rate limit")
		topicRate   = flag.Float64("topic-bytes-per-sec", 0, "per-topic produce rate limit")
		prodRate    = flag.Float64("producer-bytes-per-sec", 0, "per-producer produce rate limit")
		sample      = flag.Float64("trace-sample-rate", 0, "fraction of produce requests traced (0-1)")
		authFile    = flag.String("auth-config", "", "JSON auth config (empty = auth disabled)")
		token       = flag.String("token", "", "bearer token for inter-broker and controller calls")
		caFile      = flag.String("ca", "", "CA certificate for TLS to other brokers/controller")
		tlsCert     = flag.String("tls-cert", "", "TLS certificate (enables HTTPS)")
		tlsKey      = flag.String("tls-key", "", "TLS key")
		level       = flag.String("log-level", "info", "debug|info|warn|error")
		pprofAddr   = flag.String("pprof-addr", "", "serve net/http/pprof on this address (e.g. localhost:6061); off by default")
	)
	flag.Parse()
	logger := observability.NewLogger("broker", observability.ParseLevel(*level), os.Stderr)
	if *dataDir == "" {
		*dataDir = fmt.Sprintf("./data/broker-%d", *id)
	}
	if *advertise == "" {
		*advertise = "http://localhost" + *addr
	}
	ac, err := auth.Load(*authFile)
	if err != nil {
		fatal(err)
	}
	hc, err := transport.NewHTTPClient(transport.ClientOptions{CAFile: *caFile, Token: *token})
	if err != nil {
		fatal(err)
	}
	b, err := broker.New(broker.Config{
		ID: int32(*id), ListenAddr: *addr, AdvertiseURL: *advertise, DataDir: *dataDir, ControllerURL: *ctrl,
		HeartbeatInterval: *hb, FsyncEveryAppend: *fsync == "always", FlushInterval: *flushEvery,
		MaintenanceInterval: *maint, VerifyOnRecovery: *verify,
		MaxInflightProduceBytes: *maxInflight, MaxConcurrentRequests: *maxConc, MaxRequestBytes: *maxReq,
		Limits: ratelimit.Limits{BrokerBytesPerSec: *brokerRate, TenantBytesPerSec: *tenantRate,
			TopicBytesPerSec: *topicRate, ProducerBytesPerSec: *prodRate},
		Auth: ac, HTTPClient: hc, TLSCert: *tlsCert, TLSKey: *tlsKey,
		TraceSampleRate: *sample, Logger: logger,
	})
	if err != nil {
		fatal(err)
	}
	if err := b.Start(); err != nil {
		fatal(err)
	}
	if *pprofAddr != "" {
		// Profiling is served on a separate, normally loopback-only listener
		// so it is never exposed through the client API port.
		go func() { logger.Info("pprof listening", "addr", *pprofAddr); _ = http.ListenAndServe(*pprofAddr, nil) }()
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	if err := b.Stop(); err != nil {
		logger.Error("shutdown error", "error", err)
		os.Exit(1)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "broker:", err)
	os.Exit(1)
}
