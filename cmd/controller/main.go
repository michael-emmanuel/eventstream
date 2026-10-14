// Command controller runs the metadata controller and consumer-group
// coordinator.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/auth"
	"github.com/michael-emmanuel/eventstream/internal/controller"
	"github.com/michael-emmanuel/eventstream/internal/coordinator"
	"github.com/michael-emmanuel/eventstream/internal/observability"
	"github.com/michael-emmanuel/eventstream/internal/transport"
)

func main() {
	var (
		addr        = flag.String("addr", ":9000", "listen address")
		advertise   = flag.String("advertise", "", "URL brokers and clients use to reach this controller (default http://localhost<addr>)")
		dataDir     = flag.String("data", "./data/controller", "data directory")
		session     = flag.Duration("session-timeout", 10*time.Second, "broker session timeout")
		lease       = flag.Duration("lease", 6*time.Second, "broker write lease (must be < session timeout)")
		groupSess   = flag.Duration("group-session-timeout", 10*time.Second, "default consumer session timeout")
		rebDelay    = flag.Duration("rebalance-delay", 300*time.Millisecond, "initial rebalance delay (coalesces joins)")
		quota       = flag.Int("max-partitions-per-tenant", 0, "partition quota per tenant (0 = unlimited)")
		syncOffsets = flag.Bool("sync-offset-commits", true, "fsync every consumer offset commit")
		authFile    = flag.String("auth-config", "", "JSON auth config (empty = auth disabled)")
		token       = flag.String("token", "", "bearer token the controller uses when calling brokers")
		caFile      = flag.String("ca", "", "CA certificate for calling brokers over TLS")
		tlsCert     = flag.String("tls-cert", "", "TLS certificate (enables HTTPS)")
		tlsKey      = flag.String("tls-key", "", "TLS key")
		level       = flag.String("log-level", "info", "debug|info|warn|error")
	)
	flag.Parse()
	logger := observability.NewLogger("controller", observability.ParseLevel(*level), os.Stderr)
	if *advertise == "" {
		*advertise = "http://localhost" + *addr
	}
	ac, err := auth.Load(*authFile)
	if err != nil {
		fatal(err)
	}
	hc, err := transport.NewHTTPClient(transport.ClientOptions{CAFile: *caFile, Token: *token, Timeout: 10 * time.Second})
	if err != nil {
		fatal(err)
	}
	ctrl, err := controller.New(controller.Config{
		DataDir: *dataDir, AdvertiseURL: *advertise, SessionTimeout: *session, LeaseDuration: *lease,
		MaxPartitionsPerTenant: *quota, Auth: ac, HTTPClient: hc, Logger: logger,
		Coordinator: coordinator.Config{DefaultSessionTimeout: *groupSess, InitialRebalanceDelay: *rebDelay, SyncOffsetWrites: *syncOffsets},
	})
	if err != nil {
		fatal(err)
	}
	ctrl.Start()
	srv := &http.Server{Addr: *addr, Handler: ctrl.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		var err error
		if *tlsCert != "" {
			err = srv.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatal(err)
		}
	}()
	logger.Info("controller listening", "addr", *addr, "advertise", *advertise, "auth", ac.Enabled,
		"session_timeout", session.String(), "lease", lease.String())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	ctrl.Stop()
	logger.Info("controller stopped")
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "controller:", err)
	os.Exit(1)
}
