// Command gateway is an optional stateless HTTP front door. It accepts
// topic-level requests (POST /v1/topics/{topic}/events with no partition),
// chooses the partition from the key, and forwards to the partition leader
// using the same idempotent producer as native clients.
//
// The gateway exists for clients that cannot run partition-aware logic
// (curl, browsers, other languages). It costs an extra network hop and
// re-batches records; latency-sensitive Go clients should use pkg/client
// directly. Because the gateway is stateless, a generic L4/L7 load balancer
// in front of several gateways is appropriate: that is where round-robin
// load balancing still belongs.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/observability"
	"github.com/michael-emmanuel/eventstream/internal/protocol"
	"github.com/michael-emmanuel/eventstream/internal/transport"
	"github.com/michael-emmanuel/eventstream/pkg/client"
)

func main() {
	var (
		addr    = flag.String("addr", ":8080", "listen address")
		brokers = flag.String("brokers", defaultBrokers(), "comma-separated bootstrap broker URLs (env STREAM_BROKERS)")
		token   = flag.String("token", "", "bearer token for brokers")
		acks    = flag.String("acks", "all", "default acks")
		linger  = flag.Duration("linger", 2*time.Millisecond, "batching linger")
		level   = flag.String("log-level", "info", "log level")
	)
	flag.Parse()
	logger := observability.NewLogger("gateway", observability.ParseLevel(*level), os.Stderr)
	cl, err := client.New(client.Options{Brokers: strings.Split(*brokers, ","), Token: *token})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	producers := map[string]*client.Producer{}
	for _, a := range []string{"all", "leader", "quorum"} {
		producers[a] = cl.NewProducer(client.ProducerConfig{Acks: a, Linger: *linger})
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("POST /v1/topics/{topic}/events", func(w http.ResponseWriter, r *http.Request) {
		topic := r.PathValue("topic")
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 8<<20))
		if err != nil {
			protocol.WriteError(w, protocol.Errorf(413, protocol.CodeInvalidRequest, "%v", err))
			return
		}
		// Accept either {"records":[...]} or a single record object.
		var req protocol.GatewayProduceRequest
		if bytes.Contains(body, []byte(`"records"`)) {
			err = json.Unmarshal(body, &req)
		} else {
			var one protocol.Record
			err = json.Unmarshal(body, &one)
			req.Records = []protocol.Record{one}
		}
		if err != nil || len(req.Records) == 0 {
			protocol.WriteError(w, protocol.Errorf(400, protocol.CodeInvalidRequest, "invalid body: %v", err))
			return
		}
		a := req.Acks
		if a == "" {
			a = r.URL.Query().Get("acks")
		}
		if a == "" {
			a = *acks
		}
		pr := producers[a]
		if pr == nil {
			protocol.WriteError(w, protocol.Errorf(400, protocol.CodeInvalidRequest, "acks must be all|leader|quorum"))
			return
		}
		futs := make([]*client.Future, len(req.Records))
		resp := protocol.GatewayProduceResponse{Results: make([]protocol.GatewayRecordResult, len(req.Records))}
		for i, rec := range req.Records {
			m := client.Message{Topic: topic, Headers: rec.Headers, Partition: rec.Partition}
			if rec.Key != nil {
				m.Key = []byte(*rec.Key)
			}
			if rec.Value != nil {
				m.Value = []byte(*rec.Value)
			}
			if tp := r.Header.Get(observability.TraceparentHeader); tp != "" {
				if m.Headers == nil {
					m.Headers = map[string]string{}
				}
				m.Headers[observability.TraceparentHeader] = tp
			}
			f, err := pr.Send(r.Context(), m)
			if err != nil {
				var pe *protocol.Error
				if errors.Is(err, client.ErrBufferFull) {
					protocol.WriteError(w, &protocol.Error{Status: 503, Code: protocol.CodeOverloaded, Message: err.Error(), RetryAfterMs: 500})
					return
				}
				if errors.As(err, &pe) {
					pe.Status = 404
					protocol.WriteError(w, pe)
					return
				}
				protocol.WriteError(w, protocol.Errorf(500, protocol.CodeInternal, "%v", err))
				return
			}
			futs[i] = f
		}
		status := 200
		for i, f := range futs {
			res := f.Wait(r.Context())
			resp.Results[i] = protocol.GatewayRecordResult{Topic: topic, Partition: res.Partition, Offset: res.Offset}
			if res.Err != nil {
				resp.Results[i].Error = res.Err.Error()
				status = 207
			}
		}
		protocol.WriteJSON(w, status, resp)
	})
	// Reads are partition-scoped (offsets are partition-local); the gateway
	// forwards them to the leader.
	mux.HandleFunc("GET /v1/topics/{topic}/partitions/{partition}/events", func(w http.ResponseWriter, r *http.Request) {
		p, err := strconv.Atoi(r.PathValue("partition"))
		if err != nil {
			protocol.WriteError(w, protocol.Errorf(400, protocol.CodeInvalidRequest, "bad partition"))
			return
		}
		q := r.URL.Query()
		off, _ := strconv.ParseInt(q.Get("offset"), 10, 64)
		maxEv, _ := strconv.Atoi(q.Get("max_events"))
		maxB, _ := strconv.Atoi(q.Get("max_bytes"))
		waitMs, _ := strconv.Atoi(q.Get("wait_ms"))
		resp, err := cl.Fetch(r.Context(), r.PathValue("topic"), int32(p), off,
			client.FetchOptions{MaxEvents: maxEv, MaxBytes: maxB, Wait: time.Duration(waitMs) * time.Millisecond})
		if err != nil {
			var pe *protocol.Error
			if errors.As(err, &pe) {
				protocol.WriteError(w, pe)
				return
			}
			protocol.WriteError(w, protocol.Errorf(502, protocol.CodeUnavailable, "%v", err))
			return
		}
		protocol.WriteJSON(w, 200, resp)
	})
	srv := &http.Server{Addr: *addr, Handler: transport.Middleware(mux, 4096, logger, nil), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}()
	logger.Info("gateway listening", "addr", *addr, "brokers", *brokers)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	for _, p := range producers {
		p.Close(ctx)
	}
}

func defaultBrokers() string {
	if v := os.Getenv("STREAM_BROKERS"); v != "" {
		return v
	}
	return "http://localhost:9091,http://localhost:9092,http://localhost:9093"
}
