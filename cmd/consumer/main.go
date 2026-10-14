// Command consumer is a demo consumer-group member. It prints its partition
// assignment on every rebalance, processes records with an optional
// artificial delay (to create lag), and commits offsets using a chosen
// strategy so the delivery-semantics tradeoffs can be observed:
//
//	-commit after   process, then commit   (at-least-once; duplicates on crash)
//	-commit before  commit, then process   (at-most-once; loss on crash)
//	-commit auto    periodic commit of what earlier polls returned
//
// -crash-after N exits abruptly (no commit, no leave) after N records, to
// demonstrate redelivery and session-timeout-driven rebalancing.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/observability"
	"github.com/michael-emmanuel/eventstream/internal/protocol"
	"github.com/michael-emmanuel/eventstream/pkg/client"
)

func main() {
	var (
		brokers  = flag.String("brokers", defaultBrokers(), "bootstrap brokers (env STREAM_BROKERS)")
		group    = flag.String("group", "fraud-detector", "consumer group")
		topics   = flag.String("topic", "orders", "comma-separated topics")
		name     = flag.String("name", "", "client ID (default hostname-pid)")
		delay    = flag.Duration("process-delay", 0, "artificial processing time per record (slow consumer)")
		commit   = flag.String("commit", "after", "after|before|auto")
		crash    = flag.Int("crash-after", 0, "exit abruptly after N records (0 = never)")
		assignor = flag.String("assignor", "range", "range|roundrobin|sticky")
		reset    = flag.String("reset", "earliest", "earliest|latest when no committed offset")
		quiet    = flag.Bool("quiet", false, "do not print every record")
		session  = flag.Duration("session-timeout", 10*time.Second, "group session timeout")
		token    = flag.String("token", "", "bearer token")
		trace    = flag.Bool("trace", false, "emit consumer spans linked to producer traces")
	)
	flag.Parse()
	if *name == "" {
		h, _ := os.Hostname()
		*name = fmt.Sprintf("%s-%d", h, os.Getpid())
	}
	logger := observability.NewLogger("consumer", slog.LevelInfo, os.Stderr).With("client", *name)
	cl, err := client.New(client.Options{Brokers: strings.Split(*brokers, ","), Token: *token})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var tracer *observability.Tracer
	if *trace {
		tracer = observability.NewTracer(logger, 1)
	}
	cs, err := cl.NewConsumer(client.ConsumerConfig{
		Group: *group, Topics: strings.Split(*topics, ","), ClientID: *name, Assignor: *assignor,
		AutoOffsetReset: *reset, AutoCommit: *commit == "auto", SessionTimeout: *session, Logger: logger,
		OnAssign: func(ps []protocol.TopicPartition) { fmt.Printf("[%s] ASSIGNED %v\n", *name, ps) },
		OnRevoke: func(ps []protocol.TopicPartition) { fmt.Printf("[%s] REVOKED  %v\n", *name, ps) },
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	processed := 0
	for ctx.Err() == nil {
		recs, err := cs.Poll(ctx, time.Second)
		if err != nil {
			if ctx.Err() == nil {
				logger.Warn("poll", "error", err)
				time.Sleep(500 * time.Millisecond)
			}
			continue
		}
		if len(recs) == 0 {
			continue
		}
		if *commit == "before" {
			// At-most-once: if we crash during processing, these records
			// are never seen again.
			if err := cs.CommitRecords(ctx, recs); err != nil {
				logger.Warn("commit", "error", err)
			}
		}
		for _, r := range recs {
			var span *observability.Span
			if tracer != nil {
				// A link, not a parent: the consumer's work is causally
				// related to the producer's trace but happens later, maybe
				// many times (replay), in a different process.
				_, span = tracer.Start(ctx, "consumer.process", observability.SpanContext{})
				if sc, ok := observability.ParseTraceparent(r.Headers[observability.TraceparentHeader]); ok {
					span.AddLink(sc)
				}
				span.SetAttr("partition", fmt.Sprintf("%s-%d", r.Topic, r.Partition))
				span.SetAttr("offset", r.Offset)
				span.SetAttr("queue_time_ms", time.Since(r.Timestamp).Milliseconds())
			}
			if *delay > 0 {
				time.Sleep(*delay)
			}
			if span != nil {
				span.End(nil)
			}
			processed++
			if !*quiet {
				fmt.Printf("[%s] %s-%d@%d key=%s value=%s\n", *name, r.Topic, r.Partition, r.Offset, r.Key, r.Value)
			}
			if *crash > 0 && processed >= *crash {
				fmt.Printf("[%s] CRASHING after %d records (no commit, no leave)\n", *name, processed)
				os.Exit(3)
			}
		}
		if *commit == "after" {
			// At-least-once: a crash before this line means the batch is
			// redelivered to whoever owns the partition next.
			if err := cs.CommitRecords(ctx, recs); err != nil {
				logger.Warn("commit", "error", err)
			}
		}
	}
	cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs.Close(cctx)
	st := cs.Stats()
	fmt.Fprintf(os.Stderr, "[%s] processed=%d rebalances=%d commits=%d\n", *name, processed, st.Rebalances, st.Commits)
}

func defaultBrokers() string {
	if v := os.Getenv("STREAM_BROKERS"); v != "" {
		return v
	}
	return "http://localhost:9091,http://localhost:9092,http://localhost:9093"
}
