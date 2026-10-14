// Command producer is a small demo producer: it emits keyed JSON events at
// a fixed rate, optionally with a skewed key distribution to create a hot
// partition, and prints where each record landed.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/observability"
	"github.com/michael-emmanuel/eventstream/pkg/client"
)

func main() {
	var (
		brokers = flag.String("brokers", defaultBrokers(), "bootstrap brokers (env STREAM_BROKERS)")
		topic   = flag.String("topic", "orders", "topic")
		rate    = flag.Float64("rate", 5, "events per second")
		count   = flag.Int("n", 0, "stop after N events (0 = forever)")
		keys    = flag.Int("keys", 20, "number of distinct keys (user IDs)")
		hot     = flag.Float64("hot-key-share", 0, "fraction of events using the single key user-0 (e.g. 0.7)")
		acks    = flag.String("acks", "all", "all|leader|quorum")
		quiet   = flag.Bool("quiet", false, "do not print every record")
		schema  = flag.String("schema-version", "1", "schema_version header")
		sample  = flag.Float64("trace-sample-rate", 0, "fraction of events traced")
		token   = flag.String("token", "", "bearer token")
		level   = flag.String("log-level", "warn", "client log level (debug shows every retry)")
	)
	flag.Parse()
	cl, err := client.New(client.Options{Brokers: strings.Split(*brokers, ","), Token: *token})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var tracer *observability.Tracer
	if *sample > 0 {
		tracer = observability.NewTracer(observability.NewLogger("producer", 0, os.Stderr), *sample)
	}
	pr := cl.NewProducer(client.ProducerConfig{Acks: *acks, Tracer: tracer,
		Logger: observability.NewLogger("producer", observability.ParseLevel(*level), os.Stderr)})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	tick := time.NewTicker(time.Duration(float64(time.Second) / *rate))
	defer tick.Stop()
	sent := 0
	for *count == 0 || sent < *count {
		select {
		case <-ctx.Done():
			goto done
		case <-tick.C:
		}
		user := fmt.Sprintf("user-%d", rand.Intn(*keys))
		if *hot > 0 && rand.Float64() < *hot {
			user = "user-0"
		}
		value := fmt.Sprintf(`{"order_id":%d,"user":"%s","amount":%d,"ts":"%s"}`,
			sent, user, rand.Intn(10000), time.Now().UTC().Format(time.RFC3339Nano))
		f, err := pr.Send(ctx, client.Message{Topic: *topic, Key: []byte(user), Value: []byte(value),
			Headers: map[string]string{"event_type": "order.created", "schema_version": *schema}})
		if err != nil {
			fmt.Fprintln(os.Stderr, "send:", err)
			continue
		}
		sent++
		go func(user string) {
			r := f.Wait(context.Background())
			if r.Err != nil {
				fmt.Fprintf(os.Stderr, "FAILED %s: %v\n", user, r.Err)
				return
			}
			if !*quiet {
				fmt.Printf("%-8s -> %s-%d @ offset %d (%.1fms)\n", user, r.Topic, r.Partition, r.Offset,
					float64(r.Latency.Microseconds())/1000)
			}
		}(user)
	}
done:
	fctx, cancel := context.WithTimeout(context.Background(), 40*time.Second) // > delivery timeout
	defer cancel()
	pr.Close(fctx)
	st := pr.Stats()
	fmt.Fprintf(os.Stderr, "sent=%d delivered=%d retries=%d errors=%d duplicates_suppressed=%d\n",
		sent, st.Records, st.Retries, st.Errors, st.Duplicates)
}

func defaultBrokers() string {
	if v := os.Getenv("STREAM_BROKERS"); v != "" {
		return v
	}
	return "http://localhost:9091,http://localhost:9092,http://localhost:9093"
}
