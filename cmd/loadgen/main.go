// Command loadgen drives a configurable produce (and optionally consume)
// workload and reports measured throughput, latency percentiles, consumer
// lag and broker-side resource deltas.
//
// It reports what it measured on the machine it ran on; it does not
// extrapolate. Results depend heavily on hardware, fsync policy, batch size
// and whether brokers share a machine with the load generator. See
// docs/performance.md for how to run and interpret it.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/metadata"
	"github.com/michael-emmanuel/eventstream/internal/observability"
	"github.com/michael-emmanuel/eventstream/pkg/client"
)

type config struct {
	Brokers    string        `json:"brokers"`
	Topic      string        `json:"topic"`
	Partitions int           `json:"partitions"`
	RF         int           `json:"replication_factor"`
	Rate       float64       `json:"target_rate"`
	Duration   time.Duration `json:"duration"`
	Size       int           `json:"record_bytes"`
	Keys       int           `json:"keys"`
	HotShare   float64       `json:"hot_key_share"`
	Acks       string        `json:"acks"`
	Producers  int           `json:"producers"`
	Linger     time.Duration `json:"linger"`
	Batch      int           `json:"batch_max_records"`
	Consume    bool          `json:"consume"`
	Consumers  int           `json:"consumers"`
	ProcDelay  time.Duration `json:"consumer_process_delay"`
}

type report struct {
	Config        config             `json:"config"`
	StartedAt     time.Time          `json:"started_at"`
	ElapsedSec    float64            `json:"elapsed_sec"`
	Sent          int64              `json:"sent"`
	Acked         int64              `json:"acked"`
	Errors        int64              `json:"errors"`
	Retries       int64              `json:"retries"`
	BufferFull    int64              `json:"buffer_full_rejections"`
	ProduceRate   float64            `json:"produce_records_per_sec"`
	ProduceMBps   float64            `json:"produce_payload_mb_per_sec"`
	AckLatencyMs  map[string]float64 `json:"ack_latency_ms"`
	Consumed      int64              `json:"consumed"`
	ConsumeRate   float64            `json:"consume_records_per_sec"`
	E2ELatencyMs  map[string]float64 `json:"end_to_end_latency_ms,omitempty"`
	MaxLag        int64              `json:"max_consumer_lag,omitempty"`
	FinalLag      int64              `json:"final_consumer_lag,omitempty"`
	PartitionSkew map[string]int64   `json:"records_per_partition"`
	LoadgenCPUSec float64            `json:"loadgen_cpu_seconds"`
	LoadgenHeapMB float64            `json:"loadgen_heap_mb"`
	Broker        map[string]float64 `json:"broker_metric_deltas"`
}

// samples keeps up to cap latency samples using reservoir sampling, so
// memory stays bounded on long runs while percentiles stay unbiased.
type samples struct {
	mu   sync.Mutex
	vals []float64
	seen int64
	cap  int
}

func (s *samples) add(v float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen++
	if len(s.vals) < s.cap {
		s.vals = append(s.vals, v)
		return
	}
	if j := rand.Int63n(s.seen); j < int64(s.cap) {
		s.vals[j] = v
	}
}

func (s *samples) summary() map[string]float64 {
	s.mu.Lock()
	v := append([]float64(nil), s.vals...)
	s.mu.Unlock()
	if len(v) == 0 {
		return nil
	}
	sort.Float64s(v)
	sum := 0.0
	for _, x := range v {
		sum += x
	}
	return map[string]float64{
		"p50": round(observability.Quantile(v, 0.50)), "p95": round(observability.Quantile(v, 0.95)),
		"p99": round(observability.Quantile(v, 0.99)), "max": round(v[len(v)-1]), "mean": round(sum / float64(len(v))),
	}
}

func round(v float64) float64 { return float64(int64(v*1000)) / 1000 }

func main() {
	var c config
	flag.StringVar(&c.Brokers, "brokers", defaultBrokers(), "bootstrap brokers (env STREAM_BROKERS)")
	flag.StringVar(&c.Topic, "topic", "loadtest", "topic (created if missing)")
	flag.IntVar(&c.Partitions, "partitions", 6, "partitions when creating the topic")
	flag.IntVar(&c.RF, "rf", 3, "replication factor when creating the topic")
	flag.Float64Var(&c.Rate, "rate", 1000, "target records/sec (0 = as fast as possible)")
	flag.DurationVar(&c.Duration, "duration", 30*time.Second, "test duration")
	flag.IntVar(&c.Size, "size", 1024, "record value bytes")
	flag.IntVar(&c.Keys, "keys", 10000, "distinct keys")
	flag.Float64Var(&c.HotShare, "hot-key-share", 0, "fraction of records using one hot key")
	flag.StringVar(&c.Acks, "acks", "all", "all|leader|quorum")
	flag.IntVar(&c.Producers, "producers", 4, "concurrent producer instances")
	flag.DurationVar(&c.Linger, "linger", 5*time.Millisecond, "producer linger")
	flag.IntVar(&c.Batch, "batch", 500, "producer max records per batch")
	flag.BoolVar(&c.Consume, "consume", true, "also consume and measure end-to-end latency and lag")
	flag.IntVar(&c.Consumers, "consumers", 2, "consumer instances")
	flag.DurationVar(&c.ProcDelay, "process-delay", 0, "artificial per-record consumer delay")
	jsonOut := flag.String("json", "", "write the report as JSON to this file")
	token := flag.String("token", "", "bearer token")
	flag.Parse()

	cl, err := client.New(client.Options{Brokers: strings.Split(c.Brokers, ","), Token: *token})
	must(err)
	ctx := context.Background()
	if _, err := cl.Partitions(ctx, c.Topic); err != nil {
		_, err := cl.CreateTopic(ctx, c.Topic, c.Partitions, c.RF, map[string]any{"retention_ms": 3600_000})
		must(err)
		time.Sleep(2 * time.Second) // let leaders settle
	}
	md, err := cl.Metadata(ctx)
	must(err)
	before := scrapeBrokers(md)

	rep := report{Config: c, StartedAt: time.Now(), PartitionSkew: map[string]int64{}}
	ack := &samples{cap: 1 << 20}
	e2e := &samples{cap: 1 << 20}
	var sent, acked, errs, bufFull, consumed atomic.Int64
	var skewMu sync.Mutex

	// Consumers start first, from the end of the log, in a fresh group.
	group := fmt.Sprintf("loadgen-%d", time.Now().UnixNano())
	var cwg sync.WaitGroup
	var maxLag atomic.Int64
	consumeCtx, stopConsume := context.WithCancel(ctx)
	if c.Consume {
		for i := 0; i < c.Consumers; i++ {
			cs, err := cl.NewConsumer(client.ConsumerConfig{Group: group, Topics: []string{c.Topic},
				ClientID: fmt.Sprintf("loadgen-c%d", i), AutoOffsetReset: "latest", FetchWait: 500 * time.Millisecond,
				FetchMaxEvents: 2000, SessionTimeout: 10 * time.Second, AutoCommit: true, AutoCommitInterval: time.Second})
			must(err)
			cwg.Add(1)
			go func() {
				defer cwg.Done()
				defer cs.Close(context.Background())
				for consumeCtx.Err() == nil {
					recs, err := cs.Poll(consumeCtx, 200*time.Millisecond)
					if err != nil {
						continue
					}
					now := time.Now().UnixNano()
					for _, r := range recs {
						if ts, err := strconv.ParseInt(r.Headers["lg_ts"], 10, 64); err == nil {
							e2e.add(float64(now-ts) / 1e6)
						}
						if c.ProcDelay > 0 {
							time.Sleep(c.ProcDelay)
						}
					}
					consumed.Add(int64(len(recs)))
				}
			}()
		}
		go func() {
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for {
				select {
				case <-consumeCtx.Done():
					return
				case <-t.C:
					if l, err := cl.Lag(ctx, group); err == nil && l.TotalLag > maxLag.Load() {
						maxLag.Store(l.TotalLag)
					}
				}
			}
		}()
		time.Sleep(2 * time.Second) // let the group form
	}

	runCtx, cancel := context.WithTimeout(ctx, c.Duration)
	defer cancel()
	payload := make([]byte, c.Size)
	for i := range payload {
		payload[i] = 'a' + byte(i%26)
	}
	value := string(payload)
	start := time.Now()
	var pwg sync.WaitGroup
	producers := make([]*client.Producer, c.Producers)
	for i := range producers {
		producers[i] = cl.NewProducer(client.ProducerConfig{Acks: c.Acks, Linger: c.Linger, BatchMaxRecords: c.Batch,
			MaxBufferedBytes: 64 << 20, MaxBlock: 200 * time.Millisecond})
	}
	futs := make(chan *client.Future, 100_000)
	var awg sync.WaitGroup
	for i := 0; i < 8; i++ {
		awg.Add(1)
		go func() {
			defer awg.Done()
			for f := range futs {
				r := f.Wait(context.Background())
				if r.Err != nil {
					errs.Add(1)
					continue
				}
				acked.Add(1)
				ack.add(float64(r.Latency.Microseconds()) / 1000)
				skewMu.Lock()
				rep.PartitionSkew[fmt.Sprintf("%s-%d", r.Topic, r.Partition)]++
				skewMu.Unlock()
			}
		}()
	}
	perProducer := c.Rate / float64(c.Producers)
	for i, pr := range producers {
		pwg.Add(1)
		go func(id int, pr *client.Producer) {
			defer pwg.Done()
			rnd := rand.New(rand.NewSource(int64(id)))
			pStart := time.Now()
			n := 0
			for runCtx.Err() == nil {
				if perProducer > 0 {
					due := int(time.Since(pStart).Seconds() * perProducer)
					if n >= due {
						time.Sleep(time.Millisecond)
						continue
					}
				}
				key := "k-" + strconv.Itoa(rnd.Intn(c.Keys))
				if c.HotShare > 0 && rnd.Float64() < c.HotShare {
					key = "hot"
				}
				f, err := pr.Send(runCtx, client.Message{Topic: c.Topic, Key: []byte(key), Value: []byte(value),
					Headers: map[string]string{"lg_ts": strconv.FormatInt(time.Now().UnixNano(), 10)}})
				if err != nil {
					if runCtx.Err() == nil {
						bufFull.Add(1)
					}
					continue
				}
				sent.Add(1)
				n++
				futs <- f
			}
		}(i, pr)
	}
	progress := time.NewTicker(5 * time.Second)
	go func() {
		var last int64
		for range progress.C {
			a := acked.Load()
			fmt.Fprintf(os.Stderr, "  t=%4.0fs acked=%d (+%d/5s) errors=%d consumed=%d max_lag=%d\n",
				time.Since(start).Seconds(), a, a-last, errs.Load(), consumed.Load(), maxLag.Load())
			last = a
		}
	}()
	pwg.Wait()
	progress.Stop()
	elapsed := time.Since(start)
	fctx, fcancel := context.WithTimeout(ctx, 30*time.Second)
	for _, pr := range producers {
		pr.Close(fctx)
		st := pr.Stats()
		rep.Retries += st.Retries
	}
	fcancel()
	close(futs)
	awg.Wait()
	if c.Consume {
		deadline := time.Now().Add(15 * time.Second)
		for consumed.Load() < acked.Load() && time.Now().Before(deadline) {
			time.Sleep(200 * time.Millisecond)
		}
		stopConsume()
		cwg.Wait() // consumers commit on close
		if l, err := cl.Lag(ctx, group); err == nil {
			rep.FinalLag = l.TotalLag
		}
	}
	after := scrapeBrokers(md)

	rep.ElapsedSec = round(elapsed.Seconds())
	rep.Sent, rep.Acked, rep.Errors, rep.BufferFull = sent.Load(), acked.Load(), errs.Load(), bufFull.Load()
	rep.ProduceRate = round(float64(rep.Acked) / elapsed.Seconds())
	rep.ProduceMBps = round(float64(rep.Acked) * float64(c.Size) / elapsed.Seconds() / 1e6)
	rep.AckLatencyMs = ack.summary()
	rep.Consumed = consumed.Load()
	rep.ConsumeRate = round(float64(rep.Consumed) / elapsed.Seconds())
	rep.E2ELatencyMs = e2e.summary()
	rep.MaxLag = maxLag.Load()
	rep.LoadgenCPUSec, rep.LoadgenHeapMB = selfUsage()
	rep.Broker = map[string]float64{}
	for k, v := range after {
		rep.Broker[k] = round(v - before[k])
	}
	if n := rep.Broker["broker_fsync_duration_seconds_count"]; n > 0 {
		rep.Broker["avg_fsync_ms"] = round(rep.Broker["broker_fsync_duration_seconds_sum"] / n * 1000)
	}
	printReport(rep)
	if *jsonOut != "" {
		b, _ := json.MarshalIndent(rep, "", "  ")
		must(os.WriteFile(*jsonOut, b, 0o644))
	}
}

func printReport(r report) {
	c := r.Config
	fmt.Printf("\n=== load test: %s, %d-byte records, acks=%s, target %.0f rec/s, %d producers ===\n",
		c.Topic, c.Size, c.Acks, c.Rate, c.Producers)
	fmt.Printf("elapsed            %.1fs\n", r.ElapsedSec)
	fmt.Printf("produced (acked)   %d records  %.0f rec/s  %.2f MB/s payload\n", r.Acked, r.ProduceRate, r.ProduceMBps)
	fmt.Printf("errors / retries   %d / %d   (buffer-full rejections: %d)\n", r.Errors, r.Retries, r.BufferFull)
	fmt.Printf("ack latency ms     p50=%v p95=%v p99=%v max=%v\n", r.AckLatencyMs["p50"], r.AckLatencyMs["p95"], r.AckLatencyMs["p99"], r.AckLatencyMs["max"])
	if c.Consume {
		fmt.Printf("consumed           %d records  %.0f rec/s\n", r.Consumed, r.ConsumeRate)
		fmt.Printf("end-to-end ms      p50=%v p95=%v p99=%v max=%v\n", r.E2ELatencyMs["p50"], r.E2ELatencyMs["p95"], r.E2ELatencyMs["p99"], r.E2ELatencyMs["max"])
		fmt.Printf("consumer lag       max=%d final=%d\n", r.MaxLag, r.FinalLag)
	}
	keys := make([]string, 0, len(r.PartitionSkew))
	for k := range r.PartitionSkew {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Printf("per partition     ")
	for _, k := range keys {
		fmt.Printf(" %s=%d", k, r.PartitionSkew[k])
	}
	fmt.Println()
	fmt.Printf("loadgen process    cpu=%.1fs (user+sys) heap=%.0fMB\n", r.LoadgenCPUSec, r.LoadgenHeapMB)
	fmt.Println("broker deltas (all brokers):")
	bk := make([]string, 0, len(r.Broker))
	for k := range r.Broker {
		bk = append(bk, k)
	}
	sort.Strings(bk)
	for _, k := range bk {
		fmt.Printf("  %-48s %v\n", k, r.Broker[k])
	}
}

func selfUsage() (cpu, heapMB float64) {
	cpu = round(observability.ProcessCPUSeconds())
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return cpu, round(float64(ms.HeapAlloc) / 1e6)
}

// scrapeBrokers sums selected counters across all brokers' /metrics.
func scrapeBrokers(md *metadata.Cluster) map[string]float64 {
	want := []string{"broker_produce_bytes_total", "broker_produce_records_total", "broker_network_in_bytes_total",
		"broker_network_out_bytes_total", "broker_replica_bytes_total", "broker_fetch_records_total",
		"broker_fsync_duration_seconds_sum", "broker_fsync_duration_seconds_count", "broker_throttled_requests_total",
		"broker_rejected_requests_total", "broker_produce_duplicates_total", "process_cpu_seconds_total"}
	out := map[string]float64{}
	hc := &http.Client{Timeout: 3 * time.Second}
	for _, b := range md.Brokers {
		resp, err := hc.Get(b.URL + "/metrics")
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "#") {
				continue
			}
			for _, w := range want {
				if strings.HasPrefix(line, w+"{") || strings.HasPrefix(line, w+" ") {
					f := strings.Fields(line)
					v, _ := strconv.ParseFloat(f[len(f)-1], 64)
					out[w] += v
				}
			}
		}
		resp.Body.Close()
	}
	return out
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func defaultBrokers() string {
	if v := os.Getenv("STREAM_BROKERS"); v != "" {
		return v
	}
	return "http://localhost:9091,http://localhost:9092,http://localhost:9093"
}
