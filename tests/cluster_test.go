package tests

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/fault"
	"github.com/michael-emmanuel/eventstream/internal/protocol"
	"github.com/michael-emmanuel/eventstream/internal/ratelimit"
	"github.com/michael-emmanuel/eventstream/pkg/client"
)

func readAll(t *testing.T, cl *client.Client, topic string, p int32) []protocol.ConsumerRecord {
	t.Helper()
	var out []protocol.ConsumerRecord
	o, err := cl.Offsets(ctx(t, 5*time.Second), topic, p, -1)
	if err != nil {
		t.Fatal(err)
	}
	off := o.LogStartOffset
	for off < o.HighWatermark {
		r, err := cl.Fetch(ctx(t, 5*time.Second), topic, p, off, client.FetchOptions{MaxEvents: 1000})
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Records) == 0 {
			break
		}
		out = append(out, r.Records...)
		off = r.NextOffset
	}
	return out
}

func mustCreate(t *testing.T, cl *client.Client, name string, parts, rf int, cfg map[string]any) {
	t.Helper()
	if _, err := cl.CreateTopic(ctx(t, 5*time.Second), name, parts, rf, cfg); err != nil {
		t.Fatal(err)
	}
}

func produceN(t *testing.T, pr *client.Producer, topic string, n int, keyf func(i int) string) []client.Result {
	t.Helper()
	futs := make([]*client.Future, n)
	for i := 0; i < n; i++ {
		f, err := pr.Send(ctx(t, 10*time.Second), client.Message{Topic: topic, Key: []byte(keyf(i)),
			Value: []byte(fmt.Sprintf("v-%d", i)), Headers: map[string]string{"seq": strconv.Itoa(i)}})
		if err != nil {
			t.Fatal(err)
		}
		futs[i] = f
	}
	res := make([]client.Result, n)
	for i, f := range futs {
		res[i] = f.Wait(ctx(t, 30*time.Second))
		if res[i].Err != nil {
			t.Fatalf("record %d: %v", i, res[i].Err)
		}
	}
	return res
}

// Keyed records land on stable partitions, offsets are partition-local and
// contiguous, and records for one key are read back in production order.
func TestProduceConsumeOrderingPerKey(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client()
	mustCreate(t, cl, "orders", 4, 3, map[string]any{"min_insync_replicas": 2})
	c.waitTopicReady(cl, "orders")
	pr := cl.NewProducer(client.ProducerConfig{})
	defer pr.Close(context.Background())
	res := produceN(t, pr, "orders", 400, func(i int) string { return fmt.Sprintf("user-%d", i%10) })

	keyPartition := map[string]int32{}
	for i, r := range res {
		k := fmt.Sprintf("user-%d", i%10)
		if p, ok := keyPartition[k]; ok && p != r.Partition {
			t.Fatalf("key %s routed to %d and %d", k, p, r.Partition)
		}
		keyPartition[k] = r.Partition
	}
	total := 0
	for p := int32(0); p < 4; p++ {
		recs := readAll(t, cl, "orders", p)
		total += len(recs)
		lastSeq := map[string]int{}
		for i, r := range recs {
			if r.Offset != int64(i) {
				t.Fatalf("partition %d: offset %d at position %d (offsets must be contiguous and partition-local)", p, r.Offset, i)
			}
			seq, _ := strconv.Atoi(r.Headers["seq"])
			if prev, ok := lastSeq[*r.Key]; ok && seq <= prev {
				t.Fatalf("key %s out of order: %d after %d", *r.Key, seq, prev)
			}
			lastSeq[*r.Key] = seq
		}
	}
	if total != 400 {
		t.Fatalf("read %d records, produced 400", total)
	}
}

// Records acknowledged with acks=all survive the loss of the partition
// leader; the producer retries through the failover transparently; the
// restarted broker catches up and rejoins the ISR.
func TestLeaderFailoverPreservesAcknowledgedWrites(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client()
	mustCreate(t, cl, "payments", 1, 3, map[string]any{"min_insync_replicas": 2})
	c.waitTopicReady(cl, "payments")
	pr := cl.NewProducer(client.ProducerConfig{RetryBackoff: 50 * time.Millisecond})
	defer pr.Close(context.Background())
	produceN(t, pr, "payments", 100, func(i int) string { return "k" })

	before := c.partition("payments", 0)
	c.kill(before.Leader)
	c.waitFor("new leader", 10*time.Second, func() bool {
		ps := c.partition("payments", 0)
		return ps.Leader >= 0 && ps.Leader != before.Leader && ps.LeaderEpoch > before.LeaderEpoch
	})
	// Writes continue against the new leader; the producer re-routes.
	produceN(t, pr, "payments", 50, func(i int) string { return "k" })
	recs := readAll(t, cl, "payments", 0)
	if len(recs) != 150 {
		t.Fatalf("expected 150 acknowledged records after failover, found %d", len(recs))
	}
	for i, r := range recs {
		if want := fmt.Sprintf("v-%d", i%100); i < 100 && *r.Value != want {
			t.Fatalf("offset %d = %q, want %q", i, *r.Value, want)
		}
	}
	// The old leader restarts with its data, follows, and rejoins the ISR.
	c.startBroker(before.Leader)
	c.waitFor("restarted broker back in ISR", 15*time.Second, func() bool {
		ps := c.partition("payments", 0)
		for _, r := range ps.ISR {
			if r == before.Leader {
				return true
			}
		}
		return false
	})
}

// The broker appends a batch and then the response is lost. The producer
// retries with the same sequence numbers and the broker returns the original
// offsets instead of appending again: no duplicates in the log.
func TestIdempotentProducerSurvivesLostResponses(t *testing.T) {
	c := newCluster(t, 1)
	cl := c.client()
	mustCreate(t, cl, "events", 1, 1, nil)
	c.waitTopicReady(cl, "events")
	c.broker(1).Faults().Set(fault.Config{DropProduceResponse: 0.4})
	pr := cl.NewProducer(client.ProducerConfig{BatchMaxRecords: 5, Linger: time.Millisecond, RetryBackoff: 10 * time.Millisecond})
	defer pr.Close(context.Background())
	res := produceN(t, pr, "events", 200, func(i int) string { return "k" })
	c.broker(1).Faults().Set(fault.Config{})
	st := pr.Stats()
	if st.Retries == 0 || st.Duplicates == 0 {
		t.Fatalf("fault injection did not exercise retries: %+v", st)
	}
	recs := readAll(t, cl, "events", 0)
	if len(recs) != 200 {
		t.Fatalf("log has %d records for 200 sends (duplicates or loss)", len(recs))
	}
	for i, r := range recs {
		if *r.Value != fmt.Sprintf("v-%d", i) {
			t.Fatalf("offset %d holds %q: retries reordered or duplicated records", i, *r.Value)
		}
		if res[i].Offset != int64(i) {
			t.Fatalf("result %d reported offset %d", i, res[i].Offset)
		}
	}
	t.Logf("producer stats with 40%% lost responses: %+v", st)
}

// The same scenario without idempotence shows why it matters.
func TestNonIdempotentRetriesDuplicate(t *testing.T) {
	c := newCluster(t, 1)
	cl := c.client()
	mustCreate(t, cl, "dupes", 1, 1, nil)
	c.waitTopicReady(cl, "dupes")
	c.broker(1).Faults().Set(fault.Config{DropProduceResponse: 0.4})
	pr := cl.NewProducer(client.ProducerConfig{DisableIdempotence: true, BatchMaxRecords: 5, Linger: time.Millisecond, RetryBackoff: 10 * time.Millisecond})
	defer pr.Close(context.Background())
	produceN(t, pr, "dupes", 100, func(i int) string { return "k" })
	c.broker(1).Faults().Set(fault.Config{})
	if n := len(readAll(t, cl, "dupes", 0)); n <= 100 {
		t.Fatalf("expected duplicates without idempotence, got %d records", n)
	} else {
		t.Logf("100 sends without idempotence produced %d records", n)
	}
}

// Consumers split partitions; when one leaves, the survivor takes over and
// every record is processed at least once.
func TestConsumerGroupRebalanceAtLeastOnce(t *testing.T) {
	c := newCluster(t, 2)
	cl := c.client()
	mustCreate(t, cl, "clicks", 6, 2, nil)
	c.waitTopicReady(cl, "clicks")
	pr := cl.NewProducer(client.ProducerConfig{})
	produceN(t, pr, "clicks", 600, func(i int) string { return strconv.Itoa(i) })
	pr.Close(context.Background())

	seen := map[string]int{}
	var mu sync.Mutex
	newConsumer := func(name string) *client.Consumer {
		cs, err := cl.NewConsumer(client.ConsumerConfig{Group: "analytics", Topics: []string{"clicks"}, ClientID: name,
			SessionTimeout: 2 * time.Second, HeartbeatInterval: 100 * time.Millisecond, FetchWait: 200 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		return cs
	}
	consume := func(cs *client.Consumer, stop <-chan struct{}, limit int) {
		n := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			recs, err := cs.Poll(ctx(t, 10*time.Second), 100*time.Millisecond)
			if err != nil {
				continue
			}
			mu.Lock()
			for _, r := range recs {
				seen[fmt.Sprintf("%d-%d", r.Partition, r.Offset)]++
			}
			mu.Unlock()
			if len(recs) > 0 {
				cs.CommitRecords(ctx(t, 5*time.Second), recs)
			}
			n += len(recs)
			if limit > 0 && n >= limit {
				return
			}
		}
	}
	a, b := newConsumer("a"), newConsumer("b")
	stopA := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); consume(a, stopA, 0) }()
	consume(b, nil, 150) // b processes some records, then leaves
	if got := len(b.Assignment()); got == 0 || got == 6 {
		t.Fatalf("expected b to hold part of the assignment, got %d", got)
	}
	b.Close(ctx(t, 5*time.Second))

	c.waitFor("survivor owns all partitions and finished", 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(a.Assignment()) == 6 && len(seen) == 600
	})
	close(stopA)
	wg.Wait()
	a.Close(ctx(t, 5*time.Second))
	if st := a.Stats(); st.Rebalances < 2 {
		t.Fatalf("expected a rebalance on a, stats %+v", st)
	}
	lag, err := cl.Lag(ctx(t, 5*time.Second), "analytics")
	if err != nil || lag.TotalLag != 0 {
		t.Fatalf("lag after full consumption: %+v %v", lag, err)
	}
}

// Committing after processing gives at-least-once: a consumer that crashes
// after processing but before committing causes reprocessing.
func TestCrashBeforeCommitReprocesses(t *testing.T) {
	c := newCluster(t, 1)
	cl := c.client()
	mustCreate(t, cl, "jobs", 1, 1, nil)
	c.waitTopicReady(cl, "jobs")
	pr := cl.NewProducer(client.ProducerConfig{})
	produceN(t, pr, "jobs", 20, func(i int) string { return "k" })
	pr.Close(context.Background())

	poll := func(commit bool) int {
		cs, _ := cl.NewConsumer(client.ConsumerConfig{Group: "workers", Topics: []string{"jobs"},
			SessionTimeout: time.Second, HeartbeatInterval: 100 * time.Millisecond, FetchWait: 100 * time.Millisecond})
		defer cs.Close(ctx(t, 5*time.Second))
		var got []client.Record
		for i := 0; i < 50 && len(got) < 20; i++ {
			recs, _ := cs.Poll(ctx(t, 10*time.Second), 100*time.Millisecond)
			got = append(got, recs...)
		}
		if commit {
			cs.CommitRecords(ctx(t, 5*time.Second), got)
		}
		return len(got)
	}
	if n := poll(false); n != 20 { // processed, "crashed" before commit
		t.Fatalf("first run read %d", n)
	}
	if n := poll(true); n != 20 { // everything is redelivered
		t.Fatalf("expected redelivery of 20 uncommitted records, got %d", n)
	}
	lag, _ := cl.Lag(ctx(t, 5*time.Second), "workers")
	if lag.TotalLag != 0 {
		t.Fatalf("lag %d", lag.TotalLag)
	}
}

// Replay: reset a stopped group to a timestamp and reprocess history.
func TestReplayFromTimestamp(t *testing.T) {
	c := newCluster(t, 1)
	cl := c.client()
	mustCreate(t, cl, "audit", 2, 1, nil)
	c.waitTopicReady(cl, "audit")
	pr := cl.NewProducer(client.ProducerConfig{})
	defer pr.Close(context.Background())
	produceN(t, pr, "audit", 50, func(i int) string { return strconv.Itoa(i) })
	time.Sleep(50 * time.Millisecond)
	cut := time.Now()
	time.Sleep(50 * time.Millisecond)
	produceN(t, pr, "audit", 30, func(i int) string { return strconv.Itoa(i) })

	// The group has consumed everything (offsets at the end).
	if _, err := cl.ResetOffsets(ctx(t, 5*time.Second), "rebuild", protocol.ResetOffsetsRequest{Topic: "audit", To: "latest"}); err != nil {
		t.Fatal(err)
	}
	lag, _ := cl.Lag(ctx(t, 5*time.Second), "rebuild")
	if lag.TotalLag != 0 {
		t.Fatalf("lag %d", lag.TotalLag)
	}
	// Rewind to the cut: exactly the 30 later records become lag again.
	_, err := cl.ResetOffsets(ctx(t, 5*time.Second), "rebuild", protocol.ResetOffsetsRequest{
		Topic: "audit", To: "timestamp", Timestamp: cut.UTC().Format(time.RFC3339Nano)})
	if err != nil {
		t.Fatal(err)
	}
	lag, _ = cl.Lag(ctx(t, 5*time.Second), "rebuild")
	if lag.TotalLag != 30 {
		t.Fatalf("replay from timestamp should expose 30 records, lag=%d", lag.TotalLag)
	}
	// And to the beginning.
	cl.ResetOffsets(ctx(t, 5*time.Second), "rebuild", protocol.ResetOffsetsRequest{Topic: "audit", To: "earliest"})
	lag, _ = cl.Lag(ctx(t, 5*time.Second), "rebuild")
	if lag.TotalLag != 80 {
		t.Fatalf("lag after reset to earliest %d", lag.TotalLag)
	}
}

// Broker-level rate limits push back with 429 + Retry-After; the producer
// honours it and still delivers everything, just slower.
func TestRateLimitingBackpressure(t *testing.T) {
	c := newCluster(t, 1)
	cl := c.client()
	mustCreate(t, cl, "metrics", 1, 1, nil)
	c.waitTopicReady(cl, "metrics")
	c.broker(1).Limiter().SetLimits(ratelimit.Limits{TopicBytesPerSec: 20_000})

	v := string(make([]byte, 2000))
	var throttled int
	for i := 0; i < 50; i++ {
		_, err := cl.Produce(ctx(t, 5*time.Second), "metrics", 0, protocol.ProduceRequest{
			Records: []protocol.Record{{Value: &v}}, Acks: "leader"})
		var pe *protocol.Error
		if errors.As(err, &pe) && pe.Code == protocol.CodeThrottled {
			if pe.RetryAfterMs <= 0 {
				t.Fatal("throttle without retry-after")
			}
			throttled++
		}
	}
	if throttled == 0 {
		t.Fatal("rate limit never applied")
	}
	pr := cl.NewProducer(client.ProducerConfig{BatchMaxRecords: 10})
	defer pr.Close(context.Background())
	start := time.Now()
	produceN(t, pr, "metrics", 300, func(i int) string { return "k" })
	if pr.Stats().Retries == 0 {
		t.Fatal("producer was never throttled")
	}
	t.Logf("300 records under a 20KB/s topic quota took %v with %d throttled retries", time.Since(start), pr.Stats().Retries)
}

// A broker crash (no clean shutdown) followed by restart: recovery runs,
// the high-water mark is restored, data is intact.
func TestBrokerCrashRecovery(t *testing.T) {
	c := newCluster(t, 1)
	cl := c.client()
	mustCreate(t, cl, "logs", 2, 1, map[string]any{"segment_bytes": 4096, "max_event_bytes": 1024})
	c.waitTopicReady(cl, "logs")
	pr := cl.NewProducer(client.ProducerConfig{})
	produceN(t, pr, "logs", 300, func(i int) string { return strconv.Itoa(i) })
	pr.Close(context.Background())
	before := map[int32]int{}
	for p := int32(0); p < 2; p++ {
		before[p] = len(readAll(t, cl, "logs", p))
	}
	c.kill(1)
	c.startBroker(1)
	cl = c.client()
	c.waitTopicReady(cl, "logs")
	for p := int32(0); p < 2; p++ {
		if n := len(readAll(t, cl, "logs", p)); n != before[p] {
			t.Fatalf("partition %d: %d records before crash, %d after", p, before[p], n)
		}
	}
}

// Retention and compaction triggered through the broker admin API.
func TestRetentionAndCompaction(t *testing.T) {
	c := newCluster(t, 1)
	cl := c.client()
	mustCreate(t, cl, "profiles", 1, 1, map[string]any{
		"cleanup_policy": "compact", "segment_bytes": 2048, "max_event_bytes": 1024})
	mustCreate(t, cl, "clicks", 1, 1, map[string]any{
		"segment_bytes": 2048, "max_event_bytes": 1024, "retention_bytes": 4096})
	c.waitTopicReady(cl, "profiles")
	c.waitTopicReady(cl, "clicks")
	pr := cl.NewProducer(client.ProducerConfig{BatchMaxRecords: 10})
	defer pr.Close(context.Background())
	produceN(t, pr, "profiles", 300, func(i int) string { return fmt.Sprintf("user-%d", i%5) })
	produceN(t, pr, "clicks", 300, func(i int) string { return "k" })

	if _, err := cl.BrokerJSON(ctx(t, 10*time.Second), c.broker(1).URL(), "POST", "/v1/admin/maintenance", nil); err != nil {
		t.Fatal(err)
	}
	// Compaction: the latest value per key survives, offsets are preserved.
	latest := map[string]string{}
	recs := readAll(t, cl, "profiles", 0)
	for _, r := range recs {
		latest[*r.Key] = *r.Value
	}
	if len(recs) >= 300 {
		t.Fatalf("compaction removed nothing (%d records)", len(recs))
	}
	for k := 0; k < 5; k++ {
		if want := fmt.Sprintf("v-%d", 295+k); latest[fmt.Sprintf("user-%d", k)] != want {
			t.Fatalf("user-%d = %q, want %q", k, latest[fmt.Sprintf("user-%d", k)], want)
		}
	}
	// Retention: old segments deleted, log start advanced.
	o, _ := cl.Offsets(ctx(t, 5*time.Second), "clicks", 0, -1)
	if o.LogStartOffset == 0 || o.HighWatermark != 300 {
		t.Fatalf("retention did not advance log start: %+v", o)
	}
	// A fetch below the log start is out of range, not silently skipped.
	_, err := cl.Fetch(ctx(t, 5*time.Second), "clicks", 0, 0, client.FetchOptions{})
	var pe *protocol.Error
	if !errors.As(err, &pe) || pe.Code != protocol.CodeOffsetOutOfRange {
		t.Fatalf("err=%v", err)
	}
}

// Long polling: a fetch with wait_ms blocks until a record arrives.
func TestLongPollDeliversPromptly(t *testing.T) {
	c := newCluster(t, 1)
	cl := c.client()
	mustCreate(t, cl, "alerts", 1, 1, nil)
	c.waitTopicReady(cl, "alerts")
	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		r, err := cl.Fetch(ctx(t, 10*time.Second), "alerts", 0, 0, client.FetchOptions{Wait: 5 * time.Second})
		if err != nil || len(r.Records) != 1 {
			t.Errorf("long poll: %v %d", err, len(r.Records))
		}
		done <- time.Since(start)
	}()
	time.Sleep(300 * time.Millisecond)
	v := "fire"
	if _, err := cl.Produce(ctx(t, 5*time.Second), "alerts", 0, protocol.ProduceRequest{Records: []protocol.Record{{Value: &v}}}); err != nil {
		t.Fatal(err)
	}
	if d := <-done; d > 2*time.Second {
		t.Fatalf("long poll returned after %v", d)
	}
}

// Regression test for a bug found during the live failover demo: a client
// whose only bootstrap broker was the one that died kept asking that dead
// broker for metadata and never learned the new leader. Clients must learn
// the full broker list from metadata and use it for later refreshes.
func TestClientSurvivesLossOfItsOnlyBootstrapBroker(t *testing.T) {
	c := newCluster(t, 3)
	setup := c.client()
	mustCreate(t, setup, "single-bootstrap", 1, 3, map[string]any{"min_insync_replicas": 2})
	c.waitTopicReady(setup, "single-bootstrap")
	leader := c.partition("single-bootstrap", 0).Leader
	cl, err := client.New(client.Options{Brokers: []string{c.broker(leader).URL()}, MetadataTTL: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	pr := cl.NewProducer(client.ProducerConfig{})
	defer pr.Close(context.Background())
	produceN(t, pr, "single-bootstrap", 10, func(i int) string { return "k" })
	c.kill(leader)
	produceN(t, pr, "single-bootstrap", 10, func(i int) string { return "k" })
	if n := len(readAll(t, c.client(), "single-bootstrap", 0)); n != 20 {
		t.Fatalf("got %d records", n)
	}
}
