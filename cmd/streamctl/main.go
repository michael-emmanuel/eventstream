// Command streamctl is the operator CLI: topics, produce/consume, consumer
// groups, lag, replay, fault injection, maintenance, and offline segment
// tools for recovery demonstrations.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/protocol"
	"github.com/michael-emmanuel/eventstream/internal/segment"
	"github.com/michael-emmanuel/eventstream/pkg/client"
)

const usage = `streamctl - operate an event streaming cluster

Global flags (or env STREAM_BROKERS, STREAM_CONTROLLER, STREAM_TOKEN):
  -brokers URL[,URL]   bootstrap brokers   (default http://localhost:9091)
  -controller URL      controller          (default from metadata)
  -token TOKEN         bearer token

Commands:
  cluster                                   brokers, leaders, under-replicated partitions
  topics list
  topics create NAME [-p 6] [-rf 3] [key=value ...]   e.g. retention_ms=60000 cleanup_policy=compact
  topics describe NAME
  topics alter NAME key=value ...
  topics add-partitions NAME TOTAL
  topics delete NAME
  produce TOPIC [-key K] [-partition N] [-acks all|leader|quorum] [-n COUNT] [VALUE ...]   (stdin if no VALUE; "key<TAB>value" lines)
  consume TOPIC -p N [-offset O] [-n MAX] [-follow]
  offsets TOPIC -p N [-time RFC3339|ms]
  groups list | describe G | lag G
  groups reset G -topic T -to earliest|latest|offset|timestamp [-offset N] [-time T] [-dry-run]
  broker BROKER_URL partitions              replica state: role, LEO, HWM, ISR, follower lag
  broker BROKER_URL faults [clear | key=value ...]   e.g. replica_fetch_delay_ms=2000
  broker BROKER_URL limits [key=value ...]  e.g. topic_bytes_per_sec=100000
  broker BROKER_URL maintenance             run retention and compaction now
  elect-preferred                           move leadership back to preferred replicas
  segments DIR                              list segments of a partition directory (offline)
  verify DIR                                fully verify checksums of a partition directory (offline)
  corrupt DIR -mode torn|flip|garbage       damage the active segment (offline; recovery demos)
`

var (
	brokers    = env("STREAM_BROKERS", "http://localhost:9091,http://localhost:9092,http://localhost:9093")
	controller = env("STREAM_CONTROLLER", "")
	token      = env("STREAM_TOKEN", "")
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	gf := flag.NewFlagSet("streamctl", flag.ExitOnError)
	gf.StringVar(&brokers, "brokers", brokers, "")
	gf.StringVar(&controller, "controller", controller, "")
	gf.StringVar(&token, "token", token, "")
	gf.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	gf.Parse(os.Args[1:])
	args := gf.Args()
	if len(args) == 0 {
		gf.Usage()
		os.Exit(2)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var err error
	switch args[0] {
	case "cluster":
		err = printAdmin(ctx, "GET", "/v1/cluster", nil)
	case "topics":
		err = topics(ctx, args[1:])
	case "produce":
		err = produce(ctx, args[1:])
	case "consume":
		err = consume(ctx, args[1:])
	case "offsets":
		err = offsets(ctx, args[1:])
	case "groups":
		err = groups(ctx, args[1:])
	case "broker":
		err = brokerCmd(ctx, args[1:])
	case "elect-preferred":
		err = printAdmin(ctx, "POST", "/v1/admin/preferred-election", nil)
	case "segments":
		err = segments(args[1:])
	case "verify":
		err = verify(args[1:])
	case "corrupt":
		err = corrupt(args[1:])
	case "help", "-h", "--help":
		gf.Usage()
	default:
		err = fmt.Errorf("unknown command %q (try help)", args[0])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func newClient() *client.Client {
	cl, err := client.New(client.Options{Brokers: strings.Split(brokers, ","), ControllerURL: controller, Token: token})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return cl
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

func printAdmin(ctx context.Context, method, path string, in any) error {
	out, err := newClient().AdminJSON(ctx, method, path, in)
	if err != nil {
		return err
	}
	printJSON(out)
	return nil
}

// parseKV turns key=value pairs into a JSON-able map with typed values.
func parseKV(args []string) (map[string]any, error) {
	m := map[string]any{}
	for _, a := range args {
		k, v, ok := strings.Cut(a, "=")
		if !ok {
			return nil, fmt.Errorf("expected key=value, got %q", a)
		}
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			m[k] = i
		} else if f, err := strconv.ParseFloat(v, 64); err == nil {
			m[k] = f
		} else if b, err := strconv.ParseBool(v); err == nil {
			m[k] = b
		} else {
			m[k] = v
		}
	}
	return m, nil
}

// splitFlags separates positional args from flags so flags may follow them.
func splitFlags(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) > 0 {
			pos = append(pos, args[0])
			args = args[1:]
		}
	}
	return pos, nil
}

func topics(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("topics: missing subcommand")
	}
	cl := newClient()
	switch args[0] {
	case "list":
		return printAdmin(ctx, "GET", "/v1/topics", nil)
	case "create":
		fs := flag.NewFlagSet("create", flag.ExitOnError)
		p := fs.Int("p", 6, "partitions")
		rf := fs.Int("rf", 3, "replication factor")
		pos, err := splitFlags(fs, args[1:])
		if err != nil || len(pos) == 0 {
			return fmt.Errorf("usage: topics create NAME [-p N] [-rf N] [key=value ...]")
		}
		cfg, err := parseKV(pos[1:])
		if err != nil {
			return err
		}
		t, err := cl.CreateTopic(ctx, pos[0], *p, *rf, cfg)
		if err != nil {
			return err
		}
		printJSON(t)
	case "describe":
		if len(args) < 2 {
			return fmt.Errorf("usage: topics describe NAME")
		}
		return printAdmin(ctx, "GET", "/v1/topics/"+args[1], nil)
	case "alter":
		if len(args) < 3 {
			return fmt.Errorf("usage: topics alter NAME key=value ...")
		}
		cfg, err := parseKV(args[2:])
		if err != nil {
			return err
		}
		return printAdmin(ctx, "PATCH", "/v1/topics/"+args[1]+"/config", cfg)
	case "add-partitions":
		if len(args) < 3 {
			return fmt.Errorf("usage: topics add-partitions NAME TOTAL")
		}
		n, _ := strconv.Atoi(args[2])
		return printAdmin(ctx, "POST", "/v1/topics/"+args[1]+"/partitions", protocol.AddPartitionsRequest{TotalPartitions: n})
	case "delete":
		if len(args) < 2 {
			return fmt.Errorf("usage: topics delete NAME")
		}
		return cl.DeleteTopic(ctx, args[1])
	default:
		return fmt.Errorf("unknown topics subcommand %q", args[0])
	}
	return nil
}

func produce(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	key := fs.String("key", "", "record key")
	part := fs.Int("partition", -1, "explicit partition")
	acks := fs.String("acks", "all", "all|leader|quorum")
	n := fs.Int("n", 1, "repeat each value N times (suffixing -i)")
	hdrs := fs.String("headers", "", "comma-separated k=v headers")
	pos, err := splitFlags(fs, args)
	if err != nil || len(pos) == 0 {
		return fmt.Errorf("usage: produce TOPIC [flags] [VALUE ...]")
	}
	topic, values := pos[0], pos[1:]
	cl := newClient()
	pr := cl.NewProducer(client.ProducerConfig{Acks: *acks})
	defer pr.Close(context.Background())
	headers := map[string]string{}
	if *hdrs != "" {
		for _, kv := range strings.Split(*hdrs, ",") {
			k, v, _ := strings.Cut(kv, "=")
			headers[k] = v
		}
	}
	send := func(k, v string) (*client.Future, error) {
		m := client.Message{Topic: topic, Value: []byte(v), Headers: headers}
		if v == "null" {
			m.Value = nil // tombstone
		}
		if k != "" {
			m.Key = []byte(k)
		}
		if *part >= 0 {
			p := int32(*part)
			m.Partition = &p
		}
		return pr.Send(ctx, m)
	}
	var futs []*client.Future
	if len(values) == 0 {
		sc := bufio.NewScanner(os.Stdin)
		for sc.Scan() {
			k, v := *key, sc.Text()
			if a, b, ok := strings.Cut(v, "\t"); ok {
				k, v = a, b
			}
			f, err := send(k, v)
			if err != nil {
				return err
			}
			futs = append(futs, f)
		}
	} else {
		for _, v := range values {
			for i := 0; i < *n; i++ {
				val := v
				if *n > 1 {
					val = fmt.Sprintf("%s-%d", v, i)
				}
				f, err := send(*key, val)
				if err != nil {
					return err
				}
				futs = append(futs, f)
			}
		}
	}
	failed := 0
	for _, f := range futs {
		r := f.Wait(ctx)
		if r.Err != nil {
			failed++
			fmt.Fprintf(os.Stderr, "error: %v\n", r.Err)
			continue
		}
		dup := ""
		if r.Duplicate {
			dup = " (duplicate)"
		}
		fmt.Printf("%s-%d@%d%s\n", r.Topic, r.Partition, r.Offset, dup)
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d records failed", failed, len(futs))
	}
	return nil
}

func consume(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	p := fs.Int("p", 0, "partition")
	off := fs.Int64("offset", -1, "start offset (-1 = earliest)")
	max := fs.Int("n", 0, "stop after N records (0 = until caught up, or forever with -follow)")
	follow := fs.Bool("follow", false, "keep long-polling for new records")
	pos, err := splitFlags(fs, args)
	if err != nil || len(pos) == 0 {
		return fmt.Errorf("usage: consume TOPIC -p N [-offset O] [-n MAX] [-follow]")
	}
	cl := newClient()
	offset := *off
	if offset < 0 {
		o, err := cl.Offsets(ctx, pos[0], int32(*p), -1)
		if err != nil {
			return err
		}
		offset = o.LogStartOffset
	}
	count := 0
	for {
		wait := time.Duration(0)
		if *follow {
			wait = 10 * time.Second
		}
		r, err := cl.Fetch(ctx, pos[0], int32(*p), offset, client.FetchOptions{MaxEvents: 500, Wait: wait})
		if err != nil {
			return err
		}
		for _, rec := range r.Records {
			b, _ := json.Marshal(rec)
			fmt.Println(string(b))
			count++
			if *max > 0 && count >= *max {
				return nil
			}
		}
		offset = r.NextOffset
		if len(r.Records) == 0 && !*follow {
			fmt.Fprintf(os.Stderr, "caught up at offset %d (high-water mark %d)\n", offset, r.HighWatermark)
			return nil
		}
	}
}

func offsets(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("offsets", flag.ExitOnError)
	p := fs.Int("p", 0, "partition")
	ts := fs.String("time", "", "timestamp (RFC3339 or unix ms)")
	pos, err := splitFlags(fs, args)
	if err != nil || len(pos) == 0 {
		return fmt.Errorf("usage: offsets TOPIC -p N [-time T]")
	}
	t := int64(-1)
	if *ts != "" {
		if t, err = parseTime(*ts); err != nil {
			return err
		}
	}
	o, err := newClient().Offsets(ctx, pos[0], int32(*p), t)
	if err != nil {
		return err
	}
	printJSON(o)
	return nil
}

func parseTime(s string) (int64, error) {
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return v, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0, fmt.Errorf("time must be RFC3339 or unix ms: %q", s)
	}
	return t.UnixMilli(), nil
}

func groups(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("groups: missing subcommand")
	}
	switch args[0] {
	case "list":
		return printAdmin(ctx, "GET", "/v1/groups", nil)
	case "describe":
		if len(args) < 2 {
			return fmt.Errorf("usage: groups describe GROUP")
		}
		return printAdmin(ctx, "GET", "/v1/groups/"+args[1], nil)
	case "lag":
		if len(args) < 2 {
			return fmt.Errorf("usage: groups lag GROUP")
		}
		lag, err := newClient().Lag(ctx, args[1])
		if err != nil {
			return err
		}
		fmt.Printf("%-24s %9s %12s %12s %10s  %s\n", "PARTITION", "", "COMMITTED", "HWM", "LAG", "OWNER")
		for _, p := range lag.Partitions {
			fmt.Printf("%-24s %9s %12d %12d %10d  %s%s\n", fmt.Sprintf("%s-%d", p.Topic, p.Partition), "",
				p.Committed, p.HighWatermark, p.Lag, p.Owner, p.Error)
		}
		fmt.Printf("TOTAL LAG %d\n", lag.TotalLag)
	case "reset":
		fs := flag.NewFlagSet("reset", flag.ExitOnError)
		topic := fs.String("topic", "", "topic")
		to := fs.String("to", "earliest", "earliest|latest|offset|timestamp")
		off := fs.Int64("offset", 0, "offset for -to offset")
		ts := fs.String("time", "", "timestamp for -to timestamp (RFC3339 or ms)")
		dry := fs.Bool("dry-run", false, "show target offsets without committing")
		pos, err := splitFlags(fs, args[1:])
		if err != nil || len(pos) == 0 || *topic == "" {
			return fmt.Errorf("usage: groups reset GROUP -topic T -to earliest|latest|offset|timestamp")
		}
		out, err := newClient().ResetOffsets(ctx, pos[0], protocol.ResetOffsetsRequest{Topic: *topic, To: *to, Offset: *off, Timestamp: *ts, DryRun: *dry})
		if err != nil {
			return err
		}
		printJSON(out)
	default:
		return fmt.Errorf("unknown groups subcommand %q", args[0])
	}
	return nil
}

func brokerCmd(ctx context.Context, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: broker BROKER_URL partitions|faults|limits|maintenance")
	}
	u, cmd, rest := args[0], args[1], args[2:]
	cl := newClient()
	call := func(method, path string, in any) error {
		out, err := cl.BrokerJSON(ctx, u, method, path, in)
		if err != nil {
			return err
		}
		printJSON(out)
		return nil
	}
	switch cmd {
	case "partitions":
		return call("GET", "/v1/admin/partitions", nil)
	case "faults":
		if len(rest) == 0 {
			return call("GET", "/v1/admin/faults", nil)
		}
		if rest[0] == "clear" {
			return call("POST", "/v1/admin/faults", map[string]any{})
		}
		kv, err := parseKV(rest)
		if err != nil {
			return err
		}
		return call("POST", "/v1/admin/faults", kv)
	case "limits":
		if len(rest) == 0 {
			return call("GET", "/v1/admin/limits", nil)
		}
		kv, err := parseKV(rest)
		if err != nil {
			return err
		}
		return call("POST", "/v1/admin/limits", kv)
	case "maintenance":
		return call("POST", "/v1/admin/maintenance", nil)
	}
	return fmt.Errorf("unknown broker subcommand %q", cmd)
}

func segmentBases(dir string) ([]int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var bases []int64
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), segment.LogSuffix) {
			b, err := strconv.ParseInt(strings.TrimSuffix(e.Name(), segment.LogSuffix), 10, 64)
			if err == nil {
				bases = append(bases, b)
			}
		}
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	if len(bases) == 0 {
		return nil, fmt.Errorf("no segments in %s", dir)
	}
	return bases, nil
}

func segments(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: segments DIR")
	}
	bases, err := segmentBases(args[0])
	if err != nil {
		return err
	}
	fmt.Printf("%-22s %10s %8s %12s\n", "SEGMENT", "BYTES", "INDEX", "NEXT_OFFSET")
	for _, b := range bases {
		s, _, err := segment.Open(args[0], b, segment.Config{}, segment.VerifyFull)
		if err != nil {
			fmt.Printf("%020d  ERROR: %v\n", b, err)
			continue
		}
		fmt.Printf("%020d  %10d %8d %12d\n", b, s.Size(), s.IndexEntries(), s.NextOffset())
		s.Close()
	}
	return nil
}

func verify(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: verify DIR")
	}
	bases, err := segmentBases(args[0])
	if err != nil {
		return err
	}
	bad := 0
	for _, b := range bases {
		s, res, err := segment.Open(args[0], b, segment.Config{}, segment.VerifyFull)
		if err != nil {
			bad++
			fmt.Printf("segment %020d: CORRUPT: %v\n", b, err)
			continue
		}
		fmt.Printf("segment %020d: ok (%d records, %d bytes)\n", b, res.Records, res.ValidBytes)
		s.Close()
	}
	if bad > 0 {
		return fmt.Errorf("%d corrupt segment(s); the broker will truncate a corrupt tail of the active segment on startup, but sealed-segment corruption needs re-replication", bad)
	}
	return nil
}

func corrupt(args []string) error {
	fs := flag.NewFlagSet("corrupt", flag.ExitOnError)
	mode := fs.String("mode", "torn", "torn (partial final record) | flip (bit flip in final record) | garbage (append junk)")
	pos, err := splitFlags(fs, args)
	if err != nil || len(pos) == 0 {
		return fmt.Errorf("usage: corrupt DIR -mode torn|flip|garbage   (stop the broker first)")
	}
	bases, err := segmentBases(pos[0])
	if err != nil {
		return err
	}
	path := filepath.Join(pos[0], fmt.Sprintf("%020d%s", bases[len(bases)-1], segment.LogSuffix))
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	switch *mode {
	case "torn":
		if len(b) < 10 {
			return fmt.Errorf("segment too small")
		}
		b = b[:len(b)-7]
	case "flip":
		if len(b) < 4 {
			return fmt.Errorf("segment too small")
		}
		b[len(b)-3] ^= 0xFF
	case "garbage":
		b = append(b, []byte{0, 0, 0, 200, 1, 2, 3, 4, 5, 6, 7, 8, 9}...)
	default:
		return fmt.Errorf("unknown mode %q", *mode)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return err
	}
	fmt.Printf("damaged %s (%s); restart the broker and watch the recovery log line\n", path, *mode)
	return nil
}
