package broker

import (
	"errors"
	"strconv"
	"time"

	"github.com/michael-emmanuel/eventstream/internal/observability"
)

var errorIs = errors.Is

type brokerMetrics struct {
	requests           *observability.CounterVec
	requestLatency     *observability.HistogramVec
	bytesIn            *observability.CounterVec
	bytesOut           *observability.CounterVec
	produceRecords     *observability.CounterVec
	produceBytes       *observability.CounterVec
	produceErrors      *observability.CounterVec
	produceLatency     *observability.HistogramVec
	batchSize          *observability.HistogramVec
	duplicates         *observability.CounterVec
	fetchRecords       *observability.CounterVec
	fetchBytes         *observability.CounterVec
	fetchLatency       *observability.HistogramVec
	throttled          *observability.CounterVec
	rejected           *observability.CounterVec
	replicaFetchErrors *observability.CounterVec
	replicaBytes       *observability.CounterVec
	replicaRecords     *observability.CounterVec
	truncations        *observability.CounterVec
	roleChanges        *observability.CounterVec
	isrChanges         *observability.CounterVec
	heartbeatErrors    *observability.CounterVec
	fsync              *observability.HistogramVec
	recoveryTruncated  *observability.CounterVec
	retentionBytes     *observability.CounterVec
	compactionRemoved  *observability.CounterVec
}

func newBrokerMetrics(r *observability.Registry, b *Broker) *brokerMetrics {
	m := &brokerMetrics{
		requests:           r.Counter("broker_requests_total", "HTTP requests by route and status.", "route", "code"),
		requestLatency:     r.Histogram("broker_request_duration_seconds", "HTTP request latency by route.", nil, "route"),
		bytesIn:            r.Counter("broker_network_in_bytes_total", "Request body bytes received.", "route"),
		bytesOut:           r.Counter("broker_network_out_bytes_total", "Response bytes sent.", "route"),
		produceRecords:     r.Counter("broker_produce_records_total", "Records appended as leader.", "topic", "partition"),
		produceBytes:       r.Counter("broker_produce_bytes_total", "Encoded bytes appended as leader.", "topic", "partition"),
		produceErrors:      r.Counter("broker_produce_errors_total", "Failed produce requests.", "topic", "reason"),
		produceLatency:     r.Histogram("broker_produce_latency_seconds", "Produce latency including replication wait, by acks.", nil, "acks"),
		batchSize:          r.Histogram("broker_produce_batch_records", "Records per produce request.", observability.CountBuckets),
		duplicates:         r.Counter("broker_produce_duplicates_total", "Produce retries recognized as duplicates.", "topic"),
		fetchRecords:       r.Counter("broker_fetch_records_total", "Records returned to consumers.", "topic"),
		fetchBytes:         r.Counter("broker_fetch_bytes_total", "Payload bytes returned to consumers.", "topic"),
		fetchLatency:       r.Histogram("broker_fetch_latency_seconds", "Consumer fetch latency (includes long-poll wait).", nil),
		throttled:          r.Counter("broker_throttled_requests_total", "Requests rejected by rate limits.", "scope"),
		rejected:           r.Counter("broker_rejected_requests_total", "Requests shed by backpressure.", "reason"),
		replicaFetchErrors: r.Counter("broker_replica_fetch_errors_total", "Failed follower fetches.", "reason"),
		replicaBytes:       r.Counter("broker_replica_bytes_total", "Bytes replicated as follower.", "topic"),
		replicaRecords:     r.Counter("broker_replica_records_total", "Records replicated as follower.", "topic"),
		truncations:        r.Counter("broker_replica_truncations_total", "Follower truncations after divergence."),
		roleChanges:        r.Counter("broker_role_changes_total", "Partition role transitions.", "role"),
		isrChanges:         r.Counter("broker_isr_changes_total", "ISR changes accepted by the controller."),
		heartbeatErrors:    r.Counter("broker_controller_heartbeat_errors_total", "Failed controller heartbeats."),
		fsync:              r.Histogram("broker_fsync_duration_seconds", "fsync latency (disk latency).", nil),
		recoveryTruncated:  r.Counter("broker_recovery_truncated_bytes_total", "Bytes truncated by crash recovery."),
		retentionBytes:     r.Counter("broker_retention_deleted_bytes_total", "Bytes deleted by retention.", "topic"),
		compactionRemoved:  r.Counter("broker_compaction_removed_records_total", "Records removed by compaction.", "topic"),
	}
	r.RegisterCollector(func(e *observability.Emitter) {
		v := 0.0
		if b.leaseValid() {
			v = 1
		}
		e.Gauge("broker_lease_valid", "1 if the broker holds a controller lease (may accept writes).", v)
		e.Gauge("broker_inflight_produce_bytes", "Bytes of produce requests in memory.", float64(b.inflight.inUse()))
		e.Gauge("broker_long_poll_waiters", "Fetch requests waiting for data.", float64(len(b.pollers)))
		if md := b.md.Load(); md != nil {
			e.Gauge("broker_metadata_version", "Metadata version applied.", float64(md.Version))
		}
		for k, p := range b.partitions() {
			st := p.State()
			lbl := []string{"topic", k.topic, "partition", strconv.Itoa(int(k.p))}
			leader := 0.0
			if st.Role == "leader" {
				leader = 1
			}
			e.Gauge("partition_is_leader", "1 if this broker leads the partition.", leader, lbl...)
			e.Gauge("partition_log_start_offset", "First retained offset.", float64(st.LogStartOffset), lbl...)
			e.Gauge("partition_log_end_offset", "Log end offset (next offset to be written).", float64(st.LogEndOffset), lbl...)
			e.Gauge("partition_high_watermark", "High-water mark (consumer-visible end).", float64(st.HighWatermark), lbl...)
			e.Gauge("partition_size_bytes", "Bytes on disk.", float64(st.SizeBytes), lbl...)
			e.Gauge("partition_segments", "Segment files.", float64(st.Segments), lbl...)
			e.Gauge("partition_isr_size", "In-sync replicas (as seen by this replica).", float64(len(st.ISR)), lbl...)
			e.Gauge("partition_producer_ids", "Idempotent producers tracked.", float64(st.Producers), lbl...)
			off := 0.0
			if st.Failed != "" {
				off = 1
			}
			e.Gauge("partition_offline", "1 if the replica failed (storage error).", off, lbl...)
			if st.Role == "leader" {
				ur := 0.0
				if st.UnderReplicated {
					ur = 1
				}
				e.Gauge("partition_under_replicated", "1 if ISR < replicas (reported by leader).", ur, lbl...)
				for _, f := range st.Followers {
					fl := append(append([]string(nil), lbl...), "follower", strconv.Itoa(int(f.BrokerID)))
					e.Gauge("partition_replica_lag_records", "Records the follower is behind the leader LEO.", float64(f.LagRecords), fl...)
					e.Gauge("partition_replica_last_caught_up_seconds", "Seconds since the follower was last fully caught up.", float64(f.LastCaughtUpAgoMs)/1000, fl...)
				}
			}
		}
	})
	return m
}

func (m *brokerMetrics) observeRequest(route string, status int, d time.Duration, in, out int64) {
	m.requests.With(route, strconv.Itoa(status)).Inc()
	m.requestLatency.With(route).Observe(d.Seconds())
	if in > 0 {
		m.bytesIn.With(route).Add(float64(in))
	}
	m.bytesOut.With(route).Add(float64(out))
}

func (m *brokerMetrics) forgetPartition(k tpKey) {}
