# Observability

Every process serves Prometheus text metrics at `GET /metrics` and writes
structured JSON logs to stderr. Traces are exported as structured log
lines (`msg="span"`). Example scrape and alert configuration:
[deploy/prometheus.yml](../deploy/prometheus.yml),
[deploy/alerts.yml](../deploy/alerts.yml).

## Metrics reference

### Broker

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `broker_requests_total` | counter | route, code | requests by route and HTTP status (rate = requests/sec) |
| `broker_request_duration_seconds` | histogram | route | request latency |
| `broker_network_in_bytes_total` / `_out_` | counter | route | request/response bytes (bytes/sec) |
| `broker_produce_records_total` / `_bytes_total` | counter | topic, partition | records and encoded bytes appended as leader |
| `broker_produce_latency_seconds` | histogram | acks | produce latency including replication wait |
| `broker_produce_batch_records` | histogram | | records per produce request |
| `broker_produce_errors_total` | counter | topic, reason | failed produces (not_leader, timeout, storage, ...) |
| `broker_produce_duplicates_total` | counter | topic | retries recognized as duplicates |
| `broker_fetch_records_total` / `_bytes_total` | counter | topic | consumer reads |
| `broker_fetch_latency_seconds` | histogram | | fetch latency including long-poll wait |
| `broker_fsync_duration_seconds` | histogram | | disk latency (fsync) |
| `broker_throttled_requests_total` | counter | scope | rate-limit rejections |
| `broker_rejected_requests_total` | counter | reason | backpressure shedding (memory, long_poll_capacity) |
| `broker_replica_bytes_total` / `_records_total` | counter | topic | replicated as follower |
| `broker_replica_fetch_errors_total` | counter | reason | follower fetch failures |
| `broker_replica_truncations_total` | counter | | divergence truncations |
| `broker_role_changes_total` | counter | role | leader/follower transitions |
| `broker_isr_changes_total` | counter | | ISR changes accepted |
| `broker_controller_heartbeat_errors_total` | counter | | failed heartbeats |
| `broker_lease_valid` | gauge | | 1 if this broker may accept writes |
| `broker_inflight_produce_bytes` | gauge | | memory held by produce requests |
| `broker_long_poll_waiters` | gauge | | parked fetches |
| `broker_recovery_truncated_bytes_total` | counter | | bytes removed by crash recovery |
| `broker_retention_deleted_bytes_total` | counter | topic | retention deletions |
| `broker_compaction_removed_records_total` | counter | topic | compaction removals |
| `go_goroutines`, `go_memstats_*`, `go_gc_*`, `process_cpu_seconds_total` | | | runtime: goroutines, heap, GC, CPU (getrusage) |

### Partition (exported by each broker for its replicas)

| Metric | Meaning |
|---|---|
| `partition_log_end_offset` | LEO; its rate is events/sec into the partition |
| `partition_high_watermark` | consumer-visible end |
| `partition_log_start_offset` | first retained offset |
| `partition_size_bytes`, `partition_segments` | disk usage |
| `partition_is_leader` | 1 on the leader |
| `partition_isr_size` | ISR size as seen by this replica |
| `partition_under_replicated` | leader only: ISR < replicas |
| `partition_replica_lag_records{follower}` | leader only: follower LEO behind leader LEO |
| `partition_replica_last_caught_up_seconds{follower}` | leader only: drives ISR membership |
| `partition_offline` | 1 if this replica hit a storage error |
| `partition_producer_ids` | idempotent producers tracked |

### Controller and consumer groups

| Metric | Meaning |
|---|---|
| `controller_brokers_alive`, `controller_broker_alive{broker}` | membership |
| `controller_under_replicated_partitions` | cluster-wide |
| `controller_offline_partitions` | partitions with no leader |
| `controller_leader_elections_total{reason}` | session_expired, restart, storage_failure, preferred, unclean, offline |
| `controller_metadata_version` | increments on every change |
| `group_members{group}`, `group_generation{group}`, `group_rebalancing{group}` | group state |
| `group_rebalances_total{group}`, `group_rebalance_duration_seconds{group}` | rebalance count and duration |
| `group_offset_commits_total{group,result}` | ok, fenced, rejected, error |
| `group_members_expired_total{group}` | session expiries |

### Producer and consumer (client side)

`Producer.Stats()`: records, batches, retries, errors, duplicates, PID
resets; per-record latency in `Result.Latency`. `Consumer.Stats()`:
records, rebalances, commits, generation, assignment size. The load
generator turns these into p50/p95/p99.

## Consumer lag

```
lag(partition) = high_water_mark - committed_offset
```

`GET /v1/groups/{group}/lag` (controller) or `streamctl groups lag G`
returns per-partition committed offset, HWM, lag and owner. Lag uses the
HWM, not the LEO, because consumers can only read up to the HWM; uncommitted
records are not "behind", they are not readable yet.

**Why lag beats consumer CPU as a health signal.** CPU measures how hard a
consumer works, not whether it keeps up. A consumer blocked on a slow
database, an external API with rate limits, lock contention, or network
latency has **low CPU, high latency and huge lag**. Lag measures the thing
users care about (how stale downstream state is) regardless of where the
bottleneck is. Convert it to time when possible:
`lag_seconds ~ lag / ingest_rate` or, better, `now - timestamp of the
record at the committed offset`.

## Monitoring guide

| Area | Watch | Healthy | Investigate when |
|---|---|---|---|
| broker health | `broker_lease_valid`, `controller_broker_alive`, `up` | all 1 | any 0 for > 30 s |
| partition health | `controller_offline_partitions` | 0 | > 0 (page immediately) |
| replication | `controller_under_replicated_partitions`, `partition_replica_last_caught_up_seconds` | 0 / < replica_lag_max | > 0 for 5 min; catch-up time rising |
| consumer lag | lag per group, trend | flat or falling | rising for > N min, or lag_seconds > SLO |
| producer throughput | rate(`broker_produce_records_total`) | matches expectation | sudden drop (producers failing) or spike (hot partition) |
| consumer throughput | rate(`broker_fetch_records_total`) | ~ producer rate per group | below producer rate |
| disk utilization | `partition_size_bytes` sum vs disk | < 70% | growth rate x retention > capacity |
| disk latency | p99 `broker_fsync_duration_seconds` | low ms | > 100 ms |
| network throughput | rate(`broker_network_*_bytes_total`) | < 60% NIC | near NIC limit |
| rebalance frequency | rate(`group_rebalances_total`) | rare | > a few per 10 min |
| backpressure | `broker_throttled_requests_total`, `broker_rejected_requests_total` | 0 or expected tenants | unexpected scopes/tenants |

## Investigation playbooks

<a id="playbook-consumer-lag-increased-20x"></a>
### Consumer lag increased 20x

1. **Did producer throughput increase?** Compare
   `rate(broker_produce_records_total{topic})` now vs before. If input
   grew 20x, consumers may be fine and simply under-provisioned; capacity
   problem, not a bug.
2. **Did consumer throughput decrease?** `rate(broker_fetch_records_total{topic})`
   and the group's commit rate (`group_offset_commits_total`). Throughput
   dropping while input is flat means the consumers slowed or stopped.
3. **Check downstream dependencies.** Database latency, external API
   errors/rate limits. Most lag incidents are downstream slowness.
4. **Check partition skew.** Is lag on all partitions or a few? One
   partition lagging = hot partition or one stuck consumer (look at the
   `owner` column of the lag endpoint).
5. **Check rebalance activity.** `group_rebalances_total`,
   `group_rebalancing`. Constant rebalances = no progress (storm playbook).
6. **Check broker latency.** p99 `broker_fetch_latency_seconds`, fsync
   latency, `broker_rejected_requests_total`. Slow brokers slow everyone.
7. **Check consumer CPU.** High CPU: processing got more expensive (code
   change, larger payloads). Low CPU with lag: waiting on I/O (step 3) or
   on locks.
8. **Check consumer errors.** Retries of a poison record can pin a
   partition; look for repeated processing of the same offset.
9. **Would more consumers help?** Only if (a) the group has fewer members
   than partitions, and (b) the bottleneck is consumer-side CPU or
   per-consumer I/O concurrency, not a shared downstream that is already
   saturated. Adding consumers to a group with members >= partitions does
   nothing; adding them when the database is the bottleneck makes it worse.

<a id="playbook-hot-partition"></a>
### Hot partition

1. `topk(5, rate(broker_produce_records_total[5m]))` by partition: one
   partition far above the others.
2. Its leader broker: higher CPU, disk, `partition_replica_lag_records`
   on its followers.
3. Its consumer: lag grows on that partition only.
4. Identify the key: `streamctl consume TOPIC -p N -n 1000` and count keys.
5. Decide with the owning team whether that key needs ordering: if not,
   salt or change the key; if yes, give it a dedicated partition/topic and
   make its consumer faster.

### Under-replicated partitions

1. Which brokers are out of ISR? (`streamctl topics describe`, controller
   logs `ISR changed`.)
2. Is the broker alive? (`controller_broker_alive`.) Dead: restart/replace.
3. Alive but lagging: its `broker_replica_fetch_errors_total{reason}` and
   fsync latency; network between it and leaders.
4. Did a reassignment or catch-up start (large lag, steadily falling)?
   Then wait, possibly throttle.

<a id="playbook-rebalance-storm"></a>
### Rebalance storm

1. `increase(group_rebalances_total[10m])` high; `group_rebalancing` near 1.
2. Coordinator logs `rebalance started ... reason=`: member joined/left/
   expired. Expired members point to session timeouts too short for GC
   pauses or long processing; joins/leaves point to a deploy or crash loop.
3. Check consumer logs for `poll interval exceeded` (processing longer than
   `MaxPollInterval`).
4. Mitigate: longer session timeout, graceful shutdown with Leave, slower
   rolling deploys, sticky assignor; fix the crash loop.

### Sudden increase in p99 produce latency

1. Which acks? `broker_produce_latency_seconds{acks}`. If only `all`: a
   follower is slow (replica lag, ISR shrink pending). If all modes: the
   leader itself.
2. Disk: p99 fsync latency; another tenant replaying history (page-cache
   eviction, disk reads competing with writes).
3. CPU/GC: `process_cpu_seconds_total`, `go_gc_pause_seconds_total`; batch
   sizes (`broker_produce_batch_records`) shrinking means more requests
   for the same data.
4. Backpressure: in-flight bytes near the limit, throttling.
5. Network: retransmits, NIC saturation.
6. Profile (`-pprof-addr`, see [performance.md](performance.md)).

## Logs

JSON lines with `component` (broker, controller, producer, consumer) and
`broker`. Important messages to alert or search on:

| Message | Meaning |
|---|---|
| `recovery truncated incomplete or corrupt log tail` | crash recovery removed bytes |
| `became leader` / `became follower` | role change with epoch |
| `follower log diverged from leader; truncated` | epoch-based truncation |
| `leader changed` | controller election (reason, old, new, epoch, ISR) |
| `UNCLEAN leader election` | data may have been lost |
| `partition offline` | no ISR member available |
| `broker session expired` | broker declared dead |
| `ISR changed` | ISR shrink/expand |
| `rebalance started` / `rebalance completed` | with reason, partitions moved, duration |
| `fsync failed; replica taken offline` | storage failure |
| `authorization denied` (`audit=true`) | security audit trail |
| `offset out of range; applying reset policy` (client) | consumer skipped data |

## Tracing

Enable with `broker -trace-sample-rate 0.01`, `producer -trace-sample-rate`
and `consumer -trace`. Spans (`producer.send`, `broker.produce`,
`consumer.process` with a link to the producer span and `queue_time_ms`)
share the W3C `traceparent` format, so an OpenTelemetry collector could
ingest them with a log-to-trace pipeline, or the exporter in
`internal/observability/tracing.go` could be swapped for OTLP.
