# Fault Injection

The point is to watch the system misbehave on purpose and see the
mechanisms react, not just to run the happy path.

## Mechanisms

| Mechanism | How | Scope |
|---|---|---|
| Broker fault API | `POST /v1/admin/faults` or `streamctl broker URL faults key=value ...`; `faults clear` resets | one broker, takes effect immediately |
| Process signals | `scripts/cluster.sh kill N` (SIGKILL), `pause N` (SIGSTOP), `resume N`, `restart N` | process crash, freeze, restart |
| Offline file damage | `streamctl corrupt DIR -mode torn|flip|garbage` (broker stopped) | storage corruption |
| Client flags | `consumer -process-delay`, `-crash-after N`, `-commit before`; `producer -hot-key-share` | slow/crashing consumers, skew |
| In tests | `fault.Injector` in-process, `Broker.Kill()` | every failure test in `tests/` |

Broker fault configuration (`internal/fault.Config`):

| Key | Effect |
|---|---|
| `disk_write_delay_ms` | sleep before each log write (slow disk) |
| `disk_write_error_rate` | probability a log write fails (failing disk) |
| `disk_sync_error_rate` | probability an fsync fails |
| `replica_fetch_delay_ms` | follower sleeps before each fetch (lagging replica) |
| `replica_fetch_drop_rate` | probability a follower fetch fails (packet loss / timeouts) |
| `isolate_from_controller` | heartbeats and metadata calls fail (partition from controller) |
| `isolate_from_brokers` | replication in and out fails, including parked long polls |
| `request_delay_ms` | delay every client request (network latency) |
| `drop_produce_response` | probability of appending, then killing the connection (lost response) |
| `crash_on_next_produce` | exit the process after the next append, before responding |

All examples assume `scripts/cluster.sh start 3` and
`export STREAM_BROKERS=http://localhost:9091,http://localhost:9092,http://localhost:9093`.
Find a partition's leader with `streamctl topics describe orders`.

---

## 1. Broker crash

```
bin/producer -topic orders -rate 200 -n 2000 -quiet &
scripts/cluster.sh kill 2
```

**Expect**: producers to partitions led by broker 2 see connection errors
and retry; after the session timeout (6s in the local cluster) the
controller logs `broker session expired` and `leader changed` for each of
its partitions; retries succeed against the new leaders; all 2,000 records
end up in the topic exactly once. Other partitions are unaffected.
**Shows**: failure detection by heartbeat, ISR-based election, idempotent
retries, client metadata refresh.

## 2. Leader crash during writes, then restart

```
scripts/cluster.sh kill 1;  sleep 10;  scripts/cluster.sh restart 1
streamctl cluster            # under_replicated returns to 0
streamctl elect-preferred    # leadership returns to broker 1
```

**Expect**: on restart, `partition recovered` lines per partition, `became
follower`, ISR expansion; `under_replicated` back to 0; preferred election
moves leadership back.
**Shows**: crash recovery, catch-up replication, ISR expansion, leader
rebalancing.

## 3. Replica delay (lagging follower)

```
streamctl broker http://localhost:9093 faults replica_fetch_delay_ms=3000
streamctl broker http://localhost:909<leader> partitions   # follower lag, last_caught_up_ago_ms
streamctl cluster                                            # under_replicated > 0 after replica_lag_max_ms
streamctl broker http://localhost:9093 faults clear
```

**Expect**: lag on broker 3's replicas grows; after `replica_lag_max_ms` it
leaves the ISR (`ISR changed` in controller log); `acks=all` latency rises
until the shrink, then recovers; after clearing, it catches up and rejoins.
**Shows**: ISR mechanics; why `acks=all` latency is set by the slowest ISR
member; why shrinks need the controller.

## 4. Network timeout / latency

```
streamctl broker http://localhost:9091 faults request_delay_ms=500
bin/loadgen -topic orders -rate 500 -duration 15s
```

**Expect**: ack latency on partitions led by broker 1 rises by ~500 ms;
producer batches grow (more records accumulate while each request is in
flight), so throughput holds up better than latency.
**Shows**: batching absorbing latency; per-partition latency differences.

## 5. Packet loss (conceptually)

```
streamctl broker http://localhost:9093 faults replica_fetch_drop_rate=0.3
```

**Expect**: `broker_replica_fetch_errors_total{reason="injected"}` rises on
broker 3, follower backs off and retries; replication continues more
slowly; ISR may flap if loss is severe.
**Shows**: pull replication is naturally retry-safe (fetches are
idempotent reads at an offset).

## 6. Disk failure

```
streamctl broker http://localhost:909<leader> faults disk_write_error_rate=1
bin/streamctl produce orders -key k x
```

**Expect**: the write returns `STORAGE_ERROR`; the replica is marked
offline (`partition_offline=1`); the next heartbeat reports it; the
controller moves leadership (`reason=storage_failure`); producers retry to
the new leader. The failed replica stays offline until the broker restarts.
**Shows**: never continuing after a storage error; per-partition failure
reporting. Test: `TestDiskFailureMovesLeadership`.

## 7. Slow disk

```
streamctl broker http://localhost:9091 faults disk_write_delay_ms=200
bin/loadgen -topic orders -rate 2000 -duration 15s
```

**Expect**: produce latency on broker 1's partitions rises; in-flight bytes
grow; with enough load the broker sheds (`BROKER_OVERLOADED`) and producers
back off; producer `ErrBufferFull` if the application outruns everything.
**Shows**: backpressure chain from disk to application.
Test: `TestProducerBufferBackpressure`.

## 8. Slow consumer

```
bin/consumer -group slowpoke -topic orders -process-delay 20ms -quiet &
bin/producer -topic orders -rate 150 -n 1200 -quiet
watch -n1 bin/streamctl groups lag slowpoke
```

**Expect** (observed): lag grows ~100 records/s (150/s in, ~50/s out):
218, 409, 627, 829. Producers are not slowed; consumer memory stays flat
(bounded prefetch).
**Shows**: the log decouples producers from consumers; lag is the signal.

## 9. Producer retry (lost responses)

```
streamctl broker http://localhost:909<leader of events-0> faults drop_produce_response=0.5
bin/producer -topic events -rate 100 -n 300 -quiet
streamctl offsets events -p 0
```

**Expect** (observed): `sent=300 delivered=300 retries=28 errors=0`, high
watermark exactly 300, `broker_produce_duplicates_total` = 28. Every
dropped response was retried and recognized as a duplicate.
**Shows**: idempotent producers. Compare `TestNonIdempotentRetriesDuplicate`.

## 10. Consumer crash

```
bin/consumer -group g -topic orders -name c1 -quiet &
bin/consumer -group g -topic orders -name c2 -crash-after 50 &
```

**Expect**: c2 exits after 50 records without committing or leaving; after
the group session timeout the coordinator logs `member session expired` and
rebalances; c1 receives all partitions and reprocesses c2's uncommitted
records (at-least-once).
**Shows**: session-timeout failure detection, redelivery, why idempotent
processing matters.

## 11. Network partition (split brain attempt)

```
streamctl broker http://localhost:909<leader> faults isolate_from_controller=true isolate_from_brokers=true
curl -s -XPOST localhost:909<leader>/v1/topics/orders/partitions/0/events -d '{"acks":"leader","records":[{"value":"in-window"}]}'
sleep 5
curl -s -XPOST ... (same)        # LEASE_EXPIRED
streamctl topics describe orders # new leader, epoch + 1
streamctl broker http://localhost:909<old leader> faults clear
```

**Expect**: first write accepted (inside the lease), later writes refused
with `LEASE_EXPIRED`, new leader elected after the session timeout; after
healing, the old leader logs `follower log diverged from leader; truncated`
and the `in-window` record is gone from every replica.
**Shows**: leases, epochs, truncation. Full analysis:
[distributed-consistency.md](distributed-consistency.md).
Test: `TestNetworkPartitionFencesStaleLeader`.

## 12. Controller outage

```
scripts/cluster.sh kill-controller
curl -s -XPOST localhost:<leader>/v1/topics/orders/partitions/0/events -d '{"records":[{"value":"in-lease"}]}'   # accepted
sleep 5
curl -s -XPOST ... (same)            # LEASE_EXPIRED
streamctl produce orders x           # fails: a NEW idempotent producer cannot get a producer ID
streamctl consume orders -p 0        # reads keep working
scripts/cluster.sh start-controller  # state reloads from data/controller; writes resume
```

**Observed**: the in-lease write returned offsets; the write after 5s
returned `LEASE_EXPIRED`; `streamctl produce` failed with `allocate
producer id: ... connection refused` (producer IDs come from the
controller, so new idempotent producers cannot start while it is down;
already-running producers keep their IDs); after restart, writes resumed
and all brokers were alive with 0 under-replicated partitions.
**Shows**: the controller is a single point of failure for writes in this
design, by choice (consistency over availability). Production systems hand
out producer IDs in blocks to brokers so ID allocation does not need the
controller on the request path.
Test: `TestControllerOutageStopsWritesAfterLease`.

## 13. Torn write and corruption

```
kill -9 $(cat data/pids/broker-3)
streamctl corrupt data/broker-3/orders-0 -mode torn
streamctl verify data/broker-3/orders-0       # reports the damage
scripts/cluster.sh restart 3
grep "recovery truncated" data/logs/broker-3.log
```

**Expect** (observed on `events-0`): `truncated_bytes: 62, cause: record:
truncated frame, log_end: 304`, then the replica re-fetched offset 304 from
the leader (LEO 305 = leader). `-mode flip` produces a checksum failure
instead; `-mode garbage` an implausible length.
**Shows**: physical recovery, then logical repair via replication.

## 14. Frozen process (GC pause / hung disk)

```
scripts/cluster.sh pause 1; sleep 8; scripts/cluster.sh resume 1
```

**Expect**: while frozen, broker 1's lease expires and the controller
elects new leaders; on resume, broker 1 finds its lease expired (writes
refused), learns the new epochs, and becomes a follower. A frozen process
is indistinguishable from a partitioned one, which is why fencing must not
depend on the old leader noticing anything in time.

## Experiment checklist

For each experiment, record: the metric that shows the failure
(`controller_offline_partitions`, lag, `broker_lease_valid`, ...), time to
detect, time to recover, records lost (compare producer acknowledgements to
the HWM sum), records duplicated (count by producer sequence), and what an
operator would have needed to see on a dashboard.
