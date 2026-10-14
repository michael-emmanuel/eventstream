# Demonstrations

Fifteen scenarios against a real local cluster. "Observed" sections quote
output from runs on the development sandbox (2 vCPUs, everything on one
machine); your numbers will differ, the behavior should not.

## Setup (all demos)

```
make build                  # or: go build -o bin/ ./cmd/...
scripts/cluster.sh start 3  # controller :9000, brokers :9091-9093
export STREAM_BROKERS=http://localhost:9091,http://localhost:9092,http://localhost:9093
S=bin/streamctl
```

Docker alternative: `docker compose up -d --build`, then
`docker compose run --rm tools streamctl <command>` (the compose file
creates `orders` and `profiles` and runs a producer and three consumers).

Useful views throughout:

```
$S cluster                                   # brokers, leader counts, under-replicated
$S topics describe orders                    # leader, epoch, replicas, ISR per partition
$S broker http://localhost:9091 partitions   # role, LEO, HWM, follower lag on broker 1
$S groups lag fraud-detector
curl -s localhost:9091/metrics | grep partition_
```

---

## Demo 1: Basic production and consumption

**Setup**
```
$S topics create orders -p 4 -rf 3 min_insync_replicas=2
$S produce orders -key user-123 '{"event":"order.created"}' '{"event":"order.paid"}'
$S produce orders -key user-456 hello -n 3
curl -s -XPOST localhost:<leader-port>/v1/topics/orders/partitions/0/events \
  -d '{"acks":"all","records":[{"key":"user-1","value":"via-curl","headers":{"event_type":"order.created"}}]}'
$S consume orders -p 0
```
**Expected**: each record reports `topic-partition@offset`; consuming
returns records in offset order and stops at the high-water mark.
**Observed**
```
orders-3@0
orders-3@1            <- both user-123 events, same partition, consecutive offsets
orders-0@0 .. @2      <- user-456
{"topic":"orders","partition":0,"base_offset":3,"last_offset":3,...}   <- curl
caught up at offset 4 (high-water mark 4)
```
**Principle**: offsets are assigned by the partition leader; the key picks
the partition.
**Tradeoff**: the broker API is partition-scoped (route to the leader);
the gateway offers a topic-scoped API at the cost of a hop.

## Demo 2: Partition ordering

**Setup**: produce 400 records over 10 keys to a 4-partition topic and read
each partition back (`TestProduceConsumeOrderingPerKey` does exactly this).
```
for i in $(seq 1 40); do for u in 1 2 3; do printf "user-$u\tstep-$i\n"; done; done | $S produce orders
for p in 0 1 2 3; do $S consume orders -p $p | grep user-1; done
```
**Expected**: all `user-1` events in one partition, `step-1 ... step-40`
in order; offsets in each partition contiguous from 0; events of
different users in different partitions interleave arbitrarily.
**Principle**: ordering is per partition; key routing makes it per key.
**Tradeoff**: no global order; per-key order costs load balance (Demo 11).

## Demo 3: Consumer group scaling

**Setup**
```
bin/consumer -group fraud-detector -topic orders -name fraud-1 -quiet &
bin/consumer -group fraud-detector -topic orders -name fraud-2 -quiet &
bin/producer -topic orders -rate 50 -n 100 -quiet
$S groups lag fraud-detector
```
**Observed**
```
[fraud-1] ASSIGNED [orders-0 orders-1]
[fraud-2] ASSIGNED [orders-2 orders-3]
PARTITION   COMMITTED   HWM   LAG  OWNER
orders-0           33    33     0  fraud-1-0a37f0f28289
orders-1           18    18     0  fraud-1-0a37f0f28289
orders-2           22    22     0  fraud-2-9fa3589a84d7
orders-3           33    33     0  fraud-2-9fa3589a84d7
```
Start a third and fourth consumer: each gets one partition. A fifth gets
nothing (more members than partitions).
**Principle**: one owner per partition per group; parallelism is capped by
partitions.
**Tradeoff**: ordering per partition vs consumer parallelism.

## Demo 4: Consumer failure and rebalance

**Setup**: with the two consumers from Demo 3 running,
`kill -9 <pid of fraud-2>` (no graceful leave).
**Expected**: for up to the session timeout, fraud-2's partitions are not
consumed; then the coordinator expires it and rebalances.
**Observed**
```
[fraud-1] REVOKED  [orders-0 orders-1]
[fraud-1] ASSIGNED [orders-0 orders-1 orders-2 orders-3]
groups describe: Stable gen 2, rebalances 2, members [fraud-1: 4 partitions]
```
Records fraud-2 processed but had not committed are processed again by
fraud-1.
**Principle**: failure detection by heartbeat; generation-fenced
reassignment; at-least-once.
**Tradeoff**: shorter session timeout = faster recovery but more false
expirations (rebalance storms).

## Demo 5: Producer retry and idempotency

**Setup**
```
$S topics create events -p 1 -rf 3 min_insync_replicas=2
$S broker http://localhost:<leader> faults drop_produce_response=0.5
bin/producer -topic events -rate 100 -n 300 -quiet
$S broker http://localhost:<leader> faults clear
$S offsets events -p 0
curl -s localhost:<leader>/metrics | grep broker_produce_duplicates_total
```
**Observed**
```
sent=300 delivered=300 retries=28 errors=0 duplicates_suppressed=77
"high_watermark": 300
broker_produce_duplicates_total{topic="events"} 28
```
Half the responses were destroyed after the append; every retry was
recognized; the log holds exactly 300 records.
**Principle**: producer ID + sequence numbers make retries idempotent.
**Tradeoff**: one in-flight batch per partition; dedup state per producer.
Without idempotence the same fault duplicates records
(`TestNonIdempotentRetriesDuplicate`).

## Demo 6: Leader failure

**Setup**: find the leader of `orders-0`, then
```
bin/producer -brokers http://localhost:<leader port> -topic orders -rate 200 -n 2000 -quiet &
sleep 2; scripts/cluster.sh kill <leader>
```
(the producer's *only* bootstrap broker is the one being killed).
**Observed**
```
broker-1 killed (SIGKILL)
sent=2000 delivered=2000 retries=37 errors=0 duplicates_suppressed=0
records added to topic: 2000 (sent 2000; bootstrap was ONLY the killed broker)
controller: "leader changed" partition 0 old 1 new 2 epoch +1 reason session_expired
```
**Principle**: ISR election, epoch bump, client metadata refresh,
idempotent retries.
**Tradeoff**: write unavailability for ~session timeout vs false failovers.

*Note*: the first run of this demo failed (958 records timed out): the
client only knew its bootstrap broker, which was dead, so it never learned
the new leader. The client now learns all brokers from metadata; the
regression test is `TestClientSurvivesLossOfItsOnlyBootstrapBroker`. This
is the kind of bug only a fault-injection demo finds.

## Demo 7: Replica lag

**Setup**
```
$S topics create telemetry -p 1 -rf 3 replica_lag_max_ms=3000
$S broker http://localhost:<a follower> faults replica_fetch_delay_ms=4000
bin/producer -topic telemetry -rate 50 -acks leader -quiet &
$S broker http://localhost:<leader> partitions   # follower lag_records, last_caught_up_ago_ms
$S cluster                                       # under_replicated: 1
$S broker http://localhost:<follower> faults clear
```
**Expected**: follower lag grows; after 3s it leaves the ISR; the leader
reports `under_replicated`; after clearing, it catches up and rejoins.
**Principle**: ISR membership by time-since-caught-up; shrink via
controller. (`TestSlowReplicaLeavesAndRejoinsISR`.)
**Tradeoff**: small `replica_lag_max_ms` keeps `acks=all` latency low but
shrinks the ISR (durability) on transient hiccups.

## Demo 8: Broker recovery

**Setup**
```
$S produce events -n 5 tail-record
kill -9 $(cat data/pids/broker-3)
$S corrupt data/broker-3/events-0 -mode torn
$S verify data/broker-3/events-0
scripts/cluster.sh restart 3
grep "recovery truncated" data/logs/broker-3.log
$S broker http://localhost:9093 partitions
```
**Observed**
```
segment ...: CORRUPT: ... at byte 59192: record: truncated frame
{'msg': 'recovery truncated incomplete or corrupt log tail', 'partition': 'events-0',
 'truncated_bytes': 62, 'cause': 'record: truncated frame', 'log_end': 304}
events follower LEO 305 HWM 305
```
Recovery cut the torn record (LEO 304), then replication restored it from
the leader (LEO 305).
**Principle**: physical recovery (checksums, truncation) then logical
repair (replication).
**Tradeoff**: full verification of every segment at startup is safest but
slow; default verifies the active segment and sealed tails
(`-verify-on-recovery` for all).

## Demo 9: Consumer lag

**Setup**
```
$S topics create slow -p 2 -rf 3
bin/consumer -group slowpoke -topic slow -quiet -process-delay 20ms &
bin/producer -topic slow -rate 150 -n 1200 -quiet &
watch -n2 $S groups lag slowpoke
```
**Observed**
```
t=2s TOTAL LAG 218
t=4s TOTAL LAG 409
t=6s TOTAL LAG 627
t=8s TOTAL LAG 829
```
150/s in, ~50/s out (20 ms each): lag grows ~100/s, exactly the difference.
**Principle**: the log absorbs rate mismatch; lag is the health signal.
**Tradeoff**: decoupling means producers never slow down for consumers; a
consumer that never catches up eventually hits retention.

## Demo 10: Backpressure

**Setup**
```
$S broker http://localhost:9091 limits topic_bytes_per_sec=20000
bin/producer -topic orders -rate 500 -n 1000 -quiet
curl -s localhost:9091/metrics | grep broker_throttled
$S broker http://localhost:9091 limits topic_bytes_per_sec=0
```
Then overload: `bin/loadgen -topic lt -rate 100000 -duration 10s`.
**Expected/observed**: throttled requests get 429 with Retry-After; the
producer slows down and still delivers everything
(`TestRateLimitingBackpressure`). Under the 100k/s load test on the
sandbox the acknowledged rate plateaued at ~28.7k/s, latency grew into
seconds, memory stayed bounded, and no request failed.
**Principle**: bounded buffers at every stage; overload turns into waiting
at the edge.
**Tradeoff**: rejecting work vs queueing it (latency vs memory).

## Demo 11: Hot partition

**Setup**
```
bin/loadgen -topic hot -partitions 6 -rf 3 -rate 2000 -duration 8s -hot-key-share 0.7 -size 256
```
**Observed**
```
per partition  hot-0=11974 hot-1=761 hot-2=804 hot-3=844 hot-4=804 hot-5=804
```
74 percent of records on one partition, one leader, one consumer.
**Principle**: key routing preserves order by concentrating a key.
**Tradeoff**: ordering vs load distribution; mitigations in
[system-design.md](system-design.md#2-partitioning-strategies-and-hot-partitions)
(`routing.Salted` spreads the key and gives up its ordering).

## Demo 12: Replay

**Setup**
```
bin/producer -topic orders -rate 20 -n 100 -quiet
T=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 2
bin/producer -topic orders -rate 20 -n 50 -quiet
# stop all consumers of fraud-detector first
$S groups reset fraud-detector -topic orders -to timestamp -time $T --dry-run
$S groups reset fraud-detector -topic orders -to timestamp -time $T
$S groups lag fraud-detector        # exactly the records after T
$S groups reset fraud-detector -topic orders -to earliest
```
**Expected**: the reset moves every partition's committed offset to the
first record at or after T; lag equals the records produced after T
(`TestReplayFromTimestamp`: 30 of 80). Resetting an active group is refused
(`GROUP_NOT_EMPTY`).
**Principle**: history is retained; consumer position is just a number.
**Tradeoff**: replay re-runs side effects; consumers must be idempotent.

## Demo 13: Retention

**Setup**
```
$S topics create clicks -p 1 -rf 3 segment_bytes=4096 max_event_bytes=1024 retention_bytes=8192
bin/producer -topic clicks -rate 200 -n 400 -quiet
$S broker http://localhost:<leader> maintenance
$S offsets clicks -p 0
$S consume clicks -p 0 -offset 0      # OFFSET_OUT_OF_RANGE
```
**Expected**: maintenance reports deleted segments and a new
`log_start_offset`; reads below it fail with `OFFSET_OUT_OF_RANGE` and the
valid range. The active segment is never deleted.
(`TestRetentionAndCompaction`.)
**Principle**: retention deletes whole sealed segments from the start of the
log.
**Tradeoff**: segment granularity; disk vs replay window.

## Demo 14: Compaction

**Setup**
```
$S topics create profiles -p 1 -rf 3 cleanup_policy=compact segment_bytes=4096 max_event_bytes=1024
for i in $(seq 1 60); do printf "user-%d\tstate-%d\n" $((i%4)) $i; done | $S produce profiles
for i in $(seq 1 60); do printf "user-%d\tstate-%d\n" $((i%4)) $((i+100)); done | $S produce profiles
$S produce profiles -key user-3 null          # tombstone
$S broker http://localhost:<leader> maintenance
$S consume profiles -p 0
```
**Observed**
```
{'partition': 'profiles-0', 'compacted_segments': 2, 'records_removed': 117}
116 user-1 state-157
117 user-2 state-158
119 user-0 state-160
120 user-3 None              <- tombstone kept until tombstone_retention_ms
```
**Principle**: latest value per key survives; offsets preserved with gaps.
**Tradeoff**: compaction keeps state forever but costs rewrite I/O; it is
not retention.

## Demo 15: Load testing

**Setup**
```
for r in 1000 10000 100000; do
  bin/loadgen -topic lt$r -partitions 6 -rf 3 -rate $r -duration 10s -size 1024 -json lt-$r.json
done
```
**Observed** (sandbox, everything co-located on 2 vCPUs)

| target | acked/s | ack p50 / p99 ms | e2e p50 / p99 ms | max lag |
|---|---|---|---|---|
| 1,000 | 1,000 | 7.9 / 33.0 | 8.1 / 33.9 | 668 |
| 10,000 | 9,991 | 96.7 / 268.7 | 125 / 399 | 8,908 |
| 100,000 | 28,668 | 1,853 / 4,527 | 5,691 / 8,693 | 131,079 |

Replicated bytes were exactly 2.0x appended bytes in every run (RF 3).
**Principle**: throughput is bounded by the bottleneck resource (here CPU
in HTTP/JSON handling); beyond it, latency grows instead of throughput.
**Tradeoff**: see [performance.md](performance.md) for the profile and the
next optimizations.

## Cleanup

```
scripts/cluster.sh stop      # keep data
scripts/cluster.sh clean     # delete data
```
