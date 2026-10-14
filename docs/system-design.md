# System Design

This is the long-form design document. Each section states the problem,
what the code does, and where the code is. Tradeoff analysis lives in
[tradeoffs.md](tradeoffs.md); guarantees in [consistency.md](consistency.md).

Contents

1. [Topics and partitions](#1-topics-and-partitions)
2. [Partitioning strategies and hot partitions](#2-partitioning-strategies-and-hot-partitions)
3. [Storage engine](#storage-engine)
4. [Producers: batching, acks, idempotence](#4-producers-batching-acks-idempotence)
5. [Replication](#5-replication)
6. [Leader election, fencing and leases](#6-leader-election-fencing-and-leases)
7. [Consumers, groups and offsets](#7-consumers-groups-and-offsets)
8. [Replay](#8-replay)
9. [Retention and compaction](#9-retention-and-compaction)
10. [Backpressure, flow control and rate limiting](#10-backpressure-flow-control-and-rate-limiting)
11. [Batching and long polling](#11-batching-and-long-polling)
12. [Metadata caching and partition-aware routing](#12-metadata-caching-and-partition-aware-routing)
13. [Ordering guarantees](#13-ordering-guarantees)
14. [Multi-tenancy](#14-multi-tenancy)
15. [Tracing asynchronous flows](#15-tracing-asynchronous-flows)

---

## 1. Topics and partitions

A **topic** is a named stream with configuration
(`internal/metadata.TopicConfig`):

| Config                    | Default  | Meaning                                                    |
|---------------------------|----------|------------------------------------------------------------|
| partitions                | (create) | number of independent ordered logs                         |
| replication_factor        | (create) | copies of each partition                                   |
| min_insync_replicas       | 1        | ISR size required for `acks=all` writes                    |
| retention_ms / retention_bytes | 7d / unlimited | time and size retention (per partition)          |
| segment_bytes / segment_ms| 64 MiB / 1d | segment roll thresholds                                 |
| cleanup_policy            | delete   | `delete`, `compact`, or `compact,delete`                   |
| max_event_bytes           | 1 MiB    | per-record limit                                           |
| compression               | none     | only `none` is implemented                                 |
| unclean_leader_election   | false    | allow an out-of-sync replica to become leader              |
| tombstone_retention_ms    | 1d       | how long delete markers survive compaction                 |
| replica_lag_max_ms        | 10s      | a follower not caught up for this long leaves the ISR      |
| tenant                    | ""       | owning tenant (forced from the principal when auth is on)  |

**Why topic configuration is a scaling boundary.** Partition count is the
upper bound on consumer parallelism within a group and on how many brokers
can share a topic's write load; it is hard to change later because keys
remap (`hash(key) % n` changes with `n`). Replication factor multiplies
storage, disk writes and network. Retention determines how much disk the
topic occupies and how far back replay can go. Each of these is cheap to
choose and expensive to change on a topic holding terabytes, so topic
creation is effectively capacity planning.

A **partition** is one ordered, append-only sequence with partition-local
offsets:

```
orders-0:  0  1  2  3  4  5 ...
orders-1:  0  1  2 ...
orders-2:  0  1  2  3 ...
```

`orders-0` offset 3 and `orders-1` offset 3 are unrelated. There is no
topic-wide sequence. Partitions are the unit of ordering, replication,
leadership, consumer assignment, and parallelism.

Admin operations: create, describe, list, delete topics; alter config; add
partitions (`streamctl topics ...`, controller `/v1/topics`).

## 2. Partitioning strategies and hot partitions

`internal/routing`:

| Strategy        | Use                                  | Ordering                         | Load                   |
|-----------------|--------------------------------------|----------------------------------|------------------------|
| hash(key)       | default when a key is set            | per key (while partition count is fixed) | follows key distribution |
| round robin     | default for keyless records          | none between records             | even                   |
| explicit        | caller names the partition           | per partition                    | caller's responsibility |
| salted hash     | known hot keys (`routing.Salted`)    | lost for the salted key          | spreads the hot key over N partitions |
| consistent hash | `routing.Ring`, for comparison only  | per key                          | even with vnodes       |

The hash function (FNV-1a 32) is part of the contract: changing it remaps
every key, just like changing the partition count.

**Why key-based partitioning.** All events for `user_id=123` land in one
partition, which has one leader assigning offsets in arrival order, and one
consumer in each group. So "order.created" is always processed before
"order.paid" for that user without any cross-partition coordination.

**The fundamental tradeoff: ordering vs load distribution.** Ordering is
only defined inside a partition, so everything that must stay ordered must
share a partition, and therefore a single leader, a single disk write
stream, and a single consumer per group. If one key carries 70 percent of
traffic, 70 percent of traffic goes to one partition no matter how many
partitions exist (`TestHotKeySkew`; live run: `hot-0=11974` vs ~800 for each
other partition, see [demo.md](demo.md#demo-11-hot-partition)).

Mitigations, from least to most invasive:

1. **Better key selection**: key by `order_id` instead of `customer_id` if
   only per-order ordering is needed.
2. **Composite keys**: `customer_id:region` or `customer_id:day` when the
   ordering requirement is narrower than the key.
3. **Key salting**: `customer_id#0..N-1` (`routing.Salted`). The hot key
   spreads over N partitions; ordering for it is lost unless consumers
   re-sequence (for example by a per-key sequence number in the payload).
4. **Virtual partitions**: many logical sub-streams mapped onto fewer
   physical partitions, so remapping a hot sub-stream moves less.
5. **Partition splitting**: increase partition count. Keys remap, so
   ordering across the change is broken; consumers must drain the old
   layout first (or tolerate it).
6. **Producer-side load distribution**: spread a high-volume tenant's
   keyless or loosely ordered traffic round-robin.
7. **Dedicated partitions or topics** for high-volume tenants (explicit
   partitioning), which also isolates them from noisy-neighbor effects.

Detection: `broker_produce_records_total{topic,partition}` skew, a single
`partition_log_end_offset` racing ahead, one consumer's lag growing while
others idle. See [observability.md](observability.md#playbook-hot-partition).

## Storage engine

### Log, segments, index

```
Log (one per partition replica)                         internal/log
 |-- Segment base=0         sealed   [0 ... 183711]     internal/segment
 |     000...000.log       frames, append-only
 |     000...000.index     sparse: every ~4 KiB of log -> (offset, file position)
 |-- Segment base=183712   sealed
 |-- Segment base=367420   ACTIVE  (only segment written)
```

API (`internal/log.Log`): `Append(records)`, `AppendRaw(frames)`
(followers), `Read/ReadFrames(offset, maxOffset, maxBytes, maxRecords)`,
`ReadOne`, `Flush`, `Truncate`, `ResetTo`, `OffsetForTimestamp`,
`ApplyRetention`, `Compact`, and `Open` which performs recovery.

**Appends are sequential and batched.** A produce request becomes one
encoded buffer and one `pwrite` at the end of the active segment, never a
write per record. `BenchmarkAppend1KB` vs `BenchmarkAppendBatch100x1KB`
show the effect on the development sandbox (roughly 200 MB/s vs 510 MB/s;
the numbers are machine-specific, the ratio is the point).

**Why segments exist.**

* Retention deletes whole files instead of rewriting a huge one.
* Compaction rewrites one bounded file at a time and swaps it atomically.
* Recovery only needs to deep-scan the active segment.
* Index positions fit in 32 bits.
* Sealed segments are immutable, which makes concurrent reads simple.

Costs: more files and file descriptors (one open file per segment here),
and more index entries in memory. With 1 GiB segments and 20 TB per broker
that is ~20,000 open files: acceptable with a raised `ulimit -n`, but a
production system would close or memory-map cold segments lazily.

**Sparse index.** One entry (8 bytes) per ~4 KiB of log. Lookup = binary
search for the greatest entry <= target, then a forward scan of at most
~4 KiB. Tradeoffs:

| Denser index                      | Sparser index                         |
|-----------------------------------|---------------------------------------|
| faster lookups (shorter scan)     | slower lookups                        |
| more memory and larger files      | less memory                           |
| slower startup if rebuilt         | faster rebuild                        |

At 8 bytes per 4 KiB the index is ~0.2 percent of data. Known limitation:
indexes are held in memory for *all* segments; for 20 TB per broker that
would be ~40 GB, so production systems memory-map index files and let the
page cache keep only hot ones resident.

**Timestamps.** Broker log-append time, clamped to never decrease within a
partition. That makes `OffsetForTimestamp` a monotonic search (find the
first segment whose last timestamp >= T, scan it). There is no separate time
index; it would be the next optimization for replay-by-time on very large
partitions.

### Crash recovery

`log.Open` runs on every startup (`internal/log/log.go`):

1. **Discover segments** by file name; sort by base offset.
2. **Remove leftovers** of interrupted operations: `*.cleaned` (interrupted
   compaction), `*.tmp` (interrupted index write), index files without a log.
3. **Sealed segments**: load the persisted index and validate it
   (monotonic, positions inside the file). If valid, verify only the bytes
   after the last index entry; otherwise rebuild the index with a full scan.
   With `-verify-on-recovery`, verify every frame. Corruption inside a
   sealed segment is reported (`ErrCorrupt`), never silently truncated.
4. **Active segment**: verify every frame (length bounds, CRC, version,
   strictly increasing offsets). At the first bad frame, **truncate the
   tail** and fsync. This is the torn-write case.
5. **Rebuild indexes** where missing or invalid.
6. **Check segment continuity** (no overlaps).
7. **Restore the high-water mark** from the `high-watermark` checkpoint
   (clamped to the log range).
8. **Rebuild producer and leader-epoch state** by scanning record metadata.
9. **Rejoin the cluster**: the broker registers with a new *incarnation*;
   the controller treats a restart as a failure (moves leadership away,
   drops it from ISRs), and the replica catches up as a follower,
   truncating any suffix that diverged from the new leader.

**Logical vs physical recovery.**

* *Physical corruption recovery* is steps 2-6: making the bytes on disk
  self-consistent. It answers "which bytes are valid records?" and can
  only ever remove data (a torn tail).
* *Logical recovery* is steps 7-9: making this replica's history agree with
  the cluster's. It answers "which of my valid records are actually part of
  the committed history?" A physically perfect record that was appended by a
  deposed leader and never replicated is logically invalid, and is removed by
  epoch-based truncation when the replica follows the new leader.

Demo: [demo.md](demo.md#demo-8-broker-recovery) shows a SIGKILLed broker
with a deliberately torn tail truncating 62 bytes on restart and then
re-fetching the lost record from the leader.

**Durability of a crash.** A *process* crash loses nothing that reached the
kernel page cache. A *machine* crash (power loss) loses what was not
fsynced. Brokers fsync either on every append (`-fsync always`) or on an
interval (`-fsync interval -flush-interval 200ms`, default). With the
interval policy, durability of acknowledged writes comes from replication:
`acks=all` means every ISR replica has the record in its page cache, and
the cluster loses it only if all of them lose power within the flush
interval. See [tradeoffs.md](tradeoffs.md#35-storage-engine-choices).

**fsync failure.** A failed fsync takes the replica offline
(`partition.Flush` marks it failed) instead of retrying: after an fsync
error the kernel may have dropped the dirty pages, so the file's true state
is unknown. The broker reports the partition offline in its next heartbeat,
and the controller moves leadership (`TestDiskFailureMovesLeadership`).

## 4. Producers: batching, acks, idempotence

`pkg/client.Producer`:

* **Accumulator per partition**: records wait up to `Linger` (5 ms) or until
  `BatchMaxRecords`/`BatchMaxBytes`, then go as one request.
* **Bounded memory**: `MaxBufferedBytes` (32 MiB). `Send` blocks up to
  `MaxBlock` when full, then returns `ErrBufferFull`. Backpressure reaches
  the application instead of the heap.
* **One in-flight batch per partition**: retries can never reorder records.
* **Retries** with exponential backoff and jitter, honoring `Retry-After`,
  until `DeliveryTimeout` (30s). Leadership errors and transport errors
  invalidate the metadata cache.

**Acknowledgement modes** (`partition.Acks`):

| acks    | Returns when                                    | Survives                          | Latency      |
|---------|-------------------------------------------------|-----------------------------------|--------------|
| leader  | the leader appended (page cache)                | nothing beyond the leader         | lowest       |
| all     | the HWM passed the batch (every ISR member has it), ISR >= min_insync_replicas | loss of ISR-1 replicas | + one follower round trip |
| quorum  | a majority of *all* replicas have it            | see caveat                        | between      |

*If a producer is acknowledged before replication completes and the leader
dies:* with `acks=leader` the record may exist only on the dead leader. The
new leader (from the ISR) does not have it, and when the old leader returns
it truncates the record as divergent. The producer was told "success"; the
record is gone. That is the documented price of `acks=leader`.
`TestDivergentFollowerTruncatesUsingLeaderEpochs` demonstrates it.

*Quorum caveat:* `acks=quorum` acknowledges when a majority of replicas have
the record, but the controller elects from the ISR, which may contain a
replica that is lagging (within `replica_lag_max_ms`) and lacks it. A quorum
ack is only as strong as a quorum *election*. The mode exists to make that
point concrete; see [consistency.md](consistency.md#ack-modes).

**Idempotent production.** The failure it solves:

```
producer ---- batch(seq 40..49) ----> leader appends at offsets 900..909
producer <--X-- response lost --------
producer ---- retry batch(seq 40..49) -> leader: seq 40..49 <= lastSeq 49
                                          -> duplicate, return offsets 900..909
```

* **Producer ID** from the controller (`POST /v1/producer-ids`), persisted
  so IDs are never reused.
* **Sequence numbers** per (producer ID, partition), incremented per record.
* **Leader check** (`partition/producers.go`): a batch must start at
  `lastSeq + 1`. A batch entirely `<= lastSeq` is a duplicate; the last
  five batches per producer are remembered so the original offsets can be
  returned. A gap (`first > lastSeq + 1`) is `OUT_OF_ORDER_SEQUENCE`.
* **State survives failover and restart**: followers rebuild producer state
  from the `producerID`/`sequence` stored in each replicated record, and
  brokers rebuild it from the log on startup
  (`TestDedupStateFollowsLeadership`, `TestProducerStateSurvivesRestart`).
* **Expiration**: producer state idle longer than 15 minutes is dropped
  (bounded memory). A producer silent longer than that must start a new
  sequence space.
* **Unknown outcome**: if a batch exhausts its delivery timeout, the
  producer cannot know whether it was appended, so it takes a new producer
  ID for that partition. Ordering relative to the failed batch is not
  guaranteed across that boundary.

Measured: with 40 percent of produce responses dropped by fault injection,
200 sends produced exactly 200 records in order (`TestIdempotentProducerSurvivesLostResponses`);
the same run without idempotence produced duplicates (`TestNonIdempotentRetriesDuplicate`).

**This is not exactly-once processing.** Idempotence removes duplicates
created by producer retries within one producer session. It does nothing
about a consumer that processes a record and crashes before committing, or
an application that re-sends a business event with a new producer.

## 5. Replication

Leader/follower, pull-based. For partition 0 with replicas `[1, 2, 3]`:

```
Broker 1 (leader)        Broker 2 (follower)       Broker 3 (follower)
  log ... 97 98 99         log ... 97 98             log ... 97
            ^                        ^                          ^
           LEO=100                  LEO=99                     LEO=98
  HWM = min(100, 99, 98) = 98  (records 0..97 are committed)
```

* Followers loop `GET /internal/v1/replica/fetch?offset=<LEO>&last_epoch=<epoch of last record>&leader_epoch=<E>`.
  The leader long-polls (up to 500 ms) when the follower is caught up.
* The **fetch offset is the acknowledgement**: a follower asking for
  offset N has everything below N.
* Payload: raw frames, byte-identical to the leader's log. The follower
  verifies every CRC before appending.
* The leader tracks, per follower: LEO, last fetch time, last caught-up
  time (`partition.followerState`).

### High-water mark

```
                    log start                 HWM                     LEO
                       |                       |                       |
leader:                [ 0 ........................ 97 | 98  99 ]      100
                       |<--- committed: in every ISR -->|<- leader only ->|
                       |<--- visible to consumers ----->|

consumer group G:               committed offset = 60
                       [ 0 ......... 59 | 60 ............ 97 ]
                       |<- processed -->|<--- lag = 98 - 60 = 38 --->|
```

Three different numbers:

* **Log end offset (LEO)**: next offset the leader will assign. Includes
  records not yet replicated.
* **High-water mark (HWM)**: `min(LEO)` over the leader and every ISR
  member. Records below it are committed: they exist on every in-sync
  replica, so any clean leader election preserves them. Consumers can only
  read below the HWM, because records above it can still be truncated by a
  failover, and a consumer must never act on data that later "un-happens".
* **Consumer offset**: a consumer group's position, owned by the
  coordinator, unrelated to replication. Lag is `HWM - committed offset`.

Followers learn the HWM from fetch responses (capped at their own LEO) and
checkpoint it, so a broker that becomes leader starts with a sensible HWM.
On the leader the HWM never moves backwards.

### In-sync replicas

A follower is **in sync** if it has been fully caught up to the leader's
LEO within `replica_lag_max_ms` (default 10s). This "time since last caught
up" definition (rather than "N records behind") avoids flapping on bursty
traffic: a follower that is always a few hundred records behind during a
burst but catches up between bursts stays in sync.

* **Shrink**: the leader proposes removing a lagging follower; the
  controller accepts only from the current leader in the current epoch.
  The leader applies the shrink **only after** the controller accepts it.
  Otherwise: leader shrinks ISR, advances HWM past what follower F has,
  dies; controller (which still lists F in the ISR) elects F; committed
  records are lost. `TestISRShrinkAndExpand` checks that the HWM waits.
* **Expand**: a follower that has caught up to the HWM (and to the start of
  the current leader epoch) is counted immediately for HWM purposes
  ("maximal ISR", the conservative direction) and proposed to the
  controller.
* **Under-replicated**: ISR smaller than the replica set. Durability is
  reduced; with `min_insync_replicas` not met, `acks=all` writes are
  rejected with `NOT_ENOUGH_REPLICAS` rather than accepted with weaker
  durability than promised.

Demo: a follower slowed with `replica_fetch_delay_ms=2000` leaves the ISR
after `replica_lag_max_ms` and rejoins when the fault is cleared
(`TestSlowReplicaLeavesAndRejoinsISR`).

### Divergence and truncation (leader epochs)

Each record stores the epoch of the leader that appended it, and each
replica keeps the epoch history `[(epoch, start offset), ...]`. A follower
sends `last_epoch` (the epoch of its last record) with every fetch. The
leader answers "for that epoch, my log ends at offset X". If the follower's
log extends past X in that epoch, it truncates to X and retries.

```
A (old leader, epoch 1):  0..9 (epoch 1, committed)  10..14 (epoch 1, acks=leader, never replicated)
B (new leader, epoch 2):  0..9 (epoch 1)             10..12 (epoch 2, new writes)

A fetches with last_epoch=1, offset=15
B: "epoch 1 ends at offset 10 in my log"   -> A truncates to 10
A fetches offset=10, last_epoch=1          -> receives 10..12 (epoch 2); logs converge
```

This is more precise than "truncate to the high-water mark on becoming a
follower", which in some double-failure sequences either discards committed
records or keeps divergent ones. `TestDivergentFollowerTruncatesUsingLeaderEpochs`
and `TestNetworkPartitionFencesStaleLeader` exercise it.

A follower whose fetch offset is below the leader's log start (the leader
already deleted what it needs) resets its log to the leader's log start
(`TestFollowerBehindLogStartResets`).

## 6. Leader election, fencing and leases

**What is implemented** (`internal/controller`): a single controller process
that detects broker failure by missed heartbeats and reassigns leadership.
It is **not** a consensus system: there is one controller, its state is a
local file, and if it is down no elections happen.

Election when a broker fails (session timeout, restart with a new
incarnation, or reported disk failure):

1. **Detect**: no heartbeat for `session-timeout` (10s default).
2. **Eligible replicas**: live members of the partition's ISR, excluding
   replicas the broker reported offline.
3. **Select**: the first eligible replica in replica order (so the
   preferred replica wins when healthy).
4. **Update metadata**: increment `leader_epoch`, set leader, drop dead
   members from the ISR, persist (`cluster.json`, atomic rename + fsync),
   bump metadata version.
5. **Redirect**: brokers learn within milliseconds (metadata long-poll) and
   change roles; clients get `NOT_LEADER` (with a leader hint) or a
   connection error, invalidate their cache, and retry.
6. **Resume**: the new leader serves reads and writes; followers truncate
   divergent suffixes and catch up.

No ISR member alive: the partition goes **offline** (leader -1) and stays
unavailable until an ISR member returns, unless `unclean_leader_election`
is set, in which case any live replica is elected and acknowledged writes
may be lost. The controller logs unclean elections at error level and counts
them in `controller_leader_elections_total{reason="unclean"}`.

**Split brain and how it is prevented here.** Two mechanisms:

* **Fencing by leader epoch.** Every replica rejects requests carrying an
  older epoch: followers will not fetch from (or accept data from) a leader
  in an older epoch, and the controller will not accept ISR changes from a
  leader in an older epoch. A deposed leader cannot get its writes replicated
  or committed.
* **Leases.** A broker may act as leader only while its lease is valid.
  Each heartbeat grants a lease of `lease` (6s), measured by the broker
  from when it *sent* the heartbeat using the monotonic clock. The
  controller waits `session-timeout` (10s) after the last heartbeat it
  *received*. Since send happens before receive and lease < session, the
  old leader stops accepting writes before the controller can elect a new
  one, assuming clock *rates* (not wall clocks) differ by less than the
  4s margin over 10s. When the lease lapses, writes fail with
  `LEASE_EXPIRED`.

What remains: a window of up to the lease duration in which an isolated
leader accepts `acks=leader` writes that will later be truncated. `acks=all`
writes in that window cannot complete because no follower replicates them.
`TestNetworkPartitionFencesStaleLeader` walks through exactly this.

**How production systems do it.** They store the controller's state in a
replicated log driven by a consensus protocol (Raft in Kafka's KRaft and in
etcd; Zab in ZooKeeper). A quorum of controller nodes agrees on every epoch
change, so there is never more than one controller and never two leaders for
the same epoch, even if a controller node is partitioned away. Leader
election for partitions is then a metadata write through that log. This
project's controller is the single-node version of that design: the epoch
discipline is the same, the replication of the controller is missing.
See [distributed-consistency.md](distributed-consistency.md).

## 7. Consumers, groups and offsets

**Reading.** `GET /v1/topics/{t}/partitions/{p}/events?offset=&max_events=&max_bytes=&wait_ms=`
returns committed records (below the HWM). Single-record reads are
`max_events=1`. `OFFSET_OUT_OF_RANGE` (416) carries the valid range.

**Groups** (`internal/coordinator`). A group is a set of members sharing
the partitions of their subscribed topics; each partition has one owner per
group. Different groups are independent: `fraud-detector` and `analytics`
each receive every record.

```
group fraud-detector, topic orders (6 partitions), range assignor:
  fraud-1 -> P0, P1
  fraud-2 -> P2, P3
  fraud-3 -> P4, P5
```

**Membership and rebalancing (eager).**

```
Stable(gen 4) -- join / leave / session expiry / subscription change -->
PreparingRebalance:
   heartbeats answer REBALANCE_IN_PROGRESS
   members stop fetching, (optionally) commit, call Join again
   Join blocks until all members rejoined (or rebalance timeout kicks stragglers)
   and InitialRebalanceDelay (300 ms) has passed, coalescing bursts of joins
--> Stable(gen 5) with a new assignment returned to every member
```

* Assignors: `range` (default), `roundrobin`, `sticky` (keeps previous
  ownership where balanced; `TestStickyMinimizesMovement` shows 0 surviving
  partitions moved vs 2 for range when one of four members leaves).
* **Fencing**: commits carry the generation; a member that missed a
  rebalance gets `ILLEGAL_GENERATION` and its commit is rejected, so a
  paused "zombie" cannot overwrite the new owner's progress. Commits are also
  checked against ownership.
* **Session timeout** (10s default) bounds how long a crashed consumer's
  partitions sit idle. **Max poll interval** (5 min) makes a consumer that
  is alive but stuck (not calling `Poll`) leave the group.

**Eager vs cooperative.** Eager rebalancing revokes every partition from
every member, then reassigns: the whole group stops consuming for the
duration (stop-the-world), even for partitions that end up with the same
owner. Cooperative rebalancing revokes only the partitions that move, in
two rounds; unaffected partitions keep flowing. This project implements
eager plus a sticky assignor (the prerequisite: you can only avoid revoking
what you know will not move). See [tradeoffs.md](tradeoffs.md#15-eager-vs-cooperative-rebalancing).

**Rebalance storms.** Every membership change stops the group. If members
time out *because* of the rebalance (long `OnRevoke` work, slow restarts,
session timeout shorter than a GC pause, a deploy restarting members one by
one), each rebalance triggers the next. Symptoms: `group_rebalances_total`
climbing, `group_rebalancing=1` most of the time, lag growing with healthy
CPUs. Mitigations: the initial rebalance delay (coalesces joins;
`TestRebalanceStormCoalescing`), sticky assignment, leaving explicitly on
shutdown instead of waiting for session expiry, generous session timeouts,
static membership (not implemented), cooperative rebalancing.

**Offsets.**

| Term             | Meaning                                                         |
|------------------|-----------------------------------------------------------------|
| fetch position   | where the fetcher will read next (prefetched, client-only)      |
| current position | offset after the last record returned by `Poll` (client-only)   |
| committed offset | durable group position = last processed + 1 (coordinator)       |
| replay offset    | committed offset moved backwards by an admin reset              |

Committed offsets are stored by the coordinator in an append-only file of
JSON lines (`consumer-offsets.log`), replayed at startup and compacted as it
grows. It is a small event log of consumer progress.

Commit strategies (`cmd/consumer -commit ...`):

| Strategy                 | Crash after processing, before commit | Crash after commit, before processing | Result       |
|--------------------------|---------------------------------------|---------------------------------------|--------------|
| commit after processing  | record redelivered                    | n/a                                   | at-least-once |
| commit before processing | n/a                                   | record never processed                | at-most-once |
| auto-commit (periodic)   | up to one interval redelivered        | n/a, if processing finishes before the next `Poll` | at-least-once |

**Delivery semantics.** The default is **at-least-once processing**: process,
then commit. A crash between the two redelivers the batch
(`TestCrashBeforeCommitReprocesses`). Committing first turns the same crash
into data loss. Applications make at-least-once safe with idempotent
processing:

* a natural idempotency key (`order_id`) with an upsert or unique constraint;
* storing the processed offset in the same transaction as the side effect
  (and seeding the consumer position from it on startup);
* a dedup table of `(topic, partition, offset)` with a TTL.

## 8. Replay

Replay is what separates a log from a queue: history is still there.

* **From an offset**: `GET ...?offset=N`, or `streamctl consume -offset N`.
* **From a timestamp**: `GET .../offsets?timestamp=ms` gives the first
  offset at or after T; `groups reset G -to timestamp -time 2026-09-01T00:00:00Z`
  moves a whole group.
* **Reset a group**: `earliest`, `latest`, `offset`, `timestamp`. Only for a
  group with no active members (`GROUP_NOT_EMPTY` otherwise), because
  rewinding under a running consumer would race its commits.
* **Consume history**: a new group with `AutoOffsetReset=earliest`.

Uses: recovering from a consumer bug (fix, reset, reprocess); debugging
(replay the exact inputs that caused a failure); rebuilding a materialized
view or search index from scratch; bootstrapping a brand-new consumer;
migrating data to a new store by replaying into it while the old path
keeps running.

Caveats: replay reprocesses side effects (consumers must be idempotent or
replay into an isolated environment); replay range is bounded by retention;
compacted topics replay the latest value per key, not every change.

## 9. Retention and compaction

**Retention** (`log.ApplyRetention`, run by the broker maintenance loop or
`streamctl broker URL maintenance`):

* time: delete a sealed segment once its newest record is older than
  `retention_ms`;
* bytes: delete the oldest sealed segment while the partition would still
  be at or above `retention_bytes` without it;
* never deletes the active segment, never deletes past the HWM, and only
  ever removes a prefix (offsets stay contiguous);
* segment-granular: data lives up to `retention + segment age` (the
  `segment_ms` roll makes low-volume partitions eligible eventually).

*Retention vs deletion:* retention is policy-driven expiry of old history;
it never targets individual records. Deleting a specific record (for
example a user's data) needs compaction with tombstones, or crypto-shredding
(encrypt per user, delete the key).

*Disk pressure:* retention by bytes caps a partition, but a full disk still
fails appends. The broker exposes `partition_size_bytes`; operators should
alert well before the disk fills (see [failure-modes.md](failure-modes.md#disk-full)).

**Compaction** (`log.Compact`, topics with `cleanup_policy=compact`):

```
before:  0:user-1=A  1:user-2=X  2:user-1=B  3:user-1=C  4:user-2=null(tombstone)  5:user-3=Q
after:   3:user-1=C  4:user-2=null  5:user-3=Q            (offsets preserved, gaps allowed)
later:   3:user-1=C  5:user-3=Q                            (tombstone expired)
```

* Builds `key -> latest offset` over the whole log, rewrites each sealed
  segment keeping only latest-per-key records, swaps files with an atomic
  rename (crash-safe: leftovers are discarded on recovery).
* Tombstones survive `tombstone_retention_ms` so lagging consumers still
  see the delete.
* Ordering: surviving records keep their offsets and relative order;
  per-key, the latest value is never reordered against older ones because
  older ones are gone.
* Compaction is not retention: retention removes *old* data; compaction
  removes *superseded* data and keeps a full snapshot of current state
  regardless of age.
* Cost: a full scan of the log per pass plus a rewrite of every sealed
  segment that has garbage; memory O(distinct keys).

Simplified relative to production compactors: no dirty-ratio scheduling,
no bounded-memory offset map with multiple passes, no I/O throttling, no
merging of small segments, and compaction runs inline in the maintenance
loop. It is correct, not efficient.

## 10. Backpressure, flow control and rate limiting

No buffer in the system is unbounded:

| Buffer                          | Bound                                  | When full                           |
|---------------------------------|----------------------------------------|-------------------------------------|
| producer accumulator            | `MaxBufferedBytes` (32 MiB)            | `Send` blocks, then `ErrBufferFull` |
| broker produce memory           | `-max-inflight-produce-bytes` (64 MiB) | 503 `BROKER_OVERLOADED`, Retry-After |
| broker request concurrency      | `-max-concurrent-requests`             | 503 immediately (load shedding)     |
| broker long-poll waiters        | `MaxLongPollWaiters`                   | fetch returns immediately, no wait  |
| replication fetch               | `ReplicaFetchMaxBytes` (1 MiB)         | follower gets the rest next fetch   |
| consumer fetch                  | `max_bytes`, `max_events`              | rest on next fetch                  |
| consumer prefetch               | one batch per partition + `QueuedBatches` | fetchers block (stop fetching)   |
| producer dedup state            | 5 batches per producer, TTL            | oldest dropped                      |
| rate limiter buckets            | idle buckets cleaned                   | -                                   |

**Producer rate > broker capacity.** Producer accumulators fill, requests
queue, broker in-flight bytes fill. The broker sheds with 503/429 and a
Retry-After; the producer backs off; its buffer fills; `Send` blocks; the
application slows. Each stage converts excess load into *waiting at the
edge* rather than memory growth in the middle. The loadgen run at a
100k/s target on a 2-vCPU sandbox shows the signature: acknowledged rate
plateaued (~28.7k/s) and latency climbed into seconds while memory stayed
bounded and no request failed ([performance.md](performance.md#measured-runs)).

**Producer rate > consumer rate.** This is *not* backpressure to producers:
the log decouples them. The log absorbs the difference on disk and consumer
lag grows (demo: 150/s in, ~50/s out, lag grew ~100/s). Retention is the
real limit: if lag grows past retention, unread data is deleted and the
consumer gets `OFFSET_OUT_OF_RANGE` (logged loudly, then the reset policy
applies). The responses are adding consumers (up to the partition count),
making processing faster, or increasing partitions.

**Slow consumer and memory.**

```
slow consumer -> buffers grow -> memory pressure -> GC pressure -> latency -> process failure
```

The consumer client breaks this chain at the first arrow: each partition
fetcher holds at most one batch and blocks on a bounded queue until `Poll`
takes it. A slow application simply stops fetching; the data waits on the
broker's disk, where it costs nothing.

**Rate limiting** (`internal/ratelimit`, from the earlier token-bucket
project, extended): byte-rate token buckets at broker, tenant, topic and
producer scope, checked all-or-nothing (a request rejected by one scope
does not consume tokens from the others; `TestAllOrNothingAcrossScopes`).
Rejected requests get 429 with `Retry-After` computed from the bucket
deficit; the producer honors it (`TestRateLimitingBackpressure`).

Why broker-level limits are needed even with client-side limits: client
limits are advisory and per-client. A misconfigured or malicious client
ignores them, a thousand well-behaved clients still sum to an overload, and
only the broker sees the aggregate it must protect.

## 11. Batching and long polling

**Batching** improves throughput by amortizing per-request costs:

* fewer syscalls (one write per batch, one read per fetch);
* fewer network round trips and HTTP/JSON envelopes;
* sequential, larger disk writes;
* better compression ratios (if compression were implemented);
* serialization and checksum setup amortized.

Cost: latency. A record waits up to `Linger` for its batch, and a large
batch takes longer to transmit, append and replicate. Throughput vs
latency is the fundamental dial: small linger for interactive traffic,
larger batches for pipelines. Oversized batches also increase memory per
request and the blast radius of a retry. Benchmark:
`BenchmarkAppend1KB` vs `BenchmarkAppendBatch100x1KB`.

**Long polling.** Instead of `poll, poll, poll` against an empty partition,
`wait_ms=5000` parks the request on the partition's notification channel
until the HWM moves or the timeout passes (`TestLongPollDeliversPromptly`).

* *Connection utilization*: one request per idle consumer per wait period
  instead of one per poll interval.
* *Latency*: data is delivered as soon as it is committed, not at the next
  poll tick.
* *Server resources*: each parked request holds a goroutine (a few KB) and
  a connection. Bounded by `MaxLongPollWaiters`; beyond that, fetches
  return immediately rather than tying up more resources.
* *Timeout handling*: the server caps `wait_ms` (30s); clients set their
  HTTP timeout above it.
* *Cancellation*: a disconnecting client cancels the request context; the
  waiter exits immediately. Broker shutdown cancels all waiters.

Replication uses the same mechanism (leader parks follower fetches).

## 12. Metadata caching and partition-aware routing

**What is cached**: topic metadata, partition leadership, broker URLs
(`metadata.Cache`, TTL 10s in clients), consumer group coordinator address
(in metadata). **Event data is not cached by the application**: the OS page
cache already keeps recent segments in memory, and tail reads hit it.

* **TTL** bounds staleness when nothing goes wrong.
* **Invalidation on error**: `NOT_LEADER`, `LEASE_EXPIRED`,
  `UNKNOWN_TOPIC`, and transport errors invalidate the cache immediately.
* **Single flight**: concurrent misses share one refresh.
* **Stale on failure**: if refresh fails, stale metadata is served; the
  broker will correct a wrong guess.
* **Correctness never depends on freshness**: brokers check leadership and
  epochs on every request. Stale metadata costs a round trip, never data.
* **Broker list learned from metadata**: clients fetch metadata from any
  live broker they know about, not only the bootstrap list. A bug here was
  found by the live failover demo (a client whose only bootstrap broker was
  the one killed never learned the new leader) and is covered by
  `TestClientSurvivesLossOfItsOnlyBootstrapBroker`.
* Brokers learn metadata by long-polling the controller, so a leader change
  reaches every broker within one round trip.

**Partition-aware routing instead of round robin.** A request for
`orders-2` goes straight to the broker leading `orders-2`. Round-robin
across brokers would send 2/3 of requests (with 3 brokers) to a non-leader,
which must either reject (extra round trip) or forward (extra hop, extra
bandwidth on the broker network, and the forwarding broker's CPU and
memory spent on someone else's traffic).

**Where a generic load balancer is still useful**: in front of stateless
gateways (`cmd/gateway`), for bootstrap/metadata requests, and for admin
and controller endpoints. The gateway itself is partition-aware behind the
load balancer.

## 13. Ordering guarantees

* **Guaranteed**: records in one partition are stored, replicated and
  delivered in offset order. Records produced by one producer to one
  partition appear in send order, including across retries (one in-flight
  batch per partition plus sequence numbers).
* **Per key**: same key -> same partition while the partition count is
  unchanged -> ordered.
* **Not guaranteed**: any order across partitions, across topics, or across
  producers (two producers writing the same key interleave by arrival at the
  leader); order across a partition-count increase for keys that move;
  order across a producer ID reset after a failed batch.

Example: `user-1: created(p3,o7)`, `user-2: created(p0,o2)`. A consumer of
p0 may process user-2's event before a consumer of p3 processes user-1's,
even though user-1's was produced first. That is fine if no business rule
spans users.

**Why not global ordering?** A total order needs one sequencer: every write
in the topic funnels through one leader's log append, one disk, one
replication stream, and every consumer group processes the topic with one
consumer. Throughput is then bounded by a single machine and a single
consumer, which removes the scalability that partitioning exists to provide.
Systems that need a global order use a single partition (and accept its
throughput) or impose order downstream for the subset of events that need
it (for example with per-entity sequence numbers or timestamps plus a
reorder buffer).

## 14. Multi-tenancy

* Topics have an owning **tenant**; with auth enabled it is taken from the
  creating principal.
* **Tenant isolation in authorization**: a principal can only access its own
  tenant's topics unless an ACL names the topic explicitly; a wildcard ACL
  never crosses tenants (`TestTenantIsolation`).
* **Quotas implemented**: per-tenant, per-topic, per-producer and broker-wide
  produce byte rates; per-tenant partition quota at topic creation
  (`-max-partitions-per-tenant`); broker-wide concurrent request limit.
* **Quotas designed but not implemented**: per-tenant fetch bandwidth (would
  delay fetch responses rather than reject them), per-tenant connection
  limits (needs connection-level identity), per-tenant disk quotas.

**The noisy-neighbor problem.** Tenants share brokers, disks, page cache,
network and the controller. One tenant's traffic spike can raise everyone's
latency (queueing on the same disk and CPU), evict everyone's hot data from
the page cache (a tenant replaying a month of history), or exhaust shared
limits (in-flight bytes, request slots). Quotas convert "one tenant's spike
becomes everyone's outage" into "one tenant gets 429s". The remaining shared
resources (page cache, disk bandwidth, controller) are why large
deployments also isolate big tenants physically: dedicated brokers or
clusters.

## 15. Tracing asynchronous flows

`internal/observability/tracing.go` implements W3C trace context with spans
exported as structured log lines (same data model as OpenTelemetry; an
OTLP exporter would replace one function).

```
producer.send (span)  --traceparent header--> broker.produce (child span)
       |
       +-- traceparent stored as a record header
                                   ... seconds, hours, or a replay week later ...
consumer.process (new trace) --link--> producer span
```

Why asynchronous tracing is harder than request/response tracing:

* **No single request**: the producer's request ends when the record is
  acknowledged; consumption happens later, possibly much later, in another
  process. A parent/child tree would have parents that ended long before
  their children started.
* **Fan-out**: one record is consumed by every group, so it has many
  "children".
* **Fan-in**: one consumer batch contains records from many traces.
* **Replay**: the same record may be processed many times, months apart.
* **Sampling**: sampling decisions made at produce time must be carried in
  the data, and a sampled producer trace says nothing about which
  consumptions are interesting.

So consumer spans start new traces and **link** to the producer span
(`Span.AddLink`), and record the queueing time
(`now - record.Timestamp`), which is the number that explains most
end-to-end latency in practice.
