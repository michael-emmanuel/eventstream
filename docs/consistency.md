# Consistency Model

"Eventually consistent" is not a description. This document states, for
each piece of state, who owns it, how it changes, what readers can observe,
and what happens during failover.

## Summary table

| State                         | Owner / source of truth                    | Consistency                                                                 | Can be stale?                         |
|-------------------------------|--------------------------------------------|-----------------------------------------------------------------------------|---------------------------------------|
| Partition log contents < HWM  | ISR replicas                               | **Linearizable per partition** (single writer, epoch-fenced, read from leader) | No, for reads served by the current leader |
| Partition log contents >= HWM | leader only                                | Tentative: may be truncated                                                 | n/a (never shown to consumers)        |
| High-water mark               | leader (followers learn it lazily)          | Monotonic on the leader; follower copy lags                                 | Follower copy, briefly                |
| Leader epoch, leader, ISR     | controller (`cluster.json`)                | Strongly consistent *only while there is one controller* (single process)   | Broker and client caches: yes         |
| Broker/client metadata cache  | copy of controller state                    | Eventually consistent, bounded by long-poll (brokers) / TTL or error (clients) | Yes, by design                      |
| Producer dedup state          | partition replicas (rebuilt from records)   | Same as log: replicated with the records it describes                       | Recent sequences of a timed-out batch |
| Consumer committed offsets    | coordinator (`consumer-offsets.log`)        | Strongly consistent (single writer, fsynced, generation-fenced)             | Client's in-memory position: by design |
| Consumer group membership     | coordinator                                 | Strongly consistent; members learn changes on next heartbeat               | Member's view: up to one heartbeat    |

## Producer acknowledgement

An acknowledgement is a promise about which failures the record survives.

<a id="ack-modes"></a>

| acks     | Promise when the producer receives success                                         | Not promised                                              |
|----------|------------------------------------------------------------------------------------|-----------------------------------------------------------|
| `leader` | The leader appended the record (in its page cache).                                | Survival of leader loss. The record may be truncated later. |
| `all`    | Every member of the ISR (which had >= `min_insync_replicas` members) has the record, and it is below the HWM, so it is committed. | Survival of loss of *all* ISR members within the fsync interval (page cache only). |
| `quorum` | A majority of all assigned replicas have the record.                              | Survival of failover: the controller elects from the ISR, which may contain a lagging replica that lacks the record. |

An error is not a promise either way. In particular `REPLICATION_TIMEOUT`
and transport errors mean **unknown**: the record may be in the log and may
become committed. The idempotent producer resolves "unknown" by retrying
with the same sequence numbers: the leader returns the original offsets if
the batch was appended, or appends it if not. Without idempotence,
"unknown" resolves to "maybe duplicated".

## Leader state

The leader is the only writer for its partition. It assigns offsets
under the partition's append lock, so offset order is arrival order.
Every append is stamped with the leader's epoch. A leader accepts writes
only while:

* its role is leader for the latest epoch it has seen,
* its lease from the controller is valid,
* its local storage has not failed,
* for `acks=all`, the ISR has at least `min_insync_replicas` members.

## Follower state

A follower's log is a prefix of the leader's log *for the epochs they share*,
plus possibly a divergent suffix written by a previous leader. The divergent
suffix is always above the cluster's HWM (it was never replicated to the
ISR), and it is removed by epoch-based truncation before the follower
appends anything new. Followers never serve consumers.

## High-water mark

* Defined as `min(LEO)` over the leader and the ISR.
* Monotonic on any given leader.
* The new leader after a clean election has every record below the old
  HWM (it was in the ISR). Its own HWM may start lower than the old
  leader's (followers learn the HWM one fetch late), so for a short time
  after failover consumers may see the end of the log "pause"; it never
  moves backwards past committed data, and no committed record disappears.
* After an **unclean** election this no longer holds: committed records
  may be lost and consumers may get `OFFSET_OUT_OF_RANGE` for offsets they
  already consumed. That is why unclean election is off by default.

## Consumer-visible events

A consumer reading from the current leader sees exactly the committed
prefix of the log, in order, with no gaps except those created by
compaction. Two consumers reading the same partition see the same records
at the same offsets. Once a record at offset N has been returned to any
consumer, every later read of offset N (by anyone, after any clean
failover) returns the same record.

What a consumer can observe that may surprise it:

* **Redelivery** after a rebalance or crash (at-least-once).
* **Stale reads from a deposed leader**: a leader isolated from the cluster
  keeps serving reads of its committed prefix until it learns of the new
  epoch. Those reads are of committed data (never of data that will be
  truncated), but they are not the latest: writes accepted by the new
  leader are invisible to it. Reads are not lease-gated in this design.
* **Lag spikes during failover** (HWM pause above).
* **`OFFSET_OUT_OF_RANGE`** when retention deleted the committed position.

## Consumer committed offsets

* Written only by the coordinator, under its lock, fsynced before the
  commit is acknowledged.
* Fenced: a commit is accepted only from a current member, in the current
  generation, for partitions it owns. After a rebalance, the previous owner's
  late commits are rejected.
* Read by members at the start of each generation.
* The client's in-memory position is ahead of the committed offset by
  design; the difference is what gets redelivered after a crash.

## Metadata consistency

* **Controller**: the single writer; changes are persisted before they are
  published. Version numbers are strictly increasing.
* **Brokers**: apply snapshots in version order, ignore older versions, and
  ignore leader epochs older than the one they already have, so out-of-order
  delivery cannot resurrect an old leader.
* **Clients**: cache with TTL; correctness is enforced by brokers, not by the
  cache. Stale metadata costs latency, never correctness.

## What happens during failover

<a id="failover"></a>

Timeline for a leader crash (defaults: heartbeat 1s, lease 6s, session 10s):

```
t=0      leader L crashes
t=0..10  writes to the partition fail (connection refused / NOT_LEADER);
         producers retry and buffer; consumers of this partition stall;
         other partitions are unaffected
t~10     controller declares L dead, elects F from the ISR, epoch E -> E+1,
         persists, publishes metadata version v+1
t~10+    F applies BecomeLeader(E+1): records its epoch start offset
         other followers fetch from F with last_epoch; any divergent
         suffix is truncated
         producers/consumers refresh metadata on their next retry
t~10+δ   writes resume; HWM advances once ISR members fetch from F
later    L restarts with a new incarnation: recovers its log, follows F,
         truncates anything it had above F's epoch boundary, catches up,
         rejoins the ISR; preferred election can move leadership back
```

Guarantees across the failover:

| Record was ...                                  | After clean failover                    |
|-------------------------------------------------|-----------------------------------------|
| acknowledged with `acks=all`                    | present                                 |
| below the HWM (visible to any consumer)         | present, same offset                    |
| acknowledged with `acks=leader`, not replicated | lost (truncated when L returns)         |
| in flight, no response                          | unknown; idempotent retry resolves it   |
| committed consumer offset                       | unaffected (coordinator state)          |

Measured on the local cluster: SIGKILL of a partition leader while
producing 2,000 records at 200/s with `acks=all`: 2,000 records in the
topic afterwards, 37 retries, 0 errors, 0 duplicates
([demo.md](demo.md#demo-6-leader-failure)).

## What is strongly consistent, and what is not

**Strongly consistent (linearizable)**:
* appends to a partition and reads of committed records from its current
  leader;
* leader epoch assignment (single controller);
* consumer offset commits and group generations (single coordinator).

**Eventually consistent (converges, bounded staleness)**:
* broker and client metadata caches;
* follower copies of the log and of the HWM;
* the ISR as seen by brokers versus the controller (bounded by the ISR
  loop interval and metadata propagation).

**Can become stale**:
* any client's idea of who leads a partition;
* reads served by a leader that has been deposed but not yet informed;
* lag numbers (computed from a moment-in-time HWM and committed offset);
* the HWM checkpoint on disk (rewritten every 2 seconds).

**Not provided at all**:
* ordering across partitions;
* exactly-once processing or transactions across partitions;
* durability across loss of every ISR replica within the fsync interval
  (with the default interval fsync policy);
* controller fault tolerance: the controller is a single process with no
  consensus. While it is down, no failover happens, and after
  `lease` seconds all brokers stop accepting writes. That is a deliberate
  choice of consistency over availability; see
  [distributed-consistency.md](distributed-consistency.md).
