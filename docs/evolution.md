# Design Evolution

Architecture should follow requirements. This document replays how the
system would grow from the earlier in-memory pub/sub project into the
current design, one requirement at a time, and what each step costs.

## From In-Memory Pub/Sub to Distributed Event Streaming

The earlier pub/sub project taught: fan-out to subscribers, subscriber
lifecycle (subscribe/unsubscribe, cleanup of dead subscribers), per-subscriber
buffering, the slow-subscriber problem, backpressure (block, drop, or
disconnect), and concurrency around a shared subscriber map.

Those lessons survive, but in different places:

| Pub/sub concept               | What it became here                                                              |
|-------------------------------|----------------------------------------------------------------------------------|
| fan-out to subscribers        | consumer **groups**; fan-out is free because each group reads the same log      |
| per-subscriber buffer         | the **log itself**; a slow consumer's backlog lives on disk, not in broker memory |
| slow subscriber: block/drop   | slow consumer: **lag grows**; nothing blocks producers, nothing is dropped until retention |
| subscriber lifecycle          | group **membership**, heartbeats, session timeouts, generations                 |
| message delivered = forgotten | message delivered = **offset advanced**; history retained for replay            |
| backpressure on publish       | backpressure only for **broker capacity** (rate limits, memory budget), not consumer speed |
| single process                | partitioned, **replicated**, failover with leader epochs                         |

The problems that had to be solved to get there, and that pub/sub never had:

1. **Durability**: messages must survive a process crash -> append-only log,
   checksums, crash recovery.
2. **Ordering at scale**: one ordered stream cannot scale -> partitions,
   per-key ordering, no global order.
3. **Progress tracking**: subscribers in pub/sub have no position -> offsets
   and committed offsets.
4. **Coordinated consumption**: several instances of one service must split
   work -> consumer groups and rebalancing.
5. **Machine loss**: one process means one failure domain -> replication,
   HWM, ISR, leader election, fencing.
6. **Bounded history**: a log grows forever -> retention and compaction.
7. **Shared infrastructure**: many teams on one cluster -> tenancy, quotas,
   ACLs.

---

## V1: single-node in-memory pub/sub

* **Problem**: deliver events from producers to several subscribers.
* **Architecture**: topic -> list of subscriber channels; publish writes to
  each channel.
* **Benefit**: microsecond latency, trivial code.
* **New complexity**: slow subscribers (bounded channel: block publisher or
  drop).
* **New failure modes**: process crash loses everything in flight; a
  subscriber that is down misses events forever; no replay.

## V2: durable append-only log

* **Problem**: events must survive restarts; a subscriber that was down
  must catch up.
* **Architecture change**: every event is appended to a file with a
  checksum; subscribers read from a position. (`internal/record`,
  `internal/segment`, `internal/log`.)
* **Benefit**: durability and catch-up; the log replaces per-subscriber
  buffers.
* **New complexity**: on-disk format, versioning, fsync policy, torn writes,
  segment files and indexes to avoid scanning, recovery on startup.
* **New failure modes**: corrupted or partially written records; disk full;
  fsync errors.

## V3: partitioned log

* **Problem**: one log is one disk write stream and one reader: it cannot
  scale beyond one machine's throughput.
* **Architecture change**: topics split into partitions; key-based routing.
  (`internal/routing`.)
* **Benefit**: parallel writes and reads; per-key ordering preserved.
* **New complexity**: choosing partition counts, partitioners, no global
  order.
* **New failure modes**: hot partitions; ordering surprises across
  partitions; remapping when partition counts change.

## V4: consumer offsets

* **Problem**: consumers must resume where they left off after a restart.
* **Architecture change**: committed offsets stored durably per
  (consumer, partition). (`coordinator/offsets.go`.)
* **Benefit**: resumable consumption; replay by moving the offset.
* **New complexity**: commit timing and delivery semantics (at-least-once
  vs at-most-once).
* **New failure modes**: duplicates after crashes; data loss with
  commit-before-processing; committed offsets deleted by retention.

## V5: consumer groups

* **Problem**: many instances of one service must share the work, each
  partition processed by exactly one of them.
* **Architecture change**: coordinator with membership, heartbeats,
  assignment, generations. (`internal/coordinator`.)
* **Benefit**: horizontal consumption with ordering intact.
* **New complexity**: rebalance protocol, session timeouts, fencing zombie
  members.
* **New failure modes**: rebalance storms; stop-the-world pauses; zombies
  committing stale offsets (prevented by generations).

## V6: replication

* **Problem**: losing a broker's disk loses data.
* **Architecture change**: each partition on RF brokers; leader/follower
  pull replication; HWM; ISR; ack modes. (`internal/partition`,
  `broker/replica.go`.)
* **Benefit**: survives machine loss.
* **New complexity**: deciding when a record is committed; follower lag;
  ISR membership; HWM visibility to consumers.
* **New failure modes**: slow followers stalling writes; under-replicated
  partitions; divergence between replicas.

## V7: leader election

* **Problem**: when a leader dies, someone must take over without losing
  committed data or creating two leaders.
* **Architecture change**: controller with broker liveness, ISR-based
  election, leader epochs, leases, epoch-based truncation.
  (`internal/controller`, `partition/epochs.go`.)
* **Benefit**: automatic failover in ~session timeout.
* **New complexity**: fencing, leases, clock assumptions, controller
  durability.
* **New failure modes**: split brain (prevented), stale leaders (fenced),
  unclean elections (opt-in data loss), and a new single point of failure:
  the controller.

## V8: retention

* **Problem**: logs grow without bound.
* **Architecture change**: segment-level deletion by time and size, bounded
  by the HWM. (`log.ApplyRetention`.)
* **Benefit**: bounded disk.
* **New complexity**: segment rolling by age, interaction with lagging
  consumers.
* **New failure modes**: consumers behind retention silently skipping data
  (made loud: `OFFSET_OUT_OF_RANGE` + warning).

## V9: compaction

* **Problem**: some topics represent current state per key (profiles,
  configuration); time retention would delete still-current values.
* **Architecture change**: key-based compaction with tombstones.
  (`log.Compact`.)
* **Benefit**: a compacted topic is a durable, replayable snapshot.
* **New complexity**: offset gaps, tombstone retention, rewrite with atomic
  swap, compaction I/O.
* **New failure modes**: crash mid-compaction (handled by discarding
  `.cleaned` files), compaction falling behind.

## V10: horizontal scaling

* **Problem**: more brokers, more partitions, more clients.
* **Architecture change**: partition-aware routing with metadata caches,
  metadata long-polling, batching and long polling everywhere, bounded
  buffers and backpressure, observability. (`pkg/client`,
  `internal/metadata`, `internal/observability`.)
* **Benefit**: throughput grows with partitions and brokers.
* **New complexity**: stale metadata, partition placement, rebalancing
  data between brokers ([scaling.md](scaling.md)).
* **New failure modes**: clients stuck on dead bootstrap brokers (found
  and fixed), thundering herds on metadata (single-flight cache).

## V11: multi-tenant architecture

* **Problem**: several teams share the cluster; one must not starve the
  others or read their data.
* **Architecture change**: authentication, ACLs, tenant ownership of topics,
  per-tenant/topic/producer quotas, partition quotas, audit logs.
  (`internal/auth`, `internal/ratelimit`.)
* **Benefit**: safe sharing.
* **New complexity**: identity and secret management, quota tuning.
* **New failure modes**: noisy neighbors on shared resources not covered by
  quotas (page cache, disk bandwidth); misconfigured ACLs.

## V12: multi-region architecture

* **Problem**: survive a region outage; serve users close to them.
* **Architecture change** (designed, not built): asynchronous cross-region
  mirroring, active-passive failover, offset translation.
  ([multi-region.md](multi-region.md).)
* **Benefit**: regional disaster recovery.
* **New complexity**: RPO/RTO tradeoffs, consumer offset mapping, data
  sovereignty.
* **New failure modes**: inter-region partitions, replication lag at
  failover, split-brain writes across regions.

## The pattern

Every step solved one concrete problem and introduced a new failure mode
that the next step had to address. A design that starts at V12 carries
eleven steps of complexity for requirements that may never arrive; one that
stops at V1 cannot be retrofitted with durability. The useful skill is
knowing which step the current requirements justify.
