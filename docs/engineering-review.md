# Engineering Review

This document outlines how to present the system architecture at three levels of technical depth. It details 65 core review topics specifying what the review panel evaluates, the ideal architectural approach grounded in this codebase, and advanced follow-up considerations. It also highlights common anti-patterns to avoid along with their respective trade-offs, concluding with a staff-level technical discussion.

Contents: [A. 30 seconds](#a-30-second-explanation) ·
[B. 2 minutes](#b-2-minute-explanation) ·
[C. 10 minutes](#c-10-minute-architecture-walkthrough) ·
[D. Components](#d-component-deep-dives) ·
[E. Storage](#e-storage-deep-dives) ·
[F. Replication](#f-replication-deep-dives) ·
[G. Partitioning](#g-partitioning-questions) ·
[H. Consumer groups](#h-consumer-group-questions) ·
[I. Failure modes](#i-failure-mode-questions) ·
[J. Consistency](#j-consistency-questions) ·
[K. Performance](#k-performance-questions) ·
[L. Scaling](#l-scaling-questions) ·
[M. Tradeoffs](#m-tradeoff-questions) ·
[N. Staff-level](#staff-level-design-discussion)

---

## A. 30-second explanation

"It's a distributed event log, built from scratch in Go: a simplified
Kafka-style system. Topics are split into partitions; each partition is an
append-only, segmented, checksummed log on disk, replicated to followers
with a leader per partition. Producers get idempotent retries through
producer IDs and sequence numbers; consumers read by offset, form groups
that split partitions, and can replay history. A controller handles
failure detection and leader election with epochs and leases. I built it to
make the failure modes visible: there are fault-injection hooks, failure
tests, and docs on what is and isn't guaranteed. It deliberately has no
consensus layer; the controller is a single node, and I can explain exactly
what that costs."

## B. 2-minute explanation

1. **Why a log**: a queue forgets a message once it is processed; a log
   keeps an ordered history and consumers just hold a position. That gives
   replay, many independent consumer groups, and rebuilding state from
   history.
2. **Partitioning**: one log cannot scale, so topics have partitions;
   ordering is per partition; keys route related events to the same
   partition. No global order: that is the price of parallelism.
3. **Storage**: each partition is segment files with a sparse offset index.
   Writes are batched appends; recovery verifies CRCs and truncates torn
   tails; retention deletes whole segments; compaction keeps the latest
   value per key.
4. **Replication**: followers pull raw frames from the leader. The
   high-water mark is the minimum log end across in-sync replicas;
   consumers only see below it. `acks=all` waits for the HWM.
5. **Failover**: the controller detects dead brokers by heartbeat, elects
   a new leader from the ISR, and bumps a leader epoch that every replica
   checks. Brokers hold leases shorter than the controller's timeout, so an
   isolated leader stops writing before a replacement starts. Divergent
   suffixes are truncated using epoch history stored in each record.
6. **Clients**: the producer batches per partition, keeps one batch in
   flight, and retries with the same sequence numbers so the leader
   deduplicates. Consumers are at-least-once: commit after processing.
7. **Evidence**: integration tests kill leaders, partition the network,
   drop responses, fail disks; a load generator and pprof show the
   bottleneck is HTTP/JSON per record, not the storage engine.

## C. 10-minute architecture walkthrough

Use [architecture.md](architecture.md) as the whiteboard. Suggested flow:

1. **Requirements and the log/queue distinction** (1 min). Replay, multiple
   groups, ordering, durability.
2. **Data model** (1 min). Partition-local offsets; the binary frame with
   CRC, producer ID, sequence, leader epoch; JSON on the wire vs binary on
   disk; why followers can copy bytes.
3. **Write path** (2 min). Client partitioner -> leader via cached
   metadata -> auth -> memory budget -> token buckets -> dedup check -> one
   `pwrite` -> wait for HWM if `acks=all`. Name each backpressure point.
4. **Replication** (2 min). Pull fetch, fetch offset as ack, HWM, ISR by
   time-since-caught-up, shrink only with controller approval and why.
5. **Failure** (2 min). Leader crash timeline; leases vs session timeout;
   epoch fencing; epoch-based truncation example (old leader's
   `acks=leader` writes disappear).
6. **Read path and groups** (1 min). Long-poll fetch below HWM; eager
   rebalance with generations; commit-after-processing; lag = HWM -
   committed.
7. **What I'd change for production** (1 min). Raft controller, binary
   batch protocol + compression, sharded coordinator, mmap indexes, tiered
   storage, cooperative rebalancing.

Pause after step 5 for questions: failure handling is where the
conversation usually goes.

## D. Component deep dives

- **Broker** (`internal/broker`): hosts replicas; HTTP handlers for
  produce/fetch/replica fetch/admin; loops for heartbeat (lease), metadata
  long-poll (role changes), ISR proposals, fsync, HWM checkpoints,
  maintenance; one fetcher goroutine per followed partition. Every loop has
  an owner, a context and a WaitGroup.
- **Partition** (`internal/partition`): the replication state machine.
  Role, epoch, ISR, follower progress, HWM, producer table, epoch cache.
  Two locks: append lock (log mutations + dedup state), state lock (never
  held across I/O).
- **Log / segment / record**: storage engine; see section E.
- **Controller** (`internal/controller`): broker sessions, elections, ISR
  changes, topic CRUD, producer IDs. Single process, `cluster.json` written
  atomically.
- **Coordinator** (`internal/coordinator`): groups, assignors, generations,
  offset store as an append-only JSON-lines log.
- **Client** (`pkg/client`): metadata cache (single-flight, stale on error,
  learns brokers), idempotent batching producer, group consumer with
  bounded prefetch and heartbeat thread.
- **Cross-cutting**: rate limiter (token buckets, all-or-nothing), fault
  injector, auth (tokens, ACLs, tenants), observability (Prometheus text,
  W3C trace context, slog).

## E. Storage deep dives

### Q22. What happens if a broker crashes during an append?

- **Testing**: understanding of torn writes and recovery.
- **Strong answer**: The append is one `pwrite` of a batch of frames at the
  end of the active segment. A crash can leave a prefix of that buffer on
  disk: the last frame may be partial. The in-memory size is published only
  after the write returns, so no reader ever saw it. On restart, `log.Open`
  fully verifies the active segment: length bounds, CRC, version,
  increasing offsets; at the first bad frame it truncates and fsyncs. A
  partially written record was never acknowledged by this replica (leaders
  respond after the append returns; followers acknowledge on their next
  fetch), so nothing acknowledged is lost by the truncation. Demo 8 shows
  62 bytes truncated and the record re-fetched from the leader.
- **Follow-up**: power loss is different: an acknowledged record that was
  only in the page cache can be lost or torn on _this_ disk. Durability
  then comes from other ISR replicas, or from `-fsync always`.
- **Weak answer**: "the write is atomic" (multi-KB writes are not atomic
  across power loss) or "we replay a write-ahead log" (the log _is_ the WAL).
- **Tradeoff**: fsync per append (safe, ~100x slower in the benchmark) vs
  interval fsync + replication.

### Q23. How do you recover an incomplete log segment?

- **Testing**: concrete recovery algorithm; physical vs logical recovery.
- **Strong answer**: Scan frames from the start (or from the last valid
  index entry for sealed segments). Stop at the first frame that fails
  bounds, CRC, version or offset monotonicity. For the active segment,
  truncate there. For a sealed segment, refuse to truncate (that would
  create an offset gap followed by valid segments) and report corruption;
  the fix is re-replication. Then rebuild missing or invalid indexes,
  delete leftover `.cleaned`/`.tmp` files from interrupted operations,
  restore the HWM from its checkpoint, rebuild producer and epoch state by
  scanning record metadata. That is physical recovery. Logical recovery
  happens when the replica rejoins: it fetches with its last epoch and
  truncates any suffix the new leader does not have.
- **Follow-up**: Why not trust the index for the active segment? It may be
  stale or missing (persisted only on seal/close).
- **Weak answer**: "delete the corrupt segment" (loses valid data and
  breaks offsets).
- **Tradeoff**: full verification of all segments at startup (slow,
  `-verify-on-recovery`) vs active segment + sealed tails (fast).

### Q24. Why use segment files?

- **Testing**: file-level reasoning for retention and recovery.
- **Strong answer**: Retention becomes deleting files; compaction rewrites
  one bounded file and swaps it atomically; recovery deep-scans only the
  active segment; sealed segments are immutable so readers need no
  coordination with writers; index positions fit in 32 bits.
- **Follow-up**: cost? More open files (one per segment here) and more
  index entries; production systems open cold segments lazily.
- **Weak answer**: "to keep files small".
- **Tradeoff**: small segments (fine-grained retention, many files) vs
  large (few files, coarse retention).

### Q25. Why use an index?

- **Testing**: read path cost.
- **Strong answer**: Consumers start at arbitrary offsets. Without an index
  you scan the segment from the beginning (up to 64 MiB here) for every
  fetch. With the sparse index, binary search gives a file position within
  ~4 KiB of the target, then a short scan.
- **Follow-up**: why not compute position from offset? Records are
  variable length, and compaction creates gaps.
- **Weak answer**: "databases have indexes".
- **Tradeoff**: memory and write cost vs lookup cost.

### Q26. Why a sparse index?

- **Testing**: space/time tradeoff.
- **Strong answer**: Offsets are monotonic, so a sorted sample is enough:
  one 8-byte entry per ~4 KiB is ~0.2 percent of data, and the worst-case
  scan is ~4 KiB, which is one buffered read. A dense index would be ~1
  percent or more for small records with no practical speedup.
- **Follow-up**: limitation here: all indexes live in memory. At 30 TB per
  broker that is ~59 GB; production mmaps index files.
- **Weak answer**: "to save space" with no lookup-cost reasoning.
- **Tradeoff**: index size vs lookup speed vs startup rebuild time.

### Q27. Why batch requests?

- **Testing**: throughput economics.
- **Strong answer**: Per-request costs (HTTP, JSON, auth, syscall, lock,
  fsync) dominate small records. Batching amortizes them: the benchmark
  shows ~1.8x raw append throughput for batches of 100 vs single records,
  and the bigger savings are network round trips and HTTP overhead.
  Producers linger 5 ms per partition; under load batches grow on their own
  because records accumulate while a batch is in flight.
- **Follow-up**: why does that make overload graceful? Larger batches mean
  fewer requests for the same bytes, so throughput degrades less.
- **Weak answer**: "fewer network calls" only.
- **Tradeoff**: throughput vs latency (linger).

### Q28. What happens if batches become too large?

- **Testing**: second-order effects.
- **Strong answer**: Latency (waiting to fill, longer transfer and
  replication), memory per request (request size limit 8 MiB, in-flight
  budget 64 MiB), GC pressure, head-of-line blocking (one in-flight batch
  per partition), larger retry blast radius, and fetch responses that
  exceed consumer limits. The broker rejects requests over the size limit;
  the reader always returns at least one record so an oversized record
  cannot wedge a consumer.
- **Follow-up**: how to pick sizes? From latency SLO: linger <= budget for
  batching; size so a batch transmits in a few ms on the network.
- **Weak answer**: "bigger is always better".
- **Tradeoff**: throughput vs tail latency and memory.

## F. Replication deep dives

### Q7. What happens if a partition leader dies?

- **Testing**: end-to-end failover reasoning.
- **Strong answer**: Writes fail (connection refused); producers retry and
  buffer. After the session timeout the controller declares the broker
  dead, picks the first live ISR member, increments the leader epoch,
  persists, and publishes metadata. The new leader records the epoch start
  offset; followers fetch with their last epoch and truncate anything
  beyond the epoch boundary; clients refresh metadata and resume. Records
  acked with `acks=all` survive; `acks=leader` records not yet replicated
  are lost. Measured: 2,000 records at 200/s through a SIGKILL of the
  leader, 2,000 in the topic, 0 duplicates.
- **Follow-up**: what if no ISR member is alive? Partition offline; unclean
  election only if configured, and it may lose committed data.
- **Weak answer**: "a follower takes over" with no mention of epochs, ISR,
  or what is lost.
- **Tradeoff**: session timeout: fast failover vs false positives.

### Q8. How do replicas know what they have committed?

- **Testing**: HWM propagation.
- **Strong answer**: Only the leader knows directly: committed = below the
  HWM = min LEO over the ISR, learned from follower fetch offsets. Followers
  learn the HWM in fetch responses, capped at their own LEO, and checkpoint
  it. A follower's HWM lags by one fetch, which is fine because followers
  do not serve consumers.
- **Follow-up**: what does a new leader do with records above its HWM? It
  keeps them; they become committed once followers have them. Followers
  with extra records beyond the new leader's log truncate.
- **Weak answer**: "every replica tracks commits" (no protocol).
- **Tradeoff**: lazy HWM propagation (cheap) vs extra round trips.

### Q9. What is the high-water mark?

- **Testing**: precise definitions.
- **Strong answer**: The offset below which records exist on every
  in-sync replica. It differs from the log end offset (next offset the
  leader will assign, includes unreplicated records) and from consumer
  offsets (group positions). Consumers only read below it so they never
  see a record that a failover could remove.
- **Follow-up**: can it go backwards? Not on a leader. After failover the
  new leader's starts at its own last-known value, which can briefly be
  lower, so consumers see a pause, never committed records disappearing.
- **Weak answer**: "the latest offset".
- **Tradeoff**: visibility latency (one extra round trip) vs never exposing
  uncommitted data.

### Q32. How does replication affect throughput?

- **Testing**: resource accounting.
- **Strong answer**: Leader egress grows by (RF-1) x ingress (measured
  exactly 2.0x at RF 3), disk writes by RF x, and `acks=all` latency
  includes a follower round trip. Leader CPU rises for serving follower
  fetches (~15 percent of broker CPU in the profile). Per-partition
  throughput is unchanged; cluster capacity per broker drops.
- **Follow-up**: how to reduce? Compression, larger batches, rack-aware
  placement to keep replication off scarce links.
- **Weak answer**: "it doesn't, it's async".
- **Tradeoff**: durability vs bandwidth.

### Q33. How does replication affect storage?

- **Testing**: arithmetic.
- **Strong answer**: Linear: RF copies. 1 GB/s ingest at RF 3 is 259
  TB/day. Retention multiplies it. Unlike erasure coding, replication
  does not reduce overhead with size; tiered storage moves old data to
  object storage that does its own (cheaper) redundancy.
- **Follow-up**: why not erasure-code the log locally? Reconstruction cost
  on the hot path; fine for cold tiers.
- **Weak answer**: no number.
- **Tradeoff**: RF vs cost.

### Q34. How would you choose a replication factor?

- **Testing**: mapping failure tolerance to config.
- **Strong answer**: From the failures to survive _with writes available_
  and _without data loss_: RF 3 + min ISR 2 survives one failure with
  writes, two without loss (writes stop). RF 5 + min ISR 3 for two
  failures with writes. RF 1 for re-derivable data. Also consider
  rolling restarts (a planned outage counts against the budget) and
  failure domains (replicas in different zones).
- **Follow-up**: why not RF 2? With min ISR 2 any failure stops writes;
  with min ISR 1 a second failure can lose acknowledged data.
- **Weak answer**: "3 because that's the default".
- **Tradeoff**: cost vs availability vs durability.

### Q35. What is an in-sync replica?

- **Testing**: ISR semantics and why time-based.
- **Strong answer**: A replica that has been fully caught up to the
  leader's LEO within `replica_lag_max_ms`. The HWM waits for ISR members
  only, so the ISR defines both durability (who has committed data) and
  latency (who `acks=all` waits for). Membership changes go through the
  controller and only the current leader in the current epoch may change it.
- **Follow-up**: why time-based instead of "N records behind"? Bursty
  traffic makes healthy followers transiently many records behind;
  time-since-caught-up does not flap.
- **Weak answer**: "replicas that are alive".
- **Tradeoff**: small lag threshold (low latency, frequent shrinks) vs
  large (stable, slow writes when a follower is slow).

### Q36. What happens when a follower falls behind?

- **Testing**: interplay of ISR, HWM and controller.
- **Strong answer**: While it is in the ISR, the HWM waits for it, so
  `acks=all` latency rises. After `replica_lag_max_ms` the leader proposes
  a shrink; it applies it only after the controller accepts, then the HWM
  advances. The follower keeps fetching; once caught up to the HWM it is
  counted again immediately and proposed for re-entry. If it falls behind
  the leader's log start (retention), it resets and re-copies.
- **Follow-up**: why must the shrink wait for the controller? Otherwise a
  leader could commit records the controller's ISR member lacks, die, and
  the controller would elect that member: committed data lost.
  (`TestISRShrinkAndExpand` checks the HWM waits.)
- **Weak answer**: "it gets kicked out" with no ordering of steps.
- **Tradeoff**: brief write stall vs safety.

### Q37. What is split brain?

- **Testing**: definition and consequences.
- **Strong answer**: Two nodes both believe they are the leader for the
  same data and both accept writes, creating two divergent histories that
  cannot be merged for an ordered log. Here it would be two leaders for one
  partition accepting writes concurrently.
- **Follow-up**: how is it prevented here? Lease < session timeout (old
  leader stops before new one starts) plus epoch fencing (nothing from the
  old epoch can be replicated or committed).
- **Weak answer**: "when the network splits".
- **Tradeoff**: preventing it costs availability during partitions.

### Q38. How would you prevent stale leaders?

- **Testing**: fencing mechanisms.
- **Strong answer**: Monotonic epochs checked by every participant
  (followers reject fetch responses and requests from older epochs; the
  controller rejects ISR changes from older epochs; brokers ignore metadata
  with older epochs), plus a lease the leader must hold to write, measured
  with a monotonic clock from when the heartbeat was sent. A stale leader
  can still accept a write in the window (acks=leader), but that write is
  never committed and is truncated on rejoin.
- **Follow-up**: what if the process pauses between the lease check and the
  append? It can append once; epochs make that append harmless.
- **Weak answer**: "use a heartbeat".
- **Tradeoff**: lease length: shorter = smaller window but more
  sensitivity to controller hiccups.

### Q39. What is fencing?

- **Testing**: the general concept.
- **Strong answer**: Making a deposed actor's actions ineffective rather
  than relying on it to stop. Typically a monotonically increasing token
  (epoch) attached to every request, and every resource rejects tokens
  older than the newest it has seen. Leader epochs here; generations for
  consumer groups (a zombie consumer's commits are rejected).
- **Follow-up**: why are leases alone insufficient? Pauses and clock-rate
  drift: the old leader may not know its lease expired.
- **Weak answer**: "killing the old node" (STONITH is one form, not the
  definition).
- **Tradeoff**: every resource must check the token.

### Q40. Why isn't leader election the same as consensus?

- **Testing**: depth on distributed agreement.
- **Strong answer**: Choosing a leader is easy for one decider. Consensus
  is making several deciders agree on one value per epoch despite crashes,
  lost messages and pauses. Here, one controller decides, so there is
  nothing to agree on, and that controller is a single point of failure.
  Adding a second controller without consensus would allow two
  simultaneous decisions for the same epoch: split brain at the metadata
  level. Raft gives a replicated controller whose decisions are unique per
  term.
- **Follow-up**: which guarantees need consensus? Unique leader per epoch
  across controller failures, fault-tolerant ISR membership, producer ID
  uniqueness after controller failover.
- **Weak answer**: "they're the same thing" or "Raft elects leaders".
- **Tradeoff**: consensus complexity vs single point of failure.

## G. Partitioning questions

### Q1. Why is this a log rather than a queue?

- **Testing**: the core idea.
- **Strong answer**: A queue tracks whether each message has been
  processed and deletes it when it has. A log keeps an ordered history for
  a retention period, and consumers only hold a position. That enables
  replay, many independent consumer groups at no extra cost, rebuilding
  state, and per-partition ordering. Consumer progress is one number per
  partition, not a flag per message.
- **Follow-up**: what does a log lose? Per-message acks and redelivery of
  one failed message; parallelism beyond partition count.
- **Weak answer**: "logs are faster".
- **Tradeoff**: history and ordering vs per-message work distribution.

### Q2. Why use partitions?

- **Testing**: scaling model.
- **Strong answer**: One log has one leader, one write stream and one
  consumer per group; partitions are independent logs, so writes, storage,
  replication and consumption parallelize across brokers and consumers.
  They are also the unit of ordering and failover.
- **Follow-up**: what limits partition count? Metadata size, files and
  memory per partition, election time on failover.
- **Weak answer**: "for parallelism" without what is lost.
- **Tradeoff**: parallelism vs global ordering and per-partition overhead.

### Q3. Why not have one global ordering?

- **Testing**: understanding the cost of total order.
- **Strong answer**: A total order needs a single sequencer: one leader's
  append path, one replication stream, and one consumer per group for the
  whole topic. Throughput would be capped at one machine. Most business
  rules need order per entity, not globally, so per-partition ordering with
  key routing gives the useful guarantee at a fraction of the cost.
- **Follow-up**: what if some events truly need a global order? Single
  partition for that stream, or ordering downstream by sequence number.
- **Weak answer**: "it's hard".
- **Tradeoff**: ordering scope vs scalability.

### Q4. How do you preserve ordering?

- **Testing**: end-to-end ordering, including retries.
- **Strong answer**: Same key -> same partition (while partition count is
  fixed). The leader assigns offsets in arrival order under one append
  lock. The producer keeps one batch in flight per partition, so a retry
  can never overtake a later batch, and sequence numbers stop a retried
  batch from being appended twice. Consumers process one partition in one
  thread (one owner per group). Commit order follows offset order.
- **Follow-up**: what breaks it? Increasing partitions; producer ID reset
  after a failed batch; multiple producers for one key; parallel processing
  inside a consumer.
- **Weak answer**: "Kafka guarantees order" (only per partition).
- **Tradeoff**: one in-flight batch limits per-partition throughput.

### Q5. What happens if one key becomes extremely hot?

- **Testing**: skew handling.
- **Strong answer**: All its records go to one partition: one leader, one
  disk stream, one consumer per group. Adding partitions does not help.
  Demo: 70 percent hot key -> 74 percent of records on one of 6 partitions.
  Mitigations: change the key if ordering is narrower than the key, composite
  keys, salting (`routing.Salted`, gives up per-key order), dedicated
  partitions/topics, or faster processing for that partition.
- **Follow-up**: how to keep order with salting? Per-key sequence numbers in
  the payload and a reorder buffer at the consumer.
- **Weak answer**: "add partitions".
- **Tradeoff**: ordering vs load distribution.

### Q6. Why use key-based partitioning?

- **Testing**: rationale.
- **Strong answer**: It co-locates related events so their order is
  preserved without coordination, and keeps per-key state (aggregations,
  caches) local to one consumer. It's stateless: any client computes
  `hash(key) % n`.
- **Follow-up**: why does the hash function matter? Different clients
  must agree; changing it remaps everything.
- **Weak answer**: "to distribute load" (round robin distributes better).
- **Tradeoff**: locality and order vs balance.

## H. Consumer group questions

### Q10. What is consumer lag?

- **Testing**: the main health metric.
- **Strong answer**: How far a group's committed position is behind the
  readable end of a partition: `HWM - committed offset`, summed or per
  partition. It measures staleness of downstream state.
- **Follow-up**: lag in records vs in time? Time is what users feel;
  `lag / rate` or the timestamp of the record at the committed offset.
- **Weak answer**: "how slow the consumer is".
- **Tradeoff**: committed-offset lag lags reality by the commit interval.

### Q11. How do you calculate consumer lag?

- **Testing**: precise inputs.
- **Strong answer**: Per partition: HWM from the leader minus the group's
  committed offset; if no commit, from log start; if the commit is below log
  start (deleted by retention), from log start. Implemented in
  `controller.Lag`, which queries leaders in parallel.
- **Follow-up**: why HWM and not LEO? Records above the HWM cannot be read
  yet; counting them would blame consumers for replication delay.
- **Weak answer**: "LEO minus current offset".
- **Tradeoff**: point-in-time; two different reads, not atomic.

### Q12. Why do consumers use offsets?

- **Testing**: position vs per-message state.
- **Strong answer**: In an ordered log, everything before the position is
  done; one number per partition captures progress, can be stored cheaply,
  moved for replay, and compared to the HWM for lag.
- **Follow-up**: how does that interact with parallel processing inside a
  consumer? You can only commit the lowest contiguous completed offset.
- **Weak answer**: "to know where to start".
- **Tradeoff**: no per-message ack (a poison message blocks the partition).

### Q13. Why not delete an event after consumption?

- **Testing**: decoupling.
- **Strong answer**: There is no single "the consumer": many groups read
  the same data at different speeds, new consumers start later, and replay
  needs history. Deleting would couple storage to the slowest or newest
  consumer. Retention decouples storage lifetime from consumption.
- **Follow-up**: what about privacy deletion? Compaction with tombstones,
  or crypto-shredding.
- **Weak answer**: "because disk is cheap" (true but beside the point: the
  reason is that consumption is not a single event).
- **Tradeoff**: disk vs replay capability.

### Q14. How does replay work?

- **Testing**: mechanics.
- **Strong answer**: Fetch from an older offset; for a group, stop
  consumers and reset committed offsets to earliest, a specific offset, or
  the first offset at or after a timestamp (broker log-append time,
  monotonic per partition, so a forward search works). The coordinator
  refuses resets while members are active.
- **Follow-up**: replay on a compacted topic? You see the latest value per
  key, not every change.
- **Weak answer**: "re-send the events".
- **Tradeoff**: replay re-executes side effects.

### Q15. Why is replay useful?

- **Testing**: product thinking.
- **Strong answer**: Fix a consumer bug and reprocess; debug with the exact
  input; rebuild a search index or materialized view; bootstrap a new
  service from history; migrate to a new database by replaying into it
  while the old path runs.
- **Follow-up**: how long should retention be? Long enough for the longest
  recovery or rebuild you want to support.
- **Weak answer**: "for backups".
- **Tradeoff**: retention cost.

### Q16. What does at-least-once mean here?

- **Testing**: precision.
- **Strong answer**: Every committed record is processed by the group at
  least once; after a crash or rebalance, records processed since the last
  commit are processed again. Achieved by committing after processing.
  (`TestCrashBeforeCommitReprocesses`.)
- **Follow-up**: and on the produce side? Retries plus idempotence give
  "each send appears once" per producer session.
- **Weak answer**: "messages might be duplicated" without the mechanism.
- **Tradeoff**: duplicates vs loss.

### Q17. Why can't you guarantee exactly-once processing?

- **Testing**: honesty about distributed atomicity.
- **Strong answer**: Exactly-once processing needs the side effect and the
  offset commit to be atomic. When the side effect is in another system
  (database, email, payment API), the log cannot make that atomic. Even
  with transactions inside the log (not implemented here), external
  effects need idempotence. The achievable property is "effectively once":
  at-least-once delivery plus idempotent processing.
- **Follow-up**: how does Kafka's EOS work? Transactional produce +
  transactional offset commit, for read-process-write within Kafka only.
- **Weak answer**: "use transactions".
- **Tradeoff**: application complexity vs platform complexity.

### Q18. How do you make consumers idempotent?

- **Testing**: practical patterns.
- **Strong answer**: Natural keys with upserts/unique constraints; store
  the processed offset in the same transaction as the output and resume
  from it; dedup table keyed by `(topic, partition, offset)` or business
  ID with a TTL; design commands as set-state rather than increments.
- **Follow-up**: increments (counters)? Store per-source sequence numbers
  and apply only newer ones.
- **Weak answer**: "check if processed" without where that state lives.
- **Tradeoff**: extra storage and writes.

### Q19. What happens if the consumer crashes after processing?

- **Testing**: commit timing.
- **Strong answer**: If it crashed after processing but before committing:
  the next owner resumes from the last commit and reprocesses. If it
  crashed after committing: nothing is redone.
- **Follow-up**: how much is redone? Up to one commit interval or batch.
- **Weak answer**: "data is lost".
- **Tradeoff**: commit frequency vs commit cost.

### Q20. What happens if it crashes before committing?

- **Testing**: same, from the other side.
- **Strong answer**: With commit-after-processing: redelivery (duplicates).
  With commit-before-processing: the commit already moved the position past
  records it never processed: data loss for that group. The demo consumer
  supports both (`-commit after|before`).
- **Follow-up**: when is commit-before acceptable? Metrics or sampling
  where loss is cheaper than duplicates.
- **Weak answer**: "it restarts from the beginning".
- **Tradeoff**: at-least-once vs at-most-once.

### Q21. When should offsets be committed?

- **Testing**: judgment.
- **Strong answer**: After the side effects of the records are durable;
  per batch, not per record (coordinator fsync per commit); also in the
  revoke callback before a rebalance; and periodically for idle partitions.
  Auto-commit here only commits records returned by an earlier poll, so it
  is at-least-once if processing finishes before the next poll.
- **Follow-up**: parallel processing inside a consumer? Commit the lowest
  offset below which everything is done.
- **Weak answer**: "every few seconds" without what that means.
- **Tradeoff**: duplicates after crash vs commit load.

### Q29. Why long polling?

- **Testing**: request efficiency.
- **Strong answer**: Short polling either wastes requests (tight loop) or
  adds latency (sleep). Long polling parks the fetch until data commits or
  a timeout: near-zero latency and few requests. Same mechanism for
  replication. Waiters are bounded and cancel on disconnect.
- **Follow-up**: costs? A goroutine and connection per parked request;
  timeouts must align across proxies.
- **Weak answer**: "it's like websockets".
- **Tradeoff**: connections held vs latency/requests.

### Q30. How does backpressure work?

- **Testing**: system-wide flow control.
- **Strong answer**: Bounded buffers at every hop, each with an explicit
  overflow behavior: producer buffer (block then `ErrBufferFull`), broker
  in-flight bytes (503 + Retry-After), token buckets (429 + Retry-After),
  request concurrency (503), long-poll waiters (return immediately),
  bounded consumer prefetch (fetchers block). Overload propagates back to
  the producing application instead of growing memory. Measured: at 3.5x
  capacity, rate plateaued, latency grew, memory stayed flat, 0 errors.
- **Follow-up**: why doesn't a slow consumer backpressure producers? The
  log decouples them; lag grows instead.
- **Weak answer**: "drop messages when full".
- **Tradeoff**: rejection vs queueing.

### Q31. How do you prevent an overloaded consumer from exhausting memory?

- **Testing**: client-side flow control.
- **Strong answer**: Fetch size limits (`max_events`, `max_bytes`), at
  most one prefetched batch per partition, a bounded queue to `Poll`;
  fetchers block when the queue is full. Memory is bounded by
  partitions x batch size + queue, independent of backlog. The backlog
  stays on the broker's disk.
- **Follow-up**: what if processing is asynchronous inside the app? Bound
  in-flight records there too (semaphore) and commit only completed
  prefixes.
- **Weak answer**: "add more memory".
- **Tradeoff**: prefetch depth (throughput) vs memory.

### Q44. How do consumers react to partition reassignment?

- **Testing**: rebalance protocol.
- **Strong answer**: Heartbeat returns REBALANCE_IN_PROGRESS; on next poll
  the consumer stops fetchers, discards prefetched batches, commits (still
  allowed in the old generation), runs the revoke callback, and rejoins.
  Join returns the new generation and assignment; positions come from
  committed offsets; fetchers restart. A consumer that misses this gets
  ILLEGAL_GENERATION on commit and rejoins.
- **Follow-up**: what if it is stuck processing? Max poll interval: it
  leaves the group itself.
- **Weak answer**: "it gets new partitions".
- **Tradeoff**: eager simplicity vs pause.

### Q45. What causes rebalance storms?

- **Testing**: feedback loops.
- **Strong answer**: Membership changes that cause more membership
  changes: session timeouts shorter than GC pauses or processing time,
  rolling deploys restarting members one by one (each restart = two
  rebalances), crash loops, slow revoke callbacks pushing others past
  rebalance timeouts. Mitigations: initial rebalance delay (coalescing,
  `TestRebalanceStormCoalescing`), graceful leave, longer session timeouts,
  sticky/cooperative assignment, static membership.
- **Follow-up**: how to detect? `group_rebalances_total` rate,
  `group_rebalancing` near 1, lag rising with idle CPUs.
- **Weak answer**: "too many consumers".
- **Tradeoff**: detection speed vs stability.

## I. Failure-mode questions

### Q41. What happens during a network partition?

- **Testing**: CAP reasoning with specifics.
- **Strong answer**: Walk the isolated-leader scenario: within its lease
  it can accept `acks=leader` writes (at risk), `acks=all` cannot complete;
  after the lease it refuses writes; after the session timeout the
  controller elects a new leader in a higher epoch; on heal, the old leader
  truncates divergent records. Reads from the isolated leader are stale but
  committed. The side with the controller wins.
  (`TestNetworkPartitionFencesStaleLeader`.)
- **Follow-up**: controller on the minority side? The minority keeps the
  cluster; a Raft controller fixes this by requiring a quorum.
- **Weak answer**: "CAP says pick two".
- **Tradeoff**: write availability vs consistency during partitions.

### Q43. What happens if metadata is stale?

- **Testing**: cache correctness.
- **Strong answer**: A client sends to the wrong broker; the broker
  rejects with NOT_LEADER and a leader hint, or the connection fails; the
  client invalidates its cache and refreshes from any live broker it knows.
  Brokers ignore older metadata versions and older epochs. Staleness costs
  latency, never correctness, because the authority re-checks.
- **Follow-up**: real bug found: client only knew one bootstrap broker,
  which died; fixed by learning brokers from metadata.
- **Weak answer**: "use a short TTL".
- **Tradeoff**: TTL vs metadata load.

### Q58. How would you debug rising consumer lag?

- **Testing**: structured investigation.
- **Strong answer**: Input rate up or output rate down? Then: downstream
  dependency latency; partition skew (one partition or all); rebalances;
  broker fetch latency; consumer CPU high (processing cost) vs low (waiting);
  errors/poison records; finally whether more consumers can help (only if
  members < partitions and the bottleneck is per-consumer). Full playbook
  in [observability.md](observability.md#playbook-consumer-lag-increased-20x).
- **Follow-up**: when do more consumers make it worse? Shared downstream
  already saturated.
- **Weak answer**: "add consumers".
- **Tradeoff**: n/a, method.

### Q59. How would you detect a hot partition?

- **Testing**: metrics design.
- **Strong answer**: Per-partition produce rate
  (`broker_produce_records_total{partition}`), LEO growth per partition,
  per-partition lag, one broker's resource use. Then sample records to find
  the key.
- **Follow-up**: alert on skew: max/median partition rate above a ratio.
- **Weak answer**: "check broker CPU".
- **Tradeoff**: per-partition labels increase metric cardinality.

### Q60. How would you investigate a sudden increase in p99 latency?

- **Testing**: tail latency debugging.
- **Strong answer**: Which operation and which acks; all partitions or
  some (slow follower or leader); disk fsync p99; page-cache eviction from a
  replaying consumer; GC pauses and CPU; shrinking batch sizes; throttling;
  network. Then profile with pprof. Correlate with deploys and ISR changes.
- **Follow-up**: why p99 and not average? Averages hide head-of-line
  blocking from one slow replica.
- **Weak answer**: "scale up".
- **Tradeoff**: n/a.

## J. Consistency questions

### Q42. What consistency guarantees does the system provide?

- **Testing**: precision instead of slogans.
- **Strong answer**: Per partition, reads from the leader below the HWM are
  linearizable: single writer, fenced by epoch, records below the HWM never
  disappear after a clean election. `acks=all` writes are durable against
  ISR-1 failures. Consumer commits and generations are strongly consistent
  (single coordinator). Metadata caches are eventually consistent but never
  trusted for safety. Not provided: cross-partition order, exactly-once,
  durability across all-ISR power loss within the fsync interval, or
  controller fault tolerance. Full table in [consistency.md](consistency.md).
- **Follow-up**: stale reads? A deposed leader can serve old (committed)
  data until it learns of the new epoch.
- **Weak answer**: "eventually consistent".
- **Tradeoff**: lease-gated reads would remove stale reads at a latency cost.

### Q55. What would happen if two regions lose connectivity?

- **Testing**: multi-region reasoning.
- **Strong answer**: With async mirroring (active-passive): primary keeps
  working, mirror lag grows, failing over would lose the unmirrored tail;
  do not fail over on a link partition alone. With home-region
  active-active: each region serves its own writers, cross-region views go
  stale. With a synchronous stretch cluster: the side with the controller
  quorum continues, the other stops writing.
- **Follow-up**: who decides failover? A witness in a third region or a
  human.
- **Weak answer**: "data syncs later".
- **Tradeoff**: RPO vs availability.

### Q56. What would you sacrifice to maintain availability?

- **Testing**: explicit tradeoffs.
- **Strong answer**: Durability guarantees (allow `acks=leader`, lower
  min ISR, unclean election), freshness (serve stale reads), and ordering
  across a failover (new producer IDs, salting). For example, enabling
  unclean election keeps a partition writable when all ISR members are
  down, at the cost of losing committed data.
- **Follow-up**: which topics deserve that? Telemetry yes, payments no.
- **Weak answer**: "consistency" with no specifics.
- **Tradeoff**: per-topic configuration.

### Q57. What would you sacrifice to maintain ordering?

- **Testing**: same, other axis.
- **Strong answer**: Write availability during failover (single leader per
  partition, wait for election), load balance (hot keys stay hot), consumer
  parallelism (one consumer per partition), and per-partition throughput
  (one in-flight batch per producer per partition).
- **Follow-up**: ordering across a partition-count change? Drain-then-switch
  or migrate to a new topic.
- **Weak answer**: "performance".
- **Tradeoff**: ordering scope.

## K. Performance questions

### Q47. What becomes the bottleneck first?

- **Testing**: evidence-based reasoning.
- **Strong answer**: In this implementation, CPU in the HTTP/JSON request
  path: the pprof profile under load shows ~12 percent JSON decode, ~20
  percent syscalls, ~13 percent GC, while the storage engine itself is
  cheap (encode 1 KiB in ~140 ns). Next: GC from per-request allocations,
  coordinator fsync rate with many groups, leader network egress, then
  disk capacity. In a mature system with a binary protocol, network and
  disk usually bind first.
- **Follow-up**: how did you measure? pprof on a broker under loadgen.
- **Weak answer**: "the disk".
- **Tradeoff**: where to invest.

### Q65. What is the most expensive operation in the system?

- **Testing**: cost awareness.
- **Strong answer**: Per byte: replication (every byte crosses the network
  RF-1 times and is written RF times). Per event: request handling (HTTP +
  JSON). Per incident: leader failover (seconds of unavailability, metadata
  churn, client retries) and partition reassignment (copies a partition's
  whole retained log). Per operation: fsync (benchmark: ~100x a buffered
  append) and full-log compaction.
- **Follow-up**: which would you optimize first? Request handling
  (binary batches), then compression to cut replication bytes.
- **Weak answer**: one item without units.
- **Tradeoff**: n/a.

## L. Scaling questions

### Q46. How would you scale to 1 million events/sec?

- **Testing**: capacity math and design changes.
- **Strong answer**: At 2 KB: 2 GB/s ingest, 4 GB/s replication at RF 3,
  518 TB/day stored. Size brokers by the max of network, disk bandwidth,
  disk capacity and CPU. With this implementation's measured ~15k
  records/s per broker core, ~67 cores just for brokers: so first switch to
  a binary batch protocol and compression. Partitions: enough for consumer
  parallelism (e.g. 2 GB/s / 10 MB/s per consumer = 200, x2 headroom).
- **Follow-up**: what changes in the control plane? Not much at this size;
  the coordinator's commit fsync rate needs sharding with many groups.
- **Weak answer**: "add brokers".
- **Tradeoff**: cost vs headroom.

### Q48. How would you scale to 10 million events/sec?

- **Testing**: second-order limits.
- **Strong answer**: 20 GB/s ingest (2 KB): per-record overhead is now
  everything: batch format, compression, zero-copy, multiplexed protocol.
  Thousands of partitions; Raft controller with delta metadata; sharded
  coordinator; tiered storage (5 PB/day at RF 3); multiple clusters by
  domain to limit blast radius.
- **Follow-up**: what breaks in metadata? Full snapshots per change, and
  failover of a broker with thousands of leaders.
- **Weak answer**: "10x the previous answer".
- **Tradeoff**: one huge cluster vs many.

### Q49. How would you handle a topic with 100,000 partitions?

- **Testing**: per-partition overhead.
- **Strong answer**: Costs scale per partition: open files and index
  memory, fetcher goroutines and connections (multiplex per broker pair),
  metadata (deltas, per-topic subscriptions), elections on broker failure
  (thousands at once: batch them), consumer assignment computation and
  rebalance time (cooperative), small segments (larger segment size,
  lazy opening). Question whether the topic needs that many: often it
  needs fewer partitions and faster consumers.
- **Follow-up**: why would someone want that? Per-tenant partitions for
  isolation.
- **Weak answer**: "it works, partitions are cheap".
- **Tradeoff**: parallelism vs overhead.

### Q50. How would you rebalance partitions without taking the system offline?

- **Testing**: reassignment protocol.
- **Strong answer**: Add the new replica to the replica set, let it copy
  (throttled), wait until it is in the ISR, move leadership (epoch bump,
  clients redirect), then remove the old replica. At no point is the
  partition without an in-sync leader. Details in
  [scaling.md](scaling.md#how-a-partition-moves-without-downtime-reassignment-design).
- **Follow-up**: how to minimize movement? Minimal-move plans, fix leader
  imbalance with preferred election first, tiered storage.
- **Weak answer**: "copy the files and restart".
- **Tradeoff**: speed of move vs impact on live traffic.

### Q51. How would you migrate a partition?

- **Testing**: same mechanics, data focus.
- **Strong answer**: Same as Q50; the cost is copying the retained log.
  A 500 GB partition at a 100 MB/s throttle is ~1.4 hours. During copy the
  new replica is not in the ISR so it does not slow writes.
- **Follow-up**: what if the source dies mid-copy? Another ISR member
  continues as leader; the copy resumes from its offset.
- **Weak answer**: "rsync".
- **Tradeoff**: throttle level.

### Q52. How would you add brokers?

- **Testing**: understanding that brokers alone do not add capacity.
- **Strong answer**: New brokers register and are immediately used for new
  topics, but existing partitions stay put; capacity for existing topics
  only grows by reassigning partitions to them (Q50) or adding partitions
  (key remap). Also run preferred-leader election to spread leadership.
- **Follow-up**: automate? Continuous balancer with throttles and goals
  (disk, network, leader count).
- **Weak answer**: "start them and they share load".
- **Tradeoff**: data movement vs balance.

### Q53. How would you remove brokers?

- **Testing**: draining.
- **Strong answer**: Reassign every replica off it (copy elsewhere, wait for
  ISR), move leadership off, shrink replica sets, then stop it. Killing it
  first leaves partitions under-replicated until replaced.
- **Follow-up**: controlled shutdown for restarts: move leadership before
  stopping (not implemented here; restart = failover).
- **Weak answer**: "shut it down".
- **Tradeoff**: speed vs risk.

## M. Tradeoff questions

### Q54. How would you implement multi-region replication?

- **Testing**: design with existing building blocks.
- **Strong answer**: A mirror service: consumer group in the source, an
  idempotent producer into the target, offset translation records so
  consumers can fail over. Active-passive first; home-region active-active
  for keyed data later; synchronous stretch only for small critical topics
  after the controller is Raft-based. See [multi-region.md](multi-region.md).
- **Follow-up**: why idempotent producer in the mirror? Mirror retries
  would otherwise duplicate.
- **Weak answer**: "replicate the disks".
- **Tradeoff**: RPO vs latency.

### Q61. What would you redesign at 100x scale?

- **Testing**: prioritization.
- **Strong answer**: Controller (Raft, deltas, batched elections), wire
  protocol and record format (binary batches, compression), coordinator
  (sharded, offsets in a replicated internal topic), storage (tiered,
  mmap'd indexes, lazy segments), replication (multiplexed fetches), and
  operations (automated balancing, rack awareness). Keep the log model,
  epochs, HWM/ISR, idempotent producer design and backpressure.
- **Follow-up**: order? Controller first (correctness/availability), then
  protocol (cost).
- **Weak answer**: "rewrite in Rust".
- **Tradeoff**: effort vs risk.

### Q62. What would you replace first with a mature infrastructure component?

- **Testing**: build vs buy judgment.
- **Strong answer**: The controller's state store with etcd or a Raft
  library: it is the single point of failure and consensus is the hardest
  part to get right. Then observability (OpenTelemetry SDK, Prometheus
  client), and identity (mTLS/OIDC). Arguably the whole system is "use
  Kafka/Redpanda/Pulsar" in production; this project exists to understand
  them.
- **Follow-up**: why not replace the storage engine with RocksDB? An LSM
  adds write amplification for an append-only workload.
- **Weak answer**: "nothing, it's custom".
- **Tradeoff**: control vs reliability.

### Q63. What part of this architecture is intentionally simplified?

- **Testing**: honesty and awareness.
- **Strong answer**: Single-node controller without consensus; HTTP/JSON
  protocol; per-record format without compression; full-scan compaction;
  in-memory indexes; no partition reassignment; eager rebalancing only; no
  transactions; static API keys; local disks only; startup scans the log to
  rebuild producer state instead of snapshots.
- **Follow-up**: which simplification would bite first in production? The
  controller.
- **Weak answer**: "nothing important".
- **Tradeoff**: scope vs realism.

### Q64. Which guarantees are hardest to provide?

- **Testing**: depth.
- **Strong answer**: Exactly-once processing with external side effects
  (impossible in general); no split brain under partitions and pauses
  (needs fencing, not just timeouts); durability with low latency (fsync
  vs replication); ordering together with elasticity (partition changes
  remap keys); and fault-tolerant metadata (consensus).
- **Follow-up**: which did you get wrong first? The client metadata
  refresh: a liveness bug, found by fault injection.
- **Weak answer**: "performance".
- **Tradeoff**: n/a.

---

# Staff-Level Design Discussion

**What is the scalability ceiling?** Per partition: one leader's append
path and one consumer per group; per cluster: the controller (single
writer, full snapshots) and the coordinator (single fsync stream). With the
current protocol, broker CPU per record caps throughput well before disks;
measured ~15k records/s per broker core including replication and one
consumer group.

**What is the metadata bottleneck?** Every change bumps a global version
and every broker re-downloads the full snapshot via long poll; size grows
with topics x partitions x RF. At tens of thousands of partitions, each
ISR change costs megabytes of propagation across the fleet. Fix: delta
log, per-topic subscriptions, batching.

**What is the partition-management bottleneck?** Broker failure triggers
one election per partition it led, all through one controller; failover
time grows with leadership concentration. Partition count also multiplies
open files, index memory and fetcher goroutines. Fix: batched elections,
multiplexed replication, lazy segment opening.

**What is the replication bottleneck?** Leader egress: (RF-1) x ingress
plus consumer fetches; follower fetch request rate (one request per
partition here). Fix: compression, multiplexed fetches, rack-aware placement,
replication throttling during catch-up.

**What happens during correlated broker failures?** If all ISR members of a
partition fail together (same rack, same power), the partition goes
offline (or loses data with unclean election). With RF 3 placed without
rack awareness (as here), a rack loss can take entire partitions. Fix:
rack/zone-aware placement so replicas span failure domains; capacity
headroom so survivors can absorb led partitions.

**What happens when an entire availability zone fails?** With zone-aware
placement and RF 3 across 3 zones: each partition loses one replica; ISR
shrinks; writes continue with min ISR 2; leadership moves; survivors carry
1.5x load (plan headroom). The controller must not be in only that zone
(Raft quorum across zones). Without zone awareness: some partitions lose
all replicas.

**How would you design cross-region replication?** Async mirror per
topic (consumer + idempotent producer), offset translation records,
active-passive with witness-based failover; see
[multi-region.md](multi-region.md).

**How would you preserve ordering across regions?** One writer per key at a
time (home region); fenced handover when a key moves; per-key sequence
numbers in payloads so consumers can detect and repair reordering. Never
order by timestamps from different regions.

**How would you migrate billions of events?** Replay. Stand up the new
destination, start a consumer group from earliest (or a snapshot plus a
compacted topic for state), dual-run old and new paths, compare outputs,
cut over reads, then writes. For moving between clusters: mirror topics and
translate offsets; throttle to protect live traffic; verify counts per
partition and checksums of samples.

**How would you increase partition count?** Either add partitions and
accept remapping (drain consumers first when per-key order matters), or
create a new topic with the new count and migrate via replay with a
cut-over point. For future-proofing, over-partition at creation or use
consistent hashing with virtual partitions so splits move less.

**How would you rebalance partitions without massive data movement?** Fix
leader imbalance first (free); plan minimal replica moves; throttle;
prefer moving small partitions; tiered storage so only local tails move.

**How would you isolate tenants?** Authorization (tenant-scoped ACLs,
implemented), quotas on bytes/sec, partitions, connections and fetch
bandwidth (produce rate and partitions implemented), and physical
isolation for large tenants (dedicated brokers or cells), because page
cache, disk bandwidth and the controller are shared and not quota-able here.

**How would you prevent a noisy topic from affecting unrelated topics?**
Topic quotas (implemented for produce), fetch quotas, separate replication
throttles, and placement that keeps heavy topics off brokers hosting
latency-sensitive ones. Replays of old data are the classic noisy neighbor:
they evict the page cache; throttle them or serve them from a separate tier.

**What is the blast radius of a broker failure?** Partitions it led:
seconds of write unavailability; partitions it followed: reduced
durability; clients: retries. With 1/N of leadership per broker, ~1/N of
partitions blip. The controller's blast radius is the whole cluster.

**How do you perform rolling upgrades?** One broker at a time: graceful stop
(flush, indexes, checkpoints), restart, wait for `under_replicated == 0`,
next. For format or protocol changes, two-phase: deploy readers of the new
version everywhere first, then enable writers (inter-broker protocol
version gate). Controller upgrades require the Raft quorum to keep a
majority up.

**How do you change the event format?** The frame has a version byte;
readers reject unknown versions. Roll out "read v1+v2, write v1", then
"write v2"; old segments age out. Followers copy bytes, so all brokers must
read v2 before any leader writes it. See [data-model.md](data-model.md).

**How do you handle schema evolution?** At the edges: `event_type` and
`schema_version` headers, a registry interface with backward/forward/full
compatibility checks (`pkg/schema`), consumers upgrading old payloads with
defaults during replay. The broker never parses payloads.

**How do you prevent stale leaders?** Epochs checked everywhere + leases
shorter than the session timeout + truncation of divergent suffixes on
rejoin. Covered in Q37-Q39.

**How do you handle clock skew?** Never compare wall clocks across machines
for safety: leases are monotonic durations. Timestamps are broker-local,
clamped monotonic per partition. Skew affects only time-based retention
and timestamp replay precision; run NTP.

**What happens if the metadata coordinator becomes unavailable?** Here:
reads continue, writes stop after the lease (6s), no failovers, no topic
changes, no consumer rebalances or commits. Chosen deliberately: without a
controller a broker cannot distinguish "controller down" from "I am
partitioned away". Production: Raft controller so unavailability requires
losing a majority.

**Which guarantees are impossible without consensus?** A unique leader per
epoch that survives the failure of the node issuing epochs; fault-tolerant
ISR membership; never-reused producer IDs across controller failover;
atomic multi-step metadata changes (topic creation, reassignment) that
survive controller crashes mid-operation.

**What would you sacrifice under extreme scale?** Global anything:
cross-topic ordering, synchronous cross-region durability, full metadata on
every client, one cluster for everyone. Also per-record features: per-record
headers and timestamps move into batch headers; per-message tracing becomes
sampled; exactly-once-style guarantees stay at the application layer.
