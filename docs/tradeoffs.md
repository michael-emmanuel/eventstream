# Tradeoffs

Every decision below is specific to this codebase: what it does, what it
gave up, and when the other choice would be right. Each entry follows the
same structure: problem, chosen approach, alternatives, why chosen,
benefits, costs, failure modes, scaling implications, when another approach
would be better.

The capacity and scale review is at the end:
[10x / 100x / 1,000x](#10x-traffic).

---

## 1. Queue vs durable log

**Problem.** Deliver events from producers to consumers.
**Chosen.** A durable, append-only, partitioned log; consumers track offsets; data is removed by retention, never by consumption.
**Alternatives.** A queue with per-message acknowledgement and deletion (the previous in-memory pub/sub project, RabbitMQ/SQS-style); a database table polled by consumers.
**Why chosen.** The requirements include replay, multiple independent consumer groups, and ordering. A queue deletes on ack, so a second consumer or a replay has nothing to read; per-message acks scatter state across messages instead of one offset per partition.
**Benefits.** Replay and backfill for free; any number of consumer groups at no extra storage cost; consumer state is one integer per partition; sequential I/O.
**Costs.** Disk sized by retention, not by backlog; no per-message acknowledgement, delay, or redelivery of a single poison message (a consumer must skip or park it itself); consumer parallelism capped by partitions.
**Failure modes.** A poison record blocks its partition for that group; consumers falling behind retention lose data.
**Scaling.** Scales with partitions and brokers; read fan-out scales with page cache and disk bandwidth, not with message count.
**Better alternative when.** Work queues with independent tasks, per-task retries/visibility timeouts, and no need for history (job queues, email sending): a queue is simpler and parallelizes beyond partition counts.

## 2. Push vs pull consumers

**Problem.** How records reach consumers.
**Chosen.** Pull: consumers fetch from an offset at their own pace (with long polling).
**Alternatives.** Broker pushes to subscribers (the in-memory pub/sub design).
**Why chosen.** Consumers differ by orders of magnitude in speed. With push the broker must track per-consumer buffers and rates and decide what to do when a consumer is slow; with pull, a slow consumer simply asks less often, and its backlog sits on disk.
**Benefits.** Natural backpressure; trivial replay (fetch any offset); batching chosen by the consumer; broker keeps no per-consumer delivery state.
**Costs.** An idle consumer must poll (mitigated by long polling); a little added latency vs an ideal push.
**Failure modes.** Without long polling, empty polls waste CPU and network; with it, many idle consumers hold goroutines (bounded by `MaxLongPollWaiters`).
**Scaling.** Broker cost per consumer is per request, not per message in flight.
**Better alternative when.** Very low latency fan-out to a small number of always-fast subscribers (in-memory notification, websockets).

## 3. Polling vs long polling

**Problem.** Consumers and followers at the end of the log.
**Chosen.** Long polling: `wait_ms` parks the request on the partition's notification channel until new data is committed.
**Alternatives.** Short polling with sleep; server-sent streams (persistent push over one connection).
**Why chosen.** Delivers data as soon as it commits without the request rate of tight polling, and keeps the stateless request/response model (no per-connection subscription state).
**Benefits.** Low latency (demo: a parked fetch returned within milliseconds of the produce); few requests on idle partitions; same mechanism serves replication.
**Costs.** A goroutine and a connection per parked request; timeout tuning; HTTP client timeouts must exceed `wait_ms`.
**Failure modes.** Exhausting waiter slots (bounded; excess fetches return immediately); proxies with shorter idle timeouts killing parked requests.
**Scaling.** Memory per waiter is small (goroutine stack), but connections per broker grow with consumers x partitions in this one-request-per-partition design; a multi-partition fetch protocol would collapse that.
**Better alternative when.** Tens of thousands of partitions per consumer: a multiplexed streaming protocol with one connection carrying many partitions.

## 4. Batch vs individual requests

**Problem.** Per-request overhead (HTTP, JSON, auth, syscalls, fsync) dominates small records.
**Chosen.** Batches everywhere: producer accumulators (linger 5 ms / 500 records / 512 KiB), one `pwrite` per produce batch, multi-record fetches, raw multi-frame replication.
**Alternatives.** One request per record.
**Why chosen.** Benchmark on the dev sandbox: appending 1 KiB records one at a time ~200 MB/s vs batches of 100 ~510 MB/s, and the HTTP/JSON overhead per request is far larger than the append itself.
**Benefits.** Throughput; fewer round trips; better disk utilization.
**Costs.** Latency up to linger; larger memory per request; a failed batch fails (or retries) many records at once.
**Failure modes.** Batches larger than `max_request_bytes` rejected; very large batches increase GC pressure and tail latency.
**Scaling.** Essential at scale: per-record requests cap a broker at tens of thousands of requests per second regardless of bandwidth.
**Better alternative when.** Strictly interactive, low-rate traffic where every millisecond matters: linger 0 (still batches what is already queued).

## 5. Synchronous vs asynchronous replication

**Problem.** When is a write acknowledged relative to replication?
**Chosen.** Configurable per request: `acks=all` waits for the ISR (synchronous with respect to in-sync replicas), `acks=leader` does not (asynchronous).
**Alternatives.** Always synchronous; always asynchronous; semi-synchronous (one follower).
**Why chosen.** The right answer differs per use case (payments vs click logs), and the same cluster should serve both.
**Benefits.** Explicit, per-producer durability/latency choice.
**Costs.** `acks=all` adds a follower round trip (follower fetch + next fetch as ack); `acks=leader` can lose acknowledged data on leader failure.
**Failure modes.** Misconfigured producers using `acks=leader` for critical data; a slow ISR member slows every `acks=all` write until it is shrunk out.
**Scaling.** Async replication across regions is the only practical option (latency); within a region, sync is affordable.
**Better alternative when.** Always-sync when compliance demands it, enforced server-side (min ISR + rejecting `acks=leader` for a topic).

## 6. Leader/follower replication

**Problem.** Keep replicas identical.
**Chosen.** Single leader per partition, followers pull raw frames.
**Alternatives.** Multi-leader; leaderless quorum writes (Dynamo-style); chain replication.
**Why chosen.** A log needs a single order. One leader assigns offsets, so replicas agree on order by construction; no conflict resolution.
**Benefits.** Simple ordering; follower logs are byte-identical; reads and writes go to one place.
**Costs.** The leader is a hotspot per partition; failover is needed when it dies (seconds of write unavailability).
**Failure modes.** Split brain if two leaders exist (prevented with epochs and leases); slow leader slows its partition.
**Scaling.** Leadership spreads across brokers by spreading partitions; preferred-leader election rebalances it.
**Better alternative when.** Key-value data where order between keys does not matter and write availability during partitions is paramount (leaderless).

## 7. Replication factor

**Problem.** How many copies?
**Chosen.** Per topic, typical 3 with `min_insync_replicas=2`.
**Alternatives.** 1 (no redundancy), 2, 5.
**Why chosen.** RF=3 / min ISR 2 tolerates one failure with writes still available, and a second failure without losing committed data (writes stop instead).
**Benefits.** Survives a broker or disk loss and a rolling restart concurrently.
**Costs.** 3x storage, 3x disk writes, 2x leader egress for replication (measured: `broker_replica_bytes_total` = 2.0x `broker_produce_bytes_total` in every load test run).
**Failure modes.** RF=2 with min ISR 2 becomes unavailable on any single failure; RF=2 with min ISR 1 can lose data on a second failure.
**Scaling.** Storage and network budget scale linearly with RF ([capacity-planning.md](capacity-planning.md)).
**Better alternative when.** RF=1 for reproducible, derived data; RF=5 for small, critical topics (metadata-like) that must survive two concurrent failures with writes available.

## 8. ACK semantics

**Problem.** What does "success" mean to a producer?
**Chosen.** `leader`, `all`, and an educational `quorum`.
**Alternatives.** Only `all`; fsync-before-ack.
**Why chosen.** Exposes the latency/durability dial and, with `quorum`, the subtle point that an ack rule is only as strong as the election rule paired with it.
**Benefits.** Producers pick per workload; tests demonstrate each.
**Costs.** `quorum` is weaker than it sounds here (ISR election may pick a replica without the record); documented rather than hidden.
**Failure modes.** See [consistency.md](consistency.md#ack-modes).
**Scaling.** None directly; `all` makes the slowest ISR member the latency floor.
**Better alternative when.** With a Raft-style quorum *election*, quorum acks become safe and are the natural choice (tolerates slow minorities).

## 9. Strong vs eventual consistency

**Problem.** Which state must be linearizable?
**Chosen.** Strong for per-partition log contents (single writer, read from leader, below HWM), leader epochs, group generations and offset commits. Eventual for metadata caches and follower copies.
**Alternatives.** Everything strong (read from quorum, synchronous metadata); everything eventual.
**Why chosen.** Correctness requires a single order per partition and a single owner per epoch; performance requires caches. Caches are safe because the authority re-checks.
**Benefits.** Fast paths stay local; safety does not depend on cache freshness.
**Costs.** Stale reads from a deposed leader; extra round trips on stale metadata.
**Failure modes.** A bug that trusts a cache for a safety decision (avoided: brokers check epochs and roles on every request).
**Scaling.** Caches remove the controller from the data path, which is what lets brokers scale independently of it.
**Better alternative when.** Strict read-after-write across clients requires lease-gated or quorum reads.

## 10. Partition count

**Problem.** How many partitions per topic?
**Chosen.** Set at creation; can grow (keys remap); recommended sizing in [capacity-planning.md](capacity-planning.md).
**Alternatives.** Few large partitions; very many small ones; automatic splitting.
**Why chosen.** Explicit and predictable; the system shows the cost of changing it rather than hiding it.
**Benefits.** More partitions = more consumer parallelism and more brokers sharing load.
**Costs.** Each partition costs file handles, index memory, a fetcher goroutine per follower, metadata, and election work on failover; more partitions = longer failovers and larger metadata.
**Failure modes.** Too few: consumers cannot keep up and adding partitions breaks key ordering. Too many: slow controller operations, metadata bloat, many tiny segments.
**Scaling.** Partition count is the parallelism ceiling for both brokers and consumers.
**Better alternative when.** Workloads that can tolerate splitting (no key ordering): auto-scaling partitions or a segment-per-key-range design.

## 11. Hash vs round-robin partitioning

**Problem.** Where does a record go?
**Chosen.** Hash when a key is present, round robin otherwise, explicit when requested.
**Alternatives.** Always round robin; sticky batching partitioner (fill one partition's batch, then move on).
**Why chosen.** Keys express ordering requirements; records without keys have none, so spread them.
**Benefits.** Per-key order; even spread for keyless data.
**Costs.** Key skew becomes partition skew; round robin defeats batching at low rates (each partition's batch fills slowly).
**Failure modes.** Hot partitions (demo 11: 74 percent of traffic on one partition).
**Scaling.** Hash distribution quality matters more as partition counts grow.
**Better alternative when.** High-rate keyless producers: a sticky partitioner gives larger batches per partition.

## 12. Key-based partitioning

**Problem.** Keep related events ordered.
**Chosen.** `hash(key) % partitions` (FNV-1a).
**Alternatives.** Consistent hashing (implemented as `routing.Ring` for comparison), range partitioning, explicit routing tables.
**Why chosen.** Stateless, deterministic, cheap; no routing table to replicate.
**Benefits.** Any client computes the partition locally.
**Costs.** Changing partition count remaps ~all keys (measured: 89.5 percent moved going from 8 to 9 partitions, vs 16 percent with a consistent-hash ring); either way, moved keys lose ordering across the change.
**Failure modes.** Different clients using different hash functions break co-location (the hash is part of the contract).
**Scaling.** Plan partition counts ahead; growth means a migration, not a config change.
**Better alternative when.** Partitions must be added often: consistent hashing or a routing table with explicit migration of key ranges.

## 13. Hot partitions

**Problem.** Skewed keys overload one partition.
**Chosen.** Detect (per-partition metrics), and mitigate at the producer (salting via `routing.Salted`, composite keys, explicit partitions).
**Alternatives.** Automatic broker-side splitting of hot partitions.
**Why chosen.** Only the application knows whether a key's ordering matters; automatic splitting silently breaks ordering.
**Benefits.** Explicit, testable tradeoff (`TestHotKeySkew`).
**Costs.** Requires application changes; salting destroys per-key order.
**Failure modes.** A hot partition's single consumer becomes the group's bottleneck; its leader broker becomes hot.
**Scaling.** Skew gets worse at scale (power-law tenants).
**Better alternative when.** Workloads without ordering needs: automatic load-aware placement.

## 14. Consumer groups

**Problem.** Scale consumption while preserving per-partition order.
**Chosen.** Each partition owned by exactly one member of a group; coordinator assigns.
**Alternatives.** Shared subscription with per-message acks (queue semantics); every consumer reads everything.
**Why chosen.** Single owner per partition preserves order and makes progress one offset.
**Benefits.** Ordered processing per partition; trivial progress tracking.
**Costs.** Parallelism capped at partition count; idle members if members > partitions; rebalances pause the group.
**Failure modes.** Rebalance storms; zombie members (fenced by generation).
**Scaling.** Coordinator load grows with members x heartbeat rate.
**Better alternative when.** Per-message parallelism matters more than order: shared subscriptions (Pulsar "shared") or a queue.

## 15. Eager vs cooperative rebalancing

**Problem.** Changing assignments when membership changes.
**Chosen.** Eager (revoke all, reassign) with a sticky assignor and an initial rebalance delay.
**Alternatives.** Cooperative/incremental (revoke only moving partitions over two rounds); static membership.
**Why chosen.** Eager is simple and obviously correct: no partition can have two owners because nobody owns anything during the rebalance.
**Benefits.** Correctness is easy to reason about; sticky assignment still minimizes state rebuilds.
**Costs.** Stop-the-world: the whole group pauses on every change.
**Failure modes.** Rebalance storms during rolling deploys.
**Scaling.** Pause duration grows with group size (waiting for the slowest member to rejoin).
**Better alternative when.** Large groups or frequent deploys: cooperative rebalancing (sticky assignment is the prerequisite already in place).

## 16. Offset commit timing

**Problem.** When to record progress.
**Chosen.** Default commit-after-processing (`CommitRecords`), optional auto-commit of records returned by earlier polls; the demo consumer also supports commit-before.
**Alternatives.** Commit per record; transactional commit with output.
**Why chosen.** At-least-once is the safe default; the alternatives are shown so their failure modes can be observed.
**Benefits.** No silent loss.
**Costs.** Duplicates after crashes; commit traffic (one fsync per commit request on the coordinator).
**Failure modes.** Commit-before loses data on crash; very infrequent commits cause large redelivery.
**Scaling.** Commit rate x groups loads the coordinator's single fsync stream (a scaling limit; see 10x review).
**Better alternative when.** Output to a transactional store: store offsets in that store, in the same transaction.

## 17. At-least-once processing

**Problem.** Delivery guarantee to applications.
**Chosen.** At-least-once.
**Alternatives.** At-most-once; exactly-once via transactions.
**Why chosen.** Loss is usually worse than duplicates, and duplicates are fixable with idempotent processing.
**Benefits.** Simple, robust, no coordination between the log and external systems.
**Costs.** Applications must be idempotent.
**Failure modes.** Non-idempotent side effects (emails, payments) repeated.
**Scaling.** No coordination cost.
**Better alternative when.** Read-process-write pipelines entirely inside the log can use transactions (not implemented).

## 18. Exactly-once limitations

**Problem.** Users ask for exactly-once.
**Chosen.** Not provided. Idempotent production only.
**Alternatives.** Transactions across partitions plus transactional offset commits (Kafka EOS).
**Why chosen.** Exactly-once *processing* requires the side effect and the offset commit to be atomic. When the side effect is outside the log (a database, an email), the log cannot make that atomic; only the application can (idempotent writes or offsets stored with the output). Claiming otherwise would be misleading.
**Benefits.** Honest guarantees, simpler system.
**Costs.** Applications own deduplication.
**Failure modes.** Teams assuming exactly-once and writing non-idempotent consumers.
**Scaling.** Transactions add coordinator load and latency; not having them avoids it.
**Better alternative when.** Stream-processing pipelines that only read and write the log.

## 19. Idempotent producers

**Problem.** Producer retries duplicate records when a response is lost.
**Chosen.** Producer ID + per-partition sequence; leader rejects gaps, recognizes duplicates, returns original offsets; state replicated via record fields and rebuilt from the log.
**Alternatives.** Client-supplied request IDs with a broker-side dedup table; no dedup.
**Why chosen.** Sequences need O(1) state per producer per partition (vs a table of request IDs), detect gaps (lost batches), and travel with the data so they survive failover.
**Benefits.** Retries are safe (measured: 0 duplicates with 40 percent of responses dropped).
**Costs.** One in-flight batch per partition (reduces pipelining); producer state memory; startup scan to rebuild it.
**Failure modes.** State expiry after 15 minutes of producer silence; a new producer ID per application restart means restarts can duplicate.
**Scaling.** State is O(active producers x partitions); expiry bounds it. Kafka allows 5 in-flight batches with idempotence by tracking more state.
**Better alternative when.** Very high per-partition throughput from one producer: allow several in-flight batches with per-batch sequence tracking.

## 20. Producer sequence numbers

**Problem.** Represent producer progress per partition.
**Chosen.** Per (producer, partition), per record, starting at 0; stored in each record.
**Alternatives.** Per-batch sequence; global per producer.
**Why chosen.** Per-record sequences let the leader map any retried batch to exact offsets and let followers rebuild state record by record.
**Benefits.** Exact duplicate detection with offsets; gap detection.
**Costs.** 8 bytes per record (part of the 56-byte overhead).
**Failure modes.** A batch that times out leaves the next sequence unknown; the producer takes a new ID for that partition and ordering across that boundary is not guaranteed.
**Scaling.** Constant per record.
**Better alternative when.** Per-batch sequences with batch-level headers save space once batching is part of the storage format.

## 21. Retention

**Problem.** Bound disk usage and age of data.
**Chosen.** Segment-granular deletion by time and/or bytes, never beyond the HWM, never the active segment.
**Alternatives.** Record-level expiry (TTL per record); tiered storage (move old segments to object storage).
**Why chosen.** Deleting whole files is O(1) and never rewrites data.
**Benefits.** Cheap, predictable.
**Costs.** Granularity: data can outlive retention by up to one segment's age; the active segment is never deleted (rolled by `segment_ms`).
**Failure modes.** Consumers behind retention lose data; retention set longer than disk allows fills disks.
**Scaling.** Disk = ingest x RF x retention. At large scale retention drives cluster size more than throughput does.
**Better alternative when.** Long retention at scale: tiered storage keeps recent data local and old data in object storage.

## 22. Compaction

**Problem.** Keep the latest state per key indefinitely without unbounded growth.
**Chosen.** Simple full-log compaction of sealed segments with tombstone retention; atomic file swap.
**Alternatives.** Incremental dirty-segment compaction with a bounded offset map (Kafka's cleaner); compaction into an external key-value store.
**Why chosen.** Correctness and clarity over efficiency.
**Benefits.** Compacted topics act as a durable changelog/snapshot (`TestCompactionKeepsLatestPerKey`).
**Costs.** Rescans the whole log each pass; O(keys) memory; rewrites every segment with garbage; not throttled.
**Failure modes.** Compaction I/O competing with traffic; memory for very high-cardinality keys.
**Scaling.** Needs incremental compaction and throttling beyond small topics.
**Better alternative when.** Large compacted topics: Kafka-style cleaner with dirty ratio and bounded memory.

## 23. Segment size

**Problem.** Size of each log file.
**Chosen.** 64 MiB default, configurable per topic; roll also by age (`segment_ms`).
**Alternatives.** 1 GiB (Kafka default); very small.
**Why chosen.** Small enough that retention and compaction are fine-grained on a laptop-sized demo; large enough to keep file counts low.
**Benefits.** Retention granularity, cheap rolls, fast tail recovery.
**Costs.** More files per partition at high volume.
**Failure modes.** Very small segments: file-descriptor exhaustion, many index entries; very large: coarse retention, long compaction per segment.
**Scaling.** Production would use larger segments plus lazy opening of cold segments.
**Better alternative when.** High-volume topics: 512 MiB-1 GiB.

## 24. Sparse indexes

**Problem.** Find an offset's position without scanning a segment.
**Chosen.** One entry per ~4 KiB, in memory, persisted on seal and close, validated on load.
**Alternatives.** Dense index (one entry per record); no index (scan); B-tree.
**Why chosen.** Offsets are monotonic, so a sparse sorted array plus a short scan gives O(log n) + 4 KiB per lookup with ~0.2 percent space overhead.
**Benefits.** Small; fast to rebuild; simple.
**Costs.** Up to ~4 KiB scanned per lookup; all indexes in memory (limitation).
**Failure modes.** A corrupt index file is detected and rebuilt (`TestCorruptIndexIsRebuilt`).
**Scaling.** At tens of TB per broker, in-memory indexes become tens of GB; memory-mapped, lazily loaded index files are needed.
**Better alternative when.** Random point lookups dominate: denser index.

## 25. Batch size

**Problem.** How big should batches be?
**Chosen.** Producer: 500 records / 512 KiB / 5 ms linger. Consumer fetch: 500 records / 1 MiB. Replication: 1 MiB.
**Alternatives.** Larger for throughput, smaller for latency.
**Why chosen.** Middle-of-the-road defaults that keep p50 latency in single-digit milliseconds at moderate load (measured ~8 ms p50 end-to-end at 1k/s on the sandbox).
**Benefits.** Good default behaviour.
**Costs.** Not optimal for either extreme.
**Failure modes.** Oversized batches hit request limits and inflate tail latency; under load, batches grow naturally (records queue while one batch is in flight), which is good for throughput.
**Scaling.** At scale, also move per-record fields (producer ID, base offset/timestamp) into a batch header to cut the 56-byte per-record overhead.
**Better alternative when.** Bulk pipelines: larger batches and linger.

## 26. Compression

**Problem.** Reduce disk and network.
**Chosen.** Not implemented (`compression` must be `none`).
**Alternatives.** Batch-level compression (gzip/lz4/zstd) done by producers, stored compressed, decompressed by consumers.
**Why chosen.** Scope: compression belongs at the batch level (compressing one record at a time compresses poorly), and the storage format here is per-record. Adding it properly means a batch format.
**Benefits.** Simpler format and code.
**Costs.** Measured network out was ~3x produce bytes in load tests (replication + JSON fetches); compression would cut both disk and network substantially for text payloads.
**Failure modes.** n/a.
**Scaling.** At scale, network and disk are usually the binding constraints; compression is one of the first things to add.
**Better alternative when.** Any production deployment with compressible payloads.

## 27. Backpressure

**Problem.** Load above capacity must not exhaust memory.
**Chosen.** Bounded buffers at every stage: producer buffer, broker in-flight bytes, request concurrency, long-poll waiters, consumer prefetch; rejections carry Retry-After.
**Alternatives.** Unbounded queues (latency and memory grow until OOM); drop silently.
**Why chosen.** Explicit, observable overload behavior; failure moves to the edge.
**Benefits.** Bounded memory under any load (loadgen at 3.5x capacity: latency rose, memory stayed flat, no errors).
**Costs.** Callers must handle 429/503 and `ErrBufferFull`.
**Failure modes.** Mis-sized bounds cause premature shedding or too much queueing.
**Scaling.** Bounds should be sized per broker resources.
**Better alternative when.** n/a; the question is only where to put the bounds.

## 28. Rate limiting

**Problem.** Protect brokers and tenants from each other.
**Chosen.** Token buckets (bytes/sec) at broker, tenant, topic and producer scope, all-or-nothing.
**Alternatives.** Throttling by delaying responses (Kafka quota style); concurrency limits only.
**Why chosen.** Token buckets allow bursts while bounding averages; explicit 429 + Retry-After is easy to observe and test.
**Benefits.** Noisy neighbors get throttled instead of degrading everyone.
**Costs.** Per-broker buckets: a tenant's effective global limit is the per-broker limit x brokers it touches; a global limiter needs coordination.
**Failure modes.** A global lock around bucket updates (single mutex in `Limiter`) can contend at very high request rates.
**Scaling.** Shard limiter state; distribute global quotas periodically.
**Better alternative when.** Clients that do not handle 429 well: delay responses instead of rejecting.

## 29. Metadata caching

**Problem.** Every request needs leadership information.
**Chosen.** Client TTL cache with single-flight refresh, invalidation on errors, stale-on-failure; broker list learned from metadata; brokers long-poll the controller.
**Alternatives.** Ask the controller per request; push metadata to clients.
**Why chosen.** Removes the controller from the hot path; correctness enforced by brokers.
**Benefits.** No extra round trip per request; controller outage does not stop reads.
**Costs.** Stale routing after leader changes (one extra round trip).
**Failure modes.** Relying on bootstrap brokers only (a real bug found during the failover demo, now fixed and tested).
**Scaling.** Metadata size grows with partitions; full snapshots per change become expensive at 100k+ partitions (use deltas).
**Better alternative when.** Very large clusters: incremental metadata deltas and per-topic subscriptions.

## 30. Leader election

**Problem.** Choose a new leader when one fails.
**Chosen.** Controller picks the first live ISR member in replica order; epoch + 1; unclean election opt-in.
**Alternatives.** Replicas elect among themselves (Raft per partition); choose the replica with the highest LEO.
**Why chosen.** ISR membership guarantees the choice has all committed data; a central controller makes the decision unique.
**Benefits.** Simple, deterministic, preferred replica returns as leader.
**Costs.** Requires a consistent controller; failover time = session timeout + propagation.
**Failure modes.** No ISR member alive: offline partition (availability loss) unless unclean (data loss).
**Scaling.** Controller elects for every partition of a failed broker in one pass; with 100k partitions this is the dominant failover cost (batching and deltas needed).
**Better alternative when.** Per-partition Raft when partitions are few and failover must be fastest.

## 31. Consensus

**Problem.** Agreement on metadata among controller nodes.
**Chosen.** None: a single controller with durable file state.
**Alternatives.** Raft-replicated controller (KRaft, etcd), ZooKeeper.
**Why chosen.** Scope. The educational value is in the epoch discipline, which is the same with or without a replicated controller; implementing Raft correctly is a project in itself.
**Benefits.** Simple, inspectable state.
**Costs.** Single point of failure for every control-plane change; writes stop cluster-wide after lease expiry if it is down.
**Failure modes.** Controller disk loss loses cluster metadata.
**Scaling.** Single node also caps metadata write throughput.
**Better alternative when.** Any real deployment: 3 or 5 Raft controller nodes.

## 32. Fencing

**Problem.** Stop a deposed leader from causing damage.
**Chosen.** Leader epochs checked by every replica and the controller, plus broker leases shorter than the controller session.
**Alternatives.** Leases only; STONITH (kill the old leader); storage-level fencing.
**Why chosen.** Leases alone fail under process pauses and clock-rate drift; epochs alone allow an isolated leader to accept `acks=leader` writes indefinitely. Together they bound the window and guarantee nothing from the old epoch is committed.
**Benefits.** No split-brain commits; bounded window of at-risk writes.
**Costs.** Controller outage stops writes after the lease.
**Failure modes.** A thread paused between the lease check and the append can append one batch after the lease; epochs ensure it is truncated.
**Scaling.** Constant cost.
**Better alternative when.** Shared storage systems use storage-level fencing tokens.

## 33. Multi-region replication

**Problem.** Survive a region loss; serve users near them.
**Chosen.** Not implemented; designed in [multi-region.md](multi-region.md): asynchronous cross-region replication of topics (mirroring) with active-passive failover as the first step.
**Alternatives.** Stretched cluster with synchronous replication across regions; active-active.
**Why chosen.** Synchronous cross-region replication puts inter-region RTT (tens of ms) on every `acks=all` write and makes the controller quorum span regions.
**Benefits.** Regional latency stays local.
**Costs.** RPO > 0 (async lag), offsets differ between regions (consumers need offset translation on failover).
**Failure modes.** Replication lag at failover = lost data; split brain if both regions accept writes for the same keys.
**Scaling.** Cross-region bandwidth is expensive; replicate only what is needed.
**Better alternative when.** RPO = 0 requirements: synchronous stretch cluster with three regions.

## 34. Active-active vs active-passive

**Problem.** Which regions accept writes?
**Chosen (design).** Active-passive first; active-active only for data partitioned by home region.
**Alternatives.** Full active-active with conflict resolution.
**Why chosen.** A log's value is its order; two regions writing the same partition have no common order without cross-region coordination.
**Benefits.** Simple failover semantics.
**Costs.** Passive region capacity mostly idle; failover is an operation, not automatic.
**Failure modes.** Failing over with replication lag loses the tail.
**Scaling.** Active-active by home region scales writes with regions.
**Better alternative when.** Globally distributed writers to the same entities with tolerance for conflict resolution (CRDTs, last-writer-wins).

## 35. Storage engine choices

**Problem.** How to store partition data.
**Chosen.** Custom segmented append-only files, sparse index, page cache, fsync on interval (default) or per append.
**Alternatives.** Embedded KV store (Pebble/RocksDB/bbolt); mmap-based segments; object storage.
**Why chosen.** A log is already the optimal on-disk structure for append + sequential read; an LSM tree would add write amplification (compactions) for no benefit, and building it makes recovery mechanics visible.
**Benefits.** Sequential writes, zero-copy-friendly layout, simple recovery, byte-identical replication.
**Costs.** Every concern (indexes, recovery, compaction) is hand-written.
**Failure modes.** Bugs in recovery are data loss bugs; mitigated by recovery and corruption tests and fuzzing of the decoder.
**Scaling.** Next steps: mmap'd indexes, lazy segment opening, sendfile-style zero copy (needs a binary protocol), tiered storage.
**Better alternative when.** Workloads with random updates and point reads: an LSM or B-tree.

**fsync policy, specifically.** Measured on the sandbox: `BenchmarkAppendSync1KB` ~3.5 MB/s vs ~200 MB/s without per-append fsync. The default (`-fsync interval`, 200 ms) relies on replication for durability between flushes, which is the standard choice for replicated logs; `-fsync always` is available for single-replica or compliance cases.

---

## 10x Traffic

Assume today's reference load (see capacity-planning: 500k events/s, 2 KB,
RF 3) becomes 5M events/s.

* **Partitions**: per-partition throughput does not grow, so partitions grow
  ~10x. Partition count must be increased with a planned migration (keys
  remap).
* **Brokers**: network and disk scale linearly: ~10x brokers.
* **Client protocol**: HTTP/1.1 + JSON becomes the CPU bottleneck (12
  percent of broker CPU in JSON decode and 20 percent in syscalls in the
  sandbox profile). Move to a binary, multiplexed protocol and batch-level
  record format.
* **Compression**: becomes mandatory for network cost.
* **Coordinator**: offset commits through one fsync stream saturate; shard
  the coordinator by group hash (offsets as a partitioned internal topic).
* **Indexes**: in-memory indexes exceed memory; switch to mmap'd lazily
  loaded indexes.

## 100x Traffic

* **Controller**: single-node controller and full-snapshot metadata no
  longer viable: Raft quorum, delta-based metadata propagation, batched
  elections.
* **Storage**: retention at 100x ingest needs tiered storage (object store
  for cold segments).
* **Replication**: per-partition fetcher goroutines and connections become
  a problem; multiplex replication per broker pair.
* **Placement**: automated partition rebalancing with throttled data
  movement and rack/zone-aware replica placement.
* **Multi-cluster**: one cluster per failure domain / business unit with
  mirroring between them; blast radius limits matter more than raw scale.

## 1,000x Traffic

* Single-cluster designs reach their limits on metadata (millions of
  partitions), failover storms, and operational blast radius. Expect a
  fleet of clusters with a routing layer that maps topics to clusters.
* Storage disaggregation (brokers become mostly stateless caches over object
  storage) to decouple compute from retention.
* Per-tenant cells to contain noisy neighbors and failures.

## What Breaks First?

In order, for this codebase on commodity hardware:

1. **CPU on the broker in JSON/HTTP handling** (observed at ~29k records/s
   on 2 vCPUs with everything co-located).
2. **GC pressure** from per-request allocations (12 percent of profile).
3. **Coordinator fsync rate** for offset commits with many groups.
4. **Network egress** on leaders (replication 2x + consumers per group).
5. **Disk capacity** (retention).
6. **Controller** (single node, full snapshots) as partitions and brokers grow.

## What Must Be Sharded?

* Partition data and leadership (already sharded by partition).
* Consumer group coordination and offset storage (today: one coordinator).
* Rate-limiter state for global quotas.
* Metadata propagation (per-topic deltas, not one global snapshot).

## What Must Be Replicated?

* Partition data (already, RF configurable).
* Controller metadata: topics, assignments, epochs, ISR, producer ID
  counter (today: **not replicated**; first thing to fix).
* Consumer offsets (today: single coordinator file).
* Producer dedup state (already, via record fields).

## What Can Be Eventually Consistent?

* Client and broker metadata caches.
* Lag metrics and dashboards.
* Rate-limiter state for soft global quotas.
* Follower copies of the HWM.
* Cross-region mirrors.

## What Must Be Strongly Consistent?

* Leader epoch per partition (one leader per epoch).
* ISR membership as used for elections.
* Committed offsets and group generations.
* Producer ID allocation (no reuse).
* Topic IDs and configuration changes affecting placement.

## What Would I Replace?

1. Controller -> Raft-replicated metadata quorum.
2. HTTP/JSON client protocol -> binary, multiplexed, batch-oriented protocol.
3. Record format -> batch format with compression and delta encoding.
4. Coordinator offset file -> partitioned, replicated internal topic.
5. In-memory indexes -> mmap'd index files.
6. Hand-rolled metrics/tracing exporters -> OpenTelemetry SDK and Prometheus client.
7. Static API keys -> mTLS identities or an identity provider, with a secret manager.

## What Would I Keep?

* Partitioned append-only log with partition-local offsets.
* Single leader per partition, pull replication of raw frames.
* Leader epochs in every record and epoch-based truncation.
* HWM over a controller-approved ISR; shrink only after the controller agrees.
* Idempotent producer design (ID + per-partition sequence stored in records).
* Generation-fenced consumer commits.
* Bounded buffers and explicit backpressure everywhere.
* Segment files with sparse indexes and checksum-verified recovery.

## What Should NOT Be Optimized Yet?

* Zero-copy reads and memory-mapped segments: the JSON API copies anyway.
* Lock-free data structures: no lock contention shows up in profiles.
* Custom allocators/object pools: reduce allocations in the request path
  first (they are dominated by JSON).
* Compaction performance: compacted topics are small in every current use.
* Cross-region active-active: no requirement justifies its complexity.
