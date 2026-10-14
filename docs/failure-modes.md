# Failure Modes

For each failure: how it is detected, how the system recovers, the risk of
data loss and duplicate processing, the effect on availability and
consistency, and what an operator should do. "Test" points to the automated
test or demo that exercises it.

Defaults referenced below: heartbeat 1s, lease 6s, broker session 10s,
consumer session 10s, `replica_lag_max_ms` 10s, fsync interval 200ms.

---

## Producer crash

| | |
|---|---|
| **Failure** | Producer process dies with records in its accumulator or in flight. |
| **Detection** | None needed by the cluster; producer state for its ID expires after 15 min idle. |
| **Recovery** | Application restarts and gets a new producer ID. |
| **Data loss risk** | Records still in the client buffer (never sent, or sent but unacknowledged) are lost unless the application re-sends them from its own source of truth. |
| **Duplicate risk** | If the application re-sends unacknowledged records with a **new** producer ID, the broker cannot recognize them: records that were actually appended are duplicated. Idempotence is per producer session. |
| **Availability impact** | None for the cluster. |
| **Consistency impact** | None. |
| **Operational response** | Applications that need no-loss production must treat "acknowledged" as the commit point and re-send from a durable source (outbox table) on restart, with business-level dedup keys. |

## Consumer crash

| | |
|---|---|
| **Failure** | Consumer dies between processing and committing, or mid-batch. |
| **Detection** | Coordinator: no heartbeat for the session timeout (or immediate if it sent Leave). |
| **Recovery** | Group rebalances; partition reassigned; new owner resumes from the last committed offset. |
| **Data loss risk** | None with commit-after-processing. With commit-before-processing, the committed-but-unprocessed batch is lost. |
| **Duplicate risk** | Yes: everything processed since the last commit is processed again (at-least-once). |
| **Availability impact** | The crashed member's partitions are idle for up to the session timeout, and the whole group pauses during the rebalance. |
| **Consistency impact** | None on the log. Downstream side effects may be applied twice unless idempotent. |
| **Operational response** | Make processing idempotent. Tune session timeout (fast detection vs false positives). Test: `TestCrashBeforeCommitReprocesses`, `TestConsumerGroupRebalanceAtLeastOnce`, demo 4. |

## Broker crash

| | |
|---|---|
| **Failure** | Broker process dies (SIGKILL, OOM, panic). |
| **Detection** | Controller: no heartbeat for the session timeout. Clients: connection refused. |
| **Recovery** | Controller re-elects leaders it held (from ISR) and removes it from ISRs. On restart: log recovery (truncate torn tails, rebuild indexes), new incarnation, follows new leaders, truncates divergent suffixes, catches up, rejoins ISR. |
| **Data loss risk** | None for `acks=all` records while another ISR member survives. `acks=leader` records not yet replicated are lost. A process crash loses nothing in the page cache; see machine crash under disk failure. |
| **Duplicate risk** | None in the log with idempotent producers (retries are deduplicated by the new leader using replicated producer state). |
| **Availability impact** | Partitions it led are unavailable for writes for ~session timeout. Others unaffected. |
| **Consistency impact** | None after clean election. |
| **Operational response** | Restart it; watch `controller_under_replicated_partitions` return to 0; run preferred election to rebalance leadership. Test: `TestLeaderFailoverPreservesAcknowledgedWrites`, `TestBrokerCrashRecovery`, demos 6 and 8. |

## Leader crash

| | |
|---|---|
| **Failure** | The leader of a specific partition fails. |
| **Detection** | Session expiry at the controller; reported disk failure; restart with new incarnation. |
| **Recovery** | Clean election from ISR, epoch + 1, metadata published; clients retry on `NOT_LEADER` or transport error. |
| **Data loss risk** | `acks=all`: none (ISR >= `min_insync_replicas` had it). `acks=leader`: unreplicated suffix lost. Unclean election (opt-in): committed data may be lost. |
| **Duplicate risk** | Producer retries of batches the old leader accepted are deduplicated by the new leader. |
| **Availability impact** | ~session timeout of write unavailability for that partition; consumers of it stall. |
| **Consistency impact** | HWM on the new leader may briefly lag the old HWM; it never goes backwards over committed data. |
| **Operational response** | As broker crash. If the partition is offline (no ISR member alive), decide explicitly whether to wait for an ISR member or enable unclean election and accept loss. |

## Follower crash

| | |
|---|---|
| **Failure** | A replica that is not leader fails. |
| **Detection** | Leader: follower stops fetching (ISR shrink after `replica_lag_max_ms`). Controller: session expiry (removes it from ISR immediately). |
| **Recovery** | ISR shrinks; HWM advances without it. On return it truncates if needed, catches up, and is re-added. |
| **Data loss risk** | None. |
| **Duplicate risk** | None. |
| **Availability impact** | `acks=all` writes stall until the ISR shrink is accepted (bounded); rejected with `NOT_ENOUGH_REPLICAS` if ISR < `min_insync_replicas`. |
| **Consistency impact** | Reduced durability (fewer copies) while under-replicated. |
| **Operational response** | Alert on `controller_under_replicated_partitions` for > 5 min; replace the broker if it does not return. |

## Network partition

| | |
|---|---|
| **Failure** | Brokers and/or controller cannot reach each other; clients may still reach an isolated broker. |
| **Detection** | Broker: heartbeats fail, lease expires. Controller: session expiry. Leader: followers stop fetching. |
| **Recovery** | Isolated leader self-fences at lease expiry; controller elects on its side; on heal, epoch-based truncation removes divergent writes. Full analysis: [distributed-consistency.md](distributed-consistency.md). |
| **Data loss risk** | `acks=leader` writes accepted by the isolated leader within its lease are truncated. `acks=all` writes cannot complete on the isolated side. |
| **Duplicate risk** | None in the log. |
| **Availability impact** | Writes unavailable for affected partitions until election; the isolated side refuses writes after lease expiry. If the controller is on the minority side, the majority of brokers can be unavailable. |
| **Consistency impact** | Stale reads possible from an isolated leader (committed but not latest). |
| **Operational response** | Fix the network; verify `broker_replica_truncations_total` and under-replicated partitions settle. Test: `TestNetworkPartitionFencesStaleLeader`. |

## Replica lag

| | |
|---|---|
| **Failure** | A follower keeps up too slowly (slow disk, saturated network, CPU). |
| **Detection** | `partition_replica_lag_records`, `partition_replica_last_caught_up_seconds`, ISR shrink. |
| **Recovery** | Leaves ISR after `replica_lag_max_ms`; rejoins once it catches up to the HWM. |
| **Data loss risk** | None. |
| **Duplicate risk** | None. |
| **Availability impact** | `acks=all` latency rises (HWM waits for the slowest ISR member) until shrink. |
| **Consistency impact** | Fewer replicas protect new data while it is out of the ISR. |
| **Operational response** | Find the bottleneck on the follower (disk latency, NIC). Consider replication throttling during catch-up. Test: `TestSlowReplicaLeavesAndRejoinsISR`, demo 7. |

## Disk failure

| | |
|---|---|
| **Failure** | Write or fsync errors (failing device, read-only remount), or loss of the disk. Includes machine crash (power loss). |
| **Detection** | Append/fsync returns an error; the replica is marked failed and reported in the next heartbeat as an offline partition. |
| **Recovery** | Controller moves leadership away; the broker stops serving that replica. Replacing the disk = the replica starts empty and re-replicates from the leader (reset to leader's log start). |
| **Data loss risk** | None for `acks=all` while another ISR member survives. On power loss of every ISR replica within the fsync interval, recently acknowledged writes can be lost (they were only in page caches). `-fsync always` closes that window at a large throughput cost. |
| **Duplicate risk** | None. |
| **Availability impact** | Brief, as leader failover. |
| **Consistency impact** | None after failover. |
| **Operational response** | Replace the device; do not retry fsync on the same file after an error. Test: `TestDiskFailureMovesLeadership`. |

<a id="disk-full"></a>
## Disk full

| | |
|---|---|
| **Failure** | No space left on device. |
| **Detection** | Appends fail with ENOSPC (treated as a storage failure); `partition_size_bytes` growth; OS disk alerts. |
| **Recovery** | The replica goes offline and leadership moves, but if every broker is filling for the same reason (retention too long for the ingest rate), they will all fail. Free space by lowering retention (`topics alter`), running maintenance, or adding capacity. |
| **Data loss risk** | None directly; lowering retention deletes old data deliberately. |
| **Duplicate risk** | None. |
| **Availability impact** | Severe and correlated: all replicas of a partition tend to fill together. |
| **Consistency impact** | None. |
| **Operational response** | Alert at 70-80 percent disk. Capacity plan retention from ingest rate ([capacity-planning.md](capacity-planning.md)). A production system would reserve headroom and reject produce before the disk is actually full. |

## Corrupted segment

| | |
|---|---|
| **Failure** | Bit rot or a bad write inside a segment. |
| **Detection** | CRC mismatch on recovery (active segment, sealed-segment tail), on replication (follower verifies), on consumption, or by `streamctl verify`. |
| **Recovery** | Active-segment tail: truncated at the first bad frame, re-fetched from the leader. Sealed segment: not truncated (would break offset continuity); the read fails with a checksum error. The fix is to rebuild the replica from a healthy one. |
| **Data loss risk** | None if a healthy replica exists. A corrupt record on a single replica of an RF=1 topic is lost. |
| **Duplicate risk** | None. |
| **Availability impact** | Reads hitting the bad range fail on that replica. |
| **Consistency impact** | Corruption is never replicated (followers verify CRCs) and never silently served (consumers decode with CRC). |
| **Operational response** | `streamctl verify DIR`; wipe and re-replicate the replica. Test: `TestVerifyDetectsSealedCorruption`, `TestRecoveryDetectsCorruptTail`. |

## Incomplete write

| | |
|---|---|
| **Failure** | Crash in the middle of an append: a partial frame at the end of the active segment. |
| **Detection** | Recovery reads a length prefix whose frame extends past end of file (`ErrTruncated`) or fails CRC. |
| **Recovery** | Truncate the tail at the last complete frame; log `recovery truncated incomplete or corrupt log tail` with byte count; increment `broker_recovery_truncated_bytes_total`. |
| **Data loss risk** | After a *process* crash, a partially written record was never acknowledged by this replica (a follower acknowledges on its next fetch, after the write completed; a leader responds after the append returns). After a *power loss*, a record acknowledged from the page cache can be torn on disk; it is re-fetched from replicas that have it (observed in demo 8). Loss requires every ISR replica to lose it. |
| **Duplicate risk** | None. |
| **Availability impact** | Restart only. |
| **Consistency impact** | None. |
| **Operational response** | None required. Test: `TestRecoveryTruncatesTornWrite`, demo 8 (62 bytes truncated, record re-replicated). |

## Stale metadata

| | |
|---|---|
| **Failure** | A client or broker acts on an old view of leadership. |
| **Detection** | The broker answers `NOT_LEADER` (with hint), `FENCED_LEADER_EPOCH` or `UNKNOWN_LEADER_EPOCH`; connection errors. |
| **Recovery** | Clients invalidate the cache and refetch from any known live broker; brokers ignore metadata older than what they have. |
| **Data loss risk** | None: brokers validate leadership on every request. |
| **Duplicate risk** | None with idempotent producers. |
| **Availability impact** | Extra round trips; a client whose only known broker is dead would be stuck (fixed: clients learn the full broker list from metadata; `TestClientSurvivesLossOfItsOnlyBootstrapBroker`). |
| **Consistency impact** | None. |
| **Operational response** | Configure several bootstrap brokers or a DNS name for all of them. |

## Rebalance storm

| | |
|---|---|
| **Failure** | Group membership changes repeatedly; the group spends most of its time rebalancing. |
| **Detection** | `group_rebalances_total` rate, `group_rebalancing` gauge, `group_rebalance_duration_seconds`, lag rising while consumer CPU is low. |
| **Recovery** | Stabilize membership: increase session timeout, shorten work in revoke callbacks, deploy consumers with leave-on-shutdown, stagger restarts. The coordinator coalesces bursts with the initial rebalance delay. |
| **Data loss risk** | None. |
| **Duplicate risk** | High: every rebalance redelivers uncommitted work. |
| **Availability impact** | The whole group stops during each eager rebalance. |
| **Consistency impact** | None. |
| **Operational response** | See [observability.md](observability.md#playbook-rebalance-storm). Longer term: sticky + cooperative rebalancing, static membership. |

## Hot partition

| | |
|---|---|
| **Failure** | One partition receives a disproportionate share of traffic. |
| **Detection** | Skewed `broker_produce_records_total{partition}`, one consumer's lag growing, one broker's CPU/disk high. |
| **Recovery** | Key redesign, salting, dedicated partitions; see [system-design.md](system-design.md#2-partitioning-strategies-and-hot-partitions). |
| **Data loss risk** | None, unless the hot partition's consumer falls behind retention. |
| **Duplicate risk** | None. |
| **Availability impact** | Higher latency for that partition and for others sharing its leader. |
| **Consistency impact** | Mitigations like salting give up per-key ordering. |
| **Operational response** | Identify the key (sample records), decide whether its ordering is required. Demo 11. |

## Slow consumer

| | |
|---|---|
| **Failure** | A consumer processes slower than records arrive. |
| **Detection** | Lag grows (`/v1/groups/{g}/lag`), end-to-end latency rises. |
| **Recovery** | Faster processing, more consumers (up to partition count), more partitions. |
| **Data loss risk** | If lag exceeds retention, unread records are deleted; the consumer gets `OFFSET_OUT_OF_RANGE` and the reset policy skips ahead (logged as a warning). |
| **Duplicate risk** | None beyond normal at-least-once. |
| **Availability impact** | None for producers or other groups (the log decouples them). Memory on the consumer stays bounded (bounded prefetch). |
| **Consistency impact** | None. |
| **Operational response** | Lag playbook in [observability.md](observability.md#playbook-consumer-lag-increased-20x). Demo 9. |

## Overloaded producer

| | |
|---|---|
| **Failure** | Producers send faster than brokers accept. |
| **Detection** | `broker_throttled_requests_total`, `broker_rejected_requests_total`, rising produce latency, producer `ErrBufferFull`. |
| **Recovery** | Backpressure chain: broker 429/503 with Retry-After -> producer backoff -> producer buffer fills -> `Send` blocks/fails -> application slows. |
| **Data loss risk** | Only if the application drops records when `Send` fails. |
| **Duplicate risk** | None (idempotent retries). |
| **Availability impact** | Throttled tenant/topic slows; others protected by quotas. |
| **Consistency impact** | None. |
| **Operational response** | Raise quotas or capacity, or accept throttling as intended. Test: `TestRateLimitingBackpressure`, `TestProducerBufferBackpressure`, demo 10. |

## Coordinator (controller) failure

| | |
|---|---|
| **Failure** | Controller process down or unreachable. |
| **Detection** | Broker heartbeat errors (`broker_controller_heartbeat_errors_total`), `broker_lease_valid=0`. |
| **Recovery** | Restart the controller; it reloads `cluster.json` and consumer offsets, and gives every known broker a full session timeout to check in before declaring anything dead. |
| **Data loss risk** | None for the log. Consumer offset commits are fsynced before acknowledgement. Loss of the controller's disk loses cluster metadata: there is no replica. |
| **Duplicate risk** | Consumers cannot commit while it is down; on recovery, uncommitted work is redone. |
| **Availability impact** | Severe: after `lease` all writes stop; no failover, no rebalances, no topic changes; new idempotent producers cannot obtain a producer ID (existing ones keep theirs). Reads continue. |
| **Consistency impact** | Preserved (writes stop rather than risk split brain). |
| **Operational response** | Restart quickly; back up `data/controller`. The production fix is a consensus-replicated controller. Test: `TestControllerOutageStopsWritesAfterLease`. |

## Clock skew

| | |
|---|---|
| **Failure** | Wall clocks differ between machines, or jump. |
| **Detection** | Timestamp jumps across a failover; NTP monitoring. |
| **Recovery** | None needed for correctness. |
| **Data loss risk** | Time-based retention on a broker with a fast clock deletes segments early (retention compares record timestamps with local now). |
| **Duplicate risk** | Timestamp-based replay may start slightly early (duplicates) or late (gap) across a leader change between skewed brokers. |
| **Availability impact** | None: leases use monotonic durations, not wall-clock comparisons. |
| **Consistency impact** | None on ordering (offsets, not timestamps, define order; timestamps are clamped monotonic per partition). |
| **Operational response** | Run NTP/chrony; alert on offset > 100ms; prefer offset-based replay when precision matters. |

## Process restart (planned)

| | |
|---|---|
| **Failure** | Rolling restart for deploys or config changes. |
| **Detection** | Graceful: SIGTERM -> broker stops accepting requests, stops fetchers, flushes, writes indexes and HWM checkpoints, closes. |
| **Recovery** | On start: fast recovery (indexes valid), new incarnation -> controller moves leadership away immediately (instead of waiting for session timeout), broker catches up and rejoins ISR; preferred election restores leadership balance. |
| **Data loss risk** | None. |
| **Duplicate risk** | None. |
| **Availability impact** | Leadership moves twice per broker; in-flight requests to it are retried by clients. |
| **Consistency impact** | None. |
| **Operational response** | Restart one broker at a time; wait for `controller_under_replicated_partitions == 0` before the next. A production system would add controlled shutdown (move leadership *before* stopping). |
