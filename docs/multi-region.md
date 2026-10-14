# Multi-Region Architecture

Not implemented. This document evaluates three options against this
system's design and recommends an evolution path.

## Constraints that do not go away

* **Latency**: inter-region round trips are tens to hundreds of ms
  (vs < 1 ms within a zone). Any synchronous cross-region step puts that on
  every write.
* **Partitions between regions are normal**, not exceptional.
* **Offsets are per replica set.** A partition mirrored into another
  cluster gets its own offsets there; offset 1,000 in region A is not
  offset 1,000 in region B. Consumer failover needs offset translation.
* **Ordering is defined by one leader.** Two regions independently
  accepting writes to the "same" partition produce two orders.

## Option A: single-region primary with remote replicas

```
Region A (primary)                       Region B
brokers + controller  --- async --->     mirror brokers (read-only topics)
producers, consumers                     local consumers (read replicas)
```

* **Replication latency**: asynchronous; region B lags by the mirroring
  delay (seconds).
* **Ordering**: preserved per partition (mirror copies in order).
* **Conflict resolution**: none needed; B never writes.
* **Failover**: manual promotion of B; producers repointed.
* **Data sovereignty**: data leaves its region; may be disallowed.
* **RPO**: the mirror lag at failure (unacknowledged tail lost).
* **RTO**: minutes (promotion + client redirection).
* **Cost**: 2x storage for mirrored topics + inter-region bandwidth.
* **Complexity**: low.

Good for: read locality (analytics in another region), disaster recovery
for non-critical data.

## Option B: active-passive regions

Same as A, but B is a full standby cluster ready to take writes:

* Mirror topics **and** consumer group offsets (translated: mirror records
  a mapping of source offset -> target offset at intervals).
* On failover: stop producers in A (if reachable), promote B, start
  consumers in B from translated offsets (they reprocess a little: at-least-once
  across regions too).
* **RPO**: mirror lag. **RTO**: minutes, automatable.
* **Risk**: split brain if A is not actually down and keeps accepting
  writes; requires a fencing decision outside both regions (a third-region
  witness or a human).
* **Failback** is a second migration: B -> A mirroring, then switch back.

## Option C: active-active regions

Both regions accept writes.

* **For the same keys**: two leaders, two orders. Requires conflict
  resolution (last-writer-wins by timestamp, which needs well-behaved
  clocks; CRDTs; or application merge) and loses the log's main property,
  a single order per key.
* **Partitioned by home region** (users in EU write to EU, US to US;
  each region mirrors the other's topics read-only): each key still has one
  writer and one order; global readers see both regions' streams. This is
  active-active for the *cluster*, active-passive per *key*.
* **Synchronous stretch cluster** across three regions (one replica each,
  `acks=all`, Raft controller spanning regions): one order, RPO 0, but
  every write pays inter-region latency and the controller quorum spans
  regions.
* **Cost/complexity**: highest; every cross-region failure mode applies.

## Preserving ordering across regions

* Single writer per key at any moment (home region), and a fenced
  handover when a key's home changes (stop writes in old region, drain
  mirror, then accept in the new region).
* Within a key, carry a monotonic sequence number in the payload so
  consumers can detect reordering or gaps after failover.
* Do not rely on timestamps from different regions for ordering.

## Comparison

| | A: primary + replicas | B: active-passive | C: active-active (home region) | C: stretch (sync) |
|---|---|---|---|---|
| write latency | local | local | local | + inter-region RTT |
| RPO | mirror lag | mirror lag | mirror lag for remote reads | 0 |
| RTO | manual | minutes | regional writes unaffected | seconds |
| ordering | preserved | preserved | per home region | preserved |
| conflicts | none | split-brain risk at failover | none if homes are respected | none |
| sovereignty | data copied out | data copied out | can keep data home | data everywhere |
| cost | 2x for mirrored topics | 2x | 2x+ | 3x + bandwidth |
| complexity | low | medium | high | high |

## What this project would evolve toward

1. **Option B first.** A mirror process is a consumer group in region A and
   an idempotent producer in region B (both already exist in `pkg/client`),
   plus an offset-translation topic. The idempotent producer prevents
   duplicates from mirror retries; the mirror's committed offsets give its
   exactly where-to-resume point.
2. **Home-region active-active** for topics keyed by user/tenant once
   applications can route writes by home region.
3. **Stretch clusters** only for small, critical topics where RPO 0
   justifies the latency, and only after the controller is Raft-based.

## If the two regions lose connectivity

* A/B: the primary keeps working; the mirror lag grows; failover would lose
  the unmirrored tail. Do not fail over automatically on a network
  partition alone: you cannot distinguish "A is down" from "the link is
  down" without a witness.
* Home-region active-active: both regions keep serving their own users;
  cross-region views go stale and catch up when the link returns.
* Stretch: the side with the controller quorum keeps writing; the other
  side stops (consistency over availability, as in a single region).

What to sacrifice:

* To stay available during a region partition: give up a single global
  order and RPO 0 (async, home-region writes).
* To keep a single order: give up write availability on the minority side
  and pay inter-region latency on every write.
