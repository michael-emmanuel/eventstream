# Architecture

This document describes what is built, how the pieces fit, and which
process owns which state. For *why* each decision was made, see
[tradeoffs.md](tradeoffs.md); for the guarantees that result, see
[consistency.md](consistency.md).

## The one idea

An event stream is not a queue. A queue answers "has this item been
processed?" and forgets the item once the answer is yes. A log answers
"what is the durable, ordered history of events?" and keeps that history
for a retention period regardless of who has read it. Consumers do not
remove anything; they hold a *position* (an offset) in the history.

Everything below follows from that choice:

| Because the system is a log...            | ...it needs                                   |
|-------------------------------------------|-----------------------------------------------|
| history outlives consumption              | append-only storage, retention, compaction    |
| readers hold positions, not leases        | offsets, committed offsets, replay            |
| order is part of the data                 | partitions with a single writer (the leader)  |
| one ordered sequence cannot scale         | partitioning, per-partition ordering only     |
| history must survive machine loss         | replication, high-water mark, leader epochs   |
| many independent readers                  | consumer groups with partition ownership      |

## Components

```
                         Producers (pkg/client, curl)
                                   |
                     +-------------+-------------+
                     |  optional gateway         |   topic-level API, key routing,
                     |  (cmd/gateway, stateless) |   re-batching; behind a generic LB
                     +-------------+-------------+
                                   |  partition-aware routing:
                                   |  every request goes to the partition leader
          +------------------------+-------------------------+
          |                        |                         |
   +------v------+          +------v------+           +------v------+
   |  Broker 1   |<-------->|  Broker 2   |<--------->|  Broker 3   |   pull replication
   | P0 leader   |  fetch   | P1 leader   |   fetch   | P2 leader   |   (followers fetch
   | P1 follower |          | P2 follower |           | P0 follower |    raw frames)
   | P2 follower |          | P0 follower |           | P1 follower |
   +------+------+          +------+------+           +------+------+
          |  heartbeat + lease     |                         |
          |  metadata long-poll    |                         |
          |  ISR change requests   |                         |
          +------------------------+-------------------------+
                                   |
                       +-----------v------------+
                       |  Controller            |  brokers, topics, partition leaders,
                       |  + group coordinator   |  leader epochs, ISR, producer IDs,
                       |  (single process,      |  consumer groups, committed offsets
                       |   file-backed)         |
                       +-----------+------------+
                                   |
                     join / heartbeat / commit / lag
                                   |
             +---------------------+----------------------+
             |                     |                      |
        Consumer 1            Consumer 2             Consumer 3
        (group fraud-detector: each partition owned by one member)
```

| Process    | Package(s)                                   | Owns (durable state)                                     |
|------------|----------------------------------------------|----------------------------------------------------------|
| broker     | `internal/broker`, `partition`, `log`, `segment`, `record` | partition logs (segments + indexes), HWM checkpoints |
| controller | `internal/controller`, `internal/coordinator` | `cluster.json` (topics, assignments, epochs, ISR, next producer ID), `consumer-offsets.log` |
| gateway    | `cmd/gateway` + `pkg/client`                 | nothing (stateless)                                      |
| clients    | `pkg/client`                                 | nothing durable; producer sequence numbers in memory     |

Supporting packages: `routing` (partitioners), `ratelimit` (token buckets),
`metadata` (snapshot types and client cache), `protocol` (wire types and
error codes), `transport` (HTTP plumbing), `auth` (tokens, ACLs, tenants),
`fault` (fault injection), `observability` (metrics, tracing, logs),
`pkg/schema` (schema versioning abstraction).

## Data path

### Produce

```
client                               leader broker                         followers
  |  POST .../partitions/2/events      |                                      |
  |  {producer_id, base_sequence,      |                                      |
  |   acks, records}                   |                                      |
  |----------------------------------->| 1 authenticate, authorize (ACL)      |
  |                                    | 2 memory budget: in-flight bytes     |
  |                                    |   (503 BROKER_OVERLOADED if full)    |
  |                                    | 3 token buckets: broker/tenant/      |
  |                                    |   topic/producer (429 THROTTLED)     |
  |                                    | 4 idempotency: sequence check        |
  |                                    |   duplicate -> original offsets      |
  |                                    | 5 assign offsets + log-append time,  |
  |                                    |   encode frames, ONE write() call    |
  |                                    | 6 acks=leader: respond now           |
  |                                    |   acks=all: wait until HWM > offset  |
  |                                    |<---- fetch(offset=N, last_epoch) ----|
  |                                    |----- raw frames -------------------->| verify CRC, append
  |                                    |<---- fetch(offset=N+k) --------------| (= acknowledgement)
  |                                    | 7 HWM = min LEO over ISR -> advance  |
  |<-------- {base_offset, ...} -------|                                      |
```

### Consume

```
consumer                          leader broker
  | GET .../events?offset=O&max_events=500&wait_ms=2000
  |-------------------------------->| read [O, HWM) via sparse index,
  |                                 | at most max_bytes / max_events;
  |                                 | if empty: park until HWM moves or timeout
  |<---- records, next_offset ------|
  | process
  | POST controller /v1/groups/G/offsets {member, generation, offset=last+1}
```

## Storage layout

```
data/broker-1/
  orders-0/
    topic-id                          guards against a deleted+recreated topic reusing old data
    high-watermark                    checkpointed HWM (text)
    00000000000000000000.log          sealed segment (immutable)
    00000000000000000000.index        sparse index: (relative offset uint32, position uint32)
    00000000000000183712.log          active segment (only file written to)
  orders-3/
    ...
data/controller/
  cluster.json                        all metadata, rewritten atomically on change
  groups/consumer-offsets.log         append-only JSON lines, compacted on growth
```

The on-disk record format is described in [data-model.md](data-model.md).
Segment, index and recovery mechanics are in
[system-design.md](system-design.md#storage-engine).

## Control path

* **Membership and liveness.** Brokers heartbeat the controller (default 1s).
  Each heartbeat grants a *lease* (default 6s of a 10s session). A broker
  silent for the session timeout is declared dead.
* **Metadata.** Brokers long-poll `GET /v1/metadata?min_version=v+1` and
  apply role changes within milliseconds of a controller decision.
  Clients cache metadata from any broker and refresh on `NOT_LEADER` or
  transport errors.
* **Leader election.** On broker death the controller picks the first live
  replica in the ISR (in replica order), increments the partition's
  *leader epoch*, persists, and publishes. No ISR member alive: the partition
  goes offline unless the topic allows unclean election.
* **ISR changes.** Leaders propose shrink/expand; the controller accepts
  only from the current leader in the current epoch.
* **Consumer groups.** The coordinator (same process) runs join/heartbeat/
  leave, computes assignments, fences commits by generation, and stores
  offsets.

## Goroutines and lifecycles

Every goroutine is started by an owner that can stop it and wait for it.

| Owner        | Goroutine                          | Stopped by                         |
|--------------|------------------------------------|------------------------------------|
| Broker       | HTTP server                        | `Stop` (Shutdown) / `Kill` (Close) |
| Broker       | heartbeat, metadata, ISR, flush, checkpoint, maintenance loops | broker context cancel + WaitGroup |
| Broker       | one fetcher per followed partition | metadata (role change) or `Stop`   |
| Controller   | broker session expiry loop         | `Stop`                             |
| Coordinator  | member session expiry loop         | `Stop`                             |
| Producer     | one sender per partition           | `Close` (after `Flush`)            |
| Consumer     | heartbeat loop; one fetcher per assigned partition | `Close`; fetchers restart on each rebalance |
| HTTP request | long-poll fetch                    | data arrives, timeout, client disconnect, or broker context cancel |

Long-poll waiters are bounded (`MaxLongPollWaiters`); request concurrency is
bounded (`MaxConcurrentRequests`). No channel in the system is unbounded.

## Concurrency inside a partition

```
Log (internal/log)
  appendMu   serializes writers (append, truncate, reset)        writers only
  maintMu    serializes retention / compaction / truncation       maintenance only
  mu         RWMutex over the segment slice; readers hold it shared during a read,
             so a segment cannot be deleted or swapped under them. Appends to the
             active segment do not take it (only rolling does).
  Segment    size/next offset published with atomics after the bytes are written;
             readers use pread (ReadAt) and never see unwritten bytes.

Partition (internal/partition)
  appendMu   serializes log mutations and guards producer (dedup) state
  mu         replication state: role, epoch, ISR, follower progress, HWM, notify channel
             never held across disk I/O or while waiting
  lock order: Partition.appendMu -> Partition.mu -> Log locks
```

There is no broker-wide lock on the data path: two partitions never contend.
The broker's partition map uses an RWMutex that is read-locked for lookups.

## Simplifications (stated plainly)

* **Controller is a single process with file-backed state. No Raft, no
  consensus.** It is a single point of failure for *changes* (failover,
  topic creation, rebalances). See [distributed-consistency.md](distributed-consistency.md).
* **HTTP/1.1 + JSON** for clients. Replication ships raw binary frames over
  HTTP. A production system would use a binary, multiplexed protocol.
* **Local filesystem** per broker. No tiered or remote storage.
* **No transactions** and no exactly-once processing. Idempotent producers
  only (per partition, per producer session).
* **No compression** (config accepted only as `none`).
* **Partition reassignment** (moving replicas between brokers) is designed
  in [scaling.md](scaling.md) but not implemented; replicas are placed at
  topic creation.
* **Security**: static API keys, ACLs, tenants and optional TLS. No identity
  provider, no key rotation. See [security.md](security.md).
