# Distributed Event Streaming Platform

An educational distributed event log written from first principles in Go,
using only the standard library. It implements the core ideas behind
Kafka-style systems (partitioned append-only logs, leader/follower
replication with a high-water mark, leader epochs, idempotent producers,
consumer groups, offsets, replay, retention and compaction) and makes their
failure behavior observable through fault injection, failure tests and
metrics.

It is a distributed-systems laboratory, not a Kafka replacement. Every
simplification is stated in [Limitations](#limitations).

```
~12,800 lines of Go plus ~3,400 lines of tests · no third-party dependencies
go test ./... and go test -race ./... pass · unit, integration, failure, recovery and fuzz tests
```

## The idea

A queue answers "has this item been processed?" and forgets it. A log
answers "what is the durable, ordered history?" and keeps it; consumers only
hold a position. That one decision explains the whole design: append-only
storage, offsets, replay, retention, per-partition ordering, partition
ownership in consumer groups, and replication of an ordered history.

## Architecture

```
                         Producers (pkg/client, curl)
                                   |
                     +-------------+-------------+
                     |  gateway (optional)       |  topic-level API, key routing
                     +-------------+-------------+
                                   |  partition-aware routing to the leader
          +------------------------+-------------------------+
          |                        |                         |
   +------v------+          +------v------+           +------v------+
   |  Broker 1   |<-------->|  Broker 2   |<--------->|  Broker 3   |  followers pull
   | P0 leader   |  fetch   | P1 leader   |   fetch   | P2 leader   |  raw frames from
   | P1,P2 follow|          | P0,P2 follow|           | P0,P1 follow|  leaders
   +------+------+          +------+------+           +------+------+
          |      heartbeat + lease, metadata long-poll, ISR changes
          +------------------------+-------------------------+
                                   |
                     +-------------v-------------+
                     | controller + group        |  liveness, elections, epochs,
                     | coordinator (single node) |  ISR, topics, offsets
                     +-------------+-------------+
                                   |
            consumer groups: join / heartbeat / commit / lag
```

Details: [docs/architecture.md](docs/architecture.md).

## Core capabilities

| Area          | Implemented                                                                                                                                                                    |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Topics        | create, describe, list, delete, alter config, add partitions; per-topic retention, segment size, cleanup policy, min ISR, max event size, tenant                               |
| Storage       | segmented append-only log, versioned binary records with CRC32C, sparse offset index, crash recovery (torn-tail truncation, index rebuild), interval or per-append fsync       |
| Partitioning  | hash(key), round robin, explicit, salted hot keys; consistent-hash ring for comparison                                                                                         |
| Producers     | batching with linger, bounded buffer, retries with backoff and Retry-After, idempotence (producer ID + per-partition sequences), acks `leader` / `all` / `quorum`              |
| Replication   | leader/follower pull replication of raw frames, high-water mark, ISR by time-since-caught-up, controller-approved ISR changes, leader-epoch divergence truncation              |
| Failover      | heartbeat failure detection, ISR-based election, leader epochs as fencing tokens, broker leases, unclean election opt-in, preferred-leader election, storage-failure reporting |
| Consumers     | offset reads, batch reads, long polling, consumer groups (range / round robin / sticky), eager rebalancing with generations, commit fencing, auto-commit or manual commit      |
| Replay        | from offset, from timestamp, group reset to earliest / latest / offset / timestamp                                                                                             |
| Cleanup       | segment-level retention by time and bytes, key-based compaction with tombstones                                                                                                |
| Flow control  | broker memory budget, request concurrency limit, bounded long-poll waiters, bounded consumer prefetch                                                                          |
| Quotas        | token buckets per broker, tenant, topic, producer; partition quota per tenant                                                                                                  |
| Security      | bearer tokens, topic ACLs, tenant isolation, optional TLS, audit log lines                                                                                                     |
| Observability | Prometheus metrics (broker, partition, replication, groups, runtime), consumer lag endpoint, JSON logs, W3C trace context spans                                                |
| Tooling       | `streamctl` CLI, demo producer/consumer, gateway, load generator, fault injection API, offline segment verify/corrupt tools                                                    |

## Event lifecycle

1. The producer picks a partition (`hash(key) % n`) and appends the record
   to that partition's batch.
2. After `linger` or when full, the batch is sent to the partition leader
   with the producer ID and base sequence number.
3. The leader checks quotas and memory budget, deduplicates by sequence,
   assigns offsets and log-append time, encodes frames with CRCs and appends
   them in one write.
4. Followers fetch the new frames, verify CRCs, append, and acknowledge by
   fetching the next offset.
5. When every in-sync replica has it, the high-water mark passes the
   record: it is committed, and an `acks=all` producer is answered.
6. Consumers in each group fetch it (long poll), process it, and commit
   `offset + 1`.
7. Retention eventually deletes the segment containing it (or compaction
   removes it once a newer value for its key exists).

## Partition model

```
orders-0:  0 1 2 3 4 5 ...      offsets are partition-local
orders-1:  0 1 2 ...            orders-0@3 and orders-1@3 are unrelated
orders-2:  0 1 2 3 ...          ordering is guaranteed within a partition only
```

Same key, same partition, same order (while the partition count is fixed).
No global order across partitions, by design: see
[docs/system-design.md](docs/system-design.md#13-ordering-guarantees).

## Replication model

```
leader     [0 ............................ 97 | 98 99 ]  LEO 100
follower A [0 ............................ 97 | 98 ]     LEO 99
follower B [0 ............................ 97 ]          LEO 98
            |<------- committed (HWM = 98) ->|
            |<------- visible to consumers ->|
```

- HWM = minimum log end offset across the leader and the in-sync replicas.
- `acks=all` waits for the HWM; `acks=leader` does not and can lose
  acknowledged data on failover.
- Leader epochs in every record let a returning old leader truncate exactly
  its unreplicated suffix.
- Isolated leaders stop writing when their lease (6s) expires, before the
  controller's session timeout (10s) elects a replacement.

## Consumer groups

```
group fraud-detector, topic orders (6 partitions)
  fraud-1 -> P0 P1      each partition has exactly one owner per group
  fraud-2 -> P2 P3      groups are independent: every group sees every record
  fraud-3 -> P4 P5      lag = high-water mark - committed offset
```

Default semantics: **at-least-once** (process, then commit). Exactly-once
processing is not claimed: see
[docs/tradeoffs.md](docs/tradeoffs.md#18-exactly-once-limitations).

## Quick start

Requirements: Go 1.23+ (developed on 1.24). Linux or macOS.

```
make check                       # gofmt, go vet, go test -race
make build                       # binaries in ./bin
scripts/cluster.sh start 3       # controller :9000, brokers :9091-9093
export STREAM_BROKERS=http://localhost:9091,http://localhost:9092,http://localhost:9093

bin/streamctl topics create orders -p 6 -rf 3 min_insync_replicas=2
bin/streamctl produce orders -key user-123 '{"event":"order.created"}'
bin/streamctl consume orders -p 0
bin/streamctl cluster
```

## Local cluster

Plain processes (no Docker needed): `scripts/cluster.sh` starts a
controller and N brokers with data and logs under `./data`, and can
`kill`, `pause`, `resume` and `restart` brokers and kill/start the
controller for failure demos.

Docker: `docker compose up -d --build` runs a controller, 3 brokers, a
gateway on :8080, a demo producer and 3 consumers in group
`fraud-detector`; use `docker compose run --rm tools streamctl ...` for the
CLI and `docker compose kill broker-2` to crash a broker. (The compose file
was validated with `docker compose config`; it was not run in the
development sandbox, which has no Docker daemon.)

## Example producer

```go
cl, _ := client.New(client.Options{Brokers: []string{"http://localhost:9091"}})
p := cl.NewProducer(client.ProducerConfig{Acks: "all"}) // idempotent by default
defer p.Close(context.Background())

f, err := p.Send(ctx, client.Message{
    Topic:   "orders",
    Key:     []byte("user-123"),
    Value:   []byte(`{"order_id":42,"amount":1999}`),
    Headers: map[string]string{"event_type": "order.created", "schema_version": "1"},
})
if err != nil { /* ErrBufferFull: backpressure from the cluster */ }
res := f.Wait(ctx) // res.Partition, res.Offset, res.Err
```

Or over HTTP, directly to the partition leader:

```
curl -XPOST localhost:9091/v1/topics/orders/partitions/0/events \
  -d '{"acks":"all","records":[{"key":"user-123","value":"hello","headers":{"event_type":"order.created"}}]}'
```

or to the gateway, which routes by key: `POST localhost:8080/v1/topics/orders/events`
with `{"key":"user-123","value":"hello"}`.

## Example consumer

```go
c, _ := cl.NewConsumer(client.ConsumerConfig{Group: "fraud-detector", Topics: []string{"orders"}})
defer c.Close(context.Background())
for {
    recs, err := c.Poll(ctx, time.Second)
    if err != nil { continue }
    for _, r := range recs {
        process(r) // must be idempotent: at-least-once delivery
    }
    c.CommitRecords(ctx, recs) // commit after processing
}
```

Demo binaries: `bin/producer -topic orders -rate 20` and
`bin/consumer -group fraud-detector -topic orders -process-delay 10ms -commit after`.

## Failure demonstrations

[docs/demo.md](docs/demo.md) has 15 scripted scenarios with observed output;
[docs/fault-injection.md](docs/fault-injection.md) explains each experiment.
Highlights from runs on the development sandbox:

| Scenario                                                                                                         | Observed                                                                                                                              |
| ---------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| SIGKILL the partition leader while producing 2,000 records at 200/s, producer bootstrapped only from that broker | 2,000 records in the topic, 37 retries, 0 errors, 0 duplicates                                                                        |
| 50% of produce responses dropped after append                                                                    | 300 sent, 300 in the log, 28 duplicate retries recognized                                                                             |
| torn write in a crashed broker's active segment                                                                  | 62 bytes truncated on restart, record re-fetched from the leader                                                                      |
| leader isolated from controller and brokers                                                                      | writes accepted only within the lease, then `LEASE_EXPIRED`; new leader in epoch+1; old leader's unreplicated write truncated on heal |
| consumer killed with -9                                                                                          | group rebalanced after session timeout; survivor took all partitions                                                                  |
| 70% of traffic on one key                                                                                        | 74% of records on one of 6 partitions                                                                                                 |

The automated equivalents are in `tests/` (`TestNetworkPartitionFencesStaleLeader`,
`TestLeaderFailoverPreservesAcknowledgedWrites`,
`TestIdempotentProducerSurvivesLostResponses`, `TestDiskFailureMovesLeadership`,
`TestSlowReplicaLeavesAndRejoinsISR`, `TestControllerOutageStopsWritesAfterLease`, ...).

## Load testing

```
bin/loadgen -topic lt -partitions 6 -rf 3 -rate 10000 -duration 30s -size 1024 -json result.json
```

Reports produce and consume throughput, ack and end-to-end latency
percentiles, consumer lag, per-partition skew, and broker-side deltas
(bytes appended and replicated, network, fsync latency). Measured on a
2-vCPU sandbox with all processes co-located (one run each, not a benchmark
of the design):

| Target    | Acked/s                | Ack p50 / p99 | End-to-end p50 / p99 |
| --------- | ---------------------- | ------------- | -------------------- |
| 1,000/s   | 1,000                  | 7.9 / 33 ms   | 8.1 / 34 ms          |
| 10,000/s  | 9,991                  | 97 / 269 ms   | 125 / 399 ms         |
| 100,000/s | 28,668 (CPU-saturated) | 1.9 / 4.5 s   | 5.7 / 8.7 s          |

At saturation throughput plateaued, latency rose, memory stayed bounded,
and nothing failed. pprof shows the cost is HTTP/JSON, syscalls and GC, not
the storage engine. See [docs/performance.md](docs/performance.md).

## Observability

`GET /metrics` on every process (Prometheus text format): request rates and
latencies, produce/fetch throughput, fsync latency, partition LEO/HWM,
replica lag, ISR size, under-replicated and offline partitions, leader
elections, throttling and shedding, group rebalances, Go runtime and CPU.
Consumer lag: `streamctl groups lag G` / `GET /v1/groups/{g}/lag`.
Investigation playbooks and alert examples:
[docs/observability.md](docs/observability.md), [deploy/alerts.yml](deploy/alerts.yml).

## Scaling

Brokers add capacity only through partitions; adding brokers to a topic
with few partitions adds idle machines. Capacity math for 500k events/s,
10 KB events, 10M events/s, RF 5 and 30-day retention:
[docs/capacity-planning.md](docs/capacity-planning.md). Growth from 1 to
1,000 brokers and partition movement: [docs/scaling.md](docs/scaling.md).

## Tradeoffs

35 design decisions with alternatives, costs, failure modes and when the
other choice is better, plus the 10x / 100x / 1,000x review:
[docs/tradeoffs.md](docs/tradeoffs.md).

## Documentation index

| Document                                                      | Contents                                                                                                                        |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| [architecture.md](docs/architecture.md)                       | components, data and control paths, goroutine lifecycles, locking                                                               |
| [system-design.md](docs/system-design.md)                     | every subsystem in depth: storage, replication, election, groups, replay, retention, compaction, backpressure, routing, tracing |
| [data-model.md](docs/data-model.md)                           | record format, versioning, schema evolution, checksums, limits                                                                  |
| [consistency.md](docs/consistency.md)                         | what is strong, eventual, stale; failover guarantees                                                                            |
| [distributed-consistency.md](docs/distributed-consistency.md) | network partitions, leases, quorum, consensus, CAP in concrete scenarios                                                        |
| [failure-modes.md](docs/failure-modes.md)                     | 19 failures: detection, recovery, loss/duplicate risk, response                                                                 |
| [tradeoffs.md](docs/tradeoffs.md)                             | 35 decisions and the scale review                                                                                               |
| [capacity-planning.md](docs/capacity-planning.md)             | bandwidth, storage, memory, partitions, brokers                                                                                 |
| [scaling.md](docs/scaling.md)                                 | 1 to 1,000 brokers, partition reassignment                                                                                      |
| [evolution.md](docs/evolution.md)                             | V1 pub/sub to V12 multi-region; from in-memory pub/sub to streaming                                                             |
| [observability.md](docs/observability.md)                     | metrics reference, lag, playbooks                                                                                               |
| [performance.md](docs/performance.md)                         | benchmarks, load tests, profile, method                                                                                         |
| [multi-region.md](docs/multi-region.md)                       | primary+replicas, active-passive, active-active                                                                                 |
| [security.md](docs/security.md)                               | auth, ACLs, tenants, TLS, audit                                                                                                 |
| [fault-injection.md](docs/fault-injection.md)                 | 14 experiments                                                                                                                  |
| [demo.md](docs/demo.md)                                       | 15 demonstrations with observed output                                                                                          |
| [engineering-review.md](docs/engineering-review.md)           | 30-second to 10-minute explanations, 65 questions, staff-level discussion                                                       |
| [api/openapi/openapi.yaml](api/openapi/openapi.yaml)          | HTTP API                                                                                                                        |

## Engineering review

[docs/engineering-review.md](docs/engineering-review.md): explanations at three
lengths and 65 questions, each with what is being tested, a strong answer
grounded in this code, a follow-up, the common weak answer and the
tradeoff; plus a staff-level design discussion.

## Repository layout

```
cmd/
  broker/ controller/ gateway/        servers
  streamctl/                          operator CLI
  producer/ consumer/ loadgen/        demo clients and load generator
internal/
  record/        binary record format, CRC32C, fuzz test
  segment/       one log file + sparse index, recovery modes
  log/           segmented partition log: append, read, recovery, retention, compaction, truncation
  partition/     replica state machine: roles, epochs, HWM, ISR, idempotent producer state
  broker/        HTTP API, replication fetchers, background loops, metrics
  controller/    broker liveness, leader election, ISR changes, topics, producer IDs
  coordinator/   consumer groups, assignors, offset store
  metadata/      cluster metadata types, client cache
  routing/       partitioners, consistent-hash ring
  ratelimit/     hierarchical token buckets
  auth/          tokens, ACLs, tenants
  fault/         fault injection
  observability/ metrics, tracing, logging
  protocol/      wire types and error codes
  transport/     HTTP client/server plumbing
pkg/
  client/        Go client: metadata cache, producer, consumer, admin
  schema/        schema versioning abstraction
tests/           multi-broker integration and failure tests
api/openapi/     API specification
docs/            design documentation
scripts/         local cluster script
deploy/          auth, Prometheus and alert examples
docker/          Dockerfile
```

## Testing

```
go test ./...                 all tests (~15 s)
go test -race ./...           with the race detector (CI runs this)
go test -run '^$' -bench . ./internal/record ./internal/log ./internal/routing
go test -run '^$' -fuzz FuzzDecode ./internal/record
CLUSTER_LOGS=1 go test -v -run TestNetworkPartition ./tests/   # watch a failover in the logs
```

Covered: record encoding and corruption detection (including fuzzing),
segment index persistence, log append/read/rolling, torn-write and
corrupt-tail recovery, sealed-segment corruption, retention (time, bytes,
HWM bound), compaction (latest-per-key, tombstones, crash leftovers),
truncation, concurrent appends/reads/retention, idempotent producers and
dedup across failover and restart, HWM and acks, quorum acks, ISR shrink
and expand, divergence truncation, epoch fencing, controller elections
(clean, unclean, offline, restart, storage failure), group rebalancing,
fencing, sticky assignment, offset persistence, rate limiting, auth, and
multi-broker integration scenarios listed above.

## Limitations

Stated plainly, because the purpose is engineering judgment, not
pretending a portfolio project replaces Kafka:

- **No consensus.** The controller is a single process that persists
  metadata to a local file. It implements leader epochs, leases and
  fencing, but not Raft. If it is down, no failover, topic change or
  rebalance happens, and brokers stop accepting writes when their leases
  expire; new idempotent producers cannot obtain producer IDs. Production
  systems use a consensus-backed metadata quorum.
- **Simplified leader election** for educational purposes (single decider).
- **HTTP/1.1 + JSON** client protocol; values are UTF-8 strings in the JSON
  API (storage is binary-safe).
- **Local filesystem** storage only; no tiered or remote storage.
- **No compression, no transactions, no exactly-once processing.**
  Idempotence is per producer session.
- **Compaction** rescans the full log, holds keys in memory and is not
  throttled.
- **Indexes are held in memory** for all segments; producer state is
  rebuilt by scanning the log at startup (no snapshots).
- **Partition reassignment** between brokers is designed but not
  implemented; replica placement is not rack-aware.
- **Eager rebalancing only** (sticky assignor, no cooperative protocol).
- **Security**: static API keys from a file, ACLs, tenant isolation and
  optional server TLS. No mTLS, no identity provider, no key rotation, no
  encryption at rest.
- **Tracing** uses W3C trace context with spans exported as log lines, not
  the OpenTelemetry SDK.
- **Performance numbers** in this repository come from a 2-vCPU sandbox
  with all processes on one machine; they are not benchmarks of the design.

## Roadmap

1. Raft-replicated controller with delta metadata and batched elections.
2. Binary batch protocol and batch-level record format with compression.
3. Coordinator sharding; offsets in a replicated internal topic.
4. Partition reassignment with throttled data movement; rack-aware placement.
5. Cooperative rebalancing and static group membership.
6. mmap'd, lazily loaded indexes; producer-state snapshots.
7. Tiered storage for old segments.
8. Cross-cluster mirroring with offset translation (active-passive).

## From the earlier portfolio projects

This project grew out of an in-memory pub/sub system (fan-out, subscriber
lifecycle, buffering, slow subscribers, backpressure). What changes when
pub/sub becomes durable, partitioned, replicated, replayable and
horizontally scalable is the subject of
[docs/evolution.md](docs/evolution.md#from-in-memory-pubsub-to-distributed-event-streaming).
It also reuses ideas from the other projects: the token-bucket rate limiter
(`internal/ratelimit`, now hierarchical and all-or-nothing), the HTTP
server patterns (middleware, graceful shutdown, load shedding), the worker
pool discipline (every goroutine owned and stoppable), the cache (metadata
cache with TTL and single flight), and the load balancer (replaced by
partition-aware routing, with a generic balancer still appropriate in front
of the stateless gateway).

Module path: `github.com/michael-emmanuel/eventstream` (rename in `go.mod` to
publish under your own account).
