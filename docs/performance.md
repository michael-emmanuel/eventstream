# Performance

Principle: measure first, then optimize the measured bottleneck. This
document explains the performance-relevant design, shows how to measure,
and reports what was actually measured on the development machine. It does
not extrapolate those numbers to other hardware.

## Environment of all numbers in this document

* Sandbox VM, **2 vCPUs** (Intel Xeon @ 2.80GHz), local SSD-backed
  filesystem, Go 1.24, Linux.
* For cluster runs, the controller, 3 brokers and the load generator all ran
  **on the same 2 vCPUs**, communicating over loopback. CPU was the binding
  constraint; real deployments separate these.
* Each number comes from one run. Treat them as orders of magnitude and
  ratios, not as benchmarks of the design.

## Micro-benchmarks

`go test -run '^$' -bench . -benchmem ./internal/record ./internal/log ./internal/routing`

| Benchmark | Result | What it shows |
|---|---|---|
| `BenchmarkEncode` (1 KiB value) | 137 ns/op, 0 allocs | encoding + CRC32C is cheap (hardware CRC) |
| `BenchmarkDecode` (1 KiB value) | 285 ns/op, 3 allocs | decode allocates headers; still ~4 GB/s |
| `BenchmarkAppend1KB` | 3.3 µs/op, ~315 MB/s | one `pwrite` per record (page cache, no fsync) |
| `BenchmarkAppendBatch100x1KB` | 176 µs per 100, ~580 MB/s | batching amortizes per-write cost: ~1.8x |
| `BenchmarkAppendSync1KB` | 305 µs/op, ~3.4 MB/s | fsync per append: ~100x slower |
| `BenchmarkRead` (64 KiB reads) | 170 µs/op, 315 KB allocated, 79 allocs | read path allocates a 32 KiB buffer per segment read: an identified optimization target |
| `BenchmarkHashPartition` | 8.9 ns/op | partitioning is free |

Two conclusions drive the design: **fsync per append is two orders of
magnitude more expensive than a buffered append** (hence interval fsync +
replication for durability), and **batching nearly doubles raw append
throughput** before even counting network and HTTP savings.

## Measured runs

`loadgen -rate R -duration 10s -size 1024` against a 3-broker local
cluster, topic with 6 partitions, RF 3, `acks=all`, one consumer group of
2 consumers, 4 producer instances.

| Target rate | Acked rate | Payload MB/s | Ack p50 / p95 / p99 (ms) | End-to-end p50 / p99 (ms) | Max lag | Errors |
|---|---|---|---|---|---|---|
| 1,000/s | 1,000/s | 1.0 | 7.9 / 16.0 / 33.0 | 8.1 / 33.9 | 668 | 0 |
| 10,000/s | 9,991/s | 10.2 | 96.7 / 207 / 269 | 125 / 399 | 8,908 | 0 |
| 100,000/s | **28,668/s** (saturated) | 29.4 | 1,853 / 3,775 / 4,527 | 5,691 / 8,693 | 131,079 | 0 |

Broker-side deltas (sum over 3 brokers) for the same runs:

| Target | Appended bytes | Replicated bytes | Ratio | Network out | Avg fsync | Broker CPU-s / 10 s |
|---|---|---|---|---|---|---|
| 1,000/s | 11.2 MB | 22.3 MB | 2.00 | 37.9 MB | 0.61 ms | 9.7 |
| 10,000/s | 111.5 MB | 223.0 MB | 2.00 | 356 MB | 1.54 ms | 13.3 |
| 100,000/s | 320.7 MB | 641.3 MB | 2.00 | 1,023 MB | 2.11 ms | 18.6 |

Reading the results:

* **Replication costs exactly (RF-1) x ingress** in every run, as the
  capacity model predicts.
* **Network out is ~3.2x appended bytes**: 2x replication plus consumer
  fetches, whose JSON encoding is larger than the binary record.
* **Saturation behaves as designed.** At 3.5x the achievable rate, the
  acknowledged rate plateaued, latency grew into seconds (records queued in
  producer buffers), memory stayed bounded (loadgen heap 8 MB), and nothing
  failed. That is backpressure working: overload became latency at the
  edge, not memory growth or errors in the middle.
* **Lag at saturation** reached ~131k records but recovered to 0 after
  production stopped.
* The ~29k records/s ceiling is CPU on a machine where 3 brokers, a
  controller and a load generator share 2 vCPUs. It says how expensive the
  current protocol is per record, not what the design can do on separate
  hardware.

Reproduce:

```
scripts/cluster.sh start 3
bin/loadgen -topic lt -partitions 6 -rf 3 -rate 10000 -duration 30s -size 1024 -json result.json
```

Variants worth running: `-acks leader` (removes the follower round trip
from ack latency), `-size 100` and `-size 10000` (per-record vs per-byte
costs), `-hot-key-share 0.7` (skew), `-process-delay 1ms` (slow consumers),
broker `-fsync always` (fsync cost under load).

## Where the CPU goes

Broker profile under ~10,000 records/s (`broker -pprof-addr localhost:6061`,
`go tool pprof -top -cum`, 8-second CPU profile; the broker was ~33 percent
busy):

| Share (cumulative) | Where |
|---|---|
| ~47% | `net/http` connection serving overall |
| ~20% | produce handler (`handleProduce`) |
| ~20% | raw syscalls (network reads/writes, file I/O) |
| ~16% | follower fetch loop (`runFetcher`), of which ~12% HTTP round trip |
| ~15% | serving follower fetches (`handleReplicaFetch`), ~8% log reads |
| ~12% | JSON decoding of produce requests |
| ~13% | garbage collection (background mark workers) |
| ~16% | allocation (`mallocgc`), notably slice growth |

So the cost is in the **protocol** (HTTP request per batch per partition,
JSON), **allocation**, and **syscalls**, not in the storage engine. The
next optimizations, in order of expected payoff:

1. Binary, multiplexed client protocol with multi-partition requests (one
   request carries batches for many partitions): fewer syscalls and no
   JSON.
2. Batch-level record format so producers send already-encoded batches the
   broker can append and replicate without decoding each record.
3. Buffer pooling on the read path (`BenchmarkRead` shows 315 KB allocated
   per 64 KB read).
4. Multi-partition replication fetches (one request per source broker
   instead of one per partition).

## Design notes by topic

**Sequential disk writes.** Every append goes to the end of the active
segment in one `pwrite`; reads of recent data are sequential too. Random
I/O only happens for consumers reading far behind.

**fsync.** Interval fsync (200 ms default) runs on the broker's flush loop,
off the request path. `-fsync always` puts an fsync on every produce
request: correct for single-replica durability, ~100x slower per append in
the micro-benchmark.

**Batching.** See [system-design.md](system-design.md#11-batching-and-long-polling).
Under load batches grow automatically (records accumulate while the previous
batch is in flight), which is why throughput degrades gracefully.

**Page cache.** The broker never caches records in the Go heap. Recent
segments are in the OS page cache, which serves tail reads for consumers and
followers from memory and survives broker process restarts. Consumers that
replay old data evict it, which is why replay can hurt the latency of
tail consumers (noisy neighbor).

**Memory mapping.** Not used for segments: reads use `pread` with a
buffered reader, which is simpler and portable, and the JSON API copies data
anyway. mmap would help index files at large scale (see capacity planning).
Zero-copy (`sendfile`) only makes sense with a binary protocol that can
send stored bytes unchanged; replication already ships stored bytes
unchanged, so it would be the first beneficiary.

**Serialization.** Binary records with hardware CRC cost ~140 ns to encode
1 KiB; the JSON layer around them costs far more. That ratio is the main
argument for a binary client protocol.

**Compression.** Not implemented; see [tradeoffs.md](tradeoffs.md#26-compression).

**Network syscalls.** One HTTP request per batch per partition. With many
partitions and small batches, request count (not bytes) becomes the limit.

**Lock contention.** No lock is shared across partitions on the data path.
Within a partition, appends serialize on the append lock (required: one
order); readers do not take it. The rate limiter uses one mutex per
broker and is the most likely contention point at very high request rates;
a mutex profile (`/debug/pprof/mutex`, after
`runtime.SetMutexProfileFraction`) would confirm before changing it.

**Goroutine scheduling.** One goroutine per request, per follower
partition, per producer partition and per consumer partition. Goroutines
are cheap, but per-partition goroutines scale with partition count; at
thousands of partitions per broker, multiplexing (one fetcher per source
broker) matters more than scheduler tuning.

**Garbage collection.** ~13 percent of broker CPU in the profile. The
dominant allocations are per-request JSON structures and per-read buffers.
Reduce allocations first; tune `GOGC`/`GOMEMLIMIT` second.

## How to find a bottleneck

1. **State the symptom with a metric**: throughput plateau, p99 latency,
   lag. Pick the SLO-relevant one.
2. **Check saturation of each resource**: CPU (`process_cpu_seconds_total`),
   disk latency (fsync histogram), network (`broker_network_*`), memory
   (`broker_inflight_produce_bytes`, heap), and limits (throttling,
   rejections). The saturated one is the bottleneck; if none is saturated,
   it is latency-bound (waiting: replication round trips, a slow follower,
   downstream).
3. **Profile the saturated process**:
   ```
   broker -pprof-addr localhost:6061 ...
   go tool pprof -top -cum http://localhost:6061/debug/pprof/profile?seconds=10
   go tool pprof http://localhost:6061/debug/pprof/heap
   go tool pprof http://localhost:6061/debug/pprof/allocs
   curl localhost:6061/debug/pprof/goroutine?debug=1   # leaks, parked waiters
   ```
4. **Reproduce in a benchmark** if the hotspot is a function
   (`go test -bench`), then change one thing and compare
   (`benchstat` on repeated runs).
5. **Verify concurrency safety** of any change with `go test -race ./...`.
6. **Re-run the load test** and confirm the system-level metric moved.

## Tools

```
go test ./...                                  correctness
go test -race ./...                            data races (CI runs it)
go test -run '^$' -bench . -benchmem ./...     micro-benchmarks
go test -run '^$' -fuzz FuzzDecode ./internal/record   decoder robustness
go test -cpuprofile cpu.out -bench BenchmarkRead ./internal/log
go tool pprof cpu.out
bin/loadgen ...                                system-level measurement
```
