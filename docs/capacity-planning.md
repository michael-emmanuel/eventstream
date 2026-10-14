# Capacity Planning

All figures below are arithmetic from stated inputs. Where a hardware
number is needed (NIC speed, disk bandwidth, disk size) it is labeled as an
**assumption**; substitute your own. The only measured numbers come from
the load generator on the development sandbox and are labeled as such
([performance.md](performance.md#measured-runs)). Units are decimal
(1 KB = 1,000 bytes, 1 GB = 10^9 bytes) to keep the math readable.

## Formulas

```
ingress_bytes/s       = events/s x avg_event_bytes x (1 + format_overhead)
replication_bytes/s   = ingress x (RF - 1)              leader -> followers
disk_write_bytes/s    = ingress x RF                    cluster-wide
storage               = ingress x RF x retention_seconds
consumer_egress/s     = ingress x consumer_groups       (tail reads)
broker_network_in/s   = ingress + replication            (cluster-wide, spread over brokers)
broker_network_out/s  = replication + consumer_egress
partitions_needed     >= max(ingress / per_partition_write_limit,
                             ingress / per_consumer_throughput)
brokers_needed        = max over resources (network, disk bandwidth, disk capacity, CPU)
                        of (cluster demand / per-broker usable capacity)
```

Format overhead in this project: 56 bytes per record plus headers
([data-model.md](data-model.md)). For 2 KB events with ~50 bytes of headers,
about 5 percent; the reference numbers below use payload bytes and note
where overhead matters.

## Reference workload

```
500,000 events/s, 2 KB average, RF = 3, 7-day retention, 3 consumer groups
```

| Quantity                       | Calculation                         | Result                     |
|--------------------------------|-------------------------------------|----------------------------|
| raw ingress                    | 500,000 x 2,000 B                   | **1.0 GB/s** (8 Gbit/s)    |
| with ~5% format overhead       | 1.0 x 1.05                          | 1.05 GB/s                  |
| replication bandwidth          | 1.0 GB/s x (3 - 1)                  | **2.0 GB/s**               |
| disk writes (cluster)          | 1.0 GB/s x 3                        | 3.0 GB/s                   |
| storage per hour (raw)         | 1.0 GB/s x 3,600 s                  | **3.6 TB/h**               |
| storage per hour (RF 3)        | 3.6 TB x 3                          | 10.8 TB/h                  |
| storage per day (raw)          | 3.6 TB x 24                         | **86.4 TB/day**            |
| storage per day (RF 3)         | 86.4 TB x 3                         | **259.2 TB/day**           |
| 7-day retention (RF 3)         | 259.2 TB x 7                        | 1.81 PB                    |
| consumer egress (3 groups)     | 1.0 GB/s x 3                        | 3.0 GB/s                   |
| broker network in (cluster)    | 1.0 + 2.0                           | 3.0 GB/s                   |
| broker network out (cluster)   | 2.0 + 3.0                           | 5.0 GB/s                   |

### Brokers needed

**Assumptions** (replace with your hardware): usable 1.2 GB/s per direction
per broker NIC after headroom; usable 0.5 GB/s sustained disk write per
broker at 50 percent headroom; 30 TB usable disk per broker at 75 percent
fill; CPU from the measured efficiency below.

| Resource        | Cluster demand | Per broker | Brokers |
|-----------------|----------------|------------|---------|
| network out     | 5.0 GB/s       | 1.2 GB/s   | 5       |
| network in      | 3.0 GB/s       | 1.2 GB/s   | 3       |
| disk write      | 3.0 GB/s       | 0.5 GB/s   | 6       |
| disk capacity   | 1.81 PB        | 30 TB      | **61**  |

Retention, not throughput, sizes this cluster. Options: shorter local
retention plus tiered storage (1 day local = 259 TB = 9 brokers, older data
in object storage), compression (text payloads commonly compress several
fold, which also cuts network), or RF 2 for re-derivable data.

### CPU (this implementation)

Measured on the 2-vCPU sandbox ([performance.md](performance.md#measured-runs)):
at 28,668 records/s (1 KB, RF 3, `acks=all`, one consumer group), the three
brokers together used 18.6 CPU-seconds in 10 seconds, about **15,000
records/s per broker core**, including replication and serving one consumer
group, using HTTP/JSON. At that efficiency the reference workload needs
roughly 500,000 / 15,000 ~ **33 broker cores** before headroom. The profile
shows JSON decoding, syscalls and GC dominate; a binary batch protocol is
the lever, not more cores.

### Memory

| Consumer of memory                 | Sizing                                                         |
|------------------------------------|----------------------------------------------------------------|
| page cache for tail reads          | keep the last T seconds of leader data hot: 60 s x 1.0 GB/s = 60 GB cluster-wide (per broker = 60 GB / brokers). Consumers that fall out of this window read from disk and compete with writes. |
| produce in-flight budget           | `-max-inflight-produce-bytes` (64 MB default) per broker       |
| fetch buffers                      | concurrent fetches x `max_bytes` (bounded by request limit)    |
| sparse index (this implementation) | 8 bytes per 4 KB of log = 1/512 of disk: 30 TB/broker -> ~59 GB. **Limitation**: indexes are held in memory; production needs mmap'd, lazily loaded indexes. |
| producer dedup state               | ~100 bytes x active producers x partitions                     |
| Go heap headroom                   | GOGC default doubles live heap; budget 2x live allocations     |

### Partitions

**Assumption**: one consumer instance processes 10 MB/s of this topic
(application-dependent; measure yours).

```
consumer parallelism:  1.0 GB/s / 10 MB/s  = 100 partitions minimum
headroom for growth (2x) and failover      = 200 partitions
spread over brokers:   200 x RF 3 / 9 brokers ~ 67 replicas per broker
```

Partition count is hard to change later (keys remap), so size it for the
consumer parallelism you expect at peak in two years, not today.

### Consumers

```
consumer instances per group  = ceil(ingest / per_consumer_throughput) x headroom
                              <= partitions   (extra instances sit idle)
catch-up time after an outage = backlog / (consumer_throughput - ingest)
```

Example: a group down for 1 hour has 3.6 TB of backlog. With 200 consumers
at 10 MB/s = 2.0 GB/s against 1.0 GB/s ingest, catch-up takes
3.6 TB / 1.0 GB/s net = 1 hour. With only 120 consumers (1.2 GB/s), it takes
3.6 TB / 0.2 GB/s = 5 hours. Spare consumer capacity is what determines
recovery time.

## Sensitivity

### 10 KB events (500,000/s)

| Quantity                 | Result           | vs reference |
|--------------------------|------------------|--------------|
| ingress                  | 5.0 GB/s         | 5x           |
| replication              | 10.0 GB/s        | 5x           |
| disk writes              | 15.0 GB/s        | 5x           |
| storage/day (RF 3)       | 1.296 PB         | 5x           |
| network out (3 groups)   | 25.0 GB/s        | 5x           |

Per-record CPU and request overhead stay the same, so CPU grows less than
5x; network and disk grow exactly 5x. Bytes, not events, become the
constraint. Format overhead drops to ~1 percent.

### 10 million events/s (2 KB)

| Quantity                 | Result           | vs reference |
|--------------------------|------------------|--------------|
| ingress                  | 20 GB/s          | 20x          |
| replication              | 40 GB/s          | 20x          |
| disk writes              | 60 GB/s          | 20x          |
| storage/day (RF 3)       | 5.18 PB          | 20x          |
| broker cores (measured efficiency) | ~670   | 20x          |

At this rate per-record costs dominate: the JSON API and per-record
storage format would consume hundreds of cores. Required changes: binary
batch protocol, batch-level record format with compression, sharded
coordinator, Raft controller with delta metadata, and thousands of
partitions (consumer parallelism of ~2,000 at 10 MB/s each).

### Replication factor 5 (reference otherwise)

| Quantity                 | Result           | vs RF 3     |
|--------------------------|------------------|-------------|
| replication              | 4.0 GB/s         | 2x          |
| disk writes              | 5.0 GB/s         | 1.67x       |
| storage/day              | 432 TB           | 1.67x       |
| leader network out       | +2 GB/s          |             |

Buys tolerance of two failures with writes still available
(`min_insync_replicas=3`). Usually reserved for small critical topics.

### 30-day retention (reference otherwise)

```
259.2 TB/day x 30 = 7.78 PB on disk (RF 3)
```

At 30 TB per broker that is ~260 brokers whose CPU and network would be
mostly idle: the signal to adopt tiered storage, keeping a few days local.

## Worked check against a measured run

Loadgen on the sandbox, 10,000 records/s target, 1 KB values, RF 3, 10 s:

| Quantity                       | Predicted                                  | Measured (broker metric deltas)          |
|--------------------------------|--------------------------------------------|-------------------------------------------|
| records acknowledged           | 100,000                                    | 99,940                                    |
| encoded bytes appended         | 99,940 x ~1,116 B (1,024 + 56 + headers + key) = ~111.5 MB | 111.5 MB (`broker_produce_bytes_total`) |
| replication bytes              | 2 x appended = ~223 MB                      | 223.0 MB (`broker_replica_bytes_total`)  |
| network in                     | produce JSON ~ appended size               | 109.7 MB                                  |

The 2x replication ratio held exactly in every run, which is a useful
sanity check that the monitoring and the model agree.

## Planning checklist

1. Peak events/s and average/percentile event size (size distribution
   matters for batching and limits).
2. Retention required by replay use cases (debugging, rebuilds, new
   consumers) and by compliance.
3. Number of consumer groups and their speed.
4. Durability: RF and `min_insync_replicas` per topic.
5. Compute brokers per resource; take the max; add headroom for one broker
   failure (its leaders move to the others) and for catch-up traffic.
6. Partition count for peak consumer parallelism.
7. Validate with `loadgen` on target hardware before committing.
