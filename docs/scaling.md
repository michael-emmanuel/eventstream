# Scaling Model and Partition Movement

## The rule

Brokers add capacity only through partitions. A topic with 4 partitions
has at most 4 leaders, so at most 4 brokers do its write work and at most
4 consumers per group do its read work, however many brokers exist. Adding
brokers to a cluster whose busy topics have few partitions adds idle
machines. Throughput scales with *partitions placed on more brokers*, and
partition count is fixed per topic until someone migrates it.

## 1 broker

* All partitions led by one process; RF must be 1. No failover: broker loss
  is total unavailability, disk loss is total data loss.
* Metadata trivial; the controller can share the machine.
* Useful for development and for learning the storage engine.

## 3 brokers

* RF 3 with `min_insync_replicas=2`: tolerates one failure with writes
  available. Leadership spreads round-robin (`TestCreateTopicSpreadsLeaders`:
  6 partitions -> 2 leaders per broker).
* Losing a broker moves its leaders to the other two: each survivor carries
  1.5x its normal leader load. Plan headroom for that.
* This is the local cluster in `scripts/cluster.sh` and `docker-compose.yml`.

## 10 brokers

* Partitions per broker matters more than brokers: a 60-partition topic at
  RF 3 is 180 replicas = 18 per broker.
* Replica placement starts to need **rack/zone awareness** (not implemented):
  three replicas on three brokers in the same rack do not survive a rack
  power failure.
* Leader imbalance after failures becomes visible; preferred-leader election
  (`streamctl elect-preferred`) fixes it.
* Adding a broker requires **moving partitions** (below), which this
  project does not automate.

## 100 brokers

* Thousands of partitions; metadata snapshots of hundreds of KB to MB.
  Sending full snapshots to every broker on every change (what this
  controller does) becomes wasteful: switch to **deltas**.
* A broker failure triggers hundreds of leader elections in one controller
  pass; propagation time dominates failover. Batch elections; prioritize.
* Correlated failures (a rack, a zone) are now routine events.
* The single-process controller is unacceptable: a Raft quorum is required.
* Replication: each broker follows partitions led by many others; one
  fetcher per partition (this design) means hundreds of goroutines and
  connections per broker. Multiplex: one fetcher per source broker.

## 1,000 brokers (conceptually)

* Hundreds of thousands of partitions; metadata in the hundreds of MB.
  Clients cannot download full metadata; they subscribe to the topics they
  use.
* Failure is constant: at any moment several brokers are down or
  restarting. Automation (self-healing replacement, continuous rebalancing
  with throttles) is mandatory.
* Blast radius: one cluster this large means one misconfiguration or
  controller bug affects everything. Large organizations run many clusters
  (cells) of moderate size with mirroring between them, rather than one
  giant cluster.

## What scales with what

| Dimension              | Grows with                                    | Pressure point                          |
|------------------------|-----------------------------------------------|-----------------------------------------|
| metadata size          | topics x partitions x RF                      | controller memory, propagation bandwidth |
| controller load        | partition state changes (elections, ISR)      | single-writer throughput, fsync rate    |
| failover time          | partitions led by the failed broker           | election batch + propagation            |
| replication traffic    | ingest x (RF - 1)                             | leader NIC egress                       |
| rebalance cost (consumers) | members x partitions                      | stop-the-world pause                    |
| partition movement     | bytes in moved partitions                     | network, disk on source/target          |
| storage                | ingest x RF x retention                       | disks; tiered storage                   |
| consumer group scaling | partitions                                    | members > partitions idle               |

## Partition rebalancing: adding a broker

Before:

```
Broker 1: P0 P1 P2 P3      (leaders, RF 1 for simplicity)
Broker 2: P4 P5 P6 P7
```

Add Broker 3. Nothing happens automatically: new topics will use it, old
partitions stay where they are. To spread load, some partitions must move:

```
Target:
Broker 1: P0 P1 P2
Broker 2: P4 P5 P6
Broker 3: P3 P7
```

### How a partition moves without downtime (reassignment design)

Not implemented here; this is how it would be built on the existing
mechanisms (and how production systems do it).

1. **Expand the replica set.** Controller changes P3's replicas from `[1]`
   to `[1, 3]` (target appended). Broker 3 creates an empty replica and
   starts fetching from leader 1. Nothing changes for clients.
2. **Data transfer.** Broker 3 copies P3's entire retained log. This is the
   expensive step: a 500 GB partition is 500 GB over the network and onto
   broker 3's disk, competing with live traffic on broker 1's disk and NIC.
3. **Throttle.** Replication for reassignment is rate-limited (separately
   from normal replication) so it does not starve producers and consumers.
   The cost of throttling is a longer move.
4. **Wait for ISR.** Broker 3 is added to the ISR once caught up. During the
   copy, it is *not* in the ISR, so it does not slow `acks=all` writes.
5. **Leader movement.** If the target leader differs, the controller moves
   leadership (epoch + 1), exactly like a preferred-leader election. Clients
   get `NOT_LEADER`, refresh metadata, and continue: one round trip.
6. **Shrink.** Controller sets replicas to `[3]`; broker 1 deletes its copy.

Temporary effects: replication lag on the new replica (expected, it is not
in the ISR), extra disk and network load on the source, one leadership
change, and clients refreshing metadata. Nothing is unavailable.

### Avoiding enormous data movement

* **Move as little as possible**: compute the minimal set of moves that
  balances load (like the sticky consumer assignor does for groups), not a
  fresh assignment that reshuffles everything.
* **Move leadership before data**: leader imbalance is fixed instantly by
  preferred-leader election; only replica imbalance needs copying.
* **Prefer moving small or young partitions**; move during low-traffic
  windows.
* **Throttle and parallelize carefully**: limit concurrent moves per
  broker.
* **Tiered storage** changes the economics completely: if old segments
  live in object storage, a moving replica copies only the local tail.
* **Over-partition up front** so that adding brokers moves whole partitions
  rather than requiring splits.

## Removing a broker

Reverse of the above: reassign every replica it hosts to other brokers,
wait until each new replica is in sync, move leadership off, shrink the
replica sets, then stop the broker. Killing a broker without draining it
leaves its partitions under-replicated until replaced.

## Increasing partition count

`POST /v1/topics/{t}/partitions` (`streamctl topics add-partitions`) is
implemented. Consequences:

* `hash(key) % n` changes for most keys (measured: 89.5 percent of keys
  move going 8 -> 9 partitions). A key's new events go to a new partition
  while its old events remain in the old one: a consumer of the new
  partition may process a key's new events before another consumer has
  finished its old ones.
* Safe procedure when per-key order matters: pause producers (or switch
  them to the new count at a known point), let consumers drain the old
  partitions to zero lag, then resume. Or create a new topic with more
  partitions and migrate with a replay.
* Consumer groups rebalance to include the new partitions on their next
  membership change; a production coordinator would watch metadata and
  trigger the rebalance itself.
