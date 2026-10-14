# Network Partitions, Availability and Consistency

CAP is often summarized as "pick two of three". That framing is misleading.
Partitions are not optional: networks drop packets, switches fail, GC
pauses and frozen VMs look exactly like partitions to the rest of the
cluster. The real statement is narrower and more useful:

> **When a network partition happens**, a replicated system must choose,
> for each operation, between answering (availability) and answering only
> with data that reflects every acknowledged write (consistency).
> **When there is no partition**, it can have both, and the interesting
> tradeoff is latency vs consistency (PACELC).

So the questions are concrete: *during which partitions, for which
operations, does this system refuse to answer, and what does it risk when
it does answer?*

## The actors

```
       clients                 clients
          |                       |
      +---+---+               +---+---+               +-------+
      |  B1   | <-- repl. --> |  B2   | <-- repl. --> |  B3   |
      +---+---+               +---+---+               +---+---+
          \                       |                       /
           \______ heartbeat, metadata, ISR changes ______/
                                  |
                           +------+------+
                           | controller  |   single process, file-backed
                           +-------------+
```

Partition 0: replicas `[B1, B2, B3]`, leader B1, ISR `[B1, B2, B3]`,
`min_insync_replicas=2`.

## Scenario 1: the leader is cut off from everything (clients can reach it)

B1 cannot reach the controller, B2 or B3. Clients can still reach B1.

| Question                                   | Answer in this system                                                  |
|--------------------------------------------|------------------------------------------------------------------------|
| Who believes they are leader?              | B1, until its lease expires (at most `lease` = 6s after its last successful heartbeat). After ~10s the controller makes B2 leader in epoch E+1. B1 never believes it is leader of E+1. |
| Can both accept writes?                    | Only in the first window, and only weakly: B1 accepts `acks=leader` writes until its lease expires; B2 accepts writes only after the controller elects it, which happens after B1's lease has expired. `acks=all` writes to B1 cannot succeed at any time (no follower can fetch them). |
| How is split brain prevented?              | Lease shorter than session timeout (B1 stops before B2 starts), plus epoch fencing (B2/B3 and the controller reject anything from epoch E once they know E+1). |
| Clients connected to B1?                   | Writes: success with `acks=leader` (at risk) until lease expiry, then `LEASE_EXPIRED`; `acks=all` times out. Reads: B1 keeps serving its committed prefix (stale but never wrong). Clients that refresh metadata are redirected to B2. |
| When can B1 rejoin?                        | As soon as the network heals: it learns epoch E+1 from metadata, becomes a follower of B2. |
| How is stale state discarded?              | B1 fetches from B2 with `last_epoch=E`; B2 answers with where epoch E ended in its log; B1 truncates everything after that (the `acks=leader` writes from the window) and catches up. |

`TestNetworkPartitionFencesStaleLeader` runs exactly this scenario with
fault injection (`isolate_from_controller`, `isolate_from_brokers`) and
checks every row of the table.

**Choice made:** consistency for `acks=all`, availability for `acks=leader`
during at most one lease period, then consistency.

## Scenario 2: a follower is cut off

B3 cannot reach B1. Writes continue; B3 stops being caught up. After
`replica_lag_max_ms` B1 proposes ISR `[B1, B2]`, the controller accepts,
and the HWM advances without B3. While the shrink is pending, `acks=all`
writes wait (the HWM waits for B3). That stall, bounded by
`replica_lag_max_ms` plus one controller round trip, is the price of never
shrinking the ISR without the controller's agreement.

**Choice made:** brief write unavailability (latency) to keep the guarantee
that every ISR member, as recorded by the controller, has every committed
record.

## Scenario 3: the leader and one follower are separated from the other follower and the controller

B1 and B2 can talk to each other but not to B3 or the controller. B1's lease
expires (no heartbeats reach the controller), so B1 stops accepting writes
even though it still has a follower in sync. The controller sees B1 and B2
as dead after the session timeout; only B3 is alive. B3 was in the ISR, so
it is elected (if it was not, the partition goes offline).

Is that safe, given that B1 and B2 kept replicating to each other? Yes: B1
could not get the controller to remove B3 from the ISR, and the HWM is
computed over the controller-approved ISR, so the HWM never advanced past
what B3 had. Every committed record is on B3. Writes B1 accepted after the
partition began were acknowledged only with `acks=leader` (or not at all),
and are truncated when B1 and B2 rejoin. This is the payoff of applying ISR
shrinks only after the controller accepts them.

**Choice made:** the side with the controller wins, even if it is the
minority of brokers. In a consensus-backed design the side with the
*controller quorum* wins, which is the same rule with a fault-tolerant
controller.

## Scenario 4: the controller is down (or partitioned from all brokers)

No heartbeats are accepted, so every broker's lease expires after `lease`
seconds and every partition stops accepting writes. Reads continue.
No failover, topic change, or consumer-group operation can happen.
`TestControllerOutageStopsWritesAfterLease` demonstrates it.

This is a strong choice for consistency. The alternative (keep serving
writes without a lease) is exactly what makes split brain possible:
without a coordinator, a broker cannot distinguish "the controller is down"
from "I have been partitioned away and someone else is now leader".

The cure is not to weaken the lease but to make the controller fault
tolerant: three or five controller nodes running a consensus protocol, so
that "the controller is down" requires losing a majority of them.

## Scenario 5: clock problems

* **Clock skew** (wall clocks disagree): does not affect correctness.
  Leases are measured with each process's monotonic clock as durations,
  never by comparing timestamps between machines. Record timestamps are
  broker-local and only clamped to be non-decreasing per partition; skew
  between brokers shows up as timestamp jumps after a failover, which can
  make timestamp-based replay start slightly early or late.
* **Clock rate drift / pauses**: the lease safety argument assumes the
  broker's clock does not run more than ~40% slower than the controller's
  (6s lease vs 10s session). A process frozen for longer than the lease
  (SIGSTOP, a very long GC pause, a VM migration) can resume believing its
  lease is valid only if it checks before reading the clock again. The
  write path checks the lease right before appending, but a thread paused
  *between* the check and the append could still append once. Epoch fencing
  prevents that record from being committed or replicated; it will be
  truncated. This is the classic reason production systems use fencing
  tokens and not leases alone.

## Quorum, leader election and consensus

* **Quorum** in this system appears in two places: `acks=quorum`
  (majority of replicas acknowledged), and conceptually in what a
  consensus-backed controller would use. Partition-level commitment uses
  the **ISR** rule (all in-sync replicas), not a majority rule. ISR
  replication tolerates `f` failures with `f+1` replicas (vs `2f+1` for
  majority quorums) but needs an external, consistent authority to decide
  who is in the ISR. That authority is the controller, which is exactly why
  the controller is the component that must be strongly consistent.
* **Leader election is not consensus.** Picking a leader is easy; what is
  hard is making every participant agree on *which* leader for *which*
  epoch, even when messages are lost and nodes pause. Here, agreement comes
  for free because there is only one controller deciding; it would be lost
  the moment two controllers existed. Consensus (Raft, Paxos, Zab) is what
  lets several controller nodes act as one without a single point of failure.
* **Guarantees that are impossible without consensus** (or an equivalent
  single authority): unique leadership per epoch across controller failures;
  fault-tolerant ISR membership; atomic topic creation across controller
  nodes; failover that survives the loss of the node making failover
  decisions.

## Stale reads and write availability, summarized

| Situation                          | Writes (`acks=all`)      | Writes (`acks=leader`)          | Reads                                |
|------------------------------------|--------------------------|---------------------------------|--------------------------------------|
| healthy                            | available                | available                       | consistent                           |
| leader isolated, within lease      | unavailable (timeout)    | available, at risk              | stale on isolated leader             |
| leader isolated, after lease       | unavailable              | unavailable (`LEASE_EXPIRED`)   | stale on isolated leader             |
| after election                     | available on new leader  | available on new leader         | consistent on new leader             |
| follower lagging (pre-shrink)      | delayed                  | available                       | consistent                           |
| ISR < min_insync_replicas          | rejected                 | available                       | consistent                           |
| all ISR members down               | unavailable (offline)    | unavailable                     | unavailable                          |
| controller down                    | unavailable after lease  | unavailable after lease         | available (from current leaders)     |

## What a production version would change

1. Replace the controller with a Raft-replicated metadata log (3 or 5
   nodes). Elections, ISR changes and topic changes become log entries.
2. Make brokers fence themselves based on the metadata log's epoch, and
   keep serving reads/writes while the controller quorum is merely slow,
   using the quorum's lease rather than a single process's.
3. Optionally lease-gate reads on the leader (or read from the quorum) to
   remove stale reads from a deposed leader.

See [multi-region.md](multi-region.md) for the same questions across
regions, where the partition is not hypothetical but a property of the
speed of light.
