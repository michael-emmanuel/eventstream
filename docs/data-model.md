# Data Model and Record Format

## Conceptual event

```
Event {
    Topic           implied by directory (not stored per record)
    Partition       implied by directory (not stored per record)
    Offset          partition-local position, assigned by the leader
    Timestamp       broker log-append time (ms), monotonic per partition
    Key             optional bytes; drives partitioning and compaction
    Value           bytes, or null (tombstone)
    Headers         ordered (key, bytes) pairs: event_type, schema_version, traceparent, ...
    ProducerID      0 if not idempotent
    SequenceNumber  per (producer, partition), -1 if not idempotent
    LeaderEpoch     epoch of the leader that first appended it
    Checksum        CRC32C over the encoded body
}
```

Topic and partition are not repeated in every record: the directory a
segment lives in already says which partition it belongs to. On a 200-byte
event, storing `"orders"` and a partition number in each record would add
several percent for no information.

`LeaderEpoch` is not in the original brief but is essential: it is what
lets a follower find exactly where its log diverged from a new leader's
(see [consistency.md](consistency.md#failover)). Each record carries it, so
the leader-epoch history is rebuilt from the log itself at startup.

## Binary frame (format version 1)

All integers are big-endian. Source of truth: `internal/record/record.go`.

```
offset  size  field
     0     4  length        bytes after this field (crc + body)
     4     4  crc32c        Castagnoli CRC of the body
     8     1  version       1
     9     1  attributes    bit 0: tombstone (value is null)
    10     4  leaderEpoch   int32
    14     8  offset        int64
    22     8  timestamp     int64, unix ms
    30     8  producerID    int64
    38     8  sequence      int64
    46     4  keyLen        int32, -1 = null key
    50     k  key
           4  valueLen      int32, -1 = tombstone
           v  value
           2  headerCount   uint16 (max 64)
              repeated: keyLen uint16, key, valueLen int32, value
```

Fixed overhead per record: **56 bytes** plus 6 bytes per header plus header
key/value bytes. For a 1 KiB event with three short headers that is roughly
8 percent; for a 100-byte event it is over 50 percent. This is one reason
batching formats in production systems hoist shared fields (base offset,
base timestamp, producer ID) into a batch header and delta-encode per-record
fields. That optimization is deliberately not done here; it is listed in
[tradeoffs.md](tradeoffs.md#25-batch-size).

## Why binary storage and JSON on the wire

The storage format is independent of the API on purpose:

* **Followers replicate raw frames.** A follower appends bytes exactly as
  the leader stored them; no decode/re-encode, and the CRC computed once by
  the leader protects the data all the way to every replica's disk and back
  out to consumers.
* **The API can change without migrating data.** The JSON API could be
  replaced by a binary protocol, or gain new fields, without touching a
  single segment.
* **Binary-safe storage, text-only API.** Stored values are arbitrary bytes.
  The JSON API represents values as UTF-8 strings, so binary payloads are
  not supported through it (invalid UTF-8 is replaced by U+FFFD by Go's JSON
  encoder). A binary client protocol would remove that limitation.

## Versioning and backward compatibility

* Every frame starts its body with a **format version byte**. Readers reject
  unknown versions with `ErrUnsupportedVersion` rather than misinterpreting
  bytes.
* Introducing format v2 safely requires the classic two-step rollout:
  1. Upgrade every broker to a build that can **read** v1 and v2 but still
     **writes** v1.
  2. Flip the writer to v2.
  Because followers copy frames byte-for-byte, a follower running old code
  would reject v2 frames from an upgraded leader; the rollout order (and an
  inter-broker protocol version gate) prevents that. Old segments are never
  rewritten; they age out through retention or are rewritten by compaction.
* The JSON API is versioned by URL prefix (`/v1/...`). Adding optional
  response fields is backward compatible; removing or retyping fields is not.

## Application schema evolution

The broker never parses payloads. Schema is a contract between producers
and consumers, expressed with two headers:

```
event_type:      order.created
schema_version:  2
```

`pkg/schema` provides a `Registry` interface (an in-memory implementation
is included; a remote registry service would implement the same interface),
compatibility checking, validation, and an `Upgrade` helper:

| Mode     | Guarantees                                      | Allowed changes                             | Safe deploy order        |
|----------|-------------------------------------------------|---------------------------------------------|--------------------------|
| backward | new readers can read old data                   | add field with default; remove field        | consumers first          |
| forward  | old readers can read new data                   | add field; remove optional field            | producers first          |
| full     | both                                            | add/remove optional fields with defaults    | any order                |

Type changes are always incompatible: emit a new `event_type` instead.

Why this matters more for a log than for a queue: a log keeps old events.
A consumer replaying seven days of history will see every schema version
written in that window. Consumers must therefore handle **older** events
(backward compatibility, `schema.Upgrade` fills defaults) and, when
producers deploy first, **newer** events (forward compatibility,
`schema.Resolve` falls back to the latest known version and ignores
unknown fields).

## Checksums

* **Algorithm:** CRC32C (Castagnoli) over the body. Go uses hardware CRC
  instructions on amd64 and arm64, so the cost is small relative to the
  syscall and copy costs (see `BenchmarkEncode`/`BenchmarkDecode`).
* **Coverage:** the body. The length prefix is not covered by the CRC; it
  is validated by bounds (minimum body size, `MaxRecordSize`) and indirectly
  by the CRC of the bytes it delimits. A corrupted length either fails the
  bounds check or makes the CRC check fail on the wrong bytes.
* **Where checked:**
  * recovery: every frame of the active segment, and the tail of sealed
    segments after their last index entry (all frames with
    `-verify-on-recovery`);
  * replication: every frame a follower receives, before it is appended;
  * consumption: every frame decoded for a consumer;
  * offline: `streamctl verify DIR`.
* **What it detects:** torn writes (truncated frames), bit rot, misdirected
  writes, malformed frames.
* **What it does not do:** a CRC is not authentication. Anyone who can
  modify the file can recompute the CRC. Protecting against malicious
  modification requires a keyed MAC or signature (and key management),
  plus filesystem access control. Checksums protect against accidents,
  not adversaries.

## Limits

| Limit                         | Default        | Where                                    | Why                                     |
|-------------------------------|----------------|------------------------------------------|-----------------------------------------|
| `max_event_bytes`             | 1 MiB          | topic config                             | bounds memory per record; large blobs belong in object storage with a pointer in the event |
| frame size on read            | max_event_bytes + 1 KiB | reader                          | a corrupt length field cannot trigger a huge allocation |
| request body                  | 8 MiB          | broker `-max-request-bytes`              | bounds memory per request               |
| in-flight produce bytes       | 64 MiB         | broker `-max-inflight-produce-bytes`     | backpressure                            |
| fetch `max_bytes`             | 4 MiB cap      | broker                                   | bounds memory per fetch                 |
| headers per record            | 64             | record format                            | bounds decode work                      |
| header key length             | 65535          | record format (uint16)                   |                                         |
| segment size                  | < 2 GiB        | `segment_bytes`                          | index positions are uint32              |
| partitions per topic          | 10,000         | controller                               | metadata size                           |
