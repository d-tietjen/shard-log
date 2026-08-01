# ShardLog native protocol

The native protocol is the high-throughput binary boundary for ShardLog.
Loki HTTP and OTLP remain compatibility interfaces; native clients avoid JSON,
HTTP header parsing, and OTLP protobuf transcoding.

The current pre-release protocol is version 1. It supports authentication,
grouped append, indexed query, and ping over a persistent TCP connection. A connection may
carry multiple in-flight requests. Responses can complete out of order and
are correlated by the caller's 128-bit request ID.

## Frame

Every request and response starts with a fixed 32-byte little-endian header:

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 4 | Magic `SLNP` |
| 4 | 1 | Version, currently `1` |
| 5 | 1 | Opcode: append `1`, query `2`, ping `3`, authenticate `4` |
| 6 | 1 | Flags; bit 0 marks a response |
| 7 | 1 | Status: OK `0`, bad request `1`, internal `2`, unsupported `3`, unauthorized `4`, unavailable `5`, too many requests `6`, timeout `7` |
| 8 | 16 | Request ID |
| 24 | 4 | Payload length |
| 28 | 4 | First four bytes of the BLAKE3 payload digest |

Payloads are limited to 16 MiB before allocation. Unknown flags, versions,
opcodes, oversized payloads, invalid UTF-8, truncation, trailing bytes, count
mismatches, and checksum mismatches fail closed.

In production mode the first frame on every connection must be authenticate
opcode `4`, with the configured bearer token as its UTF-8 payload. The server
closes the connection after a failed authentication and rejects repeated
authentication after success. Every append and query tenant must equal the
single configured tenant. Authentication is omitted only in explicit insecure
development mode.

## Grouped append batch

An append payload starts with:

| Bytes | Field |
| ---: | --- |
| 4 | Magic `SLB1` |
| 2 | Tenant byte length |
| 2 | Stream count |
| 4 | Total record count |
| 4 | Reserved, zero |
| variable | UTF-8 tenant |

Each stream then stores:

| Bytes | Field |
| ---: | --- |
| 2 | Label count |
| 2 | Reserved, zero |
| 4 | Entry count |
| variable | Length-prefixed UTF-8 label key/value pairs |
| variable | Entries |

Labels are encoded once per stream rather than once per log line. Each entry
contains an unsigned 64-bit Unix-nanosecond timestamp, a 32-bit line length,
a 16-bit structured-metadata count, two reserved zero bytes, the UTF-8 line,
and length-prefixed UTF-8 metadata key/value pairs.

The production native service validates this representation into a bounded
borrowed view over the received frame. Message bodies and field values remain
UTF-8 slices of the frame; record descriptors contain only timestamps, stream
IDs, metadata ranges, and ordinals. Normalized label keys are allocated once
per stream and normalized metadata keys once per batch. That view implements
the structural encoder's record interface, so compression and embedded-index
construction run directly without allocating `OtlpLogEvent`, `Arc<str>`, or
per-record field vectors.

The public owned decoder remains available for clients, query results, and
compatibility sinks. A byte-for-byte equivalence test requires the borrowed
and owned paths to produce the same authoritative `SLW1` pack and transient
embedded index. Both paths use the same bounded cursor validation for UTF-8,
counts, duplicate keys, reserved bytes, framing, and trailing data. The Loki
adapter encodes accepted pushes into this same grouped batch before entering
the native preparation path.

A successful append response contains the 24-byte `SLA1` acknowledgement:
logical partition ID, first offset, and last offset. The server sends it only
after the shard-stream append is durable and the owner stripe's indexed
checkpoint covers the batch. The indexed wait is bounded by
`--indexed-ack-timeout-seconds` and waits on shard-stream's checkpoint
notification rather than polling.

Offsets are lane assigned. They must increase for one logical partition but
may contain gaps occupied by sibling partitions on the same shard-stream lane.
ShardLog preserves those offsets exactly, accepts monotonic gaps, and rejects
duplicates or regressions.

## Indexed query

A query payload starts with a fixed 32-byte `SLQ1` header containing tenant
length, label count, term count, direction, limit, inclusive start timestamp,
and exclusive end timestamp. `u64::MAX` means an absent timestamp bound. It is
followed by the tenant, exact label pairs, and exact case-insensitive message
tokens.

Labels and terms use AND semantics and execute against ShardLog's postings.
The coordinator fans out over the tenant's hidden partitions, merges by
`(timestamp, offset)`, applies oldest/newest order, and enforces the requested
limit. Results use the same grouped `SLB1` batch, allowing clients to reuse one
decoder for pushed records and query results.

The native query operation intentionally exposes ShardLog's fastest indexed
primitive. Full LogQL remains on the Loki-compatible HTTP interface.

## Server and client

`shard-log-server` listens on Loki HTTP port `3100` and native port `3101` by
default:

```text
shard-log-server \
  --auth-token-file /run/secrets/shard-log-token \
  --default-tenant production \
  --listen 0.0.0.0:3100 \
  --native-listen 0.0.0.0:3101 \
  --shards 16 \
  --tenant-partitions 256
```

The corpus loader selects the protocol without changing parsing, file spans,
core count, connections, or batch target:

```text
shard-log-loki-load /path/to/docker-json.log \
  --protocol native \
  --port 3101 \
  --workers 32 \
  --batch-bytes 1048576 \
  --pipeline-depth 1
```

The server supports up to 64 in-flight requests per connection and may return
responses out of order. `--pipeline-depth` lets the benchmark client exercise
that capability. Depth 1 is the Adam loopback default: depths 2 and 4 regressed
because additional simultaneous indexed-checkpoint waiters increased
contention. A remote client with meaningful network latency may benefit from a
larger bounded depth and must measure it independently.

`--append-linger-micros` controls shard-stream write/sync grouping. The
production default remains 250 microseconds. A 1 millisecond Adam ablation did
not improve the measured median.

## Initial Adam ablation

On the same 1 GiB prefix of the immutable ClickHouse Docker JSON corpus, both
legs used the same release binaries, CPUs `0-15`, 16 persistent connections,
1 MiB target batches, durability policy, indexed acknowledgement, and fresh
directories:

| Protocol | Source throughput | Wire bytes | Durable bytes |
| --- | ---: | ---: | ---: |
| Native | **68.45 MiB/s** | **832,404,552** | 1,665,078,800 |
| Loki JSON | 60.23 MiB/s | 953,863,512 | 1,665,132,560 |

Native was 13.6% faster and reduced client-to-server bytes by 12.7%. The nearly
identical durable size is expected because the Loki adapter converts into the
native grouped representation before append. These numbers measure the
current unreclaimed source pack plus recovery journal; they are not the final
compressed-object storage result.

The current single-copy implementation no longer writes the recovery journal
unless `--recovery-journal` is selected. On the same 1 GiB native workload, the
best fully indexed result after the optimization series is 423.37 MiB/s and
about 795 MiB of authoritative WAL, with a three-run current median of
240.78 MiB/s. See `BENCHMARKS.md`; the best result is not presented as a stable
multi-GiB/s rate.
