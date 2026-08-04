# ShardTelemetry native protocol

The native protocol is ShardTelemetry's high-throughput binary interface on TCP port `3101`. This pre-release repository ships exactly one protocol and one durable format: native protocol v2 carrying checksummed `STEL` envelopes. There is no legacy append decoder or dual-format storage path.

## Frame

Every request and response begins with a fixed 32-byte little-endian header:

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 4 | Magic `STNP` |
| 4 | 1 | Version `2` |
| 5 | 1 | Opcode: append `1`, query `2`, ping `3`, authenticate `4` |
| 6 | 1 | Flags; bit 0 marks a response |
| 7 | 1 | Status |
| 8 | 16 | Caller-selected request ID |
| 24 | 4 | Payload length |
| 28 | 4 | First four bytes of the BLAKE3 payload digest |

Frames are limited to 64 MiB. Unknown flags, versions, opcodes, oversized payloads, malformed UTF-8, truncation, trailing bytes, count mismatches, and checksum mismatches fail closed. Multiple requests may be in flight on one connection and responses may complete out of order.

In production mode the first frame must authenticate with the configured bearer token. Append and query tenants must match the authenticated single tenant.

## Signal-aware append

An append payload is one `STB2` batch containing 1–256 routed partitions. Its header contains the partition count and reserved zero bytes. Each partition stores:

| Bytes | Field |
| ---: | --- |
| 16 | Signal-specific shard-stream topic ID |
| 4 | Logical partition ID |
| 4 | Encoded envelope length |
| variable | One checksummed `STEL` envelope |

The topic must match the envelope signal. Duplicate topic/partition pairs are rejected. The server validates the complete request before appending partitions in parallel under bounded backpressure.

A successful append returns one `STM2` acknowledgement entry per input partition, in request order. Each entry contains topic ID, partition ID, first durable offset, and last durable offset. Every offset represents exactly one log record, span, or metric point.

## Indexed log query

The current native query opcode exposes the fastest exact log lookup primitive. Its `STQ2` request supports tenant, exact labels, exact case-insensitive message terms, an optional time range, result limit, and sort direction.

Log query responses use the response-only `STR2` format. Labels are grouped once per returned stream, but `STR2` is never accepted by append or storage code. Trace and metric native query messages will use their signal-native result schemas rather than reusing the log response.

Full LogQL, PromQL, and TraceQL remain on the compatible HTTP APIs.

## Ports and durability

`shard-telemetry-server` listens on Loki/Prometheus HTTP `3100`, native TCP `3101`, OTLP/gRPC `4317`, and OTLP/HTTP `4318` by default. Native append acknowledgement means the authoritative shard-stream append is durable; configurations may additionally wait for owner-stripe query visibility.

The corpus loader's `--protocol native` mode constructs `STEL` log envelopes and `STB2` partition batches. Historical pre-v2 native payloads are intentionally unsupported because ShardTelemetry has not released a stable storage or protocol format.
