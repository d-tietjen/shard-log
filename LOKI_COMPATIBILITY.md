# Loki compatibility

ShardLog targets the stable Grafana Loki 3.7.2 HTTP contract. The differential
oracle is the immutable container image:

```text
grafana/loki:3.7.2
sha256:191d4fdfb7264f16989f0a57f320872620a5a7c2ceeec6229212c4190ec49b86
```

Compatibility is tested with Loki multi-tenancy headers enabled at the protocol
boundary. `X-Scope-OrgID` scopes ingestion, lookup, metadata, live tailing, and
deletion requests.

## Implemented wire surface

- `POST /loki/api/v1/push`
  - JSON streams, optional structured metadata, and mandatory string timestamps.
  - Native raw-Snappy Loki `PushRequest` protobuf.
- `POST /otlp/v1/logs`
  - Binary OTLP `ExportLogsServiceRequest`.
- `GET|POST /loki/api/v1/query`
- `GET|POST /loki/api/v1/query_range`
- `GET|POST /loki/api/v1/labels`
- `GET|POST /loki/api/v1/label/{name}/values`
- `GET|POST /loki/api/v1/series`
- `GET|POST /loki/api/v1/index/stats`
- `GET|POST /loki/api/v1/index/volume`
- `GET|POST /loki/api/v1/index/volume_range`
- `GET|POST /loki/api/v1/patterns`
- `GET|POST /loki/api/v1/detected_fields`
- `GET|POST /loki/api/v1/detected_field/{name}/values`
- `GET /loki/api/v1/tail`
- `GET|POST|PUT|DELETE /loki/api/v1/delete`
- `GET|POST /loki/api/v1/format_query`
- `/ready`, `/metrics`, `/config`, `/services`, `/log_level`, `/flush`,
  `/ingester/prepare_shutdown`, `/ingester/shutdown`, and build information.
- Deprecated `/api/prom` push, query, label, series, and tail aliases.

The authenticated `GET /shardlog/api/v1/clickhouse/scan` Arrow source is a
ShardLog extension, not a Loki route. It is absent unless explicitly enabled
with a bearer-token file. Its versioned schema and query contract are in
`CLICKHOUSE_COMPATIBILITY.md`.

The route matrix is executable in
`loki_api::tests::stable_loki_route_surface_has_no_missing_or_wrong_method_routes`.
Behavior tests cover tenant-scoped JSON push and query, labels, repeated
`match[]`, series, index statistics, structured metadata, detected fields,
native Snappy protobuf, and durable restart recovery.

Push responses are not acknowledged until shard-stream durability and the
stripe-owned indexed checkpoint have both advanced. A tenant is transparently
striped over bounded internal partitions; hidden partitions are merged by
timestamp before Loki response formatting. Live tail subscribers register
before their initial lookback to avoid an ingest race and receive bounded lag
notifications.

Accepted Loki pushes are encoded into ShardLog's stream-grouped native batch
before entering shard-stream. The sink therefore avoids an OTLP protobuf
transcode while preserving the Loki wire contract. Native clients can bypass
HTTP and JSON entirely on TCP port `3101`; that protocol is documented in
`NATIVE_PROTOCOL.md`.

## Compatibility gates still open

This pre-release implementation must not yet be described as a complete Loki
replacement:

- The evaluator currently supports selectors and exact/negative/regular
  expression line filters. The remaining stable LogQL parser stages, label
  filters and formatting stages, unwrap/range functions, vector aggregation,
  binary operators, grouping, and vector matching remain release blockers.
- POST form-body parameters need differential coverage in addition to URL
  parameters.
- Pattern detection is currently an empty successful result.
- Delete requests have Loki-compatible lifecycle endpoints but do not yet
  filter or physically compact matching records.
- Query statistics expose the stable envelope but not all Loki 3.7 counters.
- Durable compressed-block publication, sealed-block cold reads, and source-WAL
  reclamation are not complete. Until they are, total disk usage includes the
  authoritative shard-stream pack and sink recovery journal.

No full-corpus result is accepted while any of these gates is open.

## Full Loki oracle measurement

The complete 80 GiB corpus has now been ingested through Loki's HTTP API on
Adam. After `/flush` and WAL checkpoint reclamation, Loki occupied
3,782,890,631 bytes (22.71x) and the client sustained 83.87 MiB/s over 976.70
seconds. Peak WAL-inclusive disk was roughly 30–40 GiB. Full provenance and the
provisional same-corpus comparison are in `BENCHMARKS.md`.

## First identical-wire ablation

Adam, CPUs `0-15`, 16 persistent HTTP connections, 1 MiB JSON pushes, the same
128 MiB prefix of the immutable 80 GiB ClickHouse Docker JSON corpus:

| Engine | Source MiB/s | Records | Total disk bytes |
| --- | ---: | ---: | ---: |
| ShardLog durable API | 43.16 | 949,018 | 241,490,286 |
| Loki 3.7.2 | 57.63 | 949,018 | 120,076,424 |

The result is an ablation, not an accepted benchmark. It exposed two ShardLog
costs that the earlier direct compressor did not include: source packs plus a
second raw sink journal, while compressed sealed blocks were memory-owned.
The production storage path must publish compressed blocks and reclaim covered
raw data before the 80 GiB three-engine campaign is valid.
