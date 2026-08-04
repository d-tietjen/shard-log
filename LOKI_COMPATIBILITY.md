# Loki compatibility

ShardTelemetry targets the stable Grafana Loki 3.7.2 HTTP contract. The differential
oracle is the immutable container image:

```text
grafana/loki:3.7.2
sha256:191d4fdfb7264f16989f0a57f320872620a5a7c2ceeec6229212c4190ec49b86
```

Compatibility tests retain Loki multi-tenancy headers. The production binary is
intentionally single-tenant: a global bearer token authenticates the request,
an omitted `X-Scope-OrgID` resolves to the configured tenant, and any different
tenant header is rejected. Multi-tenant isolation and HA belong to the licensed
distribution.

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

The authenticated `GET /shardtelemetry/api/v1/clickhouse/scan` Arrow source is a
ShardTelemetry extension, not a Loki route. It is absent unless explicitly enabled
with a bearer-token file. Its versioned schema and query contract are in
`CLICKHOUSE_COMPATIBILITY.md`.

The route matrix is executable in
`loki_api::tests::stable_loki_route_surface_has_no_missing_or_wrong_method_routes`.
Behavior tests cover tenant-scoped JSON push and query, labels, repeated
`match[]`, series, index statistics, structured metadata, detected fields,
native Snappy protobuf, structural pattern samples, durable logical deletion,
production authentication/draining, POST form bodies, parser pipelines, metric
queries, and durable cold-tier restart recovery.

Push responses are not acknowledged until shard-stream durability and the
stripe-owned indexed checkpoint have both advanced. A tenant is transparently
striped over bounded internal partitions; hidden partitions are merged by
timestamp before Loki response formatting. Live tail subscribers register
before their initial lookback to avoid an ingest race and receive bounded lag
notifications.

Accepted Loki pushes are encoded into ShardTelemetry's stream-grouped native batch
before entering shard-stream. The sink therefore avoids an OTLP protobuf
transcode while preserving the Loki wire contract. Native clients can bypass
HTTP and JSON entirely on TCP port `3101`; that protocol is documented in
`NATIVE_PROTOCOL.md`.

## LogQL evaluator

The single-tenant evaluator implements selectors; exact, negative, and regular
expression line filters; JSON, logfmt, regexp, pattern, and unpack parsers;
typed label filters; `line_format`, `label_format`, `drop`, `keep`, and
`decolorize`; and parser-error labels. Historical queries and live tail use the
same transformation path.

Metric queries implement `count_over_time`, `rate`, `bytes_over_time`,
`bytes_rate`, `absent_over_time`, unwrapped sum/average/minimum/maximum,
standard deviation, variance, quantile, first, last, and counter-rate ranges.
Vector sum/average/minimum/maximum/count, standard deviation, variance,
top/bottom-k, sort, `by`/`without`, arithmetic/comparison/set operators,
`bool`, `on`/`ignoring`, and `group_left`/`group_right` are supported. Range
queries return Loki matrices and instant queries return vectors or scalars.

GET URL parameters and POST `application/x-www-form-urlencoded` bodies run
through one precedence and validation path. Unsupported expressions fail with
HTTP 400 instead of silently producing approximate results.

## Known differences

- The engine is intentionally single-tenant. Loki's ruler, compactor ring,
  query-frontend scheduler, and multi-tenant administrative APIs are not part
  of this storage process.
- Pattern detection reuses the structural compressor's dynamic-value
  classifier and emits Loki's pattern/sample envelope, but still needs
  differential clustering tests against Loki's probabilistic pattern ingester.
- Delete requests are checksummed, atomically persisted, and immediately filter
  Loki, native, tail, and Arrow reads. General selector deletion is logical;
  physical compaction currently occurs only for whole append batches expired by
  the global retention window.
- Query statistics report measured lines, bytes, return count, and elapsed
  throughput, but not every Loki 3.7 storage-stage counter.
- Compressed frames and independent query indexes are durably published and
  served cold from bounded object ranges. Until covered shard-stream source
  packs are reclaimed, storage accounting must continue to report those raw
  authority bytes separately from ShardTelemetry's compressed catalog.

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
| ShardTelemetry durable API | 43.16 | 949,018 | 241,490,286 |
| Loki 3.7.2 | 57.63 | 949,018 | 120,076,424 |

The result is a historical ablation, not the current storage path. It exposed
source packs plus a second raw sink journal while compressed sealed blocks were
memory-owned. The worker now publishes compressed payload and query-index
groups and releases resident copies; a replacement full-corpus run must still
account separately for covered raw source packs until reclamation is enabled.
