# ClickHouse query compatibility

ShardLog delegates analytical SQL semantics to a pinned ClickHouse query node
and remains responsible for log ingestion, indexing, compression, and tiered
storage. The query evaluator remains unmodified. Production automatic pushdown
uses the narrow in-tree `StorageShardLog` adapter described below; the generic
URL path remains available for an entirely stock ClickHouse binary. The pinned
compatibility target is `ClickHouse 26.3.17.56 LTS`. The Adam acceptance image is pinned as
`clickhouse@sha256:badd3bb0d34055bfa521b7b71bbee92aa7ec0025a90f1a1a5ec49c5b8ee0ba90`.

This boundary makes ClickHouse, rather than a second SQL implementation, the
semantic authority for expressions, types, aggregate functions, joins,
subqueries, common table expressions, window functions, JSON functions,
materialized views, output formats, and query errors. Clients that need this
surface connect to ClickHouse. Loki, OTLP, and ShardLog-native clients continue
to connect directly to ShardLog.

## Versioned columnar source

The first executable adapter is a versioned Arrow IPC stream:

```text
GET /shardlog/api/v1/clickhouse/scan
```

The route is absent by default. It is registered only when
`shard-log-server` receives `--clickhouse-token-file`. Every request must send
the exact token as `Authorization: Bearer ...`. The file must contain a
non-empty token. Treat this as an administrative credential: the holder may
select a tenant with `X-Scope-OrgID`.

Run the endpoint on loopback or behind an authenticated TLS/mTLS proxy. Do not
send the bearer token over an untrusted plaintext network.

The Arrow schema is version 1:

| Column | Arrow type | ClickHouse type | Meaning |
| --- | --- | --- | --- |
| `tenant` | `Utf8` | `String` | Loki tenant |
| `timestamp` | `Timestamp(Nanosecond, UTC)` | `DateTime64(9, 'UTC')` | Event time |
| `partition` | `UInt32` | `UInt32` | Logical partition |
| `offset` | `UInt64` | `UInt64` | Durable offset |
| `message` | `Utf8` | `String` | Original log line |
| `labels` | `Map<Utf8, Utf8>` | `Map(String, String)` | Loki stream labels |
| `metadata` | `Map<Utf8, Utf8>` | `Map(String, String)` | Structured metadata |

The response content type is `application/vnd.apache.arrow.stream` and carries
`X-ShardLog-Schema-Version: 1` plus the pinned ClickHouse target.

The scan is streamed in bounded 8,192-row batches. It never materializes the
complete tenant in the HTTP layer. The durable store pages each logical
partition by offset and queries owner stripes in parallel.

## Storage pushdown contract

The URL query accepts these fail-closed parameters:

| Parameter | Behavior |
| --- | --- |
| `start_ns` | Inclusive unsigned Unix-nanosecond timestamp |
| `end_ns` | Exclusive unsigned Unix-nanosecond timestamp |
| `term` | Repeatable case-insensitive indexed message token; AND semantics |
| `label.NAME` | Repeatable exact stream-label equality |
| `metadata.NAME` | Repeatable exact structured-metadata equality |
| `columns` | Comma-separated projection in requested output order |
| `limit` | Optional global row limit |

Unknown parameters, columns, empty projections, duplicate columns, and invalid
ranges are rejected. Tenant, time, term, label, and metadata constraints are
translated to `LogQuery` before records are reconstructed. Projection controls
which Arrow arrays are allocated and transmitted.

The generic ClickHouse `URL` engine does not infer these parameters from a SQL
`WHERE` clause. It therefore supports explicit pushdown in the source URL.

The pinned `StorageShardLog` adapter in `clickhouse/adapter` subclasses
ClickHouse's `StorageURL` and overrides only its URI-parameter hook. It obtains
the physical projection and analyzed filter DAG from `SelectQueryInfo` and
automatically translates safe timestamp and exact map equalities into the same
scan contract. The original filter remains in ClickHouse as a residual, so an
unsupported expression loses performance rather than correctness. See
`clickhouse/adapter/README.md` for installation, DDL, and the exact pushdown
rules.

## ClickHouse source

With ShardLog listening locally and the token supplied by a protected secret
source, ClickHouse can query the stream directly:

```sql
SELECT
    labels['service_name'] AS service,
    count() AS records,
    quantileTDigest(0.99)(lengthUTF8(message)) AS p99_message_bytes
FROM url(
    'http://127.0.0.1:3100/shardlog/api/v1/clickhouse/scan',
    'ArrowStream',
    'tenant String, timestamp DateTime64(9, \'UTC\'), partition UInt32, offset UInt64, message String, labels Map(String, String), metadata Map(String, String)',
    headers(
        'Authorization' = 'Bearer REPLACE_FROM_SECRET_STORE',
        'X-Scope-OrgID' = 'fake'
    )
)
GROUP BY service
ORDER BY records DESC;
```

`clickhouse/shardlog-url.sql` contains the generic URL-engine template and
`clickhouse/shardlog-engine.sql` contains the automatic-pushdown template.
ClickHouse stores engine headers in table metadata, so production
deployments should inject a short-lived credential or use a trusted local
proxy rather than committing a token to SQL.

## Differential gate

`scripts/run-clickhouse-compatibility.sh` evaluates the same deterministic
query matrix against:

1. the live ShardLog Arrow source; and
2. an equivalent ClickHouse `Memory` table populated from that source.

It compares exact serialized results for filters, native-map grouping,
conditional and exact aggregates, arrays, windows, CTEs, joins, timestamp/map
predicates, mixed residual predicates, disjunctions, missing-map default-value
semantics, aliases, subqueries, and aggregate combinators. The harness refuses
a ClickHouse version other than `26.3.17.56` unless
`STRICT_CLICKHOUSE_VERSION=0` is supplied for developer smoke testing.

Set `SHARDLOG_ADAPTER_MODE=1`, or run
`scripts/run-clickhouse-adapter-compatibility.sh`, to create a
`StorageShardLog` source table and exercise automatic pushdown. Adapter mode
requires a ClickHouse binary or image built with the pinned adapter.

This proves the adapter and evaluator path; it does not replace the larger
compatibility corpus. The release gate is the applicable ClickHouse SQL test
suite plus generated differential combinations of nullable values, nested
types, aliases, lambdas, aggregate combinators, joins, windows, and errors.

## Compatibility status

| Area | Status |
| --- | --- |
| ClickHouse `SELECT` evaluator semantics | Supplied by pinned ClickHouse |
| Bounded typed ShardLog scan | Implemented |
| Authentication and tenant selection | Implemented; route disabled by default |
| Explicit timestamp/term/label/metadata pushdown | Implemented |
| Explicit column projection | Implemented |
| Automatic plan-to-scan pushdown | Implemented for projection, timestamp bounds, exact label/metadata equality, and safe trivial limits; custom-binary acceptance pending |
| ClickHouse native/HTTP client surface | Supplied by ClickHouse query node |
| Full ClickHouse SQL regression corpus | Pending import and classification |
| 80 GiB cold/warm analytical benchmark | Pending adapter acceptance run |

## Initial acceptance evidence

On 2026-07-31, the differential smoke ran on Adam against the exact official
ClickHouse `26.3.17.56` image above. The final native-map Arrow stream had
SHA-256 `be3c7f12f4ecbcee5132c1474521e49008cfd6ea0fee5c96647b1f2b8883c01d`.
Three synthetic records covered two streams, labels, metadata, multiple
timestamps, and case-varying error terms. The initial six gates and the
expanded predicate/semantic gates all produced byte-identical serialized
results:

```text
PASS row-count
PASS group-map
PASS aggregates
PASS window
PASS cte-array
PASS self-join
PASS timestamp-map-filter
PASS mixed-residual
PASS disjunction
PASS missing-map-key
PASS missing-map-equality
PASS alias-subquery
PASS aggregate-combinators
ClickHouse compatibility smoke passed with 26.3.17.56
```

The exact 26.3 analyzer was also inspected on Adam. It rewrites constant map
lookups to dynamic inputs such as `labels.key_app` and `metadata.key_code` and
constant-folds time bounds to `DateTime64(9, 'UTC')` values. The adapter handles
those canonical forms using ClickHouse's own String text deserializer and
retains every original filter as a residual.

The installer applied cleanly to a sparse checkout of the exact tag, and the
Rust API, formatting, and strict Clippy gates pass. A custom ClickHouse binary
has not yet been built: Adam currently has 56 GiB free at 94% utilization and
has no retained ClickHouse source/build cache. The adapter performance gate
must run on a build worker with enough scratch capacity, then transfer only the
pinned image to Adam.

The isolated 581 MiB source/build/data directory from the initial run was
removed because Adam was at 93% disk utilization. The pinned stock ClickHouse
image remains installed for the full corpus campaign.

ShardLog must not claim standalone ClickHouse compatibility while the pending
gates remain open. The current claim is narrower and precise: the pinned
ClickHouse evaluator can execute its complete analytical SQL surface over the
versioned ShardLog log source.
