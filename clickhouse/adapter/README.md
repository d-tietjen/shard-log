# StorageShardLog adapter

`StorageShardLog` is a narrow, pinned ClickHouse storage adapter. It subclasses
ClickHouse's `StorageURL`, so the existing ClickHouse HTTP transport, Arrow
reader, residual filters, and query evaluator remain authoritative. The
adapter only adds scan parameters that are logically implied by the analyzed
query.

The integration target is exactly `v26.3.17.56-lts`. ClickHouse does not expose
a stable dynamically loadable storage-engine ABI, so this adapter is compiled
into the pinned query-node binary. It is intentionally kept to two source files
and one registration patch.

## Apply to ClickHouse

```bash
git clone --branch v26.3.17.56-lts --recurse-submodules \
  https://github.com/ClickHouse/ClickHouse.git clickhouse-shardlog

CLICKHOUSE_SOURCE=$PWD/clickhouse-shardlog \
  ./scripts/apply-clickhouse-adapter.sh
```

The installer refuses a checkout that is not at the exact tag and refuses to
overwrite different adapter files. Build the resulting source with the normal
ClickHouse release build process. The adapter source is included by
ClickHouse's existing `Storages` source glob; no CMake patch is required.

## Table definition

```sql
CREATE TABLE logs
(
    tenant String,
    timestamp DateTime64(9, 'UTC'),
    partition UInt32,
    offset UInt64,
    message String,
    labels Map(String, String),
    metadata Map(String, String)
)
ENGINE = ShardLog(
    'http://127.0.0.1:3100/shardlog/api/v1/clickhouse/scan',
    'ArrowStream',
    headers(
        'Authorization' = 'Bearer REPLACE_FROM_SECRET_STORE',
        'X-Scope-OrgID' = 'fake'
    )
);
```

Use loopback or mTLS and inject a short-lived token. ClickHouse stores engine
arguments in table metadata, so a long-lived bearer credential in DDL is not a
suitable production secret boundary.

## Pushdown rules

The adapter automatically pushes:

- requested physical columns;
- `timestamp` `<`, `<=`, `=`, `>=`, and `>` bounds, normalized to the
  endpoint's inclusive-start/exclusive-end nanosecond range;
- exact `labels['key'] = 'value'` constraints;
- exact `metadata['key'] = 'value'` constraints; and
- a ClickHouse-classified trivial `LIMIT` only when an analyzed filter DAG is
  present and the complete filter is represented by the rules above.

ClickHouse may rewrite constant String map lookup into dynamic inputs such as
`labels.key_app`. The adapter parses that key with ClickHouse's own String text
serialization and projects the parent `labels` or `metadata` map, so this
optimization remains exact for escaped keys as well as simple identifiers.

Only top-level conjunctions are decomposed. Disjunctions, negation, message
functions, regexes, casts that do not constant-fold, empty-string map equality,
and unknown expressions remain residual ClickHouse work. Empty map equality is
not pushed because ClickHouse treats a missing `Map(String, String)` key as the
empty default string while ShardLog's field index distinguishes missing from
stored empty. A hash, parser, or type mismatch can reduce
pushdown efficiency, but cannot change query results: the original ClickHouse
filter is retained and evaluated over every returned row.

Run `scripts/run-clickhouse-adapter-compatibility.sh` with the custom binary or
image. The same query corpus is evaluated over `StorageShardLog` and over a
ClickHouse `Memory` snapshot, with exact serialized-result comparison.
