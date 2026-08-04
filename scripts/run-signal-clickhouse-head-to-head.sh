#!/usr/bin/env bash
set -euo pipefail

SHARD_TELEMETRY_REPOSITORY=${SHARD_TELEMETRY_REPOSITORY:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)}
SHARD_TELEMETRY_BIN=${SHARD_TELEMETRY_BIN:-$SHARD_TELEMETRY_REPOSITORY/target/release/shard-telemetry-signal-bench}
RESULT_ROOT=${RESULT_ROOT:-/home/dtietjen/shard-telemetry-signal-clickhouse-head-to-head}
RUN_ID=${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
RECORDS=${RECORDS:-262144}
LOOKUP_ITERATIONS=${LOOKUP_ITERATIONS:-2000}
CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-sha256:770156c537ca9124046e138a3b5845c64ea58ce8722de7a2e05fd827f4976520}

for command in awk cmp docker lscpu sha256sum stat taskset; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 2
    }
done
[[ -x $SHARD_TELEMETRY_BIN ]] || {
    echo "ShardTelemetry benchmark binary is not executable: $SHARD_TELEMETRY_BIN" >&2
    exit 2
}

CPU=$(lscpu -p=CPU,CORE | awk -F, '!/^#/ && !seen[$2]++ { print $1; exit }')
[[ -n $CPU ]] || {
    echo "unable to select one physical CPU" >&2
    exit 2
}
RUN_DIR=$RESULT_ROOT/$RUN_ID
[[ ! -e $RUN_DIR ]] || {
    echo "result directory already exists: $RUN_DIR" >&2
    exit 2
}
mkdir -p "$RUN_DIR"
exec > >(tee "$RUN_DIR/harness.log") 2>&1

CH_CONTAINER="shard-telemetry-signals-${RUN_ID//[^a-zA-Z0-9_.-]/-}"
CH_STARTED=0
cleanup() {
    if [[ $CH_STARTED -eq 1 ]]; then
        docker logs "$CH_CONTAINER" >"$RUN_DIR/clickhouse-container.log" 2>&1 || true
        docker stop --time 60 "$CH_CONTAINER" >/dev/null 2>&1 || true
        docker rm "$CH_CONTAINER" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT INT TERM

IMAGE_ID=$(docker image inspect "$CLICKHOUSE_IMAGE" --format '{{.Id}}')
[[ $IMAGE_ID == "$CLICKHOUSE_IMAGE" ]] || {
    echo "ClickHouse image mismatch: got $IMAGE_ID, expected $CLICKHOUSE_IMAGE" >&2
    exit 2
}

CORPUS_DIR=$RUN_DIR/corpus
echo "ShardTelemetry: generating equal-input corpus and running on CPU $CPU"
/usr/bin/time -f 'wall_seconds=%e\nuser_seconds=%U\nsystem_seconds=%S\nmax_rss_kib=%M' \
    -o "$RUN_DIR/shard-telemetry-process-time.txt" \
    taskset -c "$CPU" "$SHARD_TELEMETRY_BIN" \
    --records "$RECORDS" \
    --iterations "$LOOKUP_ITERATIONS" \
    --clickhouse-dir "$CORPUS_DIR" \
    >"$RUN_DIR/shard-telemetry-signal.txt"
cat "$RUN_DIR/shard-telemetry-signal.txt"

manifest_value() {
    local key=$1
    awk -F= -v key="$key" '$1 == key { print substr($0, length(key) + 2); exit }' \
        "$CORPUS_DIR/manifest.env"
}

TRACE_SOURCE_BYTES=$(manifest_value trace_source_bytes)
METRIC_SOURCE_BYTES=$(manifest_value metric_source_bytes)
TRACE_ID_HEX=$(manifest_value trace_id_hex)
TRACE_LOOKUP_ROWS=$(manifest_value trace_lookup_rows)
SERIES_ID=$(manifest_value series_id)
METRIC_LOOKUP_ROWS=$(manifest_value metric_lookup_rows)
RESOURCE_ID=$(manifest_value resource_id)
SERVICE_NAME=$(manifest_value service_name)

sha256sum "$CORPUS_DIR"/* >"$RUN_DIR/corpus-sha256.txt"
{
    echo "run_id=$RUN_ID"
    echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "physical_cpu=$CPU"
    echo "records_per_signal=$RECORDS"
    echo "lookup_iterations=$LOOKUP_ITERATIONS"
    echo "product_repository=$SHARD_TELEMETRY_REPOSITORY"
    echo "product_revision=$(git -C "$SHARD_TELEMETRY_REPOSITORY" rev-parse HEAD)"
    echo "product_status=$(git -C "$SHARD_TELEMETRY_REPOSITORY" status --porcelain | wc -l) modified entries"
    echo "binary=$SHARD_TELEMETRY_BIN"
    echo "binary_sha256=$(sha256sum "$SHARD_TELEMETRY_BIN" | awk '{ print $1 }')"
    echo "clickhouse_image=$CLICKHOUSE_IMAGE"
    echo "clickhouse_image_id=$IMAGE_ID"
    echo "kernel=$(uname -srmo)"
    lscpu
} >"$RUN_DIR/provenance.txt"

CH_DATA=$RUN_DIR/clickhouse-data
CH_LOGS=$RUN_DIR/clickhouse-logs
mkdir -p "$CH_DATA" "$CH_LOGS"
echo "ClickHouse: starting isolated one-core container"
docker run --detach \
    --name "$CH_CONTAINER" \
    --network none \
    --cpuset-cpus "$CPU" \
    --ulimit nofile=262144:262144 \
    --env CLICKHOUSE_SKIP_USER_SETUP=1 \
    --volume "$CH_DATA:/var/lib/clickhouse" \
    --volume "$CH_LOGS:/var/log/clickhouse-server" \
    "$CLICKHOUSE_IMAGE" >"$RUN_DIR/clickhouse-container-id.txt"
CH_STARTED=1

for _ in $(seq 1 120); do
    if docker exec "$CH_CONTAINER" clickhouse-client --query 'SELECT 1' >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done
docker exec "$CH_CONTAINER" clickhouse-client --query 'SELECT version()' \
    >"$RUN_DIR/clickhouse-version.txt"

docker exec "$CH_CONTAINER" clickhouse-client --multiquery --query "
CREATE DATABASE benchmark;
CREATE TABLE benchmark.traces
(
    tenant String CODEC(ZSTD(1)),
    trace_id FixedString(16) CODEC(ZSTD(1)),
    span_id FixedString(8) CODEC(ZSTD(1)),
    parent_span_id Nullable(FixedString(8)) CODEC(ZSTD(1)),
    durable_offset UInt64 CODEC(Delta, ZSTD(1)),
    start_ns UInt64 CODEC(DoubleDelta, ZSTD(1)),
    duration_ns UInt64 CODEC(T64, ZSTD(1)),
    name String CODEC(ZSTD(1)),
    kind Int32 CODEC(T64, ZSTD(1)),
    status_code Int32 CODEC(T64, ZSTD(1)),
    resource_id UInt128 CODEC(ZSTD(1)),
    scope_id UInt128 CODEC(ZSTD(1)),
    service_name String CODEC(ZSTD(1)),
    deployment_environment String CODEC(ZSTD(1)),
    http_route String CODEC(ZSTD(1)),
    http_status Int64 CODEC(T64, ZSTD(1)),
    raw String CODEC(ZSTD(1)),
    INDEX resource_filter resource_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX service_filter service_name TYPE set(128) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tenant, trace_id, start_ns, span_id)
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         fsync_after_insert = 1;

CREATE TABLE benchmark.metrics
(
    tenant String CODEC(ZSTD(1)),
    series_id UInt128 CODEC(ZSTD(1)),
    durable_offset UInt64 CODEC(Delta, ZSTD(1)),
    timestamp_ns UInt64 CODEC(DoubleDelta, ZSTD(1)),
    start_ns UInt64 CODEC(DoubleDelta, ZSTD(1)),
    metric_name String CODEC(ZSTD(1)),
    unit String CODEC(ZSTD(1)),
    metric_kind String CODEC(ZSTD(1)),
    resource_id UInt128 CODEC(ZSTD(1)),
    scope_id UInt128 CODEC(ZSTD(1)),
    service_name String CODEC(ZSTD(1)),
    deployment_environment String CODEC(ZSTD(1)),
    http_route String CODEC(ZSTD(1)),
    http_status Int64 CODEC(T64, ZSTD(1)),
    instance String CODEC(ZSTD(1)),
    value Float64 CODEC(Gorilla, ZSTD(1)),
    exemplar_trace_id Nullable(FixedString(16)) CODEC(ZSTD(1)),
    raw String CODEC(ZSTD(1)),
    INDEX resource_filter resource_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX service_filter service_name TYPE set(128) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (tenant, series_id, timestamp_ns, durable_offset)
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         fsync_after_insert = 1;
"

echo "ClickHouse: ingesting traces on CPU $CPU"
/usr/bin/time -f 'wall_seconds=%e\nuser_seconds=%U\nsystem_seconds=%S\nmax_rss_kib=%M' \
    -o "$RUN_DIR/clickhouse-trace-time.txt" \
    taskset -c "$CPU" docker exec -i "$CH_CONTAINER" clickhouse-client \
    --query 'INSERT INTO benchmark.traces FORMAT RowBinary' \
    <"$CORPUS_DIR/traces.rowbinary"

echo "ClickHouse: ingesting metrics on CPU $CPU"
/usr/bin/time -f 'wall_seconds=%e\nuser_seconds=%U\nsystem_seconds=%S\nmax_rss_kib=%M' \
    -o "$RUN_DIR/clickhouse-metric-time.txt" \
    taskset -c "$CPU" docker exec -i "$CH_CONTAINER" clickhouse-client \
    --query 'INSERT INTO benchmark.metrics FORMAT RowBinary' \
    <"$CORPUS_DIR/metrics.rowbinary"

docker exec "$CH_CONTAINER" clickhouse-client --query 'SYSTEM FLUSH LOGS'
docker exec "$CH_CONTAINER" clickhouse-client --query "
SELECT table, sum(rows), sum(bytes_on_disk), sum(data_compressed_bytes),
       sum(data_uncompressed_bytes), sum(marks_bytes), count()
FROM system.parts
WHERE active AND database = 'benchmark'
GROUP BY table
ORDER BY table
FORMAT TSVWithNames
" >"$RUN_DIR/clickhouse-parts.tsv"

docker exec "$CH_CONTAINER" clickhouse-client --query "
SELECT raw FROM benchmark.traces
WHERE tenant = 'production-example' AND trace_id = unhex('$TRACE_ID_HEX')
ORDER BY start_ns, span_id
FORMAT RowBinary
" >"$RUN_DIR/trace-lookup-actual.rowbinary"
docker exec "$CH_CONTAINER" clickhouse-client --query "
SELECT raw FROM benchmark.metrics
WHERE tenant = 'production-example' AND series_id = toUInt128('$SERIES_ID')
ORDER BY timestamp_ns, durable_offset
FORMAT RowBinary
" >"$RUN_DIR/metric-lookup-actual.rowbinary"
cmp "$CORPUS_DIR/trace-lookup-expected.rowbinary" "$RUN_DIR/trace-lookup-actual.rowbinary"
cmp "$CORPUS_DIR/metric-lookup-expected.rowbinary" "$RUN_DIR/metric-lookup-actual.rowbinary"

TRACE_ROWS=$(docker exec "$CH_CONTAINER" clickhouse-client --query "
SELECT count() FROM benchmark.traces
WHERE tenant = 'production-example' AND trace_id = unhex('$TRACE_ID_HEX')")
METRIC_ROWS=$(docker exec "$CH_CONTAINER" clickhouse-client --query "
SELECT count() FROM benchmark.metrics
WHERE tenant = 'production-example' AND series_id = toUInt128('$SERIES_ID')")
[[ $TRACE_ROWS -eq $TRACE_LOOKUP_ROWS ]] || {
    echo "trace lookup row mismatch: got $TRACE_ROWS, expected $TRACE_LOOKUP_ROWS" >&2
    exit 1
}
[[ $METRIC_ROWS -eq $METRIC_LOOKUP_ROWS ]] || {
    echo "metric lookup row mismatch: got $METRIC_ROWS, expected $METRIC_LOOKUP_ROWS" >&2
    exit 1
}

TRACE_QUERY="SELECT raw FROM benchmark.traces WHERE tenant = 'production-example' AND trace_id = unhex('$TRACE_ID_HEX') LIMIT 32 FORMAT Null"
METRIC_QUERY="SELECT raw FROM benchmark.metrics WHERE tenant = 'production-example' AND series_id = toUInt128('$SERIES_ID') LIMIT 100 FORMAT Null"
CORRELATION_QUERY="SELECT raw FROM benchmark.traces WHERE resource_id = toUInt128('$RESOURCE_ID') AND service_name = '$SERVICE_NAME' LIMIT 1000 FORMAT Null"
docker exec "$CH_CONTAINER" clickhouse-benchmark --concurrency 1 \
    --iterations "$LOOKUP_ITERATIONS" --query "$TRACE_QUERY" \
    >"$RUN_DIR/clickhouse-trace-lookup.txt" 2>&1
docker exec "$CH_CONTAINER" clickhouse-benchmark --concurrency 1 \
    --iterations "$LOOKUP_ITERATIONS" --query "$METRIC_QUERY" \
    >"$RUN_DIR/clickhouse-metric-lookup.txt" 2>&1
docker exec "$CH_CONTAINER" clickhouse-benchmark --concurrency 1 \
    --iterations "$LOOKUP_ITERATIONS" --query "$CORRELATION_QUERY" \
    >"$RUN_DIR/clickhouse-correlation-lookup.txt" 2>&1

field_from_signal() {
    local signal=$1
    local field=$2
    awk -v signal="$signal" -v field="$field" '
        $1 == signal {
            for (position = 2; position <= NF; position++) {
                split($position, pair, "=")
                if (pair[1] == field) {
                    print pair[2]
                    exit
                }
            }
        }
    ' "$RUN_DIR/shard-telemetry-signal.txt"
}

TRACE_STORED=$(field_from_signal traces stored_bytes)
TRACE_ENCODE=$(field_from_signal traces encode_mib_s)
METRIC_STORED=$(field_from_signal metrics stored_bytes)
METRIC_ENCODE=$(field_from_signal metrics encode_mib_s)
CH_TRACE_STORED=$(awk -F'\t' '$1 == "traces" { print $3 }' "$RUN_DIR/clickhouse-parts.tsv")
CH_METRIC_STORED=$(awk -F'\t' '$1 == "metrics" { print $3 }' "$RUN_DIR/clickhouse-parts.tsv")
CH_TRACE_SECONDS=$(awk -F= '$1 == "wall_seconds" { print $2 }' "$RUN_DIR/clickhouse-trace-time.txt")
CH_METRIC_SECONDS=$(awk -F= '$1 == "wall_seconds" { print $2 }' "$RUN_DIR/clickhouse-metric-time.txt")

{
    printf 'signal\tengine\tsource_bytes\tstored_bytes\tratio\tencode_mib_s\n'
    awk -v source="$TRACE_SOURCE_BYTES" -v stored="$TRACE_STORED" -v rate="$TRACE_ENCODE" \
        'BEGIN { printf "traces\tShardTelemetry\t%.0f\t%.0f\t%.4f\t%.2f\n", source, stored, source / stored, rate }'
    awk -v source="$TRACE_SOURCE_BYTES" -v stored="$CH_TRACE_STORED" -v seconds="$CH_TRACE_SECONDS" \
        'BEGIN { printf "traces\tClickHouse\t%.0f\t%.0f\t%.4f\t%.2f\n", source, stored, source / stored, source / 1048576 / seconds }'
    awk -v source="$METRIC_SOURCE_BYTES" -v stored="$METRIC_STORED" -v rate="$METRIC_ENCODE" \
        'BEGIN { printf "metrics\tShardTelemetry\t%.0f\t%.0f\t%.4f\t%.2f\n", source, stored, source / stored, rate }'
    awk -v source="$METRIC_SOURCE_BYTES" -v stored="$CH_METRIC_STORED" -v seconds="$CH_METRIC_SECONDS" \
        'BEGIN { printf "metrics\tClickHouse\t%.0f\t%.0f\t%.4f\t%.2f\n", source, stored, source / stored, source / 1048576 / seconds }'
} >"$RUN_DIR/summary.tsv"

sha256sum "$RUN_DIR/trace-lookup-actual.rowbinary" \
    "$RUN_DIR/metric-lookup-actual.rowbinary" >"$RUN_DIR/query-result-sha256.txt"
cat "$RUN_DIR/summary.tsv"
echo "trace_lookup_rows=$TRACE_ROWS metric_lookup_rows=$METRIC_ROWS"
echo "results=$RUN_DIR"
