#!/usr/bin/env bash
set -euo pipefail

SOURCE=${SOURCE:-/home/dtietjen/log-compression-samples/clickhouse-docker-json-error-loop-tail-80g-20260729.log}
EXPECTED_SHA256=${EXPECTED_SHA256:-4fd6379bd89fcb44688a3ebd611729416c82f110fbf49ffef905d9df0ebf0508}
EXPECTED_FILE_BYTES=${EXPECTED_FILE_BYTES:-85899345920}
SOURCE_LIMIT_BYTES=${SOURCE_LIMIT_BYTES:-1073741824}
SHARD_LOG_BUILD_BIN=${SHARD_LOG_BUILD_BIN:-/home/dtietjen/shard-log-query-head-to-head-20260730-v7/shard-log/target/release/shard-log-structural-bench}
SHARD_LOG_QUERY_BIN=${SHARD_LOG_QUERY_BIN:-/home/dtietjen/shard-log-query-head-to-head-20260730-v7/shard-log/target/release/shard-log-pack-query-bench}
SOURCE_ARCHIVE=${SOURCE_ARCHIVE:-/home/dtietjen/shard-log-query-head-to-head-20260730-v7.tar.gz}
SHARD_STREAM_SOURCE=${SHARD_STREAM_SOURCE:-/home/dtietjen/shard-log-query-head-to-head-20260730-v7/shard-stream}
SHARD_STREAM_REVISION=${SHARD_STREAM_REVISION:-13ee7903d42cabe9bd5c0df0fa8e4a4fdc660ea7}
EXPECTED_SHARD_STREAM_SOURCE_TREE_SHA256=${EXPECTED_SHARD_STREAM_SOURCE_TREE_SHA256:-ad9fdfd9b13fb9635d40a5df6308cdecca7de81a5e3ebfe45db8b5e47f229308}
EXPECTED_REJECTED_RECORDS=${EXPECTED_REJECTED_RECORDS:-0}
RESULT_ROOT=${RESULT_ROOT:-/home/dtietjen/shard-log-query-head-to-head}
RUN_ID=${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
CORE_COUNT=${CORE_COUNT:-16}
BLOCK_BYTES=${BLOCK_BYTES:-8MiB}
QUERY_ITERATIONS=${QUERY_ITERATIONS:-200}
CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-sha256:770156c537ca9124046e138a3b5845c64ea58ce8722de7a2e05fd827f4976520}
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CONFIG=$SCRIPT_DIR/clickhouse-benchmark.xml
CLICKHOUSE_INGEST=$SCRIPT_DIR/clickhouse-ingest-range.sh

if [[ $CORE_COUNT -ne 16 ]]; then
    echo "this comparison is fixed at 16 physical cores" >&2
    exit 2
fi
for command in awk cmp cp dd docker find lscpu sha256sum sort stat taskset wc xargs; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 2
    }
done
for path in \
    "$SOURCE" \
    "$SHARD_LOG_BUILD_BIN" \
    "$SHARD_LOG_QUERY_BIN" \
    "$SOURCE_ARCHIVE" \
    "$SHARD_STREAM_SOURCE" \
    "$CLICKHOUSE_CONFIG" \
    "$CLICKHOUSE_INGEST"; do
    [[ -e $path ]] || {
        echo "required path does not exist: $path" >&2
        exit 2
    }
done

mapfile -t PHYSICAL_CPUS < <(
    lscpu -p=CPU,CORE |
        awk -F, '!/^#/ && !seen[$2]++ { print $1 }' |
        awk -v count="$CORE_COUNT" 'NR <= count'
)
[[ ${#PHYSICAL_CPUS[@]} -eq $CORE_COUNT ]] || {
    echo "could not resolve $CORE_COUNT physical CPUs" >&2
    exit 2
}
CPU_SET=$(IFS=,; echo "${PHYSICAL_CPUS[*]}")
CONTAINER_UID=$(id -u)
CONTAINER_GID=$(id -g)
SOURCE_FILE_BYTES=$(stat -c %s "$SOURCE")
[[ $SOURCE_FILE_BYTES -eq $EXPECTED_FILE_BYTES ]] || {
    echo "source byte length mismatch" >&2
    exit 2
}
[[ $SOURCE_LIMIT_BYTES -le $SOURCE_FILE_BYTES ]] || {
    echo "source limit exceeds source file" >&2
    exit 2
}

RUN_DIR=$RESULT_ROOT/$RUN_ID
[[ ! -e $RUN_DIR ]] || {
    echo "result directory already exists: $RUN_DIR" >&2
    exit 2
}
mkdir -p "$RUN_DIR"
exec > >(tee "$RUN_DIR/harness.log") 2>&1

CH_CONTAINER="shard-log-query-ch-${RUN_ID//[^a-zA-Z0-9_.-]/-}"
CH_STARTED=0
cleanup() {
    if [[ $CH_STARTED -eq 1 ]]; then
        docker logs "$CH_CONTAINER" >"$RUN_DIR/clickhouse-container.log" 2>&1 || true
        docker stop --time 60 "$CH_CONTAINER" >/dev/null 2>&1 || true
        docker rm "$CH_CONTAINER" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT INT TERM

SOURCE_SHA256=$(sha256sum "$SOURCE" | awk '{ print $1 }')
[[ $SOURCE_SHA256 == "$EXPECTED_SHA256" ]] || {
    echo "source SHA-256 mismatch" >&2
    exit 2
}
IMAGE_ID=$(docker image inspect "$CLICKHOUSE_IMAGE" --format '{{.Id}}')
[[ $IMAGE_ID == "$CLICKHOUSE_IMAGE" ]] || {
    echo "ClickHouse image mismatch" >&2
    exit 2
}
SHARD_STREAM_SOURCE_TREE_SHA256=$(
    cd "$SHARD_STREAM_SOURCE"
    find \
        Cargo.toml \
        Cargo.lock \
        rust-toolchain.toml \
        crates/shard-stream-core \
        crates/shard-stream-engine \
        crates/shard-stream-protocol \
        crates/shardlog \
        -type f -print0 |
        sort -z |
        xargs -0 sha256sum |
        sha256sum |
        awk '{ print $1 }'
)
[[ $SHARD_STREAM_SOURCE_TREE_SHA256 == "$EXPECTED_SHARD_STREAM_SOURCE_TREE_SHA256" ]] || {
    echo "shard-stream source-tree hash mismatch" >&2
    exit 2
}

{
    echo "run_id=$RUN_ID"
    echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "source=$SOURCE"
    echo "source_file_bytes=$SOURCE_FILE_BYTES"
    echo "source_limit_bytes=$SOURCE_LIMIT_BYTES"
    echo "source_sha256=$SOURCE_SHA256"
    echo "physical_cpu_set=$CPU_SET"
    echo "core_count=$CORE_COUNT"
    echo "block_bytes=$BLOCK_BYTES"
    echo "query_iterations=$QUERY_ITERATIONS"
    echo "expected_rejected_records=$EXPECTED_REJECTED_RECORDS"
    echo "shard_log_build_binary=$SHARD_LOG_BUILD_BIN"
    echo "shard_log_build_sha256=$(sha256sum "$SHARD_LOG_BUILD_BIN" | awk '{ print $1 }')"
    echo "shard_log_query_binary=$SHARD_LOG_QUERY_BIN"
    echo "shard_log_query_sha256=$(sha256sum "$SHARD_LOG_QUERY_BIN" | awk '{ print $1 }')"
    echo "source_archive=$SOURCE_ARCHIVE"
    echo "source_archive_sha256=$(sha256sum "$SOURCE_ARCHIVE" | awk '{ print $1 }')"
    echo "shard_stream_source=$SHARD_STREAM_SOURCE"
    echo "shard_stream_revision=$SHARD_STREAM_REVISION"
    echo "shard_stream_source_tree_sha256=$SHARD_STREAM_SOURCE_TREE_SHA256"
    echo "clickhouse_image=$CLICKHOUSE_IMAGE"
    echo "clickhouse_image_id=$IMAGE_ID"
    echo "kernel=$(uname -srmo)"
    docker version --format 'docker_server={{.Server.Version}}'
    lscpu
} >"$RUN_DIR/provenance.txt"

prewarm_range() {
    local label=$1
    local skip_bytes=$2
    local count_bytes=$3
    local started ended
    started=$(date +%s%N)
    dd if="$SOURCE" \
        of=/dev/null \
        bs=64M \
        iflag=skip_bytes,count_bytes \
        skip="$skip_bytes" \
        count="$count_bytes" \
        status=none
    ended=$(date +%s%N)
    awk -v label="$label" -v elapsed_ns="$((ended - started))" \
        'BEGIN { printf "%s prewarm seconds: %.6f\n", label, elapsed_ns / 1000000000 }'
}

SHARD_PACKS=$RUN_DIR/shard-log-packs
echo "ShardLog: prewarming requested source prefix"
prewarm_range ShardLog 0 "$SOURCE_LIMIT_BYTES"
echo "ShardLog: building persistent query index on CPUs $CPU_SET"
/usr/bin/time -f 'wall_seconds=%e\nuser_seconds=%U\nsystem_seconds=%S\nmax_rss_kib=%M' \
    -o "$RUN_DIR/shard-log-ingest-time.txt" \
    taskset -c "$CPU_SET" "$SHARD_LOG_BUILD_BIN" "$SOURCE" \
    --limit-bytes "$SOURCE_LIMIT_BYTES" \
    --block-bytes "$BLOCK_BYTES" \
    --workers "$CORE_COUNT" \
    --locality disabled \
    --dictionary disabled \
    --index persistent \
    --output-dir "$SHARD_PACKS" \
    --report "$RUN_DIR/shard-log-report.txt"

SOURCE_SKIP_BYTES=$(awk -F': ' '$1 == "leading partial bytes discarded" { print $2 }' "$RUN_DIR/shard-log-report.txt")
COMPLETE_LINE_SOURCE_BYTES=$(awk -F': ' '$1 == "complete-line input bytes" { print $2 }' "$RUN_DIR/shard-log-report.txt")
SHARD_ACCEPTED_SOURCE_BYTES=$(awk -F': ' '$1 == "source bytes" { print $2 }' "$RUN_DIR/shard-log-report.txt")
SHARD_RECORDS=$(awk -F': ' '$1 == "records" { print $2 }' "$RUN_DIR/shard-log-report.txt")
SHARD_REJECTED=$(awk -F': ' '$1 == "rejected complete records" { print $2 }' "$RUN_DIR/shard-log-report.txt")
[[ $SHARD_REJECTED -eq $EXPECTED_REJECTED_RECORDS ]] || {
    echo "ShardLog rejected $SHARD_REJECTED complete records, expected $EXPECTED_REJECTED_RECORDS" >&2
    exit 1
}
[[ $((SHARD_ACCEPTED_SOURCE_BYTES + $(awk -F': ' '$1 == "rejected complete-record bytes" { print $2 }' "$RUN_DIR/shard-log-report.txt"))) -eq $COMPLETE_LINE_SOURCE_BYTES ]] || {
    echo "ShardLog accepted plus rejected bytes do not cover the complete-line source range" >&2
    exit 1
}

CH_DATA=$RUN_DIR/clickhouse-data
CH_LOGS=$RUN_DIR/clickhouse-logs
mkdir -p "$CH_DATA" "$CH_LOGS"
echo "ClickHouse: starting indexed container on CPUs $CPU_SET"
docker run --detach \
    --name "$CH_CONTAINER" \
    --network none \
    --cpuset-cpus "$CPU_SET" \
    --ulimit nofile=262144:262144 \
    --user "$CONTAINER_UID:$CONTAINER_GID" \
    --env CLICKHOUSE_SKIP_USER_SETUP=1 \
    --volume "$SOURCE:/benchmark/input.json:ro" \
    --volume "$SCRIPT_DIR:/benchmark-scripts:ro" \
    --volume "$CLICKHOUSE_CONFIG:/etc/clickhouse-server/config.d/benchmark.xml:ro" \
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
CREATE TABLE benchmark.logs
(
    time DateTime64(9, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    stream LowCardinality(String) CODEC(ZSTD(1)),
    log String CODEC(ZSTD(1)),
    INDEX log_text log TYPE text(
        tokenizer = 'splitByNonAlpha',
        preprocessor = lower(log)
    )
)
ENGINE = MergeTree
ORDER BY (stream, time)
SETTINGS index_granularity = 8192, fsync_after_insert = 1;
"

echo "ClickHouse: prewarming the exact complete-line source range"
prewarm_range ClickHouse "$SOURCE_SKIP_BYTES" "$COMPLETE_LINE_SOURCE_BYTES"
echo "ClickHouse: ingesting and building text index"
/usr/bin/time -f 'wall_seconds=%e\nuser_seconds=%U\nsystem_seconds=%S\nmax_rss_kib=%M' \
    -o "$RUN_DIR/clickhouse-ingest-time.txt" \
    docker exec \
    --env CORE_COUNT="$CORE_COUNT" \
    --env SOURCE_SKIP_BYTES="$SOURCE_SKIP_BYTES" \
    --env SOURCE_BYTES="$COMPLETE_LINE_SOURCE_BYTES" \
    --env ALLOW_ERRORS_NUM="$EXPECTED_REJECTED_RECORDS" \
    "$CH_CONTAINER" \
    /bin/bash /benchmark-scripts/clickhouse-ingest-range.sh

CH_PARSE_ERRORS=$CH_DATA/user_files/clickhouse-parse-errors.csv
if [[ $EXPECTED_REJECTED_RECORDS -gt 0 ]]; then
    [[ -s $CH_PARSE_ERRORS ]] || {
        echo "ClickHouse did not retain its expected parse-error evidence" >&2
        exit 1
    }
    cp "$CH_PARSE_ERRORS" "$RUN_DIR/clickhouse-parse-errors.csv"
    CH_RECORDED_ERRORS=$(wc -l <"$CH_PARSE_ERRORS")
    [[ $CH_RECORDED_ERRORS -eq $EXPECTED_REJECTED_RECORDS ]] || {
        echo "ClickHouse recorded $CH_RECORDED_ERRORS parse errors, expected $EXPECTED_REJECTED_RECORDS" >&2
        exit 1
    }
elif [[ -s $CH_PARSE_ERRORS ]]; then
    echo "ClickHouse unexpectedly recorded parse errors" >&2
    exit 1
fi

docker exec "$CH_CONTAINER" clickhouse-client --query 'SYSTEM FLUSH LOGS'
docker exec "$CH_CONTAINER" clickhouse-client --query "
SELECT count() FROM benchmark.logs
" >"$RUN_DIR/clickhouse-row-count.txt"
CH_RECORDS=$(tr -d '[:space:]' <"$RUN_DIR/clickhouse-row-count.txt")
[[ $CH_RECORDS -eq $SHARD_RECORDS ]] || {
    echo "record-count mismatch: ShardLog=$SHARD_RECORDS ClickHouse=$CH_RECORDS" >&2
    exit 1
}
docker exec "$CH_CONTAINER" clickhouse-client --query "
SELECT
    sum(rows) AS rows,
    sum(bytes_on_disk) AS bytes_on_disk,
    sum(data_compressed_bytes) AS data_compressed_bytes,
    sum(data_uncompressed_bytes) AS data_uncompressed_bytes,
    sum(marks_bytes) AS marks_bytes,
    count() AS active_parts
FROM system.parts
WHERE active AND database = 'benchmark' AND table = 'logs'
FORMAT TSVWithNames
" >"$RUN_DIR/clickhouse-parts.tsv"
docker exec "$CH_CONTAINER" clickhouse-client --query "
SELECT
    name,
    type,
    data_compressed_bytes,
    data_uncompressed_bytes
FROM system.data_skipping_indices
WHERE database = 'benchmark' AND table = 'logs'
FORMAT TSVWithNames
" >"$RUN_DIR/clickhouse-index.tsv"

run_shard_query() {
    local name=$1
    shift
    taskset -c "$CPU_SET" "$SHARD_LOG_QUERY_BIN" "$SHARD_PACKS" \
        --limit 100 \
        --iterations "$QUERY_ITERATIONS" \
        --emit-results "$RUN_DIR/shard-${name}-results.tsv" \
        "$@" >"$RUN_DIR/shard-${name}.txt"
}

run_clickhouse_query() {
    local name=$1
    local indexed_query=$2
    local scan_query=$3
    local result_query=$4
    docker exec "$CH_CONTAINER" clickhouse-client --query "$result_query FORMAT TabSeparatedRaw" \
        >"$RUN_DIR/clickhouse-${name}-results.tsv"
    cmp "$RUN_DIR/shard-${name}-results.tsv" "$RUN_DIR/clickhouse-${name}-results.tsv"
    sha256sum \
        "$RUN_DIR/shard-${name}-results.tsv" \
        "$RUN_DIR/clickhouse-${name}-results.tsv" \
        >"$RUN_DIR/${name}-result-checksums.txt"

    docker exec "$CH_CONTAINER" clickhouse-client --query \
        "$indexed_query SETTINGS use_query_cache=0,use_query_condition_cache=0,max_threads=$CORE_COUNT FORMAT Null" \
        >/dev/null
    docker exec "$CH_CONTAINER" clickhouse-benchmark \
        --iterations "$QUERY_ITERATIONS" \
        --concurrency 1 \
        --delay 0 \
        --query "$indexed_query SETTINGS use_query_cache=0,use_query_condition_cache=0,max_threads=$CORE_COUNT FORMAT Null" \
        >"$RUN_DIR/clickhouse-${name}-indexed.txt" 2>&1

    docker exec "$CH_CONTAINER" clickhouse-client --query \
        "$scan_query SETTINGS use_skip_indexes=0,use_query_cache=0,use_query_condition_cache=0,max_threads=$CORE_COUNT FORMAT Null" \
        >/dev/null
    docker exec "$CH_CONTAINER" clickhouse-benchmark \
        --iterations "$QUERY_ITERATIONS" \
        --concurrency 1 \
        --delay 0 \
        --query "$scan_query SETTINGS use_skip_indexes=0,use_query_cache=0,use_query_condition_cache=0,max_threads=$CORE_COUNT FORMAT Null" \
        >"$RUN_DIR/clickhouse-${name}-scan.txt" 2>&1
}

echo "Running warm query matrix"
run_shard_query latest
run_clickhouse_query \
    latest \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs ORDER BY time DESC LIMIT 100" \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs ORDER BY time DESC LIMIT 100" \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs ORDER BY time DESC LIMIT 100"

run_shard_query stream --field docker.stream=stderr
run_clickhouse_query \
    stream \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE stream = 'stderr' ORDER BY time DESC LIMIT 100" \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE stream = 'stderr' ORDER BY time DESC LIMIT 100" \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE stream = 'stderr' ORDER BY time DESC LIMIT 100"

run_shard_query cannot --term cannot
run_clickhouse_query \
    cannot \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE hasToken(log, 'cannot') ORDER BY time DESC LIMIT 100" \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE hasTokenCaseInsensitive(log, 'cannot') ORDER BY time DESC LIMIT 100" \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE hasToken(log, 'cannot') ORDER BY time DESC LIMIT 100"

run_shard_query error-and \
    --term cannot \
    --term exception \
    --term file \
    --term access \
    --term error
run_clickhouse_query \
    error-and \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE hasAllTokens(log, 'cannot exception file access error') ORDER BY time DESC LIMIT 100" \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE hasTokenCaseInsensitive(log, 'cannot') AND hasTokenCaseInsensitive(log, 'exception') AND hasTokenCaseInsensitive(log, 'file') AND hasTokenCaseInsensitive(log, 'access') AND hasTokenCaseInsensitive(log, 'error') ORDER BY time DESC LIMIT 100" \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE hasAllTokens(log, 'cannot exception file access error') ORDER BY time DESC LIMIT 100"

run_shard_query miss --term shardlogtermthatdoesnotexist
run_clickhouse_query \
    miss \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE hasToken(log, 'shardlogtermthatdoesnotexist') ORDER BY time DESC LIMIT 100" \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE hasTokenCaseInsensitive(log, 'shardlogtermthatdoesnotexist') ORDER BY time DESC LIMIT 100" \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE hasToken(log, 'shardlogtermthatdoesnotexist') ORDER BY time DESC LIMIT 100"

docker exec "$CH_CONTAINER" clickhouse-client --query 'SYSTEM FLUSH LOGS'
docker exec "$CH_CONTAINER" clickhouse-client --query "
SELECT
    query_duration_ms,
    read_rows,
    read_bytes,
    result_rows,
    memory_usage,
    query
FROM system.query_log
WHERE event_time >= now() - INTERVAL 1 HOUR
  AND type = 'QueryFinish'
  AND query LIKE 'SELECT toUnixTimestamp64Nano%'
ORDER BY event_time_microseconds
FORMAT TSVWithNames
" >"$RUN_DIR/clickhouse-query-log.tsv"

{
    printf 'engine\trecords\tcomplete_line_source_bytes\taccepted_source_bytes\trejected_records\tstored_bytes\tingest_seconds\tingest_mib_s\n'
    SHARD_STORED=$(awk -F': ' '$1 == "durable pack plus manifest" { split($2, value, " "); print value[1] }' "$RUN_DIR/shard-log-report.txt")
    SHARD_SECONDS=$(awk -F= '$1 == "wall_seconds" { print $2 }' "$RUN_DIR/shard-log-ingest-time.txt")
    CH_STORED=$(awk -F'\t' 'NR == 2 { print $2 }' "$RUN_DIR/clickhouse-parts.tsv")
    CH_SECONDS=$(awk -F= '$1 == "wall_seconds" { print $2 }' "$RUN_DIR/clickhouse-ingest-time.txt")
    awk -v records="$SHARD_RECORDS" -v source="$COMPLETE_LINE_SOURCE_BYTES" \
        -v accepted="$SHARD_ACCEPTED_SOURCE_BYTES" -v rejected="$SHARD_REJECTED" \
        -v stored="$SHARD_STORED" -v seconds="$SHARD_SECONDS" \
        'BEGIN { printf "ShardLog\t%.0f\t%.0f\t%.0f\t%.0f\t%.0f\t%.6f\t%.2f\n", records, source, accepted, rejected, stored, seconds, source / 1048576 / seconds }'
    awk -v records="$CH_RECORDS" -v source="$COMPLETE_LINE_SOURCE_BYTES" \
        -v accepted="$SHARD_ACCEPTED_SOURCE_BYTES" -v rejected="$SHARD_REJECTED" \
        -v stored="$CH_STORED" -v seconds="$CH_SECONDS" \
        'BEGIN { printf "ClickHouse-text\t%.0f\t%.0f\t%.0f\t%.0f\t%.0f\t%.6f\t%.2f\n", records, source, accepted, rejected, stored, seconds, source / 1048576 / seconds }'
} >"$RUN_DIR/ingest-summary.tsv"

cat "$RUN_DIR/ingest-summary.tsv"
echo "results: $RUN_DIR"
