#!/usr/bin/env bash
set -euo pipefail

BASELINE_RUN=${BASELINE_RUN:-/home/dtietjen/shard-telemetry-query-head-to-head/80gib-v7-20260730T205000Z}
SHARD_PACKS=${SHARD_PACKS:-$BASELINE_RUN/shard-telemetry-packs}
CLICKHOUSE_SOURCE_DATA=${CLICKHOUSE_SOURCE_DATA:-$BASELINE_RUN/clickhouse-data}
SHARD_TELEMETRY_QUERY_BIN=${SHARD_TELEMETRY_QUERY_BIN:-/home/dtietjen/shard-telemetry-cold-query-20260730-v2/shard-telemetry/target/release/shard-telemetry-pack-query-bench}
SOURCE_ARCHIVE=${SOURCE_ARCHIVE:-/home/dtietjen/shard-telemetry-cold-query-20260730-v2.tar.gz}
SHARD_STREAM_SOURCE=${SHARD_STREAM_SOURCE:-/home/dtietjen/shard-telemetry-query-head-to-head-20260730-v7/shard-stream}
SHARD_STREAM_REVISION=${SHARD_STREAM_REVISION:-13ee7903d42cabe9bd5c0df0fa8e4a4fdc660ea7}
DSIM_REPOSITORY=${DSIM_REPOSITORY:-/home/dtietjen/deterministic-simulation}
RESULT_ROOT=${RESULT_ROOT:-/home/dtietjen/shard-telemetry-query-head-to-head}
RUN_ID=${RUN_ID:-cold-current-$(date -u +%Y%m%dT%H%M%SZ)}
CORE_COUNT=${CORE_COUNT:-16}
WARM_ITERATIONS=${WARM_ITERATIONS:-20}
COLD_ITERATIONS=${COLD_ITERATIONS:-5}
MISS_ITERATIONS=${MISS_ITERATIONS:-1}
CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-sha256:770156c537ca9124046e138a3b5845c64ea58ce8722de7a2e05fd827f4976520}
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CONFIG=$SCRIPT_DIR/clickhouse-benchmark.xml

if [[ $CORE_COUNT -ne 16 ]]; then
    echo "this comparison is fixed at 16 physical cores" >&2
    exit 2
fi
for command in awk cmp cp docker find git lscpu python3 sha256sum sort stat taskset; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 2
    }
done
for path in \
    "$SHARD_PACKS" \
    "$CLICKHOUSE_SOURCE_DATA" \
    "$SHARD_TELEMETRY_QUERY_BIN" \
    "$SOURCE_ARCHIVE" \
    "$SHARD_STREAM_SOURCE" \
    "$DSIM_REPOSITORY" \
    "$CLICKHOUSE_CONFIG"; do
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
RUN_DIR=$RESULT_ROOT/$RUN_ID
[[ ! -e $RUN_DIR ]] || {
    echo "result directory already exists: $RUN_DIR" >&2
    exit 2
}
mkdir -p "$RUN_DIR"
exec > >(tee "$RUN_DIR/harness.log") 2>&1

CH_CONTAINER="shard-telemetry-cold-query-${RUN_ID//[^a-zA-Z0-9_.-]/-}"
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
    echo "ClickHouse image mismatch" >&2
    exit 2
}
{
    echo "run_id=$RUN_ID"
    echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "execution_tier=native_linux_host_performance"
    echo "replay_claim=none"
    echo "baseline_run=$BASELINE_RUN"
    echo "shard_packs=$SHARD_PACKS"
    echo "shard_manifest_sha256=$(sha256sum "$SHARD_PACKS/manifest.bin" | awk '{ print $1 }')"
    echo "shard_index_sha256=$(sha256sum "$SHARD_PACKS/query-index.bin" | awk '{ print $1 }')"
    echo "shard_query_binary=$SHARD_TELEMETRY_QUERY_BIN"
    echo "shard_query_binary_sha256=$(sha256sum "$SHARD_TELEMETRY_QUERY_BIN" | awk '{ print $1 }')"
    echo "shard_telemetry_product_revision=unborn"
    echo "shard_telemetry_source_archive=$SOURCE_ARCHIVE"
    echo "shard_telemetry_source_archive_sha256=$(sha256sum "$SOURCE_ARCHIVE" | awk '{ print $1 }')"
    echo "harness_script_sha256=$(sha256sum "$0" | awk '{ print $1 }')"
    echo "shard_stream_revision=$SHARD_STREAM_REVISION"
    echo "deterministic_simulation_revision=$(git -C "$DSIM_REPOSITORY" rev-parse HEAD)"
    echo "provenance_gap=ShardTelemetry repository is pre-release and has no commit; source is identified by archive and binary SHA-256"
    echo "clickhouse_source_data=$CLICKHOUSE_SOURCE_DATA"
    echo "clickhouse_image=$CLICKHOUSE_IMAGE"
    echo "physical_cpu_set=$CPU_SET"
    echo "warm_iterations=$WARM_ITERATIONS"
    echo "cold_iterations=$COLD_ITERATIONS"
    echo "cold_method=resident indexes plus POSIX_FADV_DONTNEED over immutable record payloads before each timed query"
    echo "kernel=$(uname -srmo)"
    docker version --format 'docker_server={{.Server.Version}}'
    lscpu
} >"$RUN_DIR/provenance.txt"

CH_DATA=$RUN_DIR/clickhouse-data
CH_LOGS=$RUN_DIR/clickhouse-logs
mkdir -p "$CH_DATA" "$CH_LOGS"
echo "Copying the verified ClickHouse data snapshot"
cp --archive --reflink=auto "$CLICKHOUSE_SOURCE_DATA/." "$CH_DATA/"

CONTAINER_UID=$(id -u)
CONTAINER_GID=$(id -g)
echo "Starting ClickHouse on CPUs $CPU_SET"
docker run --detach \
    --name "$CH_CONTAINER" \
    --network none \
    --cpuset-cpus "$CPU_SET" \
    --ulimit nofile=262144:262144 \
    --user "$CONTAINER_UID:$CONTAINER_GID" \
    --env CLICKHOUSE_SKIP_USER_SETUP=1 \
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
docker exec "$CH_CONTAINER" clickhouse-client --query \
    'SELECT count() FROM benchmark.logs' >"$RUN_DIR/clickhouse-row-count.txt"

drop_clickhouse_caches() {
    for command in \
        'SYSTEM DROP UNCOMPRESSED CACHE' \
        'SYSTEM DROP QUERY CONDITION CACHE'; do
        docker exec "$CH_CONTAINER" clickhouse-client --query "$command" \
            >>"$RUN_DIR/clickhouse-cache-drop.log" 2>&1 || true
    done
    python3 - "$CH_DATA" <<'PY'
import os
import sys

root = os.path.join(sys.argv[1], "store")
payload_files = {
    "time.bin",
    "stream.bin",
    "stream.dict.bin",
    "log.bin",
    "log.size.bin",
}
if os.path.isdir(root):
    for current, _, files in os.walk(root):
        if "skp_idx_log_text.pst.idx" not in files:
            continue
        for name in payload_files.intersection(files):
            path = os.path.join(current, name)
            try:
                descriptor = os.open(path, os.O_RDONLY)
            except OSError:
                continue
            try:
                os.posix_fadvise(
                    descriptor,
                    0,
                    0,
                    os.POSIX_FADV_DONTNEED,
                )
            finally:
                os.close(descriptor)
PY
}

run_shard_query() {
    local name=$1
    local iterations=$2
    local cold_iterations=$3
    shift 3
    echo "ShardTelemetry: $name"
    taskset -c "$CPU_SET" "$SHARD_TELEMETRY_QUERY_BIN" "$SHARD_PACKS" \
        --limit 100 \
        --iterations "$iterations" \
        --cold-iterations "$cold_iterations" \
        --workers "$CORE_COUNT" \
        --emit-results "$RUN_DIR/shard-${name}-results.tsv" \
        "$@" >"$RUN_DIR/shard-${name}.txt"
}

run_clickhouse_query() {
    local name=$1
    local warm_iterations=$2
    local cold_iterations=$3
    local query=$4
    echo "ClickHouse: $name"
    docker exec "$CH_CONTAINER" clickhouse-client \
        --query "$query FORMAT TabSeparatedRaw" \
        >"$RUN_DIR/clickhouse-${name}-results.tsv"
    cmp "$RUN_DIR/shard-${name}-results.tsv" "$RUN_DIR/clickhouse-${name}-results.tsv"
    sha256sum \
        "$RUN_DIR/shard-${name}-results.tsv" \
        "$RUN_DIR/clickhouse-${name}-results.tsv" \
        >"$RUN_DIR/${name}-result-checksums.txt"

    docker exec "$CH_CONTAINER" clickhouse-client --query \
        "$query SETTINGS use_query_cache=0,use_query_condition_cache=0,enable_filesystem_cache=0,max_threads=$CORE_COUNT FORMAT Null" \
        >/dev/null
    docker exec "$CH_CONTAINER" clickhouse-benchmark \
        --iterations "$warm_iterations" \
        --concurrency 1 \
        --delay 0 \
        --query "$query SETTINGS use_query_cache=0,use_query_condition_cache=0,enable_filesystem_cache=0,max_threads=$CORE_COUNT FORMAT Null" \
        >"$RUN_DIR/clickhouse-${name}-warm.txt" 2>&1

    for iteration in $(seq 1 "$cold_iterations"); do
        drop_clickhouse_caches
        docker exec "$CH_CONTAINER" clickhouse-client \
            --query_id "cold-${name}-${RUN_ID}-${iteration}" \
            --query "$query SETTINGS use_query_cache=0,use_query_condition_cache=0,enable_filesystem_cache=0,max_threads=$CORE_COUNT FORMAT Null" \
            >/dev/null
    done
    docker exec "$CH_CONTAINER" clickhouse-client --query 'SYSTEM FLUSH LOGS'
    docker exec "$CH_CONTAINER" clickhouse-client --query "
SELECT query_id, query_duration_ms, read_rows, read_bytes, memory_usage
FROM system.query_log
WHERE type = 'QueryFinish'
  AND startsWith(query_id, 'cold-${name}-${RUN_ID}-')
ORDER BY query_id
FORMAT TSVWithNames
" >"$RUN_DIR/clickhouse-${name}-cold.tsv"
    docker exec "$CH_CONTAINER" clickhouse-client --query \
        "EXPLAIN indexes = 1 $query" >"$RUN_DIR/clickhouse-${name}-explain.txt"
}

run_pair() {
    local name=$1
    local warm_iterations=$2
    local cold_iterations=$3
    local clickhouse_query=$4
    shift 4
    run_shard_query "$name" "$warm_iterations" "$cold_iterations" "$@"
    run_clickhouse_query "$name" "$warm_iterations" "$cold_iterations" "$clickhouse_query"
}

run_pair latest "$WARM_ITERATIONS" "$COLD_ITERATIONS" \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs ORDER BY time DESC LIMIT 100"
run_pair stream "$WARM_ITERATIONS" "$COLD_ITERATIONS" \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE stream = 'stderr' ORDER BY time DESC LIMIT 100" \
    --field docker.stream=stderr
run_pair cannot "$WARM_ITERATIONS" "$COLD_ITERATIONS" \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE hasToken(log, 'cannot') ORDER BY time DESC LIMIT 100" \
    --term cannot
run_pair error-and "$WARM_ITERATIONS" "$COLD_ITERATIONS" \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE hasAllTokens(log, 'cannot exception file access error') ORDER BY time DESC LIMIT 100" \
    --term cannot --term exception --term file --term access --term error
run_pair term-miss "$WARM_ITERATIONS" "$COLD_ITERATIONS" \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE hasToken(log, 'shardtelemetrytermthatdoesnotexist') ORDER BY time DESC LIMIT 100" \
    --term shardtelemetrytermthatdoesnotexist
run_pair contains "$WARM_ITERATIONS" "$COLD_ITERATIONS" \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE positionCaseInsensitiveUTF8(log, 'Cannot log message') > 0 ORDER BY time DESC LIMIT 100" \
    --contains "Cannot log message"
run_pair regex "$WARM_ITERATIONS" "$COLD_ITERATIONS" \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE match(log, '^Cannot log message.*Poco::Exception') ORDER BY time DESC LIMIT 100" \
    --regex '^Cannot log message.*Poco::Exception'
run_pair contains-miss "$MISS_ITERATIONS" "$MISS_ITERATIONS" \
    "SELECT toUnixTimestamp64Nano(time),hex(stream),hex(log) FROM benchmark.logs WHERE positionCaseInsensitiveUTF8(log, 'shardtelemetrysubstringthatdoesnotexist') > 0 ORDER BY time DESC LIMIT 100" \
    --contains shardtelemetrysubstringthatdoesnotexist

docker exec "$CH_CONTAINER" clickhouse-client --query 'SYSTEM FLUSH LOGS'
docker exec "$CH_CONTAINER" clickhouse-client --query "
SELECT query_id, query_duration_ms, read_rows, read_bytes, result_rows, memory_usage, query
FROM system.query_log
WHERE event_time >= now() - INTERVAL 2 HOUR
  AND type = 'QueryFinish'
  AND (startsWith(query_id, 'cold-') OR query LIKE 'SELECT toUnixTimestamp64Nano%')
ORDER BY event_time_microseconds
FORMAT TSVWithNames
" >"$RUN_DIR/clickhouse-query-log.tsv"

{
    printf 'artifact\tsha256\n'
    for artifact in \
        provenance.txt \
        clickhouse-row-count.txt \
        clickhouse-query-log.tsv \
        shard-latest.txt \
        shard-stream.txt \
        shard-cannot.txt \
        shard-error-and.txt \
        shard-term-miss.txt \
        shard-contains.txt \
        shard-regex.txt \
        shard-contains-miss.txt; do
        printf '%s\t' "$artifact"
        sha256sum "$RUN_DIR/$artifact" | awk '{ print $1 }'
    done
} >"$RUN_DIR/checksums.tsv"

echo "results: $RUN_DIR"
