#!/usr/bin/env bash
set -euo pipefail

SOURCE=${SOURCE:-/home/dtietjen/log-compression-samples/clickhouse-docker-json-error-loop-tail-80g-20260729.log}
EXPECTED_SHA256=${EXPECTED_SHA256:-4fd6379bd89fcb44688a3ebd611729416c82f110fbf49ffef905d9df0ebf0508}
EXPECTED_FILE_BYTES=${EXPECTED_FILE_BYTES:-85899345920}
SHARD_TELEMETRY_BIN=${SHARD_TELEMETRY_BIN:-/home/dtietjen/shard-telemetry-target-20260729/release/shard-telemetry-structural-bench}
LOCALITY_MODE=${LOCALITY_MODE:-disabled}
RESULT_ROOT=${RESULT_ROOT:-/home/dtietjen/shard-telemetry-head-to-head}
RUN_ID=${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
CORE_COUNT=${CORE_COUNT:-16}
BLOCK_BYTES=${BLOCK_BYTES:-8MiB}
CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-sha256:770156c537ca9124046e138a3b5845c64ea58ce8722de7a2e05fd827f4976520}
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CONFIG=$SCRIPT_DIR/clickhouse-benchmark.xml

[[ -x $SHARD_TELEMETRY_BIN ]] || {
    echo "ShardTelemetry binary is not executable: $SHARD_TELEMETRY_BIN" >&2
    exit 2
}
[[ $LOCALITY_MODE == enabled || $LOCALITY_MODE == disabled ]] || {
    echo "LOCALITY_MODE must be enabled or disabled, got: $LOCALITY_MODE" >&2
    exit 2
}

if [[ $CORE_COUNT -ne 16 ]]; then
    echo "this comparison is fixed at 16 physical cores" >&2
    exit 2
fi
for command in awk dd docker lscpu sha256sum stat taskset; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 2
    }
done
[[ -f $SOURCE ]] || {
    echo "source does not exist: $SOURCE" >&2
    exit 2
}
[[ -f $CLICKHOUSE_CONFIG ]] || {
    echo "ClickHouse benchmark config does not exist: $CLICKHOUSE_CONFIG" >&2
    exit 2
}

mapfile -t PHYSICAL_CPUS < <(
    lscpu -p=CPU,CORE |
        awk -F, '!/^#/ && !seen[$2]++ { print $1 }' |
        awk -v count="$CORE_COUNT" 'NR <= count'
)
if [[ ${#PHYSICAL_CPUS[@]} -ne $CORE_COUNT ]]; then
    echo "found ${#PHYSICAL_CPUS[@]} physical cores, expected $CORE_COUNT" >&2
    exit 2
fi
CPU_SET=$(IFS=,; echo "${PHYSICAL_CPUS[*]}")
CONTAINER_UID=$(id -u)
CONTAINER_GID=$(id -g)

RUN_DIR=$RESULT_ROOT/$RUN_ID
[[ ! -e $RUN_DIR ]] || {
    echo "result directory already exists: $RUN_DIR" >&2
    exit 2
}
mkdir -p "$RUN_DIR"
exec > >(tee "$RUN_DIR/harness.log") 2>&1

CH_CONTAINER="shard-telemetry-ch-${RUN_ID//[^a-zA-Z0-9_.-]/-}"
CH_STARTED=0
cleanup() {
    if [[ $CH_STARTED -eq 1 ]]; then
        docker logs "$CH_CONTAINER" >"$RUN_DIR/clickhouse-container.log" 2>&1 || true
        docker stop --time 60 "$CH_CONTAINER" >/dev/null 2>&1 || true
        docker rm "$CH_CONTAINER" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT INT TERM

SOURCE_BYTES=$(stat -c %s "$SOURCE")
if [[ -n $EXPECTED_FILE_BYTES && $SOURCE_BYTES -ne $EXPECTED_FILE_BYTES ]]; then
    echo "source size mismatch: got $SOURCE_BYTES, expected $EXPECTED_FILE_BYTES" >&2
    exit 2
fi
SOURCE_SHA256=$(sha256sum "$SOURCE" | awk '{ print $1 }')
if [[ -n $EXPECTED_SHA256 && $SOURCE_SHA256 != "$EXPECTED_SHA256" ]]; then
    echo "source SHA-256 mismatch: got $SOURCE_SHA256, expected $EXPECTED_SHA256" >&2
    exit 2
fi
IMAGE_ID=$(docker image inspect "$CLICKHOUSE_IMAGE" --format '{{.Id}}')
if [[ $IMAGE_ID != "$CLICKHOUSE_IMAGE" ]]; then
    echo "ClickHouse image mismatch: got $IMAGE_ID, expected $CLICKHOUSE_IMAGE" >&2
    exit 2
fi

{
    echo "run_id=$RUN_ID"
    echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "source=$SOURCE"
    echo "source_bytes=$SOURCE_BYTES"
    echo "source_sha256=$SOURCE_SHA256"
    echo "physical_cpu_set=$CPU_SET"
    echo "clickhouse_container_uid_gid=$CONTAINER_UID:$CONTAINER_GID"
    echo "core_count=$CORE_COUNT"
    echo "block_bytes=$BLOCK_BYTES"
    echo "shard_telemetry_binary=$SHARD_TELEMETRY_BIN"
    echo "shard_telemetry_binary_sha256=$(sha256sum "$SHARD_TELEMETRY_BIN" | awk '{ print $1 }')"
    echo "shard_telemetry_locality=$LOCALITY_MODE"
    echo "clickhouse_image=$CLICKHOUSE_IMAGE"
    echo "clickhouse_image_id=$IMAGE_ID"
    echo "clickhouse_source_mode=skip-first-line"
    echo "kernel=$(uname -srmo)"
    docker version --format 'docker_server={{.Server.Version}}'
    lscpu
} >"$RUN_DIR/provenance.txt"

prewarm_source() {
    local engine=$1
    local started ended
    started=$(date +%s%N)
    dd if="$SOURCE" of=/dev/null bs=64M status=none
    ended=$(date +%s%N)
    awk -v engine="$engine" -v elapsed_ns="$((ended - started))" \
        'BEGIN { printf "%s source prewarm: %.6f seconds\n", engine, elapsed_ns / 1000000000 }'
}

run_shard_telemetry() {
    echo "ShardTelemetry: prewarming $SOURCE_BYTES bytes"
    prewarm_source ShardTelemetry
    echo "ShardTelemetry: ingesting on CPUs $CPU_SET"
    /usr/bin/time -f 'wall_seconds=%e\nuser_seconds=%U\nsystem_seconds=%S\nmax_rss_kib=%M' \
        -o "$RUN_DIR/shard-telemetry-time.txt" \
        taskset -c "$CPU_SET" "$SHARD_TELEMETRY_BIN" "$SOURCE" \
        --limit-bytes "$SOURCE_BYTES" \
        --block-bytes "$BLOCK_BYTES" \
        --workers "$CORE_COUNT" \
        --locality "$LOCALITY_MODE" \
        --output-dir "$RUN_DIR/shard-telemetry-packs" \
        --report "$RUN_DIR/shard-telemetry-report.txt"
}

run_shard_telemetry

REFERENCE_REPORT=$RUN_DIR/shard-telemetry-report.txt
SHARD_RECORDS=$(awk -F': ' '$1 == "records" { print $2 }' "$REFERENCE_REPORT")
SHARD_REJECTED_RECORDS=$(awk -F': ' '$1 == "rejected complete records" { print $2 }' "$REFERENCE_REPORT")
SHARD_SOURCE_BYTES=$(awk -F': ' '$1 == "source bytes" { print $2 }' "$REFERENCE_REPORT")

CH_DATA=$RUN_DIR/clickhouse-data
CH_LOGS=$RUN_DIR/clickhouse-logs
mkdir -p "$CH_DATA" "$CH_LOGS"
echo "ClickHouse: starting isolated container $CH_CONTAINER on CPUs $CPU_SET"
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
docker exec "$CH_CONTAINER" clickhouse-client --query 'SELECT 1' >/dev/null
docker exec "$CH_CONTAINER" clickhouse-client --query 'SELECT version()' \
    >"$RUN_DIR/clickhouse-version.txt"

docker exec "$CH_CONTAINER" clickhouse-client --multiquery --query "
CREATE DATABASE benchmark;
CREATE TABLE benchmark.logs
(
    time DateTime64(9, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    stream LowCardinality(String) CODEC(ZSTD(1)),
    log String CODEC(ZSTD(1))
)
ENGINE = MergeTree
ORDER BY (stream, time)
SETTINGS index_granularity = 8192, fsync_after_insert = 1;
"

echo "ClickHouse: prewarming $SOURCE_BYTES bytes"
prewarm_source ClickHouse
echo "ClickHouse: ingesting on CPUs $CPU_SET"
/usr/bin/time -f 'wall_seconds=%e\nuser_seconds=%U\nsystem_seconds=%S\nmax_rss_kib=%M' \
    -o "$RUN_DIR/clickhouse-time.txt" \
    docker exec \
    --env CORE_COUNT="$CORE_COUNT" \
    --env ALLOWED_ERRORS="$SHARD_REJECTED_RECORDS" \
    "$CH_CONTAINER" \
    /bin/bash /benchmark-scripts/clickhouse-ingest-skip-first-line.sh

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
SELECT count() FROM benchmark.logs
" >"$RUN_DIR/clickhouse-row-count.txt"

CH_RECORDS=$(tr -d '[:space:]' <"$RUN_DIR/clickhouse-row-count.txt")
CH_STORED_BYTES=$(awk -F'\t' 'NR == 2 { print $2 }' "$RUN_DIR/clickhouse-parts.tsv")
CH_ELAPSED=$(awk -F= '$1 == "wall_seconds" { print $2 }' "$RUN_DIR/clickhouse-time.txt")

if [[ $SHARD_RECORDS -ne $CH_RECORDS ]]; then
    echo "record-count mismatch: ShardTelemetry=$SHARD_RECORDS ClickHouse=$CH_RECORDS" >&2
    exit 1
fi

{
    printf 'engine\tsource_bytes\trecords\tstored_bytes\telapsed_seconds\tthroughput_mib_s\tcompression_ratio\n'
    shard_stored_bytes=$(awk -F': ' '$1 == "durable pack plus manifest" {
        split($2, value, " "); print value[1]
    }' "$REFERENCE_REPORT")
    shard_elapsed=$(awk -F': ' '$1 == "ingest elapsed seconds" { print $2 }' "$REFERENCE_REPORT")
    awk -v source="$SHARD_SOURCE_BYTES" -v records="$SHARD_RECORDS" \
        -v stored="$shard_stored_bytes" -v elapsed="$shard_elapsed" \
        'BEGIN { printf "ShardTelemetry\t%.0f\t%.0f\t%.0f\t%.6f\t%.2f\t%.2f\n",
            source, records, stored, elapsed,
            source / 1048576 / elapsed, source / stored }'
    awk -v source="$SHARD_SOURCE_BYTES" -v records="$CH_RECORDS" \
        -v stored="$CH_STORED_BYTES" -v elapsed="$CH_ELAPSED" \
        'BEGIN { printf "ClickHouse\t%.0f\t%.0f\t%.0f\t%.6f\t%.2f\t%.2f\n",
            source, records, stored, elapsed, source / 1048576 / elapsed, source / stored }'
} >"$RUN_DIR/summary.tsv"

cat "$RUN_DIR/summary.tsv"
echo "results: $RUN_DIR"
