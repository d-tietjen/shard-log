#!/usr/bin/env bash
set -euo pipefail

SOURCE=${SOURCE:-/home/dtietjen/log-compression-samples/clickhouse-docker-json-error-loop-tail-80g-20260729.log}
EXPECTED_SHA256=${EXPECTED_SHA256:-4fd6379bd89fcb44688a3ebd611729416c82f110fbf49ffef905d9df0ebf0508}
EXPECTED_FILE_BYTES=${EXPECTED_FILE_BYTES:-85899345920}
LOADER_BIN=${LOADER_BIN:-target/release/shard-telemetry-loki-load}
LOKI_IMAGE=${LOKI_IMAGE:-sha256:191d4fdfb7264f16989f0a57f320872620a5a7c2ceeec6229212c4190ec49b86}
RESULT_ROOT=${RESULT_ROOT:-/home/dtietjen/shard-telemetry-loki-benchmarks}
RUN_ID=${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
CORE_COUNT=${CORE_COUNT:-16}
LOKI_PORT=${LOKI_PORT:-33100}
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
LOKI_CONFIG=${LOKI_CONFIG:-$SCRIPT_DIR/loki-benchmark.yaml}

for command in awk curl dd docker du lscpu sha256sum stat taskset; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 2
    }
done
[[ -f $SOURCE ]] || {
    echo "source does not exist: $SOURCE" >&2
    exit 2
}
[[ -x $LOADER_BIN ]] || {
    echo "Loki loader is not executable: $LOADER_BIN" >&2
    exit 2
}
[[ -f $LOKI_CONFIG ]] || {
    echo "Loki benchmark config does not exist: $LOKI_CONFIG" >&2
    exit 2
}
[[ $CORE_COUNT -eq 16 ]] || {
    echo "this comparison is fixed at 16 physical cores" >&2
    exit 2
}

SOURCE_BYTES=$(stat -c %s "$SOURCE")
[[ $SOURCE_BYTES -eq $EXPECTED_FILE_BYTES ]] || {
    echo "source size mismatch: got $SOURCE_BYTES, expected $EXPECTED_FILE_BYTES" >&2
    exit 2
}
SOURCE_SHA256=$(sha256sum "$SOURCE" | awk '{ print $1 }')
[[ $SOURCE_SHA256 == "$EXPECTED_SHA256" ]] || {
    echo "source SHA-256 mismatch: got $SOURCE_SHA256, expected $EXPECTED_SHA256" >&2
    exit 2
}
IMAGE_ID=$(docker image inspect "$LOKI_IMAGE" --format '{{.Id}}')
[[ $IMAGE_ID == "$LOKI_IMAGE" ]] || {
    echo "Loki image mismatch: got $IMAGE_ID, expected $LOKI_IMAGE" >&2
    exit 2
}

mapfile -t PHYSICAL_CPUS < <(
    lscpu -p=CPU,CORE |
        awk -F, '!/^#/ && !seen[$2]++ { print $1 }' |
        awk -v count="$CORE_COUNT" 'NR <= count'
)
[[ ${#PHYSICAL_CPUS[@]} -eq $CORE_COUNT ]] || {
    echo "found ${#PHYSICAL_CPUS[@]} physical cores, expected $CORE_COUNT" >&2
    exit 2
}
CPU_SET=$(IFS=,; echo "${PHYSICAL_CPUS[*]}")

RUN_DIR=$RESULT_ROOT/$RUN_ID
[[ ! -e $RUN_DIR ]] || {
    echo "result directory already exists: $RUN_DIR" >&2
    exit 2
}
mkdir -p "$RUN_DIR/loki-data"
exec > >(tee "$RUN_DIR/harness.log") 2>&1

CONTAINER="shard-telemetry-loki-${RUN_ID//[^a-zA-Z0-9_.-]/-}"
STARTED=0
cleanup() {
    if [[ $STARTED -eq 1 ]]; then
        docker logs "$CONTAINER" >"$RUN_DIR/loki-container.log" 2>&1 || true
        docker stop --time 60 "$CONTAINER" >/dev/null 2>&1 || true
        docker rm "$CONTAINER" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT INT TERM

{
    echo "run_id=$RUN_ID"
    echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "source=$SOURCE"
    echo "source_bytes=$SOURCE_BYTES"
    echo "source_sha256=$SOURCE_SHA256"
    echo "physical_cpu_set=$CPU_SET"
    echo "loader_binary=$LOADER_BIN"
    echo "loader_sha256=$(sha256sum "$LOADER_BIN" | awk '{ print $1 }')"
    echo "loki_image=$LOKI_IMAGE"
    echo "loki_image_id=$IMAGE_ID"
    echo "loki_config_sha256=$(sha256sum "$LOKI_CONFIG" | awk '{ print $1 }')"
    echo "kernel=$(uname -srmo)"
} >"$RUN_DIR/provenance.txt"

echo "Loki: starting $CONTAINER on CPUs $CPU_SET"
docker run -d \
    --name "$CONTAINER" \
    --cpuset-cpus "$CPU_SET" \
    --user "$(id -u):$(id -g)" \
    -p "127.0.0.1:$LOKI_PORT:3100" \
    -v "$RUN_DIR/loki-data:/loki" \
    -v "$LOKI_CONFIG:/etc/loki/benchmark.yaml:ro" \
    "$LOKI_IMAGE" \
    -config.file=/etc/loki/benchmark.yaml >"$RUN_DIR/container-id.txt"
STARTED=1

ready=0
for _ in $(seq 1 120); do
    if curl --fail --silent "http://127.0.0.1:$LOKI_PORT/ready" >/dev/null; then
        ready=1
        break
    fi
    sleep 1
done
[[ $ready -eq 1 ]] || {
    echo "Loki did not become ready" >&2
    exit 1
}

echo "Loki: prewarming $SOURCE_BYTES bytes"
prewarm_started=$(date +%s%N)
dd if="$SOURCE" of=/dev/null bs=64M status=none
prewarm_finished=$(date +%s%N)
awk -v elapsed_ns="$((prewarm_finished - prewarm_started))" \
    'BEGIN { printf "Loki source prewarm: %.6f seconds\n", elapsed_ns / 1000000000 }'

echo "Loki: ingesting on CPUs $CPU_SET"
taskset -c "$CPU_SET" "$LOADER_BIN" "$SOURCE" \
    --host 127.0.0.1 \
    --port "$LOKI_PORT" \
    --protocol loki \
    --workers "$CORE_COUNT" \
    --batch-bytes 1048576 \
    --tenant benchmark | tee "$RUN_DIR/loader.log"

curl --fail --silent --show-error -X POST \
    "http://127.0.0.1:$LOKI_PORT/flush" >"$RUN_DIR/flush-response.txt"
sleep 10
docker logs "$CONTAINER" >"$RUN_DIR/loki-container.log" 2>&1
docker stop --time 60 "$CONTAINER" >/dev/null
docker rm "$CONTAINER" >/dev/null
STARTED=0

SETTLED_BYTES=$(du -sb "$RUN_DIR/loki-data" | awk '{ print $1 }')
REPRESENTED_BYTES=$(awk -F': ' '$1 == "source bytes" { print $2 }' "$RUN_DIR/loader.log")
RECORDS=$(awk -F': ' '$1 == "records" { print $2 }' "$RUN_DIR/loader.log")
WIRE_BYTES=$(awk -F': ' '$1 == "pushed wire bytes" { print $2 }' "$RUN_DIR/loader.log")
ELAPSED=$(awk -F': ' '$1 == "ingest elapsed seconds" { print $2 }' "$RUN_DIR/loader.log")
THROUGHPUT=$(awk -F': ' '$1 == "source throughput MiB/s" { print $2 }' "$RUN_DIR/loader.log")
RATIO=$(awk -v source="$REPRESENTED_BYTES" -v stored="$SETTLED_BYTES" \
    'BEGIN { printf "%.2f", source / stored }')

{
    printf 'engine\tsource_bytes\trecords\twire_bytes\tsettled_bytes\telapsed_seconds\tthroughput_mib_s\tcompression_ratio\n'
    printf 'Loki\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$REPRESENTED_BYTES" "$RECORDS" "$WIRE_BYTES" "$SETTLED_BYTES" \
        "$ELAPSED" "$THROUGHPUT" "$RATIO"
} | tee "$RUN_DIR/summary.tsv"

find "$RUN_DIR/loki-data" -type f -printf '%s\t%p\n' | sort -nr >"$RUN_DIR/files.tsv"
echo "results: $RUN_DIR"
