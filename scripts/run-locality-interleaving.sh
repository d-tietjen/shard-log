#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
    echo "usage: $0 RESULT_DIR INPUT1 INPUT2 [INPUT...]" >&2
    exit 2
fi

RESULT_DIR=$1
shift
INPUTS=("$@")
INTERLEAVE_BIN=${INTERLEAVE_BIN:-target/release/shard-log-interleave}
SHARD_LOG_BIN=${SHARD_LOG_BIN:-target/release/shard-log-structural-bench}
CPU_SET=${CPU_SET:-0-15}
WORKERS=${WORKERS:-16}
BLOCK_BYTES=${BLOCK_BYTES:-8MiB}
LIMIT_BYTES=${LIMIT_BYTES:-85899345920}

[[ ! -e $RESULT_DIR ]] || {
    echo "result directory already exists: $RESULT_DIR" >&2
    exit 2
}
[[ -x $INTERLEAVE_BIN ]] || {
    echo "interleave binary is not executable: $INTERLEAVE_BIN" >&2
    exit 2
}
[[ -x $SHARD_LOG_BIN ]] || {
    echo "structural benchmark binary is not executable: $SHARD_LOG_BIN" >&2
    exit 2
}
for input in "${INPUTS[@]}"; do
    [[ -f $input ]] || {
        echo "input does not exist: $input" >&2
        exit 2
    }
done

mkdir -p "$RESULT_DIR"
INTERLEAVED=$RESULT_DIR/interleaved-docker-json.log
"$INTERLEAVE_BIN" "$INTERLEAVED" --limit-bytes "$LIMIT_BYTES" "${INPUTS[@]}" \
    >"$RESULT_DIR/interleave-report.txt"

{
    printf 'cpu_set=%s\nworkers=%s\nblock_bytes=%s\nlimit_bytes=%s\n' \
        "$CPU_SET" "$WORKERS" "$BLOCK_BYTES" "$LIMIT_BYTES"
    sha256sum "$INTERLEAVED"
    for input in "${INPUTS[@]}"; do
        sha256sum "$input"
    done
} >"$RESULT_DIR/provenance.txt"

for mode in disabled enabled; do
    dd if="$INTERLEAVED" of=/dev/null bs=64M status=none
    /usr/bin/time -f 'wall_seconds=%e\nuser_seconds=%U\nsystem_seconds=%S\nmax_rss_kib=%M' \
        -o "$RESULT_DIR/${mode}-time.txt" \
        taskset -c "$CPU_SET" "$SHARD_LOG_BIN" "$INTERLEAVED" \
        --limit-bytes "$(stat -c %s "$INTERLEAVED")" \
        --block-bytes "$BLOCK_BYTES" \
        --workers "$WORKERS" \
        --locality "$mode" \
        --output-dir "$RESULT_DIR/${mode}-packs" \
        --report "$RESULT_DIR/${mode}-report.txt"
done

{
    printf 'mode\tsource_bytes\tstored_bytes\telapsed_seconds\tthroughput_mib_s\tcompression_ratio\tfallback_rate\n'
    for mode in disabled enabled; do
        awk -F': ' -v mode="$mode" '
            $1 == "source bytes" { source = $2 }
            $1 == "durable pack plus manifest" { split($2, value, " "); stored = value[1] }
            $1 == "ingest elapsed seconds" { elapsed = $2 }
            $1 == "locality fallback rate" { fallback = $2 }
            END {
                printf "%s\t%.0f\t%.0f\t%.6f\t%.2f\t%.2f\t%.6f\n",
                    mode, source, stored, elapsed, source / 1048576 / elapsed,
                    source / stored, fallback
            }
        ' "$RESULT_DIR/${mode}-report.txt"
    done
} >"$RESULT_DIR/summary.tsv"

cat "$RESULT_DIR/summary.tsv"
