#!/usr/bin/env bash
set -euo pipefail

: "${CORE_COUNT:?CORE_COUNT must be set}"
: "${ALLOWED_ERRORS:?ALLOWED_ERRORS must be set}"

tail -n +2 /benchmark/input.json |
    clickhouse-client \
        --query 'INSERT INTO benchmark.logs FORMAT JSONEachRow' \
        --max_threads="$CORE_COUNT" \
        --max_insert_threads="$CORE_COUNT" \
        --date_time_input_format=best_effort \
        --input_format_parallel_parsing=1 \
        --input_format_allow_errors_num="$ALLOWED_ERRORS" \
        --input_format_allow_errors_ratio=0
