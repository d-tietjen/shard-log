#!/usr/bin/env bash
set -euo pipefail

: "${CORE_COUNT:?CORE_COUNT must be set}"
: "${SOURCE_SKIP_BYTES:?SOURCE_SKIP_BYTES must be set}"
: "${SOURCE_BYTES:?SOURCE_BYTES must be set}"
: "${ALLOW_ERRORS_NUM:=0}"

dd if=/benchmark/input.json \
    bs=64M \
    iflag=skip_bytes,count_bytes \
    skip="$SOURCE_SKIP_BYTES" \
    count="$SOURCE_BYTES" \
    status=none |
    clickhouse-client \
        --query 'INSERT INTO benchmark.logs FORMAT JSONEachRow' \
        --max_threads="$CORE_COUNT" \
        --max_insert_threads="$CORE_COUNT" \
        --date_time_input_format=best_effort \
        --input_format_parallel_parsing=1 \
        --input_format_allow_errors_num="$ALLOW_ERRORS_NUM" \
        --input_format_allow_errors_ratio=1 \
        --input_format_record_errors_file_path=/var/lib/clickhouse/user_files/clickhouse-parse-errors.csv \
        --errors_output_format=CSV
