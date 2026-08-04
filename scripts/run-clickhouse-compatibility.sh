#!/usr/bin/env bash
set -euo pipefail

if [[ -z ${SHARD_TELEMETRY_CLICKHOUSE_TOKEN:-} ]]; then
    : "${SHARD_TELEMETRY_CLICKHOUSE_TOKEN_FILE:?set SHARD_TELEMETRY_CLICKHOUSE_TOKEN or SHARD_TELEMETRY_CLICKHOUSE_TOKEN_FILE}"
    [[ -f $SHARD_TELEMETRY_CLICKHOUSE_TOKEN_FILE ]] || {
        echo "token file does not exist: $SHARD_TELEMETRY_CLICKHOUSE_TOKEN_FILE" >&2
        exit 2
    }
    SHARD_TELEMETRY_CLICKHOUSE_TOKEN=$(<"$SHARD_TELEMETRY_CLICKHOUSE_TOKEN_FILE")
fi
[[ -n $SHARD_TELEMETRY_CLICKHOUSE_TOKEN ]] || {
    echo "ShardTelemetry ClickHouse token must not be empty" >&2
    exit 2
}

CLICKHOUSE_BIN=${CLICKHOUSE_BIN:-clickhouse}
CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-}
CLICKHOUSE_NETWORK=${CLICKHOUSE_NETWORK:-host}
SHARD_TELEMETRY_URL=${SHARD_TELEMETRY_URL:-http://127.0.0.1:3100/shardtelemetry/api/v1/clickhouse/scan}
SHARD_TELEMETRY_TENANT=${SHARD_TELEMETRY_TENANT:-fake}
EXPECTED_CLICKHOUSE_VERSION=${EXPECTED_CLICKHOUSE_VERSION:-26.3.17.56}
STRICT_CLICKHOUSE_VERSION=${STRICT_CLICKHOUSE_VERSION:-1}
SHARD_TELEMETRY_ADAPTER_MODE=${SHARD_TELEMETRY_ADAPTER_MODE:-0}

if [[ -n $CLICKHOUSE_IMAGE ]]; then
    command -v docker >/dev/null || {
        echo "CLICKHOUSE_IMAGE requires docker" >&2
        exit 2
    }
else
    command -v "$CLICKHOUSE_BIN" >/dev/null || {
        echo "missing ClickHouse binary: $CLICKHOUSE_BIN" >&2
        exit 2
    }
fi

run_clickhouse() {
    if [[ -n $CLICKHOUSE_IMAGE ]]; then
        docker run --rm --network "$CLICKHOUSE_NETWORK" -i "$CLICKHOUSE_IMAGE" clickhouse "$@"
    else
        "$CLICKHOUSE_BIN" "$@"
    fi
}

OBSERVED_CLICKHOUSE_VERSION=$(
    run_clickhouse local --version |
        awk '{ for (field = 1; field <= NF; field++) if ($field == "version") print $(field + 1) }'
)
if [[ $STRICT_CLICKHOUSE_VERSION -eq 1 && $OBSERVED_CLICKHOUSE_VERSION != "$EXPECTED_CLICKHOUSE_VERSION" ]]; then
    echo "ClickHouse version mismatch: expected $EXPECTED_CLICKHOUSE_VERSION, observed $OBSERVED_CLICKHOUSE_VERSION" >&2
    exit 2
fi

escape_sql() {
    local value=$1
    value=${value//\\/\\\\}
    value=${value//\'/\'\'}
    printf '%s' "$value"
}

URL_SQL=$(escape_sql "$SHARD_TELEMETRY_URL")
TOKEN_SQL=$(escape_sql "$SHARD_TELEMETRY_CLICKHOUSE_TOKEN")
TENANT_SQL=$(escape_sql "$SHARD_TELEMETRY_TENANT")
STRUCTURE="tenant String, timestamp DateTime64(9, 'UTC'), partition UInt32, offset UInt64, message String, labels Map(String, String), metadata Map(String, String)"
STRUCTURE_SQL=$(escape_sql "$STRUCTURE")
if [[ $SHARD_TELEMETRY_ADAPTER_MODE -eq 1 ]]; then
    SOURCE=shardtelemetry_source
    SOURCE_SETUP="CREATE TABLE shardtelemetry_source ($STRUCTURE) ENGINE = ShardTelemetry('$URL_SQL', 'ArrowStream', headers('Authorization' = 'Bearer $TOKEN_SQL', 'X-Scope-OrgID' = '$TENANT_SQL'));"
else
    SOURCE="url('$URL_SQL', 'ArrowStream', '$STRUCTURE_SQL', headers('Authorization' = 'Bearer $TOKEN_SQL', 'X-Scope-OrgID' = '$TENANT_SQL'))"
    SOURCE_SETUP=
fi

RESULT_DIR=$(mktemp -d "${TMPDIR:-/tmp}/shard-telemetry-clickhouse-compat.XXXXXX")
cleanup() {
    rm -rf -- "$RESULT_DIR"
}
trap cleanup EXIT INT TERM

queries=(
    "row-count|SELECT count() AS rows FROM __TABLE__"
    "group-map|SELECT labels['app'] AS app, count() AS rows FROM __TABLE__ GROUP BY app ORDER BY app"
    "aggregates|SELECT countIf(positionCaseInsensitive(message, 'error') > 0) AS errors, uniqExact(partition) AS partitions, quantileExact(lengthUTF8(message)) AS median_message FROM __TABLE__"
    "window|SELECT timestamp, partition, offset, row_number() OVER (PARTITION BY labels['app'] ORDER BY timestamp, partition, offset) AS row_number FROM __TABLE__ ORDER BY timestamp, partition, offset"
    "cte-array|WITH parsed AS (SELECT labels['app'] AS app, metadata['code'] AS code FROM __TABLE__) SELECT app, arraySort(groupArray(code)) AS codes FROM parsed GROUP BY app ORDER BY app"
    "self-join|SELECT count() AS matching_rows FROM __TABLE__ AS left_logs INNER JOIN __TABLE__ AS right_logs ON left_logs.partition = right_logs.partition AND left_logs.offset = right_logs.offset"
    "timestamp-map-filter|SELECT partition, offset, message FROM __TABLE__ WHERE timestamp >= toDateTime64(0, 9, 'UTC') AND timestamp < toDateTime64(4102444800, 9, 'UTC') AND labels['app'] = 'api' AND metadata['code'] = '500' ORDER BY partition, offset"
    "mixed-residual|SELECT partition, offset FROM __TABLE__ WHERE labels['app'] = 'api' AND positionCaseInsensitive(message, 'error') > 0 ORDER BY partition, offset"
    "disjunction|SELECT partition, offset FROM __TABLE__ WHERE labels['app'] = 'api' OR metadata['code'] = '200' ORDER BY partition, offset"
    "missing-map-key|SELECT labels['missing'] AS missing, count() AS rows FROM __TABLE__ GROUP BY missing ORDER BY missing"
    "missing-map-equality|SELECT count() AS rows FROM __TABLE__ WHERE labels['missing'] = ''"
    "alias-subquery|SELECT app, count() AS rows FROM (SELECT labels['app'] AS app, offset FROM __TABLE__ WHERE timestamp >= toDateTime64(0, 9, 'UTC')) GROUP BY app ORDER BY app"
    "aggregate-combinators|SELECT countIf(metadata['code'] = '500') AS failures, uniqExactIf(offset, labels['app'] = 'api') AS api_offsets FROM __TABLE__"
)

for entry in "${queries[@]}"; do
    name=${entry%%|*}
    query=${entry#*|}
    external_query=${query//__TABLE__/$SOURCE}
    reference_query=${query//__TABLE__/reference}
    external_output=$RESULT_DIR/$name.external
    reference_output=$RESULT_DIR/$name.reference

    {
        [[ -z $SOURCE_SETUP ]] || printf '%s\n' "$SOURCE_SETUP"
        printf '%s FORMAT JSONCompactEachRow\n' "$external_query"
    } | run_clickhouse local --multiquery >"$external_output"

    {
        [[ -z $SOURCE_SETUP ]] || printf '%s\n' "$SOURCE_SETUP"
        printf '%s\n' "CREATE TABLE reference ($STRUCTURE) ENGINE = Memory;"
        printf '%s\n' "INSERT INTO reference SELECT * FROM $SOURCE;"
        printf '%s FORMAT JSONCompactEachRow\n' "$reference_query"
    } | run_clickhouse local --multiquery >"$reference_output"

    if ! cmp -s "$external_output" "$reference_output"; then
        echo "compatibility mismatch: $name" >&2
        diff -u "$reference_output" "$external_output" >&2 || true
        exit 1
    fi
    echo "PASS $name"
done

if [[ $SHARD_TELEMETRY_ADAPTER_MODE -eq 1 ]]; then
    echo "StorageShardTelemetry compatibility smoke passed with $OBSERVED_CLICKHOUSE_VERSION"
else
    echo "ClickHouse compatibility smoke passed with $OBSERVED_CLICKHOUSE_VERSION"
fi
