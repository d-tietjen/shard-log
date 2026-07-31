#!/usr/bin/env bash
set -euo pipefail

: "${CLICKHOUSE_SOURCE:?set CLICKHOUSE_SOURCE to a pinned ClickHouse source checkout}"

EXPECTED_TAG=${EXPECTED_CLICKHOUSE_TAG:-v26.3.17.56-lts}
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
ADAPTER_DIR=$REPO_DIR/clickhouse/adapter

[[ -d $CLICKHOUSE_SOURCE/.git ]] || {
    echo "CLICKHOUSE_SOURCE is not a Git checkout: $CLICKHOUSE_SOURCE" >&2
    exit 2
}

OBSERVED_TAG=$(git -C "$CLICKHOUSE_SOURCE" describe --tags --exact-match 2>/dev/null || true)
if [[ $OBSERVED_TAG != "$EXPECTED_TAG" ]]; then
    echo "ClickHouse checkout mismatch: expected $EXPECTED_TAG, observed ${OBSERVED_TAG:-no exact tag}" >&2
    exit 2
fi

for adapter_file in StorageShardLog.h StorageShardLog.cpp; do
    source_file=$ADAPTER_DIR/$adapter_file
    target_file=$CLICKHOUSE_SOURCE/src/Storages/$adapter_file
    if [[ -e $target_file ]] && ! cmp -s "$source_file" "$target_file"; then
        echo "refusing to overwrite different adapter file: $target_file" >&2
        exit 2
    fi
done

if ! rg -q 'registerStorageShardLog' "$CLICKHOUSE_SOURCE/src/Storages/registerStorages.cpp"; then
    git -C "$CLICKHOUSE_SOURCE" apply --check "$ADAPTER_DIR/register-storage-shardlog.patch"
    git -C "$CLICKHOUSE_SOURCE" apply "$ADAPTER_DIR/register-storage-shardlog.patch"
fi

install -m 0644 "$ADAPTER_DIR/StorageShardLog.h" "$CLICKHOUSE_SOURCE/src/Storages/StorageShardLog.h"
install -m 0644 "$ADAPTER_DIR/StorageShardLog.cpp" "$CLICKHOUSE_SOURCE/src/Storages/StorageShardLog.cpp"

echo "Applied StorageShardLog to $CLICKHOUSE_SOURCE at $OBSERVED_TAG"
