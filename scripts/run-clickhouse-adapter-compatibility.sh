#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
export SHARDLOG_ADAPTER_MODE=1
exec "$SCRIPT_DIR/run-clickhouse-compatibility.sh" "$@"
