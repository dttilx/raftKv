#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONF_FILE="${CONF_FILE:-$ROOT_DIR/test.conf}"

echo "[perf] start cluster"
"$ROOT_DIR/scripts/cluster_start.sh" 3 "$CONF_FILE" "$ROOT_DIR/cluster.pids"

echo "[perf] record perf + generate flamegraph (needs sudo)"
"$ROOT_DIR/scripts/perf_flamegraph.sh" "$CONF_FILE" "$ROOT_DIR/perf-out"

echo "[perf] stop cluster"
"$ROOT_DIR/scripts/cluster_stop.sh" "$ROOT_DIR/cluster.pids"

echo "[perf] DONE"

