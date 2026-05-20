#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONF_FILE="${CONF_FILE:-$ROOT_DIR/test.conf}"

rm -f "$ROOT_DIR"/snapshotPersist*.txt "$ROOT_DIR"/raftstatePersist*.txt || true

echo "[snapshot] start cluster"
"$ROOT_DIR/scripts/cluster_start.sh" 3 "$CONF_FILE" "$ROOT_DIR/cluster.pids"

echo "[snapshot] write enough data to trigger snapshots (maxraftstate=500, threshold ~50 bytes-ish)"
"$ROOT_DIR/scripts/workload_bench.sh" "$CONF_FILE" 8 15 20000 128 100 || true

echo "[snapshot] check snapshot files"
ls -lh "$ROOT_DIR"/snapshotPersist*.txt || true

echo "[snapshot] stop cluster"
"$ROOT_DIR/scripts/cluster_stop.sh" "$ROOT_DIR/cluster.pids"

echo "[snapshot] restart cluster (recovery from snapshot/raftstate)"
"$ROOT_DIR/scripts/cluster_start.sh" 3 "$CONF_FILE" "$ROOT_DIR/cluster.pids"
sleep 2

echo "[snapshot] verify deterministic keys after restart"
"$ROOT_DIR/scripts/consistency_check.sh" "$CONF_FILE" 2000 1

echo "[snapshot] stop cluster"
"$ROOT_DIR/scripts/cluster_stop.sh" "$ROOT_DIR/cluster.pids"

echo "[snapshot] DONE"

