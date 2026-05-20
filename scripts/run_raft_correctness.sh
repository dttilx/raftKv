#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONF_FILE="${CONF_FILE:-$ROOT_DIR/test.conf}"

echo "[raft-correctness] start cluster"
"$ROOT_DIR/scripts/cluster_start.sh" 3 "$CONF_FILE" "$ROOT_DIR/cluster.pids"

echo "[raft-correctness] basic consistency check (write deterministic keys then verify)"
"$ROOT_DIR/scripts/consistency_check.sh" "$CONF_FILE" 2000 2

echo "[raft-correctness] kill one node then re-check"
"$ROOT_DIR/scripts/fault_kill_one.sh" "$ROOT_DIR/cluster.pids"
sleep 2
"$ROOT_DIR/scripts/consistency_check.sh" "$CONF_FILE" 2000 1

echo "[raft-correctness] stop cluster"
"$ROOT_DIR/scripts/cluster_stop.sh" "$ROOT_DIR/cluster.pids"

echo "[raft-correctness] DONE"

