#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONF_FILE="${CONF_FILE:-$ROOT_DIR/test.conf}"

echo "[fault-network] start cluster"
"$ROOT_DIR/scripts/cluster_start.sh" 3 "$CONF_FILE" "$ROOT_DIR/cluster.pids"

echo "[fault-network] warmup workload"
"$ROOT_DIR/scripts/workload_bench.sh" "$CONF_FILE" 4 5 2000 32 50 || true

echo "[fault-network] apply 2-1 partition (needs sudo)"
sudo "$ROOT_DIR/scripts/net_partition_2_1.sh" apply "$CONF_FILE" 0 1 2

echo "[fault-network] run workload under partition (expect retries/latency changes, but no inconsistency)"
"$ROOT_DIR/scripts/workload_bench.sh" "$CONF_FILE" 6 10 5000 32 50 || true

echo "[fault-network] clear partition"
sudo "$ROOT_DIR/scripts/net_partition_2_1.sh" clear "$CONF_FILE"
sleep 2

echo "[fault-network] final consistency check"
"$ROOT_DIR/scripts/consistency_check.sh" "$CONF_FILE" 2000 1

echo "[fault-network] stop cluster"
"$ROOT_DIR/scripts/cluster_stop.sh" "$ROOT_DIR/cluster.pids"

echo "[fault-network] DONE"

