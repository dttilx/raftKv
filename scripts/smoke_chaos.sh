#!/usr/bin/env bash
# =============================================================================
# smoke_chaos.sh — 故障冒烟：杀单节点后再做一致性校验
# 流程：cluster_up → consistency → kill 指定节点端口进程 → 等待 → consistency → down
# 用法：./scripts/smoke_chaos.sh
# 环境变量：RAFTKV_KILL_NODE=1（杀 node1 对应端口，默认 index 1）
#           RAFTKV_CHAOS_SLEEP=8、RAFTKV_CONSISTENCY_KEYS=300
# =============================================================================
set -euo pipefail
source "$(dirname "$0")/common.sh"

CONSISTENCY_KEYS="${RAFTKV_CONSISTENCY_KEYS:-300}"
KILL_NODE_INDEX="${RAFTKV_KILL_NODE:-1}"
CHAOS_SLEEP="${RAFTKV_CHAOS_SLEEP:-8}"

cleanup() {
  "$(dirname "$0")/cluster_down.sh" || true
}
trap cleanup EXIT

export RAFTKV_CLEAN_PERSIST=1
"$(dirname "$0")/cluster_up.sh"

echo "== consistency before chaos =="
"$RAFTKV_ROOT/bin/kv_consistency" -c "$CONF" -k "$CONSISTENCY_KEYS" -r 1

read_ports_from_conf "$CONF"
kill_port="${PORTS[$KILL_NODE_INDEX]}"
echo "== kill node index $KILL_NODE_INDEX (port $kill_port) =="

if command -v fuser >/dev/null 2>&1; then
  fuser -k "${kill_port}/tcp" 2>/dev/null || true
else
  pid="$(ss -tlnp 2>/dev/null | grep ":${kill_port} " | sed -n 's/.*pid=\([0-9]*\).*/\1/p' | head -n1 || true)"
  if [[ -n "${pid:-}" ]]; then
    kill -9 "$pid" 2>/dev/null || true
  else
    echo "could not find pid for port $kill_port" >&2
    exit 1
  fi
fi

sleep "$CHAOS_SLEEP"

echo "== consistency after chaos =="
"$RAFTKV_ROOT/bin/kv_consistency" -c "$CONF" -k "$CONSISTENCY_KEYS" -r 1

echo "smoke_chaos OK"
