#!/usr/bin/env bash
# =============================================================================
# check_cluster.sh — 检查三节点端口是否在监听
# 用法：./scripts/check_cluster.sh
# =============================================================================
set -euo pipefail
source "$(dirname "$0")/common.sh"

if [[ ! -f "$CONF" ]]; then
  echo "missing $CONF (run ./scripts/cluster_up.sh first)" >&2
  exit 1
fi

read_ports_from_conf "$CONF"
ok=1
for port in "${PORTS[@]}"; do
  if command -v nc >/dev/null 2>&1; then
    if nc -z 127.0.0.1 "$port" 2>/dev/null; then
      echo "OK  127.0.0.1:$port"
    else
      echo "FAIL 127.0.0.1:$port (not listening)" >&2
      ok=0
    fi
  else
    echo "skip TCP probe (nc missing); port $port" >&2
  fi
done

if [[ $ok -ne 1 ]]; then
  echo "hint: tail -50 $CLUSTER_LOG" >&2
  exit 1
fi
echo "all ${#PORTS[@]} ports OK"
