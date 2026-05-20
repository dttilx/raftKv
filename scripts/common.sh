#!/usr/bin/env bash
# Shared helpers for cluster scripts.
set -euo pipefail

RAFTKV_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$RAFTKV_ROOT"

CONF="${RAFTKV_CONF:-test.conf}"
FIXED_CONF="${RAFTKV_FIXED_CONF:-deploy/test.conf.fixed}"
PID_FILE="${RAFTKV_PID_FILE:-.cluster.pid}"
LOG_DIR="${RAFTKV_LOG_DIR:-logs}"
CLUSTER_LOG="${LOG_DIR}/cluster.log"

read_ports_from_conf() {
  local conf="$1"
  PORTS=()
  local i=0
  while true; do
    local key="node${i}port"
    local line
    line="$(grep -E "^${key}=" "$conf" 2>/dev/null | head -n1 || true)"
    if [[ -z "$line" ]]; then
      break
    fi
    PORTS+=("${line#*=}")
    i=$((i + 1))
  done
  if [[ ${#PORTS[@]} -eq 0 ]]; then
    echo "no node*port entries in $conf" >&2
    return 1
  fi
}
