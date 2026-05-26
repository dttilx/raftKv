#!/usr/bin/env bash
# =============================================================================
# wait_ready.sh — 等待集群就绪
# 对 test.conf 中各节点端口做 TCP 探测（nc 或 /dev/tcp），全通后再等待选举稳定。
# 由 cluster_up.sh 调用；失败 exit 1。
# 环境变量：RAFTKV_WAIT_SECS（默认30）、RAFTKV_ELECTION_PAUSE（默认3秒）
# =============================================================================
set -euo pipefail
source "$(dirname "$0")/common.sh"

WAIT_SECS="${RAFTKV_WAIT_SECS:-30}"
ELECTION_PAUSE="${RAFTKV_ELECTION_PAUSE:-3}"

if [[ ! -f "$CONF" ]]; then
  echo "config not found: $CONF" >&2
  exit 1
fi

read_ports_from_conf "$CONF"

tcp_probe() {
  local host="$1"
  local port="$2"
  if command -v nc >/dev/null 2>&1; then
    nc -z "$host" "$port" 2>/dev/null
    return $?
  fi
  (echo >/dev/tcp/"$host"/"$port") >/dev/null 2>&1
}

deadline=$((SECONDS + WAIT_SECS))
while ((SECONDS < deadline)); do
  all_ok=1
  for port in "${PORTS[@]}"; do
    if ! tcp_probe 127.0.0.1 "$port"; then
      all_ok=0
      break
    fi
  done
  if [[ $all_ok -eq 1 ]]; then
    echo "all ${#PORTS[@]} ports listening: ${PORTS[*]}"
    sleep "$ELECTION_PAUSE"
    # 选举等待后再探一次：避免「短暂监听后子进程崩溃」仍报 cluster is ready
    for port in "${PORTS[@]}"; do
      if ! tcp_probe 127.0.0.1 "$port"; then
        echo "port $port closed after election pause; check $CLUSTER_LOG" >&2
        exit 1
      fi
    done
    child_count=0
    if [[ -f "$PID_FILE" ]]; then
      parent_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
      if [[ -n "${parent_pid:-}" ]]; then
        child_count="$(pgrep -P "$parent_pid" 2>/dev/null | wc -l | tr -d ' ')"
      fi
    fi
    if [[ "$child_count" -lt "${#PORTS[@]}" ]]; then
      echo "warning: raftCoreRun child count=$child_count (expected ${#PORTS[@]}); ports are OK, continuing" >&2
      echo "if bench hits gRPC code=14, run: ./scripts/diagnose_cluster.sh" >&2
    fi
    exit 0
  fi
  sleep 0.2
done

echo "timeout after ${WAIT_SECS}s; ports not ready: ${PORTS[*]}" >&2
exit 1
