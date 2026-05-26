#!/usr/bin/env bash
# =============================================================================
# diagnose_cluster.sh — 集群 / gRPC code=14 一键诊断（不启动、不停止集群）
# 用法：./scripts/diagnose_cluster.sh
# 日志：logs/cluster.log（nohup 输出）；默认不用 journalctl（非 systemd 服务）
# =============================================================================
set -euo pipefail
source "$(dirname "$0")/common.sh"

echo "========== raftKv cluster diagnose =========="
echo "time: $(date -Is 2>/dev/null || date)"
echo "root: $RAFTKV_ROOT"
echo

if [[ -f "$PID_FILE" ]]; then
  echo "[pid file] $PID_FILE -> $(cat "$PID_FILE" 2>/dev/null || echo '?')"
else
  echo "[pid file] missing (cluster not started via cluster_up?)"
fi
echo

echo "[raftCoreRun processes]"
pgrep -a raftCoreRun 2>/dev/null || echo "(none)"
echo

if [[ -f "$PID_FILE" ]]; then
  parent_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${parent_pid:-}" ]] && kill -0 "$parent_pid" 2>/dev/null; then
  children="$(pgrep -P "$parent_pid" 2>/dev/null | tr '\n' ' ' || true)"
  child_n="$(pgrep -P "$parent_pid" 2>/dev/null | wc -l | tr -d ' ')"
  echo "[parent $parent_pid] child count=$child_n pids: ${children:-none}"
  else
  echo "[parent] not running (stale .cluster.pid?)"
  fi
fi
echo

if [[ -f "$CONF" ]]; then
  read_ports_from_conf "$CONF"
  echo "[test.conf ports] ${PORTS[*]}"
  echo "[TCP listen]"
  for port in "${PORTS[@]}"; do
    if command -v ss >/dev/null 2>&1; then
      line="$(ss -tlnp 2>/dev/null | grep ":${port} " || true)"
      if [[ -n "$line" ]]; then
        echo "  OK  :$port  $line"
      else
        echo "  FAIL :$port  (not listening)"
      fi
    elif command -v nc >/dev/null 2>&1; then
      if nc -z 127.0.0.1 "$port" 2>/dev/null; then
        echo "  OK  :$port (nc)"
      else
        echo "  FAIL :$port (nc)"
      fi
    fi
  done
else
  echo "[test.conf] missing: $CONF"
fi
echo

echo "[persist files]"
ls -la "$RAFTKV_ROOT"/raftstatePersist*.txt "$RAFTKV_ROOT"/snapshotPersist*.txt 2>/dev/null || echo "(none)"
echo

if [[ -f "$CLUSTER_LOG" ]]; then
  echo "[cluster.log last 40 lines]"
  tail -n 40 "$CLUSTER_LOG"
else
  echo "[cluster.log] missing: $CLUSTER_LOG"
fi
echo

echo "========== hints =========="
echo "- gRPC code=14 = Clerk 连不上 KV 端口（进程未监听或已崩溃）"
echo "- 健康集群: ss 上应有 19001/19002/19003 三行 LISTEN，且 parent 下 child count=3"
echo "- 修复: ./scripts/cluster_down.sh && ./scripts/cluster_up.sh"
echo "- 详细日志: tail -f logs/cluster.log  （不是 journalctl，除非你自己做了 systemd 单元）"
