#!/usr/bin/env bash
# =============================================================================
# fault_inject.sh — 故障注入/恢复工具
# 场景：
#   - kill-restart : kill 指定节点后等待并重启整个集群
#   - partition    : 使用 iptables 隔离一个节点端口（需要 root）
#   - delay-loss   : 使用 tc 注入回环网卡时延/丢包（需要 root）
# 用法示例：
#   ./scripts/fault_inject.sh --scenario kill-restart --node 1 --sleep 6
#   sudo ./scripts/fault_inject.sh --scenario partition --node 1 --action inject
#   sudo ./scripts/fault_inject.sh --scenario partition --node 1 --action recover
# =============================================================================
set -euo pipefail
source "$(dirname "$0")/common.sh"

SCENARIO=""
ACTION="inject"
NODE_INDEX=1
SLEEP_SECS=6
DELAY_MS=120
LOSS_PERCENT=8
CONF_FILE="$CONF"

usage() {
  cat <<'EOF'
Usage: fault_inject.sh --scenario <kill-restart|partition|delay-loss> [options]

Options:
  --action <inject|recover>  inject/recover (default: inject)
  --node <index>             node index for partition/kill-restart (default: 1)
  --sleep <secs>             sleep seconds for kill-restart (default: 6)
  --delay-ms <ms>            delay for delay-loss (default: 120)
  --loss-pct <pct>           loss percent for delay-loss (default: 8)
  --conf <path>              config file (default: test.conf)
  -h, --help                 show help
EOF
}

require_root() {
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    echo "scenario requires root; rerun with sudo" >&2
    exit 1
  fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --scenario)
        SCENARIO="${2:-}"
        shift 2
        ;;
      --action)
        ACTION="${2:-}"
        shift 2
        ;;
      --node)
        NODE_INDEX="${2:-}"
        shift 2
        ;;
      --sleep)
        SLEEP_SECS="${2:-}"
        shift 2
        ;;
      --delay-ms)
        DELAY_MS="${2:-}"
        shift 2
        ;;
      --loss-pct)
        LOSS_PERCENT="${2:-}"
        shift 2
        ;;
      --conf)
        CONF_FILE="${2:-}"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "unknown arg: $1" >&2
        usage
        exit 2
        ;;
    esac
  done
  if [[ -z "$SCENARIO" ]]; then
    echo "--scenario is required" >&2
    usage
    exit 2
  fi
}

read_node_port() {
  read_ports_from_conf "$CONF_FILE"
  if [[ "$NODE_INDEX" -lt 0 || "$NODE_INDEX" -ge "${#PORTS[@]}" ]]; then
    echo "node index out of range: $NODE_INDEX" >&2
    exit 2
  fi
  TARGET_PORT="${PORTS[$NODE_INDEX]}"
}

kill_by_port() {
  local port="$1"
  if command -v fuser >/dev/null 2>&1; then
    fuser -k "${port}/tcp" 2>/dev/null || true
    return 0
  fi
  local pid
  pid="$(ss -tlnp 2>/dev/null | sed -n "s/.*:${port} .*pid=\([0-9]*\).*/\1/p" | head -n1 || true)"
  if [[ -n "${pid:-}" ]]; then
    kill -9 "$pid" 2>/dev/null || true
  fi
}

inject_kill_restart() {
  read_node_port
  echo "inject kill-restart node=$NODE_INDEX port=$TARGET_PORT sleep=${SLEEP_SECS}s"
  kill_by_port "$TARGET_PORT"
  sleep "$SLEEP_SECS"
  echo "restart whole cluster to restore target node"
  "$(dirname "$0")/cluster_down.sh"
  export RAFTKV_CLEAN_PERSIST=0
  "$(dirname "$0")/cluster_up.sh"
}

inject_partition() {
  require_root
  read_node_port
  echo "inject partition on loopback port=$TARGET_PORT"
  iptables -I OUTPUT -p tcp --dport "$TARGET_PORT" -j DROP
  iptables -I INPUT -p tcp --sport "$TARGET_PORT" -j DROP
}

recover_partition() {
  require_root
  read_node_port
  echo "recover partition on loopback port=$TARGET_PORT"
  iptables -D OUTPUT -p tcp --dport "$TARGET_PORT" -j DROP 2>/dev/null || true
  iptables -D INPUT -p tcp --sport "$TARGET_PORT" -j DROP 2>/dev/null || true
}

inject_delay_loss() {
  require_root
  echo "inject delay-loss on lo delay=${DELAY_MS}ms loss=${LOSS_PERCENT}%"
  tc qdisc replace dev lo root netem delay "${DELAY_MS}ms" loss "${LOSS_PERCENT}%"
}

recover_delay_loss() {
  require_root
  echo "recover delay-loss on lo"
  tc qdisc del dev lo root 2>/dev/null || true
}

main() {
  parse_args "$@"
  case "$SCENARIO" in
    kill-restart)
      if [[ "$ACTION" != "inject" ]]; then
        echo "kill-restart supports only --action inject" >&2
        exit 2
      fi
      inject_kill_restart
      ;;
    partition)
      if [[ "$ACTION" == "inject" ]]; then
        inject_partition
      elif [[ "$ACTION" == "recover" ]]; then
        recover_partition
      else
        echo "invalid --action $ACTION" >&2
        exit 2
      fi
      ;;
    delay-loss)
      if [[ "$ACTION" == "inject" ]]; then
        inject_delay_loss
      elif [[ "$ACTION" == "recover" ]]; then
        recover_delay_loss
      else
        echo "invalid --action $ACTION" >&2
        exit 2
      fi
      ;;
    *)
      echo "unsupported scenario: $SCENARIO" >&2
      exit 2
      ;;
  esac
}

main "$@"
