#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    sleep 0.5
    kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$PID_FILE"
fi

pkill -f '[/]bin/raftCoreRun' 2>/dev/null || true
pkill -f 'raftCoreRun' 2>/dev/null || true
sleep 0.5

if [[ -f "$CONF" ]]; then
  read_ports_from_conf "$CONF" || true
  for port in "${PORTS[@]:-}"; do
    if command -v fuser >/dev/null 2>&1; then
      fuser -k "${port}/tcp" 2>/dev/null || true
    fi
  done
fi

echo "cluster stopped"
