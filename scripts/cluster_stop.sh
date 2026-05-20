#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="${1:-$ROOT_DIR/cluster.pids}"

if [[ ! -f "$PID_FILE" ]]; then
  echo "pidfile not found: $PID_FILE"
  echo "If needed, stop manually: pkill -f raftCoreRun"
  exit 1
fi

PIDS="$(cat "$PID_FILE" | tr '\n' ' ')"
echo "stopping pids: $PIDS"

set +e
kill -TERM $PIDS 2>/dev/null
sleep 1
kill -KILL $PIDS 2>/dev/null
set -e

rm -f "$PID_FILE"
echo "cluster stopped."

