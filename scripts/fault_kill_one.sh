#!/usr/bin/env bash
set -euo pipefail

# Kill one raftCoreRun child process to simulate 1-node failure.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="${1:-$ROOT_DIR/cluster.pids}"

if [[ ! -f "$PID_FILE" ]]; then
  echo "pidfile not found: $PID_FILE"
  exit 1
fi

PIDS=($(cat "$PID_FILE"))
if [[ "${#PIDS[@]}" -lt 2 ]]; then
  echo "not enough pids in pidfile (need parent+children)."
  exit 1
fi

# Prefer killing a child (not the first one, which is likely the parent setsid process)
TARGET="${PIDS[1]}"
echo "killing one node pid=$TARGET"
kill -KILL "$TARGET"

