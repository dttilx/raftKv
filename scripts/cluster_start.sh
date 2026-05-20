#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT_DIR/bin/raftCoreRun"

NODES="${1:-3}"
CONF_FILE="${2:-$ROOT_DIR/test.conf}"
PID_FILE="${3:-$ROOT_DIR/cluster.pids}"

if [[ ! -x "$BIN" ]]; then
  echo "missing executable: $BIN"
  echo "run: $ROOT_DIR/scripts/build.sh"
  exit 1
fi

rm -f "$PID_FILE"

echo "starting cluster: nodes=$NODES conf=$CONF_FILE"

set +e
setsid "$BIN" -n "$NODES" -f "$CONF_FILE" >"$ROOT_DIR/cluster.out" 2>&1 &
PARENT_PID=$!
set -e

sleep 1

echo "$PARENT_PID" >>"$PID_FILE"
for _ in $(seq 1 50); do
  CHILDREN="$(pgrep -P "$PARENT_PID" || true)"
  if [[ -n "$CHILDREN" ]]; then
    while read -r cpid; do
      [[ -n "$cpid" ]] && echo "$cpid" >>"$PID_FILE"
    done <<<"$CHILDREN"
    break
  fi
  sleep 0.1
done

sort -u "$PID_FILE" -o "$PID_FILE"
echo "cluster started. pidfile=$PID_FILE"
echo "tail logs: tail -f $ROOT_DIR/cluster.out"

