#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

CLEAN_PERSIST="${RAFTKV_CLEAN_PERSIST:-0}"
NODE_NUM="${RAFTKV_NODE_NUM:-3}"

if [[ ! -x "$RAFTKV_ROOT/bin/raftCoreRun" ]]; then
  echo "bin/raftCoreRun not found; build first: cmake -S . -B cmake-build && cmake --build cmake-build -j" >&2
  exit 1
fi

"$(dirname "$0")/cluster_down.sh" || true

if [[ "$CLEAN_PERSIST" == "1" ]]; then
  rm -f "$RAFTKV_ROOT"/raftstatePersist*.txt "$RAFTKV_ROOT"/snapshotPersist*.txt
fi

if [[ ! -f "$FIXED_CONF" ]]; then
  echo "fixed config missing: $FIXED_CONF" >&2
  exit 1
fi
cp "$FIXED_CONF" "$CONF"
echo "installed config: $CONF from $FIXED_CONF"

mkdir -p "$LOG_DIR"
nohup "$RAFTKV_ROOT/bin/raftCoreRun" -f "$CONF" -u >>"$CLUSTER_LOG" 2>&1 &
echo $! >"$PID_FILE"
echo "raftCoreRun parent pid $(cat "$PID_FILE"), log: $CLUSTER_LOG"

"$(dirname "$0")/wait_ready.sh"
echo "cluster is ready"
