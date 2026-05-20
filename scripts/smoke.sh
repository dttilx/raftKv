#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

CONSISTENCY_KEYS="${RAFTKV_CONSISTENCY_KEYS:-500}"
CONSISTENCY_ROUNDS="${RAFTKV_CONSISTENCY_ROUNDS:-1}"
RUN_BENCH="${RAFTKV_SMOKE_BENCH:-1}"

cleanup() {
  "$(dirname "$0")/cluster_down.sh" || true
}
trap cleanup EXIT

if [[ ! -x "$RAFTKV_ROOT/bin/kv_consistency" ]]; then
  echo "bin/kv_consistency not found; build first" >&2
  exit 1
fi

export RAFTKV_CLEAN_PERSIST=1
"$(dirname "$0")/cluster_up.sh"

echo "== kv_consistency =="
"$RAFTKV_ROOT/bin/kv_consistency" -c "$CONF" -k "$CONSISTENCY_KEYS" -r "$CONSISTENCY_ROUNDS"

if [[ "$RUN_BENCH" == "1" ]]; then
  echo "== bench_ci =="
  "$(dirname "$0")/bench_ci.sh"
fi

echo "smoke OK"
