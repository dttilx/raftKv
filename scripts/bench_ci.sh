#!/usr/bin/env bash
# Short bench for CI: assert errors=0; print QPS (no hard QPS gate).
set -euo pipefail
source "$(dirname "$0")/common.sh"

THREADS="${RAFTKV_BENCH_THREADS:-2}"
SECONDS="${RAFTKV_BENCH_SECONDS:-5}"
KEYS="${RAFTKV_BENCH_KEYS:-500}"

if [[ ! -x "$RAFTKV_ROOT/bin/kv_bench" ]]; then
  echo "bin/kv_bench not found" >&2
  exit 1
fi

out="$("$RAFTKV_ROOT/bin/kv_bench" -c "$CONF" -t "$THREADS" -s "$SECONDS" -k "$KEYS" -v 32 -w 10)"
echo "$out"

errors="$(echo "$out" | sed -n 's/.*errors=\([0-9]*\).*/\1/p' | head -n1)"
qps="$(echo "$out" | sed -n 's/.*qps=\([0-9.]*\).*/\1/p' | head -n1)"

if [[ -z "${errors:-}" ]]; then
  echo "failed to parse kv_bench errors from output" >&2
  exit 1
fi
if [[ "$errors" != "0" ]]; then
  echo "kv_bench errors=$errors (expected 0)" >&2
  exit 1
fi

echo "bench_ci OK (errors=0, qps=${qps:-n/a}, informational only)"
