#!/usr/bin/env bash
# =============================================================================
# bench_nightly.sh — README 同参压测（8 线程 × 15s），供 GitHub Actions 定时任务
# 前置：已编译 bin/kv_bench；会 cluster_up → bench → cluster_down
# 环境变量：RAFTKV_BENCH_OUT（默认 logs/bench-nightly.txt）
# =============================================================================
set -euo pipefail
source "$(dirname "$0")/common.sh"

THREADS="${RAFTKV_BENCH_THREADS:-8}"
BENCH_SEC="${RAFTKV_BENCH_SECONDS:-15}"
KEYS="${RAFTKV_BENCH_KEYS:-10000}"
VALUE_BYTES="${RAFTKV_BENCH_VALUE_BYTES:-64}"
WRITE_PERCENT="${RAFTKV_BENCH_WRITE_PERCENT:-10}"
OUT="${RAFTKV_BENCH_OUT:-${LOG_DIR}/bench-nightly.txt}"

cleanup() {
  "$(dirname "$0")/cluster_down.sh" || true
}
trap cleanup EXIT

if [[ ! -x "$RAFTKV_ROOT/bin/kv_bench" ]]; then
  echo "bin/kv_bench not found; build first" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"
export RAFTKV_CLEAN_PERSIST=1
"$(dirname "$0")/cluster_up.sh"

echo "== kv_bench (nightly params) =="
out="$("$RAFTKV_ROOT/bin/kv_bench" -c "$CONF" -t "$THREADS" -s "$BENCH_SEC" -k "$KEYS" \
  -v "$VALUE_BYTES" -w "$WRITE_PERCENT")"
echo "$out" | tee "$OUT"

errors="$(echo "$out" | sed -n 's/.*errors=\([0-9]*\).*/\1/p' | head -n1)"
qps="$(echo "$out" | sed -n 's/.*qps=\([0-9.]*\).*/\1/p' | head -n1)"
p50="$(echo "$out" | sed -n 's/.*p50=\([0-9.]*\).*/\1/p' | head -n1)"
p99="$(echo "$out" | sed -n 's/.*p99=\([0-9.]*\).*/\1/p' | head -n1)"

if [[ -z "${errors:-}" ]]; then
  echo "failed to parse kv_bench output" >&2
  exit 1
fi
if [[ "$errors" != "0" ]]; then
  echo "kv_bench errors=$errors (expected 0)" >&2
  exit 1
fi

echo "bench_nightly OK errors=0 qps=${qps:-n/a} p50_us=${p50:-n/a} p99_us=${p99:-n/a}"
echo "saved: $OUT"
