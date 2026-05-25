#!/usr/bin/env bash
# =============================================================================
# bench_ci.sh — CI 用短压测（需集群已启动且 test.conf 有效）
# 运行 kv_bench，断言 errors=0；qps 仅打印，不作为失败条件。
# 通常由 smoke.sh 调用；也可在 cluster_up 后单独执行。
# 环境变量：RAFTKV_BENCH_THREADS、RAFTKV_BENCH_SECONDS、RAFTKV_BENCH_KEYS
# =============================================================================
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
