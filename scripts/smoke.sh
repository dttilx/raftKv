#!/usr/bin/env bash
# =============================================================================
# smoke.sh — 一键集成冒烟（本地 / CI 主入口）
# 流程：cluster_up → kv_consistency → bench_ci（可选）→ 退出时 cluster_down
# 用法（项目根目录、已编译 bin/）：
#   chmod +x scripts/*.sh
#   ./scripts/smoke.sh
# 环境变量：
#   RAFTKV_CLEAN_PERSIST=1   启动前清空 persist（smoke 内已默认 export）
#   RAFTKV_CONSISTENCY_KEYS=500   一致性校验 key 数量
#   RAFTKV_CONSISTENCY_ROUNDS=1   一致性读回轮数
#   RAFTKV_SMOKE_BENCH=0       跳过短压测，只跑一致性
# 成功末尾打印：smoke OK
# =============================================================================
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
