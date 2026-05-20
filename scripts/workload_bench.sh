#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT_DIR/bin/kv_bench"

# 参数顺序（与 kv_bench 一致）：
#   1) conf_file     : 客户端连接配置（node0ip/node0port...），通常由 cluster_start 生成
#   2) threads       : 并发线程数（每线程 1 个 Clerk）
#   3) seconds       : 压测持续时间
#   4) keySpace      : 随机 key 的空间大小（key="k<0..keySpace-1>"）
#   5) valueBytes    : Put 写入 value 的字节数
#   6) writePercent  : Put 占比（0-100），其余为 Get
CONF_FILE="${1:-$ROOT_DIR/test.conf}"
THREADS="${2:-8}"
SECONDS="${3:-15}"
KEYSPACE="${4:-10000}"
VALUE_BYTES="${5:-64}"
WRITE_PCT="${6:-10}"

if [[ ! -x "$BIN" ]]; then
  echo "missing executable: $BIN"
  echo "run: $ROOT_DIR/scripts/build.sh"
  exit 1
fi

"$BIN" -c "$CONF_FILE" -t "$THREADS" -s "$SECONDS" -k "$KEYSPACE" -v "$VALUE_BYTES" -w "$WRITE_PCT"

