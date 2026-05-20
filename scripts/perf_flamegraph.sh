#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONF_FILE="${1:-$ROOT_DIR/test.conf}"
OUT_DIR="${2:-$ROOT_DIR/perf-out}"

FLAMEGRAPH_DIR="${FLAMEGRAPH_DIR:-$ROOT_DIR/FlameGraph}"
BENCH="$ROOT_DIR/bin/kv_bench"

mkdir -p "$OUT_DIR"

if [[ ! -x "$BENCH" ]]; then
  echo "missing executable: $BENCH"
  echo "run: $ROOT_DIR/scripts/build.sh"
  exit 1
fi
if [[ ! -d "$FLAMEGRAPH_DIR" ]]; then
  echo "missing FlameGraph dir: $FLAMEGRAPH_DIR"
  echo "run: git clone https://github.com/brendangregg/FlameGraph $FLAMEGRAPH_DIR"
  exit 1
fi

echo "Starting perf record during benchmark..."
echo "Output dir: $OUT_DIR"

sudo perf record -F 99 -g -- "$BENCH" -c "$CONF_FILE" -t 8 -s 15 -k 10000 -v 64 -w 10

sudo perf script >"$OUT_DIR/out.perf"
"$FLAMEGRAPH_DIR/stackcollapse-perf.pl" "$OUT_DIR/out.perf" >"$OUT_DIR/out.folded"
"$FLAMEGRAPH_DIR/flamegraph.pl" "$OUT_DIR/out.folded" >"$OUT_DIR/flame.svg"

echo "flamegraph generated: $OUT_DIR/flame.svg"

