#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT_DIR/bin/kv_consistency"

CONF_FILE="${1:-$ROOT_DIR/test.conf}"
KEYS="${2:-2000}"
ROUNDS="${3:-2}"

if [[ ! -x "$BIN" ]]; then
  echo "missing executable: $BIN"
  echo "run: $ROOT_DIR/scripts/build.sh"
  exit 1
fi

"$BIN" -c "$CONF_FILE" -k "$KEYS" -r "$ROUNDS"

