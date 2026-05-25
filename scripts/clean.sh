#!/usr/bin/env bash
# =============================================================================
# clean.sh — 清理一级本地产物（构建目录、运行态文件、持久化）
# 不删源码。用法：./scripts/clean.sh
# 清理后需重新：cmake -S . -B cmake-build && cmake --build cmake-build -j
# =============================================================================
set -euo pipefail
source "$(dirname "$0")/common.sh"

"$(dirname "$0")/cluster_down.sh" 2>/dev/null || true

rm -rf "$RAFTKV_ROOT/cmake-build" "$RAFTKV_ROOT/cmake-build-debug" \
  "$RAFTKV_ROOT/build" "$RAFTKV_ROOT/lib" "$RAFTKV_ROOT/bin" "$RAFTKV_ROOT/logs"
rm -f "$RAFTKV_ROOT/test.conf" "$RAFTKV_ROOT/.cluster.pid"
rm -f "$RAFTKV_ROOT"/raftstatePersist*.txt "$RAFTKV_ROOT"/snapshotPersist*.txt
rm -f "$RAFTKV_ROOT/CMakeCache.txt" "$RAFTKV_ROOT/cmake_install.cmake"
rm -rf "$RAFTKV_ROOT/CMakeFiles"

echo "clean OK (rebuild before run)"
