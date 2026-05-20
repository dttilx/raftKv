#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/cmake-build}"
BUILD_TYPE="${BUILD_TYPE:-RelWithDebInfo}"

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

cmake -DCMAKE_BUILD_TYPE="$BUILD_TYPE" "$ROOT_DIR"
cmake --build . -j"$(nproc)"

echo "Build done."
echo "  build_dir=$BUILD_DIR"
echo "  bin_dir=$ROOT_DIR/bin"

