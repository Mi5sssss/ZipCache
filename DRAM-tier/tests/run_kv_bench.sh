#!/usr/bin/env sh
set -eu

ROOT=$(cd -- "$(dirname "$0")/.." && pwd)
BIN=${BIN:-"$ROOT/build/bin/qpl_lz4_kv_bench"}

if [ ! -x "$BIN" ]; then
  echo "error: benchmark binary not found at $BIN (set BIN=...)" >&2
  exit 1
fi

# Silesia corpus is expected at the repo root (via git submodule)
if [ -z "${SILESIA_CORPUS_DIR:-}" ]; then
    export SILESIA_CORPUS_DIR="$ROOT/../SilesiaCorpus"
fi

if [ ! -f "$SILESIA_CORPUS_DIR/samba.zip" ]; then
    echo "Warning: samba.zip not found in $SILESIA_CORPUS_DIR"
    echo "Please run: git submodule update --init SilesiaCorpus"
fi

SIZES=${KV_BLOCK_SIZES:-"4096 8192 16384"}
OCC=${KV_OCCUPANCY_PCT:-50}
QPATH=${KV_QPL_PATH:-software}
USE_SILESIA=${KV_BENCH_USE_SILESIA:-1}
BLOCKS=${KV_BLOCKS:-4096}

for sz in $SIZES; do
  echo "=== KV bench block_size=${sz} occupancy=${OCC}% qpl_path=${QPATH} blocks=${BLOCKS} ==="
  KV_BLOCK_SIZE=$sz \
  KV_OCCUPANCY_PCT=$OCC \
  KV_QPL_PATH=$QPATH \
  KV_BENCH_USE_SILESIA=$USE_SILESIA \
  KV_BLOCKS=$BLOCKS \
    "$BIN"
  echo ""
done
