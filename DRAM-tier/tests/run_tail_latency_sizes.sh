#!/usr/bin/env sh
set -eu

ROOT=$(cd -- "$(dirname "$0")/.." && pwd)
BIN=${BIN:-"$ROOT/build/bin/tail_latency_compare"}
SRC_H="$ROOT/lib/bplustree_compressed.h"

if [ ! -x "$BIN" ]; then
  echo "error: tail_latency_compare not found at $BIN (set BIN=...)" >&2
  exit 1
fi

SIZES=${COMPRESSED_LEAF_SIZES:-"4096 8192 16384"}

backup="$SRC_H.bak_run_sizes"
cp "$SRC_H" "$backup"
trap 'cp "$backup" "$SRC_H" && rm -f "$backup"' EXIT INT TERM

for sz in $SIZES; do
  echo "=== building with COMPRESSED_LEAF_SIZE=$sz ==="
  # update header
  tmp="$SRC_H.tmp"
  sed "s/^#define COMPRESSED_LEAF_SIZE .*/#define COMPRESSED_LEAF_SIZE $sz/; s/^#define MAX_COMPRESSED_SIZE .*/#define MAX_COMPRESSED_SIZE (COMPRESSED_LEAF_SIZE * 2)/" \
    "$SRC_H" > "$tmp"
  mv "$tmp" "$SRC_H"

  cmake --build "$ROOT/build" --target tail_latency_compare >/dev/null

  echo "--- running tail_latency_compare (Silesia) ---"
  TAIL_LATENCY_USE_SILESIA=1 "$BIN"
  echo ""
done
