---
description: Run B+Tree end-to-end latency and workload benchmarks
---

Prerequisites: run `/build` first.

## Tail latency benchmark (tree traversal + codec)

// turbo
1. Default tail latency comparison (LZ4 + QPL + zlib-accel, read-only and 80/20):
   `DRAM-tier/build/bin/tail_latency_compare`

2. Multi-size sweep:
   `sh DRAM-tier/tests/run_tail_latency_sizes.sh`

## DCPerf-style workload benchmark

// turbo
3. Default DCPerf workload (single sub-page):
   `DC_SUBPAGES=1 DRAM-tier/build/bin/dcperf_workload_benchmark`

Working directory for all commands: `/home/xier2/2025-03-17-intel-zipcache/bplustree/bplustree`

## Notes

- `tail_latency_compare` runs all three codecs (LZ4, QPL, zlib-accel) and prints p50/p90/p95/p99 latencies.
- `dcperf_workload_benchmark` uses DCPerf-inspired size distributions. Set `DC_SUBPAGES` to control sub-page count.
- QPL path/mode can be set via `BTREE_QPL_PATH` and `BTREE_QPL_MODE`.
