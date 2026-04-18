---
description: QPL hardware (IAA) specific testing workflow — only on machines with Intel IAA
---

Prerequisites: run `/build` first. Verify IAA devices exist: `ls /sys/bus/dsa/devices/`

## Check IAA availability

1. Check for IAA devices:
   `ls /sys/bus/dsa/devices/ 2>/dev/null && echo "IAA devices found" || echo "No IAA devices — hardware tests will fail"`

## B+Tree tests with QPL hardware

2. Mixed concurrency with hardware QPL, fixed Huffman:
   `BTREE_QPL_PATH=hardware BTREE_QPL_MODE=fixed DRAM-tier/build/bin/bpt_compressed_mixed_concurrency`

3. Mixed concurrency with hardware QPL, dynamic Huffman:
   `BTREE_QPL_PATH=hardware BTREE_QPL_MODE=dynamic DRAM-tier/build/bin/bpt_compressed_mixed_concurrency`

## Codec benchmarks with QPL hardware

4. Block-level mixed workload, hardware path:
   `KV_CODEC=qpl KV_QPL_PATH=hardware KV_THREADS=32 KV_DURATION_SEC=60 DRAM-tier/build/bin/qpl_lz4_mixed_workload`

5. Async batch benchmark, hardware path:
   `KV_PATH=hardware KV_THREADS=32 DRAM-tier/build/bin/qpl_lz4_kv_bench_async`

## Thread scaling sweep (for Intel collaboration)

6. Thread scaling test (hardware):
   ```
   for t in 1 2 4 8 16 32 64; do
     echo "=== $t threads ==="
     KV_CODEC=qpl KV_QPL_PATH=hardware KV_THREADS=$t KV_DURATION_SEC=10 DRAM-tier/build/bin/qpl_lz4_mixed_workload 2>&1 | tail -5
   done
   ```

Working directory for all commands: `/home/xier2/2025-03-17-intel-zipcache/bplustree/bplustree`

## Important Notes

- Hardware mode **fails fast** if IAA is unavailable — no silent fallback to software
- This is by design: silent fallback hides queue/submission contention issues Intel is investigating
- Compare hardware results with QPL software (`KV_QPL_PATH=software`) and LZ4 baselines
