---
description: Run block-level codec benchmarks (LZ4 vs QPL vs zlib-accel, no B+Tree)
---

Prerequisites: run `/build` first. For Silesia corpus data, run `git submodule update --init SilesiaCorpus`.

## Quick codec bench (KV-shaped blocks)

// turbo
1. Single-thread KV bench with run_kv_bench.sh:
   `sh DRAM-tier/tests/run_kv_bench.sh`

## Multi-threaded codec benchmarks

2. Multi-thread sync benchmark (LZ4):
   `KV_THREADS=32 DRAM-tier/build/bin/qpl_lz4_kv_bench_mt`

3. QPL async batch benchmark:
   `KV_THREADS=32 KV_PATH=software DRAM-tier/build/bin/qpl_lz4_kv_bench_async`

## Mixed workload (block-level, not B+Tree)

4. LZ4 baseline:
   `KV_THREADS=32 KV_DURATION_SEC=10 DRAM-tier/build/bin/qpl_lz4_mixed_workload`

5. QPL software path:
   `KV_CODEC=qpl KV_QPL_PATH=software KV_THREADS=32 KV_DURATION_SEC=10 DRAM-tier/build/bin/qpl_lz4_mixed_workload`

6. QPL dynamic Huffman:
   `KV_CODEC=qpl KV_QPL_PATH=software KV_QPL_MODE=dynamic KV_THREADS=32 KV_DURATION_SEC=10 DRAM-tier/build/bin/qpl_lz4_mixed_workload`

7. zlib API baseline:
   `KV_CODEC=zlib_accel KV_THREADS=32 KV_DURATION_SEC=10 DRAM-tier/build/bin/qpl_lz4_mixed_workload`

## QPL hardware (only on IAA machines)

8. QPL hardware path (will fail without IAA hardware):
   `KV_CODEC=qpl KV_QPL_PATH=hardware KV_THREADS=32 DRAM-tier/build/bin/qpl_lz4_mixed_workload`

Working directory for all commands: `/home/xier2/2025-03-17-intel-zipcache/bplustree/bplustree`
