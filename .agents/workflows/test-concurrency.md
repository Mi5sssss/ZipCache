---
description: Run B+Tree concurrency stress tests (some have known deadlocks)
---

Prerequisites: run `/build` first.

## Single-thread baseline (should pass)

// turbo
1. Single-thread concurrency sanity check:
   `BTREE_THREADS=1 DRAM-tier/build/bin/test_compression_concurrency`

// turbo
2. Single-thread mixed workload (read+write only, no delete/scan):
   `BTREE_THREADS=1 BTREE_DELETE_PCT=0 BTREE_SCAN_PCT=0 BTREE_READ_PCT=50 BTREE_WRITE_PCT=50 DRAM-tier/build/bin/bpt_compressed_mixed_concurrency`

## Multi-thread tests (currently expose deadlock — expect failure)

3. Multi-thread concurrency (DEFAULT: 4 threads, will likely deadlock):
   `BTREE_THREADS=4 DRAM-tier/build/bin/test_compression_concurrency`

4. Multi-thread mixed workload (DEFAULT: 4 threads, 2s, will likely deadlock):
   `DRAM-tier/build/bin/bpt_compressed_mixed_concurrency`

## Long stress runs (use env vars to control duration)

5. Extended mixed workload (only after deadlock is fixed):
   `BTREE_THREADS=8 BTREE_DURATION_SEC=30 BTREE_KEY_SPACE=8192 DRAM-tier/build/bin/bpt_compressed_mixed_concurrency`

Working directory for all commands: `/home/xier2/2025-03-17-intel-zipcache/bplustree/bplustree`

## Status

- **BLOCKED**: Multi-writer deadlock prevents multi-thread runs
- Single-thread runs pass (confirms correctness without concurrency)
- Root cause: likely tree-level rwlock + leaf-level lock ordering issue
