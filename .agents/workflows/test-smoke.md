---
description: Run all correctness smoke tests (quick, local dev safe)
---
// turbo-all

Prerequisites: run `/build` first.

1. LZ4 smoke test:
   `DRAM-tier/build/bin/bpt_compressed_lz4_smoke`

2. QPL smoke test:
   `DRAM-tier/build/bin/bpt_compressed_qpl_smoke`

3. zlib-accel smoke test:
   `DRAM-tier/build/bin/bpt_compressed_zlib_accel_smoke`

4. CRUD fuzz (known issue: may expose range-scan bug where get_range returns out-of-range value):
   `DRAM-tier/build/bin/bpt_compressed_crud_fuzz`

5. Single-thread concurrency check (multi-thread with BTREE_THREADS>1 will deadlock — known bug):
   `BTREE_THREADS=1 DRAM-tier/build/bin/test_compression_concurrency`

6. Split/payload/stats stress (known issue: stats drift after split/delete/reinsert):
   `DRAM-tier/build/bin/bpt_compressed_split_payload_stats`

Working directory for all commands: `/home/xier2/2025-03-17-intel-zipcache/bplustree/bplustree`

## Known Failures

- `bpt_compressed_crud_fuzz`: range-scan can return out-of-range values
- `test_compression_concurrency` with BTREE_THREADS>1: multi-writer deadlock
- `bpt_compressed_split_payload_stats`: incremental stats exceed calculated stats
