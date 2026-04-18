# Codec and Block-Level Benchmarks

These benchmarks do not exercise the B+Tree. They use synthetic ZipCache-like blocks or KV-shaped pages to isolate codec behavior.

## Current coverage

- `qpl_lz4_block_bench.c`: fixed-size block LZ4 vs QPL microbenchmark.
- `qpl_lz4_block_sizes.c`: block-size sweep for LZ4 vs QPL.
- `qpl_lz4_kv_bench.c`: single-thread KV-shaped block benchmark.
- `qpl_lz4_kv_bench_mt.cpp`: multi-thread synchronous KV-shaped block benchmark.
- `qpl_lz4_kv_bench_async.cpp`: async QPL batch benchmark with job reuse.
- `qpl_lz4_mixed_workload.cpp`: block-level read/write/compaction workload for LZ4, QPL, and zlib/zlib-accel API paths. Default input is packed 4KB blocks with no zero padding (`KV_PACKED_BLOCKS=1`); set `KV_PACKED_BLOCKS=0` to use the older sparse KV-shaped layout with `KV_OCCUPANCY_PCT`. QPL compaction uses batched async submission; LZ4 and zlib/zlib-accel compaction use synchronous background compression. Set `KV_QPL_ASYNC_FOREGROUND=1` to batch QPL foreground read/write submit/wait with `KV_BATCH_SIZE`.
- `clock_resolution_test.cpp`: timing helper.
- `decompress_fail_test.cpp`: decompression debugging helper.

## Tests we still need

- Add explicit zlib compression-level controls so `zlib_accel` can be compared against QPL fixed and QPL dynamic more fairly.
- Add report fields that separate read QPS, write QPS, compact QPS, and total QPS because total currently includes compaction.
- Add a zlib-accel preload detection printout or runtime note, so plain zlib runs are not confused with real hardware-offloaded zlib-accel.
- Add fixed workload seeds and repeated runs to reduce noise in short-duration comparisons.
- Add QPL hardware queue scaling tests: thread count, batch size, fixed vs dynamic Huffman, and hardware/software/auto paths.
- Add failure-mode tests for unavailable hardware path, QPL busy/retry behavior, and zlib-accel fallback.
- Add codec correctness verification after every compress/decompress path, not only performance counters.
- Add a CSV/JSON report mode to make Intel-side result parsing less fragile.
