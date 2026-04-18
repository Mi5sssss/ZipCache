# Codec and Block-Level Benchmarks

These benchmarks do not exercise the B+Tree. They use synthetic ZipCache-like blocks or KV-shaped pages to isolate codec behavior.

## Current coverage

- `qpl_lz4_block_bench.c`: fixed-size block LZ4 vs QPL microbenchmark.
- `qpl_lz4_block_sizes.c`: block-size sweep for LZ4 vs QPL.
- `qpl_lz4_kv_bench.c`: single-thread KV-shaped block benchmark.
- `qpl_lz4_kv_bench_mt.cpp`: multi-thread synchronous KV-shaped block benchmark.
- `qpl_lz4_kv_bench_async.cpp`: async QPL batch benchmark with job reuse.
- `qpl_lz4_mixed_workload.cpp`: block-level read/write/compaction workload for LZ4, QPL, and zlib/zlib-accel API paths.
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
