# DRAM-tier Tests

This directory separates tests by what they actually exercise:

- `btree/`: real B+Tree and compressed B+Tree tests. These call `bplus_tree_*` or `bplus_tree_compressed_*`, including multi-thread stress tests that currently expose B+Tree write-path issues.
- `codec/`: block-level codec benchmarks. These do not traverse the B+Tree; they isolate LZ4, QPL, and zlib/zlib-accel compression costs on ZipCache-like blocks.
- `legacy/`: disabled or old-API tests kept for reference.

Scripts such as `run_kv_bench.sh` and `run_tail_latency_sizes.sh` remain in this directory because they invoke built binaries rather than compile source files directly.
