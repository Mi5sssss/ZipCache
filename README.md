# ZipCache DRAM-tier B+Tree

This repository contains the ZipCache DRAM-tier compressed B+Tree prototype. The current focus is the in-memory cache tier: compact B+Tree leaf pages, reduce foreground compression stalls, and evaluate CPU and hardware-assisted compression paths on realistic key-value cache workloads.

Detailed benchmark commands and result tables are kept separately in [DRAM-tier/tests/btree/IAA_EVALUATION.md](DRAM-tier/tests/btree/IAA_EVALUATION.md).

## What This Prototype Provides

- A compressed DRAM-tier B+Tree for small key-value cache objects.
- Per-leaf compressed subpages plus a software landing buffer for recent writes.
- Point operations: `put`, `put_with_payload`, `get`, and `delete`.
- Range lookup support for tests and correctness validation.
- Sharded B+Tree mode via `BTREE_SHARDS` for higher multi-thread throughput.
- Optional background landing-buffer compaction for moving expensive merge/recompress work out of foreground writes.
- Codec-level instrumentation for throughput, latency, compression ratio, CPU usage, and QPL/zlib API call counts.

## Supported Codecs

| Codec | Path | Hardware Story |
|---|---|---|
| `LZ4` | CPU baseline | Always CPU. This is the baseline throughput/latency target. |
| `QPL` | Intel QPL deflate | `BTREE_QPL_PATH=software` uses CPU. `BTREE_QPL_PATH=hardware` uses IAA and fails fast if IAA is unavailable. |
| `zlib-accel` | zlib API with optional Intel shim | Requires `LD_PRELOAD=<zlib-accel-build>/libzlib_accel.so` to use Intel zlib-accel. Without preload, it is only regular system zlib and is not a headline comparison target. |

Hardware enablement is intentionally outside the B+Tree code. Intel QPL and zlib-accel own IAA device discovery, queue use, and fallback behavior. ZipCache controls how often and where it calls those APIs.

## Key Optimizations

- `BTREE_QPL_JOB_CACHE=thread` reuses per-thread QPL jobs and avoids repeated job allocation/init on the foreground path.
- `BTREE_ZLIB_STREAM_CACHE=thread` optionally reuses per-thread zlib streams for zlib/zlib-accel API experiments.
- `BTREE_LANDING_BUFFER_BYTES` controls per-leaf uncompressed landing-buffer capacity, trading compression ratio for fewer recompressions.
- `BTREE_BG_COMPACTION=1` enables background landing-buffer compaction.
- `BTREE_BG_CODEC=qpl|zlib_accel|lz4|all` can restrict background compaction to specific codec trees, so LZ4 is not perturbed during QPL/zlib-accel tuning.
- `BTREE_MEASURE_LATENCY=1` enables sampled point read/write latency in benchmark output.
- `COLLECT_CPU=1` enables CPU efficiency metrics in the evaluation script.

## Hardware-Oriented Codec API Optimizations

ZipCache does not directly configure Intel IAA work queues. QPL and zlib-accel are responsible for hardware discovery, queue assignment, submission, completion, and software fallback policy. The B+Tree optimizes the way it calls those APIs so that, when IAA is enabled by the runtime, the foreground cache path performs fewer expensive setup and compression operations.

Implemented optimizations for QPL and zlib-accel:

- Per-thread QPL job reuse: `BTREE_QPL_JOB_CACHE=thread` keeps QPL job state hot per worker thread, avoiding repeated `qpl_get_job_size` / `qpl_init_job` style setup on each compressed leaf operation.
- QPL hardware fail-fast mode: `BTREE_QPL_PATH=hardware` is supported for evaluation so missing IAA configuration is exposed instead of silently falling back to software.
- QPL fixed and dynamic Huffman modes: `BTREE_QPL_MODE=fixed|dynamic` allows evaluating throughput-oriented fixed Huffman versus higher-compression dynamic Huffman.
- Optional per-thread zlib stream reuse: `BTREE_ZLIB_STREAM_CACHE=thread` reduces repeated zlib stream setup overhead when using the zlib API or the zlib-accel preload shim.
- Landing-buffer write absorption: recent writes are first stored in the per-leaf landing buffer, reducing immediate recompress operations and therefore reducing QPL/zlib-accel API submissions on write-heavy workloads.
- Optional background compaction: `BTREE_BG_COMPACTION=1` can move landing-buffer merge/recompress work out of foreground writes, which is useful when hardware compression has higher submit latency but enough device throughput.
- Codec-scoped background work: `BTREE_BG_CODEC=qpl|zlib_accel|lz4|all` lets hardware-focused experiments tune QPL or zlib-accel without changing the LZ4 baseline behavior.
- API call counters and CPU metrics: the benchmark reports QPL/zlib call counts, latency, CPU usage, and QPS per CPU core, which helps separate host-side API overhead from actual hardware compression throughput.

The expected hardware benefit is workload-dependent. IAA is most likely to help write-heavy or mixed workloads where compression dominates CPU time. Read-only workloads may still be limited by tree traversal, locking, landing-buffer hits, decompression latency, and API submission overhead.

## Build

Run from this directory:

```bash
git submodule update --init SilesiaCorpus

cmake -S DRAM-tier -B DRAM-tier/build_check
cmake --build DRAM-tier/build_check -j$(nproc)
```

Main binaries are written to:

```text
DRAM-tier/build_check/bin/
```

## Smoke Tests

Run these before performance evaluation:

```bash
DRAM-tier/build_check/bin/bpt_compressed_lz4_smoke
DRAM-tier/build_check/bin/bpt_compressed_qpl_smoke
DRAM-tier/build_check/bin/bpt_compressed_zlib_accel_smoke
DRAM-tier/build_check/bin/bpt_compressed_crud_fuzz
DRAM-tier/build_check/bin/test_compression_concurrency
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
DRAM-tier/build_check/bin/bpt_compressed_split_payload_stats
```

Expected result: every command passes. For `bpt_compressed_mixed_concurrency`, each codec row should show `mismatches=0`.

## Evaluation

Performance evaluation is intentionally documented outside this product-level README. Use the dedicated evaluation document for benchmark methodology, result tables, IAA commands, zlib-accel preload usage, and CPU-efficiency interpretation:

- [Codec and IAA evaluation](DRAM-tier/tests/btree/IAA_EVALUATION.md)
- [Concurrency scaling plan and implementation notes](DRAM-tier/tests/btree/CONCURRENCY_SCALING_PLAN.md)
- [B+Tree test inventory](DRAM-tier/tests/btree/README.md)
- [All DRAM-tier test categories](DRAM-tier/tests/README.md)

The main benchmark driver is `DRAM-tier/tests/btree/run_iaa_eval.sh`; exact command lines are in the evaluation document.

## Important Runtime Knobs

| Variable | Default | Meaning |
|---|---:|---|
| `BTREE_SHARDS` | `1` in the library, `8` in evaluation scripts | Hash-sharded compressed B+Tree coordinator. |
| `BTREE_LANDING_BUFFER_BYTES` | `512` | Effective per-leaf landing-buffer size. Larger values reduce recompression but reduce memory savings. |
| `BTREE_QPL_PATH` | test-controlled | `software`, `hardware`, or `auto`; use `hardware` for IAA validation. |
| `BTREE_QPL_MODE` | `fixed` | `fixed` or `dynamic` Huffman mode. |
| `BTREE_QPL_JOB_CACHE` | `thread` | Reuse per-thread QPL jobs. |
| `BTREE_ZLIB_STREAM_CACHE` | `none` | Optional `thread` mode for zlib stream reuse. |
| `BTREE_BG_COMPACTION` | `0` | Enable optional background landing-buffer compaction. |
| `BTREE_BG_CODEC` | `all` | Restrict background compaction to one codec path. |
| `BTREE_MEASURE_LATENCY` | `0` in tests, `1` in evaluation script | Collect sampled point read/write latency. |
| `COLLECT_CPU` | `1` in evaluation script | Collect CPU efficiency metrics. |

## Current Limitations

- The DRAM-tier B+Tree is still a fixed-record prototype: `typedef int key_t` and fixed inline `payload[COMPRESSED_VALUE_BYTES]`.
- `BTREE_VALUE_BYTES=128` is an evaluation format using real Silesia bytes, not final arbitrary key/value ZipCache support.
- `BTREE_OUT_OF_LOCK_REBUILD=1` is experimental and should not be used for baseline claims.
- `BTREE_BG_COMPACTION=1` is an optional background compaction mode; keep it explicit in any benchmark claim.
- zlib-accel hardware selection is controlled by the zlib-accel runtime and `LD_PRELOAD`; verify zlib-accel runtime behavior when collecting hardware claims.

## Key Files

- Compressed B+Tree library: `DRAM-tier/lib/bplustree_compressed.c`
- Public compressed B+Tree API: `DRAM-tier/lib/bplustree_compressed.h`
- Main evaluation script: `DRAM-tier/tests/btree/run_iaa_eval.sh`
- B+Tree tests and benchmarks: `DRAM-tier/tests/btree/`
- Codec-only tests and microbenchmarks: `DRAM-tier/tests/codec/`
- Evaluation details and result tables: `DRAM-tier/tests/btree/IAA_EVALUATION.md`
