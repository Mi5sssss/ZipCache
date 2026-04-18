# ZipCache DRAM-tier B+Tree Codec and IAA Evaluation

This document contains the benchmark commands and result tables for the ZipCache DRAM-tier compressed B+Tree. The main repository README is intentionally product-level; keep detailed evaluation methodology here.

## Evaluation Scope

The main benchmark compares real compressed B+Tree operations. A codec-only mixed workload is also included to isolate LZ4, QPL, and zlib/zlib-accel behavior on 4KB ZipCache-like blocks.

Codecs and engines:

- `LZ4`: CPU baseline.
- `QPL auto fixed`: Intel QPL deflate with `qpl_path_auto` and fixed Huffman mode.
- `QPL auto dynamic`: Intel QPL deflate with `qpl_path_auto` and dynamic Huffman mode.
- `zlib API software fallback`: `COMPRESS_ZLIB_ACCEL` without `LD_PRELOAD`; this is the zlib API software fallback reference on a non-IAA or non-preload run.
- `zlib-accel`: Intel zlib-accel shim through `LD_PRELOAD=<zlib-accel-build>/libzlib_accel.so`.

QPL `auto` follows the QPL C API definition of `qpl_path_auto`: auto-detection of the equipment for execution. ZipCache does not force the path in normal evaluation. If Intel configures IAA for QPL, the same ZipCache command can use IAA through the QPL runtime. Forced `software` and forced `hardware` remain available only for debugging or strict diagnostics.

In this codebase, `COMPRESS_ZLIB_ACCEL` calls the zlib API. It only becomes Intel zlib-accel when the process is run with the zlib-accel shared library preloaded. Without preload, the same benchmark path is reported as `zlib API software fallback`. If the Intel shim is preloaded but cannot offload to IAA/QAT and zlib fallback is enabled in the shim configuration, the row should still be reported as `engine=zlib_accel_preload`, because the selected runtime was zlib-accel.

## Build

If the repository already exists, update the `restruct` branch first:

```bash
git checkout restruct
git pull origin restruct
```

Run from the B+Tree repository root:

```bash
git submodule update --init SilesiaCorpus

cmake -S DRAM-tier -B DRAM-tier/build_check
cmake --build DRAM-tier/build_check -j$(nproc)
```

Main binaries are written to:

```text
DRAM-tier/build_check/bin/
```

## Smoke Test

Run this before collecting performance numbers:

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

## Quick 8-Thread Commands

The commands below run real DRAM-tier compressed B+Tree workloads. `run_iaa_eval.sh` includes the LZ4 CPU baseline by default. It also includes QPL auto fixed/dynamic and the zlib API software fallback unless filtered through the script variables.

Update and build:

```bash
git checkout restruct
git pull origin restruct

git submodule update --init SilesiaCorpus

cmake -S DRAM-tier -B DRAM-tier/build_check
cmake --build DRAM-tier/build_check -j$(nproc)
```

Default 8-thread B+Tree evaluation. This includes the LZ4 baseline:

```bash
THREADS_LIST='8' \
BTREE_DURATION_SEC=5 \
BTREE_KEY_SPACE=50000 \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_LANDING_BUFFER_BYTES=512 \
DRAM-tier/tests/btree/run_iaa_eval.sh
```

QPL auto evaluation after Intel IAA is configured. ZipCache still requests `qpl_path_auto`; the Intel QPL runtime selects the execution path:

```bash
QPL_EVAL_PATH=auto \
BTREE_QPL_JOB_CACHE=thread \
BTREE_ZLIB_STREAM_CACHE=thread \
THREADS_LIST='8' \
BTREE_DURATION_SEC=5 \
BTREE_KEY_SPACE=50000 \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_LANDING_BUFFER_BYTES=512 \
DRAM-tier/tests/btree/run_iaa_eval.sh
```

zlib-accel evaluation. Replace `ZLIB_ACCEL_SO` with the actual path to `libzlib_accel.so`:

```bash
QPL_EVAL_PATH=auto \
BTREE_QPL_JOB_CACHE=thread \
BTREE_ZLIB_STREAM_CACHE=thread \
RUN_ZLIB_ACCEL=1 \
ZLIB_ACCEL_SO=/path/to/libzlib_accel.so \
THREADS_LIST='8' \
BTREE_DURATION_SEC=5 \
BTREE_KEY_SPACE=50000 \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_LANDING_BUFFER_BYTES=512 \
DRAM-tier/tests/btree/run_iaa_eval.sh
```

If only the LZ4 baseline is needed, disable the other default rows:

```bash
QPL_AUTO_CODECS='' \
RUN_QPL_DYNAMIC=0 \
RUN_ZLIB_SOFTWARE_FALLBACK=0 \
THREADS_LIST='8' \
BTREE_DURATION_SEC=5 \
BTREE_KEY_SPACE=50000 \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_LANDING_BUFFER_BYTES=512 \
DRAM-tier/tests/btree/run_iaa_eval.sh
```

Results are written to:

```text
DRAM-tier/tests/btree/results/iaa_eval/<timestamp>/summary.tsv
```

## Codec-Only Mixed Workload

This benchmark does not traverse the B+Tree. It isolates codec behavior on packed 4KB blocks generated from `SilesiaCorpus/samba.zip`, with no zero padding. It is useful for validating codec compression ratio, compression/decompression throughput, and codec-level latency before running full B+Tree tests.

LZ4 CPU baseline:

```bash
KV_CODEC=lz4 \
KV_THREADS=8 \
KV_DURATION_SEC=5 \
KV_BLOCKS=50000 \
KV_BENCH_USE_SILESIA=1 \
KV_PACKED_BLOCKS=1 \
KV_CPU_WORK_US=0 \
DRAM-tier/build_check/bin/qpl_lz4_mixed_workload
```

QPL auto fixed after Intel IAA is configured. `KV_QPL_PATH=auto` lets the QPL runtime select the execution path:

```bash
KV_CODEC=qpl \
KV_QPL_PATH=auto \
KV_QPL_MODE=fixed \
KV_QPL_ASYNC_FOREGROUND=1 \
KV_BATCH_SIZE=8 \
KV_THREADS=8 \
KV_DURATION_SEC=5 \
KV_BLOCKS=50000 \
KV_BENCH_USE_SILESIA=1 \
KV_PACKED_BLOCKS=1 \
KV_CPU_WORK_US=0 \
DRAM-tier/build_check/bin/qpl_lz4_mixed_workload
```

QPL auto dynamic:

```bash
KV_CODEC=qpl \
KV_QPL_PATH=auto \
KV_QPL_MODE=dynamic \
KV_QPL_ASYNC_FOREGROUND=1 \
KV_BATCH_SIZE=8 \
KV_THREADS=8 \
KV_DURATION_SEC=5 \
KV_BLOCKS=50000 \
KV_BENCH_USE_SILESIA=1 \
KV_PACKED_BLOCKS=1 \
KV_CPU_WORK_US=0 \
DRAM-tier/build_check/bin/qpl_lz4_mixed_workload
```

zlib-accel. Replace `ZLIB_ACCEL_SO` with the actual path to `libzlib_accel.so`:

```bash
LD_PRELOAD=/path/to/libzlib_accel.so \
KV_CODEC=zlib_accel \
KV_THREADS=8 \
KV_DURATION_SEC=5 \
KV_BLOCKS=50000 \
KV_BENCH_USE_SILESIA=1 \
KV_PACKED_BLOCKS=1 \
KV_CPU_WORK_US=0 \
DRAM-tier/build_check/bin/qpl_lz4_mixed_workload
```

Local software baseline collected without IAA and without zlib-accel preload:

| Codec path | Ratio | Total QPS (K/s) | Read QPS (K/s) | Write QPS (K/s) | Read P50 (us) | Read P99 (us) | Write P50 (us) | Write P99 (us) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| LZ4 CPU | 2.051x | 1330.06 | 1036.00 | 133.91 | 2.52 | 4.01 | 11.07 | 19.20 |
| QPL software fixed, sync foreground | 2.031x | 408.97 | 226.57 | 79.90 | 14.52 | 27.46 | 18.73 | 32.41 |
| QPL software dynamic, sync foreground | 2.785x | 261.31 | 175.79 | 43.08 | 20.31 | 36.55 | 37.76 | 54.16 |
| QPL software fixed, async foreground batch=8 | 2.031x | 363.16 | 200.01 | 74.03 | 15.31 | 21.60 | 19.25 | 28.26 |
| QPL software dynamic, async foreground batch=8 | 2.785x | 247.87 | 163.89 | 41.23 | 19.91 | 28.26 | 38.88 | 57.02 |
| zlib API software fallback | 2.876x | 151.25 | 123.09 | 13.68 | 25.15 | 37.16 | 124.42 | 190.97 |

For this codec-only workload, total QPS includes read, write, and background compaction operations. The default thread split is `R=4 W=2 C=2` for `KV_THREADS=8` and workload percentages `50/30/20`.

## CPU-Aware QPL Auto Baseline Results

These numbers were collected on a non-IAA machine. They are a CPU-only baseline for comparing against future Intel IAA runs. Each row first warms the tree by inserting the full key space, then runs the timed workload.

Configuration:

| Setting | Value |
|---|---|
| Threads | 8 and 32 |
| Duration | 5 seconds per workload |
| Key space | 50,000 keys |
| Value source | `SilesiaCorpus/samba.zip` |
| Value bytes | 128 bytes per key |
| Shards | 8 |
| Landing buffer | 512 bytes per leaf |
| Background compaction | off |
| QPL mode | `qpl_path_auto`, fixed and dynamic Huffman |
| zlib mode | zlib API software fallback, no `LD_PRELOAD` |
| CPU metrics | timed-window `getrusage()` per codec process |
| Correctness | all rows had `mismatches=0` |

Result directory:

```text
DRAM-tier/tests/btree/results/cpu_aware_qpl_auto/20260418T204916Z/
```

Reproduction command:

```bash
THREADS_LIST='8 32' \
BTREE_DURATION_SEC=5 \
BTREE_KEY_SPACE=50000 \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_LANDING_BUFFER_BYTES=512 \
BTREE_BG_COMPACTION=0 \
COLLECT_CPU=1 \
BTREE_MEASURE_LATENCY=1 \
BTREE_LATENCY_SAMPLES_PER_THREAD=4096 \
QPL_EVAL_PATH=auto \
RUN_QPL_DYNAMIC=1 \
RUN_ZLIB_SOFTWARE_FALLBACK=1 \
DRAM-tier/tests/btree/run_iaa_eval.sh
```

`Memory saved %` in the result tables is the whole-tree resident memory saving:

```text
Memory saved % = 100 * (1 - compressed_bytes / total_bytes)
```

`compressed_bytes` includes compressed subpage bytes plus live uncompressed landing-buffer bytes. `total_bytes` is the logical resident key-value bytes represented by the tree. This is a DRAM-tier memory-efficiency metric, not a raw codec-only compression ratio.

### 8-Thread Results

| Workload | Codec | QPS (Mops/s) | Read p99 us | Read p999 us | Write p99 us | Write p999 us | CPU cores | Kops/s/core | Memory saved % |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| R:W 10:0 (read-only) | LZ4 | 3.69 | 4.249 | 13.005 | NA | NA | 7.43 | 496.5 | 18.87 |
| R:W 10:0 (read-only) | QPL auto fixed | 0.64 | 19.023 | 29.916 | NA | NA | 6.55 | 98.3 | 16.58 |
| R:W 10:0 (read-only) | QPL auto dynamic | 0.40 | 30.058 | 44.247 | NA | NA | 6.51 | 61.9 | 29.55 |
| R:W 10:0 (read-only) | zlib API software fallback | 0.40 | 32.016 | 200.770 | NA | NA | 6.82 | 58.1 | 29.06 |
| R:W 0:10 (write-only) | LZ4 | 2.16 | NA | NA | 16.302 | 32.564 | 7.88 | 273.6 | 25.21 |
| R:W 0:10 (write-only) | QPL auto fixed | 0.40 | NA | NA | 55.602 | 5985.484 | 4.38 | 91.1 | 22.23 |
| R:W 0:10 (write-only) | QPL auto dynamic | 0.25 | NA | NA | 112.127 | 10477.396 | 5.00 | 50.1 | 36.02 |
| R:W 0:10 (write-only) | zlib API software fallback | 0.09 | NA | NA | 294.856 | 16258.164 | 4.95 | 19.0 | 35.11 |
| R:W 8:2 | LZ4 | 2.78 | 6.227 | 16.014 | 18.707 | 32.729 | 7.99 | 347.8 | 24.72 |
| R:W 8:2 | QPL auto fixed | 0.38 | 23.270 | 95.503 | 59.273 | 1189.604 | 4.91 | 78.2 | 20.62 |
| R:W 8:2 | QPL auto dynamic | 0.26 | 32.110 | 4366.147 | 112.850 | 6175.840 | 5.24 | 50.3 | 34.78 |
| R:W 8:2 | zlib API software fallback | 0.19 | 42.947 | 10519.454 | 261.094 | 15142.936 | 5.35 | 34.8 | 34.44 |
| R:W 5:5 | LZ4 | 2.60 | 6.624 | 18.003 | 20.938 | 33.953 | 7.98 | 325.2 | 25.28 |
| R:W 5:5 | QPL auto fixed | 0.40 | 21.683 | 3136.440 | 55.175 | 4399.642 | 4.80 | 82.8 | 21.70 |
| R:W 5:5 | QPL auto dynamic | 0.26 | 31.742 | 836.235 | 112.075 | 8995.446 | 5.18 | 49.8 | 35.56 |
| R:W 5:5 | zlib API software fallback | 0.13 | 46.120 | 11070.237 | 268.462 | 16273.082 | 4.76 | 26.6 | 34.76 |
| R:W 2:8 | LZ4 | 2.53 | 4.372 | 14.687 | 21.856 | 29.075 | 7.98 | 316.8 | 24.96 |
| R:W 2:8 | QPL auto fixed | 0.41 | 22.949 | 77.083 | 55.320 | 10334.597 | 4.70 | 88.1 | 22.00 |
| R:W 2:8 | QPL auto dynamic | 0.24 | 32.324 | 7334.178 | 110.118 | 13013.421 | 4.82 | 49.7 | 35.95 |
| R:W 2:8 | zlib API software fallback | 0.11 | 51.444 | 12120.510 | 284.264 | 16260.474 | 5.05 | 21.5 | 35.05 |

### 32-Thread Results

| Workload | Codec | QPS (Mops/s) | Read p99 us | Read p999 us | Write p99 us | Write p999 us | CPU cores | Kops/s/core | Memory saved % |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| R:W 10:0 (read-only) | LZ4 | 8.50 | 6.607 | 12.832 | NA | NA | 21.56 | 394.1 | 18.87 |
| R:W 10:0 (read-only) | QPL auto fixed | 1.26 | 19.314 | 936.494 | NA | NA | 13.02 | 96.5 | 16.58 |
| R:W 10:0 (read-only) | QPL auto dynamic | 0.78 | 27.997 | 24039.509 | NA | NA | 12.61 | 61.9 | 29.55 |
| R:W 10:0 (read-only) | zlib API software fallback | 0.79 | 29.960 | 20049.773 | NA | NA | 13.93 | 56.6 | 29.06 |
| R:W 0:10 (write-only) | LZ4 | 2.56 | NA | NA | 15.892 | 4983.432 | 11.40 | 224.5 | 25.05 |
| R:W 0:10 (write-only) | QPL auto fixed | 1.06 | NA | NA | 55.745 | 17310.542 | 11.98 | 88.9 | 22.31 |
| R:W 0:10 (write-only) | QPL auto dynamic | 0.59 | NA | NA | 108.623 | 17188.923 | 11.92 | 49.5 | 36.48 |
| R:W 0:10 (write-only) | zlib API software fallback | 0.20 | NA | NA | 902.441 | 30615.325 | 12.14 | 16.8 | 35.58 |
| R:W 8:2 | LZ4 | 3.10 | 3.862 | 15.976 | 17.912 | 9455.479 | 9.83 | 314.9 | 24.78 |
| R:W 8:2 | QPL auto fixed | 0.96 | 22.430 | 9029.303 | 58.368 | 14697.333 | 12.41 | 77.7 | 21.68 |
| R:W 8:2 | QPL auto dynamic | 0.64 | 31.170 | 16450.194 | 113.849 | 16644.431 | 12.94 | 49.4 | 35.60 |
| R:W 8:2 | zlib API software fallback | 0.38 | 43.986 | 24042.467 | 290.412 | 28236.701 | 11.96 | 31.7 | 34.96 |
| R:W 5:5 | LZ4 | 2.54 | 3.804 | 695.262 | 16.724 | 6514.148 | 9.59 | 265.2 | 25.09 |
| R:W 5:5 | QPL auto fixed | 0.91 | 22.052 | 12054.777 | 55.884 | 17361.501 | 11.21 | 81.1 | 22.19 |
| R:W 5:5 | QPL auto dynamic | 0.56 | 32.317 | 17270.457 | 111.820 | 19345.258 | 11.32 | 49.1 | 36.10 |
| R:W 5:5 | zlib API software fallback | 0.28 | 53.881 | 19528.855 | 342.380 | 24415.688 | 12.54 | 22.7 | 35.38 |
| R:W 2:8 | LZ4 | 2.28 | 3.811 | 1181.156 | 16.065 | 5469.991 | 9.13 | 249.9 | 25.12 |
| R:W 2:8 | QPL auto fixed | 0.95 | 22.646 | 7959.618 | 55.984 | 14537.962 | 11.07 | 85.5 | 22.29 |
| R:W 2:8 | QPL auto dynamic | 0.54 | 32.548 | 13645.796 | 110.109 | 25175.305 | 11.00 | 49.1 | 36.22 |
| R:W 2:8 | zlib API software fallback | 0.22 | 60.036 | 18388.898 | 333.249 | 35768.970 | 12.03 | 18.7 | 35.65 |

Interpretation:

- On this non-IAA machine, CPU LZ4 is the throughput baseline to beat.
- QPL auto rows use `qpl_path_auto`; this table records the requested QPL path, not the internal device selected by QPL.
- QPL auto dynamic and zlib API software fallback save more memory than LZ4/QPL fixed on this dataset, but they are significantly slower on this machine.
- The Intel experiment is to rerun this matrix after configuring IAA for QPL and/or with `RUN_ZLIB_ACCEL=1 ZLIB_ACCEL_SO=<zlib-accel-build>/libzlib_accel.so`, then compare `QPS`, latency, `CPU cores`, and `Kops/s/core`.
- A useful IAA result can be either higher throughput than LZ4 or much lower CPU cores at similar throughput.

## One-Command IAA Evaluation

The evaluation script runs real B+Tree `put/get` workloads and writes a normalized TSV summary with codec, engine, QPL mode, QPS, sampled latency, host CPU usage, correctness mismatches, and whole-tree compression ratio.

By default, `run_iaa_eval.sh` runs each codec in a separate process. The benchmark reports timed-window CPU usage through `getrusage()`, while the script also wraps the process with `/usr/bin/time` for RSS and context-switch counters. Per-codec CPU metrics are only meaningful when LZ4, QPL, and zlib-accel are not mixed in the same process.

Default run. This includes LZ4, QPL auto fixed, QPL auto dynamic, and the zlib API software fallback path. If Intel has configured QPL to use IAA, the QPL auto rows can use hardware without changing the ZipCache command:

```bash
THREADS_LIST='4 8 16 32' \
BTREE_DURATION_SEC=5 \
BTREE_KEY_SPACE=50000 \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_LANDING_BUFFER_BYTES=512 \
DRAM-tier/tests/btree/run_iaa_eval.sh
```

QPL auto with Intel IAA configured externally:

```bash
QPL_EVAL_PATH=auto \
THREADS_LIST='4 8 16 32' \
BTREE_DURATION_SEC=5 \
BTREE_KEY_SPACE=50000 \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_LANDING_BUFFER_BYTES=512 \
DRAM-tier/tests/btree/run_iaa_eval.sh
```

QPL auto plus Intel zlib-accel:

```bash
QPL_EVAL_PATH=auto \
RUN_ZLIB_ACCEL=1 \
ZLIB_ACCEL_SO=<zlib-accel-build>/libzlib_accel.so \
THREADS_LIST='4 8 16 32' \
BTREE_DURATION_SEC=5 \
BTREE_KEY_SPACE=50000 \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_LANDING_BUFFER_BYTES=512 \
DRAM-tier/tests/btree/run_iaa_eval.sh
```

Hardware-oriented performance sweep:

```bash
QPL_EVAL_PATH=auto \
RUN_ZLIB_ACCEL=1 \
RUN_BG_SWEEP=1 \
ZLIB_ACCEL_SO=<zlib-accel-build>/libzlib_accel.so \
THREADS_LIST='8 32' \
LANDING_LIST='512 1024 2048' \
BTREE_DURATION_SEC=5 \
BTREE_KEY_SPACE=50000 \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_BG_CODEC=qpl \
BTREE_BG_THREADS=1 \
BTREE_BG_LANDING_HIGH_WATERMARK_PCT=75 \
COLLECT_CPU=1 \
BTREE_QPL_JOB_CACHE=thread \
BTREE_ZLIB_STREAM_CACHE=thread \
BTREE_MEASURE_LATENCY=1 \
BTREE_LATENCY_SAMPLES_PER_THREAD=4096 \
DRAM-tier/tests/btree/run_iaa_eval.sh
```

For a zlib-accel-focused sweep, use the same command but set:

```bash
BTREE_BG_CODEC=zlib_accel
```

Results are written under:

```text
DRAM-tier/tests/btree/results/iaa_eval/<timestamp>/
DRAM-tier/tests/btree/results/iaa_eval/<timestamp>/summary.tsv
```

## Summary TSV Columns

| Column | Meaning |
|---|---|
| `run` | Evaluation configuration, for example `lz4_cpu`, `qpl_auto_fixed`, `qpl_auto_dynamic`, `zlib_software_fallback`, `qpl_hardware_fixed`, `qpl_hardware_dynamic`, or `zlib_accel`. |
| `workload` | `read_only` (`R:W 10:0`), `write_only` (`R:W 0:10`), `read_write_8_2`, `read_write_5_5`, or `read_write_2_8`. |
| `threads` | Worker thread count. |
| `codec` | `lz4`, `qpl`, or `zlib_accel`. |
| `engine` | `cpu`, `auto`, `hardware`, `zlib_software_fallback`, or `zlib_accel_preload`. |
| `qpl_mode` | `fixed`, `dynamic`, or `NA`. |
| `qps` | B+Tree operations per second. |
| `mismatches` | Correctness mismatches; must be `0`. |
| `ratio` | Whole-tree effective memory ratio: `total_bytes / compressed_bytes`. |
| `saved_pct` | Whole-tree resident memory saved percentage: `100 * (1 - compressed_bytes / total_bytes)`. |
| `read_p50_us`, `read_p99_us`, `read_p999_us` | Sampled point-read B+Tree API latency. |
| `write_p50_us`, `write_p99_us`, `write_p999_us` | Sampled point-write B+Tree API latency. |
| `wall_sec` | Timed workload wall-clock seconds reported by the benchmark. |
| `user_sec`, `sys_sec` | Timed workload user/system CPU seconds from `getrusage()`. |
| `cpu_pct` | Timed workload average host CPU percentage; `800%` means about eight logical CPUs busy. |
| `cpu_cores` | `cpu_pct / 100`, easier for comparing average core consumption. |
| `qps_per_cpu_core` | `qps / cpu_cores`; useful for evaluating host CPU efficiency. |
| `qpl_compress_calls`, `qpl_decompress_calls` | B+Tree-side QPL API call counters. |
| `zlib_compress_calls`, `zlib_decompress_calls` | B+Tree-side zlib API call counters. |
| `bg_*`, `fg_*` | Background/foreground compaction counters. |
| `log`, `cpu_log` | Per-run logs. |

Important rows for IAA comparison:

- `codec=lz4, engine=cpu`: CPU LZ4 baseline.
- `codec=qpl, engine=auto, qpl_mode=fixed`: QPL auto fixed Huffman.
- `codec=qpl, engine=auto, qpl_mode=dynamic`: QPL auto dynamic Huffman.
- `codec=zlib_accel, engine=zlib_software_fallback`: zlib API software fallback baseline, no preload.
- `codec=qpl, engine=hardware, qpl_mode=fixed`: forced hardware diagnostic, fixed Huffman.
- `codec=qpl, engine=hardware, qpl_mode=dynamic`: forced hardware diagnostic, dynamic Huffman.
- `codec=zlib_accel, engine=zlib_accel_preload`: Intel zlib-accel through `LD_PRELOAD`.

Do not treat the zlib API software fallback row as a hardware result. It exists to show the no-offload baseline for the same `COMPRESS_ZLIB_ACCEL` B+Tree path.

## Manual Single-Run Commands

QPL auto fixed:

```bash
BTREE_CODEC_FILTER=qpl \
BTREE_QPL_PATH=auto \
BTREE_QPL_MODE=fixed \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_THREADS=32 \
BTREE_DURATION_SEC=10 \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

QPL auto dynamic:

```bash
BTREE_CODEC_FILTER=qpl \
BTREE_QPL_PATH=auto \
BTREE_QPL_MODE=dynamic \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_THREADS=32 \
BTREE_DURATION_SEC=10 \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

zlib-accel:

```bash
LD_PRELOAD=<zlib-accel-build>/libzlib_accel.so \
BTREE_CODEC_FILTER=zlib_accel \
BTREE_QPL_PATH=auto \
BTREE_QPL_MODE=fixed \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_THREADS=32 \
BTREE_DURATION_SEC=10 \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

For normal QPL validation, prefer `BTREE_QPL_PATH=auto`. Use `BTREE_QPL_PATH=software` or `BTREE_QPL_PATH=hardware` only when you explicitly need to force a path for debugging.

## Workloads

`run_iaa_eval.sh` uses five point-operation workloads:

| Workload | Read | Write | Delete | Scan | Purpose |
|---|---:|---:|---:|---:|---|
| `R:W 10:0` (`read_only`) | 100% | 0% | 0% | 0% | Decompression/read-path throughput. |
| `R:W 0:10` (`write_only`) | 0% | 100% | 0% | 0% | Compression/write-path throughput. |
| `R:W 8:2` (`read_write_8_2`) | 80% | 20% | 0% | 0% | Read-dominant cache service path. |
| `R:W 5:5` (`read_write_5_5`) | 50% | 50% | 0% | 0% | Balanced point read/write pressure. |
| `R:W 2:8` (`read_write_2_8`) | 20% | 80% | 0% | 0% | Write-dominant compaction pressure. |

The script intentionally omits range scans from the default promotion matrix because point operations are the cleaner path for comparing codec and hardware acceleration.

## Important Runtime Variables

| Variable | Default | Meaning |
|---|---:|---|
| `QPL_EVAL_PATH` | `auto` | QPL path requested by the evaluation script: `auto`, `software`, or `hardware`. |
| `RUN_IAA` | `0` | Optional strict hardware diagnostic. Normal evaluation does not require this. |
| `RUN_QPL_DYNAMIC` | `1` | Include QPL dynamic Huffman run using `QPL_EVAL_PATH`. |
| `RUN_ZLIB_SOFTWARE_FALLBACK` | `1` | Include the zlib API software fallback baseline without `LD_PRELOAD`. |
| `RUN_ZLIB_ACCEL` | `0` | Include an `LD_PRELOAD` zlib-accel run. |
| `ZLIB_ACCEL_SO` | empty | Path to `libzlib_accel.so`; required when `RUN_ZLIB_ACCEL=1`. |
| `COLLECT_CPU` | `1` | Also collect per-codec process RSS/context-switch data with `/usr/bin/time`. |
| `LZ4_CODECS` | `lz4` | Codec filter for the LZ4 baseline run. |
| `QPL_AUTO_CODECS` | `qpl` | Codec filter for QPL auto fixed/dynamic runs. |
| `IAA_CODECS` | `qpl` | Codec filters for optional forced-hardware diagnostic runs. |
| `ZLIB_ACCEL_CODECS` | `zlib_accel` | Codec filters for the zlib-accel preload run. |
| `RUN_BG_SWEEP` | `0` | Run each workload with `BG_SWEEP_VALUES`, usually background off/on. |
| `LANDING_LIST` | `BTREE_LANDING_BUFFER_BYTES` | Landing-buffer sizes to sweep, for example `512 1024 2048`. |
| `THREADS_LIST` | `4 8 16 32` | Thread counts for throughput evaluation. |
| `BTREE_DURATION_SEC` | `5` | Timed duration for each throughput workload. |
| `BTREE_KEY_SPACE` | `50000` | Number of keys in the workload. |
| `BTREE_USE_SILESIA` | `1` | Use `SilesiaCorpus/samba.zip` as the value source. |
| `BTREE_VALUE_BYTES` | `128` | Bytes copied into the current fixed-width value slot. |
| `BTREE_SHARDS` | `8` | Hash shards behind the compressed B+Tree coordinator. |
| `BTREE_LANDING_BUFFER_BYTES` | `512` | Effective per-leaf landing buffer size. |
| `BTREE_QPL_JOB_CACHE` | `thread` | Reuse per-thread QPL jobs. |
| `BTREE_ZLIB_STREAM_CACHE` | `none` | `thread` reuses per-thread zlib streams. |
| `BTREE_BG_CODEC` | `all` | Restrict background compaction to `all`, `lz4`, `qpl`, or `zlib_accel`. |
| `BTREE_MEASURE_LATENCY` | `1` | Collect sampled point-read/write latency in `summary.tsv`. |

## Background Compaction Evaluation

Background compaction is optional and disabled by default. It can improve memory saving and reduce foreground synchronous compaction pressure, but on CPU-only runs it can reduce QPS due to CPU and lock contention. Treat it as an explicit experiment:

```bash
RUN_BG_SWEEP=1 \
BG_SWEEP_VALUES='0 1' \
BTREE_BG_THREADS=1 \
BTREE_BG_LANDING_HIGH_WATERMARK_PCT=75 \
DRAM-tier/tests/btree/run_iaa_eval.sh
```

On IAA hardware, rerun the same sweep because background work may hide foreground compression stalls, but it can also contend for IAA work queues.
