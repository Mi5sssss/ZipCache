# ZipCache DRAM-tier B+Tree Codec and IAA Evaluation

This document contains the benchmark commands and result tables for the ZipCache DRAM-tier compressed B+Tree. The main repository README is intentionally product-level; keep detailed evaluation methodology here.

## Evaluation Scope

The benchmark compares real compressed B+Tree operations, not standalone codec microbenchmarks.

Codecs and engines:

- `LZ4`: CPU baseline.
- `QPL software fixed`: Intel QPL deflate software path with fixed Huffman mode.
- `QPL IAA fixed`: Intel QPL hardware path with fixed Huffman mode.
- `QPL IAA dynamic`: Intel QPL hardware path with dynamic Huffman mode.
- `zlib-accel`: Intel zlib-accel shim through `LD_PRELOAD=<zlib-accel-build>/libzlib_accel.so`.

Regular system zlib is not a headline comparison target. In this codebase, `COMPRESS_ZLIB_ACCEL` calls the zlib API. It only becomes Intel zlib-accel when the process is run with the zlib-accel shared library preloaded. If the Intel shim cannot offload to IAA/QAT and zlib fallback is enabled in the shim configuration, the shim falls back to zlib internally; this should still be reported as `engine=zlib_accel_preload`, not as the system zlib baseline.

## Build

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

## CPU-Aware Software Baseline Results

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
| QPL mode | software path, fixed Huffman |
| CPU metrics | timed-window `getrusage()` per codec process |
| Correctness | all rows had `mismatches=0` |

Result directory:

```text
DRAM-tier/tests/btree/results/cpu_aware_main/20260418T021637Z/
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
DRAM-tier/tests/btree/run_iaa_eval.sh
```

### 8-Thread Results

| Workload | Codec | QPS (Mops/s) | Read p99 us | Read p999 us | Write p99 us | Write p999 us | CPU cores | Kops/s/core | Saved % |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| read-only | LZ4 | 3.68 | 3.162 | 4.501 | NA | NA | 8.00 | 460.6 | 18.87 |
| write-only | LZ4 | 1.97 | NA | NA | 19.220 | 29.660 | 7.98 | 247.4 | 24.94 |
| R:W 8:2 | LZ4 | 2.86 | 4.315 | 12.501 | 17.695 | 29.001 | 7.99 | 358.4 | 24.70 |
| R:W 5:5 | LZ4 | 2.38 | 4.464 | 13.606 | 17.295 | 29.875 | 7.98 | 297.6 | 25.18 |
| R:W 2:8 | LZ4 | 2.10 | 4.476 | 14.838 | 16.129 | 23.362 | 7.98 | 263.7 | 25.06 |
| read-only | QPL software fixed | 0.93 | 18.207 | 29.946 | NA | NA | 8.00 | 116.1 | 16.58 |
| write-only | QPL software fixed | 0.83 | NA | NA | 52.549 | 77.309 | 7.97 | 104.3 | 22.27 |
| R:W 8:2 | QPL software fixed | 0.73 | 21.855 | 34.928 | 53.303 | 75.660 | 7.99 | 91.9 | 21.42 |
| R:W 5:5 | QPL software fixed | 0.74 | 23.236 | 34.852 | 60.474 | 74.337 | 7.98 | 92.4 | 22.05 |
| R:W 2:8 | QPL software fixed | 0.79 | 22.252 | 43.173 | 52.061 | 71.633 | 7.97 | 99.6 | 22.20 |

### 32-Thread Results

| Workload | Codec | QPS (Mops/s) | Read p99 us | Read p999 us | Write p99 us | Write p999 us | CPU cores | Kops/s/core | Saved % |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| read-only | LZ4 | 9.77 | 3.306 | 6.553 | NA | NA | 22.11 | 441.7 | 18.87 |
| write-only | LZ4 | 3.82 | NA | NA | 18.217 | 1476.349 | 16.11 | 237.2 | 24.92 |
| R:W 8:2 | LZ4 | 5.16 | 4.026 | 225.811 | 20.103 | 3134.183 | 15.33 | 336.5 | 25.07 |
| R:W 5:5 | LZ4 | 4.66 | 5.119 | 770.157 | 19.912 | 2000.469 | 14.92 | 312.0 | 25.10 |
| R:W 2:8 | LZ4 | 3.89 | 4.767 | 828.165 | 17.801 | 1680.226 | 15.05 | 258.5 | 24.99 |
| read-only | QPL software fixed | 2.46 | 18.218 | 31.550 | NA | NA | 23.26 | 105.7 | 16.58 |
| write-only | QPL software fixed | 1.57 | NA | NA | 57.997 | 4905.686 | 16.38 | 95.9 | 22.34 |
| R:W 8:2 | QPL software fixed | 1.61 | 22.143 | 2281.299 | 52.173 | 6293.935 | 18.94 | 84.8 | 21.98 |
| R:W 5:5 | QPL software fixed | 1.43 | 22.861 | 2816.751 | 54.713 | 5685.689 | 16.33 | 87.5 | 22.43 |
| R:W 2:8 | QPL software fixed | 1.46 | 23.717 | 3474.000 | 56.207 | 5009.532 | 15.98 | 91.3 | 22.26 |

Interpretation:

- On this non-IAA machine, CPU LZ4 is the throughput baseline to beat.
- QPL software fixed is slower than LZ4 because this is still CPU-side deflate, not IAA offload.
- The Intel experiment is to rerun this matrix with `BTREE_QPL_PATH=hardware` and/or `RUN_ZLIB_ACCEL=1 ZLIB_ACCEL_SO=<zlib-accel-build>/libzlib_accel.so`, then compare `QPS`, latency, `CPU cores`, and `Kops/s/core`.
- A useful IAA result can be either higher throughput than LZ4 or much lower CPU cores at similar throughput.

## One-Command IAA Evaluation

The evaluation script runs real B+Tree `put/get` workloads and writes a normalized TSV summary with codec, engine, QPL mode, QPS, sampled latency, host CPU usage, correctness mismatches, and whole-tree compression ratio.

By default, `run_iaa_eval.sh` runs each codec in a separate process. The benchmark reports timed-window CPU usage through `getrusage()`, while the script also wraps the process with `/usr/bin/time` for RSS and context-switch counters. Per-codec CPU metrics are only meaningful when LZ4, QPL, and zlib-accel are not mixed in the same process.

Software-only baseline, no IAA required:

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

QPL IAA fixed/dynamic:

```bash
cd DRAM-tier
../scripts/enable_iax_user
accel-config list | grep iax
cd ..

RUN_IAA=1 \
THREADS_LIST='4 8 16 32' \
BTREE_DURATION_SEC=5 \
BTREE_KEY_SPACE=50000 \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_LANDING_BUFFER_BYTES=512 \
DRAM-tier/tests/btree/run_iaa_eval.sh
```

QPL IAA plus Intel zlib-accel:

```bash
RUN_IAA=1 \
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

IAA performance sprint sweep:

```bash
RUN_IAA=1 \
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
| `run` | Evaluation configuration, for example `software_fixed`, `qpl_iaa_fixed`, `qpl_iaa_dynamic`, or `zlib_accel`. |
| `workload` | `read_only`, `write_only`, `read_write_8_2`, `read_write_5_5`, or `read_write_2_8`. |
| `threads` | Worker thread count. |
| `codec` | `lz4`, `qpl`, or `zlib_accel`. |
| `engine` | `cpu`, `software`, `hardware`, `none`, or `zlib_accel_preload`. |
| `qpl_mode` | `fixed`, `dynamic`, or `NA`. |
| `qps` | B+Tree operations per second. |
| `mismatches` | Correctness mismatches; must be `0`. |
| `ratio` | Whole-tree effective memory ratio: `total_bytes / compressed_bytes`. |
| `saved_pct` | Whole-tree memory saved percentage. |
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
- `codec=qpl, engine=software, qpl_mode=fixed`: QPL software baseline.
- `codec=qpl, engine=hardware, qpl_mode=fixed`: IAA fixed Huffman.
- `codec=qpl, engine=hardware, qpl_mode=dynamic`: IAA dynamic Huffman.
- `codec=zlib_accel, engine=zlib_accel_preload`: Intel zlib-accel through `LD_PRELOAD`.

Do not use `codec=zlib_accel, engine=zlib_system` for headline claims. That path is only regular system zlib through the zlib API.

## Manual Single-Run Commands

QPL software fixed:

```bash
BTREE_CODEC_FILTER=qpl \
BTREE_QPL_PATH=software \
BTREE_QPL_MODE=fixed \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_THREADS=32 \
BTREE_DURATION_SEC=10 \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

QPL IAA dynamic:

```bash
BTREE_CODEC_FILTER=qpl \
BTREE_QPL_PATH=hardware \
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
BTREE_QPL_PATH=software \
BTREE_QPL_MODE=fixed \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_THREADS=32 \
BTREE_DURATION_SEC=10 \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

For QPL IAA validation, prefer `BTREE_QPL_PATH=hardware` rather than `auto`. Hardware mode fails fast when IAA is unavailable, which avoids hiding setup or queue-submission issues behind software fallback.

## Workloads

`run_iaa_eval.sh` uses five point-operation workloads:

| Workload | Read | Write | Delete | Scan | Purpose |
|---|---:|---:|---:|---:|---|
| `read_only` | 100% | 0% | 0% | 0% | Decompression/read-path throughput. |
| `write_only` | 0% | 100% | 0% | 0% | Compression/write-path throughput. |
| `read_write_8_2` | 80% | 20% | 0% | 0% | Read-dominant cache service path. |
| `read_write_5_5` | 50% | 50% | 0% | 0% | Balanced point read/write pressure. |
| `read_write_2_8` | 20% | 80% | 0% | 0% | Write-dominant compaction pressure. |

The script intentionally omits range scans from the default promotion matrix because point operations are the cleaner path for comparing codec and hardware acceleration.

## Important Runtime Variables

| Variable | Default | Meaning |
|---|---:|---|
| `RUN_IAA` | `0` | Run QPL with `BTREE_QPL_PATH=hardware` fixed and dynamic modes. |
| `RUN_QPL_DYNAMIC` | `1` | Include QPL dynamic Huffman hardware run when `RUN_IAA=1`. |
| `RUN_ZLIB_ACCEL` | `0` | Include an `LD_PRELOAD` zlib-accel run. |
| `ZLIB_ACCEL_SO` | empty | Path to `libzlib_accel.so`; required when `RUN_ZLIB_ACCEL=1`. |
| `COLLECT_CPU` | `1` | Also collect per-codec process RSS/context-switch data with `/usr/bin/time`. |
| `SOFTWARE_CODECS` | `lz4 qpl` | Codec filters for the software baseline run. |
| `IAA_CODECS` | `qpl` | Codec filters for QPL IAA fixed/dynamic runs. |
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
