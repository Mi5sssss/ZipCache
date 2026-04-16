# ZipCache DRAM-tier B+Tree Compression Tests

This repository currently focuses on the ZipCache DRAM-tier compressed B+Tree prototype. The active tests compare three compression paths:

- `LZ4`: CPU LZ4 baseline.
- `QPL`: Intel QPL deflate path, configurable as software or IAA hardware.
- `zlib_accel`: zlib API path. By default this is the system zlib backend; with `LD_PRELOAD=/path/to/libzlib_accel.so`, Intel zlib-accel can transparently choose hardware acceleration when available.

The main goal of these tests is to measure real B+Tree behavior: tree traversal, leaf compression/decompression, landing buffer effects, sharding, concurrency, and whole-tree effective compression ratio.

## Build

From the repository root:

```bash
cd /home/xier2/2025-03-17-intel-zipcache/bplustree/bplustree
git submodule update --init SilesiaCorpus

cmake -S DRAM-tier -B DRAM-tier/build_check
cmake --build DRAM-tier/build_check -j$(nproc)
```

Main binaries are under:

```text
DRAM-tier/build_check/bin/
```

## What To Run First

Run correctness smoke tests before collecting performance numbers:

```bash
DRAM-tier/build_check/bin/bpt_compressed_lz4_smoke
DRAM-tier/build_check/bin/bpt_compressed_qpl_smoke
DRAM-tier/build_check/bin/bpt_compressed_zlib_accel_smoke
DRAM-tier/build_check/bin/bpt_compressed_crud_fuzz
DRAM-tier/build_check/bin/test_compression_concurrency
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
DRAM-tier/build_check/bin/bpt_compressed_split_payload_stats
```

Expected result: all commands should pass. For `bpt_compressed_mixed_concurrency`, all printed codec rows should show `mismatches=0`.

Do not use `BTREE_OUT_OF_LOCK_REBUILD=1` for baseline claims. It is an experimental write-path rebuild prototype.

## B+Tree Performance Tests

There are two primary real B+Tree performance tests.

### Mixed Concurrency Test

`bpt_compressed_mixed_concurrency` runs real tree operations with configurable read/write/delete/scan ratios. It first populates the tree, then starts the timed workload.

Short local run:

```bash
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_LANDING_BUFFER_BYTES=512 \
BTREE_THREADS=4 \
BTREE_DURATION_SEC=1 \
BTREE_KEY_SPACE=4096 \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

Longer stress run:

```bash
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_LANDING_BUFFER_BYTES=512 \
BTREE_THREADS=32 \
BTREE_DURATION_SEC=10 \
BTREE_KEY_SPACE=50000 \
BTREE_HOT_PCT=100 \
BTREE_READ_PCT=50 \
BTREE_WRITE_PCT=35 \
BTREE_DELETE_PCT=10 \
BTREE_SCAN_PCT=5 \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

This binary runs LZ4, QPL, and zlib_accel in one execution and prints one result line per codec:

```text
mixed_concurrency[lz4]: ...
mixed_concurrency[qpl]: ...
mixed_concurrency[zlib_accel]: ...
```

### Per-Operation Throughput Test

`bpt_compressed_throughput_bench` reports warmup, put/get/range/delete/stats, and mixed-workload throughput.

```bash
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_LANDING_BUFFER_BYTES=512 \
BENCH_THREADS=4 \
BENCH_DURATION_SEC=1 \
BENCH_WARMUP_KEYS=1000 \
BENCH_KEY_SPACE=4096 \
BENCH_OPS=1000 \
DRAM-tier/build_check/bin/bpt_compressed_throughput_bench
```

Use a longer run for stable numbers:

```bash
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_LANDING_BUFFER_BYTES=512 \
BENCH_THREADS=32 \
BENCH_DURATION_SEC=10 \
BENCH_WARMUP_KEYS=10000 \
BENCH_KEY_SPACE=50000 \
BENCH_OPS=10000 \
DRAM-tier/build_check/bin/bpt_compressed_throughput_bench
```

## Scaling Sweep

Use the sweep script to run multiple workloads, thread counts, shard counts, and landing-buffer sizes.

```bash
SHARDS_LIST='1 8' \
LANDING_LIST='512 2048' \
THREADS_LIST='1 4 8 16 32' \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_DURATION_SEC=2 \
BTREE_KEY_SPACE=50000 \
BENCH_DURATION_SEC=2 \
BENCH_WARMUP_KEYS=10000 \
BENCH_KEY_SPACE=50000 \
DRAM-tier/tests/btree/run_scaling_sweep.sh
```

Output:

```text
DRAM-tier/tests/btree/results/<timestamp>/summary.tsv
DRAM-tier/tests/btree/results/<timestamp>/*.log
```

`summary.tsv` columns:

| Column | Meaning |
|---|---|
| `case` | Workload type: `read_only`, `read_heavy`, `point_mixed`, `mixed_with_scan`, `write_heavy`, `bench_mixed`. |
| `value_source` | `1` means Silesia samba payloads; `0` means synthetic payloads. |
| `value_bytes` | Payload bytes copied into the current fixed-width record format. |
| `landing_bytes` | Effective per-leaf software landing buffer size. |
| `shards` | Number of hash shards behind the compressed tree coordinator. |
| `threads` | Worker thread count. |
| `codec` | `lz4`, `qpl`, or `zlib_accel`. |
| `qps` | Operations per second. |
| `mismatches` | Correctness mismatches; should be `0`. |
| `ratio` | Whole-tree effective memory ratio: `total_bytes / compressed_bytes`. |
| `saved_pct` | Whole-tree memory saved percentage. |
| `compressed_bytes` | Resident compressed bytes plus uncompressed landing-buffer bytes. |
| `total_bytes` | Resident logical KV bytes represented by the tree. |
| `log` | Full log file for that run. |

Note: `read_only` still has `ratio` because the benchmark populates the B+Tree before the timed read-only phase. The ratio is the final whole-tree resident memory ratio, not compression performed by read operations.

## Switching Compression Paths

### LZ4

LZ4 is always included in B+Tree benchmark binaries. No environment variable is needed:

```bash
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

Look for:

```text
mixed_concurrency[lz4]
```

### QPL Software

Use QPL software mode when no IAA hardware is available or when collecting a software baseline:

```bash
BTREE_QPL_PATH=software \
BTREE_QPL_MODE=fixed \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

Dynamic Huffman mode:

```bash
BTREE_QPL_PATH=software \
BTREE_QPL_MODE=dynamic \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

Look for:

```text
mixed_concurrency[qpl]
```

### QPL IAA Hardware

Use this only on an Intel IAA-capable machine. Hardware mode must fail fast if IAA is unavailable; this is intentional because silent fallback hides hardware setup or queue contention problems.

Prepare IAA work queues:

```bash
cd /home/xier2/2025-03-17-intel-zipcache/bplustree/bplustree/DRAM-tier
../scripts/enable_iax_user
accel-config list | grep iax
```

Run fixed Huffman on hardware:

```bash
cd /home/xier2/2025-03-17-intel-zipcache/bplustree/bplustree
BTREE_QPL_PATH=hardware \
BTREE_QPL_MODE=fixed \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_LANDING_BUFFER_BYTES=512 \
BTREE_THREADS=32 \
BTREE_DURATION_SEC=10 \
BTREE_KEY_SPACE=50000 \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

Run dynamic Huffman on hardware:

```bash
BTREE_QPL_PATH=hardware \
BTREE_QPL_MODE=dynamic \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_LANDING_BUFFER_BYTES=512 \
BTREE_THREADS=32 \
BTREE_DURATION_SEC=10 \
BTREE_KEY_SPACE=50000 \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

For IAA validation, do not use `BTREE_QPL_PATH=auto`. Use `hardware` so failures are visible.

### zlib and Intel zlib-accel

Without `LD_PRELOAD`, `zlib_accel` uses the normal zlib API backend:

```bash
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

With Intel zlib-accel:

```bash
LD_PRELOAD=/path/to/libzlib_accel.so \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_THREADS=32 \
BTREE_DURATION_SEC=10 \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

Look for:

```text
mixed_concurrency[zlib_accel]
```

## Comparing Software vs IAA Hardware

Run the same workload twice: first with `BTREE_QPL_PATH=software`, then with `BTREE_QPL_PATH=hardware`.

Software baseline:

```bash
BTREE_QPL_PATH=software \
BTREE_QPL_MODE=fixed \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_LANDING_BUFFER_BYTES=512 \
BTREE_THREADS=32 \
BTREE_DURATION_SEC=10 \
BTREE_KEY_SPACE=50000 \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

IAA hardware:

```bash
BTREE_QPL_PATH=hardware \
BTREE_QPL_MODE=fixed \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_LANDING_BUFFER_BYTES=512 \
BTREE_THREADS=32 \
BTREE_DURATION_SEC=10 \
BTREE_KEY_SPACE=50000 \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

Compare the `mixed_concurrency[qpl]` rows:

```text
qps=...
ratio=...
saved_pct=...
mismatches=0
```

For a full sweep:

```bash
BTREE_QPL_PATH=software \
BTREE_QPL_MODE=fixed \
SHARDS_LIST='8' \
LANDING_LIST='512 2048' \
THREADS_LIST='1 4 8 16 32' \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
DRAM-tier/tests/btree/run_scaling_sweep.sh

BTREE_QPL_PATH=hardware \
BTREE_QPL_MODE=fixed \
SHARDS_LIST='8' \
LANDING_LIST='512 2048' \
THREADS_LIST='1 4 8 16 32' \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
DRAM-tier/tests/btree/run_scaling_sweep.sh
```

If hardware QPS regresses at high thread counts while IAA utilization is not saturated, inspect IAA work queue count and thread-to-queue distribution. This was the suspected issue in prior Intel analysis.

## Non-B+Tree Codec Benchmark

`qpl_lz4_mixed_workload` does not run the B+Tree. It is useful for isolating codec behavior on ZipCache-like 4KB blocks.

LZ4:

```bash
cd /home/xier2/2025-03-17-intel-zipcache/bplustree/bplustree/DRAM-tier
KV_CODEC=lz4 \
KV_BLOCKS=100000 \
KV_THREADS=32 \
KV_DURATION_SEC=60 \
./build_check/bin/qpl_lz4_mixed_workload
```

QPL software:

```bash
KV_CODEC=qpl \
KV_QPL_PATH=software \
KV_QPL_MODE=fixed \
KV_BLOCKS=100000 \
KV_THREADS=32 \
KV_DURATION_SEC=60 \
./build_check/bin/qpl_lz4_mixed_workload
```

QPL hardware:

```bash
KV_CODEC=qpl \
KV_QPL_PATH=hardware \
KV_QPL_MODE=fixed \
KV_BLOCKS=100000 \
KV_THREADS=32 \
KV_DURATION_SEC=60 \
./build_check/bin/qpl_lz4_mixed_workload
```

zlib-accel path:

```bash
LD_PRELOAD=/path/to/libzlib_accel.so \
KV_CODEC=zlib_accel \
KV_BLOCKS=100000 \
KV_THREADS=32 \
KV_DURATION_SEC=60 \
./build_check/bin/qpl_lz4_mixed_workload
```

## Important Environment Variables

| Variable | Values | Default | Used by | Meaning |
|---|---|---:|---|---|
| `BTREE_QPL_PATH` | `auto`, `software`, `hardware` | `auto` | B+Tree tests | QPL execution path. Use `hardware` for IAA validation. |
| `BTREE_QPL_MODE` | `fixed`, `dynamic` | `fixed` | B+Tree tests | QPL Huffman mode. |
| `BTREE_USE_SILESIA` | `0`, `1` | `1` in sweep | B+Tree tests | Use `SilesiaCorpus/samba.zip` as real payload source. |
| `BTREE_VALUE_BYTES` | bytes | `128` | B+Tree tests | Payload bytes copied into fixed-width records. |
| `BTREE_SHARDS` | integer | `1` | B+Tree tests | Hash-sharded compressed B+Tree count. |
| `BTREE_LANDING_BUFFER_BYTES` | `256`, `512`, `1024`, `2048` | `512` | B+Tree tests | Effective per-leaf software landing buffer size. |
| `BTREE_THREADS` | integer | test-specific | `bpt_compressed_mixed_concurrency` | Worker threads. |
| `BTREE_DURATION_SEC` | seconds | `2` | `bpt_compressed_mixed_concurrency` | Timed workload duration. |
| `BTREE_KEY_SPACE` | integer | `50000` in sweep | B+Tree tests | Number of keys used by the workload. |
| `BENCH_THREADS` | integer | test-specific | `bpt_compressed_throughput_bench` | Worker threads for mixed phase. |
| `BENCH_DURATION_SEC` | seconds | `3` | `bpt_compressed_throughput_bench` | Mixed phase duration. |
| `KV_CODEC` | `lz4`, `qpl`, `zlib_accel` | `lz4` | codec benchmark | Select one codec for non-B+Tree benchmark. |
| `KV_QPL_PATH` | `auto`, `software`, `hardware` | `auto` | codec benchmark | QPL execution path. |
| `KV_QPL_MODE` | `fixed`, `dynamic` | `fixed` | codec benchmark | QPL Huffman mode. |

## Current Local Reference Results

Short Silesia samba 128B sweep, 4 threads, 8 shards:

| Case | Landing bytes | Codec | QPS | Mismatches | Ratio | Saved pct |
|---|---:|---|---:|---:|---:|---:|
| read_only | 512 | lz4 | 3,043,055.4 | 0 | 1.265 | 20.95 |
| read_only | 512 | qpl | 622,578.2 | 0 | 1.240 | 19.35 |
| read_only | 512 | zlib_accel | 333,647.6 | 0 | 1.395 | 28.34 |
| write_heavy | 512 | lz4 | 1,095,140.6 | 0 | 1.328 | 24.69 |
| write_heavy | 512 | qpl | 424,085.0 | 0 | 1.295 | 22.75 |
| write_heavy | 512 | zlib_accel | 152,430.2 | 0 | 1.464 | 31.68 |
| read_only | 2048 | lz4 | 5,030,195.5 | 0 | 1.013 | 1.31 |
| read_only | 2048 | qpl | 4,522,192.6 | 0 | 1.013 | 1.30 |
| read_only | 2048 | zlib_accel | 3,779,495.1 | 0 | 1.017 | 1.70 |
| write_heavy | 2048 | lz4 | 2,010,276.6 | 0 | 1.138 | 12.09 |
| write_heavy | 2048 | qpl | 1,111,282.8 | 0 | 1.123 | 10.92 |
| write_heavy | 2048 | zlib_accel | 626,590.9 | 0 | 1.165 | 14.18 |

Interpretation:

- Larger landing buffers improve short-run QPS because they reduce recompression and QPL/zlib submissions.
- Larger landing buffers reduce effective memory savings because more data remains uncompressed in the landing buffer.
- 512B is the safer default for capacity-oriented ZipCache evaluation. 2048B is a throughput-oriented knob.

## Current Limitations

- The B+Tree still uses fixed integer keys and fixed inline payload slots:

```c
typedef int key_t;

struct kv_pair {
    key_t key;
    int stored_value;
    uint8_t payload[COMPRESSED_VALUE_BYTES];
};
```

- `BTREE_VALUE_BYTES=128` is an interim benchmark format, not final arbitrary key/value support.
- The next design step is a variable-length byte-key/byte-value API with a slotted leaf-page layout.
- `BTREE_OUT_OF_LOCK_REBUILD=1` is experimental and should not be used for baseline claims.
- Hardware IAA results must be collected on an IAA-capable machine with configured work queues.

## Key Paths

- Compressed B+Tree library: `DRAM-tier/lib/bplustree_compressed.c`
- B+Tree tests: `DRAM-tier/tests/btree/`
- Codec-only tests: `DRAM-tier/tests/codec/`
- Test docs: `DRAM-tier/tests/btree/README.md`
- Concurrency plan: `DRAM-tier/tests/btree/CONCURRENCY_SCALING_PLAN.md`

## Citation

```bibtex
@inproceedings{xie2024zipcache,
  title={ZipCache: A DRAM/SSD Cache with Built-in Transparent Compression},
  author={Xie, Rui and Ma, Linsen and Zhong, Alex and Chen, Feng and Zhang, Tong},
  booktitle={Proceedings of the International Symposium on Memory Systems},
  pages={116--128},
  year={2024}
}
```
