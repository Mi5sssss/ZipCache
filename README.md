# ZipCache (DRAM/SSD Cache with Built‑in Transparent Compression)

## Quick Start

> [!NOTE]
> If you want to use the Silesia corpus for benchmarks, please run `git submodule update --init SilesiaCorpus` first.

All active development and tests build out of the `DRAM-tier` subdirectory. The
top-level `build` folder is kept only for legacy binaries; please work inside
`DRAM-tier` instead.

```bash
cd DRAM-tier
cmake -S . -B build
cmake --build build -j$(nproc)
```

Key executables live in `DRAM-tier/build/bin/`. Examples:

- `tail_latency_compare` – synthetic latency mixes (read-only, 80/20 read/write)
- `dcperf_workload_benchmark` – TaoBench-inspired size distribution and mix
- `bpt_compressed_lz4_smoke`, `bpt_compressed_qpl_smoke` – basic smoke tests
- `bpt_compressed_synthetic_test` – legacy synthetic benchmark

## Benchmark Scripts (LZ4 vs QPL)

- KV-shaped microbench (Silesia payload by default): `DRAM-tier/tests/run_kv_bench.sh`
  - Env knobs: `KV_BLOCK_SIZES` (default `4096 8192 16384`), `KV_QPL_PATH` (`software|hardware|auto`, default `software`), `KV_OCCUPANCY_PCT` (default 50), `KV_BLOCKS` (default 4096).
- Tree end-to-end (rebuild per leaf size): `DRAM-tier/tests/run_tail_latency_sizes.sh`
  - Env knob: `COMPRESSED_LEAF_SIZES` (default `4096 8192 16384`). Script edits `COMPRESSED_LEAF_SIZE`/`MAX_COMPRESSED_SIZE`, rebuilds `tail_latency_compare`, runs with `TAIL_LATENCY_USE_SILESIA=1`, and restores the header afterward.

Quick one-click runs (after building `DRAM-tier/build`):

```bash
# Microbench across 4K/8K/16K, software QPL, Silesia payload
sh ./tests/run_kv_bench.sh

# Tree benchmark across 4K/8K/16K leaf sizes (rebuilds per size, restores header)
sh ./tests/run_tail_latency_sizes.sh

# Multi-threaded Throughput Benchmark (C++)
# Measures scalability of QPL vs LZ4.
# Set KV_THREADS to your CPU core count (e.g., 32).
KV_THREADS=32 ./build/bin/qpl_lz4_kv_bench_mt
```

### Software-path results (Silesia)

KV microbench (`run_kv_bench.sh`, occ=50%, blocks=4096, QPL=software): simulates leaf-shaped key/value payloads only (no tree traversal), to isolate codec throughput/latency. It performs a fresh compress/decompress per block (no landing buffer or other amortization), so absolute numbers can be lower than end-to-end trees where some work is avoided.

| Block | LZ4 comp p50/p90/p95/p99 (µs) | LZ4 decomp p50/p90/p95/p99 (µs) | LZ4 ratio | LZ4  (comp/decomp) | QPL comp p50/p90/p95/p99 (µs) | QPL decomp p50/p90/p95/p99 (µs) | QPL ratio | QPL  (comp/decomp) |
|-------|-------------------------------|----------------------------------|-----------|------------------------|--------------------------------|----------------------------------|-----------|-------------------------|
| 4KB   | 9.7 / 11.6 / 12.2 / 17.5      | 1.58 / 1.84 / 1.93 / 2.13        | 2.63x     | ~410 MB/s / 2627 MB/s  | 18.98 / 22.04 / 23.01 / 25.21  | 7.80 / 9.30 / 9.85 / 10.84       | 2.55x     | ~206 MB/s / 503 MB/s    |
| 8KB   | 13.57 / 18.24 / 19.53 / 22.72 | 2.83 / 3.17 / 3.28 / 3.73        | 3.21x     | ~560 MB/s / 2858 MB/s  | 31.38 / 38.48 / 40.54 / 43.79  | 13.01 / 16.44 / 17.45 / 19.30    | 3.11x     | ~251 MB/s / 607 MB/s    |
| 16KB  | 26.87 / 31.33 / 32.70 / 36.83 | 5.72 / 6.21 / 6.38 / 6.97        | 3.26x     | ~606 MB/s / 2862 MB/s  | 62.60 / 73.89 / 77.38 / 83.23  | 25.14 / 30.44 / 32.49 / 35.90    | 3.06x     | ~254 MB/s / 628 MB/s    |

Tree end-to-end `tail_latency_compare` (`run_tail_latency_sizes.sh`, read-only p50, throughput Mops/s): includes tree traversal/buffering overhead in addition to codec cost.

| Leaf size | LZ4 p50 (µs) | LZ4 throughput (Mops/s) | LZ4 ratio | QPL p50 (µs) | QPL throughput (Mops/s) | QPL ratio |
|-----------|--------------|-------------------------|-----------|--------------|-------------------------|-----------|
| 4KB       | 0.923        | 1.13                    | 1.75x     | 5.77         | 0.19                    | 1.67x     |
| 8KB       | 1.522        | 0.66                    | 2.07x     | 9.94         | 0.11                    | 1.94x     |
| 16KB      | 3.053        | 0.33                    | 2.23x     | 19.35        | 0.05                    | 2.08x     |

Notes: All QPL numbers above are software path; hardware QPL (if available) may differ. Microbench isolates codec cost; tree results include traversal and buffering overhead.

### Test environment

- CPU: Intel Xeon Gold 6134 @ 3.20GHz (x86_64, 32 cores, 46-bit physical / 48-bit virtual address, little endian).

## Key Paths

- DRAM tier library: `DRAM-tier/lib/` (e.g., `bplustree_compressed.c`)
- Benchmarks: `DRAM-tier/build/` (e.g., `2025-09-04_synthetic_compression_benchmark.md`)
- SSD/LO tiers: `SSD-tier/`, `LO-tier/`

## Minimal Usage (DRAM Tier)

```c
#include "DRAM-tier/lib/bplustree_compressed.h"

struct compression_config cfg = {
  .algo = COMPRESS_QPL,   // or COMPRESS_LZ4
  .default_sub_pages = 16,
  .compression_level = 0
};

struct bplus_tree_compressed *t =
  bplus_tree_compressed_init_with_config(16, 64, &cfg);

bplus_tree_compressed_put(t, key, value);
bplus_tree_compressed_get(t, key);
bplus_tree_compressed_deinit(t);
```

## Notes

- Use Intel QPL to offload (de)compression when supported; falls back to software.
- Super‑leaf under‑filling improves in‑SSD write reduction; page‑based DRAM→SSD eviction reduces host‑side write amplification.

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
