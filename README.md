# ZipCache (DRAM/SSD Cache with Built‑in Transparent Compression)

Implementation of the ZipCache design from MEMSYS’24: a hybrid DRAM/SSD cache that treats compression as a first‑class primitive. This repo provides B+‑tree–based tiers with compressed leaves, decompression early termination, and SSD‑oriented super‑leaf layout.

Paper: Xie, Ma, Zhong, Chen, Zhang. “ZipCache: A DRAM/SSD Cache with Built‑in Transparent Compression.” MEMSYS 2024.

## What It Implements

- **B+ tree indexing**: Keys kept sorted to improve compressibility and locality.
- **DRAM tier (`BT_DRAM`)**: Compressed leaf pages, hash‑guided sub‑pages for just‑in‑need decompression (early termination), per‑page write buffering.
- **SSD tier (`BT_SSD`)**: Super‑leaf pages (e.g., 16–64KB) partitioned into 4KB sub‑pages to decouple page size from I/O granularity; supports under‑filling for higher in‑SSD compression.
- **Large objects (`BT_LO`)**: SSD‑resident, 4KB‑aligned storage with in‑memory index.
- **Compression engines**: LZ4 and Intel QPL (hardware‑assisted path when available).

## Quick Start

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
