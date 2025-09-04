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

- Build
  - `mkdir build && cd build && cmake .. && make -j$(nproc)`
- DRAM tier benchmark
  - `cd DRAM-tier/build && ./bin/synthetic_compression_benchmark`
- Smoke tests
  - `./bin/bpt_compressed_lz4_smoke` and `./bin/bpt_compressed_qpl_smoke`

## Performance Results (kept from original)

QPL consistently outperforms LZ4 across all metrics:

| Data Type | Engine | Compression Ratio | Throughput (ops/sec) | P99 Latency (μs) |
|-----------|--------|-------------------|----------------------|------------------|
| Low Compressibility | LZ4 | 1.143x | 252,832 | 4.77 |
| Low Compressibility | QPL | 1.213x | 291,327 | 4.05 |
| Medium Compressibility | LZ4 | 1.455x | 323,513 | 5.01 |
| Medium Compressibility | QPL | 1.594x | 370,753 | 2.86 |
| High Compressibility | LZ4 | 2.065x | 392,169 | 3.10 |
| High Compressibility | QPL | 2.493x | 404,204 | 2.86 |

Key advantages of QPL:
- 6–21% better compression ratios
- 3–15% higher insertion throughput
- 8–43% lower P99 latency
- Automatic hardware/software path selection

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
