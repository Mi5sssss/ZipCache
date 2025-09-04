# ZipCache: Multi-Tier B+ Tree with Intel QPL Compression

A high-performance multi-tier B+ tree implementation featuring Intel Query Processing Library (QPL) compression engine alongside LZ4, designed for modern storage hierarchies.

## Features

- **Multi-Tier Architecture**: DRAM-tier, LO-tier, and SSD-tier B+ tree implementations
- **Intel QPL Integration**: Hardware-accelerated compression with software fallback
- **Dual Compression Engines**: LZ4 and QPL with user-selectable algorithms
- **High Performance**: Up to 21% better compression ratios and 15% higher throughput with QPL
- **Thread-Safe Operations**: Comprehensive locking mechanisms for concurrent access
- **Comprehensive Benchmarks**: Extensive performance testing and validation suites

## Performance Results

QPL consistently outperforms LZ4 across all metrics:

| Data Type | Engine | Compression Ratio | Throughput (ops/sec) | P99 Latency (μs) |
|-----------|--------|-------------------|---------------------|------------------|
| **Low Compressibility** | LZ4 | 1.143x | 252,832 | 4.77 |
| **Low Compressibility** | QPL | 1.213x | 291,327 | 4.05 |
| **Medium Compressibility** | LZ4 | 1.455x | 323,513 | 5.01 |
| **Medium Compressibility** | QPL | 1.594x | 370,753 | 2.86 |
| **High Compressibility** | LZ4 | 2.065x | 392,169 | 3.10 |
| **High Compressibility** | QPL | 2.493x | 404,204 | 2.86 |

**Key Advantages of QPL:**
- 6-21% better compression ratios
- 3-15% higher insertion throughput  
- 8-43% lower P99 latency
- Automatic hardware/software path selection

## Quick Start

### Building the Project

```bash
# Clone the repository
git clone https://github.com/Mi5sssss/ZipCache.git
cd ZipCache

# Build all tiers
mkdir build && cd build
cmake ..
make -j$(nproc)
```

### DRAM-Tier Usage

```c
#include "DRAM-tier/lib/bplustree_compressed.h"

// Initialize with QPL compression
struct compression_config config = {
    .algo = COMPRESS_QPL,  // or COMPRESS_LZ4
    .default_sub_pages = 16,
    .compression_level = 0
};

struct bplus_tree_compressed *tree = 
    bplus_tree_compressed_init_with_config(16, 64, &config);

// Insert data
bplus_tree_compressed_put(tree, key, value);

// Retrieve data
int result = bplus_tree_compressed_get(tree, key);

// Cleanup
bplus_tree_compressed_deinit(tree);
```

### Running Benchmarks

```bash
# DRAM-tier QPL vs LZ4 comparison
cd DRAM-tier/build
./bin/synthetic_compression_benchmark

# Individual smoke tests
./bin/bpt_compressed_lz4_smoke
./bin/bpt_compressed_qpl_smoke
```

## Architecture

### DRAM-Tier
- In-memory B+ tree with leaf node compression
- LZ4 and Intel QPL compression engines
- Thread-safe operations with fine-grained locking
- Configurable compression levels and buffer management

### LO-Tier (Large Object)
- Optimized for handling large objects
- Variable-length key/value support
- Efficient memory management for bulk data

### SSD-Tier
- Disk-based B+ tree with hybrid memory/storage design
- Super leaf splitting optimization
- Hashed I/O operations for improved performance

## Dependencies

- **Intel QPL Library**: For hardware-accelerated compression
- **LZ4**: Fast compression library
- **CMake**: Build system (3.10+)
- **GCC**: C compiler with C11 support

### Installing Intel QPL

```bash
# Ubuntu/Debian
sudo apt-get install intel-qpl-dev

# Or build from source
git clone https://github.com/intel/qpl.git
cd qpl && mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc) && sudo make install
```

## Project Structure

```
├── DRAM-tier/          # In-memory compressed B+ tree
│   ├── lib/            # Core library implementation
│   ├── examples/       # Usage examples and benchmarks
│   └── tests/          # Unit tests and smoke tests
├── LO-tier/            # Large object tier implementation  
├── SSD-tier/           # Disk-based B+ tree implementation
├── zipcache.c/h        # Multi-tier cache coordination
└── tests_legacy/      # Legacy test suites and results
```

## Testing

Comprehensive test suite included:

```bash
# Run all tests
make test

# Specific tier tests
cd DRAM-tier/build && ctest
cd LO-tier/build && ctest  
cd SSD-tier/build && ctest
```

## Benchmarks

Performance benchmarking tools:

- `synthetic_compression_benchmark`: Controlled compressibility testing
- `compression_benchmark`: Real-world data compression comparison
- `zipcache_performance_benchmark`: Multi-tier system evaluation

## License

This project is open source. See individual components for specific licensing terms.

## Contributing

Contributions welcome! Please ensure:
- Code follows existing style conventions
- Tests pass for all modified components
- Performance regressions are documented
- New features include appropriate benchmarks

## Citation

If you use ZipCache in your research, please cite:

```bibtex
@misc{zipcache2025,
  title={ZipCache: Multi-Tier B+ Tree with Intel QPL Compression},
  author={xier2},
  year={2025},
  url={https://github.com/Mi5sssss/ZipCache}
}
```

---

**Performance Note**: QPL hardware acceleration provides even better performance than software-only results shown above. Deploy on QPL-capable hardware for optimal performance.