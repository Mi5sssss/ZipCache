# ZipCache DRAM-tier Build Instructions

## Prerequisites

### Required Dependencies
- **CMake** >= 3.15
- **GCC/G++** with C++11 support
- **LZ4** development library
- **Intel QPL (Query Processing Library)** for IAA hardware acceleration

### Installing Dependencies on CentOS/RHEL/Rocky Linux
```bash
# Install build tools
sudo yum groupinstall "Development Tools"
sudo yum install cmake

# Install LZ4
sudo yum install lz4-devel

# Install Intel QPL (if not already installed)
# Follow Intel's QPL installation guide at:
# https://github.com/intel/qpl
```

### Installing Dependencies on Ubuntu/Debian
```bash
# Install build tools
sudo apt-get update
sudo apt-get install build-essential cmake

# Install LZ4
sudo apt-get install liblz4-dev

# Install Intel QPL
# Follow Intel's QPL installation guide
```

## Build Instructions

### Standard Build (All Benchmarks)

```bash
# Navigate to DRAM-tier directory
cd /path/to/ZipCache/DRAM-tier

# Configure the project (this creates build files)
cmake -S . -B build

# Build all targets
cmake --build build -j$(nproc)
```

### Build Output

After a successful build, executables will be located in:
- `build/bin/` - All benchmark executables
- `build/lib/` - Compiled libraries

Key benchmarks include:
- `qpl_lz4_kv_bench` - Single-threaded compression benchmark
- `qpl_lz4_kv_bench_mt` - Multi-threaded synchronous benchmark
- `qpl_lz4_kv_bench_async` - Async batched benchmark (demonstrates IAA throughput)
- `qpl_lz4_mixed_workload` - Mixed read/write workload with compression
- `bplustree_demo` - B+Tree demo
- `bpt_compressed_qpl_smoke` - QPL compression smoke test

## Running Benchmarks

### QPL Async Benchmark (Recommended for IAA)
This benchmark demonstrates the best QPL/IAA performance using asynchronous batched operations:

```bash
# Run with hardware IAA acceleration
export KV_THREADS=32
export KV_PATH=hardware
./build/bin/qpl_lz4_kv_bench_async

# Run with QPL software path (for testing without IAA hardware)
export KV_PATH=software
./build/bin/qpl_lz4_kv_bench_async
```

### Multi-threaded Synchronous Benchmark
```bash
export KV_THREADS=32
./build/bin/qpl_lz4_kv_bench_mt
```

### Mixed Workload Benchmark
```bash
# 70% reads, 30% writes, 32 threads
export KV_THREADS=32
./build/bin/qpl_lz4_mixed_workload
```

## Environment Variables

- `KV_THREADS` - Number of worker threads (default: 8)
- `KV_PATH` - QPL execution path: `hardware`, `software`, or `auto` (default: auto)
- `KV_BATCH_SIZE` - Batch size for async benchmarks (default: 8)

## Troubleshooting

### Error: "could not load cache"
**Cause:** CMake configuration step was not run.
**Solution:** Run `cmake -S . -B build` before `cmake --build build`.

### Error: "QPL not found"
**Cause:** Intel QPL library is not installed or not in the library path.
**Solution:** 
1. Install QPL following Intel's guide
2. Set `QPL_DIR` environment variable:
   ```bash
   export QPL_DIR=/path/to/qpl/install
   cmake -S . -B build -DCMAKE_PREFIX_PATH=$QPL_DIR
   ```

### Error: "LZ4 not found"
**Cause:** LZ4 development library is not installed.
**Solution:** Install `lz4-devel` (RHEL/CentOS) or `liblz4-dev` (Ubuntu).

### Performance Issues
- **Low QPL throughput:** Use the async benchmark (`qpl_lz4_kv_bench_async`) instead of synchronous benchmarks
- **Hardware not detected:** Check `/sys/bus/dsa/devices/` for IAA devices, ensure drivers are loaded
- **CPU pinning:** For best performance, consider pinning threads to physical cores

## Clean Build

To start fresh:
```bash
rm -rf build
cmake -S . -B build
cmake --build build -j$(nproc)
```

## Contact

For questions or issues, please contact the ZipCache development team.
