---
description: Build all DRAM-tier targets from the repo root
---
// turbo-all

1. Configure CMake from repo root:
   `cmake -S DRAM-tier -B DRAM-tier/build`

2. Build all targets:
   `cmake --build DRAM-tier/build -j$(nproc)`

3. Verify key binaries:
   `ls -1 DRAM-tier/build/bin/bpt_compressed_* DRAM-tier/build/bin/tail_latency_compare DRAM-tier/build/bin/qpl_lz4_* DRAM-tier/build/bin/test_compression_concurrency DRAM-tier/build/bin/dcperf_workload_benchmark 2>/dev/null | head -20`

Working directory for all commands: `/home/xier2/2025-03-17-intel-zipcache/bplustree/bplustree`
