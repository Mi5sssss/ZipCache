# DRAM-tier B+ Tree Mixed Workload Compression Benchmark

**Date:** September 4, 2025  \n**Objective:** Evaluate compression under mixed insert/read workloads with lazy buffering.

## Test Configuration

- **Number of key-value pairs:** 100000
- **Value size:** 64 bytes (fixed)
- **Mixed workload:** Enabled
  - Insert batch size: 10000
  - Reads per batch: 5000

### Test Scenarios

- **Scenario A:** 45 random bytes + 19 zero bytes (70% random data)
- **Scenario B:** 32 random bytes + 32 zero bytes (50% random data)
- **Scenario C:** 19 random bytes + 45 zero bytes (30% random data)

## Benchmark Results

| Scenario | Algorithm | Ins Throughput (ops/sec) | Ins P99 (μs) | Buf Read Throughput (ops/sec) | Buf Read P99 (μs) | Post Flush Throughput (ops/sec) | Post Flush P99 (μs) | Compression Ratio |
|----------|-----------|-------------------------|--------------|-------------------------------|-------------------|-------------------------------|---------------------|-------------------|
| Scenario A | LZ4 | 966808 | 1.19 | 0 | 0.00 | 0 | 0.00 | 1.143x |
| Scenario A | QPL | 1097661 | 1.19 | 0 | 0.00 | 0 | 0.00 | 1.213x |
| Scenario B | LZ4 | 1511690 | 1.19 | 0 | 0.00 | 0 | 0.00 | 1.455x |
| Scenario B | QPL | 1511074 | 1.19 | 0 | 0.00 | 0 | 0.00 | 1.594x |
| Scenario C | LZ4 | 2031366 | 1.19 | 0 | 0.00 | 0 | 0.00 | 2.065x |
| Scenario C | QPL | 2143122 | 1.19 | 0 | 0.00 | 0 | 0.00 | 2.496x |

## Buffer Hit Analysis

- Scenario A + LZ4: buffered hits Δ 0, misses Δ 0; post-flush hits Δ 0, misses Δ 0
- Scenario A + QPL: buffered hits Δ 0, misses Δ 0; post-flush hits Δ 0, misses Δ 0
- Scenario B + LZ4: buffered hits Δ 0, misses Δ 0; post-flush hits Δ 0, misses Δ 0
- Scenario B + QPL: buffered hits Δ 0, misses Δ 0; post-flush hits Δ 0, misses Δ 0
- Scenario C + LZ4: buffered hits Δ 0, misses Δ 0; post-flush hits Δ 0, misses Δ 0
- Scenario C + QPL: buffered hits Δ 0, misses Δ 0; post-flush hits Δ 0, misses Δ 0
