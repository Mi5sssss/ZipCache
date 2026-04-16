#include <chrono>
#include <iostream>
#include <vector>
#include <algorithm>
#include <lz4.h>
#include <cstring>

int main() {
    // Test clock resolution
    std::cout << "=== Clock Resolution Test ===\n";
    
    auto t1 = std::chrono::steady_clock::now();
    auto t2 = std::chrono::steady_clock::now();
    double min_measurable = std::chrono::duration<double, std::micro>(t2 - t1).count();
    
    std::cout << "Minimum measurable interval: " << min_measurable << " us\n";
    
    // Test LZ4 decompression latency with proper measurement
    const int block_size = 4096;
    std::vector<char> src(block_size, 'A'); // Highly compressible
    std::vector<char> compressed(block_size);
    std::vector<char> decompressed(block_size);
    
    // Compress once
    int comp_size = LZ4_compress_default(src.data(), compressed.data(), block_size, block_size);
    std::cout << "\nCompressed " << block_size << " bytes to " << comp_size << " bytes\n";
    
    // Warm up cache
    for (int i = 0; i < 1000; i++) {
        LZ4_decompress_safe(compressed.data(), decompressed.data(), comp_size, block_size);
    }
    
    // Measure 10000 decompressions
    std::vector<double> latencies;
    latencies.reserve(10000);
    
    for (int i = 0; i < 10000; i++) {
        auto start = std::chrono::steady_clock::now();
        int result = LZ4_decompress_safe(compressed.data(), decompressed.data(), comp_size, block_size);
        auto end = std::chrono::steady_clock::now();
        
        if (result > 0) {
            double us = std::chrono::duration<double, std::micro>(end - start).count();
            latencies.push_back(us);
        }
    }
    
    // Compute statistics
    std::sort(latencies.begin(), latencies.end());
    
    double sum = 0;
    for (double l : latencies) sum += l;
    double avg = sum / latencies.size();
    
    size_t p50_idx = latencies.size() * 50 / 100;
    size_t p99_idx = latencies.size() * 99 / 100;
    size_t p999_idx = latencies.size() * 999 / 1000;
    
    std::cout << "\n=== LZ4 Decompression (4KB, from cache) ===\n";
    std::cout << "Average: " << avg << " us\n";
    std::cout << "P50: " << latencies[p50_idx] << " us\n";
    std::cout << "P99: " << latencies[p99_idx] << " us\n";
    std::cout << "P99.9: " << latencies[p999_idx] << " us\n";
    std::cout << "Min: " << latencies[0] << " us\n";
    std::cout << "Max: " << latencies.back() << " us\n";
    
    return 0;
}
