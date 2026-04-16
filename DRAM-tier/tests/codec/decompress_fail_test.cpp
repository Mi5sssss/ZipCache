#include <cstdio>
#include <cstring>
#include <lz4.h>
#include <vector>
#include <chrono>

int main() {
    // Simulate the same scenario as the benchmark
    const int block_size = 4096;
    
    // Create uncompressed data (like fill_kv_blocks does)
    std::vector<uint8_t> uncompressed(block_size);
    for (int i = 0; i < block_size; i++) {
        uncompressed[i] = (uint8_t)(i & 0xff);
    }
    
    // Try to "decompress" uncompressed data (what the benchmark is doing!)
    std::vector<uint8_t> output(block_size);
    
    auto t0 = std::chrono::steady_clock::now();
    int result = LZ4_decompress_safe(
        (const char*)uncompressed.data(),
        (char*)output.data(),
        block_size / 2,  // Assume it's "compressed" to 2KB (WRONG!)
        block_size
    );
    auto t1 = std::chrono::steady_clock::now();
    
    double us = std::chrono::duration<double, std::micro>(t1 - t0).count();
    
    printf("Result: %d (negative = error)\n", result);
    printf("Latency: %.3f us\n", us);
    
    // Now do it properly - compress first, then decompress
    printf("\n=== Proper Test ===\n");
    std::vector<char> compressed(block_size);
    int comp_size = LZ4_compress_default(
        (const char*)uncompressed.data(),
        compressed.data(),
        block_size,
        block_size
    );
    printf("Compressed size: %d bytes\n", comp_size);
    
    auto t2 = std::chrono::steady_clock::now();
    int result2 = LZ4_decompress_safe(
        compressed.data(),
        (char*)output.data(),
        comp_size,
        block_size
    );
    auto t3 = std::chrono::steady_clock::now();
    
    double us2 = std::chrono::duration<double, std::micro>(t3 - t2).count();
    printf("Result: %d bytes\n", result2);
    printf("Latency: %.3f us\n", us2);
    
    return 0;
}
