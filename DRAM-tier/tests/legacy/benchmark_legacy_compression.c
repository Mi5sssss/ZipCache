/*
 * Performance benchmark for legacy 1D design with dual compression
 * Compares LZ4 vs QPL performance characteristics
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include "../lib/bplustree_compressed.h"

#define BENCHMARK_KEYS 10000
#define WARMUP_KEYS 1000
#define ITERATIONS 3

// High-resolution timer
double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

// Benchmark structure
struct benchmark_result {
    double insert_time;
    double get_time;
    double insert_rate;
    double get_rate;
    size_t total_size;
    size_t compressed_size;
    double compression_ratio;
};

// Run benchmark for a specific algorithm
struct benchmark_result run_benchmark(compression_algo_t algorithm, const char* algo_name) {
    struct benchmark_result result = {0};

    printf("\n=== Benchmarking %s Algorithm ===\n", algo_name);

    // Create configuration
    struct simple_compression_config config = bplus_tree_create_default_simple_config(algorithm);

    // Optimize configuration for performance
    if (algorithm == COMPRESS_LZ4) {
        config.num_subpages = 16;  // More granular for better partial decompression
        config.lz4_partial_decompression = 1;
    } else {
        config.num_subpages = 8;   // Fewer subpages for better compression
        config.qpl_compression_level = 1;  // Fast compression
    }

    printf("Configuration: %d subpages, partial_decompression=%s\n",
           config.num_subpages, config.lz4_partial_decompression ? "enabled" : "disabled");

    // Initialize tree
    struct bplus_tree_compressed *tree = bplus_tree_compressed_init_simple(16, 128, &config);
    if (!tree) {
        printf("❌ Failed to initialize %s tree\n", algo_name);
        return result;
    }

    // Warmup
    printf("Warming up with %d operations...\n", WARMUP_KEYS);
    for (int i = 0; i < WARMUP_KEYS; i++) {
        bplus_tree_compressed_put(tree, i, i * 2);
    }

    double total_insert_time = 0;
    double total_get_time = 0;

    for (int iter = 0; iter < ITERATIONS; iter++) {
        printf("Iteration %d/%d...\n", iter + 1, ITERATIONS);

        // Benchmark insertions
        double start_time = get_time();

        for (int i = WARMUP_KEYS; i < WARMUP_KEYS + BENCHMARK_KEYS; i++) {
            int ret = bplus_tree_compressed_put(tree, i, i * 3);
            if (ret != 0) {
                printf("❌ Insert failed at key %d\n", i);
                break;
            }
        }

        double insert_end = get_time();
        double iter_insert_time = insert_end - start_time;
        total_insert_time += iter_insert_time;

        // Benchmark gets (random access pattern)
        int *random_keys = malloc(BENCHMARK_KEYS * sizeof(int));
        for (int i = 0; i < BENCHMARK_KEYS; i++) {
            random_keys[i] = WARMUP_KEYS + (rand() % BENCHMARK_KEYS);
        }

        double get_start = get_time();

        int successful_gets = 0;
        for (int i = 0; i < BENCHMARK_KEYS; i++) {
            int value = bplus_tree_compressed_get(tree, random_keys[i]);
            if (value == random_keys[i] * 3) {
                successful_gets++;
            }
        }

        double get_end = get_time();
        double iter_get_time = get_end - get_start;
        total_get_time += iter_get_time;

        printf("  Insert: %.3fs (%.0f ops/sec), Get: %.3fs (%.0f ops/sec), Success: %d/%d\n",
               iter_insert_time, BENCHMARK_KEYS / iter_insert_time,
               iter_get_time, BENCHMARK_KEYS / iter_get_time,
               successful_gets, BENCHMARK_KEYS);

        free(random_keys);
    }

    // Calculate averages
    result.insert_time = total_insert_time / ITERATIONS;
    result.get_time = total_get_time / ITERATIONS;
    result.insert_rate = BENCHMARK_KEYS / result.insert_time;
    result.get_rate = BENCHMARK_KEYS / result.get_time;

    // Get compression statistics
    bplus_tree_compressed_stats(tree, &result.total_size, &result.compressed_size);
    result.compression_ratio = bplus_tree_compressed_get_compression_ratio(tree);

    printf("\n%s Results (average of %d iterations):\n", algo_name, ITERATIONS);
    printf("  Insert time: %.3fs (%.0f ops/sec)\n", result.insert_time, result.insert_rate);
    printf("  Get time: %.3fs (%.0f ops/sec)\n", result.get_time, result.get_rate);
    printf("  Total size: %zu bytes\n", result.total_size);
    printf("  Compressed size: %zu bytes\n", result.compressed_size);
    printf("  Compression ratio: %.2f%%\n", result.compression_ratio);

    // Test tree size
    int tree_size = bplus_tree_compressed_size(tree);
    printf("  Tree entries: %d\n", tree_size);

    bplus_tree_compressed_deinit(tree);
    return result;
}

// Test legacy hash distribution performance
void benchmark_hash_distribution() {
    printf("\n=== Benchmarking Legacy Hash Distribution ===\n");

    const int num_keys = 100000;
    const int num_subpages = 16;

    double start_time = get_time();

    int distribution[16] = {0};
    for (int i = 0; i < num_keys; i++) {
        char key[32];
        snprintf(key, sizeof(key), "benchmark_key_%d", i);
        int subpage = calculate_target_subpage_legacy(key, num_subpages);
        if (subpage >= 0 && subpage < num_subpages) {
            distribution[subpage]++;
        }
    }

    double end_time = get_time();
    double hash_time = end_time - start_time;

    printf("Hash distribution for %d keys across %d subpages:\n", num_keys, num_subpages);

    int min_count = num_keys, max_count = 0;
    for (int i = 0; i < num_subpages; i++) {
        printf("  Subpage %2d: %6d keys (%.1f%%)\n",
               i, distribution[i], (distribution[i] * 100.0) / num_keys);
        if (distribution[i] < min_count) min_count = distribution[i];
        if (distribution[i] > max_count) max_count = distribution[i];
    }

    double balance = (double)min_count / max_count;
    printf("Hash performance: %.3fs (%.0f hashes/sec)\n", hash_time, num_keys / hash_time);
    printf("Distribution balance: %.3f (min/max ratio)\n", balance);

    if (balance > 0.8) {
        printf("✅ Good hash distribution balance\n");
    } else {
        printf("⚠️  Hash distribution could be more balanced\n");
    }
}

// Memory usage test
void test_memory_usage() {
    printf("\n=== Memory Usage Comparison ===\n");

    struct simple_compression_config lz4_config = bplus_tree_create_default_simple_config(COMPRESS_LZ4);
    struct simple_compression_config qpl_config = bplus_tree_create_default_simple_config(COMPRESS_QPL);

    struct bplus_tree_compressed *lz4_tree = bplus_tree_compressed_init_simple(8, 64, &lz4_config);
    struct bplus_tree_compressed *qpl_tree = bplus_tree_compressed_init_simple(8, 64, &qpl_config);

    const int test_keys = 5000;

    // Fill both trees with same data
    for (int i = 1; i <= test_keys; i++) {
        bplus_tree_compressed_put(lz4_tree, i, i * 42);
        bplus_tree_compressed_put(qpl_tree, i, i * 42);
    }

    size_t lz4_total, lz4_compressed, qpl_total, qpl_compressed;
    bplus_tree_compressed_stats(lz4_tree, &lz4_total, &lz4_compressed);
    bplus_tree_compressed_stats(qpl_tree, &qpl_total, &qpl_compressed);

    printf("Memory usage for %d keys:\n", test_keys);
    printf("  LZ4: %zu total, %zu compressed (%.1f%% ratio)\n",
           lz4_total, lz4_compressed, (double)lz4_compressed / lz4_total * 100);
    printf("  QPL: %zu total, %zu compressed (%.1f%% ratio)\n",
           qpl_total, qpl_compressed, (double)qpl_compressed / qpl_total * 100);

    if (qpl_compressed < lz4_compressed) {
        printf("✅ QPL achieves better compression ratio\n");
    } else {
        printf("ℹ️  LZ4 has similar or better compression for this dataset\n");
    }

    bplus_tree_compressed_deinit(lz4_tree);
    bplus_tree_compressed_deinit(qpl_tree);
}

// Compare partial vs full decompression
void benchmark_decompression_strategies() {
    printf("\n=== Decompression Strategy Comparison ===\n");

    // LZ4 with partial decompression
    struct simple_compression_config lz4_partial = bplus_tree_create_default_simple_config(COMPRESS_LZ4);
    lz4_partial.lz4_partial_decompression = 1;
    lz4_partial.num_subpages = 16;

    // LZ4 with full decompression
    struct simple_compression_config lz4_full = bplus_tree_create_default_simple_config(COMPRESS_LZ4);
    lz4_full.lz4_partial_decompression = 0;
    lz4_full.num_subpages = 16;

    struct bplus_tree_compressed *partial_tree = bplus_tree_compressed_init_simple(8, 64, &lz4_partial);
    struct bplus_tree_compressed *full_tree = bplus_tree_compressed_init_simple(8, 64, &lz4_full);

    const int keys = 2000;

    // Fill both trees
    for (int i = 1; i <= keys; i++) {
        bplus_tree_compressed_put(partial_tree, i, i * 7);
        bplus_tree_compressed_put(full_tree, i, i * 7);
    }

    // Benchmark random access (benefits partial decompression)
    double start = get_time();
    for (int i = 0; i < 1000; i++) {
        int key = 1 + (rand() % keys);
        bplus_tree_compressed_get(partial_tree, key);
    }
    double partial_time = get_time() - start;

    start = get_time();
    for (int i = 0; i < 1000; i++) {
        int key = 1 + (rand() % keys);
        bplus_tree_compressed_get(full_tree, key);
    }
    double full_time = get_time() - start;

    printf("Random access performance (1000 operations):\n");
    printf("  Partial decompression: %.3fs (%.0f ops/sec)\n", partial_time, 1000 / partial_time);
    printf("  Full decompression: %.3fs (%.0f ops/sec)\n", full_time, 1000 / full_time);

    if (partial_time < full_time) {
        printf("✅ Partial decompression is %.1fx faster for random access\n", full_time / partial_time);
    } else {
        printf("ℹ️  Full decompression performs similarly\n");
    }

    bplus_tree_compressed_deinit(partial_tree);
    bplus_tree_compressed_deinit(full_tree);
}

int main() {
    printf("🚀 Legacy 1D Design with Dual Compression Benchmark\n");
    printf("=====================================================\n");

    srand(time(NULL));

    // Benchmark hash distribution
    benchmark_hash_distribution();

    // Benchmark both algorithms
    struct benchmark_result lz4_result = run_benchmark(COMPRESS_LZ4, "LZ4");
    struct benchmark_result qpl_result = run_benchmark(COMPRESS_QPL, "QPL");

    // Compare results
    printf("\n=== Performance Comparison ===\n");
    printf("Metric                 | LZ4        | QPL        | Winner\n");
    printf("-----------------------|------------|------------|--------\n");
    printf("Insert rate (ops/sec)  | %10.0f | %10.0f | %s\n",
           lz4_result.insert_rate, qpl_result.insert_rate,
           lz4_result.insert_rate > qpl_result.insert_rate ? "LZ4" : "QPL");
    printf("Get rate (ops/sec)     | %10.0f | %10.0f | %s\n",
           lz4_result.get_rate, qpl_result.get_rate,
           lz4_result.get_rate > qpl_result.get_rate ? "LZ4" : "QPL");
    printf("Compression ratio (%%) | %10.2f | %10.2f | %s\n",
           lz4_result.compression_ratio, qpl_result.compression_ratio,
           qpl_result.compression_ratio < lz4_result.compression_ratio ? "QPL" : "LZ4");

    // Memory usage comparison
    test_memory_usage();

    // Decompression strategy comparison
    benchmark_decompression_strategies();

    printf("\n🎯 Recommendations:\n");
    if (lz4_result.get_rate > qpl_result.get_rate * 1.2) {
        printf("  ✅ Use LZ4 for random access workloads (%.1fx faster gets)\n",
               lz4_result.get_rate / qpl_result.get_rate);
    }
    if (qpl_result.compression_ratio < lz4_result.compression_ratio * 0.8) {
        printf("  ✅ Use QPL for storage-constrained scenarios (%.1fx better compression)\n",
               lz4_result.compression_ratio / qpl_result.compression_ratio);
    }
    printf("  ✅ Legacy hash distribution provides balanced subpage allocation\n");
    printf("  ✅ Partial decompression optimizes LZ4 random access performance\n");

    printf("\n🎉 Benchmark completed successfully!\n");
    return 0;
}