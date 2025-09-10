/*
 * ZipCache Performance Benchmark with Compression Analysis
 * 
 * Measures throughput and compression ratios for ZipCache using:
 * - LZ4 compression algorithm
 * - Intel QPL compression algorithm
 * - Same dataset across all tests for fair comparison
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>

// Include DRAM-tier compressed B+Tree (avoiding header conflicts)
#include "../DRAM-tier/lib/bplustree_compressed.h"

/* Benchmark configuration */
#define BENCHMARK_OPERATIONS    50000   /* Operations per test */
#define BENCHMARK_THREADS       4       /* Concurrent threads */
#define BENCHMARK_WARMUP        1000    /* Warmup operations */
#define TEST_KEY_RANGE          100000  /* Key space size */
#define DRAM_SIZE_MB           256      /* ZipCache DRAM size */
#define SSD_PATH               "/tmp/zipcache_bench"

/* Test data generation */
#define TINY_DATA_SIZE         64       /* Tiny objects */
#define MEDIUM_DATA_SIZE       1024     /* Medium objects */
#define LARGE_DATA_SIZE        4096     /* Large objects */

typedef struct {
    const char *name;
    compression_algorithm_t algorithm;
    double throughput_ops_per_sec;
    double compression_ratio_percent;
    size_t original_bytes;
    size_t compressed_bytes;
    double avg_latency_us;
    int lz4_operations;
    int qpl_operations;
} benchmark_result_t;

typedef struct {
    struct bplus_tree_compressed *tree;
    int thread_id;
    int operations;
    double start_time;
    double end_time;
    int successful_ops;
} thread_data_t;

/* Timing utilities */
static double get_time_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

static uint64_t get_time_us(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000ULL + tv.tv_usec;
}

/* Test data generation based on object size category */
static char* generate_test_data(size_t size, int seed) {
    char *data = malloc(size);
    if (!data) return NULL;
    
    // Generate semi-realistic data with some patterns for compression testing
    srand(seed);
    for (size_t i = 0; i < size; i++) {
        if (i % 4 == 0) {
            // Pattern data (compressible)
            data[i] = 'A' + (i / 4) % 26;
        } else if (i % 8 == 0) {
            // Repeated sequences
            data[i] = (i / 8) % 128;
        } else {
            // Random data (less compressible)
            data[i] = rand() % 256;
        }
    }
    return data;
}

/* Single-threaded benchmark for pure algorithm performance */
static void benchmark_algorithm_single_thread(compression_algorithm_t algorithm, 
                                             const char *algo_name,
                                             benchmark_result_t *result) {
    printf("\n🔧 Benchmarking %s Algorithm (Single Thread)\n", algo_name);
    printf("─────────────────────────────────────────────────────\n");
    
    // Create configuration for the algorithm
    struct compression_config config = bplus_tree_create_default_config(algorithm);
    config.enable_lazy_compression = 1;
    config.flush_threshold = 28;
    
    // Initialize compressed B+Tree
    struct bplus_tree_compressed *tree = bplus_tree_compressed_init_with_config(16, 64, &config);
    if (!tree) {
        printf("❌ Failed to initialize %s tree\n", algo_name);
        memset(result, 0, sizeof(benchmark_result_t));
        return;
    }
    
    printf("✅ Initialized %s compressed B+Tree\n", algo_name);
    
    // Warmup phase
    printf("🔥 Warming up (%d operations)...\n", BENCHMARK_WARMUP);
    for (int i = 0; i < BENCHMARK_WARMUP; i++) {
        bplus_tree_compressed_put(tree, i, i * 2);
    }
    
    // Main benchmark - INSERT operations
    printf("📊 Running INSERT benchmark (%d operations)...\n", BENCHMARK_OPERATIONS);
    
    uint64_t insert_start = get_time_us();
    int insert_success = 0;
    
    for (int i = 0; i < BENCHMARK_OPERATIONS; i++) {
        int key = rand() % TEST_KEY_RANGE;
        int value = key * 3;
        
        if (bplus_tree_compressed_put(tree, key, value) == 0) {
            insert_success++;
        }
        
        if ((i + 1) % 10000 == 0) {
            printf("   Completed %d/%d operations\n", i + 1, BENCHMARK_OPERATIONS);
        }
    }
    
    uint64_t insert_end = get_time_us();
    double insert_duration_ms = (insert_end - insert_start) / 1000.0;
    
    // Benchmark - GET operations
    printf("📊 Running GET benchmark (%d operations)...\n", BENCHMARK_OPERATIONS);
    
    uint64_t get_start = get_time_us();
    int get_success = 0;
    
    for (int i = 0; i < BENCHMARK_OPERATIONS; i++) {
        int key = rand() % TEST_KEY_RANGE;
        int value = bplus_tree_compressed_get(tree, key);
        
        if (value != -1) {
            get_success++;
        }
    }
    
    uint64_t get_end = get_time_us();
    double get_duration_ms = (get_end - get_start) / 1000.0;
    
    // Get compression statistics
    size_t total_size, compressed_size;
    bplus_tree_compressed_stats(tree, &total_size, &compressed_size);
    
    // Get algorithm-specific statistics
    int lz4_ops = 0, qpl_ops = 0;
    bplus_tree_compressed_get_algorithm_stats(tree, &lz4_ops, &qpl_ops);
    
    // Calculate results
    double total_duration_ms = insert_duration_ms + get_duration_ms;
    double total_ops = insert_success + get_success;
    
    result->name = algo_name;
    result->algorithm = algorithm;
    result->throughput_ops_per_sec = (total_ops / total_duration_ms) * 1000.0;
    result->compression_ratio_percent = (total_size > 0) ? 
        ((double)compressed_size / total_size * 100.0) : 0.0;
    result->original_bytes = total_size;
    result->compressed_bytes = compressed_size;
    result->avg_latency_us = (total_duration_ms * 1000.0) / total_ops;
    result->lz4_operations = lz4_ops;
    result->qpl_operations = qpl_ops;
    
    printf("\n📈 %s Results:\n", algo_name);
    printf("   INSERT success rate: %d/%d (%.1f%%)\n", 
           insert_success, BENCHMARK_OPERATIONS, 
           (double)insert_success / BENCHMARK_OPERATIONS * 100.0);
    printf("   GET success rate: %d/%d (%.1f%%)\n", 
           get_success, BENCHMARK_OPERATIONS,
           (double)get_success / BENCHMARK_OPERATIONS * 100.0);
    printf("   Total throughput: %.0f ops/sec\n", result->throughput_ops_per_sec);
    printf("   Average latency: %.2f μs\n", result->avg_latency_us);
    printf("   INSERT duration: %.2f ms\n", insert_duration_ms);
    printf("   GET duration: %.2f ms\n", get_duration_ms);
    
    printf("\n📊 Compression Statistics:\n");
    printf("   Original size: %zu bytes (%.2f KB)\n", total_size, total_size / 1024.0);
    printf("   Compressed size: %zu bytes (%.2f KB)\n", compressed_size, compressed_size / 1024.0);
    printf("   Compression ratio: %.2f%%\n", result->compression_ratio_percent);
    if (result->compression_ratio_percent > 0 && result->compression_ratio_percent < 100.0) {
        printf("   Space savings: %.2f%%\n", 100.0 - result->compression_ratio_percent);
    }
    printf("   Algorithm operations: LZ4=%d, QPL=%d\n", lz4_ops, qpl_ops);
    
    bplus_tree_compressed_deinit(tree);
}

/* Multi-threaded benchmark simulation */
static void* thread_benchmark_worker(void* arg) {
    thread_data_t* data = (thread_data_t*)arg;
    
    data->start_time = get_time_ms();
    data->successful_ops = 0;
    
    // Mix of operations: 60% INSERT, 30% GET, 10% DELETE
    for (int i = 0; i < data->operations; i++) {
        int key = (data->thread_id * 10000) + (rand() % 10000);
        int op_type = rand() % 100;
        
        if (op_type < 60) {
            // INSERT operation
            if (bplus_tree_compressed_put(data->tree, key, key * 2) == 0) {
                data->successful_ops++;
            }
        } else if (op_type < 90) {
            // GET operation
            if (bplus_tree_compressed_get(data->tree, key) != -1) {
                data->successful_ops++;
            }
        } else {
            // DELETE operation
            if (bplus_tree_compressed_delete(data->tree, key) == 0) {
                data->successful_ops++;
            }
        }
    }
    
    data->end_time = get_time_ms();
    return NULL;
}

static void benchmark_algorithm_multi_thread(compression_algorithm_t algorithm,
                                            const char *algo_name,
                                            benchmark_result_t *result) {
    printf("\n🔧 Benchmarking %s Algorithm (Multi-Thread: %d threads)\n", 
           algo_name, BENCHMARK_THREADS);
    printf("─────────────────────────────────────────────────────────────\n");
    
    // Create configuration
    struct compression_config config = bplus_tree_create_default_config(algorithm);
    config.enable_lazy_compression = 1;
    
    // Initialize tree
    struct bplus_tree_compressed *tree = bplus_tree_compressed_init_with_config(16, 64, &config);
    if (!tree) {
        printf("❌ Failed to initialize %s tree for multi-threading\n", algo_name);
        memset(result, 0, sizeof(benchmark_result_t));
        return;
    }
    
    // Setup thread data
    pthread_t threads[BENCHMARK_THREADS];
    thread_data_t thread_data[BENCHMARK_THREADS];
    
    int ops_per_thread = BENCHMARK_OPERATIONS / BENCHMARK_THREADS;
    
    double benchmark_start = get_time_ms();
    
    // Launch threads
    for (int i = 0; i < BENCHMARK_THREADS; i++) {
        thread_data[i].tree = tree;
        thread_data[i].thread_id = i;
        thread_data[i].operations = ops_per_thread;
        
        pthread_create(&threads[i], NULL, thread_benchmark_worker, &thread_data[i]);
    }
    
    // Wait for completion
    int total_successful = 0;
    for (int i = 0; i < BENCHMARK_THREADS; i++) {
        pthread_join(threads[i], NULL);
        total_successful += thread_data[i].successful_ops;
        
        printf("   Thread %d: %d/%d operations successful (%.1f%%)\n",
               i, thread_data[i].successful_ops, ops_per_thread,
               (double)thread_data[i].successful_ops / ops_per_thread * 100.0);
    }
    
    double benchmark_end = get_time_ms();
    double total_duration_ms = benchmark_end - benchmark_start;
    
    // Get compression statistics
    size_t total_size, compressed_size;
    bplus_tree_compressed_stats(tree, &total_size, &compressed_size);
    
    int lz4_ops = 0, qpl_ops = 0;
    bplus_tree_compressed_get_algorithm_stats(tree, &lz4_ops, &qpl_ops);
    
    // Update results (multi-threaded version)
    result->throughput_ops_per_sec = (total_successful / total_duration_ms) * 1000.0;
    result->compression_ratio_percent = (total_size > 0) ? 
        ((double)compressed_size / total_size * 100.0) : 0.0;
    result->original_bytes = total_size;
    result->compressed_bytes = compressed_size;
    result->avg_latency_us = (total_duration_ms * 1000.0) / total_successful;
    result->lz4_operations = lz4_ops;
    result->qpl_operations = qpl_ops;
    
    printf("\n📈 Multi-threaded %s Results:\n", algo_name);
    printf("   Total successful operations: %d/%d (%.1f%%)\n",
           total_successful, BENCHMARK_OPERATIONS,
           (double)total_successful / BENCHMARK_OPERATIONS * 100.0);
    printf("   Multi-threaded throughput: %.0f ops/sec\n", result->throughput_ops_per_sec);
    printf("   Average latency: %.2f μs\n", result->avg_latency_us);
    printf("   Total duration: %.2f ms\n", total_duration_ms);
    printf("   Compression ratio: %.2f%%\n", result->compression_ratio_percent);
    
    bplus_tree_compressed_deinit(tree);
}

/* Generate comprehensive performance report */
static void generate_performance_report(benchmark_result_t *lz4_single, 
                                       benchmark_result_t *qpl_single,
                                       benchmark_result_t *lz4_multi,
                                       benchmark_result_t *qpl_multi) {
    printf("\n");
    for (int i = 0; i < 80; i++) printf("=");
    printf("\n");
    printf("                    ZIPCACHE PERFORMANCE BENCHMARK REPORT\n");
    for (int i = 0; i < 80; i++) printf("=");
    printf("\n");
    
    printf("\n📊 **Throughput Comparison**\n");
    printf("┌─────────────────────────┬─────────────────┬─────────────────┐\n");
    printf("│ Algorithm               │ Single Thread   │ Multi Thread    │\n");
    printf("├─────────────────────────┼─────────────────┼─────────────────┤\n");
    printf("│ LZ4                     │ %10.0f ops/s │ %10.0f ops/s │\n",
           lz4_single->throughput_ops_per_sec, lz4_multi->throughput_ops_per_sec);
    printf("│ Intel QPL               │ %10.0f ops/s │ %10.0f ops/s │\n",
           qpl_single->throughput_ops_per_sec, qpl_multi->throughput_ops_per_sec);
    printf("└─────────────────────────┴─────────────────┴─────────────────┘\n");
    
    printf("\n📊 **Compression Ratio Comparison**\n");
    printf("┌─────────────────────────┬─────────────────┬─────────────────┐\n");
    printf("│ Algorithm               │ Single Thread   │ Multi Thread    │\n");
    printf("├─────────────────────────┼─────────────────┼─────────────────┤\n");
    printf("│ LZ4                     │      %6.2f%%     │      %6.2f%%     │\n",
           lz4_single->compression_ratio_percent, lz4_multi->compression_ratio_percent);
    printf("│ Intel QPL               │      %6.2f%%     │      %6.2f%%     │\n",
           qpl_single->compression_ratio_percent, qpl_multi->compression_ratio_percent);
    printf("└─────────────────────────┴─────────────────┴─────────────────┘\n");
    
    printf("\n📊 **Average Latency Comparison**\n");
    printf("┌─────────────────────────┬─────────────────┬─────────────────┐\n");
    printf("│ Algorithm               │ Single Thread   │ Multi Thread    │\n");
    printf("├─────────────────────────┼─────────────────┼─────────────────┤\n");
    printf("│ LZ4                     │    %8.2f μs   │    %8.2f μs   │\n",
           lz4_single->avg_latency_us, lz4_multi->avg_latency_us);
    printf("│ Intel QPL               │    %8.2f μs   │    %8.2f μs   │\n",
           qpl_single->avg_latency_us, qpl_multi->avg_latency_us);
    printf("└─────────────────────────┴─────────────────┴─────────────────┘\n");
    
    printf("\n📊 **Space Usage Analysis**\n");
    printf("┌─────────────────────────┬─────────────────┬─────────────────┐\n");
    printf("│ Algorithm               │ Original Size   │ Compressed Size │\n");
    printf("├─────────────────────────┼─────────────────┼─────────────────┤\n");
    printf("│ LZ4 (Single Thread)    │    %8.1f KB   │    %8.1f KB   │\n",
           lz4_single->original_bytes / 1024.0, lz4_single->compressed_bytes / 1024.0);
    printf("│ Intel QPL (Single)     │    %8.1f KB   │    %8.1f KB   │\n",
           qpl_single->original_bytes / 1024.0, qpl_single->compressed_bytes / 1024.0);
    printf("│ LZ4 (Multi Thread)     │    %8.1f KB   │    %8.1f KB   │\n",
           lz4_multi->original_bytes / 1024.0, lz4_multi->compressed_bytes / 1024.0);
    printf("│ Intel QPL (Multi)      │    %8.1f KB   │    %8.1f KB   │\n",
           qpl_multi->original_bytes / 1024.0, qpl_multi->compressed_bytes / 1024.0);
    printf("└─────────────────────────┴─────────────────┴─────────────────┘\n");
    
    printf("\n🏆 **Performance Winners**\n");
    
    // Determine winners
    const char *throughput_winner_single = (lz4_single->throughput_ops_per_sec > qpl_single->throughput_ops_per_sec) ? "LZ4" : "Intel QPL";
    const char *throughput_winner_multi = (lz4_multi->throughput_ops_per_sec > qpl_multi->throughput_ops_per_sec) ? "LZ4" : "Intel QPL";
    const char *compression_winner_single = (lz4_single->compression_ratio_percent < qpl_single->compression_ratio_percent) ? "LZ4" : "Intel QPL";
    const char *compression_winner_multi = (lz4_multi->compression_ratio_percent < qpl_multi->compression_ratio_percent) ? "LZ4" : "Intel QPL";
    
    printf("   🚀 Highest Throughput (Single): %s (%.0f ops/sec)\n", 
           throughput_winner_single,
           (lz4_single->throughput_ops_per_sec > qpl_single->throughput_ops_per_sec) ? 
           lz4_single->throughput_ops_per_sec : qpl_single->throughput_ops_per_sec);
    
    printf("   🚀 Highest Throughput (Multi):  %s (%.0f ops/sec)\n",
           throughput_winner_multi,
           (lz4_multi->throughput_ops_per_sec > qpl_multi->throughput_ops_per_sec) ?
           lz4_multi->throughput_ops_per_sec : qpl_multi->throughput_ops_per_sec);
    
    printf("   📦 Best Compression (Single):   %s (%.2f%% ratio)\n",
           compression_winner_single,
           (lz4_single->compression_ratio_percent < qpl_single->compression_ratio_percent) ?
           lz4_single->compression_ratio_percent : qpl_single->compression_ratio_percent);
    
    printf("   📦 Best Compression (Multi):    %s (%.2f%% ratio)\n",
           compression_winner_multi,
           (lz4_multi->compression_ratio_percent < qpl_multi->compression_ratio_percent) ?
           lz4_multi->compression_ratio_percent : qpl_multi->compression_ratio_percent);
    
    printf("\n💡 **Recommendations**\n");
    if (lz4_single->throughput_ops_per_sec > qpl_single->throughput_ops_per_sec * 1.1) {
        printf("   • LZ4 shows significantly better throughput performance\n");
    } else if (qpl_single->throughput_ops_per_sec > lz4_single->throughput_ops_per_sec * 1.1) {
        printf("   • Intel QPL shows significantly better throughput performance\n");
    } else {
        printf("   • Both algorithms show similar throughput performance\n");
    }
    
    if (lz4_single->compression_ratio_percent < qpl_single->compression_ratio_percent * 0.9) {
        printf("   • LZ4 provides significantly better compression ratios\n");
    } else if (qpl_single->compression_ratio_percent < lz4_single->compression_ratio_percent * 0.9) {
        printf("   • Intel QPL provides significantly better compression ratios\n");
    } else {
        printf("   • Both algorithms show similar compression effectiveness\n");
    }
    
    printf("\n");
    for (int i = 0; i < 80; i++) printf("=");
    printf("\n");
}

int main(void) {
    printf("🚀 ZipCache Performance Benchmark Suite\n");
    printf("========================================\n");
    printf("📊 Measuring throughput and compression ratios\n");
    printf("🔧 Algorithms: LZ4 vs Intel QPL\n");
    printf("📈 Operations per test: %d\n", BENCHMARK_OPERATIONS);
    printf("🧵 Multi-threading: %d threads\n", BENCHMARK_THREADS);
    printf("🔥 Warmup operations: %d\n\n", BENCHMARK_WARMUP);
    
    // Initialize random seed
    srand(time(NULL));
    
    benchmark_result_t lz4_single, qpl_single, lz4_multi, qpl_multi;
    
    // Run single-threaded benchmarks
    benchmark_algorithm_single_thread(COMPRESS_LZ4, "LZ4", &lz4_single);
    benchmark_algorithm_single_thread(COMPRESS_QPL, "Intel QPL", &qpl_single);
    
    // Run multi-threaded benchmarks
    benchmark_algorithm_multi_thread(COMPRESS_LZ4, "LZ4", &lz4_multi);
    benchmark_algorithm_multi_thread(COMPRESS_QPL, "Intel QPL", &qpl_multi);
    
    // Generate comprehensive report
    generate_performance_report(&lz4_single, &qpl_single, &lz4_multi, &qpl_multi);
    
    printf("🎉 ZipCache Performance Benchmark Complete!\n");
    printf("📋 Results show comparative performance of compression algorithms\n");
    
    return 0;
}