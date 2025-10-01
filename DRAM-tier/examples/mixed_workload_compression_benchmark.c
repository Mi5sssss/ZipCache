#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <math.h>
#include <stdbool.h>
#include <lz4.h>

#include "../lib/bplustree_compressed.h"

#define NUM_KEYS 1000
#define VALUE_SIZE 64

#define MIXED_WORKLOAD_ENABLED 1
#define MIXED_INSERT_BATCH 100
#define MIXED_READS_PER_BATCH 50

// Test scenario definitions
typedef struct {
    const char *name;
    const char *description;
    int random_bytes;    // Number of random bytes in the value
    int zero_bytes;      // Number of zero bytes in the value
} test_scenario_t;

// Define the three test scenarios
static const test_scenario_t scenarios[] = {
    {"Scenario A", "Low Compressibility (70% Random)", 45, 19},
    {"Scenario B", "Medium Compressibility (50% Random)", 32, 32},
    {"Scenario C", "High Compressibility (30% Random)", 19, 45}
};

#define NUM_SCENARIOS (sizeof(scenarios) / sizeof(scenarios[0]))

// Performance metrics structure
typedef struct {
    const char *scenario_name;
    const char *algorithm_name;
    double compression_ratio;
    double throughput;
    double p99_latency;
    double total_time;
    double read_throughput_buffered;
    double read_p99_buffered;
    double read_throughput_postflush;
    double read_p99_postflush;
    int buffer_hits_before_flush;
    int buffer_misses_before_flush;
    int buffer_hits_after_flush;
    int buffer_misses_after_flush;
    size_t original_size;
    size_t compressed_size;
    int successful_insertions;
    int read_operations_buffered;
    int read_operations_postflush;
} benchmark_result_t;

// Timing utilities
double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

// Compare function for qsort (for P99 calculation)
int compare_doubles(const void *a, const void *b) {
    double da = *(const double*)a;
    double db = *(const double*)b;
    return (da > db) - (da < db);
}

// Calculate P99 latency from latency array
double calculate_p99_latency(double *latencies, int count) {
    if (count == 0) return 0.0;
    qsort(latencies, count, sizeof(double), compare_doubles);
    int index = (int)(0.99 * count);
    if (index >= count) index = count - 1;
    return latencies[index] * 1000000.0; // Convert to microseconds
}

// Generate synthetic value with controlled compressibility
void generate_synthetic_value(char *buffer, int random_bytes, int zero_bytes) {
    for (int i = 0; i < random_bytes; i++) {
        buffer[i] = (char)(rand() % 256);
    }
    memset(buffer + random_bytes, 0x00, zero_bytes);
}

// Test direct compression to measure achievable compression ratio
double test_direct_compression_ratio(const test_scenario_t *scenario, compression_algo_t algo, const char *algo_name) {
    printf("  Testing direct %s compression for %s...\n", algo_name, scenario->name);

    char input_buffer[VALUE_SIZE];
    char compressed_buffer[VALUE_SIZE * 2];

    size_t total_original = 0;
    size_t total_compressed = 0;

    qpl_job *test_qpl_job = NULL;
    uint8_t *test_qpl_buffer = NULL;

    if (algo == COMPRESS_QPL) {
        uint32_t job_size;
        qpl_status status = qpl_get_job_size(qpl_path_auto, &job_size);
        if (status == QPL_STS_OK) {
            test_qpl_buffer = malloc(job_size);
            if (test_qpl_buffer) {
                test_qpl_job = (qpl_job*)test_qpl_buffer;
                status = qpl_init_job(qpl_path_auto, test_qpl_job);
                if (status != QPL_STS_OK) {
                    free(test_qpl_buffer);
                    test_qpl_job = NULL;
                    test_qpl_buffer = NULL;
                }
            }
        }
    }

    for (int i = 0; i < 1000; i++) {
        generate_synthetic_value(input_buffer, scenario->random_bytes, scenario->zero_bytes);

        int compressed_size = 0;
        if (algo == COMPRESS_LZ4) {
            compressed_size = LZ4_compress_default(input_buffer, compressed_buffer, VALUE_SIZE, VALUE_SIZE * 2);
        } else if (algo == COMPRESS_QPL && test_qpl_job) {
            test_qpl_job->op = qpl_op_compress;
            test_qpl_job->next_in_ptr = (uint8_t*)input_buffer;
            test_qpl_job->available_in = VALUE_SIZE;
            test_qpl_job->next_out_ptr = (uint8_t*)compressed_buffer;
            test_qpl_job->available_out = VALUE_SIZE * 2;
            test_qpl_job->level = qpl_default_level;
            test_qpl_job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

            qpl_status status = qpl_execute_job(test_qpl_job);
            if (status == QPL_STS_OK) {
                compressed_size = test_qpl_job->total_out;
            }
        } else {
            compressed_size = LZ4_compress_default(input_buffer, compressed_buffer, VALUE_SIZE, VALUE_SIZE * 2);
        }

        if (compressed_size > 0) {
            total_original += VALUE_SIZE;
            total_compressed += compressed_size;
        }
    }

    if (test_qpl_job) {
        qpl_fini_job(test_qpl_job);
    }
    if (test_qpl_buffer) {
        free(test_qpl_buffer);
    }

    double ratio = (double)total_original / total_compressed;
    printf("    Direct compression ratio: %.3fx (%.1f%% savings)\n",
           ratio, (1.0 - 1.0/ratio) * 100.0);

    return ratio;
}

typedef struct {
    double total_time;
    int ops;
    int buffer_hits_delta;
    int buffer_misses_delta;
} read_phase_stats;

static read_phase_stats perform_random_reads(struct bplus_tree_compressed *ct_tree,
                                             int keys_inserted,
                                             int max_reads,
                                             double *latency_store,
                                             int *latency_index,
                                             bool flush_before_phase)
{
    read_phase_stats stats = {0};
    if (keys_inserted <= 0 || max_reads <= 0) {
        return stats;
    }

    if (flush_before_phase) {
        bplus_tree_compressed_flush_all_buffers(ct_tree);
    }

    int hits_before = 0;
    int misses_before = 0;
    bplus_tree_compressed_get_buffer_stats(ct_tree, &hits_before, &misses_before);

    uint8_t read_buffer[VALUE_SIZE];
    size_t actual_len = 0;

    double phase_start = get_time();
    int reads_executed = 0;

    for (int i = 0; i < max_reads; i++) {
        int key = i % keys_inserted;
        double read_start = get_time();
        int rc = bplus_tree_compressed_get(ct_tree, key);
        double read_end = get_time();

        if (rc >= 0) {
            latency_store[(*latency_index)++] = read_end - read_start;
            reads_executed++;
        } else if (i < 5) {
            printf("    Read failed for key %d: rc=%d\n", key, rc);
        }
    }

    double phase_end = get_time();
    stats.total_time = phase_end - phase_start;
    stats.ops = reads_executed;

    int hits_after = 0;
    int misses_after = 0;
    bplus_tree_compressed_get_buffer_stats(ct_tree, &hits_after, &misses_after);
    stats.buffer_hits_delta = hits_after - hits_before;
    stats.buffer_misses_delta = misses_after - misses_before;

    return stats;
}

benchmark_result_t run_benchmark(const test_scenario_t *scenario, compression_algo_t algo, const char *algo_name) {
    benchmark_result_t result = {0};
    result.scenario_name = scenario->name;
    result.algorithm_name = algo_name;

    printf("\n=== BENCHMARKING %s WITH %s ===\n", scenario->name, algo_name);
    printf("Configuration: %d random bytes + %d zero bytes = %d total bytes\n",
           scenario->random_bytes, scenario->zero_bytes, VALUE_SIZE);

    struct compression_config config = {
        .default_layout = LEAF_TYPE_LZ4_HASHED,
        .algo = algo,
        .default_sub_pages = 16,
        .compression_level = 0,
        .buffer_size = 512,
        .flush_threshold = 100,
        .enable_lazy_compression = 1
    };

    struct bplus_tree_compressed *ct_tree = bplus_tree_compressed_init_with_config(16, 64, &config);
    if (!ct_tree) {
        printf("Error: Failed to initialize compressed B+ tree\n");
        return result;
    }
    bplus_tree_compressed_set_compression(ct_tree, 1);

    double *insert_latencies = malloc(NUM_KEYS * sizeof(double));
    if (!insert_latencies) {
        printf("Error: Failed to allocate latency array\n");
        bplus_tree_compressed_deinit(ct_tree);
        return result;
    }

    double *buffered_read_latencies = NULL;
    double *postflush_read_latencies = NULL;
    int buffered_latency_capacity = 0;
    int postflush_latency_capacity = 0;
    int buffered_latency_count = 0;
    int postflush_latency_count = 0;
    double buffered_total_time = 0.0;
    double postflush_total_time = 0.0;

    if (MIXED_WORKLOAD_ENABLED) {
        buffered_latency_capacity = (NUM_KEYS / MIXED_INSERT_BATCH + 2) * MIXED_READS_PER_BATCH;
        postflush_latency_capacity = buffered_latency_capacity;
        buffered_read_latencies = malloc(buffered_latency_capacity * sizeof(double));
        postflush_read_latencies = malloc(postflush_latency_capacity * sizeof(double));
    }

    if (MIXED_WORKLOAD_ENABLED && (!buffered_read_latencies || !postflush_read_latencies)) {
        printf("Error: Failed to allocate read latency buffers\n");
        free(insert_latencies);
        free(buffered_read_latencies);
        free(postflush_read_latencies);
        bplus_tree_compressed_deinit(ct_tree);
        return result;
    }

    char value_buffer[VALUE_SIZE];

    printf("Inserting %d key-value pairs...\n", NUM_KEYS);

    double start_time = get_time();
    int successful = 0;

    for (int i = 0; i < NUM_KEYS; i++) {
        generate_synthetic_value(value_buffer, scenario->random_bytes, scenario->zero_bytes);

        double insert_start = get_time();
        int insert_result = bplus_tree_compressed_put(ct_tree, i, i * 1000 + rand() % 1000);
        double insert_end = get_time();

        if (insert_result == 0) {
            insert_latencies[successful] = insert_end - insert_start;
            successful++;
        } else {
            printf("Warning: Failed to insert key %d\n", i);
        }

        if ((i + 1) % 10000 == 0) {
            printf("  Inserted %d/%d pairs (%.1f%%)\n", i + 1, NUM_KEYS, (i + 1) * 100.0 / NUM_KEYS);
        }

        if (MIXED_WORKLOAD_ENABLED && ((i + 1) % MIXED_INSERT_BATCH == 0)) {
            printf("  Performing %d buffered reads after %d inserts...\n", MIXED_READS_PER_BATCH, i + 1);
            read_phase_stats buffered_stats = perform_random_reads(ct_tree,
                                                                    i + 1, MIXED_READS_PER_BATCH,
                                                                    buffered_read_latencies,
                                                                    &buffered_latency_count,
                                                                    false);
            result.buffer_hits_before_flush += buffered_stats.buffer_hits_delta;
            result.buffer_misses_before_flush += buffered_stats.buffer_misses_delta;
            result.read_operations_buffered += buffered_stats.ops;
            buffered_total_time += buffered_stats.total_time;
            printf("    Buffered reads: %d ops, hits Δ %d, misses Δ %d\n", 
                   buffered_stats.ops, buffered_stats.buffer_hits_delta, buffered_stats.buffer_misses_delta);

            printf("  Performing %d post-flush reads...\n", MIXED_READS_PER_BATCH);
            read_phase_stats postflush_stats = perform_random_reads(ct_tree,
                                                                     i + 1, MIXED_READS_PER_BATCH,
                                                                     postflush_read_latencies,
                                                                     &postflush_latency_count,
                                                                     true);
            result.buffer_hits_after_flush += postflush_stats.buffer_hits_delta;
            result.buffer_misses_after_flush += postflush_stats.buffer_misses_delta;
            result.read_operations_postflush += postflush_stats.ops;
            postflush_total_time += postflush_stats.total_time;
            printf("    Post-flush reads: %d ops, hits Δ %d, misses Δ %d\n", 
                   postflush_stats.ops, postflush_stats.buffer_hits_delta, postflush_stats.buffer_misses_delta);
        }
    }

    double end_time = get_time();
    result.total_time = end_time - start_time;
    result.successful_insertions = successful;
    result.throughput = successful / result.total_time;
    result.p99_latency = calculate_p99_latency(insert_latencies, successful);

    if (result.read_operations_buffered > 0 && buffered_latency_count > 0 && buffered_total_time > 0.0) {
        result.read_p99_buffered = calculate_p99_latency(buffered_read_latencies, buffered_latency_count);
        result.read_throughput_buffered = result.read_operations_buffered / buffered_total_time;
    }
    if (result.read_operations_postflush > 0 && postflush_latency_count > 0 && postflush_total_time > 0.0) {
        result.read_p99_postflush = calculate_p99_latency(postflush_read_latencies, postflush_latency_count);
        result.read_throughput_postflush = result.read_operations_postflush / postflush_total_time;
    }

    printf("Benchmark Results:\n");
    printf("  Successful insertions: %d/%d\n", successful, NUM_KEYS);
    printf("  Total time: %.3f seconds\n", result.total_time);
    printf("  Throughput: %.0f insertions/second\n", result.throughput);
    printf("  P99 Latency: %.2f microseconds\n", result.p99_latency);

    if (result.read_operations_buffered > 0) {
        printf("  Buffered Reads: %d ops, %.0f ops/sec, P99 %.2f μs, buffer hits Δ %d, misses Δ %d\n",
               result.read_operations_buffered,
               result.read_throughput_buffered,
               result.read_p99_buffered,
               result.buffer_hits_before_flush,
               result.buffer_misses_before_flush);
    }
    if (result.read_operations_postflush > 0) {
        printf("  Post-flush Reads: %d ops, %.0f ops/sec, P99 %.2f μs, buffer hits Δ %d, misses Δ %d\n",
               result.read_operations_postflush,
               result.read_throughput_postflush,
               result.read_p99_postflush,
               result.buffer_hits_after_flush,
               result.buffer_misses_after_flush);
    }

    size_t total_uncompressed = 0;
    size_t total_compressed = 0;
    int stats_result = bplus_tree_compressed_stats(ct_tree, &total_uncompressed, &total_compressed);

    if (stats_result == 0 && total_uncompressed > 0) {
        result.compression_ratio = (double)total_uncompressed / total_compressed;
        result.original_size = total_uncompressed;
        result.compressed_size = total_compressed;

        printf("  Original size: %zu bytes (%.2f MB)\n",
               total_uncompressed, total_uncompressed / (1024.0 * 1024.0));
        printf("  Compressed size: %zu bytes (%.2f MB)\n",
               total_compressed, total_compressed / (1024.0 * 1024.0));
        printf("  Compression ratio: %.3fx (%.1f%% savings)\n",
               result.compression_ratio, (1.0 - 1.0/result.compression_ratio) * 100.0);
    } else {
        printf("  Compression statistics not available from API\n");
        result.compression_ratio = test_direct_compression_ratio(scenario, algo, algo_name);
        result.original_size = successful * VALUE_SIZE;
        result.compressed_size = (size_t)(result.original_size / result.compression_ratio);
        printf("  Estimated compression ratio: %.3fx\n", result.compression_ratio);
    }

    free(insert_latencies);
    free(buffered_read_latencies);
    free(postflush_read_latencies);
    bplus_tree_compressed_deinit(ct_tree);

    return result;
}

void write_results_to_file(benchmark_result_t *results, int num_results) {
    FILE *report = fopen("2025-09-04_mixed_workload_compression_benchmark.md", "w");
    if (!report) {
        printf("Error: Cannot create results file\n");
        return;
    }

    fprintf(report, "# DRAM-tier B+ Tree Mixed Workload Compression Benchmark\n\n");
    fprintf(report, "**Date:** September 4, 2025  \\n");
    fprintf(report, "**Objective:** Evaluate compression under mixed insert/read workloads with lazy buffering.\n\n");

    fprintf(report, "## Test Configuration\n\n");
    fprintf(report, "- **Number of key-value pairs:** %d\n", NUM_KEYS);
    fprintf(report, "- **Value size:** %d bytes (fixed)\n", VALUE_SIZE);
    fprintf(report, "- **Mixed workload:** %s\n",
            MIXED_WORKLOAD_ENABLED ? "Enabled" : "Disabled");
    if (MIXED_WORKLOAD_ENABLED) {
        fprintf(report, "  - Insert batch size: %d\n", MIXED_INSERT_BATCH);
        fprintf(report, "  - Reads per batch: %d\n", MIXED_READS_PER_BATCH);
    }
    fprintf(report, "\n### Test Scenarios\n\n");
    for (int i = 0; i < NUM_SCENARIOS; i++) {
        fprintf(report, "- **%s:** %d random bytes + %d zero bytes (%.0f%% random data)\n",
                scenarios[i].name, scenarios[i].random_bytes, scenarios[i].zero_bytes,
                scenarios[i].random_bytes * 100.0 / VALUE_SIZE);
    }

    fprintf(report, "\n## Benchmark Results\n\n");
    fprintf(report, "| Scenario | Algorithm | Ins Throughput (ops/sec) | Ins P99 (μs) | Buf Read Throughput (ops/sec) | Buf Read P99 (μs) | Post Flush Throughput (ops/sec) | Post Flush P99 (μs) | Compression Ratio |\n");
    fprintf(report, "|----------|-----------|-------------------------|--------------|-------------------------------|-------------------|-------------------------------|---------------------|-------------------|\n");

    for (int i = 0; i < num_results; i++) {
        fprintf(report,
                "| %s | %s | %.0f | %.2f | %.0f | %.2f | %.0f | %.2f | %.3fx |\n",
                results[i].scenario_name,
                results[i].algorithm_name,
                results[i].throughput,
                results[i].p99_latency,
                results[i].read_operations_buffered > 0 ? results[i].read_throughput_buffered : 0.0,
                results[i].read_p99_buffered,
                results[i].read_operations_postflush > 0 ? results[i].read_throughput_postflush : 0.0,
                results[i].read_p99_postflush,
                results[i].compression_ratio);
    }

    fprintf(report, "\n## Buffer Hit Analysis\n\n");
    for (int i = 0; i < num_results; i++) {
        fprintf(report,
                "- %s + %s: buffered hits Δ %d, misses Δ %d; post-flush hits Δ %d, misses Δ %d\n",
                results[i].scenario_name,
                results[i].algorithm_name,
                results[i].buffer_hits_before_flush,
                results[i].buffer_misses_before_flush,
                results[i].buffer_hits_after_flush,
                results[i].buffer_misses_after_flush);
    }

    fclose(report);
    printf("\nResults written to: 2025-09-04_mixed_workload_compression_benchmark.md\n");
}

int main() {
    printf("DRAM-tier B+ Tree Mixed Workload Compression Benchmark\n");
    printf("=========================================================\n");
    printf("Testing controlled compressibility scenarios with LZ4 and QPL under mixed workloads\n");
    printf("Configuration: %d keys, %d-byte synthetic values\n\n", NUM_KEYS, VALUE_SIZE);

    srand(42);

    benchmark_result_t results[NUM_SCENARIOS * 2];
    int result_count = 0;

    for (int s = 0; s < NUM_SCENARIOS; s++) {
        printf("\n" "=" "=" "=" " TESTING %s " "=" "=" "=" "\n", scenarios[s].name);
        printf("Value structure: %d random bytes + %d zero bytes\n",
               scenarios[s].random_bytes, scenarios[s].zero_bytes);

        results[result_count++] = run_benchmark(&scenarios[s], COMPRESS_LZ4, "LZ4");
        results[result_count++] = run_benchmark(&scenarios[s], COMPRESS_QPL, "QPL");
    }

    write_results_to_file(results, result_count);

    printf("\n" "=" "=" "=" " BENCHMARK SUMMARY " "=" "=" "=" "\n");
    printf("Completed %d benchmark tests across %d scenarios:\n", result_count, NUM_SCENARIOS);

    for (int i = 0; i < result_count; i++) {
        printf("- %s + %s: %.0f insert ops/sec, %.2f μs insert P99, %.3fx compression\n",
               results[i].scenario_name,
               results[i].algorithm_name,
               results[i].throughput,
               results[i].p99_latency,
               results[i].compression_ratio);
        if (results[i].read_operations_buffered > 0) {
            printf("    Buffered reads: %.0f ops/sec, %.2f μs P99 (hits Δ %d, misses Δ %d)\n",
                   results[i].read_throughput_buffered,
                   results[i].read_p99_buffered,
                   results[i].buffer_hits_before_flush,
                   results[i].buffer_misses_before_flush);
        }
        if (results[i].read_operations_postflush > 0) {
            printf("    Post-flush reads: %.0f ops/sec, %.2f μs P99 (hits Δ %d, misses Δ %d)\n",
                   results[i].read_throughput_postflush,
                   results[i].read_p99_postflush,
                   results[i].buffer_hits_after_flush,
                   results[i].buffer_misses_after_flush);
        }
    }

    printf("\nDetailed results and analysis saved to markdown report.\n");
    return 0;
}
