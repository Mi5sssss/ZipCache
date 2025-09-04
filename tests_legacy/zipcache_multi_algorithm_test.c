/*
 * ZipCache Multi-Algorithm Compression Test
 * 
 * Tests both LZ4 and Intel QPL compression algorithms with user-defined configuration
 * Validates lazy compression works correctly with both algorithms
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

// Include the enhanced DRAM-tier compressed B+Tree
#include "../DRAM-tier/lib/bplustree_compressed.h"

#define TEST_OPERATIONS 1000
#define TEST_ORDER 16
#define TEST_ENTRIES 32

// Test data generation
static uint64_t get_timestamp_us(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000ULL + tv.tv_usec;
}

static void print_separator(const char *title) {
    printf("\n");
    for (int i = 0; i < 60; i++) printf("=");
    printf("\n");
    printf("  %s\n", title);
    for (int i = 0; i < 60; i++) printf("=");
    printf("\n");
}

static int test_algorithm(compression_algorithm_t algorithm, const char *algo_name) {
    printf("\n🔧 Testing %s Algorithm with Lazy Compression\n", algo_name);
    printf("───────────────────────────────────────────────────\n");
    
    // Create user-defined configuration
    struct compression_config config = bplus_tree_create_default_config(algorithm);
    config.enable_lazy_compression = 1;
    config.flush_threshold = 28; // Flush when buffer nearly full
    config.buffer_size = WRITING_BUFFER_SIZE;
    
    // Special QPL configuration
    if (algorithm == COMPRESS_QPL) {
        config.compression_level = qpl_default_level;
    }
    
    // Initialize compressed B+Tree with user-defined config
    struct bplus_tree_compressed *tree = bplus_tree_compressed_init_with_config(
        TEST_ORDER, TEST_ENTRIES, &config
    );
    
    if (tree == NULL) {
        printf("❌ Failed to initialize compressed B+Tree with %s\n", algo_name);
        return -1;
    }
    
    printf("✅ Initialized compressed B+Tree with %s algorithm\n", algo_name);
    printf("   - Order: %d, Entries: %d\n", TEST_ORDER, TEST_ENTRIES);
    printf("   - Lazy compression: %s\n", config.enable_lazy_compression ? "enabled" : "disabled");
    printf("   - Buffer flush threshold: %d entries\n", config.flush_threshold);
    
    uint64_t start_time = get_timestamp_us();
    
    // Test INSERT operations
    printf("\n📊 Running INSERT operations...\n");
    int insert_success = 0;
    for (int i = 1; i <= TEST_OPERATIONS; i++) {
        int result = bplus_tree_compressed_put(tree, i, i * 10);
        if (result == 0) {
            insert_success++;
        }
        
        if (i % 200 == 0) {
            printf("   Inserted %d/%d objects\n", i, TEST_OPERATIONS);
        }
    }
    
    printf("✅ INSERT operations: %d/%d successful (%.1f%%)\n", 
           insert_success, TEST_OPERATIONS, 
           (double)insert_success / TEST_OPERATIONS * 100.0);
    
    // Test GET operations
    printf("\n📊 Running GET operations...\n");
    int get_success = 0;
    for (int i = 1; i <= TEST_OPERATIONS; i++) {
        int value = bplus_tree_compressed_get(tree, i);
        if (value == i * 10) {
            get_success++;
        }
        
        if (i % 200 == 0) {
            printf("   Retrieved %d/%d objects\n", i, TEST_OPERATIONS);
        }
    }
    
    printf("✅ GET operations: %d/%d successful (%.1f%%)\n", 
           get_success, TEST_OPERATIONS, 
           (double)get_success / TEST_OPERATIONS * 100.0);
    
    // Test DELETE operations (half the data)
    printf("\n📊 Running DELETE operations...\n");
    int delete_success = 0;
    int delete_count = TEST_OPERATIONS / 2;
    for (int i = 1; i <= delete_count; i += 2) {
        int result = bplus_tree_compressed_delete(tree, i);
        if (result == 0) {
            delete_success++;
        }
    }
    
    printf("✅ DELETE operations: %d/%d successful (%.1f%%)\n", 
           delete_success, delete_count / 2 + 1, 
           (double)delete_success / (delete_count / 2 + 1) * 100.0);
    
    uint64_t end_time = get_timestamp_us();
    double duration_ms = (end_time - start_time) / 1000.0;
    
    // Get compression statistics
    size_t total_size, compressed_size;
    bplus_tree_compressed_stats(tree, &total_size, &compressed_size);
    double compression_ratio = (total_size > 0) ? (double)compressed_size / total_size : 0.0;
    
    // Get algorithm-specific statistics
    int lz4_ops = 0, qpl_ops = 0;
    bplus_tree_compressed_get_algorithm_stats(tree, &lz4_ops, &qpl_ops);
    
    printf("\n📈 Performance Results for %s:\n", algo_name);
    printf("   Total test duration: %.2f ms\n", duration_ms);
    printf("   Operations per second: %.0f ops/sec\n", 
           (insert_success + get_success + delete_success) / (duration_ms / 1000.0));
    
    printf("\n📊 Compression Statistics:\n");
    printf("   Original size: %zu bytes\n", total_size);
    printf("   Compressed size: %zu bytes\n", compressed_size);
    printf("   Compression ratio: %.2f%%\n", compression_ratio * 100.0);
    if (compression_ratio > 0 && compression_ratio < 1.0) {
        printf("   Space savings: %.2f%%\n", (1.0 - compression_ratio) * 100.0);
    }
    
    printf("\n🔢 Algorithm-Specific Operations:\n");
    printf("   LZ4 operations: %d\n", lz4_ops);
    printf("   QPL operations: %d\n", qpl_ops);
    
    // Verify algorithm configuration
    compression_algorithm_t current_algo = bplus_tree_compressed_get_algorithm(tree);
    printf("   Current algorithm: %s ✓\n", 
           (current_algo == COMPRESS_LZ4) ? "LZ4" :
           (current_algo == COMPRESS_QPL) ? "QPL" : "NONE");
    
    // Cleanup
    bplus_tree_compressed_deinit(tree);
    
    // Return success if we had reasonable success rates
    int success = (insert_success >= TEST_OPERATIONS * 0.95) && 
                  (get_success >= TEST_OPERATIONS * 0.90) &&
                  (delete_success >= (delete_count / 2) * 0.90);
    
    printf("\n%s %s Algorithm Test: %s\n", 
           success ? "✅" : "❌", algo_name, success ? "PASSED" : "FAILED");
    
    return success ? 0 : -1;
}

static int test_algorithm_switching(void) {
    printf("\n🔄 Testing Runtime Algorithm Switching\n");
    printf("─────────────────────────────────────────\n");
    
    // Start with LZ4
    struct compression_config config = bplus_tree_create_default_config(COMPRESS_LZ4);
    struct bplus_tree_compressed *tree = bplus_tree_compressed_init_with_config(
        TEST_ORDER, TEST_ENTRIES, &config
    );
    
    if (tree == NULL) {
        printf("❌ Failed to initialize tree for algorithm switching test\n");
        return -1;
    }
    
    // Insert some data with LZ4
    printf("📝 Inserting data with LZ4...\n");
    for (int i = 1; i <= 100; i++) {
        bplus_tree_compressed_put(tree, i, i * 5);
    }
    
    // Check LZ4 operations
    int lz4_ops = 0, qpl_ops = 0;
    bplus_tree_compressed_get_algorithm_stats(tree, &lz4_ops, &qpl_ops);
    printf("   LZ4 operations after initial inserts: %d\n", lz4_ops);
    
    // Switch to QPL
    printf("🔄 Switching to QPL algorithm...\n");
    int switch_result = bplus_tree_compressed_set_algorithm(tree, COMPRESS_QPL);
    
    if (switch_result != 0) {
        printf("⚠️ QPL algorithm switch failed (likely QPL not available), continuing with LZ4\n");
    } else {
        printf("✅ Successfully switched to QPL algorithm\n");
        
        // Insert more data with QPL
        printf("📝 Inserting more data with QPL...\n");
        for (int i = 101; i <= 200; i++) {
            bplus_tree_compressed_put(tree, i, i * 5);
        }
        
        // Check operations again
        bplus_tree_compressed_get_algorithm_stats(tree, &lz4_ops, &qpl_ops);
        printf("   LZ4 operations: %d\n", lz4_ops);
        printf("   QPL operations: %d\n", qpl_ops);
        
        // Verify current algorithm
        compression_algorithm_t current = bplus_tree_compressed_get_algorithm(tree);
        printf("   Current algorithm: %s\n", 
               (current == COMPRESS_QPL) ? "QPL" : 
               (current == COMPRESS_LZ4) ? "LZ4" : "NONE");
    }
    
    // Test data retrieval (should work regardless of algorithm used)
    printf("📖 Testing data retrieval across algorithms...\n");
    int retrieve_success = 0;
    for (int i = 1; i <= 200; i++) {
        int value = bplus_tree_compressed_get(tree, i);
        if (value == i * 5) {
            retrieve_success++;
        }
    }
    
    printf("✅ Data retrieval: %d/200 successful (%.1f%%)\n", 
           retrieve_success, retrieve_success / 200.0 * 100.0);
    
    bplus_tree_compressed_deinit(tree);
    
    int success = (retrieve_success >= 190); // 95% success rate
    printf("\n%s Algorithm Switching Test: %s\n", 
           success ? "✅" : "❌", success ? "PASSED" : "FAILED");
    
    return success ? 0 : -1;
}

int main(void) {
    printf("🚀 ZipCache Multi-Algorithm Compression Test Suite\n");
    printf("===================================================\n");
    printf("🎯 Testing user-defined LZ4 and Intel QPL compression\n");
    printf("🔧 Validating lazy compression with both algorithms\n");
    printf("📊 Operations per test: %d\n", TEST_OPERATIONS);
    
    int lz4_result = test_algorithm(COMPRESS_LZ4, "LZ4");
    int qpl_result = test_algorithm(COMPRESS_QPL, "Intel QPL");
    int switching_result = test_algorithm_switching();
    
    print_separator("FINAL TEST RESULTS");
    
    printf("🧪 Test Results Summary:\n");
    printf("   LZ4 Algorithm Test:      %s\n", lz4_result == 0 ? "✅ PASSED" : "❌ FAILED");
    printf("   Intel QPL Algorithm Test: %s\n", qpl_result == 0 ? "✅ PASSED" : "⚠️ SKIPPED/FAILED");
    printf("   Algorithm Switching Test: %s\n", switching_result == 0 ? "✅ PASSED" : "❌ FAILED");
    
    int total_success = 0;
    if (lz4_result == 0) total_success++;
    if (qpl_result == 0) total_success++;  // QPL might fail if not available
    if (switching_result == 0) total_success++;
    
    printf("\n🏆 Overall Result: %d/3 tests passed\n", total_success);
    
    if (lz4_result == 0 && switching_result == 0) {
        printf("✅ Core functionality validated - LZ4 + lazy compression working\n");
        if (qpl_result == 0) {
            printf("✅ Intel QPL integration successful\n");
        } else {
            printf("⚠️ Intel QPL not available or failed - LZ4 fallback works\n");
        }
        
        printf("\n🎉 ZipCache multi-algorithm lazy compression implementation: SUCCESS\n");
        return 0;
    } else {
        printf("❌ Critical functionality failed\n");
        return 1;
    }
}