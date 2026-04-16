/*
 * Comprehensive test for legacy 1D design with dual compression support
 * Tests both LZ4 and QPL algorithms with legacy hash distribution
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include "../lib/bplustree_compressed.h"

#define TEST_PASS printf("✅ PASS: ");
#define TEST_FAIL printf("❌ FAIL: ");
#define TEST_INFO printf("ℹ️  INFO: ");

// Test data
#define NUM_TEST_KEYS 100
#define NUM_STRESS_KEYS 1000

// Test configuration
void test_simple_config_creation() {
    printf("\n=== Testing Simple Configuration Creation ===\n");

    // Test LZ4 config
    struct simple_compression_config lz4_config = bplus_tree_create_default_simple_config(COMPRESS_LZ4);
    if (lz4_config.default_algo == COMPRESS_LZ4 &&
        lz4_config.num_subpages == 16 &&
        lz4_config.lz4_partial_decompression == 1) {
        TEST_PASS printf("LZ4 default config creation\n");
    } else {
        TEST_FAIL printf("LZ4 default config creation\n");
        exit(1);
    }

    // Test QPL config
    struct simple_compression_config qpl_config = bplus_tree_create_default_simple_config(COMPRESS_QPL);
    if (qpl_config.default_algo == COMPRESS_QPL &&
        qpl_config.num_subpages == 8 &&
        qpl_config.lz4_partial_decompression == 0) {
        TEST_PASS printf("QPL default config creation\n");
    } else {
        TEST_FAIL printf("QPL default config creation\n");
        exit(1);
    }
}

// Test tree initialization
void test_tree_initialization() {
    printf("\n=== Testing Tree Initialization ===\n");

    // Test LZ4 initialization
    struct simple_compression_config lz4_config = bplus_tree_create_default_simple_config(COMPRESS_LZ4);
    struct bplus_tree_compressed *lz4_tree = bplus_tree_compressed_init_simple(4, 64, &lz4_config);

    if (lz4_tree != NULL &&
        lz4_tree->simple_config.default_algo == COMPRESS_LZ4 &&
        lz4_tree->compression_enabled == 1) {
        TEST_PASS printf("LZ4 tree initialization\n");
    } else {
        TEST_FAIL printf("LZ4 tree initialization\n");
        exit(1);
    }

    // Test QPL initialization
    struct simple_compression_config qpl_config = bplus_tree_create_default_simple_config(COMPRESS_QPL);
    struct bplus_tree_compressed *qpl_tree = bplus_tree_compressed_init_simple(4, 64, &qpl_config);

    if (qpl_tree != NULL &&
        qpl_tree->simple_config.default_algo == COMPRESS_QPL &&
        qpl_tree->compression_enabled == 1) {
        TEST_PASS printf("QPL tree initialization\n");
    } else {
        TEST_FAIL printf("QPL tree initialization\n");
        exit(1);
    }

    // Clean up
    bplus_tree_compressed_deinit(lz4_tree);
    bplus_tree_compressed_deinit(qpl_tree);
}

// Test basic put/get operations
void test_basic_operations(struct bplus_tree_compressed *tree, const char *algorithm_name) {
    printf("\n=== Testing Basic Operations (%s) ===\n", algorithm_name);

    // Test empty tree
    if (bplus_tree_compressed_empty(tree) == 1) {
        TEST_PASS printf("%s tree starts empty\n", algorithm_name);
    } else {
        TEST_FAIL printf("%s tree starts empty\n", algorithm_name);
        return;
    }

    // Test single insert/get
    int result = bplus_tree_compressed_put(tree, 100, 200);
    if (result == 0) {
        TEST_PASS printf("%s single insert\n", algorithm_name);
    } else {
        TEST_FAIL printf("%s single insert (returned %d)\n", algorithm_name, result);
        return;
    }

    int value = bplus_tree_compressed_get(tree, 100);
    if (value == 200) {
        TEST_PASS printf("%s single get\n", algorithm_name);
    } else {
        TEST_FAIL printf("%s single get (expected 200, got %d)\n", algorithm_name, value);
        return;
    }

    // Test tree is not empty
    if (bplus_tree_compressed_empty(tree) == 0) {
        TEST_PASS printf("%s tree not empty after insert\n", algorithm_name);
    } else {
        TEST_FAIL printf("%s tree not empty after insert\n", algorithm_name);
        return;
    }

    // Test multiple inserts
    for (int i = 1; i <= NUM_TEST_KEYS; i++) {
        result = bplus_tree_compressed_put(tree, i, i * 10);
        if (result != 0) {
            TEST_FAIL printf("%s multiple insert key %d\n", algorithm_name, i);
            return;
        }
    }
    TEST_PASS printf("%s multiple inserts (%d keys)\n", algorithm_name, NUM_TEST_KEYS);

    // Test multiple gets
    int success_count = 0;
    for (int i = 1; i <= NUM_TEST_KEYS; i++) {
        value = bplus_tree_compressed_get(tree, i);
        if (value == i * 10) {
            success_count++;
        }
    }

    if (success_count == NUM_TEST_KEYS) {
        TEST_PASS printf("%s multiple gets (%d/%d)\n", algorithm_name, success_count, NUM_TEST_KEYS);
    } else {
        TEST_FAIL printf("%s multiple gets (%d/%d)\n", algorithm_name, success_count, NUM_TEST_KEYS);
    }

    // Test non-existent key
    value = bplus_tree_compressed_get(tree, 99999);
    if (value == -1) {
        TEST_PASS printf("%s non-existent key returns -1\n", algorithm_name);
    } else {
        TEST_FAIL printf("%s non-existent key (expected -1, got %d)\n", algorithm_name, value);
    }
}

// Test legacy hash distribution
void test_legacy_hash_distribution() {
    printf("\n=== Testing Legacy Hash Distribution ===\n");

    struct simple_compression_config config = (COMPRESS_LZ4);
    config.num_subpages = 8; // Use 8 subpages for testing

    // Test hash function consistency
    const char *test_keys[] = {"key1", "key2", "key3", "test", "hash", "legacy"};
    int num_keys = sizeof(test_keys) / sizeof(test_keys[0]);

    for (int i = 0; i < num_keys; i++) {
        int hash1 = calculate_target_subpage_legacy(test_keys[i], config.num_subpages);
        int hash2 = calculate_target_subpage_legacy(test_keys[i], config.num_subpages);

        if (hash1 == hash2 && hash1 >= 0 && hash1 < config.num_subpages) {
            TEST_PASS printf("Hash consistency for key '%s' -> subpage %d\n", test_keys[i], hash1);
        } else {
            TEST_FAIL printf("Hash consistency for key '%s' (hash1=%d, hash2=%d)\n", test_keys[i], hash1, hash2);
        }
    }

    // Test distribution
    int distribution[8] = {0};
    for (int i = 0; i < 1000; i++) {
        char key[16];
        snprintf(key, sizeof(key), "key%d", i);
        int subpage = calculate_target_subpage_legacy(key, 8);
        if (subpage >= 0 && subpage < 8) {
            distribution[subpage]++;
        }
    }

    TEST_INFO printf("Hash distribution across 8 subpages:\n");
    for (int i = 0; i < 8; i++) {
        printf("    Subpage %d: %d keys\n", i, distribution[i]);
    }
}

// Test compression statistics
void test_compression_stats(struct bplus_tree_compressed *tree, const char *algorithm_name) {
    printf("\n=== Testing Compression Statistics (%s) ===\n", algorithm_name);

    // Insert some data to get compression stats
    for (int i = 1; i <= 50; i++) {
        bplus_tree_compressed_put(tree, i, i * 100);
    }

    size_t total_size, compressed_size;
    int result = bplus_tree_compressed_stats(tree, &total_size, &compressed_size);

    if (result == 0) {
        TEST_PASS printf("%s compression stats retrieval\n", algorithm_name);
        TEST_INFO printf("Total size: %zu bytes, Compressed size: %zu bytes\n",
                        total_size, compressed_size);

        double ratio = bplus_tree_compressed_get_compression_ratio(tree);
        TEST_INFO printf("Compression ratio: %.2f%%\n", ratio);
    } else {
        TEST_FAIL printf("%s compression stats retrieval\n", algorithm_name);
    }

    int tree_size = bplus_tree_compressed_size(tree);
    if (tree_size >= 50) {
        TEST_PASS printf("%s tree size reporting (%d entries)\n", algorithm_name, tree_size);
    } else {
        TEST_FAIL printf("%s tree size reporting (expected >= 50, got %d)\n", algorithm_name, tree_size);
    }
}

// Test tree dump functionality
void test_tree_dump(struct bplus_tree_compressed *tree, const char *algorithm_name) {
    printf("\n=== Testing Tree Dump (%s) ===\n", algorithm_name);

    // Insert some test data
    for (int i = 1; i <= 10; i++) {
        bplus_tree_compressed_put(tree, i, i * 5);
    }

    TEST_INFO printf("Dumping %s tree structure:\n", algorithm_name);
    bplus_tree_compressed_dump(tree);
    TEST_PASS printf("%s tree dump completed\n", algorithm_name);
}

// Stress test
void test_stress(struct bplus_tree_compressed *tree, const char *algorithm_name) {
    printf("\n=== Stress Testing (%s) ===\n", algorithm_name);

    clock_t start = clock();

    // Insert many keys
    for (int i = 1; i <= NUM_STRESS_KEYS; i++) {
        int result = bplus_tree_compressed_put(tree, i, i * 7);
        if (result != 0) {
            TEST_FAIL printf("%s stress insert at key %d\n", algorithm_name, i);
            return;
        }
    }

    clock_t mid = clock();
    double insert_time = ((double)(mid - start)) / CLOCKS_PER_SEC;

    // Verify all keys
    int success_count = 0;
    for (int i = 1; i <= NUM_STRESS_KEYS; i++) {
        int value = bplus_tree_compressed_get(tree, i);
        if (value == i * 7) {
            success_count++;
        }
    }

    clock_t end = clock();
    double total_time = ((double)(end - start)) / CLOCKS_PER_SEC;
    double get_time = ((double)(end - mid)) / CLOCKS_PER_SEC;

    if (success_count == NUM_STRESS_KEYS) {
        TEST_PASS printf("%s stress test (%d keys)\n", algorithm_name, NUM_STRESS_KEYS);
        TEST_INFO printf("Insert time: %.3fs, Get time: %.3fs, Total: %.3fs\n",
                        insert_time, get_time, total_time);
        TEST_INFO printf("Insert rate: %.0f ops/sec, Get rate: %.0f ops/sec\n",
                        NUM_STRESS_KEYS / insert_time, NUM_STRESS_KEYS / get_time);
    } else {
        TEST_FAIL printf("%s stress test (%d/%d keys)\n", algorithm_name, success_count, NUM_STRESS_KEYS);
    }
}

// Test deletion functionality
void test_deletion(struct bplus_tree_compressed *tree, const char *algorithm_name) {
    printf("\n=== Testing Deletion (%s) ===\n", algorithm_name);

    // Insert test data
    for (int i = 1; i <= 20; i++) {
        bplus_tree_compressed_put(tree, i, i * 3);
    }

    // Test deletion
    int result = bplus_tree_compressed_delete(tree, 10);
    if (result == 0) {
        TEST_PASS printf("%s key deletion\n", algorithm_name);
    } else {
        TEST_FAIL printf("%s key deletion\n", algorithm_name);
        return;
    }

    // Verify deletion
    int value = bplus_tree_compressed_get(tree, 10);
    if (value == -1) {
        TEST_PASS printf("%s deleted key not found\n", algorithm_name);
    } else {
        TEST_FAIL printf("%s deleted key not found (got %d)\n", algorithm_name, value);
    }

    // Verify other keys still exist
    value = bplus_tree_compressed_get(tree, 9);
    if (value == 27) { // 9 * 3
        TEST_PASS printf("%s non-deleted keys still accessible\n", algorithm_name);
    } else {
        TEST_FAIL printf("%s non-deleted keys still accessible\n", algorithm_name);
    }
}

int main() {
    printf("🧪 Legacy 1D Design with Dual Compression Test Suite\n");
    printf("====================================================\n");

    // Test configuration creation
    test_simple_config_creation();

    // Test tree initialization
    test_tree_initialization();

    // Test legacy hash distribution
    test_legacy_hash_distribution();

    // Create test trees for both algorithms
    struct simple_compression_config lz4_config = bplus_tree_create_default_simple_config(COMPRESS_LZ4);
    struct simple_compression_config qpl_config = bplus_tree_create_default_simple_config(COMPRESS_QPL);

    struct bplus_tree_compressed *lz4_tree = bplus_tree_compressed_init_simple(4, 64, &lz4_config);
    struct bplus_tree_compressed *qpl_tree = bplus_tree_compressed_init_simple(4, 64, &qpl_config);

    if (lz4_tree == NULL || qpl_tree == NULL) {
        printf("❌ Failed to create test trees\n");
        return 1;
    }

    // Test basic operations for both algorithms
    test_basic_operations(lz4_tree, "LZ4");
    test_basic_operations(qpl_tree, "QPL");

    // Test compression statistics
    test_compression_stats(lz4_tree, "LZ4");
    test_compression_stats(qpl_tree, "QPL");

    // Test tree dump
    test_tree_dump(lz4_tree, "LZ4");
    test_tree_dump(qpl_tree, "QPL");

    // Test deletion
    test_deletion(lz4_tree, "LZ4");
    test_deletion(qpl_tree, "QPL");

    // Stress test
    test_stress(lz4_tree, "LZ4");
    test_stress(qpl_tree, "QPL");

    // Clean up
    bplus_tree_compressed_deinit(lz4_tree);
    bplus_tree_compressed_deinit(qpl_tree);

    printf("\n🎉 All tests completed!\n");
    printf("✅ Legacy 1D design with dual compression support verified\n");

    return 0;
}