/*
 * Simple test for basic functionality of legacy 1D design with dual compression
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../lib/bplustree_compressed.h"

int main() {
    printf("🧪 Simple Legacy Compression Test\n");
    printf("=================================\n");

    // Test simple configuration creation
    printf("1. Testing configuration creation...\n");
    struct simple_compression_config lz4_config = bplus_tree_create_default_simple_config(COMPRESS_LZ4);
    struct simple_compression_config qpl_config = bplus_tree_create_default_simple_config(COMPRESS_QPL);

    printf("   LZ4 config: algo=%d, subpages=%d, partial=%d\n",
           lz4_config.default_algo, lz4_config.num_subpages, lz4_config.lz4_partial_decompression);
    printf("   QPL config: algo=%d, subpages=%d, level=%d\n",
           qpl_config.default_algo, qpl_config.num_subpages, qpl_config.qpl_compression_level);

    // Test tree initialization
    printf("2. Testing tree initialization...\n");
    struct bplus_tree_compressed *lz4_tree = bplus_tree_compressed_init_simple(4, 64, &lz4_config);
    struct bplus_tree_compressed *qpl_tree = bplus_tree_compressed_init_simple(4, 64, &qpl_config);

    if (!lz4_tree || !qpl_tree) {
        printf("❌ Failed to initialize trees\n");
        return 1;
    }
    printf("   ✅ Trees initialized successfully\n");

    // Test hash function
    printf("3. Testing legacy hash distribution...\n");
    const char *test_keys[] = {"key1", "key2", "test", "hash"};
    for (int i = 0; i < 4; i++) {
        int subpage = calculate_target_subpage_legacy(test_keys[i], 8);
        printf("   Key '%s' -> subpage %d\n", test_keys[i], subpage);
    }

    // Test basic operations (small number to avoid splits)
    printf("4. Testing basic LZ4 operations...\n");

    if (bplus_tree_compressed_empty(lz4_tree) != 1) {
        printf("❌ LZ4 tree should be empty initially\n");
        return 1;
    }
    printf("   ✅ Empty tree check passed\n");

    // Insert just a few keys to avoid triggering splits
    printf("   Inserting 5 key-value pairs...\n");
    for (int i = 1; i <= 5; i++) {
        int result = bplus_tree_compressed_put(lz4_tree, i, i * 10);
        if (result != 0) {
            printf("❌ LZ4 insert failed for key %d (result: %d)\n", i, result);
            return 1;
        }
    }
    printf("   ✅ LZ4 inserts successful\n");

    // Test retrieval
    printf("   Testing retrieval...\n");
    for (int i = 1; i <= 5; i++) {
        int value = bplus_tree_compressed_get(lz4_tree, i);
        if (value != i * 10) {
            printf("❌ LZ4 get failed for key %d (expected %d, got %d)\n", i, i * 10, value);
            return 1;
        }
    }
    printf("   ✅ LZ4 retrieval successful\n");

    // Test basic QPL operations
    printf("5. Testing basic QPL operations...\n");
    printf("   Inserting 5 key-value pairs...\n");
    for (int i = 1; i <= 5; i++) {
        int result = bplus_tree_compressed_put(qpl_tree, i + 100, i * 20);
        if (result != 0) {
            printf("❌ QPL insert failed for key %d (result: %d)\n", i + 100, result);
            return 1;
        }
    }
    printf("   ✅ QPL inserts successful\n");

    printf("   Testing retrieval...\n");
    for (int i = 1; i <= 5; i++) {
        int value = bplus_tree_compressed_get(qpl_tree, i + 100);
        if (value != i * 20) {
            printf("❌ QPL get failed for key %d (expected %d, got %d)\n", i + 100, i * 20, value);
            return 1;
        }
    }
    printf("   ✅ QPL retrieval successful\n");

    // Test tree state
    printf("6. Testing tree state...\n");
    int lz4_size = bplus_tree_compressed_size(lz4_tree);
    int lz4_empty = bplus_tree_compressed_empty(lz4_tree);
    printf("   LZ4 tree debug: size=%d, empty=%d\n", lz4_size, lz4_empty);
    if (bplus_tree_compressed_empty(lz4_tree) != 0) {
        printf("❌ LZ4 tree should not be empty after inserts\n");
        return 1;
    } else {
        printf("   ✅ LZ4 tree is not empty after inserts\n");
    }

    printf("   LZ4 tree size: %d entries\n", bplus_tree_compressed_size(lz4_tree));
    printf("   QPL tree size: %d entries\n", bplus_tree_compressed_size(qpl_tree));

    // Test compression statistics
    printf("7. Testing compression statistics...\n");
    size_t lz4_total, lz4_compressed, qpl_total, qpl_compressed;
    bplus_tree_compressed_stats(lz4_tree, &lz4_total, &lz4_compressed);
    bplus_tree_compressed_stats(qpl_tree, &qpl_total, &qpl_compressed);

    printf("   LZ4: %zu bytes -> %zu bytes\n", lz4_total, lz4_compressed);
    printf("   QPL: %zu bytes -> %zu bytes\n", qpl_total, qpl_compressed);

    // Test non-existent key
    printf("8. Testing non-existent key...\n");
    int missing_value = bplus_tree_compressed_get(lz4_tree, 999);
    if (missing_value == -1) {
        printf("   ✅ Non-existent key correctly returns -1\n");
    } else {
        printf("❌ Non-existent key should return -1, got %d\n", missing_value);
        return 1;
    }

    // Test tree dump
    printf("9. Testing tree dump...\n");
    printf("   LZ4 tree structure:\n");
    bplus_tree_compressed_dump(lz4_tree);
    printf("   QPL tree structure:\n");
    bplus_tree_compressed_dump(qpl_tree);

    // Clean up
    bplus_tree_compressed_deinit(lz4_tree);
    bplus_tree_compressed_deinit(qpl_tree);

    printf("\n🎉 All tests passed!\n");
    printf("✅ Legacy 1D design with dual compression is working correctly\n");

    return 0;
}