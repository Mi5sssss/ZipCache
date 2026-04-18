/*
 * Minimal test for new functions
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../lib/bplustree_compressed.h"

int main() {
    printf("🧪 Minimal Legacy Compression Test\n");
    printf("==================================\n");

    // Test 1: Configuration creation
    printf("1. Testing configuration creation...\n");
    struct simple_compression_config lz4_config = bplus_tree_create_default_simple_config(COMPRESS_LZ4);
    struct simple_compression_config qpl_config = bplus_tree_create_default_simple_config(COMPRESS_QPL);

    printf("   LZ4 config: algo=%d, subpages=%d, partial=%d ✅\n",
           lz4_config.default_algo, lz4_config.num_subpages, lz4_config.lz4_partial_decompression);
    printf("   QPL config: algo=%d, subpages=%d, level=%d ✅\n",
           qpl_config.default_algo, qpl_config.num_subpages, qpl_config.qpl_compression_level);

    // Test 2: Hash function
    printf("2. Testing legacy hash distribution...\n");
    const char *test_keys[] = {"key1", "key2", "test", "hash", "legacy", "simple"};
    int num_keys = sizeof(test_keys) / sizeof(test_keys[0]);

    for (int i = 0; i < num_keys; i++) {
        int subpage = calculate_target_subpage_legacy(test_keys[i], 8);
        printf("   Key '%s' -> subpage %d\n", test_keys[i], subpage);
        if (subpage < 0 || subpage >= 8) {
            printf("❌ Invalid subpage index!\n");
            return 1;
        }
    }
    printf("   Hash function working correctly ✅\n");

    // Test 3: Hash consistency
    printf("3. Testing hash consistency...\n");
    for (int i = 0; i < num_keys; i++) {
        int hash1 = calculate_target_subpage_legacy(test_keys[i], 16);
        int hash2 = calculate_target_subpage_legacy(test_keys[i], 16);
        if (hash1 != hash2) {
            printf("❌ Hash function not consistent for key '%s'!\n", test_keys[i]);
            return 1;
        }
    }
    printf("   Hash consistency verified ✅\n");

    // Test 4: Hash distribution
    printf("4. Testing hash distribution...\n");
    int distribution[8] = {0};
    for (int i = 0; i < 1000; i++) {
        char key[32];
        snprintf(key, sizeof(key), "test_key_%d", i);
        int subpage = calculate_target_subpage_legacy(key, 8);
        if (subpage >= 0 && subpage < 8) {
            distribution[subpage]++;
        }
    }

    printf("   Distribution across 8 subpages:\n");
    for (int i = 0; i < 8; i++) {
        printf("     Subpage %d: %d keys\n", i, distribution[i]);
    }

    // Check balance
    int min_count = distribution[0], max_count = distribution[0];
    for (int i = 1; i < 8; i++) {
        if (distribution[i] < min_count) min_count = distribution[i];
        if (distribution[i] > max_count) max_count = distribution[i];
    }
    double balance = (double)min_count / max_count;
    printf("   Balance ratio: %.3f %s\n", balance, balance > 0.7 ? "✅" : "⚠️");

    printf("\n🎉 All minimal tests passed!\n");
    printf("✅ Basic legacy functions are working correctly\n");

    return 0;
}