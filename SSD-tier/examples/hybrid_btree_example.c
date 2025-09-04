#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "../lib/bplustree.h"

#define TEST_KEYS 1000
#define TEST_ORDER 16
#define TEST_ENTRIES 64

int main() {
    printf("Hybrid B+Tree Example\n");
    printf("=====================\n");
    printf("Non-leaf nodes: In memory\n");
    printf("Leaf nodes: On SSD/disk\n\n");
    
    // Initialize hybrid B+tree
    struct bplus_tree *tree = bplus_tree_init(TEST_ORDER, TEST_ENTRIES, "/tmp/hybrid_btree_example.dat");
    if (tree == NULL) {
        fprintf(stderr, "Failed to initialize hybrid B+Tree\n");
        return 1;
    }
    
    printf("Initialized hybrid B+Tree:\n");
    printf("- Order: %d (max children per non-leaf node)\n", TEST_ORDER);
    printf("- Entries: %d (max entries per disk leaf node)\n", TEST_ENTRIES);
    printf("- Disk file: /tmp/hybrid_btree_example.dat\n\n");
    
    // Insert test data
    printf("Inserting %d key-value pairs...\n", TEST_KEYS);
    clock_t start = clock();
    
    int successful_inserts = 0;
    for (int i = 1; i <= TEST_KEYS; i++) {  // Start from 1 to avoid the key=0 issue
        int result = bplus_tree_put(tree, i, i * 100);
        if (result == 0) {
            successful_inserts++;
        }
        
        if (i % 100 == 0) {
            printf("  Inserted %d keys...\n", i);
        }
    }
    
    clock_t end = clock();
    double insert_time = ((double)(end - start)) / CLOCKS_PER_SEC;
    printf("Insert completed: %d/%d successful\n", successful_inserts, TEST_KEYS);
    printf("Insert time: %.4f seconds\n", insert_time);
    printf("Insert rate: %.0f ops/sec\n\n", successful_inserts / insert_time);
    
    // Test retrieval
    printf("Testing data retrieval...\n");
    start = clock();
    
    int successful_gets = 0;
    int errors = 0;
    for (int i = 1; i <= TEST_KEYS; i++) {
        long value = bplus_tree_get(tree, i);
        if (value == i * 100) {
            successful_gets++;
        } else {
            errors++;
            if (errors <= 5) {  // Show first few errors
                printf("  Get error for key %d: expected %d, got %ld\n", i, i * 100, value);
            }
        }
    }
    
    end = clock();
    double get_time = ((double)(end - start)) / CLOCKS_PER_SEC;
    printf("Get completed: %d/%d successful\n", successful_gets, TEST_KEYS);
    printf("Get time: %.4f seconds\n", get_time);
    printf("Get rate: %.0f ops/sec\n", successful_gets / get_time);
    printf("Errors: %d (%.2f%%)\n\n", errors, (double)errors / TEST_KEYS * 100.0);
    
    // Test some specific lookups
    printf("Testing specific lookups:\n");
    int test_keys[] = {1, 50, 100, 500, 1000};
    int num_test_keys = sizeof(test_keys) / sizeof(test_keys[0]);
    
    for (int i = 0; i < num_test_keys; i++) {
        int key = test_keys[i];
        long value = bplus_tree_get(tree, key);
        printf("  Key %d: %s (got %ld)\n", 
               key, 
               value == key * 100 ? "✓" : "✗",
               value);
    }
    
    // Test non-existent key
    long non_existent = bplus_tree_get(tree, 9999);
    printf("  Non-existent key 9999: %s (got %ld)\n", 
           non_existent == -1 ? "✓" : "✗", non_existent);
    
    // Show memory vs disk usage
    printf("\nMemory vs Disk Analysis:\n");
    printf("- Non-leaf nodes are stored in memory for fast traversal\n");
    printf("- Leaf nodes are stored on disk to save memory\n");
    printf("- Tree level: %d\n", tree->level);
    printf("- Disk file exists: %s\n", access("/tmp/hybrid_btree_example.dat", F_OK) == 0 ? "Yes" : "No");
    
    // Show tree structure
    printf("\nTree structure:\n");
    bplus_tree_dump(tree);
    
    // Cleanup
    bplus_tree_deinit(tree);
    printf("\n✓ Cleanup completed\n");
    printf("Note: Disk file /tmp/hybrid_btree_example.dat contains the leaf nodes\n");
    
    return 0;
}
