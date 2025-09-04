#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "../lib/bplustree_compressed.h"

int main() {
    printf("Simple Compressed B+Tree Test\n");
    printf("============================\n");
    
    // Initialize compressed B+Tree
    struct bplus_tree_compressed *ct_tree = bplus_tree_compressed_init(16, 32);
    assert(ct_tree != NULL);
    
    printf("Tree initialized successfully\n");
    
    // Test 1: Check if tree is empty initially
    printf("1. Testing empty tree...\n");
    int empty = bplus_tree_compressed_empty(ct_tree);
    printf("   Tree empty: %d (expected: 1)\n", empty);
    
    // Test 2: Insert a key
    printf("2. Testing insert...\n");
    int result = bplus_tree_compressed_put(ct_tree, 1, 100);
    printf("   Insert result: %d (expected: 0)\n", result);
    
    // Test 3: Check if tree is not empty
    printf("3. Testing non-empty tree...\n");
    empty = bplus_tree_compressed_empty(ct_tree);
    printf("   Tree empty: %d (expected: 0)\n", empty);
    
    // Test 4: Get the key
    printf("4. Testing get...\n");
    int value = bplus_tree_compressed_get(ct_tree, 1);
    printf("   Get value: %d (expected: 100)\n", value);
    
    // Test 5: Delete the key
    printf("5. Testing delete...\n");
    result = bplus_tree_compressed_delete(ct_tree, 1);
    printf("   Delete result: %d (expected: 0)\n", result);
    
    // Test 6: Check if tree is empty again
    printf("6. Testing empty tree after delete...\n");
    empty = bplus_tree_compressed_empty(ct_tree);
    printf("   Tree empty: %d (expected: 1)\n", empty);
    
    // Test 7: Try to get deleted key
    printf("7. Testing get after delete...\n");
    value = bplus_tree_compressed_get(ct_tree, 1);
    printf("   Get value: %d (expected: -1)\n", value);
    
    // Test 8: Check tree size
    printf("8. Testing tree size...\n");
    int size = bplus_tree_compressed_size(ct_tree);
    printf("   Tree size: %d (expected: 0)\n", size);
    
    // Test 9: Compression statistics
    printf("9. Testing compression statistics...\n");
    size_t total_size, compressed_size;
    result = bplus_tree_compressed_stats(ct_tree, &total_size, &compressed_size);
    printf("   Stats result: %d\n", result);
    printf("   Total size: %zu bytes\n", total_size);
    printf("   Compressed size: %zu bytes\n", compressed_size);
    
    // Cleanup
    bplus_tree_compressed_deinit(ct_tree);
    printf("Tree deinitialized successfully\n");
    
    printf("\n=== TEST COMPLETED ===\n");
    return 0;
}
