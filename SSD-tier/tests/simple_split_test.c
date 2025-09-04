#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "../lib/bplustree.h"

void test_simple_split() {
    printf("Testing Simple Super-Leaf Split...\n");
    
    // Remove test file if it exists
    unlink("/mnt/zipcache_test/simple_split_test.dat");
    
    // Create disk manager
    struct disk_manager *dm = disk_manager_init("simple_split_test.dat");
    assert(dm != NULL);
    printf("✓ Disk manager initialized\n");
    
    // Create super-leaf
    struct bplus_super_leaf *super_leaf = super_leaf_create(dm);
    assert(super_leaf != NULL);
    printf("✓ Super-leaf created\n");
    
    // Insert a small number of entries (enough to fill a few sub-pages)
    printf("📝 Inserting 100 entries...\n");
    for (int i = 1; i <= 100; i++) {
        int result = super_leaf_insert_hashed(dm, super_leaf, i, i * 10);
        if (result != 0) {
            printf("❌ Failed to insert key %d (result: %d)\n", i, result);
            break;
        }
        if (i % 20 == 0) {
            printf("  Inserted %d entries, total: %d\n", i, super_leaf->total_entries);
        }
    }
    
    printf("📊 Before split: %d total entries, %d active sub-pages\n", 
           super_leaf->total_entries, super_leaf->active_sub_pages);
    
    // Force a split by calling the function directly
    printf("🔄 Triggering manual split...\n");
    PromotedKey promoted = split_super_leaf(dm, super_leaf);
    
    if (promoted.right_sibling) {
        printf("✅ Split successful! Promoted key: %d\n", promoted.key);
        printf("📊 Left leaf entries: %d\n", super_leaf->total_entries);
        printf("📊 Right leaf entries: %d\n", promoted.right_sibling->total_entries);
        
        // Test that all data is still accessible
        printf("🔍 Verifying data integrity...\n");
        int found_left = 0, found_right = 0, missing = 0;
        
        for (int i = 1; i <= 100; i++) {
            long left_value = super_leaf_search_hashed(dm, super_leaf, i);
            long right_value = super_leaf_search_hashed(dm, promoted.right_sibling, i);
            
            if (left_value == i * 10) {
                found_left++;
            } else if (right_value == i * 10) {
                found_right++;
            } else {
                missing++;
                if (missing <= 5) {  // Show first few missing items
                    printf("❌ Key %d missing (left: %ld, right: %ld)\n", i, left_value, right_value);
                }
            }
        }
        
        printf("✅ Found: %d in left, %d in right, %d missing\n", found_left, found_right, missing);
        printf("📊 Data integrity: %.1f%%\n", (double)(found_left + found_right) / 100 * 100);
        
        super_leaf_free(promoted.right_sibling);
    } else {
        printf("❌ Split failed!\n");
    }
    
    // Cleanup
    super_leaf_free(super_leaf);
    disk_manager_deinit(dm);
    unlink("/mnt/zipcache_test/simple_split_test.dat");
    printf("✓ Simple split test completed\n");
}

int main() {
    printf("Simple Super-Leaf Split Test\n");
    printf("============================\n\n");
    
    // Create test directory if it doesn't exist
    mkdir("/mnt/zipcache_test", 0755);
    
    test_simple_split();
    
    printf("\nTest completed!\n");
    
    return 0;
}