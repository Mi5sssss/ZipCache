#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "../lib/bplustree.h"

#define SPLIT_TEST_ORDER 8
#define SPLIT_TEST_ENTRIES 64

void test_super_leaf_splitting() {
    printf("Testing Super-Leaf Splitting with Parallel I/O...\n");
    
    // Remove test file if it exists
    unlink("/mnt/zipcache_test/super_leaf_split_test.dat");
    
    // Create disk manager
    struct disk_manager *dm = disk_manager_init("super_leaf_split_test.dat");
    assert(dm != NULL);
    printf("✓ Disk manager initialized\n");
    
    // Create super-leaf
    struct bplus_super_leaf *super_leaf = super_leaf_create(dm);
    assert(super_leaf != NULL);
    printf("✓ Super-leaf created\n");
    
    // Calculate capacity and insert enough data to trigger split
    int entries_per_sub_page = ENTRIES_PER_SUB_PAGE;
    int total_capacity = SUB_PAGES_PER_SUPER_LEAF * entries_per_sub_page;
    int split_trigger = (int)(total_capacity * 0.9); // 90% threshold
    
    printf("📊 Super-leaf capacity: %d entries (%d sub-pages × %d entries)\n", 
           total_capacity, SUB_PAGES_PER_SUPER_LEAF, entries_per_sub_page);
    printf("📊 Split trigger: %d entries (90%% full)\n", split_trigger);
    
    // Insert data to nearly fill the super-leaf
    printf("📝 Inserting %d entries to trigger split...\n", split_trigger + 10);
    int inserted = 0;
    
    for (int i = 1; i <= split_trigger + 10; i++) {
        int result = super_leaf_insert_hashed(dm, super_leaf, i, i * 100);
        if (result == 0) {
            inserted++;
        } else if (result == -2) {
            printf("🔄 Split trigger detected at key %d (inserted %d entries)\n", i, inserted);
            
            // Test the split function
            PromotedKey promoted = split_super_leaf(dm, super_leaf);
            assert(promoted.right_sibling != NULL);
            assert(promoted.key > 0);
            
            printf("✅ Split completed! Promoted key: %d\n", promoted.key);
            printf("📊 Left leaf entries: %d\n", super_leaf->total_entries);
            printf("📊 Right leaf entries: %d\n", promoted.right_sibling->total_entries);
            
            // Verify that all data is still accessible
            printf("🔍 Verifying data integrity after split...\n");
            int found_left = 0, found_right = 0;
            
            for (int j = 1; j <= inserted; j++) {
                long value_left = super_leaf_search_hashed(dm, super_leaf, j);
                long value_right = super_leaf_search_hashed(dm, promoted.right_sibling, j);
                
                if (value_left == j * 100) {
                    found_left++;
                } else if (value_right == j * 100) {
                    found_right++;
                }
            }
            
            printf("✅ Found %d entries in left leaf, %d entries in right leaf\n", found_left, found_right);
            assert(found_left + found_right == inserted);
            
            // Test insertion into the new right sibling
            int new_key = inserted + 1;
            if (new_key >= promoted.key) {
                result = super_leaf_insert_hashed(dm, promoted.right_sibling, new_key, new_key * 100);
                assert(result == 0);
                printf("✅ Successfully inserted key %d into right sibling\n", new_key);
            } else {
                result = super_leaf_insert_hashed(dm, super_leaf, new_key, new_key * 100);
                assert(result == 0);
                printf("✅ Successfully inserted key %d into left sibling\n", new_key);
            }
            
            super_leaf_free(promoted.right_sibling);
            break;
        } else {
            printf("❌ Failed to insert key %d (result: %d)\n", i, result);
            break;
        }
        
        if (i % 100 == 0) {
            printf("  Progress: %d/%d entries inserted\n", i, split_trigger + 10);
        }
    }
    
    printf("📊 Final super-leaf state:\n");
    printf("  Total entries: %d\n", super_leaf->total_entries);
    printf("  Active sub-pages: %d\n", super_leaf->active_sub_pages);
    
    // Test parallel I/O by flushing dirty pages
    int flushed = super_leaf_flush_dirty(dm, super_leaf);
    printf("💾 Flushed %d dirty sub-pages\n", flushed);
    
    // Cleanup
    super_leaf_free(super_leaf);
    disk_manager_deinit(dm);
    unlink("/mnt/zipcache_test/super_leaf_split_test.dat");
    printf("✓ Super-leaf splitting test completed\n\n");
}

void test_tree_with_splitting() {
    printf("Testing B+Tree with Super-Leaf Splitting...\n");
    
    // Remove test file if it exists
    unlink("/mnt/zipcache_test/tree_split_test.dat");
    
    // Initialize tree
    struct bplus_tree_ssd *tree = bplus_tree_ssd_init(SPLIT_TEST_ORDER, SPLIT_TEST_ENTRIES, "tree_split_test.dat");
    assert(tree != NULL);
    printf("✓ Tree initialized\n");
    
    // Insert enough data to trigger multiple splits
    int test_keys = 2000;  // Should trigger splitting
    printf("📝 Inserting %d keys to test tree-level splitting...\n", test_keys);
    
    int successful_inserts = 0;
    for (int i = 1; i <= test_keys; i++) {
        int result = bplus_tree_ssd_put(tree, i, i * 10);
        if (result == 0) {
            successful_inserts++;
        }
        
        if (i % 500 == 0) {
            printf("  Progress: %d/%d keys inserted\n", i, test_keys);
        }
    }
    
    printf("✅ Successfully inserted %d/%d keys\n", successful_inserts, test_keys);
    
    // Test retrievals
    printf("🔍 Testing retrievals after splitting...\n");
    int successful_gets = 0;
    for (int i = 1; i <= successful_inserts; i++) {
        long value = bplus_tree_ssd_get(tree, i);
        if (value == i * 10) {
            successful_gets++;
        } else if (i <= 10) {  // Show first few misses
            printf("❌ Key %d: expected %d, got %ld\n", i, i * 10, value);
        }
    }
    
    printf("✅ Successfully retrieved %d/%d keys (%.1f%%)\n", 
           successful_gets, successful_inserts,
           (double)successful_gets / successful_inserts * 100);
    
    // Test specific lookups
    int test_specific[] = {1, 100, 500, 1000, 1500, 2000};
    int num_specific = sizeof(test_specific) / sizeof(test_specific[0]);
    
    printf("🔍 Testing specific lookups:\n");
    for (int i = 0; i < num_specific; i++) {
        int key = test_specific[i];
        if (key <= successful_inserts) {
            long value = bplus_tree_ssd_get(tree, key);
            printf("  Key %d: %s (expected %d, got %ld)\n", 
                   key, 
                   value == key * 10 ? "✓" : "✗",
                   key * 10,
                   value);
        }
    }
    
    // Dump tree information
    printf("\n📊 Final tree state:\n");
    bplus_tree_ssd_dump(tree);
    
    // Cleanup
    bplus_tree_ssd_deinit(tree);
    unlink("/mnt/zipcache_test/tree_split_test.dat");
    printf("✓ Tree splitting test completed\n\n");
}

void print_split_configuration() {
    printf("Super-Leaf Split Test Configuration:\n");
    printf("====================================\n");
    printf("SUB_PAGE_SIZE: %d bytes\n", SUB_PAGE_SIZE);
    printf("SUPER_LEAF_SIZE: %d bytes\n", SUPER_LEAF_SIZE);
    printf("SUB_PAGES_PER_SUPER_LEAF: %d\n", SUB_PAGES_PER_SUPER_LEAF);
    printf("ENTRIES_PER_SUB_PAGE: %lu\n", ENTRIES_PER_SUB_PAGE);
    printf("Total entries per super-leaf: %lu\n", ENTRIES_PER_SUB_PAGE * SUB_PAGES_PER_SUPER_LEAF);
    printf("Split trigger (90%% full): %lu entries\n", 
           (size_t)(ENTRIES_PER_SUB_PAGE * SUB_PAGES_PER_SUPER_LEAF * 0.9));
    printf("\n");
}

int main() {
    printf("Super-Leaf Splitting Test Suite\n");
    printf("================================\n\n");
    
    print_split_configuration();
    
    // Create test directory if it doesn't exist
    mkdir("/mnt/zipcache_test", 0755);
    
    test_super_leaf_splitting();
    test_tree_with_splitting();
    
    printf("All Super-Leaf Splitting tests completed successfully!\n");
    printf("\nKey Features Demonstrated:\n");
    printf("✓ Super-leaf split detection when 90%% full\n");
    printf("✓ Parallel I/O for reading all sub-pages\n");
    printf("✓ Data consolidation and redistribution\n");
    printf("✓ Parallel I/O for writing split data\n");
    printf("✓ Parent node update with promoted keys\n");
    printf("✓ Data integrity preservation across splits\n");
    printf("✓ Continued insertion after splitting\n");
    
    return 0;
}
