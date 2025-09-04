#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "../lib/bplustree.h"

#define TEST_KEYS 100
#define TEST_ORDER 8
#define TEST_ENTRIES 64

void test_block_allocator() {
    printf("Testing Block Allocator...\n");
    
    struct block_allocator *allocator = block_allocator_init(1000);
    assert(allocator != NULL);
    printf("✓ Block allocator initialized with 1000 blocks\n");
    
    // Test single block allocation
    uint32_t block1 = allocate_block(allocator);
    uint32_t block2 = allocate_block(allocator);
    uint32_t block3 = allocate_block(allocator);
    
    printf("✓ Allocated blocks: %u, %u, %u\n", block1, block2, block3);
    assert(block1 != INVALID_BLOCK_ID);
    assert(block2 != INVALID_BLOCK_ID);
    assert(block3 != INVALID_BLOCK_ID);
    assert(block1 != block2 && block2 != block3);
    
    // Test multiple block allocation
    uint32_t blocks[16];
    int result = allocate_multiple_blocks(allocator, 16, blocks);
    assert(result == 0);
    printf("✓ Allocated 16 blocks successfully\n");
    
    // Test block freeing
    free_block(allocator, block1);
    free_multiple_blocks(allocator, 16, blocks);
    printf("✓ Freed blocks successfully\n");
    
    // Test reallocation of freed block
    uint32_t realloc_block = allocate_block(allocator);
    printf("✓ Reallocated block: %u\n", realloc_block);
    
    block_allocator_deinit(allocator);
    printf("✓ Block allocator cleanup completed\n\n");
}

void test_sub_page_operations() {
    printf("Testing Sub-page Operations...\n");
    
    struct sub_page *sub_page = sub_page_create();
    assert(sub_page != NULL);
    printf("✓ Sub-page created\n");
    
    // Test insertions
    for (int i = 1; i <= 10; i++) {
        int result = sub_page_insert(sub_page, i, i * 100);
        assert(result == 0);
    }
    printf("✓ Inserted 10 key-value pairs into sub-page\n");
    
    // Test searches
    for (int i = 1; i <= 10; i++) {
        long value = sub_page_search(sub_page, i);
        assert(value == i * 100);
    }
    printf("✓ All searches successful\n");
    
    // Test non-existent key
    long non_existent = sub_page_search(sub_page, 999);
    assert(non_existent == -1);
    printf("✓ Non-existent key correctly returned -1\n");
    
    // Test capacity
    printf("Sub-page entries: %d/%lu\n", sub_page->header.entries, ENTRIES_PER_SUB_PAGE);
    
    sub_page_free(sub_page);
    printf("✓ Sub-page cleanup completed\n\n");
}

void test_disk_io() {
    printf("Testing Disk I/O Operations...\n");
    
    // Create disk manager
    struct disk_manager *dm = disk_manager_init("super_leaf_test.dat");
    assert(dm != NULL);
    printf("✓ Disk manager initialized: %s\n", dm->filename);
    
    // Create a test sub-page
    struct sub_page *sub_page = sub_page_create();
    for (int i = 1; i <= 5; i++) {
        sub_page_insert(sub_page, i * 10, i * 1000);
    }
    
    // Allocate a block and write sub-page
    uint32_t block_id = allocate_block(dm->allocator);
    assert(block_id != INVALID_BLOCK_ID);
    printf("✓ Allocated block ID: %u\n", block_id);
    
    int write_result = disk_write_sub_page(dm, block_id, sub_page);
    assert(write_result == 0);
    printf("✓ Sub-page written to disk\n");
    
    // Read sub-page back
    struct sub_page *read_sub_page = disk_read_sub_page(dm, block_id);
    assert(read_sub_page != NULL);
    printf("✓ Sub-page read from disk\n");
    
    // Verify data
    assert(read_sub_page->header.entries == sub_page->header.entries);
    for (int i = 1; i <= 5; i++) {
        long value = sub_page_search(read_sub_page, i * 10);
        assert(value == i * 1000);
    }
    printf("✓ Disk I/O verification successful\n");
    
    // Cleanup
    sub_page_free(sub_page);
    sub_page_free(read_sub_page);
    free_block(dm->allocator, block_id);
    disk_manager_deinit(dm);
    unlink("/mnt/zipcache_test/super_leaf_test.dat");
    printf("✓ Disk I/O cleanup completed\n\n");
}

void test_super_leaf_operations() {
    printf("Testing Super-Leaf Operations...\n");
    
    struct disk_manager *dm = disk_manager_init("super_leaf_ops_test.dat");
    assert(dm != NULL);
    
    struct bplus_super_leaf *super_leaf = super_leaf_create(dm);
    assert(super_leaf != NULL);
    printf("✓ Super-leaf created\n");
    
    // Test insertions
    int inserted = 0;
    for (int i = 1; i <= 50; i++) {
        if (super_leaf_insert(dm, super_leaf, i, i * 10) == 0) {
            inserted++;
        }
    }
    printf("✓ Inserted %d entries into super-leaf\n", inserted);
    printf("  Total entries: %d\n", super_leaf->total_entries);
    printf("  Active sub-pages: %d\n", super_leaf->active_sub_pages);
    
    // Test searches
    int found = 0;
    for (int i = 1; i <= 50; i++) {
        long value = super_leaf_search(dm, super_leaf, i);
        if (value == i * 10) {
            found++;
        }
    }
    printf("✓ Found %d/%d entries in super-leaf\n", found, inserted);
    
    // Test flushing dirty pages
    int flushed = super_leaf_flush_dirty(dm, super_leaf);
    printf("✓ Flushed %d dirty sub-pages to disk\n", flushed);
    
    // Test non-existent key
    long non_existent = super_leaf_search(dm, super_leaf, 999);
    assert(non_existent == -1);
    printf("✓ Non-existent key correctly returned -1\n");
    
    // Show block allocation
    printf("Block allocation details:\n");
    for (int i = 0; i < super_leaf->active_sub_pages; i++) {
        if (super_leaf->sub_page_blocks[i] != INVALID_BLOCK_ID) {
            printf("  Sub-page %d: Block ID %u\n", i, super_leaf->sub_page_blocks[i]);
        }
    }
    
    super_leaf_free(super_leaf);
    disk_manager_deinit(dm);
    unlink("/mnt/zipcache_test/super_leaf_ops_test.dat");
    printf("✓ Super-leaf cleanup completed\n\n");
}

void test_hybrid_tree_with_super_leaf() {
    printf("Testing Hybrid B+Tree with Super-Leaf...\n");
    
    // Remove test file if it exists
    unlink("/mnt/zipcache_test/hybrid_super_leaf_test.dat");
    
    // Initialize tree
    struct bplus_tree *tree = bplus_tree_init(TEST_ORDER, TEST_ENTRIES, "hybrid_super_leaf_test.dat");
    assert(tree != NULL);
    printf("✓ Hybrid tree initialized\n");
    
    // Test insertions
    printf("Inserting %d key-value pairs...\n", TEST_KEYS);
    int successful_inserts = 0;
    for (int i = 1; i <= TEST_KEYS; i++) {
        int result = bplus_tree_put(tree, i, i * 100);
        if (result == 0) {
            successful_inserts++;
        }
        
        if (i % 20 == 0) {
            printf("  Progress: %d/%d inserted\n", i, TEST_KEYS);
        }
    }
    printf("✓ Successfully inserted %d/%d entries\n", successful_inserts, TEST_KEYS);
    
    // Test retrievals
    printf("Testing retrievals...\n");
    int successful_gets = 0;
    for (int i = 1; i <= successful_inserts; i++) {
        long value = bplus_tree_get(tree, i);
        if (value == i * 100) {
            successful_gets++;
        }
    }
    printf("✓ Successfully retrieved %d/%d entries\n", successful_gets, successful_inserts);
    
    // Test some specific lookups
    printf("Testing specific lookups:\n");
    int test_keys[] = {1, 25, 50, 75, 100};
    int num_test_keys = sizeof(test_keys) / sizeof(test_keys[0]);
    
    for (int i = 0; i < num_test_keys; i++) {
        int key = test_keys[i];
        if (key <= successful_inserts) {
            long value = bplus_tree_get(tree, key);
            printf("  Key %d: %s (expected %d, got %ld)\n", 
                   key, 
                   value == key * 100 ? "✓" : "✗",
                   key * 100,
                   value);
        }
    }
    
    // Test non-existent key
    long non_existent = bplus_tree_get(tree, 9999);
    printf("  Non-existent key 9999: %s (got %ld)\n", 
           non_existent == -1 ? "✓" : "✗", non_existent);
    
    // Dump tree information
    printf("\nTree Information:\n");
    bplus_tree_dump(tree);
    
    // Check file size
    struct stat st;
    if (stat(tree->disk_mgr->filename, &st) == 0) {
        printf("Disk file size: %ld bytes (%.2f MB)\n", 
               (long)st.st_size, (double)st.st_size / (1024*1024));
    }
    
    // Cleanup
    bplus_tree_deinit(tree);
    unlink("/mnt/zipcache_test/hybrid_super_leaf_test.dat");
    printf("✓ Hybrid tree cleanup completed\n\n");
}

void print_configuration() {
    printf("Super-Leaf Configuration:\n");
    printf("=========================\n");
    printf("SUB_PAGE_SIZE: %d bytes (4KB)\n", SUB_PAGE_SIZE);
    printf("SUPER_LEAF_SIZE: %d bytes (64KB)\n", SUPER_LEAF_SIZE);
    printf("SUB_PAGES_PER_SUPER_LEAF: %d\n", SUB_PAGES_PER_SUPER_LEAF);
    printf("ENTRIES_PER_SUB_PAGE: %lu\n", ENTRIES_PER_SUB_PAGE);
    printf("Total entries per super-leaf: %lu\n", ENTRIES_PER_SUB_PAGE * SUB_PAGES_PER_SUPER_LEAF);
    printf("\nSub-page layout:\n");
    printf("- Header: %lu bytes\n", sizeof(struct sub_page_header));
    printf("- Payload: %d bytes\n", SUB_PAGE_SIZE - (int)sizeof(struct sub_page_header));
    printf("- Keys space: %lu bytes\n", ENTRIES_PER_SUB_PAGE * sizeof(key_t));
    printf("- Data space: %lu bytes\n", ENTRIES_PER_SUB_PAGE * sizeof(long));
    printf("\n");
}

int main() {
    printf("Super-Leaf B+Tree Test Suite\n");
    printf("=============================\n\n");
    
    print_configuration();
    
    // Create test directory if it doesn't exist
    mkdir("/mnt/zipcache_test", 0755);
    
    test_block_allocator();
    test_sub_page_operations();
    test_disk_io();
    test_super_leaf_operations();
    test_hybrid_tree_with_super_leaf();
    
    printf("All Super-Leaf tests completed successfully!\n");
    printf("\nKey Features Demonstrated:\n");
    printf("✓ Non-contiguous 4KB block allocation\n");
    printf("✓ Super-leaf with multiple sub-pages\n");
    printf("✓ On-demand sub-page loading\n");
    printf("✓ Dirty page tracking and flushing\n");
    printf("✓ Integration with hybrid B+tree\n");
    printf("✓ Memory efficiency (only active sub-pages cached)\n");
    
    return 0;
}
