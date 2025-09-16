#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>

#include "../lib/bplustree.h"

#define TEST_KEYS 10
#define TEST_ORDER 4
#define TEST_ENTRIES 64

void test_basic_operations() {
    printf("Testing basic hybrid B+tree operations...\n");
    
    // // Remove test file if it exists
    // unlink("/tmp/test_hybrid_btree.dat");
    
    // // Initialize tree
    // struct bplus_tree *tree = bplus_tree_init(TEST_ORDER, TEST_ENTRIES, "/tmp/test_hybrid_btree.dat");
    
    
    const char *tree_path = "/tmp/test_hybrid_btree.dat";

    // Remove test file if it exists
    unlink(tree_path);

    // Initialize tree
    struct bplus_tree_ssd *tree = bplus_tree_ssd_init(TEST_ORDER, TEST_ENTRIES, tree_path);
    assert(tree != NULL);
    printf("✓ Tree initialization successful\n");
    
    // Test insertions
    printf("Inserting %d key-value pairs...\n", TEST_KEYS);
    for (int i = 0; i < TEST_KEYS; i++) {
        // int result = bplus_tree_put(tree, i, i * 10);
        int result = bplus_tree_ssd_put(tree, i, i * 10);
        if (result != 0) {
            printf("✗ Insert failed for key %d\n", i);
        } else {
            printf("✓ Inserted key %d with value %d\n", i, i * 10);
        }
    }
    
    // Test retrievals
    printf("Testing retrievals...\n");
    for (int i = 0; i < TEST_KEYS; i++) {
        long value = bplus_tree_ssd_get(tree, i);
        if (value == i * 10) {
            printf("✓ Retrieved key %d: expected %d, got %ld\n", i, i * 10, value);
        } else {
            printf("✗ Retrieved key %d: expected %d, got %ld\n", i, i * 10, value);
        }
    }
    
    // Test non-existent key
    // long non_existent = bplus_tree_get(tree, 999);
    long non_existent = bplus_tree_ssd_get(tree, 999);
    if (non_existent == -1) {
        printf("✓ Non-existent key correctly returned -1\n");
    } else {
        printf("✗ Non-existent key returned %ld instead of -1\n", non_existent);
    }
    
    // Dump tree structure
    printf("Tree structure:\n");
    // bplus_tree_dump(tree);
    bplus_tree_ssd_dump(tree);
    
    // Cleanup
    // bplus_tree_deinit(tree);
    // unlink("/tmp/test_hybrid_btree.dat");
    bplus_tree_ssd_deinit(tree);
    unlink(tree_path);
    printf("✓ Cleanup completed\n");
}

void test_disk_operations() {
    printf("\nTesting disk operations directly...\n");
    
    // Test disk manager
    struct disk_manager *dm = disk_manager_init("/tmp/test_leaf.dat");
    if (!dm) {
        printf("✗ Disk manager initialization failed\n");
        return;
    }
    printf("✓ Disk manager initialized\n");
    
    // Create a test leaf
    struct bplus_leaf_disk *leaf = calloc(1, sizeof(*leaf));
    // leaf->type = BPLUS_TREE_LEAF;
    leaf->type = BPLUS_SSD_TREE_LEAF;
    leaf->entries = 3;
    leaf->key[0] = 10; leaf->data[0] = 100;
    leaf->key[1] = 20; leaf->data[1] = 200;
    leaf->key[2] = 30; leaf->data[2] = 300;
    leaf->next_leaf = -1;
    leaf->prev_leaf = -1;
    
    // Write leaf to disk
    off_t offset = disk_write_leaf(dm, leaf);
    if (offset >= 0) {
        printf("✓ Leaf written to disk at offset %ld\n", (long)offset);
    } else {
        printf("✗ Failed to write leaf to disk\n");
        free(leaf);
        return;
    }
    
    // Read leaf back from disk
    struct bplus_leaf_disk *read_leaf = disk_read_leaf(dm, offset);
    if (read_leaf) {
        printf("✓ Leaf read back from disk\n");
        printf("  Entries: %d\n", read_leaf->entries);
        for (int i = 0; i < read_leaf->entries; i++) {
            printf("  Key[%d]=%d, Data[%d]=%ld\n", 
                   i, read_leaf->key[i], i, read_leaf->data[i]);
        }
        
        // Verify data
        int correct = 1;
        if (read_leaf->entries != leaf->entries) correct = 0;
        for (int i = 0; i < leaf->entries && correct; i++) {
            if (read_leaf->key[i] != leaf->key[i] || 
                read_leaf->data[i] != leaf->data[i]) {
                correct = 0;
            }
        }
        
        if (correct) {
            printf("✓ Disk read/write verification successful\n");
        } else {
            printf("✗ Disk read/write verification failed\n");
        }
        
        disk_free_leaf(read_leaf);
    } else {
        printf("✗ Failed to read leaf from disk\n");
    }
    
    free(leaf);
    // Note: disk_manager_deinit would be called if dm was part of a tree
    close(dm->fd);
    free(dm);
    unlink("/tmp/test_leaf.dat");
}

int main() {
    printf("Hybrid B+Tree Test Suite\n");
    printf("========================\n\n");
    
    test_disk_operations();
    test_basic_operations();
    
    printf("\nAll tests completed!\n");
    return 0;
}
