/*
 * Thread-safe B+Tree wrapper implementation
 */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include "bplustree_threadsafe.h"

struct bplus_tree_threadsafe *bplus_tree_threadsafe_init(int order, int entries)
{
    struct bplus_tree_threadsafe *ts_tree = calloc(1, sizeof(*ts_tree));
    if (ts_tree == NULL) {
        return NULL;
    }
    
    // Initialize the underlying B+Tree
    ts_tree->tree = bplus_tree_init(order, entries);
    if (ts_tree->tree == NULL) {
        free(ts_tree);
        return NULL;
    }
    
    // Initialize the read-write lock
    int ret = pthread_rwlock_init(&ts_tree->rwlock, NULL);
    if (ret != 0) {
        bplus_tree_deinit(ts_tree->tree);
        free(ts_tree);
        return NULL;
    }
    
    ts_tree->initialized = 1;
    return ts_tree;
}

void bplus_tree_threadsafe_deinit(struct bplus_tree_threadsafe *ts_tree)
{
    if (ts_tree == NULL) {
        return;
    }
    
    if (ts_tree->initialized) {
        // Acquire write lock to ensure no other threads are accessing
        pthread_rwlock_wrlock(&ts_tree->rwlock);
        
        // Deinitialize the underlying B+Tree
        if (ts_tree->tree != NULL) {
            bplus_tree_deinit(ts_tree->tree);
            ts_tree->tree = NULL;
        }
        
        // Release lock and destroy it
        pthread_rwlock_unlock(&ts_tree->rwlock);
        pthread_rwlock_destroy(&ts_tree->rwlock);
        ts_tree->initialized = 0;
    }
    
    free(ts_tree);
}

int bplus_tree_threadsafe_put(struct bplus_tree_threadsafe *ts_tree, key_t key, int data)
{
    if (ts_tree == NULL || !ts_tree->initialized || ts_tree->tree == NULL) {
        return -1;
    }
    
    // Acquire write lock for insert/update operations
    int ret = pthread_rwlock_wrlock(&ts_tree->rwlock);
    if (ret != 0) {
        return -1;
    }
    
    // Perform the operation
    int result = bplus_tree_put(ts_tree->tree, key, data);
    
    // Release write lock
    pthread_rwlock_unlock(&ts_tree->rwlock);
    
    return result;
}

int bplus_tree_threadsafe_get(struct bplus_tree_threadsafe *ts_tree, key_t key)
{
    if (ts_tree == NULL || !ts_tree->initialized || ts_tree->tree == NULL) {
        return -1;
    }
    
    // Acquire read lock for get operations
    int ret = pthread_rwlock_rdlock(&ts_tree->rwlock);
    if (ret != 0) {
        return -1;
    }
    
    // Perform the operation
    int result = bplus_tree_get(ts_tree->tree, key);
    
    // Release read lock
    pthread_rwlock_unlock(&ts_tree->rwlock);
    
    return result;
}

int bplus_tree_threadsafe_delete(struct bplus_tree_threadsafe *ts_tree, key_t key)
{
    if (ts_tree == NULL || !ts_tree->initialized || ts_tree->tree == NULL) {
        return -1;
    }
    
    // Acquire write lock for delete operations
    int ret = pthread_rwlock_wrlock(&ts_tree->rwlock);
    if (ret != 0) {
        return -1;
    }
    
    // Perform the operation (delete by setting value to 0)
    int result = bplus_tree_put(ts_tree->tree, key, 0);
    
    // Release write lock
    pthread_rwlock_unlock(&ts_tree->rwlock);
    
    return result;
}

int bplus_tree_threadsafe_get_range(struct bplus_tree_threadsafe *ts_tree, key_t key1, key_t key2)
{
    if (ts_tree == NULL || !ts_tree->initialized || ts_tree->tree == NULL) {
        return -1;
    }
    
    // Acquire read lock for range scan operations
    int ret = pthread_rwlock_rdlock(&ts_tree->rwlock);
    if (ret != 0) {
        return -1;
    }
    
    // Perform the operation
    int result = bplus_tree_get_range(ts_tree->tree, key1, key2);
    
    // Release read lock
    pthread_rwlock_unlock(&ts_tree->rwlock);
    
    return result;
}

void bplus_tree_threadsafe_dump(struct bplus_tree_threadsafe *ts_tree)
{
    if (ts_tree == NULL || !ts_tree->initialized || ts_tree->tree == NULL) {
        return;
    }
    
    // Acquire read lock for dump operations
    int ret = pthread_rwlock_rdlock(&ts_tree->rwlock);
    if (ret != 0) {
        return;
    }
    
    // Perform the operation (only if debug is enabled)
#ifdef _BPLUS_TREE_DEBUG
    bplus_tree_dump(ts_tree->tree);
#else
    printf("Tree dump not available (debug mode disabled)\n");
#endif
    
    // Release read lock
    pthread_rwlock_unlock(&ts_tree->rwlock);
}

int bplus_tree_threadsafe_size(struct bplus_tree_threadsafe *ts_tree)
{
    if (ts_tree == NULL || !ts_tree->initialized || ts_tree->tree == NULL) {
        return 0;
    }
    
    // Acquire read lock
    int ret = pthread_rwlock_rdlock(&ts_tree->rwlock);
    if (ret != 0) {
        return 0;
    }
    
    // Get the size (entries count)
    int size = ts_tree->tree->entries;
    
    // Release read lock
    pthread_rwlock_unlock(&ts_tree->rwlock);
    
    return size;
}

int bplus_tree_threadsafe_empty(struct bplus_tree_threadsafe *ts_tree)
{
    if (ts_tree == NULL || !ts_tree->initialized || ts_tree->tree == NULL) {
        return 1;
    }
    
    // Acquire read lock
    int ret = pthread_rwlock_rdlock(&ts_tree->rwlock);
    if (ret != 0) {
        return 1;
    }
    
    // Check if tree is empty
    int empty = (ts_tree->tree->root == NULL);
    
    // Release read lock
    pthread_rwlock_unlock(&ts_tree->rwlock);
    
    return empty;
}
