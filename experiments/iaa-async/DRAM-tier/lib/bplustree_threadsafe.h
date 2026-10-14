/*
 * Thread-safe B+Tree wrapper
 */

#ifndef _BPLUS_TREE_THREADSAFE_H
#define _BPLUS_TREE_THREADSAFE_H

#include <pthread.h>
#include "bplustree.h"

/**
 * Thread-safe B+Tree structure
 * Wraps the original bplus_tree with read-write locks
 */
struct bplus_tree_threadsafe {
    struct bplus_tree *tree;           // Original B+Tree
    pthread_rwlock_t rwlock;           // Read-write lock for concurrency control
    int initialized;                   // Initialization flag
};

/**
 * Initialize a thread-safe B+Tree
 * @param order The order of the B+Tree (number of children per node)
 * @param entries The maximum number of entries per leaf node
 * @return Pointer to thread-safe B+Tree, or NULL on failure
 */
struct bplus_tree_threadsafe *bplus_tree_threadsafe_init(int order, int entries);

/**
 * Deinitialize a thread-safe B+Tree
 * @param ts_tree Pointer to thread-safe B+Tree
 */
void bplus_tree_threadsafe_deinit(struct bplus_tree_threadsafe *ts_tree);

/**
 * Thread-safe insert/update operation
 * @param ts_tree Pointer to thread-safe B+Tree
 * @param key The key to insert/update
 * @param data The value to associate with the key
 * @return 0 on success, -1 on failure
 */
int bplus_tree_threadsafe_put(struct bplus_tree_threadsafe *ts_tree, key_t key, int data);

/**
 * Thread-safe get operation
 * @param ts_tree Pointer to thread-safe B+Tree
 * @param key The key to look up
 * @return The value associated with the key, or -1 if not found
 */
int bplus_tree_threadsafe_get(struct bplus_tree_threadsafe *ts_tree, key_t key);

/**
 * Thread-safe delete operation
 * @param ts_tree Pointer to thread-safe B+Tree
 * @param key The key to delete
 * @return 0 on success, -1 on failure
 */
int bplus_tree_threadsafe_delete(struct bplus_tree_threadsafe *ts_tree, key_t key);

/**
 * Thread-safe range scan operation
 * @param ts_tree Pointer to thread-safe B+Tree
 * @param key1 Start of range
 * @param key2 End of range
 * @return A value in the range, or -1 if not found
 */
int bplus_tree_threadsafe_get_range(struct bplus_tree_threadsafe *ts_tree, key_t key1, key_t key2);

/**
 * Thread-safe tree dump (for debugging)
 * @param ts_tree Pointer to thread-safe B+Tree
 */
void bplus_tree_threadsafe_dump(struct bplus_tree_threadsafe *ts_tree);

/**
 * Get the number of entries in the tree (thread-safe)
 * @param ts_tree Pointer to thread-safe B+Tree
 * @return Number of entries
 */
int bplus_tree_threadsafe_size(struct bplus_tree_threadsafe *ts_tree);

/**
 * Check if the tree is empty (thread-safe)
 * @param ts_tree Pointer to thread-safe B+Tree
 * @return 1 if empty, 0 if not empty
 */
int bplus_tree_threadsafe_empty(struct bplus_tree_threadsafe *ts_tree);

#endif /* _BPLUS_TREE_THREADSAFE_H */
