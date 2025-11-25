
#ifndef _BPLUS_TREE_COMPRESSED_H
#define _BPLUS_TREE_COMPRESSED_H

#define _GNU_SOURCE
#include <pthread.h>
#include <lz4.h>
#include <lz4hc.h>
#include <qpl/qpl.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include "bplustree.h"

// 4KB leaf node size for optimal compression
#define COMPRESSED_LEAF_SIZE 4096
#define MAX_COMPRESSED_SIZE 8192  // Fixed size for LZ4 compression buffer

// Legacy constants from btree.h
#define KEY_SIZE 8
#define COMPRESSED_VALUE_BYTES 64
#define LANDING_BUFFER_BYTES 1024
#define TOTAL_SUBPAGES_BYTES 4096



// Leaf layout personality selection (unified hashed layout focus)
typedef enum {
    LEAF_TYPE_LZ4_HASHED = 0,
    LEAF_TYPE_QPL_APPEND = 1
} leaf_layout_t;

// Global compression algorithm selection (applies to unified hashed layout)
typedef enum {
    COMPRESS_LZ4 = 0,
    COMPRESS_QPL = 1
} compression_algo_t;

// Original compression configuration (kept for backward compatibility)
struct compression_config {
    leaf_layout_t default_layout;
    compression_algo_t algo;
    int default_sub_pages;
    int compression_level;
    int buffer_size;
    int flush_threshold;
    int enable_lazy_compression;
};

// Simplified compression configuration for dual algorithm support
struct simple_compression_config {
    compression_algo_t default_algo;           // COMPRESS_LZ4 or COMPRESS_QPL
    int num_subpages;                         // Number of hash buckets (legacy style)
    int buffer_size;                          // Landing buffer size (default 512)
    int lz4_partial_decompression;            // Enable partial decompression for LZ4
    int qpl_compression_level;                // QPL compression level
    int enable_background_compression;        // Background thread support
};

// Per–sub-page compressed block index entry for unified hashed layout
struct subpage_index_entry {
    uint32_t offset;               // Offset into compressed_data
    uint32_t length;               // Compressed length of this sub-page block
    uint32_t uncompressed_bytes;   // Uncompressed bytes for this sub-page
};

// Simplified leaf node structure with legacy 1D layout + dual compression
struct simple_leaf_node {
    char landing_buffer[LANDING_BUFFER_BYTES];  // 512 bytes (legacy style)
    char *compressed_data;                      // Single compressed block
    int compressed_size;                        // Size of compressed data
    int num_subpages;                          // Number of hash buckets (legacy)
    bool is_compressed;                        // Compression state
    compression_algo_t compression_algo;       // LZ4 or QPL (stored per leaf)

    // LZ4-specific fields (only used when algo == COMPRESS_LZ4)
    struct subpage_index_entry *subpage_index; // For partial decompression
    int num_subpage_entries;                   // Number of indexed subpages

    // Per-leaf concurrency control
    pthread_rwlock_t rwlock;                   // Read-write lock for this leaf

    // Statistics
    size_t uncompressed_bytes;
    size_t compressed_bytes;
};


// Thread-safe compressed B+Tree structure
struct bplus_tree_compressed {
    struct bplus_tree *tree;                // Original B+Tree
    pthread_rwlock_t rwlock;                // Read-write lock for concurrency control
    int initialized;                        // Initialization flag
    int compression_enabled;                // Whether compression is enabled
    int debug_mode;                         // Enable debug output (0=off, 1=on)

    // Compression configuration
    struct compression_config config;       // User-defined compression settings
    struct simple_compression_config simple_config;  // Simplified dual-algorithm configuration
    int is_simple_mode;                     // Flag: 1 for simple API, 0 for legacy API

    // QPL support
    qpl_job *qpl_job_ptr;                  // QPL job for compression operations
    uint8_t *qpl_job_buffer;               // QPL job buffer
    pthread_mutex_t qpl_lock;              // Mutex for QPL operations

    // Global compression statistics
    size_t total_uncompressed_size;         // Total uncompressed size
    size_t total_compressed_size;           // Total compressed size
    int compression_operations;             // Number of compression operations
    int decompression_operations;           // Number of decompression operations
};

/**
 * Initialize a compressed thread-safe B+Tree with default LZ4 compression
 * @param order The order of the B+Tree (for non-leaf nodes)
 * @param entries The maximum number of entries per leaf node
 * @return Pointer to compressed B+Tree, or NULL on failure
 */
struct bplus_tree_compressed *bplus_tree_compressed_init(int order, int entries);

/**
 * Initialize a compressed thread-safe B+Tree with user-defined compression
 * @param order The order of the B+Tree (for non-leaf nodes)
 * @param entries The maximum number of entries per leaf node
 * @param config Compression configuration (algorithm, parameters, etc.)
 * @return Pointer to compressed B+Tree, or NULL on failure
 */
struct bplus_tree_compressed *bplus_tree_compressed_init_with_config(int order, int entries, 
                                                                   struct compression_config *config);

/**
 * Deinitialize a compressed B+Tree
 * @param ct_tree Pointer to compressed B+Tree
 */
void bplus_tree_compressed_deinit(struct bplus_tree_compressed *ct_tree);

/**
 * Set debug mode for the compressed B+Tree
 * @param ct_tree Pointer to compressed B+Tree
 * @param enable 1 to enable debug output, 0 to disable
 */
void bplus_tree_compressed_set_debug(struct bplus_tree_compressed *ct_tree, int enable);


/**
 * Thread-safe insert/update operation with compression
 * @param ct_tree Pointer to compressed B+Tree
 * @param key The key to insert/update
 * @param data The value to associate with the key
 * @return 0 on success, -1 on failure
 */
int bplus_tree_compressed_put(struct bplus_tree_compressed *ct_tree, key_t key, int data);

/**
 * Insert/update with an explicit payload stored inline in the leaf (up to COMPRESSED_VALUE_BYTES).
 * stored_value is returned by bplus_tree_compressed_get for compatibility.
 */
int bplus_tree_compressed_put_with_payload(struct bplus_tree_compressed *ct_tree,
                                           key_t key,
                                           const uint8_t *payload,
                                           size_t payload_len,
                                           int stored_value);

/**
 * Thread-safe delete operation with compression
 * @param ct_tree Pointer to compressed B+Tree
 * @param key The key to remove
 * @return 0 on success, -1 if the key is not present or on failure
 */
int bplus_tree_compressed_delete(struct bplus_tree_compressed *ct_tree, key_t key);

/**
 * Thread-safe get operation with decompression
 * @param ct_tree Pointer to compressed B+Tree
 * @param key The key to look up
 * @return The value associated with the key, or -1 if not found
 */
int bplus_tree_compressed_get(struct bplus_tree_compressed *ct_tree, key_t key);

/**
 * Thread-safe range scan operation with decompression
 * @param ct_tree Pointer to compressed B+Tree
 * @param key1 Start of range
 * @param key2 End of range
 * @return A value in the range, or -1 if not found
 */
int bplus_tree_compressed_get_range(struct bplus_tree_compressed *ct_tree, key_t key1, key_t key2);

/**
 * Get compression statistics (incremental counters)
 * @param ct_tree Pointer to compressed B+Tree
 * @param total_size Pointer to store total uncompressed size
 * @param compressed_size Pointer to store total compressed size
 * @return 0 on success, -1 on failure
 */
int bplus_tree_compressed_stats(struct bplus_tree_compressed *ct_tree,
                                size_t *total_size, size_t *compressed_size);

/**
 * Calculate actual compression statistics by walking all leaves
 * @param ct_tree Pointer to compressed B+Tree
 * @param total_size Pointer to store total uncompressed size
 * @param compressed_size Pointer to store total compressed size
 * @return 0 on success, -1 on failure
 */
int bplus_tree_compressed_calculate_stats(struct bplus_tree_compressed *ct_tree,
                                          size_t *total_size, size_t *compressed_size);

/**
 * Initialize Intel QPL for the tree (internal function)
 * @param ct_tree Pointer to compressed B+Tree
 * @return 0 on success, -1 on failure
 */
int init_qpl(struct bplus_tree_compressed *ct_tree);

/**
 * Cleanup Intel QPL resources (internal function)
 * @param ct_tree Pointer to compressed B+Tree
 */
void cleanup_qpl(struct bplus_tree_compressed *ct_tree);

/**
 * Create default leaf configuration
 * @param default_layout Default leaf layout to use
 * @return Default compression configuration
 */
struct compression_config bplus_tree_create_default_leaf_config(leaf_layout_t default_layout);

/**
 * Internal helper function for inserting into leaf
 * @param ct_tree Pointer to compressed B+Tree
 * @param leaf Pointer to simple leaf node
 * @param key The key to insert
 * @param value The value to insert
 * @return 0 on success, -1 if leaf needs splitting
 */
int insert_into_leaf(struct bplus_tree_compressed *ct_tree,
                     struct simple_leaf_node *leaf,
                     key_t key,
                     int stored_value,
                     const uint8_t *payload,
                     size_t payload_len);

#endif /* _BPLUS_TREE_COMPRESSED_H */
