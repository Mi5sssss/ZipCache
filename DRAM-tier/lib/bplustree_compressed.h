
#ifndef _BPLUS_TREE_COMPRESSED_H
#define _BPLUS_TREE_COMPRESSED_H

#define _GNU_SOURCE
#include <pthread.h>
#include <lz4.h>
#include <qpl/qpl.h>
#include "bplustree.h"

// 4KB leaf node size for optimal compression
#define COMPRESSED_LEAF_SIZE 4096
#define MAX_COMPRESSED_SIZE 8192  // Fixed size for LZ4 compression buffer



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

// Compression configuration
struct compression_config {
    // Layout is unified (hashed) for both algorithms; keep field for backward compat
    leaf_layout_t default_layout;
    compression_algo_t algo;        // Global algorithm: LZ4 (early term) or QPL (full leaf)
    int default_sub_pages;          // n: number of hashed sub-pages per leaf (e.g., 16)
    int compression_level;          // For QPL (qpl_default_level, qpl_high_level)
    int buffer_size;                // Configurable buffer size (default 512)
    int flush_threshold;            // When to trigger background flush
    int enable_lazy_compression;    // Enable/disable lazy compression
};

// Per–sub-page compressed block index entry for unified hashed layout
struct subpage_index_entry {
    uint32_t offset;               // Offset into compressed_data
    uint32_t length;               // Compressed length of this sub-page block
    uint32_t uncompressed_bytes;   // Uncompressed bytes for this sub-page
};

// Lazy compression buffer settings
#define WRITING_BUFFER_SIZE 512      // 512 bytes buffer per leaf (< 4KB)
#define MAX_BUFFER_ENTRIES 32        // Max KV pairs in buffer (16 bytes each)
#define QPL_COMPRESSION_BUFFER_SIZE 16384  // 16KB buffer for QPL operations

// Writing buffer entry for lazy compression
struct buffer_entry {
    key_t key;
    value_t value;
    char operation;  // 'I' for insert, 'D' for delete, 'U' for update
};

// Writing buffer for each leaf node
struct writing_buffer {
    struct buffer_entry entries[MAX_BUFFER_ENTRIES];
    int count;                       // Number of entries in buffer
    int dirty;                       // Flag indicating buffer has changes
    pthread_mutex_t buffer_lock;     // Lock for this buffer
};

/* Deprecated: in-tree extended leaf struct not used with external metadata map */
#if 0
struct bplus_leaf_compressed {
    struct bplus_leaf base;
    int is_compressed;
    int original_entries;
    int compressed_size;
    char compressed_data[MAX_COMPRESSED_SIZE];
    struct writing_buffer *buffer;
    size_t uncompressed_bytes;
    size_t compressed_bytes;
};
#endif

// Background thread work queue item
struct flush_work_item {
    struct bplus_leaf *leaf;
    struct flush_work_item *next;
};

// Work queue for background thread
struct work_queue {
    struct flush_work_item *head;
    struct flush_work_item *tail;
    int count;
    pthread_mutex_t queue_lock;
    pthread_cond_t queue_cond;
};

// Thread-safe compressed B+Tree structure
struct bplus_tree_compressed {
    struct bplus_tree *tree;                // Original B+Tree
    pthread_rwlock_t rwlock;                // Read-write lock for concurrency control
    int initialized;                        // Initialization flag
    int compression_enabled;                // Whether compression is enabled
    
    // Compression configuration
    struct compression_config config;       // User-defined compression settings
    
    // QPL support
    qpl_job *qpl_job_ptr;                  // QPL job for compression operations
    uint8_t *qpl_job_buffer;               // QPL job buffer
    pthread_mutex_t qpl_lock;              // Mutex for QPL operations
    
    // Lazy compression support
    pthread_t background_thread;            // Background compression thread
    struct work_queue *work_queue;          // Work queue for background thread
    int shutdown_flag;                      // Flag to shutdown background thread
    int buffer_flush_threshold;             // Entries threshold to trigger flush
    
    // Global compression statistics
    size_t total_uncompressed_size;         // Total uncompressed size
    size_t total_compressed_size;           // Total compressed size
    int compression_operations;             // Number of compression operations
    int decompression_operations;           // Number of decompression operations
    int lz4_operations;                     // LZ4-specific operations
    int qpl_operations;                     // QPL-specific operations
    int buffer_hits;                        // Buffer hits for reads
    int buffer_misses;                      // Buffer misses for reads
    int background_flushes;                 // Background flush operations
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
 * Compress a leaf node using the configured algorithm
 * @param ct_tree Pointer to compressed B+Tree (for algorithm config)
 * @param leaf Pointer to compressed leaf node
 * @return 0 on success, -1 on failure
 */
/* Deprecated prototype (using external metadata instead) */
#if 0
int compress_leaf_node(struct bplus_tree_compressed *ct_tree, struct bplus_leaf_compressed *leaf);
#endif

/**
 * Decompress a leaf node using the stored algorithm
 * @param ct_tree Pointer to compressed B+Tree (for QPL job if needed)
 * @param leaf Pointer to compressed leaf node
 * @return 0 on success, -1 on failure
 */
/* Deprecated prototype (using external metadata instead) */
#if 0
int decompress_leaf_node(struct bplus_tree_compressed *ct_tree, struct bplus_leaf_compressed *leaf);
#endif

/**
 * Compress a leaf node using LZ4 (internal function)
 * @param leaf Pointer to compressed leaf node
 * @return 0 on success, -1 on failure
 */
/* Deprecated prototype (using external metadata instead) */
#if 0
int compress_leaf_node_lz4(struct bplus_leaf_compressed *leaf);
#endif

/**
 * Decompress a leaf node using LZ4 (internal function)
 * @param leaf Pointer to compressed leaf node
 * @return 0 on success, -1 on failure
 */
/* Deprecated prototype (using external metadata instead) */
#if 0
int decompress_leaf_node_lz4(struct bplus_leaf_compressed *leaf);
#endif

/**
 * Compress a leaf node using Intel QPL (internal function)
 * @param ct_tree Pointer to compressed B+Tree (for QPL job)
 * @param leaf Pointer to compressed leaf node
 * @return 0 on success, -1 on failure
 */
/* Deprecated prototype (using external metadata instead) */
#if 0
int compress_leaf_node_qpl(struct bplus_tree_compressed *ct_tree, struct bplus_leaf_compressed *leaf);
#endif

/**
 * Decompress a leaf node using Intel QPL (internal function)
 * @param ct_tree Pointer to compressed B+Tree (for QPL job)
 * @param leaf Pointer to compressed leaf node
 * @return 0 on success, -1 on failure
 */
/* Deprecated prototype (using external metadata instead) */
#if 0
int decompress_leaf_node_qpl(struct bplus_tree_compressed *ct_tree, struct bplus_leaf_compressed *leaf);
#endif

/**
 * Thread-safe insert/update operation with compression
 * @param ct_tree Pointer to compressed B+Tree
 * @param key The key to insert/update
 * @param data The value to associate with the key
 * @return 0 on success, -1 on failure
 */
int bplus_tree_compressed_put(struct bplus_tree_compressed *ct_tree, key_t key, int data);

/**
 * Thread-safe get operation with decompression
 * @param ct_tree Pointer to compressed B+Tree
 * @param key The key to look up
 * @return The value associated with the key, or -1 if not found
 */
int bplus_tree_compressed_get(struct bplus_tree_compressed *ct_tree, key_t key);

/**
 * Thread-safe delete operation with compression
 * @param ct_tree Pointer to compressed B+Tree
 * @param key The key to delete
 * @return 0 on success, -1 on failure
 */
int bplus_tree_compressed_delete(struct bplus_tree_compressed *ct_tree, key_t key);

/**
 * Thread-safe range scan operation with decompression
 * @param ct_tree Pointer to compressed B+Tree
 * @param key1 Start of range
 * @param key2 End of range
 * @return A value in the range, or -1 if not found
 */
int bplus_tree_compressed_get_range(struct bplus_tree_compressed *ct_tree, key_t key1, key_t key2);

/**
 * Get compression statistics
 * @param ct_tree Pointer to compressed B+Tree
 * @param total_size Pointer to store total uncompressed size
 * @param compressed_size Pointer to store total compressed size
 * @return 0 on success, -1 on failure
 */
int bplus_tree_compressed_stats(struct bplus_tree_compressed *ct_tree, 
                               size_t *total_size, size_t *compressed_size);

/**
 * Get the number of entries in the tree (thread-safe)
 * @param ct_tree Pointer to compressed B+Tree
 * @return Number of entries
 */
int bplus_tree_compressed_size(struct bplus_tree_compressed *ct_tree);

/**
 * Check if the tree is empty (thread-safe)
 * @param ct_tree Pointer to compressed B+Tree
 * @return 1 if empty, 0 if not empty
 */
int bplus_tree_compressed_empty(struct bplus_tree_compressed *ct_tree);

/**
 * Enable or disable compression
 * @param ct_tree Pointer to compressed B+Tree
 * @param enabled 1 to enable, 0 to disable
 */
void bplus_tree_compressed_set_compression(struct bplus_tree_compressed *ct_tree, int enabled);



/**
 * Set compression configuration (user-defined)
 * @param ct_tree Pointer to compressed B+Tree
 * @param config Compression configuration
 * @return 0 on success, -1 on failure
 */
int bplus_tree_compressed_set_config(struct bplus_tree_compressed *ct_tree,
                                     struct compression_config *config);

/**
 * Get current compression configuration
 * @param ct_tree Pointer to compressed B+Tree
 * @param config Pointer to store current configuration
 * @return 0 on success, -1 on failure
 */
int bplus_tree_compressed_get_config(struct bplus_tree_compressed *ct_tree,
                                     struct compression_config *config);

/**
 * Get compression ratio
 * @param ct_tree Pointer to compressed B+Tree
 * @return Compression ratio as percentage (0-100)
 */
double bplus_tree_compressed_get_compression_ratio(struct bplus_tree_compressed *ct_tree);

/**
 * Get algorithm-specific statistics
 * @param ct_tree Pointer to compressed B+Tree
 * @param lz4_ops Pointer to store LZ4 operation count
 * @param qpl_ops Pointer to store QPL operation count
 * @return 0 on success, -1 on failure
 */
int bplus_tree_compressed_get_algorithm_stats(struct bplus_tree_compressed *ct_tree,
                                              int *lz4_ops, int *qpl_ops);

/**
 * Print compressed tree structure for debugging
 * @param ct_tree Pointer to compressed B+Tree
 */
void bplus_tree_compressed_dump(struct bplus_tree_compressed *ct_tree);

/**
 * Force flush all buffers (useful for shutdown or consistency)
 * @param ct_tree Pointer to compressed B+Tree
 * @return 0 on success, -1 on failure
 */
int bplus_tree_compressed_flush_all_buffers(struct bplus_tree_compressed *ct_tree);

// Internal lazy compression functions

/**
 * Search for a key in the writing buffer
 * @param buffer Pointer to writing buffer
 * @param key The key to search for
 * @return Index if found, -1 if not found
 */
int search_buffer(struct writing_buffer *buffer, key_t key);

/**
 * Add an entry to the writing buffer
 * @param ct_tree Pointer to compressed B+Tree
 * @param leaf Pointer to leaf node
 * @param key The key
 * @param value The value
 * @param operation The operation type ('I', 'U', 'D')
 * @return 0 on success, -1 on failure (buffer full)
 */
int add_to_buffer(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf,
                  key_t key, int value, char operation);

/**
 * Flush a writing buffer to the leaf node (background thread function)
 * @param ct_tree Pointer to compressed B+Tree (for config/algorithm/QPL)
 * @param leaf Pointer to leaf node
 * @return 0 on success, -1 on failure
 */
int flush_buffer_to_leaf(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf);

/**
 * Background thread function for lazy compression
 * @param arg Pointer to compressed B+Tree
 * @return NULL
 */
void *background_compression_thread(void *arg);

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

#endif /* _BPLUS_TREE_COMPRESSED_H */
