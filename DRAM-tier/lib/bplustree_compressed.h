
#ifndef _BPLUS_TREE_COMPRESSED_H
#define _BPLUS_TREE_COMPRESSED_H

#define _GNU_SOURCE
#include <pthread.h>
#include <lz4.h>
#include <lz4hc.h>
#include "qpl_compat.h"
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include "bplustree.h"

// 4KB leaf node size for optimal compression
#ifndef ZIPCACHE_AGG_LAYOUT
#define ZIPCACHE_AGG_LAYOUT 0
#endif
#if ZIPCACHE_AGG_LAYOUT >= 2
#define COMPRESSED_LEAF_SIZE 16384
#else
#define COMPRESSED_LEAF_SIZE 4096
#endif
#define MAX_COMPRESSED_SIZE (COMPRESSED_LEAF_SIZE * 2) // Temporary bound only.

// Legacy constants from btree.h
#define KEY_SIZE 8
#define COMPRESSED_VALUE_BYTES 128
#define TOTAL_SUBPAGES_BYTES 4096
#define ACTIVE_DELTA_ENTRIES 3
#define ZIPCACHE_COMPACT_LEAVES 1

/*
 * Fixed-record v1 representation.  Keeping it in the public prototype header
 * lets the leaf reserve exactly three active records instead of a 2 KiB byte
 * array whose effective capacity was configured at runtime.
 */
struct kv_pair {
    key_t key;
    int stored_value;
    uint8_t payload[COMPRESSED_VALUE_BYTES];
};

#define LANDING_BUFFER_BYTES (ACTIVE_DELTA_ENTRIES * sizeof(struct kv_pair))
#define LANDING_BUFFER_DEFAULT_BYTES LANDING_BUFFER_BYTES

struct compressed_pending_task;
struct compaction_scheduler;

/* Compressed trees never use the ordinary leaf's 64 key/value slots.
 * Keep the common node prefix used by internal nodes and list teardown. */
struct compressed_leaf_ref {
    int type;
    int parent_key_idx;
    struct bplus_non_leaf *parent;
    struct list_head link;
    int entries;
    value_t payload;
};



// Leaf layout personality selection (unified hashed layout focus)
typedef enum {
    LEAF_TYPE_LZ4_HASHED = 0,
    LEAF_TYPE_QPL_APPEND = 1
} leaf_layout_t;

// Global compression algorithm selection (applies to unified hashed layout)
typedef enum {
    COMPRESS_LZ4 = 0,
    COMPRESS_QPL = 1,
    COMPRESS_ZLIB_ACCEL = 2,
    /* Test-only ideal codec used to expose accelerator demand on non-IAA hosts. */
    COMPRESS_COPY = 3,
    /* Uncompressed, exact-live-record storage; not padded COPY. */
    COMPRESS_RAW_PACKED = 4,
    COMPRESS_ZSTD_EXPERIMENT = 5
} compression_algo_t;

typedef enum {
    QPL_HUFFMAN_FIXED = 0,
    QPL_HUFFMAN_DYNAMIC = 1
} qpl_huffman_mode_t;

// Original compression configuration (kept for backward compatibility)
struct compression_config {
    leaf_layout_t default_layout;
    compression_algo_t algo;
    int default_sub_pages;
    int compression_level;
    int buffer_size;
    int flush_threshold;
    int enable_lazy_compression;
    qpl_path_t qpl_path;
    qpl_huffman_mode_t qpl_huffman_mode;
};

// Simplified compression configuration for dual algorithm support
struct simple_compression_config {
    compression_algo_t default_algo;           // COMPRESS_LZ4 or COMPRESS_QPL
    int num_subpages;                         // Number of hash buckets (legacy style)
    int buffer_size;                          // Effective landing buffer bytes; max is LANDING_BUFFER_BYTES.
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
    union {
        struct kv_pair active[ACTIVE_DELTA_ENTRIES];
        char landing_buffer[LANDING_BUFFER_BYTES];  // Source-compatible internal alias.
    };
    struct compressed_pending_task *pending;    // At most one sealed delta.
    char *compressed_data;                      // Immutable exact-size compressed base.
    int compressed_size;                        // Size of compressed data
    size_t compressed_capacity;                 // Requested resident bytes.
    size_t compressed_usable;                   // Allocator-usable resident bytes.
    int num_subpages;                          // Number of hash buckets (legacy)
    bool is_compressed;                        // Compression state
    compression_algo_t compression_algo;       // LZ4 or QPL (stored per leaf)

    // LZ4-specific fields (only used when algo == COMPRESS_LZ4)
    struct subpage_index_entry *subpage_index; // For partial decompression
    int num_subpage_entries;                   // Number of indexed subpages
    struct subpage_index_entry inline_index;  // Single-subpage allocation-free index.

    // Per-leaf concurrency control
    pthread_rwlock_t rwlock;                   // Read-write lock for this leaf
    uint64_t generation;                       // Incremented on leaf content changes
    uint64_t base_version;                     // Changes only when base is replaced.
    uint64_t pending_token;                    // Identifies the installed pending task.

    // Statistics
    size_t uncompressed_bytes;
    size_t compressed_bytes;
};


// Thread-safe compressed B+Tree structure
struct bplus_tree_compressed {
    struct bplus_tree *tree;                // Original B+Tree
    struct bplus_tree_compressed **shards;  // Optional shard children
    int shard_count;                        // >1 means this object routes to shards
    pthread_rwlock_t rwlock;                // Read-write lock for concurrency control
    int initialized;                        // Initialization flag
    int compression_enabled;                // Whether compression is enabled
    int debug_mode;                         // Enable debug output (0=off, 1=on)
    struct compaction_scheduler *scheduler; // Shared across all shards.
    int owns_scheduler;                     // Only the root starts/stops it.

    // Compression configuration
    struct compression_config config;       // User-defined compression settings
    compression_algo_t requested_algo;      // Before explicit unavailable-library fallback.
    uint64_t codec_fallbacks;               // Explicit application fallback attempts, not opaque QPL auto decisions.
    struct simple_compression_config simple_config;  // Simplified dual-algorithm configuration
    int is_simple_mode;                     // Flag: 1 for simple API, 0 for legacy API

    // QPL job pool
    qpl_job **qpl_job_pool;                // Array of job pointers
    uint8_t **qpl_job_buffers;             // Backing buffers for jobs
    int qpl_pool_size;                     // Number of jobs in pool
    size_t qpl_pool_bytes;                 // Requested bytes for pool bookkeeping/jobs.
    int qpl_initialized;                   // Path validation completed.

    // Pool management
    int *qpl_job_free_list;                // Stack of free job indices
    int qpl_free_count;                    // Number of free jobs
    pthread_mutex_t qpl_pool_lock;         // Protects pool structures
    pthread_cond_t qpl_pool_cond;          // Signals job availability

    // Global compression statistics (atomic for lock-free concurrent updates)
    size_t total_uncompressed_size;     // Total uncompressed size
    size_t total_compressed_size;       // Total compressed size
    int compression_operations;         // Number of compression operations
    int decompression_operations;       // Number of decompression operations

    // Optional background landing-buffer compaction.
    int bg_compaction_enabled;
    int bg_thread_count;
    pthread_t *bg_threads;
    volatile int bg_shutdown;
    int bg_scan_interval_us;
    int bg_landing_high_watermark_pct;
    int bg_max_leaves_per_pass;
    int bg_max_compactions_per_sec;
    int bg_trylock_only;
    int bg_codec_filter;  // -1=all, otherwise compression_algo_t
    pthread_mutex_t bg_scan_lock;
    pthread_mutex_t bg_queue_lock;
    pthread_cond_t bg_queue_cond;
    key_t *bg_dirty_keys;
    int bg_queue_capacity;
    int bg_queue_head;
    int bg_queue_tail;
    int bg_queue_count;
    uint64_t bg_passes;
    uint64_t bg_compactions;
    uint64_t bg_trylock_misses;
    uint64_t bg_skipped;
    uint64_t bg_errors;
    uint64_t bg_enqueue_attempts;
    uint64_t bg_enqueued;
    uint64_t bg_enqueue_duplicates;
    uint64_t bg_queue_full;
    uint64_t bg_queue_pops;
    uint64_t fg_landing_full;
    uint64_t fg_sync_compactions;
    uint64_t fg_sync_compaction_errors;
    uint64_t fg_split_fallbacks;

    // Codec API counters for QPL/zlib API-level optimization.
    uint64_t qpl_compress_calls;
    uint64_t qpl_decompress_calls;
    uint64_t qpl_tls_jobs;
    uint64_t qpl_pool_jobs;
    uint64_t qpl_errors;
    uint64_t zlib_compress_calls;
    uint64_t zlib_decompress_calls;
    uint64_t zlib_stream_reuses;
    uint64_t zlib_stream_inits;
    uint64_t zlib_errors;

    /* Optional producer-side IAA submission-shape instrumentation. */
    int submission_profile_enabled;
    uint64_t submission_logical_reads;
    uint64_t submission_logical_writes;
    uint64_t submission_compress_calls;
    uint64_t submission_decompress_calls;
    uint64_t submission_compress_input_bytes;
    uint64_t submission_decompress_input_bytes;
    uint64_t submission_calls_under_write_lock;
    uint64_t submission_input_bytes_under_write_lock;
    uint64_t submission_current_inflight;
    uint64_t submission_peak_inflight;
#ifdef ZIPCACHE_AGG_EXPERIMENT
    /* Init-only execution policy. Never stored in a leaf or encoded frame. */
    int split_routes_enabled;
    qpl_path_t split_routes[3]; /* compression, point read, maintenance read */
#endif
};

struct bplus_tree_memory_stats {
    size_t live_kv_bytes;
    size_t tree_metadata_bytes;
    size_t leaf_metadata_bytes;
    size_t compressed_requested_bytes;
    size_t compressed_usable_bytes;
    size_t active_allocated_bytes;
    size_t pending_allocated_bytes;
    size_t scheduler_and_qpl_bytes;
    size_t tree_metadata_usable_bytes;
    size_t leaf_metadata_usable_bytes; /* active requested bytes subtracted */
    size_t pending_requested_bytes;
    size_t scheduler_and_qpl_usable_bytes;
    size_t process_qpl_tls_bytes; /* process scope, never sum across roots */
    size_t process_zlib_workspace_bytes; /* process scope, allocator-usable */
    size_t process_zstd_workspace_bytes; /* experimental CPU comparator, process scope */
    size_t leaf_count;
    size_t fill_histogram[129]; /* distinct live records; final bucket >=128 */
    size_t failed_pending_count;
};

struct bplus_tree_allocation_audit {
    int enabled;
    size_t requested, usable, peak_requested, peak_usable;
    size_t live_allocations, tracker_bytes;
};
struct bplus_tree_backend_info {
    compression_algo_t requested, effective;
    const char *library_version;
    const char *execution_path;
    uint64_t explicit_fallbacks;
};
int bplus_tree_compressed_backend_info(struct bplus_tree_compressed *tree,
                                      struct bplus_tree_backend_info *info);
void bplus_tree_compressed_allocation_audit(struct bplus_tree_allocation_audit *out,
                                           int reset_peak);
void bplus_tree_compressed_release_thread_resources(void);
/* Deterministic contract-test controls; unavailable unless explicitly enabled. */
struct bplus_tree_contract_stats {
    uint64_t accepted, terminal, busy, failures, out_of_order, in_flight;
};
int bplus_tree_compressed_test_contract(struct bplus_tree_compressed *tree, int gate_open,
                                       struct bplus_tree_contract_stats *stats);
/* Diagnostic only: quiesce producers; visitor cannot reenter the tree. */
int bplus_tree_compressed_verify(struct bplus_tree_compressed *tree,
                                 int (*visitor)(const struct kv_pair *, void *), void *context);

struct bplus_tree_scheduler_stats {
    uint64_t queue_depth;
    uint64_t queue_peak;
    uint64_t batches;
    uint64_t submitted_tasks;
    uint64_t completed_tasks;
    uint64_t failed_tasks;
    uint64_t retry_count;
    uint64_t queue_full_fallbacks;
    uint64_t synchronous_fallbacks;
    uint64_t split_fallbacks;
    uint64_t total_queue_wait_ns;
    uint64_t max_queue_wait_ns;
    uint64_t submitted_jobs, terminal_jobs, failed_jobs, busy_jobs, outstanding_jobs;
    uint64_t decompress_phase_ns, merge_phase_ns, compress_phase_ns, commit_phase_ns;
    uint64_t ready_at_wait_samples;
};

struct bplus_tree_submission_stats {
    uint64_t logical_reads;
    uint64_t logical_writes;
    uint64_t compress_calls;
    uint64_t decompress_calls;
    uint64_t compress_input_bytes;
    uint64_t decompress_input_bytes;
    uint64_t calls_under_write_lock;
    uint64_t input_bytes_under_write_lock;
    uint64_t current_inflight;
    uint64_t peak_inflight;
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

/* Additive full-record API. On failure the caller's output is unchanged.
 * Lifetime is the caller's responsibility, as with existing tree APIs. */
enum bplus_record_status {
    BPLUS_RECORD_OK = 0, BPLUS_RECORD_NOT_FOUND = 1,
    BPLUS_RECORD_NO_SPACE = 2, BPLUS_RECORD_CODEC_ERROR = 3,
    BPLUS_RECORD_INVALID = 4, BPLUS_RECORD_CLOSED = 5
};
int bplus_tree_compressed_get_record(struct bplus_tree_compressed *tree,
                                     key_t key, struct kv_pair *out);
#ifdef ZIPCACHE_AGG_EXPERIMENT
/* Current-thread, one-shot fault injection; -1 disables. Test builds only. */
void bplus_tree_compressed_test_fail_allocation(long after);
/* Per-leaf snapshot, not a multi-key transaction. At most 32 keys. */
int bplus_tree_compressed_get_many_records(struct bplus_tree_compressed *tree,
    const key_t *keys, size_t count, struct kv_pair *out, int *statuses);
enum bplus_codec_origin { BPLUS_ORIGIN_GET, BPLUS_ORIGIN_COMPACTION,
    BPLUS_ORIGIN_SPLIT, BPLUS_ORIGIN_DELETE, BPLUS_ORIGIN_OTHER, BPLUS_ORIGIN_COUNT };
struct bplus_aggregation_stats {
    uint64_t compress[BPLUS_ORIGIN_COUNT], decompress[BPLUS_ORIGIN_COUNT];
    uint64_t encode_bytes[BPLUS_ORIGIN_COUNT], decode_bytes[BPLUS_ORIGIN_COUNT];
    uint64_t active_hits, pending_hits, base_hits, misses, bypass_blocks;
    uint64_t merged_updates, merge_count, pending_wait_ns, read_lock_wait_ns;
    uint64_t queue_wait_ns, copied_compressed_bytes;
    uint64_t admission_wait_ns, admission_waits;
    uint64_t codec_ns[BPLUS_ORIGIN_COUNT][2]; /* decode, encode; includes job preparation */
    uint64_t codec_failures[BPLUS_ORIGIN_COUNT][2];
    uint64_t route_calls[3], route_completed[3], route_failed[3];
    uint64_t route_input_bytes[3], route_output_bytes[3], route_ns[3];
    uint64_t route_busy[3], route_last_status[3];
    uint64_t accel_counters[20]; /* frozen shim's per-thread counters, aggregated at call boundaries */
};
/* Test/diagnostic only: policy is immutable after initialization. */
int bplus_tree_compressed_execution_routes(struct bplus_tree_compressed *tree,
                                          qpl_path_t paths[3]);
/* Process-local test setup, before creating any tree/worker; never edits /etc. */
int bplus_tree_compressed_accel_configure(int compress, int decompress,
                                         const char **library);
/* Process-scoped diagnostic counters: reset only with quiescent producers. */
void bplus_tree_compressed_aggregation_stats(struct bplus_aggregation_stats *out, int reset);
#endif

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
 * Compatibility range lookup with decompression. This is not the future
 * ordered callback-based SCAN API.
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

/* Resident-memory and bounded-scheduler diagnostics for the v1 architecture. */
int bplus_tree_compressed_memory_stats(struct bplus_tree_compressed *ct_tree,
                                       struct bplus_tree_memory_stats *stats);
int bplus_tree_compressed_scheduler_stats(struct bplus_tree_compressed *ct_tree,
                                          struct bplus_tree_scheduler_stats *stats);
int bplus_tree_compressed_drain_background(struct bplus_tree_compressed *ct_tree);

/**
 * Get background landing-buffer compaction counters.
 * All output pointers are optional.
 */
int bplus_tree_compressed_bg_stats(struct bplus_tree_compressed *ct_tree,
                                   uint64_t *passes,
                                   uint64_t *compactions,
                                   uint64_t *trylock_misses,
                                   uint64_t *skipped,
                                   uint64_t *errors);

/**
 * Get Phase-8 foreground/background compaction counters.
 * All output pointers are optional.
 */
int bplus_tree_compressed_compaction_stats(struct bplus_tree_compressed *ct_tree,
                                           uint64_t *fg_landing_full,
                                           uint64_t *fg_sync_compactions,
                                           uint64_t *fg_sync_compaction_errors,
                                           uint64_t *fg_split_fallbacks,
                                           uint64_t *bg_enqueue_attempts,
                                           uint64_t *bg_enqueued,
                                           uint64_t *bg_enqueue_duplicates,
                                           uint64_t *bg_queue_full,
                                           uint64_t *bg_queue_pops);

/**
 * Get codec API counters. These counters expose B+Tree-side API usage, not
 * low-level IAA queue state. All output pointers are optional.
 */
int bplus_tree_compressed_codec_stats(struct bplus_tree_compressed *ct_tree,
                                      uint64_t *qpl_compress_calls,
                                      uint64_t *qpl_decompress_calls,
                                      uint64_t *qpl_tls_jobs,
                                      uint64_t *qpl_pool_jobs,
                                      uint64_t *qpl_errors,
                                      uint64_t *zlib_compress_calls,
                                      uint64_t *zlib_decompress_calls,
                                      uint64_t *zlib_stream_reuses,
                                      uint64_t *zlib_stream_inits,
                                      uint64_t *zlib_errors);

/*
 * Reset and read producer-side submission instrumentation.  Reset is intended
 * for a quiescent tree after workload warmup; it also starts a configured CSV
 * trace (BTREE_SUBMISSION_TRACE=/path/to/events.csv).
 */
void bplus_tree_compressed_submission_reset(struct bplus_tree_compressed *ct_tree);
int bplus_tree_compressed_submission_stats(
    struct bplus_tree_compressed *ct_tree,
    struct bplus_tree_submission_stats *stats);

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
