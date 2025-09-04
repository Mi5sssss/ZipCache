/*
 * ZipCache - High-Performance Multi-Tier Caching System
 * 
 * This system orchestrates three specialized B+tree implementations:
 * - BT_DRAM: In-memory cache for tiny/medium objects
 * - BT_LO: Large Object cache with SSD storage pointers  
 * - BT_SSD: SSD-tier cache with super-leaf pages
 */

#ifndef _ZIPCACHE_H
#define _ZIPCACHE_H

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>

/* Include all tier implementations */
#include "DRAM-tier/lib/bplustree_compressed.h"
#include "LO-tier/lib/bplustree_lo.h"
#include "SSD-tier/lib/bplustree.h"

/* Object size thresholds for tier classification (defaults) */
#define ZIPCACHE_TINY_DEFAULT       128     /* 128 bytes */
#define ZIPCACHE_MEDIUM_DEFAULT     2048    /* 2KB */
#define ZIPCACHE_LARGE_THRESHOLD    SIZE_MAX

/* Cache configuration constants */
#define ZIPCACHE_DRAM_SIZE_MB       256     /* DRAM tier size in MB */
#define ZIPCACHE_EVICTION_THRESHOLD 0.9     /* Eviction trigger at 90% full */
#define ZIPCACHE_MAX_KEY_SIZE       256     /* Maximum key size */
#define ZIPCACHE_MAX_OBJECTS        1000000 /* Maximum cached objects */

/* Special tombstone marker for invalidated objects */
#define ZIPCACHE_TOMBSTONE_MARKER   ((void*)0xDEADBEEF)
#define ZIPCACHE_TOMBSTONE_SIZE     0

/* Object classification types */
typedef enum {
    ZIPCACHE_OBJ_TINY = 0,      /* 0 - 128 bytes */
    ZIPCACHE_OBJ_MEDIUM,        /* 129 - 2048 bytes */
    ZIPCACHE_OBJ_LARGE,         /* > 2048 bytes */
    ZIPCACHE_OBJ_UNKNOWN
} zipcache_obj_type_t;

/* Cache operation results */
typedef enum {
    ZIPCACHE_OK = 0,
    ZIPCACHE_ERROR = -1,
    ZIPCACHE_NOT_FOUND = -2,
    ZIPCACHE_OUT_OF_MEMORY = -3,
    ZIPCACHE_INVALID_SIZE = -4,
    ZIPCACHE_IO_ERROR = -5,
    ZIPCACHE_TOMBSTONE = -6
} zipcache_result_t;

/* Large object storage descriptor */
typedef struct {
    uint64_t lba;           /* Logical Block Address on SSD */
    uint32_t size;          /* Object size in bytes */
    uint32_t checksum;      /* Data integrity checksum */
    uint64_t timestamp;     /* Last access timestamp */
} zipcache_large_obj_t;

/* Cache statistics */
typedef struct {
    uint64_t hits_dram;         /* DRAM tier hits */
    uint64_t hits_lo;           /* Large Object tier hits */
    uint64_t hits_ssd;          /* SSD tier hits */
    uint64_t misses;            /* Total cache misses */
    uint64_t puts_tiny;         /* Tiny object puts */
    uint64_t puts_medium;       /* Medium object puts */
    uint64_t puts_large;        /* Large object puts */
    uint64_t evictions;         /* DRAM evictions to SSD */
    uint64_t promotions;        /* SSD promotions to DRAM */
    uint64_t tombstones;        /* Tombstone invalidations */
    size_t memory_used;         /* Current DRAM usage */
    size_t memory_capacity;     /* Total DRAM capacity */
} zipcache_stats_t;

/* Eviction policy state */
typedef struct {
    uint32_t *access_bits;      /* Second-chance access bits */
    uint32_t clock_hand;        /* Current clock hand position */
    uint32_t total_pages;       /* Total DRAM pages */
    pthread_mutex_t lock;       /* Eviction synchronization */
} zipcache_eviction_t;

/* DRAM object store for holding actual object data */
typedef struct {
    void *ptr;
    size_t size;
    int valid;
} dram_object_t;

typedef struct {
    dram_object_t *objects;
    int capacity;
    int count;
    pthread_mutex_t lock;
} dram_object_store_t;

/* Main ZipCache instance */
typedef struct {
    /* Tier implementations */
    struct bplus_tree_compressed *bt_dram;     /* DRAM tier (tiny/medium objects) */
    dram_object_store_t dram_store; /* DRAM object store */
    struct bplus_tree_lo *bt_lo;    /* Large Object tier */
    struct bplus_tree_ssd *bt_ssd;      /* SSD tier (super-leaf pages) */
    
    /* Configuration */
    size_t tiny_threshold;          /* Tiny object size limit */
    size_t medium_threshold;        /* Medium object size limit */
    size_t dram_capacity;           /* DRAM tier capacity */
    
    /* Runtime state */
    zipcache_stats_t stats;         /* Performance statistics */
    zipcache_eviction_t eviction;   /* Eviction policy state */
    
    /* Thread synchronization */
    pthread_mutex_t cache_lock;     /* Cache-wide lock for operations */
    pthread_mutex_t stats_lock;     /* Statistics update lock */
    pthread_t eviction_thread;      /* Background eviction thread */
    int shutdown_flag;              /* Shutdown signal */
    
    /* SSD storage management */
    int ssd_fd;                     /* SSD file descriptor */
    char ssd_path[256];             /* SSD storage path */
    uint64_t ssd_offset;            /* Current SSD write offset */
    pthread_mutex_t ssd_lock;       /* SSD I/O synchronization */
    
} zipcache_t;

/* ============================================================================
 * MAIN API FUNCTIONS
 * ============================================================================ */

/**
 * Initialize ZipCache instance with specified configuration (using defaults)
 */
zipcache_t *zipcache_init(size_t dram_capacity_mb, const char *ssd_path);

/**
 * Extended initializer with explicit thresholds.
 * Returns NULL on invalid thresholds.
 */
zipcache_t *zipcache_init_ex(size_t dram_capacity_mb, const char *ssd_path,
                             size_t tiny_max, size_t medium_max);

/**
 * Shutdown ZipCache and cleanup all resources
 */
void zipcache_destroy(zipcache_t *cache);

/**
 * Store an object in the cache (PUT operation)
 * Routes to appropriate tier based on object size
 */
zipcache_result_t zipcache_put(zipcache_t *cache, const char *key, 
                               const void *value, size_t size);

/**
 * Retrieve an object from the cache (GET operation)
 * Searches all tiers in order: DRAM -> LO -> SSD
 * Implements cache promotion for inclusive policy
 */
zipcache_result_t zipcache_get(zipcache_t *cache, const char *key,
                               void **value, size_t *size);

/**
 * Remove an object from all cache tiers
 */
zipcache_result_t zipcache_delete(zipcache_t *cache, const char *key);

/**
 * Get current cache statistics
 */
void zipcache_get_stats(zipcache_t *cache, zipcache_stats_t *stats);

/**
 * Reset cache statistics
 */
void zipcache_reset_stats(zipcache_t *cache);

/* ============================================================================
 * INTERNAL HELPER FUNCTIONS
 * ============================================================================ */

/**
 * Classify object based on size thresholds
 */
zipcache_obj_type_t zipcache_classify_object(zipcache_t *cache, size_t size);

/* Backward-compat: allow calling classification with defaults only.
 * Define ZIPCACHE_DISABLE_CLASSIFY_SIZE_COMPAT to disable this alias. */
static inline zipcache_obj_type_t zipcache_classify_size_default(size_t size) {
    if (size <= ZIPCACHE_TINY_DEFAULT) return ZIPCACHE_OBJ_TINY;
    if (size <= ZIPCACHE_MEDIUM_DEFAULT) return ZIPCACHE_OBJ_MEDIUM;
    return ZIPCACHE_OBJ_LARGE;
}
#ifdef ZIPCACHE_ENABLE_CLASSIFY_SIZE_COMPAT
#define zipcache_classify_object(size) zipcache_classify_size_default(size)
#endif

/**
 * Update thresholds at runtime (thread-safe). Affects new operations only.
 * Returns ZIPCACHE_OK or ZIPCACHE_INVALID_SIZE on bad input.
 */
int zipcache_set_thresholds(zipcache_t *cache, size_t tiny_max, size_t medium_max);

/**
 * Read current thresholds (thread-safe).
 */
void zipcache_get_thresholds(zipcache_t *cache, size_t *tiny_max, size_t *medium_max);

/**
 * Write router - directs PUT operations to appropriate tier
 */
zipcache_result_t zipcache_route_put(zipcache_t *cache, const char *key,
                                     const void *value, size_t size,
                                     zipcache_obj_type_t type);

/**
 * Coordinated read - searches all tiers in optimal order
 */
zipcache_result_t zipcache_coordinated_read(zipcache_t *cache, const char *key,
                                           void **value, size_t *size);

/**
 * Cache promotion - move object from SSD to DRAM tier
 */
zipcache_result_t zipcache_promote_object(zipcache_t *cache, const char *key,
                                          const void *value, size_t size);

/**
 * Data consistency - handle invalidations and tombstones
 */
zipcache_result_t zipcache_invalidate_stale(zipcache_t *cache, const char *key,
                                           zipcache_obj_type_t new_type);

/**
 * Background eviction - move cold DRAM data to SSD
 */
void *zipcache_eviction_thread(void *arg);

/**
 * Eviction policy - second-chance algorithm implementation
 */
zipcache_result_t zipcache_evict_cold_pages(zipcache_t *cache, size_t target_bytes);

/**
 * Large object storage - write object directly to SSD
 */
zipcache_result_t zipcache_write_large_object(zipcache_t *cache, const void *value, 
                                              size_t size, zipcache_large_obj_t *descriptor);

/**
 * Large object retrieval - read object from SSD
 */
zipcache_result_t zipcache_read_large_object(zipcache_t *cache, 
                                             const zipcache_large_obj_t *descriptor,
                                             void **value, size_t *size);

/**
 * Memory usage monitoring
 */
size_t zipcache_get_dram_usage(zipcache_t *cache);

/**
 * Check if eviction is needed
 */
int zipcache_needs_eviction(zipcache_t *cache);

/* ============================================================================
 * UTILITY FUNCTIONS
 * ============================================================================ */

/**
 * Calculate checksum for data integrity
 */
uint32_t zipcache_checksum(const void *data, size_t size);

/**
 * Get current timestamp in microseconds
 */
uint64_t zipcache_timestamp(void);

/**
 * Hash function for key distribution
 */
uint32_t zipcache_hash_key(const char *key);

/**
 * Safe string operations
 */
int zipcache_safe_strncpy(char *dest, const char *src, size_t dest_size);

/**
 * Memory allocation with alignment
 */
void *zipcache_aligned_alloc(size_t size, size_t alignment);

/**
 * Free aligned memory
 */
void zipcache_aligned_free(void *ptr);

/* ============================================================================
 * DEBUG AND MONITORING
 * ============================================================================ */

/**
 * Dump cache state for debugging
 */
void zipcache_dump_state(zipcache_t *cache);

/**
 * Validate cache consistency
 */
int zipcache_validate_consistency(zipcache_t *cache);

/**
 * Print performance statistics
 */
void zipcache_print_stats(zipcache_t *cache);

/**
 * Enable/disable debug logging
 */
void zipcache_set_debug(int enable);

#endif /* _ZIPCACHE_H */
