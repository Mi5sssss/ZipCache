/*
 * ZipCache - High-Performance Multi-Tier Caching System
 * Implementation of the main orchestration logic
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <errno.h>
#include <assert.h>
#include <pthread.h>

#include "zipcache.h"

/* ============================================================================
 * DRAM OBJECT STORE HELPERS
 * ============================================================================ */

static int dram_store_add(dram_object_store_t *store, void *value, size_t size)
{
    pthread_mutex_lock(&store->lock);
    if (store->count == store->capacity) {
        // For simplicity, we don't handle resizing. In a real system, we would realloc.
        pthread_mutex_unlock(&store->lock);
        return -1; 
    }
    int handle = store->count;
    store->objects[handle].ptr = value;
    store->objects[handle].size = size;
    store->objects[handle].valid = 1;
    store->count++;
    pthread_mutex_unlock(&store->lock);
    return handle;
}

static dram_object_t* dram_store_get(dram_object_store_t *store, int handle)
{
    if (handle < 0 || handle >= store->count) {
        return NULL;
    }
    // No lock needed for read if we assume handles are never reused.
    // A robust implementation would need more careful synchronization.
    if (store->objects[handle].valid) {
        return &store->objects[handle];
    }
    return NULL;
}

/* Debug logging */
static int g_zipcache_debug = 0;

#define ZIPCACHE_DEBUG(fmt, ...) do { \
    if (g_zipcache_debug) { \
        printf("[ZipCache] " fmt "\n", ##__VA_ARGS__); \
    } \
} while(0)

#define ZIPCACHE_ERROR(fmt, ...) \
    fprintf(stderr, "[ZipCache ERROR] " fmt "\n", ##__VA_ARGS__)

/* ============================================================================
 * INITIALIZATION AND DESTRUCTION
 * ============================================================================ */

zipcache_t *zipcache_init(size_t dram_capacity_mb, const char *ssd_path)
{
    zipcache_t *cache = calloc(1, sizeof(zipcache_t));
    if (!cache) {
        ZIPCACHE_ERROR("Failed to allocate cache structure");
        return NULL;
    }

    /* Set configuration */
    cache->tiny_threshold = ZIPCACHE_TINY_DEFAULT;
    cache->medium_threshold = ZIPCACHE_MEDIUM_DEFAULT;
    cache->dram_capacity = dram_capacity_mb * 1024 * 1024;
    
    /* Initialize SSD storage path */
    if (zipcache_safe_strncpy(cache->ssd_path, ssd_path, sizeof(cache->ssd_path)) != 0) {
        ZIPCACHE_ERROR("SSD path too long: %s", ssd_path);
        free(cache);
        return NULL;
    }

    ZIPCACHE_DEBUG("Initializing ZipCache:");
    ZIPCACHE_DEBUG("  DRAM capacity: %zu MB", dram_capacity_mb);
    ZIPCACHE_DEBUG("  Tiny threshold: %zu bytes", cache->tiny_threshold);
    ZIPCACHE_DEBUG("  Medium threshold: %zu bytes", cache->medium_threshold);
    ZIPCACHE_DEBUG("  SSD path: %s", cache->ssd_path);

    /* Initialize DRAM tier (BT_DRAM) */
    cache->bt_dram = bplus_tree_init(16, 64); /* In-memory only */
    if (!cache->bt_dram) {
        ZIPCACHE_ERROR("Failed to initialize DRAM tier");
        free(cache);
        return NULL;
    }
    ZIPCACHE_DEBUG("✓ DRAM tier initialized");

    /* Initialize Large Object tier (BT_LO) */
    char lo_path[300];
    snprintf(lo_path, sizeof(lo_path), "%s.lo", ssd_path);
    cache->bt_lo = bplus_tree_lo_init(16);
    if (!cache->bt_lo) {
        ZIPCACHE_ERROR("Failed to initialize Large Object tier");
        bplus_tree_deinit(cache->bt_dram);
        free(cache);
        return NULL;
    }
    ZIPCACHE_DEBUG("✓ Large Object tier initialized");

    /* Initialize SSD tier (BT_SSD) */
    char ssd_btree_path[300];
    snprintf(ssd_btree_path, sizeof(ssd_btree_path), "%s.ssd", ssd_path);
    cache->bt_ssd = bplus_tree_ssd_init(16, 64, ssd_btree_path);
    if (!cache->bt_ssd) {
        ZIPCACHE_ERROR("Failed to initialize SSD tier");
        bplus_tree_lo_deinit(cache->bt_lo);
        bplus_tree_deinit(cache->bt_dram);
        free(cache);
        return NULL;
    }
    ZIPCACHE_DEBUG("✓ SSD tier initialized");

    /* Open SSD storage file for large objects */
    char ssd_storage_path[300];
    snprintf(ssd_storage_path, sizeof(ssd_storage_path), "%s.storage", ssd_path);
    cache->ssd_fd = open(ssd_storage_path, O_CREAT | O_RDWR, 0644);
    if (cache->ssd_fd < 0) {
        ZIPCACHE_ERROR("Failed to open SSD storage file: %s", strerror(errno));
        bplus_tree_ssd_deinit(cache->bt_ssd);
        bplus_tree_lo_deinit(cache->bt_lo);
        bplus_tree_deinit(cache->bt_dram);
        free(cache);
        return NULL;
    }
    cache->ssd_offset = 0;
    ZIPCACHE_DEBUG("✓ SSD storage file opened: %s", ssd_storage_path);

    /* Initialize synchronization */
    if (pthread_mutex_init(&cache->cache_lock, NULL) != 0 ||
        pthread_mutex_init(&cache->stats_lock, NULL) != 0 ||
        pthread_mutex_init(&cache->ssd_lock, NULL) != 0 ||
        pthread_mutex_init(&cache->eviction.lock, NULL) != 0) {
        ZIPCACHE_ERROR("Failed to initialize synchronization primitives");
        close(cache->ssd_fd);
        bplus_tree_ssd_deinit(cache->bt_ssd);
        bplus_tree_lo_deinit(cache->bt_lo);
        bplus_tree_deinit(cache->bt_dram);
        free(cache);
        return NULL;
    }

    /* Initialize eviction state */
    cache->eviction.total_pages = (cache->dram_capacity / 4096) + 1;
    cache->eviction.access_bits = calloc(cache->eviction.total_pages, sizeof(uint32_t));
    if (!cache->eviction.access_bits) {
        ZIPCACHE_ERROR("Failed to allocate eviction state");
        pthread_mutex_destroy(&cache->cache_lock);
        pthread_mutex_destroy(&cache->stats_lock);
        pthread_mutex_destroy(&cache->ssd_lock);
        pthread_mutex_destroy(&cache->eviction.lock);
        close(cache->ssd_fd);
        bplus_tree_ssd_deinit(cache->bt_ssd);
        bplus_tree_lo_deinit(cache->bt_lo);
        bplus_tree_deinit(cache->bt_dram);
        free(cache);
        return NULL;
    }
    cache->eviction.clock_hand = 0;

    /* Initialize statistics */
    cache->stats.memory_capacity = cache->dram_capacity;
    
    /* Start background eviction thread */
    cache->shutdown_flag = 0;
    if (pthread_create(&cache->eviction_thread, NULL, zipcache_eviction_thread, cache) != 0) {
        ZIPCACHE_ERROR("Failed to create eviction thread");
        free(cache->eviction.access_bits);
        pthread_mutex_destroy(&cache->cache_lock);
        pthread_mutex_destroy(&cache->stats_lock);
        pthread_mutex_destroy(&cache->ssd_lock);
        pthread_mutex_destroy(&cache->eviction.lock);
        close(cache->ssd_fd);
        bplus_tree_ssd_deinit(cache->bt_ssd);
        bplus_tree_lo_deinit(cache->bt_lo);
        bplus_tree_deinit(cache->bt_dram);
        free(cache);
        return NULL;
    }

    ZIPCACHE_DEBUG("✅ ZipCache initialization complete");
    return cache;
}

zipcache_t *zipcache_init_ex(size_t dram_capacity_mb, const char *ssd_path,
                             size_t tiny_max, size_t medium_max)
{
    if (tiny_max == 0 || tiny_max >= medium_max || medium_max >= ZIPCACHE_LARGE_THRESHOLD) {
        ZIPCACHE_ERROR("Invalid thresholds: tiny=%zu, medium=%zu", tiny_max, medium_max);
        return NULL;
    }
    zipcache_t *cache = zipcache_init(dram_capacity_mb, ssd_path);
    if (!cache) return NULL;

    pthread_mutex_lock(&cache->cache_lock);
    cache->tiny_threshold = tiny_max;
    cache->medium_threshold = medium_max;
    pthread_mutex_unlock(&cache->cache_lock);

    ZIPCACHE_DEBUG("Thresholds set (init_ex): tiny=%zu, medium=%zu", tiny_max, medium_max);
    return cache;
}

int zipcache_set_thresholds(zipcache_t *cache, size_t tiny_max, size_t medium_max)
{
    if (!cache) return ZIPCACHE_ERROR;
    if (tiny_max == 0 || tiny_max >= medium_max || medium_max >= ZIPCACHE_LARGE_THRESHOLD) {
        return ZIPCACHE_INVALID_SIZE;
    }
    pthread_mutex_lock(&cache->cache_lock);
    cache->tiny_threshold = tiny_max;
    cache->medium_threshold = medium_max;
    pthread_mutex_unlock(&cache->cache_lock);
    ZIPCACHE_DEBUG("Thresholds updated: tiny=%zu, medium=%zu", tiny_max, medium_max);
    return ZIPCACHE_OK;
}

void zipcache_get_thresholds(zipcache_t *cache, size_t *tiny_max, size_t *medium_max)
{
    if (!cache) return;
    pthread_mutex_lock(&cache->cache_lock);
    if (tiny_max) *tiny_max = cache->tiny_threshold;
    if (medium_max) *medium_max = cache->medium_threshold;
    pthread_mutex_unlock(&cache->cache_lock);
}

void zipcache_destroy(zipcache_t *cache)
{
    if (!cache) return;

    ZIPCACHE_DEBUG("Shutting down ZipCache...");

    /* Signal shutdown to eviction thread */
    cache->shutdown_flag = 1;
    pthread_join(cache->eviction_thread, NULL);

    /* Cleanup synchronization */
    pthread_mutex_destroy(&cache->cache_lock);
    pthread_mutex_destroy(&cache->stats_lock);
    pthread_mutex_destroy(&cache->ssd_lock);
    pthread_mutex_destroy(&cache->eviction.lock);

    /* Cleanup tiers */
    bplus_tree_ssd_deinit(cache->bt_ssd);
    bplus_tree_lo_deinit(cache->bt_lo);
    bplus_tree_deinit(cache->bt_dram);

    /* Cleanup SSD storage */
    if (cache->ssd_fd >= 0) {
        fsync(cache->ssd_fd);
        close(cache->ssd_fd);
    }

    /* Cleanup eviction state */
    free(cache->eviction.access_bits);

    /* Print final statistics */
    ZIPCACHE_DEBUG("Final statistics:");
    zipcache_print_stats(cache);

    free(cache);
    ZIPCACHE_DEBUG("✓ ZipCache shutdown complete");
}

/* ============================================================================
 * OBJECT CLASSIFICATION AND ROUTING
 * ============================================================================ */

zipcache_obj_type_t zipcache_classify_object(zipcache_t *cache, size_t size)
{
    if (!cache) return ZIPCACHE_OBJ_UNKNOWN;
    if (size <= cache->tiny_threshold) {
        return ZIPCACHE_OBJ_TINY;
    } else if (size <= cache->medium_threshold) {
        return ZIPCACHE_OBJ_MEDIUM;
    } else {
        return ZIPCACHE_OBJ_LARGE;
    }
}

zipcache_result_t zipcache_route_put(zipcache_t *cache, const char *key,
                                     const void *value, size_t size,
                                     zipcache_obj_type_t type)
{
    zipcache_result_t result;
    
    ZIPCACHE_DEBUG("Routing PUT: key=%s, size=%zu, type=%d", key, size, type);

    switch (type) {
        case ZIPCACHE_OBJ_TINY:
        case ZIPCACHE_OBJ_MEDIUM:
            /* Route to DRAM tier */
            ZIPCACHE_DEBUG("→ Routing to DRAM tier");
            
            /* Insert into DRAM B+tree */
            int dram_result = bplus_tree_put(cache->bt_dram, 
                                           zipcache_hash_key(key), 
                                           (long)value); /* Store pointer for now */
            
            if (dram_result == 0) {
                /* Update statistics */
                pthread_mutex_lock(&cache->stats_lock);
                if (type == ZIPCACHE_OBJ_TINY) {
                    cache->stats.puts_tiny++;
                } else {
                    cache->stats.puts_medium++;
                }
                cache->stats.memory_used += size;
                pthread_mutex_unlock(&cache->stats_lock);
                
                /* Invalidate any large version of this object */
                zipcache_invalidate_stale(cache, key, type);
                
                result = ZIPCACHE_OK;
                ZIPCACHE_DEBUG("✓ DRAM tier PUT successful");
            } else {
                result = ZIPCACHE_ERROR;
                ZIPCACHE_DEBUG("❌ DRAM tier PUT failed");
            }
            break;

        case ZIPCACHE_OBJ_LARGE:
            /* Route to Large Object tier */
            ZIPCACHE_DEBUG("→ Routing to Large Object tier");
            
            /* Write object directly to SSD storage */
            zipcache_large_obj_t descriptor;
            result = zipcache_write_large_object(cache, value, size, &descriptor);
            if (result != ZIPCACHE_OK) {
                ZIPCACHE_DEBUG("❌ Failed to write large object to SSD");
                break;
            }
            
            /* Insert descriptor into LO B+tree */
            struct object_pointer optr = { .lba = descriptor.lba, .size = descriptor.size, .checksum = descriptor.checksum };
            int lo_result = bplus_tree_lo_put(cache->bt_lo,
                                            zipcache_hash_key(key),
                                            optr);
            
            if (lo_result == 0) {
                /* Update statistics */
                pthread_mutex_lock(&cache->stats_lock);
                cache->stats.puts_large++;
                pthread_mutex_unlock(&cache->stats_lock);
                
                /* Insert tombstone in DRAM tier to invalidate small versions */
                bplus_tree_put(cache->bt_dram, 
                              zipcache_hash_key(key),
                              (long)ZIPCACHE_TOMBSTONE_MARKER);
                
                pthread_mutex_lock(&cache->stats_lock);
                cache->stats.tombstones++;
                pthread_mutex_unlock(&cache->stats_lock);
                
                result = ZIPCACHE_OK;
                ZIPCACHE_DEBUG("✓ Large Object tier PUT successful, tombstone inserted");
            } else {
                result = ZIPCACHE_ERROR;
                ZIPCACHE_DEBUG("❌ Large Object tier PUT failed");
            }
            break;

        default:
            result = ZIPCACHE_INVALID_SIZE;
            ZIPCACHE_ERROR("Unknown object type: %d", type);
    }

    return result;
}

/* ============================================================================
 * MAIN API IMPLEMENTATION
 * ============================================================================ */

zipcache_result_t zipcache_put(zipcache_t *cache, const char *key, 
                               const void *value, size_t size)
{
    if (!cache || !key || !value || size == 0) {
        return ZIPCACHE_ERROR;
    }

    if (size > ZIPCACHE_LARGE_THRESHOLD || strlen(key) > ZIPCACHE_MAX_KEY_SIZE) {
        return ZIPCACHE_INVALID_SIZE;
    }

    ZIPCACHE_DEBUG("PUT operation: key='%s', size=%zu", key, size);

    /* Step 1: Classify object by size */
    zipcache_obj_type_t type = zipcache_classify_object(cache, size);
    ZIPCACHE_DEBUG("Object classified as: %s", 
                   (type == ZIPCACHE_OBJ_TINY) ? "TINY" :
                   (type == ZIPCACHE_OBJ_MEDIUM) ? "MEDIUM" : "LARGE");

    /* Step 2: Acquire write lock for cache consistency */
    pthread_mutex_lock(&cache->cache_lock);

    /* Step 3: Route to appropriate tier */
    zipcache_result_t result = zipcache_route_put(cache, key, value, size, type);

    /* Step 4: Check if eviction is needed */
    if (result == ZIPCACHE_OK && zipcache_needs_eviction(cache)) {
        ZIPCACHE_DEBUG("Eviction needed, triggering background eviction");
        /* Eviction will be handled by background thread */
    }

    pthread_mutex_unlock(&cache->cache_lock);

    ZIPCACHE_DEBUG("PUT operation complete: result=%d", result);
    return result;
}

zipcache_result_t zipcache_get(zipcache_t *cache, const char *key,
                               void **value, size_t *size)
{
    if (!cache || !key || !value || !size) {
        return ZIPCACHE_ERROR;
    }

    if (strlen(key) > ZIPCACHE_MAX_KEY_SIZE) {
        return ZIPCACHE_INVALID_SIZE;
    }

    ZIPCACHE_DEBUG("GET operation: key='%s'", key);

    /* Acquire read lock */
    pthread_mutex_lock(&cache->cache_lock);

    /* Perform coordinated read across all tiers */
    zipcache_result_t result = zipcache_coordinated_read(cache, key, value, size);

    pthread_mutex_unlock(&cache->cache_lock);

    ZIPCACHE_DEBUG("GET operation complete: result=%d", result);
    return result;
}

/* ============================================================================
 * COORDINATED READ PATH WITH PROMOTION
 * ============================================================================ */

zipcache_result_t zipcache_coordinated_read(zipcache_t *cache, const char *key,
                                           void **value, size_t *size)
{
    uint32_t key_hash = zipcache_hash_key(key);
    
    ZIPCACHE_DEBUG("Coordinated read for key='%s' (hash=%u)", key, key_hash);

    /* Step 1: Search DRAM tier first (fastest) */
    ZIPCACHE_DEBUG("→ Searching DRAM tier...");
    long dram_result = bplus_tree_get(cache->bt_dram, key_hash);
    
    if (dram_result > 0) {
        /* Check if it's a tombstone */
        if (dram_result == (long)ZIPCACHE_TOMBSTONE_MARKER) {
            ZIPCACHE_DEBUG("Found tombstone in DRAM tier");
            pthread_mutex_lock(&cache->stats_lock);
            cache->stats.misses++;
            pthread_mutex_unlock(&cache->stats_lock);
            return ZIPCACHE_TOMBSTONE;
        }
        
        /* Valid object found in DRAM */
        *value = (void*)dram_result;
        *size = 0; /* Size would be stored separately in real implementation */
        
        pthread_mutex_lock(&cache->stats_lock);
        cache->stats.hits_dram++;
        pthread_mutex_unlock(&cache->stats_lock);
        
        ZIPCACHE_DEBUG("✓ Found in DRAM tier");
        return ZIPCACHE_OK;
    }

    /* Step 2: Search Large Object tier */
    ZIPCACHE_DEBUG("→ Searching Large Object tier...");
    struct object_pointer optr = bplus_tree_lo_get(cache->bt_lo, key_hash);
    if (object_pointer_is_valid(&optr)) {
        zipcache_large_obj_t descriptor = { .lba = optr.lba, .size = optr.size, .checksum = optr.checksum, .timestamp = 0 };
        /* Large object found, read from SSD storage */
        ZIPCACHE_DEBUG("Found large object descriptor (LBA=%lu, size=%u)", 
                       descriptor.lba, descriptor.size);
        
        zipcache_result_t read_result = zipcache_read_large_object(cache, &descriptor, 
                                                                   value, size);
        if (read_result == ZIPCACHE_OK) {
            pthread_mutex_lock(&cache->stats_lock);
            cache->stats.hits_lo++;
            pthread_mutex_unlock(&cache->stats_lock);
            
            ZIPCACHE_DEBUG("✓ Found in Large Object tier");
            return ZIPCACHE_OK;
        }
    }

    /* Step 3: Search SSD tier (slowest) */
    ZIPCACHE_DEBUG("→ Searching SSD tier...");
    long ssd_result = bplus_tree_ssd_get(cache->bt_ssd, key_hash);
    
    if (ssd_result > 0) {
        /* Object found in SSD tier */
        *value = (void*)ssd_result;
        *size = 0; /* Size would be retrieved from SSD metadata */
        
        pthread_mutex_lock(&cache->stats_lock);
        cache->stats.hits_ssd++;
        pthread_mutex_unlock(&cache->stats_lock);
        
        /* Promote to DRAM tier if it's tiny/medium (inclusive policy) */
        if (*size <= cache->medium_threshold) {
            ZIPCACHE_DEBUG("Promoting object from SSD to DRAM tier");
            zipcache_promote_object(cache, key, *value, *size);
            
            pthread_mutex_lock(&cache->stats_lock);
            cache->stats.promotions++;
            pthread_mutex_unlock(&cache->stats_lock);
        }
        
        ZIPCACHE_DEBUG("✓ Found in SSD tier");
        return ZIPCACHE_OK;
    }

    /* Step 4: Not found in any tier */
    pthread_mutex_lock(&cache->stats_lock);
    cache->stats.misses++;
    pthread_mutex_unlock(&cache->stats_lock);
    
    ZIPCACHE_DEBUG("❌ Not found in any tier");
    return ZIPCACHE_NOT_FOUND;
}

zipcache_result_t zipcache_promote_object(zipcache_t *cache, const char *key,
                                          const void *value, size_t size)
{
    ZIPCACHE_DEBUG("Promoting object: key='%s', size=%zu", key, size);
    
    /* Insert into DRAM tier */
    uint32_t key_hash = zipcache_hash_key(key);
    int result = bplus_tree_put(cache->bt_dram, key_hash, (long)value);
    
    if (result == 0) {
        pthread_mutex_lock(&cache->stats_lock);
        cache->stats.memory_used += size;
        pthread_mutex_unlock(&cache->stats_lock);
        
        ZIPCACHE_DEBUG("✓ Object promoted to DRAM tier");
        return ZIPCACHE_OK;
    } else {
        ZIPCACHE_DEBUG("❌ Failed to promote object to DRAM tier");
        return ZIPCACHE_ERROR;
    }
}

/* ============================================================================
 * UTILITY FUNCTIONS
 * ============================================================================ */

uint32_t zipcache_hash_key(const char *key)
{
    /* Simple FNV-1a hash function */
    uint32_t hash = 2166136261U;
    for (const char *p = key; *p; p++) {
        hash ^= (uint32_t)*p;
        hash *= 16777619U;
    }
    return hash;
}

int zipcache_safe_strncpy(char *dest, const char *src, size_t dest_size)
{
    if (!dest || !src || dest_size == 0) {
        return -1;
    }
    
    strncpy(dest, src, dest_size - 1);
    dest[dest_size - 1] = '\0';
    
    return (strlen(src) >= dest_size) ? -1 : 0;
}

uint64_t zipcache_timestamp(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;
}

size_t zipcache_get_dram_usage(zipcache_t *cache)
{
    pthread_mutex_lock(&cache->stats_lock);
    size_t usage = cache->stats.memory_used;
    pthread_mutex_unlock(&cache->stats_lock);
    return usage;
}

int zipcache_needs_eviction(zipcache_t *cache)
{
    size_t usage = zipcache_get_dram_usage(cache);
    double usage_ratio = (double)usage / cache->dram_capacity;
    return usage_ratio >= ZIPCACHE_EVICTION_THRESHOLD;
}

void zipcache_set_debug(int enable)
{
    g_zipcache_debug = enable;
    if (enable) {
        printf("[ZipCache] Debug logging enabled\n");
    }
}

void zipcache_print_stats(zipcache_t *cache)
{
    pthread_mutex_lock(&cache->stats_lock);
    zipcache_stats_t stats = cache->stats;
    pthread_mutex_unlock(&cache->stats_lock);

    uint64_t total_hits = stats.hits_dram + stats.hits_lo + stats.hits_ssd;
    uint64_t total_requests = total_hits + stats.misses;
    double hit_rate = total_requests > 0 ? (double)total_hits / total_requests * 100.0 : 0.0;
    double memory_usage = (double)stats.memory_used / stats.memory_capacity * 100.0;

    printf("ZipCache Statistics:\n");
    printf("==================\n");
    printf("Cache Hits:\n");
    printf("  DRAM tier: %lu (%.1f%%)\n", stats.hits_dram, 
           total_requests > 0 ? (double)stats.hits_dram / total_requests * 100.0 : 0.0);
    printf("  LO tier:   %lu (%.1f%%)\n", stats.hits_lo,
           total_requests > 0 ? (double)stats.hits_lo / total_requests * 100.0 : 0.0);
    printf("  SSD tier:  %lu (%.1f%%)\n", stats.hits_ssd,
           total_requests > 0 ? (double)stats.hits_ssd / total_requests * 100.0 : 0.0);
    printf("Cache Misses: %lu (%.1f%%)\n", stats.misses,
           total_requests > 0 ? (double)stats.misses / total_requests * 100.0 : 0.0);
    printf("Overall Hit Rate: %.2f%%\n", hit_rate);
    printf("\n");
    printf("Object Puts:\n");
    printf("  Tiny:   %lu\n", stats.puts_tiny);
    printf("  Medium: %lu\n", stats.puts_medium);
    printf("  Large:  %lu\n", stats.puts_large);
    printf("\n");
    printf("System Operations:\n");
    printf("  Evictions:  %lu\n", stats.evictions);
    printf("  Promotions: %lu\n", stats.promotions);
    printf("  Tombstones: %lu\n", stats.tombstones);
    printf("\n");
    printf("Memory Usage:\n");
    printf("  Used:     %zu bytes (%.1f MB)\n", stats.memory_used, 
           (double)stats.memory_used / (1024*1024));
    printf("  Capacity: %zu bytes (%.1f MB)\n", stats.memory_capacity,
           (double)stats.memory_capacity / (1024*1024));
    printf("  Usage:    %.2f%%\n", memory_usage);
    printf("\n");
}

/* ============================================================================
 * DATA CONSISTENCY AND INVALIDATION
 * ============================================================================ */

zipcache_result_t zipcache_invalidate_stale(zipcache_t *cache, const char *key,
                                           zipcache_obj_type_t new_type)
{
    uint32_t key_hash = zipcache_hash_key(key);
    
    ZIPCACHE_DEBUG("Invalidating stale data for key='%s', new_type=%d", key, new_type);

    /* Small PUT invalidates Large: Remove from LO tier */
    if (new_type == ZIPCACHE_OBJ_TINY || new_type == ZIPCACHE_OBJ_MEDIUM) {
        ZIPCACHE_DEBUG("→ Removing any large version from LO tier");
        
        /* Check if large version exists */
        struct object_pointer optr = bplus_tree_lo_get(cache->bt_lo, key_hash);
        if (object_pointer_is_valid(&optr)) {
            /* Large version exists, delete it */
            ZIPCACHE_DEBUG("Found large version (LBA=%lu), deleting", (unsigned long)optr.lba);
            
            /* Delete from LO B+tree */
            bplus_tree_lo_delete(cache->bt_lo, key_hash);
            
            /* TODO: Free the storage space on SSD (for production implementation) */
            
            ZIPCACHE_DEBUG("✓ Large version invalidated");
        }
    }

    return ZIPCACHE_OK;
}

zipcache_result_t zipcache_delete(zipcache_t *cache, const char *key)
{
    if (!cache || !key) {
        return ZIPCACHE_ERROR;
    }

    ZIPCACHE_DEBUG("DELETE operation: key='%s'", key);
    
    uint32_t key_hash = zipcache_hash_key(key);
    zipcache_result_t result = ZIPCACHE_NOT_FOUND;

    pthread_mutex_lock(&cache->cache_lock);

    /* Delete from DRAM tier */
    int dram_result = bplus_tree_put(cache->bt_dram, key_hash, 0);
    if (dram_result == 0) {
        ZIPCACHE_DEBUG("✓ Deleted from DRAM tier");
        result = ZIPCACHE_OK;
    }

    /* Delete from LO tier */
    int lo_result = bplus_tree_lo_delete(cache->bt_lo, key_hash);
    if (lo_result == 0) {
        ZIPCACHE_DEBUG("✓ Deleted from LO tier");
        result = ZIPCACHE_OK;
    }

    /* Delete from SSD tier */
    int ssd_result = bplus_tree_ssd_put(cache->bt_ssd, key_hash, 0);
    if (ssd_result == 0) {
        ZIPCACHE_DEBUG("✓ Deleted from SSD tier");
        result = ZIPCACHE_OK;
    }

    pthread_mutex_unlock(&cache->cache_lock);

    ZIPCACHE_DEBUG("DELETE operation complete: result=%d", result);
    return result;
}

/* ============================================================================
 * LARGE OBJECT SSD STORAGE
 * ============================================================================ */

zipcache_result_t zipcache_write_large_object(zipcache_t *cache, const void *value, 
                                              size_t size, zipcache_large_obj_t *descriptor)
{
    if (!cache || !value || !descriptor || size == 0) {
        return ZIPCACHE_ERROR;
    }

    ZIPCACHE_DEBUG("Writing large object: size=%zu", size);

    pthread_mutex_lock(&cache->ssd_lock);

    /* Align to 4KB boundaries for SSD efficiency */
    size_t aligned_size = ((size + 4095) / 4096) * 4096;
    
    /* Allocate aligned buffer and copy data */
    void *aligned_buffer = zipcache_aligned_alloc(aligned_size, 4096);
    if (!aligned_buffer) {
        pthread_mutex_unlock(&cache->ssd_lock);
        return ZIPCACHE_OUT_OF_MEMORY;
    }
    
    memcpy(aligned_buffer, value, size);
    memset((char*)aligned_buffer + size, 0, aligned_size - size); /* Zero padding */

    /* Write to SSD storage */
    off_t write_offset = cache->ssd_offset;
    ssize_t written = pwrite(cache->ssd_fd, aligned_buffer, aligned_size, write_offset);
    
    if (written != (ssize_t)aligned_size) {
        ZIPCACHE_ERROR("Failed to write large object: %s", strerror(errno));
        zipcache_aligned_free(aligned_buffer);
        pthread_mutex_unlock(&cache->ssd_lock);
        return ZIPCACHE_IO_ERROR;
    }

    /* Update SSD offset */
    cache->ssd_offset += aligned_size;
    
    /* Force write to disk */
    fsync(cache->ssd_fd);
    
    zipcache_aligned_free(aligned_buffer);
    pthread_mutex_unlock(&cache->ssd_lock);

    /* Fill descriptor */
    descriptor->lba = write_offset;
    descriptor->size = size;
    descriptor->checksum = zipcache_checksum(value, size);
    descriptor->timestamp = zipcache_timestamp();

    ZIPCACHE_DEBUG("✓ Large object written: LBA=%lu, size=%u, checksum=%u", 
                   descriptor->lba, descriptor->size, descriptor->checksum);

    return ZIPCACHE_OK;
}

zipcache_result_t zipcache_read_large_object(zipcache_t *cache, 
                                             const zipcache_large_obj_t *descriptor,
                                             void **value, size_t *size)
{
    if (!cache || !descriptor || !value || !size) {
        return ZIPCACHE_ERROR;
    }

    ZIPCACHE_DEBUG("Reading large object: LBA=%lu, size=%u", 
                   descriptor->lba, descriptor->size);

    /* Allocate buffer for object */
    void *buffer = malloc(descriptor->size);
    if (!buffer) {
        return ZIPCACHE_OUT_OF_MEMORY;
    }

    /* Read from SSD storage */
    pthread_mutex_lock(&cache->ssd_lock);
    ssize_t read_bytes = pread(cache->ssd_fd, buffer, descriptor->size, descriptor->lba);
    pthread_mutex_unlock(&cache->ssd_lock);

    if (read_bytes != (ssize_t)descriptor->size) {
        ZIPCACHE_ERROR("Failed to read large object: %s", strerror(errno));
        free(buffer);
        return ZIPCACHE_IO_ERROR;
    }

    /* Verify data integrity */
    uint32_t checksum = zipcache_checksum(buffer, descriptor->size);
    if (checksum != descriptor->checksum) {
        ZIPCACHE_ERROR("Large object checksum mismatch: expected=%u, got=%u", 
                       descriptor->checksum, checksum);
        free(buffer);
        return ZIPCACHE_IO_ERROR;
    }

    *value = buffer;
    *size = descriptor->size;

    ZIPCACHE_DEBUG("✓ Large object read successfully");
    return ZIPCACHE_OK;
}

uint32_t zipcache_checksum(const void *data, size_t size)
{
    /* Simple CRC32-like checksum */
    uint32_t checksum = 0;
    const uint8_t *bytes = (const uint8_t*)data;
    
    for (size_t i = 0; i < size; i++) {
        checksum = (checksum << 1) ^ bytes[i];
        checksum ^= (checksum >> 16);
    }
    
    return checksum;
}

void *zipcache_aligned_alloc(size_t size, size_t alignment)
{
    void *ptr = NULL;
    int result = posix_memalign(&ptr, alignment, size);
    return (result == 0) ? ptr : NULL;
}

void zipcache_aligned_free(void *ptr)
{
    free(ptr);
}

/* ============================================================================
 * BACKGROUND EVICTION MECHANISM
 * ============================================================================ */

void *zipcache_eviction_thread(void *arg)
{
    zipcache_t *cache = (zipcache_t*)arg;
    
    ZIPCACHE_DEBUG("Eviction thread started");

    while (!cache->shutdown_flag) {
        /* Check if eviction is needed */
        if (zipcache_needs_eviction(cache)) {
            ZIPCACHE_DEBUG("Starting eviction cycle");
            
            /* Calculate target bytes to evict (10% of capacity) */
            size_t target_bytes = cache->dram_capacity * 0.1;
            
            pthread_mutex_lock(&cache->cache_lock);
            zipcache_result_t evict_result = zipcache_evict_cold_pages(cache, target_bytes);
            pthread_mutex_unlock(&cache->cache_lock);
            
            if (evict_result == ZIPCACHE_OK) {
                pthread_mutex_lock(&cache->stats_lock);
                cache->stats.evictions++;
                pthread_mutex_unlock(&cache->stats_lock);
                
                ZIPCACHE_DEBUG("✓ Eviction cycle completed");
            } else {
                ZIPCACHE_DEBUG("❌ Eviction cycle failed");
            }
        }

        /* Sleep for 1 second before next check */
        sleep(1);
    }

    ZIPCACHE_DEBUG("Eviction thread stopped");
    return NULL;
}

zipcache_result_t zipcache_evict_cold_pages(zipcache_t *cache, size_t target_bytes)
{
    ZIPCACHE_DEBUG("Evicting cold pages: target=%zu bytes", target_bytes);
    
    size_t evicted_bytes = 0;
    uint32_t pages_scanned = 0;
    uint32_t max_scan = cache->eviction.total_pages * 2; /* Prevent infinite loops */

    pthread_mutex_lock(&cache->eviction.lock);

    while (evicted_bytes < target_bytes && pages_scanned < max_scan) {
        uint32_t page_idx = cache->eviction.clock_hand;
        
        /* Check access bit using second-chance algorithm */
        if (cache->eviction.access_bits[page_idx]) {
            /* Page was accessed, give it a second chance */
            cache->eviction.access_bits[page_idx] = 0;
        } else {
            /* Page is cold, evict it */
            ZIPCACHE_DEBUG("Evicting page %u", page_idx);
            
            /* TODO: In a real implementation, we would:
             * 1. Read the DRAM page data
             * 2. Decompress any medium objects  
             * 3. Merge objects into SSD super-leaf pages
             * 4. Free the DRAM page
             * 
             * For now, we simulate by estimating page size
             */
            size_t page_size = 4096; /* Estimate 4KB per page */
            evicted_bytes += page_size;
            
            /* Update memory usage */
            pthread_mutex_lock(&cache->stats_lock);
            if (cache->stats.memory_used >= page_size) {
                cache->stats.memory_used -= page_size;
            }
            pthread_mutex_unlock(&cache->stats_lock);
        }

        /* Advance clock hand */
        cache->eviction.clock_hand = (cache->eviction.clock_hand + 1) % cache->eviction.total_pages;
        pages_scanned++;
    }

    pthread_mutex_unlock(&cache->eviction.lock);

    ZIPCACHE_DEBUG("✓ Evicted %zu bytes across %u pages", evicted_bytes, pages_scanned);
    return ZIPCACHE_OK;
}

/* ============================================================================
 * STATISTICS AND MONITORING
 * ============================================================================ */

void zipcache_get_stats(zipcache_t *cache, zipcache_stats_t *stats)
{
    if (!cache || !stats) return;

    pthread_mutex_lock(&cache->stats_lock);
    *stats = cache->stats;
    pthread_mutex_unlock(&cache->stats_lock);
}

void zipcache_reset_stats(zipcache_t *cache)
{
    if (!cache) return;

    pthread_mutex_lock(&cache->stats_lock);
    memset(&cache->stats, 0, sizeof(cache->stats));
    cache->stats.memory_capacity = cache->dram_capacity;
    pthread_mutex_unlock(&cache->stats_lock);
    
    ZIPCACHE_DEBUG("Statistics reset");
}

void zipcache_dump_state(zipcache_t *cache)
{
    if (!cache) return;

    printf("ZipCache State Dump:\n");
    printf("===================\n");
    printf("Configuration:\n");
    printf("  Tiny threshold:   %zu bytes\n", cache->tiny_threshold);
    printf("  Medium threshold: %zu bytes\n", cache->medium_threshold);
    printf("  DRAM capacity:    %zu bytes (%.1f MB)\n", cache->dram_capacity,
           (double)cache->dram_capacity / (1024*1024));
    printf("  SSD path:         %s\n", cache->ssd_path);
    printf("\n");

    printf("Eviction State:\n");
    printf("  Total pages:  %u\n", cache->eviction.total_pages);
    printf("  Clock hand:   %u\n", cache->eviction.clock_hand);
    printf("  SSD offset:   %lu\n", cache->ssd_offset);
    printf("\n");

    zipcache_print_stats(cache);
}

int zipcache_validate_consistency(zipcache_t *cache)
{
    if (!cache) return 0;

    /* Basic consistency checks */
    int consistent = 1;

    /* Check that memory usage doesn't exceed capacity */
    pthread_mutex_lock(&cache->stats_lock);
    if (cache->stats.memory_used > cache->stats.memory_capacity) {
        ZIPCACHE_ERROR("Memory usage (%zu) exceeds capacity (%zu)", 
                       cache->stats.memory_used, cache->stats.memory_capacity);
        consistent = 0;
    }
    pthread_mutex_unlock(&cache->stats_lock);

    /* Check that clock hand is within bounds */
    if (cache->eviction.clock_hand >= cache->eviction.total_pages) {
        ZIPCACHE_ERROR("Clock hand (%u) out of bounds (max %u)", 
                       cache->eviction.clock_hand, cache->eviction.total_pages);
        consistent = 0;
    }

    /* Check file descriptor validity */
    if (cache->ssd_fd < 0) {
        ZIPCACHE_ERROR("Invalid SSD file descriptor: %d", cache->ssd_fd);
        consistent = 0;
    }

    return consistent;
}
