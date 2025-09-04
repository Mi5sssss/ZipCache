/* File cleaned: removed stray characters */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include "bplustree_compressed.h"

// Node type constants (fallbacks if not provided by headers)
#ifndef BPLUS_TREE_LEAF
#define BPLUS_TREE_LEAF 0
#endif
#ifndef BPLUS_TREE_NON_LEAF
#define BPLUS_TREE_NON_LEAF 1
#endif

// Forward declarations
static int compress_leaf_node_with_metadata_ex(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf);
static int decompress_leaf_node_with_metadata_ex(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf);
static int decompress_leaf_partial_lz4(struct bplus_leaf *leaf, int metadata_idx, int sub_page_index, int subcap);
static int add_leaf_metadata(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf);
static int find_leaf_metadata_index(struct bplus_leaf *leaf);
void *background_compression_thread(void *arg);
void cleanup_qpl(struct bplus_tree_compressed *ct_tree);
int init_qpl(struct bplus_tree_compressed *ct_tree);
static int hash_key_to_sub_page(key_t key, int num_sub_pages);


struct bplus_tree_compressed *bplus_tree_compressed_init(int order, int entries)
{
    // Initialize with a default configuration
    struct compression_config config = bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    return bplus_tree_compressed_init_with_config(order, entries, &config);
}

void bplus_tree_compressed_deinit(struct bplus_tree_compressed *ct_tree)
{
    if (ct_tree == NULL) {
        return;
    }
    
    if (ct_tree->initialized) {
        // Signal shutdown to background thread
        if (ct_tree->config.enable_lazy_compression) {
            ct_tree->shutdown_flag = 1;
            pthread_mutex_lock(&ct_tree->work_queue->queue_lock);
            pthread_cond_signal(&ct_tree->work_queue->queue_cond);
            pthread_mutex_unlock(&ct_tree->work_queue->queue_lock);
            pthread_join(ct_tree->background_thread, NULL);
        }
        
        // Flush all pending buffers
        bplus_tree_compressed_flush_all_buffers(ct_tree);
        
        pthread_rwlock_wrlock(&ct_tree->rwlock);
        
        cleanup_qpl(ct_tree);
        
        if (ct_tree->work_queue != NULL) {
            struct flush_work_item *item = ct_tree->work_queue->head;
            while (item != NULL) {
                struct flush_work_item *next = item->next;
                free(item);
                item = next;
            }
            pthread_mutex_destroy(&ct_tree->work_queue->queue_lock);
            pthread_cond_destroy(&ct_tree->work_queue->queue_cond);
            free(ct_tree->work_queue);
        }
        
        if (ct_tree->tree != NULL) {
            bplus_tree_deinit(ct_tree->tree);
            ct_tree->tree = NULL;
        }
        
        pthread_rwlock_unlock(&ct_tree->rwlock);
        pthread_rwlock_destroy(&ct_tree->rwlock);
        ct_tree->initialized = 0;
    }
    
    free(ct_tree);
}

// Dynamic table to store compression metadata for leaf nodes
struct leaf_meta_entry {
    struct bplus_leaf *leaf;
    leaf_layout_t layout;
    int is_compressed;
    int original_entries;
    int compressed_size;
    char *compressed_data;
    size_t uncompressed_bytes;
    size_t compressed_bytes;
    struct writing_buffer *buffer; /* per-leaf write buffer */

    // -- Hashed Layout Specific Fields --
    int num_sub_pages; // e.g., 16
    struct subpage_index_entry *subpage_index; // per-sub-page compressed block index
    int subpage_index_count;                   // equals num_sub_pages when allocated

    // -- Append Layout Specific Fields --
    size_t data_end_offset; // Tracks end of the log
};

static struct leaf_meta_entry *leaf_metadata = NULL;
static int metadata_count = 0;
static int metadata_cap = 0;
static pthread_mutex_t g_meta_lock = PTHREAD_MUTEX_INITIALIZER;


// ==================== LAZY COMPRESSION FUNCTIONS ====================

int search_buffer(struct writing_buffer *buffer, key_t key)
{
    if (buffer == NULL || buffer->count == 0) {
        return -1;
    }
    
    for (int i = 0; i < buffer->count; i++) {
        if (buffer->entries[i].key == key) {
            return i;
        }
    }
    
    return -1;
}

int add_to_buffer(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf,
                  key_t key, int value, char operation)
{
    if (ct_tree == NULL || leaf == NULL) {
        return -1;
    }
    
    struct writing_buffer *buffer = NULL;
    
    int metadata_idx = find_leaf_metadata_index(leaf);
    if (metadata_idx == -1) {
        metadata_idx = add_leaf_metadata(ct_tree, leaf);
        if (metadata_idx == -1) {
            return -1;
        }
    }
    
    if (leaf_metadata[metadata_idx].leaf != NULL) {
        if (leaf_metadata[metadata_idx].buffer == NULL) {
            pthread_mutex_lock(&g_meta_lock);
            if (leaf_metadata[metadata_idx].buffer == NULL) {
                leaf_metadata[metadata_idx].buffer = calloc(1, sizeof(struct writing_buffer));
                if (leaf_metadata[metadata_idx].buffer) {
                    pthread_mutex_init(&leaf_metadata[metadata_idx].buffer->buffer_lock, NULL);
                }
            }
            pthread_mutex_unlock(&g_meta_lock);
        }
        buffer = leaf_metadata[metadata_idx].buffer;
    }
    
    if (buffer == NULL) {
        return -1;
    }
    
    pthread_mutex_lock(&buffer->buffer_lock);
    
    if (buffer->count >= MAX_BUFFER_ENTRIES) {
        pthread_mutex_unlock(&buffer->buffer_lock);
        return -1; 
    }
    
    int existing_idx = -1;
    for (int i = 0; i < buffer->count; i++) {
        if (buffer->entries[i].key == key) {
            existing_idx = i;
            break;
        }
    }
    
    if (existing_idx != -1) {
        buffer->entries[existing_idx].value = value;
        buffer->entries[existing_idx].operation = operation;
    } else {
        buffer->entries[buffer->count].key = key;
        buffer->entries[buffer->count].value = value;
        buffer->entries[buffer->count].operation = operation;
        buffer->count++;
    }
    
    buffer->dirty = 1;
    
    int needs_flush = (buffer->count >= ct_tree->buffer_flush_threshold);
    
    pthread_mutex_unlock(&buffer->buffer_lock);
    
    if (needs_flush) {
        struct flush_work_item *work_item = malloc(sizeof(struct flush_work_item));
        if (work_item != NULL) {
            work_item->leaf = leaf;
            work_item->next = NULL;
            
            pthread_mutex_lock(&ct_tree->work_queue->queue_lock);
            
            if (ct_tree->work_queue->tail == NULL) {
                ct_tree->work_queue->head = work_item;
                ct_tree->work_queue->tail = work_item;
            } else {
                ct_tree->work_queue->tail->next = work_item;
                ct_tree->work_queue->tail = work_item;
            }
            ct_tree->work_queue->count++;
            
            pthread_cond_signal(&ct_tree->work_queue->queue_cond);
            pthread_mutex_unlock(&ct_tree->work_queue->queue_lock);
        }
    }
    
    return 0;
}

void *background_compression_thread(void *arg)
{
    struct bplus_tree_compressed *ct_tree = (struct bplus_tree_compressed *)arg;
    
    while (!ct_tree->shutdown_flag) {
        pthread_mutex_lock(&ct_tree->work_queue->queue_lock);
        
        while (ct_tree->work_queue->count == 0 && !ct_tree->shutdown_flag) {
            pthread_cond_wait(&ct_tree->work_queue->queue_cond, &ct_tree->work_queue->queue_lock);
        }
        
        if (ct_tree->shutdown_flag) {
            pthread_mutex_unlock(&ct_tree->work_queue->queue_lock);
            break;
        }
        
        struct flush_work_item *work_item = ct_tree->work_queue->head;
        if (work_item != NULL) {
            ct_tree->work_queue->head = work_item->next;
            if (ct_tree->work_queue->head == NULL) {
                ct_tree->work_queue->tail = NULL;
            }
            ct_tree->work_queue->count--;
        }
        
        pthread_mutex_unlock(&ct_tree->work_queue->queue_lock);
        
        if (work_item != NULL) {
            flush_buffer_to_leaf(ct_tree, work_item->leaf);
            ct_tree->background_flushes++;
            free(work_item);
        }
    }
    
    return NULL;
}

// =================== LAYOUT-SPECIFIC FLUSH HELPERS ===================

static int flush_to_hashed_leaf(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf, struct writing_buffer *buffer, int metadata_idx)
{
    // Decompress the entire leaf before modification
    if (leaf_metadata[metadata_idx].is_compressed) {
        if (decompress_leaf_node_with_metadata_ex(ct_tree, leaf) != 0) {
            return -1;
        }
    }

    int num_sub_pages = leaf_metadata[metadata_idx].num_sub_pages;
    if (num_sub_pages <= 0) return -1;
    int sub_page_capacity = ct_tree->tree->entries / num_sub_pages;

    // Process each operation from the buffer
    for (int i = 0; i < buffer->count; i++) {
        key_t key = buffer->entries[i].key;
        value_t value = buffer->entries[i].value;
        
        int sub_page_index = hash_key_to_sub_page(key, num_sub_pages);
        int start_index = sub_page_index * sub_page_capacity;
        int end_index = start_index + sub_page_capacity;
        
        // Find an empty slot in the sub-page or update existing key
        int target_idx = -1;
        int empty_slot = -1;
        for (int j = start_index; j < end_index; j++) {
            if (leaf->key[j] == key) {
                target_idx = j;
                break;
            }
            if (leaf->key[j] == 0 && empty_slot == -1) { // Assuming 0 is an invalid key
                empty_slot = j;
            }
        }

        if (target_idx != -1) {
            // Key exists, update it
            leaf->data[target_idx] = value;
        } else if (empty_slot != -1) {
            // Found an empty slot, insert new key-value
            leaf->key[empty_slot] = key;
            leaf->data[empty_slot] = value;
            if (empty_slot >= leaf->entries) {
                leaf->entries = empty_slot + 1;
            }
        } else {
            // Sub-page is full, this should trigger a split.
            // The main `bplus_tree_put` handles splitting, so we can call it.
            // This is a simplification; a more robust implementation might need
            // to handle the split more directly here.
            bplus_tree_put(ct_tree->tree, key, value);
        }
    }

    // Re-compress the leaf after modifications
    return compress_leaf_node_with_metadata_ex(ct_tree, leaf);
}

/* Append-log layout helper disabled under unified hashed layout focus. */
#if 0
static int flush_to_append_leaf(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf, struct writing_buffer *buffer, int metadata_idx)
{
    return -1;
}
#endif

/* Append-log compaction disabled under unified hashed layout focus. */
#if 0
static void compact_append_leaf(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf)
{
}
#endif


int flush_buffer_to_leaf(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf)
{
    if (leaf == NULL) {
        return -1;
    }
    
    int metadata_idx = find_leaf_metadata_index(leaf);
    if (metadata_idx == -1) {
        return 0; 
    }
    
    struct writing_buffer *buffer = leaf_metadata[metadata_idx].buffer;
    if (buffer == NULL || buffer->count == 0 || !buffer->dirty) {
        return 0; 
    }
    
    pthread_mutex_lock(&buffer->buffer_lock);

    int result = -1;
    /* Unified hashed layout only */
    result = flush_to_hashed_leaf(ct_tree, leaf, buffer, metadata_idx);

    if (result == 0) {
        buffer->count = 0;
        buffer->dirty = 0;
    }
    
    pthread_mutex_unlock(&buffer->buffer_lock);
    
    return result;
}

int bplus_tree_compressed_flush_all_buffers(struct bplus_tree_compressed *ct_tree)
{
    if (ct_tree == NULL) {
        return -1;
    }
    
    for (int i = 0; i < metadata_count; i++) {
        if (leaf_metadata[i].leaf != NULL) {
            flush_buffer_to_leaf(ct_tree, leaf_metadata[i].leaf);
        }
    }
    
    return 0;
}

static int find_leaf_metadata_index(struct bplus_leaf *leaf)
{
    int idx = -1;
    pthread_mutex_lock(&g_meta_lock);
    for (int i = 0; i < metadata_count; i++) {
        if (leaf_metadata[i].leaf == leaf) { idx = i; break; }
    }
    pthread_mutex_unlock(&g_meta_lock);
    return idx;
}

static int add_leaf_metadata(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf)
{
    pthread_mutex_lock(&g_meta_lock);
    if (metadata_count == metadata_cap) {
        int new_cap = (metadata_cap == 0) ? 128 : metadata_cap * 2;
        struct leaf_meta_entry *new_arr = realloc(leaf_metadata, new_cap * sizeof(*new_arr));
        if (!new_arr) { pthread_mutex_unlock(&g_meta_lock); return -1; }
        if (new_cap > metadata_cap) {
            memset(new_arr + metadata_cap, 0, (new_cap - metadata_cap) * sizeof(*new_arr));
        }
        leaf_metadata = new_arr;
        metadata_cap = new_cap;
    }
    int idx = metadata_count++;
    leaf_metadata[idx].leaf = leaf;
    leaf_metadata[idx].is_compressed = 0;
    leaf_metadata[idx].original_entries = 0;
    leaf_metadata[idx].compressed_size = 0;
    leaf_metadata[idx].compressed_data = malloc(MAX_COMPRESSED_SIZE);
    leaf_metadata[idx].uncompressed_bytes = 0;
    leaf_metadata[idx].compressed_bytes = 0;
    leaf_metadata[idx].buffer = NULL;

    leaf_metadata[idx].layout = ct_tree->config.default_layout;
    leaf_metadata[idx].num_sub_pages = ct_tree->config.default_sub_pages;
    leaf_metadata[idx].data_end_offset = 0;
    leaf_metadata[idx].subpage_index = NULL;
    leaf_metadata[idx].subpage_index_count = 0;

    if (leaf_metadata[idx].compressed_data == NULL) {
        metadata_count--;
        pthread_mutex_unlock(&g_meta_lock);
        return -1;
    }

    /* allocate subpage index when hashed layout configured */
    if (leaf_metadata[idx].num_sub_pages > 0) {
        leaf_metadata[idx].subpage_index = calloc((size_t)leaf_metadata[idx].num_sub_pages,
                                                  sizeof(struct subpage_index_entry));
        if (leaf_metadata[idx].subpage_index) {
            leaf_metadata[idx].subpage_index_count = leaf_metadata[idx].num_sub_pages;
        } else {
            /* allocation failure is non-fatal; leave index NULL */
            leaf_metadata[idx].subpage_index_count = 0;
        }
    }
    pthread_mutex_unlock(&g_meta_lock);
    return idx;
}

static void remove_leaf_metadata(struct bplus_leaf *leaf)
{
    pthread_mutex_lock(&g_meta_lock);
    for (int i = 0; i < metadata_count; i++) {
        if (leaf_metadata[i].leaf == leaf) {
            if (leaf_metadata[i].compressed_data) free(leaf_metadata[i].compressed_data);
            if (leaf_metadata[i].buffer) {
                pthread_mutex_destroy(&leaf_metadata[i].buffer->buffer_lock);
                free(leaf_metadata[i].buffer);
            }
            if (leaf_metadata[i].subpage_index) {
                free(leaf_metadata[i].subpage_index);
            }
            if (i < metadata_count - 1) {
                leaf_metadata[i] = leaf_metadata[metadata_count - 1];
            }
            metadata_count--;
            break;
        }
    }
    pthread_mutex_unlock(&g_meta_lock);
}

static int compress_leaf_node_with_metadata_ex(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf)
{
    if (leaf == NULL || leaf->entries == 0) {
        return 0;
    }

    int idx = find_leaf_metadata_index(leaf);
    if (idx == -1) {
        idx = add_leaf_metadata(ct_tree, leaf);
        if (idx == -1) return -1;
    }

    if (leaf_metadata[idx].is_compressed) {
        return 0;
    }

        /* Unified hashed layout: compress per-sub-page and build index */
        {
            int num_sub_pages = leaf_metadata[idx].num_sub_pages;
            if (num_sub_pages <= 0) {
                return 0; /* nothing to do */
            }

            int capacity = ct_tree->tree->entries;
            int subcap = capacity / num_sub_pages;
            size_t per_sub_uncompressed = (size_t)subcap * (sizeof(key_t) + sizeof(value_t));

            if (leaf_metadata[idx].subpage_index == NULL || leaf_metadata[idx].subpage_index_count < num_sub_pages) {
                leaf_metadata[idx].subpage_index = calloc((size_t)num_sub_pages, sizeof(struct subpage_index_entry));
                if (!leaf_metadata[idx].subpage_index) return -1;
                leaf_metadata[idx].subpage_index_count = num_sub_pages;
            }

            size_t offset = 0;
            char *tmp = (char*)malloc(per_sub_uncompressed);
            if (!tmp) return -1;

            /* Choose compression algorithm based on user configuration */
            if (ct_tree->config.algo == COMPRESS_LZ4) {
                /* LZ4 compression path */
                for (int sp = 0; sp < num_sub_pages; sp++) {
                    int start = sp * subcap;
                    memcpy(tmp, &leaf->key[start], subcap * sizeof(key_t));
                    memcpy(tmp + subcap * sizeof(key_t), &leaf->data[start], subcap * sizeof(value_t));
                    int max_out = MAX_COMPRESSED_SIZE - (int)offset;
                    if (max_out <= 0) { free(tmp); return -1; }
                    int out_len = LZ4_compress_default(tmp, leaf_metadata[idx].compressed_data + offset,
                                                       (int)per_sub_uncompressed, max_out);
                    if (out_len <= 0) { free(tmp); return 0; }
                    leaf_metadata[idx].subpage_index[sp].offset = (uint32_t)offset;
                    leaf_metadata[idx].subpage_index[sp].length = (uint32_t)out_len;
                    leaf_metadata[idx].subpage_index[sp].uncompressed_bytes = (uint32_t)per_sub_uncompressed;
                    offset += (size_t)out_len;
                    if (offset > MAX_COMPRESSED_SIZE) { free(tmp); return 0; }
                }
            } else if (ct_tree->config.algo == COMPRESS_QPL) {
                /* QPL compression path */
                if (!ct_tree->qpl_job_ptr) {
                    free(tmp);
                    return -1; /* QPL not initialized */
                }
                
                pthread_mutex_lock(&ct_tree->qpl_lock);
                
                for (int sp = 0; sp < num_sub_pages; sp++) {
                    int start = sp * subcap;
                    memcpy(tmp, &leaf->key[start], subcap * sizeof(key_t));
                    memcpy(tmp + subcap * sizeof(key_t), &leaf->data[start], subcap * sizeof(value_t));
                    
                    size_t max_out = MAX_COMPRESSED_SIZE - offset;
                    if (max_out <= 0) { 
                        pthread_mutex_unlock(&ct_tree->qpl_lock);
                        free(tmp); 
                        return -1; 
                    }
                    
                    /* Configure QPL job for compression */
                    ct_tree->qpl_job_ptr->op = qpl_op_compress;
                    ct_tree->qpl_job_ptr->next_in_ptr = (uint8_t*)tmp;
                    ct_tree->qpl_job_ptr->available_in = (uint32_t)per_sub_uncompressed;
                    ct_tree->qpl_job_ptr->next_out_ptr = (uint8_t*)(leaf_metadata[idx].compressed_data + offset);
                    ct_tree->qpl_job_ptr->available_out = (uint32_t)max_out;
                    ct_tree->qpl_job_ptr->level = ct_tree->config.compression_level;
                    ct_tree->qpl_job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
                    
                    /* Execute QPL compression */
                    qpl_status status = qpl_execute_job(ct_tree->qpl_job_ptr);
                    if (status != QPL_STS_OK) {
                        pthread_mutex_unlock(&ct_tree->qpl_lock);
                        free(tmp);
                        return 0; /* Compression failed, treat as incompressible */
                    }
                    
                    uint32_t out_len = ct_tree->qpl_job_ptr->total_out;
                    if (out_len == 0) {
                        pthread_mutex_unlock(&ct_tree->qpl_lock);
                        free(tmp);
                        return 0; /* No compression achieved */
                    }
                    
                    leaf_metadata[idx].subpage_index[sp].offset = (uint32_t)offset;
                    leaf_metadata[idx].subpage_index[sp].length = out_len;
                    leaf_metadata[idx].subpage_index[sp].uncompressed_bytes = (uint32_t)per_sub_uncompressed;
                    offset += (size_t)out_len;
                    if (offset > MAX_COMPRESSED_SIZE) { 
                        pthread_mutex_unlock(&ct_tree->qpl_lock);
                        free(tmp); 
                        return 0; 
                    }
                }
                
                pthread_mutex_unlock(&ct_tree->qpl_lock);
            } else {
                /* Unknown algorithm */
                free(tmp);
                return -1;
            }

            free(tmp);
            leaf_metadata[idx].compressed_size = (int)offset;
            leaf_metadata[idx].compressed_bytes = offset;
            leaf_metadata[idx].uncompressed_bytes = (size_t)num_sub_pages * per_sub_uncompressed;
            leaf_metadata[idx].original_entries = leaf->entries;
            leaf_metadata[idx].is_compressed = 1;
            return 0;
        }
}

static int decompress_leaf_partial_lz4(struct bplus_leaf *leaf, int metadata_idx, int sub_page_index, int subcap)
{
    if (leaf_metadata[metadata_idx].subpage_index == NULL) return -1;
    if (sub_page_index < 0 || sub_page_index >= leaf_metadata[metadata_idx].subpage_index_count) return -1;
    struct subpage_index_entry *ent = &leaf_metadata[metadata_idx].subpage_index[sub_page_index];
    size_t tmp_size = (size_t)subcap * (sizeof(key_t) + sizeof(value_t));
    char *tmp = (char*)malloc(tmp_size);
    if (!tmp) return -1;
    int out = LZ4_decompress_safe(leaf_metadata[metadata_idx].compressed_data + ent->offset,
                                  tmp,
                                  ent->length,
                                  (int)tmp_size);
    if (out != (int)tmp_size) { free(tmp); return -1; }
    int start = sub_page_index * subcap;
    memcpy(&leaf->key[start], tmp, subcap * sizeof(key_t));
    memcpy(&leaf->data[start], tmp + subcap * sizeof(key_t), subcap * sizeof(value_t));
    free(tmp);
    return 0;
}

static int decompress_leaf_partial_qpl(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf, int metadata_idx, int sub_page_index, int subcap)
{
    if (leaf_metadata[metadata_idx].subpage_index == NULL) return -1;
    if (sub_page_index < 0 || sub_page_index >= leaf_metadata[metadata_idx].subpage_index_count) return -1;
    if (!ct_tree->qpl_job_ptr) return -1; /* QPL not initialized */
    
    struct subpage_index_entry *ent = &leaf_metadata[metadata_idx].subpage_index[sub_page_index];
    size_t tmp_size = (size_t)subcap * (sizeof(key_t) + sizeof(value_t));
    char *tmp = (char*)malloc(tmp_size);
    if (!tmp) return -1;
    
    pthread_mutex_lock(&ct_tree->qpl_lock);
    
    /* Configure QPL job for decompression */
    ct_tree->qpl_job_ptr->op = qpl_op_decompress;
    ct_tree->qpl_job_ptr->next_in_ptr = (uint8_t*)(leaf_metadata[metadata_idx].compressed_data + ent->offset);
    ct_tree->qpl_job_ptr->available_in = ent->length;
    ct_tree->qpl_job_ptr->next_out_ptr = (uint8_t*)tmp;
    ct_tree->qpl_job_ptr->available_out = (uint32_t)tmp_size;
    ct_tree->qpl_job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
    
    /* Execute QPL decompression */
    qpl_status status = qpl_execute_job(ct_tree->qpl_job_ptr);
    if (status != QPL_STS_OK || ct_tree->qpl_job_ptr->total_out != tmp_size) {
        pthread_mutex_unlock(&ct_tree->qpl_lock);
        free(tmp);
        return -1;
    }
    
    pthread_mutex_unlock(&ct_tree->qpl_lock);
    
    int start = sub_page_index * subcap;
    memcpy(&leaf->key[start], tmp, subcap * sizeof(key_t));
    memcpy(&leaf->data[start], tmp + subcap * sizeof(key_t), subcap * sizeof(value_t));
    free(tmp);
    return 0;
}

static int decompress_leaf_node_with_metadata_ex(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf)
{
    if (leaf == NULL) {
        return 0;
    }
    
    int idx = find_leaf_metadata_index(leaf);
    if (idx == -1 || !leaf_metadata[idx].is_compressed || leaf_metadata[idx].compressed_size <= 0) {
        return 0;
    }

    /* Unified hashed layout: decompress each sub-page block using the correct algorithm */
    int num_sub_pages = leaf_metadata[idx].num_sub_pages;
    int subcap = ct_tree->tree->entries / num_sub_pages;
    
    if (ct_tree->config.algo == COMPRESS_LZ4) {
        /* LZ4 decompression path */
        for (int sp = 0; sp < num_sub_pages; sp++) {
            if (decompress_leaf_partial_lz4(leaf, idx, sp, subcap) != 0) {
                return -1;
            }
        }
    } else if (ct_tree->config.algo == COMPRESS_QPL) {
        /* QPL decompression path */
        for (int sp = 0; sp < num_sub_pages; sp++) {
            if (decompress_leaf_partial_qpl(ct_tree, leaf, idx, sp, subcap) != 0) {
                return -1;
            }
        }
    } else {
        /* Unknown algorithm */
        return -1;
    }
    
    leaf->entries = leaf_metadata[idx].original_entries;
    leaf_metadata[idx].is_compressed = 0;
    
    return 0;
}

static struct bplus_leaf* find_leaf_for_key(struct bplus_tree *tree, key_t key)
{
    struct bplus_node *node = tree->root;
    
    while (node != NULL) {
        if (node->type == BPLUS_TREE_LEAF) {
            return (struct bplus_leaf*)node;
        } else {
            struct bplus_non_leaf *nln = (struct bplus_non_leaf*)node;
            int i;
            for (i = 0; i < nln->children - 1; i++) {
                if (key < nln->key[i]) {
                    node = nln->sub_ptr[i];
                    break;
                }
            }
            if (i == nln->children - 1) {
                node = nln->sub_ptr[i];
            }
        }
    }
    
    return NULL;
}

// ==================== LAYOUT-SPECIFIC GET HELPERS ====================

static int hash_key_to_sub_page(key_t key, int num_sub_pages) {
    if (num_sub_pages == 0) return 0;
    return key % num_sub_pages;
}

static int get_from_hashed_leaf(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf, key_t key)
{
    int metadata_idx = find_leaf_metadata_index(leaf);
    if (metadata_idx == -1) return -1;

    int num_sub_pages = leaf_metadata[metadata_idx].num_sub_pages;
    if (num_sub_pages <= 0) return -1;

    int sub_page_index = hash_key_to_sub_page(key, num_sub_pages);
    int sub_page_capacity = ct_tree->tree->entries / num_sub_pages;

    if (leaf_metadata[metadata_idx].is_compressed) {
        if (decompress_leaf_partial_lz4(leaf, metadata_idx, sub_page_index, sub_page_capacity) != 0) {
            return -1;
        }
    }

    int start_index = sub_page_index * sub_page_capacity;
    int end_index = start_index + sub_page_capacity;

    for (int i = start_index; i < end_index; i++) {
        if (leaf->key[i] == key) {
            return leaf->data[i];
        }
    }

    return -1;
}

/* Append-log GET helper disabled under unified hashed layout focus. */
#if 0
static int get_from_append_leaf(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf, key_t key)
{
    return -1;
}
#endif


int bplus_tree_compressed_put(struct bplus_tree_compressed *ct_tree, key_t key, int data)
{
    if (ct_tree == NULL || !ct_tree->initialized || ct_tree->tree == NULL) {
        fprintf(stderr, "compressed_put: invalid tree state\n");
        return -1;
    }
    // fprintf(stderr, "compressed_put: key=%d data=%d algo=%d\n", (int)key, data, ct_tree->config.algo);
    
    pthread_rwlock_wrlock(&ct_tree->rwlock);
    
    struct bplus_leaf *leaf = find_leaf_for_key(ct_tree->tree, key);
    
    if (leaf != NULL && ct_tree->config.enable_lazy_compression) {
        char operation = (data == 0) ? 'D' : 'I';
        if (add_to_buffer(ct_tree, leaf, key, data, operation) == 0) {
            pthread_rwlock_unlock(&ct_tree->rwlock);
            return 0;
        }
        flush_buffer_to_leaf(ct_tree, leaf);
    }
    
    if (leaf != NULL) {
        /* ensure base tree arrays are accessible before modifying */
        int midx = find_leaf_metadata_index(leaf);
        if (midx != -1 && leaf_metadata[midx].is_compressed) {
            decompress_leaf_node_with_metadata_ex(ct_tree, leaf);
        }
    }
    
    struct bplus_node *new_sibling = bplus_tree_put(ct_tree->tree, key, data);
    
    leaf = find_leaf_for_key(ct_tree->tree, key);

    if (new_sibling != NULL && ct_tree->config.enable_lazy_compression) {
        add_leaf_metadata(ct_tree, (struct bplus_leaf *)new_sibling);
        compress_leaf_node_with_metadata_ex(ct_tree, (struct bplus_leaf *)new_sibling);
    }

    if (leaf != NULL && ct_tree->config.enable_lazy_compression) {
        /* Optionally recompress; keep uncompressed when lazy disabled */
        compress_leaf_node_with_metadata_ex(ct_tree, leaf);
    }
    
    pthread_rwlock_unlock(&ct_tree->rwlock);
    return 0;
}

int bplus_tree_compressed_get(struct bplus_tree_compressed *ct_tree, key_t key)
{
    if (ct_tree == NULL || !ct_tree->initialized || ct_tree->tree == NULL) {
        return -1;
    }
    pthread_rwlock_rdlock(&ct_tree->rwlock);
    /* Simplicity and correctness: defer to base tree lookup */
    int result = bplus_tree_get(ct_tree->tree, key);
    pthread_rwlock_unlock(&ct_tree->rwlock);
    return result;
}

int bplus_tree_compressed_delete(struct bplus_tree_compressed *ct_tree, key_t key)
{
    return bplus_tree_compressed_put(ct_tree, key, 0);
}

int bplus_tree_compressed_get_range(struct bplus_tree_compressed *ct_tree, key_t key1, key_t key2)
{
    return -1;
}

int bplus_tree_compressed_stats(struct bplus_tree_compressed *ct_tree, 
                               size_t *total_size, size_t *compressed_size)
{
    if (ct_tree == NULL) return -1;
    
    pthread_rwlock_rdlock(&ct_tree->rwlock);
    
    size_t total_uncompressed = 0;
    size_t total_compressed = 0;
    
    for (int i = 0; i < metadata_count; i++) {
        if (leaf_metadata[i].leaf != NULL) {
            total_uncompressed += leaf_metadata[i].uncompressed_bytes;
            if (leaf_metadata[i].is_compressed) {
                total_compressed += leaf_metadata[i].compressed_bytes;
            } else {
                total_compressed += leaf_metadata[i].uncompressed_bytes;
            }
        }
    }
    
    *total_size = total_uncompressed;
    *compressed_size = total_compressed;
    
    ct_tree->total_uncompressed_size = total_uncompressed;
    ct_tree->total_compressed_size = total_compressed;
    
    pthread_rwlock_unlock(&ct_tree->rwlock);
    return 0;
}

void bplus_tree_compressed_dump(struct bplus_tree_compressed *ct_tree)
{
    if (ct_tree == NULL) return;
    
    pthread_rwlock_rdlock(&ct_tree->rwlock);
    
    printf("=== Compressed B+Tree with Lazy Compression ===\n");
    printf("Compression enabled: %s\n", ct_tree->compression_enabled ? "Yes" : "No");
    printf("Layout: Hashed (%d sub-pages)\n", ct_tree->config.default_sub_pages);
    printf("Algorithm: %s\n", ct_tree->config.algo == COMPRESS_LZ4 ? "LZ4" : "QPL");
    
    // ... (rest of dump function)

    pthread_rwlock_unlock(&ct_tree->rwlock);
}

int bplus_tree_compressed_size(struct bplus_tree_compressed *ct_tree)
{
    if (ct_tree == NULL || ct_tree->tree == NULL) return 0;
    // Conservatively return total entries tracked by base tree
    return ct_tree->tree->entries;
}

int bplus_tree_compressed_empty(struct bplus_tree_compressed *ct_tree)
{
    return bplus_tree_compressed_size(ct_tree) == 0;
}

void bplus_tree_compressed_set_compression(struct bplus_tree_compressed *ct_tree, int enabled)
{
    if (!ct_tree) return;
    ct_tree->compression_enabled = enabled ? 1 : 0;
}

int bplus_tree_compressed_set_config(struct bplus_tree_compressed *ct_tree,
                                     struct compression_config *config)
{
    if (!ct_tree || !config) return -1;
    ct_tree->config = *config;
    return 0;
}

int bplus_tree_compressed_get_config(struct bplus_tree_compressed *ct_tree,
                                     struct compression_config *config)
{
    if (!ct_tree || !config) return -1;
    *config = ct_tree->config;
    return 0;
}

double bplus_tree_compressed_get_compression_ratio(struct bplus_tree_compressed *ct_tree)
{
    size_t total = 0, compressed = 0;
    if (bplus_tree_compressed_stats(ct_tree, &total, &compressed) != 0 || total == 0) {
        return 0.0;
    }
    double ratio = 100.0 * (double)compressed / (double)total;
    return ratio;
}

struct compression_config bplus_tree_create_default_leaf_config(leaf_layout_t default_layout)
{
    struct compression_config config;
    config.default_layout = default_layout;
    config.algo = COMPRESS_LZ4;
    config.default_sub_pages = 16;
    config.compression_level = (default_layout == LEAF_TYPE_QPL_APPEND) ? 1 : 0; // qpl_default_level is 1
    config.buffer_size = WRITING_BUFFER_SIZE;
    config.flush_threshold = MAX_BUFFER_ENTRIES - 4;
    config.enable_lazy_compression = 1;
    return config;
}

struct bplus_tree_compressed *bplus_tree_compressed_init_with_config(int order, int entries, 
                                                                   struct compression_config *config)
{
    struct bplus_tree_compressed *ct_tree = calloc(1, sizeof(*ct_tree));
    if (ct_tree == NULL) return NULL;
    
    ct_tree->tree = bplus_tree_init(order, entries);
    if (ct_tree->tree == NULL) {
        free(ct_tree);
        return NULL;
    }
    
    pthread_rwlock_init(&ct_tree->rwlock, NULL);
    
    ct_tree->initialized = 1;
    ct_tree->compression_enabled = 1;
    ct_tree->config = *config;
    
    // Always init QPL, cleanup if unused. Simplifies logic.
    if (init_qpl(ct_tree) != 0) {
        fprintf(stderr, "Warning: QPL initialization failed, QPL layouts will not be available\n");
    } else {
        fprintf(stderr, "QPL initialization successful\n");
    }
    
    if (config->enable_lazy_compression) {
        ct_tree->work_queue = calloc(1, sizeof(struct work_queue));
        if (ct_tree->work_queue == NULL) {
            // ... cleanup ...
            return NULL;
        }
        
        pthread_mutex_init(&ct_tree->work_queue->queue_lock, NULL);
        pthread_cond_init(&ct_tree->work_queue->queue_cond, NULL);
        ct_tree->work_queue->head = NULL;
        ct_tree->work_queue->tail = NULL;
        ct_tree->work_queue->count = 0;
        
        pthread_create(&ct_tree->background_thread, NULL, background_compression_thread, ct_tree);
    }
    
    return ct_tree;
}

int init_qpl(struct bplus_tree_compressed *ct_tree)
{
    if (ct_tree->qpl_job_ptr) return 0; // Already initialized

    pthread_mutex_init(&ct_tree->qpl_lock, NULL);
    
    uint32_t job_size;
    qpl_status status = qpl_get_job_size(qpl_path_auto, &job_size);
    if (status != QPL_STS_OK) return -1;
    
    ct_tree->qpl_job_buffer = malloc(job_size);
    if (!ct_tree->qpl_job_buffer) return -1;
    
    ct_tree->qpl_job_ptr = (qpl_job*)ct_tree->qpl_job_buffer;
    status = qpl_init_job(qpl_path_auto, ct_tree->qpl_job_ptr);
    if (status != QPL_STS_OK) {
        free(ct_tree->qpl_job_buffer);
        ct_tree->qpl_job_buffer = NULL;
        return -1;
    }
    
    /* Detect and log hardware/software path */
    const char *path_description;
    switch (ct_tree->qpl_job_ptr->data_ptr.path) {
        case qpl_path_hardware:
            path_description = "QPL Hardware-Accelerated Path";
            break;
        case qpl_path_software:
            path_description = "QPL Software Fallback Path";
            break;
        case qpl_path_auto:
            /* Auto-selection - determine what was actually chosen */
            if (ct_tree->qpl_job_ptr->data_ptr.path == qpl_path_hardware) {
                path_description = "QPL Hardware-Accelerated Path (auto-detected)";
            } else {
                path_description = "QPL Software Fallback Path (auto-selected)";
            }
            break;
        default:
            path_description = "QPL Unknown Path";
            break;
    }
    
    printf("QPL Initialization: %s active\n", path_description);
    
    return 0;
}

void cleanup_qpl(struct bplus_tree_compressed *ct_tree)
{
    if (ct_tree->qpl_job_ptr) {
        qpl_fini_job(ct_tree->qpl_job_ptr);
        ct_tree->qpl_job_ptr = NULL;
    }
    if (ct_tree->qpl_job_buffer) {
        free(ct_tree->qpl_job_buffer);
        ct_tree->qpl_job_buffer = NULL;
    }
    pthread_mutex_destroy(&ct_tree->qpl_lock);
}
