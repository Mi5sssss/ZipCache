/* File cleaned: removed stray characters */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <stdint.h>
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
static int compress_leaf_node_with_metadata_ex(struct bplus_tree_compressed *ct_tree, struct custom_leaf_node *leaf);
static int decompress_leaf_node_with_metadata_ex(struct bplus_tree_compressed *ct_tree, struct custom_leaf_node *leaf);
static int decompress_leaf_partial_lz4(struct custom_leaf_node *leaf, int sub_page_index);
static struct bplus_leaf *find_leaf_for_key(struct bplus_tree *tree, key_t key);
void cleanup_qpl(struct bplus_tree_compressed *ct_tree);
int init_qpl(struct bplus_tree_compressed *ct_tree);
static int hash_key_to_sub_page(key_t key, int num_sub_pages);
static int bplus_tree_compressed_put_internal(struct bplus_tree_compressed *ct_tree, key_t key, int data);
static key_t split_leaf(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf, struct bplus_leaf **new_leaf_out);
static void bplus_tree_insert_internal(struct bplus_tree *tree, key_t key, struct bplus_node *left, struct bplus_node *right);



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
        pthread_rwlock_wrlock(&ct_tree->rwlock);
        
        cleanup_qpl(ct_tree);
        
        if (ct_tree->tree != NULL) {
            // Here we should iterate through the tree and free custom_leaf_nodes
            bplus_tree_deinit(ct_tree->tree);
            ct_tree->tree = NULL;
        }
        
        pthread_rwlock_unlock(&ct_tree->rwlock);
        pthread_rwlock_destroy(&ct_tree->rwlock);
        ct_tree->initialized = 0;
    }
    
    free(ct_tree);
}

// Simplified kv-pair struct to be stored in buffers
struct kv_pair {
    key_t key;
    value_t value;
};

// qsort comparison function for kv_pair
int compare_kv_pairs(const void *a, const void *b) {
    struct kv_pair *pa = (struct kv_pair *)a;
    struct kv_pair *pb = (struct kv_pair *)b;
    if (pa->key < pb->key) return -1;
    if (pa->key > pb->key) return 1;
    return 0;
}


static key_t split_leaf(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf, struct bplus_leaf **new_leaf_out)
{
    struct custom_leaf_node *custom_leaf = (struct custom_leaf_node *)leaf->data[0];

    // 1. Create a temporary buffer to hold all key-value pairs.
    int max_pairs = (WRITING_BUFFER_SIZE + COMPRESSED_LEAF_SIZE) / sizeof(struct kv_pair);
    struct kv_pair *all_pairs = malloc(max_pairs * sizeof(struct kv_pair));
    int pair_count = 0;

    // 2. Decompress the main data area and collect kv-pairs.
    char uncompressed_pages[COMPRESSED_LEAF_SIZE];
    if (custom_leaf->is_compressed) {
        int decompress_size = LZ4_decompress_safe(custom_leaf->compressed_data, uncompressed_pages, custom_leaf->compressed_size, COMPRESSED_LEAF_SIZE);
        if (decompress_size < 0) { free(all_pairs); return 0; }

        struct kv_pair *p = (struct kv_pair *)uncompressed_pages;
        struct kv_pair *end = (struct kv_pair *)(uncompressed_pages + COMPRESSED_LEAF_SIZE);
        while (p < end) {
            if (p->key != 0) {
                all_pairs[pair_count++] = *p;
            }
            p++;
        }
    }

    // 3. Collect kv-pairs from the landing buffer.
    struct kv_pair *p_landing = (struct kv_pair *)custom_leaf->landing_buffer;
    struct kv_pair *end_landing = (struct kv_pair *)(custom_leaf->landing_buffer + WRITING_BUFFER_SIZE);
    while (p_landing < end_landing) {
        if (p_landing->key != 0) {
            all_pairs[pair_count++] = *p_landing;
        }
        p_landing++;
    }

    // 4. Sort all collected key-value pairs.
    qsort(all_pairs, pair_count, sizeof(struct kv_pair), compare_kv_pairs);

    // 5. Reset the original leaf and create a new leaf.
    memset(custom_leaf->landing_buffer, 0, WRITING_BUFFER_SIZE);
    memset(custom_leaf->compressed_data, 0, MAX_COMPRESSED_SIZE);
    custom_leaf->is_compressed = 0;
    custom_leaf->compressed_size = 0;

    struct bplus_leaf *new_bplus_leaf = (struct bplus_leaf *)bplus_node_new(ct_tree->tree, BPLUS_TREE_LEAF);
    struct custom_leaf_node *new_custom_leaf = calloc(1, sizeof(struct custom_leaf_node));
    new_custom_leaf->compressed_data = malloc(MAX_COMPRESSED_SIZE);
    new_custom_leaf->num_sub_pages = custom_leaf->num_sub_pages;
    new_bplus_leaf->data[0] = (value_t)new_custom_leaf;
    *new_leaf_out = new_bplus_leaf;

    // 6. Distribute the sorted pairs into the two leaves.
    int midpoint = pair_count / 2;
    for (int i = 0; i < midpoint; i++) {
        insert_into_leaf(ct_tree, custom_leaf, all_pairs[i].key, all_pairs[i].value);
    }
    for (int i = midpoint; i < pair_count; i++) {
        insert_into_leaf(ct_tree, new_custom_leaf, all_pairs[i].key, all_pairs[i].value);
    }

    // 7. The split key is the first key in the new leaf.
    key_t split_key = all_pairs[midpoint].key;
    free(all_pairs);

    return split_key;
}


int insert_into_leaf(struct bplus_tree_compressed *ct_tree, struct custom_leaf_node *leaf, key_t key, value_t value)
{
    // 1. Search for key in landing buffer
    struct kv_pair *p = (struct kv_pair *)leaf->landing_buffer;
    struct kv_pair *end = (struct kv_pair *)(leaf->landing_buffer + WRITING_BUFFER_SIZE);
    struct kv_pair *free_slot = NULL;

    while (p < end) {
        if (p->key == 0) { // Assuming 0 is an invalid key marking a free slot
            if (free_slot == NULL) {
                free_slot = p;
            }
        } else if (p->key == key) {
            // Key exists, update value
            p->value = value;
            return 0; // Success
        }
        p++;
    }

    // 2. If key not found, add to a free slot in landing buffer
    if (free_slot != NULL) {
        free_slot->key = key;
        free_slot->value = value;
        return 0; // Success
    }

    // 3. Landing buffer is full, flush to compressed sub-pages
    char uncompressed_pages[COMPRESSED_LEAF_SIZE];
    if (leaf->is_compressed) {
         int decompress_size = LZ4_decompress_safe(leaf->compressed_data, uncompressed_pages, leaf->compressed_size, COMPRESSED_LEAF_SIZE);
         if (decompress_size < 0) {
             return -1; // Decompression failed
         }
    } else {
        memset(uncompressed_pages, 0, COMPRESSED_LEAF_SIZE);
    }

    // Add landing buffer contents to the uncompressed page data
    p = (struct kv_pair *)leaf->landing_buffer;
    while (p < end) {
        if (p->key != 0) {
            int sub_page_index = hash_key_to_sub_page(p->key, leaf->num_sub_pages);
            int sub_page_size = COMPRESSED_LEAF_SIZE / leaf->num_sub_pages;
            struct kv_pair *sub_page_start = (struct kv_pair *)(uncompressed_pages + sub_page_index * sub_page_size);
            struct kv_pair *sub_page_end = (struct kv_pair *)((char*)sub_page_start + sub_page_size);
            struct kv_pair *sub_page_free_slot = NULL;
            
            struct kv_pair *current = sub_page_start;
            while(current < sub_page_end) {
                if (current->key == 0 && sub_page_free_slot == NULL) {
                    sub_page_free_slot = current;
                } else if (current->key == p->key) {
                    sub_page_free_slot = current; // Overwrite
                    break;
                }
                current++;
            }

            if (sub_page_free_slot != NULL) {
                sub_page_free_slot->key = p->key;
                sub_page_free_slot->value = p->value;
            } else {
                return -1; 
            }
        }
        p++;
    }

    // Clear the landing buffer
    memset(leaf->landing_buffer, 0, WRITING_BUFFER_SIZE);

    // Add the new key/value that triggered the flush
    int sub_page_index = hash_key_to_sub_page(key, leaf->num_sub_pages);
    int sub_page_size = COMPRESSED_LEAF_SIZE / leaf->num_sub_pages;
    struct kv_pair *sub_page_start = (struct kv_pair *)(uncompressed_pages + sub_page_index * sub_page_size);
    struct kv_pair *sub_page_end = (struct kv_pair *)((char*)sub_page_start + sub_page_size);
    struct kv_pair *sub_page_free_slot = NULL;
    
    struct kv_pair *current = sub_page_start;
    while(current < sub_page_end) {
        if (current->key == 0) {
            sub_page_free_slot = current;
            break;
        }
        current++;
    }

    if (sub_page_free_slot != NULL) {
        sub_page_free_slot->key = key;
        sub_page_free_slot->value = value;
    } else {
        return -1; // Sub-page is full
    }

    // 4. Re-compress the pages, this time sub-page by sub-page to build the index
    size_t running_offset = 0;
    if (leaf->subpage_index == NULL) {
        leaf->subpage_index = calloc(leaf->num_sub_pages, sizeof(struct subpage_index_entry));
    }

    for (int i = 0; i < leaf->num_sub_pages; i++) {
        char *sub_page_start = uncompressed_pages + i * (COMPRESSED_LEAF_SIZE / leaf->num_sub_pages);
        int sub_page_size = COMPRESSED_LEAF_SIZE / leaf->num_sub_pages;
        int max_compress_size = LZ4_compressBound(sub_page_size);
        char *compressed_buffer = malloc(max_compress_size);

        int compress_size = LZ4_compress_default(sub_page_start, compressed_buffer, sub_page_size, max_compress_size);
        if (compress_size > 0) {
            memcpy(leaf->compressed_data + running_offset, compressed_buffer, compress_size);
            leaf->subpage_index[i].offset = running_offset;
            leaf->subpage_index[i].length = compress_size;
            running_offset += compress_size;
        } else {
            // Handle compression failure
            free(compressed_buffer);
            return -1;
        }
        free(compressed_buffer);
    }

    leaf->compressed_size = running_offset;
    leaf->is_compressed = 1;

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

static void bplus_tree_insert_internal(struct bplus_tree *tree, key_t key, struct bplus_node *left, struct bplus_node *right)
{
    struct bplus_node *parent = left->parent;
    if (parent == NULL) {
        struct bplus_node *new_root = bplus_node_new(tree, BPLUS_TREE_NON_LEAF);
        struct bplus_non_leaf *root_node = (struct bplus_non_leaf *)new_root;
        root_node->key[0] = key;
        root_node->sub_ptr[0] = left;
        root_node->sub_ptr[1] = right;
        root_node->children = 2;
        left->parent = new_root;
        right->parent = new_root;
        tree->root = new_root;
        return;
    }

    struct bplus_non_leaf *parent_node = (struct bplus_non_leaf *)parent;
    if (parent_node->children < tree->order) {
        int i = parent_node->children - 1;
        while (i > 0 && parent_node->key[i - 1] > key) {
            parent_node->key[i] = parent_node->key[i - 1];
            parent_node->sub_ptr[i + 1] = parent_node->sub_ptr[i];
            i--;
        }
        parent_node->key[i] = key;
        parent_node->sub_ptr[i + 1] = right;
        parent_node->children++;
    } else {
        // Parent is full, need to split
        struct bplus_non_leaf *new_parent = (struct bplus_non_leaf *)bplus_node_new(tree, BPLUS_TREE_NON_LEAF);
        key_t new_key;
        int midpoint = (tree->order) / 2;
        
        // Copy upper half to new parent
        int j = 0;
        for (int i = midpoint; i < parent_node->children; i++) {
            new_parent->key[j] = parent_node->key[i];
            new_parent->sub_ptr[j] = parent_node->sub_ptr[i];
            j++;
        }
        new_parent->sub_ptr[j] = parent_node->sub_ptr[parent_node->children];
        new_parent->children = j;
        parent_node->children = midpoint;

        if (key < parent_node->key[midpoint -1]) {
            bplus_tree_insert_internal(tree, key, left, right);
        } else {
            bplus_tree_insert_internal(tree, key, left, right);
        }

        new_key = parent_node->key[midpoint-1];
        bplus_tree_insert_internal(tree, new_key, (struct bplus_node *)parent_node, (struct bplus_node *)new_parent);
    }
}


static int hash_key_to_sub_page(key_t key, int num_sub_pages) {
    if (num_sub_pages == 0) return 0;
    return key % num_sub_pages;
}


int bplus_tree_compressed_put(struct bplus_tree_compressed *ct_tree, key_t key, int data)
{
    return bplus_tree_compressed_put_internal(ct_tree, key, data);
}

static int bplus_tree_compressed_put_internal(struct bplus_tree_compressed *ct_tree,
                                              key_t key,
                                              int data)
{
    if (ct_tree == NULL || !ct_tree->initialized || ct_tree->tree == NULL) {
        return -1;
    }

    pthread_rwlock_wrlock(&ct_tree->rwlock);

    struct bplus_leaf *leaf = find_leaf_for_key(ct_tree->tree, key);
    if (leaf == NULL) {
        // Tree is empty, create the first leaf.
        leaf = (struct bplus_leaf *)bplus_node_new(ct_tree->tree, BPLUS_TREE_LEAF);
        ct_tree->tree->root = (struct bplus_node *)leaf;
    }

    struct custom_leaf_node *custom_leaf = (struct custom_leaf_node*)leaf->data[0];
    if (custom_leaf == NULL) {
        custom_leaf = calloc(1, sizeof(struct custom_leaf_node));
        if(!custom_leaf) {            
            pthread_rwlock_unlock(&ct_tree->rwlock);
            return -1;
        }
        custom_leaf->compressed_data = malloc(MAX_COMPRESSED_SIZE);
        if(!custom_leaf->compressed_data) {
            free(custom_leaf);
            pthread_rwlock_unlock(&ct_tree->rwlock);
            return -1;
        }
        custom_leaf->num_sub_pages = ct_tree->config.default_sub_pages;
        leaf->data[0] = (value_t)custom_leaf;
    }

    int result = insert_into_leaf(ct_tree, custom_leaf, key, data);

    if (result == -1) { 
        struct bplus_leaf *new_leaf = NULL;
        key_t split_key = split_leaf(ct_tree, leaf, &new_leaf);
        
        // Use the base library's put function to handle the internal node split
        bplus_tree_put(ct_tree->tree, split_key, (long)new_leaf->data[0]);
        result = 0; // Split successful
    }

    pthread_rwlock_unlock(&ct_tree->rwlock);
    return result;
}


int bplus_tree_compressed_get(struct bplus_tree_compressed *ct_tree, key_t key)
{
    if (ct_tree == NULL || !ct_tree->initialized || ct_tree->tree == NULL) {
        return -1;
    }
    pthread_rwlock_rdlock(&ct_tree->rwlock);

    struct bplus_leaf *leaf = find_leaf_for_key(ct_tree->tree, key);
    if (!leaf) {
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return -1;
    }

    struct custom_leaf_node *custom_leaf = (struct custom_leaf_node*)leaf->data[0];
    if (!custom_leaf) {
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return -1;
    }

    // 1. Search landing buffer
    struct kv_pair *p = (struct kv_pair *)custom_leaf->landing_buffer;
    struct kv_pair *end = (struct kv_pair *)(custom_leaf->landing_buffer + WRITING_BUFFER_SIZE);
    while (p < end) {
        if (p->key == key) {
            pthread_rwlock_unlock(&ct_tree->rwlock);
            return p->value;
        }
        p++;
    }

    // 2. Search compressed sub-pages
    if (!custom_leaf->is_compressed) {
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return -1; // Not in buffer and not compressed means not found
    }

    if (ct_tree->config.algo == COMPRESS_LZ4) {
        int sub_page_index = hash_key_to_sub_page(key, custom_leaf->num_sub_pages);
        if (custom_leaf->subpage_index == NULL) { pthread_rwlock_unlock(&ct_tree->rwlock); return -1; }

        struct subpage_index_entry *index_entry = &custom_leaf->subpage_index[sub_page_index];
        int sub_page_size = COMPRESSED_LEAF_SIZE / custom_leaf->num_sub_pages;
        char *sub_page_buffer = malloc(sub_page_size);

        int decompress_size = LZ4_decompress_safe(custom_leaf->compressed_data + index_entry->offset, sub_page_buffer, index_entry->length, sub_page_size);
        if(decompress_size < 0) { 
            free(sub_page_buffer);
            pthread_rwlock_unlock(&ct_tree->rwlock); 
            return -1; 
        }

        struct kv_pair *sp_p = (struct kv_pair *)sub_page_buffer;
        struct kv_pair *sp_end = (struct kv_pair *)(sub_page_buffer + sub_page_size);
        while (sp_p < sp_end) {
            if (sp_p->key == key) {
                int value = sp_p->value;
                free(sub_page_buffer);
                pthread_rwlock_unlock(&ct_tree->rwlock);
                return value;
            }
            sp_p++;
        }
        free(sub_page_buffer);

    } else if (ct_tree->config.algo == COMPRESS_QPL) {
        char uncompressed_pages[COMPRESSED_LEAF_SIZE];
        int decompress_size = LZ4_decompress_safe(custom_leaf->compressed_data, uncompressed_pages, custom_leaf->compressed_size, COMPRESSED_LEAF_SIZE);
        if(decompress_size < 0) { pthread_rwlock_unlock(&ct_tree->rwlock); return -1; }

        struct kv_pair *p_full = (struct kv_pair *)uncompressed_pages;
        struct kv_pair *end_full = (struct kv_pair *)(uncompressed_pages + COMPRESSED_LEAF_SIZE);
        while(p_full < end_full) {
            if(p_full->key == key) {
                pthread_rwlock_unlock(&ct_tree->rwlock);
                return p_full->value;
            }
            p_full++;
        }
    }

    pthread_rwlock_unlock(&ct_tree->rwlock);
    return -1;
}


struct compression_config bplus_tree_create_default_leaf_config(leaf_layout_t default_layout)
{
    struct compression_config config;
    config.default_layout = default_layout;
    config.algo = COMPRESS_LZ4;
    config.default_sub_pages = 16;
    config.compression_level = 1;
    config.buffer_size = WRITING_BUFFER_SIZE;
    config.flush_threshold = 0; // Not used in this model
    config.enable_lazy_compression = 0; // Not used in this model
    return config;
}

struct bplus_tree_compressed *bplus_tree_compressed_init_with_config(int order, int entries, 
                                                                   struct compression_config *config)
{
    struct bplus_tree_compressed *ct_tree = calloc(1, sizeof(*ct_tree));
    if (ct_tree == NULL) return NULL;

    int fixed_entries = 1; // Each leaf in base tree just points to one custom leaf
    
    ct_tree->tree = bplus_tree_init(order, fixed_entries);
    if (ct_tree->tree == NULL) {
        free(ct_tree);
        return NULL;
    }
    
    pthread_rwlock_init(&ct_tree->rwlock, NULL);
    
    ct_tree->initialized = 1;
    ct_tree->compression_enabled = 1;
    ct_tree->config = *config;
    
    if (init_qpl(ct_tree) != 0) {
        fprintf(stderr, "Warning: QPL initialization failed, QPL layouts will not be available\n");
    } else {
        fprintf(stderr, "QPL initialization successful\n");
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
