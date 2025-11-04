#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stddef.h>
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
static struct bplus_leaf *find_leaf_for_key(struct bplus_tree *tree, key_t key);
void cleanup_qpl(struct bplus_tree_compressed *ct_tree);
int init_qpl(struct bplus_tree_compressed *ct_tree);
static int hash_key_to_sub_page(key_t key, int num_sub_pages);
static int bplus_tree_compressed_put_internal(struct bplus_tree_compressed *ct_tree, key_t key, int data);
static key_t split_leaf(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf, struct bplus_leaf **new_leaf_out);
static int bplus_tree_insert_internal(struct bplus_tree *tree, key_t key, struct bplus_node *left, struct bplus_node *right);
static int ensure_custom_leaf(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf, struct simple_leaf_node **out_leaf);
static int compressed_parent_node_build(struct bplus_tree *tree, struct bplus_node *left, struct bplus_node *right, key_t key, int level);
static int compressed_non_leaf_insert(struct bplus_tree *tree, struct bplus_non_leaf *node, struct bplus_node *l_ch, struct bplus_node *r_ch, key_t key, int level);
static void compressed_leaf_free(struct simple_leaf_node *leaf);



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
            struct list_head *head = &ct_tree->tree->list[0];
            struct list_head *pos, *n;
            list_for_each_safe(pos, n, head) {
                struct bplus_leaf *leaf = list_entry(pos, struct bplus_leaf, link);
                if (leaf->type != BPLUS_TREE_LEAF) {
                    continue;
                }
                if (leaf->data[0] != 0) {
                    struct simple_leaf_node *custom_leaf = (struct simple_leaf_node *)leaf->data[0];
                    compressed_leaf_free(custom_leaf);
                    leaf->data[0] = 0;
                }
            }
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

static int kv_vector_put(struct kv_pair **pairs,
                         size_t *count,
                         size_t *capacity,
                         key_t key,
                         value_t value)
{
    for (size_t i = 0; i < *count; i++) {
        if ((*pairs)[i].key == key) {
            (*pairs)[i].value = value;
            return 0;
        }
    }

    if (*count == *capacity) {
        size_t new_capacity = (*capacity == 0) ? 16 : (*capacity * 2);
        struct kv_pair *new_pairs = realloc(*pairs, new_capacity * sizeof(struct kv_pair));
        if (!new_pairs) {
            return -1;
        }
        *pairs = new_pairs;
        *capacity = new_capacity;
    }

    (*pairs)[*count].key = key;
    (*pairs)[*count].value = value;
    (*count)++;
    return 0;
}

static int positive_mod_i32(key_t value, int mod)
{
    if (mod <= 0) {
        return 0;
    }
    int rem = value % mod;
    if (rem < 0) {
        rem += mod;
    }
    return rem;
}

static bool subpage_needed_for_range(key_t min_key, key_t max_key, int bucket, int num_subpages)
{
    if (num_subpages <= 0) {
        return false;
    }

    if (max_key < min_key) {
        key_t tmp = min_key;
        min_key = max_key;
        max_key = tmp;
    }

    int normalized_bucket = bucket % num_subpages;
    if (normalized_bucket < 0) {
        normalized_bucket += num_subpages;
    }

    int start_mod = positive_mod_i32(min_key, num_subpages);
    int delta = normalized_bucket - start_mod;
    if (delta < 0) {
        delta += num_subpages;
    }

    long long first_candidate = (long long)min_key + delta;
    return first_candidate <= (long long)max_key;
}

static int compress_subpage(struct bplus_tree_compressed *ct_tree,
                            struct simple_leaf_node *leaf,
                            const uint8_t *src,
                            uint32_t src_size,
                            uint8_t *dst,
                            uint32_t dst_capacity)
{
    if (leaf->compression_algo == COMPRESS_QPL && ct_tree->qpl_job_ptr) {
        pthread_mutex_lock(&ct_tree->qpl_lock);
        qpl_job *job = ct_tree->qpl_job_ptr;
        job->op = qpl_op_compress;
        job->next_in_ptr = (uint8_t *)src;
        job->available_in = src_size;
        job->next_out_ptr = dst;
        job->available_out = dst_capacity;
        job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
        job->level = qpl_default_level;
        qpl_status status = qpl_execute_job(job);
        uint32_t produced = job->total_out;
        pthread_mutex_unlock(&ct_tree->qpl_lock);
        if (status == QPL_STS_OK && produced > 0 && produced <= dst_capacity) {
            return (int)produced;
        }
    }

    int level = ct_tree->config.compression_level;
    if (level < 0) {
        int acceleration = -level;
        if (acceleration <= 0) {
            acceleration = 1;
        }
        return LZ4_compress_fast((const char *)src,
                                 (char *)dst,
                                 (int)src_size,
                                 (int)dst_capacity,
                                 acceleration);
    }

    if (level > 1) {
#ifdef LZ4HC_CLEVEL_MAX
        if (level > LZ4HC_CLEVEL_MAX) {
            level = LZ4HC_CLEVEL_MAX;
        }
#endif
        return LZ4_compress_HC((const char *)src,
                               (char *)dst,
                               (int)src_size,
                               (int)dst_capacity,
                               level);
    }

    return LZ4_compress_default((const char *)src, (char *)dst, (int)src_size, (int)dst_capacity);
}

static int decompress_subpage(struct bplus_tree_compressed *ct_tree,
                              struct simple_leaf_node *leaf,
                              const uint8_t *src,
                              uint32_t src_size,
                              uint8_t *dst,
                              uint32_t dst_capacity)
{
    if (leaf->compression_algo == COMPRESS_QPL && ct_tree->qpl_job_ptr) {
        pthread_mutex_lock(&ct_tree->qpl_lock);
        qpl_job *job = ct_tree->qpl_job_ptr;
        job->op = qpl_op_decompress;
        job->next_in_ptr = (uint8_t *)src;
        job->available_in = src_size;
        job->next_out_ptr = dst;
        job->available_out = dst_capacity;
        job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
        qpl_status status = qpl_execute_job(job);
        uint32_t produced = job->total_out;
        pthread_mutex_unlock(&ct_tree->qpl_lock);
        if (status == QPL_STS_OK && produced > 0 && produced <= dst_capacity) {
            return (int)produced;
        }
    }

    return LZ4_decompress_safe((const char *)src, (char *)dst, (int)src_size, (int)dst_capacity);
}

static int compressed_leaf_collect_pairs(struct bplus_tree_compressed *ct_tree,
                                         struct simple_leaf_node *leaf,
                                         struct kv_pair **out_pairs,
                                         size_t *out_count)
{
    size_t capacity = 0;
    size_t count = 0;
    struct kv_pair *pairs = NULL;

    struct kv_pair *landing = (struct kv_pair *)leaf->landing_buffer;
    struct kv_pair *landing_end = (struct kv_pair *)(leaf->landing_buffer + LANDING_BUFFER_BYTES);
    while (landing < landing_end) {
        if (landing->key != 0) {
            if (kv_vector_put(&pairs, &count, &capacity, landing->key, landing->value) != 0) {
                free(pairs);
                return -1;
            }
        }
        landing++;
    }

    if (leaf->is_compressed && leaf->subpage_index && leaf->num_subpages > 0) {
        int sub_page_size = COMPRESSED_LEAF_SIZE / leaf->num_subpages;
        if (sub_page_size <= 0) {
            sub_page_size = COMPRESSED_LEAF_SIZE;
        }

        for (int bucket = 0; bucket < leaf->num_subpages; bucket++) {
            struct subpage_index_entry *entry = &leaf->subpage_index[bucket];
            if (!entry || entry->length <= 0) {
                continue;
            }

            char *sub_page_buffer = malloc(sub_page_size);
            if (!sub_page_buffer) {
                free(pairs);
                return -1;
            }

            int decompress_size = decompress_subpage(ct_tree,
                                                     leaf,
                                                     (const uint8_t *)leaf->compressed_data + entry->offset,
                                                     entry->length,
                                                     (uint8_t *)sub_page_buffer,
                                                     sub_page_size);

            if (decompress_size < 0) {
                free(sub_page_buffer);
                free(pairs);
                return -1;
            }

            struct kv_pair *sp = (struct kv_pair *)sub_page_buffer;
            struct kv_pair *sp_end = (struct kv_pair *)(sub_page_buffer + sub_page_size);
            while (sp < sp_end) {
                if (sp->key != 0) {
                    if (kv_vector_put(&pairs, &count, &capacity, sp->key, sp->value) != 0) {
                        free(sub_page_buffer);
                        free(pairs);
                        return -1;
                    }
                }
                sp++;
            }

            free(sub_page_buffer);
        }
    }

    *out_pairs = pairs;
    *out_count = count;
    return 0;
}

static int compressed_key_binary_search(const key_t *arr, int len, key_t target)
{
    int low = -1;
    int high = len;
    while (low + 1 < high) {
        int mid = low + (high - low) / 2;
        if (target > arr[mid]) {
            low = mid;
        } else {
            high = mid;
        }
    }
    if (high >= len || arr[high] != target) {
        return -high - 1;
    }
    return high;
}

static int compressed_leaf_rebuild_with_pairs(struct bplus_tree_compressed *ct_tree,
                                              struct simple_leaf_node *leaf,
                                              struct kv_pair *pairs,
                                              size_t count)
{
    memset(leaf->landing_buffer, 0, sizeof(leaf->landing_buffer));
    if (leaf->compressed_data) {
        memset(leaf->compressed_data, 0, MAX_COMPRESSED_SIZE);
    }
    if (leaf->subpage_index && leaf->num_subpages > 0) {
        memset(leaf->subpage_index, 0, leaf->num_subpages * sizeof(struct subpage_index_entry));
    }

    if (count == 0) {
        leaf->is_compressed = false;
        leaf->compressed_size = 0;
        leaf->compressed_bytes = 0;
        leaf->uncompressed_bytes = 0;
        return 0;
    }

    qsort(pairs, count, sizeof(struct kv_pair), compare_kv_pairs);

    size_t landing_slots = LANDING_BUFFER_BYTES / sizeof(struct kv_pair);
    size_t landing_count = count < landing_slots ? count : landing_slots;

    for (size_t i = 0; i < landing_count; i++) {
        struct kv_pair *slot = ((struct kv_pair *)leaf->landing_buffer) + i;
        slot->key = pairs[i].key;
        slot->value = pairs[i].value;
    }

    if (landing_count == count) {
        leaf->is_compressed = false;
        leaf->compressed_size = 0;
        leaf->compressed_bytes = 0;
        leaf->uncompressed_bytes = landing_count * sizeof(struct kv_pair);
        return 0;
    }

    if (leaf->num_subpages <= 0) {
        leaf->num_subpages = 1;
    }

    if (leaf->subpage_index == NULL) {
        leaf->subpage_index = calloc(leaf->num_subpages, sizeof(struct subpage_index_entry));
        if (!leaf->subpage_index) {
            return -1;
        }
    }

    int sub_page_size = COMPRESSED_LEAF_SIZE / leaf->num_subpages;
    if (sub_page_size <= 0) {
        sub_page_size = COMPRESSED_LEAF_SIZE;
    }

    char *uncompressed_pages = calloc(1, COMPRESSED_LEAF_SIZE);
    if (!uncompressed_pages) {
        return -1;
    }

    size_t hashed_start = landing_count;
    size_t hashed_count = count - landing_count;
    size_t bucket_capacity = sub_page_size / sizeof(struct kv_pair);

    for (size_t i = 0; i < hashed_count; i++) {
        struct kv_pair *entry = &pairs[hashed_start + i];
        int bucket = positive_mod_i32(entry->key, leaf->num_subpages);
        struct kv_pair *bucket_start = (struct kv_pair *)(uncompressed_pages + bucket * sub_page_size);
        bool placed = false;
        for (size_t j = 0; j < bucket_capacity; j++) {
            if (bucket_start[j].key == 0 || bucket_start[j].key == entry->key) {
                bucket_start[j] = *entry;
                placed = true;
                break;
            }
        }
        if (!placed) {
            free(uncompressed_pages);
            return -1;
        }
    }

    char *temp_compressed = malloc(MAX_COMPRESSED_SIZE);
    if (!temp_compressed) {
        free(uncompressed_pages);
        return -1;
    }

    struct subpage_index_entry *temp_index = calloc(leaf->num_subpages, sizeof(struct subpage_index_entry));
    if (!temp_index) {
        free(temp_compressed);
        free(uncompressed_pages);
        return -1;
    }

    size_t running_offset = 0;
    for (int bucket = 0; bucket < leaf->num_subpages; bucket++) {
        struct kv_pair *bucket_start = (struct kv_pair *)(uncompressed_pages + bucket * sub_page_size);
        bool bucket_empty = true;
        for (size_t j = 0; j < bucket_capacity; j++) {
            if (bucket_start[j].key != 0) {
                bucket_empty = false;
                break;
            }
        }

        if (bucket_empty) {
            temp_index[bucket].offset = running_offset;
            temp_index[bucket].length = 0;
            continue;
        }

        uint32_t dest_capacity = MAX_COMPRESSED_SIZE - (uint32_t)running_offset;
        if (dest_capacity == 0) {
            free(temp_index);
            free(temp_compressed);
            free(uncompressed_pages);
            return -1;
        }

        int compressed_size = compress_subpage(ct_tree,
                                               leaf,
                                               (const uint8_t *)bucket_start,
                                               sub_page_size,
                                               (uint8_t *)temp_compressed + running_offset,
                                               dest_capacity);
        if (compressed_size <= 0 || running_offset + (size_t)compressed_size > MAX_COMPRESSED_SIZE) {
            free(temp_index);
            free(temp_compressed);
            free(uncompressed_pages);
            return -1;
        }

        temp_index[bucket].offset = running_offset;
        temp_index[bucket].length = (uint32_t)compressed_size;
        running_offset += (size_t)compressed_size;
    }

    if (leaf->compressed_data == NULL) {
        leaf->compressed_data = calloc(1, MAX_COMPRESSED_SIZE);
        if (!leaf->compressed_data) {
            free(temp_index);
            free(temp_compressed);
            free(uncompressed_pages);
            return -1;
        }
    }

    memcpy(leaf->compressed_data, temp_compressed, running_offset);
    if (running_offset < MAX_COMPRESSED_SIZE) {
        memset(leaf->compressed_data + running_offset, 0, MAX_COMPRESSED_SIZE - running_offset);
    }
    memcpy(leaf->subpage_index, temp_index, leaf->num_subpages * sizeof(struct subpage_index_entry));

    free(temp_index);
    free(temp_compressed);
    free(uncompressed_pages);

    leaf->is_compressed = true;
    leaf->compressed_size = running_offset;
    leaf->compressed_bytes = running_offset;
    leaf->uncompressed_bytes = hashed_count * sizeof(struct kv_pair);
    leaf->num_subpage_entries = leaf->num_subpages;
    return 0;
}

static int compressed_leaf_min_key(struct bplus_tree_compressed *ct_tree,
                                   struct simple_leaf_node *leaf,
                                   key_t *out_key)
{
    struct kv_pair *pairs = NULL;
    size_t count = 0;
    if (compressed_leaf_collect_pairs(ct_tree, leaf, &pairs, &count) != 0) {
        return -1;
    }
    if (count == 0) {
        free(pairs);
        return -1;
    }
    qsort(pairs, count, sizeof(struct kv_pair), compare_kv_pairs);
    *out_key = pairs[0].key;
    free(pairs);
    return 0;
}

static int node_min_key(struct bplus_tree_compressed *ct_tree,
                        struct bplus_node *node,
                        key_t *out_key)
{
    if (node == NULL) {
        return -1;
    }

    if (node->type == BPLUS_TREE_LEAF) {
        struct bplus_leaf *leaf = (struct bplus_leaf *)node;
        if (leaf->data[0] == 0) {
            return -1;
        }
        struct simple_leaf_node *custom_leaf = (struct simple_leaf_node *)leaf->data[0];
        return compressed_leaf_min_key(ct_tree, custom_leaf, out_key);
    }

    struct bplus_non_leaf *non_leaf = (struct bplus_non_leaf *)node;
    for (int i = 0; i < non_leaf->children; i++) {
        if (non_leaf->sub_ptr[i]) {
            if (node_min_key(ct_tree, non_leaf->sub_ptr[i], out_key) == 0) {
                return 0;
            }
        }
    }

    return -1;
}

static void propagate_min_key_change(struct bplus_node *node, key_t new_min_key)
{
    struct bplus_non_leaf *parent = node->parent;
    struct bplus_node *child = node;
    key_t current_min = new_min_key;

    while (parent) {
        int slot = child->parent_key_idx;
        if (slot >= 0) {
            parent->key[slot] = current_min;
        }

        if (slot != 0) {
            break;
        }

        current_min = parent->key[0];
        child = (struct bplus_node *)parent;
        parent = parent->parent;
    }
}

static void compressed_leaf_free(struct simple_leaf_node *leaf)
{
    if (!leaf) {
        return;
    }
    pthread_rwlock_destroy(&leaf->rwlock);
    free(leaf->compressed_data);
    free(leaf->subpage_index);
    free(leaf);
}

static int remove_leaf_from_parent(struct bplus_tree_compressed *ct_tree,
                                   struct bplus_leaf *leaf)
{
    struct bplus_non_leaf *parent = leaf->parent;

    list_del(&leaf->link);

    if (parent == NULL) {
        free(leaf);
        ct_tree->tree->root = NULL;
        ct_tree->tree->level = 0;
        return 0;
    }

    int child_slot = leaf->parent_key_idx + 1;
    if (child_slot < 0) {
        child_slot = 0;
    }

    for (int i = child_slot; i < parent->children - 1; i++) {
        parent->sub_ptr[i] = parent->sub_ptr[i + 1];
    }
    parent->sub_ptr[parent->children - 1] = NULL;

    if (parent->children > 1) {
        int key_remove_index = child_slot == 0 ? 0 : child_slot - 1;
        for (int i = key_remove_index; i < parent->children - 1; i++) {
            parent->key[i] = parent->key[i + 1];
        }
        parent->key[parent->children - 1] = 0;
    }

    parent->children--;

    for (int i = 0; i < parent->children; i++) {
        if (parent->sub_ptr[i]) {
            parent->sub_ptr[i]->parent = parent;
            parent->sub_ptr[i]->parent_key_idx = i - 1;
        }
    }

    if (parent->children > 0) {
        if (child_slot == 0 && parent->children > 1) {
            key_t new_key;
            if (node_min_key(ct_tree, parent->sub_ptr[1], &new_key) == 0) {
                parent->key[0] = new_key;
                propagate_min_key_change((struct bplus_node *)parent, parent->key[0]);
            }
        } else if (child_slot > 0 && child_slot - 1 < parent->children - 1) {
            int idx = child_slot - 1;
            key_t new_key;
            if (node_min_key(ct_tree, parent->sub_ptr[idx + 1], &new_key) == 0) {
                parent->key[idx] = new_key;
                if (idx == 0) {
                    propagate_min_key_change((struct bplus_node *)parent, parent->key[0]);
                }
            }
        }
    }

    free(leaf);

    if (parent->children == 1) {
        struct bplus_node *only_child = parent->sub_ptr[0];
        struct bplus_non_leaf *grand = parent->parent;

        if (grand == NULL) {
            only_child->parent = NULL;
            only_child->parent_key_idx = -1;
            ct_tree->tree->root = only_child;
            if (ct_tree->tree->level > 0) {
                ct_tree->tree->level--;
            }
            list_del(&parent->link);
            free(parent);
            return 0;
        }

        int parent_slot = parent->parent_key_idx + 1;
        if (parent_slot < 0) {
            parent_slot = 0;
        }

        grand->sub_ptr[parent_slot] = only_child;
        only_child->parent = grand;
        only_child->parent_key_idx = parent->parent_key_idx;

        key_t new_key;
        if (parent_slot > 0 && node_min_key(ct_tree, only_child, &new_key) == 0) {
            grand->key[parent_slot - 1] = new_key;
            if (parent_slot - 1 == 0) {
                propagate_min_key_change((struct bplus_node *)grand, grand->key[0]);
            }
        }

        list_del(&parent->link);
        free(parent);
    }

    return 0;
}


static key_t split_leaf(struct bplus_tree_compressed *ct_tree, struct bplus_leaf *leaf, struct bplus_leaf **new_leaf_out)
{
    // fprintf(stderr, "DEBUG split_leaf: ENTERED split_leaf function\n");
    // fflush(stderr);

    struct simple_leaf_node *custom_leaf = (struct simple_leaf_node *)leaf->data[0];

    // Lock the leaf being split (caller should have tree lock)
    pthread_rwlock_wrlock(&custom_leaf->rwlock);
    // fprintf(stderr, "DEBUG split_leaf: Acquired leaf rwlock\n");
    // fflush(stderr);

    // 1. Create a temporary buffer to hold all key-value pairs.
    int max_pairs = (LANDING_BUFFER_BYTES + COMPRESSED_LEAF_SIZE) / sizeof(struct kv_pair);
    struct kv_pair *all_pairs = malloc(max_pairs * sizeof(struct kv_pair));
    int pair_count = 0;
    // fprintf(stderr, "DEBUG split_leaf: Allocated buffer for %d max pairs\n", max_pairs);
    // fflush(stderr);

    // 2. Decompress the main data area and collect kv-pairs.
    char uncompressed_pages[COMPRESSED_LEAF_SIZE];
    int compressed_pairs = 0;
    if (custom_leaf->is_compressed) {
        // fprintf(stderr, "DEBUG split_leaf: Leaf is compressed, size=%d, num_subpages=%d\n",
        //         custom_leaf->compressed_size, custom_leaf->num_subpages);
        // fflush(stderr);

        // Decompress each sub-page individually using the subpage index
        memset(uncompressed_pages, 0, COMPRESSED_LEAF_SIZE);
        for (int i = 0; i < custom_leaf->num_subpages && custom_leaf->subpage_index; i++) {
            int offset = custom_leaf->subpage_index[i].offset;
            int length = custom_leaf->subpage_index[i].length;
            int sub_page_size = COMPRESSED_LEAF_SIZE / custom_leaf->num_subpages;
            char *dest = uncompressed_pages + i * sub_page_size;

            int decompress_size = decompress_subpage(ct_tree,
                                                     custom_leaf,
                                                     (const uint8_t *)custom_leaf->compressed_data + offset,
                                                     length,
                                                     (uint8_t *)dest,
                                                     sub_page_size);

            if (decompress_size < 0) {
                free(all_pairs);
                pthread_rwlock_unlock(&custom_leaf->rwlock);
                return 0;
            }
        }
        // fprintf(stderr, "DEBUG split_leaf: Successfully decompressed all subpages\n");
        // fflush(stderr);

        struct kv_pair *p = (struct kv_pair *)uncompressed_pages;
        struct kv_pair *end = (struct kv_pair *)(uncompressed_pages + COMPRESSED_LEAF_SIZE);
        while (p < end) {
            if (p->key != 0) {
                all_pairs[pair_count++] = *p;
                compressed_pairs++;
            }
            p++;
        }
        // fprintf(stderr, "DEBUG split_leaf: Found %d pairs in compressed data\n", compressed_pairs);
        // fflush(stderr);
    } else {
        // fprintf(stderr, "DEBUG split_leaf: Leaf is NOT compressed\n");
        // fflush(stderr);
    }

    // 3. Collect kv-pairs from the landing buffer.
    int landing_pairs = 0;
    struct kv_pair *p_landing = (struct kv_pair *)custom_leaf->landing_buffer;
    struct kv_pair *end_landing = (struct kv_pair *)(custom_leaf->landing_buffer + LANDING_BUFFER_BYTES);
    while (p_landing < end_landing) {
        if (p_landing->key != 0) {
            all_pairs[pair_count++] = *p_landing;
            landing_pairs++;
        }
        p_landing++;
    }
    // fprintf(stderr, "DEBUG split_leaf: Found %d pairs in landing buffer\n", landing_pairs);
    // fprintf(stderr, "DEBUG split_leaf: Total pair_count=%d before sort\n", pair_count);
    // fflush(stderr);

    // 4. Sort all collected key-value pairs.
    qsort(all_pairs, pair_count, sizeof(struct kv_pair), compare_kv_pairs);

    // Print first few keys after sorting
    // fprintf(stderr, "DEBUG split_leaf: First 5 keys after sort: ");
    // for (int i = 0; i < 5 && i < pair_count; i++) {
    //     fprintf(stderr, "%d ", all_pairs[i].key);
    // }
    // fprintf(stderr, "\n");
    // fflush(stderr);

    // 5. Reset the original leaf and create a new leaf.
    memset(custom_leaf->landing_buffer, 0, LANDING_BUFFER_BYTES);
    memset(custom_leaf->compressed_data, 0, MAX_COMPRESSED_SIZE);
    custom_leaf->is_compressed = false;
    custom_leaf->compressed_size = 0;

    struct bplus_leaf *new_bplus_leaf = (struct bplus_leaf *)bplus_node_new(ct_tree->tree, BPLUS_TREE_LEAF);
    struct simple_leaf_node *new_custom_leaf = calloc(1, sizeof(struct simple_leaf_node));
    if (!new_custom_leaf) {
        free(all_pairs);
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        return 0;
    }

    new_custom_leaf->compressed_data = calloc(1, MAX_COMPRESSED_SIZE);
    if (!new_custom_leaf->compressed_data) {
        free(new_custom_leaf);
        free(all_pairs);
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        return 0;
    }

    new_custom_leaf->num_subpages = custom_leaf->num_subpages;
    new_custom_leaf->compression_algo = custom_leaf->compression_algo;
    pthread_rwlock_init(&new_custom_leaf->rwlock, NULL);
    new_bplus_leaf->data[0] = (value_t)new_custom_leaf;

    list_add(&new_bplus_leaf->link, &leaf->link);
    new_bplus_leaf->parent = leaf->parent;
    if (leaf->parent) {
        int next_idx = (leaf->parent_key_idx < 0) ? 0 : leaf->parent_key_idx + 1;
        new_bplus_leaf->parent_key_idx = next_idx;
    } else {
        new_bplus_leaf->parent_key_idx = 0;
    }

    *new_leaf_out = new_bplus_leaf;

    // 6. Distribute the sorted pairs into the two leaves.
    int midpoint = pair_count / 2;
    // fprintf(stderr, "DEBUG split_leaf: midpoint=%d\n", midpoint);
    // fflush(stderr);
    for (int i = 0; i < midpoint; i++) {
        insert_into_leaf(ct_tree, custom_leaf, all_pairs[i].key, all_pairs[i].value);
    }
    for (int i = midpoint; i < pair_count; i++) {
        insert_into_leaf(ct_tree, new_custom_leaf, all_pairs[i].key, all_pairs[i].value);
    }

    // 7. The split key is the first key in the new leaf.
    key_t split_key = all_pairs[midpoint].key;
    // fprintf(stderr, "DEBUG split_leaf: Returning split_key=%d\n", split_key);
    // fflush(stderr);
    free(all_pairs);

    pthread_rwlock_unlock(&custom_leaf->rwlock);
    return split_key;
}


int insert_into_leaf(struct bplus_tree_compressed *ct_tree, struct simple_leaf_node *leaf, key_t key, value_t value)
{
    struct kv_pair *landing_begin = (struct kv_pair *)leaf->landing_buffer;
    struct kv_pair *landing_end = (struct kv_pair *)(leaf->landing_buffer + LANDING_BUFFER_BYTES);
    struct kv_pair *slot = landing_begin;
    struct kv_pair *free_slot = NULL;

    while (slot < landing_end) {
        if (slot->key == key) {
            slot->value = value;
            return 0;
        }
        if (slot->key == 0 && free_slot == NULL) {
            free_slot = slot;
        }
        slot++;
    }

    if (free_slot != NULL) {
        free_slot->key = key;
        free_slot->value = value;
        return 0;
    }

    if (ct_tree->debug_mode) {
        fprintf(stderr, "LANDING BUFFER FULL: Compressing for key=%d\n", key);
        fflush(stderr);
    }

    char landing_backup[LANDING_BUFFER_BYTES];
    memcpy(landing_backup, leaf->landing_buffer, LANDING_BUFFER_BYTES);

    if (leaf->num_subpages <= 0) {
        leaf->num_subpages = ct_tree->config.default_sub_pages > 0
                                ? ct_tree->config.default_sub_pages
                                : 1;
    }

    int sub_page_size = COMPRESSED_LEAF_SIZE / leaf->num_subpages;
    if (sub_page_size <= 0) {
        sub_page_size = COMPRESSED_LEAF_SIZE;
    }

    char uncompressed_pages[COMPRESSED_LEAF_SIZE];
    memset(uncompressed_pages, 0, sizeof(uncompressed_pages));

    if (leaf->is_compressed) {
        if (leaf->subpage_index == NULL) {
            return -1;
        }
        for (int bucket = 0; bucket < leaf->num_subpages; bucket++) {
            struct subpage_index_entry *entry = &leaf->subpage_index[bucket];
            if (entry->length <= 0) {
                continue;
            }
            if (entry->offset + entry->length > (size_t)leaf->compressed_size) {
                return -1;
            }
            char *dest = uncompressed_pages + bucket * sub_page_size;
            int rc = decompress_subpage(ct_tree,
                                        leaf,
                                        (const uint8_t *)leaf->compressed_data + entry->offset,
                                        entry->length,
                                        (uint8_t *)dest,
                                        sub_page_size);
            if (rc < 0) {
                return -1;
            }
        }
    }

    struct kv_pair *backup_slot = (struct kv_pair *)landing_backup;
    while (backup_slot < (struct kv_pair *)(landing_backup + LANDING_BUFFER_BYTES)) {
        if (backup_slot->key != 0) {
            int bucket = positive_mod_i32(backup_slot->key, leaf->num_subpages);
            struct kv_pair *bucket_begin = (struct kv_pair *)(uncompressed_pages + bucket * sub_page_size);
            struct kv_pair *bucket_end = bucket_begin + (sub_page_size / (int)sizeof(struct kv_pair));
            struct kv_pair *target = NULL;
            struct kv_pair *cursor = bucket_begin;
            while (cursor < bucket_end) {
                if (cursor->key == backup_slot->key) {
                    target = cursor;
                    break;
                }
                if (cursor->key == 0 && target == NULL) {
                    target = cursor;
                }
                cursor++;
            }
            if (!target) {
                return -1;
            }
            *target = *backup_slot;
        }
        backup_slot++;
    }

    int bucket = positive_mod_i32(key, leaf->num_subpages);
    struct kv_pair *bucket_begin = (struct kv_pair *)(uncompressed_pages + bucket * sub_page_size);
    int bucket_capacity = sub_page_size / (int)sizeof(struct kv_pair);
    bool placed = false;
    for (int i = 0; i < bucket_capacity; i++) {
        if (bucket_begin[i].key == 0 || bucket_begin[i].key == key) {
            bucket_begin[i].key = key;
            bucket_begin[i].value = value;
            placed = true;
            break;
        }
    }
    if (!placed) {
        if (ct_tree->debug_mode) {
            fprintf(stderr, "SPLIT TRIGGERED: bucket %d is FULL (capacity=%d), key=%d\n",
                    bucket, bucket_capacity, key);
            fflush(stderr);
        }
        return -1;
    }

    size_t hashed_pairs = 0;
    for (int b = 0; b < leaf->num_subpages; b++) {
        struct kv_pair *start = (struct kv_pair *)(uncompressed_pages + b * sub_page_size);
        for (int j = 0; j < bucket_capacity; j++) {
            if (start[j].key != 0) {
                hashed_pairs++;
            }
        }
    }

    if (hashed_pairs == 0) {
        memset(leaf->landing_buffer, 0, LANDING_BUFFER_BYTES);
        leaf->is_compressed = false;
        leaf->compressed_size = 0;
        leaf->compressed_bytes = 0;
        leaf->uncompressed_bytes = 0;
        return 0;
    }

    struct subpage_index_entry *temp_index = calloc(leaf->num_subpages, sizeof(struct subpage_index_entry));
    if (!temp_index) {
        return -1;
    }

    char *temp_compressed = malloc(MAX_COMPRESSED_SIZE);
    if (!temp_compressed) {
        free(temp_index);
        return -1;
    }

    size_t running_offset = 0;
    for (int b = 0; b < leaf->num_subpages; b++) {
        struct kv_pair *start = (struct kv_pair *)(uncompressed_pages + b * sub_page_size);
        bool bucket_empty = true;
        for (int j = 0; j < bucket_capacity; j++) {
            if (start[j].key != 0) {
                bucket_empty = false;
                break;
            }
        }

        if (bucket_empty) {
            temp_index[b].offset = running_offset;
            temp_index[b].length = 0;
            continue;
        }

        uint32_t dest_capacity = MAX_COMPRESSED_SIZE - (uint32_t)running_offset;
        if (dest_capacity == 0) {
            free(temp_compressed);
            free(temp_index);
            return -1;
        }

        int compressed_size = compress_subpage(ct_tree,
                                               leaf,
                                               (const uint8_t *)start,
                                               sub_page_size,
                                               (uint8_t *)temp_compressed + running_offset,
                                               dest_capacity);
        if (compressed_size <= 0 || running_offset + (size_t)compressed_size > MAX_COMPRESSED_SIZE) {
            free(temp_compressed);
            free(temp_index);
            return -1;
        }

        temp_index[b].offset = running_offset;
        temp_index[b].length = (uint32_t)compressed_size;
        running_offset += (size_t)compressed_size;
    }

    if (leaf->compressed_data == NULL) {
        leaf->compressed_data = calloc(1, MAX_COMPRESSED_SIZE);
        if (!leaf->compressed_data) {
            free(temp_compressed);
            free(temp_index);
            return -1;
        }
    }

    if (leaf->subpage_index == NULL) {
        leaf->subpage_index = calloc(leaf->num_subpages, sizeof(struct subpage_index_entry));
        if (!leaf->subpage_index) {
            free(temp_compressed);
            free(temp_index);
            return -1;
        }
    }

    memset(leaf->landing_buffer, 0, LANDING_BUFFER_BYTES);
    memcpy(leaf->compressed_data, temp_compressed, running_offset);
    if (running_offset < MAX_COMPRESSED_SIZE) {
        memset(leaf->compressed_data + running_offset, 0, MAX_COMPRESSED_SIZE - running_offset);
    }
    memcpy(leaf->subpage_index, temp_index, leaf->num_subpages * sizeof(struct subpage_index_entry));

    leaf->is_compressed = true;
    leaf->compressed_size = running_offset;
    leaf->compressed_bytes = running_offset;
    leaf->uncompressed_bytes = hashed_pairs * sizeof(struct kv_pair);

    free(temp_compressed);
    free(temp_index);
    return 0;
}

static int ensure_custom_leaf(struct bplus_tree_compressed *ct_tree,
                              struct bplus_leaf *leaf,
                              struct simple_leaf_node **out_leaf)
{
    struct simple_leaf_node *custom_leaf = NULL;

    if (leaf == NULL) {
        return -1;
    }

    if (leaf->entries == 0 && leaf->data[0] != 0) {
        custom_leaf = (struct simple_leaf_node *)leaf->data[0];
        if (out_leaf) {
            *out_leaf = custom_leaf;
        }
        return 0;
    }

    custom_leaf = calloc(1, sizeof(*custom_leaf));
    if (!custom_leaf) {
        return -1;
    }

    custom_leaf->compressed_data = calloc(1, MAX_COMPRESSED_SIZE);
    if (!custom_leaf->compressed_data) {
        free(custom_leaf);
        return -1;
    }

    custom_leaf->num_subpages = ct_tree->config.default_sub_pages;
    custom_leaf->compression_algo = ct_tree->config.algo;
    pthread_rwlock_init(&custom_leaf->rwlock, NULL);

    if (leaf->entries > 0) {
        pthread_rwlock_wrlock(&custom_leaf->rwlock);
        for (int i = 0; i < leaf->entries; i++) {
            if (insert_into_leaf(ct_tree, custom_leaf, leaf->key[i], leaf->data[i]) != 0) {
                pthread_rwlock_unlock(&custom_leaf->rwlock);
                free(custom_leaf->compressed_data);
                pthread_rwlock_destroy(&custom_leaf->rwlock);
                free(custom_leaf);
                return -1;
            }
        }
        pthread_rwlock_unlock(&custom_leaf->rwlock);
    }

    leaf->entries = 0;
    memset(leaf->key, 0, sizeof(leaf->key));
    memset(leaf->data, 0, sizeof(leaf->data));
    leaf->data[0] = (value_t)custom_leaf;

    if (out_leaf) {
        *out_leaf = custom_leaf;
    }

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

static void compressed_non_leaf_simple_insert(struct bplus_non_leaf *node,
                                              struct bplus_node *l_ch,
                                              struct bplus_node *r_ch,
                                              key_t key,
                                              int insert)
{
    int i;
    for (i = node->children - 1; i > insert; i--) {
        node->key[i] = node->key[i - 1];
        node->sub_ptr[i + 1] = node->sub_ptr[i];
        node->sub_ptr[i + 1]->parent_key_idx = i;
    }
    node->key[i] = key;
    node->sub_ptr[i] = l_ch;
    node->sub_ptr[i]->parent_key_idx = i - 1;
    node->sub_ptr[i + 1] = r_ch;
    node->sub_ptr[i + 1]->parent_key_idx = i;
    node->children++;
}

static key_t compressed_non_leaf_split_left(struct bplus_non_leaf *node,
                                            struct bplus_non_leaf *left,
                                            struct bplus_node *l_ch,
                                            struct bplus_node *r_ch,
                                            key_t key,
                                            int insert,
                                            int split)
{
    int i, j, order = node->children;
    key_t split_key;

    __list_add(&left->link, node->link.prev, &node->link);

    for (i = 0, j = 0; i < split + 1; i++, j++) {
        if (j == insert) {
            left->sub_ptr[j] = l_ch;
            left->sub_ptr[j]->parent = left;
            left->sub_ptr[j]->parent_key_idx = j - 1;
            left->sub_ptr[j + 1] = r_ch;
            left->sub_ptr[j + 1]->parent = left;
            left->sub_ptr[j + 1]->parent_key_idx = j;
            j++;
        } else {
            left->sub_ptr[j] = node->sub_ptr[i];
            left->sub_ptr[j]->parent = left;
            left->sub_ptr[j]->parent_key_idx = j - 1;
        }
    }
    left->children = split + 1;

    for (i = 0, j = 0; i < split; j++) {
        if (j == insert) {
            left->key[j] = key;
        } else {
            left->key[j] = node->key[i];
            i++;
        }
    }
    if (insert == split) {
        left->key[insert] = key;
        left->sub_ptr[insert] = l_ch;
        left->sub_ptr[insert]->parent = left;
        left->sub_ptr[insert]->parent_key_idx = j - 1;
        node->sub_ptr[0] = r_ch;
        split_key = key;
    } else {
        node->sub_ptr[0] = node->sub_ptr[split];
        split_key = node->key[split - 1];
    }
    node->sub_ptr[0]->parent = node;
    node->sub_ptr[0]->parent_key_idx = -1;

    for (i = split, j = 0; i < order - 1; i++, j++) {
        node->key[j] = node->key[i];
        node->sub_ptr[j + 1] = node->sub_ptr[i + 1];
        node->sub_ptr[j + 1]->parent = node;
        node->sub_ptr[j + 1]->parent_key_idx = j;
    }
    node->sub_ptr[j] = node->sub_ptr[i];
    node->children = j + 1;

    return split_key;
}

static key_t compressed_non_leaf_split_right1(struct bplus_non_leaf *node,
                                              struct bplus_non_leaf *right,
                                              struct bplus_node *l_ch,
                                              struct bplus_node *r_ch,
                                              key_t key,
                                              int insert,
                                              int split)
{
    int i, j, order = node->children;
    key_t split_key;

    list_add(&right->link, &node->link);
    split_key = node->key[split - 1];
    node->children = split;

    right->key[0] = key;
    right->sub_ptr[0] = l_ch;
    right->sub_ptr[0]->parent = right;
    right->sub_ptr[0]->parent_key_idx = -1;
    right->sub_ptr[1] = r_ch;
    right->sub_ptr[1]->parent = right;
    right->sub_ptr[1]->parent_key_idx = 0;

    for (i = split, j = 1; i < order - 1; i++, j++) {
        right->key[j] = node->key[i];
        right->sub_ptr[j + 1] = node->sub_ptr[i + 1];
        right->sub_ptr[j + 1]->parent = right;
        right->sub_ptr[j + 1]->parent_key_idx = j;
    }
    right->children = j + 1;

    return split_key;
}

static key_t compressed_non_leaf_split_right2(struct bplus_non_leaf *node,
                                              struct bplus_non_leaf *right,
                                              struct bplus_node *l_ch,
                                              struct bplus_node *r_ch,
                                              key_t key,
                                              int insert,
                                              int split)
{
    int i, j, order = node->children;
    key_t split_key;

    node->children = split + 1;
    list_add(&right->link, &node->link);
    split_key = node->key[split];

    right->sub_ptr[0] = node->sub_ptr[split + 1];
    right->sub_ptr[0]->parent = right;
    right->sub_ptr[0]->parent_key_idx = -1;

    for (i = split + 1, j = 0; i < order - 1; j++) {
        if (j != insert - split - 1) {
            right->key[j] = node->key[i];
            right->sub_ptr[j + 1] = node->sub_ptr[i + 1];
            right->sub_ptr[j + 1]->parent = right;
            right->sub_ptr[j + 1]->parent_key_idx = j;
            i++;
        }
    }

    if (j > insert - split - 1) {
        right->children = j + 1;
    } else {
        assert(j == insert - split - 1);
        right->children = j + 2;
    }

    j = insert - split - 1;
    right->key[j] = key;
    right->sub_ptr[j] = l_ch;
    right->sub_ptr[j]->parent = right;
    right->sub_ptr[j]->parent_key_idx = j - 1;
    right->sub_ptr[j + 1] = r_ch;
    right->sub_ptr[j + 1]->parent = right;
    right->sub_ptr[j + 1]->parent_key_idx = j;

    return split_key;
}

static int compressed_parent_node_build(struct bplus_tree *tree,
                                        struct bplus_node *left,
                                        struct bplus_node *right,
                                        key_t key,
                                        int level)
{
    if (left->parent == NULL && right->parent == NULL) {
        struct bplus_non_leaf *parent = (struct bplus_non_leaf *)bplus_node_new(tree, BPLUS_TREE_NON_LEAF);
        parent->key[0] = key;
        parent->sub_ptr[0] = left;
        parent->sub_ptr[0]->parent = parent;
        parent->sub_ptr[0]->parent_key_idx = -1;
        parent->sub_ptr[1] = right;
        parent->sub_ptr[1]->parent = parent;
        parent->sub_ptr[1]->parent_key_idx = 0;
        parent->children = 2;
        tree->root = (struct bplus_node *)parent;
        list_add(&parent->link, &tree->list[++tree->level]);
        return 0;
    } else if (right->parent == NULL) {
        right->parent = left->parent;
        return compressed_non_leaf_insert(tree, left->parent, left, right, key, level + 1);
    } else {
        left->parent = right->parent;
        return compressed_non_leaf_insert(tree, right->parent, left, right, key, level + 1);
    }
}

static int compressed_non_leaf_insert(struct bplus_tree *tree,
                                      struct bplus_non_leaf *node,
                                      struct bplus_node *l_ch,
                                      struct bplus_node *r_ch,
                                      key_t key,
                                      int level)
{
    int insert = compressed_key_binary_search(node->key, node->children - 1, key);
    assert(insert < 0);
    insert = -insert - 1;

    if (node->children == tree->order) {
        key_t split_key;
        int split = node->children / 2;
        struct bplus_non_leaf *sibling = (struct bplus_non_leaf *)bplus_node_new(tree, BPLUS_TREE_NON_LEAF);

        if (insert < split) {
            split_key = compressed_non_leaf_split_left(node, sibling, l_ch, r_ch, key, insert, split);
        } else if (insert == split) {
            split_key = compressed_non_leaf_split_right1(node, sibling, l_ch, r_ch, key, insert, split);
        } else {
            split_key = compressed_non_leaf_split_right2(node, sibling, l_ch, r_ch, key, insert, split);
        }

        if (insert < split) {
            return compressed_parent_node_build(tree, (struct bplus_node *)sibling,
                                               (struct bplus_node *)node, split_key, level);
        } else {
            return compressed_parent_node_build(tree, (struct bplus_node *)node,
                                               (struct bplus_node *)sibling, split_key, level);
        }
    } else {
        compressed_non_leaf_simple_insert(node, l_ch, r_ch, key, insert);
        l_ch->parent = node;
        r_ch->parent = node;
        return 0;
    }
}

static int bplus_tree_insert_internal(struct bplus_tree *tree,
                                      key_t key,
                                      struct bplus_node *left,
                                      struct bplus_node *right)
{
    return compressed_parent_node_build(tree, left, right, key, 0);
}


static int hash_key_to_sub_page(key_t key, int num_sub_pages) {
    if (num_sub_pages == 0) return 0;
    return key % num_sub_pages;
}


int bplus_tree_compressed_put(struct bplus_tree_compressed *ct_tree, key_t key, int data)
{
    if (data == 0) {
        return bplus_tree_compressed_delete(ct_tree, key);
    }
    return bplus_tree_compressed_put_internal(ct_tree, key, data);
}

static int bplus_tree_compressed_put_internal(struct bplus_tree_compressed *ct_tree,
                                              key_t key,
                                              int data)
{
    if (ct_tree == NULL || !ct_tree->initialized || ct_tree->tree == NULL) {
        return -1;
    }

    // Use tree-level lock only for finding/creating the leaf
    pthread_rwlock_wrlock(&ct_tree->rwlock);

    struct bplus_leaf *leaf = find_leaf_for_key(ct_tree->tree, key);
    if (leaf == NULL) {
        // Tree is empty, create the first leaf.
        leaf = (struct bplus_leaf *)bplus_node_new(ct_tree->tree, BPLUS_TREE_LEAF);
        ct_tree->tree->root = (struct bplus_node *)leaf;
        list_add(&leaf->link, &ct_tree->tree->list[0]);
    }

    struct simple_leaf_node *custom_leaf = NULL;
    if (ensure_custom_leaf(ct_tree, leaf, &custom_leaf) != 0 || custom_leaf == NULL) {
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return -1;
    }

    // Now lock this specific leaf
    pthread_rwlock_wrlock(&custom_leaf->rwlock);
    pthread_rwlock_unlock(&ct_tree->rwlock);  // Release tree lock

    size_t old_uncompressed = custom_leaf->uncompressed_bytes;
    size_t old_compressed = custom_leaf->compressed_bytes;

    int result = insert_into_leaf(ct_tree, custom_leaf, key, data);

    // Update global statistics if compression happened
    if (result == 0 && custom_leaf->is_compressed) {
        pthread_rwlock_wrlock(&ct_tree->rwlock);
        ct_tree->total_uncompressed_size += (custom_leaf->uncompressed_bytes - old_uncompressed);
        ct_tree->total_compressed_size += (custom_leaf->compressed_bytes - old_compressed);
        ct_tree->compression_operations++;
        pthread_rwlock_unlock(&ct_tree->rwlock);
    }

    if (result == -1) {
        // Need to split - reacquire tree lock for structural modification
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        pthread_rwlock_wrlock(&ct_tree->rwlock);

        if (ct_tree->debug_mode) {
            fprintf(stderr, "=== EXECUTING SPLIT for key %d ===\n", key);
            fflush(stderr);
        }
        struct bplus_leaf *new_leaf = NULL;
        key_t split_key = split_leaf(ct_tree, leaf, &new_leaf);
        if (ct_tree->debug_mode) {
            fprintf(stderr, "=== SPLIT COMPLETE, split_key=%d ===\n", split_key);
            fflush(stderr);
        }

        // Insert the split key into the parent, creating new internal nodes as needed
        // This modifies tree structure but doesn't touch leaf data
        struct bplus_node *left_node = (struct bplus_node *)leaf;
        struct bplus_node *right_node = (struct bplus_node *)new_leaf;

        // Insert split key into parent (handles internal node structure only)
        int parent_rc = bplus_tree_insert_internal(ct_tree->tree, split_key, left_node, right_node);
        if (parent_rc != 0) {
            pthread_rwlock_unlock(&ct_tree->rwlock);
            return parent_rc;
        }

        pthread_rwlock_unlock(&ct_tree->rwlock);

        // Retry the insert on the appropriate leaf
        if (key < split_key) {
            pthread_rwlock_wrlock(&custom_leaf->rwlock);
            result = insert_into_leaf(ct_tree, custom_leaf, key, data);
            pthread_rwlock_unlock(&custom_leaf->rwlock);
        } else {
            struct simple_leaf_node *new_custom_leaf = (struct simple_leaf_node*)new_leaf->data[0];
            pthread_rwlock_wrlock(&new_custom_leaf->rwlock);
            result = insert_into_leaf(ct_tree, new_custom_leaf, key, data);
            pthread_rwlock_unlock(&new_custom_leaf->rwlock);
        }
    } else {
        pthread_rwlock_unlock(&custom_leaf->rwlock);
    }

    return result;
}


int bplus_tree_compressed_get(struct bplus_tree_compressed *ct_tree, key_t key)
{
    if (ct_tree == NULL || !ct_tree->initialized || ct_tree->tree == NULL) {
        return -1;
    }

    // Use tree-level lock only for finding the leaf
    pthread_rwlock_rdlock(&ct_tree->rwlock);
    struct bplus_leaf *leaf = find_leaf_for_key(ct_tree->tree, key);
    if (!leaf) {
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return -1;
    }

    struct simple_leaf_node *custom_leaf = (struct simple_leaf_node*)leaf->data[0];

    if (leaf->entries > 0 || custom_leaf == NULL) {
        pthread_rwlock_unlock(&ct_tree->rwlock);
        pthread_rwlock_wrlock(&ct_tree->rwlock);

        leaf = find_leaf_for_key(ct_tree->tree, key);
        if (!leaf) {
            pthread_rwlock_unlock(&ct_tree->rwlock);
            return -1;
        }

        if (ensure_custom_leaf(ct_tree, leaf, &custom_leaf) != 0 || custom_leaf == NULL) {
            pthread_rwlock_unlock(&ct_tree->rwlock);
            return -1;
        }
    }

    // Lock this specific leaf for reading
    pthread_rwlock_rdlock(&custom_leaf->rwlock);
    pthread_rwlock_unlock(&ct_tree->rwlock);  // Release tree lock

    // 1. Search landing buffer
    struct kv_pair *p = (struct kv_pair *)custom_leaf->landing_buffer;
    struct kv_pair *end = (struct kv_pair *)(custom_leaf->landing_buffer + LANDING_BUFFER_BYTES);
    int landing_count = 0;
    key_t first_key = -1, last_key = -1;
    while (p < end) {
        if (p->key != 0) {
            landing_count++;
            if (first_key == -1) first_key = p->key;
            last_key = p->key;
        }
        if (p->key == key) {
            int value = p->value;
            pthread_rwlock_unlock(&custom_leaf->rwlock);
            // fprintf(stderr, "DEBUG get: Found key=%d in landing buffer, value=%d\n", key, value);
            // fflush(stderr);
            return value;
        }
        p++;
    }

    // fprintf(stderr, "DEBUG get: key=%d not in landing buffer (has %d keys from %d to %d), is_compressed=%d\n",
    //         key, landing_count, first_key, last_key, custom_leaf->is_compressed);
    // fflush(stderr);

    // 2. Search compressed sub-pages
    if (!custom_leaf->is_compressed) {
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        // fprintf(stderr, "DEBUG get: Leaf not compressed, returning -1\n");
        // fflush(stderr);
        return -1; // Not in buffer and not compressed means not found
    }

    // fprintf(stderr, "DEBUG get: key=%d, algo=%d, is_compressed=%d, subpage_index=%p\n",
    //         key, ct_tree->config.algo, custom_leaf->is_compressed, custom_leaf->subpage_index);
    // fflush(stderr);

    int result = -1;
    if (custom_leaf->subpage_index == NULL || custom_leaf->num_subpages <= 0) {
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        return -1;
    }

    int target_bucket = hash_key_to_sub_page(key, custom_leaf->num_subpages);
    int sub_page_size = COMPRESSED_LEAF_SIZE / custom_leaf->num_subpages;
    if (sub_page_size <= 0) {
        sub_page_size = COMPRESSED_LEAF_SIZE;
    }

    struct subpage_index_entry *entry = &custom_leaf->subpage_index[target_bucket];
    if (!entry || entry->length <= 0) {
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        return -1;
    }

    char *buffer = malloc(sub_page_size);
    if (!buffer) {
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        return -1;
    }

    int rc = decompress_subpage(ct_tree,
                                custom_leaf,
                                (const uint8_t *)custom_leaf->compressed_data + entry->offset,
                                entry->length,
                                (uint8_t *)buffer,
                                sub_page_size);
    if (rc < 0) {
        free(buffer);
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        return -1;
    }

    struct kv_pair *sp = (struct kv_pair *)buffer;
    struct kv_pair *sp_end = sp + (sub_page_size / (int)sizeof(struct kv_pair));
    while (sp < sp_end) {
        if (sp->key == key) {
            result = sp->value;
            break;
        }
        sp++;
    }
    free(buffer);

    pthread_rwlock_unlock(&custom_leaf->rwlock);
    return result;
}

int bplus_tree_compressed_get_range(struct bplus_tree_compressed *ct_tree, key_t key1, key_t key2)
{
    if (ct_tree == NULL || !ct_tree->initialized || ct_tree->tree == NULL) {
        return -1;
    }
    if (ct_tree->tree->root == NULL) {
        return -1;
    }

    key_t min_key = key1 <= key2 ? key1 : key2;
    key_t max_key = key1 <= key2 ? key2 : key1;

    pthread_rwlock_rdlock(&ct_tree->rwlock);

    struct bplus_leaf *leaf = find_leaf_for_key(ct_tree->tree, min_key);
    if (!leaf) {
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return -1;
    }

    int last_value = -1;
    bool found = false;
    bool stop = false;

    while (leaf && !stop) {
        struct simple_leaf_node *custom_leaf = (struct simple_leaf_node *)leaf->data[0];
        if (custom_leaf == NULL) {
            if (list_is_last(&leaf->link, &ct_tree->tree->list[0])) {
                break;
            }
            leaf = list_next_entry(leaf, link);
            continue;
        }

        pthread_rwlock_rdlock(&custom_leaf->rwlock);

        struct kv_pair *leaf_pairs = NULL;
        size_t leaf_count = 0;
        size_t leaf_capacity = 0;
        int rc = 0;

        if (custom_leaf->is_compressed && custom_leaf->subpage_index && custom_leaf->num_subpages > 0) {
            int sub_page_size = COMPRESSED_LEAF_SIZE / custom_leaf->num_subpages;
            if (sub_page_size <= 0) {
                sub_page_size = COMPRESSED_LEAF_SIZE;
            }

            for (int bucket = 0; bucket < custom_leaf->num_subpages; bucket++) {
                if (!subpage_needed_for_range(min_key, max_key, bucket, custom_leaf->num_subpages)) {
                    continue;
                }

                struct subpage_index_entry *entry = &custom_leaf->subpage_index[bucket];
                if (!entry || entry->length <= 0) {
                    continue;
                }

                char *sub_page_buffer = malloc(sub_page_size);
                if (!sub_page_buffer) {
                    rc = -1;
                    break;
                }

                int decompress_size = decompress_subpage(ct_tree,
                                                         custom_leaf,
                                                         (const uint8_t *)custom_leaf->compressed_data + entry->offset,
                                                         entry->length,
                                                         (uint8_t *)sub_page_buffer,
                                                         sub_page_size);

                if (decompress_size < 0) {
                    free(sub_page_buffer);
                    rc = -1;
                    break;
                }

                struct kv_pair *sp = (struct kv_pair *)sub_page_buffer;
                struct kv_pair *sp_end = (struct kv_pair *)(sub_page_buffer + sub_page_size);
                while (sp < sp_end) {
                    if (sp->key != 0) {
                        if (kv_vector_put(&leaf_pairs, &leaf_count, &leaf_capacity, sp->key, sp->value) != 0) {
                            rc = -1;
                            break;
                        }
                    }
                    sp++;
                }

                free(sub_page_buffer);

                if (rc != 0) {
                    break;
                }
            }
        }

        if (rc == 0) {
            struct kv_pair *landing = (struct kv_pair *)custom_leaf->landing_buffer;
            struct kv_pair *landing_end = (struct kv_pair *)(custom_leaf->landing_buffer + LANDING_BUFFER_BYTES);
            while (landing < landing_end) {
                if (landing->key != 0) {
                    if (kv_vector_put(&leaf_pairs, &leaf_count, &leaf_capacity, landing->key, landing->value) != 0) {
                        rc = -1;
                        break;
                    }
                }
                landing++;
            }
        }

        pthread_rwlock_unlock(&custom_leaf->rwlock);

        if (rc != 0) {
            free(leaf_pairs);
            pthread_rwlock_unlock(&ct_tree->rwlock);
            return -1;
        }

        if (leaf_count == 0) {
            free(leaf_pairs);
            if (list_is_last(&leaf->link, &ct_tree->tree->list[0])) {
                break;
            }
            leaf = list_next_entry(leaf, link);
            continue;
        }

        qsort(leaf_pairs, leaf_count, sizeof(struct kv_pair), compare_kv_pairs);

        key_t leaf_max_key = leaf_pairs[leaf_count - 1].key;

        for (size_t i = 0; i < leaf_count; i++) {
            if (leaf_pairs[i].key < min_key) {
                continue;
            }
            if (leaf_pairs[i].key > max_key) {
                stop = true;
                break;
            }
            found = true;
            last_value = (int)leaf_pairs[i].value;
        }

        free(leaf_pairs);

        if (stop || leaf_max_key > max_key) {
            break;
        }

        if (list_is_last(&leaf->link, &ct_tree->tree->list[0])) {
            break;
        }
        leaf = list_next_entry(leaf, link);
    }

    pthread_rwlock_unlock(&ct_tree->rwlock);
    return found ? last_value : -1;
}

int bplus_tree_compressed_delete(struct bplus_tree_compressed *ct_tree, key_t key)
{
    if (ct_tree == NULL || !ct_tree->initialized || ct_tree->tree == NULL) {
        return -1;
    }

    pthread_rwlock_wrlock(&ct_tree->rwlock);

    struct bplus_leaf *leaf = find_leaf_for_key(ct_tree->tree, key);
    if (!leaf || leaf->data[0] == 0) {
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return -1;
    }

    struct simple_leaf_node *custom_leaf = (struct simple_leaf_node *)leaf->data[0];
    pthread_rwlock_wrlock(&custom_leaf->rwlock);

    struct kv_pair *pairs = NULL;
    size_t count = 0;
    if (compressed_leaf_collect_pairs(ct_tree, custom_leaf, &pairs, &count) != 0) {
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return -1;
    }

    if (count == 0) {
        free(pairs);
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return -1;
    }

    qsort(pairs, count, sizeof(struct kv_pair), compare_kv_pairs);

    key_t old_min = pairs[0].key;
    ssize_t remove_index = -1;
    ssize_t low = 0;
    ssize_t high = (ssize_t)count - 1;
    while (low <= high) {
        ssize_t mid = low + (high - low) / 2;
        if (pairs[mid].key == key) {
            remove_index = mid;
            break;
        } else if (pairs[mid].key < key) {
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }

    if (remove_index < 0) {
        free(pairs);
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return -1;
    }

    memmove(&pairs[remove_index], &pairs[remove_index + 1],
            (count - (size_t)remove_index - 1) * sizeof(struct kv_pair));

    size_t new_count = count - 1;
    key_t new_min = 0;
    bool min_changed = false;
    if (new_count > 0) {
        new_min = pairs[0].key;
        min_changed = (new_min != old_min);
    }

    int rebuild_rc = compressed_leaf_rebuild_with_pairs(ct_tree, custom_leaf, pairs, new_count);
    free(pairs);
    if (rebuild_rc != 0) {
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return -1;
    }

    bool leaf_empty = (new_count == 0);
    bool rebalanced = false;

    if (leaf_empty) {
        struct bplus_leaf *left_leaf = list_is_first(&leaf->link, &ct_tree->tree->list[0])
                                      ? NULL : list_prev_entry(leaf, link);
        struct bplus_leaf *right_leaf = list_is_last(&leaf->link, &ct_tree->tree->list[0])
                                       ? NULL : list_next_entry(leaf, link);

        if (left_leaf && left_leaf->data[0] != 0 && !rebalanced) {
            struct simple_leaf_node *left_custom = (struct simple_leaf_node *)left_leaf->data[0];
            pthread_rwlock_wrlock(&left_custom->rwlock);
            struct kv_pair *left_pairs = NULL;
            size_t left_count = 0;
            if (compressed_leaf_collect_pairs(ct_tree, left_custom, &left_pairs, &left_count) == 0 && left_count > 1) {
                qsort(left_pairs, left_count, sizeof(struct kv_pair), compare_kv_pairs);
                struct kv_pair borrowed = left_pairs[left_count - 1];
                left_count--;
                if (compressed_leaf_rebuild_with_pairs(ct_tree, left_custom, left_pairs, left_count) == 0) {
                    struct kv_pair temp[1];
                    temp[0] = borrowed;
                    if (compressed_leaf_rebuild_with_pairs(ct_tree, custom_leaf, temp, 1) == 0) {
                        new_count = 1;
                        new_min = borrowed.key;
                        min_changed = true;
                        leaf_empty = false;
                        rebalanced = true;
                    }
                }
            }
            free(left_pairs);
            pthread_rwlock_unlock(&left_custom->rwlock);
        }

        if (!rebalanced && right_leaf && right_leaf->data[0] != 0) {
            struct simple_leaf_node *right_custom = (struct simple_leaf_node *)right_leaf->data[0];
            pthread_rwlock_wrlock(&right_custom->rwlock);
            struct kv_pair *right_pairs = NULL;
            size_t right_count = 0;
            if (compressed_leaf_collect_pairs(ct_tree, right_custom, &right_pairs, &right_count) == 0 && right_count > 1) {
                qsort(right_pairs, right_count, sizeof(struct kv_pair), compare_kv_pairs);
                struct kv_pair borrowed = right_pairs[0];
                memmove(&right_pairs[0], &right_pairs[1], (right_count - 1) * sizeof(struct kv_pair));
                right_count--;
                if (compressed_leaf_rebuild_with_pairs(ct_tree, right_custom, right_pairs, right_count) == 0) {
                    struct kv_pair temp[1];
                    temp[0] = borrowed;
                    if (compressed_leaf_rebuild_with_pairs(ct_tree, custom_leaf, temp, 1) == 0) {
                        new_count = 1;
                        new_min = borrowed.key;
                        min_changed = true;
                        leaf_empty = false;
                        rebalanced = true;

                        if (right_count > 0) {
                            key_t right_new_min = right_pairs[0].key;
                            propagate_min_key_change((struct bplus_node *)right_leaf, right_new_min);
                        }
                    }
                }
            }
            free(right_pairs);
            pthread_rwlock_unlock(&right_custom->rwlock);
        }
    }

    if (leaf_empty) {
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        leaf->data[0] = 0;
        compressed_leaf_free(custom_leaf);
        remove_leaf_from_parent(ct_tree, leaf);
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return 0;
    }

    if (min_changed) {
        propagate_min_key_change((struct bplus_node *)leaf, new_min);
    }

    pthread_rwlock_unlock(&custom_leaf->rwlock);
    pthread_rwlock_unlock(&ct_tree->rwlock);
    return 0;
}

int bplus_tree_compressed_stats(struct bplus_tree_compressed *ct_tree,
                                size_t *total_size, size_t *compressed_size)
{
    if (ct_tree == NULL || !ct_tree->initialized) {
        return -1;
    }

    pthread_rwlock_rdlock(&ct_tree->rwlock);

    if (total_size) {
        *total_size = ct_tree->total_uncompressed_size;
    }
    if (compressed_size) {
        *compressed_size = ct_tree->total_compressed_size;
    }

    pthread_rwlock_unlock(&ct_tree->rwlock);
    return 0;
}

int bplus_tree_compressed_calculate_stats(struct bplus_tree_compressed *ct_tree,
                                          size_t *total_size, size_t *compressed_size)
{
    if (ct_tree == NULL || !ct_tree->initialized || ct_tree->tree == NULL) {
        return -1;
    }

    pthread_rwlock_rdlock(&ct_tree->rwlock);

    size_t total_uncompressed = 0;
    size_t total_compressed = 0;
    size_t landing_buffer_bytes = 0;

    // Walk through all leaf nodes
    struct list_head *head = &ct_tree->tree->list[0];
    struct list_head *pos, *n;
    int leaf_count = 0;
    list_for_each_safe(pos, n, head) {
        struct bplus_leaf *leaf = list_entry(pos, struct bplus_leaf, link);
        if (leaf->type != BPLUS_TREE_LEAF) {
            continue;
        }
        if (leaf->data[0] == 0) {
            continue;
        }

        leaf_count++;
        struct simple_leaf_node *custom_leaf = (struct simple_leaf_node *)leaf->data[0];
        pthread_rwlock_rdlock(&custom_leaf->rwlock);

        // Count key-value pairs in landing buffer
        size_t landing_pairs = 0;
        struct kv_pair *p = (struct kv_pair *)custom_leaf->landing_buffer;
        struct kv_pair *end = (struct kv_pair *)(custom_leaf->landing_buffer + LANDING_BUFFER_BYTES);
        while (p < end) {
            if (p->key != 0) {
                landing_pairs++;
            }
            p++;
        }
        landing_buffer_bytes += landing_pairs * sizeof(struct kv_pair);

        // Add compressed data statistics
        if (custom_leaf->is_compressed) {
            total_uncompressed += custom_leaf->uncompressed_bytes;
            total_compressed += custom_leaf->compressed_bytes;
            if (ct_tree->debug_mode) {
                printf("        Leaf %d: COMPRESSED uncomp=%zu comp=%zu landing_pairs=%zu\n",
                       leaf_count, custom_leaf->uncompressed_bytes, custom_leaf->compressed_bytes, landing_pairs);
            }
        } else if (ct_tree->debug_mode && landing_pairs > 0) {
            printf("        Leaf %d: uncompressed landing_pairs=%zu\n",
                   leaf_count, landing_pairs);
        }

        pthread_rwlock_unlock(&custom_leaf->rwlock);
    }

    pthread_rwlock_unlock(&ct_tree->rwlock);

    // Temporary debug output
    if (ct_tree->debug_mode) {
        printf("      [TOTAL: leaves=%d uncomp=%zu comp=%zu landing=%zu]\n",
                leaf_count, total_uncompressed, total_compressed, landing_buffer_bytes);
        fflush(stdout);
    }

    if (total_size) {
        *total_size = total_uncompressed + landing_buffer_bytes;
    }
    if (compressed_size) {
        *compressed_size = total_compressed + landing_buffer_bytes;
    }

    return 0;
}


struct compression_config bplus_tree_create_default_leaf_config(leaf_layout_t default_layout)
{
    struct compression_config config;
    config.default_layout = default_layout;
    config.algo = COMPRESS_LZ4;
    config.default_sub_pages = 16;
    config.compression_level = 1;
    config.buffer_size = LANDING_BUFFER_BYTES;
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
    ct_tree->debug_mode = 0;  // Debug off by default
    ct_tree->config = *config;

    if (init_qpl(ct_tree) != 0) {
        fprintf(stderr, "Warning: QPL initialization failed, QPL layouts will not be available\n");
    } else {
        fprintf(stderr, "QPL initialization successful\n");
    }

    return ct_tree;
}

void bplus_tree_compressed_set_debug(struct bplus_tree_compressed *ct_tree, int enable)
{
    if (ct_tree != NULL && ct_tree->initialized) {
        ct_tree->debug_mode = enable ? 1 : 0;
    }
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
