#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stddef.h>
#include <limits.h>
#include <time.h>
#include <unistd.h>
#ifdef HAVE_ZLIB
#include <zlib.h>
#endif
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
static qpl_job *acquire_qpl_job(struct bplus_tree_compressed *ct_tree, int *job_index_out);
static void release_qpl_job(struct bplus_tree_compressed *ct_tree, int job_index);
static qpl_job *acquire_qpl_tls_job(struct bplus_tree_compressed *ct_tree, int is_compress);
static int compressed_tree_is_sharded(const struct bplus_tree_compressed *ct_tree);
static struct bplus_tree_compressed *compressed_tree_shard_for_key(struct bplus_tree_compressed *ct_tree,
                                                                  key_t key);
static int hash_key_to_sub_page(key_t key, int num_sub_pages);
static int bplus_tree_compressed_put_internal(struct bplus_tree_compressed *ct_tree,
                                              key_t key,
                                              int data,
                                              const uint8_t *payload,
                                              size_t payload_len);
static int insert_into_leaf_maybe_out_of_lock(struct bplus_tree_compressed *ct_tree,
                                              struct simple_leaf_node *leaf,
                                              key_t key,
                                              int stored_value,
                                              const uint8_t *payload,
                                              size_t payload_len,
                                              int *handled,
                                              int *result,
                                              size_t *old_uncompressed,
                                              size_t *old_compressed);
static int split_leaf(struct bplus_tree_compressed *ct_tree,
                      struct bplus_leaf *leaf,
                      struct bplus_leaf **new_leaf_out,
                      key_t *split_key_out);
static struct bplus_tree_compressed *bplus_tree_compressed_init_internal(int order,
                                                                        int entries,
                                                                        struct compression_config *config,
                                                                        int allow_sharding);
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

    if (compressed_tree_is_sharded(ct_tree)) {
        for (int i = 0; i < ct_tree->shard_count; i++) {
            bplus_tree_compressed_deinit(ct_tree->shards[i]);
        }
        free(ct_tree->shards);
        ct_tree->shards = NULL;
        ct_tree->shard_count = 0;
        ct_tree->initialized = 0;
        free(ct_tree);
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
// Stores both the API-visible integer value and an inline payload for compression.
struct kv_pair {
    key_t key;
    int stored_value;
    uint8_t payload[COMPRESSED_VALUE_BYTES];
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
                         int stored_value,
                         const uint8_t *payload,
                         size_t payload_len)
{
    for (size_t i = 0; i < *count; i++) {
        if ((*pairs)[i].key == key) {
            (*pairs)[i].stored_value = stored_value;
            if (payload && payload_len > 0) {
                size_t copy_len = payload_len > COMPRESSED_VALUE_BYTES
                                      ? COMPRESSED_VALUE_BYTES
                                      : payload_len;
                memcpy((*pairs)[i].payload, payload, copy_len);
                if (copy_len < COMPRESSED_VALUE_BYTES) {
                    memset((*pairs)[i].payload + copy_len, 0, COMPRESSED_VALUE_BYTES - copy_len);
                }
            }
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
    (*pairs)[*count].stored_value = stored_value;
    if (payload && payload_len > 0) {
        size_t copy_len = payload_len > COMPRESSED_VALUE_BYTES ? COMPRESSED_VALUE_BYTES : payload_len;
        memcpy((*pairs)[*count].payload, payload, copy_len);
        if (copy_len < COMPRESSED_VALUE_BYTES) {
            memset((*pairs)[*count].payload + copy_len, 0, COMPRESSED_VALUE_BYTES - copy_len);
        }
    } else {
        memset((*pairs)[*count].payload, 0, COMPRESSED_VALUE_BYTES);
        memcpy((*pairs)[*count].payload, &stored_value,
               sizeof(stored_value) > COMPRESSED_VALUE_BYTES ? COMPRESSED_VALUE_BYTES : sizeof(stored_value));
    }
    (*count)++;
    return 0;
}

static int normalize_landing_buffer_bytes(int requested)
{
    if (requested <= 0) {
        requested = LANDING_BUFFER_DEFAULT_BYTES;
    }
    if (requested > LANDING_BUFFER_BYTES) {
        requested = LANDING_BUFFER_BYTES;
    }
    int slot_size = (int)sizeof(struct kv_pair);
    if (requested < slot_size) {
        requested = slot_size;
    }
    requested -= requested % slot_size;
    if (requested < slot_size) {
        requested = slot_size;
    }
    return requested;
}

static int landing_buffer_bytes_for_tree(const struct bplus_tree_compressed *ct_tree)
{
    if (!ct_tree) {
        return LANDING_BUFFER_DEFAULT_BYTES;
    }
    return normalize_landing_buffer_bytes(ct_tree->config.buffer_size);
}

static int landing_buffer_capacity_for_tree(const struct bplus_tree_compressed *ct_tree)
{
    return landing_buffer_bytes_for_tree(ct_tree) / (int)sizeof(struct kv_pair);
}

static void apply_landing_buffer_env(struct compression_config *config)
{
    if (!config) {
        return;
    }
    const char *value = getenv("BTREE_LANDING_BUFFER_BYTES");
    if (value && *value) {
        char *end = NULL;
        errno = 0;
        long parsed = strtol(value, &end, 10);
        if (errno == 0 && end != value && *end == '\0' && parsed > 0 && parsed <= INT_MAX) {
            config->buffer_size = (int)parsed;
        } else {
            fprintf(stderr,
                    "Invalid BTREE_LANDING_BUFFER_BYTES=%s; using default %d\n",
                    value,
                    LANDING_BUFFER_DEFAULT_BYTES);
            config->buffer_size = LANDING_BUFFER_DEFAULT_BYTES;
        }
    }
    config->buffer_size = normalize_landing_buffer_bytes(config->buffer_size);
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

static int parse_shard_count(void)
{
    const char *value = getenv("BTREE_SHARDS");
    if (!value || !*value) {
        return 1;
    }

    char *end = NULL;
    errno = 0;
    long parsed = strtol(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed < 1 || parsed > 1024) {
        fprintf(stderr, "Invalid BTREE_SHARDS=%s; expected integer in [1, 1024]\n", value);
        return 1;
    }

    return (int)parsed;
}

static int parse_range_point_lookup_threshold(void)
{
    static int cached_threshold = -1;
    int cached = __atomic_load_n(&cached_threshold, __ATOMIC_RELAXED);
    if (cached >= 0) {
        return cached;
    }

    const char *value = getenv("BTREE_RANGE_POINT_LOOKUP_THRESHOLD");
    if (!value || !*value) {
        __atomic_store_n(&cached_threshold, 256, __ATOMIC_RELAXED);
        return 256;
    }

    char *end = NULL;
    errno = 0;
    long parsed = strtol(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed < 0 || parsed > INT_MAX) {
        fprintf(stderr,
                "Invalid BTREE_RANGE_POINT_LOOKUP_THRESHOLD=%s; expected integer in [0, %d]\n",
                value,
                INT_MAX);
        __atomic_store_n(&cached_threshold, 256, __ATOMIC_RELAXED);
        return 256;
    }

    __atomic_store_n(&cached_threshold, (int)parsed, __ATOMIC_RELAXED);
    return (int)parsed;
}

static int compressed_tree_is_sharded(const struct bplus_tree_compressed *ct_tree)
{
    return ct_tree && ct_tree->shard_count > 1 && ct_tree->shards != NULL;
}

static struct bplus_tree_compressed *compressed_tree_shard_for_key(struct bplus_tree_compressed *ct_tree,
                                                                  key_t key)
{
    if (!compressed_tree_is_sharded(ct_tree)) {
        return ct_tree;
    }
    return ct_tree->shards[positive_mod_i32(key, ct_tree->shard_count)];
}

static qpl_job *acquire_qpl_job(struct bplus_tree_compressed *ct_tree, int *job_index_out)
{
    if (!ct_tree || ct_tree->qpl_pool_size <= 0 || !ct_tree->qpl_job_pool || !ct_tree->qpl_job_free_list) {
        return NULL;
    }

    pthread_mutex_lock(&ct_tree->qpl_pool_lock);
    while (ct_tree->qpl_free_count == 0 && ct_tree->qpl_pool_size > 0) {
        pthread_cond_wait(&ct_tree->qpl_pool_cond, &ct_tree->qpl_pool_lock);
    }

    if (ct_tree->qpl_pool_size <= 0 || ct_tree->qpl_free_count <= 0) {
        pthread_mutex_unlock(&ct_tree->qpl_pool_lock);
        return NULL;
    }

    ct_tree->qpl_free_count--;
    int job_index = ct_tree->qpl_job_free_list[ct_tree->qpl_free_count];
    qpl_job *job = ct_tree->qpl_job_pool[job_index];
    pthread_mutex_unlock(&ct_tree->qpl_pool_lock);

    if (job_index_out) {
        *job_index_out = job_index;
    }
    return job;
}

static int qpl_hardware_strict(const struct bplus_tree_compressed *ct_tree)
{
    return ct_tree &&
           ct_tree->config.algo == COMPRESS_QPL &&
           ct_tree->config.qpl_path == qpl_path_hardware;
}

struct qpl_tls_state {
    qpl_job *compress_job;
    uint8_t *compress_buffer;
    qpl_path_t compress_path;
    qpl_job *decompress_job;
    uint8_t *decompress_buffer;
    qpl_path_t decompress_path;
};

static pthread_key_t qpl_tls_key;
static pthread_once_t qpl_tls_once = PTHREAD_ONCE_INIT;
static int qpl_tls_key_ready = 0;

static void qpl_tls_destroy_job(qpl_job **job, uint8_t **buffer)
{
    if (job && *job) {
        qpl_fini_job(*job);
        *job = NULL;
    }
    if (buffer && *buffer) {
        free(*buffer);
        *buffer = NULL;
    }
}

static void qpl_tls_destroy(void *ptr)
{
    struct qpl_tls_state *state = (struct qpl_tls_state *)ptr;
    if (!state) {
        return;
    }

    qpl_tls_destroy_job(&state->compress_job, &state->compress_buffer);
    qpl_tls_destroy_job(&state->decompress_job, &state->decompress_buffer);
    free(state);
}

static void qpl_tls_make_key(void)
{
    qpl_tls_key_ready = (pthread_key_create(&qpl_tls_key, qpl_tls_destroy) == 0);
}

static struct qpl_tls_state *qpl_tls_get_state(void)
{
    pthread_once(&qpl_tls_once, qpl_tls_make_key);
    if (!qpl_tls_key_ready) {
        return NULL;
    }

    struct qpl_tls_state *state = pthread_getspecific(qpl_tls_key);
    if (state) {
        return state;
    }

    state = calloc(1, sizeof(*state));
    if (!state) {
        return NULL;
    }
    state->compress_path = qpl_path_auto;
    state->decompress_path = qpl_path_auto;

    if (pthread_setspecific(qpl_tls_key, state) != 0) {
        free(state);
        return NULL;
    }

    return state;
}

static qpl_job *qpl_tls_prepare_job(qpl_job **job,
                                    uint8_t **buffer,
                                    qpl_path_t *initialized_path,
                                    qpl_path_t requested_path)
{
    if (*job && *initialized_path == requested_path) {
        return *job;
    }

    qpl_tls_destroy_job(job, buffer);

    uint32_t job_size = 0;
    qpl_status status = qpl_get_job_size(requested_path, &job_size);
    if (status != QPL_STS_OK || job_size == 0) {
        return NULL;
    }

    *buffer = malloc(job_size);
    if (!*buffer) {
        return NULL;
    }
    *job = (qpl_job *)*buffer;

    status = qpl_init_job(requested_path, *job);
    if (status != QPL_STS_OK) {
        free(*buffer);
        *buffer = NULL;
        *job = NULL;
        return NULL;
    }

    *initialized_path = requested_path;
    return *job;
}

static qpl_job *acquire_qpl_tls_job(struct bplus_tree_compressed *ct_tree, int is_compress)
{
    if (!ct_tree || ct_tree->config.algo != COMPRESS_QPL) {
        return NULL;
    }

    struct qpl_tls_state *state = qpl_tls_get_state();
    if (!state) {
        return NULL;
    }

    qpl_path_t path = ct_tree->config.qpl_path;
    if (is_compress) {
        return qpl_tls_prepare_job(&state->compress_job,
                                   &state->compress_buffer,
                                   &state->compress_path,
                                   path);
    }

    return qpl_tls_prepare_job(&state->decompress_job,
                               &state->decompress_buffer,
                               &state->decompress_path,
                               path);
}

static void release_qpl_job(struct bplus_tree_compressed *ct_tree, int job_index)
{
    if (!ct_tree || job_index < 0 || job_index >= ct_tree->qpl_pool_size) {
        return;
    }

    pthread_mutex_lock(&ct_tree->qpl_pool_lock);
    if (ct_tree->qpl_free_count < ct_tree->qpl_pool_size) {
        ct_tree->qpl_job_free_list[ct_tree->qpl_free_count] = job_index;
        ct_tree->qpl_free_count++;
        pthread_cond_signal(&ct_tree->qpl_pool_cond);
    }
    pthread_mutex_unlock(&ct_tree->qpl_pool_lock);
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
    if (leaf->compression_algo == COMPRESS_QPL) {
        qpl_job *tls_job = acquire_qpl_tls_job(ct_tree, 1);
        if (tls_job) {
            tls_job->op = qpl_op_compress;
            tls_job->next_in_ptr = (uint8_t *)src;
            tls_job->available_in = src_size;
            tls_job->total_in = 0;
            tls_job->next_out_ptr = dst;
            tls_job->available_out = dst_capacity;
            tls_job->total_out = 0;
            tls_job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
            if (ct_tree->config.qpl_huffman_mode == QPL_HUFFMAN_DYNAMIC) {
                tls_job->flags |= QPL_FLAG_DYNAMIC_HUFFMAN;
            }
            tls_job->level = qpl_default_level;
            qpl_status status = qpl_execute_job(tls_job);
            uint32_t produced = tls_job->total_out;
            if (status == QPL_STS_OK && produced > 0 && produced <= dst_capacity) {
                return (int)produced;
            }
            if (qpl_hardware_strict(ct_tree)) {
                return -1;
            }
        }

        if (ct_tree->qpl_pool_size > 0) {
            int job_index = -1;
            qpl_job *job = acquire_qpl_job(ct_tree, &job_index);
            if (job) {
                job->op = qpl_op_compress;
                job->next_in_ptr = (uint8_t *)src;
                job->available_in = src_size;
                job->total_in = 0;
                job->next_out_ptr = dst;
                job->available_out = dst_capacity;
                job->total_out = 0;
                job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
                if (ct_tree->config.qpl_huffman_mode == QPL_HUFFMAN_DYNAMIC) {
                    job->flags |= QPL_FLAG_DYNAMIC_HUFFMAN;
                }
                job->level = qpl_default_level;
                qpl_status status = qpl_execute_job(job);
                uint32_t produced = job->total_out;
                release_qpl_job(ct_tree, job_index);
                if (status == QPL_STS_OK && produced > 0 && produced <= dst_capacity) {
                    return (int)produced;
                }
            }
            if (qpl_hardware_strict(ct_tree)) {
                return -1;
            }
        } else if (qpl_hardware_strict(ct_tree)) {
            return -1;
        }
    }

#ifdef HAVE_ZLIB
    if (leaf->compression_algo == COMPRESS_ZLIB_ACCEL) {
        uLongf produced = (uLongf)dst_capacity;
        int level = ct_tree->config.compression_level;
        if (level < Z_NO_COMPRESSION || level > Z_BEST_COMPRESSION) {
            level = Z_DEFAULT_COMPRESSION;
        }

        int status = compress2((Bytef *)dst,
                               &produced,
                               (const Bytef *)src,
                               (uLong)src_size,
                               level);
        if (status == Z_OK && produced > 0 && produced <= dst_capacity) {
            return (int)produced;
        }
    }
#endif

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
    if (leaf->compression_algo == COMPRESS_QPL) {
        qpl_job *tls_job = acquire_qpl_tls_job(ct_tree, 0);
        if (tls_job) {
            tls_job->op = qpl_op_decompress;
            tls_job->next_in_ptr = (uint8_t *)src;
            tls_job->available_in = src_size;
            tls_job->total_in = 0;
            tls_job->next_out_ptr = dst;
            tls_job->available_out = dst_capacity;
            tls_job->total_out = 0;
            tls_job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
            qpl_status status = qpl_execute_job(tls_job);
            uint32_t produced = tls_job->total_out;
            if (status == QPL_STS_OK && produced > 0 && produced <= dst_capacity) {
                return (int)produced;
            }
            if (qpl_hardware_strict(ct_tree)) {
                return -1;
            }
        }

        if (ct_tree->qpl_pool_size > 0) {
            int job_index = -1;
            qpl_job *job = acquire_qpl_job(ct_tree, &job_index);
            if (job) {
                job->op = qpl_op_decompress;
                job->next_in_ptr = (uint8_t *)src;
                job->available_in = src_size;
                job->total_in = 0;
                job->next_out_ptr = dst;
                job->available_out = dst_capacity;
                job->total_out = 0;
                job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
                qpl_status status = qpl_execute_job(job);
                uint32_t produced = job->total_out;
                release_qpl_job(ct_tree, job_index);
                if (status == QPL_STS_OK && produced > 0 && produced <= dst_capacity) {
                    return (int)produced;
                }
            }
            if (qpl_hardware_strict(ct_tree)) {
                return -1;
            }
        } else if (qpl_hardware_strict(ct_tree)) {
            return -1;
        }
    }

#ifdef HAVE_ZLIB
    if (leaf->compression_algo == COMPRESS_ZLIB_ACCEL) {
        uLongf produced = (uLongf)dst_capacity;
        int status = uncompress((Bytef *)dst,
                                &produced,
                                (const Bytef *)src,
                                (uLong)src_size);
        if (status == Z_OK && produced > 0 && produced <= dst_capacity) {
            return (int)produced;
        }
    }
#endif

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

    if (leaf->is_compressed && leaf->subpage_index && leaf->num_subpages > 0) {
        int sub_page_size = COMPRESSED_LEAF_SIZE / leaf->num_subpages;
        if (sub_page_size <= 0) {
            sub_page_size = COMPRESSED_LEAF_SIZE;
        }
        uint8_t sub_page_buffer[COMPRESSED_LEAF_SIZE];

        for (int bucket = 0; bucket < leaf->num_subpages; bucket++) {
            struct subpage_index_entry *entry = &leaf->subpage_index[bucket];
            if (!entry || entry->length <= 0) {
                continue;
            }

            int decompress_size = decompress_subpage(ct_tree,
                                                     leaf,
                                                     (const uint8_t *)leaf->compressed_data + entry->offset,
                                                     entry->length,
                                                     sub_page_buffer,
                                                     sub_page_size);

            if (decompress_size < 0) {
                free(pairs);
                return -1;
            }

            struct kv_pair *sp = (struct kv_pair *)sub_page_buffer;
            struct kv_pair *sp_end = (struct kv_pair *)(void *)(sub_page_buffer + sub_page_size);
            while (sp < sp_end) {
                if (sp->key != 0) {
                    if (kv_vector_put(&pairs,
                                      &count,
                                      &capacity,
                                      sp->key,
                                      sp->stored_value,
                                      sp->payload,
                                      COMPRESSED_VALUE_BYTES) != 0) {
                        free(pairs);
                        return -1;
                    }
                }
                sp++;
            }
        }
    }

    /*
     * Landing entries are newer than the compressed image. Insert them after
     * compressed entries so kv_vector_put overwrites stale compressed values
     * for updated keys.
     */
    int landing_capacity = landing_buffer_capacity_for_tree(ct_tree);
    struct kv_pair *landing = (struct kv_pair *)leaf->landing_buffer;
    for (int i = 0; i < landing_capacity; i++) {
        if (landing[i].key != 0) {
            if (kv_vector_put(&pairs,
                              &count,
                              &capacity,
                              landing[i].key,
                              landing[i].stored_value,
                              landing[i].payload,
                              COMPRESSED_VALUE_BYTES) != 0) {
                free(pairs);
                return -1;
            }
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
    int debug = getenv("TAIL_LATENCY_DEBUG") != NULL;
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
        leaf->generation++;
        return 0;
    }

    qsort(pairs, count, sizeof(struct kv_pair), compare_kv_pairs);

    size_t landing_slots = (size_t)landing_buffer_capacity_for_tree(ct_tree);
    size_t landing_count = count < landing_slots ? count : landing_slots;

    for (size_t i = 0; i < landing_count; i++) {
        struct kv_pair *slot = ((struct kv_pair *)leaf->landing_buffer) + i;
        slot->key = pairs[i].key;
        slot->stored_value = pairs[i].stored_value;
        memcpy(slot->payload, pairs[i].payload, COMPRESSED_VALUE_BYTES);
    }

    if (landing_count == count) {
        leaf->is_compressed = false;
        leaf->compressed_size = 0;
        leaf->compressed_bytes = 0;
        leaf->uncompressed_bytes = landing_count * sizeof(struct kv_pair);
        leaf->generation++;
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

    char uncompressed_pages[COMPRESSED_LEAF_SIZE];
    memset(uncompressed_pages, 0, sizeof(uncompressed_pages));

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
            return -1;
        }
    }

    char temp_compressed[MAX_COMPRESSED_SIZE];

    struct subpage_index_entry *temp_index = calloc(leaf->num_subpages, sizeof(struct subpage_index_entry));
    if (!temp_index) {
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
            return -1;
        }

        temp_index[bucket].offset = running_offset;
        temp_index[bucket].length = (uint32_t)compressed_size;
        running_offset += (size_t)compressed_size;
    }
    if (debug) {
        fprintf(stderr, "[insert] compressed buckets bytes=%zu\n", running_offset);
    }

    if (leaf->compressed_data == NULL) {
        leaf->compressed_data = calloc(1, MAX_COMPRESSED_SIZE);
        if (!leaf->compressed_data) {
            free(temp_index);
            return -1;
        }
    }

    memcpy(leaf->compressed_data, temp_compressed, running_offset);
    if (running_offset < MAX_COMPRESSED_SIZE) {
        memset(leaf->compressed_data + running_offset, 0, MAX_COMPRESSED_SIZE - running_offset);
    }
    memcpy(leaf->subpage_index, temp_index, leaf->num_subpages * sizeof(struct subpage_index_entry));

    free(temp_index);

    leaf->is_compressed = true;
    leaf->compressed_size = running_offset;
    leaf->compressed_bytes = running_offset;
    leaf->uncompressed_bytes = hashed_count * sizeof(struct kv_pair);
    leaf->num_subpage_entries = leaf->num_subpages;
    leaf->generation++;
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


static int split_leaf(struct bplus_tree_compressed *ct_tree,
                      struct bplus_leaf *leaf,
                      struct bplus_leaf **new_leaf_out,
                      key_t *split_key_out)
{
    // fprintf(stderr, "DEBUG split_leaf: ENTERED split_leaf function\n");
    // fflush(stderr);

    struct simple_leaf_node *custom_leaf = (struct simple_leaf_node *)leaf->data[0];
    if (!custom_leaf) {
        return -1;
    }

    // Lock the leaf being split (caller holds tree wrlock)
    pthread_rwlock_wrlock(&custom_leaf->rwlock);

    struct kv_pair *all_pairs = NULL;
    size_t pair_count = 0;
    if (compressed_leaf_collect_pairs(ct_tree, custom_leaf, &all_pairs, &pair_count) != 0 ||
        pair_count == 0) {
        free(all_pairs);
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        return -1;
    }

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
    if (!new_bplus_leaf) {
        free(all_pairs);
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        return -1;
    }
    struct simple_leaf_node *new_custom_leaf = calloc(1, sizeof(struct simple_leaf_node));
    if (!new_custom_leaf) {
        free(all_pairs);
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        return -1;
    }

    new_custom_leaf->compressed_data = calloc(1, MAX_COMPRESSED_SIZE);
    if (!new_custom_leaf->compressed_data) {
        free(new_custom_leaf);
        free(all_pairs);
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        return -1;
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
    size_t midpoint = pair_count / 2;
    // fprintf(stderr, "DEBUG split_leaf: midpoint=%d\n", midpoint);
    // fflush(stderr);
    for (size_t i = 0; i < midpoint; i++) {
        insert_into_leaf(ct_tree,
                         custom_leaf,
                         all_pairs[i].key,
                         all_pairs[i].stored_value,
                         all_pairs[i].payload,
                         COMPRESSED_VALUE_BYTES);
    }
    for (size_t i = midpoint; i < pair_count; i++) {
        insert_into_leaf(ct_tree,
                         new_custom_leaf,
                         all_pairs[i].key,
                         all_pairs[i].stored_value,
                         all_pairs[i].payload,
                         COMPRESSED_VALUE_BYTES);
    }

    // 7. The split key is the first key in the new leaf.
    key_t split_key = all_pairs[midpoint].key;
    // fprintf(stderr, "DEBUG split_leaf: Returning split_key=%d\n", split_key);
    // fflush(stderr);
    free(all_pairs);

    if (new_leaf_out) {
        *new_leaf_out = new_bplus_leaf;
    }
    if (split_key_out) {
        *split_key_out = split_key;
    }
    pthread_rwlock_unlock(&custom_leaf->rwlock);
    return 0;
}


int insert_into_leaf(struct bplus_tree_compressed *ct_tree,
                     struct simple_leaf_node *leaf,
                     key_t key,
                     int stored_value,
                     const uint8_t *payload,
                     size_t payload_len)
{
    int debug = getenv("TAIL_LATENCY_DEBUG") != NULL;
    int trace = getenv("TAIL_LATENCY_TRACE") != NULL;
    if (trace) {
        fprintf(stderr, "[insert] key=%d stored=%d\n", key, stored_value);
    }
    int landing_capacity = landing_buffer_capacity_for_tree(ct_tree);
    if (landing_capacity < 0) {
        landing_capacity = 0;
    }
    struct kv_pair *landing = (struct kv_pair *)leaf->landing_buffer;
    struct kv_pair *free_slot = NULL;

    for (int i = 0; i < landing_capacity; i++) {
        struct kv_pair *slot = landing + i;
        if (slot->key == key) {
            slot->stored_value = stored_value;
            if (payload && payload_len > 0) {
                size_t copy_len = payload_len > COMPRESSED_VALUE_BYTES ? COMPRESSED_VALUE_BYTES : payload_len;
                memcpy(slot->payload, payload, copy_len);
                if (copy_len < COMPRESSED_VALUE_BYTES) {
                    memset(slot->payload + copy_len, 0, COMPRESSED_VALUE_BYTES - copy_len);
                }
            }
            leaf->generation++;
            if (trace) fprintf(stderr, "[insert] updated existing in landing\n");
            return 0;
        }
        if (slot->key == 0 && free_slot == NULL) {
            free_slot = slot;
        }
    }

    if (free_slot != NULL) {
        free_slot->key = key;
        free_slot->stored_value = stored_value;
        if (payload && payload_len > 0) {
            size_t copy_len = payload_len > COMPRESSED_VALUE_BYTES ? COMPRESSED_VALUE_BYTES : payload_len;
            memcpy(free_slot->payload, payload, copy_len);
            if (copy_len < COMPRESSED_VALUE_BYTES) {
                memset(free_slot->payload + copy_len, 0, COMPRESSED_VALUE_BYTES - copy_len);
            }
        } else {
            memset(free_slot->payload, 0, COMPRESSED_VALUE_BYTES);
            memcpy(free_slot->payload, &stored_value,
                   sizeof(stored_value) > COMPRESSED_VALUE_BYTES ? COMPRESSED_VALUE_BYTES : sizeof(stored_value));
        }
        leaf->generation++;
        if (trace) fprintf(stderr, "[insert] placed in landing\n");
        return 0;
    }

    if (ct_tree->debug_mode) {
        fprintf(stderr, "LANDING BUFFER FULL: Compressing for key=%d\n", key);
        fflush(stderr);
    }
    if (debug) {
        fprintf(stderr, "[insert] landing full key=%d\n", key);
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
            if (debug) {
                fprintf(stderr, "[insert] decompress bucket=%d len=%d\n", bucket, entry->length);
            }
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

    for (int i = 0; i < landing_capacity; i++) {
        struct kv_pair *backup_slot = ((struct kv_pair *)landing_backup) + i;
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
    }

    int bucket = positive_mod_i32(key, leaf->num_subpages);
    struct kv_pair *bucket_begin = (struct kv_pair *)(uncompressed_pages + bucket * sub_page_size);
    int bucket_capacity = sub_page_size / (int)sizeof(struct kv_pair);
    bool placed = false;
    for (int i = 0; i < bucket_capacity; i++) {
        if (bucket_begin[i].key == 0 || bucket_begin[i].key == key) {
            bucket_begin[i].key = key;
            bucket_begin[i].stored_value = stored_value;
            if (payload && payload_len > 0) {
                size_t copy_len = payload_len > COMPRESSED_VALUE_BYTES ? COMPRESSED_VALUE_BYTES : payload_len;
                memcpy(bucket_begin[i].payload, payload, copy_len);
                if (copy_len < COMPRESSED_VALUE_BYTES) {
                    memset(bucket_begin[i].payload + copy_len, 0, COMPRESSED_VALUE_BYTES - copy_len);
                }
            } else {
                memset(bucket_begin[i].payload, 0, COMPRESSED_VALUE_BYTES);
                memcpy(bucket_begin[i].payload, &stored_value,
                       sizeof(stored_value) > COMPRESSED_VALUE_BYTES ? COMPRESSED_VALUE_BYTES : sizeof(stored_value));
            }
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
        leaf->generation++;
        return 0;
    }

    struct subpage_index_entry *temp_index = calloc(leaf->num_subpages, sizeof(struct subpage_index_entry));
    if (!temp_index) {
        return -1;
    }

    char temp_compressed[MAX_COMPRESSED_SIZE];

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
            free(temp_index);
            return -1;
        }

        temp_index[b].offset = running_offset;
        temp_index[b].length = (uint32_t)compressed_size;
        running_offset += (size_t)compressed_size;
    }
    if (trace) fprintf(stderr, "[insert] compressed buckets bytes=%zu\n", running_offset);

    if (leaf->compressed_data == NULL) {
        leaf->compressed_data = calloc(1, MAX_COMPRESSED_SIZE);
        if (!leaf->compressed_data) {
            free(temp_index);
            return -1;
        }
    }

    if (leaf->subpage_index == NULL) {
        leaf->subpage_index = calloc(leaf->num_subpages, sizeof(struct subpage_index_entry));
        if (!leaf->subpage_index) {
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
    leaf->generation++;

    free(temp_index);
    return 0;
}

static int out_of_lock_rebuild_enabled(void)
{
    const char *value = getenv("BTREE_OUT_OF_LOCK_REBUILD");
    return value && strcmp(value, "1") == 0;
}

static void kv_pair_set_value(struct kv_pair *entry,
                              key_t key,
                              int stored_value,
                              const uint8_t *payload,
                              size_t payload_len)
{
    entry->key = key;
    entry->stored_value = stored_value;
    if (payload && payload_len > 0) {
        size_t copy_len = payload_len > COMPRESSED_VALUE_BYTES ? COMPRESSED_VALUE_BYTES : payload_len;
        memcpy(entry->payload, payload, copy_len);
        if (copy_len < COMPRESSED_VALUE_BYTES) {
            memset(entry->payload + copy_len, 0, COMPRESSED_VALUE_BYTES - copy_len);
        }
    } else {
        memset(entry->payload, 0, COMPRESSED_VALUE_BYTES);
        memcpy(entry->payload, &stored_value,
               sizeof(stored_value) > COMPRESSED_VALUE_BYTES ? COMPRESSED_VALUE_BYTES : sizeof(stored_value));
    }
}

/*
 * Phase-2 optimization: when the landing buffer is full, build the next
 * compressed leaf image outside the leaf write lock. The caller enters with
 * leaf->rwlock held for write; this helper always returns with it held.
 */
static int insert_into_leaf_maybe_out_of_lock(struct bplus_tree_compressed *ct_tree,
                                              struct simple_leaf_node *leaf,
                                              key_t key,
                                              int stored_value,
                                              const uint8_t *payload,
                                              size_t payload_len,
                                              int *handled,
                                              int *result,
                                              size_t *old_uncompressed,
                                              size_t *old_compressed)
{
    *handled = 0;
    *result = 0;

    if (!out_of_lock_rebuild_enabled()) {
        return 0;
    }

    int landing_capacity = landing_buffer_capacity_for_tree(ct_tree);
    struct kv_pair *landing = (struct kv_pair *)leaf->landing_buffer;
    for (int i = 0; i < landing_capacity; i++) {
        if (landing[i].key == key || landing[i].key == 0) {
            return 0;
        }
    }

    int num_subpages = leaf->num_subpages;
    if (num_subpages <= 0) {
        num_subpages = ct_tree->config.default_sub_pages > 0
                         ? ct_tree->config.default_sub_pages
                         : 1;
    }

    int sub_page_size = COMPRESSED_LEAF_SIZE / num_subpages;
    if (sub_page_size <= 0) {
        sub_page_size = COMPRESSED_LEAF_SIZE;
    }

    uint64_t snapshot_generation = leaf->generation;
    size_t snapshot_uncompressed = leaf->uncompressed_bytes;
    size_t snapshot_compressed = leaf->compressed_bytes;
    bool snapshot_is_compressed = leaf->is_compressed;
    int snapshot_compressed_size = leaf->compressed_size;
    compression_algo_t snapshot_algo = leaf->compression_algo;

    char landing_backup[LANDING_BUFFER_BYTES];
    memcpy(landing_backup, leaf->landing_buffer, sizeof(landing_backup));

    char *compressed_copy = NULL;
    struct subpage_index_entry *index_copy = NULL;

    if (snapshot_is_compressed) {
        if (!leaf->subpage_index ||
            snapshot_compressed_size < 0 ||
            snapshot_compressed_size > MAX_COMPRESSED_SIZE) {
            return 0;
        }

        index_copy = calloc((size_t)num_subpages, sizeof(*index_copy));
        if (!index_copy) {
            return 0;
        }
        memcpy(index_copy,
               leaf->subpage_index,
               (size_t)num_subpages * sizeof(*index_copy));

        if (snapshot_compressed_size > 0) {
            compressed_copy = malloc((size_t)snapshot_compressed_size);
            if (!compressed_copy) {
                free(index_copy);
                return 0;
            }
            memcpy(compressed_copy, leaf->compressed_data, (size_t)snapshot_compressed_size);
        }
    }

    pthread_rwlock_unlock(&leaf->rwlock);

    int build_result = 0;
    char uncompressed_pages[COMPRESSED_LEAF_SIZE];
    memset(uncompressed_pages, 0, sizeof(uncompressed_pages));

    struct simple_leaf_node snapshot_leaf;
    memset(&snapshot_leaf, 0, sizeof(snapshot_leaf));
    snapshot_leaf.compression_algo = snapshot_algo;
    snapshot_leaf.num_subpages = num_subpages;
    snapshot_leaf.is_compressed = snapshot_is_compressed;
    snapshot_leaf.compressed_data = compressed_copy;
    snapshot_leaf.compressed_size = snapshot_compressed_size;
    snapshot_leaf.subpage_index = index_copy;

    if (snapshot_is_compressed) {
        for (int bucket = 0; bucket < num_subpages; bucket++) {
            struct subpage_index_entry *entry = &index_copy[bucket];
            if (entry->length <= 0) {
                continue;
            }
            if (entry->offset + entry->length > (uint32_t)snapshot_compressed_size) {
                build_result = -1;
                break;
            }
            int rc = decompress_subpage(ct_tree,
                                        &snapshot_leaf,
                                        (const uint8_t *)compressed_copy + entry->offset,
                                        entry->length,
                                        (uint8_t *)uncompressed_pages + bucket * sub_page_size,
                                        sub_page_size);
            if (rc < 0) {
                build_result = -1;
                break;
            }
        }
    }

    int bucket_capacity = sub_page_size / (int)sizeof(struct kv_pair);
    if (bucket_capacity <= 0) {
        build_result = -1;
    }

    if (build_result == 0) {
        for (int i = 0; i < landing_capacity; i++) {
            struct kv_pair *backup_slot = ((struct kv_pair *)landing_backup) + i;
            if (backup_slot->key == 0) {
                continue;
            }
            int bucket = positive_mod_i32(backup_slot->key, num_subpages);
            struct kv_pair *bucket_begin = (struct kv_pair *)(uncompressed_pages + bucket * sub_page_size);
            struct kv_pair *target = NULL;
            for (int j = 0; j < bucket_capacity; j++) {
                if (bucket_begin[j].key == backup_slot->key) {
                    target = &bucket_begin[j];
                    break;
                }
                if (bucket_begin[j].key == 0 && target == NULL) {
                    target = &bucket_begin[j];
                }
            }
            if (!target) {
                build_result = -1;
                break;
            }
            *target = *backup_slot;
        }
    }

    if (build_result == 0) {
        int bucket = positive_mod_i32(key, num_subpages);
        struct kv_pair *bucket_begin = (struct kv_pair *)(uncompressed_pages + bucket * sub_page_size);
        bool placed = false;
        for (int i = 0; i < bucket_capacity; i++) {
            if (bucket_begin[i].key == 0 || bucket_begin[i].key == key) {
                kv_pair_set_value(&bucket_begin[i], key, stored_value, payload, payload_len);
                placed = true;
                break;
            }
        }
        if (!placed) {
            build_result = -1;
        }
    }

    size_t hashed_pairs = 0;
    if (build_result == 0) {
        for (int b = 0; b < num_subpages; b++) {
            struct kv_pair *start = (struct kv_pair *)(uncompressed_pages + b * sub_page_size);
            for (int j = 0; j < bucket_capacity; j++) {
                if (start[j].key != 0) {
                    hashed_pairs++;
                }
            }
        }
    }

    char temp_compressed[MAX_COMPRESSED_SIZE];
    struct subpage_index_entry *temp_index = NULL;
    size_t running_offset = 0;
    if (build_result == 0) {
        temp_index = calloc((size_t)num_subpages, sizeof(*temp_index));
        if (!temp_index) {
            build_result = -1;
        }
    }

    if (build_result == 0) {
        struct simple_leaf_node output_leaf;
        memset(&output_leaf, 0, sizeof(output_leaf));
        output_leaf.compression_algo = snapshot_algo;
        output_leaf.num_subpages = num_subpages;

        for (int b = 0; b < num_subpages; b++) {
            struct kv_pair *start = (struct kv_pair *)(uncompressed_pages + b * sub_page_size);
            bool bucket_empty = true;
            for (int j = 0; j < bucket_capacity; j++) {
                if (start[j].key != 0) {
                    bucket_empty = false;
                    break;
                }
            }

            if (bucket_empty) {
                temp_index[b].offset = (uint32_t)running_offset;
                temp_index[b].length = 0;
                continue;
            }

            uint32_t dest_capacity = MAX_COMPRESSED_SIZE - (uint32_t)running_offset;
            if (dest_capacity == 0) {
                build_result = -1;
                break;
            }

            int compressed_size = compress_subpage(ct_tree,
                                                   &output_leaf,
                                                   (const uint8_t *)start,
                                                   sub_page_size,
                                                   (uint8_t *)temp_compressed + running_offset,
                                                   dest_capacity);
            if (compressed_size <= 0 || running_offset + (size_t)compressed_size > MAX_COMPRESSED_SIZE) {
                build_result = -1;
                break;
            }

            temp_index[b].offset = (uint32_t)running_offset;
            temp_index[b].length = (uint32_t)compressed_size;
            running_offset += (size_t)compressed_size;
        }
    }

    pthread_rwlock_wrlock(&leaf->rwlock);

    if (leaf->generation != snapshot_generation) {
        *old_uncompressed = leaf->uncompressed_bytes;
        *old_compressed = leaf->compressed_bytes;
        *result = insert_into_leaf(ct_tree, leaf, key, stored_value, payload, payload_len);
        *handled = 1;
        free(compressed_copy);
        free(index_copy);
        free(temp_index);
        return 0;
    }

    if (build_result != 0) {
        *old_uncompressed = snapshot_uncompressed;
        *old_compressed = snapshot_compressed;
        *result = -1;
        *handled = 1;
        free(compressed_copy);
        free(index_copy);
        free(temp_index);
        return 0;
    }

    if (leaf->compressed_data == NULL) {
        leaf->compressed_data = calloc(1, MAX_COMPRESSED_SIZE);
        if (!leaf->compressed_data) {
            *old_uncompressed = snapshot_uncompressed;
            *old_compressed = snapshot_compressed;
            *result = -1;
            *handled = 1;
            free(compressed_copy);
            free(index_copy);
            free(temp_index);
            return 0;
        }
    }

    if (leaf->subpage_index == NULL) {
        leaf->subpage_index = calloc((size_t)num_subpages, sizeof(*leaf->subpage_index));
        if (!leaf->subpage_index) {
            *old_uncompressed = snapshot_uncompressed;
            *old_compressed = snapshot_compressed;
            *result = -1;
            *handled = 1;
            free(compressed_copy);
            free(index_copy);
            free(temp_index);
            return 0;
        }
    }

    memset(leaf->landing_buffer, 0, LANDING_BUFFER_BYTES);
    memcpy(leaf->compressed_data, temp_compressed, running_offset);
    if (running_offset < MAX_COMPRESSED_SIZE) {
        memset(leaf->compressed_data + running_offset, 0, MAX_COMPRESSED_SIZE - running_offset);
    }
    memcpy(leaf->subpage_index, temp_index, (size_t)num_subpages * sizeof(*temp_index));

    leaf->num_subpages = num_subpages;
    leaf->num_subpage_entries = num_subpages;
    leaf->is_compressed = true;
    leaf->compressed_size = (int)running_offset;
    leaf->compressed_bytes = running_offset;
    leaf->uncompressed_bytes = hashed_pairs * sizeof(struct kv_pair);
    leaf->generation++;

    *old_uncompressed = snapshot_uncompressed;
    *old_compressed = snapshot_compressed;
    *result = 0;
    *handled = 1;

    free(compressed_copy);
    free(index_copy);
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
        for (int i = 0; i < leaf->entries; i++) {
            if (insert_into_leaf(ct_tree,
                                 custom_leaf,
                                 leaf->key[i],
                                 (int)leaf->data[i],
                                 NULL,
                                 0) != 0) {
                free(custom_leaf->compressed_data);
                pthread_rwlock_destroy(&custom_leaf->rwlock);
                free(custom_leaf);
                return -1;
            }
        }
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
    if (compressed_tree_is_sharded(ct_tree)) {
        return bplus_tree_compressed_put(compressed_tree_shard_for_key(ct_tree, key), key, data);
    }

    if (data == 0) {
        return bplus_tree_compressed_delete(ct_tree, key);
    }
    return bplus_tree_compressed_put_internal(ct_tree, key, data, NULL, 0);
}

int bplus_tree_compressed_put_with_payload(struct bplus_tree_compressed *ct_tree,
                                           key_t key,
                                           const uint8_t *payload,
                                           size_t payload_len,
                                           int stored_value)
{
    if (compressed_tree_is_sharded(ct_tree)) {
        return bplus_tree_compressed_put_with_payload(compressed_tree_shard_for_key(ct_tree, key),
                                                      key,
                                                      payload,
                                                      payload_len,
                                                      stored_value);
    }

    if (stored_value == 0) {
        stored_value = 1; // avoid delete semantics
    }
    return bplus_tree_compressed_put_internal(ct_tree, key, stored_value, payload, payload_len);
}

static int bplus_tree_compressed_put_internal(struct bplus_tree_compressed *ct_tree,
                                              key_t key,
                                              int data,
                                              const uint8_t *payload,
                                              size_t payload_len)
{
    if (ct_tree == NULL || !ct_tree->initialized || ct_tree->tree == NULL) {
        return -1;
    }

    // --- Fast path: tree rdlock → find leaf → leaf wrlock → release tree ---
    pthread_rwlock_rdlock(&ct_tree->rwlock);

    struct bplus_leaf *leaf = find_leaf_for_key(ct_tree->tree, key);
    if (leaf == NULL) {
        // Tree is empty — need wrlock to create first leaf
        pthread_rwlock_unlock(&ct_tree->rwlock);
        pthread_rwlock_wrlock(&ct_tree->rwlock);
        leaf = find_leaf_for_key(ct_tree->tree, key);
        if (leaf == NULL) {
            leaf = (struct bplus_leaf *)bplus_node_new(ct_tree->tree, BPLUS_TREE_LEAF);
            ct_tree->tree->root = (struct bplus_node *)leaf;
            list_add(&leaf->link, &ct_tree->tree->list[0]);
        }
        struct simple_leaf_node *custom_leaf = NULL;
        if (ensure_custom_leaf(ct_tree, leaf, &custom_leaf) != 0 || custom_leaf == NULL) {
            pthread_rwlock_unlock(&ct_tree->rwlock);
            return -1;
        }
        // Under wrlock, do insert directly (tree was empty, very first insert)
        int result = insert_into_leaf(ct_tree, custom_leaf, key, data, payload, payload_len);
        if (result == 0 && custom_leaf->is_compressed) {
            __atomic_add_fetch(&ct_tree->total_uncompressed_size, custom_leaf->uncompressed_bytes, __ATOMIC_SEQ_CST);
            __atomic_add_fetch(&ct_tree->total_compressed_size, custom_leaf->compressed_bytes, __ATOMIC_SEQ_CST);
            __atomic_add_fetch(&ct_tree->compression_operations, 1, __ATOMIC_SEQ_CST);
        }
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return result;
    }

    struct simple_leaf_node *custom_leaf = NULL;
    if (leaf->entries > 0 || leaf->data[0] == 0) {
        // Need ensure_custom_leaf — requires tree wrlock
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
        // Downgrade: release wrlock, take rdlock (allows other threads to proceed)
        pthread_rwlock_unlock(&ct_tree->rwlock);
        pthread_rwlock_rdlock(&ct_tree->rwlock);
        // Re-find leaf (tree may have changed during lock switch)
        leaf = find_leaf_for_key(ct_tree->tree, key);
        if (!leaf || leaf->data[0] == 0) {
            pthread_rwlock_unlock(&ct_tree->rwlock);
            return -1;
        }
        custom_leaf = (struct simple_leaf_node *)leaf->data[0];
    } else {
        custom_leaf = (struct simple_leaf_node *)leaf->data[0];
    }

    // Lock this specific leaf for writing, then release tree lock
    pthread_rwlock_wrlock(&custom_leaf->rwlock);
    pthread_rwlock_unlock(&ct_tree->rwlock);

    size_t old_uncompressed = custom_leaf->uncompressed_bytes;
    size_t old_compressed = custom_leaf->compressed_bytes;

    int result = 0;
    int handled_out_of_lock = 0;
    if (insert_into_leaf_maybe_out_of_lock(ct_tree,
                                           custom_leaf,
                                           key,
                                           data,
                                           payload,
                                           payload_len,
                                           &handled_out_of_lock,
                                           &result,
                                           &old_uncompressed,
                                           &old_compressed) != 0) {
        result = -1;
        handled_out_of_lock = 1;
    }
    if (!handled_out_of_lock) {
        result = insert_into_leaf(ct_tree, custom_leaf, key, data, payload, payload_len);
    }
    if (result != 0 && ct_tree->debug_mode) {
        fprintf(stderr, "[put_internal] insert_into_leaf returned %d for key=%d\n", result, key);
    }

    // Update global statistics atomically (no tree lock needed)
    if (result == 0 && custom_leaf->is_compressed) {
        __atomic_add_fetch(&ct_tree->total_uncompressed_size,
                         custom_leaf->uncompressed_bytes - old_uncompressed, __ATOMIC_SEQ_CST);
        __atomic_add_fetch(&ct_tree->total_compressed_size,
                         custom_leaf->compressed_bytes - old_compressed, __ATOMIC_SEQ_CST);
        __atomic_add_fetch(&ct_tree->compression_operations, 1, __ATOMIC_SEQ_CST);
    }

    if (result == -1) {
        // Need to split: release leaf → tree wrlock → split → unlock → retry
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        pthread_rwlock_wrlock(&ct_tree->rwlock);

        // Re-find leaf under tree wrlock (tree may have changed)
        leaf = find_leaf_for_key(ct_tree->tree, key);
        if (!leaf || leaf->data[0] == 0) {
            pthread_rwlock_unlock(&ct_tree->rwlock);
            return -1;
        }
        custom_leaf = (struct simple_leaf_node *)leaf->data[0];

        if (ct_tree->debug_mode) {
            fprintf(stderr, "=== EXECUTING SPLIT for key %d ===\n", key);
            fflush(stderr);
        }
        struct bplus_leaf *new_leaf = NULL;
        key_t split_key = 0;
        int split_rc = split_leaf(ct_tree, leaf, &new_leaf, &split_key);
        if (ct_tree->debug_mode) {
            fprintf(stderr, "=== SPLIT COMPLETE, split_key=%d ===\n", split_key);
            fflush(stderr);
        }
        if (split_rc != 0 || new_leaf == NULL) {
            if (ct_tree->debug_mode) {
                fprintf(stderr, "[put_internal] split failed rc=%d new_leaf=%p\n", split_rc, (void *)new_leaf);
            }
            pthread_rwlock_unlock(&ct_tree->rwlock);
            return -1;
        }

        // Insert split key into parent
        struct bplus_node *left_node = (struct bplus_node *)leaf;
        struct bplus_node *right_node = (struct bplus_node *)new_leaf;
        int parent_rc = bplus_tree_insert_internal(ct_tree->tree, split_key, left_node, right_node);
        if (parent_rc != 0) {
            pthread_rwlock_unlock(&ct_tree->rwlock);
            return parent_rc;
        }

        // Retry insert under tree wrlock (we already have it, just do it)
        if (key < split_key) {
            result = insert_into_leaf(ct_tree, custom_leaf, key, data, payload, payload_len);
        } else {
            struct simple_leaf_node *new_custom_leaf = (struct simple_leaf_node*)new_leaf->data[0];
            result = insert_into_leaf(ct_tree, new_custom_leaf, key, data, payload, payload_len);
        }
        pthread_rwlock_unlock(&ct_tree->rwlock);
    } else {
        pthread_rwlock_unlock(&custom_leaf->rwlock);
    }

    return result;
}


int bplus_tree_compressed_get(struct bplus_tree_compressed *ct_tree, key_t key)
{
    if (compressed_tree_is_sharded(ct_tree)) {
        return bplus_tree_compressed_get(compressed_tree_shard_for_key(ct_tree, key), key);
    }

    if (ct_tree == NULL || !ct_tree->initialized || ct_tree->tree == NULL) {
        return -1;
    }

    // Tree rdlock to find the leaf
    pthread_rwlock_rdlock(&ct_tree->rwlock);
    struct bplus_leaf *leaf = find_leaf_for_key(ct_tree->tree, key);
    if (!leaf) {
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return -1;
    }

    struct simple_leaf_node *custom_leaf = (struct simple_leaf_node*)leaf->data[0];

    if (leaf->entries > 0 || custom_leaf == NULL) {
        // Need ensure_custom_leaf — upgrade to write lock
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
        // Downgrade to rdlock
        pthread_rwlock_unlock(&ct_tree->rwlock);
        pthread_rwlock_rdlock(&ct_tree->rwlock);
        leaf = find_leaf_for_key(ct_tree->tree, key);
        if (!leaf || leaf->data[0] == 0) {
            pthread_rwlock_unlock(&ct_tree->rwlock);
            return -1;
        }
        custom_leaf = (struct simple_leaf_node *)leaf->data[0];
    }

    // Lock this leaf for reading, then release tree lock
    pthread_rwlock_rdlock(&custom_leaf->rwlock);
    pthread_rwlock_unlock(&ct_tree->rwlock);

    // 1. Search landing buffer
    struct kv_pair *p = (struct kv_pair *)custom_leaf->landing_buffer;
    struct kv_pair *end = (struct kv_pair *)(custom_leaf->landing_buffer +
                                             landing_buffer_bytes_for_tree(ct_tree));
    while (p < end) {
        if (p->key == key) {
            int value = p->stored_value;
            pthread_rwlock_unlock(&custom_leaf->rwlock);
            return value;
        }
        p++;
    }

    // 2. Search compressed sub-pages
    if (!custom_leaf->is_compressed) {
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        return -1;
    }

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
    if (entry->length > MAX_COMPRESSED_SIZE ||
        entry->offset + entry->length > (uint32_t)custom_leaf->compressed_size) {
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        return -1;
    }

    /*
     * Copy the compressed subpage while the leaf is locked, then perform the
     * expensive decompression outside the leaf lock. The returned value is
     * still a consistent snapshot from the time of the copy, but writers no
     * longer wait for LZ4/QPL/zlib decompression on the read path.
     */
    uint8_t compressed_copy[MAX_COMPRESSED_SIZE];
    uint32_t compressed_len = entry->length;
    struct simple_leaf_node leaf_snapshot;
    memset(&leaf_snapshot, 0, sizeof(leaf_snapshot));
    leaf_snapshot.compression_algo = custom_leaf->compression_algo;
    leaf_snapshot.num_subpages = custom_leaf->num_subpages;
    memcpy(compressed_copy,
           (const uint8_t *)custom_leaf->compressed_data + entry->offset,
           compressed_len);
    pthread_rwlock_unlock(&custom_leaf->rwlock);

    uint8_t buffer[COMPRESSED_LEAF_SIZE];

    int rc = decompress_subpage(ct_tree,
                                &leaf_snapshot,
                                compressed_copy,
                                compressed_len,
                                buffer,
                                sub_page_size);
    if (rc < 0) {
        return -1;
    }

    int result = -1;
    struct kv_pair *sp = (struct kv_pair *)buffer;
    struct kv_pair *sp_end = sp + (sub_page_size / (int)sizeof(struct kv_pair));
    while (sp < sp_end) {
        if (sp->key == key) {
            result = sp->stored_value;
            break;
        }
        sp++;
    }
    return result;
}

int bplus_tree_compressed_get_range(struct bplus_tree_compressed *ct_tree, key_t key1, key_t key2)
{
    key_t min_key = key1 <= key2 ? key1 : key2;
    key_t max_key = key1 <= key2 ? key2 : key1;

    if (compressed_tree_is_sharded(ct_tree)) {
        for (key_t key = max_key;; key--) {
            int value = bplus_tree_compressed_get(compressed_tree_shard_for_key(ct_tree, key), key);
            if (value >= 0) {
                return value;
            }
            if (key == min_key) {
                break;
            }
        }
        return -1;
    }

    int point_lookup_threshold = parse_range_point_lookup_threshold();
    long long range_width = (long long)max_key - (long long)min_key + 1LL;
    if (point_lookup_threshold > 0 &&
        range_width > 0 &&
        range_width <= (long long)point_lookup_threshold) {
        for (key_t key = max_key;; key--) {
            int value = bplus_tree_compressed_get(ct_tree, key);
            if (value >= 0) {
                return value;
            }
            if (key == min_key) {
                break;
            }
        }
        return -1;
    }

    if (ct_tree == NULL || !ct_tree->initialized || ct_tree->tree == NULL) {
        return -1;
    }
    if (ct_tree->tree->root == NULL) {
        return -1;
    }

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
            uint8_t sub_page_buffer[COMPRESSED_LEAF_SIZE];

            for (int bucket = 0; bucket < custom_leaf->num_subpages; bucket++) {
                if (!subpage_needed_for_range(min_key, max_key, bucket, custom_leaf->num_subpages)) {
                    continue;
                }

                struct subpage_index_entry *entry = &custom_leaf->subpage_index[bucket];
                if (!entry || entry->length <= 0) {
                    continue;
                }

                int decompress_size = decompress_subpage(ct_tree,
                                                         custom_leaf,
                                                         (const uint8_t *)custom_leaf->compressed_data + entry->offset,
                                                         entry->length,
                                                         sub_page_buffer,
                                                         sub_page_size);

                if (decompress_size < 0) {
                    rc = -1;
                    break;
                }

                struct kv_pair *sp = (struct kv_pair *)sub_page_buffer;
                struct kv_pair *sp_end = (struct kv_pair *)(void *)(sub_page_buffer + sub_page_size);
                while (sp < sp_end) {
                    if (sp->key != 0) {
                    if (kv_vector_put(&leaf_pairs,
                                      &leaf_count,
                                      &leaf_capacity,
                                      sp->key,
                                      sp->stored_value,
                                      sp->payload,
                                      COMPRESSED_VALUE_BYTES) != 0) {
                            rc = -1;
                            break;
                        }
                    }
                    sp++;
                }

                if (rc != 0) {
                    break;
                }
            }
        }

        if (rc == 0) {
            int landing_capacity = landing_buffer_capacity_for_tree(ct_tree);
            struct kv_pair *landing = (struct kv_pair *)custom_leaf->landing_buffer;
            for (int i = 0; i < landing_capacity; i++) {
                if (landing[i].key != 0) {
                    if (kv_vector_put(&leaf_pairs,
                                      &leaf_count,
                                      &leaf_capacity,
                                      landing[i].key,
                                      landing[i].stored_value,
                                      landing[i].payload,
                                      COMPRESSED_VALUE_BYTES) != 0) {
                        rc = -1;
                        break;
                    }
                }
            }
        }


        if (rc != 0) {
            pthread_rwlock_unlock(&custom_leaf->rwlock);
            free(leaf_pairs);
            pthread_rwlock_unlock(&ct_tree->rwlock);
            return -1;
        }

        pthread_rwlock_unlock(&custom_leaf->rwlock);

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
            last_value = leaf_pairs[i].stored_value;
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
    if (compressed_tree_is_sharded(ct_tree)) {
        return bplus_tree_compressed_delete(compressed_tree_shard_for_key(ct_tree, key), key);
    }

    if (ct_tree == NULL || !ct_tree->initialized || ct_tree->tree == NULL) {
        return -1;
    }

    // Fast path: missing keys and non-min-key deletes do not need tree structure updates.
    pthread_rwlock_rdlock(&ct_tree->rwlock);
    struct bplus_leaf *fast_leaf = find_leaf_for_key(ct_tree->tree, key);
    if (!fast_leaf || fast_leaf->data[0] == 0) {
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return -1;
    }

    struct simple_leaf_node *fast_custom_leaf = (struct simple_leaf_node *)fast_leaf->data[0];
    pthread_rwlock_wrlock(&fast_custom_leaf->rwlock);
    pthread_rwlock_unlock(&ct_tree->rwlock);

    struct kv_pair *fast_pairs = NULL;
    size_t fast_count = 0;
    if (compressed_leaf_collect_pairs(ct_tree, fast_custom_leaf, &fast_pairs, &fast_count) == 0 && fast_count > 0) {
        ssize_t fast_remove_index = -1;
        key_t fast_min_key = fast_pairs[0].key;
        for (size_t i = 0; i < fast_count; i++) {
            if (fast_pairs[i].key < fast_min_key) {
                fast_min_key = fast_pairs[i].key;
            }
            if (fast_pairs[i].key == key) {
                fast_remove_index = (ssize_t)i;
            }
        }

        if (fast_remove_index < 0) {
            free(fast_pairs);
            pthread_rwlock_unlock(&fast_custom_leaf->rwlock);
            return -1;
        }

        size_t fast_new_count = fast_count - 1;
        if (key != fast_min_key &&
            fast_new_count > 0) {
            memmove(&fast_pairs[fast_remove_index], &fast_pairs[fast_remove_index + 1],
                    (fast_count - (size_t)fast_remove_index - 1) * sizeof(struct kv_pair));
            int fast_rebuild_rc = compressed_leaf_rebuild_with_pairs(ct_tree,
                                                                     fast_custom_leaf,
                                                                     fast_pairs,
                                                                     fast_new_count);
            free(fast_pairs);
            pthread_rwlock_unlock(&fast_custom_leaf->rwlock);
            return fast_rebuild_rc == 0 ? 0 : -1;
        }
    }

    free(fast_pairs);
    pthread_rwlock_unlock(&fast_custom_leaf->rwlock);

    // Slow path handles deleting the leaf minimum, empty leaves, parent key updates, and rebalancing.
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
        leaf->data[0] = 0;
        pthread_rwlock_unlock(&custom_leaf->rwlock);
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
    /*
     * The historical incremental counters are incomplete for delete, split,
     * reinsert, and rebalance paths. Return exact leaf-walk stats for API
     * correctness; if near-free counters are needed later, update every leaf
     * rebuild/install site before re-enabling them.
     */
    return bplus_tree_compressed_calculate_stats(ct_tree, total_size, compressed_size);
}

int bplus_tree_compressed_calculate_stats(struct bplus_tree_compressed *ct_tree,
                                          size_t *total_size, size_t *compressed_size)
{
    if (compressed_tree_is_sharded(ct_tree)) {
        size_t total = 0;
        size_t compressed = 0;
        for (int i = 0; i < ct_tree->shard_count; i++) {
            size_t shard_total = 0;
            size_t shard_compressed = 0;
            if (bplus_tree_compressed_calculate_stats(ct_tree->shards[i],
                                                      &shard_total,
                                                      &shard_compressed) != 0) {
                return -1;
            }
            total += shard_total;
            compressed += shard_compressed;
        }
        if (total_size) {
            *total_size = total;
        }
        if (compressed_size) {
            *compressed_size = compressed;
        }
        return 0;
    }

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
        struct kv_pair *end = (struct kv_pair *)(custom_leaf->landing_buffer +
                                                 landing_buffer_bytes_for_tree(ct_tree));
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
    config.default_sub_pages = 1; // large inline payloads -> fewer buckets to keep capacity
    config.compression_level = 1;
    config.buffer_size = LANDING_BUFFER_DEFAULT_BYTES;
    config.flush_threshold = 0; // Not used in this model
    config.enable_lazy_compression = 0; // Not used in this model
    config.qpl_path = qpl_path_auto;
    config.qpl_huffman_mode = QPL_HUFFMAN_FIXED;
    return config;
}

static struct bplus_tree_compressed *bplus_tree_compressed_init_internal(int order,
                                                                        int entries,
                                                                        struct compression_config *config,
                                                                        int allow_sharding)
{
    struct compression_config effective_config = *config;
    apply_landing_buffer_env(&effective_config);

    struct bplus_tree_compressed *ct_tree = calloc(1, sizeof(*ct_tree));
    if (ct_tree == NULL) return NULL;

    int shard_count = allow_sharding ? parse_shard_count() : 1;
    if (shard_count > 1) {
        ct_tree->shards = calloc((size_t)shard_count, sizeof(*ct_tree->shards));
        if (!ct_tree->shards) {
            free(ct_tree);
            return NULL;
        }
        ct_tree->shard_count = shard_count;
        ct_tree->initialized = 1;
        ct_tree->compression_enabled = 1;
        ct_tree->debug_mode = 0;
        ct_tree->config = effective_config;

        for (int i = 0; i < shard_count; i++) {
            ct_tree->shards[i] = bplus_tree_compressed_init_internal(order, entries, &effective_config, 0);
            if (!ct_tree->shards[i]) {
                for (int j = 0; j < i; j++) {
                    bplus_tree_compressed_deinit(ct_tree->shards[j]);
                }
                free(ct_tree->shards);
                free(ct_tree);
                return NULL;
            }
        }
        return ct_tree;
    }

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
    ct_tree->config = effective_config;

    if (ct_tree->config.algo == COMPRESS_QPL) {
        if (init_qpl(ct_tree) != 0) {
            fprintf(stderr, "Warning: QPL initialization failed, QPL layouts will not be available\n");
            if (ct_tree->config.qpl_path == qpl_path_hardware) {
                bplus_tree_deinit(ct_tree->tree);
                pthread_rwlock_destroy(&ct_tree->rwlock);
                free(ct_tree);
                return NULL;
            }
        } else {
            fprintf(stderr, "QPL initialization successful\n");
        }
    }

    return ct_tree;
}

struct bplus_tree_compressed *bplus_tree_compressed_init_with_config(int order, int entries,
                                                                   struct compression_config *config)
{
    return bplus_tree_compressed_init_internal(order, entries, config, 1);
}

void bplus_tree_compressed_set_debug(struct bplus_tree_compressed *ct_tree, int enable)
{
    if (compressed_tree_is_sharded(ct_tree)) {
        for (int i = 0; i < ct_tree->shard_count; i++) {
            bplus_tree_compressed_set_debug(ct_tree->shards[i], enable);
        }
        ct_tree->debug_mode = enable ? 1 : 0;
        return;
    }

    if (ct_tree != NULL && ct_tree->initialized) {
        ct_tree->debug_mode = enable ? 1 : 0;
    }
}

int init_qpl(struct bplus_tree_compressed *ct_tree)
{
    if (ct_tree->qpl_pool_size > 0) {
        return 0; // Already initialized
    }

    uint32_t job_size = 0;
    qpl_path_t qpl_path = ct_tree->config.qpl_path;
    qpl_status status = qpl_get_job_size(qpl_path, &job_size);
    if (status != QPL_STS_OK || job_size == 0) {
        return -1;
    }

    long cores = sysconf(_SC_NPROCESSORS_ONLN);
    int pool_size = (cores > 0 && cores < INT_MAX) ? (int)cores : 1;
    if (pool_size < 1) {
        pool_size = 1;
    }
    int max_pool = 16;
    const char *pool_env = getenv("BTREE_QPL_POOL_SIZE");
    if (pool_env && *pool_env) {
        char *end = NULL;
        errno = 0;
        long parsed = strtol(pool_env, &end, 10);
        if (errno == 0 && end != pool_env && *end == '\0' && parsed > 0 && parsed <= INT_MAX) {
            max_pool = (int)parsed;
        }
    }
    if (pool_size > max_pool) {
        pool_size = max_pool;
    }

    qpl_job **jobs = calloc((size_t)pool_size, sizeof(qpl_job *));
    uint8_t **buffers = calloc((size_t)pool_size, sizeof(uint8_t *));
    int *free_list = calloc((size_t)pool_size, sizeof(int));
    if (!jobs || !buffers || !free_list) {
        free(jobs);
        free(buffers);
        free(free_list);
        return -1;
    }

    int lock_initialized = 0;
    int cond_initialized = 0;
    if (pthread_mutex_init(&ct_tree->qpl_pool_lock, NULL) != 0) {
        goto fail;
    }
    lock_initialized = 1;
    if (pthread_cond_init(&ct_tree->qpl_pool_cond, NULL) != 0) {
        goto fail;
    }
    cond_initialized = 1;

    int initialized_jobs = 0;
    for (int i = 0; i < pool_size; i++) {
        buffers[i] = malloc(job_size);
        if (!buffers[i]) {
            goto fail;
        }
        jobs[i] = (qpl_job *)buffers[i];
        status = qpl_init_job(qpl_path, jobs[i]);
        if (status != QPL_STS_OK) {
            goto fail;
        }
        free_list[i] = i;
        initialized_jobs++;
    }

    ct_tree->qpl_job_pool = jobs;
    ct_tree->qpl_job_buffers = buffers;
    ct_tree->qpl_job_free_list = free_list;
    ct_tree->qpl_pool_size = pool_size;
    ct_tree->qpl_free_count = pool_size;
    return 0;

fail:
    for (int i = 0; i < pool_size; i++) {
        if (jobs && jobs[i] && i < initialized_jobs) {
            qpl_fini_job(jobs[i]);
        }
        if (buffers && buffers[i]) {
            free(buffers[i]);
        }
    }
    free(jobs);
    free(buffers);
    free(free_list);
    if (cond_initialized) {
        pthread_cond_destroy(&ct_tree->qpl_pool_cond);
    }
    if (lock_initialized) {
        pthread_mutex_destroy(&ct_tree->qpl_pool_lock);
    }
    return -1;
}

void cleanup_qpl(struct bplus_tree_compressed *ct_tree)
{
    if (ct_tree == NULL) {
        return;
    }
    if (ct_tree->qpl_pool_size <= 0 &&
        !ct_tree->qpl_job_pool &&
        !ct_tree->qpl_job_buffers &&
        !ct_tree->qpl_job_free_list) {
        return;
    }

    pthread_mutex_lock(&ct_tree->qpl_pool_lock);
    int pool_size = ct_tree->qpl_pool_size;
    ct_tree->qpl_pool_size = 0;
    ct_tree->qpl_free_count = 0;
    pthread_cond_broadcast(&ct_tree->qpl_pool_cond);
    pthread_mutex_unlock(&ct_tree->qpl_pool_lock);

    if (ct_tree->qpl_job_pool && ct_tree->qpl_job_buffers) {
        for (int i = 0; i < pool_size; i++) {
            if (ct_tree->qpl_job_pool[i]) {
                qpl_fini_job(ct_tree->qpl_job_pool[i]);
            }
            if (ct_tree->qpl_job_buffers[i]) {
                free(ct_tree->qpl_job_buffers[i]);
            }
        }
    }

    free(ct_tree->qpl_job_pool);
    free(ct_tree->qpl_job_buffers);
    free(ct_tree->qpl_job_free_list);
    ct_tree->qpl_job_pool = NULL;
    ct_tree->qpl_job_buffers = NULL;
    ct_tree->qpl_job_free_list = NULL;

    pthread_cond_destroy(&ct_tree->qpl_pool_cond);
    pthread_mutex_destroy(&ct_tree->qpl_pool_lock);
}
