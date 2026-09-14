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
#include <strings.h>
#ifdef ZIPCACHE_AGG_EXPERIMENT
#include <dlfcn.h>
#endif
#if defined(__APPLE__)
#include <malloc/malloc.h>
#elif defined(__GLIBC__)
#include <malloc.h>
#endif
#ifdef HAVE_ZLIB
#include <zlib.h>
#endif
#ifdef HAVE_ZSTD
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#endif
#include "bplustree_compressed.h"
#include "compressed_alloc_audit.h"

// Node type constants (fallbacks if not provided by headers)
#ifndef BPLUS_TREE_LEAF
#define BPLUS_TREE_LEAF 0
#endif
#ifndef BPLUS_TREE_NON_LEAF
#define BPLUS_TREE_NON_LEAF 1
#endif

/* Only the common node prefix is consumed by generic tree/list code. */
_Static_assert(offsetof(struct compressed_leaf_ref, entries) == offsetof(struct bplus_node, count),
               "compressed leaf must retain the common node prefix");
static struct compressed_leaf_ref *compressed_leaf_ref_new(void)
{
    struct compressed_leaf_ref *ref = calloc(1, sizeof(*ref));
    if (ref) {
        ref->type = BPLUS_TREE_LEAF;
        ref->parent_key_idx = -1;
        list_init(&ref->link);
    }
    return ref;
}

static void leaf_index_release(struct simple_leaf_node *leaf)
{
    if (leaf->subpage_index != &leaf->inline_index) free(leaf->subpage_index);
    leaf->subpage_index = NULL;
}

static struct subpage_index_entry *leaf_index_allocate(struct simple_leaf_node *leaf, int count)
{
    if (count == 1) {
        memset(&leaf->inline_index, 0, sizeof(leaf->inline_index));
        return &leaf->inline_index;
    }
    return count > 0 ? calloc((size_t)count, sizeof(*leaf->subpage_index)) : NULL;
}

static void leaf_index_adopt(struct simple_leaf_node *leaf,
                             struct subpage_index_entry *index, int count)
{
    leaf_index_release(leaf);
    if (count == 1) {
        leaf->inline_index = index[0];
        free(index);
        leaf->subpage_index = &leaf->inline_index;
    } else {
        leaf->subpage_index = index;
    }
}

enum pending_task_state {
    PENDING_TASK_QUEUED = 0,
    PENDING_TASK_RUNNING = 1,
    PENDING_TASK_FAILED = 2
};

struct compressed_pending_task {
    struct bplus_tree_compressed *tree;
    struct simple_leaf_node *leaf;
    size_t count;
    uint64_t token;
    uint64_t base_version;
    uint64_t enqueued_ns;
    int attempts;
    enum pending_task_state state;
    struct kv_pair records[]; /* Three normally, four only for include-trigger. */
};

static enum pending_task_state pending_task_state_load(
    const struct compressed_pending_task *task)
{
    return (enum pending_task_state)__atomic_load_n(&task->state, __ATOMIC_ACQUIRE);
}

static void pending_task_state_store(struct compressed_pending_task *task,
                                     enum pending_task_state state)
{
    __atomic_store_n(&task->state, state, __ATOMIC_RELEASE);
}

struct compressed_page_image {
    char *data;
    size_t size;
    size_t usable;
    struct subpage_index_entry *index;
    int num_subpages;
    size_t uncompressed_bytes;
};

struct split_reservation {
    struct bplus_non_leaf *nodes[BPLUS_MAX_LEVEL];
    int count;
};

static void split_reservation_destroy(struct split_reservation *reserve)
{
    while (reserve->count) free(reserve->nodes[--reserve->count]);
}

static int split_reservation_prepare(struct bplus_tree *tree,
                                     struct compressed_leaf_ref *leaf,
                                     struct split_reservation *reserve)
{
    memset(reserve, 0, sizeof(*reserve));
    struct bplus_non_leaf *parent = leaf->parent;
    int needed = 0;
    while (parent && parent->children == tree->order) {
        needed++;
        parent = parent->parent;
    }
    if (!parent) {
        if (tree->level + 1 >= BPLUS_MAX_LEVEL) return -1;
        needed++;
    }
    for (int i = 0; i < needed; i++) {
        struct bplus_non_leaf *node = calloc(1, sizeof(*node));
        if (!node) { split_reservation_destroy(reserve); return -1; }
        node->type = BPLUS_TREE_NON_LEAF;
        node->parent_key_idx = -1;
        list_init(&node->link);
        reserve->nodes[reserve->count++] = node;
    }
    return 0;
}

static struct bplus_non_leaf *split_reservation_take(struct split_reservation *reserve)
{
    assert(reserve && reserve->count > 0);
    return reserve->nodes[--reserve->count];
}

/* Minimal transport-neutral submit/completion slot. Buffer ownership stays
 * with the production batch until every accepted submission is terminal. */
struct codec_async_slot {
    uint8_t *next_in_ptr, *next_out_ptr;
    uint32_t available_in, available_out, total_in, total_out, flags;
    int op, level;
    struct compressed_pending_task *task;
    int accepted, terminal, result;
    uint64_t sequence;
};

struct compaction_scheduler {
    pthread_mutex_t lock;
    pthread_cond_t work_available;
    pthread_cond_t state_changed;
    pthread_t worker;
#ifdef ZIPCACHE_AGG_EXPERIMENT
    void *worker_stack;
    size_t worker_stack_bytes;
    int admission_control;
    size_t admission_reserved, pending_byte_limit, pending_charge;
    int split_test_mode;
#endif
    int started;
    int accepting;
    int shutdown;
    int batch_size;
    int queue_capacity;
    int queue_head;
    int queue_tail;
    int queue_count;
    int running_count;
    struct compressed_pending_task **queue;
    struct bplus_tree_compressed *root;

    qpl_job **qpl_jobs;
    uint8_t **qpl_job_buffers;
    int qpl_job_count;
    size_t qpl_job_bytes;
    uint8_t *raw_workspaces;
    uint8_t *compressed_workspaces;
    struct codec_async_slot *codec_slots;
    int contract_mode; /* 0=real; 1=immediate, 2=reverse, 3=busy-once, 4=error, 5=bad-length */
    int contract_gate_open;
    struct bplus_tree_contract_stats contract_stats;
    uint64_t decompress_phase_ns, merge_phase_ns, compress_phase_ns, commit_phase_ns;
    uint64_t ready_at_wait_samples;

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
};

enum submission_logical_kind {
    SUBMISSION_LOGICAL_NONE = 0,
    SUBMISSION_LOGICAL_READ = 1,
    SUBMISSION_LOGICAL_WRITE = 2
};

struct submission_trace_event {
    uint64_t timestamp_ns;
    uint64_t thread_id;
    uint64_t op_id;
    uint64_t leaf_id;
    uint32_t input_bytes;
    uint32_t output_capacity;
    uint8_t codec_kind;
    uint8_t logical_kind;
    uint8_t under_write_lock;
};

struct submission_trace_state {
    struct submission_trace_event *events;
    uint64_t capacity;
    uint64_t next_index;
    int active;
    char *path;
};

static struct submission_trace_state submission_trace;
static pthread_once_t submission_trace_once = PTHREAD_ONCE_INIT;
static uint64_t submission_next_op_id;
static __thread uint64_t submission_tls_op_id;
static __thread int submission_tls_logical_kind;
static __thread int submission_tls_write_critical;
static int compaction_test_codec_failure_consumed;
static size_t qpl_tls_live_bytes;
static size_t zlib_live_bytes;
#ifdef ZIPCACHE_AGG_EXPERIMENT
static struct bplus_aggregation_stats agg_stats;
static __thread int agg_origin = BPLUS_ORIGIN_OTHER;
static int split_zlib_window_bits = 15;
#define AGG_INC(field, n) __atomic_add_fetch(&agg_stats.field, (uint64_t)(n), __ATOMIC_RELAXED)
void bplus_tree_compressed_aggregation_stats(struct bplus_aggregation_stats *out, int reset)
{
    uint64_t *src = (uint64_t *)&agg_stats, *dst = (uint64_t *)out;
    for (size_t i = 0; i < sizeof(agg_stats) / sizeof(uint64_t); i++) {
        uint64_t v = reset ? __atomic_exchange_n(src+i, 0, __ATOMIC_RELAXED)
                           : __atomic_load_n(src+i, __ATOMIC_RELAXED);
        if (dst) dst[i] = v;
    }
}
#else
#define AGG_INC(field, n) ((void)0)
#endif

static uint64_t submission_now_ns(void)
{
    struct timespec ts;
#ifdef ZIPCACHE_AGG_EXPERIMENT
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
#else
    clock_gettime(CLOCK_MONOTONIC, &ts);
#endif
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void submission_trace_dump(void)
{
    if (!submission_trace.path || !submission_trace.events || !submission_trace.active) {
        return;
    }

    FILE *out = fopen(submission_trace.path, "w");
    if (!out) {
        fprintf(stderr, "Unable to write BTREE_SUBMISSION_TRACE=%s: %s\n",
                submission_trace.path,
                strerror(errno));
        return;
    }

    uint64_t observed = __atomic_load_n(&submission_trace.next_index, __ATOMIC_ACQUIRE);
    uint64_t stored = observed < submission_trace.capacity ? observed : submission_trace.capacity;
    fprintf(out, "# zipcache_submission_trace_v1\n");
    fprintf(out, "# observed_events=%llu stored_events=%llu dropped_events=%llu\n",
            (unsigned long long)observed,
            (unsigned long long)stored,
            (unsigned long long)(observed - stored));
    fprintf(out,
            "timestamp_ns,thread_id,op_id,leaf_id,codec,input_bytes,output_capacity,logical,under_write_lock\n");
    for (uint64_t i = 0; i < stored; i++) {
        const struct submission_trace_event *event = &submission_trace.events[i];
        fprintf(out,
                "%llu,%llu,%llu,%llu,%c,%u,%u,%c,%u\n",
                (unsigned long long)event->timestamp_ns,
                (unsigned long long)event->thread_id,
                (unsigned long long)event->op_id,
                (unsigned long long)event->leaf_id,
                event->codec_kind == 1 ? 'C' : 'D',
                event->input_bytes,
                event->output_capacity,
                event->logical_kind == SUBMISSION_LOGICAL_READ ? 'R' :
                    (event->logical_kind == SUBMISSION_LOGICAL_WRITE ? 'W' : 'B'),
                event->under_write_lock);
    }
    fclose(out);
}

static void submission_trace_init(void)
{
    const char *path = getenv("BTREE_SUBMISSION_TRACE");
    if (!path || !*path) {
        return;
    }

    uint64_t capacity = 2000000ULL;
    const char *capacity_env = getenv("BTREE_SUBMISSION_TRACE_MAX_EVENTS");
    if (capacity_env && *capacity_env) {
        char *end = NULL;
        errno = 0;
        unsigned long long parsed = strtoull(capacity_env, &end, 10);
        if (errno == 0 && end != capacity_env && *end == '\0' && parsed > 0) {
            capacity = (uint64_t)parsed;
        }
    }

    submission_trace.events = calloc((size_t)capacity, sizeof(*submission_trace.events));
    if (!submission_trace.events) {
        fprintf(stderr, "Unable to allocate submission trace for %llu events\n",
                (unsigned long long)capacity);
        return;
    }
    submission_trace.path = strdup(path);
    if (!submission_trace.path) {
        free(submission_trace.events);
        submission_trace.events = NULL;
        return;
    }
    submission_trace.capacity = capacity;
    atexit(submission_trace_dump);
}

static void submission_trace_reset(void)
{
    pthread_once(&submission_trace_once, submission_trace_init);
    if (!submission_trace.events) {
        return;
    }
    __atomic_store_n(&submission_trace.next_index, 0, __ATOMIC_RELEASE);
    __atomic_store_n(&submission_next_op_id, 0, __ATOMIC_RELEASE);
    submission_trace.active = 1;
}

static void submission_trace_record(uint8_t codec_kind,
                                    uint32_t input_bytes,
                                    uint32_t output_capacity,
                                    const struct simple_leaf_node *leaf)
{
    pthread_once(&submission_trace_once, submission_trace_init);
    if (!submission_trace.active || !submission_trace.events) {
        return;
    }

    uint64_t index = __atomic_fetch_add(&submission_trace.next_index, 1, __ATOMIC_RELAXED);
    if (index >= submission_trace.capacity) {
        return;
    }

    struct submission_trace_event *event = &submission_trace.events[index];
    event->timestamp_ns = submission_now_ns();
    event->thread_id = (uint64_t)(uintptr_t)pthread_self();
    event->op_id = submission_tls_op_id;
    event->leaf_id = (uint64_t)(uintptr_t)leaf;
    event->input_bytes = input_bytes;
    event->output_capacity = output_capacity;
    event->codec_kind = codec_kind;
    event->logical_kind = (uint8_t)submission_tls_logical_kind;
    event->under_write_lock = submission_tls_write_critical ? 1 : 0;
}

static void submission_profile_begin_logical(struct bplus_tree_compressed *ct_tree,
                                             enum submission_logical_kind kind)
{
    submission_tls_logical_kind = kind;
    submission_tls_write_critical = 0;
    if (!ct_tree || !ct_tree->submission_profile_enabled) {
        submission_tls_op_id = 0;
        return;
    }

    submission_tls_op_id = __atomic_add_fetch(&submission_next_op_id, 1, __ATOMIC_RELAXED);
    if (kind == SUBMISSION_LOGICAL_READ) {
        __atomic_add_fetch(&ct_tree->submission_logical_reads, 1, __ATOMIC_RELAXED);
    } else if (kind == SUBMISSION_LOGICAL_WRITE) {
        __atomic_add_fetch(&ct_tree->submission_logical_writes, 1, __ATOMIC_RELAXED);
    }
}

static int submission_profile_codec_begin(struct bplus_tree_compressed *ct_tree,
                                          int is_compress,
                                          uint32_t input_bytes,
                                          uint32_t output_capacity,
                                          const struct simple_leaf_node *leaf)
{
    if (!ct_tree || !ct_tree->submission_profile_enabled) {
        return 0;
    }

    if (is_compress) {
        __atomic_add_fetch(&ct_tree->submission_compress_calls, 1, __ATOMIC_RELAXED);
        __atomic_add_fetch(&ct_tree->submission_compress_input_bytes,
                           input_bytes,
                           __ATOMIC_RELAXED);
    } else {
        __atomic_add_fetch(&ct_tree->submission_decompress_calls, 1, __ATOMIC_RELAXED);
        __atomic_add_fetch(&ct_tree->submission_decompress_input_bytes,
                           input_bytes,
                           __ATOMIC_RELAXED);
    }
    if (submission_tls_write_critical) {
        __atomic_add_fetch(&ct_tree->submission_calls_under_write_lock, 1, __ATOMIC_RELAXED);
        __atomic_add_fetch(&ct_tree->submission_input_bytes_under_write_lock,
                           input_bytes,
                           __ATOMIC_RELAXED);
    }

    uint64_t current = __atomic_add_fetch(&ct_tree->submission_current_inflight,
                                          1,
                                          __ATOMIC_RELAXED);
    uint64_t peak = __atomic_load_n(&ct_tree->submission_peak_inflight, __ATOMIC_RELAXED);
    while (current > peak &&
           !__atomic_compare_exchange_n(&ct_tree->submission_peak_inflight,
                                        &peak,
                                        current,
                                        0,
                                        __ATOMIC_RELAXED,
                                        __ATOMIC_RELAXED)) {
    }

    submission_trace_record(is_compress ? 1 : 2,
                            input_bytes,
                            output_capacity,
                            leaf);
    return 1;
}

static void submission_profile_codec_end(struct bplus_tree_compressed *ct_tree, int profiled)
{
    if (profiled) {
        __atomic_sub_fetch(&ct_tree->submission_current_inflight, 1, __ATOMIC_RELAXED);
    }
}

// Forward declarations
static struct compressed_leaf_ref *find_leaf_for_key(struct bplus_tree *tree, key_t key);
void cleanup_qpl(struct bplus_tree_compressed *ct_tree);
int init_qpl(struct bplus_tree_compressed *ct_tree);
static qpl_job *acquire_qpl_job(struct bplus_tree_compressed *ct_tree, int *job_index_out);
static void release_qpl_job(struct bplus_tree_compressed *ct_tree, int job_index);
static qpl_job *acquire_qpl_tls_job(struct bplus_tree_compressed *ct_tree, int is_compress);
static int qpl_tls_job_cache_enabled(void);
static int zlib_stream_cache_enabled(void);
static int background_codec_allowed(const struct bplus_tree_compressed *ct_tree,
                                    const struct simple_leaf_node *leaf);
static int compressed_tree_is_sharded(const struct bplus_tree_compressed *ct_tree);
static struct bplus_tree_compressed *compressed_tree_shard_for_key(struct bplus_tree_compressed *ct_tree,
                                                                  key_t key);
static int compressed_leaf_landing_count(struct bplus_tree_compressed *ct_tree,
                                         struct simple_leaf_node *leaf);
static int compressed_leaf_flush_landing_locked(struct bplus_tree_compressed *ct_tree,
                                                struct simple_leaf_node *leaf);
static void background_maybe_enqueue_key(struct bplus_tree_compressed *ct_tree,
                                         struct simple_leaf_node *leaf,
                                         key_t key);
static void __attribute__((unused)) start_background_compaction(
    struct bplus_tree_compressed *ct_tree);
static void stop_background_compaction(struct bplus_tree_compressed *ct_tree);
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
                      struct compressed_leaf_ref *leaf,
                      struct compressed_leaf_ref **new_leaf_out,
                      key_t *split_key_out);
static struct bplus_tree_compressed *bplus_tree_compressed_init_internal(int order,
                                                                        int entries,
                                                                        struct compression_config *config,
                                                                        int allow_sharding);
static int bplus_tree_insert_internal(struct bplus_tree *tree, key_t key, struct bplus_node *left, struct bplus_node *right, struct split_reservation *reserve);
static int ensure_custom_leaf(struct bplus_tree_compressed *ct_tree, struct compressed_leaf_ref *leaf, struct simple_leaf_node **out_leaf);
static int compressed_parent_node_build(struct bplus_tree *tree, struct bplus_node *left, struct bplus_node *right, key_t key, int level, struct split_reservation *reserve);
static int compressed_non_leaf_insert(struct bplus_tree *tree, struct bplus_non_leaf *node, struct bplus_node *l_ch, struct bplus_node *r_ch, key_t key, int level, struct split_reservation *reserve);
static void compressed_leaf_free(struct simple_leaf_node *leaf);
static void kv_pair_set_value(struct kv_pair *entry,
                              key_t key,
                              int stored_value,
                              const uint8_t *payload,
                              size_t payload_len);
static int compressed_leaf_collect_base_pairs(struct bplus_tree_compressed *ct_tree,
                                              struct simple_leaf_node *leaf,
                                              struct kv_pair **out_pairs,
                                              size_t *out_count);
static int compressed_build_base_image(struct bplus_tree_compressed *ct_tree,
                                       struct simple_leaf_node *leaf,
                                       struct kv_pair *pairs,
                                       size_t count,
                                       struct compressed_page_image *image);
static void compressed_page_image_destroy(struct compressed_page_image *image);
static struct compaction_scheduler *compaction_scheduler_create(
    struct bplus_tree_compressed *root);
static void compaction_scheduler_destroy(struct compaction_scheduler *scheduler);
static void compaction_scheduler_attach(struct bplus_tree_compressed *tree,
                                        struct compaction_scheduler *scheduler,
                                        int owner);
static int compaction_scheduler_enqueue_locked(
    struct bplus_tree_compressed *tree,
    struct simple_leaf_node *leaf,
    struct compressed_pending_task *task);
static uint64_t compaction_scheduler_wait_for_change(struct compaction_scheduler *scheduler);
static void compaction_fail_or_retry(struct compaction_scheduler *scheduler,
                                     struct compressed_pending_task *task);
static int compaction_insert_locked(struct bplus_tree_compressed *ct_tree,
                                    struct simple_leaf_node *leaf,
                                    key_t key,
                                    int stored_value,
                                    const uint8_t *payload,
                                    size_t payload_len,
                                    int *must_wait,
                                    int *used_sync_fallback);



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
        if (ct_tree->owns_scheduler && ct_tree->scheduler) {
            compaction_scheduler_destroy(ct_tree->scheduler);
            ct_tree->scheduler = NULL;
            ct_tree->owns_scheduler = 0;
        }
        for (int i = 0; i < ct_tree->shard_count; i++) {
            ct_tree->shards[i]->scheduler = NULL;
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
        if (ct_tree->owns_scheduler && ct_tree->scheduler) {
            compaction_scheduler_destroy(ct_tree->scheduler);
            ct_tree->scheduler = NULL;
            ct_tree->owns_scheduler = 0;
        }
        stop_background_compaction(ct_tree);

        pthread_rwlock_wrlock(&ct_tree->rwlock);
        
        cleanup_qpl(ct_tree);
        
        if (ct_tree->tree != NULL) {
            struct list_head *head = &ct_tree->tree->list[0];
            struct list_head *pos, *n;
            list_for_each_safe(pos, n, head) {
                struct compressed_leaf_ref *leaf = list_entry(pos, struct compressed_leaf_ref, link);
                if (leaf->type != BPLUS_TREE_LEAF) {
                    continue;
                }
                if (leaf->payload != 0) {
                    struct simple_leaf_node *custom_leaf = (struct simple_leaf_node *)leaf->payload;
                    compressed_leaf_free(custom_leaf);
                    leaf->payload = 0;
                }
            }
            for (int level = 0; level <= ct_tree->tree->level; level++) {
                list_for_each_safe(pos, n, &ct_tree->tree->list[level]) {
                    audit_forget(list_entry(pos, struct bplus_node, link));
                }
            }
            audit_forget(ct_tree->tree);
            bplus_tree_deinit(ct_tree->tree);
            ct_tree->tree = NULL;
        }
        
        pthread_rwlock_unlock(&ct_tree->rwlock);
        pthread_rwlock_destroy(&ct_tree->rwlock);
        pthread_mutex_destroy(&ct_tree->bg_scan_lock);
        pthread_mutex_destroy(&ct_tree->bg_queue_lock);
        pthread_cond_destroy(&ct_tree->bg_queue_cond);
        free(ct_tree->bg_dirty_keys);
        ct_tree->bg_dirty_keys = NULL;
        ct_tree->initialized = 0;
    }
    
    free(ct_tree);
}

static size_t allocation_usable_size(const void *ptr, size_t requested)
{
    if (!ptr) {
        return 0;
    }
#if defined(__APPLE__)
    (void)requested;
    return malloc_size(ptr);
#elif defined(__GLIBC__)
    (void)requested;
    return malloc_usable_size((void *)ptr);
#else
    return requested;
#endif
}

static void compressed_leaf_adopt_base(struct simple_leaf_node *leaf,
                                       char *data,
                                       size_t length,
                                       size_t usable)
{
    char *old = leaf->compressed_data;
    leaf->compressed_data = data;
    leaf->compressed_size = (int)length;
    leaf->compressed_capacity = length;
    leaf->compressed_usable = usable;
    leaf->compressed_bytes = length;
    leaf->is_compressed = length > 0;
    leaf->base_version++;
    free(old);
}

/* Build first, then atomically replace the immutable resident base under lock. */
static int compressed_leaf_replace_base(struct simple_leaf_node *leaf,
                                        const void *data,
                                        size_t length)
{
    if (!leaf || (!data && length != 0) || length > (size_t)INT_MAX) {
        return -1;
    }

    char *replacement = NULL;
    if (length > 0) {
        replacement = malloc(length);
        if (!replacement) {
            return -1;
        }
        memcpy(replacement, data, length);
    }

    compressed_leaf_adopt_base(leaf,
                               replacement,
                               length,
                               allocation_usable_size(replacement, length));
    return 0;
}

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
    if ((size_t)requested > (size_t)LANDING_BUFFER_BYTES) {
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
                    "Invalid BTREE_LANDING_BUFFER_BYTES=%s; using default %zu\n",
                    value,
                    (size_t)LANDING_BUFFER_DEFAULT_BYTES);
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

static int env_int_clamped(const char *name, int default_value, int min_value, int max_value)
{
    const char *value = getenv(name);
    if (!value || !*value) {
        return default_value;
    }

    char *end = NULL;
    errno = 0;
    long parsed = strtol(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' ||
        parsed < min_value || parsed > max_value) {
        fprintf(stderr,
                "Invalid %s=%s; expected integer in [%d, %d], using %d\n",
                name,
                value,
                min_value,
                max_value,
                default_value);
        return default_value;
    }
    return (int)parsed;
}

static int env_bool_enabled(const char *name, int default_value)
{
    const char *value = getenv(name);
    if (!value || !*value) {
        return default_value;
    }
    if (strcmp(value, "1") == 0 ||
        strcasecmp(value, "true") == 0 ||
        strcasecmp(value, "yes") == 0 ||
        strcasecmp(value, "on") == 0) {
        return 1;
    }
    if (strcmp(value, "0") == 0 ||
        strcasecmp(value, "false") == 0 ||
        strcasecmp(value, "no") == 0 ||
        strcasecmp(value, "off") == 0) {
        return 0;
    }
    fprintf(stderr, "Invalid %s=%s; expected 0/1, using %d\n", name, value, default_value);
    return default_value;
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

static int qpl_tls_job_cache_enabled(void)
{
    static int cached = -1;
    int current = __atomic_load_n(&cached, __ATOMIC_RELAXED);
    if (current >= 0) {
        return current;
    }

    const char *value = getenv("BTREE_QPL_JOB_CACHE");
    int enabled = 1;
    if (value && *value) {
        if (strcasecmp(value, "thread") == 0 ||
            strcasecmp(value, "tls") == 0 ||
            strcmp(value, "1") == 0 ||
            strcasecmp(value, "true") == 0 ||
            strcasecmp(value, "on") == 0) {
            enabled = 1;
        } else if (strcasecmp(value, "pool") == 0 ||
                   strcasecmp(value, "global") == 0 ||
                   strcasecmp(value, "none") == 0 ||
                   strcmp(value, "0") == 0 ||
                   strcasecmp(value, "false") == 0 ||
                   strcasecmp(value, "off") == 0) {
            enabled = 0;
        } else {
            fprintf(stderr,
                    "Invalid BTREE_QPL_JOB_CACHE=%s; use thread or pool, using thread\n",
                    value);
        }
    }

    __atomic_store_n(&cached, enabled, __ATOMIC_RELAXED);
    return enabled;
}

struct qpl_tls_state {
    qpl_job *compress_job;
    uint8_t *compress_buffer;
    size_t compress_bytes;
    qpl_path_t compress_path;
    qpl_job *decompress_job;
    uint8_t *decompress_buffer;
    size_t decompress_bytes;
    qpl_path_t decompress_path;
#ifdef ZIPCACHE_AGG_EXPERIMENT
    qpl_job *maintenance_job;
    uint8_t *maintenance_buffer;
    size_t maintenance_bytes;
    qpl_path_t maintenance_path;
#endif
};

static pthread_key_t qpl_tls_key;
static pthread_once_t qpl_tls_once = PTHREAD_ONCE_INIT;
static int qpl_tls_key_ready = 0;

static void qpl_tls_destroy_job(qpl_job **job, uint8_t **buffer, size_t *bytes)
{
    if (job && *job) {
        qpl_fini_job(*job);
        *job = NULL;
    }
    if (buffer && *buffer) {
        free(*buffer);
        *buffer = NULL;
    }
    if (bytes && *bytes > 0) {
        __atomic_sub_fetch(&qpl_tls_live_bytes, *bytes, __ATOMIC_RELAXED);
        *bytes = 0;
    }
}

static void qpl_tls_destroy(void *ptr)
{
    struct qpl_tls_state *state = (struct qpl_tls_state *)ptr;
    if (!state) {
        return;
    }

    qpl_tls_destroy_job(&state->compress_job,
                        &state->compress_buffer,
                        &state->compress_bytes);
    qpl_tls_destroy_job(&state->decompress_job,
                        &state->decompress_buffer,
                        &state->decompress_bytes);
#ifdef ZIPCACHE_AGG_EXPERIMENT
    qpl_tls_destroy_job(&state->maintenance_job, &state->maintenance_buffer,
                        &state->maintenance_bytes);
#endif
    __atomic_sub_fetch(&qpl_tls_live_bytes, allocation_usable_size(state, sizeof(*state)), __ATOMIC_RELAXED);
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

    __atomic_add_fetch(&qpl_tls_live_bytes, allocation_usable_size(state, sizeof(*state)), __ATOMIC_RELAXED);

    return state;
}

static qpl_job *qpl_tls_prepare_job(qpl_job **job,
                                    uint8_t **buffer,
                                    size_t *bytes,
                                    qpl_path_t *initialized_path,
                                    qpl_path_t requested_path)
{
    if (*job && *initialized_path == requested_path) {
        return *job;
    }

    qpl_tls_destroy_job(job, buffer, bytes);

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

    *bytes = allocation_usable_size(*buffer, job_size);
    __atomic_add_fetch(&qpl_tls_live_bytes, *bytes, __ATOMIC_RELAXED);
    *initialized_path = requested_path;
    return *job;
}

static qpl_job *acquire_qpl_tls_job(struct bplus_tree_compressed *ct_tree, int is_compress)
{
    if (!ct_tree || ct_tree->config.algo != COMPRESS_QPL) {
        return NULL;
    }
    if (!qpl_tls_job_cache_enabled()) {
        return NULL;
    }

    struct qpl_tls_state *state = qpl_tls_get_state();
    if (!state) {
        return NULL;
    }

    qpl_path_t path = ct_tree->config.qpl_path;
#ifdef ZIPCACHE_AGG_EXPERIMENT
    if (ct_tree->split_routes_enabled) {
        int role = is_compress ? 0 : agg_origin == BPLUS_ORIGIN_GET ? 1 : 2;
        path = ct_tree->split_routes[role];
        /* Reuse the read job when both decoding routes are identical. */
        if (role == 2 && path != ct_tree->split_routes[1])
            return qpl_tls_prepare_job(&state->maintenance_job,
                &state->maintenance_buffer, &state->maintenance_bytes,
                &state->maintenance_path, path);
    }
#endif
    if (is_compress) {
        return qpl_tls_prepare_job(&state->compress_job,
                                   &state->compress_buffer,
                                   &state->compress_bytes,
                                   &state->compress_path,
                                   path);
    }

    return qpl_tls_prepare_job(&state->decompress_job,
                               &state->decompress_buffer,
                               &state->decompress_bytes,
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

#ifdef HAVE_ZLIB
static voidpf compressed_zalloc(voidpf opaque, uInt items, uInt size)
{
    (void)opaque;
    void *ptr = calloc(items, size);
    __atomic_add_fetch(&zlib_live_bytes, allocation_usable_size(ptr, (size_t)items * size), __ATOMIC_RELAXED);
    return ptr;
}
static void compressed_zfree(voidpf opaque, voidpf ptr)
{
    (void)opaque;
    __atomic_sub_fetch(&zlib_live_bytes, allocation_usable_size(ptr, 0), __ATOMIC_RELAXED);
    free(ptr);
}
static int compressed_zlib_once(int compressing, int level, const uint8_t *src,
                                uint32_t src_size, uint8_t *dst, uint32_t capacity)
{
    z_stream stream = {0};
    stream.zalloc = compressed_zalloc;
    stream.zfree = compressed_zfree;
    int window = 15;
#ifdef ZIPCACHE_AGG_EXPERIMENT
    window = split_zlib_window_bits;
#endif
    int status = compressing ? deflateInit2(&stream, level, Z_DEFLATED, window, 8, Z_DEFAULT_STRATEGY) : inflateInit(&stream);
    if (status != Z_OK) return -1;
    stream.next_in = (Bytef *)src;
    stream.avail_in = src_size;
    stream.next_out = dst;
    stream.avail_out = capacity;
    status = compressing ? deflate(&stream, Z_FINISH) : inflate(&stream, Z_FINISH);
    int produced = (int)stream.total_out;
    int complete = status == Z_STREAM_END && stream.avail_in == 0;
    if (compressing) deflateEnd(&stream); else inflateEnd(&stream);
    return complete && produced > 0 ? produced : -1;
}

struct zlib_tls_state {
    z_stream deflate_stream;
    int deflate_initialized;
    int deflate_level;
    z_stream inflate_stream;
    int inflate_initialized;
};

static pthread_key_t zlib_tls_key;
static pthread_once_t zlib_tls_once = PTHREAD_ONCE_INIT;
static int zlib_tls_key_ready = 0;

static void zlib_tls_destroy(void *ptr)
{
    struct zlib_tls_state *state = (struct zlib_tls_state *)ptr;
    if (!state) {
        return;
    }
    if (state->deflate_initialized) {
        deflateEnd(&state->deflate_stream);
    }
    if (state->inflate_initialized) {
        inflateEnd(&state->inflate_stream);
    }
    __atomic_sub_fetch(&zlib_live_bytes, allocation_usable_size(state, sizeof(*state)), __ATOMIC_RELAXED);
    free(state);
}

static void zlib_tls_make_key(void)
{
    zlib_tls_key_ready = (pthread_key_create(&zlib_tls_key, zlib_tls_destroy) == 0);
}

static struct zlib_tls_state *zlib_tls_get_state(void)
{
    pthread_once(&zlib_tls_once, zlib_tls_make_key);
    if (!zlib_tls_key_ready) {
        return NULL;
    }

    struct zlib_tls_state *state = pthread_getspecific(zlib_tls_key);
    if (state) {
        return state;
    }

    state = calloc(1, sizeof(*state));
    if (!state) {
        return NULL;
    }
    state->deflate_level = INT_MIN;
    __atomic_add_fetch(&zlib_live_bytes, allocation_usable_size(state, sizeof(*state)), __ATOMIC_RELAXED);
    if (pthread_setspecific(zlib_tls_key, state) != 0) {
        __atomic_sub_fetch(&zlib_live_bytes, allocation_usable_size(state, sizeof(*state)), __ATOMIC_RELAXED);
        free(state);
        return NULL;
    }
    return state;
}

static int zlib_stream_cache_enabled(void)
{
    static int cached = -1;
    int current = __atomic_load_n(&cached, __ATOMIC_RELAXED);
    if (current >= 0) {
        return current;
    }

    const char *value = getenv("BTREE_ZLIB_STREAM_CACHE");
    int enabled = 0;
    if (value && *value) {
        enabled = (strcasecmp(value, "thread") == 0 ||
                   strcasecmp(value, "tls") == 0 ||
                   strcmp(value, "1") == 0 ||
                   strcasecmp(value, "true") == 0 ||
                   strcasecmp(value, "on") == 0);
        if (!enabled &&
            strcasecmp(value, "none") != 0 &&
            strcmp(value, "0") != 0 &&
            strcasecmp(value, "false") != 0 &&
            strcasecmp(value, "off") != 0) {
            fprintf(stderr,
                    "Invalid BTREE_ZLIB_STREAM_CACHE=%s; use none or thread, using none\n",
                    value);
        }
    }

    __atomic_store_n(&cached, enabled, __ATOMIC_RELAXED);
    return enabled;
}

static int zlib_tls_compress(struct bplus_tree_compressed *ct_tree,
                             const uint8_t *src,
                             uint32_t src_size,
                             uint8_t *dst,
                             uint32_t dst_capacity,
                             int level)
{
    struct zlib_tls_state *state = zlib_tls_get_state();
    if (!state) {
        return -1;
    }

    if (state->deflate_initialized && state->deflate_level != level) {
        deflateEnd(&state->deflate_stream);
        memset(&state->deflate_stream, 0, sizeof(state->deflate_stream));
        state->deflate_initialized = 0;
        state->deflate_level = INT_MIN;
    }

    if (!state->deflate_initialized) {
        memset(&state->deflate_stream, 0, sizeof(state->deflate_stream));
        state->deflate_stream.zalloc = compressed_zalloc;
        state->deflate_stream.zfree = compressed_zfree;
        int window = 15;
#ifdef ZIPCACHE_AGG_EXPERIMENT
        window = split_zlib_window_bits;
#endif
        int init_status = deflateInit2(&state->deflate_stream, level, Z_DEFLATED, window, 8, Z_DEFAULT_STRATEGY);
        if (init_status != Z_OK) {
            return -1;
        }
        state->deflate_initialized = 1;
        state->deflate_level = level;
        if (ct_tree) {
            __atomic_add_fetch(&ct_tree->zlib_stream_inits, 1, __ATOMIC_RELAXED);
        }
    } else if (ct_tree) {
        __atomic_add_fetch(&ct_tree->zlib_stream_reuses, 1, __ATOMIC_RELAXED);
    }

    state->deflate_stream.next_in = (Bytef *)src;
    state->deflate_stream.avail_in = src_size;
    state->deflate_stream.next_out = (Bytef *)dst;
    state->deflate_stream.avail_out = dst_capacity;

    int status = deflate(&state->deflate_stream, Z_FINISH);
    uint32_t produced = dst_capacity - state->deflate_stream.avail_out;
    uInt remaining_in = state->deflate_stream.avail_in;
    int reset_status = deflateReset(&state->deflate_stream);

    if (status == Z_STREAM_END &&
        reset_status == Z_OK &&
        remaining_in == 0 &&
        produced > 0 &&
        produced <= dst_capacity) {
        return (int)produced;
    }
    return -1;
}

static int zlib_tls_decompress(struct bplus_tree_compressed *ct_tree,
                               const uint8_t *src,
                               uint32_t src_size,
                               uint8_t *dst,
                               uint32_t dst_capacity)
{
    struct zlib_tls_state *state = zlib_tls_get_state();
    if (!state) {
        return -1;
    }

    if (!state->inflate_initialized) {
        memset(&state->inflate_stream, 0, sizeof(state->inflate_stream));
        state->inflate_stream.zalloc = compressed_zalloc;
        state->inflate_stream.zfree = compressed_zfree;
        int init_status = inflateInit(&state->inflate_stream);
        if (init_status != Z_OK) {
            return -1;
        }
        state->inflate_initialized = 1;
        if (ct_tree) {
            __atomic_add_fetch(&ct_tree->zlib_stream_inits, 1, __ATOMIC_RELAXED);
        }
    } else if (ct_tree) {
        __atomic_add_fetch(&ct_tree->zlib_stream_reuses, 1, __ATOMIC_RELAXED);
    }

    state->inflate_stream.next_in = (Bytef *)src;
    state->inflate_stream.avail_in = src_size;
    state->inflate_stream.next_out = (Bytef *)dst;
    state->inflate_stream.avail_out = dst_capacity;

    int status = inflate(&state->inflate_stream, Z_FINISH);
    uint32_t produced = dst_capacity - state->inflate_stream.avail_out;
    int reset_status = inflateReset(&state->inflate_stream);

    if (status == Z_STREAM_END &&
        reset_status == Z_OK &&
        produced > 0 &&
        produced <= dst_capacity) {
        return (int)produced;
    }
    return -1;
}
#else
static int zlib_stream_cache_enabled(void)
{
    return 0;
}
#endif

#include "bplustree_zstd_experiment.inc"
void bplus_tree_compressed_release_thread_resources(void)
{
    if (qpl_tls_key_ready) {
        void *state = pthread_getspecific(qpl_tls_key);
        pthread_setspecific(qpl_tls_key, NULL);
        qpl_tls_destroy(state);
    }
#ifdef HAVE_ZLIB
    if (zlib_tls_key_ready) {
        void *state = pthread_getspecific(zlib_tls_key);
        pthread_setspecific(zlib_tls_key, NULL);
        zlib_tls_destroy(state);
    }
#endif
#ifdef HAVE_ZSTD
    if (zstd_key_ready) {
        void *state=pthread_getspecific(zstd_key); pthread_setspecific(zstd_key,NULL); zstd_destroy(state);
    }
#endif
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

static int qpl_software_fallback_once(struct bplus_tree_compressed *ct_tree,
                                      int is_compress,
                                      const uint8_t *src,
                                      uint32_t src_size,
                                      uint8_t *dst,
                                      uint32_t dst_capacity)
{
    if (!ct_tree || ct_tree->config.qpl_path != qpl_path_auto) {
        return -1;
    }

    __atomic_add_fetch(&ct_tree->codec_fallbacks, 1, __ATOMIC_RELAXED);

    uint32_t job_size = 0;
    if (qpl_get_job_size(qpl_path_software, &job_size) != QPL_STS_OK || job_size == 0) {
        return -1;
    }
    uint8_t *buffer = malloc(job_size);
    if (!buffer) {
        return -1;
    }
    qpl_job *job = (qpl_job *)buffer;
    if (qpl_init_job(qpl_path_software, job) != QPL_STS_OK) {
        free(buffer);
        return -1;
    }

    job->op = is_compress ? qpl_op_compress : qpl_op_decompress;
    job->next_in_ptr = (uint8_t *)src;
    job->available_in = src_size;
    job->total_in = 0;
    job->next_out_ptr = dst;
    job->available_out = dst_capacity;
    job->total_out = 0;
    job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
    if (is_compress && ct_tree->config.qpl_huffman_mode == QPL_HUFFMAN_DYNAMIC) {
        job->flags |= QPL_FLAG_DYNAMIC_HUFFMAN;
    }
    job->level = qpl_default_level;
    qpl_status status = qpl_execute_job(job);
    uint32_t produced = job->total_out;
    qpl_fini_job(job);
    free(buffer);
    return status == QPL_STS_OK && produced > 0 && produced <= dst_capacity
        ? (int)produced : -1;
}

#include "bplustree_execution_split.inc"

static int codec_compress_bytes(struct bplus_tree_compressed *ct_tree,
                                 struct simple_leaf_node *leaf,
                                 const uint8_t *src,
                                 uint32_t src_size,
                                 uint8_t *dst,
                                 uint32_t dst_capacity)
{
    if (leaf->compression_algo == COMPRESS_ZSTD_EXPERIMENT) {
#ifdef HAVE_ZSTD
        return zstd_transform(1,src,src_size,dst,dst_capacity);
#else
        return -1;
#endif
    }
    if (leaf->compression_algo == COMPRESS_RAW_PACKED) {
        uint32_t used = 0;
        for (uint32_t off = 0; off + sizeof(struct kv_pair) <= src_size;
             off += sizeof(struct kv_pair)) {
            key_t key;
            memcpy(&key, src + off, sizeof(key));
            if (!key) continue;
            if (used + sizeof(struct kv_pair) > dst_capacity) return -1;
            memcpy(dst + used, src + off, sizeof(struct kv_pair));
            used += sizeof(struct kv_pair);
        }
        /* Explicit empty-block marker; no padded-page capacity benefit. */
        if (!used) { if (!dst_capacity) return -1; dst[0] = 0; return 1; }
        return (int)used;
    }
    if (leaf->compression_algo == COMPRESS_COPY) {
        if (src_size == 0 || src_size > dst_capacity) {
            return -1;
        }
        memcpy(dst, src, src_size);
        return (int)src_size;
    }

    if (leaf->compression_algo == COMPRESS_QPL) {
#ifdef ZIPCACHE_AGG_EXPERIMENT
        if (ct_tree->split_routes_enabled)
            return split_qpl_transform(ct_tree, 1, src, src_size, dst, dst_capacity);
#endif
        if (ct_tree) {
            __atomic_add_fetch(&ct_tree->qpl_compress_calls, 1, __ATOMIC_RELAXED);
        }
        qpl_job *tls_job = acquire_qpl_tls_job(ct_tree, 1);
        if (tls_job) {
            __atomic_add_fetch(&ct_tree->qpl_tls_jobs, 1, __ATOMIC_RELAXED);
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
            __atomic_add_fetch(&ct_tree->qpl_errors, 1, __ATOMIC_RELAXED);
            if (qpl_hardware_strict(ct_tree)) {
                return -1;
            }
        }

        if (ct_tree->qpl_pool_size > 0) {
            int job_index = -1;
            qpl_job *job = acquire_qpl_job(ct_tree, &job_index);
            if (job) {
                __atomic_add_fetch(&ct_tree->qpl_pool_jobs, 1, __ATOMIC_RELAXED);
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
                __atomic_add_fetch(&ct_tree->qpl_errors, 1, __ATOMIC_RELAXED);
            }
            if (qpl_hardware_strict(ct_tree)) {
                return -1;
            }
        } else if (qpl_hardware_strict(ct_tree)) {
            return -1;
        }

        int software_size = qpl_software_fallback_once(ct_tree,
                                                       1,
                                                       src,
                                                       src_size,
                                                       dst,
                                                       dst_capacity);
        return software_size;
    }

#ifdef HAVE_ZLIB
    if (leaf->compression_algo == COMPRESS_ZLIB_ACCEL) {
        if (ct_tree) {
            __atomic_add_fetch(&ct_tree->zlib_compress_calls, 1, __ATOMIC_RELAXED);
        }
        int level = ct_tree->config.compression_level;
        if (level < Z_NO_COMPRESSION || level > Z_BEST_COMPRESSION) {
            level = Z_DEFAULT_COMPRESSION;
        }

        if (zlib_stream_cache_enabled()) {
            int cached_size = zlib_tls_compress(ct_tree, src, src_size, dst, dst_capacity, level);
            if (cached_size > 0) {
                return cached_size;
            }
        }

        int produced = compressed_zlib_once(1, level, src, src_size, dst, dst_capacity);
        if (produced > 0) return produced;
        if (ct_tree) {
            __atomic_add_fetch(&ct_tree->zlib_errors, 1, __ATOMIC_RELAXED);
        }
        return -1;
    }
#endif

    if (leaf->compression_algo != COMPRESS_LZ4) return -1;

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

#if ZIPCACHE_AGG_LAYOUT > 0
static int aggregate_compress(struct bplus_tree_compressed *, struct simple_leaf_node *,
                             const uint8_t *, uint32_t, uint8_t *, uint32_t);
#endif
static int compress_subpage_impl(struct bplus_tree_compressed *tree, struct simple_leaf_node *leaf,
                                const uint8_t *src, uint32_t n, uint8_t *dst, uint32_t cap)
{
#if ZIPCACHE_AGG_LAYOUT > 0
    return aggregate_compress(tree, leaf, src, n, dst, cap);
#else
    return codec_compress_bytes(tree, leaf, src, n, dst, cap);
#endif
}
static int compress_subpage(struct bplus_tree_compressed *ct_tree,
                            struct simple_leaf_node *leaf,
                            const uint8_t *src,
                            uint32_t src_size,
                            uint8_t *dst,
                            uint32_t dst_capacity)
{
    int profiled = ZIPCACHE_AGG_LAYOUT ? 0 : submission_profile_codec_begin(ct_tree,
                                                  1,
                                                  src_size,
                                                  dst_capacity,
                                                  leaf);
    int result = compress_subpage_impl(ct_tree, leaf, src, src_size, dst, dst_capacity);
#if defined(ZIPCACHE_AGG_EXPERIMENT) && ZIPCACHE_AGG_LAYOUT == 0
    AGG_INC(compress[agg_origin], 1); AGG_INC(encode_bytes[agg_origin], src_size);
#endif
    submission_profile_codec_end(ct_tree, profiled);
    return result;
}

static int codec_decompress_bytes(struct bplus_tree_compressed *ct_tree,
                                   struct simple_leaf_node *leaf,
                                   const uint8_t *src,
                                   uint32_t src_size,
                                   uint8_t *dst,
                                   uint32_t dst_capacity)
{
    if (leaf->compression_algo == COMPRESS_ZSTD_EXPERIMENT) {
#ifdef HAVE_ZSTD
        return zstd_transform(0,src,src_size,dst,dst_capacity);
#else
        return -1;
#endif
    }
    if (leaf->compression_algo == COMPRESS_RAW_PACKED) {
        if (src_size == 1 && src[0] == 0) {
            memset(dst, 0, dst_capacity);
            return (int)dst_capacity;
        }
        if (!src_size || src_size % sizeof(struct kv_pair) || src_size > dst_capacity)
            return -1;
        memset(dst, 0, dst_capacity);
        memcpy(dst, src, src_size);
        return (int)dst_capacity;
    }
    if (leaf->compression_algo == COMPRESS_COPY) {
        if (src_size == 0 || src_size > dst_capacity) {
            return -1;
        }
        memcpy(dst, src, src_size);
        return (int)src_size;
    }

    if (leaf->compression_algo == COMPRESS_QPL) {
#ifdef ZIPCACHE_AGG_EXPERIMENT
        if (ct_tree->split_routes_enabled)
            return split_qpl_transform(ct_tree, 0, src, src_size, dst, dst_capacity);
#endif
        if (ct_tree) {
            __atomic_add_fetch(&ct_tree->qpl_decompress_calls, 1, __ATOMIC_RELAXED);
        }
        qpl_job *tls_job = acquire_qpl_tls_job(ct_tree, 0);
        if (tls_job) {
            __atomic_add_fetch(&ct_tree->qpl_tls_jobs, 1, __ATOMIC_RELAXED);
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
            __atomic_add_fetch(&ct_tree->qpl_errors, 1, __ATOMIC_RELAXED);
            if (qpl_hardware_strict(ct_tree)) {
                return -1;
            }
        }

        if (ct_tree->qpl_pool_size > 0) {
            int job_index = -1;
            qpl_job *job = acquire_qpl_job(ct_tree, &job_index);
            if (job) {
                __atomic_add_fetch(&ct_tree->qpl_pool_jobs, 1, __ATOMIC_RELAXED);
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
                __atomic_add_fetch(&ct_tree->qpl_errors, 1, __ATOMIC_RELAXED);
            }
            if (qpl_hardware_strict(ct_tree)) {
                return -1;
            }
        } else if (qpl_hardware_strict(ct_tree)) {
            return -1;
        }

        int software_size = qpl_software_fallback_once(ct_tree,
                                                       0,
                                                       src,
                                                       src_size,
                                                       dst,
                                                       dst_capacity);
        return software_size;
    }

#ifdef HAVE_ZLIB
    if (leaf->compression_algo == COMPRESS_ZLIB_ACCEL) {
        if (ct_tree) {
            __atomic_add_fetch(&ct_tree->zlib_decompress_calls, 1, __ATOMIC_RELAXED);
        }
        if (zlib_stream_cache_enabled()) {
            int cached_size = zlib_tls_decompress(ct_tree, src, src_size, dst, dst_capacity);
            if (cached_size > 0) {
                return cached_size;
            }
        }

        int produced = compressed_zlib_once(0, 0, src, src_size, dst, dst_capacity);
        if (produced > 0) return produced;
        if (ct_tree) {
            __atomic_add_fetch(&ct_tree->zlib_errors, 1, __ATOMIC_RELAXED);
        }
        return -1;
    }
#endif

    if (leaf->compression_algo != COMPRESS_LZ4) return -1;
    return LZ4_decompress_safe((const char *)src, (char *)dst, (int)src_size, (int)dst_capacity);
}

#include "bplustree_aggregation_codec.inc"

static int decompress_subpage_impl(struct bplus_tree_compressed *tree, struct simple_leaf_node *leaf,
                                  const uint8_t *src, uint32_t n, uint8_t *dst, uint32_t cap)
{
#if ZIPCACHE_AGG_LAYOUT > 0
    return aggregate_decompress(tree, leaf, src, n, dst, cap);
#else
    return codec_decompress_bytes(tree, leaf, src, n, dst, cap);
#endif
}
static int decompress_subpage(struct bplus_tree_compressed *ct_tree,
                              struct simple_leaf_node *leaf,
                              const uint8_t *src,
                              uint32_t src_size,
                              uint8_t *dst,
                              uint32_t dst_capacity)
{
    int profiled = ZIPCACHE_AGG_LAYOUT ? 0 : submission_profile_codec_begin(ct_tree,
                                                  0,
                                                  src_size,
                                                  dst_capacity,
                                                  leaf);
    int result = decompress_subpage_impl(ct_tree, leaf, src, src_size, dst, dst_capacity);
#if defined(ZIPCACHE_AGG_EXPERIMENT) && ZIPCACHE_AGG_LAYOUT == 0
    AGG_INC(decompress[agg_origin], 1); AGG_INC(decode_bytes[agg_origin], dst_capacity);
#endif
    submission_profile_codec_end(ct_tree, profiled);
    /* Every stored block encodes its complete logical subpage. Reject short
     * successful decodes before readers can inspect uninitialized tail bytes. */
    return result == (int)dst_capacity ? result : -1;
}

static int compressed_leaf_landing_count(struct bplus_tree_compressed *ct_tree,
                                         struct simple_leaf_node *leaf)
{
    if (!ct_tree || !leaf) {
        return 0;
    }

    int landing_capacity = landing_buffer_capacity_for_tree(ct_tree);
    struct kv_pair *landing = (struct kv_pair *)leaf->landing_buffer;
    int count = 0;
    for (int i = 0; i < landing_capacity; i++) {
        if (landing[i].key != 0) {
            count++;
        }
    }
    return count;
}

static int compressed_leaf_collect_base_pairs(struct bplus_tree_compressed *ct_tree,
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

    *out_pairs = pairs;
    *out_count = count;
    return 0;
}

static int compressed_leaf_collect_pairs(struct bplus_tree_compressed *ct_tree,
                                         struct simple_leaf_node *leaf,
                                         struct kv_pair **out_pairs,
                                         size_t *out_count)
{
    struct kv_pair *pairs = NULL;
    size_t count = 0;
    size_t capacity = 0;
    if (compressed_leaf_collect_base_pairs(ct_tree, leaf, &pairs, &count) != 0) {
        return -1;
    }
    capacity = count;

    /* Pending is newer than base and older than active. */
    if (leaf->pending) {
        for (size_t i = 0; i < leaf->pending->count; i++) {
            struct kv_pair *entry = &leaf->pending->records[i];
            if (entry->key != 0 &&
                kv_vector_put(&pairs,
                              &count,
                              &capacity,
                              entry->key,
                              entry->stored_value,
                              entry->payload,
                              COMPRESSED_VALUE_BYTES) != 0) {
                free(pairs);
                return -1;
            }
        }
    }

    /* Active is the newest layer and therefore overwrites pending/base. */
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

/*
 * Build a base-only image without touching resident leaf state.  Return -2
 * when the logical 4 KiB page cannot hold the merged key set; callers route
 * that case to the existing structural split path.
 */
static int compressed_build_base_image(struct bplus_tree_compressed *ct_tree,
                                       struct simple_leaf_node *leaf,
                                       struct kv_pair *pairs,
                                       size_t count,
                                       struct compressed_page_image *image)
{
    if (!ct_tree || !leaf || !image) {
        return -1;
    }
    memset(image, 0, sizeof(*image));

    int num_subpages = leaf->num_subpages > 0
                         ? leaf->num_subpages
                         : (ct_tree->config.default_sub_pages > 0
                              ? ct_tree->config.default_sub_pages
                              : 1);
    int sub_page_size = COMPRESSED_LEAF_SIZE / num_subpages;
    if (sub_page_size <= 0) {
        return -1;
    }

    struct subpage_index_entry *index = calloc((size_t)num_subpages, sizeof(*index));
    if (!index) {
        return -1;
    }

    if (count == 0) {
        image->index = index;
        image->num_subpages = num_subpages;
        return 0;
    }

    qsort(pairs, count, sizeof(*pairs), compare_kv_pairs);

    uint8_t raw[COMPRESSED_LEAF_SIZE];
    uint8_t compressed[MAX_COMPRESSED_SIZE];
    memset(raw, 0, sizeof(raw));

    size_t bucket_capacity = (size_t)sub_page_size / sizeof(struct kv_pair);
    for (size_t i = 0; i < count; i++) {
        int bucket = positive_mod_i32(pairs[i].key, num_subpages);
        struct kv_pair *slots = (struct kv_pair *)(void *)(raw + bucket * sub_page_size);
        bool placed = false;
        for (size_t j = 0; j < bucket_capacity; j++) {
            if (slots[j].key == 0 || slots[j].key == pairs[i].key) {
                slots[j] = pairs[i];
                placed = true;
                break;
            }
        }
        if (!placed) {
            free(index);
            return -2;
        }
    }

    struct simple_leaf_node codec_leaf;
    memset(&codec_leaf, 0, sizeof(codec_leaf));
    codec_leaf.compression_algo = leaf->compression_algo;
    codec_leaf.num_subpages = num_subpages;

    size_t running_offset = 0;
    for (int bucket = 0; bucket < num_subpages; bucket++) {
        struct kv_pair *slots = (struct kv_pair *)(void *)(raw + bucket * sub_page_size);
        bool empty = true;
        for (size_t j = 0; j < bucket_capacity; j++) {
            if (slots[j].key != 0) {
                empty = false;
                break;
            }
        }
        index[bucket].offset = (uint32_t)running_offset;
        index[bucket].uncompressed_bytes = empty ? 0U : (uint32_t)sub_page_size;
        if (empty) {
            continue;
        }

        uint32_t available = MAX_COMPRESSED_SIZE - (uint32_t)running_offset;
        int produced = compress_subpage(ct_tree,
                                        &codec_leaf,
                                        raw + bucket * sub_page_size,
                                        (uint32_t)sub_page_size,
                                        compressed + running_offset,
                                        available);
        if (produced <= 0 || running_offset + (size_t)produced > MAX_COMPRESSED_SIZE) {
            free(index);
            return -1;
        }
        index[bucket].length = (uint32_t)produced;
        running_offset += (size_t)produced;
    }

    char *data = NULL;
    if (running_offset > 0) {
        data = malloc(running_offset);
        if (!data) {
            free(index);
            return -1;
        }
        memcpy(data, compressed, running_offset);
    }

    image->data = data;
    image->size = running_offset;
    image->usable = allocation_usable_size(data, running_offset);
    image->index = index;
    image->num_subpages = num_subpages;
    image->uncompressed_bytes = count * sizeof(struct kv_pair);
    return 0;
}

static void compressed_page_image_destroy(struct compressed_page_image *image)
{
    if (!image) {
        return;
    }
    free(image->data);
    free(image->index);
    memset(image, 0, sizeof(*image));
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
    struct kv_pair new_active[ACTIVE_DELTA_ENTRIES];
    memset(new_active, 0, sizeof(new_active));

    if (count == 0) {
        if (compressed_leaf_replace_base(leaf, NULL, 0) != 0) {
            return -1;
        }
        memset(leaf->landing_buffer, 0, sizeof(leaf->landing_buffer));
        if (leaf->subpage_index && leaf->num_subpages > 0) {
            memset(leaf->subpage_index, 0,
                   (size_t)leaf->num_subpages * sizeof(struct subpage_index_entry));
        }
        leaf->uncompressed_bytes = 0;
        leaf->generation++;
        return 0;
    }

    qsort(pairs, count, sizeof(struct kv_pair), compare_kv_pairs);

    size_t landing_slots = (size_t)landing_buffer_capacity_for_tree(ct_tree);
    size_t landing_count = count < landing_slots ? count : landing_slots;

    for (size_t i = 0; i < landing_count; i++) {
        struct kv_pair *slot = &new_active[i];
        slot->key = pairs[i].key;
        slot->stored_value = pairs[i].stored_value;
        memcpy(slot->payload, pairs[i].payload, COMPRESSED_VALUE_BYTES);
    }

    if (landing_count == count) {
        if (compressed_leaf_replace_base(leaf, NULL, 0) != 0) {
            return -1;
        }
        memcpy(leaf->active, new_active, sizeof(new_active));
        if (leaf->subpage_index && leaf->num_subpages > 0) {
            memset(leaf->subpage_index, 0,
                   (size_t)leaf->num_subpages * sizeof(struct subpage_index_entry));
        }
        leaf->uncompressed_bytes = landing_count * sizeof(struct kv_pair);
        leaf->generation++;
        return 0;
    }

    if (leaf->num_subpages <= 0) {
        leaf->num_subpages = 1;
    }

    if (leaf->subpage_index == NULL) {
        leaf->subpage_index = leaf_index_allocate(leaf, leaf->num_subpages);
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

    if (compressed_leaf_replace_base(leaf, temp_compressed, running_offset) != 0) {
        free(temp_index);
        return -1;
    }
    memcpy(leaf->active, new_active, sizeof(new_active));
    memcpy(leaf->subpage_index, temp_index, leaf->num_subpages * sizeof(struct subpage_index_entry));

    free(temp_index);

    leaf->uncompressed_bytes = hashed_count * sizeof(struct kv_pair);
    leaf->num_subpage_entries = leaf->num_subpages;
    leaf->generation++;
    return 0;
}

static int compressed_leaf_flush_landing_locked(struct bplus_tree_compressed *ct_tree,
                                                struct simple_leaf_node *leaf)
{
    if (!ct_tree || !leaf) {
        return -1;
    }
    if (compressed_leaf_landing_count(ct_tree, leaf) == 0) {
        return 0;
    }

    struct kv_pair *pairs = NULL;
    size_t count = 0;
    if (compressed_leaf_collect_pairs(ct_tree, leaf, &pairs, &count) != 0) {
        free(pairs);
        return -1;
    }

    if (count == 0) {
        memset(leaf->landing_buffer, 0, sizeof(leaf->landing_buffer));
        if (compressed_leaf_replace_base(leaf, NULL, 0) != 0) {
            free(pairs);
            return -1;
        }
        leaf->uncompressed_bytes = 0;
        leaf->generation++;
        free(pairs);
        return 0;
    }

    qsort(pairs, count, sizeof(struct kv_pair), compare_kv_pairs);

    if (leaf->num_subpages <= 0) {
        leaf->num_subpages = ct_tree->config.default_sub_pages > 0
                                ? ct_tree->config.default_sub_pages
                                : 1;
    }

    int sub_page_size = COMPRESSED_LEAF_SIZE / leaf->num_subpages;
    if (sub_page_size <= 0) {
        sub_page_size = COMPRESSED_LEAF_SIZE;
    }

    int bucket_capacity = sub_page_size / (int)sizeof(struct kv_pair);
    if (bucket_capacity <= 0) {
        free(pairs);
        return -1;
    }

    char uncompressed_pages[COMPRESSED_LEAF_SIZE];
    memset(uncompressed_pages, 0, sizeof(uncompressed_pages));

    for (size_t i = 0; i < count; i++) {
        struct kv_pair *entry = &pairs[i];
        int bucket = positive_mod_i32(entry->key, leaf->num_subpages);
        struct kv_pair *bucket_begin = (struct kv_pair *)(uncompressed_pages + bucket * sub_page_size);
        bool placed = false;
        for (int j = 0; j < bucket_capacity; j++) {
            if (bucket_begin[j].key == 0 || bucket_begin[j].key == entry->key) {
                bucket_begin[j] = *entry;
                placed = true;
                break;
            }
        }
        if (!placed) {
            free(pairs);
            return -1;
        }
    }

    struct subpage_index_entry *temp_index = calloc((size_t)leaf->num_subpages, sizeof(*temp_index));
    if (!temp_index) {
        free(pairs);
        return -1;
    }

    char temp_compressed[MAX_COMPRESSED_SIZE];
    size_t running_offset = 0;

    struct simple_leaf_node output_leaf;
    memset(&output_leaf, 0, sizeof(output_leaf));
    output_leaf.compression_algo = leaf->compression_algo;
    output_leaf.num_subpages = leaf->num_subpages;

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
            temp_index[b].offset = (uint32_t)running_offset;
            temp_index[b].length = 0;
            continue;
        }

        uint32_t dest_capacity = MAX_COMPRESSED_SIZE - (uint32_t)running_offset;
        if (dest_capacity == 0) {
            free(temp_index);
            free(pairs);
            return -1;
        }

        int compressed_size = compress_subpage(ct_tree,
                                               &output_leaf,
                                               (const uint8_t *)start,
                                               sub_page_size,
                                               (uint8_t *)temp_compressed + running_offset,
                                               dest_capacity);
        if (compressed_size <= 0 || running_offset + (size_t)compressed_size > MAX_COMPRESSED_SIZE) {
            free(temp_index);
            free(pairs);
            return -1;
        }

        temp_index[b].offset = (uint32_t)running_offset;
        temp_index[b].length = (uint32_t)compressed_size;
        running_offset += (size_t)compressed_size;
    }

    if (leaf->subpage_index == NULL) {
        leaf->subpage_index = leaf_index_allocate(leaf, leaf->num_subpages);
        if (!leaf->subpage_index) {
            free(temp_index);
            free(pairs);
            return -1;
        }
    }

    if (compressed_leaf_replace_base(leaf, temp_compressed, running_offset) != 0) {
        free(temp_index);
        free(pairs);
        return -1;
    }
    memset(leaf->landing_buffer, 0, sizeof(leaf->landing_buffer));
    memcpy(leaf->subpage_index, temp_index, (size_t)leaf->num_subpages * sizeof(*temp_index));

    leaf->uncompressed_bytes = count * sizeof(struct kv_pair);
    leaf->num_subpage_entries = leaf->num_subpages;
    leaf->generation++;

    __atomic_add_fetch(&ct_tree->compression_operations, 1, __ATOMIC_SEQ_CST);

    free(temp_index);
    free(pairs);
    return 1;
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
        struct compressed_leaf_ref *leaf = (struct compressed_leaf_ref *)node;
        if (leaf->payload == 0) {
            return -1;
        }
        struct simple_leaf_node *custom_leaf = (struct simple_leaf_node *)leaf->payload;
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

    while (parent) {
        /* parent_key_idx is the separator immediately to this child's left:
         * child slot 0 uses -1, slot 1 uses key[0], and so on.  A non-leftmost
         * child's minimum changes exactly one separator and cannot change the
         * parent subtree minimum.  A leftmost child's minimum changes no key
         * in this parent, but must continue toward the first ancestor where
         * the parent itself is non-leftmost. */
        int separator_index = child->parent_key_idx;
        if (separator_index >= 0) {
            parent->key[separator_index] = new_min_key;
            return;
        }

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
    free(leaf->pending);
    leaf->pending = NULL;
    free(leaf->compressed_data);
    leaf_index_release(leaf);
    free(leaf);
}

static int parse_background_codec_filter(void)
{
    const char *value = getenv("BTREE_BG_CODEC");
    if (!value || !*value || strcasecmp(value, "all") == 0) {
        return -1;
    }
    if (strcasecmp(value, "lz4") == 0) {
        return COMPRESS_LZ4;
    }
    if (strcasecmp(value, "qpl") == 0) {
        return COMPRESS_QPL;
    }
    if (strcasecmp(value, "zlib") == 0 ||
        strcasecmp(value, "zlib_accel") == 0 ||
        strcasecmp(value, "zlib-accel") == 0) {
        return COMPRESS_ZLIB_ACCEL;
    }

    fprintf(stderr,
            "Invalid BTREE_BG_CODEC=%s; use all, lz4, qpl, or zlib_accel; using all\n",
            value);
    return -1;
}

static int background_codec_allowed(const struct bplus_tree_compressed *ct_tree,
                                    const struct simple_leaf_node *leaf)
{
    if (!ct_tree || !leaf) {
        return 0;
    }
    if (ct_tree->bg_codec_filter < 0) {
        return 1;
    }
    return leaf->compression_algo == (compression_algo_t)ct_tree->bg_codec_filter;
}

static void background_compaction_scan_once(struct bplus_tree_compressed *ct_tree)
{
    if (!ct_tree || !ct_tree->initialized || !ct_tree->tree || !ct_tree->tree->root) {
        return;
    }

    if (pthread_mutex_trylock(&ct_tree->bg_scan_lock) != 0) {
        __atomic_add_fetch(&ct_tree->bg_skipped, 1, __ATOMIC_RELAXED);
        return;
    }

    __atomic_add_fetch(&ct_tree->bg_passes, 1, __ATOMIC_RELAXED);

    pthread_rwlock_rdlock(&ct_tree->rwlock);

    int visited = 0;
    int compacted_this_pass = 0;
    int max_leaves = ct_tree->bg_max_leaves_per_pass > 0
                       ? ct_tree->bg_max_leaves_per_pass
                       : INT_MAX;
    int high_pct = ct_tree->bg_landing_high_watermark_pct;
    if (high_pct < 1) {
        high_pct = 1;
    } else if (high_pct > 100) {
        high_pct = 100;
    }

    struct list_head *head = &ct_tree->tree->list[0];
    struct list_head *pos, *n;
    list_for_each_safe(pos, n, head) {
        if (__atomic_load_n(&ct_tree->bg_shutdown, __ATOMIC_RELAXED)) {
            break;
        }
        if (visited++ >= max_leaves) {
            break;
        }

        struct compressed_leaf_ref *leaf = list_entry(pos, struct compressed_leaf_ref, link);
        if (leaf->type != BPLUS_TREE_LEAF || leaf->payload == 0) {
            continue;
        }

        struct simple_leaf_node *custom_leaf = (struct simple_leaf_node *)leaf->payload;
        if (!background_codec_allowed(ct_tree, custom_leaf)) {
            continue;
        }
        int lock_rc = ct_tree->bg_trylock_only
                        ? pthread_rwlock_trywrlock(&custom_leaf->rwlock)
                        : pthread_rwlock_wrlock(&custom_leaf->rwlock);
        if (lock_rc != 0) {
            __atomic_add_fetch(&ct_tree->bg_trylock_misses, 1, __ATOMIC_RELAXED);
            continue;
        }

        int landing_count = compressed_leaf_landing_count(ct_tree, custom_leaf);
        int landing_capacity = landing_buffer_capacity_for_tree(ct_tree);
        int should_compact = 0;
        if (landing_count > 0 && landing_capacity > 0) {
            should_compact = landing_count * 100 >= landing_capacity * high_pct;
        }

        if (should_compact) {
            int rc = compressed_leaf_flush_landing_locked(ct_tree, custom_leaf);
            if (rc > 0) {
                __atomic_add_fetch(&ct_tree->bg_compactions, 1, __ATOMIC_RELAXED);
                compacted_this_pass++;
            } else if (rc < 0) {
                __atomic_add_fetch(&ct_tree->bg_errors, 1, __ATOMIC_RELAXED);
            }
        }

        pthread_rwlock_unlock(&custom_leaf->rwlock);

        if (ct_tree->bg_max_compactions_per_sec > 0 && compacted_this_pass > 0) {
            usleep((useconds_t)(1000000 / ct_tree->bg_max_compactions_per_sec));
        }
    }

    pthread_rwlock_unlock(&ct_tree->rwlock);
    pthread_mutex_unlock(&ct_tree->bg_scan_lock);
}

static int background_dirty_queue_pop(struct bplus_tree_compressed *ct_tree, key_t *key_out)
{
    if (!ct_tree || !key_out || !ct_tree->bg_dirty_keys || ct_tree->bg_queue_capacity <= 0) {
        return 0;
    }

    int popped = 0;
    pthread_mutex_lock(&ct_tree->bg_queue_lock);
    if (ct_tree->bg_queue_count > 0) {
        *key_out = ct_tree->bg_dirty_keys[ct_tree->bg_queue_head];
        ct_tree->bg_queue_head = (ct_tree->bg_queue_head + 1) % ct_tree->bg_queue_capacity;
        ct_tree->bg_queue_count--;
        popped = 1;
    }
    pthread_mutex_unlock(&ct_tree->bg_queue_lock);

    if (popped) {
        __atomic_add_fetch(&ct_tree->bg_queue_pops, 1, __ATOMIC_RELAXED);
    }
    return popped;
}

static int background_compact_key(struct bplus_tree_compressed *ct_tree, key_t key)
{
    if (!ct_tree || !ct_tree->initialized || !ct_tree->tree || !ct_tree->tree->root) {
        return 0;
    }

    pthread_rwlock_rdlock(&ct_tree->rwlock);

    struct compressed_leaf_ref *leaf = find_leaf_for_key(ct_tree->tree, key);
    if (!leaf || leaf->type != BPLUS_TREE_LEAF || leaf->payload == 0) {
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return 0;
    }

    struct simple_leaf_node *custom_leaf = (struct simple_leaf_node *)leaf->payload;
    if (!background_codec_allowed(ct_tree, custom_leaf)) {
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return 0;
    }
    int lock_rc = ct_tree->bg_trylock_only
                    ? pthread_rwlock_trywrlock(&custom_leaf->rwlock)
                    : pthread_rwlock_wrlock(&custom_leaf->rwlock);
    if (lock_rc != 0) {
        __atomic_add_fetch(&ct_tree->bg_trylock_misses, 1, __ATOMIC_RELAXED);
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return 0;
    }

    int landing_count = compressed_leaf_landing_count(ct_tree, custom_leaf);
    int landing_capacity = landing_buffer_capacity_for_tree(ct_tree);
    int high_pct = ct_tree->bg_landing_high_watermark_pct;
    if (high_pct < 1) {
        high_pct = 1;
    } else if (high_pct > 100) {
        high_pct = 100;
    }

    int compacted = 0;
    if (landing_count > 0 &&
        landing_capacity > 0 &&
        landing_count * 100 >= landing_capacity * high_pct) {
        int rc = compressed_leaf_flush_landing_locked(ct_tree, custom_leaf);
        if (rc > 0) {
            __atomic_add_fetch(&ct_tree->bg_compactions, 1, __ATOMIC_RELAXED);
            compacted = 1;
        } else if (rc < 0) {
            __atomic_add_fetch(&ct_tree->bg_errors, 1, __ATOMIC_RELAXED);
        }
    }

    pthread_rwlock_unlock(&custom_leaf->rwlock);
    pthread_rwlock_unlock(&ct_tree->rwlock);

    if (ct_tree->bg_max_compactions_per_sec > 0 && compacted) {
        usleep((useconds_t)(1000000 / ct_tree->bg_max_compactions_per_sec));
    }
    return compacted;
}

static void background_maybe_enqueue_key(struct bplus_tree_compressed *ct_tree,
                                         struct simple_leaf_node *leaf,
                                         key_t key)
{
    if (!ct_tree || !leaf || !ct_tree->bg_compaction_enabled ||
        !ct_tree->bg_dirty_keys || ct_tree->bg_queue_capacity <= 0) {
        return;
    }
    if (!background_codec_allowed(ct_tree, leaf)) {
        return;
    }

    int landing_count = compressed_leaf_landing_count(ct_tree, leaf);
    int landing_capacity = landing_buffer_capacity_for_tree(ct_tree);
    int high_pct = ct_tree->bg_landing_high_watermark_pct;
    if (landing_capacity <= 0 || landing_count <= 0) {
        return;
    }
    if (high_pct < 1) {
        high_pct = 1;
    } else if (high_pct > 100) {
        high_pct = 100;
    }
    if (landing_count * 100 < landing_capacity * high_pct) {
        return;
    }

    __atomic_add_fetch(&ct_tree->bg_enqueue_attempts, 1, __ATOMIC_RELAXED);

    pthread_mutex_lock(&ct_tree->bg_queue_lock);
    for (int i = 0; i < ct_tree->bg_queue_count; i++) {
        int idx = (ct_tree->bg_queue_head + i) % ct_tree->bg_queue_capacity;
        if (ct_tree->bg_dirty_keys[idx] == key) {
            __atomic_add_fetch(&ct_tree->bg_enqueue_duplicates, 1, __ATOMIC_RELAXED);
            pthread_mutex_unlock(&ct_tree->bg_queue_lock);
            return;
        }
    }

    if (ct_tree->bg_queue_count >= ct_tree->bg_queue_capacity) {
        __atomic_add_fetch(&ct_tree->bg_queue_full, 1, __ATOMIC_RELAXED);
        pthread_mutex_unlock(&ct_tree->bg_queue_lock);
        return;
    }

    ct_tree->bg_dirty_keys[ct_tree->bg_queue_tail] = key;
    ct_tree->bg_queue_tail = (ct_tree->bg_queue_tail + 1) % ct_tree->bg_queue_capacity;
    ct_tree->bg_queue_count++;
    __atomic_add_fetch(&ct_tree->bg_enqueued, 1, __ATOMIC_RELAXED);
    pthread_cond_signal(&ct_tree->bg_queue_cond);
    pthread_mutex_unlock(&ct_tree->bg_queue_lock);
}

static void *background_compaction_worker(void *arg)
{
    struct bplus_tree_compressed *ct_tree = (struct bplus_tree_compressed *)arg;
    while (!__atomic_load_n(&ct_tree->bg_shutdown, __ATOMIC_RELAXED)) {
        key_t key = 0;
        int did_work = 0;
        while (background_dirty_queue_pop(ct_tree, &key)) {
            did_work |= background_compact_key(ct_tree, key);
            if (__atomic_load_n(&ct_tree->bg_shutdown, __ATOMIC_RELAXED)) {
                break;
            }
        }
        if (!did_work) {
            background_compaction_scan_once(ct_tree);
        }
        int interval_us = ct_tree->bg_scan_interval_us;
        if (interval_us <= 0) {
            interval_us = 1000;
        }
        usleep((useconds_t)interval_us);
    }
    return NULL;
}

static void configure_background_compaction(struct bplus_tree_compressed *ct_tree)
{
    ct_tree->bg_compaction_enabled = env_bool_enabled("BTREE_BG_COMPACTION", 0);
    ct_tree->bg_thread_count = env_int_clamped("BTREE_BG_THREADS", 1, 1, 64);
    ct_tree->bg_scan_interval_us = env_int_clamped("BTREE_BG_SCAN_INTERVAL_US", 1000, 1, 10000000);
    ct_tree->bg_landing_high_watermark_pct =
        env_int_clamped("BTREE_BG_LANDING_HIGH_WATERMARK_PCT", 75, 1, 100);
    ct_tree->bg_max_leaves_per_pass =
        env_int_clamped("BTREE_BG_MAX_LEAVES_PER_PASS", 128, 1, INT_MAX);
    ct_tree->bg_max_compactions_per_sec =
        env_int_clamped("BTREE_BG_MAX_COMPACTIONS_PER_SEC", 0, 0, 1000000);
    ct_tree->bg_trylock_only = env_bool_enabled("BTREE_BG_TRYLOCK_ONLY", 1);
    ct_tree->bg_codec_filter = parse_background_codec_filter();
    if (ct_tree->bg_compaction_enabled &&
        ct_tree->bg_codec_filter >= 0 &&
        ct_tree->config.algo != (compression_algo_t)ct_tree->bg_codec_filter) {
        ct_tree->bg_compaction_enabled = 0;
    }
    ct_tree->bg_queue_capacity =
        env_int_clamped("BTREE_BG_QUEUE_CAPACITY", 4096, 0, 10000000);
}

static void start_background_compaction(struct bplus_tree_compressed *ct_tree)
{
    if (!ct_tree || !ct_tree->bg_compaction_enabled || ct_tree->bg_thread_count <= 0) {
        return;
    }

    if (ct_tree->bg_queue_capacity > 0) {
        ct_tree->bg_dirty_keys = calloc((size_t)ct_tree->bg_queue_capacity, sizeof(*ct_tree->bg_dirty_keys));
        if (!ct_tree->bg_dirty_keys) {
            ct_tree->bg_queue_capacity = 0;
        }
    }

    ct_tree->bg_threads = calloc((size_t)ct_tree->bg_thread_count, sizeof(*ct_tree->bg_threads));
    if (!ct_tree->bg_threads) {
        free(ct_tree->bg_dirty_keys);
        ct_tree->bg_dirty_keys = NULL;
        ct_tree->bg_compaction_enabled = 0;
        return;
    }

    __atomic_store_n(&ct_tree->bg_shutdown, 0, __ATOMIC_RELAXED);
    int started = 0;
    for (int i = 0; i < ct_tree->bg_thread_count; i++) {
        if (pthread_create(&ct_tree->bg_threads[i],
                           NULL,
                           background_compaction_worker,
                           ct_tree) != 0) {
            break;
        }
        started++;
    }

    if (started != ct_tree->bg_thread_count) {
        __atomic_store_n(&ct_tree->bg_shutdown, 1, __ATOMIC_RELAXED);
        for (int i = 0; i < started; i++) {
            pthread_join(ct_tree->bg_threads[i], NULL);
        }
        free(ct_tree->bg_threads);
        ct_tree->bg_threads = NULL;
        free(ct_tree->bg_dirty_keys);
        ct_tree->bg_dirty_keys = NULL;
        ct_tree->bg_thread_count = 0;
        ct_tree->bg_compaction_enabled = 0;
    }
}

static void stop_background_compaction(struct bplus_tree_compressed *ct_tree)
{
    if (!ct_tree || !ct_tree->bg_threads || ct_tree->bg_thread_count <= 0) {
        return;
    }

    __atomic_store_n(&ct_tree->bg_shutdown, 1, __ATOMIC_RELAXED);
    pthread_mutex_lock(&ct_tree->bg_queue_lock);
    pthread_cond_broadcast(&ct_tree->bg_queue_cond);
    pthread_mutex_unlock(&ct_tree->bg_queue_lock);
    for (int i = 0; i < ct_tree->bg_thread_count; i++) {
        pthread_join(ct_tree->bg_threads[i], NULL);
    }
    free(ct_tree->bg_threads);
    ct_tree->bg_threads = NULL;
    free(ct_tree->bg_dirty_keys);
    ct_tree->bg_dirty_keys = NULL;
    ct_tree->bg_thread_count = 0;
}

static int remove_leaf_from_parent(struct bplus_tree_compressed *ct_tree,
                                   struct compressed_leaf_ref *leaf)
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


/* Transfer owned representation only, never copy an initialized pthread lock. */
static void compressed_leaf_move_representation(struct simple_leaf_node *dst,
                                                 struct simple_leaf_node *src)
{
    leaf_index_release(dst);
    if (src->subpage_index == &src->inline_index) {
        dst->inline_index = src->inline_index;
        dst->subpage_index = &dst->inline_index;
    } else {
        dst->subpage_index = src->subpage_index;
    }
    src->subpage_index = NULL;
    memcpy(dst->active, src->active, sizeof(dst->active));
    dst->num_subpages = src->num_subpages;
    dst->num_subpage_entries = src->num_subpage_entries;
    dst->uncompressed_bytes = src->uncompressed_bytes;
    compressed_leaf_adopt_base(dst, src->compressed_data,
                               src->compressed_capacity, src->compressed_usable);
    src->compressed_data = NULL;
    dst->generation++;
}

static int split_leaf_impl(struct bplus_tree_compressed *ct_tree,
                      struct compressed_leaf_ref *leaf,
                      struct compressed_leaf_ref **new_leaf_out,
                      key_t *split_key_out)
{
    struct simple_leaf_node *original = (struct simple_leaf_node *)leaf->payload;
    if (!original) return -1;
    pthread_rwlock_wrlock(&original->rwlock);
    struct kv_pair *pairs = NULL;
    size_t count = 0;
    struct compressed_leaf_ref *right_ref = NULL;
    struct simple_leaf_node *left_image = NULL, *right_image = NULL;
    struct split_reservation reserve = {0};
    int rc = -1;
    const char *fault_env = getenv("BTREE_TEST_FAIL_SPLIT_STAGE");
    int fault = fault_env ? atoi(fault_env) : 0;
    if (compressed_leaf_collect_pairs(ct_tree, original, &pairs, &count) || count < 2)
        goto done;
    qsort(pairs, count, sizeof(*pairs), compare_kv_pairs);
    if (fault == 1 || split_reservation_prepare(ct_tree->tree, leaf, &reserve))
        goto done;
    right_ref = compressed_leaf_ref_new();
    left_image = calloc(1, sizeof(*left_image));
    right_image = calloc(1, sizeof(*right_image));
    if (!right_ref || !left_image || !right_image) goto done;
    /* These staging objects own bytes, but are not visible to readers/workers. */
    left_image->num_subpages = right_image->num_subpages = original->num_subpages;
    left_image->compression_algo = right_image->compression_algo = original->compression_algo;
    size_t middle = count / 2;
    if (fault == 2 ||
        compressed_leaf_rebuild_with_pairs(ct_tree, left_image, pairs, middle))
        goto done;
    if (fault == 3 ||
        compressed_leaf_rebuild_with_pairs(ct_tree, right_image, pairs + middle, count - middle))
        goto done;
    if (pthread_rwlock_init(&right_image->rwlock, NULL)) goto done;

    /* All fallible preparation is complete. The tree write lock prevents any
     * observer from seeing one half without the other or an unbuilt parent. */
    compressed_leaf_move_representation(original, left_image);
    original->pending = NULL;
    right_ref->payload = (value_t)right_image;
    right_ref->parent = leaf->parent;
    right_ref->parent_key_idx = leaf->parent_key_idx + 1;
    list_add(&right_ref->link, &leaf->link);
    key_t split_key = pairs[middle].key;
    bplus_tree_insert_internal(ct_tree->tree, split_key,
                               (struct bplus_node *)leaf,
                               (struct bplus_node *)right_ref, &reserve);
    if (new_leaf_out) *new_leaf_out = right_ref;
    if (split_key_out) *split_key_out = split_key;
    right_ref = NULL;
    right_image = NULL;
    rc = 0;
done:
    split_reservation_destroy(&reserve);
    if (left_image) {
        leaf_index_release(left_image);
        free(left_image->compressed_data);
        free(left_image);
    }
    if (right_image) {
        leaf_index_release(right_image);
        free(right_image->compressed_data);
        free(right_image);
    }
    free(right_ref);
    free(pairs);
    pthread_rwlock_unlock(&original->rwlock);
    return rc;
}


static int split_leaf(struct bplus_tree_compressed *tree, struct compressed_leaf_ref *leaf,
                      struct compressed_leaf_ref **out, key_t *key)
{
#ifdef ZIPCACHE_AGG_EXPERIMENT
    int previous=agg_origin; agg_origin=BPLUS_ORIGIN_SPLIT;
#endif
    int result=split_leaf_impl(tree,leaf,out,key);
#ifdef ZIPCACHE_AGG_EXPERIMENT
    agg_origin=previous;
#endif
    return result;
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
            background_maybe_enqueue_key(ct_tree, leaf, key);
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
        background_maybe_enqueue_key(ct_tree, leaf, key);
        if (trace) fprintf(stderr, "[insert] placed in landing\n");
        return 0;
    }

    __atomic_add_fetch(&ct_tree->fg_landing_full, 1, __ATOMIC_RELAXED);
    /* The default foreground rebuild performs all following codec work while
     * the caller holds the leaf (or tree) write lock. */
    submission_tls_write_critical = 1;

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
        __atomic_add_fetch(&ct_tree->fg_split_fallbacks, 1, __ATOMIC_RELAXED);
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
        if (compressed_leaf_replace_base(leaf, NULL, 0) != 0) {
            return -1;
        }
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
            __atomic_add_fetch(&ct_tree->fg_sync_compaction_errors, 1, __ATOMIC_RELAXED);
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
            __atomic_add_fetch(&ct_tree->fg_sync_compaction_errors, 1, __ATOMIC_RELAXED);
            return -1;
        }

        temp_index[b].offset = running_offset;
        temp_index[b].length = (uint32_t)compressed_size;
        running_offset += (size_t)compressed_size;
    }
    if (trace) fprintf(stderr, "[insert] compressed buckets bytes=%zu\n", running_offset);

    if (leaf->subpage_index == NULL) {
        leaf->subpage_index = leaf_index_allocate(leaf, leaf->num_subpages);
        if (!leaf->subpage_index) {
            free(temp_index);
            __atomic_add_fetch(&ct_tree->fg_sync_compaction_errors, 1, __ATOMIC_RELAXED);
            return -1;
        }
    }

    if (compressed_leaf_replace_base(leaf, temp_compressed, running_offset) != 0) {
        free(temp_index);
        __atomic_add_fetch(&ct_tree->fg_sync_compaction_errors, 1, __ATOMIC_RELAXED);
        return -1;
    }
    memset(leaf->landing_buffer, 0, LANDING_BUFFER_BYTES);
    memcpy(leaf->subpage_index, temp_index, leaf->num_subpages * sizeof(struct subpage_index_entry));

    leaf->uncompressed_bytes = hashed_pairs * sizeof(struct kv_pair);
    leaf->generation++;

    free(temp_index);
    __atomic_add_fetch(&ct_tree->fg_sync_compactions, 1, __ATOMIC_RELAXED);
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

/* Called with the leaf write lock held. */
static int compaction_insert_locked(struct bplus_tree_compressed *ct_tree,
                                    struct simple_leaf_node *leaf,
                                    key_t key,
                                    int stored_value,
                                    const uint8_t *payload,
                                    size_t payload_len,
                                    int *must_wait,
                                    int *used_sync_fallback)
{
    if (!ct_tree || !leaf || !must_wait || !used_sync_fallback) {
        return -1;
    }
    *must_wait = 0;
    *used_sync_fallback = 0;

    struct kv_pair *free_slot = NULL;
    for (int i = 0; i < ACTIVE_DELTA_ENTRIES; i++) {
        if (leaf->active[i].key == key) {
            kv_pair_set_value(&leaf->active[i], key, stored_value, payload, payload_len);
            leaf->generation++;
            return 0;
        }
        if (leaf->active[i].key == 0 && !free_slot) {
            free_slot = &leaf->active[i];
        }
    }

    if (free_slot) {
        kv_pair_set_value(free_slot, key, stored_value, payload, payload_len);
        leaf->generation++;
        return 0;
    }

    if (leaf->pending) {
        if (pending_task_state_load(leaf->pending) == PENDING_TASK_FAILED) {
            return -1;
        }
        *must_wait = 1;
        return 0;
    }

    int include_trigger = env_bool_enabled("BTREE_BG_INCLUDE_TRIGGER", 0);
    size_t task_bytes = sizeof(struct compressed_pending_task) +
        (size_t)(ACTIVE_DELTA_ENTRIES + include_trigger) * sizeof(struct kv_pair);
#ifdef ZIPCACHE_AGG_EXPERIMENT
    struct compaction_scheduler *admission = ct_tree->scheduler;
    if (admission->admission_control) {
        pthread_mutex_lock(&admission->lock);
        size_t slots = admission->pending_byte_limit / admission->pending_charge;
        size_t live = admission->submitted_tasks - admission->completed_tasks;
        if (!admission->accepting || admission->shutdown ||
            slots <= admission->failed_tasks) {
            pthread_mutex_unlock(&admission->lock);
            return -1; /* Failed pending remains charged and readable. */
        }
        if ((size_t)admission->queue_count + admission->admission_reserved >= (size_t)admission->queue_capacity ||
            live + admission->admission_reserved >= slots) {
            *must_wait = 2;
            pthread_mutex_unlock(&admission->lock);
            return 0;
        }
        admission->admission_reserved++;
        pthread_mutex_unlock(&admission->lock);
    }
#endif
    struct compressed_pending_task *task =
        env_bool_enabled("BTREE_TEST_FAIL_PENDING_ALLOC", 0)
            ? NULL : calloc(1, task_bytes);
    if (!task) {
#ifdef ZIPCACHE_AGG_EXPERIMENT
        if (admission->admission_control) {
            pthread_mutex_lock(&admission->lock);
            admission->admission_reserved--;
            pthread_cond_broadcast(&admission->state_changed);
            pthread_mutex_unlock(&admission->lock);
            return -1;
        }
#endif
        *used_sync_fallback = 1;
        return 1;
    }
    task->tree = ct_tree;
    task->leaf = leaf;
    task->base_version = leaf->base_version;
    task->token = ++leaf->pending_token;
    task->enqueued_ns = submission_now_ns();
    pending_task_state_store(task, PENDING_TASK_QUEUED);
    for (int i = 0; i < ACTIVE_DELTA_ENTRIES; i++) {
        if (leaf->active[i].key != 0) {
            task->records[task->count++] = leaf->active[i];
        }
    }

    struct kv_pair old_active[ACTIVE_DELTA_ENTRIES];
    memcpy(old_active, leaf->active, sizeof(old_active));
    memset(leaf->active, 0, sizeof(leaf->active));
    leaf->pending = task;
    if (include_trigger) {
        kv_pair_set_value(&task->records[task->count++], key, stored_value, payload, payload_len);
    } else {
        kv_pair_set_value(&leaf->active[0], key, stored_value, payload, payload_len);
    }

    if (compaction_scheduler_enqueue_locked(ct_tree, leaf, task) != 0) {
        memcpy(leaf->active, old_active, sizeof(old_active));
        leaf->pending = NULL;
        free(task);
#ifdef ZIPCACHE_AGG_EXPERIMENT
        if (admission->admission_control) return -1;
#endif
        *used_sync_fallback = 1;
        return 1;
    }

    leaf->generation++;
    return 0;
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

    if (leaf->subpage_index == NULL) {
        leaf->subpage_index = leaf_index_allocate(leaf, num_subpages);
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

    if (compressed_leaf_replace_base(leaf, temp_compressed, running_offset) != 0) {
        *old_uncompressed = snapshot_uncompressed;
        *old_compressed = snapshot_compressed;
        *result = -1;
        *handled = 1;
        free(compressed_copy);
        free(index_copy);
        free(temp_index);
        return 0;
    }
    memset(leaf->landing_buffer, 0, LANDING_BUFFER_BYTES);
    memcpy(leaf->subpage_index, temp_index, (size_t)num_subpages * sizeof(*temp_index));

    leaf->num_subpages = num_subpages;
    leaf->num_subpage_entries = num_subpages;
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
                              struct compressed_leaf_ref *leaf,
                              struct simple_leaf_node **out_leaf)
{
    struct simple_leaf_node *custom_leaf = NULL;

    if (leaf == NULL) {
        return -1;
    }

    if (leaf->entries == 0 && leaf->payload != 0) {
        custom_leaf = (struct simple_leaf_node *)leaf->payload;
        if (out_leaf) {
            *out_leaf = custom_leaf;
        }
        return 0;
    }

    custom_leaf = calloc(1, sizeof(*custom_leaf));
    if (!custom_leaf) {
        return -1;
    }

    custom_leaf->num_subpages = ct_tree->config.default_sub_pages;
    custom_leaf->compression_algo = ct_tree->config.algo;
    pthread_rwlock_init(&custom_leaf->rwlock, NULL);

    leaf->entries = 0;
    leaf->payload = (value_t)custom_leaf;

    if (out_leaf) {
        *out_leaf = custom_leaf;
    }

    return 0;
}

static struct compressed_leaf_ref* find_leaf_for_key(struct bplus_tree *tree, key_t key)
{
    struct bplus_node *node = tree->root;
    
    while (node != NULL) {
        if (node->type == BPLUS_TREE_LEAF) {
            return (struct compressed_leaf_ref*)node;
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
    (void)insert;
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
                                        int level, struct split_reservation *reserve)
{
    if (left->parent == NULL && right->parent == NULL) {
        struct bplus_non_leaf *parent = split_reservation_take(reserve);
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
        return compressed_non_leaf_insert(tree, left->parent, left, right, key, level + 1, reserve);
    } else {
        left->parent = right->parent;
        return compressed_non_leaf_insert(tree, right->parent, left, right, key, level + 1, reserve);
    }
}

static int compressed_non_leaf_insert(struct bplus_tree *tree,
                                      struct bplus_non_leaf *node,
                                      struct bplus_node *l_ch,
                                      struct bplus_node *r_ch,
                                      key_t key,
                                      int level, struct split_reservation *reserve)
{
    int insert = compressed_key_binary_search(node->key, node->children - 1, key);
    assert(insert < 0);
    insert = -insert - 1;

    if (node->children == tree->order) {
        key_t split_key;
        int split = node->children / 2;
        struct bplus_non_leaf *sibling = split_reservation_take(reserve);

        if (insert < split) {
            split_key = compressed_non_leaf_split_left(node, sibling, l_ch, r_ch, key, insert, split);
        } else if (insert == split) {
            split_key = compressed_non_leaf_split_right1(node, sibling, l_ch, r_ch, key, insert, split);
        } else {
            split_key = compressed_non_leaf_split_right2(node, sibling, l_ch, r_ch, key, insert, split);
        }

        if (insert < split) {
            return compressed_parent_node_build(tree, (struct bplus_node *)sibling,
                                               (struct bplus_node *)node, split_key, level, reserve);
        } else {
            return compressed_parent_node_build(tree, (struct bplus_node *)node,
                                               (struct bplus_node *)sibling, split_key, level, reserve);
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
                                      struct bplus_node *right, struct split_reservation *reserve)
{
    return compressed_parent_node_build(tree, left, right, key, 0, reserve);
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
#ifdef ZIPCACHE_AGG_EXPERIMENT
    /* This is the top-level foreground operation. Structural helpers override
     * its origin for their duration; ordinary seals/rebuilds are compaction. */
    agg_origin = BPLUS_ORIGIN_COMPACTION;
#endif
    if (ct_tree == NULL || !ct_tree->initialized || ct_tree->tree == NULL) {
        return -1;
    }

    submission_profile_begin_logical(ct_tree, SUBMISSION_LOGICAL_WRITE);

retry_lookup:
    // --- Fast path: tree rdlock → find leaf → leaf wrlock → release tree ---
    pthread_rwlock_rdlock(&ct_tree->rwlock);

    struct compressed_leaf_ref *leaf = find_leaf_for_key(ct_tree->tree, key);
    if (leaf == NULL) {
        // Tree is empty — need wrlock to create first leaf
        pthread_rwlock_unlock(&ct_tree->rwlock);
        pthread_rwlock_wrlock(&ct_tree->rwlock);
        leaf = find_leaf_for_key(ct_tree->tree, key);
        if (leaf == NULL) {
            leaf = compressed_leaf_ref_new();
            if (!leaf) {
                pthread_rwlock_unlock(&ct_tree->rwlock);
                return -1;
            }
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
    if (leaf->entries > 0 || leaf->payload == 0) {
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
        if (!leaf || leaf->payload == 0) {
            pthread_rwlock_unlock(&ct_tree->rwlock);
            return -1;
        }
        custom_leaf = (struct simple_leaf_node *)leaf->payload;
    } else {
        custom_leaf = (struct simple_leaf_node *)leaf->payload;
    }

    // Lock this specific leaf for writing, then release tree lock
    pthread_rwlock_wrlock(&custom_leaf->rwlock);
    pthread_rwlock_unlock(&ct_tree->rwlock);

    size_t old_uncompressed = custom_leaf->uncompressed_bytes;
    size_t old_compressed = custom_leaf->compressed_bytes;

    int result = 0;
    int handled_out_of_lock = 0;
    if (ct_tree->scheduler) {
        int must_wait = 0;
        int used_sync_fallback = 0;
        int async_result = compaction_insert_locked(ct_tree,
                                                    custom_leaf,
                                                    key,
                                                    data,
                                                    payload,
                                                    payload_len,
                                                    &must_wait,
                                                    &used_sync_fallback);
        if (must_wait) {
            struct compaction_scheduler *scheduler = ct_tree->scheduler;
            pthread_rwlock_unlock(&custom_leaf->rwlock);
#ifdef ZIPCACHE_AGG_EXPERIMENT
            uint64_t admission_elapsed =
#endif
                compaction_scheduler_wait_for_change(scheduler);
#ifdef ZIPCACHE_AGG_EXPERIMENT
            if (must_wait == 2) {
                AGG_INC(admission_waits, 1);
                AGG_INC(admission_wait_ns, admission_elapsed);
            }
#endif
            goto retry_lookup;
        }
        if (async_result < 0) {
            pthread_rwlock_unlock(&custom_leaf->rwlock);
            return -1;
        }
        if (used_sync_fallback) {
            pthread_mutex_lock(&ct_tree->scheduler->lock);
            ct_tree->scheduler->synchronous_fallbacks++;
            pthread_mutex_unlock(&ct_tree->scheduler->lock);
            result = insert_into_leaf(ct_tree, custom_leaf, key, data, payload, payload_len);
        } else {
            result = 0;
        }
        handled_out_of_lock = 1;
    } else {
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
        if (!leaf || leaf->payload == 0) {
            pthread_rwlock_unlock(&ct_tree->rwlock);
            return -1;
        }
        custom_leaf = (struct simple_leaf_node *)leaf->payload;

        if (ct_tree->debug_mode) {
            fprintf(stderr, "=== EXECUTING SPLIT for key %d ===\n", key);
            fflush(stderr);
        }
        struct compressed_leaf_ref *new_leaf = NULL;
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

        // Retry insert under tree wrlock (we already have it, just do it)
        if (key < split_key) {
            result = insert_into_leaf(ct_tree, custom_leaf, key, data, payload, payload_len);
        } else {
            struct simple_leaf_node *new_custom_leaf = (struct simple_leaf_node*)new_leaf->payload;
            result = insert_into_leaf(ct_tree, new_custom_leaf, key, data, payload, payload_len);
        }
        pthread_rwlock_unlock(&ct_tree->rwlock);
    } else {
        pthread_rwlock_unlock(&custom_leaf->rwlock);
    }

    return result;
}


#include "bplustree_compressed_read.inc"

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

    pthread_rwlock_rdlock(&ct_tree->rwlock);
    if (ct_tree->tree->root == NULL) {
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return -1;
    }

    struct compressed_leaf_ref *leaf = find_leaf_for_key(ct_tree->tree, min_key);
    if (!leaf) {
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return -1;
    }

    int last_value = -1;
    bool found = false;
    bool stop = false;

    while (leaf && !stop) {
        struct simple_leaf_node *custom_leaf = (struct simple_leaf_node *)leaf->payload;
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

        if (rc == 0 && custom_leaf->pending) {
            for (size_t i = 0; i < custom_leaf->pending->count; i++) {
                struct kv_pair *entry = &custom_leaf->pending->records[i];
                if (entry->key != 0 &&
                    kv_vector_put(&leaf_pairs,
                                  &leaf_count,
                                  &leaf_capacity,
                                  entry->key,
                                  entry->stored_value,
                                  entry->payload,
                                  COMPRESSED_VALUE_BYTES) != 0) {
                    rc = -1;
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

static int bplus_tree_compressed_delete_impl(struct bplus_tree_compressed *ct_tree, key_t key)
{
    if (compressed_tree_is_sharded(ct_tree)) {
        return bplus_tree_compressed_delete(compressed_tree_shard_for_key(ct_tree, key), key);
    }

    if (ct_tree == NULL || !ct_tree->initialized || ct_tree->tree == NULL) {
        return -1;
    }

retry_delete:
    // Fast path: missing keys and non-min-key deletes do not need tree structure updates.
    pthread_rwlock_rdlock(&ct_tree->rwlock);
    struct compressed_leaf_ref *fast_leaf = find_leaf_for_key(ct_tree->tree, key);
    if (!fast_leaf || fast_leaf->payload == 0) {
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return -1;
    }

    struct simple_leaf_node *fast_custom_leaf = (struct simple_leaf_node *)fast_leaf->payload;
    pthread_rwlock_wrlock(&fast_custom_leaf->rwlock);
    pthread_rwlock_unlock(&ct_tree->rwlock);

    if (fast_custom_leaf->pending &&
        pending_task_state_load(fast_custom_leaf->pending) != PENDING_TASK_FAILED) {
        struct compaction_scheduler *scheduler = ct_tree->scheduler;
        pthread_rwlock_unlock(&fast_custom_leaf->rwlock);
        compaction_scheduler_wait_for_change(scheduler);
        goto retry_delete;
    }

    struct kv_pair *fast_pairs = NULL;
    size_t fast_count = 0;
    if (compressed_leaf_collect_pairs(ct_tree, fast_custom_leaf, &fast_pairs, &fast_count) == 0 && fast_count > 0) {
        if (fast_custom_leaf->pending &&
            pending_task_state_load(fast_custom_leaf->pending) == PENDING_TASK_FAILED) {
            free(fast_custom_leaf->pending);
            fast_custom_leaf->pending = NULL;
        }
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

    struct compressed_leaf_ref *leaf = find_leaf_for_key(ct_tree->tree, key);
    if (!leaf || leaf->payload == 0) {
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return -1;
    }

    struct simple_leaf_node *custom_leaf = (struct simple_leaf_node *)leaf->payload;
    pthread_rwlock_wrlock(&custom_leaf->rwlock);

    if (custom_leaf->pending &&
        pending_task_state_load(custom_leaf->pending) != PENDING_TASK_FAILED) {
        struct compaction_scheduler *scheduler = ct_tree->scheduler;
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        pthread_rwlock_unlock(&ct_tree->rwlock);
        compaction_scheduler_wait_for_change(scheduler);
        goto retry_delete;
    }

    struct kv_pair *pairs = NULL;
    size_t count = 0;
    if (compressed_leaf_collect_pairs(ct_tree, custom_leaf, &pairs, &count) != 0) {
        pthread_rwlock_unlock(&custom_leaf->rwlock);
        pthread_rwlock_unlock(&ct_tree->rwlock);
        return -1;
    }

    if (custom_leaf->pending &&
        pending_task_state_load(custom_leaf->pending) == PENDING_TASK_FAILED) {
        free(custom_leaf->pending);
        custom_leaf->pending = NULL;
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

    /*
     * If this delete empties the leaf, the rebalance path may rebuild a
     * neighbour.  Drain that neighbour before changing any resident state;
     * otherwise its worker could later commit against a base_version that the
     * borrow operation replaced.
     */
    if (count == 1) {
        struct compressed_leaf_ref *left_candidate =
            list_is_first(&leaf->link, &ct_tree->tree->list[0])
                ? NULL : list_prev_entry(leaf, link);
        struct compressed_leaf_ref *right_candidate =
            list_is_last(&leaf->link, &ct_tree->tree->list[0])
                ? NULL : list_next_entry(leaf, link);
        struct simple_leaf_node *pending_neighbour = NULL;

        if (left_candidate && left_candidate->payload != 0) {
            struct simple_leaf_node *candidate =
                (struct simple_leaf_node *)left_candidate->payload;
            pthread_rwlock_rdlock(&candidate->rwlock);
            if (candidate->pending &&
                pending_task_state_load(candidate->pending) != PENDING_TASK_FAILED) {
                pending_neighbour = candidate;
            }
            pthread_rwlock_unlock(&candidate->rwlock);
        }
        if (!pending_neighbour && right_candidate && right_candidate->payload != 0) {
            struct simple_leaf_node *candidate =
                (struct simple_leaf_node *)right_candidate->payload;
            pthread_rwlock_rdlock(&candidate->rwlock);
            if (candidate->pending &&
                pending_task_state_load(candidate->pending) != PENDING_TASK_FAILED) {
                pending_neighbour = candidate;
            }
            pthread_rwlock_unlock(&candidate->rwlock);
        }

        if (pending_neighbour) {
            struct compaction_scheduler *scheduler = ct_tree->scheduler;
            free(pairs);
            pthread_rwlock_unlock(&custom_leaf->rwlock);
            pthread_rwlock_unlock(&ct_tree->rwlock);
            compaction_scheduler_wait_for_change(scheduler);
            goto retry_delete;
        }
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
        struct compressed_leaf_ref *left_leaf = list_is_first(&leaf->link, &ct_tree->tree->list[0])
                                      ? NULL : list_prev_entry(leaf, link);
        struct compressed_leaf_ref *right_leaf = list_is_last(&leaf->link, &ct_tree->tree->list[0])
                                       ? NULL : list_next_entry(leaf, link);

        if (left_leaf && left_leaf->payload != 0 && !rebalanced) {
            struct simple_leaf_node *left_custom = (struct simple_leaf_node *)left_leaf->payload;
            pthread_rwlock_wrlock(&left_custom->rwlock);
            if (left_custom->pending &&
                pending_task_state_load(left_custom->pending) == PENDING_TASK_FAILED) {
                free(left_custom->pending);
                left_custom->pending = NULL;
            }
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

        if (!rebalanced && right_leaf && right_leaf->payload != 0) {
            struct simple_leaf_node *right_custom = (struct simple_leaf_node *)right_leaf->payload;
            pthread_rwlock_wrlock(&right_custom->rwlock);
            if (right_custom->pending &&
                pending_task_state_load(right_custom->pending) == PENDING_TASK_FAILED) {
                free(right_custom->pending);
                right_custom->pending = NULL;
            }
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
        leaf->payload = 0;
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

int bplus_tree_compressed_delete(struct bplus_tree_compressed *tree, key_t key)
{
#ifdef ZIPCACHE_AGG_EXPERIMENT
    int previous = agg_origin;
    agg_origin = BPLUS_ORIGIN_DELETE;
#endif
    int result = bplus_tree_compressed_delete_impl(tree, key);
#ifdef ZIPCACHE_AGG_EXPERIMENT
    agg_origin = previous;
#endif
    return result;
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

int bplus_tree_compressed_bg_stats(struct bplus_tree_compressed *ct_tree,
                                   uint64_t *passes,
                                   uint64_t *compactions,
                                   uint64_t *trylock_misses,
                                   uint64_t *skipped,
                                   uint64_t *errors)
{
    if (compressed_tree_is_sharded(ct_tree)) {
        uint64_t total_passes = 0;
        uint64_t total_compactions = 0;
        uint64_t total_trylock_misses = 0;
        uint64_t total_skipped = 0;
        uint64_t total_errors = 0;
        for (int i = 0; i < ct_tree->shard_count; i++) {
            uint64_t shard_passes = 0;
            uint64_t shard_compactions = 0;
            uint64_t shard_trylock_misses = 0;
            uint64_t shard_skipped = 0;
            uint64_t shard_errors = 0;
            if (bplus_tree_compressed_bg_stats(ct_tree->shards[i],
                                               &shard_passes,
                                               &shard_compactions,
                                               &shard_trylock_misses,
                                               &shard_skipped,
                                               &shard_errors) != 0) {
                return -1;
            }
            total_passes += shard_passes;
            total_compactions += shard_compactions;
            total_trylock_misses += shard_trylock_misses;
            total_skipped += shard_skipped;
            total_errors += shard_errors;
        }
        if (passes) {
            *passes = total_passes;
        }
        if (compactions) {
            *compactions = total_compactions;
        }
        if (trylock_misses) {
            *trylock_misses = total_trylock_misses;
        }
        if (skipped) {
            *skipped = total_skipped;
        }
        if (errors) {
            *errors = total_errors;
        }
        return 0;
    }

    if (ct_tree == NULL || !ct_tree->initialized) {
        return -1;
    }
    if (passes) {
        *passes = __atomic_load_n(&ct_tree->bg_passes, __ATOMIC_RELAXED);
    }
    if (compactions) {
        *compactions = __atomic_load_n(&ct_tree->bg_compactions, __ATOMIC_RELAXED);
    }
    if (trylock_misses) {
        *trylock_misses = __atomic_load_n(&ct_tree->bg_trylock_misses, __ATOMIC_RELAXED);
    }
    if (skipped) {
        *skipped = __atomic_load_n(&ct_tree->bg_skipped, __ATOMIC_RELAXED);
    }
    if (errors) {
        *errors = __atomic_load_n(&ct_tree->bg_errors, __ATOMIC_RELAXED);
    }
    return 0;
}

int bplus_tree_compressed_compaction_stats(struct bplus_tree_compressed *ct_tree,
                                           uint64_t *fg_landing_full,
                                           uint64_t *fg_sync_compactions,
                                           uint64_t *fg_sync_compaction_errors,
                                           uint64_t *fg_split_fallbacks,
                                           uint64_t *bg_enqueue_attempts,
                                           uint64_t *bg_enqueued,
                                           uint64_t *bg_enqueue_duplicates,
                                           uint64_t *bg_queue_full,
                                           uint64_t *bg_queue_pops)
{
    if (compressed_tree_is_sharded(ct_tree)) {
        uint64_t total_fg_landing_full = 0;
        uint64_t total_fg_sync_compactions = 0;
        uint64_t total_fg_sync_compaction_errors = 0;
        uint64_t total_fg_split_fallbacks = 0;
        uint64_t total_bg_enqueue_attempts = 0;
        uint64_t total_bg_enqueued = 0;
        uint64_t total_bg_enqueue_duplicates = 0;
        uint64_t total_bg_queue_full = 0;
        uint64_t total_bg_queue_pops = 0;

        for (int i = 0; i < ct_tree->shard_count; i++) {
            uint64_t shard_fg_landing_full = 0;
            uint64_t shard_fg_sync_compactions = 0;
            uint64_t shard_fg_sync_compaction_errors = 0;
            uint64_t shard_fg_split_fallbacks = 0;
            uint64_t shard_bg_enqueue_attempts = 0;
            uint64_t shard_bg_enqueued = 0;
            uint64_t shard_bg_enqueue_duplicates = 0;
            uint64_t shard_bg_queue_full = 0;
            uint64_t shard_bg_queue_pops = 0;

            if (bplus_tree_compressed_compaction_stats(ct_tree->shards[i],
                                                       &shard_fg_landing_full,
                                                       &shard_fg_sync_compactions,
                                                       &shard_fg_sync_compaction_errors,
                                                       &shard_fg_split_fallbacks,
                                                       &shard_bg_enqueue_attempts,
                                                       &shard_bg_enqueued,
                                                       &shard_bg_enqueue_duplicates,
                                                       &shard_bg_queue_full,
                                                       &shard_bg_queue_pops) != 0) {
                return -1;
            }

            total_fg_landing_full += shard_fg_landing_full;
            total_fg_sync_compactions += shard_fg_sync_compactions;
            total_fg_sync_compaction_errors += shard_fg_sync_compaction_errors;
            total_fg_split_fallbacks += shard_fg_split_fallbacks;
            total_bg_enqueue_attempts += shard_bg_enqueue_attempts;
            total_bg_enqueued += shard_bg_enqueued;
            total_bg_enqueue_duplicates += shard_bg_enqueue_duplicates;
            total_bg_queue_full += shard_bg_queue_full;
            total_bg_queue_pops += shard_bg_queue_pops;
        }

        if (fg_landing_full) {
            *fg_landing_full = total_fg_landing_full;
        }
        if (fg_sync_compactions) {
            *fg_sync_compactions = total_fg_sync_compactions;
        }
        if (fg_sync_compaction_errors) {
            *fg_sync_compaction_errors = total_fg_sync_compaction_errors;
        }
        if (fg_split_fallbacks) {
            *fg_split_fallbacks = total_fg_split_fallbacks;
        }
        if (bg_enqueue_attempts) {
            *bg_enqueue_attempts = total_bg_enqueue_attempts;
        }
        if (bg_enqueued) {
            *bg_enqueued = total_bg_enqueued;
        }
        if (bg_enqueue_duplicates) {
            *bg_enqueue_duplicates = total_bg_enqueue_duplicates;
        }
        if (bg_queue_full) {
            *bg_queue_full = total_bg_queue_full;
        }
        if (bg_queue_pops) {
            *bg_queue_pops = total_bg_queue_pops;
        }
        return 0;
    }

    if (ct_tree == NULL || !ct_tree->initialized) {
        return -1;
    }
    if (fg_landing_full) {
        *fg_landing_full = __atomic_load_n(&ct_tree->fg_landing_full, __ATOMIC_RELAXED);
    }
    if (fg_sync_compactions) {
        *fg_sync_compactions = __atomic_load_n(&ct_tree->fg_sync_compactions, __ATOMIC_RELAXED);
    }
    if (fg_sync_compaction_errors) {
        *fg_sync_compaction_errors = __atomic_load_n(&ct_tree->fg_sync_compaction_errors, __ATOMIC_RELAXED);
    }
    if (fg_split_fallbacks) {
        *fg_split_fallbacks = __atomic_load_n(&ct_tree->fg_split_fallbacks, __ATOMIC_RELAXED);
    }
    if (bg_enqueue_attempts) {
        *bg_enqueue_attempts = __atomic_load_n(&ct_tree->bg_enqueue_attempts, __ATOMIC_RELAXED);
    }
    if (bg_enqueued) {
        *bg_enqueued = __atomic_load_n(&ct_tree->bg_enqueued, __ATOMIC_RELAXED);
    }
    if (bg_enqueue_duplicates) {
        *bg_enqueue_duplicates = __atomic_load_n(&ct_tree->bg_enqueue_duplicates, __ATOMIC_RELAXED);
    }
    if (bg_queue_full) {
        *bg_queue_full = __atomic_load_n(&ct_tree->bg_queue_full, __ATOMIC_RELAXED);
    }
    if (bg_queue_pops) {
        *bg_queue_pops = __atomic_load_n(&ct_tree->bg_queue_pops, __ATOMIC_RELAXED);
    }
    return 0;
}

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
                                      uint64_t *zlib_errors)
{
    if (compressed_tree_is_sharded(ct_tree)) {
        uint64_t total_qpl_compress_calls = 0;
        uint64_t total_qpl_decompress_calls = 0;
        uint64_t total_qpl_tls_jobs = 0;
        uint64_t total_qpl_pool_jobs = 0;
        uint64_t total_qpl_errors = 0;
        uint64_t total_zlib_compress_calls = 0;
        uint64_t total_zlib_decompress_calls = 0;
        uint64_t total_zlib_stream_reuses = 0;
        uint64_t total_zlib_stream_inits = 0;
        uint64_t total_zlib_errors = 0;

        for (int i = 0; i < ct_tree->shard_count; i++) {
            uint64_t shard_qpl_compress_calls = 0;
            uint64_t shard_qpl_decompress_calls = 0;
            uint64_t shard_qpl_tls_jobs = 0;
            uint64_t shard_qpl_pool_jobs = 0;
            uint64_t shard_qpl_errors = 0;
            uint64_t shard_zlib_compress_calls = 0;
            uint64_t shard_zlib_decompress_calls = 0;
            uint64_t shard_zlib_stream_reuses = 0;
            uint64_t shard_zlib_stream_inits = 0;
            uint64_t shard_zlib_errors = 0;

            if (bplus_tree_compressed_codec_stats(ct_tree->shards[i],
                                                  &shard_qpl_compress_calls,
                                                  &shard_qpl_decompress_calls,
                                                  &shard_qpl_tls_jobs,
                                                  &shard_qpl_pool_jobs,
                                                  &shard_qpl_errors,
                                                  &shard_zlib_compress_calls,
                                                  &shard_zlib_decompress_calls,
                                                  &shard_zlib_stream_reuses,
                                                  &shard_zlib_stream_inits,
                                                  &shard_zlib_errors) != 0) {
                return -1;
            }

            total_qpl_compress_calls += shard_qpl_compress_calls;
            total_qpl_decompress_calls += shard_qpl_decompress_calls;
            total_qpl_tls_jobs += shard_qpl_tls_jobs;
            total_qpl_pool_jobs += shard_qpl_pool_jobs;
            total_qpl_errors += shard_qpl_errors;
            total_zlib_compress_calls += shard_zlib_compress_calls;
            total_zlib_decompress_calls += shard_zlib_decompress_calls;
            total_zlib_stream_reuses += shard_zlib_stream_reuses;
            total_zlib_stream_inits += shard_zlib_stream_inits;
            total_zlib_errors += shard_zlib_errors;
        }

        if (qpl_compress_calls) {
            *qpl_compress_calls = total_qpl_compress_calls;
        }
        if (qpl_decompress_calls) {
            *qpl_decompress_calls = total_qpl_decompress_calls;
        }
        if (qpl_tls_jobs) {
            *qpl_tls_jobs = total_qpl_tls_jobs;
        }
        if (qpl_pool_jobs) {
            *qpl_pool_jobs = total_qpl_pool_jobs;
        }
        if (qpl_errors) {
            *qpl_errors = total_qpl_errors;
        }
        if (zlib_compress_calls) {
            *zlib_compress_calls = total_zlib_compress_calls;
        }
        if (zlib_decompress_calls) {
            *zlib_decompress_calls = total_zlib_decompress_calls;
        }
        if (zlib_stream_reuses) {
            *zlib_stream_reuses = total_zlib_stream_reuses;
        }
        if (zlib_stream_inits) {
            *zlib_stream_inits = total_zlib_stream_inits;
        }
        if (zlib_errors) {
            *zlib_errors = total_zlib_errors;
        }
        return 0;
    }

    if (ct_tree == NULL || !ct_tree->initialized) {
        return -1;
    }

    if (qpl_compress_calls) {
        *qpl_compress_calls = __atomic_load_n(&ct_tree->qpl_compress_calls, __ATOMIC_RELAXED);
    }
    if (qpl_decompress_calls) {
        *qpl_decompress_calls = __atomic_load_n(&ct_tree->qpl_decompress_calls, __ATOMIC_RELAXED);
    }
    if (qpl_tls_jobs) {
        *qpl_tls_jobs = __atomic_load_n(&ct_tree->qpl_tls_jobs, __ATOMIC_RELAXED);
    }
    if (qpl_pool_jobs) {
        *qpl_pool_jobs = __atomic_load_n(&ct_tree->qpl_pool_jobs, __ATOMIC_RELAXED);
    }
    if (qpl_errors) {
        *qpl_errors = __atomic_load_n(&ct_tree->qpl_errors, __ATOMIC_RELAXED);
    }
    if (zlib_compress_calls) {
        *zlib_compress_calls = __atomic_load_n(&ct_tree->zlib_compress_calls, __ATOMIC_RELAXED);
    }
    if (zlib_decompress_calls) {
        *zlib_decompress_calls = __atomic_load_n(&ct_tree->zlib_decompress_calls, __ATOMIC_RELAXED);
    }
    if (zlib_stream_reuses) {
        *zlib_stream_reuses = __atomic_load_n(&ct_tree->zlib_stream_reuses, __ATOMIC_RELAXED);
    }
    if (zlib_stream_inits) {
        *zlib_stream_inits = __atomic_load_n(&ct_tree->zlib_stream_inits, __ATOMIC_RELAXED);
    }
    if (zlib_errors) {
        *zlib_errors = __atomic_load_n(&ct_tree->zlib_errors, __ATOMIC_RELAXED);
    }
    return 0;
}

static void submission_reset_tree_counters(struct bplus_tree_compressed *ct_tree)
{
    if (!ct_tree) {
        return;
    }
    if (compressed_tree_is_sharded(ct_tree)) {
        for (int i = 0; i < ct_tree->shard_count; i++) {
            submission_reset_tree_counters(ct_tree->shards[i]);
        }
        return;
    }

    __atomic_store_n(&ct_tree->submission_logical_reads, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&ct_tree->submission_logical_writes, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&ct_tree->submission_compress_calls, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&ct_tree->submission_decompress_calls, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&ct_tree->submission_compress_input_bytes, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&ct_tree->submission_decompress_input_bytes, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&ct_tree->submission_calls_under_write_lock, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&ct_tree->submission_input_bytes_under_write_lock, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&ct_tree->submission_current_inflight, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&ct_tree->submission_peak_inflight, 0, __ATOMIC_RELAXED);
}

void bplus_tree_compressed_submission_reset(struct bplus_tree_compressed *ct_tree)
{
    submission_reset_tree_counters(ct_tree);
    submission_trace_reset();
}

int bplus_tree_compressed_submission_stats(
    struct bplus_tree_compressed *ct_tree,
    struct bplus_tree_submission_stats *stats)
{
    if (!ct_tree || !stats || !ct_tree->initialized) {
        return -1;
    }
    memset(stats, 0, sizeof(*stats));

    if (compressed_tree_is_sharded(ct_tree)) {
        for (int i = 0; i < ct_tree->shard_count; i++) {
            struct bplus_tree_submission_stats shard_stats;
            if (bplus_tree_compressed_submission_stats(ct_tree->shards[i], &shard_stats) != 0) {
                return -1;
            }
            stats->logical_reads += shard_stats.logical_reads;
            stats->logical_writes += shard_stats.logical_writes;
            stats->compress_calls += shard_stats.compress_calls;
            stats->decompress_calls += shard_stats.decompress_calls;
            stats->compress_input_bytes += shard_stats.compress_input_bytes;
            stats->decompress_input_bytes += shard_stats.decompress_input_bytes;
            stats->calls_under_write_lock += shard_stats.calls_under_write_lock;
            stats->input_bytes_under_write_lock += shard_stats.input_bytes_under_write_lock;
            stats->current_inflight += shard_stats.current_inflight;
            if (shard_stats.peak_inflight > stats->peak_inflight) {
                stats->peak_inflight = shard_stats.peak_inflight;
            }
        }
        return 0;
    }

    stats->logical_reads = __atomic_load_n(&ct_tree->submission_logical_reads, __ATOMIC_RELAXED);
    stats->logical_writes = __atomic_load_n(&ct_tree->submission_logical_writes, __ATOMIC_RELAXED);
    stats->compress_calls = __atomic_load_n(&ct_tree->submission_compress_calls, __ATOMIC_RELAXED);
    stats->decompress_calls = __atomic_load_n(&ct_tree->submission_decompress_calls, __ATOMIC_RELAXED);
    stats->compress_input_bytes =
        __atomic_load_n(&ct_tree->submission_compress_input_bytes, __ATOMIC_RELAXED);
    stats->decompress_input_bytes =
        __atomic_load_n(&ct_tree->submission_decompress_input_bytes, __ATOMIC_RELAXED);
    stats->calls_under_write_lock =
        __atomic_load_n(&ct_tree->submission_calls_under_write_lock, __ATOMIC_RELAXED);
    stats->input_bytes_under_write_lock =
        __atomic_load_n(&ct_tree->submission_input_bytes_under_write_lock, __ATOMIC_RELAXED);
    stats->current_inflight =
        __atomic_load_n(&ct_tree->submission_current_inflight, __ATOMIC_RELAXED);
    stats->peak_inflight =
        __atomic_load_n(&ct_tree->submission_peak_inflight, __ATOMIC_RELAXED);
    return 0;
}

static int compaction_scheduler_enqueue_locked(
    struct bplus_tree_compressed *tree,
    struct simple_leaf_node *leaf,
    struct compressed_pending_task *task)
{
    (void)leaf;
    struct compaction_scheduler *scheduler = tree ? tree->scheduler : NULL;
    if (!scheduler || !task) {
        return -1;
    }

    pthread_mutex_lock(&scheduler->lock);
#ifdef ZIPCACHE_AGG_EXPERIMENT
    if (scheduler->admission_control) {
        assert(scheduler->admission_reserved > 0);
        scheduler->admission_reserved--;
    }
#endif
    if (!scheduler->accepting || scheduler->shutdown ||
        scheduler->queue_count >= scheduler->queue_capacity) {
        scheduler->queue_full_fallbacks++;
        pthread_mutex_unlock(&scheduler->lock);
        return -1;
    }

    scheduler->queue[scheduler->queue_tail] = task;
    scheduler->queue_tail = (scheduler->queue_tail + 1) % scheduler->queue_capacity;
    scheduler->queue_count++;
    scheduler->submitted_tasks++;
    if ((uint64_t)scheduler->queue_count > scheduler->queue_peak) {
        scheduler->queue_peak = (uint64_t)scheduler->queue_count;
    }
    pthread_cond_signal(&scheduler->work_available);
    pthread_mutex_unlock(&scheduler->lock);
    return 0;
}

static uint64_t compaction_scheduler_wait_for_change(struct compaction_scheduler *scheduler)
{
#ifdef ZIPCACHE_AGG_EXPERIMENT
    uint64_t wait_start = submission_now_ns();
#endif
    if (!scheduler) {
        return 0;
    }
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_nsec += 10 * 1000 * 1000;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }
    pthread_mutex_lock(&scheduler->lock);
    (void)pthread_cond_timedwait(&scheduler->state_changed, &scheduler->lock, &deadline);
#ifdef ZIPCACHE_AGG_EXPERIMENT
    uint64_t elapsed = submission_now_ns()-wait_start;
    AGG_INC(pending_wait_ns, elapsed);
#endif
    pthread_mutex_unlock(&scheduler->lock);
#ifdef ZIPCACHE_AGG_EXPERIMENT
    return elapsed;
#else
    return 0;
#endif
}

static void compaction_scheduler_note_completion(struct compaction_scheduler *scheduler,
                                                 int success,
                                                 int split)
{
    pthread_mutex_lock(&scheduler->lock);
    if (success) {
        scheduler->completed_tasks++;
    } else {
        scheduler->failed_tasks++;
    }
    if (split) {
        scheduler->split_fallbacks++;
    }
    pthread_cond_broadcast(&scheduler->state_changed);
    pthread_mutex_unlock(&scheduler->lock);
}

static int compaction_commit_image(struct compaction_scheduler *scheduler,
                                   struct compressed_pending_task *task,
                                   struct compressed_page_image *image)
{
    struct simple_leaf_node *leaf = task->leaf;
    int committed = 0;
    pthread_rwlock_wrlock(&leaf->rwlock);
    if (leaf->pending == task &&
        leaf->pending_token == task->token &&
        leaf->base_version == task->base_version) {
        leaf_index_adopt(leaf, image->index, image->num_subpages);
        image->index = NULL;
        leaf->num_subpages = image->num_subpages;
        leaf->num_subpage_entries = image->num_subpages;
        leaf->uncompressed_bytes = image->uncompressed_bytes;
        compressed_leaf_adopt_base(leaf,
                                   image->data,
                                   image->size,
                                   image->usable);
        image->data = NULL;
        leaf->pending = NULL;
        leaf->generation++;
        committed = 1;
    }
    pthread_rwlock_unlock(&leaf->rwlock);

    if (committed) {
        __atomic_add_fetch(&task->tree->bg_compactions, 1, __ATOMIC_RELAXED);
        free(task);
        compaction_scheduler_note_completion(scheduler, 1, 0);
    }
    return committed ? 0 : -1;
}

static int compaction_split_overflow(struct compaction_scheduler *scheduler,
                                     struct compressed_pending_task *task)
{
    struct bplus_tree_compressed *tree = task->tree;
    struct compressed_leaf_ref *wrapper = NULL;
    struct compressed_leaf_ref *new_leaf = NULL;
    key_t split_key = 0;
    int result = -1;

    pthread_rwlock_wrlock(&tree->rwlock);
    if (tree->tree) {
        struct list_head *head = &tree->tree->list[0];
        struct list_head *pos, *next;
        list_for_each_safe(pos, next, head) {
            struct compressed_leaf_ref *candidate = list_entry(pos, struct compressed_leaf_ref, link);
            if (candidate->type == BPLUS_TREE_LEAF &&
                candidate->payload == (value_t)task->leaf) {
                wrapper = candidate;
                break;
            }
        }
    }

    if (wrapper && task->leaf->pending == task &&
        split_leaf(tree, wrapper, &new_leaf, &split_key) == 0 && new_leaf) {
        result = 0; /* split_leaf publishes both leaves and the reserved parent path. */
    }
    pthread_rwlock_unlock(&tree->rwlock);

    if (result == 0) {
        __atomic_add_fetch(&tree->fg_split_fallbacks, 1, __ATOMIC_RELAXED);
        free(task);
        compaction_scheduler_note_completion(scheduler, 1, 1);
        return 0;
    }

    pthread_rwlock_wrlock(&task->leaf->rwlock);
    if (task->leaf->pending == task) {
        pending_task_state_store(task, PENDING_TASK_FAILED);
    }
    pthread_rwlock_unlock(&task->leaf->rwlock);
    compaction_scheduler_note_completion(scheduler, 0, 1);
    return -1;
}

static int compaction_process_task_impl(struct compaction_scheduler *scheduler,
                                   struct compressed_pending_task *task)
{
    struct kv_pair *pairs = NULL;
    size_t count = 0;
    size_t capacity = 0;
    struct compressed_page_image image;
    memset(&image, 0, sizeof(image));

    if (env_bool_enabled("BTREE_TEST_FAIL_CODEC_ALWAYS", 0)) {
        return -1;
    }
    if (env_bool_enabled("BTREE_TEST_FAIL_CODEC_ONCE", 0) &&
        __atomic_exchange_n(&compaction_test_codec_failure_consumed,
                            1,
                            __ATOMIC_RELAXED) == 0) {
        return -1;
    }

    if (compressed_leaf_collect_base_pairs(task->tree, task->leaf, &pairs, &count) != 0) {
        free(pairs);
        return -1;
    }
    capacity = count;
    for (size_t i = 0; i < task->count; i++) {
        struct kv_pair *entry = &task->records[i];
        if (entry->key != 0 &&
            kv_vector_put(&pairs,
                          &count,
                          &capacity,
                          entry->key,
                          entry->stored_value,
                          entry->payload,
                          COMPRESSED_VALUE_BYTES) != 0) {
            free(pairs);
            return -1;
        }
    }

    AGG_INC(merged_updates, task->count);
    AGG_INC(merge_count, 1);
    int build = compressed_build_base_image(task->tree, task->leaf, pairs, count, &image);
    free(pairs);
    if (build == -2) {
        compressed_page_image_destroy(&image);
        return compaction_split_overflow(scheduler, task);
    }
    if (build != 0) {
        compressed_page_image_destroy(&image);
        return -1;
    }

    int commit = compaction_commit_image(scheduler, task, &image);
    compressed_page_image_destroy(&image);
    return commit;
}

static int compaction_process_task(struct compaction_scheduler *scheduler,
                                   struct compressed_pending_task *task)
{
#ifdef ZIPCACHE_AGG_EXPERIMENT
    int previous = agg_origin;
    agg_origin = BPLUS_ORIGIN_COMPACTION;
#endif
    int result = compaction_process_task_impl(scheduler, task);
#ifdef ZIPCACHE_AGG_EXPERIMENT
    agg_origin = previous;
#endif
    return result;
}

/*
 * Use one reusable QPL job per leaf in the batch.  This path is deliberately
 * limited to the v1 4 KiB/one-subpage representation.  Other codecs and
 * legacy multi-subpage configurations use the same scheduler but execute the
 * existing codec helper sequentially outside the leaf write lock.
 *
 * Return 1 when every task in the batch was handled (committed, split,
 * retried, or marked failed), and 0 when the batch is not eligible.
 */
static void codec_contract_complete(struct compaction_scheduler *scheduler, int i)
{
    struct codec_async_slot *slot = &scheduler->codec_slots[i];
    assert(slot->accepted && !slot->terminal);
    int produced = -1;
    if (scheduler->contract_mode != 4) {
        produced = slot->op == qpl_op_compress
            ? compress_subpage_impl(slot->task->tree, slot->task->leaf,
                slot->next_in_ptr, slot->available_in, slot->next_out_ptr, slot->available_out)
            : decompress_subpage_impl(slot->task->tree, slot->task->leaf,
                slot->next_in_ptr, slot->available_in, slot->next_out_ptr, slot->available_out);
    }
    slot->total_out = scheduler->contract_mode == 5 ? slot->available_out + 1 :
        (produced > 0 ? (uint32_t)produced : 0);
    slot->result = produced > 0 ? QPL_STS_OK : QPL_STS_NOT_SUPPORTED_MODE_ERR;
    for (int j = 0; j < scheduler->batch_size; j++) {
        struct codec_async_slot *other = &scheduler->codec_slots[j];
        if (other->accepted && !other->terminal && other->sequence < slot->sequence) {
            scheduler->contract_stats.out_of_order++;
            break;
        }
    }
    slot->terminal = 1;
    scheduler->contract_stats.terminal++;
    scheduler->contract_stats.in_flight--;
    if (slot->result != QPL_STS_OK || scheduler->contract_mode == 5)
        scheduler->contract_stats.failures++;
    pthread_cond_broadcast(&scheduler->state_changed);
}

static qpl_status codec_backend_submit(struct compaction_scheduler *scheduler, int i)
{
    struct codec_async_slot *slot = &scheduler->codec_slots[i];
    if (!scheduler->contract_mode) {
        qpl_job *job = scheduler->qpl_jobs[i];
        job->op = slot->op;
        job->next_in_ptr = slot->next_in_ptr; job->available_in = slot->available_in;
        job->next_out_ptr = slot->next_out_ptr; job->available_out = slot->available_out;
        job->total_in = job->total_out = 0;
        job->flags = slot->flags; job->level = slot->level;
        qpl_status result = qpl_submit_job(job);
        pthread_mutex_lock(&scheduler->lock);
        if (result == QPL_STS_OK) {
            scheduler->contract_stats.accepted++;
            scheduler->contract_stats.in_flight++;
        }
#ifdef HAVE_QPL
        if (result == QPL_STS_QUEUES_ARE_BUSY_ERR) scheduler->contract_stats.busy++;
#endif
        pthread_mutex_unlock(&scheduler->lock);
        return result;
    }
    pthread_mutex_lock(&scheduler->lock);
    assert(!slot->accepted || slot->terminal);
    if (scheduler->contract_mode == 3 && !scheduler->contract_stats.busy) {
        scheduler->contract_stats.busy++;
        pthread_mutex_unlock(&scheduler->lock);
        return QPL_STS_NOT_SUPPORTED_MODE_ERR; /* Rejected, not an accepted job. */
    }
    slot->accepted = 1; slot->terminal = 0;
    slot->sequence = ++scheduler->contract_stats.accepted;
    scheduler->contract_stats.in_flight++;
    if (scheduler->contract_mode == 1) codec_contract_complete(scheduler, i);
    pthread_cond_broadcast(&scheduler->state_changed);
    pthread_mutex_unlock(&scheduler->lock);
    return QPL_STS_OK;
}

static qpl_status codec_backend_wait(struct compaction_scheduler *scheduler, int i)
{
    pthread_mutex_lock(&scheduler->lock);
    if (scheduler->queue_count) scheduler->ready_at_wait_samples++;
    pthread_mutex_unlock(&scheduler->lock);
    if (!scheduler->contract_mode) {
        qpl_status result = qpl_wait_job(scheduler->qpl_jobs[i]);
        scheduler->codec_slots[i].total_out = scheduler->qpl_jobs[i]->total_out;
        pthread_mutex_lock(&scheduler->lock);
        scheduler->contract_stats.terminal++;
        scheduler->contract_stats.in_flight--;
        if (result != QPL_STS_OK) scheduler->contract_stats.failures++;
        pthread_mutex_unlock(&scheduler->lock);
        return result;
    }
    pthread_mutex_lock(&scheduler->lock);
    while (!scheduler->contract_gate_open && !scheduler->shutdown)
        pthread_cond_wait(&scheduler->state_changed, &scheduler->lock);
    /* Deterministic completion events, not simulated device service times. */
    for (int j = scheduler->batch_size - 1; j >= 0; j--) {
        struct codec_async_slot *slot = &scheduler->codec_slots[j];
        if (slot->accepted && !slot->terminal) codec_contract_complete(scheduler, j);
    }
    qpl_status result = scheduler->codec_slots[i].result;
    pthread_mutex_unlock(&scheduler->lock);
    return result;
}

int bplus_tree_compressed_test_contract(struct bplus_tree_compressed *tree, int gate_open,
                                       struct bplus_tree_contract_stats *stats)
{
    struct compaction_scheduler *scheduler = tree ? tree->scheduler : NULL;
    if (!scheduler) return -1;
#ifdef ZIPCACHE_AGG_EXPERIMENT
    if (!scheduler->contract_mode && !scheduler->split_test_mode) return -1;
#else
    if (!scheduler->contract_mode) return -1;
#endif
    pthread_mutex_lock(&scheduler->lock);
    if (gate_open >= 0) scheduler->contract_gate_open = !!gate_open;
    if (stats) *stats = scheduler->contract_stats;
    pthread_cond_broadcast(&scheduler->state_changed);
    pthread_mutex_unlock(&scheduler->lock);
    return 0;
}

static int compaction_process_codec_batch(struct compaction_scheduler *scheduler,
                                        struct compressed_pending_task **batch,
                                        int count)
{
#if ZIPCACHE_AGG_LAYOUT > 0
    /* The legacy adapter emits unframed full pages. A format-aware asynchronous
     * adapter is a later, capacity-gated experiment, not an implicit fallback. */
    return 0;
#endif
    if (!scheduler || !batch || count <= 0 ||
        !scheduler->codec_slots || (!scheduler->contract_mode && scheduler->qpl_job_count < count)) {
        return 0;
    }

    for (int i = 0; i < count; i++) {
        struct compressed_pending_task *task = batch[i];
        if (!task || !task->tree || !task->leaf ||
            (!scheduler->contract_mode && task->leaf->compression_algo != COMPRESS_QPL) ||
            task->leaf->num_subpages != 1) {
            return 0;
        }
    }

    struct kv_pair **pairs = calloc((size_t)count, sizeof(*pairs));
    size_t *pair_counts = calloc((size_t)count, sizeof(*pair_counts));
    size_t *pair_capacities = calloc((size_t)count, sizeof(*pair_capacities));
    int *submitted = calloc((size_t)count, sizeof(*submitted));
    int *profiled = calloc((size_t)count, sizeof(*profiled));
    int *overflow = calloc((size_t)count, sizeof(*overflow));
    struct compressed_page_image *images = calloc((size_t)count, sizeof(*images));
    if (env_bool_enabled("BTREE_TEST_FAIL_BATCH_ALLOC", 0)) { free(submitted); submitted = NULL; }
    int batch_error = (!pairs || !pair_counts || !pair_capacities ||
                       !submitted || !profiled || !overflow || !images);

    memset(scheduler->raw_workspaces,
           0,
           (size_t)count * COMPRESSED_LEAF_SIZE);

    uint64_t phase_start = submission_now_ns();
    /* Phase 1: submit every base decompression before waiting for any one. */
    for (int i = 0; i < count && !batch_error; i++) {
        struct compressed_pending_task *task = batch[i];
        struct simple_leaf_node *leaf = task->leaf;
        uint8_t *raw = scheduler->raw_workspaces + (size_t)i * COMPRESSED_LEAF_SIZE;
        const uint8_t *source = NULL;
        uint32_t source_size = 0;

        pthread_rwlock_rdlock(&leaf->rwlock);
        if (leaf->pending != task || leaf->pending_token != task->token ||
            leaf->base_version != task->base_version) {
            batch_error = 1;
        } else if (leaf->is_compressed && leaf->compressed_data &&
                   leaf->subpage_index && leaf->subpage_index[0].length > 0) {
            source = (const uint8_t *)leaf->compressed_data + leaf->subpage_index[0].offset;
            source_size = leaf->subpage_index[0].length;
        }
        pthread_rwlock_unlock(&leaf->rwlock);
        if (batch_error || source_size == 0) {
            continue;
        }

        struct codec_async_slot *job = &scheduler->codec_slots[i];
        job->task = task;
        job->op = qpl_op_decompress;
        job->next_in_ptr = (uint8_t *)source;
        job->available_in = source_size;
        job->total_in = 0;
        job->next_out_ptr = raw;
        job->available_out = COMPRESSED_LEAF_SIZE;
        job->total_out = 0;
        job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
        profiled[i] = submission_profile_codec_begin(task->tree,
                                                     0,
                                                     source_size,
                                                     COMPRESSED_LEAF_SIZE,
                                                     leaf);
        __atomic_add_fetch(&task->tree->qpl_decompress_calls, 1, __ATOMIC_RELAXED);
        __atomic_add_fetch(&task->tree->qpl_pool_jobs, 1, __ATOMIC_RELAXED);
        qpl_status status = codec_backend_submit(scheduler, i);
        if (status != QPL_STS_OK) {
            submission_profile_codec_end(task->tree, profiled[i]);
            profiled[i] = 0;
            __atomic_add_fetch(&task->tree->qpl_errors, 1, __ATOMIC_RELAXED);
            batch_error = 1;
            break;
        }
        submitted[i] = 1;
    }

    for (int i = 0; i < count; i++) {
        if (!submitted || !submitted[i]) {
            continue;
        }
        qpl_status status = codec_backend_wait(scheduler, i);
        submission_profile_codec_end(batch[i]->tree, profiled[i]);
        profiled[i] = 0;
        if (status != QPL_STS_OK || scheduler->codec_slots[i].total_out != COMPRESSED_LEAF_SIZE) {
            __atomic_add_fetch(&batch[i]->tree->qpl_errors, 1, __ATOMIC_RELAXED);
            batch_error = 1;
        }
    }

    uint64_t phase_now = submission_now_ns();
    pthread_mutex_lock(&scheduler->lock);
    scheduler->decompress_phase_ns += phase_now - phase_start;
    pthread_mutex_unlock(&scheduler->lock);
    phase_start = phase_now;
    /* CPU merge: pending overwrites the immutable base. */
    for (int i = 0; i < count && !batch_error; i++) {
        uint8_t *raw = scheduler->raw_workspaces + (size_t)i * COMPRESSED_LEAF_SIZE;
        struct kv_pair *slots = (struct kv_pair *)(void *)raw;
        size_t slot_count = COMPRESSED_LEAF_SIZE / sizeof(struct kv_pair);
        for (size_t j = 0; j < slot_count; j++) {
            if (slots[j].key != 0 &&
                kv_vector_put(&pairs[i],
                              &pair_counts[i],
                              &pair_capacities[i],
                              slots[j].key,
                              slots[j].stored_value,
                              slots[j].payload,
                              COMPRESSED_VALUE_BYTES) != 0) {
                batch_error = 1;
                break;
            }
        }
        for (size_t j = 0; j < batch[i]->count && !batch_error; j++) {
            struct kv_pair *entry = &batch[i]->records[j];
            if (entry->key != 0 &&
                kv_vector_put(&pairs[i],
                              &pair_counts[i],
                              &pair_capacities[i],
                              entry->key,
                              entry->stored_value,
                              entry->payload,
                              COMPRESSED_VALUE_BYTES) != 0) {
                batch_error = 1;
            }
        }
        if (batch_error) {
            break;
        }

        qsort(pairs[i], pair_counts[i], sizeof(struct kv_pair), compare_kv_pairs);
        memset(raw, 0, COMPRESSED_LEAF_SIZE);
        slots = (struct kv_pair *)(void *)raw;
        for (size_t j = 0; j < pair_counts[i]; j++) {
            if (j >= slot_count) {
                overflow[i] = 1;
                break;
            }
            slots[j] = pairs[i][j];
        }
    }

    memset(scheduler->compressed_workspaces,
           0,
           (size_t)count * MAX_COMPRESSED_SIZE);
    if (submitted) memset(submitted, 0, (size_t)count * sizeof(*submitted));

    phase_now = submission_now_ns();
    pthread_mutex_lock(&scheduler->lock);
    scheduler->merge_phase_ns += phase_now - phase_start;
    pthread_mutex_unlock(&scheduler->lock);
    phase_start = phase_now;
    /* Phase 2: submit every merged-page compression, then wait as a group. */
    for (int i = 0; i < count && !batch_error; i++) {
        if (overflow[i]) {
            continue;
        }
        struct compressed_pending_task *task = batch[i];
        uint8_t *raw = scheduler->raw_workspaces + (size_t)i * COMPRESSED_LEAF_SIZE;
        uint8_t *compressed = scheduler->compressed_workspaces +
                              (size_t)i * MAX_COMPRESSED_SIZE;
        struct codec_async_slot *job = &scheduler->codec_slots[i];
        job->task = task;
        job->op = qpl_op_compress;
        job->next_in_ptr = raw;
        job->available_in = COMPRESSED_LEAF_SIZE;
        job->total_in = 0;
        job->next_out_ptr = compressed;
        job->available_out = MAX_COMPRESSED_SIZE;
        job->total_out = 0;
        job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
        if (task->tree->config.qpl_huffman_mode == QPL_HUFFMAN_DYNAMIC) {
            job->flags |= QPL_FLAG_DYNAMIC_HUFFMAN;
        }
        job->level = qpl_default_level;
        profiled[i] = submission_profile_codec_begin(task->tree,
                                                     1,
                                                     COMPRESSED_LEAF_SIZE,
                                                     MAX_COMPRESSED_SIZE,
                                                     task->leaf);
        __atomic_add_fetch(&task->tree->qpl_compress_calls, 1, __ATOMIC_RELAXED);
        __atomic_add_fetch(&task->tree->qpl_pool_jobs, 1, __ATOMIC_RELAXED);
        qpl_status status = codec_backend_submit(scheduler, i);
        if (status != QPL_STS_OK) {
            submission_profile_codec_end(task->tree, profiled[i]);
            profiled[i] = 0;
            __atomic_add_fetch(&task->tree->qpl_errors, 1, __ATOMIC_RELAXED);
            batch_error = 1;
            break;
        }
        submitted[i] = 1;
    }

    for (int i = 0; i < count; i++) {
        if (!submitted || !submitted[i]) {
            continue;
        }
        qpl_status status = codec_backend_wait(scheduler, i);
        submission_profile_codec_end(batch[i]->tree, profiled[i]);
        profiled[i] = 0;
        uint32_t produced = scheduler->codec_slots[i].total_out;
        if (status != QPL_STS_OK || produced == 0 || produced > MAX_COMPRESSED_SIZE) {
            __atomic_add_fetch(&batch[i]->tree->qpl_errors, 1, __ATOMIC_RELAXED);
            batch_error = 1;
            continue;
        }

        images[i].data = malloc(produced);
        images[i].index = calloc(1, sizeof(*images[i].index));
        if (!images[i].data || !images[i].index) {
            batch_error = 1;
            continue;
        }
        memcpy(images[i].data,
               scheduler->compressed_workspaces + (size_t)i * MAX_COMPRESSED_SIZE,
               produced);
        images[i].size = produced;
        images[i].usable = allocation_usable_size(images[i].data, produced);
        images[i].num_subpages = 1;
        images[i].uncompressed_bytes = pair_counts[i] * sizeof(struct kv_pair);
        images[i].index[0].offset = 0;
        images[i].index[0].length = produced;
        images[i].index[0].uncompressed_bytes = COMPRESSED_LEAF_SIZE;
    }

    phase_now = submission_now_ns();
    pthread_mutex_lock(&scheduler->lock);
    scheduler->compress_phase_ns += phase_now - phase_start;
    pthread_mutex_unlock(&scheduler->lock);
    phase_start = phase_now;
    if (batch_error) {
        for (int i = 0; i < count; i++) {
            free(pairs ? pairs[i] : NULL);
            if (images) {
                compressed_page_image_destroy(&images[i]);
            }
        }
        free(pairs);
        free(pair_counts);
        free(pair_capacities);
        free(submitted);
        free(profiled);
        free(overflow);
        free(images);

        /*
         * auto path gets its same-format QPL software fallback through
         * the existing codec helper; strict hardware returns an error and the
         * task-level retry policy below applies.
         */
        for (int i = 0; i < count; i++) {
            if (scheduler->contract_mode) {
                compaction_fail_or_retry(scheduler, batch[i]);
                continue;
            }
            if (compaction_process_task(scheduler, batch[i]) != 0 &&
                pending_task_state_load(batch[i]) != PENDING_TASK_FAILED) {
                compaction_fail_or_retry(scheduler, batch[i]);
            }
        }
        return 1;
    }

    for (int i = 0; i < count; i++) {
        free(pairs[i]);
        if (overflow[i]) {
            (void)compaction_split_overflow(scheduler, batch[i]);
            continue;
        }
        if (compaction_commit_image(scheduler, batch[i], &images[i]) != 0 &&
            pending_task_state_load(batch[i]) != PENDING_TASK_FAILED) {
            compaction_fail_or_retry(scheduler, batch[i]);
        }
        compressed_page_image_destroy(&images[i]);
    }

    free(pairs);
    free(pair_counts);
    free(pair_capacities);
    free(submitted);
    free(profiled);
    free(overflow);
    free(images);
    pthread_mutex_lock(&scheduler->lock);
    scheduler->commit_phase_ns += submission_now_ns() - phase_start;
    pthread_mutex_unlock(&scheduler->lock);
    return 1;
}

static void compaction_fail_or_retry(struct compaction_scheduler *scheduler,
                                     struct compressed_pending_task *task)
{
    task->attempts++;
    pthread_mutex_lock(&scheduler->lock);
    if (!scheduler->shutdown && task->attempts < 2 &&
        scheduler->queue_count
#ifdef ZIPCACHE_AGG_EXPERIMENT
            + (int)scheduler->admission_reserved
#endif
            < scheduler->queue_capacity) {
        pending_task_state_store(task, PENDING_TASK_QUEUED);
        scheduler->queue[scheduler->queue_tail] = task;
        scheduler->queue_tail = (scheduler->queue_tail + 1) % scheduler->queue_capacity;
        scheduler->queue_count++;
        scheduler->retry_count++;
        pthread_cond_signal(&scheduler->work_available);
        pthread_mutex_unlock(&scheduler->lock);
        return;
    }
    pthread_mutex_unlock(&scheduler->lock);

    pthread_rwlock_wrlock(&task->leaf->rwlock);
    if (task->leaf->pending == task) {
        pending_task_state_store(task, PENDING_TASK_FAILED);
    }
    pthread_rwlock_unlock(&task->leaf->rwlock);
    compaction_scheduler_note_completion(scheduler, 0, 0);
}

static void *compaction_scheduler_worker(void *argument)
{
    struct compaction_scheduler *scheduler = argument;
    struct compressed_pending_task *batch[32] = {0};

    for (;;) {
        int count = 0;
        pthread_mutex_lock(&scheduler->lock);
        while (scheduler->queue_count == 0 && !scheduler->shutdown) {
            pthread_cond_wait(&scheduler->work_available, &scheduler->lock);
        }
        if (scheduler->queue_count == 0 && scheduler->shutdown) {
            pthread_mutex_unlock(&scheduler->lock);
            break;
        }
        while (count < scheduler->batch_size && scheduler->queue_count > 0) {
            struct compressed_pending_task *task = scheduler->queue[scheduler->queue_head];
            scheduler->queue[scheduler->queue_head] = NULL;
            scheduler->queue_head = (scheduler->queue_head + 1) % scheduler->queue_capacity;
            scheduler->queue_count--;
            pending_task_state_store(task, PENDING_TASK_RUNNING);
            batch[count++] = task;
            scheduler->running_count++;

            uint64_t waited = submission_now_ns() - task->enqueued_ns;
            scheduler->total_queue_wait_ns += waited;
            AGG_INC(queue_wait_ns, waited);
            if (waited > scheduler->max_queue_wait_ns) {
                scheduler->max_queue_wait_ns = waited;
            }
        }
        scheduler->batches++;
        pthread_cond_broadcast(&scheduler->state_changed);
        pthread_mutex_unlock(&scheduler->lock);

        int test_delay_us = env_int_clamped("BTREE_TEST_WORKER_DELAY_US", 0, 0, 1000000);
        if (test_delay_us > 0) {
            usleep((useconds_t)test_delay_us);
        }

        submission_tls_write_critical = 0;
        int qpl_handled = compaction_process_codec_batch(scheduler, batch, count);
        for (int i = 0; i < count; i++) {
            if (!qpl_handled &&
                compaction_process_task(scheduler, batch[i]) != 0 &&
                pending_task_state_load(batch[i]) != PENDING_TASK_FAILED) {
                compaction_fail_or_retry(scheduler, batch[i]);
            }
            pthread_mutex_lock(&scheduler->lock);
            scheduler->running_count--;
            pthread_cond_broadcast(&scheduler->state_changed);
            pthread_mutex_unlock(&scheduler->lock);
            batch[i] = NULL;
        }
    }

    return NULL;
}

static void compaction_scheduler_release_qpl_jobs(struct compaction_scheduler *scheduler)
{
    if (!scheduler) {
        return;
    }
    assert(!scheduler->contract_stats.in_flight &&
           scheduler->contract_stats.accepted == scheduler->contract_stats.terminal);
    for (int i = 0; i < scheduler->batch_size; i++) {
        if (scheduler->qpl_jobs && scheduler->qpl_jobs[i]) {
            qpl_fini_job(scheduler->qpl_jobs[i]);
        }
        free(scheduler->qpl_job_buffers ? scheduler->qpl_job_buffers[i] : NULL);
    }
    free(scheduler->qpl_jobs);
    free(scheduler->qpl_job_buffers);
    free(scheduler->codec_slots);
    scheduler->codec_slots = NULL;
    scheduler->qpl_jobs = NULL;
    scheduler->qpl_job_buffers = NULL;
    scheduler->qpl_job_count = 0;
    scheduler->qpl_job_bytes = 0;
}

static int compaction_scheduler_init_qpl_jobs(struct compaction_scheduler *scheduler)
{
#if ZIPCACHE_AGG_LAYOUT > 0
    /* Framed layouts run synchronous per-task transforms using worker TLS.
     * Do not reserve unused legacy batch jobs, especially for a different path. */
    (void)scheduler;
    return 0;
#endif
    if (!scheduler || !scheduler->root ||
        scheduler->root->config.algo != COMPRESS_QPL) {
        return 0;
    }

    uint32_t job_size = 0;
    qpl_status status = qpl_get_job_size(scheduler->root->config.qpl_path, &job_size);
    if (status != QPL_STS_OK || job_size == 0) {
        return -1;
    }

    scheduler->qpl_jobs = calloc((size_t)scheduler->batch_size,
                                 sizeof(*scheduler->qpl_jobs));
    scheduler->qpl_job_buffers = calloc((size_t)scheduler->batch_size,
                                        sizeof(*scheduler->qpl_job_buffers));
    if (!scheduler->qpl_jobs || !scheduler->qpl_job_buffers) {
        goto fail;
    }

    for (int i = 0; i < scheduler->batch_size; i++) {
        scheduler->qpl_job_buffers[i] = malloc(job_size);
        if (!scheduler->qpl_job_buffers[i]) {
            goto fail;
        }
        scheduler->qpl_jobs[i] = (qpl_job *)scheduler->qpl_job_buffers[i];
        status = qpl_init_job(scheduler->root->config.qpl_path, scheduler->qpl_jobs[i]);
        if (status != QPL_STS_OK) {
            scheduler->qpl_jobs[i] = NULL;
            goto fail;
        }
        scheduler->qpl_job_count++;
        scheduler->qpl_job_bytes += job_size;
    }
    return 0;

fail:
    compaction_scheduler_release_qpl_jobs(scheduler);
    return -1;
}

static struct compaction_scheduler *compaction_scheduler_create(
    struct bplus_tree_compressed *root)
{
    if (!root || !env_bool_enabled("BTREE_BG_COMPACTION", 0)) {
        return NULL;
    }

    struct compaction_scheduler *scheduler = calloc(1, sizeof(*scheduler));
    if (!scheduler) {
        return NULL;
    }
    scheduler->root = root;
    scheduler->contract_mode = env_int_clamped("BTREE_TEST_CONTRACT_MODE", 0, 0, 5);
    scheduler->contract_gate_open = !env_bool_enabled("BTREE_TEST_CONTRACT_CLOSED", 0);
    if (scheduler->contract_mode && root->config.algo != COMPRESS_LZ4 &&
        root->config.algo != COMPRESS_ZLIB_ACCEL) { free(scheduler); return NULL; }
    scheduler->batch_size = env_int_clamped("BTREE_BG_BATCH_SIZE", 8, 1, 32);
    scheduler->queue_capacity = env_int_clamped("BTREE_BG_QUEUE_CAPACITY", 32, 1, 4096);
    if (scheduler->queue_capacity < scheduler->batch_size) {
        scheduler->queue_capacity = scheduler->batch_size;
    }
#ifdef ZIPCACHE_AGG_EXPERIMENT
    scheduler->split_test_mode = env_int_clamped("BTREE_TEST_SPLIT_CODEC_MODE", 0, 0, 5);
    scheduler->admission_control = env_bool_enabled("BTREE_AGG_BACKPRESSURE", 0);
    scheduler->pending_charge = sizeof(struct compressed_pending_task) +
        (ACTIVE_DELTA_ENTRIES+1)*sizeof(struct kv_pair);
    scheduler->pending_byte_limit = (size_t)env_int_clamped("BTREE_AGG_PENDING_BYTES",
        (int)((scheduler->queue_capacity+scheduler->batch_size)*scheduler->pending_charge), 0, INT_MAX);
#endif
    scheduler->queue = calloc((size_t)scheduler->queue_capacity, sizeof(*scheduler->queue));
    scheduler->raw_workspaces = calloc((size_t)scheduler->batch_size, COMPRESSED_LEAF_SIZE);
    scheduler->compressed_workspaces = calloc((size_t)scheduler->batch_size, MAX_COMPRESSED_SIZE);
    if (!scheduler->queue || !scheduler->raw_workspaces || !scheduler->compressed_workspaces) {
        free(scheduler->queue);
        free(scheduler->raw_workspaces);
        free(scheduler->compressed_workspaces);
        free(scheduler);
        return NULL;
    }

    pthread_mutex_init(&scheduler->lock, NULL);
    pthread_cond_init(&scheduler->work_available, NULL);
    pthread_cond_init(&scheduler->state_changed, NULL);
    int qpl_init_result = scheduler->contract_mode ? 0 : compaction_scheduler_init_qpl_jobs(scheduler);
    if (!qpl_init_result && (scheduler->contract_mode || scheduler->qpl_job_count)) {
        scheduler->codec_slots = calloc((size_t)scheduler->batch_size, sizeof(*scheduler->codec_slots));
        if (!scheduler->codec_slots) qpl_init_result = -1;
    }
    if (qpl_init_result != 0 &&
        (scheduler->contract_mode || (root->config.algo == COMPRESS_QPL &&
        root->config.qpl_path == qpl_path_hardware))) {
        compaction_scheduler_release_qpl_jobs(scheduler);
        pthread_cond_destroy(&scheduler->state_changed);
        pthread_cond_destroy(&scheduler->work_available);
        pthread_mutex_destroy(&scheduler->lock);
        free(scheduler->queue);
        free(scheduler->raw_workspaces);
        free(scheduler->compressed_workspaces);
        free(scheduler);
        return NULL;
    }
    scheduler->accepting = 1;
    pthread_attr_t worker_attr;
    pthread_attr_init(&worker_attr);
    int start_error = 0;
#ifdef ZIPCACHE_AGG_EXPERIMENT
    size_t page = (size_t)sysconf(_SC_PAGESIZE);
    scheduler->worker_stack_bytes = 512*1024 + page;
    scheduler->worker_stack = malloc(scheduler->worker_stack_bytes);
    if (!scheduler->worker_stack) start_error = ENOMEM;
    else {
        memset(scheduler->worker_stack, 0, scheduler->worker_stack_bytes);
        uintptr_t base = ((uintptr_t)scheduler->worker_stack+page-1) & ~(uintptr_t)(page-1);
        start_error = pthread_attr_setstack(&worker_attr, (void *)base, 512*1024);
    }
#endif
    if (!start_error) start_error = pthread_create(&scheduler->worker, &worker_attr, compaction_scheduler_worker, scheduler);
    pthread_attr_destroy(&worker_attr);
    if (start_error) {
#ifdef ZIPCACHE_AGG_EXPERIMENT
        free(scheduler->worker_stack);
#endif
        compaction_scheduler_release_qpl_jobs(scheduler);
        pthread_cond_destroy(&scheduler->state_changed);
        pthread_cond_destroy(&scheduler->work_available);
        pthread_mutex_destroy(&scheduler->lock);
        free(scheduler->queue);
        free(scheduler->raw_workspaces);
        free(scheduler->compressed_workspaces);
        free(scheduler);
        return NULL;
    }
    scheduler->started = 1;
    return scheduler;
}

static void compaction_scheduler_attach(struct bplus_tree_compressed *tree,
                                        struct compaction_scheduler *scheduler,
                                        int owner)
{
    if (!tree) {
        return;
    }
    tree->scheduler = scheduler;
    tree->owns_scheduler = owner;
    tree->bg_compaction_enabled = scheduler != NULL;
    tree->bg_thread_count = scheduler ? 1 : 0;
    tree->bg_queue_capacity = scheduler ? scheduler->queue_capacity : 0;
    if (scheduler) {
        /* v1 async semantics require all three active slots, regardless of a
         * deprecated landing-buffer environment value. */
        tree->config.buffer_size = LANDING_BUFFER_BYTES;
        tree->simple_config.buffer_size = LANDING_BUFFER_BYTES;
    }
    if (compressed_tree_is_sharded(tree)) {
        for (int i = 0; i < tree->shard_count; i++) {
            compaction_scheduler_attach(tree->shards[i], scheduler, 0);
        }
    }
}

static void compaction_scheduler_destroy(struct compaction_scheduler *scheduler)
{
    if (!scheduler) {
        return;
    }
    pthread_mutex_lock(&scheduler->lock);
    scheduler->accepting = 0;
    scheduler->shutdown = 1;
    pthread_cond_broadcast(&scheduler->work_available);
    pthread_cond_broadcast(&scheduler->state_changed);
    pthread_mutex_unlock(&scheduler->lock);

    if (scheduler->started) {
        pthread_join(scheduler->worker, NULL);
    }
#ifdef ZIPCACHE_AGG_EXPERIMENT
    free(scheduler->worker_stack);
#endif
    compaction_scheduler_release_qpl_jobs(scheduler);
    free(scheduler->queue);
    free(scheduler->raw_workspaces);
    free(scheduler->compressed_workspaces);
    pthread_cond_destroy(&scheduler->state_changed);
    pthread_cond_destroy(&scheduler->work_available);
    pthread_mutex_destroy(&scheduler->lock);
    free(scheduler);
}

static int compressed_pending_status(struct bplus_tree_compressed *tree)
{
    if (compressed_tree_is_sharded(tree)) {
        for (int i = 0; i < tree->shard_count; i++)
            if (compressed_pending_status(tree->shards[i])) return -1;
        return 0;
    }
    int result = 0;
    pthread_rwlock_rdlock(&tree->rwlock);
    struct list_head *pos, *next;
    list_for_each_safe(pos, next, &tree->tree->list[0]) {
        struct compressed_leaf_ref *ref = list_entry(pos, struct compressed_leaf_ref, link);
        struct simple_leaf_node *leaf = (struct simple_leaf_node *)ref->payload;
        if (!leaf) continue;
        pthread_rwlock_rdlock(&leaf->rwlock);
        if (leaf->pending) result = -1;
        pthread_rwlock_unlock(&leaf->rwlock);
    }
    pthread_rwlock_unlock(&tree->rwlock);
    return result;
}

int bplus_tree_compressed_drain_background(struct bplus_tree_compressed *ct_tree)
{
    if (!ct_tree || !ct_tree->initialized) {
        return -1;
    }
    struct compaction_scheduler *scheduler = ct_tree->scheduler;
    if (!scheduler) {
        return 0;
    }
    pthread_mutex_lock(&scheduler->lock);
    while (scheduler->queue_count > 0 || scheduler->running_count > 0) {
        pthread_cond_wait(&scheduler->state_changed, &scheduler->lock);
    }
    pthread_mutex_unlock(&scheduler->lock);
    /* Caller quiesces producers. An empty queue is not a successful drain
     * when a permanently failed task is still the authoritative delta. */
    return compressed_pending_status(ct_tree);
}

static int compressed_memory_stats_collect(struct bplus_tree_compressed *ct_tree,
                                           struct bplus_tree_memory_stats *stats,
                                           int (*visitor)(const struct kv_pair *, void *),
                                           void *context)
{
    if (!ct_tree || !stats || !ct_tree->initialized) {
        return -1;
    }

    stats->tree_metadata_bytes += sizeof(*ct_tree);
    stats->tree_metadata_usable_bytes += allocation_usable_size(ct_tree, sizeof(*ct_tree));
    if (compressed_tree_is_sharded(ct_tree)) {
        stats->tree_metadata_bytes +=
            (size_t)ct_tree->shard_count * sizeof(*ct_tree->shards);
        stats->tree_metadata_usable_bytes += allocation_usable_size(ct_tree->shards,
            (size_t)ct_tree->shard_count * sizeof(*ct_tree->shards));
        for (int i = 0; i < ct_tree->shard_count; i++) {
            if (compressed_memory_stats_collect(ct_tree->shards[i], stats, visitor, context) != 0) {
                return -1;
            }
        }
        return 0;
    }

    stats->scheduler_and_qpl_bytes += ct_tree->qpl_pool_bytes;
    if (ct_tree->qpl_pool_size) {
        size_t n = (size_t)ct_tree->qpl_pool_size;
        stats->scheduler_and_qpl_usable_bytes +=
            allocation_usable_size(ct_tree->qpl_job_pool, n * sizeof(qpl_job *)) +
            allocation_usable_size(ct_tree->qpl_job_buffers, n * sizeof(uint8_t *)) +
            allocation_usable_size(ct_tree->qpl_job_free_list, n * sizeof(int));
        for (int i = 0; i < ct_tree->qpl_pool_size; i++)
            stats->scheduler_and_qpl_usable_bytes += allocation_usable_size(
                ct_tree->qpl_job_buffers[i], 0);
    }
    if (!ct_tree->tree) {
        return 0;
    }

    stats->tree_metadata_bytes += sizeof(*ct_tree->tree);
    stats->tree_metadata_usable_bytes += allocation_usable_size(ct_tree->tree, sizeof(*ct_tree->tree));
    pthread_rwlock_rdlock(&ct_tree->rwlock);

    for (int level = 1; level <= ct_tree->tree->level && level < BPLUS_MAX_LEVEL; level++) {
        struct list_head *head = &ct_tree->tree->list[level];
        struct list_head *pos, *next;
        list_for_each_safe(pos, next, head) {
            stats->tree_metadata_bytes += sizeof(struct bplus_non_leaf);
            stats->tree_metadata_usable_bytes += allocation_usable_size(
                list_entry(pos, struct bplus_node, link), sizeof(struct bplus_non_leaf));
        }
    }

    struct list_head *head = &ct_tree->tree->list[0];
    struct list_head *pos, *next;
    list_for_each_safe(pos, next, head) {
        struct compressed_leaf_ref *wrapper = list_entry(pos, struct compressed_leaf_ref, link);
        if (wrapper->type != BPLUS_TREE_LEAF || wrapper->payload == 0) {
            continue;
        }
        struct simple_leaf_node *leaf = (struct simple_leaf_node *)wrapper->payload;
        stats->leaf_metadata_bytes += sizeof(*wrapper) +
                                      sizeof(*leaf) - sizeof(leaf->active);
        stats->active_allocated_bytes += sizeof(leaf->active);
        stats->leaf_metadata_usable_bytes += allocation_usable_size(wrapper, sizeof(*wrapper)) +
            allocation_usable_size(leaf, sizeof(*leaf)) - sizeof(leaf->active);

        pthread_rwlock_rdlock(&leaf->rwlock);
        stats->compressed_requested_bytes += leaf->compressed_capacity;
        stats->compressed_usable_bytes += leaf->compressed_usable;
        if (leaf->subpage_index && leaf->subpage_index != &leaf->inline_index) {
            size_t bytes = (size_t)leaf->num_subpages * sizeof(struct subpage_index_entry);
            stats->leaf_metadata_bytes += bytes;
            stats->leaf_metadata_usable_bytes += allocation_usable_size(leaf->subpage_index, bytes);
        }
        if (leaf->pending) {
            size_t bytes = sizeof(*leaf->pending) + leaf->pending->count * sizeof(struct kv_pair);
            stats->pending_requested_bytes += bytes;
            stats->pending_allocated_bytes +=
                allocation_usable_size(leaf->pending, bytes);
            if (pending_task_state_load(leaf->pending) == PENDING_TASK_FAILED)
                stats->failed_pending_count++;
        }

        struct kv_pair *pairs = NULL;
        size_t count = 0;
        if (compressed_leaf_collect_pairs(ct_tree, leaf, &pairs, &count) != 0) {
            free(pairs);
            pthread_rwlock_unlock(&leaf->rwlock);
            pthread_rwlock_unlock(&ct_tree->rwlock);
            return -1;
        }
        stats->live_kv_bytes += count * sizeof(struct kv_pair);
        stats->leaf_count++;
        stats->fill_histogram[count < 128 ? count : 128]++;
        for (size_t i = 0; visitor && i < count; i++) {
            if (visitor(&pairs[i], context)) {
                free(pairs);
                pthread_rwlock_unlock(&leaf->rwlock);
                pthread_rwlock_unlock(&ct_tree->rwlock);
                return -1;
            }
        }
        free(pairs);
        pthread_rwlock_unlock(&leaf->rwlock);
    }

    pthread_rwlock_unlock(&ct_tree->rwlock);
    return 0;
}

int bplus_tree_compressed_verify(struct bplus_tree_compressed *tree,
                                 int (*visitor)(const struct kv_pair *, void *), void *context)
{
    struct bplus_tree_memory_stats stats = {0};
    if (!visitor) return -1;
    return compressed_memory_stats_collect(tree, &stats, visitor, context);
}

int bplus_tree_compressed_memory_stats(struct bplus_tree_compressed *ct_tree,
                                       struct bplus_tree_memory_stats *stats)
{
    if (!ct_tree || !stats || !ct_tree->initialized) {
        return -1;
    }
    memset(stats, 0, sizeof(*stats));
    if (compressed_memory_stats_collect(ct_tree, stats, NULL, NULL) != 0) {
        return -1;
    }

    if (ct_tree->owns_scheduler && ct_tree->scheduler) {
        struct compaction_scheduler *scheduler = ct_tree->scheduler;
        pthread_mutex_lock(&scheduler->lock);
        stats->scheduler_and_qpl_bytes += sizeof(*scheduler);
#ifdef ZIPCACHE_AGG_EXPERIMENT
        stats->scheduler_and_qpl_bytes += scheduler->worker_stack_bytes;
        stats->scheduler_and_qpl_usable_bytes += allocation_usable_size(scheduler->worker_stack, scheduler->worker_stack_bytes);
#endif
        if (scheduler->codec_slots) {
            size_t bytes = (size_t)scheduler->batch_size * sizeof(*scheduler->codec_slots);
            stats->scheduler_and_qpl_bytes += bytes;
            stats->scheduler_and_qpl_usable_bytes += allocation_usable_size(scheduler->codec_slots, bytes);
        }
        stats->scheduler_and_qpl_bytes +=
            (size_t)scheduler->queue_capacity * sizeof(*scheduler->queue);
        stats->scheduler_and_qpl_bytes +=
            (size_t)scheduler->batch_size *
            (COMPRESSED_LEAF_SIZE + MAX_COMPRESSED_SIZE);
        stats->scheduler_and_qpl_bytes += scheduler->qpl_job_bytes;
        stats->scheduler_and_qpl_bytes +=
            (size_t)scheduler->qpl_job_count *
            (sizeof(*scheduler->qpl_jobs) + sizeof(*scheduler->qpl_job_buffers));
        stats->scheduler_and_qpl_usable_bytes +=
            allocation_usable_size(scheduler, sizeof(*scheduler)) +
            allocation_usable_size(scheduler->queue, (size_t)scheduler->queue_capacity * sizeof(void *)) +
            allocation_usable_size(scheduler->raw_workspaces, (size_t)scheduler->batch_size * COMPRESSED_LEAF_SIZE) +
            allocation_usable_size(scheduler->compressed_workspaces, (size_t)scheduler->batch_size * MAX_COMPRESSED_SIZE) +
            allocation_usable_size(scheduler->qpl_jobs, (size_t)scheduler->qpl_job_count * sizeof(void *)) +
            allocation_usable_size(scheduler->qpl_job_buffers, (size_t)scheduler->qpl_job_count * sizeof(void *));
        for (int i = 0; i < scheduler->qpl_job_count; i++)
            stats->scheduler_and_qpl_usable_bytes += allocation_usable_size(scheduler->qpl_job_buffers[i],
                scheduler->qpl_job_count ? scheduler->qpl_job_bytes / (size_t)scheduler->qpl_job_count : 0);
        pthread_mutex_unlock(&scheduler->lock);
    }
    stats->process_qpl_tls_bytes =
        __atomic_load_n(&qpl_tls_live_bytes, __ATOMIC_RELAXED);
    stats->process_zlib_workspace_bytes = __atomic_load_n(&zlib_live_bytes, __ATOMIC_RELAXED);
#ifdef HAVE_ZSTD
    stats->process_zstd_workspace_bytes = __atomic_load_n(&zstd_live_bytes, __ATOMIC_RELAXED);
#endif
    return 0;
}

int bplus_tree_compressed_backend_info(struct bplus_tree_compressed *tree,
                                      struct bplus_tree_backend_info *info)
{
    if (!tree || !info) return -1;
    memset(info, 0, sizeof(*info));
    info->requested = tree->requested_algo;
    info->effective = tree->config.algo;
    info->library_version = "builtin";
    info->execution_path = "cpu";
    info->explicit_fallbacks = __atomic_load_n(&tree->codec_fallbacks, __ATOMIC_RELAXED);
    if (info->effective == COMPRESS_LZ4) info->library_version = LZ4_versionString();
#ifdef HAVE_ZSTD
    if (info->effective == COMPRESS_ZSTD_EXPERIMENT) info->library_version = ZSTD_versionString();
#endif
#ifdef HAVE_ZLIB
    if (info->effective == COMPRESS_ZLIB_ACCEL) info->library_version = zlibVersion();
#ifdef ZIPCACHE_AGG_EXPERIMENT
    if (info->effective == COMPRESS_ZLIB_ACCEL && split_accel_counter)
        info->execution_path = "zlib-accel-per-call-observed";
#endif
#endif
#ifdef HAVE_QPL
    if (info->effective == COMPRESS_QPL) {
        info->library_version = qpl_get_library_version();
        info->execution_path = tree->config.qpl_path == qpl_path_hardware ? "strict-hardware" :
            tree->config.qpl_path == qpl_path_software ? "software" : "auto-actual-path-unobserved";
#ifdef ZIPCACHE_AGG_EXPERIMENT
        if (tree->split_routes_enabled) info->execution_path = "split-explicit-routes";
#endif
    }
#endif
    if (tree->scheduler && tree->scheduler->contract_mode)
        info->execution_path = "contract-test-real-software-codec";
    if (compressed_tree_is_sharded(tree)) for (int i = 0; i < tree->shard_count; i++) {
        struct bplus_tree_backend_info child;
        bplus_tree_compressed_backend_info(tree->shards[i], &child);
        info->explicit_fallbacks += child.explicit_fallbacks;
    }
    return 0;
}

int bplus_tree_compressed_scheduler_stats(struct bplus_tree_compressed *ct_tree,
                                          struct bplus_tree_scheduler_stats *stats)
{
    if (!ct_tree || !stats || !ct_tree->initialized) {
        return -1;
    }
    memset(stats, 0, sizeof(*stats));
    struct compaction_scheduler *scheduler = ct_tree->scheduler;
    if (!scheduler) {
        return 0;
    }

    pthread_mutex_lock(&scheduler->lock);
    stats->queue_depth = (uint64_t)scheduler->queue_count;
    stats->queue_peak = scheduler->queue_peak;
    stats->batches = scheduler->batches;
    stats->submitted_tasks = scheduler->submitted_tasks;
    stats->completed_tasks = scheduler->completed_tasks;
    stats->failed_tasks = scheduler->failed_tasks;
    stats->retry_count = scheduler->retry_count;
    stats->queue_full_fallbacks = scheduler->queue_full_fallbacks;
    stats->synchronous_fallbacks = scheduler->synchronous_fallbacks;
    stats->split_fallbacks = scheduler->split_fallbacks;
    stats->total_queue_wait_ns = scheduler->total_queue_wait_ns;
    stats->max_queue_wait_ns = scheduler->max_queue_wait_ns;
    stats->submitted_jobs = scheduler->contract_stats.accepted;
    stats->terminal_jobs = scheduler->contract_stats.terminal;
    stats->failed_jobs = scheduler->contract_stats.failures;
    stats->busy_jobs = scheduler->contract_stats.busy;
    stats->outstanding_jobs = scheduler->contract_stats.in_flight;
    stats->decompress_phase_ns = scheduler->decompress_phase_ns;
    stats->merge_phase_ns = scheduler->merge_phase_ns;
    stats->compress_phase_ns = scheduler->compress_phase_ns;
    stats->commit_phase_ns = scheduler->commit_phase_ns;
    stats->ready_at_wait_samples = scheduler->ready_at_wait_samples;
    pthread_mutex_unlock(&scheduler->lock);
    return 0;
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
        struct compressed_leaf_ref *leaf = list_entry(pos, struct compressed_leaf_ref, link);
        if (leaf->type != BPLUS_TREE_LEAF) {
            continue;
        }
        if (leaf->payload == 0) {
            continue;
        }

        leaf_count++;
        struct simple_leaf_node *custom_leaf = (struct simple_leaf_node *)leaf->payload;

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
        if (custom_leaf->pending) {
            landing_buffer_bytes += custom_leaf->pending->count * sizeof(struct kv_pair);
        }

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
#ifndef HAVE_QPL
    if (effective_config.algo == COMPRESS_QPL) {
        if (effective_config.qpl_path != qpl_path_auto) return NULL;
        fprintf(stderr, "ZipCache: QPL unavailable; explicit initialization fallback to LZ4.\n");
        effective_config.algo = COMPRESS_LZ4;
    }
#endif
#ifndef HAVE_ZLIB
    if (effective_config.algo == COMPRESS_ZLIB_ACCEL) return NULL;
#endif
#ifndef HAVE_ZSTD
    if (effective_config.algo == COMPRESS_ZSTD_EXPERIMENT) return NULL;
#endif
#if ZIPCACHE_AGG_LAYOUT > 0
    /* Experimental ordered framing owns its internal blocks; legacy hash
     * subpages remain available in the independently built L0 target. */
    if (effective_config.default_sub_pages != 1) return NULL;
    if (env_int_clamped("BTREE_TEST_CONTRACT_MODE", 0, 0, 5)) {
        fprintf(stderr, "ZipCache aggregation: legacy unframed contract adapter is unavailable for L1-L3.\n");
        return NULL;
    }
#endif

    struct bplus_tree_compressed *ct_tree = calloc(1, sizeof(*ct_tree));
    if (ct_tree == NULL) return NULL;
    ct_tree->requested_algo = config->algo;
    ct_tree->codec_fallbacks = config->algo != effective_config.algo;
#ifdef ZIPCACHE_AGG_EXPERIMENT
    if (split_routes_init(ct_tree, config)) { free(ct_tree); return NULL; }
#endif

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
        const char *profile_env = getenv("BTREE_PROFILE_SUBMISSION");
        ct_tree->submission_profile_enabled =
            profile_env && strcmp(profile_env, "1") == 0;

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
    audit_add(ct_tree->tree, sizeof(*ct_tree->tree));
    if (ct_tree->tree == NULL) {
        free(ct_tree);
        return NULL;
    }
    
    pthread_rwlock_init(&ct_tree->rwlock, NULL);
    pthread_mutex_init(&ct_tree->bg_scan_lock, NULL);
    pthread_mutex_init(&ct_tree->bg_queue_lock, NULL);
    pthread_cond_init(&ct_tree->bg_queue_cond, NULL);

    ct_tree->initialized = 1;
    ct_tree->compression_enabled = 1;
    ct_tree->debug_mode = 0;  // Debug off by default
    ct_tree->config = effective_config;
    const char *profile_env = getenv("BTREE_PROFILE_SUBMISSION");
    ct_tree->submission_profile_enabled =
        profile_env && strcmp(profile_env, "1") == 0;
    configure_background_compaction(ct_tree);

    if (ct_tree->config.algo == COMPRESS_QPL) {
        if (init_qpl(ct_tree) != 0) {
            fprintf(stderr, "Warning: QPL initialization failed, QPL layouts will not be available\n");
            if (ct_tree->config.qpl_path != qpl_path_auto
#ifdef ZIPCACHE_AGG_EXPERIMENT
                || ct_tree->split_routes_enabled
#endif
            ) {
                audit_forget(ct_tree->tree);
                bplus_tree_deinit(ct_tree->tree);
                pthread_rwlock_destroy(&ct_tree->rwlock);
                pthread_mutex_destroy(&ct_tree->bg_scan_lock);
                pthread_mutex_destroy(&ct_tree->bg_queue_lock);
                pthread_cond_destroy(&ct_tree->bg_queue_cond);
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
    int background_requested = env_bool_enabled("BTREE_BG_COMPACTION", 0);
    struct bplus_tree_compressed *tree =
        bplus_tree_compressed_init_internal(order, entries, config, 1);
    if (!tree) {
        return NULL;
    }
    struct compaction_scheduler *scheduler = compaction_scheduler_create(tree);
    if (background_requested && !scheduler &&
        (env_int_clamped("BTREE_TEST_CONTRACT_MODE", 0, 0, 5) ||
#ifdef ZIPCACHE_AGG_EXPERIMENT
         tree->split_routes_enabled || env_int_clamped("BTREE_TEST_SPLIT_CODEC_MODE", 0, 0, 5) ||
#endif
         (tree->config.algo == COMPRESS_QPL && tree->config.qpl_path == qpl_path_hardware))) {
        /* A strict hardware request must never degrade to synchronous or
         * software execution because the batcher's jobs could not start. */
        bplus_tree_compressed_deinit(tree);
        return NULL;
    }
    compaction_scheduler_attach(tree, scheduler, scheduler != NULL);
    return tree;
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
    if (ct_tree->qpl_initialized) {
        return 0; // Already initialized
    }
#ifdef ZIPCACHE_AGG_EXPERIMENT
    if (ct_tree->split_routes_enabled) {
        if (!qpl_tls_job_cache_enabled()) return -1;
        for (int i = 0; i < 3; i++) {
            uint32_t bytes = 0;
            if (qpl_get_job_size(ct_tree->split_routes[i], &bytes) != QPL_STS_OK || !bytes) return -1;
            qpl_job *job = malloc(bytes);
            if (!job) return -1;
            qpl_status rc = qpl_init_job(ct_tree->split_routes[i], job);
            if (rc == QPL_STS_OK) qpl_fini_job(job);
            free(job);
            if (rc != QPL_STS_OK) return -1;
        }
        ct_tree->qpl_initialized = 1;
        return 0;
    }
#endif

    uint32_t job_size = 0;
    qpl_path_t qpl_path = ct_tree->config.qpl_path;
    qpl_status status = qpl_get_job_size(qpl_path, &job_size);
    if (status != QPL_STS_OK || job_size == 0) {
        return -1;
    }

    /* TLS is the default foreground backend; do not also reserve a pool per shard. */
    if (qpl_tls_job_cache_enabled()) {
        uint8_t *validation_buffer = malloc(job_size);
        if (!validation_buffer) {
            return -1;
        }
        qpl_job *validation_job = (qpl_job *)validation_buffer;
        status = qpl_init_job(qpl_path, validation_job);
        if (status == QPL_STS_OK) {
            qpl_fini_job(validation_job);
        }
        free(validation_buffer);
        if (status != QPL_STS_OK) {
            return -1;
        }
        ct_tree->qpl_initialized = 1;
        return 0;
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
    ct_tree->qpl_initialized = 1;
    ct_tree->qpl_pool_bytes =
        (size_t)pool_size * (job_size + sizeof(*jobs) + sizeof(*buffers) + sizeof(*free_list));
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
    ct_tree->qpl_initialized = 0;
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
    ct_tree->qpl_pool_bytes = 0;

    pthread_cond_destroy(&ct_tree->qpl_pool_cond);
    pthread_mutex_destroy(&ct_tree->qpl_pool_lock);
}
