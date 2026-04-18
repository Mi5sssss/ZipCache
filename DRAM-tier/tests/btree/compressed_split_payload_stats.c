#define _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef HAVE_ZLIB
#include <zlib.h>
#endif

#include "bplustree_compressed.h"
#include "compressed_test_utils.h"

#define NUM_KEYS 256
#define PAYLOAD_BYTES COMPRESSED_VALUE_BYTES

struct kv_pair_view {
    key_t key;
    int stored_value;
    uint8_t payload[COMPRESSED_VALUE_BYTES];
};

static int stored_value_for_key(int key, int round)
{
    return key * 17 + round * 100000 + 1;
}

static void require_true(int condition, const char *message)
{
    if (!condition) {
        fprintf(stderr, "%s\n", message);
        exit(EXIT_FAILURE);
    }
}

static void fill_payload(int key, int round, uint8_t payload[PAYLOAD_BYTES])
{
    for (int i = 0; i < PAYLOAD_BYTES; i++) {
        payload[i] = (uint8_t)((key * 31 + round * 17 + i) & 0xff);
    }
}

static int expected_range_value(const int expected[NUM_KEYS], int lo, int hi)
{
    if (lo > hi) {
        int tmp = lo;
        lo = hi;
        hi = tmp;
    }

    int result = -1;
    for (int key = lo; key <= hi; key++) {
        if (expected[key] != -1) {
            result = expected[key];
        }
    }
    return result;
}

static int range_contains_value(const int expected[NUM_KEYS], int lo, int hi, int value)
{
    if (lo > hi) {
        int tmp = lo;
        lo = hi;
        hi = tmp;
    }

    if (value == -1) {
        return expected_range_value(expected, lo, hi) == -1;
    }

    for (int key = lo; key <= hi; key++) {
        if (expected[key] == value) {
            return 1;
        }
    }
    return 0;
}

static int decompress_bucket(struct bplus_tree_compressed *tree,
                             struct simple_leaf_node *leaf,
                             const struct subpage_index_entry *entry,
                             uint8_t *dst,
                             int dst_capacity)
{
    if (leaf->compression_algo == COMPRESS_QPL && tree->qpl_pool_size > 0) {
        int job_index = -1;
        qpl_job *job = NULL;

        pthread_mutex_lock(&tree->qpl_pool_lock);
        while (tree->qpl_free_count == 0 && tree->qpl_pool_size > 0) {
            pthread_cond_wait(&tree->qpl_pool_cond, &tree->qpl_pool_lock);
        }
        if (tree->qpl_free_count > 0) {
            tree->qpl_free_count--;
            job_index = tree->qpl_job_free_list[tree->qpl_free_count];
            job = tree->qpl_job_pool[job_index];
        }
        pthread_mutex_unlock(&tree->qpl_pool_lock);

        if (job) {
            job->op = qpl_op_decompress;
            job->next_in_ptr = (uint8_t *)leaf->compressed_data + entry->offset;
            job->available_in = entry->length;
            job->next_out_ptr = dst;
            job->available_out = (uint32_t)dst_capacity;
            job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
            qpl_status status = qpl_execute_job(job);
            uint32_t produced = job->total_out;

            pthread_mutex_lock(&tree->qpl_pool_lock);
            if (job_index >= 0 && tree->qpl_free_count < tree->qpl_pool_size) {
                tree->qpl_job_free_list[tree->qpl_free_count] = job_index;
                tree->qpl_free_count++;
                pthread_cond_signal(&tree->qpl_pool_cond);
            }
            pthread_mutex_unlock(&tree->qpl_pool_lock);

            if (status == QPL_STS_OK && produced > 0 && produced <= (uint32_t)dst_capacity) {
                return (int)produced;
            }
        }
    }

#ifdef HAVE_ZLIB
    if (leaf->compression_algo == COMPRESS_ZLIB_ACCEL) {
        uLongf produced = (uLongf)dst_capacity;
        int status = uncompress((Bytef *)dst,
                                &produced,
                                (const Bytef *)leaf->compressed_data + entry->offset,
                                (uLong)entry->length);
        if (status == Z_OK && produced > 0 && produced <= (uLongf)dst_capacity) {
            return (int)produced;
        }
    }
#endif

    return LZ4_decompress_safe((const char *)leaf->compressed_data + entry->offset,
                               (char *)dst,
                               (int)entry->length,
                               dst_capacity);
}

static void verify_pair(int key,
                        int stored_value,
                        const uint8_t payload[PAYLOAD_BYTES],
                        const int expected[NUM_KEYS],
                        const int rounds[NUM_KEYS],
                        int seen[NUM_KEYS])
{
    if (key <= 0 || key >= NUM_KEYS || expected[key] == -1) {
        return;
    }

    if (stored_value != expected[key]) {
        fprintf(stderr, "stored value mismatch key=%d got=%d expected=%d\n",
                key, stored_value, expected[key]);
        exit(EXIT_FAILURE);
    }

    uint8_t expected_payload[PAYLOAD_BYTES];
    fill_payload(key, rounds[key], expected_payload);
    if (memcmp(payload, expected_payload, PAYLOAD_BYTES) != 0) {
        fprintf(stderr, "payload mismatch key=%d round=%d\n", key, rounds[key]);
        exit(EXIT_FAILURE);
    }
    seen[key] = 1;
}

static void verify_leaf_payloads(struct bplus_tree_compressed *tree,
                                 const int expected[NUM_KEYS],
                                 const int rounds[NUM_KEYS])
{
    int seen[NUM_KEYS] = {0};

    pthread_rwlock_rdlock(&tree->rwlock);
    struct list_head *head = &tree->tree->list[0];
    struct list_head *pos, *n;
    list_for_each_safe(pos, n, head) {
        struct bplus_leaf *leaf = list_entry(pos, struct bplus_leaf, link);
        if (leaf->type != 0 || leaf->data[0] == 0) {
            continue;
        }

        struct simple_leaf_node *custom_leaf = (struct simple_leaf_node *)leaf->data[0];
        pthread_rwlock_rdlock(&custom_leaf->rwlock);

        struct kv_pair_view *landing = (struct kv_pair_view *)custom_leaf->landing_buffer;
        struct kv_pair_view *landing_end = (struct kv_pair_view *)(custom_leaf->landing_buffer + LANDING_BUFFER_BYTES);
        for (; landing < landing_end; ++landing) {
            verify_pair(landing->key,
                        landing->stored_value,
                        landing->payload,
                        expected,
                        rounds,
                        seen);
        }

        if (custom_leaf->is_compressed &&
            custom_leaf->compressed_data &&
            custom_leaf->subpage_index &&
            custom_leaf->num_subpages > 0) {
            int sub_page_size = COMPRESSED_LEAF_SIZE / custom_leaf->num_subpages;
            if (sub_page_size <= 0) {
                sub_page_size = COMPRESSED_LEAF_SIZE;
            }

            uint8_t *buffer = malloc((size_t)sub_page_size);
            if (!buffer) {
                perror("malloc");
                exit(EXIT_FAILURE);
            }

            for (int bucket = 0; bucket < custom_leaf->num_subpages; bucket++) {
                struct subpage_index_entry *entry = &custom_leaf->subpage_index[bucket];
                if (entry->length == 0) {
                    continue;
                }

                int decompressed = decompress_bucket(tree, custom_leaf, entry, buffer, sub_page_size);
                if (decompressed <= 0) {
                    fprintf(stderr, "failed to decompress bucket %d while verifying payloads\n", bucket);
                    exit(EXIT_FAILURE);
                }

                struct kv_pair_view *pairs = (struct kv_pair_view *)buffer;
                int pair_count = decompressed / (int)sizeof(struct kv_pair_view);
                for (int i = 0; i < pair_count; i++) {
                    verify_pair(pairs[i].key,
                                pairs[i].stored_value,
                                pairs[i].payload,
                                expected,
                                rounds,
                                seen);
                }
            }
            free(buffer);
        }

        pthread_rwlock_unlock(&custom_leaf->rwlock);
    }
    pthread_rwlock_unlock(&tree->rwlock);

    for (int key = 1; key < NUM_KEYS; key++) {
        if (expected[key] != -1 && !seen[key]) {
            fprintf(stderr, "key %d expected but not found in leaf payload scan\n", key);
            exit(EXIT_FAILURE);
        }
    }
}

static void validate_stats(struct bplus_tree_compressed *tree)
{
    size_t incremental_total = 0;
    size_t incremental_compressed = 0;
    size_t calculated_total = 0;
    size_t calculated_compressed = 0;

    require_true(bplus_tree_compressed_stats(tree, &incremental_total, &incremental_compressed) == 0,
                 "bplus_tree_compressed_stats failed");
    require_true(bplus_tree_compressed_calculate_stats(tree, &calculated_total, &calculated_compressed) == 0,
                 "bplus_tree_compressed_calculate_stats failed");

    if (calculated_total == 0 || calculated_compressed == 0) {
        fprintf(stderr, "calculated stats are empty: total=%zu compressed=%zu\n",
                calculated_total, calculated_compressed);
        exit(EXIT_FAILURE);
    }

    if (incremental_total > calculated_total || incremental_compressed > calculated_compressed) {
        fprintf(stderr,
                "incremental stats exceed calculated stats: inc=%zu/%zu calc=%zu/%zu\n",
                incremental_compressed,
                incremental_total,
                calculated_compressed,
                calculated_total);
        exit(EXIT_FAILURE);
    }
}

static void run_codec(compression_algo_t algo)
{
    struct compression_config cfg = bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    cfg.algo = algo;
    cfg.default_sub_pages = 1;
    cfg.enable_lazy_compression = 0;
    btree_apply_qpl_env(&cfg);

    struct bplus_tree_compressed *tree = bplus_tree_compressed_init_with_config(16, 64, &cfg);
    if (!tree) {
        fprintf(stderr, "failed to initialize %s tree\n", btree_algo_name(algo));
        exit(EXIT_FAILURE);
    }

    int expected[NUM_KEYS];
    int rounds[NUM_KEYS];
    for (int i = 0; i < NUM_KEYS; i++) {
        expected[i] = -1;
        rounds[i] = 0;
    }

    for (int key = 1; key < NUM_KEYS; key++) {
        uint8_t payload[PAYLOAD_BYTES];
        rounds[key] = 1;
        expected[key] = stored_value_for_key(key, rounds[key]);
        fill_payload(key, rounds[key], payload);
        require_true(bplus_tree_compressed_put_with_payload(tree,
                                                            key,
                                                            payload,
                                                            PAYLOAD_BYTES,
                                                            expected[key]) == 0,
                     "initial put_with_payload failed");
    }

    for (int key = 1; key < NUM_KEYS; key++) {
        require_true(bplus_tree_compressed_get(tree, key) == expected[key],
                     "post-insert get mismatch");
    }

    for (int key = 7; key < NUM_KEYS; key += 7) {
        require_true(bplus_tree_compressed_delete(tree, key) == 0,
                     "delete failed");
        expected[key] = -1;
    }

    for (int key = 7; key < NUM_KEYS; key += 14) {
        uint8_t payload[PAYLOAD_BYTES];
        rounds[key] = 2;
        expected[key] = stored_value_for_key(key, rounds[key]);
        fill_payload(key, rounds[key], payload);
        require_true(bplus_tree_compressed_put_with_payload(tree,
                                                            key,
                                                            payload,
                                                            PAYLOAD_BYTES,
                                                            expected[key]) == 0,
                     "reinsert put_with_payload failed");
    }

    for (int lo = 1; lo < NUM_KEYS; lo += 37) {
        int hi = lo + 53;
        if (hi >= NUM_KEYS) {
            hi = NUM_KEYS - 1;
        }
        int range_value = bplus_tree_compressed_get_range(tree, lo, hi);
        require_true(range_contains_value(expected, lo, hi, range_value),
                     "range returned value outside expected live range");
    }

    for (int key = 1; key < NUM_KEYS; key++) {
        int got = bplus_tree_compressed_get(tree, key);
        if (got != expected[key]) {
            fprintf(stderr,
                    "final get mismatch codec=%s key=%d expected=%d got=%d\n",
                    btree_algo_name(algo),
                    key,
                    expected[key],
                    got);
            exit(EXIT_FAILURE);
        }
    }

    verify_leaf_payloads(tree, expected, rounds);
    validate_stats(tree);

    bplus_tree_compressed_deinit(tree);
    printf("split_payload_stats[%s]: OK\n", btree_algo_name(algo));
}

int main(void)
{
    run_codec(COMPRESS_LZ4);
    run_codec(COMPRESS_QPL);
    run_codec(COMPRESS_ZLIB_ACCEL);
    printf("bpt_compressed_split_payload_stats: OK\n");
    return 0;
}
