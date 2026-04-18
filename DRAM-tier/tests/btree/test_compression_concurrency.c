#define _GNU_SOURCE

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "bplustree_compressed.h"
#include "compressed_test_utils.h"

#define DEFAULT_KEYS_PER_THREAD 50
#define DEFAULT_THREADS 4
#define THREAD_JOIN_TIMEOUT_SEC 5

struct thread_args {
    struct bplus_tree_compressed *tree;
    int thread_id;
    int start_key;
    int num_keys;
    int *success_count;
    pthread_mutex_t *stats_lock;
};

static int expected_value_for_key(int key)
{
    return key + 1000;
}

static void *worker_thread(void *arg)
{
    struct thread_args *args = (struct thread_args *)arg;
    int local_success = 0;
    int read_success = 0;

    printf("Thread %d: inserting %d keys from key %d\n",
           args->thread_id, args->num_keys, args->start_key);

    for (int i = 0; i < args->num_keys; i++) {
        key_t key = args->start_key + i;
        int ret = bplus_tree_compressed_put(args->tree, key, expected_value_for_key(key));
        if (ret == 0) {
            local_success++;
        }
    }

    for (int i = 0; i < args->num_keys; i++) {
        key_t key = args->start_key + i;
        int value = bplus_tree_compressed_get(args->tree, key);
        if (value == expected_value_for_key(key)) {
            read_success++;
        } else {
            printf("Thread %d: read mismatch key=%d got=%d expected=%d\n",
                   args->thread_id, key, value, expected_value_for_key(key));
        }
    }

    printf("Thread %d: inserts %d/%d reads %d/%d\n",
           args->thread_id,
           local_success,
           args->num_keys,
           read_success,
           args->num_keys);

    pthread_mutex_lock(args->stats_lock);
    *args->success_count += (local_success == args->num_keys &&
                             read_success == args->num_keys) ? 1 : 0;
    pthread_mutex_unlock(args->stats_lock);

    return NULL;
}

static void join_or_fail(pthread_t thread, int thread_id)
{
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += THREAD_JOIN_TIMEOUT_SEC;

    int rc = pthread_timedjoin_np(thread, NULL, &deadline);
    if (rc == ETIMEDOUT) {
        fprintf(stderr,
                "thread %d did not finish within %d seconds; likely B+Tree concurrency deadlock\n",
                thread_id,
                THREAD_JOIN_TIMEOUT_SEC);
        exit(EXIT_FAILURE);
    }
    if (rc != 0) {
        fprintf(stderr, "pthread_timedjoin_np failed for thread %d rc=%d\n", thread_id, rc);
        exit(EXIT_FAILURE);
    }
}

static void configure_codec(struct compression_config *cfg, compression_algo_t algo)
{
    *cfg = bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    cfg->algo = algo;
    cfg->default_sub_pages = 1;
    cfg->enable_lazy_compression = 0;
    btree_apply_qpl_env(cfg);
}

static int test_single_thread(compression_algo_t algo)
{
    const char *algo_name = btree_algo_name(algo);
    printf("\n=== single-thread %s ===\n", algo_name);

    struct compression_config cfg;
    configure_codec(&cfg, algo);
    cfg.default_sub_pages = 1;

    struct bplus_tree_compressed *tree = bplus_tree_compressed_init_with_config(16, 64, &cfg);
    if (!tree) {
        fprintf(stderr, "failed to initialize tree for %s\n", algo_name);
        return -1;
    }

    const int num_keys = 300;
    for (int key = 1; key <= num_keys; key++) {
        int ret = bplus_tree_compressed_put(tree, key, expected_value_for_key(key));
        if (ret != 0) {
            fprintf(stderr, "insert failed for key %d [%s]\n", key, algo_name);
            bplus_tree_compressed_deinit(tree);
            return -1;
        }
    }

    int verify_success = 0;
    for (int key = 1; key <= num_keys; key++) {
        int value = bplus_tree_compressed_get(tree, key);
        if (value == expected_value_for_key(key)) {
            verify_success++;
        } else {
            fprintf(stderr, "verification failed key=%d got=%d expected=%d [%s]\n",
                    key, value, expected_value_for_key(key), algo_name);
        }
    }

    int range_value = bplus_tree_compressed_get_range(tree, 100, 200);
    if (range_value != expected_value_for_key(200)) {
        fprintf(stderr, "range mismatch got=%d expected=%d [%s]\n",
                range_value, expected_value_for_key(200), algo_name);
        bplus_tree_compressed_deinit(tree);
        return -1;
    }

    size_t total_size = 0;
    size_t compressed_size = 0;
    assert(bplus_tree_compressed_stats(tree, &total_size, &compressed_size) == 0);
    printf("single-thread %s stats: %zu -> %zu bytes\n",
           algo_name, total_size, compressed_size);

    bplus_tree_compressed_deinit(tree);
    return verify_success == num_keys ? 0 : -1;
}

static int test_multi_thread(compression_algo_t algo, int thread_count, int keys_per_thread)
{
    const char *algo_name = btree_algo_name(algo);
    printf("\n=== multi-thread %s (%d threads, %d keys/thread) ===\n",
           algo_name, thread_count, keys_per_thread);

    struct compression_config cfg;
    configure_codec(&cfg, algo);

    struct bplus_tree_compressed *tree = bplus_tree_compressed_init_with_config(32, 128, &cfg);
    if (!tree) {
        fprintf(stderr, "failed to initialize tree for %s\n", algo_name);
        return -1;
    }

    pthread_t *threads = calloc((size_t)thread_count, sizeof(*threads));
    struct thread_args *args = calloc((size_t)thread_count, sizeof(*args));
    if (!threads || !args) {
        perror("calloc");
        free(threads);
        free(args);
        bplus_tree_compressed_deinit(tree);
        return -1;
    }

    int success_count = 0;
    pthread_mutex_t stats_lock;
    pthread_mutex_init(&stats_lock, NULL);

    for (int i = 0; i < thread_count; i++) {
        args[i].tree = tree;
        args[i].thread_id = i;
        args[i].start_key = i * keys_per_thread + 1;
        args[i].num_keys = keys_per_thread;
        args[i].success_count = &success_count;
        args[i].stats_lock = &stats_lock;

        if (pthread_create(&threads[i], NULL, worker_thread, &args[i]) != 0) {
            fprintf(stderr, "failed to create thread %d\n", i);
            thread_count = i;
            break;
        }
    }

    for (int i = 0; i < thread_count; i++) {
        join_or_fail(threads[i], i);
    }

    int random_checks = 100;
    int random_success = 0;
    int total_keys = thread_count * keys_per_thread;
    for (int i = 0; i < random_checks; i++) {
        key_t key = 1 + (rand() % total_keys);
        int value = bplus_tree_compressed_get(tree, key);
        if (value == expected_value_for_key(key)) {
            random_success++;
        }
    }

    printf("multi-thread %s: thread_success=%d/%d random=%d/%d\n",
           algo_name,
           success_count,
           thread_count,
           random_success,
           random_checks);

    pthread_mutex_destroy(&stats_lock);
    free(threads);
    free(args);
    bplus_tree_compressed_deinit(tree);

    return (success_count == thread_count &&
            random_success == random_checks) ? 0 : -1;
}

int main(void)
{
    srand((unsigned int)time(NULL));

    int threads = btree_env_int("BTREE_THREADS", DEFAULT_THREADS, 1);
    int keys_per_thread = btree_env_int("BTREE_KEYS_PER_THREAD", DEFAULT_KEYS_PER_THREAD, 1);

    printf("Compression concurrency suite: threads=%d keys_per_thread=%d\n",
           threads, keys_per_thread);

    compression_algo_t algos[] = {
        COMPRESS_LZ4,
        COMPRESS_QPL,
        COMPRESS_ZLIB_ACCEL,
    };

    int failed = 0;
    for (size_t i = 0; i < sizeof(algos) / sizeof(algos[0]); i++) {
        if (test_single_thread(algos[i]) != 0) {
            failed++;
        }
        if (test_multi_thread(algos[i], threads, keys_per_thread) != 0) {
            failed++;
        }
    }

    if (failed == 0) {
        printf("test_compression_concurrency: OK\n");
        return 0;
    }

    fprintf(stderr, "test_compression_concurrency: %d failure(s)\n", failed);
    return 1;
}
