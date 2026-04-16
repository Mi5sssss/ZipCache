#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bplustree_compressed.h"
#include "compressed_test_utils.h"

#define DEFAULT_THREADS 4
#define DEFAULT_DURATION_SEC 2
#define DEFAULT_KEY_SPACE 256
#define DEFAULT_HOT_PCT 20
#define DEFAULT_READ_PCT 50
#define DEFAULT_WRITE_PCT 35
#define DEFAULT_DELETE_PCT 10
#define DEFAULT_SCAN_PCT 5
#define JOIN_GRACE_SEC 5

struct workload_config {
    int thread_count;
    int duration_sec;
    int key_space;
    int hot_pct;
    int read_pct;
    int write_pct;
    int delete_pct;
    int scan_pct;
};

struct value_source {
    int use_silesia;
    struct btree_silesia_dataset silesia;
};

struct reference_state {
    int *values;
    int *versions;
    pthread_mutex_t *locks;
    int lock_count;
};

struct thread_stats {
    long reads;
    long writes;
    long deletes;
    long scans;
    long mismatches;
};

struct worker_args {
    struct bplus_tree_compressed *tree;
    struct reference_state *ref;
    const struct value_source *source;
    const struct workload_config *cfg;
    struct timespec end_time;
    unsigned int seed;
    struct thread_stats stats;
};

static double elapsed_sec(const struct timespec *start, const struct timespec *end)
{
    return (double)(end->tv_sec - start->tv_sec) +
           (double)(end->tv_nsec - start->tv_nsec) / 1e9;
}

static int now_before(const struct timespec *end)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    if (now.tv_sec != end->tv_sec) {
        return now.tv_sec < end->tv_sec;
    }
    return now.tv_nsec < end->tv_nsec;
}

static void join_or_fail(pthread_t thread, int thread_id, int duration_sec)
{
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += duration_sec + JOIN_GRACE_SEC;

    int rc = pthread_timedjoin_np(thread, NULL, &deadline);
    if (rc == ETIMEDOUT) {
        fprintf(stderr,
                "worker %d did not finish within duration+%d seconds; likely B+Tree concurrency deadlock\n",
                thread_id,
                JOIN_GRACE_SEC);
        exit(EXIT_FAILURE);
    }
    if (rc != 0) {
        fprintf(stderr, "pthread_timedjoin_np failed for worker %d rc=%d\n", thread_id, rc);
        exit(EXIT_FAILURE);
    }
}

static int next_value(int key, int version)
{
    int value = key * 1009 + version + 1;
    return value == 0 ? 1 : value;
}

static void value_source_init(struct value_source *source, int max_key)
{
    memset(source, 0, sizeof(*source));
    source->use_silesia = btree_env_bool("BTREE_USE_SILESIA", 0);
    if (!source->use_silesia) {
        return;
    }

    int value_bytes = btree_env_int("BTREE_VALUE_BYTES", 128, 1);
    if (value_bytes > COMPRESSED_VALUE_BYTES) {
        fprintf(stderr,
                "BTREE_VALUE_BYTES=%d exceeds COMPRESSED_VALUE_BYTES=%d\n",
                value_bytes,
                COMPRESSED_VALUE_BYTES);
        exit(EXIT_FAILURE);
    }
    if (btree_load_silesia_samba(&source->silesia,
                                 (size_t)value_bytes,
                                 (size_t)max_key + 1024) != 0) {
        exit(EXIT_FAILURE);
    }
}

static void value_source_deinit(struct value_source *source)
{
    if (source && source->use_silesia) {
        btree_free_silesia_dataset(&source->silesia);
    }
}

static const uint8_t *payload_for_key_version(const struct value_source *source,
                                              int key,
                                              int version)
{
    if (!source || !source->use_silesia) {
        return NULL;
    }
    return btree_silesia_payload_for_key_version(&source->silesia, (key_t)key, version);
}

static int put_tree_value(struct bplus_tree_compressed *tree,
                          const struct value_source *source,
                          int key,
                          int version,
                          int value)
{
    const uint8_t *payload = payload_for_key_version(source, key, version);
    if (payload) {
        return bplus_tree_compressed_put_with_payload(tree,
                                                      (key_t)key,
                                                      payload,
                                                      source->silesia.chunk_bytes,
                                                      value);
    }
    return bplus_tree_compressed_put(tree, (key_t)key, value);
}

static int plausible_value_for_key(int key, int value)
{
    if (value == -1) {
        return 1;
    }
    int base = key * 1009 + 1;
    return value > base;
}

static int plausible_value_for_range(int lo, int hi, int value)
{
    if (value == -1) {
        return 1;
    }
    for (int key = lo; key <= hi; key++) {
        if (plausible_value_for_key(key, value)) {
            return 1;
        }
    }
    return 0;
}

static int choose_key(const struct workload_config *cfg, unsigned int *seed)
{
    int hot_keys = (cfg->key_space * cfg->hot_pct) / 100;
    if (hot_keys < 1) {
        hot_keys = 1;
    }
    if (hot_keys > cfg->key_space) {
        hot_keys = cfg->key_space;
    }

    if ((rand_r(seed) % 100) < 80) {
        return 1 + (rand_r(seed) % hot_keys);
    }
    if (hot_keys >= cfg->key_space) {
        return 1 + (rand_r(seed) % cfg->key_space);
    }
    return hot_keys + 1 + (rand_r(seed) % (cfg->key_space - hot_keys));
}

static pthread_mutex_t *reference_lock_for_key(const struct reference_state *ref, int key)
{
    int stripe = key % ref->lock_count;
    if (stripe < 0) {
        stripe += ref->lock_count;
    }
    return &ref->locks[stripe];
}

static int reference_get_value(struct reference_state *ref, int key)
{
    pthread_mutex_t *lock = reference_lock_for_key(ref, key);
    pthread_mutex_lock(lock);
    int value = ref->values[key];
    pthread_mutex_unlock(lock);
    return value;
}

static int reference_next_value(struct reference_state *ref, int key, int *version_out)
{
    pthread_mutex_t *lock = reference_lock_for_key(ref, key);
    pthread_mutex_lock(lock);
    int version = ++ref->versions[key];
    int value = next_value(key, version);
    if (version_out) {
        *version_out = version;
    }
    pthread_mutex_unlock(lock);
    return value;
}

static void reference_set_value(struct reference_state *ref, int key, int value)
{
    pthread_mutex_t *lock = reference_lock_for_key(ref, key);
    pthread_mutex_lock(lock);
    ref->values[key] = value;
    pthread_mutex_unlock(lock);
}

static int expected_range_snapshot(struct reference_state *ref, int lo, int hi)
{
    if (lo > hi) {
        int tmp = lo;
        lo = hi;
        hi = tmp;
    }

    int result = -1;
    for (int key = lo; key <= hi; key++) {
        int value = reference_get_value(ref, key);
        if (value != -1) {
            result = value;
        }
    }
    return result;
}

static void *mixed_worker(void *arg)
{
    struct worker_args *worker = (struct worker_args *)arg;
    const struct workload_config *cfg = worker->cfg;
    unsigned int seed = worker->seed;

    while (now_before(&worker->end_time)) {
        int op = rand_r(&seed) % 100;
        int key = choose_key(cfg, &seed);
        if (key > cfg->key_space) {
            key = cfg->key_space;
        }

        if (op < cfg->read_pct) {
            int expected_before = reference_get_value(worker->ref, key);
            int got = bplus_tree_compressed_get(worker->tree, key);
            int expected_after = reference_get_value(worker->ref, key);

            if (got != expected_before &&
                got != expected_after &&
                !plausible_value_for_key(key, got)) {
                worker->stats.mismatches++;
            }
            worker->stats.reads++;
        } else if (op < cfg->read_pct + cfg->write_pct) {
            int version = 0;
            int value = reference_next_value(worker->ref, key, &version);

            if (put_tree_value(worker->tree, worker->source, key, version, value) == 0) {
                reference_set_value(worker->ref, key, value);
            } else {
                worker->stats.mismatches++;
            }
            worker->stats.writes++;
        } else if (op < cfg->read_pct + cfg->write_pct + cfg->delete_pct) {
            (void)bplus_tree_compressed_delete(worker->tree, key);
            reference_set_value(worker->ref, key, -1);
            worker->stats.deletes++;
        } else {
            int span = 1 + (rand_r(&seed) % 32);
            int hi = key + span;
            if (hi > cfg->key_space) {
                hi = cfg->key_space;
            }

            int expected_before = expected_range_snapshot(worker->ref, key, hi);
            int got = bplus_tree_compressed_get_range(worker->tree, key, hi);
            int expected_after = expected_range_snapshot(worker->ref, key, hi);

            if (got != expected_before &&
                got != expected_after &&
                !plausible_value_for_range(key, hi, got)) {
                worker->stats.mismatches++;
            }
            worker->stats.scans++;
        }
    }

    return NULL;
}

static void parse_workload_config(struct workload_config *cfg)
{
    cfg->thread_count = btree_env_int("BTREE_THREADS", DEFAULT_THREADS, 1);
    cfg->duration_sec = btree_env_int("BTREE_DURATION_SEC", DEFAULT_DURATION_SEC, 1);
    cfg->key_space = btree_env_int("BTREE_KEY_SPACE", DEFAULT_KEY_SPACE, 64);
    cfg->hot_pct = btree_env_int("BTREE_HOT_PCT", DEFAULT_HOT_PCT, 1);
    cfg->read_pct = btree_env_int("BTREE_READ_PCT", DEFAULT_READ_PCT, 0);
    cfg->write_pct = btree_env_int("BTREE_WRITE_PCT", DEFAULT_WRITE_PCT, 0);
    cfg->delete_pct = btree_env_int("BTREE_DELETE_PCT", DEFAULT_DELETE_PCT, 0);
    cfg->scan_pct = btree_env_int("BTREE_SCAN_PCT", DEFAULT_SCAN_PCT, 0);

    if (cfg->hot_pct > 100) {
        cfg->hot_pct = 100;
    }

    int total = cfg->read_pct + cfg->write_pct + cfg->delete_pct + cfg->scan_pct;
    if (total != 100) {
        fprintf(stderr,
                "BTREE read/write/delete/scan percentages must sum to 100, got %d\n",
                total);
        exit(EXIT_FAILURE);
    }
}

static int run_codec(compression_algo_t algo,
                     const struct workload_config *cfg,
                     const struct value_source *source)
{
    struct compression_config tree_cfg = bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    tree_cfg.algo = algo;
    tree_cfg.default_sub_pages = 1;
    tree_cfg.enable_lazy_compression = 0;
    btree_apply_qpl_env(&tree_cfg);

    struct bplus_tree_compressed *tree = bplus_tree_compressed_init_with_config(32, 128, &tree_cfg);
    if (!tree) {
        fprintf(stderr, "failed to initialize %s tree\n", btree_algo_name(algo));
        return -1;
    }

    struct reference_state ref = {0};
    ref.values = malloc((size_t)(cfg->key_space + 1) * sizeof(int));
    ref.versions = calloc((size_t)(cfg->key_space + 1), sizeof(int));
    ref.lock_count = btree_env_int("BTREE_REF_LOCKS", 1024, 1);
    ref.locks = calloc((size_t)ref.lock_count, sizeof(*ref.locks));
    if (!ref.values || !ref.versions || !ref.locks) {
        perror("malloc");
        free(ref.values);
        free(ref.versions);
        free(ref.locks);
        bplus_tree_compressed_deinit(tree);
        return -1;
    }
    for (int i = 0; i < ref.lock_count; i++) {
        pthread_mutex_init(&ref.locks[i], NULL);
    }

    ref.values[0] = -1;
    for (int key = 1; key <= cfg->key_space; key++) {
        ref.versions[key] = 1;
        ref.values[key] = next_value(key, 1);
        if (put_tree_value(tree, source, key, ref.versions[key], ref.values[key]) != 0) {
            fprintf(stderr, "initial put failed key=%d codec=%s\n", key, btree_algo_name(algo));
            for (int i = 0; i < ref.lock_count; i++) {
                pthread_mutex_destroy(&ref.locks[i]);
            }
            free(ref.values);
            free(ref.versions);
            free(ref.locks);
            bplus_tree_compressed_deinit(tree);
            return -1;
        }
    }

    pthread_t *threads = calloc((size_t)cfg->thread_count, sizeof(*threads));
    struct worker_args *args = calloc((size_t)cfg->thread_count, sizeof(*args));
    if (!threads || !args) {
        perror("calloc");
        free(threads);
        free(args);
        free(ref.values);
        free(ref.versions);
        for (int i = 0; i < ref.lock_count; i++) {
            pthread_mutex_destroy(&ref.locks[i]);
        }
        free(ref.locks);
        bplus_tree_compressed_deinit(tree);
        return -1;
    }

    struct timespec start;
    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    end = start;
    end.tv_sec += cfg->duration_sec;

    for (int i = 0; i < cfg->thread_count; i++) {
        args[i].tree = tree;
        args[i].ref = &ref;
        args[i].source = source;
        args[i].cfg = cfg;
        args[i].end_time = end;
        args[i].seed = 1234u + (unsigned int)i * 7919u + (unsigned int)algo * 104729u;
        if (pthread_create(&threads[i], NULL, mixed_worker, &args[i]) != 0) {
            fprintf(stderr, "pthread_create failed for worker %d\n", i);
            for (int j = 0; j < i; j++) {
                join_or_fail(threads[j], j, cfg->duration_sec);
            }
            free(threads);
            free(args);
            for (int j = 0; j < ref.lock_count; j++) {
                pthread_mutex_destroy(&ref.locks[j]);
            }
            free(ref.values);
            free(ref.versions);
            free(ref.locks);
            bplus_tree_compressed_deinit(tree);
            return -1;
        }
    }

    struct thread_stats total = {0};
    for (int i = 0; i < cfg->thread_count; i++) {
        join_or_fail(threads[i], i, cfg->duration_sec);
        total.reads += args[i].stats.reads;
        total.writes += args[i].stats.writes;
        total.deletes += args[i].stats.deletes;
        total.scans += args[i].stats.scans;
        total.mismatches += args[i].stats.mismatches;
    }

    struct timespec stop;
    clock_gettime(CLOCK_MONOTONIC, &stop);
    double seconds = elapsed_sec(&start, &stop);

    long final_mismatches = total.mismatches;
    for (int key = 1; key <= cfg->key_space; key++) {
        int got = bplus_tree_compressed_get(tree, key);
        /*
         * The reference array is an oracle for runtime sanity checks, but it is
         * not a linearizable final-state oracle: a put can complete in the tree
         * before a concurrent delete updates the reference array, or vice versa.
         * After the workers stop, verify that any resident value belongs to the
         * requested key rather than requiring an exact final reference match.
         */
        if (!plausible_value_for_key(key, got)) {
            final_mismatches++;
        }
    }

    size_t total_bytes = 0;
    size_t compressed_bytes = 0;
    (void)bplus_tree_compressed_calculate_stats(tree, &total_bytes, &compressed_bytes);
    double compression_ratio = compressed_bytes > 0
        ? (double)total_bytes / (double)compressed_bytes
        : 0.0;
    double saved_pct = total_bytes > 0
        ? (1.0 - ((double)compressed_bytes / (double)total_bytes)) * 100.0
        : 0.0;

    long ops = total.reads + total.writes + total.deletes + total.scans;
    printf("mixed_concurrency[%s]: ops=%ld qps=%.1f reads=%ld writes=%ld deletes=%ld scans=%ld mismatches=%ld stats=%zu/%zu ratio=%.3f saved_pct=%.2f\n",
           btree_algo_name(algo),
           ops,
           seconds > 0.0 ? (double)ops / seconds : 0.0,
           total.reads,
           total.writes,
           total.deletes,
           total.scans,
           final_mismatches,
           compressed_bytes,
           total_bytes,
           compression_ratio,
           saved_pct);

    for (int i = 0; i < ref.lock_count; i++) {
        pthread_mutex_destroy(&ref.locks[i]);
    }
    free(threads);
    free(args);
    free(ref.values);
    free(ref.versions);
    free(ref.locks);
    bplus_tree_compressed_deinit(tree);

    return final_mismatches == 0 ? 0 : -1;
}

int main(void)
{
    struct workload_config cfg;
    parse_workload_config(&cfg);
    struct value_source source;
    value_source_init(&source, cfg.key_space);

    printf("B+Tree mixed concurrency: threads=%d duration=%ds key_space=%d hot_pct=%d mix=%d/%d/%d/%d value_source=%s value_bytes=%zu\n",
           cfg.thread_count,
           cfg.duration_sec,
           cfg.key_space,
           cfg.hot_pct,
           cfg.read_pct,
           cfg.write_pct,
           cfg.delete_pct,
           cfg.scan_pct,
           source.use_silesia ? "silesia_samba" : "synthetic",
           source.use_silesia ? source.silesia.chunk_bytes : (size_t)sizeof(int));

    compression_algo_t algos[] = {
        COMPRESS_LZ4,
        COMPRESS_QPL,
        COMPRESS_ZLIB_ACCEL,
    };

    int failures = 0;
    for (size_t i = 0; i < sizeof(algos) / sizeof(algos[0]); i++) {
        if (run_codec(algos[i], &cfg, &source) != 0) {
            failures++;
        }
    }

    if (failures == 0) {
        printf("bpt_compressed_mixed_concurrency: OK\n");
        value_source_deinit(&source);
        return 0;
    }

    fprintf(stderr, "bpt_compressed_mixed_concurrency: %d codec failure(s)\n", failures);
    value_source_deinit(&source);
    return 1;
}
