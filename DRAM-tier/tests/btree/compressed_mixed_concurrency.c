#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
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

struct latency_samples {
    double *read_ns;
    double *write_ns;
    int read_count;
    int write_count;
    int max_samples;
};

struct worker_args {
    struct bplus_tree_compressed *tree;
    struct reference_state *ref;
    const struct value_source *source;
    const struct workload_config *cfg;
    struct timespec end_time;
    unsigned int seed;
    struct thread_stats stats;
    struct latency_samples latency;
};

static double elapsed_sec(const struct timespec *start, const struct timespec *end)
{
    return (double)(end->tv_sec - start->tv_sec) +
           (double)(end->tv_nsec - start->tv_nsec) / 1e9;
}

static double elapsed_ns(const struct timespec *start, const struct timespec *end)
{
    return (double)(end->tv_sec - start->tv_sec) * 1e9 +
           (double)(end->tv_nsec - start->tv_nsec);
}

static double timeval_diff_sec(const struct timeval *start, const struct timeval *end)
{
    return (double)(end->tv_sec - start->tv_sec) +
           (double)(end->tv_usec - start->tv_usec) / 1e6;
}

static void monotonic_now(struct timespec *ts)
{
    clock_gettime(CLOCK_MONOTONIC, ts);
}

static int cmp_double(const void *a, const void *b)
{
    double da = *(const double *)a;
    double db = *(const double *)b;
    if (da < db) {
        return -1;
    }
    if (da > db) {
        return 1;
    }
    return 0;
}

static double percentile_sorted(const double *sorted, int count, double pct)
{
    if (!sorted || count <= 0) {
        return 0.0;
    }
    double rank = (pct / 100.0) * (count - 1);
    int lower = (int)rank;
    int upper = lower + 1;
    double frac = rank - lower;
    if (upper >= count) {
        return sorted[count - 1];
    }
    return sorted[lower] + (sorted[upper] - sorted[lower]) * frac;
}

static void print_latency_summary(const char *prefix, double *samples, int count)
{
    if (!samples || count <= 0) {
        printf(" %s_p50_us=NA %s_p99_us=NA %s_p999_us=NA", prefix, prefix, prefix);
        return;
    }
    qsort(samples, (size_t)count, sizeof(double), cmp_double);
    printf(" %s_p50_us=%.3f %s_p99_us=%.3f %s_p999_us=%.3f",
           prefix,
           percentile_sorted(samples, count, 50.0) / 1000.0,
           prefix,
           percentile_sorted(samples, count, 99.0) / 1000.0,
           prefix,
           percentile_sorted(samples, count, 99.9) / 1000.0);
}

static void record_latency(struct latency_samples *latency, int is_write, double ns)
{
    if (!latency || latency->max_samples <= 0) {
        return;
    }
    if (is_write) {
        if (latency->write_count < latency->max_samples) {
            latency->write_ns[latency->write_count++] = ns;
        }
        return;
    }
    if (latency->read_count < latency->max_samples) {
        latency->read_ns[latency->read_count++] = ns;
    }
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
            struct timespec op_start;
            struct timespec op_end;
            monotonic_now(&op_start);
            int got = bplus_tree_compressed_get(worker->tree, key);
            monotonic_now(&op_end);
            record_latency(&worker->latency, 0, elapsed_ns(&op_start, &op_end));
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

            struct timespec op_start;
            struct timespec op_end;
            monotonic_now(&op_start);
            int put_rc = put_tree_value(worker->tree, worker->source, key, version, value);
            monotonic_now(&op_end);
            record_latency(&worker->latency, 1, elapsed_ns(&op_start, &op_end));

            if (put_rc == 0) {
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

    int measure_latency = btree_env_bool("BTREE_MEASURE_LATENCY", 0);
    int samples_per_thread = btree_env_int("BTREE_LATENCY_SAMPLES_PER_THREAD", 8192, 1);
    if (!measure_latency) {
        samples_per_thread = 0;
    }

    struct timespec start;
    struct timespec end;
    struct rusage usage_start;
    struct rusage usage_stop;
    getrusage(RUSAGE_SELF, &usage_start);
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
        args[i].latency.max_samples = samples_per_thread;
        if (samples_per_thread > 0) {
            args[i].latency.read_ns = calloc((size_t)samples_per_thread, sizeof(double));
            args[i].latency.write_ns = calloc((size_t)samples_per_thread, sizeof(double));
            if (!args[i].latency.read_ns || !args[i].latency.write_ns) {
                perror("calloc latency samples");
                for (int j = 0; j < i; j++) {
                    free(args[j].latency.read_ns);
                    free(args[j].latency.write_ns);
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
        if (pthread_create(&threads[i], NULL, mixed_worker, &args[i]) != 0) {
            fprintf(stderr, "pthread_create failed for worker %d\n", i);
            for (int j = 0; j < i; j++) {
                join_or_fail(threads[j], j, cfg->duration_sec);
                free(args[j].latency.read_ns);
                free(args[j].latency.write_ns);
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

    int total_read_samples = 0;
    int total_write_samples = 0;
    for (int i = 0; i < cfg->thread_count; i++) {
        total_read_samples += args[i].latency.read_count;
        total_write_samples += args[i].latency.write_count;
    }

    double *read_samples = NULL;
    double *write_samples = NULL;
    if (total_read_samples > 0) {
        read_samples = malloc((size_t)total_read_samples * sizeof(double));
    }
    if (total_write_samples > 0) {
        write_samples = malloc((size_t)total_write_samples * sizeof(double));
    }
    int read_pos = 0;
    int write_pos = 0;
    for (int i = 0; i < cfg->thread_count; i++) {
        if (args[i].latency.read_count > 0 && read_samples) {
            memcpy(read_samples + read_pos,
                   args[i].latency.read_ns,
                   (size_t)args[i].latency.read_count * sizeof(double));
            read_pos += args[i].latency.read_count;
        }
        if (args[i].latency.write_count > 0 && write_samples) {
            memcpy(write_samples + write_pos,
                   args[i].latency.write_ns,
                   (size_t)args[i].latency.write_count * sizeof(double));
            write_pos += args[i].latency.write_count;
        }
    }

    struct timespec stop;
    clock_gettime(CLOCK_MONOTONIC, &stop);
    getrusage(RUSAGE_SELF, &usage_stop);
    double seconds = elapsed_sec(&start, &stop);
    double bench_user_sec = timeval_diff_sec(&usage_start.ru_utime, &usage_stop.ru_utime);
    double bench_sys_sec = timeval_diff_sec(&usage_start.ru_stime, &usage_stop.ru_stime);
    double bench_cpu_cores = seconds > 0.0
        ? (bench_user_sec + bench_sys_sec) / seconds
        : 0.0;
    double bench_cpu_pct = bench_cpu_cores * 100.0;

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
    uint64_t bg_passes = 0;
    uint64_t bg_compactions = 0;
    uint64_t bg_trylock_misses = 0;
    uint64_t bg_skipped = 0;
    uint64_t bg_errors = 0;
    (void)bplus_tree_compressed_bg_stats(tree,
                                         &bg_passes,
                                         &bg_compactions,
                                         &bg_trylock_misses,
                                         &bg_skipped,
                                         &bg_errors);
    uint64_t fg_landing_full = 0;
    uint64_t fg_sync_compactions = 0;
    uint64_t fg_sync_compaction_errors = 0;
    uint64_t fg_split_fallbacks = 0;
    uint64_t bg_enqueue_attempts = 0;
    uint64_t bg_enqueued = 0;
    uint64_t bg_enqueue_duplicates = 0;
    uint64_t bg_queue_full = 0;
    uint64_t bg_queue_pops = 0;
    (void)bplus_tree_compressed_compaction_stats(tree,
                                                 &fg_landing_full,
                                                 &fg_sync_compactions,
                                                 &fg_sync_compaction_errors,
                                                 &fg_split_fallbacks,
                                                 &bg_enqueue_attempts,
                                                 &bg_enqueued,
                                                 &bg_enqueue_duplicates,
                                                 &bg_queue_full,
                                                 &bg_queue_pops);
    uint64_t qpl_compress_calls = 0;
    uint64_t qpl_decompress_calls = 0;
    uint64_t qpl_tls_jobs = 0;
    uint64_t qpl_pool_jobs = 0;
    uint64_t qpl_errors = 0;
    uint64_t zlib_compress_calls = 0;
    uint64_t zlib_decompress_calls = 0;
    uint64_t zlib_stream_reuses = 0;
    uint64_t zlib_stream_inits = 0;
    uint64_t zlib_errors = 0;
    (void)bplus_tree_compressed_codec_stats(tree,
                                            &qpl_compress_calls,
                                            &qpl_decompress_calls,
                                            &qpl_tls_jobs,
                                            &qpl_pool_jobs,
                                            &qpl_errors,
                                            &zlib_compress_calls,
                                            &zlib_decompress_calls,
                                            &zlib_stream_reuses,
                                            &zlib_stream_inits,
                                            &zlib_errors);

    long ops = total.reads + total.writes + total.deletes + total.scans;
    printf("mixed_concurrency[%s]: ops=%ld qps=%.1f reads=%ld writes=%ld deletes=%ld scans=%ld mismatches=%ld stats=%zu/%zu ratio=%.3f saved_pct=%.2f bench_wall_sec=%.6f bench_user_sec=%.6f bench_sys_sec=%.6f bench_cpu_pct=%.2f bench_cpu_cores=%.3f bg_passes=%llu bg_compactions=%llu bg_trylock_misses=%llu bg_skipped=%llu bg_errors=%llu fg_landing_full=%llu fg_sync_compactions=%llu fg_sync_compaction_errors=%llu fg_split_fallbacks=%llu bg_enqueue_attempts=%llu bg_enqueued=%llu bg_enqueue_duplicates=%llu bg_queue_full=%llu bg_queue_pops=%llu qpl_compress_calls=%llu qpl_decompress_calls=%llu qpl_tls_jobs=%llu qpl_pool_jobs=%llu qpl_errors=%llu zlib_compress_calls=%llu zlib_decompress_calls=%llu zlib_stream_reuses=%llu zlib_stream_inits=%llu zlib_errors=%llu",
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
           saved_pct,
           seconds,
           bench_user_sec,
           bench_sys_sec,
           bench_cpu_pct,
           bench_cpu_cores,
           (unsigned long long)bg_passes,
           (unsigned long long)bg_compactions,
           (unsigned long long)bg_trylock_misses,
           (unsigned long long)bg_skipped,
           (unsigned long long)bg_errors,
           (unsigned long long)fg_landing_full,
           (unsigned long long)fg_sync_compactions,
           (unsigned long long)fg_sync_compaction_errors,
           (unsigned long long)fg_split_fallbacks,
           (unsigned long long)bg_enqueue_attempts,
           (unsigned long long)bg_enqueued,
           (unsigned long long)bg_enqueue_duplicates,
           (unsigned long long)bg_queue_full,
           (unsigned long long)bg_queue_pops,
           (unsigned long long)qpl_compress_calls,
           (unsigned long long)qpl_decompress_calls,
           (unsigned long long)qpl_tls_jobs,
           (unsigned long long)qpl_pool_jobs,
           (unsigned long long)qpl_errors,
           (unsigned long long)zlib_compress_calls,
           (unsigned long long)zlib_decompress_calls,
           (unsigned long long)zlib_stream_reuses,
           (unsigned long long)zlib_stream_inits,
           (unsigned long long)zlib_errors);
    if (measure_latency) {
        print_latency_summary("read", read_samples, total_read_samples);
        print_latency_summary("write", write_samples, total_write_samples);
        printf(" latency_read_samples=%d latency_write_samples=%d",
               total_read_samples,
               total_write_samples);
    }
    printf("\n");

    for (int i = 0; i < ref.lock_count; i++) {
        pthread_mutex_destroy(&ref.locks[i]);
    }
    for (int i = 0; i < cfg->thread_count; i++) {
        free(args[i].latency.read_ns);
        free(args[i].latency.write_ns);
    }
    free(read_samples);
    free(write_samples);
    free(threads);
    free(args);
    free(ref.values);
    free(ref.versions);
    free(ref.locks);
    bplus_tree_compressed_deinit(tree);

    return final_mismatches == 0 ? 0 : -1;
}

static int codec_filter_allows(compression_algo_t algo)
{
    const char *filter = getenv("BTREE_CODEC_FILTER");
    if (!filter || filter[0] == '\0' || strcmp(filter, "all") == 0) {
        return 1;
    }
    if (strcmp(filter, btree_algo_name(algo)) == 0) {
        return 1;
    }
    if (algo == COMPRESS_ZLIB_ACCEL && strcmp(filter, "zlib") == 0) {
        return 1;
    }
    return 0;
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
    int selected = 0;
    for (size_t i = 0; i < sizeof(algos) / sizeof(algos[0]); i++) {
        if (!codec_filter_allows(algos[i])) {
            continue;
        }
        selected++;
        if (run_codec(algos[i], &cfg, &source) != 0) {
            failures++;
        }
    }

    if (selected == 0) {
        fprintf(stderr,
                "BTREE_CODEC_FILTER selected no codecs; use all, lz4, qpl, zlib, or zlib_accel\n");
        value_source_deinit(&source);
        return 1;
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
