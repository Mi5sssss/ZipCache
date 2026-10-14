#define _GNU_SOURCE
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bplustree_compressed.h"
#include "compressed_test_utils.h"

struct bench_config {
    int threads;
    int ops_per_thread;
    int keys_per_thread;
    int read_pct;
    int subpages;
    int latency_samples;
    compression_algo_t codec;
};

struct start_gate {
    volatile int ready;
    volatile int go;
};

struct worker {
    struct bplus_tree_compressed *tree;
    const struct bench_config *config;
    struct start_gate *gate;
    int worker_id;
    uint32_t seed;
    uint32_t *versions;
    uint64_t reads;
    uint64_t writes;
    uint64_t mismatches;
    double *read_latency_ns;
    double *write_latency_ns;
    int read_latency_count;
    int write_latency_count;
};

static int env_int(const char *name, int default_value, int minimum, int maximum)
{
    const char *value = getenv(name);
    if (!value || !*value) {
        return default_value;
    }
    char *end = NULL;
    long parsed = strtol(value, &end, 10);
    if (end == value || *end != '\0' || parsed < minimum || parsed > maximum) {
        fprintf(stderr, "Invalid %s=%s; expected %d..%d\n",
                name,
                value,
                minimum,
                maximum);
        exit(EXIT_FAILURE);
    }
    return (int)parsed;
}

static uint32_t next_random(uint32_t *state)
{
    uint32_t value = *state;
    value ^= value << 13;
    value ^= value >> 17;
    value ^= value << 5;
    *state = value;
    return value;
}

static int stored_value_for(int key, uint32_t version)
{
    uint32_t value = ((uint32_t)key * 104729u) ^ (version * 8191u);
    value &= 0x7fffffffu;
    return value == 0 ? 1 : (int)value;
}

static void payload_for(uint8_t payload[COMPRESSED_VALUE_BYTES], int key, uint32_t version)
{
    uint8_t a = (uint8_t)((uint32_t)key * 17u + version * 13u);
    uint8_t b = (uint8_t)((uint32_t)key * 29u + version * 7u);
    for (int i = 0; i < COMPRESSED_VALUE_BYTES; i++) {
        payload[i] = (i / 32) % 2 == 0 ? a : b;
    }
}

static double elapsed_seconds(const struct timespec *start, const struct timespec *end)
{
    return (double)(end->tv_sec - start->tv_sec) +
           (double)(end->tv_nsec - start->tv_nsec) / 1000000000.0;
}

static double elapsed_nanoseconds(const struct timespec *start, const struct timespec *end)
{
    return (double)(end->tv_sec - start->tv_sec) * 1000000000.0 +
           (double)(end->tv_nsec - start->tv_nsec);
}

static int compare_double(const void *left, const void *right)
{
    double a = *(const double *)left;
    double b = *(const double *)right;
    return (a > b) - (a < b);
}

static double percentile_us(double *samples, int count, double percentile)
{
    if (!samples || count <= 0) {
        return 0.0;
    }
    qsort(samples, (size_t)count, sizeof(*samples), compare_double);
    double position = ((double)count - 1.0) * percentile;
    int index = (int)position;
    if (index < 0) {
        index = 0;
    }
    if (index >= count) {
        index = count - 1;
    }
    return samples[index] / 1000.0;
}

static void *worker_main(void *opaque)
{
    struct worker *worker = opaque;
    const struct bench_config *config = worker->config;
    int first_key = worker->worker_id * config->keys_per_thread + 1;
    uint8_t payload[COMPRESSED_VALUE_BYTES];

    __atomic_add_fetch(&worker->gate->ready, 1, __ATOMIC_RELEASE);
    while (!__atomic_load_n(&worker->gate->go, __ATOMIC_ACQUIRE)) {
        sched_yield();
    }

    for (int operation = 0; operation < config->ops_per_thread; operation++) {
        uint32_t choice = next_random(&worker->seed);
        int key_index = (int)(next_random(&worker->seed) % (uint32_t)config->keys_per_thread);
        int key = first_key + key_index;
        struct timespec op_start;
        struct timespec op_end;
        clock_gettime(CLOCK_MONOTONIC, &op_start);

        if ((int)(choice % 100u) < config->read_pct) {
            int expected = stored_value_for(key, worker->versions[key_index]);
            int observed = bplus_tree_compressed_get(worker->tree, key);
            clock_gettime(CLOCK_MONOTONIC, &op_end);
            if (worker->read_latency_count < config->latency_samples) {
                worker->read_latency_ns[worker->read_latency_count++] =
                    elapsed_nanoseconds(&op_start, &op_end);
            }
            if (observed != expected) {
                worker->mismatches++;
            }
            worker->reads++;
        } else {
            uint32_t version = ++worker->versions[key_index];
            int stored_value = stored_value_for(key, version);
            payload_for(payload, key, version);
            if (bplus_tree_compressed_put_with_payload(worker->tree,
                                                       key,
                                                       payload,
                                                       sizeof(payload),
                                                       stored_value) != 0) {
                worker->mismatches++;
            }
            clock_gettime(CLOCK_MONOTONIC, &op_end);
            if (worker->write_latency_count < config->latency_samples) {
                worker->write_latency_ns[worker->write_latency_count++] =
                    elapsed_nanoseconds(&op_start, &op_end);
            }
            worker->writes++;
        }
    }
    return NULL;
}

static compression_algo_t parse_codec(void)
{
    const char *codec = getenv("BTREE_SUBMISSION_CODEC");
    if (!codec || !*codec || strcmp(codec, "copy") == 0) {
        return COMPRESS_COPY;
    }
    if (strcmp(codec, "lz4") == 0) {
        return COMPRESS_LZ4;
    }
    if (strcmp(codec, "qpl") == 0) {
        return COMPRESS_QPL;
    }
    fprintf(stderr, "Invalid BTREE_SUBMISSION_CODEC=%s; use copy, lz4, or qpl\n", codec);
    exit(EXIT_FAILURE);
}

static const char *codec_name(compression_algo_t codec)
{
    if (codec == COMPRESS_COPY) {
        return "copy";
    }
    return codec == COMPRESS_QPL ? "qpl" : "lz4";
}

int main(void)
{
    struct bench_config config = {
        .threads = env_int("BTREE_SUBMISSION_THREADS", 8, 1, 128),
        .ops_per_thread = env_int("BTREE_SUBMISSION_OPS_PER_THREAD", 50000, 1, 100000000),
        .keys_per_thread = env_int("BTREE_SUBMISSION_KEYS_PER_THREAD", 512, 32, 1000000),
        .read_pct = env_int("BTREE_SUBMISSION_READ_PCT", 20, 0, 100),
        .subpages = env_int("BTREE_SUBMISSION_SUBPAGES", 1, 1, 16),
        .latency_samples = env_int("BTREE_SUBMISSION_LATENCY_SAMPLES", 8192, 1, 1000000),
        .codec = parse_codec(),
    };

    if (!getenv("BTREE_PROFILE_SUBMISSION")) {
        setenv("BTREE_PROFILE_SUBMISSION", "1", 0);
    }

    struct compression_config tree_config =
        bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    tree_config.algo = config.codec;
    tree_config.default_sub_pages = config.subpages;
    tree_config.enable_lazy_compression = 0;
    btree_apply_qpl_env(&tree_config);

    struct bplus_tree_compressed *tree =
        bplus_tree_compressed_init_with_config(32, 128, &tree_config);
    if (!tree) {
        fprintf(stderr, "Unable to create submission-shape tree\n");
        return EXIT_FAILURE;
    }

    int total_keys = config.threads * config.keys_per_thread;
    uint8_t payload[COMPRESSED_VALUE_BYTES];
    for (int key = 1; key <= total_keys; key++) {
        payload_for(payload, key, 1);
        if (bplus_tree_compressed_put_with_payload(tree,
                                                   key,
                                                   payload,
                                                   sizeof(payload),
                                                   stored_value_for(key, 1)) != 0) {
            fprintf(stderr, "Warmup insert failed for key=%d\n", key);
            bplus_tree_compressed_deinit(tree);
            return EXIT_FAILURE;
        }
    }

    if (bplus_tree_compressed_drain_background(tree) != 0) {
        fprintf(stderr, "Unable to drain warmup compactions\n");
        bplus_tree_compressed_deinit(tree);
        return EXIT_FAILURE;
    }

    uint64_t compactions_before = 0;
    (void)bplus_tree_compressed_compaction_stats(tree,
                                                 NULL,
                                                 &compactions_before,
                                                 NULL,
                                                 NULL,
                                                 NULL,
                                                 NULL,
                                                 NULL,
                                                 NULL,
                                                 NULL);
    bplus_tree_compressed_submission_reset(tree);

    pthread_t *threads = calloc((size_t)config.threads, sizeof(*threads));
    struct worker *workers = calloc((size_t)config.threads, sizeof(*workers));
    if (!threads || !workers) {
        perror("calloc");
        free(threads);
        free(workers);
        bplus_tree_compressed_deinit(tree);
        return EXIT_FAILURE;
    }

    struct start_gate gate = {0};
    for (int i = 0; i < config.threads; i++) {
        workers[i].tree = tree;
        workers[i].config = &config;
        workers[i].gate = &gate;
        workers[i].worker_id = i;
        workers[i].seed = 0x9e3779b9u ^ ((uint32_t)i + 1u) * 7919u;
        workers[i].versions = malloc((size_t)config.keys_per_thread * sizeof(*workers[i].versions));
        workers[i].read_latency_ns =
            calloc((size_t)config.latency_samples, sizeof(*workers[i].read_latency_ns));
        workers[i].write_latency_ns =
            calloc((size_t)config.latency_samples, sizeof(*workers[i].write_latency_ns));
        if (!workers[i].versions || !workers[i].read_latency_ns || !workers[i].write_latency_ns) {
            perror("malloc worker state");
            return EXIT_FAILURE;
        }
        for (int key_index = 0; key_index < config.keys_per_thread; key_index++) {
            workers[i].versions[key_index] = 1;
        }
        if (pthread_create(&threads[i], NULL, worker_main, &workers[i]) != 0) {
            fprintf(stderr, "pthread_create failed for worker %d\n", i);
            return EXIT_FAILURE;
        }
    }

    while (__atomic_load_n(&gate.ready, __ATOMIC_ACQUIRE) != config.threads) {
        sched_yield();
    }
    struct timespec start;
    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    __atomic_store_n(&gate.go, 1, __ATOMIC_RELEASE);

    uint64_t reads = 0;
    uint64_t writes = 0;
    uint64_t mismatches = 0;
    for (int i = 0; i < config.threads; i++) {
        pthread_join(threads[i], NULL);
        reads += workers[i].reads;
        writes += workers[i].writes;
        mismatches += workers[i].mismatches;
    }
    clock_gettime(CLOCK_MONOTONIC, &end);

    if (bplus_tree_compressed_drain_background(tree) != 0) {
        fprintf(stderr, "Unable to drain measured compactions\n");
        return EXIT_FAILURE;
    }

    struct bplus_tree_submission_stats stats;
    if (bplus_tree_compressed_submission_stats(tree, &stats) != 0) {
        fprintf(stderr, "Unable to read submission stats\n");
        return EXIT_FAILURE;
    }

    uint64_t compactions_after = 0;
    (void)bplus_tree_compressed_compaction_stats(tree,
                                                 NULL,
                                                 &compactions_after,
                                                 NULL,
                                                 NULL,
                                                 NULL,
                                                 NULL,
                                                 NULL,
                                                 NULL,
                                                 NULL);
    uint64_t compactions = compactions_after - compactions_before;
    uint64_t operations = reads + writes;
    uint64_t jobs = stats.compress_calls + stats.decompress_calls;
    uint64_t codec_input_bytes =
        stats.compress_input_bytes + stats.decompress_input_bytes;
    uint64_t logical_bytes = operations * (uint64_t)COMPRESSED_VALUE_BYTES;
    size_t uncompressed_tree_bytes = 0;
    size_t stored_tree_bytes = 0;
    (void)bplus_tree_compressed_calculate_stats(tree,
                                                &uncompressed_tree_bytes,
                                                &stored_tree_bytes);
    double saved_pct = uncompressed_tree_bytes > 0
        ? 100.0 * (1.0 - (double)stored_tree_bytes / (double)uncompressed_tree_bytes)
        : 0.0;
    double seconds = elapsed_seconds(&start, &end);

    int read_sample_count = 0;
    int write_sample_count = 0;
    for (int i = 0; i < config.threads; i++) {
        read_sample_count += workers[i].read_latency_count;
        write_sample_count += workers[i].write_latency_count;
    }
    double *read_samples = read_sample_count > 0
        ? calloc((size_t)read_sample_count, sizeof(*read_samples)) : NULL;
    double *write_samples = write_sample_count > 0
        ? calloc((size_t)write_sample_count, sizeof(*write_samples)) : NULL;
    if ((read_sample_count > 0 && !read_samples) ||
        (write_sample_count > 0 && !write_samples)) {
        fprintf(stderr, "Unable to allocate combined latency samples\n");
        return EXIT_FAILURE;
    }
    int read_position = 0;
    int write_position = 0;
    for (int i = 0; i < config.threads; i++) {
        if (workers[i].read_latency_count > 0) {
            memcpy(read_samples + read_position,
                   workers[i].read_latency_ns,
                   (size_t)workers[i].read_latency_count * sizeof(*read_samples));
        }
        read_position += workers[i].read_latency_count;
        if (workers[i].write_latency_count > 0) {
            memcpy(write_samples + write_position,
                   workers[i].write_latency_ns,
                   (size_t)workers[i].write_latency_count * sizeof(*write_samples));
        }
        write_position += workers[i].write_latency_count;
    }
    double read_p50_us = percentile_us(read_samples, read_sample_count, 0.50);
    double read_p99_us = percentile_us(read_samples, read_sample_count, 0.99);
    double read_p999_us = percentile_us(read_samples, read_sample_count, 0.999);
    double write_p50_us = percentile_us(write_samples, write_sample_count, 0.50);
    double write_p99_us = percentile_us(write_samples, write_sample_count, 0.99);
    double write_p999_us = percentile_us(write_samples, write_sample_count, 0.999);

    struct bplus_tree_scheduler_stats scheduler_stats;
    memset(&scheduler_stats, 0, sizeof(scheduler_stats));
    (void)bplus_tree_compressed_scheduler_stats(tree, &scheduler_stats);
    struct bplus_tree_memory_stats memory_stats;
    memset(&memory_stats, 0, sizeof(memory_stats));
    (void)bplus_tree_compressed_memory_stats(tree, &memory_stats);
    size_t resident_bytes = memory_stats.tree_metadata_bytes +
                            memory_stats.leaf_metadata_bytes +
                            memory_stats.compressed_usable_bytes +
                            memory_stats.active_allocated_bytes +
                            memory_stats.pending_allocated_bytes +
                            memory_stats.scheduler_and_qpl_bytes;
    double average_batch = scheduler_stats.batches > 0
        ? (double)scheduler_stats.submitted_tasks / (double)scheduler_stats.batches
        : 0.0;
    double average_queue_wait_us = scheduler_stats.submitted_tasks > 0
        ? (double)scheduler_stats.total_queue_wait_ns /
          (double)scheduler_stats.submitted_tasks / 1000.0
        : 0.0;

    printf("submission_shape: codec=%s threads=%d subpages=%d ops=%llu reads=%llu writes=%llu "
           "mismatches=%llu seconds=%.6f qps=%.1f compress_calls=%llu decompress_calls=%llu "
           "jobs_per_op=%.6f codec_input_bytes=%llu traffic_amp=%.3f "
           "calls_under_write_lock=%llu under_write_lock_pct=%.2f "
           "sync_compactions=%llu compactions_per_write=%.6f observed_peak_codec_calls=%llu "
           "read_p50_us=%.3f read_p99_us=%.3f read_p999_us=%.3f "
           "write_p50_us=%.3f write_p99_us=%.3f write_p999_us=%.3f "
           "sched_queue_peak=%llu sched_batches=%llu sched_avg_batch=%.3f "
           "sched_submitted=%llu sched_completed=%llu sched_failed=%llu sched_retries=%llu "
           "sched_queue_full=%llu sched_sync_fallbacks=%llu sched_split_fallbacks=%llu "
           "sched_avg_wait_us=%.3f sched_max_wait_us=%.3f resident_bytes=%zu\n",
           codec_name(config.codec),
           config.threads,
           config.subpages,
           (unsigned long long)operations,
           (unsigned long long)reads,
           (unsigned long long)writes,
           (unsigned long long)mismatches,
           seconds,
           seconds > 0.0 ? (double)operations / seconds : 0.0,
           (unsigned long long)stats.compress_calls,
           (unsigned long long)stats.decompress_calls,
           operations > 0 ? (double)jobs / (double)operations : 0.0,
           (unsigned long long)codec_input_bytes,
           logical_bytes > 0 ? (double)codec_input_bytes / (double)logical_bytes : 0.0,
           (unsigned long long)stats.calls_under_write_lock,
           jobs > 0 ? 100.0 * (double)stats.calls_under_write_lock / (double)jobs : 0.0,
           (unsigned long long)compactions,
           writes > 0 ? (double)compactions / (double)writes : 0.0,
           (unsigned long long)stats.peak_inflight,
           read_p50_us,
           read_p99_us,
           read_p999_us,
           write_p50_us,
           write_p99_us,
           write_p999_us,
           (unsigned long long)scheduler_stats.queue_peak,
           (unsigned long long)scheduler_stats.batches,
           average_batch,
           (unsigned long long)scheduler_stats.submitted_tasks,
           (unsigned long long)scheduler_stats.completed_tasks,
           (unsigned long long)scheduler_stats.failed_tasks,
           (unsigned long long)scheduler_stats.retry_count,
           (unsigned long long)scheduler_stats.queue_full_fallbacks,
           (unsigned long long)scheduler_stats.synchronous_fallbacks,
           (unsigned long long)scheduler_stats.split_fallbacks,
           average_queue_wait_us,
           (double)scheduler_stats.max_queue_wait_ns / 1000.0,
           resident_bytes);
    printf("submission_shape_memory: codec=%s uncompressed_bytes=%zu stored_bytes=%zu saved_pct=%.2f live_kv_bytes=%zu tree_metadata_bytes=%zu leaf_metadata_bytes=%zu compressed_requested_bytes=%zu compressed_usable_bytes=%zu active_bytes=%zu pending_bytes=%zu scheduler_qpl_bytes=%zu resident_bytes=%zu\n",
           codec_name(config.codec),
           uncompressed_tree_bytes,
           stored_tree_bytes,
           saved_pct,
           memory_stats.live_kv_bytes,
           memory_stats.tree_metadata_bytes,
           memory_stats.leaf_metadata_bytes,
           memory_stats.compressed_requested_bytes,
           memory_stats.compressed_usable_bytes,
           memory_stats.active_allocated_bytes,
           memory_stats.pending_allocated_bytes,
           memory_stats.scheduler_and_qpl_bytes,
           resident_bytes);

    for (int i = 0; i < config.threads; i++) {
        free(workers[i].versions);
        free(workers[i].read_latency_ns);
        free(workers[i].write_latency_ns);
    }
    free(read_samples);
    free(write_samples);
    free(threads);
    free(workers);
    bplus_tree_compressed_deinit(tree);
    return mismatches == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
