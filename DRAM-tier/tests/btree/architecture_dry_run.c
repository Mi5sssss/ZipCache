/* Small, deterministic architecture probes. No production layout is changed. */
#include <inttypes.h>
#include <stddef.h>
#include <sys/resource.h>
#include <time.h>
#include "compressed_test_utils.h"

static void require(int ok, const char *message)
{
    if (!ok) { fprintf(stderr, "architecture_dry_run: %s\n", message); exit(1); }
}

static uint32_t random_next(uint32_t *state)
{
    *state ^= *state << 13; *state ^= *state >> 17; *state ^= *state << 5;
    return *state;
}

static double now_ns(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec * 1e9 + t.tv_nsec;
}

static double cpu_ns(void)
{
    struct rusage u;
    require(getrusage(RUSAGE_SELF, &u) == 0, "getrusage failed");
    return (u.ru_utime.tv_sec + u.ru_stime.tv_sec) * 1e9 +
           (u.ru_utime.tv_usec + u.ru_stime.tv_usec) * 1e3;
}

static int compare_double(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

/* Storage projection only: existing tree routines cannot use this wrapper. */
struct projected_wrapper {
    int type, parent_key_idx;
    struct bplus_non_leaf *parent;
    struct list_head link;
    int entries;
    key_t first_key;
    value_t custom_data;
};

static void packing_probe(const struct btree_silesia_dataset *data)
{
    size_t count = data->chunk_count / 30 * 30;
    uint32_t *order = malloc(count * sizeof(*order));
    struct kv_pair *records = calloc(count, sizeof(*records));
    require(order && records, "packing allocation failed");
    size_t fixed = sizeof(struct compressed_leaf_ref) + sizeof(struct simple_leaf_node);
    size_t saving = 0; /* Compact wrapper is now real; no second projected saving. */
    printf("DRYRUN {\"kind\":\"layout\",\"record\":%zu,\"wrapper\":%zu,"
           "\"projected_wrapper\":%zu,\"leaf_fixed\":%zu,\"active\":%zu,"
           "\"rwlock\":%zu,\"records\":%zu}\n", sizeof(struct kv_pair),
           sizeof(struct compressed_leaf_ref), sizeof(struct compressed_leaf_ref), fixed,
           (size_t)LANDING_BUFFER_BYTES, sizeof(pthread_rwlock_t), count);
    for (int control = 0; control < 2; control++) {
        uint32_t state = 0x98765431u;
        for (size_t i = 0; i < count; i++) {
            records[i].key = (key_t)i + 1;
            records[i].stored_value = (int)i + 1;
            for (size_t j = 0; j < COMPRESSED_VALUE_BYTES; j++)
                records[i].payload[j] = control ? (uint8_t)random_next(&state) :
                    data->data[i * COMPRESSED_VALUE_BYTES + j];
        }
        for (int shuffled = 0; shuffled < 2; shuffled++) {
            for (size_t i = 0; i < count; i++) order[i] = (uint32_t)i;
            state = 0x12345678u;
            if (shuffled) for (size_t i = count - 1; i > 0; i--) {
                size_t j = random_next(&state) % (i + 1);
                uint32_t t = order[i]; order[i] = order[j]; order[j] = t;
            }
            for (int fill = 15; fill <= 30; fill += 15) {
                size_t compressed = 0, blocks = count / (size_t)fill;
                for (size_t b = 0; b < blocks; b++) {
                    char raw[COMPRESSED_LEAF_SIZE] = {0};
                    char decoded[COMPRESSED_LEAF_SIZE];
                    char encoded[LZ4_COMPRESSBOUND(COMPRESSED_LEAF_SIZE)];
                    for (int j = 0; j < fill; j++)
                        memcpy(raw + j * sizeof(struct kv_pair),
                               &records[order[b * (size_t)fill + j]], sizeof(struct kv_pair));
                    int n = LZ4_compress_default(raw, encoded, sizeof(raw), sizeof(encoded));
                    require(n > 0, "LZ4 packing failed");
                    require(LZ4_decompress_safe(encoded, decoded, n, sizeof(decoded)) == sizeof(raw)
                            && memcmp(raw, decoded, sizeof(raw)) == 0, "packing roundtrip mismatch");
                    compressed += (size_t)n;
                }
                size_t useful = count * sizeof(struct kv_pair);
                printf("DRYRUN {\"kind\":\"packing\",\"data\":\"%s\",\"order\":\"%s\","
                       "\"fill\":%d,\"blocks\":%zu,\"useful\":%zu,\"compressed\":%zu,"
                       "\"codec_ratio\":%.6f,\"padded_ratio\":%.6f,\"current_projection\":%zu,"
                       "\"lean_projection\":%zu,\"mismatches\":0}\n",
                       control ? "random_payload" : "samba", shuffled ? "shuffled" : "ordered",
                       fill, blocks, useful, compressed, (double)useful / compressed,
                       (double)(blocks * COMPRESSED_LEAF_SIZE) / compressed,
                       compressed + blocks * fixed, compressed + blocks * (fixed - saving));
            }
        }
    }
    free(records); free(order);
}

static void tree_probe(const struct btree_silesia_dataset *data, const char *pattern,
                       compression_algo_t codec, int ops)
{
    const int keys = 1024;
    int expected[1025];
    struct compression_config config = bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    config.algo = codec; config.default_sub_pages = 1; config.enable_lazy_compression = 0;
    setenv("BTREE_SHARDS", "1", 1);
    setenv("BTREE_PROFILE_SUBMISSION", "1", 1);
    unsetenv("BTREE_SUBMISSION_TRACE");
    struct bplus_tree_compressed *tree = bplus_tree_compressed_init_with_config(32, 128, &config);
    require(tree != NULL, "tree initialization failed");
    for (int k = 1; k <= keys; k++) {
        expected[k] = k;
        require(bplus_tree_compressed_put_with_payload(tree, k,
                    data->data + (size_t)(k - 1) * COMPRESSED_VALUE_BYTES,
                    COMPRESSED_VALUE_BYTES, expected[k]) == 0, "preload failed");
    }
    require(bplus_tree_compressed_drain_background(tree) == 0, "preload drain failed");
    /* Pick a known base key, rather than assuming its post-preload placement. */
    int base_key = 0;
    for (int k = 1; k <= keys && !base_key; k++) {
        struct bplus_tree_submission_stats probe;
        bplus_tree_compressed_submission_reset(tree);
        require(bplus_tree_compressed_get(tree, k) == expected[k], "preload oracle mismatch");
        require(bplus_tree_compressed_submission_stats(tree, &probe) == 0, "probe stats failed");
        if (probe.decompress_calls == 1) base_key = k;
    }
    require(base_key > 0, "no compressed base key found");
    bplus_tree_compressed_submission_reset(tree);
    struct bplus_tree_scheduler_stats before, after;
    require(bplus_tree_compressed_scheduler_stats(tree, &before) == 0, "scheduler stats failed");
    double *read_ns = malloc((size_t)ops * sizeof(double));
    double *write_ns = malloc((size_t)ops * sizeof(double));
    require(read_ns && write_ns, "latency allocation failed");
    uint64_t reads = 0, writes = 0, hash = 14695981039346656037ULL;
    uint32_t state = 0x13579bdfu;
    double cpu_start = cpu_ns(), start = now_ns();
    for (int i = 0; i < ops; i++) {
        int k;
        int write = strncmp(pattern, "write_", 6) == 0 || strcmp(pattern, "read_after_write") == 0;
        int read = !write || strcmp(pattern, "read_after_write") == 0;
        if (strcmp(pattern, "read_hot_base") == 0) k = base_key;
        else if (strcmp(pattern, "read_after_write") == 0) k = 1;
        else if (strcmp(pattern, "write_rotate_3") == 0) k = i % 3 + 1;
        else if (strcmp(pattern, "write_rotate_4") == 0) k = i % 4 + 1;
        else k = (int)(random_next(&state) % keys) + 1;
        hash = (hash ^ (uint64_t)k) * 1099511628211ULL;
        if (write) {
            expected[k] = keys + i + 1;
            const uint8_t *payload = data->data +
                ((size_t)(k - 1) + (size_t)(i + 1) * keys) % data->chunk_count * COMPRESSED_VALUE_BYTES;
            double t = now_ns();
            require(bplus_tree_compressed_put_with_payload(tree, k, payload,
                        COMPRESSED_VALUE_BYTES, expected[k]) == 0, "measured PUT failed");
            write_ns[writes++] = now_ns() - t;
            hash = (hash ^ (uint64_t)expected[k]) * 1099511628211ULL;
        }
        if (read) {
            double t = now_ns();
            int actual = bplus_tree_compressed_get(tree, k);
            read_ns[reads++] = now_ns() - t;
            require(actual == expected[k], "measured GET oracle mismatch");
        }
    }
    double foreground_end = now_ns();
    require(bplus_tree_compressed_drain_background(tree) == 0, "measured drain failed");
    double end = now_ns(), used_cpu = cpu_ns() - cpu_start;
    struct bplus_tree_submission_stats stats;
    require(bplus_tree_compressed_submission_stats(tree, &stats) == 0, "submission stats failed");
    require(bplus_tree_compressed_scheduler_stats(tree, &after) == 0, "scheduler stats failed");
    require(stats.logical_reads == reads && stats.logical_writes == writes, "logical counter mismatch");
    require(stats.current_inflight == 0 && after.failed_tasks == before.failed_tasks, "unfinished/failed jobs");
    /* Snapshot stats before this oracle and memory collector, both of which decompress. */
    for (int k = 1; k <= keys; k++)
        require(bplus_tree_compressed_get(tree, k) == expected[k], "final exact oracle mismatch");
    struct bplus_tree_memory_stats memory;
    require(bplus_tree_compressed_memory_stats(tree, &memory) == 0, "memory stats failed");
    require(memory.live_kv_bytes == keys * sizeof(struct kv_pair), "live byte mismatch");
    size_t resident = memory.tree_metadata_bytes + memory.leaf_metadata_bytes +
        memory.active_allocated_bytes + memory.pending_allocated_bytes +
        memory.compressed_usable_bytes + memory.scheduler_and_qpl_bytes;
    qsort(read_ns, reads, sizeof(double), compare_double);
    qsort(write_ns, writes, sizeof(double), compare_double);
    printf("DRYRUN {\"kind\":\"tree\",\"pattern\":\"%s\",\"codec\":\"%s\",\"bg\":%d,"
           "\"reads\":%" PRIu64 ",\"writes\":%" PRIu64 ",\"trace_hash\":\"%016" PRIx64 "\","
           "\"compress\":%" PRIu64 ",\"decompress\":%" PRIu64 ",\"write_lock_calls\":%" PRIu64 ","
           "\"foreground_ms\":%.3f,\"including_drain_ms\":%.3f,\"cpu_ns_op\":%.3f,"
           "\"read_p50_us\":%.3f,\"read_p99_us\":%.3f,\"write_p99_us\":%.3f,"
           "\"queue_full\":%" PRIu64 ",\"split_fallbacks\":%" PRIu64 ","
           "\"live\":%zu,\"resident\":%zu,\"leaf_metadata\":%zu,\"active\":%zu,"
           "\"mismatches\":0}\n", pattern, codec == COMPRESS_COPY ? "copy" : "lz4",
           btree_env_bool("BTREE_BG_COMPACTION", 0), reads, writes, hash,
           stats.compress_calls, stats.decompress_calls, stats.calls_under_write_lock,
           (foreground_end - start) / 1e6, (end - start) / 1e6, used_cpu / (reads + writes),
           reads ? read_ns[(reads - 1) / 2] / 1e3 : 0,
           reads ? read_ns[(size_t)((reads - 1) * .99)] / 1e3 : 0,
           writes ? write_ns[(size_t)((writes - 1) * .99)] / 1e3 : 0,
           after.queue_full_fallbacks - before.queue_full_fallbacks,
           after.split_fallbacks - before.split_fallbacks,
           memory.live_kv_bytes, resident, memory.leaf_metadata_bytes, memory.active_allocated_bytes);
    free(read_ns); free(write_ns);
    bplus_tree_compressed_deinit(tree);
}

int main(int argc, char **argv)
{
    require(argc == 2 || argc == 4, "usage: architecture_dry_run packing | PATTERN copy|lz4 OPS");
    struct btree_silesia_dataset data = {0};
    require(btree_load_silesia_samba(&data, COMPRESSED_VALUE_BYTES, 0) == 0, "corpus load failed");
    if (argc == 2 && strcmp(argv[1], "packing") == 0) packing_probe(&data);
    else {
        require(argc == 4, "missing tree arguments");
        const char *patterns[] = {"read_uniform", "read_hot_base", "read_after_write",
                                  "write_rotate_3", "write_rotate_4", "write_uniform"};
        int valid = 0;
        for (size_t i = 0; i < sizeof(patterns) / sizeof(*patterns); i++)
            valid |= strcmp(argv[1], patterns[i]) == 0;
        require(valid, "unknown pattern");
        require(strcmp(argv[2], "copy") == 0 || strcmp(argv[2], "lz4") == 0, "unsupported codec");
        char *end = NULL;
        long ops = strtol(argv[3], &end, 10);
        require(end && !*end && ops > 0 && ops <= 1000000, "OPS outside 1..1000000");
        tree_probe(&data, argv[1], strcmp(argv[2], "copy") == 0 ? COMPRESS_COPY : COMPRESS_LZ4, (int)ops);
    }
    free(data.data);
    return 0;
}
