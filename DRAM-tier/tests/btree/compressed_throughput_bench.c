/*
 * Comprehensive per-operation throughput benchmark for compressed B+Tree.
 *
 * Phases:
 *   1. WARMUP  – insert WARMUP_KEYS keys to fill the tree and trigger compression
 *   2. PUT     – timed inserts of new keys
 *   3. GET     – timed point reads of existing keys
 *   4. RANGE   – timed range scans
 *   5. DELETE  – timed deletes of existing keys
 *   6. STATS   – timed stats calls
 *   7. MIXED   – timed mixed workload (configurable ratios)
 *   8. VERIFY  – correctness check after all operations
 *
 * Environment variables:
 *   BENCH_WARMUP_KEYS   (default 5000)
 *   BENCH_OPS           (default 10000)  – ops per phase
 *   BENCH_KEY_SPACE     (default 20000) – key range for random ops
 *   BENCH_RANGE_WIDTH   (default 50)    – range scan width
 *   BENCH_THREADS       (default 1)     – thread count for mixed workload
 *   BENCH_DURATION_SEC  (default 3)     – mixed workload duration
 *   BTREE_USE_SILESIA   (default 0)     – use Silesia samba.zip 128B slices as payloads
 *   BTREE_VALUE_BYTES   (default 128)   – payload bytes per value when Silesia is enabled
 *
 * Codecs tested: LZ4, QPL (software), zlib-accel
 */

#define _GNU_SOURCE
#include <assert.h>
#include <errno.h>
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bplustree_compressed.h"
#include "compressed_test_utils.h"

/* ── helpers ─────────────────────────────────────────────────────── */

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

static int rand_key(int key_space)
{
    return 1 + (rand() % key_space);
}

static int value_for_key(int key)
{
    return key * 7 + 42;
}

struct value_source {
    int use_silesia;
    struct btree_silesia_dataset silesia;
};

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

static const uint8_t *payload_for_key(const struct value_source *source, int key)
{
    if (!source || !source->use_silesia) {
        return NULL;
    }
    return btree_silesia_payload_for_key_version(&source->silesia, (key_t)key, 0);
}

static int put_value(struct bplus_tree_compressed *tree,
                     const struct value_source *source,
                     int key)
{
    int value = value_for_key(key);
    const uint8_t *payload = payload_for_key(source, key);
    if (payload) {
        return bplus_tree_compressed_put_with_payload(tree,
                                                      (key_t)key,
                                                      payload,
                                                      source->silesia.chunk_bytes,
                                                      value);
    }
    return bplus_tree_compressed_put(tree, (key_t)key, value);
}

static unsigned int fast_rand_u32(unsigned int *state)
{
    unsigned int x = *state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = x ? x : 0x9e3779b9u;
    return *state;
}

/* ── tree lifecycle ──────────────────────────────────────────────── */

static struct bplus_tree_compressed *create_tree(compression_algo_t algo)
{
    struct compression_config cfg =
        bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    cfg.algo = algo;
    cfg.default_sub_pages = 1;
    cfg.enable_lazy_compression = 0;
    btree_apply_qpl_env(&cfg);

    struct bplus_tree_compressed *tree =
        bplus_tree_compressed_init_with_config(16, 64, &cfg);
    return tree;
}

/* ── warmup ──────────────────────────────────────────────────────── */

static int warmup(struct bplus_tree_compressed *tree,
                  const struct value_source *source,
                  int warmup_keys)
{
    int ok = 0;
    for (int k = 1; k <= warmup_keys; k++) {
        if (put_value(tree, source, k) == 0)
            ok++;
    }
    return ok;
}

/* ── per-operation benchmarks ────────────────────────────────────── */

struct bench_result {
    const char *op;
    int ops;
    double elapsed_sec;
    int errors;
};

static void report(const char *algo, struct bench_result *r)
{
    double qps = r->ops / r->elapsed_sec;
    double us  = r->elapsed_sec * 1e6 / r->ops;
    printf("  %-12s  %-8s  %8d ops  %10.1f op/s  %8.2f us/op",
           algo, r->op, r->ops, qps, us);
    if (r->errors > 0)
        printf("  errors=%d", r->errors);
    printf("\n");
}

static void report_tree_memory(const char *algo,
                               const char *phase,
                               struct bplus_tree_compressed *tree)
{
    size_t total = 0;
    size_t compressed = 0;
    if (bplus_tree_compressed_calculate_stats(tree, &total, &compressed) != 0) {
        printf("  %-12s  %-8s  memory unavailable\n", algo, phase);
        return;
    }

    double ratio = compressed > 0 ? (double)total / (double)compressed : 0.0;
    double saved_pct = total > 0 ? (1.0 - ((double)compressed / (double)total)) * 100.0 : 0.0;
    printf("  %-12s  %-8s  memory total=%zu compressed=%zu ratio=%.3f saved_pct=%.2f\n",
           algo,
           phase,
           total,
           compressed,
           ratio,
           saved_pct);
}

static struct bench_result bench_put(struct bplus_tree_compressed *tree,
                                     const struct value_source *source,
                                     int ops, int key_offset)
{
    struct bench_result r = { .op = "put", .ops = ops };
    double t0 = now_sec();
    for (int i = 0; i < ops; i++) {
        int k = key_offset + i;
        if (put_value(tree, source, k) != 0)
            r.errors++;
    }
    r.elapsed_sec = now_sec() - t0;
    return r;
}

static struct bench_result bench_get(struct bplus_tree_compressed *tree,
                                     int ops, int key_space)
{
    struct bench_result r = { .op = "get", .ops = ops };
    int found = 0;
    double t0 = now_sec();
    for (int i = 0; i < ops; i++) {
        int k = rand_key(key_space);
        int v = bplus_tree_compressed_get(tree, k);
        if (v >= 0) found++;
    }
    r.elapsed_sec = now_sec() - t0;
    r.errors = ops - found;  /* misses counted as "errors" for visibility */
    return r;
}

static struct bench_result bench_range(struct bplus_tree_compressed *tree,
                                       int ops, int key_space, int range_width)
{
    struct bench_result r = { .op = "range", .ops = ops };
    int found = 0;
    double t0 = now_sec();
    for (int i = 0; i < ops; i++) {
        int lo = rand_key(key_space - range_width);
        int hi = lo + range_width;
        int v  = bplus_tree_compressed_get_range(tree, lo, hi);
        if (v >= 0) found++;
    }
    r.elapsed_sec = now_sec() - t0;
    r.errors = ops - found;
    return r;
}

static struct bench_result bench_delete(struct bplus_tree_compressed *tree,
                                        int ops, int key_space)
{
    struct bench_result r = { .op = "delete", .ops = ops };
    double t0 = now_sec();
    for (int i = 0; i < ops; i++) {
        int k = rand_key(key_space);
        if (bplus_tree_compressed_delete(tree, k) != 0)
            r.errors++;  /* key not found is normal for random deletes */
    }
    r.elapsed_sec = now_sec() - t0;
    return r;
}

static struct bench_result bench_stats(struct bplus_tree_compressed *tree,
                                       int ops)
{
    struct bench_result r = { .op = "stats", .ops = ops };
    size_t total = 0, compressed = 0;
    double t0 = now_sec();
    for (int i = 0; i < ops; i++) {
        if (bplus_tree_compressed_stats(tree, &total, &compressed) != 0)
            r.errors++;
    }
    r.elapsed_sec = now_sec() - t0;
    return r;
}

static struct bench_result bench_calc_stats(struct bplus_tree_compressed *tree,
                                            int ops)
{
    struct bench_result r = { .op = "calc_stats", .ops = ops };
    size_t total = 0, compressed = 0;
    double t0 = now_sec();
    for (int i = 0; i < ops; i++) {
        if (bplus_tree_compressed_calculate_stats(tree, &total, &compressed) != 0)
            r.errors++;
    }
    r.elapsed_sec = now_sec() - t0;
    return r;
}

/* ── correctness verification ────────────────────────────────────── */

static int verify_tree(struct bplus_tree_compressed *tree,
                       int start_key, int end_key)
{
    int correct = 0, missing = 0, wrong = 0;
    for (int k = start_key; k <= end_key; k++) {
        int expected = value_for_key(k);
        int got = bplus_tree_compressed_get(tree, k);
        if (got == expected) {
            correct++;
        } else if (got < 0) {
            missing++;
        } else {
            wrong++;
        }
    }
    printf("    verify [%d..%d]: correct=%d missing=%d wrong=%d\n",
           start_key, end_key, correct, missing, wrong);
    return wrong;  /* wrong values are the real bugs */
}

/* ── mixed workload (multi-threaded, timed) ──────────────────────── */

struct mixed_args {
    struct bplus_tree_compressed *tree;
    const struct value_source *source;
    int key_space;
    int range_width;
    int duration_sec;
    /* output */
    long puts, gets, deletes, ranges, stats_calls;
    int get_mismatches;
};

static void *mixed_worker(void *arg)
{
    struct mixed_args *a = arg;
    double deadline = now_sec() + a->duration_sec;
    unsigned int seed = (unsigned int)(uintptr_t)pthread_self();
    if (seed == 0) {
        seed = 0x12345678u;
    }
    unsigned int local_ops = 0;
    long puts = 0;
    long gets = 0;
    long deletes = 0;
    long ranges = 0;
    long stats_calls = 0;
    int get_mismatches = 0;

    while (1) {
        if ((local_ops & 1023u) == 0 && now_sec() >= deadline) {
            break;
        }
        local_ops++;

        int r = (int)(fast_rand_u32(&seed) % 100u);
        int k = 1 + (int)(fast_rand_u32(&seed) % (unsigned int)a->key_space);

        if (r < 40) {
            /* 40% put */
            put_value(a->tree, a->source, k);
            puts++;
        } else if (r < 75) {
            /* 35% get */
            int v = bplus_tree_compressed_get(a->tree, k);
            if (v >= 0 && v != value_for_key(k))
                get_mismatches++;
            gets++;
        } else if (r < 90) {
            /* 15% delete */
            bplus_tree_compressed_delete(a->tree, k);
            deletes++;
        } else if (r < 97) {
            /* 7% range */
            int lo = k;
            int hi = lo + a->range_width;
            if (hi > a->key_space) hi = a->key_space;
            bplus_tree_compressed_get_range(a->tree, lo, hi);
            ranges++;
        } else {
            /* 3% stats */
            size_t total = 0, comp = 0;
            bplus_tree_compressed_stats(a->tree, &total, &comp);
            stats_calls++;
        }
    }

    a->puts = puts;
    a->gets = gets;
    a->deletes = deletes;
    a->ranges = ranges;
    a->stats_calls = stats_calls;
    a->get_mismatches = get_mismatches;
    return NULL;
}

static int bench_mixed(struct bplus_tree_compressed *tree,
                         const struct value_source *source,
                         const char *algo_name,
                         int threads, int key_space,
                         int range_width, int duration_sec)
{
    pthread_t *tids = calloc(threads, sizeof(*tids));
    struct mixed_args *args = calloc(threads, sizeof(*args));

    for (int i = 0; i < threads; i++) {
        args[i].tree = tree;
        args[i].source = source;
        args[i].key_space = key_space;
        args[i].range_width = range_width;
        args[i].duration_sec = duration_sec;
        pthread_create(&tids[i], NULL, mixed_worker, &args[i]);
    }

    for (int i = 0; i < threads; i++)
        pthread_join(tids[i], NULL);

    long total_ops = 0;
    long total_puts = 0, total_gets = 0, total_deletes = 0;
    long total_ranges = 0, total_stats = 0;
    int total_mismatches = 0;
    for (int i = 0; i < threads; i++) {
        total_puts   += args[i].puts;
        total_gets   += args[i].gets;
        total_deletes+= args[i].deletes;
        total_ranges += args[i].ranges;
        total_stats  += args[i].stats_calls;
        total_mismatches += args[i].get_mismatches;
    }
    total_ops = total_puts + total_gets + total_deletes + total_ranges + total_stats;

    size_t total_bytes = 0;
    size_t compressed_bytes = 0;
    (void)bplus_tree_compressed_calculate_stats(tree, &total_bytes, &compressed_bytes);
    double ratio = compressed_bytes > 0
        ? (double)total_bytes / (double)compressed_bytes
        : 0.0;
    double saved_pct = total_bytes > 0
        ? (1.0 - ((double)compressed_bytes / (double)total_bytes)) * 100.0
        : 0.0;

    printf("  %-12s  mixed     %8ld ops  %10.1f op/s  [put=%ld get=%ld del=%ld range=%ld stats=%ld] mismatches=%d mem=%zu/%zu ratio=%.3f saved_pct=%.2f\n",
           algo_name, total_ops, (double)total_ops / duration_sec,
           total_puts, total_gets, total_deletes, total_ranges, total_stats,
           total_mismatches,
           compressed_bytes,
           total_bytes,
           ratio,
           saved_pct);

    free(tids);
    free(args);
    return total_mismatches;
}

/* ── main ────────────────────────────────────────────────────────── */

int main(void)
{
    srand((unsigned int)time(NULL));

    int warmup_keys  = btree_env_int("BENCH_WARMUP_KEYS",  5000, 100);
    int ops          = btree_env_int("BENCH_OPS",          10000, 100);
    int key_space    = btree_env_int("BENCH_KEY_SPACE",    20000, 1000);
    int range_width  = btree_env_int("BENCH_RANGE_WIDTH",  50, 1);
    int threads      = btree_env_int("BENCH_THREADS",      1, 1);
    int duration_sec = btree_env_int("BENCH_DURATION_SEC",  3, 1);
    struct value_source source;
    int max_payload_key = key_space + warmup_keys + ops + 1024;
    value_source_init(&source, max_payload_key);

    printf("=== Compressed B+Tree Throughput Benchmark ===\n");
    printf("warmup=%d  ops=%d  key_space=%d  range=%d  threads=%d  duration=%ds value_source=%s value_bytes=%zu\n\n",
           warmup_keys,
           ops,
           key_space,
           range_width,
           threads,
           duration_sec,
           source.use_silesia ? "silesia_samba" : "synthetic",
           source.use_silesia ? source.silesia.chunk_bytes : (size_t)sizeof(int));

    compression_algo_t algos[] = {
        COMPRESS_LZ4,
        COMPRESS_QPL,
        COMPRESS_ZLIB_ACCEL,
    };
    const int num_algos = sizeof(algos) / sizeof(algos[0]);

    int total_bugs = 0;

    for (int a = 0; a < num_algos; a++) {
        const char *name = btree_algo_name(algos[a]);
        struct bplus_tree_compressed *tree = create_tree(algos[a]);
        if (!tree) {
            printf("%-12s  SKIP (init failed)\n", name);
            continue;
        }

        /* ── Phase 1: warmup ──────────────────────────────────── */
        printf("--- %s ---\n", name);
        double w0 = now_sec();
        int warmed = warmup(tree, &source, warmup_keys);
        double w_elapsed = now_sec() - w0;
        printf("  warmup:       %d/%d keys in %.3fs (%.0f put/s)\n",
               warmed, warmup_keys, w_elapsed, warmed / w_elapsed);
        report_tree_memory(name, "warmup", tree);
        if (warmed != warmup_keys) {
            printf("  WARNING: %d warmup inserts failed\n", warmup_keys - warmed);
        }

        /* verify warmup */
        int wrong = verify_tree(tree, 1, warmup_keys);
        if (wrong > 0) {
            total_bugs += wrong;
            printf("  BUG: %d wrong values after warmup!\n", wrong);
        }

        /* ── Phase 2-6: per-operation throughput ──────────────── */
        struct bench_result r_put    = bench_put(tree, &source, ops, warmup_keys + 1);
        struct bench_result r_get    = bench_get(tree, ops, warmup_keys + ops);
        struct bench_result r_range  = bench_range(tree, ops / 2, warmup_keys + ops, range_width);
        struct bench_result r_stats  = bench_stats(tree, ops);
        struct bench_result r_cstats = bench_calc_stats(tree, ops / 10);
        struct bench_result r_del    = bench_delete(tree, ops / 2, warmup_keys + ops);

        report(name, &r_put);
        report(name, &r_get);
        report(name, &r_range);
        report(name, &r_del);
        report(name, &r_stats);
        report(name, &r_cstats);
        report_tree_memory(name, "post_ops", tree);

        /* ── verify after individual benchmarks ──────────────── */
        printf("  post-bench verification:\n");
        /* re-insert a small range to check basic correctness */
        for (int k = 1; k <= 500; k++) {
            put_value(tree, &source, k);
        }
        wrong = verify_tree(tree, 1, 500);
        if (wrong > 0) {
            total_bugs += wrong;
            printf("  BUG: %d wrong values after bench!\n", wrong);
        }

        /* ── Phase 7: mixed workload ─────────────────────────── */
        /* Re-warmup for mixed benchmark */
        bplus_tree_compressed_deinit(tree);
        tree = create_tree(algos[a]);
        if (tree) {
            warmup(tree, &source, warmup_keys);
            total_bugs += bench_mixed(tree, &source, name, threads, key_space, range_width, duration_sec);
            report_tree_memory(name, "post_mix", tree);

            /* ── Phase 8: verify after mixed ─────────────────── */
            printf("  post-mixed verification:\n");
            /* Insert known keys and verify */
            for (int k = key_space + 1; k <= key_space + 200; k++) {
                put_value(tree, &source, k);
            }
            wrong = verify_tree(tree, key_space + 1, key_space + 200);
            if (wrong > 0) {
                total_bugs += wrong;
                printf("  BUG: %d wrong values after mixed!\n", wrong);
            }

            bplus_tree_compressed_deinit(tree);
        }
        printf("\n");
    }

    printf("=== Summary: total correctness bugs = %d ===\n", total_bugs);
    value_source_deinit(&source);
    return total_bugs > 0 ? 1 : 0;
}
