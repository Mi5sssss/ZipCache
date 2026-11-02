#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bplustree_compressed.h"

static void fail(const char *message)
{
    fprintf(stderr, "compressed_async_compaction: %s\n", message);
    exit(EXIT_FAILURE);
}

static void require_true(int condition, const char *message)
{
    if (!condition) {
        fail(message);
    }
}

static int value_for(int key, int round)
{
    return key * 1009 + round * 100000 + 1;
}

static void run_case(int shards, int key_count)
{
#if ZIPCACHE_AGG_LAYOUT >= 2
    /* Exercise the same number of logical-leaf overflows in 16 KiB builds.
     * The production 4 KiB test retains its original workload. */
    key_count *= COMPRESSED_LEAF_SIZE / 4096;
#endif
    char shard_text[32];
    snprintf(shard_text, sizeof(shard_text), "%d", shards);
    setenv("BTREE_SHARDS", shard_text, 1);

    struct compression_config config =
        bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    config.algo = COMPRESS_LZ4;
    config.default_sub_pages = 1;

    struct bplus_tree_compressed *tree =
        bplus_tree_compressed_init_with_config(16, 64, &config);
    require_true(tree != NULL, "tree initialization failed");

    int *expected = malloc(((size_t)key_count + 1) * sizeof(*expected));
    require_true(expected != NULL, "expected-value allocation failed");
    for (int key = 0; key <= key_count; key++) {
        expected[key] = -1;
    }

    /* The fourth write seals active; immediate reads must still see pending. */
    for (int key = 1; key <= key_count; key++) {
        expected[key] = value_for(key, 1);
        require_true(bplus_tree_compressed_put(tree, key, expected[key]) == 0,
                     "initial put failed");
        require_true(bplus_tree_compressed_get(tree, key) == expected[key],
                     "immediate initial get mismatch");
    }
    require_true(bplus_tree_compressed_drain_background(tree) == 0,
                 "initial background drain failed");

    bplus_tree_compressed_submission_reset(tree);
    for (int round = 2; round <= 6; round++) {
        for (int key = 1; key <= key_count; key++) {
            expected[key] = value_for(key, round);
            require_true(bplus_tree_compressed_put(tree, key, expected[key]) == 0,
                         "update put failed");
            require_true(bplus_tree_compressed_get(tree, key) == expected[key],
                         "active/pending/base precedence mismatch");
        }
    }

    /* DELETE must safely drain or absorb a target leaf's pending delta. */
    for (int key = 11; key <= key_count; key += 11) {
        require_true(bplus_tree_compressed_delete(tree, key) == 0,
                     "delete while background work is active failed");
        expected[key] = -1;
        require_true(bplus_tree_compressed_get(tree, key) == -1,
                     "deleted key remained visible");
    }

    require_true(bplus_tree_compressed_drain_background(tree) == 0,
                 "final background drain failed");
    for (int key = 1; key <= key_count; key++) {
        if (bplus_tree_compressed_get(tree, key) != expected[key]) {
            fail("exact final-value oracle mismatch");
        }
    }

    struct bplus_tree_scheduler_stats scheduler_stats;
    require_true(bplus_tree_compressed_scheduler_stats(tree, &scheduler_stats) == 0,
                 "scheduler stats failed");
    require_true(scheduler_stats.submitted_tasks > 0,
                 "workload produced no background tasks");
    require_true(scheduler_stats.completed_tasks == scheduler_stats.submitted_tasks,
                 "background task was not completed exactly once");
    require_true(scheduler_stats.failed_tasks == 0,
                 "background task failed");
    require_true(scheduler_stats.split_fallbacks == 0,
                 "no-split workload unexpectedly used split fallback");

    struct bplus_tree_submission_stats submission_stats;
    require_true(bplus_tree_compressed_submission_stats(tree, &submission_stats) == 0,
                 "submission stats failed");
    require_true(submission_stats.current_inflight == 0,
                 "codec call remained in flight after drain");
    require_true(submission_stats.calls_under_write_lock == 0,
                 "ordinary compaction called a codec under the leaf write lock");

    struct bplus_tree_memory_stats memory_stats;
    require_true(bplus_tree_compressed_memory_stats(tree, &memory_stats) == 0,
                 "memory stats failed");
    size_t live_count = (size_t)key_count - (size_t)(key_count / 11);
    require_true(memory_stats.live_kv_bytes == live_count * sizeof(struct kv_pair),
                 "live KV byte accounting mismatch");
    require_true(memory_stats.active_allocated_bytes ==
                     (size_t)shards * ACTIVE_DELTA_ENTRIES * sizeof(struct kv_pair),
                 "unexpected leaf count or active allocation");
    require_true(memory_stats.compressed_requested_bytes > 0,
                 "no exact-size compressed base was resident");
    require_true(memory_stats.compressed_requested_bytes * 5 <
                     (size_t)shards * MAX_COMPRESSED_SIZE * 4,
                 "exact-size bases did not beat the former fixed allocation by 1.25x");

    free(expected);
    bplus_tree_compressed_deinit(tree);
}

static void verify_shutdown_drains_pending(void)
{
    setenv("BTREE_SHARDS", "1", 1);
    struct compression_config config =
        bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    config.algo = COMPRESS_LZ4;
    struct bplus_tree_compressed *tree =
        bplus_tree_compressed_init_with_config(16, 64, &config);
    require_true(tree != NULL, "shutdown tree initialization failed");
    for (int key = 1; key <= 4; key++) {
        require_true(bplus_tree_compressed_put(tree, key, value_for(key, 1)) == 0,
                     "shutdown setup put failed");
    }
    bplus_tree_compressed_deinit(tree);
}

static void verify_async_overflow_split(void)
{
    setenv("BTREE_SHARDS", "1", 1);
    struct compression_config config =
        bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    config.algo = COMPRESS_LZ4;
    struct bplus_tree_compressed *tree =
        bplus_tree_compressed_init_with_config(16, 64, &config);
    require_true(tree != NULL, "split tree initialization failed");

    const int overflow_keys = 96 * (COMPRESSED_LEAF_SIZE / 4096);
    for (int key = 1; key <= overflow_keys; key++) {
        require_true(bplus_tree_compressed_put(tree, key, value_for(key, 1)) == 0,
                     "split workload put failed");
    }
    require_true(bplus_tree_compressed_drain_background(tree) == 0,
                 "split background drain failed");
    for (int key = 1; key <= overflow_keys; key++) {
        require_true(bplus_tree_compressed_get(tree, key) == value_for(key, 1),
                     "split fallback lost a value");
    }

    struct bplus_tree_scheduler_stats stats;
    require_true(bplus_tree_compressed_scheduler_stats(tree, &stats) == 0,
                 "split scheduler stats failed");
    require_true(stats.split_fallbacks > 0 && stats.failed_tasks == 0,
                 "dense workload did not complete through split fallback");
    bplus_tree_compressed_deinit(tree);
}

int main(void)
{
    setenv("BTREE_BG_COMPACTION", "1", 1);
    setenv("BTREE_BG_BATCH_SIZE", "8", 1);
    setenv("BTREE_BG_QUEUE_CAPACITY", "32", 1);
    setenv("BTREE_PROFILE_SUBMISSION", "1", 1);
    /* Deprecated values cannot shrink the v1 async active[3] state. */
    setenv("BTREE_LANDING_BUFFER_BYTES", "136", 1);

    run_case(1, 24);
    run_case(8, 160);
    verify_async_overflow_split();
    verify_shutdown_drains_pending();
    printf("compressed_async_compaction: OK\n");
    return EXIT_SUCCESS;
}
