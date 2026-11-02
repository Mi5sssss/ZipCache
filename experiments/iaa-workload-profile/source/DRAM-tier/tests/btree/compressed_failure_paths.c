#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>

#include "bplustree_compressed.h"

static void require_true(int condition, const char *message)
{
    if (!condition) {
        fprintf(stderr, "compressed_failure_paths: %s\n", message);
        exit(EXIT_FAILURE);
    }
}

static struct bplus_tree_compressed *new_tree(void)
{
    struct compression_config config =
        bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    config.algo = COMPRESS_LZ4;
    config.default_sub_pages = 1;
    struct bplus_tree_compressed *tree =
        bplus_tree_compressed_init_with_config(16, 64, &config);
    require_true(tree != NULL, "tree initialization failed");
    return tree;
}

static void fill_and_verify(struct bplus_tree_compressed *tree, int first, int count)
{
    for (int key = first; key < first + count; key++) {
        require_true(bplus_tree_compressed_put(tree, key, key * 17 + 1) == 0,
                     "put failed");
    }
    for (int key = first; key < first + count; key++) {
        require_true(bplus_tree_compressed_get(tree, key) == key * 17 + 1,
                     "get mismatch");
    }
}

static void test_pending_allocation_fallback(void)
{
    setenv("BTREE_SHARDS", "1", 1);
    unsetenv("BTREE_TEST_FAIL_CODEC_ONCE");
    unsetenv("BTREE_TEST_WORKER_DELAY_US");
    struct bplus_tree_compressed *tree = new_tree();
    for (int key = 1; key <= 3; key++) {
        require_true(bplus_tree_compressed_put(tree, key, key * 17 + 1) == 0,
                     "allocation setup put failed");
    }
    setenv("BTREE_TEST_FAIL_PENDING_ALLOC", "1", 1);
    require_true(bplus_tree_compressed_put(tree, 4, 69) == 0,
                 "allocation failure did not use synchronous fallback");
    unsetenv("BTREE_TEST_FAIL_PENDING_ALLOC");
    for (int key = 1; key <= 4; key++) {
        require_true(bplus_tree_compressed_get(tree, key) == key * 17 + 1,
                     "allocation fallback lost a value");
    }
    struct bplus_tree_scheduler_stats stats;
    require_true(bplus_tree_compressed_scheduler_stats(tree, &stats) == 0 &&
                     stats.synchronous_fallbacks == 1,
                 "allocation fallback counter mismatch");
    bplus_tree_compressed_deinit(tree);
}

static void test_codec_retry(void)
{
    setenv("BTREE_SHARDS", "1", 1);
    setenv("BTREE_TEST_FAIL_CODEC_ONCE", "1", 1);
    struct bplus_tree_compressed *tree = new_tree();
    fill_and_verify(tree, 101, 4);
    require_true(bplus_tree_compressed_drain_background(tree) == 0,
                 "codec retry drain failed");
    for (int key = 101; key <= 104; key++) {
        require_true(bplus_tree_compressed_get(tree, key) == key * 17 + 1,
                     "codec retry lost a value");
    }
    struct bplus_tree_scheduler_stats stats;
    require_true(bplus_tree_compressed_scheduler_stats(tree, &stats) == 0,
                 "codec retry stats failed");
    require_true(stats.retry_count == 1 && stats.failed_tasks == 0 &&
                     stats.completed_tasks == 1,
                 "codec task did not retry exactly once");
    unsetenv("BTREE_TEST_FAIL_CODEC_ONCE");
    bplus_tree_compressed_deinit(tree);
}

static void test_permanent_codec_failure(void)
{
    setenv("BTREE_SHARDS", "1", 1);
    unsetenv("BTREE_TEST_FAIL_CODEC_ONCE");
    setenv("BTREE_TEST_FAIL_CODEC_ALWAYS", "1", 1);
    struct bplus_tree_compressed *tree = new_tree();

    fill_and_verify(tree, 201, 4);
    require_true(bplus_tree_compressed_drain_background(tree) == -1,
                 "failed pending was incorrectly reported as drained");

    struct bplus_tree_scheduler_stats stats;
    require_true(bplus_tree_compressed_scheduler_stats(tree, &stats) == 0,
                 "permanent codec failure stats failed");
    require_true(stats.retry_count == 1 && stats.failed_tasks == 1 &&
                     stats.completed_tasks == 0,
                 "permanent codec failure did not stop after one retry");

    /* pending and active remain the authoritative readable layers. */
    for (int key = 201; key <= 204; key++) {
        require_true(bplus_tree_compressed_get(tree, key) == key * 17 + 1,
                     "permanent codec failure hid a value");
    }
    require_true(bplus_tree_compressed_put(tree, 205, 205 * 17 + 1) == 0,
                 "failed pending should leave free active slots usable");
    require_true(bplus_tree_compressed_put(tree, 206, 206 * 17 + 1) == 0,
                 "failed pending should leave final active slot usable");
    require_true(bplus_tree_compressed_put(tree, 207, 207 * 17 + 1) == -1,
                 "full active plus failed pending must return an error");

    unsetenv("BTREE_TEST_FAIL_CODEC_ALWAYS");
    bplus_tree_compressed_deinit(tree);
}

static void test_queue_full_fallback(void)
{
    setenv("BTREE_SHARDS", "8", 1);
    setenv("BTREE_BG_BATCH_SIZE", "1", 1);
    setenv("BTREE_BG_QUEUE_CAPACITY", "1", 1);
    setenv("BTREE_TEST_WORKER_DELAY_US", "50000", 1);
    struct bplus_tree_compressed *tree = new_tree();

    for (int round = 0; round < 4; round++) {
        for (int shard = 0; shard < 8; shard++) {
            int residue = shard == 0 ? 8 : shard;
            int key = residue + round * 8;
            require_true(bplus_tree_compressed_put(tree, key, key * 17 + 1) == 0,
                         "queue-full workload put failed");
        }
    }
    require_true(bplus_tree_compressed_drain_background(tree) == 0,
                 "queue-full drain failed");
    for (int round = 0; round < 4; round++) {
        for (int shard = 0; shard < 8; shard++) {
            int residue = shard == 0 ? 8 : shard;
            int key = residue + round * 8;
            require_true(bplus_tree_compressed_get(tree, key) == key * 17 + 1,
                         "queue-full fallback lost a value");
        }
    }
    struct bplus_tree_scheduler_stats stats;
    require_true(bplus_tree_compressed_scheduler_stats(tree, &stats) == 0,
                 "queue-full stats failed");
    require_true(stats.queue_full_fallbacks > 0 && stats.synchronous_fallbacks > 0,
                 "bounded queue never exercised its fallback");
    unsetenv("BTREE_TEST_WORKER_DELAY_US");
    bplus_tree_compressed_deinit(tree);
}

int main(void)
{
    setenv("BTREE_BG_COMPACTION", "1", 1);
    setenv("BTREE_BG_BATCH_SIZE", "8", 1);
    setenv("BTREE_BG_QUEUE_CAPACITY", "32", 1);
    test_pending_allocation_fallback();
    test_codec_retry();
    test_permanent_codec_failure();
    test_queue_full_fallback();
    printf("compressed_failure_paths: OK\n");
    return EXIT_SUCCESS;
}
