// Minimal DRAM-tier compressed B+Tree smoke test (LZ4 hashed layout)
#include <assert.h>
#include <stdio.h>
#include "bplustree_compressed.h"

int main(void) {
    // Initialize with small parameters to keep things simple
    struct compression_config cfg = bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    cfg.algo = COMPRESS_LZ4;
    cfg.default_sub_pages = 4;
    cfg.enable_lazy_compression = 0; // direct compress on puts

    struct bplus_tree_compressed *ct = bplus_tree_compressed_init_with_config(8, 32, &cfg);
    assert(ct != NULL);

    // Insert a few keys that likely map to different sub-pages
    int ret1 = bplus_tree_compressed_put(ct, 1, 101);
    assert(ret1 == 0);
    (void)ret1;  // Suppress unused variable warning in release builds
    printf("after put 1\n");

    int ret2 = bplus_tree_compressed_put(ct, 5, 105);
    assert(ret2 == 0);
    (void)ret2;
    printf("after put 5\n");

    int ret3 = bplus_tree_compressed_put(ct, 9, 109);
    assert(ret3 == 0);
    (void)ret3;
    printf("after put 9\n");

    int ret4 = bplus_tree_compressed_put(ct, 13, 113);
    assert(ret4 == 0);
    (void)ret4;
    printf("after put 13\n");

    // Insert one more key
    int ret5 = bplus_tree_compressed_put(ct, 21, 221);
    assert(ret5 == 0);
    (void)ret5;
    printf("after put 21\n");

    // Read them back via compressed API
    int v1 = bplus_tree_compressed_get(ct, 1);
    int v5 = bplus_tree_compressed_get(ct, 5);
    int v9 = bplus_tree_compressed_get(ct, 9);
    int v13 = bplus_tree_compressed_get(ct, 13);
    int v21 = bplus_tree_compressed_get(ct, 21);

    // Use values even if asserts are compiled out
    if (!(v1 == 101 && v5 == 105 && v9 == 109 && v13 == 113 && v21 == 221)) {
        fprintf(stderr, "LZ4 smoke mismatch: %d %d %d %d %d (expected 101 105 109 113 221)\n",
                v1, v5, v9, v13, v21);
        return 1;
    }

    assert(bplus_tree_compressed_get_range(ct, 1, 21) == 221);
    assert(bplus_tree_compressed_get_range(ct, 1, 13) == 113);

    // Exercise explicit delete API
    assert(bplus_tree_compressed_delete(ct, 9) == 0);
    assert(bplus_tree_compressed_get(ct, 9) == -1);

    // Delete via zero-value PUT and ensure idempotency for missing keys
    assert(bplus_tree_compressed_put(ct, 5, 0) == 0);
    assert(bplus_tree_compressed_get(ct, 5) == -1);
    assert(bplus_tree_compressed_delete(ct, 42) == -1);

    // Remaining keys should still be visible
    assert(bplus_tree_compressed_get(ct, 1) == 101);
    assert(bplus_tree_compressed_get(ct, 13) == 113);
    assert(bplus_tree_compressed_get(ct, 21) == 221);

    assert(bplus_tree_compressed_get_range(ct, 0, 30) == 221);

    // Reinsert deleted keys and confirm values stick
    assert(bplus_tree_compressed_put(ct, 5, 505) == 0);
    assert(bplus_tree_compressed_put(ct, 9, 909) == 0);
    assert(bplus_tree_compressed_get(ct, 5) == 505);
    assert(bplus_tree_compressed_get(ct, 9) == 909);
    assert(bplus_tree_compressed_get_range(ct, 5, 9) == 909);

    bplus_tree_compressed_deinit(ct);
    printf("compressed_lz4_smoke: OK\n");
    return 0;
}
