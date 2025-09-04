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
    printf("after put 1 -> base get %d\n", bplus_tree_get(ct->tree, 1));
    int ret2 = bplus_tree_compressed_put(ct, 5, 105);
    assert(ret2 == 0);
    (void)ret2;
    printf("after put 5 -> base get %d\n", bplus_tree_get(ct->tree, 5));
    int ret3 = bplus_tree_compressed_put(ct, 9, 109);
    assert(ret3 == 0);
    (void)ret3;
    printf("after put 9 -> base get %d\n", bplus_tree_get(ct->tree, 9));
    int ret4 = bplus_tree_compressed_put(ct, 13, 113);
    assert(ret4 == 0);
    (void)ret4;
    printf("after put 13 -> base get %d\n", bplus_tree_get(ct->tree, 13));

    // Optional: dump compressed tree state for debugging
    bplus_tree_compressed_dump(ct);

    // Also exercise base tree directly to verify core insert/get
    bplus_tree_put(ct->tree, 21, 221);
    int b21 = bplus_tree_get(ct->tree, 21);
    printf("Base direct put/get 21 -> %d\n", b21);

    // Read them back (both via compressed API and base tree for verification)
    int v1 = bplus_tree_compressed_get(ct, 1);
    int v5 = bplus_tree_compressed_get(ct, 5);
    int v9 = bplus_tree_compressed_get(ct, 9);
    int v13 = bplus_tree_compressed_get(ct, 13);
    int b1 = bplus_tree_get(ct->tree, 1);
    int b5 = bplus_tree_get(ct->tree, 5);
    int b9 = bplus_tree_get(ct->tree, 9);
    int b13 = bplus_tree_get(ct->tree, 13);
    // Use values even if asserts are compiled out
    if (!(v1 == 101 && v5 == 105 && v9 == 109 && v13 == 113)) {
        fprintf(stderr, "LZ4 smoke mismatch: %d %d %d %d (base %d %d %d %d)\n", v1, v5, v9, v13, b1, b5, b9, b13);
        return 1;
    }

    bplus_tree_compressed_deinit(ct);
    printf("compressed_lz4_smoke: OK\n");
    return 0;
}
