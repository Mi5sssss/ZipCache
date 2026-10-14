// Minimal DRAM-tier compressed B+Tree smoke test (zlib API / zlib-accel via LD_PRELOAD)
#include <assert.h>
#include <stdio.h>
#include "bplustree_compressed.h"

int main(void) {
    struct compression_config cfg = bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    cfg.algo = COMPRESS_ZLIB_ACCEL;
    cfg.default_sub_pages = 4;
    cfg.enable_lazy_compression = 0;

    struct bplus_tree_compressed *ct = bplus_tree_compressed_init_with_config(8, 32, &cfg);
    assert(ct != NULL);

    int r3 = bplus_tree_compressed_put(ct, 3, 303);
    int r7 = bplus_tree_compressed_put(ct, 7, 307);
    int r11 = bplus_tree_compressed_put(ct, 11, 311);
    int r23 = bplus_tree_compressed_put(ct, 23, 323);
    if (!(r3 == 0 && r7 == 0 && r11 == 0 && r23 == 0)) {
        fprintf(stderr, "zlib-accel smoke put failure: %d %d %d %d\n", r3, r7, r11, r23);
        return 1;
    }

    int v3 = bplus_tree_compressed_get(ct, 3);
    int v7 = bplus_tree_compressed_get(ct, 7);
    int v11 = bplus_tree_compressed_get(ct, 11);
    int v23 = bplus_tree_compressed_get(ct, 23);
    if (!(v3 == 303 && v7 == 307 && v11 == 311 && v23 == 323)) {
        fprintf(stderr, "zlib-accel smoke mismatch: %d %d %d %d (expected 303 307 311 323)\n",
                v3, v7, v11, v23);
        return 1;
    }

    assert(bplus_tree_compressed_get_range(ct, 0, 25) == 323);
    assert(bplus_tree_compressed_get_range(ct, 3, 11) == 311);

    assert(bplus_tree_compressed_delete(ct, 11) == 0);
    assert(bplus_tree_compressed_get(ct, 11) == -1);

    assert(bplus_tree_compressed_put(ct, 7, 0) == 0);
    assert(bplus_tree_compressed_get(ct, 7) == -1);

    assert(bplus_tree_compressed_put(ct, 7, 707) == 0);
    assert(bplus_tree_compressed_put(ct, 11, 1111) == 0);
    assert(bplus_tree_compressed_get(ct, 7) == 707);
    assert(bplus_tree_compressed_get(ct, 11) == 1111);
    assert(bplus_tree_compressed_get_range(ct, 7, 11) == 1111);

    bplus_tree_compressed_deinit(ct);
    printf("compressed_zlib_accel_smoke: OK (uses zlib; LD_PRELOAD can enable zlib-accel)\n");
    return 0;
}
