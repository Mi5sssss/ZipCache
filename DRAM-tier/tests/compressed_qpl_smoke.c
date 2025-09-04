// Minimal DRAM-tier compressed B+Tree smoke test (QPL path)
// Note: Implementation may fall back to LZ4 behavior if QPL is unavailable or not implemented.
#include <assert.h>
#include <stdio.h>
#include "bplustree_compressed.h"

int main(void) {
    struct compression_config cfg = bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    cfg.algo = COMPRESS_QPL;            // request QPL globally
    cfg.default_sub_pages = 4;
    cfg.enable_lazy_compression = 0;

    struct bplus_tree_compressed *ct = bplus_tree_compressed_init_with_config(8, 32, &cfg);
    assert(ct != NULL);

    // Basic put/get correctness should hold regardless of backend availability
    int ret1 = bplus_tree_compressed_put(ct, 2, 202);
    assert(ret1 == 0);
    (void)ret1;  // Suppress unused variable warning in release builds
    printf("after put 2 -> base get %d\n", bplus_tree_get(ct->tree, 2));
    int ret2 = bplus_tree_compressed_put(ct, 6, 206);
    assert(ret2 == 0);
    (void)ret2;
    printf("after put 6 -> base get %d\n", bplus_tree_get(ct->tree, 6));
    int ret3 = bplus_tree_compressed_put(ct, 10, 210);
    assert(ret3 == 0);
    (void)ret3;
    printf("after put 10 -> base get %d\n", bplus_tree_get(ct->tree, 10));

    // Optional: dump state for debugging
    bplus_tree_compressed_dump(ct);

    // Base direct sanity
    bplus_tree_put(ct->tree, 22, 222);
    int b22 = bplus_tree_get(ct->tree, 22);
    printf("Base direct put/get 22 -> %d\n", b22);

    int v2 = bplus_tree_compressed_get(ct, 2);
    int v6 = bplus_tree_compressed_get(ct, 6);
    int v10 = bplus_tree_compressed_get(ct, 10);
    int b2 = bplus_tree_get(ct->tree, 2);
    int b6 = bplus_tree_get(ct->tree, 6);
    int b10 = bplus_tree_get(ct->tree, 10);
    if (!(v2 == 202 && v6 == 206 && v10 == 210)) {
        fprintf(stderr, "QPL smoke mismatch: %d %d %d (base %d %d %d)\n", v2, v6, v10, b2, b6, b10);
        return 1;
    }

    bplus_tree_compressed_deinit(ct);
    printf("compressed_qpl_smoke: OK (backend may be LZ4 fallback)\n");
    return 0;
}
