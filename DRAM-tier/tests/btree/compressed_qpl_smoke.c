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
    printf("after put 2\n");
    int ret2 = bplus_tree_compressed_put(ct, 6, 206);
    assert(ret2 == 0);
    (void)ret2;
    printf("after put 6\n");
    int ret3 = bplus_tree_compressed_put(ct, 10, 210);
    assert(ret3 == 0);
    (void)ret3;
    printf("after put 10\n");

    // Insert one more key
    int ret4 = bplus_tree_compressed_put(ct, 22, 222);
    assert(ret4 == 0);
    (void)ret4;
    printf("after put 22\n");

    // Read back via compressed API
    int v2 = bplus_tree_compressed_get(ct, 2);
    int v6 = bplus_tree_compressed_get(ct, 6);
    int v10 = bplus_tree_compressed_get(ct, 10);
    int v22 = bplus_tree_compressed_get(ct, 22);

    if (!(v2 == 202 && v6 == 206 && v10 == 210 && v22 == 222)) {
        fprintf(stderr, "QPL smoke mismatch: %d %d %d %d (expected 202 206 210 222)\n",
                v2, v6, v10, v22);
        return 1;
    }

    assert(bplus_tree_compressed_get_range(ct, 0, 25) == 222);
    assert(bplus_tree_compressed_get_range(ct, 2, 10) == 210);

    assert(bplus_tree_compressed_delete(ct, 10) == 0);
    assert(bplus_tree_compressed_get(ct, 10) == -1);

    assert(bplus_tree_compressed_put(ct, 6, 0) == 0); // implicit delete
    assert(bplus_tree_compressed_get(ct, 6) == -1);

    assert(bplus_tree_compressed_get_range(ct, 0, 25) == 222);

    assert(bplus_tree_compressed_put(ct, 6, 606) == 0);
    assert(bplus_tree_compressed_put(ct, 10, 1010) == 0);
    assert(bplus_tree_compressed_get(ct, 6) == 606);
    assert(bplus_tree_compressed_get(ct, 10) == 1010);
    assert(bplus_tree_compressed_get_range(ct, 6, 10) == 1010);

    bplus_tree_compressed_deinit(ct);
    printf("compressed_qpl_smoke: OK (backend may be LZ4 fallback)\n");
    return 0;
}
