#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "bplustree_compressed.h"

#define KEY_SPACE 256
#define OPS       2000

static int expected_range_value(const int reference[KEY_SPACE], int lo, int hi) {
    if (lo > hi) {
        int tmp = lo;
        lo = hi;
        hi = tmp;
    }
    int result = -1;
    for (int k = lo; k <= hi; ++k) {
        if (reference[k] != -1) {
            result = reference[k];
        }
    }
    return result;
}

static void run_fuzz(compression_algo_t algo, const char *label) {
    struct compression_config cfg = bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    cfg.algo = algo;
    cfg.default_sub_pages = 8;
    cfg.enable_lazy_compression = 0;

    struct bplus_tree_compressed *ct = bplus_tree_compressed_init_with_config(16, 64, &cfg);
    assert(ct != NULL);

    int reference[KEY_SPACE];
    for (int i = 0; i < KEY_SPACE; ++i) {
        reference[i] = -1;
    }

    srand(1337 + algo);
    for (int op = 0; op < OPS; ++op) {
        int choice = rand() % 4;
        int key = rand() % KEY_SPACE;

        if (choice <= 1) {
            int value = (rand() % 50000) + 1;
            assert(bplus_tree_compressed_put(ct, key, value) == 0);
            reference[key] = value;
        } else if (choice == 2) {
            int rc = bplus_tree_compressed_delete(ct, key);
            if (reference[key] == -1) {
                assert(rc == -1);
            } else {
                assert(rc == 0);
                reference[key] = -1;
            }
            (void)rc;
        } else {
            int got = bplus_tree_compressed_get(ct, key);
            if (reference[key] == -1) {
                assert(got == -1);
            } else {
                assert(got == reference[key]);
            }

            int a = rand() % KEY_SPACE;
            int b = rand() % KEY_SPACE;
            int expect = expected_range_value(reference, a, b);
            int range_val = bplus_tree_compressed_get_range(ct, a, b);
            assert(range_val == expect);
            (void)got;
            (void)expect;
            (void)range_val;
        }
    }

    bplus_tree_compressed_deinit(ct);
    printf("compressed_crud_fuzz[%s]: OK\n", label);
}

int main(void) {
    run_fuzz(COMPRESS_LZ4, "lz4");
    run_fuzz(COMPRESS_QPL, "qpl");
    return 0;
}
