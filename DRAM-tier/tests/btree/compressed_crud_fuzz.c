#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "bplustree_compressed.h"
#include "compressed_test_utils.h"

#define KEY_SPACE 256
#define OPS       2000

static void require_true(int condition, const char *message) {
    if (!condition) {
        fprintf(stderr, "%s\n", message);
        exit(EXIT_FAILURE);
    }
}

static int expected_range_value(const int reference[KEY_SPACE + 1], int lo, int hi) {
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

static int range_contains_value(const int reference[KEY_SPACE + 1], int lo, int hi, int value) {
    if (lo > hi) {
        int tmp = lo;
        lo = hi;
        hi = tmp;
    }

    if (value == -1) {
        return expected_range_value(reference, lo, hi) == -1;
    }

    for (int k = lo; k <= hi; ++k) {
        if (reference[k] == value) {
            return 1;
        }
    }
    return 0;
}

static void run_fuzz(compression_algo_t algo, const char *label) {
    struct compression_config cfg = bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    cfg.algo = algo;
    cfg.default_sub_pages = 1;
    cfg.enable_lazy_compression = 0;
    btree_apply_qpl_env(&cfg);

    struct bplus_tree_compressed *ct = bplus_tree_compressed_init_with_config(16, 64, &cfg);
    require_true(ct != NULL, "failed to initialize compressed tree");

    int reference[KEY_SPACE + 1];
    for (int i = 0; i <= KEY_SPACE; ++i) {
        reference[i] = -1;
    }

    srand(1337 + algo);
    for (int op = 0; op < OPS; ++op) {
        int choice = rand() % 4;
        int key = 1 + (rand() % KEY_SPACE);

        if (choice <= 1) {
            int value = (rand() % 50000) + 1;
            require_true(bplus_tree_compressed_put(ct, key, value) == 0, "put failed");
            reference[key] = value;
        } else if (choice == 2) {
            int rc = bplus_tree_compressed_delete(ct, key);
            if (reference[key] == -1) {
                require_true(rc == -1, "delete missing key returned success");
            } else {
                require_true(rc == 0, "delete existing key failed");
                reference[key] = -1;
            }
            (void)rc;
        } else {
            int got = bplus_tree_compressed_get(ct, key);
            if (reference[key] == -1) {
                require_true(got == -1, "get missing key returned value");
            } else {
                require_true(got == reference[key], "get returned wrong value");
            }

            int a = 1 + (rand() % KEY_SPACE);
            int b = 1 + (rand() % KEY_SPACE);
            int expect = expected_range_value(reference, a, b);
            int range_val = bplus_tree_compressed_get_range(ct, a, b);
            if (!range_contains_value(reference, a, b, range_val)) {
                int lo = a < b ? a : b;
                int hi = a < b ? b : a;
                int matching_key = -1;
                for (int k = 1; k <= KEY_SPACE; k++) {
                    if (reference[k] == range_val) {
                        matching_key = k;
                        break;
                    }
                }
                int returned_key = -1;
                for (int k = hi; k >= lo; k--) {
                    if (bplus_tree_compressed_get(ct, k) == range_val) {
                        returned_key = k;
                        break;
                    }
                }
                fprintf(stderr,
                        "range returned value outside requested live range: codec=%s op=%d range=[%d,%d] expect=%d got=%d returned_key=%d ref_at_returned=%d matching_key=%d\n",
                        label,
                        op,
                        lo,
                        hi,
                        expect,
                        range_val,
                        returned_key,
                        returned_key >= 0 ? reference[returned_key] : -1,
                        matching_key);
                exit(EXIT_FAILURE);
            }
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
    run_fuzz(COMPRESS_ZLIB_ACCEL, "zlib_accel");
    return 0;
}
