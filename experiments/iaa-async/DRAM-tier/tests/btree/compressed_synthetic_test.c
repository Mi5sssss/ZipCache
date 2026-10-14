#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bplustree_compressed.h"

#define VALUE_SIZE           64
#define NUM_KEYS             8192  // Increased to force more data per leaf
#define NUM_LOOKUPS          512
#define NUM_DIRECT_SAMPLES   512

typedef struct {
    const char *name;
    int random_bytes;
    int zero_bytes;
} scenario_t;

static const scenario_t scenarios[] = {
    {"low_compress", 40, 24},
    {"medium_compress", 32, 32},
    {"high_compress", 16, 48},
};

static double monotonic_seconds(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void fill_value(char *buf, int random_bytes, int zero_bytes)
{
    for (int i = 0; i < random_bytes; ++i) {
        buf[i] = (char)(rand() & 0xFF);
    }
    memset(buf + random_bytes, 0, zero_bytes);
}

static int hash_value(const char *buf)
{
    int h = 5381;
    for (int i = 0; i < VALUE_SIZE; ++i) {
        h = ((h << 5) + h) + (unsigned char)buf[i];
    }
    if (h < 0) {
        h = -h;
    }
    return h + 1;
}

static int compare_double(const void *a, const void *b)
{
    double da = *(const double *)a;
    double db = *(const double *)b;
    if (da < db) return -1;
    if (da > db) return 1;
    return 0;
}

static double calculate_p99(double *latencies, size_t count)
{
    if (count == 0) {
        return 0.0;
    }
    qsort(latencies, count, sizeof(double), compare_double);
    size_t idx = (size_t)ceil(0.99 * count);
    if (idx >= count) {
        idx = count - 1;
    }
    return latencies[idx] * 1e6; // microseconds
}

static double direct_compression_ratio(const scenario_t *sc, compression_algo_t algo)
{
    char input[VALUE_SIZE];
    char output[VALUE_SIZE * 2];
    size_t total_original = 0;
    size_t total_compressed = 0;

    qpl_job *job = NULL;
    uint8_t *job_buffer = NULL;
    if (algo == COMPRESS_QPL) {
        uint32_t job_size;
        if (qpl_get_job_size(qpl_path_auto, &job_size) == QPL_STS_OK) {
            job_buffer = malloc(job_size);
            if (job_buffer) {
                job = (qpl_job *)job_buffer;
                if (qpl_init_job(qpl_path_auto, job) != QPL_STS_OK) {
                    free(job_buffer);
                    job_buffer = NULL;
                    job = NULL;
                }
            }
        }
    }

    for (int i = 0; i < NUM_DIRECT_SAMPLES; ++i) {
        fill_value(input, sc->random_bytes, sc->zero_bytes);
        int compressed = 0;
        if (algo == COMPRESS_QPL && job) {
            job->op = qpl_op_compress;
            job->next_in_ptr = (uint8_t *)input;
            job->available_in = VALUE_SIZE;
            job->next_out_ptr = (uint8_t *)output;
            job->available_out = VALUE_SIZE * 2;
            job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
            job->level = qpl_default_level;
            if (qpl_execute_job(job) == QPL_STS_OK) {
                compressed = job->total_out;
            }
        }
        if (compressed <= 0) {
            compressed = LZ4_compress_default(input, output, VALUE_SIZE, VALUE_SIZE * 2);
        }
        if (compressed > 0) {
            total_original += VALUE_SIZE;
            total_compressed += (size_t)compressed;
        }
    }

    if (job) {
        qpl_fini_job(job);
    }
    free(job_buffer);

    if (total_compressed == 0) {
        return 0.0;
    }
    return (double)total_original / (double)total_compressed;
}

static void run_scenario(const scenario_t *sc, compression_algo_t algo)
{
    const char *algo_name = (algo == COMPRESS_QPL) ? "QPL" : "LZ4";
    printf("synthetic[%s][%s]\n", sc->name, algo_name);

    struct compression_config cfg = bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    cfg.algo = algo;
    cfg.default_sub_pages = 16;
    cfg.enable_lazy_compression = 0;
    cfg.buffer_size = 256;  // Reduce landing buffer to 256 bytes = 32 key-value pairs to trigger compression sooner

    struct bplus_tree_compressed *tree = bplus_tree_compressed_init_with_config(16, 64, &cfg);
    if (!tree) {
        fprintf(stderr, "failed to init tree for %s/%s\n", sc->name, algo_name);
        exit(EXIT_FAILURE);
    }

    // Enable debug mode to see compression/split events (set to 1 to enable)
    bplus_tree_compressed_set_debug(tree, 0);

    double direct_ratio = direct_compression_ratio(sc, algo);

    int expected[NUM_KEYS];
    for (int i = 0; i < NUM_KEYS; ++i) {
        expected[i] = -1;
    }

    double *latencies = malloc(NUM_KEYS * sizeof(double));
    if (!latencies) {
        fprintf(stderr, "latency allocation failed\n");
        exit(EXIT_FAILURE);
    }

    char value_buf[VALUE_SIZE];
    double total_insert_time = 0.0;
    srand(1234 + algo);

    // Insert keys in dense sequential blocks to fill landing buffers before tree splits
    // This forces compression by overflowing the 64-entry landing buffer
    for (int key = 1; key <= NUM_KEYS; ++key) {
        fill_value(value_buf, sc->random_bytes, sc->zero_bytes);
        int val = hash_value(value_buf);
        double t0 = monotonic_seconds();
        if (bplus_tree_compressed_put(tree, key, val) != 0) {
            fprintf(stderr, "put failed [%s][%s] key=%d\n", sc->name, algo_name, key);
            exit(EXIT_FAILURE);
        }
        double t1 = monotonic_seconds();
        latencies[key - 1] = t1 - t0;
        total_insert_time += latencies[key - 1];
        expected[key - 1] = val;
        int check = bplus_tree_compressed_get(tree, key);
        if (check != val) {
            fprintf(stderr, "post-insert mismatch [%s][%s] key=%d expected=%d got=%d\n",
                    sc->name, algo_name, key, val, check);
            exit(EXIT_FAILURE);
        }

        // Sample compression stats every 500 keys
        if (key % 500 == 0) {
            size_t mid_uncompressed = 0;
            size_t mid_compressed = 0;
            bplus_tree_compressed_calculate_stats(tree, &mid_uncompressed, &mid_compressed);
            if (mid_compressed > 0) {
                double mid_ratio = (double)mid_uncompressed / (double)mid_compressed;
                printf("    [key=%d: ratio=%.3f uncomp=%zu comp=%zu]\n",
                       key, mid_ratio, mid_uncompressed, mid_compressed);
            }
        }
    }

    for (int iter = 0; iter < NUM_LOOKUPS; ++iter) {
        int key = 1 + (rand() % NUM_KEYS);
        int got = bplus_tree_compressed_get(tree, key);
        if (got != expected[key - 1]) {
            fprintf(stderr, "get mismatch [%s][%s] key=%d expected=%d got=%d\n",
                    sc->name, algo_name, key, expected[key - 1], got);
            exit(EXIT_FAILURE);
        }
    }

    for (int iter = 0; iter < NUM_LOOKUPS; ++iter) {
        int a = 1 + (rand() % NUM_KEYS);
        int b = 1 + (rand() % NUM_KEYS);
        int lo = a < b ? a : b;
        int hi = a < b ? b : a;
        int last = -1;
        for (int k = lo; k <= hi; ++k) {
            if (expected[k - 1] != -1) {
                last = expected[k - 1];
            }
        }
        int range_val = bplus_tree_compressed_get_range(tree, lo, hi);
        if (range_val != last) {
            fprintf(stderr, "range mismatch [%s][%s] [%d,%d] expected=%d got=%d\n",
                    sc->name, algo_name, lo, hi, last, range_val);
            exit(EXIT_FAILURE);
        }
    }

    size_t total_uncompressed = 0;
    size_t total_compressed = 0;
    assert(bplus_tree_compressed_calculate_stats(tree, &total_uncompressed, &total_compressed) == 0);
    double tree_ratio = (total_compressed > 0)
                        ? (double)total_uncompressed / (double)total_compressed
                        : 1.0;

    printf("  [DEBUG: uncompressed=%zu compressed=%zu]\n", total_uncompressed, total_compressed);
    double throughput = total_insert_time > 0.0 ? (double)NUM_KEYS / total_insert_time : 0.0;
    double p99_us = calculate_p99(latencies, NUM_KEYS);

    printf("  direct_ratio=%.3f tree_ratio=%.3f throughput=%.1f ops/s p99=%.0f us\n",
           direct_ratio,
           tree_ratio,
           throughput,
           p99_us);

    if (tree_ratio < 1.0) {
        fprintf(stderr, "tree ratio unexpectedly low [%s][%s]: %.3f\n", sc->name, algo_name, tree_ratio);
        exit(EXIT_FAILURE);
    }

    free(latencies);
    bplus_tree_compressed_deinit(tree);
}

int main(void)
{
    for (size_t i = 0; i < sizeof(scenarios) / sizeof(scenarios[0]); ++i) {
        run_scenario(&scenarios[i], COMPRESS_LZ4);
        run_scenario(&scenarios[i], COMPRESS_QPL);
    }
    printf("compressed_synthetic_test: OK\n");
    return 0;
}
