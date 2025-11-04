#define _POSIX_C_SOURCE 200809L

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bplustree_compressed.h"

#define DEFAULT_KEY_COUNT 8192
#define DEFAULT_SAMPLE_COUNT 4096
#define DEFAULT_NUM_SUBPAGES 16
#define DEFAULT_WARMUP_COUNT 256
#define UPDATE_DELTA 17

struct stats_summary {
    double avg_ns;
    double max_ns;
    double p50_ns;
    double p90_ns;
    double p99_ns;
};

struct sample_record {
    double *prefetch_ns;
    double *update_ns;
    int count;
};

static void monotonic_now(struct timespec *ts)
{
    if (clock_gettime(CLOCK_MONOTONIC, ts) != 0) {
        perror("clock_gettime");
        exit(EXIT_FAILURE);
    }
}

static double elapsed_ns(const struct timespec *start, const struct timespec *end)
{
    return (double)(end->tv_sec - start->tv_sec) * 1e9 +
           (double)(end->tv_nsec - start->tv_nsec);
}

static int parse_env_int(const char *name, int fallback)
{
    const char *val = getenv(name);
    if (!val || *val == '\0') {
        return fallback;
    }
    char *endptr = NULL;
    long parsed = strtol(val, &endptr, 10);
    if (endptr == val || *endptr != '\0' || parsed <= 0) {
        fprintf(stderr, "Invalid value for %s: %s\n", name, val);
        exit(EXIT_FAILURE);
    }
    if (parsed > INT_MAX) {
        fprintf(stderr, "Value for %s too large: %ld\n", name, parsed);
        exit(EXIT_FAILURE);
    }
    return (int)parsed;
}

static int *parse_lz4_levels(int *out_count)
{
    const char *env = getenv("TAIL_LATENCY_LZ4_LEVELS");
    const char *source = (env && *env) ? env : "0,9,16";

    char *copy = strdup(source);
    if (!copy) {
        perror("strdup");
        exit(EXIT_FAILURE);
    }

    int capacity = 4;
    int count = 0;
    int *levels = malloc((size_t)capacity * sizeof(int));
    if (!levels) {
        perror("malloc");
        free(copy);
        exit(EXIT_FAILURE);
    }

    char *saveptr = NULL;
    for (char *token = strtok_r(copy, ",", &saveptr);
         token != NULL;
         token = strtok_r(NULL, ",", &saveptr)) {
        while (isspace((unsigned char)*token)) {
            token++;
        }
        if (*token == '\0') {
            continue;
        }

        char *endptr = NULL;
        long value = strtol(token, &endptr, 10);
        while (endptr && isspace((unsigned char)*endptr)) {
            endptr++;
        }
        if (!endptr || *endptr != '\0') {
            fprintf(stderr, "Invalid LZ4 level '%s' in %s\n", token, source);
            free(copy);
            free(levels);
            exit(EXIT_FAILURE);
        }
        if (value < INT_MIN || value > INT_MAX) {
            fprintf(stderr, "LZ4 level out of range: %ld\n", value);
            free(copy);
            free(levels);
            exit(EXIT_FAILURE);
        }

        if (count == capacity) {
            capacity *= 2;
            int *tmp = realloc(levels, (size_t)capacity * sizeof(int));
            if (!tmp) {
                perror("realloc");
                free(copy);
                free(levels);
                exit(EXIT_FAILURE);
            }
            levels = tmp;
        }
        levels[count++] = (int)value;
    }

    free(copy);

    if (count == 0) {
        fprintf(stderr, "No LZ4 levels parsed from '%s'\n", source);
        free(levels);
        exit(EXIT_FAILURE);
    }

    *out_count = count;
    return levels;
}

static void populate_tree(struct bplus_tree_compressed *tree,
                          const int *keys,
                          const int *values,
                          int count)
{
    for (int i = 0; i < count; i++) {
        int rc = bplus_tree_compressed_put(tree, (key_t)keys[i], values[i]);
        if (rc != 0) {
            fprintf(stderr, "populate_tree: put failed for key %d (rc=%d)\n", keys[i], rc);
            exit(EXIT_FAILURE);
        }
    }
}

static double percentile(const double *sorted, int count, double pct)
{
    if (count <= 0) {
        return 0.0;
    }
    double rank = (pct / 100.0) * (count - 1);
    int lower = (int)rank;
    int upper = lower + 1;
    double frac = rank - lower;
    if (upper >= count) {
        return sorted[count - 1];
    }
    return sorted[lower] + (sorted[upper] - sorted[lower]) * frac;
}

static int cmp_double(const void *a, const void *b)
{
    double da = *(const double *)a;
    double db = *(const double *)b;
    if (da < db) return -1;
    if (da > db) return 1;
    return 0;
}

static void compute_stats(const double *samples, int count, struct stats_summary *out)
{
    if (count <= 0) {
        memset(out, 0, sizeof(*out));
        return;
    }

    double sum = 0.0;
    double max = 0.0;
    for (int i = 0; i < count; i++) {
        sum += samples[i];
        if (samples[i] > max) {
            max = samples[i];
        }
    }

    double *sorted = malloc((size_t)count * sizeof(double));
    if (!sorted) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    memcpy(sorted, samples, (size_t)count * sizeof(double));
    qsort(sorted, (size_t)count, sizeof(double), cmp_double);

    out->avg_ns = sum / count;
    out->max_ns = max;
    out->p50_ns = percentile(sorted, count, 50.0);
    out->p90_ns = percentile(sorted, count, 90.0);
    out->p99_ns = percentile(sorted, count, 99.0);

    free(sorted);
}

static void summarize_and_print(const char *label, const struct sample_record *samples)
{
    struct stats_summary get_stats;
    struct stats_summary put_stats;

    compute_stats(samples->prefetch_ns, samples->count, &get_stats);
    compute_stats(samples->update_ns, samples->count, &put_stats);

    const double scale = 1e-3; // convert ns to µs for readability
    printf("%s latency (µs)\n", label);
    printf("  get : p50 %.3f  p90 %.3f  p99 %.3f  max %.3f  avg %.3f\n",
           get_stats.p50_ns * scale,
           get_stats.p90_ns * scale,
           get_stats.p99_ns * scale,
           get_stats.max_ns * scale,
           get_stats.avg_ns * scale);
    printf("  put : p50 %.3f  p90 %.3f  p99 %.3f  max %.3f  avg %.3f\n",
           put_stats.p50_ns * scale,
           put_stats.p90_ns * scale,
           put_stats.p99_ns * scale,
           put_stats.max_ns * scale,
           put_stats.avg_ns * scale);
}

static void warmup_tree(struct bplus_tree_compressed *tree,
                        const int *keys,
                        const int *baseline_values,
                        const int *updated_values,
                        int warmup_count)
{
    if (warmup_count <= 0) {
        return;
    }
    int iterations = warmup_count > 0 ? warmup_count : 0;
    for (int i = 0; i < iterations; i++) {
        key_t key = keys[i];
        (void)bplus_tree_compressed_get(tree, key);
        bplus_tree_compressed_put(tree, key, updated_values[i]);
        bplus_tree_compressed_put(tree, key, baseline_values[i]);
    }
}

static void measure_latencies(struct bplus_tree_compressed *tree,
                              const char *algo_label,
                              const int *keys,
                              const int *baseline_values,
                              const int *updated_values,
                              int sample_count,
                              struct sample_record *out_samples,
                              FILE *csv)
{
    out_samples->count = sample_count;
    out_samples->prefetch_ns = malloc((size_t)sample_count * sizeof(double));
    out_samples->update_ns = malloc((size_t)sample_count * sizeof(double));
    if (!out_samples->prefetch_ns || !out_samples->update_ns) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < sample_count; i++) {
        struct timespec start, end;

        monotonic_now(&start);
        int value = bplus_tree_compressed_get(tree, (key_t)keys[i]);
        monotonic_now(&end);
        double prefetch_ns = elapsed_ns(&start, &end);
        if (value != baseline_values[i]) {
            fprintf(stderr, "[%s] prefetch mismatch: key=%d expected=%d got=%d\n",
                    algo_label, keys[i], baseline_values[i], value);
            exit(EXIT_FAILURE);
        }

        monotonic_now(&start);
        int rc = bplus_tree_compressed_put(tree, (key_t)keys[i], updated_values[i]);
        monotonic_now(&end);
        double update_ns = elapsed_ns(&start, &end);
        if (rc != 0) {
            fprintf(stderr, "[%s] put failed for key=%d rc=%d\n", algo_label, keys[i], rc);
            exit(EXIT_FAILURE);
        }

        out_samples->prefetch_ns[i] = prefetch_ns;
        out_samples->update_ns[i] = update_ns;
        int verify = bplus_tree_compressed_get(tree, (key_t)keys[i]);
        if (verify != updated_values[i]) {
            fprintf(stderr, "[%s] verification mismatch: key=%d expected=%d got=%d\n",
                    algo_label, keys[i], updated_values[i], verify);
            exit(EXIT_FAILURE);
        }

        if (csv) {
            fprintf(csv, "%s,get,%d,%.0f\n", algo_label, i, prefetch_ns);
            fprintf(csv, "%s,put,%d,%.0f\n", algo_label, i, update_ns);
        }
    }
}

static void free_samples(struct sample_record *samples)
{
    free(samples->prefetch_ns);
    free(samples->update_ns);
    samples->prefetch_ns = NULL;
    samples->update_ns = NULL;
    samples->count = 0;
}

static void repopulate_baseline(struct bplus_tree_compressed *tree,
                                const int *keys,
                                const int *baseline_values,
                                int sample_count)
{
    for (int i = 0; i < sample_count; i++) {
        int rc = bplus_tree_compressed_put(tree, (key_t)keys[i], baseline_values[i]);
        if (rc != 0) {
            fprintf(stderr, "repopulate_baseline: put failed for key=%d rc=%d\n",
                    keys[i], rc);
            exit(EXIT_FAILURE);
        }
    }
}

static void format_lz4_label(int level, char *buffer, size_t buf_len)
{
    if (level == 0) {
        snprintf(buffer, buf_len, "LZ4(level=default)");
    } else if (level < 0) {
        snprintf(buffer, buf_len, "LZ4(accel=%d)", -level);
    } else {
        snprintf(buffer, buf_len, "LZ4(level=%d)", level);
    }
}

static void run_lz4_variant(int level,
                            int default_subpages,
                            const int *keys,
                            const int *values,
                            int key_count,
                            const int *sample_keys,
                            const int *sample_baseline,
                            const int *sample_updated,
                            int sample_count,
                            int warmup_iters,
                            FILE *csv)
{
    struct compression_config cfg = bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    cfg.algo = COMPRESS_LZ4;
    cfg.default_sub_pages = default_subpages;
    cfg.compression_level = level;

    struct bplus_tree_compressed *tree =
        bplus_tree_compressed_init_with_config(16, 32, &cfg);
    if (!tree) {
        fprintf(stderr, "Failed to initialize LZ4 tree (level=%d)\n", level);
        exit(EXIT_FAILURE);
    }

    populate_tree(tree, keys, values, key_count);

    if (warmup_iters > 0) {
        warmup_tree(tree, sample_keys, sample_baseline, sample_updated, warmup_iters);
        repopulate_baseline(tree, sample_keys, sample_baseline, sample_count);
    }

    struct sample_record samples = {0};
    char label[64];
    format_lz4_label(level, label, sizeof(label));

    measure_latencies(tree,
                      label,
                      sample_keys,
                      sample_baseline,
                      sample_updated,
                      sample_count,
                      &samples,
                      csv);

    summarize_and_print(label, &samples);
    free_samples(&samples);
    bplus_tree_compressed_deinit(tree);
}
int main(void)
{
    int key_count = parse_env_int("TAIL_LATENCY_KEY_COUNT", DEFAULT_KEY_COUNT);
    int sample_count = parse_env_int("TAIL_LATENCY_SAMPLE_COUNT", DEFAULT_SAMPLE_COUNT);
    int warmup_count = parse_env_int("TAIL_LATENCY_WARMUP", DEFAULT_WARMUP_COUNT);
    int default_subpages = parse_env_int("TAIL_LATENCY_SUBPAGES", DEFAULT_NUM_SUBPAGES);

    if (sample_count > key_count) {
        fprintf(stderr, "sample_count (%d) cannot exceed key_count (%d)\n",
                sample_count, key_count);
        return EXIT_FAILURE;
    }

    int *keys = calloc((size_t)key_count, sizeof(int));
    int *values = calloc((size_t)key_count, sizeof(int));
    if (!keys || !values) {
        perror("calloc");
        free(keys);
        free(values);
        return EXIT_FAILURE;
    }

    for (int i = 0; i < key_count; i++) {
        keys[i] = i + 1;
        values[i] = (i + 1) * 5;
    }

    int *sample_keys = calloc((size_t)sample_count, sizeof(int));
    int *sample_baseline = calloc((size_t)sample_count, sizeof(int));
    int *sample_updated = calloc((size_t)sample_count, sizeof(int));
    if (!sample_keys || !sample_baseline || !sample_updated) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

    int offset = key_count - sample_count;
    for (int i = 0; i < sample_count; i++) {
        int idx = offset + i;
        sample_keys[i] = keys[idx];
        sample_baseline[i] = values[idx];
        sample_updated[i] = values[idx] + UPDATE_DELTA;
    }

    int warmup_iters = warmup_count;
    if (warmup_iters > sample_count) {
        warmup_iters = sample_count;
    }

    const char *csv_path = getenv("TAIL_LATENCY_CSV");
    FILE *csv = NULL;
    if (csv_path && *csv_path) {
        csv = fopen(csv_path, "w");
        if (!csv) {
            fprintf(stderr, "Failed to open CSV path '%s': %s\n",
                    csv_path, strerror(errno));
            exit(EXIT_FAILURE);
        }
        fprintf(csv, "algorithm,op,index,latency_ns\n");
    }

    int level_count = 0;
    int *lz4_levels = parse_lz4_levels(&level_count);
    for (int i = 0; i < level_count; i++) {
        run_lz4_variant(lz4_levels[i],
                        default_subpages,
                        keys,
                        values,
                        key_count,
                        sample_keys,
                        sample_baseline,
                        sample_updated,
                        sample_count,
                        warmup_iters,
                        csv);
    }

    free(lz4_levels);

    struct compression_config qpl_cfg = bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    qpl_cfg.algo = COMPRESS_QPL;
    qpl_cfg.default_sub_pages = default_subpages;

    struct bplus_tree_compressed *qpl_tree =
        bplus_tree_compressed_init_with_config(16, 32, &qpl_cfg);
    if (!qpl_tree) {
        fprintf(stderr, "Failed to initialize QPL tree\n");
        exit(EXIT_FAILURE);
    }

    populate_tree(qpl_tree, keys, values, key_count);

    if (warmup_iters > 0) {
        warmup_tree(qpl_tree, sample_keys, sample_baseline, sample_updated, warmup_iters);
        repopulate_baseline(qpl_tree, sample_keys, sample_baseline, sample_count);
    }

    struct sample_record qpl_samples = {0};

    measure_latencies(qpl_tree, "QPL",
                      sample_keys, sample_baseline, sample_updated,
                      sample_count, &qpl_samples, csv);

    if (csv) {
        fclose(csv);
        printf("CSV written to %s\n", csv_path);
    }
    summarize_and_print("QPL", &qpl_samples);

    if (qpl_tree->qpl_job_ptr == NULL) {
        printf("Note: QPL initialization failed; results reflect LZ4 fallback.\n");
    }

    free_samples(&qpl_samples);
    bplus_tree_compressed_deinit(qpl_tree);
    free(sample_keys);
    free(sample_baseline);
    free(sample_updated);
    free(keys);
    free(values);
    return EXIT_SUCCESS;
}
