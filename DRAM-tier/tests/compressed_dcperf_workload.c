#define _POSIX_C_SOURCE 200809L

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bplustree_compressed.h"

#define DEFAULT_KEY_COUNT 65536
#define DEFAULT_SAMPLE_COUNT 100000
#define DEFAULT_NUM_SUBPAGES 16
#define DEFAULT_WARMUP_COUNT 4096
#define DEFAULT_HIT_RATIO 0.88
#define DEFAULT_UPDATE_RATIO 0.07
#define DEFAULT_INSERT_RATIO 0.05
#define UPDATE_DELTA 23

struct stats_summary {
    double avg_ns;
    double max_ns;
    double p50_ns;
    double p90_ns;
    double p99_ns;
};

struct sample_record {
    double *get_ns;
    double *put_ns;
    int get_count;
    int put_count;
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

static double parse_env_double(const char *name, double fallback)
{
    const char *val = getenv(name);
    if (!val || *val == '\0') {
        return fallback;
    }
    char *endptr = NULL;
    double parsed = strtod(val, &endptr);
    if (endptr == val || *endptr != '\0' || parsed < 0.0) {
        fprintf(stderr, "Invalid value for %s: %s\n", name, val);
        exit(EXIT_FAILURE);
    }
    return parsed;
}

static int *parse_lz4_levels(int *out_count)
{
    const char *env = getenv("DC_LZ4_LEVELS");
    const char *source = (env && *env) ? env : "0";

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

static int parse_json_array(const char *path,
                            const char *key,
                            int **out_values,
                            int *out_count)
{
    FILE *fp = fopen(path, "r");
    if (!fp) {
        fprintf(stderr, "Failed to open %s: %s\n", path, strerror(errno));
        return -1;
    }

    if (fseek(fp, 0, SEEK_END) != 0) {
        perror("fseek");
        fclose(fp);
        return -1;
    }
    long length = ftell(fp);
    if (length < 0) {
        perror("ftell");
        fclose(fp);
        return -1;
    }
    if (fseek(fp, 0, SEEK_SET) != 0) {
        perror("fseek");
        fclose(fp);
        return -1;
    }

    char *buffer = malloc((size_t)length + 1);
    if (!buffer) {
        perror("malloc");
        fclose(fp);
        return -1;
    }
    size_t read = fread(buffer, 1, (size_t)length, fp);
    fclose(fp);
    buffer[read] = '\0';

    char needle[128];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    char *key_start = strstr(buffer, needle);
    if (!key_start) {
        fprintf(stderr, "Key %s not found in %s\n", key, path);
        free(buffer);
        return -1;
    }

    char *cursor = strchr(key_start, '[');
    if (!cursor) {
        fprintf(stderr, "Malformed array for %s in %s\n", key, path);
        free(buffer);
        return -1;
    }
    cursor++; // move past '['

    int capacity = 8;
    int count = 0;
    int *values = malloc((size_t)capacity * sizeof(int));
    if (!values) {
        perror("malloc");
        free(buffer);
        return -1;
    }

    while (*cursor) {
        while (*cursor && isspace((unsigned char)*cursor)) {
            cursor++;
        }
        if (*cursor == ']') {
            cursor++;
            break;
        }
        char *endptr = NULL;
        long value = strtol(cursor, &endptr, 10);
        if (endptr == cursor) {
            fprintf(stderr, "Failed to parse integer in %s near '%.16s'\n", path, cursor);
            free(values);
            free(buffer);
            return -1;
        }
        if (value < INT_MIN || value > INT_MAX) {
            fprintf(stderr, "Value out of range in %s: %ld\n", path, value);
            free(values);
            free(buffer);
            return -1;
        }
        if (count == capacity) {
            capacity *= 2;
            int *tmp = realloc(values, (size_t)capacity * sizeof(int));
            if (!tmp) {
                perror("realloc");
                free(values);
                free(buffer);
                return -1;
            }
            values = tmp;
        }
        values[count++] = (int)value;
        cursor = endptr;
        while (*cursor && *cursor != ',' && *cursor != ']') {
            cursor++;
        }
        if (*cursor == ',') {
            cursor++;
        } else if (*cursor == ']') {
            cursor++;
            break;
        }
    }

    free(buffer);

    if (count == 0) {
        fprintf(stderr, "Key %s empty in %s\n", key, path);
        free(values);
        return -1;
    }

    *out_values = values;
    *out_count = count;
    return 0;
}

static void load_dcperf_distribution(const char *root_path,
                                     int **size_ranges_out,
                                     double **cdf_out,
                                     int *bucket_count_out)
{
    char path[512];
    snprintf(path, sizeof(path),
             "%s/DCPerf/packages/tao_bench/db_items.json", root_path);

    int *prob_values = NULL;
    int prob_count = 0;
    if (parse_json_array(path, "valSizeRangeProbability", &prob_values, &prob_count) != 0) {
        exit(EXIT_FAILURE);
    }

    int *size_ranges = NULL;
    int range_count = 0;
    if (parse_json_array(path, "valSizeRange", &size_ranges, &range_count) != 0) {
        free(prob_values);
        exit(EXIT_FAILURE);
    }

    if (range_count != prob_count + 1) {
        fprintf(stderr, "Unexpected range/probability count in %s (prob=%d range=%d)\n",
                path, prob_count, range_count);
        free(prob_values);
        free(size_ranges);
        exit(EXIT_FAILURE);
    }

    double *cdf = malloc((size_t)prob_count * sizeof(double));
    if (!cdf) {
        perror("malloc");
        free(prob_values);
        free(size_ranges);
        exit(EXIT_FAILURE);
    }

    long long total = 0;
    for (int i = 0; i < prob_count; i++) {
        total += prob_values[i];
    }
    if (total <= 0) {
        fprintf(stderr, "Invalid probability totals in %s\n", path);
        free(prob_values);
        free(size_ranges);
        free(cdf);
        exit(EXIT_FAILURE);
    }

    double cumulative = 0.0;
    for (int i = 0; i < prob_count; i++) {
        cumulative += (double)prob_values[i] / (double)total;
        if (cumulative > 1.0) {
            cumulative = 1.0;
        }
        cdf[i] = cumulative;
    }

    free(prob_values);

    *size_ranges_out = size_ranges;
    *cdf_out = cdf;
    *bucket_count_out = prob_count;
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

static void summarize(const char *label, const struct sample_record *samples)
{
    struct stats_summary get_stats;
    struct stats_summary put_stats;

    compute_stats(samples->get_ns, samples->get_count, &get_stats);
    compute_stats(samples->put_ns, samples->put_count, &put_stats);

    const double scale = 1e-3;
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

static int sample_bucket(const double *cdf, int count)
{
    double r = (double)rand() / (double)RAND_MAX;
    for (int i = 0; i < count; i++) {
        if (r <= cdf[i]) {
            return i;
        }
    }
    return count - 1;
}

static int sample_value_size(const int *ranges, const double *cdf, int buckets)
{
    int bucket = sample_bucket(cdf, buckets);
    int low = ranges[bucket];
    int high = ranges[bucket + 1];
    if (high <= low) {
        return low;
    }
    int span = high - low;
    return low + (rand() % span);
}

static void run_lz4_variant(int level,
                            int default_subpages,
                            const int *keys,
                            const int *values,
                            int base_key_count,
                            const int *sample_keys_init,
                            int key_capacity,
                            const int *size_ranges,
                            const double *cdf,
                            int bucket_count,
                            int sample_count,
                            int warmup_iters,
                            double hit_ratio,
                            double update_ratio,
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

    for (int i = 0; i < base_key_count; i++) {
        int rc = bplus_tree_compressed_put(tree, (key_t)keys[i], values[i]);
        if (rc != 0) {
            fprintf(stderr, "Initial put failed for key=%d rc=%d\n", keys[i], rc);
            exit(EXIT_FAILURE);
        }
    }

    int *dynamic_keys = malloc((size_t)key_capacity * sizeof(int));
    if (!dynamic_keys) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    memcpy(dynamic_keys, sample_keys_init, (size_t)key_capacity * sizeof(int));
    int current_keys = base_key_count;
    int next_key = dynamic_keys[current_keys - 1] + 1;

    if (warmup_iters > current_keys) {
        warmup_iters = current_keys;
    }
    for (int i = 0; i < warmup_iters; i++) {
        key_t key = dynamic_keys[rand() % current_keys];
        (void)bplus_tree_compressed_get(tree, key);
        int value = sample_value_size(size_ranges, cdf, bucket_count);
        bplus_tree_compressed_put(tree, key, value);
    }

    struct sample_record samples = {0};
    samples.get_ns = malloc((size_t)sample_count * sizeof(double));
    samples.put_ns = malloc((size_t)sample_count * sizeof(double));
    if (!samples.get_ns || !samples.put_ns) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    char label[64];
    if (level == 0) {
        snprintf(label, sizeof(label), "LZ4(level=default)");
    } else if (level < 0) {
        snprintf(label, sizeof(label), "LZ4(accel=%d)", -level);
    } else {
        snprintf(label, sizeof(label), "LZ4(level=%d)", level);
    }

    for (int i = 0; i < sample_count; i++) {
        double r = (double)rand() / (double)RAND_MAX;
        struct timespec start, end;
        if (r < hit_ratio && current_keys > 0) {
            key_t key = dynamic_keys[rand() % current_keys];
            monotonic_now(&start);
            int value = bplus_tree_compressed_get(tree, key);
            monotonic_now(&end);
            double get_ns = elapsed_ns(&start, &end);
            if (value < 0) {
                fprintf(stderr, "Unexpected miss for key=%d\n", key);
                exit(EXIT_FAILURE);
            }
            samples.get_ns[samples.get_count++] = get_ns;
        } else if (r < hit_ratio + update_ratio && current_keys > 0) {
            key_t key = dynamic_keys[rand() % current_keys];
            int value = sample_value_size(size_ranges, cdf, bucket_count);
            monotonic_now(&start);
            int rc = bplus_tree_compressed_put(tree, key, value);
            monotonic_now(&end);
            double put_ns = elapsed_ns(&start, &end);
            if (rc != 0) {
                fprintf(stderr, "update failed for key=%d rc=%d\n", key, rc);
                exit(EXIT_FAILURE);
            }
            samples.put_ns[samples.put_count++] = put_ns;
        } else {
            int value = sample_value_size(size_ranges, cdf, bucket_count);
            key_t key = next_key++;
            monotonic_now(&start);
            int rc = bplus_tree_compressed_put(tree, key, value);
            monotonic_now(&end);
            double put_ns = elapsed_ns(&start, &end);
            if (rc != 0) {
                fprintf(stderr, "insert failed for key=%d rc=%d\n", key, rc);
                exit(EXIT_FAILURE);
            }
            if (current_keys < key_capacity) {
                dynamic_keys[current_keys++] = (int)key;
            }
            samples.put_ns[samples.put_count++] = put_ns;
        }
    }

    if (csv) {
        for (int i = 0; i < samples.get_count; i++) {
            fprintf(csv, "%s,get,%d,%.0f\n", label, i, samples.get_ns[i]);
        }
        for (int i = 0; i < samples.put_count; i++) {
            fprintf(csv, "%s,put,%d,%.0f\n", label, i, samples.put_ns[i]);
        }
    }

    summarize(label, &samples);

    free(samples.get_ns);
    free(samples.put_ns);
    free(dynamic_keys);
    bplus_tree_compressed_deinit(tree);
}

static void run_qpl_variant(int default_subpages,
                            const int *keys,
                            const int *values,
                            int base_key_count,
                            const int *sample_keys_init,
                            int key_capacity,
                            const int *size_ranges,
                            const double *cdf,
                            int bucket_count,
                            int sample_count,
                            int warmup_iters,
                            double hit_ratio,
                            double update_ratio,
                            FILE *csv)
{
    struct compression_config cfg = bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    cfg.algo = COMPRESS_QPL;
    cfg.default_sub_pages = default_subpages;

    struct bplus_tree_compressed *tree =
        bplus_tree_compressed_init_with_config(16, 32, &cfg);
    if (!tree) {
        fprintf(stderr, "Failed to initialize QPL tree\n");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < base_key_count; i++) {
        int rc = bplus_tree_compressed_put(tree, (key_t)keys[i], values[i]);
        if (rc != 0) {
            fprintf(stderr, "Initial put failed for key=%d rc=%d\n", keys[i], rc);
            exit(EXIT_FAILURE);
        }
    }

    int *dynamic_keys = malloc((size_t)key_capacity * sizeof(int));
    if (!dynamic_keys) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    memcpy(dynamic_keys, sample_keys_init, (size_t)key_capacity * sizeof(int));
    int current_keys = base_key_count;
    int next_key = dynamic_keys[current_keys - 1] + 1;

    if (warmup_iters > current_keys) {
        warmup_iters = current_keys;
    }
    for (int i = 0; i < warmup_iters; i++) {
        key_t key = dynamic_keys[rand() % current_keys];
        (void)bplus_tree_compressed_get(tree, key);
        int value = sample_value_size(size_ranges, cdf, bucket_count);
        bplus_tree_compressed_put(tree, key, value);
    }

    struct sample_record samples = {0};
    samples.get_ns = malloc((size_t)sample_count * sizeof(double));
    samples.put_ns = malloc((size_t)sample_count * sizeof(double));
    if (!samples.get_ns || !samples.put_ns) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    const char *label = "QPL";

    for (int i = 0; i < sample_count; i++) {
        double r = (double)rand() / (double)RAND_MAX;
        struct timespec start, end;
        if (r < hit_ratio && current_keys > 0) {
            key_t key = dynamic_keys[rand() % current_keys];
            monotonic_now(&start);
            int value = bplus_tree_compressed_get(tree, key);
            monotonic_now(&end);
            double get_ns = elapsed_ns(&start, &end);
            if (value < 0) {
                fprintf(stderr, "Unexpected miss for key=%d\n", key);
                exit(EXIT_FAILURE);
            }
            samples.get_ns[samples.get_count++] = get_ns;
        } else if (r < hit_ratio + update_ratio && current_keys > 0) {
            key_t key = dynamic_keys[rand() % current_keys];
            int value = sample_value_size(size_ranges, cdf, bucket_count);
            monotonic_now(&start);
            int rc = bplus_tree_compressed_put(tree, key, value);
            monotonic_now(&end);
            double put_ns = elapsed_ns(&start, &end);
            if (rc != 0) {
                fprintf(stderr, "update failed for key=%d rc=%d\n", key, rc);
                exit(EXIT_FAILURE);
            }
            samples.put_ns[samples.put_count++] = put_ns;
        } else {
            int value = sample_value_size(size_ranges, cdf, bucket_count);
            key_t key = next_key++;
            monotonic_now(&start);
            int rc = bplus_tree_compressed_put(tree, key, value);
            monotonic_now(&end);
            double put_ns = elapsed_ns(&start, &end);
            if (rc != 0) {
                fprintf(stderr, "insert failed for key=%d rc=%d\n", key, rc);
                exit(EXIT_FAILURE);
            }
            if (current_keys < key_capacity) {
                dynamic_keys[current_keys++] = (int)key;
            }
            samples.put_ns[samples.put_count++] = put_ns;
        }
    }

    if (csv) {
        for (int i = 0; i < samples.get_count; i++) {
            fprintf(csv, "%s,get,%d,%.0f\n", label, i, samples.get_ns[i]);
        }
        for (int i = 0; i < samples.put_count; i++) {
            fprintf(csv, "%s,put,%d,%.0f\n", label, i, samples.put_ns[i]);
        }
    }

    summarize(label, &samples);

    free(samples.get_ns);
    free(samples.put_ns);
    free(dynamic_keys);
    bplus_tree_compressed_deinit(tree);
}

int main(void)
{
    srand(42);

    const char *root = getenv("BPLUSTREE_ROOT");
    if (!root || !*root) {
        root = "..";
    }

    int *size_ranges = NULL;
    double *cdf = NULL;
    int bucket_count = 0;
    load_dcperf_distribution(root, &size_ranges, &cdf, &bucket_count);

    int key_count = parse_env_int("DC_KEY_COUNT", DEFAULT_KEY_COUNT);
    int sample_count = parse_env_int("DC_SAMPLE_COUNT", DEFAULT_SAMPLE_COUNT);
    int warmup_count = parse_env_int("DC_WARMUP_COUNT", DEFAULT_WARMUP_COUNT);
    int default_subpages = parse_env_int("DC_SUBPAGES", DEFAULT_NUM_SUBPAGES);

    double hit_ratio = parse_env_double("DC_HIT_RATIO", DEFAULT_HIT_RATIO);
    double update_ratio = parse_env_double("DC_UPDATE_RATIO", DEFAULT_UPDATE_RATIO);
    double insert_ratio = parse_env_double("DC_INSERT_RATIO", DEFAULT_INSERT_RATIO);

    double sum_ratio = hit_ratio + update_ratio + insert_ratio;
    if (fabs(sum_ratio - 1.0) > 1e-6) {
        fprintf(stderr, "Hit/update/insert ratios must sum to 1.0 (current %.6f)\n", sum_ratio);
        exit(EXIT_FAILURE);
    }

    int *keys = malloc((size_t)(key_count + sample_count + 1) * sizeof(int));
    int *values = malloc((size_t)(key_count + sample_count + 1) * sizeof(int));
    if (!keys || !values) {
        perror("malloc");
        return EXIT_FAILURE;
    }

    for (int i = 0; i < key_count; i++) {
        keys[i] = i + 1;
        values[i] = sample_value_size(size_ranges, cdf, bucket_count);
    }

    int level_count = 0;
    int *levels = parse_lz4_levels(&level_count);

    const char *csv_path = getenv("DC_LATENCY_CSV");
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

    for (int i = 0; i < level_count; i++) {
        run_lz4_variant(levels[i],
                        default_subpages,
                        keys,
                        values,
                        key_count,
                        keys,
                        key_count + sample_count,
                        size_ranges,
                        cdf,
                        bucket_count,
                        sample_count,
                        warmup_count,
                        hit_ratio,
                        update_ratio,
                        csv);
    }

    run_qpl_variant(default_subpages,
                    keys,
                    values,
                    key_count,
                    keys,
                    key_count + sample_count,
                    size_ranges,
                    cdf,
                    bucket_count,
                    sample_count,
                    warmup_count,
                    hit_ratio,
                    update_ratio,
                    csv);

    if (csv) {
        fclose(csv);
        printf("CSV written to %s\n", csv_path);
    }

    free(levels);
    free(keys);
    free(values);
    free(size_ranges);
    free(cdf);
    return EXIT_SUCCESS;
}
