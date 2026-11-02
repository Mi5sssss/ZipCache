#define _POSIX_C_SOURCE 200809L

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>
#include <sys/wait.h>
#include <unistd.h>
#include <time.h>

#ifdef HAVE_ZLIB
#include <zlib.h>
#endif

#include "bplustree_compressed.h"
#include "compressed_test_utils.h"

#define DEFAULT_KEY_COUNT 8192*16
#define DEFAULT_SAMPLE_COUNT 4096*16
#define DEFAULT_NUM_SUBPAGES 1
#define DEFAULT_WARMUP_COUNT 25600
#define UPDATE_DELTA 17
#define SAMBA_KEY_BYTES 16
#define SAMBA_CHUNK_BYTES 32

struct stats_summary {
    double avg_ns;
    double max_ns;
    double p50_ns;
    double p90_ns;
    double p99_ns;
    double p999_ns;
};

struct sample_record {
    double *prefetch_ns;
    double *update_ns;
    int get_count;
    int put_count;
    double total_ns;
    int total_ops;
};

struct workload_mix {
    const char *label;
    double write_ratio; // read ratio is (1 - write_ratio)
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

static int parse_env_bool(const char *name, int fallback)
{
    const char *val = getenv(name);
    if (!val || *val == '\0') {
        return fallback;
    }
    if (strcasecmp(val, "1") == 0 || strcasecmp(val, "true") == 0 ||
        strcasecmp(val, "yes") == 0 || strcasecmp(val, "y") == 0) {
        return 1;
    }
    if (strcasecmp(val, "0") == 0 || strcasecmp(val, "false") == 0 ||
        strcasecmp(val, "no") == 0 || strcasecmp(val, "n") == 0) {
        return 0;
    }
    fprintf(stderr, "Invalid boolean for %s: %s (use 0/1, true/false)\n", name, val);
    exit(EXIT_FAILURE);
}

struct samba_dataset {
    uint8_t *data;
    size_t chunk_count;
    size_t chunk_bytes;
};

static const char *resolve_samba_zip(char *buffer, size_t buf_len)
{
    const char *override = getenv("SAMBA_ZIP_PATH");
    if (override && *override) {
        if (access(override, R_OK) == 0) {
            return override;
        }
        fprintf(stderr, "SAMBA_ZIP_PATH set to %s but not readable\n", override);
    }

    const char *dir = getenv("SILESIA_CORPUS_DIR");
    if (dir && *dir) {
        snprintf(buffer, buf_len, "%s/%s", dir, "samba.zip");
        if (access(buffer, R_OK) == 0) {
            return buffer;
        }
    }

    const char *candidates[] = {
        "SilesiaCorpus/samba.zip",
        "../SilesiaCorpus/samba.zip",
        "../../SilesiaCorpus/samba.zip",
        "../../../SilesiaCorpus/samba.zip",
        "../../../../SilesiaCorpus/samba.zip"
    };

    for (size_t i = 0; i < sizeof(candidates) / sizeof(candidates[0]); i++) {
        if (access(candidates[i], R_OK) == 0) {
            snprintf(buffer, buf_len, "%s", candidates[i]);
            return buffer;
        }
    }

    return NULL;
}

static int load_samba_dataset(size_t max_chunks, struct samba_dataset *out)
{
    char path[PATH_MAX];
    const char *resolved = resolve_samba_zip(path, sizeof(path));
    if (!resolved) {
        fprintf(stderr, "Unable to locate SilesiaCorpus/samba.zip (override with SAMBA_ZIP_PATH or SILESIA_CORPUS_DIR)\n");
        return -1;
    }

    char cmd[PATH_MAX + 32];
    snprintf(cmd, sizeof(cmd), "unzip -p \"%s\" samba", resolved);
    FILE *pipe = popen(cmd, "r");
    if (!pipe) {
        perror("popen unzip");
        return -1;
    }

    size_t capacity = SAMBA_CHUNK_BYTES * (max_chunks > 0 ? max_chunks : 1024);
    if (capacity < 64 * 1024) {
        capacity = 64 * 1024;
    }

    uint8_t *buffer = malloc(capacity);
    if (!buffer) {
        perror("malloc");
        pclose(pipe);
        return -1;
    }

    size_t total = 0;
    uint8_t scratch[8192];
    while (1) {
        size_t got = fread(scratch, 1, sizeof(scratch), pipe);
        if (got == 0) {
            if (feof(pipe)) {
                break;
            }
            if (ferror(pipe)) {
                perror("fread");
                free(buffer);
                pclose(pipe);
                return -1;
            }
        }

        if (total + got > capacity) {
            while (total + got > capacity) {
                capacity *= 2;
            }
            uint8_t *tmp = realloc(buffer, capacity);
            if (!tmp) {
                perror("realloc");
                free(buffer);
                pclose(pipe);
                return -1;
            }
            buffer = tmp;
        }
        memcpy(buffer + total, scratch, got);
        total += got;
    }

    int status = pclose(pipe);
    if (status == -1 || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fprintf(stderr, "unzip returned status %d while reading %s\n", status, resolved);
        free(buffer);
        return -1;
    }

    size_t chunk_count = total / SAMBA_CHUNK_BYTES;
    if (chunk_count == 0) {
        fprintf(stderr, "Samba dataset contained fewer than %d bytes\n", SAMBA_CHUNK_BYTES);
        free(buffer);
        return -1;
    }

    if (max_chunks > 0 && chunk_count > max_chunks) {
        chunk_count = max_chunks;
    }

    size_t usable_bytes = chunk_count * SAMBA_CHUNK_BYTES;
    if (usable_bytes != total) {
        uint8_t *trimmed = realloc(buffer, usable_bytes);
        if (trimmed) {
            buffer = trimmed;
        }
    }

    out->data = buffer;
    out->chunk_count = chunk_count;
    out->chunk_bytes = SAMBA_CHUNK_BYTES;
    return 0;
}

static int hash_bytes(const uint8_t *data, size_t len)
{
    int h = 0;
    for (size_t i = 0; i < len; i++) {
        h = (h * 31 + data[i]) & 0x7fffffff;
    }
    if (h == 0) {
        h = 1; // avoid 0, which the tree interprets as a delete
    }
    return h;
}

static void build_samba_workload(const struct samba_dataset *dataset,
                                 int *key_count,
                                 int *sample_count,
                                 int *keys,
                                 int *values,
                                 int *sample_keys,
                                 int *sample_baseline,
                                 int *sample_updated)
{
    if (*key_count > (int)dataset->chunk_count) {
        *key_count = (int)dataset->chunk_count;
    }
    if (*sample_count > *key_count) {
        *sample_count = *key_count;
    }

    for (int i = 0; i < *key_count; i++) {
        const uint8_t *chunk = dataset->data + ((size_t)i * SAMBA_CHUNK_BYTES);
        keys[i] = i + 1; // contiguous keys, aligned with contiguous slices
        values[i] = hash_bytes(chunk, SAMBA_CHUNK_BYTES);
    }

    int offset = *key_count - *sample_count;
    for (int i = 0; i < *sample_count; i++) {
        int idx = offset + i;
        sample_keys[i] = keys[idx];
        sample_baseline[i] = values[idx];

        if ((idx + 1) < *key_count) {
            const uint8_t *next_chunk = dataset->data + ((size_t)(idx + 1) * SAMBA_CHUNK_BYTES);
            sample_updated[i] = hash_bytes(next_chunk, SAMBA_CHUNK_BYTES);
        } else {
            sample_updated[i] = sample_baseline[i] + UPDATE_DELTA;
        }
    }
}

static int *parse_lz4_levels(int *out_count)
{
    const char *env = getenv("TAIL_LATENCY_LZ4_LEVELS");
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

static void populate_tree(struct bplus_tree_compressed *tree,
                          const int *keys,
                          const int *values,
                          int count,
                          const struct samba_dataset *dataset)
{
    for (int i = 0; i < count; i++) {
        if (getenv("TAIL_LATENCY_DEBUG")) {
            fprintf(stderr, "  populate idx=%d key=%d\n", i, keys[i]);
        }
        if (dataset) {
            const uint8_t *payload = dataset->data + ((size_t)(keys[i] - 1) * dataset->chunk_bytes);
            int rc = bplus_tree_compressed_put_with_payload(tree,
                                                            (key_t)keys[i],
                                                            payload,
                                                            dataset->chunk_bytes,
                                                            values[i]);
            if (rc != 0) {
                fprintf(stderr, "populate_tree: put_with_payload failed for key %d (rc=%d)\n", keys[i], rc);
                exit(EXIT_FAILURE);
            }
        } else {
            int rc = bplus_tree_compressed_put(tree, (key_t)keys[i], values[i]);
            if (rc != 0) {
                fprintf(stderr, "populate_tree: put failed for key %d (rc=%d)\n", keys[i], rc);
                exit(EXIT_FAILURE);
            }
        }
    }
}

static const uint8_t *payload_for_key(const struct samba_dataset *dataset, key_t key)
{
    if (!dataset) {
        return NULL;
    }
    size_t idx = (size_t)(key - 1);
    if (idx >= dataset->chunk_count) {
        return NULL;
    }
    return dataset->data + idx * dataset->chunk_bytes;
}

static const uint8_t *payload_for_updated(const struct samba_dataset *dataset, key_t key)
{
    if (!dataset) {
        return NULL;
    }
    size_t next_idx = (size_t)key;
    if (next_idx < dataset->chunk_count) {
        return dataset->data + next_idx * dataset->chunk_bytes;
    }
    return payload_for_key(dataset, key);
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
    out->p999_ns = percentile(sorted, count, 99.9);

    free(sorted);
}

static void summarize_and_print(const char *label, const struct sample_record *samples)
{
    const double scale = 1e-3; // convert ns to µs for readability
    printf("%s\n", label);

    if (samples->get_count > 0) {
        struct stats_summary get_stats;
        compute_stats(samples->prefetch_ns, samples->get_count, &get_stats);
        printf("  get : p50 %.3f  p90 %.3f  p99 %.3f  p999 %.3f  max %.3f  avg %.3f\n",
               get_stats.p50_ns * scale,
               get_stats.p90_ns * scale,
               get_stats.p99_ns * scale,
               get_stats.p999_ns * scale,
               get_stats.max_ns * scale,
               get_stats.avg_ns * scale);
    } else {
        printf("  get : n/a\n");
    }

    if (samples->put_count > 0) {
        struct stats_summary put_stats;
        compute_stats(samples->update_ns, samples->put_count, &put_stats);
        printf("  put : p50 %.3f  p90 %.3f  p99 %.3f  p999 %.3f  max %.3f  avg %.3f\n",
               put_stats.p50_ns * scale,
               put_stats.p90_ns * scale,
               put_stats.p99_ns * scale,
               put_stats.p999_ns * scale,
               put_stats.max_ns * scale,
               put_stats.avg_ns * scale);
    } else {
        printf("  put : n/a\n");
    }

    double seconds = samples->total_ns / 1e9;
    if (samples->total_ops > 0 && seconds > 0.0) {
        double throughput = samples->total_ops / seconds;
        printf("  throughput: %.2f Mops/s (%d ops in %.2f ms)\n",
               throughput / 1e6,
               samples->total_ops,
               seconds * 1e3);
    } else {
        printf("  throughput: n/a\n");
    }
}

static void report_compression_stats(struct bplus_tree_compressed *tree)
{
    size_t total = 0;
    size_t compressed = 0;
    if (bplus_tree_compressed_calculate_stats(tree, &total, &compressed) == 0 && total > 0) {
        double ratio = (compressed > 0) ? (double)total / (double)compressed : 0.0;
        printf("  compression: total %.2f KB  compressed %.2f KB  ratio %.3f\n",
               total / 1024.0,
               compressed / 1024.0,
               ratio);
    } else {
        printf("  compression: unavailable\n");
    }
}

static void warmup_tree(struct bplus_tree_compressed *tree,
                        const int *keys,
                        const int *baseline_values,
                        const int *updated_values,
                        int warmup_count,
                        const struct samba_dataset *dataset)
{
    if (warmup_count <= 0) {
        return;
    }
    int iterations = warmup_count > 0 ? warmup_count : 0;
    for (int i = 0; i < iterations; i++) {
        key_t key = keys[i];
        (void)bplus_tree_compressed_get(tree, key);
        const uint8_t *update_payload = payload_for_updated(dataset, key);
        if (dataset) {
            bplus_tree_compressed_put_with_payload(tree,
                                                   key,
                                                   update_payload,
                                                   dataset->chunk_bytes,
                                                   updated_values[i]);
            const uint8_t *base_payload = payload_for_key(dataset, key);
            bplus_tree_compressed_put_with_payload(tree,
                                                   key,
                                                   base_payload,
                                                   dataset->chunk_bytes,
                                                   baseline_values[i]);
        } else {
            bplus_tree_compressed_put(tree, key, updated_values[i]);
            bplus_tree_compressed_put(tree, key, baseline_values[i]);
        }
    }
}

static void measure_workload(struct bplus_tree_compressed *tree,
                             const int *keys,
                             const int *baseline_values,
                             const int *updated_values,
                             int sample_count,
                             double write_ratio,
                             struct sample_record *out_samples,
                             FILE *csv,
                             const char *csv_label,
                             const struct samba_dataset *dataset)
{
    out_samples->prefetch_ns = malloc((size_t)sample_count * sizeof(double));
    out_samples->update_ns = malloc((size_t)sample_count * sizeof(double));
    if (!out_samples->prefetch_ns || !out_samples->update_ns) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    out_samples->get_count = 0;
    out_samples->put_count = 0;
    out_samples->total_ops = 0;
    out_samples->total_ns = 0.0;

    int update_interval = INT_MAX;
    if (write_ratio > 0.0) {
        double inv = 1.0 / write_ratio;
        update_interval = (int)llround(inv);
        if (update_interval < 1) {
            update_interval = 1;
        }
    }

    struct timespec total_start, total_end;
    monotonic_now(&total_start);

    for (int i = 0; i < sample_count; i++) {
        bool do_write = (write_ratio > 0.0) && ((i + 1) % update_interval == 0);
        if (do_write) {
            struct timespec start, end;
            int value = updated_values[i];
            monotonic_now(&start);
            int rc = 0;
            if (dataset) {
                const uint8_t *update_payload = payload_for_updated(dataset, keys[i]);
                rc = bplus_tree_compressed_put_with_payload(tree,
                                                            (key_t)keys[i],
                                                            update_payload,
                                                            dataset->chunk_bytes,
                                                            value);
            } else {
                rc = bplus_tree_compressed_put(tree, (key_t)keys[i], value);
            }
            monotonic_now(&end);
            double put_ns = elapsed_ns(&start, &end);
            if (rc != 0) {
                fprintf(stderr, "put failed for key=%d rc=%d\n", keys[i], rc);
                exit(EXIT_FAILURE);
            }

            int idx = out_samples->put_count++;
            out_samples->update_ns[idx] = put_ns;
            int verify = bplus_tree_compressed_get(tree, (key_t)keys[i]);
            if (verify != value) {
                fprintf(stderr, "verification mismatch: key=%d expected=%d got=%d\n",
                        keys[i], value, verify);
                exit(EXIT_FAILURE);
            }
            if (csv) {
                fprintf(csv, "%s,put,%d,%.0f\n", csv_label, idx, put_ns);
            }
        } else {
            struct timespec start, end;
            monotonic_now(&start);
            int value = bplus_tree_compressed_get(tree, (key_t)keys[i]);
            monotonic_now(&end);
            double get_ns = elapsed_ns(&start, &end);
            if (value != baseline_values[i]) {
                fprintf(stderr, "prefetch mismatch: key=%d expected=%d got=%d\n",
                        keys[i], baseline_values[i], value);
                exit(EXIT_FAILURE);
            }

            int idx = out_samples->get_count++;
            out_samples->prefetch_ns[idx] = get_ns;
            if (csv) {
                fprintf(csv, "%s,get,%d,%.0f\n", csv_label, idx, get_ns);
            }
        }
    }

    monotonic_now(&total_end);
    out_samples->total_ns = elapsed_ns(&total_start, &total_end);
    out_samples->total_ops = out_samples->get_count + out_samples->put_count;
}

static void free_samples(struct sample_record *samples)
{
    free(samples->prefetch_ns);
    free(samples->update_ns);
    samples->prefetch_ns = NULL;
    samples->update_ns = NULL;
    samples->get_count = 0;
    samples->put_count = 0;
    samples->total_ns = 0.0;
    samples->total_ops = 0;
}

static void repopulate_baseline(struct bplus_tree_compressed *tree,
                                const int *keys,
                                const int *baseline_values,
                                int sample_count,
                                const struct samba_dataset *dataset)
{
    for (int i = 0; i < sample_count; i++) {
        int rc = 0;
        if (dataset) {
            const uint8_t *payload = payload_for_key(dataset, keys[i]);
            rc = bplus_tree_compressed_put_with_payload(tree,
                                                        (key_t)keys[i],
                                                        payload,
                                                        dataset->chunk_bytes,
                                                        baseline_values[i]);
        } else {
            rc = bplus_tree_compressed_put(tree, (key_t)keys[i], baseline_values[i]);
        }
        if (rc != 0) {
            fprintf(stderr, "repopulate_baseline: put failed for key=%d rc=%d\n",
                    keys[i], rc);
            exit(EXIT_FAILURE);
        }
    }
}

struct kv_pair_view {
    key_t key;
    int stored_value;
    uint8_t payload[COMPRESSED_VALUE_BYTES];
};

static int find_sample_index(const int *sample_keys, int sample_count, key_t key)
{
    int lo = 0;
    int hi = sample_count - 1;
    while (lo <= hi) {
        int mid = lo + (hi - lo) / 2;
        if (sample_keys[mid] == key) {
            return mid;
        }
        if (sample_keys[mid] < key) {
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }
    return -1;
}

static int decompress_bucket(struct bplus_tree_compressed *tree,
                             struct simple_leaf_node *leaf,
                             const struct subpage_index_entry *entry,
                             uint8_t *dst,
                             int dst_capacity)
{
    if (leaf->compression_algo == COMPRESS_QPL && tree->qpl_pool_size > 0) {
        qpl_job *job = NULL;
        int job_index = -1;

        pthread_mutex_lock(&tree->qpl_pool_lock);
        while (tree->qpl_free_count == 0 && tree->qpl_pool_size > 0) {
            pthread_cond_wait(&tree->qpl_pool_cond, &tree->qpl_pool_lock);
        }
        if (tree->qpl_free_count > 0) {
            tree->qpl_free_count--;
            job_index = tree->qpl_job_free_list[tree->qpl_free_count];
            job = tree->qpl_job_pool[job_index];
        }
        pthread_mutex_unlock(&tree->qpl_pool_lock);

        if (job) {
            job->op = qpl_op_decompress;
            job->next_in_ptr = (uint8_t *)leaf->compressed_data + entry->offset;
            job->available_in = entry->length;
            job->next_out_ptr = dst;
            job->available_out = (uint32_t)dst_capacity;
            job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
            qpl_status status = qpl_execute_job(job);
            uint32_t produced = job->total_out;

            pthread_mutex_lock(&tree->qpl_pool_lock);
            if (job_index >= 0 && tree->qpl_free_count < tree->qpl_pool_size) {
                tree->qpl_job_free_list[tree->qpl_free_count] = job_index;
                tree->qpl_free_count++;
                pthread_cond_signal(&tree->qpl_pool_cond);
            }
            pthread_mutex_unlock(&tree->qpl_pool_lock);

            if (status == QPL_STS_OK && produced > 0 && produced <= (uint32_t)dst_capacity) {
                return (int)produced;
            }
        }
    }

#ifdef HAVE_ZLIB
    if (leaf->compression_algo == COMPRESS_ZLIB_ACCEL) {
        uLongf produced = (uLongf)dst_capacity;
        int status = uncompress((Bytef *)dst,
                                &produced,
                                (const Bytef *)leaf->compressed_data + entry->offset,
                                (uLong)entry->length);
        if (status == Z_OK && produced > 0 && produced <= (uLongf)dst_capacity) {
            return (int)produced;
        }
    }
#endif

    return LZ4_decompress_safe((const char *)leaf->compressed_data + entry->offset,
                               (char *)dst,
                               (int)entry->length,
                               dst_capacity);
}

static void verify_sample_keys_in_leaves(struct bplus_tree_compressed *tree,
                                         const int *sample_keys,
                                         const int *sample_values,
                                         int sample_count,
                                         const struct samba_dataset *dataset)
{
    if (sample_count <= 0) {
        return;
    }

    int matched_total = 0;
    int *matched = calloc((size_t)sample_count, sizeof(int));
    if (!matched) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

    pthread_rwlock_rdlock(&tree->rwlock);
    struct list_head *head = &tree->tree->list[0];
    struct list_head *pos, *n;
    list_for_each_safe(pos, n, head) {
        struct bplus_leaf *leaf = list_entry(pos, struct bplus_leaf, link);
        if (leaf->data[0] == 0) {
            continue;
        }

        struct simple_leaf_node *custom_leaf = (struct simple_leaf_node *)leaf->data[0];
        pthread_rwlock_rdlock(&custom_leaf->rwlock);

        struct kv_pair_view *landing = (struct kv_pair_view *)custom_leaf->landing_buffer;
        struct kv_pair_view *landing_end = (struct kv_pair_view *)(custom_leaf->landing_buffer + LANDING_BUFFER_BYTES);
        for (; landing < landing_end; ++landing) {
            if (landing->key == 0) {
                continue;
            }
            int idx = find_sample_index(sample_keys, sample_count, landing->key);
            if (idx >= 0) {
                int actual = landing->stored_value;
                if (actual != sample_values[idx]) {
                    fprintf(stderr, "Landing buffer mismatch for key=%d expected=%d got=%d\n",
                            landing->key, sample_values[idx], actual);
                    pthread_rwlock_unlock(&custom_leaf->rwlock);
                    pthread_rwlock_unlock(&tree->rwlock);
                    free(matched);
                    exit(EXIT_FAILURE);
                }
                if (dataset) {
                    const uint8_t *expected_payload = payload_for_key(dataset, landing->key);
                    size_t cmp_len = dataset ? dataset->chunk_bytes : COMPRESSED_VALUE_BYTES;
                    if (!expected_payload || memcmp(expected_payload, landing->payload, cmp_len) != 0) {
                        fprintf(stderr, "Landing buffer payload mismatch for key=%d\n", landing->key);
                        pthread_rwlock_unlock(&custom_leaf->rwlock);
                        pthread_rwlock_unlock(&tree->rwlock);
                        free(matched);
                        exit(EXIT_FAILURE);
                    }
                }
                if (!matched[idx]) {
                    matched[idx] = 1;
                    matched_total++;
                }
            }
        }

        if (custom_leaf->is_compressed &&
            custom_leaf->compressed_data &&
            custom_leaf->subpage_index &&
            custom_leaf->num_subpages > 0) {
            int sub_page_size = COMPRESSED_LEAF_SIZE / custom_leaf->num_subpages;
            if (sub_page_size <= 0) {
                sub_page_size = COMPRESSED_LEAF_SIZE;
            }

            uint8_t *buffer = malloc((size_t)sub_page_size);
            if (!buffer) {
                perror("malloc");
                pthread_rwlock_unlock(&custom_leaf->rwlock);
                pthread_rwlock_unlock(&tree->rwlock);
                free(matched);
                exit(EXIT_FAILURE);
            }

            for (int bucket = 0; bucket < custom_leaf->num_subpages; bucket++) {
                struct subpage_index_entry *entry = &custom_leaf->subpage_index[bucket];
                if (!entry || entry->length <= 0) {
                    continue;
                }

                int decompressed = decompress_bucket(tree,
                                                     custom_leaf,
                                                     entry,
                                                     buffer,
                                                     sub_page_size);
                if (decompressed <= 0) {
                    fprintf(stderr, "Failed to decompress bucket %d while verifying leaf\n", bucket);
                    free(buffer);
                    pthread_rwlock_unlock(&custom_leaf->rwlock);
                    pthread_rwlock_unlock(&tree->rwlock);
                    free(matched);
                    exit(EXIT_FAILURE);
                }

                struct kv_pair_view *pairs = (struct kv_pair_view *)buffer;
                int pair_capacity = decompressed / (int)sizeof(struct kv_pair_view);
                for (int i = 0; i < pair_capacity; i++) {
                    if (pairs[i].key == 0) {
                        continue;
                    }
                    int idx = find_sample_index(sample_keys, sample_count, pairs[i].key);
                    if (idx >= 0) {
                        int actual = pairs[i].stored_value;
                        if (actual != sample_values[idx]) {
                            fprintf(stderr, "Compressed bucket mismatch for key=%d expected=%d got=%d\n",
                                    pairs[i].key, sample_values[idx], actual);
                            free(buffer);
                            pthread_rwlock_unlock(&custom_leaf->rwlock);
                            pthread_rwlock_unlock(&tree->rwlock);
                            free(matched);
                            exit(EXIT_FAILURE);
                        }
                        if (dataset) {
                            const uint8_t *expected_payload = payload_for_key(dataset, pairs[i].key);
                            size_t cmp_len = dataset ? dataset->chunk_bytes : COMPRESSED_VALUE_BYTES;
                            if (!expected_payload || memcmp(expected_payload, pairs[i].payload, cmp_len) != 0) {
                                fprintf(stderr, "Compressed bucket payload mismatch for key=%d\n", pairs[i].key);
                                free(buffer);
                                pthread_rwlock_unlock(&custom_leaf->rwlock);
                                pthread_rwlock_unlock(&tree->rwlock);
                                free(matched);
                                exit(EXIT_FAILURE);
                            }
                        }
                        if (!matched[idx]) {
                            matched[idx] = 1;
                            matched_total++;
                        }
                    }
                }
            }

            free(buffer);
        }

        pthread_rwlock_unlock(&custom_leaf->rwlock);
    }

    pthread_rwlock_unlock(&tree->rwlock);

    for (int i = 0; i < sample_count; i++) {
        if (!matched[i]) {
            fprintf(stderr, "Sample key %d not found in any leaf during verification\n", sample_keys[i]);
            free(matched);
            exit(EXIT_FAILURE);
        }
    }

    free(matched);
    (void)matched_total;
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
                            FILE *csv,
                            int verify_leaf_data,
                            const struct samba_dataset *dataset,
                            int debug)
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
    if (debug) {
        bplus_tree_compressed_set_debug(tree, 1);
    }

    if (debug) {
        fprintf(stderr, "[lz4 level=%d] populate %d keys\n", level, key_count);
    }
    populate_tree(tree, keys, values, key_count, dataset);

    if (warmup_iters > 0) {
        if (debug) {
            fprintf(stderr, "[lz4 level=%d] warmup %d iters\n", level, warmup_iters);
        }
        warmup_tree(tree, sample_keys, sample_baseline, sample_updated, warmup_iters, dataset);
        repopulate_baseline(tree, sample_keys, sample_baseline, sample_count, dataset);
    }

    if (verify_leaf_data) {
        verify_sample_keys_in_leaves(tree, sample_keys, sample_baseline, sample_count, dataset);
    }

    struct workload_mix mixes[] = {
        {"ReadOnly (100R/0W)", 0.0},
        {"Mixed (80R/20W)", 0.2},
    };
    int mix_count = (int)(sizeof(mixes) / sizeof(mixes[0]));

    char algo_label[64];
    format_lz4_label(level, algo_label, sizeof(algo_label));

    for (int m = 0; m < mix_count; m++) {
        repopulate_baseline(tree, sample_keys, sample_baseline, sample_count, dataset);

        struct sample_record samples = {0};
        char display_label[128];
        char csv_label[128];
        snprintf(display_label, sizeof(display_label), "%s %s", algo_label, mixes[m].label);
        snprintf(csv_label, sizeof(csv_label), "%s_%s", algo_label, mixes[m].label);
        for (char *p = csv_label; *p; ++p) {
            if (*p == ' ' || *p == '/' || *p == '(' || *p == ')') {
                *p = '_';
            }
        }

        measure_workload(tree,
                         sample_keys,
                         sample_baseline,
                         sample_updated,
                         sample_count,
                         mixes[m].write_ratio,
                         &samples,
                         csv,
                         csv_label,
                         dataset);

        summarize_and_print(display_label, &samples);
        report_compression_stats(tree);
        puts("");
        free_samples(&samples);
    }

    bplus_tree_compressed_deinit(tree);
}

static void run_codec_workloads(compression_algo_t algo,
                                int default_subpages,
                                const int *keys,
                                const int *values,
                                int key_count,
                                const int *sample_keys,
                                const int *sample_baseline,
                                const int *sample_updated,
                                int sample_count,
                                int warmup_iters,
                                FILE *csv,
                                int verify_leaf_data,
                                const struct samba_dataset *dataset,
                                int debug)
{
    struct compression_config cfg = bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    cfg.algo = algo;
    cfg.default_sub_pages = default_subpages;
    btree_apply_qpl_env(&cfg);

    struct bplus_tree_compressed *tree =
        bplus_tree_compressed_init_with_config(16, 32, &cfg);
    if (!tree) {
        fprintf(stderr, "Failed to initialize %s tree\n", btree_algo_name(algo));
        exit(EXIT_FAILURE);
    }
    if (debug) {
        bplus_tree_compressed_set_debug(tree, 1);
    }

    if (debug) {
        fprintf(stderr, "[%s] populate %d keys\n", btree_algo_name(algo), key_count);
    }
    populate_tree(tree, keys, values, key_count, dataset);

    if (warmup_iters > 0) {
        if (debug) {
            fprintf(stderr, "[%s] warmup %d iters\n", btree_algo_name(algo), warmup_iters);
        }
        warmup_tree(tree, sample_keys, sample_baseline, sample_updated, warmup_iters, dataset);
        repopulate_baseline(tree, sample_keys, sample_baseline, sample_count, dataset);
    }

    if (verify_leaf_data) {
        verify_sample_keys_in_leaves(tree, sample_keys, sample_baseline, sample_count, dataset);
    }

    struct workload_mix mixes[] = {
        {"ReadOnly (100R/0W)", 0.0},
        {"Mixed (80R/20W)", 0.2},
    };
    int mix_count = (int)(sizeof(mixes) / sizeof(mixes[0]));

    char algo_label[96];
    if (algo == COMPRESS_QPL) {
        snprintf(algo_label,
                 sizeof(algo_label),
                 "QPL(path=%s,mode=%s)",
                 btree_qpl_path_name(cfg.qpl_path),
                 btree_qpl_mode_name(cfg.qpl_huffman_mode));
    } else {
        snprintf(algo_label, sizeof(algo_label), "%s", btree_algo_name(algo));
    }

    for (int m = 0; m < mix_count; m++) {
        repopulate_baseline(tree, sample_keys, sample_baseline, sample_count, dataset);

        struct sample_record samples = {0};
        char display_label[128];
        char csv_label[128];
        snprintf(display_label, sizeof(display_label), "%s %s", algo_label, mixes[m].label);
        snprintf(csv_label, sizeof(csv_label), "%s_%s", algo_label, mixes[m].label);
        for (char *p = csv_label; *p; ++p) {
            if (*p == ' ' || *p == '/' || *p == '(' || *p == ')') {
                *p = '_';
            }
        }

        measure_workload(tree,
                         sample_keys,
                         sample_baseline,
                         sample_updated,
                         sample_count,
                         mixes[m].write_ratio,
                         &samples,
                         csv,
                         csv_label,
                         dataset);

        summarize_and_print(display_label, &samples);
        if (algo == COMPRESS_QPL && tree->qpl_pool_size <= 0) {
            printf("  warning: QPL job pool not initialized; falling back to LZ4 path\n");
        }
        report_compression_stats(tree);
        puts("");
        free_samples(&samples);
    }

    bplus_tree_compressed_deinit(tree);
}
int main(void)
{
    fprintf(stderr, "[main] start\n");
    int key_count = parse_env_int("TAIL_LATENCY_KEY_COUNT", DEFAULT_KEY_COUNT);
    int sample_count = parse_env_int("TAIL_LATENCY_SAMPLE_COUNT", DEFAULT_SAMPLE_COUNT);
    int warmup_count = parse_env_int("TAIL_LATENCY_WARMUP", DEFAULT_WARMUP_COUNT);
    // int default_subpages = 1; // Force single bucket; larger payloads overflow with many subpages
    int default_subpages = parse_env_int("DEFAULT_NUM_SUBPAGES", DEFAULT_NUM_SUBPAGES); // Force single bucket; larger payloads overflow with many subpages
    int use_samba_dataset = parse_env_bool("TAIL_LATENCY_USE_SILESIA", 0);
    int debug = parse_env_bool("TAIL_LATENCY_DEBUG", 0);
    if (default_subpages < 1) {
        default_subpages = 1;
    }

    srand(42);

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

    int *sample_keys = calloc((size_t)sample_count, sizeof(int));
    int *sample_baseline = calloc((size_t)sample_count, sizeof(int));
    int *sample_updated = calloc((size_t)sample_count, sizeof(int));
    if (!sample_keys || !sample_baseline || !sample_updated) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

    struct samba_dataset dataset = {0};
    if (use_samba_dataset) {
        if (load_samba_dataset((size_t)key_count, &dataset) != 0) {
            fprintf(stderr, "Failed to load samba dataset for tail latency test\n");
            return EXIT_FAILURE;
        }
        if (default_subpages > 1) {
            if (debug) {
                fprintf(stderr, "Forcing subpages to 1 for large inline payloads\n");
            }
            default_subpages = 1;
        }
        build_samba_workload(&dataset,
                             &key_count,
                             &sample_count,
                             keys,
                             values,
                             sample_keys,
                             sample_baseline,
                             sample_updated);
        printf("Using Silesia samba slices: key_count=%d sample_count=%d chunk_bytes=%zu key_bytes=%d\n",
               key_count, sample_count, dataset.chunk_bytes, SAMBA_KEY_BYTES);
    } else {
        for (int i = 0; i < key_count; i++) {
            keys[i] = i + 1;
            values[i] = (i + 1) * 5;
        }

        int offset = key_count - sample_count;
        for (int i = 0; i < sample_count; i++) {
            int idx = offset + i;
            sample_keys[i] = keys[idx];
            sample_baseline[i] = values[idx];
            sample_updated[i] = values[idx] + UPDATE_DELTA;
        }
    }

    if (debug) {
        fprintf(stderr, "[main] key_count=%d sample_count=%d warmup=%d subpages=%d use_silesia=%d\n",
                key_count, sample_count, warmup_count, default_subpages, use_samba_dataset);
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
    if (debug) {
        fprintf(stderr, "[main] parsed %d LZ4 levels\n", level_count);
    }
    for (int i = 0; i < level_count; i++) {
        if (debug) {
            fprintf(stderr, "[main] run LZ4 level=%d\n", lz4_levels[i]);
        }
        fprintf(stderr, "[trace] before LZ4 variant %d\n", i);
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
                        csv,
                        use_samba_dataset,
                        use_samba_dataset ? &dataset : NULL,
                        debug);
        fprintf(stderr, "[trace] after LZ4 variant %d\n", i);
    }

    free(lz4_levels);

    if (debug) {
        fprintf(stderr, "[main] run QPL workloads\n");
    }
    fprintf(stderr, "[trace] before QPL workloads\n");
    run_codec_workloads(COMPRESS_QPL,
                        default_subpages,
                        keys,
                        values,
                        key_count,
                        sample_keys,
                        sample_baseline,
                        sample_updated,
                        sample_count,
                        warmup_iters,
                        csv,
                        use_samba_dataset,
                        use_samba_dataset ? &dataset : NULL,
                        debug);

    fprintf(stderr, "[trace] before zlib_accel workloads\n");
    run_codec_workloads(COMPRESS_ZLIB_ACCEL,
                        default_subpages,
                        keys,
                        values,
                        key_count,
                        sample_keys,
                        sample_baseline,
                        sample_updated,
                        sample_count,
                        warmup_iters,
                        csv,
                        use_samba_dataset,
                        use_samba_dataset ? &dataset : NULL,
                        debug);

    if (csv) {
        fclose(csv);
        printf("CSV written to %s\n", csv_path);
    }
    free(sample_keys);
    free(sample_baseline);
    free(sample_updated);
    free(keys);
    free(values);
    free(dataset.data);
    return EXIT_SUCCESS;
}
