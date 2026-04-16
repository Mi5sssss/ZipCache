#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>
#include <sys/wait.h>

#include <lz4.h>
#include <qpl/qpl.h>

#include "bplustree_compressed.h"

#define DEFAULT_BLOCK_SIZE COMPRESSED_LEAF_SIZE
#define DEFAULT_BLOCKS 4096
#define DEFAULT_OCCUPANCY 50  // percent filled slots in kv page

struct kv_pair {
    key_t key;
    int stored_value;
    uint8_t payload[COMPRESSED_VALUE_BYTES];
};

struct latency_stats {
    double avg_us;
    double p50_us;
    double p90_us;
    double p95_us;
    double p99_us;
};

static void die(const char *msg)
{
    perror(msg);
    exit(EXIT_FAILURE);
}

static double elapsed_ns(const struct timespec *s, const struct timespec *e)
{
    return (double)(e->tv_sec - s->tv_sec) * 1e9 +
           (double)(e->tv_nsec - s->tv_nsec);
}

static int cmp_double(const void *a, const void *b)
{
    double da = *(const double *)a;
    double db = *(const double *)b;
    if (da < db) return -1;
    if (da > db) return 1;
    return 0;
}

static double pick_percentile(double *values, int count, double pct)
{
    if (count <= 0) {
        return 0.0;
    }
    double idx = (pct / 100.0) * (double)(count - 1);
    int lo = (int)idx;
    int hi = lo + 1;
    if (hi >= count) {
        return values[count - 1];
    }
    double frac = idx - (double)lo;
    return values[lo] + frac * (values[hi] - values[lo]);
}

static void compute_latency_stats(const double *samples_us, int count, struct latency_stats *out)
{
    if (!out || count <= 0) {
        if (out) memset(out, 0, sizeof(*out));
        return;
    }
    double *copy = malloc((size_t)count * sizeof(double));
    if (!copy) {
        memset(out, 0, sizeof(*out));
        return;
    }
    double sum = 0.0;
    for (int i = 0; i < count; i++) {
        copy[i] = samples_us[i];
        sum += samples_us[i];
    }
    qsort(copy, (size_t)count, sizeof(double), cmp_double);
    out->avg_us = sum / (double)count;
    out->p50_us = pick_percentile(copy, count, 50.0);
    out->p90_us = pick_percentile(copy, count, 90.0);
    out->p95_us = pick_percentile(copy, count, 95.0);
    out->p99_us = pick_percentile(copy, count, 99.0);
    free(copy);
}

static int parse_env_int(const char *name, int fallback)
{
    const char *v = getenv(name);
    if (!v || !*v) return fallback;
    char *end = NULL;
    long n = strtol(v, &end, 10);
    if (end == v || *end || n <= 0 || n > INT_MAX) {
        fprintf(stderr, "Invalid %s=%s\n", name, v ? v : "");
        exit(EXIT_FAILURE);
    }
    return (int)n;
}

static const char *resolve_samba_zip(char *buffer, size_t buf_len)
{
    const char *override = getenv("SAMBA_ZIP_PATH");
    if (override && *override) {
        if (access(override, R_OK) == 0) {
            return override;
        }
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

static int load_samba_bytes(uint8_t **out_buf, size_t *out_len)
{
    char path[PATH_MAX];
    const char *resolved = resolve_samba_zip(path, sizeof(path));
    if (!resolved) {
        return -1;
    }

    char cmd[PATH_MAX + 32];
    snprintf(cmd, sizeof(cmd), "unzip -p \"%s\" samba", resolved);
    FILE *pipe = popen(cmd, "r");
    if (!pipe) {
        return -1;
    }

    size_t alloc = 1 << 20; // 1MB start
    uint8_t *buf = malloc(alloc);
    if (!buf) {
        pclose(pipe);
        return -1;
    }
    size_t total = 0;
    while (1) {
        if (total == alloc) {
            size_t new_size = alloc * 2;
            uint8_t *tmp = realloc(buf, new_size);
            if (!tmp) {
                free(buf);
                pclose(pipe);
                return -1;
            }
            buf = tmp;
            alloc = new_size;
        }
        size_t got = fread(buf + total, 1, alloc - total, pipe);
        total += got;
        if (got == 0) {
            if (feof(pipe)) {
                break;
            }
            if (ferror(pipe)) {
                free(buf);
                pclose(pipe);
                return -1;
            }
        }
    }
    int status = pclose(pipe);
    if (status != 0) {
        fprintf(stderr, "Warning: unzip returned %d after reading %zu bytes\n", status, total);
    }
    if (total == 0) {
        free(buf);
        return -1;
    }
    uint8_t *trim = realloc(buf, total);
    if (trim) buf = trim;
    *out_buf = buf;
    *out_len = total;
    return 0;
}

static void fill_kv_blocks(uint8_t *buf,
                           int blocks,
                           int occupancy_pct,
                           const uint8_t *payload_src,
                           size_t payload_len,
                           size_t block_size)
{
    size_t payload_pos = 0;
    srand(1234);
    int next_key = 1;
    size_t slots = block_size / sizeof(struct kv_pair);
    for (int b = 0; b < blocks; b++) {
        struct kv_pair *page = (struct kv_pair *)(buf + (size_t)b * block_size);
        for (size_t i = 0; i < slots; i++) {
            int fill = (rand() % 100) < occupancy_pct;
            if (!fill) {
                page[i].key = 0;
                page[i].stored_value = 0;
                memset(page[i].payload, 0, sizeof(page[i].payload));
                continue;
            }
            page[i].key = next_key++;
            page[i].stored_value = page[i].key ^ 0x5a5a5a5a;
            if (payload_src && payload_len > 0) {
                size_t remain = payload_len - payload_pos;
                if (remain < sizeof(page[i].payload)) {
                    payload_pos = 0;
                }
                memcpy(page[i].payload, payload_src + payload_pos, sizeof(page[i].payload));
                payload_pos += sizeof(page[i].payload);
                if (payload_pos >= payload_len) {
                    payload_pos = 0;
                }
            } else {
                for (size_t j = 0; j < sizeof(page[i].payload); j++) {
                    page[i].payload[j] = (uint8_t)((page[i].key * 131 + j * 17) & 0xff);
                }
            }
        }
    }
}

static void bench_lz4(const uint8_t *src_blocks, int blocks, size_t block_size)
{
    size_t max_compressed = block_size * 2;
    uint8_t *compressed = malloc((size_t)blocks * max_compressed);
    if (!compressed) die("malloc compressed");
    int *sizes = malloc((size_t)blocks * sizeof(int));
    if (!sizes) die("malloc sizes");
    double *comp_lat = malloc((size_t)blocks * sizeof(double));
    double *decomp_lat = malloc((size_t)blocks * sizeof(double));
    if (!comp_lat || !decomp_lat) die("malloc latencies");

    struct timespec t0, t1;
    size_t total_out = 0;
    for (int i = 0; i < blocks; i++) {
        const char *src = (const char *)(src_blocks + (size_t)i * block_size);
        char *dst = (char *)(compressed + (size_t)i * max_compressed);
        clock_gettime(CLOCK_MONOTONIC, &t0);
        int out = LZ4_compress_default(src, dst, (int)block_size, (int)max_compressed);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        if (out <= 0) {
            fprintf(stderr, "LZ4 compress failed at block %d\n", i);
            exit(EXIT_FAILURE);
        }
        total_out += (size_t)out;
        sizes[i] = out;
        comp_lat[i] = elapsed_ns(&t0, &t1) / 1e3;
    }

    uint8_t *dst = malloc(block_size);
    if (!dst) die("malloc decompress");
    for (int i = 0; i < blocks; i++) {
        int src_len = sizes[i];
        clock_gettime(CLOCK_MONOTONIC, &t0);
        int out = LZ4_decompress_safe((const char *)(compressed + (size_t)i * max_compressed),
                                      (char *)dst,
                                      src_len,
                                      (int)block_size);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        if (out != (int)block_size) {
            fprintf(stderr, "LZ4 decompress failed at block %d (out=%d)\n", i, out);
            exit(EXIT_FAILURE);
        }
        decomp_lat[i] = elapsed_ns(&t0, &t1) / 1e3;
    }

    struct latency_stats comp_stats, decomp_stats;
    compute_latency_stats(comp_lat, blocks, &comp_stats);
    compute_latency_stats(decomp_lat, blocks, &decomp_stats);

    double ratio = ((double)blocks * (double)block_size) / (double)total_out;
    double in_mb = ((double)blocks * (double)block_size) / (1024.0 * 1024.0);
    double comp_total_us = comp_stats.avg_us * (double)blocks;
    double decomp_total_us = decomp_stats.avg_us * (double)blocks;

    printf("KV LZ4: blocks=%d size=%zuB  ratio=%.3fx  comp p50=%.2f p90=%.2f p95=%.2f p99=%.2f avg=%.2f us (%.2f GB/s)  "
           "decomp p50=%.2f p90=%.2f p95=%.2f p99=%.2f avg=%.2f us (%.2f GB/s)\n",
           blocks,
           block_size,
           ratio,
           comp_stats.p50_us, comp_stats.p90_us, comp_stats.p95_us, comp_stats.p99_us, comp_stats.avg_us,
           in_mb / (comp_total_us / 1e6),
           decomp_stats.p50_us, decomp_stats.p90_us, decomp_stats.p95_us, decomp_stats.p99_us, decomp_stats.avg_us,
           in_mb / (decomp_total_us / 1e6));

    free(compressed);
    free(sizes);
    free(comp_lat);
    free(decomp_lat);
    free(dst);
}

static qpl_path_t parse_qpl_path(void)
{
    const char *v = getenv("KV_QPL_PATH");
    if (!v || !*v) return qpl_path_auto;
    if (strcasecmp(v, "software") == 0 || strcasecmp(v, "soft") == 0) return qpl_path_software;
    if (strcasecmp(v, "hardware") == 0 || strcasecmp(v, "hw") == 0) return qpl_path_hardware;
    return qpl_path_auto;
}

static void bench_qpl(const uint8_t *src_blocks, int blocks, size_t block_size)
{
    qpl_path_t path = parse_qpl_path();
    uint32_t job_size = 0;
    qpl_status st = qpl_get_job_size(path, &job_size);
    if (st != QPL_STS_OK) {
        fprintf(stderr, "qpl_get_job_size failed: %d\n", st);
        exit(EXIT_FAILURE);
    }

    uint8_t *job_buf = malloc(job_size);
    if (!job_buf) die("malloc job");
    qpl_job *job = (qpl_job *)job_buf;
    st = qpl_init_job(path, job);
    if (st != QPL_STS_OK) {
        fprintf(stderr, "qpl_init_job failed: %d\n", st);
        exit(EXIT_FAILURE);
    }

    size_t max_compressed = block_size * 2;
    uint8_t *compressed = malloc((size_t)blocks * max_compressed);
    if (!compressed) die("malloc compressed");
    int *sizes = malloc((size_t)blocks * sizeof(int));
    if (!sizes) die("malloc sizes");
    double *comp_lat = malloc((size_t)blocks * sizeof(double));
    double *decomp_lat = malloc((size_t)blocks * sizeof(double));
    if (!comp_lat || !decomp_lat) die("malloc latencies");

    struct timespec t0, t1;
    size_t total_out = 0;
    for (int i = 0; i < blocks; i++) {
        job->op = qpl_op_compress;
        job->next_in_ptr = (uint8_t *)(src_blocks + (size_t)i * block_size);
        job->available_in = (uint32_t)block_size;
        job->next_out_ptr = compressed + (size_t)i * max_compressed;
        job->available_out = (uint32_t)max_compressed;
        job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
        job->level = qpl_default_level;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        st = qpl_execute_job(job);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        if (st != QPL_STS_OK) {
            fprintf(stderr, "qpl compress failed at block %d: %d\n", i, st);
            exit(EXIT_FAILURE);
        }
        total_out += job->total_out;
        sizes[i] = (int)job->total_out;
        comp_lat[i] = elapsed_ns(&t0, &t1) / 1e3;
    }

    uint8_t *dst = malloc(block_size);
    if (!dst) die("malloc decompress");
    for (int i = 0; i < blocks; i++) {
        job->op = qpl_op_decompress;
        job->next_in_ptr = compressed + (size_t)i * max_compressed;
        job->available_in = (uint32_t)sizes[i];
        job->next_out_ptr = dst;
        job->available_out = (uint32_t)block_size;
        job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        st = qpl_execute_job(job);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        if (st != QPL_STS_OK || job->total_out != block_size) {
            fprintf(stderr, "qpl decompress failed at block %d: status=%d out=%u\n",
                    i, st, job->total_out);
            exit(EXIT_FAILURE);
        }
        // Optional verify: memcmp original
        if (memcmp(dst, src_blocks + (size_t)i * block_size, block_size) != 0) {
            fprintf(stderr, "qpl decompress mismatch at block %d\n", i);
            exit(EXIT_FAILURE);
        }
        decomp_lat[i] = elapsed_ns(&t0, &t1) / 1e3;
    }

    struct latency_stats comp_stats, decomp_stats;
    compute_latency_stats(comp_lat, blocks, &comp_stats);
    compute_latency_stats(decomp_lat, blocks, &decomp_stats);

    double ratio = ((double)blocks * (double)block_size) / (double)total_out;
    double in_mb = ((double)blocks * (double)block_size) / (1024.0 * 1024.0);
    double comp_total_us = comp_stats.avg_us * (double)blocks;
    double decomp_total_us = decomp_stats.avg_us * (double)blocks;

    printf("KV QPL(%s): blocks=%d size=%zuB  ratio=%.3fx  comp p50=%.2f p90=%.2f p95=%.2f p99=%.2f avg=%.2f us (%.2f GB/s)  "
           "decomp p50=%.2f p90=%.2f p95=%.2f p99=%.2f avg=%.2f us (%.2f GB/s)\n",
           (path == qpl_path_software ? "soft" : path == qpl_path_hardware ? "hw" : "auto"),
           blocks,
           block_size,
           ratio,
           comp_stats.p50_us, comp_stats.p90_us, comp_stats.p95_us, comp_stats.p99_us, comp_stats.avg_us,
           in_mb / (comp_total_us / 1e6),
           decomp_stats.p50_us, decomp_stats.p90_us, decomp_stats.p95_us, decomp_stats.p99_us, decomp_stats.avg_us,
           in_mb / (decomp_total_us / 1e6));

    free(compressed);
    free(sizes);
    free(comp_lat);
    free(decomp_lat);
    free(dst);
    qpl_fini_job(job);
    free(job_buf);
}

int main(void)
{
    size_t block_size = (size_t)parse_env_int("KV_BLOCK_SIZE", DEFAULT_BLOCK_SIZE);
    if (block_size == 0) {
        block_size = DEFAULT_BLOCK_SIZE;
    }
    int blocks = parse_env_int("KV_BLOCKS", DEFAULT_BLOCKS);
    int occupancy = parse_env_int("KV_OCCUPANCY_PCT", DEFAULT_OCCUPANCY);
    if (occupancy < 1) occupancy = 1;
    if (occupancy > 100) occupancy = 100;

    int use_silesia = parse_env_int("KV_BENCH_USE_SILESIA", 1);
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    if (use_silesia) {
        if (load_samba_bytes(&payload, &payload_len) != 0) {
            fprintf(stderr, "Warning: Silesia samba not found, falling back to synthetic payload\n");
        } else {
            fprintf(stderr, "Using Silesia samba payload: %zu bytes\n", payload_len);
        }
    }

    uint8_t *blocks_buf = calloc((size_t)blocks, block_size);
    if (!blocks_buf) die("calloc blocks");
    fill_kv_blocks(blocks_buf, blocks, occupancy, payload, payload_len, block_size);

    bench_lz4(blocks_buf, blocks, block_size);
    bench_qpl(blocks_buf, blocks, block_size);

    free(blocks_buf);
    free(payload);
    return 0;
}
