#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>

#include <lz4.h>
#include <qpl/qpl.h>

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

static int parse_env_int(const char *name, int fallback)
{
    const char *v = getenv(name);
    if (!v || !*v) return fallback;
    char *end = NULL;
    long n = strtol(v, &end, 10);
    if (end == v || *end || n <= 0) {
        fprintf(stderr, "Invalid %s=%s\n", name, v);
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

    size_t alloc = 1 << 20;
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
            if (feof(pipe)) break;
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

static void bench_once(const uint8_t *data, size_t data_len, size_t block_size, int blocks)
{
    if ((size_t)blocks * block_size > data_len) {
        blocks = (int)(data_len / block_size);
    }
    if (blocks <= 0) {
        fprintf(stderr, "No blocks to process for size %zu\n", block_size);
        return;
    }

    size_t max_compressed = block_size * 2;
    uint8_t *compressed = malloc((size_t)blocks * max_compressed);
    if (!compressed) die("malloc compressed");
    int *sizes = malloc((size_t)blocks * sizeof(int));
    if (!sizes) die("malloc sizes");

    struct timespec t0, t1;
    size_t total_out = 0;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < blocks; i++) {
        const char *src = (const char *)(data + (size_t)i * block_size);
        char *dst = (char *)(compressed + (size_t)i * max_compressed);
        int out = LZ4_compress_default(src, dst, (int)block_size, (int)max_compressed);
        if (out <= 0) die("LZ4 compress");
        total_out += (size_t)out;
        sizes[i] = out;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double lz4_comp_us = elapsed_ns(&t0, &t1) / 1e3;

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < blocks; i++) {
        char *dst = malloc(block_size);
        if (!dst) die("malloc dst");
        int out = LZ4_decompress_safe((const char *)(compressed + (size_t)i * max_compressed),
                                      dst,
                                      sizes[i],
                                      (int)block_size);
        if (out != (int)block_size) die("LZ4 decompress");
        free(dst);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double lz4_decomp_us = elapsed_ns(&t0, &t1) / 1e3;

    qpl_status st;
    uint32_t job_size = 0;
    st = qpl_get_job_size(qpl_path_auto, &job_size);
    if (st != QPL_STS_OK) die("qpl_get_job_size");
    uint8_t *job_buf = malloc(job_size);
    if (!job_buf) die("malloc job");
    qpl_job *job = (qpl_job *)job_buf;
    st = qpl_init_job(qpl_path_auto, job);
    if (st != QPL_STS_OK) die("qpl_init_job");

    total_out = 0;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < blocks; i++) {
        job->op = qpl_op_compress;
        job->next_in_ptr = (uint8_t *)(data + (size_t)i * block_size);
        job->available_in = (uint32_t)block_size;
        job->next_out_ptr = compressed + (size_t)i * max_compressed;
        job->available_out = (uint32_t)max_compressed;
        job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
        job->level = qpl_default_level;
        st = qpl_execute_job(job);
        if (st != QPL_STS_OK) die("qpl compress");
        total_out += job->total_out;
        sizes[i] = (int)job->total_out;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double qpl_comp_us = elapsed_ns(&t0, &t1) / 1e3;

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < blocks; i++) {
        uint8_t *dst = malloc(block_size);
        if (!dst) die("malloc dst");
        job->op = qpl_op_decompress;
        job->next_in_ptr = compressed + (size_t)i * max_compressed;
        job->available_in = (uint32_t)sizes[i];
        job->next_out_ptr = dst;
        job->available_out = (uint32_t)block_size;
        job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
        st = qpl_execute_job(job);
        if (st != QPL_STS_OK || job->total_out != block_size) die("qpl decompress");
        free(dst);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double qpl_decomp_us = elapsed_ns(&t0, &t1) / 1e3;

    double ratio_lz4 = ((double)blocks * block_size) / (double)total_out;
    double ratio_qpl = ratio_lz4; // same total_out variable reused; recalc for qpl
    // Recompute ratio for qpl
    size_t total_out_qpl = 0;
    for (int i = 0; i < blocks; i++) total_out_qpl += (size_t)sizes[i];
    ratio_qpl = ((double)blocks * block_size) / (double)total_out_qpl;

    double in_gb = ((double)blocks * block_size) / (1024.0 * 1024.0 * 1024.0);
    printf("Block %4zuB : LZ4 comp %.2f us (%.2f GB/s) decomp %.2f us (%.2f GB/s) ratio=%.3fx | "
           "QPL comp %.2f us (%.2f GB/s) decomp %.2f us (%.2f GB/s) ratio=%.3fx\n",
           block_size,
           lz4_comp_us, in_gb / (lz4_comp_us / 1e6),
           lz4_decomp_us, in_gb / (lz4_decomp_us / 1e6),
           ratio_lz4,
           qpl_comp_us, in_gb / (qpl_comp_us / 1e6),
           qpl_decomp_us, in_gb / (qpl_decomp_us / 1e6),
           ratio_qpl);

    free(compressed);
    free(sizes);
    qpl_fini_job(job);
    free(job_buf);
}

int main(void)
{
    int blocks = parse_env_int("BLOCKS", 2048);
    const char *sizes_env = getenv("BLOCK_SIZES");
    const char *default_sizes = "4096,8192,16384";
    if (!sizes_env || !*sizes_env) sizes_env = default_sizes;

    uint8_t *payload = NULL;
    size_t payload_len = 0;
    if (load_samba_bytes(&payload, &payload_len) != 0) {
        fprintf(stderr, "Warning: could not load Silesia samba, exiting\n");
        return 1;
    }

    char *sizes_dup = strdup(sizes_env);
    if (!sizes_dup) die("strdup");
    char *tok = strtok(sizes_dup, ",");
    while (tok) {
        long sz = strtol(tok, NULL, 10);
        if (sz > 0) {
            bench_once(payload, payload_len, (size_t)sz, blocks);
        }
        tok = strtok(NULL, ",");
    }
    free(sizes_dup);
    free(payload);
    return 0;
}
