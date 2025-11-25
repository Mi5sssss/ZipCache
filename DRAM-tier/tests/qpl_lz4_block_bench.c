#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <sys/wait.h>

#include <lz4.h>
#include <qpl/qpl.h>

#define BLOCK_SIZE 4096
#define DEFAULT_BLOCKS 1024
#define MAX_COMPRESSED (BLOCK_SIZE * 2)

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

static void fill_blocks_random(uint8_t *buf, int blocks)
{
    srand(1234);
    for (int i = 0; i < blocks; i++) {
        uint8_t *b = buf + i * BLOCK_SIZE;
        for (int j = 0; j < BLOCK_SIZE; j++) {
            b[j] = (uint8_t)((i * 131 + j * 17 + rand()) & 0xff);
        }
    }
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

static int load_samba_blocks(uint8_t **out_buf, int blocks, int *out_blocks_used)
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

    size_t need_bytes = (size_t)blocks * BLOCK_SIZE;
    uint8_t *buffer = malloc(need_bytes);
    if (!buffer) {
        pclose(pipe);
        return -1;
    }

    size_t total = 0;
    while (total < need_bytes) {
        size_t got = fread(buffer + total, 1, need_bytes - total, pipe);
        if (got == 0) {
            if (feof(pipe) || ferror(pipe)) {
                break;
            }
        }
        total += got;
    }
    int status = pclose(pipe);
    /* tolerate SIGPIPE or non-zero if we already have data */
    if (total == 0 || status == -1) {
        free(buffer);
        return -1;
    }

    int available_blocks = (int)(total / BLOCK_SIZE);
    if (available_blocks <= 0) {
        free(buffer);
        return -1;
    }

    *out_blocks_used = available_blocks < blocks ? available_blocks : blocks;
    if ((size_t)(*out_blocks_used) * BLOCK_SIZE < total) {
        uint8_t *trimmed = realloc(buffer, (size_t)(*out_blocks_used) * BLOCK_SIZE);
        if (trimmed) buffer = trimmed;
    }

    *out_buf = buffer;
    return 0;
}

static void bench_lz4(const uint8_t *src_blocks, int blocks, int **out_sizes)
{
    uint8_t *compressed = malloc((size_t)blocks * MAX_COMPRESSED);
    if (!compressed) die("malloc compressed");
    int *sizes = malloc((size_t)blocks * sizeof(int));
    if (!sizes) die("malloc sizes");

    struct timespec t0, t1;
    size_t total_out = 0;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < blocks; i++) {
        const char *src = (const char *)(src_blocks + i * BLOCK_SIZE);
        char *dst = (char *)(compressed + i * MAX_COMPRESSED);
        int out = LZ4_compress_default(src, dst, BLOCK_SIZE, MAX_COMPRESSED);
        if (out <= 0) {
            fprintf(stderr, "LZ4 compress failed at block %d\n", i);
            exit(EXIT_FAILURE);
        }
        total_out += (size_t)out;
        sizes[i] = out;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double comp_ns = elapsed_ns(&t0, &t1);

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < blocks; i++) {
        char *dst = malloc(BLOCK_SIZE);
        if (!dst) die("malloc decompress buf");
        int src_len = sizes[i];
        int out = LZ4_decompress_safe((const char *)(compressed + i * MAX_COMPRESSED),
                                      dst,
                                      src_len,
                                      BLOCK_SIZE);
        if (out != BLOCK_SIZE) {
            fprintf(stderr, "LZ4 decompress failed at block %d (out=%d)\n", i, out);
            exit(EXIT_FAILURE);
        }
        free(dst);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double decomp_ns = elapsed_ns(&t0, &t1);

    double comp_us = comp_ns / 1e3;
    double decomp_us = decomp_ns / 1e3;
    double ratio = ((double)blocks * BLOCK_SIZE) / (double)total_out;
    double in_mb = ((double)blocks * BLOCK_SIZE) / (1024.0 * 1024.0);

    printf("LZ4: blocks=%d size=%dB  ratio=%.3fx  comp %.2f us (%.2f GB/s)  decomp %.2f us (%.2f GB/s)\n",
           blocks,
           BLOCK_SIZE,
           ratio,
           comp_us,
           (in_mb / (comp_us / 1e6)) / 1024.0,
           decomp_us,
           (in_mb / (decomp_us / 1e6)) / 1024.0);

    free(compressed);
    *out_sizes = sizes;
}

static void bench_qpl(const uint8_t *src_blocks, int blocks, int **out_sizes)
{
    uint8_t *compressed = malloc((size_t)blocks * MAX_COMPRESSED);
    if (!compressed) die("malloc compressed");
    int *sizes = malloc((size_t)blocks * sizeof(int));
    if (!sizes) die("malloc sizes");

    uint32_t job_size = 0;
    qpl_status st = qpl_get_job_size(qpl_path_auto, &job_size);
    if (st != QPL_STS_OK) die("qpl_get_job_size");

    uint8_t *job_buffer = (uint8_t *)malloc(job_size);
    if (!job_buffer) die("malloc job buffer");
    qpl_job *job = (qpl_job *)job_buffer;
    st = qpl_init_job(qpl_path_auto, job);
    if (st != QPL_STS_OK) die("qpl_init_job");

    struct timespec t0, t1;
    size_t total_out = 0;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < blocks; i++) {
        job->op = qpl_op_compress;
        job->next_in_ptr = (uint8_t *)(src_blocks + i * BLOCK_SIZE);
        job->available_in = BLOCK_SIZE;
        job->next_out_ptr = compressed + i * MAX_COMPRESSED;
        job->available_out = MAX_COMPRESSED;
        job->level = qpl_default_level;
        job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

        st = qpl_execute_job(job);
        if (st != QPL_STS_OK) {
            fprintf(stderr, "QPL compress failed at block %d status=%d\n", i, st);
            exit(EXIT_FAILURE);
        }
        total_out += job->total_out;
        sizes[i] = (int)job->total_out;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double comp_ns = elapsed_ns(&t0, &t1);

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < blocks; i++) {
        job->op = qpl_op_decompress;
        job->next_in_ptr = compressed + i * MAX_COMPRESSED;
        job->available_in = (uint32_t)sizes[i];
        uint8_t *dst = malloc(BLOCK_SIZE);
        if (!dst) die("malloc qpl dst");
        job->next_out_ptr = dst;
        job->available_out = BLOCK_SIZE;
        job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
        st = qpl_execute_job(job);
        if (st != QPL_STS_OK || job->total_out != BLOCK_SIZE) {
            fprintf(stderr, "QPL decompress failed at block %d status=%d out=%u\n",
                    i, st, job->total_out);
            exit(EXIT_FAILURE);
        }
        free(dst);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double decomp_ns = elapsed_ns(&t0, &t1);

    double comp_us = comp_ns / 1e3;
    double decomp_us = decomp_ns / 1e3;
    double ratio = ((double)blocks * BLOCK_SIZE) / (double)total_out;
    double in_mb = ((double)blocks * BLOCK_SIZE) / (1024.0 * 1024.0);

    printf("QPL: blocks=%d size=%dB  ratio=%.3fx  comp %.2f us (%.2f GB/s)  decomp %.2f us (%.2f GB/s)\n",
           blocks,
           BLOCK_SIZE,
           ratio,
           comp_us,
           (in_mb / (comp_us / 1e6)) / 1024.0,
           decomp_us,
           (in_mb / (decomp_us / 1e6)) / 1024.0);

    qpl_fini_job(job);
    free(job_buffer);
    free(compressed);
    *out_sizes = sizes;
}

int main(void)
{
    int blocks = parse_env_int("BLOCK_BENCH_COUNT", DEFAULT_BLOCKS);
    int use_samba = parse_env_int("BLOCK_BENCH_USE_SILESIA", 0);

    uint8_t *blocks_buf = NULL;
    int usable_blocks = blocks;

    if (use_samba) {
        if (load_samba_blocks(&blocks_buf, blocks, &usable_blocks) != 0) {
            fprintf(stderr, "Failed to load samba.zip; falling back to random data\n");
            use_samba = 0;
        } else {
            printf("Block bench: %d blocks of %d bytes each from samba\n", usable_blocks, BLOCK_SIZE);
        }
    }

    if (!use_samba) {
        blocks_buf = malloc((size_t)blocks * BLOCK_SIZE);
        if (!blocks_buf) die("malloc blocks");
        fill_blocks_random(blocks_buf, blocks);
        usable_blocks = blocks;
        printf("Block bench: %d blocks of %d bytes each (random)\n", usable_blocks, BLOCK_SIZE);
    }

    int *lz4_sizes = NULL;
    int *qpl_sizes = NULL;
    bench_lz4(blocks_buf, usable_blocks, &lz4_sizes);
    bench_qpl(blocks_buf, usable_blocks, &qpl_sizes);

    free(blocks_buf);
    free(lz4_sizes);
    free(qpl_sizes);
    return 0;
}
#define SAMBA_CHUNK_BYTES 4096
