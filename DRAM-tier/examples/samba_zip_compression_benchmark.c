#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <limits.h>
#include <unistd.h>
#include <sys/wait.h>

#include "../lib/bplustree_compressed.h"

#define VALUE_SIZE 64

typedef struct {
    const char *algorithm_name;
    compression_algo_t algo;
    double compression_ratio;
    double throughput;
    double p99_latency;
    double total_time;
    size_t original_size;
    size_t compressed_size;
    int successful_insertions;
} benchmark_result_t;

struct chunk_dataset {
    char *data;
    size_t chunk_count;
};

static double get_time_seconds(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

static int compare_doubles(const void *a, const void *b)
{
    double da = *(const double *)a;
    double db = *(const double *)b;
    return (da > db) - (da < db);
}

static double calculate_p99_latency(double *latencies, int count)
{
    if (count == 0) {
        return 0.0;
    }
    qsort(latencies, count, sizeof(double), compare_doubles);
    int index = (int)(0.99 * count);
    if (index >= count) {
        index = count - 1;
    }
    return latencies[index] * 1000000.0; /* seconds → microseconds */
}

static int calculate_hash(const char *data, size_t size)
{
    int hash = 0;
    for (size_t i = 0; i < size; i++) {
        hash = (hash * 31 + data[i]) & 0x7FFFFFFF;
    }
    return hash;
}

static FILE *open_samba_zip(const char **resolved_path)
{
    static char env_path_buffer[PATH_MAX];

    const char *zip_file_override = getenv("SAMBA_ZIP_PATH");
    if (zip_file_override && zip_file_override[0]) {
        FILE *fp = fopen(zip_file_override, "rb");
        if (fp) {
            if (resolved_path) {
                *resolved_path = zip_file_override;
            }
            return fp;
        }
    }

    const char *corpus_dir = getenv("SILESIA_CORPUS_DIR");
    if (corpus_dir && corpus_dir[0]) {
        snprintf(env_path_buffer, sizeof(env_path_buffer), "%s/%s", corpus_dir, "samba.zip");
        FILE *fp = fopen(env_path_buffer, "rb");
        if (fp) {
            if (resolved_path) {
                *resolved_path = env_path_buffer;
            }
            return fp;
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
        FILE *fp = fopen(candidates[i], "rb");
        if (fp) {
            if (resolved_path) {
                *resolved_path = candidates[i];
            }
            return fp;
        }
    }

    return NULL;
}

static int extract_zip_to_buffer(const char *zip_path, char **buffer_out, size_t *size_out)
{
    int pipefd[2];
    if (pipe(pipefd) != 0) {
        return -1;
    }

    pid_t pid = fork();
    if (pid < 0) {
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }

    if (pid == 0) {
        /* Child: redirect stdout to pipe and exec unzip */
        close(pipefd[0]);
        if (dup2(pipefd[1], STDOUT_FILENO) == -1) {
            _exit(127);
        }
        close(pipefd[1]);
        execlp("unzip", "unzip", "-p", zip_path, "samba", (char *)NULL);
        _exit(127);
    }

    close(pipefd[1]);
    FILE *stream = fdopen(pipefd[0], "rb");
    if (!stream) {
        close(pipefd[0]);
        waitpid(pid, NULL, 0);
        return -1;
    }

    size_t capacity = 1024 * 1024; /* start with 1MB */
    char *buffer = malloc(capacity);
    if (!buffer) {
        fclose(stream);
        waitpid(pid, NULL, 0);
        return -1;
    }

    size_t total = 0;
    char chunk[8192];
    while (1) {
        size_t bytes_read = fread(chunk, 1, sizeof(chunk), stream);
        if (bytes_read == 0) {
            if (feof(stream)) {
                break;
            }
            if (ferror(stream)) {
                free(buffer);
                fclose(stream);
                waitpid(pid, NULL, 0);
                return -1;
            }
        }

        if (total + bytes_read > capacity) {
            size_t new_capacity = capacity;
            while (total + bytes_read > new_capacity) {
                new_capacity *= 2;
            }
            char *tmp = realloc(buffer, new_capacity);
            if (!tmp) {
                free(buffer);
                fclose(stream);
                waitpid(pid, NULL, 0);
                return -1;
            }
            buffer = tmp;
            capacity = new_capacity;
        }

        memcpy(buffer + total, chunk, bytes_read);
        total += bytes_read;
    }

    fclose(stream);

    int status = 0;
    if (waitpid(pid, &status, 0) < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        free(buffer);
        return -1;
    }

    *buffer_out = buffer;
    *size_out = total;
    return 0;
}

static int load_samba_zip_chunks(struct chunk_dataset *dataset, int **hashes_out)
{
    const char *resolved_path = NULL;
    FILE *fp = open_samba_zip(&resolved_path);
    if (!fp) {
        fprintf(stderr, "Error: unable to locate SilesiaCorpus/samba.zip. Set SAMBA_ZIP_PATH or SILESIA_CORPUS_DIR if running outside the source tree.\n");
        return -1;
    }
    fclose(fp);

    printf("Using dataset: %s\n", resolved_path);

    char *raw_data = NULL;
    size_t raw_size = 0;
    if (extract_zip_to_buffer(resolved_path, &raw_data, &raw_size) != 0) {
        fprintf(stderr, "Error: failed to extract samba file from %s (ensure 'unzip' is available).\n", resolved_path);
        return -1;
    }

    if (raw_size < VALUE_SIZE) {
        free(raw_data);
        fprintf(stderr, "Error: extracted data smaller than chunk size\n");
        return -1;
    }

    size_t chunk_count = raw_size / VALUE_SIZE;
    if (chunk_count == 0) {
        free(raw_data);
        fprintf(stderr, "Error: insufficient data for 64-byte chunks\n");
        return -1;
    }

    size_t usable_bytes = chunk_count * VALUE_SIZE;
    if (usable_bytes != raw_size) {
        size_t dropped = raw_size - usable_bytes;
        printf("Warning: dropping %zu trailing bytes not forming a full chunk.\n", dropped);
    }

    char *data = raw_data;
    if (usable_bytes != raw_size) {
        char *trimmed = realloc(raw_data, usable_bytes);
        if (trimmed) {
            data = trimmed;
        }
    }

    int *hashes = malloc(chunk_count * sizeof(int));
    if (!hashes) {
        free(data);
        return -1;
    }

    for (size_t i = 0; i < chunk_count; i++) {
        hashes[i] = calculate_hash(data + i * VALUE_SIZE, VALUE_SIZE);
    }

    dataset->data = data;
    dataset->chunk_count = chunk_count;
    *hashes_out = hashes;
    return 0;
}

static double direct_compression_ratio(const struct chunk_dataset *dataset,
                                       compression_algo_t algo,
                                       const char *algo_name)
{
    printf("  Measuring direct %s compression ratio across %zu chunks...\n",
           algo_name, dataset->chunk_count);

    char compressed_buffer[VALUE_SIZE * 2];
    size_t total_original = 0;
    size_t total_compressed = 0;
    size_t successful = 0;

    qpl_job *qpl_job_ptr = NULL;
    uint8_t *qpl_job_buffer = NULL;

    if (algo == COMPRESS_QPL) {
        uint32_t job_size = 0;
        qpl_status status = qpl_get_job_size(qpl_path_auto, &job_size);
        if (status == QPL_STS_OK) {
            qpl_job_buffer = malloc(job_size);
            if (qpl_job_buffer) {
                qpl_job_ptr = (qpl_job *)qpl_job_buffer;
                status = qpl_init_job(qpl_path_auto, qpl_job_ptr);
                if (status != QPL_STS_OK) {
                    free(qpl_job_buffer);
                    qpl_job_buffer = NULL;
                    qpl_job_ptr = NULL;
                }
            }
        }
    }

    for (size_t i = 0; i < dataset->chunk_count; i++) {
        const char *chunk = dataset->data + i * VALUE_SIZE;
        int compressed_size = 0;

        if (algo == COMPRESS_LZ4) {
            compressed_size = LZ4_compress_default(chunk, compressed_buffer,
                                                   VALUE_SIZE, sizeof(compressed_buffer));
        } else if (algo == COMPRESS_QPL && qpl_job_ptr) {
            qpl_job_ptr->op = qpl_op_compress;
            qpl_job_ptr->next_in_ptr = (uint8_t *)chunk;
            qpl_job_ptr->available_in = VALUE_SIZE;
            qpl_job_ptr->next_out_ptr = (uint8_t *)compressed_buffer;
            qpl_job_ptr->available_out = sizeof(compressed_buffer);
            qpl_job_ptr->level = qpl_default_level;
            qpl_job_ptr->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

            qpl_status status = qpl_execute_job(qpl_job_ptr);
            if (status == QPL_STS_OK) {
                compressed_size = qpl_job_ptr->total_out;
            }
        } else {
            compressed_size = LZ4_compress_default(chunk, compressed_buffer,
                                                   VALUE_SIZE, sizeof(compressed_buffer));
        }

        if (compressed_size > 0) {
            total_original += VALUE_SIZE;
            total_compressed += (size_t)compressed_size;
            successful++;
        }
    }

    if (qpl_job_ptr) {
        qpl_fini_job(qpl_job_ptr);
    }
    if (qpl_job_buffer) {
        free(qpl_job_buffer);
    }

    double ratio = (successful == 0 || total_compressed == 0)
        ? 0.0
        : (double)total_original / (double)total_compressed;

    if (ratio > 0.0) {
        printf("    Compression ratio: %.3fx (%.1f%% savings)\n",
               ratio, (1.0 - 1.0 / ratio) * 100.0);
    } else {
        printf("    Compression ratio unavailable (compression failed)\n");
    }

    return ratio;
}

static benchmark_result_t run_benchmark(const struct chunk_dataset *dataset,
                                        const int *hashes,
                                        compression_algo_t algo,
                                        const char *algo_name)
{
    benchmark_result_t result = {
        .algorithm_name = algo_name,
        .algo = algo
    };

    printf("\n=== BENCHMARKING SAMBA.ZIP CHUNKS WITH %s ===\n", algo_name);
    printf("Total chunks: %zu (each %d bytes)\n", dataset->chunk_count, VALUE_SIZE);

    struct compression_config config = {
        .default_layout = LEAF_TYPE_LZ4_HASHED,
        .algo = algo,
        .default_sub_pages = 16,
        .compression_level = 0,
        .buffer_size = 512,
        .flush_threshold = 10,
        .enable_lazy_compression = 0
    };

    struct bplus_tree_compressed *ct_tree =
        bplus_tree_compressed_init_with_config(16, 64, &config);
    if (!ct_tree) {
        fprintf(stderr, "Error: unable to initialize compressed B+Tree\n");
        return result;
    }

    bplus_tree_compressed_set_compression(ct_tree, 1);

    double *latencies = malloc(dataset->chunk_count * sizeof(double));
    if (!latencies) {
        fprintf(stderr, "Error: unable to allocate latency array\n");
        bplus_tree_compressed_deinit(ct_tree);
        return result;
    }

    printf("Inserting chunks as key-value pairs...\n");
    double start_time = get_time_seconds();
    int successful = 0;

    for (size_t i = 0; i < dataset->chunk_count; i++) {
        int key = (int)i;
        int value = hashes[i];

        double insert_start = get_time_seconds();
        int rc = bplus_tree_compressed_put(ct_tree, key, value);
        double insert_end = get_time_seconds();

        if (rc == 0) {
            latencies[successful] = insert_end - insert_start;
            successful++;
        }

        if ((i + 1) % 10000 == 0) {
            printf("  Inserted %zu/%zu chunks (%.1f%%)\n",
                   i + 1, dataset->chunk_count,
                   (i + 1) * 100.0 / dataset->chunk_count);
        }
    }

    double end_time = get_time_seconds();
    result.total_time = end_time - start_time;
    result.successful_insertions = successful;
    result.throughput = (result.total_time > 0.0)
        ? successful / result.total_time
        : 0.0;
    result.p99_latency = calculate_p99_latency(latencies, successful);

    printf("Benchmark complete: %d/%zu successful insertions\n",
           successful, dataset->chunk_count);
    printf("  Total time: %.3f seconds\n", result.total_time);
    printf("  Throughput: %.0f ops/sec\n", result.throughput);
    printf("  P99 latency: %.2f microseconds\n", result.p99_latency);

    size_t total_uncompressed = 0;
    size_t total_compressed = 0;
    int stats_rc = bplus_tree_compressed_stats(ct_tree,
                                               &total_uncompressed,
                                               &total_compressed);

    if (stats_rc == 0 && total_compressed > 0) {
        result.compression_ratio = (double)total_uncompressed / total_compressed;
        result.original_size = total_uncompressed;
        result.compressed_size = total_compressed;
        printf("  Tree original size: %zu bytes (%.2f MB)\n",
               total_uncompressed, total_uncompressed / (1024.0 * 1024.0));
        printf("  Tree compressed size: %zu bytes (%.2f MB)\n",
               total_compressed, total_compressed / (1024.0 * 1024.0));
        printf("  Tree compression ratio: %.3fx (%.1f%% savings)\n",
               result.compression_ratio,
               (1.0 - 1.0 / result.compression_ratio) * 100.0);
    } else {
        result.compression_ratio = direct_compression_ratio(dataset, algo, algo_name);
        result.original_size = dataset->chunk_count * VALUE_SIZE;
        result.compressed_size = (result.compression_ratio > 0.0)
            ? (size_t)(result.original_size / result.compression_ratio)
            : 0;
    }

    free(latencies);
    bplus_tree_compressed_deinit(ct_tree);

    return result;
}

int main(void)
{
    printf("Samba.zip Chunk Compression Benchmark\n");
    printf("====================================\n\n");
    printf("Each chunk: %d bytes, shared across LZ4 and QPL runs\n", VALUE_SIZE);

    struct chunk_dataset dataset = {0};
    int *chunk_hashes = NULL;
    if (load_samba_zip_chunks(&dataset, &chunk_hashes) != 0) {
        return 1;
    }

    printf("Loaded %zu uniform chunks from SilesiaCorpus/samba.zip\n",
           dataset.chunk_count);

    benchmark_result_t results[2];
    results[0] = run_benchmark(&dataset, chunk_hashes, COMPRESS_LZ4, "LZ4");
    results[1] = run_benchmark(&dataset, chunk_hashes, COMPRESS_QPL, "QPL");

    printf("\n======== BENCHMARK SUMMARY ========\n");
    for (int i = 0; i < 2; i++) {
        printf("%s: throughput %.0f ops/sec, P99 %.2f us, compression %.3fx\n",
               results[i].algorithm_name,
               results[i].throughput,
               results[i].p99_latency,
               results[i].compression_ratio);
    }

    free(chunk_hashes);
    free(dataset.data);
    return 0;
}
