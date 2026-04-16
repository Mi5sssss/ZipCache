#ifndef COMPRESSED_TEST_UTILS_H
#define COMPRESSED_TEST_UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <limits.h>
#include <sys/wait.h>
#include <unistd.h>

#include "bplustree_compressed.h"

#if defined(__GNUC__)
#define BTREE_TEST_UNUSED __attribute__((unused))
#else
#define BTREE_TEST_UNUSED
#endif

static BTREE_TEST_UNUSED int btree_env_int(const char *name, int fallback, int min_value)
{
    const char *value = getenv(name);
    if (!value || !*value) {
        return fallback;
    }

    char *end = NULL;
    long parsed = strtol(value, &end, 10);
    if (end == value || *end != '\0' || parsed < min_value || parsed > 2147483647L) {
        fprintf(stderr, "Invalid %s=%s; expected integer >= %d\n", name, value, min_value);
        exit(EXIT_FAILURE);
    }
    return (int)parsed;
}

static BTREE_TEST_UNUSED int btree_env_bool(const char *name, int fallback)
{
    const char *value = getenv(name);
    if (!value || !*value) {
        return fallback;
    }
    if (strcasecmp(value, "1") == 0 ||
        strcasecmp(value, "true") == 0 ||
        strcasecmp(value, "yes") == 0 ||
        strcasecmp(value, "y") == 0) {
        return 1;
    }
    if (strcasecmp(value, "0") == 0 ||
        strcasecmp(value, "false") == 0 ||
        strcasecmp(value, "no") == 0 ||
        strcasecmp(value, "n") == 0) {
        return 0;
    }
    fprintf(stderr, "Invalid %s=%s; expected boolean\n", name, value);
    exit(EXIT_FAILURE);
}

static BTREE_TEST_UNUSED qpl_path_t btree_parse_qpl_path(void)
{
    const char *value = getenv("BTREE_QPL_PATH");
    if (!value || !*value || strcasecmp(value, "auto") == 0) {
        return qpl_path_auto;
    }
    if (strcasecmp(value, "software") == 0 || strcasecmp(value, "soft") == 0) {
        return qpl_path_software;
    }
    if (strcasecmp(value, "hardware") == 0 || strcasecmp(value, "hw") == 0) {
        return qpl_path_hardware;
    }

    fprintf(stderr, "Invalid BTREE_QPL_PATH=%s; use auto, software, or hardware\n", value);
    exit(EXIT_FAILURE);
}

static BTREE_TEST_UNUSED qpl_huffman_mode_t btree_parse_qpl_mode(void)
{
    const char *value = getenv("BTREE_QPL_MODE");
    if (!value || !*value || strcasecmp(value, "fixed") == 0) {
        return QPL_HUFFMAN_FIXED;
    }
    if (strcasecmp(value, "dynamic") == 0) {
        return QPL_HUFFMAN_DYNAMIC;
    }

    fprintf(stderr, "Invalid BTREE_QPL_MODE=%s; use fixed or dynamic\n", value);
    exit(EXIT_FAILURE);
}

static BTREE_TEST_UNUSED void btree_apply_qpl_env(struct compression_config *cfg)
{
    if (cfg->algo != COMPRESS_QPL) {
        return;
    }
    cfg->qpl_path = btree_parse_qpl_path();
    cfg->qpl_huffman_mode = btree_parse_qpl_mode();
}

static BTREE_TEST_UNUSED const char *btree_algo_name(compression_algo_t algo)
{
    switch (algo) {
    case COMPRESS_LZ4:
        return "lz4";
    case COMPRESS_QPL:
        return "qpl";
    case COMPRESS_ZLIB_ACCEL:
        return "zlib_accel";
    default:
        return "unknown";
    }
}

static BTREE_TEST_UNUSED const char *btree_qpl_path_name(qpl_path_t path)
{
    switch (path) {
    case qpl_path_software:
        return "software";
    case qpl_path_hardware:
        return "hardware";
    case qpl_path_auto:
    default:
        return "auto";
    }
}

static BTREE_TEST_UNUSED const char *btree_qpl_mode_name(qpl_huffman_mode_t mode)
{
    return mode == QPL_HUFFMAN_DYNAMIC ? "dynamic" : "fixed";
}

struct btree_silesia_dataset {
    uint8_t *data;
    size_t chunk_count;
    size_t chunk_bytes;
};

static BTREE_TEST_UNUSED const char *btree_resolve_samba_zip(char *buffer, size_t buffer_len)
{
    const char *override = getenv("SAMBA_ZIP_PATH");
    if (override && *override) {
        if (access(override, R_OK) == 0) {
            return override;
        }
        fprintf(stderr, "SAMBA_ZIP_PATH=%s is not readable\n", override);
    }

    const char *dir = getenv("SILESIA_CORPUS_DIR");
    if (dir && *dir) {
        snprintf(buffer, buffer_len, "%s/%s", dir, "samba.zip");
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
            snprintf(buffer, buffer_len, "%s", candidates[i]);
            return buffer;
        }
    }

    return NULL;
}

static BTREE_TEST_UNUSED int btree_load_silesia_samba(struct btree_silesia_dataset *out,
                                                      size_t chunk_bytes,
                                                      size_t max_chunks)
{
    if (!out || chunk_bytes == 0 || chunk_bytes > COMPRESSED_VALUE_BYTES) {
        return -1;
    }

    char path[PATH_MAX];
    const char *resolved = btree_resolve_samba_zip(path, sizeof(path));
    if (!resolved) {
        fprintf(stderr,
                "Unable to locate SilesiaCorpus/samba.zip; set SAMBA_ZIP_PATH or SILESIA_CORPUS_DIR\n");
        return -1;
    }

    char cmd[PATH_MAX + 32];
    snprintf(cmd, sizeof(cmd), "unzip -p \"%s\" samba", resolved);
    FILE *pipe = popen(cmd, "r");
    if (!pipe) {
        perror("popen unzip");
        return -1;
    }

    size_t capacity = chunk_bytes * (max_chunks > 0 ? max_chunks : 1024);
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
        fprintf(stderr, "unzip failed while reading %s (status=%d)\n", resolved, status);
        free(buffer);
        return -1;
    }

    size_t chunk_count = total / chunk_bytes;
    if (chunk_count == 0) {
        fprintf(stderr, "samba dataset has fewer than %zu bytes\n", chunk_bytes);
        free(buffer);
        return -1;
    }
    if (max_chunks > 0 && chunk_count > max_chunks) {
        chunk_count = max_chunks;
    }

    size_t usable = chunk_count * chunk_bytes;
    uint8_t *trimmed = realloc(buffer, usable);
    if (trimmed) {
        buffer = trimmed;
    }

    out->data = buffer;
    out->chunk_count = chunk_count;
    out->chunk_bytes = chunk_bytes;
    fprintf(stderr,
            "Using Silesia samba dataset: path=%s chunk_bytes=%zu chunks=%zu\n",
            resolved,
            out->chunk_bytes,
            out->chunk_count);
    return 0;
}

static BTREE_TEST_UNUSED void btree_free_silesia_dataset(struct btree_silesia_dataset *dataset)
{
    if (!dataset) {
        return;
    }
    free(dataset->data);
    dataset->data = NULL;
    dataset->chunk_count = 0;
    dataset->chunk_bytes = 0;
}

static BTREE_TEST_UNUSED const uint8_t *btree_silesia_payload_for_key_version(
    const struct btree_silesia_dataset *dataset,
    key_t key,
    int version)
{
    if (!dataset || !dataset->data || dataset->chunk_count == 0) {
        return NULL;
    }

    long long raw = (long long)key - 1LL + (long long)version;
    long long mod = raw % (long long)dataset->chunk_count;
    if (mod < 0) {
        mod += (long long)dataset->chunk_count;
    }
    return dataset->data + ((size_t)mod * dataset->chunk_bytes);
}

#endif /* COMPRESSED_TEST_UTILS_H */
