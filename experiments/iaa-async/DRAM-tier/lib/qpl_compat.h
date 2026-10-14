#ifndef ZIPCACHE_QPL_COMPAT_H
#define ZIPCACHE_QPL_COMPAT_H

/*
 * Keep the public compressed-tree API buildable when Intel QPL is absent.
 * The real definitions are used whenever CMake finds QPL.  The fallback
 * declarations deliberately make every QPL operation fail: callers using
 * qpl_path_auto can fall back to LZ4, while qpl_path_hardware remains strict.
 */
#ifdef HAVE_QPL
#include <qpl/qpl.h>
#else

#include <stdint.h>

typedef enum {
    qpl_path_software = 0,
    qpl_path_hardware = 1,
    qpl_path_auto = 2
} qpl_path_t;

typedef int qpl_status;

enum {
    QPL_STS_OK = 0,
    QPL_STS_NOT_SUPPORTED_MODE_ERR = 1
};

enum {
    qpl_op_compress = 0,
    qpl_op_decompress = 1
};

enum {
    qpl_default_level = 1
};

enum {
    QPL_FLAG_FIRST = 1u << 0,
    QPL_FLAG_LAST = 1u << 1,
    QPL_FLAG_DYNAMIC_HUFFMAN = 1u << 2
};

typedef struct qpl_job {
    int op;
    uint8_t *next_in_ptr;
    uint32_t available_in;
    uint32_t total_in;
    uint8_t *next_out_ptr;
    uint32_t available_out;
    uint32_t total_out;
    uint32_t flags;
    int level;
} qpl_job;

static inline qpl_status qpl_get_job_size(qpl_path_t path, uint32_t *size)
{
    (void)path;
    if (size) {
        *size = 0;
    }
    return QPL_STS_NOT_SUPPORTED_MODE_ERR;
}

static inline qpl_status qpl_init_job(qpl_path_t path, qpl_job *job)
{
    (void)path;
    (void)job;
    return QPL_STS_NOT_SUPPORTED_MODE_ERR;
}

static inline qpl_status qpl_fini_job(qpl_job *job)
{
    (void)job;
    return QPL_STS_NOT_SUPPORTED_MODE_ERR;
}

static inline qpl_status qpl_execute_job(qpl_job *job)
{
    (void)job;
    return QPL_STS_NOT_SUPPORTED_MODE_ERR;
}

static inline qpl_status qpl_submit_job(qpl_job *job)
{
    (void)job;
    return QPL_STS_NOT_SUPPORTED_MODE_ERR;
}

static inline qpl_status qpl_wait_job(qpl_job *job)
{
    (void)job;
    return QPL_STS_NOT_SUPPORTED_MODE_ERR;
}

#endif /* HAVE_QPL */

#endif /* ZIPCACHE_QPL_COMPAT_H */
