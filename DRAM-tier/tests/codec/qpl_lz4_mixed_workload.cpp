#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <mutex>
#include <strings.h>

#include <lz4.h>
#include <qpl/qpl.h>
#ifdef HAVE_ZLIB
#include <zlib.h>
#endif
#include <unistd.h>
#include <sys/wait.h>
#include <limits.h>

extern "C" {
#include "bplustree_compressed.h"
}

#define DEFAULT_BLOCK_SIZE COMPRESSED_LEAF_SIZE
#define DEFAULT_BLOCKS 4096
#define DEFAULT_OCCUPANCY 50
#define DEFAULT_BATCH_SIZE 8
#define DEFAULT_DURATION_SEC 60
#define DEFAULT_PACKED_BLOCKS 1

// Workload percentages
#define DEFAULT_READ_PCT 50
#define DEFAULT_WRITE_PCT 30
#define DEFAULT_COMPACT_PCT 20
#define DEFAULT_CPU_WORK_US 0.5

struct kv_pair {
    key_t key;
    int stored_value;
    uint8_t payload[COMPRESSED_VALUE_BYTES];
};

struct LatencyStats {
    double avg_us;
    double p50_us;
    double p99_us;
    double p999_us;
};

// Shared compressed block storage
struct CompressedBlock {
    std::vector<uint8_t> data;
    int compressed_size;
    std::mutex lock;
};

struct SystemMetrics {
    std::atomic<uint64_t> read_ops{0};
    std::atomic<uint64_t> write_ops{0};
    std::atomic<uint64_t> compact_ops{0};
    std::atomic<bool> running{true};
};

enum class Codec {
    LZ4,
    QPL,
    ZLIB_ACCEL
};

// Helpers
static int parse_env_int(const char *name, int fallback) {
    const char *v = std::getenv(name);
    if (!v || !*v) return fallback;
    try { return std::stoi(v); } catch (...) { return fallback; }
}

static double parse_env_double(const char *name, double fallback) {
    const char *v = std::getenv(name);
    if (!v || !*v) return fallback;
    try { return std::stod(v); } catch (...) { return fallback; }
}

static bool parse_env_bool(const char *name, bool fallback) {
    const char *v = std::getenv(name);
    if (!v || !*v) return fallback;
    if (!strcasecmp(v, "1") || !strcasecmp(v, "true") ||
        !strcasecmp(v, "yes") || !strcasecmp(v, "on")) {
        return true;
    }
    if (!strcasecmp(v, "0") || !strcasecmp(v, "false") ||
        !strcasecmp(v, "no") || !strcasecmp(v, "off")) {
        return false;
    }
    return fallback;
}

static bool qpl_async_foreground_enabled() {
    return parse_env_bool("KV_QPL_ASYNC_FOREGROUND", false);
}

static void die(const char *msg) {
    std::perror(msg);
    std::exit(EXIT_FAILURE);
}

static void simulate_cpu_work_us(double microseconds) {
    if (microseconds <= 0) return;
    auto start = std::chrono::steady_clock::now();
    volatile uint64_t x = 0;
    while (std::chrono::duration<double, std::micro>(
               std::chrono::steady_clock::now() - start).count() < microseconds) {
        x++;
    }
}

// Load data (same as async benchmark)
static const char *resolve_samba_zip(char *buffer, size_t buf_len) {
    const char *override = getenv("SAMBA_ZIP_PATH");
    if (override && *override && access(override, R_OK) == 0) return override;
    const char *dir = getenv("SILESIA_CORPUS_DIR");
    if (dir && *dir) {
        snprintf(buffer, buf_len, "%s/%s", dir, "samba.zip");
        if (access(buffer, R_OK) == 0) return buffer;
    }
    const char *candidates[] = {
        "SilesiaCorpus/samba.zip", "../SilesiaCorpus/samba.zip",
        "../../SilesiaCorpus/samba.zip", "../../../SilesiaCorpus/samba.zip"
    };
    for (const auto &c : candidates) {
        if (access(c, R_OK) == 0) {
            snprintf(buffer, buf_len, "%s", c);
            return buffer;
        }
    }
    return nullptr;
}

static std::vector<uint8_t> load_samba_bytes() {
    char path[PATH_MAX];
    const char *resolved = resolve_samba_zip(path, sizeof(path));
    if (!resolved) return {};
    std::string cmd = "unzip -p \"" + std::string(resolved) + "\" samba";
    FILE *pipe = popen(cmd.c_str(), "r");
    if (!pipe) return {};
    std::vector<uint8_t> buf;
    uint8_t tmp[4096];
    while (true) {
        size_t n = fread(tmp, 1, sizeof(tmp), pipe);
        if (n > 0) buf.insert(buf.end(), tmp, tmp + n);
        if (n < sizeof(tmp)) break;
    }
    pclose(pipe);
    return buf;
}

static void fill_kv_blocks(uint8_t *buf, int blocks, int occupancy_pct,
                           const std::vector<uint8_t> &payload, size_t block_size) {
    size_t payload_pos = 0;
    std::mt19937 rng(1234);
    std::uniform_int_distribution<int> dist(0, 99);
    int next_key = 1;
    size_t slots = block_size / sizeof(struct kv_pair);
    for (int b = 0; b < blocks; b++) {
        struct kv_pair *page = (struct kv_pair *)(buf + (size_t)b * block_size);
        for (size_t i = 0; i < slots; i++) {
            if (dist(rng) >= occupancy_pct) {
                std::memset(&page[i], 0, sizeof(struct kv_pair));
                continue;
            }
            page[i].key = next_key++;
            page[i].stored_value = page[i].key ^ 0x5a5a5a5a;
            if (!payload.empty()) {
                size_t remain = payload.size() - payload_pos;
                if (remain < sizeof(page[i].payload)) payload_pos = 0;
                std::memcpy(page[i].payload, payload.data() + payload_pos, sizeof(page[i].payload));
                payload_pos += sizeof(page[i].payload);
            } else {
                for (size_t j = 0; j < sizeof(page[i].payload); j++) {
                    page[i].payload[j] = (uint8_t)((page[i].key * 131 + j * 17) & 0xff);
                }
            }
        }
    }
}

static void fill_packed_blocks(uint8_t *buf, int blocks,
                               const std::vector<uint8_t> &payload,
                               size_t block_size) {
    size_t payload_pos = 0;
    uint32_t seed = 0x9e3779b9U;

    for (int b = 0; b < blocks; b++) {
        uint8_t *dst = buf + (size_t)b * block_size;
        if (!payload.empty()) {
            for (size_t off = 0; off < block_size;) {
                if (payload_pos >= payload.size()) {
                    payload_pos = 0;
                }
                size_t copy = std::min(block_size - off, payload.size() - payload_pos);
                std::memcpy(dst + off, payload.data() + payload_pos, copy);
                payload_pos += copy;
                off += copy;
            }
        } else {
            for (size_t off = 0; off < block_size; off++) {
                seed = seed * 1664525U + 1013904223U;
                dst[off] = (uint8_t)(seed >> 24);
            }
        }
    }
}

static LatencyStats compute_stats(std::vector<double> &samples) {
    LatencyStats out = {0};
    if (samples.empty()) return out;
    double sum = std::accumulate(samples.begin(), samples.end(), 0.0);
    out.avg_us = sum / samples.size();
    std::sort(samples.begin(), samples.end());
    auto pick = [&](double pct) {
        double idx = (pct / 100.0) * (samples.size() - 1);
        int lo = (int)idx;
        int hi = std::min(lo + 1, (int)samples.size() - 1);
        double frac = idx - lo;
        return samples[lo] + frac * (samples[hi] - samples[lo]);
    };
    out.p50_us = pick(50.0);
    out.p99_us = pick(99.0);
    out.p999_us = pick(99.9);
    return out;
}

struct ThreadContext {
    int thread_id;
    double cpu_work_us;
    bool use_dynamic_huffman;
    qpl_path_t qpl_path;
    
    const uint8_t *src_base;  // Uncompressed source data
    CompressedBlock *compressed_blocks;  // Shared compressed storage
    int total_blocks;
    size_t block_size;
    int batch_size;  // For compaction workers
    
    std::vector<uint8_t> temp_buf;  // Thread-local temp buffer
    std::vector<uint8_t> decomp_buf;
    std::vector<double> latencies;
    
    SystemMetrics *metrics;
};

// === READ WORKER (Decompress) ===
static void read_worker_lz4(ThreadContext &ctx) {
    std::mt19937 rng(ctx.thread_id * 1000);
    std::uniform_int_distribution<int> block_dist(0, ctx.total_blocks - 1);
    ctx.decomp_buf.resize(ctx.block_size);
    std::vector<uint8_t> local_compressed(ctx.block_size * 2);
    
    while (ctx.metrics->running) {
        int block_idx = block_dist(rng);
        simulate_cpu_work_us(ctx.cpu_work_us);
        
        // Read compressed data from shared storage
        int comp_size;
        {
            std::lock_guard<std::mutex> lock(ctx.compressed_blocks[block_idx].lock);
            comp_size = ctx.compressed_blocks[block_idx].compressed_size;
            if (comp_size > 0) {
                if ((size_t)comp_size > local_compressed.size()) {
                    local_compressed.resize((size_t)comp_size);
                }
                std::memcpy(local_compressed.data(), 
                           ctx.compressed_blocks[block_idx].data.data(),
                           comp_size);
            }
        }
        
        if (comp_size > 0) {
            auto t0 = std::chrono::steady_clock::now();
            int out = LZ4_decompress_safe((const char*)local_compressed.data(),
                                         (char*)ctx.decomp_buf.data(),
                                         comp_size, ctx.block_size);
            auto t1 = std::chrono::steady_clock::now();
            
            if (out > 0) {
                ctx.latencies.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
            }
        }
        ctx.metrics->read_ops++;
    }
}

static void read_worker_qpl(ThreadContext &ctx) {
    std::mt19937 rng(ctx.thread_id * 1000);
    std::uniform_int_distribution<int> block_dist(0, ctx.total_blocks - 1);
    ctx.decomp_buf.resize(ctx.block_size);
    std::vector<uint8_t> local_compressed(ctx.block_size * 2);
    
    // Initialize QPL job
    uint32_t job_size = 0;
    qpl_get_job_size(ctx.qpl_path, &job_size);
    std::vector<uint8_t> job_buf(job_size);
    qpl_job *job = (qpl_job *)job_buf.data();
    if (qpl_init_job(ctx.qpl_path, job) != QPL_STS_OK) die("qpl_init_job (read)");
    
    while (ctx.metrics->running) {
        int block_idx = block_dist(rng);
        simulate_cpu_work_us(ctx.cpu_work_us);
        
        // Read compressed data from shared storage
        int comp_size;
        {
            std::lock_guard<std::mutex> lock(ctx.compressed_blocks[block_idx].lock);
            comp_size = ctx.compressed_blocks[block_idx].compressed_size;
            if (comp_size > 0) {
                if ((size_t)comp_size > local_compressed.size()) {
                    local_compressed.resize((size_t)comp_size);
                }
                std::memcpy(local_compressed.data(),
                           ctx.compressed_blocks[block_idx].data.data(),
                           comp_size);
            }
        }
        
        if (comp_size > 0) {
            job->op = qpl_op_decompress;
            job->next_in_ptr = local_compressed.data();
            job->available_in = comp_size;
            job->next_out_ptr = ctx.decomp_buf.data();
            job->available_out = ctx.block_size;
            job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
            
            auto t0 = std::chrono::steady_clock::now();
            qpl_status st = qpl_execute_job(job);
            auto t1 = std::chrono::steady_clock::now();
            
            if (st == QPL_STS_OK) {
                ctx.latencies.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
            }
        }
        ctx.metrics->read_ops++;
    }
    
    qpl_fini_job(job);
}

static void read_worker_qpl_async(ThreadContext &ctx) {
    std::mt19937 rng(ctx.thread_id * 1000);
    std::uniform_int_distribution<int> block_dist(0, ctx.total_blocks - 1);

    int batch_size = std::max(1, ctx.batch_size);
    uint32_t job_size = 0;
    qpl_get_job_size(ctx.qpl_path, &job_size);

    std::vector<std::vector<uint8_t>> job_bufs(batch_size);
    std::vector<qpl_job *> jobs(batch_size);
    std::vector<std::vector<uint8_t>> local_compressed(batch_size);
    std::vector<std::vector<uint8_t>> decomp_bufs(batch_size);
    std::vector<int> comp_sizes(batch_size, 0);
    std::vector<char> active(batch_size, 0);

    for (int k = 0; k < batch_size; k++) {
        job_bufs[k].resize(job_size);
        jobs[k] = (qpl_job *)job_bufs[k].data();
        if (qpl_init_job(ctx.qpl_path, jobs[k]) != QPL_STS_OK) die("qpl_init_job (read async)");
        local_compressed[k].resize(ctx.block_size * 2);
        decomp_bufs[k].resize(ctx.block_size);
    }

    while (ctx.metrics->running) {
        int submitted = 0;
        simulate_cpu_work_us(ctx.cpu_work_us);
        auto t0 = std::chrono::steady_clock::now();

        for (int k = 0; k < batch_size; k++) {
            int block_idx = block_dist(rng);
            active[k] = 0;
            comp_sizes[k] = 0;

            {
                std::lock_guard<std::mutex> lock(ctx.compressed_blocks[block_idx].lock);
                comp_sizes[k] = ctx.compressed_blocks[block_idx].compressed_size;
                if (comp_sizes[k] > 0) {
                    if ((size_t)comp_sizes[k] > local_compressed[k].size()) {
                        local_compressed[k].resize((size_t)comp_sizes[k]);
                    }
                    std::memcpy(local_compressed[k].data(),
                                ctx.compressed_blocks[block_idx].data.data(),
                                comp_sizes[k]);
                }
            }

            if (comp_sizes[k] <= 0) {
                continue;
            }

            jobs[k]->op = qpl_op_decompress;
            jobs[k]->next_in_ptr = local_compressed[k].data();
            jobs[k]->available_in = comp_sizes[k];
            jobs[k]->next_out_ptr = decomp_bufs[k].data();
            jobs[k]->available_out = ctx.block_size;
            jobs[k]->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

            qpl_status st = qpl_submit_job(jobs[k]);
            if (st == QPL_STS_OK) {
                active[k] = 1;
                submitted++;
            }
        }

        int completed = 0;
        for (int k = 0; k < batch_size; k++) {
            if (!active[k]) {
                continue;
            }
            qpl_status st = qpl_wait_job(jobs[k]);
            if (st == QPL_STS_OK) {
                completed++;
            }
        }

        auto t1 = std::chrono::steady_clock::now();
        if (completed > 0) {
            double per_op_us = std::chrono::duration<double, std::micro>(t1 - t0).count() / completed;
            for (int i = 0; i < completed; i++) {
                ctx.latencies.push_back(per_op_us);
            }
        }
        ctx.metrics->read_ops += submitted;
    }

    for (int k = 0; k < batch_size; k++) {
        qpl_fini_job(jobs[k]);
    }
}

#ifdef HAVE_ZLIB
static void read_worker_zlib(ThreadContext &ctx) {
    std::mt19937 rng(ctx.thread_id * 1000);
    std::uniform_int_distribution<int> block_dist(0, ctx.total_blocks - 1);
    ctx.decomp_buf.resize(ctx.block_size);
    std::vector<uint8_t> local_compressed(ctx.block_size * 2);

    while (ctx.metrics->running) {
        int block_idx = block_dist(rng);
        simulate_cpu_work_us(ctx.cpu_work_us);

        int comp_size;
        {
            std::lock_guard<std::mutex> lock(ctx.compressed_blocks[block_idx].lock);
            comp_size = ctx.compressed_blocks[block_idx].compressed_size;
            if (comp_size > 0) {
                if ((size_t)comp_size > local_compressed.size()) {
                    local_compressed.resize(comp_size);
                }
                std::memcpy(local_compressed.data(),
                           ctx.compressed_blocks[block_idx].data.data(),
                           comp_size);
            }
        }

        if (comp_size > 0) {
            uLongf produced = (uLongf)ctx.decomp_buf.size();
            auto t0 = std::chrono::steady_clock::now();
            int status = uncompress((Bytef *)ctx.decomp_buf.data(),
                                    &produced,
                                    (const Bytef *)local_compressed.data(),
                                    (uLong)comp_size);
            auto t1 = std::chrono::steady_clock::now();

            if (status == Z_OK) {
                ctx.latencies.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
            }
        }
        ctx.metrics->read_ops++;
    }
}
#endif

// === WRITE WORKER (Compress) ===
static void write_worker_lz4(ThreadContext &ctx) {
    std::mt19937 rng(ctx.thread_id * 2000);
    std::uniform_int_distribution<int> block_dist(0, ctx.total_blocks - 1);
    ctx.temp_buf.resize(ctx.block_size * 2);
    
    while (ctx.metrics->running) {
        int block_idx = block_dist(rng);
        simulate_cpu_work_us(ctx.cpu_work_us);
        
        const char *src = (const char *)(ctx.src_base + block_idx * ctx.block_size);
        
        auto t0 = std::chrono::steady_clock::now();
        int comp_size = LZ4_compress_default(src, (char*)ctx.temp_buf.data(),
                                             ctx.block_size, ctx.block_size * 2);
        auto t1 = std::chrono::steady_clock::now();
        
        if (comp_size > 0) {
            ctx.latencies.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
            
            // Write back to shared storage
            std::lock_guard<std::mutex> lock(ctx.compressed_blocks[block_idx].lock);
            ctx.compressed_blocks[block_idx].data.resize(comp_size);
            std::memcpy(ctx.compressed_blocks[block_idx].data.data(),
                       ctx.temp_buf.data(), comp_size);
            ctx.compressed_blocks[block_idx].compressed_size = comp_size;
        }
        ctx.metrics->write_ops++;
    }
}

static void write_worker_qpl(ThreadContext &ctx) {
    std::mt19937 rng(ctx.thread_id * 2000);
    std::uniform_int_distribution<int> block_dist(0, ctx.total_blocks - 1);
    ctx.temp_buf.resize(ctx.block_size * 2);
    
    uint32_t job_size = 0;
    qpl_get_job_size(ctx.qpl_path, &job_size);
    std::vector<uint8_t> job_buf(job_size);
    qpl_job *job = (qpl_job *)job_buf.data();
    if (qpl_init_job(ctx.qpl_path, job) != QPL_STS_OK) die("qpl_init_job (write)");
    
    while (ctx.metrics->running) {
        int block_idx = block_dist(rng);
        simulate_cpu_work_us(ctx.cpu_work_us);
        
        const uint8_t *src = ctx.src_base + block_idx * ctx.block_size;
        
        job->op = qpl_op_compress;
        job->next_in_ptr = (uint8_t *)src;
        job->available_in = ctx.block_size;
        job->next_out_ptr = ctx.temp_buf.data();
        job->available_out = ctx.block_size * 2;
        job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;
        if (ctx.use_dynamic_huffman) job->flags |= QPL_FLAG_DYNAMIC_HUFFMAN;
        job->level = qpl_default_level;
        
        auto t0 = std::chrono::steady_clock::now();
        qpl_status st = qpl_execute_job(job);
        auto t1 = std::chrono::steady_clock::now();
        
        if (st == QPL_STS_OK) {
            int comp_size = job->total_out;
            ctx.latencies.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
            
            // Write back to shared storage
            std::lock_guard<std::mutex> lock(ctx.compressed_blocks[block_idx].lock);
            ctx.compressed_blocks[block_idx].data.resize(comp_size);
            std::memcpy(ctx.compressed_blocks[block_idx].data.data(),
                       ctx.temp_buf.data(), comp_size);
            ctx.compressed_blocks[block_idx].compressed_size = comp_size;
        }
        ctx.metrics->write_ops++;
    }
    
    qpl_fini_job(job);
}

static void write_worker_qpl_async(ThreadContext &ctx) {
    std::mt19937 rng(ctx.thread_id * 2000);
    std::uniform_int_distribution<int> block_dist(0, ctx.total_blocks - 1);

    int batch_size = std::max(1, ctx.batch_size);
    uint32_t job_size = 0;
    qpl_get_job_size(ctx.qpl_path, &job_size);

    std::vector<std::vector<uint8_t>> job_bufs(batch_size);
    std::vector<qpl_job *> jobs(batch_size);
    std::vector<std::vector<uint8_t>> comp_bufs(batch_size);
    std::vector<int> block_indices(batch_size, 0);
    std::vector<char> active(batch_size, 0);

    for (int k = 0; k < batch_size; k++) {
        job_bufs[k].resize(job_size);
        jobs[k] = (qpl_job *)job_bufs[k].data();
        if (qpl_init_job(ctx.qpl_path, jobs[k]) != QPL_STS_OK) die("qpl_init_job (write async)");
        comp_bufs[k].resize(ctx.block_size * 2);
    }

    while (ctx.metrics->running) {
        int submitted = 0;
        simulate_cpu_work_us(ctx.cpu_work_us);
        auto t0 = std::chrono::steady_clock::now();

        for (int k = 0; k < batch_size; k++) {
            block_indices[k] = block_dist(rng);
            const uint8_t *src = ctx.src_base + (size_t)block_indices[k] * ctx.block_size;
            active[k] = 0;

            jobs[k]->op = qpl_op_compress;
            jobs[k]->next_in_ptr = (uint8_t *)src;
            jobs[k]->available_in = ctx.block_size;
            jobs[k]->next_out_ptr = comp_bufs[k].data();
            jobs[k]->available_out = ctx.block_size * 2;
            jobs[k]->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;
            if (ctx.use_dynamic_huffman) jobs[k]->flags |= QPL_FLAG_DYNAMIC_HUFFMAN;
            jobs[k]->level = qpl_default_level;

            qpl_status st = qpl_submit_job(jobs[k]);
            if (st == QPL_STS_OK) {
                active[k] = 1;
                submitted++;
            }
        }

        int completed = 0;
        for (int k = 0; k < batch_size; k++) {
            if (!active[k]) {
                continue;
            }
            qpl_status st = qpl_wait_job(jobs[k]);
            if (st == QPL_STS_OK && jobs[k]->total_out > 0) {
                int comp_size = (int)jobs[k]->total_out;
                std::lock_guard<std::mutex> lock(ctx.compressed_blocks[block_indices[k]].lock);
                ctx.compressed_blocks[block_indices[k]].data.resize(comp_size);
                std::memcpy(ctx.compressed_blocks[block_indices[k]].data.data(),
                            comp_bufs[k].data(),
                            comp_size);
                ctx.compressed_blocks[block_indices[k]].compressed_size = comp_size;
                completed++;
            }
        }

        auto t1 = std::chrono::steady_clock::now();
        if (completed > 0) {
            double per_op_us = std::chrono::duration<double, std::micro>(t1 - t0).count() / completed;
            for (int i = 0; i < completed; i++) {
                ctx.latencies.push_back(per_op_us);
            }
        }
        ctx.metrics->write_ops += submitted;
    }

    for (int k = 0; k < batch_size; k++) {
        qpl_fini_job(jobs[k]);
    }
}

#ifdef HAVE_ZLIB
static void write_worker_zlib(ThreadContext &ctx) {
    std::mt19937 rng(ctx.thread_id * 2000);
    std::uniform_int_distribution<int> block_dist(0, ctx.total_blocks - 1);
    ctx.temp_buf.resize(ctx.block_size * 2);

    while (ctx.metrics->running) {
        int block_idx = block_dist(rng);
        simulate_cpu_work_us(ctx.cpu_work_us);

        const uint8_t *src = ctx.src_base + block_idx * ctx.block_size;
        uLongf produced = (uLongf)ctx.temp_buf.size();

        auto t0 = std::chrono::steady_clock::now();
        int status = compress2((Bytef *)ctx.temp_buf.data(),
                               &produced,
                               (const Bytef *)src,
                               (uLong)ctx.block_size,
                               Z_DEFAULT_COMPRESSION);
        auto t1 = std::chrono::steady_clock::now();

        if (status == Z_OK && produced > 0) {
            int comp_size = (int)produced;
            ctx.latencies.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());

            std::lock_guard<std::mutex> lock(ctx.compressed_blocks[block_idx].lock);
            ctx.compressed_blocks[block_idx].data.resize(comp_size);
            std::memcpy(ctx.compressed_blocks[block_idx].data.data(),
                       ctx.temp_buf.data(), comp_size);
            ctx.compressed_blocks[block_idx].compressed_size = comp_size;
        }
        ctx.metrics->write_ops++;
    }
}
#endif

// === COMPACTION WORKER (Background compress) ===
static void compaction_worker_lz4(ThreadContext &ctx, int batch_size) {
    std::mt19937 rng(ctx.thread_id * 3000);
    std::uniform_int_distribution<int> block_dist(0, ctx.total_blocks - 1);
    std::vector<uint8_t> comp_buf(ctx.block_size * 2);

    while (ctx.metrics->running) {
        for (int k = 0; k < batch_size; k++) {
            int block_idx = block_dist(rng);
            const char *src = (const char *)(ctx.src_base + (size_t)block_idx * ctx.block_size);
            int out = LZ4_compress_default(src,
                                           (char *)comp_buf.data(),
                                           (int)ctx.block_size,
                                           (int)comp_buf.size());
            if (out > 0) {
                ctx.metrics->compact_ops++;
            }
        }
    }
}

#ifdef HAVE_ZLIB
static void compaction_worker_zlib(ThreadContext &ctx, int batch_size) {
    std::mt19937 rng(ctx.thread_id * 3000);
    std::uniform_int_distribution<int> block_dist(0, ctx.total_blocks - 1);
    std::vector<uint8_t> comp_buf(ctx.block_size * 2);

    while (ctx.metrics->running) {
        for (int k = 0; k < batch_size; k++) {
            int block_idx = block_dist(rng);
            const uint8_t *src = ctx.src_base + (size_t)block_idx * ctx.block_size;
            uLongf produced = (uLongf)comp_buf.size();
            int status = compress2((Bytef *)comp_buf.data(),
                                   &produced,
                                   (const Bytef *)src,
                                   (uLong)ctx.block_size,
                                   Z_DEFAULT_COMPRESSION);
            if (status == Z_OK && produced > 0) {
                ctx.metrics->compact_ops++;
            }
        }
    }
}
#endif

static void compaction_worker_qpl(ThreadContext &ctx, int batch_size) {
    std::mt19937 rng(ctx.thread_id * 3000);
    std::uniform_int_distribution<int> block_dist(0, ctx.total_blocks - 1);
    
    uint32_t job_size = 0;
    qpl_get_job_size(ctx.qpl_path, &job_size);
    std::vector<std::vector<uint8_t>> job_bufs(batch_size);
    std::vector<qpl_job*> jobs(batch_size);
    std::vector<std::vector<uint8_t>> comp_bufs(batch_size);
    
    for (int k = 0; k < batch_size; k++) {
        job_bufs[k].resize(job_size);
        jobs[k] = (qpl_job *)job_bufs[k].data();
        if (qpl_init_job(ctx.qpl_path, jobs[k]) != QPL_STS_OK) die("qpl_init_job (compact)");
        comp_bufs[k].resize(ctx.block_size * 2);
    }
    
    while (ctx.metrics->running) {
        // Submit batch
        for (int k = 0; k < batch_size; k++) {
            int block_idx = block_dist(rng);
            const uint8_t *src = ctx.src_base + block_idx * ctx.block_size;
            
            jobs[k]->op = qpl_op_compress;
            jobs[k]->next_in_ptr = (uint8_t *)src;
            jobs[k]->available_in = ctx.block_size;
            jobs[k]->next_out_ptr = comp_bufs[k].data();
            jobs[k]->available_out = ctx.block_size * 2;
            jobs[k]->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;
            if (ctx.use_dynamic_huffman) jobs[k]->flags |= QPL_FLAG_DYNAMIC_HUFFMAN;
            jobs[k]->level = qpl_default_level;
            
            qpl_submit_job(jobs[k]);
        }
        
        // Wait all
        for (int k = 0; k < batch_size; k++) {
            qpl_wait_job(jobs[k]);
            ctx.metrics->compact_ops++;
        }
    }
    
    for (int k = 0; k < batch_size; k++) {
        qpl_fini_job(jobs[k]);
    }
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    // Config
    size_t block_size = (size_t)parse_env_int("KV_BLOCK_SIZE", DEFAULT_BLOCK_SIZE);
    int blocks = parse_env_int("KV_BLOCKS", DEFAULT_BLOCKS);
    int occupancy = parse_env_int("KV_OCCUPANCY_PCT", DEFAULT_OCCUPANCY);
    bool packed_blocks = parse_env_bool("KV_PACKED_BLOCKS", DEFAULT_PACKED_BLOCKS);
    int duration = parse_env_int("KV_DURATION_SEC", DEFAULT_DURATION_SEC);
    double cpu_work_us = parse_env_double("KV_CPU_WORK_US", DEFAULT_CPU_WORK_US);
    
    int total_threads = parse_env_int("KV_THREADS", std::thread::hardware_concurrency());
    int read_pct = parse_env_int("KV_READ_PCT", DEFAULT_READ_PCT);
    int write_pct = parse_env_int("KV_WRITE_PCT", DEFAULT_WRITE_PCT);
    int compact_pct = parse_env_int("KV_COMPACT_PCT", DEFAULT_COMPACT_PCT);
    int batch_size = parse_env_int("KV_BATCH_SIZE", DEFAULT_BATCH_SIZE);
    
    if (read_pct + write_pct + compact_pct != 100) {
        fprintf(stderr, "ERROR: Percentages must sum to 100\n");
        return 1;
    }
    
    int read_threads = (total_threads * read_pct) / 100;
    int write_threads = (total_threads * write_pct) / 100;
    int compact_threads = total_threads - read_threads - write_threads;
    bool use_qpl_async_foreground = qpl_async_foreground_enabled();
    
    std::string qpl_mode_str = std::getenv("KV_QPL_PATH") ? std::getenv("KV_QPL_PATH") : "auto";
    qpl_path_t qpath = qpl_path_software;
    if (qpl_mode_str == "hardware" || qpl_mode_str == "hw") qpath = qpl_path_hardware;
    else if (qpl_mode_str == "auto") qpath = qpl_path_auto;
    
    std::string huffman_mode = std::getenv("KV_QPL_MODE") ? std::getenv("KV_QPL_MODE") : "fixed";
    bool use_dynamic_huffman = (huffman_mode == "dynamic");
    
    std::string codec = std::getenv("KV_CODEC") ? std::getenv("KV_CODEC") : "lz4";
    Codec selected_codec = Codec::LZ4;
    if (codec == "qpl" || codec == "QPL") {
        selected_codec = Codec::QPL;
    } else if (codec == "zlib" || codec == "zlib_accel" || codec == "zlib-accel" ||
               codec == "ZLIB" || codec == "ZLIB_ACCEL") {
        selected_codec = Codec::ZLIB_ACCEL;
    }
    bool use_qpl = (selected_codec == Codec::QPL);
    bool use_zlib = (selected_codec == Codec::ZLIB_ACCEL);

#ifndef HAVE_ZLIB
    if (use_zlib) {
        fprintf(stderr, "ERROR: KV_CODEC=%s requires zlib at build time\n", codec.c_str());
        return 1;
    }
#endif

    // Load data
    std::vector<uint8_t> payload;
    if (parse_env_int("KV_BENCH_USE_SILESIA", 1)) {
        payload = load_samba_bytes();
        if (payload.empty()) std::cerr << "Warning: Silesia not found\n";
        else std::cerr << "Using Silesia payload: " << payload.size() << " bytes\n";
    }

    std::vector<uint8_t> src_buf(blocks * block_size);
    if (packed_blocks) {
        fill_packed_blocks(src_buf.data(), blocks, payload, block_size);
    } else {
        fill_kv_blocks(src_buf.data(), blocks, occupancy, payload, block_size);
    }

    // Pre-compress all blocks
    std::cerr << "Pre-compressing " << blocks << " blocks...\n";
    std::vector<CompressedBlock> compressed_blocks(blocks);
    
    if (use_qpl) {
        // Pre-compress with QPL
        uint32_t job_size = 0;
        qpl_get_job_size(qpath, &job_size);
        std::vector<uint8_t> job_buf(job_size);
        qpl_job *job = (qpl_job *)job_buf.data();
        if (qpl_init_job(qpath, job) != QPL_STS_OK) die("qpl_init_job (init)");
        
        for (int i = 0; i < blocks; i++) {
            compressed_blocks[i].data.resize(block_size * 2);
            
            job->op = qpl_op_compress;
            job->next_in_ptr = src_buf.data() + i * block_size;
            job->available_in = block_size;
            job->next_out_ptr = compressed_blocks[i].data.data();
            job->available_out = block_size * 2;
            job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;
            if (use_dynamic_huffman) job->flags |= QPL_FLAG_DYNAMIC_HUFFMAN;
            job->level = qpl_default_level;
            
            qpl_status st = qpl_execute_job(job);
            if (st == QPL_STS_OK) {
                compressed_blocks[i].compressed_size = job->total_out;
                compressed_blocks[i].data.resize(job->total_out);
            } else {
                fprintf(stderr, "Pre-compression failed for block %d: %d\n", i, st);
                compressed_blocks[i].compressed_size = 0;
            }
        }
        qpl_fini_job(job);
    } else if (use_zlib) {
#ifdef HAVE_ZLIB
        // Pre-compress with zlib. LD_PRELOAD can route this through zlib-accel.
        for (int i = 0; i < blocks; i++) {
            compressed_blocks[i].data.resize(block_size * 2);
            uLongf produced = (uLongf)compressed_blocks[i].data.size();
            int status = compress2(
                (Bytef *)compressed_blocks[i].data.data(),
                &produced,
                (const Bytef *)(src_buf.data() + i * block_size),
                (uLong)block_size,
                Z_DEFAULT_COMPRESSION);
            if (status == Z_OK && produced > 0) {
                compressed_blocks[i].compressed_size = (int)produced;
                compressed_blocks[i].data.resize((size_t)produced);
            } else {
                compressed_blocks[i].compressed_size = 0;
            }
        }
#endif
    } else {
        // Pre-compress with LZ4
        for (int i = 0; i < blocks; i++) {
            compressed_blocks[i].data.resize(block_size * 2);
            int comp_size = LZ4_compress_default(
                (const char*)(src_buf.data() + i * block_size),
                (char*)compressed_blocks[i].data.data(),
                block_size, block_size * 2);
            if (comp_size > 0) {
                compressed_blocks[i].compressed_size = comp_size;
                compressed_blocks[i].data.resize(comp_size);
            } else {
                compressed_blocks[i].compressed_size = 0;
            }
        }
    }
    
    size_t total_compressed = 0;
    for (const auto &blk : compressed_blocks) {
        total_compressed += blk.compressed_size;
    }
    double ratio = (double)(blocks * block_size) / total_compressed;
    std::cerr << "Compression ratio: " << ratio << "x "
              << "(" << (blocks * block_size / 1024.0 / 1024.0) << " MB -> "
              << (total_compressed / 1024.0 / 1024.0) << " MB)\n";

    std::cout << "=== Mixed Workload Benchmark ===\n";
    std::cout << "Duration: " << duration << "s\n";
    std::cout << "Codec: ";
    if (use_qpl) {
        std::cout << "QPL";
    } else if (use_zlib) {
        std::cout << "zlib/zlib-accel";
    } else {
        std::cout << "LZ4";
    }
    if (use_qpl) std::cout << " (" << qpl_mode_str << (use_dynamic_huffman ? ", dynamic)" : ", fixed)");
    std::cout << "\nThreads: " << total_threads << " (R=" << read_threads 
              << " W=" << write_threads << " C=" << compact_threads << ")\n";
    std::cout << "Input Layout: " << (packed_blocks ? "packed" : "kv-shaped")
              << " block_size=" << block_size
              << " occupancy=" << (packed_blocks ? 100 : occupancy) << "%\n";
    if (use_qpl) {
        std::cout << "QPL Foreground: "
                  << (use_qpl_async_foreground ? "async batch" : "sync execute")
                  << " batch_size=" << batch_size << "\n";
    }
    std::cout << "CPU Work: " << cpu_work_us << "us/op\n\n";

    // Metrics
    SystemMetrics metrics;
    std::vector<std::thread> workers;
    std::vector<ThreadContext> contexts(total_threads);
    
    int tid = 0;
    
    // Spawn read workers
    for (int i = 0; i < read_threads; i++, tid++) {
        contexts[tid].thread_id = tid;
        contexts[tid].cpu_work_us = cpu_work_us;
        contexts[tid].use_dynamic_huffman = use_dynamic_huffman;
        contexts[tid].qpl_path = qpath;
        contexts[tid].src_base = src_buf.data();
        contexts[tid].compressed_blocks = compressed_blocks.data();
        contexts[tid].total_blocks = blocks;
        contexts[tid].block_size = block_size;
        contexts[tid].batch_size = batch_size;
        contexts[tid].metrics = &metrics;
        
        if (use_qpl) {
            if (use_qpl_async_foreground) {
                workers.emplace_back(read_worker_qpl_async, std::ref(contexts[tid]));
            } else {
                workers.emplace_back(read_worker_qpl, std::ref(contexts[tid]));
            }
        } else if (use_zlib) {
#ifdef HAVE_ZLIB
            workers.emplace_back(read_worker_zlib, std::ref(contexts[tid]));
#endif
        } else {
            workers.emplace_back(read_worker_lz4, std::ref(contexts[tid]));
        }
    }
    
    // Spawn write workers
    for (int i = 0; i < write_threads; i++, tid++) {
        contexts[tid].thread_id = tid;
        contexts[tid].cpu_work_us = cpu_work_us * 0.5;  // Less CPU work for writes
        contexts[tid].use_dynamic_huffman = use_dynamic_huffman;
        contexts[tid].qpl_path = qpath;
        contexts[tid].src_base = src_buf.data();
        contexts[tid].compressed_blocks = compressed_blocks.data();
        contexts[tid].total_blocks = blocks;
        contexts[tid].block_size = block_size;
        contexts[tid].batch_size = batch_size;
        contexts[tid].metrics = &metrics;
        
        if (use_qpl) {
            if (use_qpl_async_foreground) {
                workers.emplace_back(write_worker_qpl_async, std::ref(contexts[tid]));
            } else {
                workers.emplace_back(write_worker_qpl, std::ref(contexts[tid]));
            }
        } else if (use_zlib) {
#ifdef HAVE_ZLIB
            workers.emplace_back(write_worker_zlib, std::ref(contexts[tid]));
#endif
        } else {
            workers.emplace_back(write_worker_lz4, std::ref(contexts[tid]));
        }
    }
    
    // Spawn compaction workers
    for (int i = 0; i < compact_threads; i++, tid++) {
        contexts[tid].thread_id = tid;
        contexts[tid].cpu_work_us = 0;  // No CPU work for background
        contexts[tid].use_dynamic_huffman = use_dynamic_huffman;
        contexts[tid].qpl_path = qpath;
        contexts[tid].src_base = src_buf.data();
        contexts[tid].compressed_blocks = compressed_blocks.data();
        contexts[tid].total_blocks = blocks;
        contexts[tid].block_size = block_size;
        contexts[tid].batch_size = batch_size;
        contexts[tid].metrics = &metrics;
        
        if (use_qpl) {
            workers.emplace_back(compaction_worker_qpl, std::ref(contexts[tid]), batch_size);
        } else if (use_zlib) {
#ifdef HAVE_ZLIB
            workers.emplace_back(compaction_worker_zlib, std::ref(contexts[tid]), batch_size);
#endif
        } else {
            workers.emplace_back(compaction_worker_lz4, std::ref(contexts[tid]), batch_size);
        }
    }

    // Run
    std::this_thread::sleep_for(std::chrono::seconds(duration));
    metrics.running = false;
    for (auto &t : workers) t.join();

    // Collect latencies
    std::vector<double> all_read, all_write;
    for (int i = 0; i < read_threads; i++) {
        all_read.insert(all_read.end(), contexts[i].latencies.begin(), contexts[i].latencies.end());
    }
    for (int i = read_threads; i < read_threads + write_threads; i++) {
        all_write.insert(all_write.end(), contexts[i].latencies.begin(), contexts[i].latencies.end());
    }

    // Report
    LatencyStats read_stats = compute_stats(all_read);
    LatencyStats write_stats = compute_stats(all_write);
    uint64_t total_ops = metrics.read_ops + metrics.write_ops + metrics.compact_ops;
    double qps = (double)total_ops / duration;

    std::cout << "\n=== Results ===\n";
    std::cout << "Total Ops: " << total_ops << " | QPS: " << (qps / 1000.0) << " K/s\n\n";
    
    std::cout << "Read Ops: " << metrics.read_ops.load() << "\n";
    printf("  P50: %.2f us, P99: %.2f us, P999: %.2f us\n",
           read_stats.p50_us, read_stats.p99_us, read_stats.p999_us);
    printf("  QPS: %.2f K/s\n\n", (double)metrics.read_ops.load() / duration / 1000.0);
    
    std::cout << "Write Ops: " << metrics.write_ops.load() << "\n";
    printf("  P50: %.2f us, P99: %.2f us, P999: %.2f us\n",
           write_stats.p50_us, write_stats.p99_us, write_stats.p999_us);
    printf("  QPS: %.2f K/s\n\n", (double)metrics.write_ops.load() / duration / 1000.0);
    
    std::cout << "Compact Ops: " << metrics.compact_ops.load() << "\n";

    return 0;
}
