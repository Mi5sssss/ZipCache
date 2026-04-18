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

#include <lz4.h>
#include <qpl/qpl.h>
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

struct kv_pair {
    key_t key;
    int stored_value;
    uint8_t payload[COMPRESSED_VALUE_BYTES];
};

struct LatencyStats {
    double avg_us;
    double p50_us;
    double p90_us;
    double p95_us;
    double p99_us;
};

// Helper to parse env vars
static int parse_env_int(const char *name, int fallback) {
    const char *v = std::getenv(name);
    if (!v || !*v) return fallback;
    try {
        return std::stoi(v);
    } catch (...) {
        return fallback;
    }
}

static void die(const char *msg) {
    std::perror(msg);
    std::exit(EXIT_FAILURE);
}

// --- Data Generation (Reused logic) ---

static const char *resolve_samba_zip(char *buffer, size_t buf_len) {
    const char *override = getenv("SAMBA_ZIP_PATH");
    if (override && *override) {
        if (access(override, R_OK) == 0) return override;
    }
    const char *dir = getenv("SILESIA_CORPUS_DIR");
    if (dir && *dir) {
        snprintf(buffer, buf_len, "%s/%s", dir, "samba.zip");
        if (access(buffer, R_OK) == 0) return buffer;
    }
    // Fallback relative paths
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
                // Empty slot
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
                // Synthetic
                for (size_t j = 0; j < sizeof(page[i].payload); j++) {
                    page[i].payload[j] = (uint8_t)((page[i].key * 131 + j * 17) & 0xff);
                }
            }
        }
    }
}

// --- Stats Helpers ---

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
    out.p90_us = pick(90.0);
    out.p95_us = pick(95.0);
    out.p99_us = pick(99.0);
    return out;
}

// --- Async Benchmark Logic ---

struct ThreadContext {
    int thread_id;
    int num_threads;
    int batch_size;
    bool use_dynamic_huffman;
    
    // Input
    const uint8_t *src_base;
    int total_blocks;
    size_t block_size;
    
    // Intermediate (Compressed Data)
    std::vector<uint8_t> compressed_buf;
    std::vector<int> compressed_sizes;
    size_t max_compressed_per_block;
    
    // Stats
    std::vector<double> latencies;
    size_t total_bytes;
};

// --- LZ4 Workers (for baseline comparison) ---

static void worker_lz4_compress(ThreadContext &ctx) {
    int blocks_per_thread = ctx.total_blocks / ctx.num_threads;
    int start_block = ctx.thread_id * blocks_per_thread;
    int end_block = (ctx.thread_id == ctx.num_threads - 1) ? ctx.total_blocks : start_block + blocks_per_thread;
    int my_blocks = end_block - start_block;

    ctx.max_compressed_per_block = ctx.block_size * 2;
    ctx.compressed_buf.resize(my_blocks * ctx.max_compressed_per_block);
    ctx.compressed_sizes.resize(my_blocks);
    ctx.latencies.clear();
    ctx.latencies.reserve(my_blocks);
    ctx.total_bytes = (size_t)my_blocks * ctx.block_size;

    for (int i = 0; i < my_blocks; i++) {
        const char *src = (const char *)(ctx.src_base + (size_t)(start_block + i) * ctx.block_size);
        char *dst = (char *)(ctx.compressed_buf.data() + (size_t)i * ctx.max_compressed_per_block);

        auto t0 = std::chrono::steady_clock::now();
        int out = LZ4_compress_default(src, dst, (int)ctx.block_size, (int)ctx.max_compressed_per_block);
        auto t1 = std::chrono::steady_clock::now();

        if (out <= 0) die("LZ4_compress_default failed");
        ctx.compressed_sizes[i] = out;
        ctx.latencies.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
    }
}

static void worker_lz4_decompress(ThreadContext &ctx) {
    int my_blocks = (int)ctx.compressed_sizes.size();
    std::vector<uint8_t> decomp_buf(ctx.block_size);

    ctx.latencies.clear();
    ctx.latencies.reserve(my_blocks);
    ctx.total_bytes = (size_t)my_blocks * ctx.block_size;

    for (int i = 0; i < my_blocks; i++) {
        const char *src = (const char *)(ctx.compressed_buf.data() + (size_t)i * ctx.max_compressed_per_block);
        char *dst = (char *)decomp_buf.data();

        auto t0 = std::chrono::steady_clock::now();
        int out = LZ4_decompress_safe(src, dst, ctx.compressed_sizes[i], (int)ctx.block_size);
        auto t1 = std::chrono::steady_clock::now();

        if (out < 0) die("LZ4_decompress_safe failed");
        ctx.latencies.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
    }
}

// --- QPL Workers ---


static void worker_qpl_compress_async(ThreadContext &ctx, qpl_path_t path) {
    int blocks_per_thread = ctx.total_blocks / ctx.num_threads;
    int start_block = ctx.thread_id * blocks_per_thread;
    int end_block = (ctx.thread_id == ctx.num_threads - 1) ? ctx.total_blocks : start_block + blocks_per_thread;
    int my_blocks = end_block - start_block;

    ctx.max_compressed_per_block = ctx.block_size * 2;
    ctx.compressed_buf.resize(my_blocks * ctx.max_compressed_per_block);
    ctx.compressed_sizes.resize(my_blocks);
    ctx.latencies.clear();
    ctx.latencies.reserve(my_blocks);
    ctx.total_bytes = (size_t)my_blocks * ctx.block_size;

    // Allocate job pool (ring buffer)
    uint32_t job_size = 0;
    qpl_get_job_size(path, &job_size);
    std::vector<std::vector<uint8_t>> job_bufs(ctx.batch_size);
    std::vector<qpl_job*> jobs(ctx.batch_size);
    std::vector<std::chrono::steady_clock::time_point> submit_times(my_blocks);
    
    for (int k = 0; k < ctx.batch_size; k++) {
        job_bufs[k].resize(job_size);
        jobs[k] = (qpl_job *)job_bufs[k].data();
        qpl_status st = qpl_init_job(path, jobs[k]);
        if (st != QPL_STS_OK) {
            std::cerr << "ERROR: qpl_init_job failed with status " << st;
            if (path == qpl_path_hardware) {
                std::cerr << "\nINTEL IAA HARDWARE NOT AVAILABLE.\n"
                          << "Possible reasons:\n"
                          << "  - No IAA device on this system\n"
                          << "  - IAA driver not loaded (check: lsmod | grep idxd)\n"
                          << "  - Insufficient permissions\n"
                          << "Solution: Use KV_QPL_PATH=software or KV_QPL_PATH=auto\n";
            }
            std::exit(EXIT_FAILURE);
        }
    }

    // Async batching loop
    for (int i = 0; i < my_blocks; i++) {
        int job_idx = i % ctx.batch_size;
        qpl_job *job = jobs[job_idx];

        // If this slot was used before, wait for it
        if (i >= ctx.batch_size) {
            qpl_status st = qpl_wait_job(job);
            auto t_done = std::chrono::steady_clock::now();
            
            if (st != QPL_STS_OK) die("qpl_wait_job failed");
            
            // Record the OLD job's result (from i - batch_size)
            int old_idx = i - ctx.batch_size;
            ctx.compressed_sizes[old_idx] = job->total_out;
            double lat_us = std::chrono::duration<double, std::micro>(t_done - submit_times[old_idx]).count();
            ctx.latencies.push_back(lat_us);
        }

        // Submit NEW job
        job->op = qpl_op_compress;
        job->next_in_ptr = (uint8_t *)(ctx.src_base + (size_t)(start_block + i) * ctx.block_size);
        job->available_in = (uint32_t)ctx.block_size;
        job->next_out_ptr = ctx.compressed_buf.data() + (size_t)i * ctx.max_compressed_per_block;
        job->available_out = (uint32_t)ctx.max_compressed_per_block;
        job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST | QPL_FLAG_OMIT_VERIFY;
        if (ctx.use_dynamic_huffman) {
            job->flags |= QPL_FLAG_DYNAMIC_HUFFMAN;
        }
        job->level = qpl_default_level;

        submit_times[i] = std::chrono::steady_clock::now();
        qpl_status st = qpl_submit_job(job);
        if (st != QPL_STS_OK) die("qpl_submit_job failed");
    }

    // Drain: wait for final batch
    for (int k = 0; k < ctx.batch_size && k < my_blocks; k++) {
        int remaining_idx = my_blocks - ctx.batch_size + k;
        if (remaining_idx < 0) remaining_idx = k;
        
        qpl_job *job = jobs[remaining_idx % ctx.batch_size];
        qpl_status st = qpl_wait_job(job);
        auto t_done = std::chrono::steady_clock::now();
        
        if (st != QPL_STS_OK) die("qpl drain failed");
        ctx.compressed_sizes[remaining_idx] = job->total_out;
        double lat_us = std::chrono::duration<double, std::micro>(t_done - submit_times[remaining_idx]).count();
        ctx.latencies.push_back(lat_us);
    }

    for (int k = 0; k < ctx.batch_size; k++) {
        qpl_fini_job(jobs[k]);
    }
}

static void worker_qpl_decompress_async(ThreadContext &ctx, qpl_path_t path) {
    int my_blocks = (int)ctx.compressed_sizes.size();
    
    uint32_t job_size = 0;
    qpl_get_job_size(path, &job_size);
    std::vector<std::vector<uint8_t>> job_bufs(ctx.batch_size);
    std::vector<qpl_job*> jobs(ctx.batch_size);
    std::vector<std::vector<uint8_t>> decomp_bufs(ctx.batch_size);
    std::vector<std::chrono::steady_clock::time_point> submit_times(my_blocks);
    
    for (int k = 0; k < ctx.batch_size; k++) {
        job_bufs[k].resize(job_size);
        jobs[k] = (qpl_job *)job_bufs[k].data();
        qpl_status st = qpl_init_job(path, jobs[k]);
        if (st != QPL_STS_OK) {
            std::cerr << "ERROR: qpl_init_job (decomp) failed with status " << st;
            if (path == qpl_path_hardware) {
                std::cerr << "\nINTEL IAA HARDWARE NOT AVAILABLE.\n";
            }
            std::exit(EXIT_FAILURE);
        }
        decomp_bufs[k].resize(ctx.block_size);
    }

    ctx.latencies.clear();
    ctx.latencies.reserve(my_blocks);
    ctx.total_bytes = (size_t)my_blocks * ctx.block_size;

    for (int i = 0; i < my_blocks; i++) {
        int job_idx = i % ctx.batch_size;
        qpl_job *job = jobs[job_idx];

        if (i >= ctx.batch_size) {
            qpl_status st = qpl_wait_job(job);
            auto t_done = std::chrono::steady_clock::now();
            
            if (st != QPL_STS_OK) die("qpl_wait_job decomp failed");
            
            int old_idx = i - ctx.batch_size;
            double lat_us = std::chrono::duration<double, std::micro>(t_done - submit_times[old_idx]).count();
            ctx.latencies.push_back(lat_us);
        }

        job->op = qpl_op_decompress;
        job->next_in_ptr = ctx.compressed_buf.data() + (size_t)i * ctx.max_compressed_per_block;
        job->available_in = ctx.compressed_sizes[i];
        job->next_out_ptr = decomp_bufs[job_idx].data();
        job->available_out = (uint32_t)ctx.block_size;
        job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

        submit_times[i] = std::chrono::steady_clock::now();
        qpl_status st = qpl_submit_job(job);
        if (st != QPL_STS_OK) die("qpl_submit_job decomp failed");
    }

    // Drain
    for (int k = 0; k < ctx.batch_size && k < my_blocks; k++) {
        int remaining_idx = my_blocks - ctx.batch_size + k;
        if (remaining_idx < 0) remaining_idx = k;
        
        qpl_job *job = jobs[remaining_idx % ctx.batch_size];
        qpl_status st = qpl_wait_job(job);
        auto t_done = std::chrono::steady_clock::now();
        
        if (st != QPL_STS_OK) die("qpl drain decomp failed");
        double lat_us = std::chrono::duration<double, std::micro>(t_done - submit_times[remaining_idx]).count();
        ctx.latencies.push_back(lat_us);
    }

    for (int k = 0; k < ctx.batch_size; k++) {
        qpl_fini_job(jobs[k]);
    }
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    size_t block_size = (size_t)parse_env_int("KV_BLOCK_SIZE", DEFAULT_BLOCK_SIZE);
    int blocks = parse_env_int("KV_BLOCKS", DEFAULT_BLOCKS);
    int occupancy = parse_env_int("KV_OCCUPANCY_PCT", DEFAULT_OCCUPANCY);
    int threads = parse_env_int("KV_THREADS", std::thread::hardware_concurrency());
    int batch_size = parse_env_int("KV_BATCH_SIZE", DEFAULT_BATCH_SIZE);
    if (threads < 1) threads = 1;
    if (batch_size < 1) batch_size = 1;

    std::string qpl_mode = std::getenv("KV_QPL_PATH") ? std::getenv("KV_QPL_PATH") : "auto";
    qpl_path_t qpath = qpl_path_software;
    if (qpl_mode == "hardware" || qpl_mode == "hw") qpath = qpl_path_hardware;
    else if (qpl_mode == "auto") qpath = qpl_path_auto;

    // Dynamic Huffman mode (higher compression ratio)
    std::string huffman_mode = std::getenv("KV_QPL_MODE") ? std::getenv("KV_QPL_MODE") : "fixed";
    bool use_dynamic_huffman = (huffman_mode == "dynamic");

    // Prepare Data
    std::vector<uint8_t> payload;
    if (parse_env_int("KV_BENCH_USE_SILESIA", 1)) {
        payload = load_samba_bytes();
        if (payload.empty()) std::cerr << "Warning: Silesia not found, using synthetic.\n";
        else std::cerr << "Using Silesia payload: " << payload.size() << " bytes\n";
    }

    std::vector<uint8_t> src_buf(blocks * block_size);
    fill_kv_blocks(src_buf.data(), blocks, occupancy, payload, block_size);

    std::cout << "=== KV Bench ASYNC: threads=" << threads 
              << " batch_size=" << batch_size
              << " block_size=" << block_size 
              << " blocks=" << blocks 
              << " occupancy=" << occupancy << "% ===\n\n";

    // --- Run LZ4 (Baseline) ---
    {
        std::vector<ThreadContext> contexts(threads);
        std::vector<std::thread> workers;
        
        // 1. Compress Phase
        auto t_start = std::chrono::steady_clock::now();
        for (int i = 0; i < threads; i++) {
            contexts[i].thread_id = i;
            contexts[i].num_threads = threads;
            contexts[i].src_base = src_buf.data();
            contexts[i].total_blocks = blocks;
            contexts[i].block_size = block_size;
            workers.emplace_back(worker_lz4_compress, std::ref(contexts[i]));
        }
        for (auto &t : workers) t.join();
        auto t_end = std::chrono::steady_clock::now();
        workers.clear();
        
        double comp_wall_sec = std::chrono::duration<double>(t_end - t_start).count();
        
        // Aggregate Comp Stats
        std::vector<double> all_lat;
        size_t total_in = 0;
        size_t total_compressed = 0;
        for (const auto &ctx : contexts) {
            all_lat.insert(all_lat.end(), ctx.latencies.begin(), ctx.latencies.end());
            total_in += ctx.total_bytes;
            for (int s : ctx.compressed_sizes) total_compressed += s;
        }
        LatencyStats c_stats = compute_stats(all_lat);
        double ratio = (double)total_in / total_compressed;
        double comp_gb_s = (double)total_in / (1024.0*1024.0*1024.0) / comp_wall_sec;

        // 2. Decompress Phase
        t_start = std::chrono::steady_clock::now();
        for (int i = 0; i < threads; i++) {
            workers.emplace_back(worker_lz4_decompress, std::ref(contexts[i]));
        }
        for (auto &t : workers) t.join();
        t_end = std::chrono::steady_clock::now();
        workers.clear();
        
        double decomp_wall_sec = std::chrono::duration<double>(t_end - t_start).count();
        
        // Aggregate Decomp Stats
        all_lat.clear();
        for (const auto &ctx : contexts) {
            all_lat.insert(all_lat.end(), ctx.latencies.begin(), ctx.latencies.end());
        }
        LatencyStats d_stats = compute_stats(all_lat);
        double decomp_gb_s = (double)total_in / (1024.0*1024.0*1024.0) / decomp_wall_sec;

        printf("LZ4:  ratio=%.3fx\n", ratio);
        printf("      Comp   p50=%.2f p99=%.2f avg=%.2f us | Throughput: %.2f GB/s\n", 
               c_stats.p50_us, c_stats.p99_us, c_stats.avg_us, comp_gb_s);
        printf("      Decomp p50=%.2f p99=%.2f avg=%.2f us | Throughput: %.2f GB/s\n\n", 
               d_stats.p50_us, d_stats.p99_us, d_stats.avg_us, decomp_gb_s);
    }

    {
        std::vector<ThreadContext> contexts(threads);
        std::vector<std::thread> workers;
        
        // 1. Compress Phase
        auto t_start = std::chrono::steady_clock::now();
        for (int i = 0; i < threads; i++) {
            contexts[i].thread_id = i;
            contexts[i].num_threads = threads;
            contexts[i].batch_size = batch_size;
            contexts[i].use_dynamic_huffman = use_dynamic_huffman;
            contexts[i].src_base = src_buf.data();
            contexts[i].total_blocks = blocks;
            contexts[i].block_size = block_size;
            workers.emplace_back(worker_qpl_compress_async, std::ref(contexts[i]), qpath);
        }
        for (auto &t : workers) t.join();
        auto t_end = std::chrono::steady_clock::now();
        workers.clear();
        
        double comp_wall_sec = std::chrono::duration<double>(t_end - t_start).count();
        
        // Aggregate Comp Stats
        std::vector<double> all_lat;
        size_t total_in = 0;
        size_t total_compressed = 0;
        for (const auto &ctx : contexts) {
            all_lat.insert(all_lat.end(), ctx.latencies.begin(), ctx.latencies.end());
            total_in += ctx.total_bytes;
            for (int s : ctx.compressed_sizes) total_compressed += s;
        }
        LatencyStats c_stats = compute_stats(all_lat);
        double ratio = (double)total_in / total_compressed;
        double comp_gb_s = (double)total_in / (1024.0*1024.0*1024.0) / comp_wall_sec;

        // 2. Decompress Phase
        t_start = std::chrono::steady_clock::now();
        for (int i = 0; i < threads; i++) {
            workers.emplace_back(worker_qpl_decompress_async, std::ref(contexts[i]), qpath);
        }
        for (auto &t : workers) t.join();
        t_end = std::chrono::steady_clock::now();
        workers.clear();
        
        double decomp_wall_sec = std::chrono::duration<double>(t_end - t_start).count();
        
        // Aggregate Decomp Stats
        all_lat.clear();
        for (const auto &ctx : contexts) {
            all_lat.insert(all_lat.end(), ctx.latencies.begin(), ctx.latencies.end());
        }
        LatencyStats d_stats = compute_stats(all_lat);
        double decomp_gb_s = (double)total_in / (1024.0*1024.0*1024.0) / decomp_wall_sec;

        printf("QPL(%s) ASYNC:  ratio=%.3fx\n", qpl_mode.c_str(), ratio);
        printf("      Comp   p50=%.2f p99=%.2f avg=%.2f us | Throughput: %.2f GB/s\n", 
               c_stats.p50_us, c_stats.p99_us, c_stats.avg_us, comp_gb_s);
        printf("      Decomp p50=%.2f p99=%.2f avg=%.2f us | Throughput: %.2f GB/s\n", 
               d_stats.p50_us, d_stats.p99_us, d_stats.avg_us, decomp_gb_s);
    }

    return 0;
}
