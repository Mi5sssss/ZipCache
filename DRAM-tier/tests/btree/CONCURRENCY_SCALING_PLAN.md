# DRAM-tier Compressed B+Tree Concurrency Scaling Plan

## Goal

Improve compressed DRAM-tier B+Tree throughput scaling as thread count increases.

Expected behavior:

- Total throughput should increase from 1 thread to multiple threads and then plateau.
- Per-thread throughput may drop because of synchronization and codec overhead, but total throughput should not sharply regress when moving from 8 to 16 or 32 threads.
- Correctness must remain strict: mixed workload point-read mismatches should stay at 0 for LZ4, QPL, and zlib-accel.

Current behavior does not meet this expectation. In the current benchmark, 8 threads is often the peak, and 16/32 threads can drop significantly.

## Implementation Status

Implemented in the 2026-04-14 pass:

- Added `DRAM-tier/tests/btree/run_scaling_sweep.sh`.
- The sweep script runs read-only, read-heavy, point-mixed, mixed-with-scan, write-heavy, and benchmark-mixed cases over configurable thread counts.
- Sweep output is saved under `DRAM-tier/tests/btree/results/<timestamp>/summary.tsv` and per-run logs.
- QPL now prefers lazily initialized thread-local jobs, with separate per-thread compression and decompression jobs.
- The original global QPL job pool remains as fallback, and `BTREE_QPL_POOL_SIZE` can override the retained pool cap.
- Point `get` now snapshots the target compressed subpage under the leaf lock and performs decompression after releasing the leaf lock.
- Optional transparent sharding is implemented through `BTREE_SHARDS=N`.
- When `BTREE_SHARDS>1`, `bplus_tree_compressed_init_with_config()` returns a coordinator that routes `put/get/delete` to independent compressed B+Tree shards by key hash.
- Sharded `get_range` currently scans from `max_key` down to `min_key` and does point lookups on the owning shard. This preserves current "return last value in range" semantics for correctness tests, but it is not the final range-scan performance design.
- `bpt_compressed_mixed_concurrency` now uses striped reference locks via `BTREE_REF_LOCKS` instead of one global reference mutex, so the test oracle no longer serializes the workload.
- `run_scaling_sweep.sh` now supports `SHARDS_LIST`, includes `shards` in `summary.tsv`, and passes `BTREE_SHARDS` into both mixed-concurrency and throughput benchmarks.
- Added a Phase-2 experimental out-of-lock leaf rebuild scaffold behind `BTREE_OUT_OF_LOCK_REBUILD=1`.
- The Phase-2 scaffold adds `simple_leaf_node::generation`, snapshots a full landing-buffer leaf, releases the leaf lock for decompress/merge/recompress, then reacquires the lock and installs only if the generation is unchanged.
- `BTREE_OUT_OF_LOCK_REBUILD` is default-off. It currently passes the short concurrency smoke with `BTREE_SHARDS=8`, but fails the single-tree concurrency smoke because the split/structural path can still race with the off-lock rebuild window.
- A targeted 32-thread sharded write-heavy run showed no clear QPS win from `BTREE_OUT_OF_LOCK_REBUILD=1`, so it should remain experimental until the structural race and retry policy are fixed.
- Added Phase-3 small-range scan optimization through `BTREE_RANGE_POINT_LOOKUP_THRESHOLD`.
- For non-sharded trees, ranges with width `<=BTREE_RANGE_POINT_LOOKUP_THRESHOLD` now use descending point lookups instead of the legacy leaf-walk scan that holds the global tree read lock. The default threshold is `256`, and `0` disables the optimization.
- This matches the current sharded range strategy and is safe because each point lookup uses the existing tree/leaf locking path.
- Fixed duplicate-key stale values between compressed subpages and the landing buffer. `compressed_leaf_collect_pairs()` now collects compressed entries first and landing-buffer entries last, so newer landing values overwrite stale compressed values.
- `split_leaf()` now uses `compressed_leaf_collect_pairs()` instead of manually appending compressed and landing entries, preventing splits from reintroducing duplicate keys.
- `bplus_tree_compressed_stats()` now returns exact leaf-walk stats via `bplus_tree_compressed_calculate_stats()`. This fixes stats drift after split/delete/reinsert, at the cost of making `stats` no longer a near-free counter read.

Implemented in the 2026-04-15 Phase-6 pass:

- Landing buffer size is now runtime-tunable through `BTREE_LANDING_BUFFER_BYTES`.
- Each leaf allocates a 2KB maximum landing buffer, while the effective default remains 512B to preserve baseline behavior.
- Effective landing buffer bytes are clamped to `[sizeof(struct kv_pair), 2048]` and rounded down to a `struct kv_pair` slot boundary.
- `run_scaling_sweep.sh` now supports `LANDING_LIST`, includes `landing_bytes` in `summary.tsv`, and passes `BTREE_LANDING_BUFFER_BYTES` to all mixed-concurrency and throughput-benchmark runs.
- Short correctness validation passed for 256B, 1024B, and 2048B landing-buffer configurations across LZ4, QPL, and zlib_accel smoke/stress targets listed below.
- `bpt_compressed_mixed_concurrency` and `bpt_compressed_throughput_bench` now report whole-tree effective compression as `ratio` and `saved_pct`.
- `run_scaling_sweep.sh` now records `ratio`, `saved_pct`, `compressed_bytes`, and `total_bytes` in `summary.tsv`, making landing-buffer throughput/capacity trade-offs directly comparable.

Not implemented in this pass:

- Versioned lock-free traversal was not implemented. The current B+Tree frees and relinks leaves/non-leaves during delete/split; traversing without the tree lock would be a use-after-free risk without RCU, hazard pointers, or deferred reclamation.
- Production-ready out-of-lock write-path rebuild was not implemented. A default-off scaffold exists, but it is not safe enough for baseline use.
- Production-grade range-partitioned sharding was not implemented. Current sharding is hash-based and optimized for point lookup/update throughput; range scans are correctness-preserving but not optimized.

Current handoff status:

- Baseline-safe optimizations are enabled by default: QPL TLS jobs, out-of-lock get decompression, hash sharding via `BTREE_SHARDS`, small-range point lookup, exact stats, and configurable landing-buffer size.
- `BTREE_OUT_OF_LOCK_REBUILD=1` is still experimental and must not be used for baseline claims without fixing single-tree structural races.
- Next recommended work is Phase 8 background landing-buffer compaction. The goal is to reduce foreground synchronous leaf rebuilds and codec/IAA stalls while keeping the landing-buffer throughput/capacity trade-off controllable by runtime variables.
- The benchmark value payload width is now 128B (`COMPRESSED_VALUE_BYTES=128`) so Silesia 128B slices are stored inline instead of being truncated. This is an interim benchmark format only, not the final ZipCache key/value representation.
- `run_scaling_sweep.sh` now defaults to `BTREE_USE_SILESIA=1` and `BTREE_VALUE_BYTES=128`, using continuous key order over continuous slices extracted from `SilesiaCorpus/samba.zip`.
- Important limitation: the current compressed B+Tree still uses fixed `int key_t` keys and fixed-size inline value payload slots. It cannot yet accept arbitrary byte keys or arbitrary byte values from a test/workload without truncation or integer-key adaptation.

Latest short sweep:

```text
THREADS_LIST='1 8 16 32' BTREE_DURATION_SEC=1 BENCH_DURATION_SEC=1 \
  DRAM-tier/tests/btree/run_scaling_sweep.sh
```

The run completed with 0 mismatches across all tested codecs and cases. Results were written to:

```text
DRAM-tier/tests/btree/results/20260414T151440Z/summary.tsv
```

Latest sharded sweep:

```text
SHARDS_LIST='1 8' THREADS_LIST='16 32' BTREE_DURATION_SEC=1 BENCH_DURATION_SEC=1 \
  DRAM-tier/tests/btree/run_scaling_sweep.sh
```

The run completed with 0 mismatches across all tested codecs and cases. Results were written to:

```text
DRAM-tier/tests/btree/results/20260414T152701Z/summary.tsv
```

Selected results from that run:

| Case | Shards | Threads | LZ4 op/s | QPL op/s | zlib_accel op/s |
|---|---:|---:|---:|---:|---:|
| point_mixed | 1 | 16 | 2,940,813.8 | 2,438,730.9 | 1,225,917.0 |
| point_mixed | 8 | 16 | 5,872,296.2 | 3,580,700.3 | 1,433,243.4 |
| point_mixed | 1 | 32 | 1,433,744.9 | 1,932,499.4 | 1,207,835.2 |
| point_mixed | 8 | 32 | 7,762,311.3 | 4,752,369.9 | 2,008,941.4 |
| write_heavy | 1 | 16 | 2,818,705.6 | 2,406,330.6 | 1,275,991.9 |
| write_heavy | 8 | 16 | 5,444,519.8 | 3,722,499.2 | 1,423,027.8 |
| write_heavy | 1 | 32 | 1,304,352.9 | 1,691,976.0 | 1,159,782.5 |
| write_heavy | 8 | 32 | 7,061,538.3 | 4,874,584.2 | 2,136,958.3 |
| bench_mixed | 1 | 32 | 797,696.0 | 489,472.0 | 236,544.0 |
| bench_mixed | 8 | 32 | 7,358,464.0 | 3,476,480.0 | 831,488.0 |

Final validation in this pass:

```text
cmake --build DRAM-tier/build_check -j$(nproc)
BTREE_SHARDS=8 DRAM-tier/build_check/bin/test_compression_concurrency
BTREE_SHARDS=8 BTREE_DURATION_SEC=1 BTREE_KEY_SPACE=4096 BTREE_THREADS=4 \
  DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

All three commands passed. The sharded mixed-concurrency smoke reported 0 mismatches for LZ4, QPL, and zlib_accel.

Follow-up validation on 2026-04-15:

```text
cmake --build DRAM-tier/build_check -j$(nproc)
DRAM-tier/build_check/bin/test_compression_concurrency
BTREE_SHARDS=8 BTREE_DURATION_SEC=1 BTREE_KEY_SPACE=4096 BTREE_THREADS=4 \
  DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
BTREE_SHARDS=8 BTREE_OUT_OF_LOCK_REBUILD=1 \
  DRAM-tier/build_check/bin/test_compression_concurrency
```

All four commands passed. The same concurrency smoke without sharding fails when `BTREE_OUT_OF_LOCK_REBUILD=1`, so the scaffold remains default-off.

Targeted 32-thread sharded write-heavy comparison:

| Setting | LZ4 op/s | QPL op/s | zlib_accel op/s | Mismatches |
|---|---:|---:|---:|---:|
| `BTREE_OUT_OF_LOCK_REBUILD=0` | 4,860,729.7 | 2,768,728.0 | 1,028,792.9 | 0 |
| `BTREE_OUT_OF_LOCK_REBUILD=1` | 4,892,650.5 | 2,647,010.0 | 1,035,062.4 | 0 |

Short throughput bench after this pass:

```text
BENCH_DURATION_SEC=1 BENCH_WARMUP_KEYS=5000 BENCH_KEY_SPACE=10000 BENCH_THREADS=8 \
  DRAM-tier/build_check/bin/bpt_compressed_throughput_bench
BTREE_SHARDS=8 BENCH_DURATION_SEC=1 BENCH_WARMUP_KEYS=5000 BENCH_KEY_SPACE=10000 BENCH_THREADS=8 \
  DRAM-tier/build_check/bin/bpt_compressed_throughput_bench
```

Both commands completed with `total correctness bugs = 0`.

Phase-3 range-scan validation on 2026-04-15:

```text
cmake --build DRAM-tier/build_check -j$(nproc)
DRAM-tier/build_check/bin/test_compression_concurrency
DRAM-tier/build_check/bin/bpt_compressed_lz4_smoke
DRAM-tier/build_check/bin/bpt_compressed_qpl_smoke
DRAM-tier/build_check/bin/bpt_compressed_zlib_accel_smoke
```

All commands passed.

Targeted 32-thread non-sharded mixed-with-scan comparison:

```text
BTREE_DURATION_SEC=1 BTREE_KEY_SPACE=50000 BTREE_HOT_PCT=100 \
BTREE_READ_PCT=50 BTREE_WRITE_PCT=35 BTREE_DELETE_PCT=10 BTREE_SCAN_PCT=5 \
BTREE_THREADS=32 DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

| Setting | LZ4 op/s | QPL op/s | zlib_accel op/s | Mismatches |
|---|---:|---:|---:|---:|
| `BTREE_RANGE_POINT_LOOKUP_THRESHOLD=0` | 2,925,499.2 | 1,879,410.3 | 870,190.9 | 0 |
| default threshold `256` | 3,155,731.2 | 1,895,467.4 | 933,962.2 | 0 |

Bug-fix validation on 2026-04-15:

```text
cmake --build DRAM-tier/build_check -j$(nproc)
DRAM-tier/build_check/bin/bpt_compressed_crud_fuzz
DRAM-tier/build_check/bin/bpt_compressed_split_payload_stats
DRAM-tier/build_check/bin/test_compression_concurrency
BTREE_SHARDS=8 BTREE_DURATION_SEC=1 BTREE_KEY_SPACE=4096 BTREE_THREADS=4 \
  DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
BENCH_DURATION_SEC=1 BENCH_WARMUP_KEYS=5000 BENCH_KEY_SPACE=10000 BENCH_THREADS=8 \
  DRAM-tier/build_check/bin/bpt_compressed_throughput_bench
```

All commands passed. `bpt_compressed_crud_fuzz` and `bpt_compressed_split_payload_stats` now pass across LZ4, QPL, and zlib_accel.

Short throughput note after exact stats:

`stats` throughput now matches `calc_stats` because `bplus_tree_compressed_stats()` returns exact leaf-walk stats. In the short bench, mixed throughput remains correctness-clean (`total correctness bugs = 0`) but is lower than previous runs that used incomplete near-free counters.

Phase-6 landing-buffer validation on 2026-04-15:

```text
cmake --build DRAM-tier/build_check -j$(nproc)
BTREE_LANDING_BUFFER_BYTES=2048 DRAM-tier/build_check/bin/bpt_compressed_crud_fuzz
BTREE_LANDING_BUFFER_BYTES=256 DRAM-tier/build_check/bin/bpt_compressed_split_payload_stats
BTREE_LANDING_BUFFER_BYTES=1024 DRAM-tier/build_check/bin/test_compression_concurrency
BTREE_LANDING_BUFFER_BYTES=2048 BTREE_SHARDS=8 BTREE_DURATION_SEC=1 \
  BTREE_KEY_SPACE=4096 BTREE_THREADS=4 \
  DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

All commands passed. The sharded mixed-concurrency smoke reported 0 mismatches for LZ4, QPL, and zlib_accel.

Targeted 32-thread sharded write-heavy landing-buffer comparison:

```text
BTREE_SHARDS=8 BTREE_DURATION_SEC=1 BTREE_KEY_SPACE=50000 BTREE_HOT_PCT=100 \
BTREE_READ_PCT=20 BTREE_WRITE_PCT=70 BTREE_DELETE_PCT=10 BTREE_SCAN_PCT=0 \
BTREE_THREADS=32 BTREE_LANDING_BUFFER_BYTES=<size> \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

| Landing bytes | LZ4 op/s | QPL op/s | zlib_accel op/s | Mismatches |
|---:|---:|---:|---:|---:|
| 256 | 6,404,492.2 | 3,881,967.8 | 1,641,912.1 | 0 |
| 512 | 7,023,169.8 | 4,726,153.1 | 2,111,451.7 | 0 |
| 1024 | 7,095,560.1 | 5,283,841.4 | 2,415,591.7 | 0 |
| 2048 | 8,283,969.4 | 8,402,663.3 | 6,390,420.8 | 0 |

Interpretation: Larger landing buffers reduce recompression and QPL/zlib submission frequency on write-heavy paths. This improves short-run QPS substantially, but the default remains 512B until longer runs quantify memory overhead, compression-ratio impact, and range-scan/read-side cost.

Compression-statistics output added on 2026-04-15:

- `ratio = total_bytes / compressed_bytes`.
- `saved_pct = (1 - compressed_bytes / total_bytes) * 100`.
- `total_bytes` counts resident KV bytes represented by compressed subpages plus live landing-buffer entries.
- `compressed_bytes` counts compressed subpage bytes plus live landing-buffer entries, since landing data is stored uncompressed.
- This is an effective whole-tree memory metric for the current DRAM-tier prototype, not a raw codec-only ratio. A larger landing buffer may improve QPS while reducing `saved_pct`.

Short sweep with compression-stat columns:

```text
LANDING_LIST='512 2048' SHARDS_LIST='8' THREADS_LIST='4' \
BTREE_DURATION_SEC=1 BENCH_DURATION_SEC=1 BENCH_WARMUP_KEYS=1000 \
BENCH_OPS=1000 BENCH_KEY_SPACE=4096 BTREE_KEY_SPACE=4096 \
DRAM-tier/tests/btree/run_scaling_sweep.sh
```

Results were written to:

```text
DRAM-tier/tests/btree/results/20260415T190717Z/summary.tsv
```

Selected trade-off points:

| Case | Landing bytes | Codec | QPS | Ratio | Saved pct | Compressed/Total bytes |
|---|---:|---|---:|---:|---:|---:|
| read_only | 512 | lz4 | 2,786,729.5 | 2.637 | 62.08 | 111,831 / 294,912 |
| read_only | 2048 | lz4 | 4,594,351.5 | 1.047 | 4.51 | 281,600 / 294,912 |
| write_heavy | 512 | qpl | 948,556.5 | 3.454 | 71.05 | 80,298 / 277,344 |
| write_heavy | 2048 | qpl | 1,727,271.2 | 1.406 | 28.88 | 194,429 / 273,384 |
| bench_mixed | 512 | zlib_accel | 268,288.0 | 4.589 | 78.21 | 49,078 / 225,216 |
| bench_mixed | 2048 | zlib_accel | 410,624.0 | 1.658 | 39.68 | 134,723 / 223,344 |

Interpretation: 2048B landing improves short-run QPS, but it can sharply reduce whole-tree effective memory saving. This confirms the expected throughput/capacity trade-off.

Silesia samba 128B value sweep on 2026-04-15:

```text
LANDING_LIST='512 2048' SHARDS_LIST='8' THREADS_LIST='4' \
BTREE_DURATION_SEC=1 BENCH_DURATION_SEC=1 BENCH_WARMUP_KEYS=1000 \
BENCH_OPS=1000 BENCH_KEY_SPACE=4096 BTREE_KEY_SPACE=4096 \
DRAM-tier/tests/btree/run_scaling_sweep.sh
```

Results were written to:

```text
DRAM-tier/tests/btree/results/20260415T191646Z/summary.tsv
```

Selected Silesia 128B trade-off points:

| Case | Landing bytes | Codec | QPS | Ratio | Saved pct | Compressed/Total bytes |
|---|---:|---|---:|---:|---:|---:|
| read_only | 512 | lz4 | 3,043,055.4 | 1.265 | 20.95 | 440,354 / 557,056 |
| read_only | 2048 | lz4 | 5,030,195.5 | 1.013 | 1.31 | 549,785 / 557,056 |
| write_heavy | 512 | qpl | 424,085.0 | 1.295 | 22.75 | 420,954 / 544,952 |
| write_heavy | 2048 | qpl | 1,111,282.8 | 1.123 | 10.92 | 483,652 / 542,912 |
| bench_mixed | 512 | zlib_accel | 88,064.0 | 1.466 | 31.80 | 307,952 / 451,520 |
| bench_mixed | 2048 | zlib_accel | 221,184.0 | 1.149 | 12.97 | 396,292 / 455,328 |

Interpretation: with real samba values, 512B landing gives meaningful memory savings, while 2048B landing mostly turns the leaf into a throughput-oriented uncompressed staging area. This confirms that 2048B should be treated as a throughput knob, not the default for capacity-oriented ZipCache evaluation.

## Current Baseline

Benchmark binary:

```bash
DRAM-tier/build_check/bin/bpt_compressed_throughput_bench
```

Baseline command:

```bash
BENCH_DURATION_SEC=5 \
BENCH_WARMUP_KEYS=10000 \
BENCH_KEY_SPACE=50000 \
BENCH_THREADS=<1|8|16|32> \
DRAM-tier/build_check/bin/bpt_compressed_throughput_bench
```

Mixed workload in `compressed_throughput_bench.c`:

- 40% `put`
- 35% `get`
- 15% `delete`
- 7% `get_range`
- 3% `stats`

Observed mixed workload QPS:

| Threads | LZ4 op/s | QPL op/s | zlib_accel op/s | Mismatches |
|---:|---:|---:|---:|---:|
| 1 | 557,466 | 245,350 | 83,968 | 0 |
| 8 | 2,056,397 | 636,314 | 499,302 | 0 |
| 16 | 958,874 | 553,370 | 667,034 | 0 |
| 32 | 853,811 | 377,242 | 290,406 | 0 |

Read-only isolation command:

```bash
BTREE_DURATION_SEC=2 \
BTREE_KEY_SPACE=50000 \
BTREE_HOT_PCT=100 \
BTREE_READ_PCT=100 \
BTREE_WRITE_PCT=0 \
BTREE_DELETE_PCT=0 \
BTREE_SCAN_PCT=0 \
BTREE_THREADS=<1|8|16|32> \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

Observed read-only QPS:

| Threads | LZ4 op/s | QPL op/s | zlib_accel op/s |
|---:|---:|---:|---:|
| 1 | 1,321,529 | 468,919 | 179,867 |
| 8 | 1,456,763 | 864,393 | 1,186,245 |
| 16 | 1,685,236 | 947,122 | 1,747,406 |
| 32 | 1,468,077 | 454,039 | 1,434,329 |

Interpretation:

- Read-only scales weakly until 16 threads, then regresses at 32 threads.
- Mixed workload regresses more strongly after 8 threads because writes/deletes/ranges add lock and rebuild contention.

## Relevant Code Paths

Primary files:

- `DRAM-tier/lib/bplustree_compressed.h`
- `DRAM-tier/lib/bplustree_compressed.c`
- `DRAM-tier/tests/btree/compressed_throughput_bench.c`
- `DRAM-tier/tests/btree/compressed_mixed_concurrency.c`

Important structures:

- `struct bplus_tree_compressed`
- `struct simple_leaf_node`
- `simple_leaf_node::landing_buffer[LANDING_BUFFER_BYTES]`
- `simple_leaf_node::rwlock`
- `bplus_tree_compressed::rwlock`
- `bplus_tree_compressed::qpl_job_pool`

Important functions:

- `bplus_tree_compressed_put_internal`
- `bplus_tree_compressed_get`
- `bplus_tree_compressed_get_range`
- `bplus_tree_compressed_delete`
- `insert_into_leaf`
- `compressed_leaf_rebuild_with_pairs`
- `compressed_leaf_collect_pairs`
- `compress_subpage`
- `decompress_subpage`
- `init_qpl`

## Current Design Summary

Each compressed leaf has a maximum 2KB landing buffer, with an effective default of 512B:

```c
struct simple_leaf_node {
    char landing_buffer[LANDING_BUFFER_BYTES]; // max allocation, currently 2048B
    char *compressed_data;
    int compressed_size;
    int num_subpages;
    bool is_compressed;
    compression_algo_t compression_algo;
    struct subpage_index_entry *subpage_index;
    pthread_rwlock_t rwlock;
    size_t uncompressed_bytes;
    size_t compressed_bytes;
};
```

The landing buffer is shared by all three compression modes:

- `COMPRESS_LZ4`
- `COMPRESS_QPL`
- `COMPRESS_ZLIB_ACCEL`

The effective size is controlled by `BTREE_LANDING_BUFFER_BYTES`, defaulting to 512B. It is not an IAA hardware queue/buffer. It is a software write buffer inside the ZipCache leaf. It still helps IAA/QPL because it reduces the number of hardware/software compression submissions and lets recent writes be read without decompression.

## Current Key/Value Format Limitation

The current DRAM-tier compressed B+Tree is still a fixed-record prototype:

```c
typedef int key_t;

struct kv_pair {
    key_t key;
    int stored_value;
    uint8_t payload[COMPRESSED_VALUE_BYTES];
};
```

Consequences:

- Keys are fixed 32-bit integers. Arbitrary key bytes are not supported by the tree comparator, internal separator keys, range scan, or sharding router.
- Values are fixed inline payload slots. `put_with_payload()` copies at most `COMPRESSED_VALUE_BYTES`; larger values are truncated by design.
- `get()` returns only `int stored_value`, so tests cannot validate arbitrary returned value bytes through the public API.
- Landing-buffer capacity and leaf capacity are measured in `sizeof(struct kv_pair)` slots, not actual key/value byte usage.
- The current 128B Silesia mode is useful for compressibility and landing-buffer trade-off evaluation, but it is not a general ZipCache KV API.

This means increasing `COMPRESSED_VALUE_BYTES` further is the wrong long-term direction. It changes the benchmark record size and leaf density, but it still does not solve arbitrary key/value insertion.

## Suspected Bottlenecks

1. Global tree rwlock on all operations

Current `get` takes `ct_tree->rwlock` as a read lock just to traverse to the leaf. Even read locks mutate shared lock state internally, so many reader threads contend on the same cacheline.

2. Leaf rwlock on every operation

After tree traversal, `get` takes per-leaf read lock. This is correct but can become hot under skewed keys or small key spaces.

3. Range scan holds tree read lock while walking leaves

`get_range` keeps `ct_tree->rwlock` during traversal across linked leaves. This can delay split/delete slow paths and can create lock convoying.

4. Write path does compression under leaf write lock

When the landing buffer is full, `insert_into_leaf` decompresses existing compressed subpages, merges the landing buffer, recompresses, and clears the landing buffer while holding leaf write lock.

5. Delete path rebuilds leaves under locks

The delete fast path avoids global tree write lock for non-min-key deletes, but it still collects leaf pairs and rebuilds the leaf under leaf write lock.

6. Split path still takes global tree write lock

Leaf split and parent insertion are structural operations and currently serialize under `ct_tree->rwlock`.

7. QPL job pool contention

`init_qpl` caps the global QPL job pool at 16 jobs. Threads acquire/release jobs through one mutex/cond. At 32 threads, this can become a major bottleneck. If QPL path is hardware/IAA, the submit path may also contend on too few hardware work queues.

8. NUMA and SMT effects

The current machine has 16 physical cores, 32 logical CPUs, and 2 NUMA nodes. 32 threads may regress from SMT contention, NUMA cross-node memory access, and heavier lock cacheline bouncing.

## Phase 0: Reproducible Measurement Harness

Purpose:

Make the scaling problem easy to reproduce before changing the data structure.

Tasks:

- Add a script such as `DRAM-tier/tests/btree/run_scaling_sweep.sh`.
- Sweep `threads = 1, 2, 4, 8, 16, 32`.
- Run at least these mixes:
  - read-only: `100/0/0/0`
  - read-heavy: `80/15/5/0`
  - current mixed: `50/35/10/5` for `compressed_mixed_concurrency`
  - benchmark mixed: current hardcoded `40/35/15/7/3` in `compressed_throughput_bench`
- Record QPS, mismatches, and exit code.
- Optionally run each point 3 times and report median.

Acceptance criteria:

- Script exits non-zero on any mismatch.
- Output table clearly shows scaling by codec and thread count.
- Baseline results are saved under `DRAM-tier/tests/btree/results/`.

## Phase 1: Reduce Read-Side Global Lock Contention

Purpose:

Remove `ct_tree->rwlock` from the point-read hot path as much as possible.

Current problem:

`bplus_tree_compressed_get` takes tree read lock for traversal, then leaf read lock. This makes all readers contend on the global tree lock even if they touch different leaves.

Proposed options:

Option A: Versioned optimistic traversal

- Add a tree structural version counter.
- Increment it on split, parent insert, leaf delete, or rebalance.
- `get` reads `version_before`, traverses without tree lock, takes leaf read lock, reads `version_after`.
- If version changed or leaf looks invalid, release leaf and retry with tree read lock fallback.

Option B: Root pointer RCU-like read

- Keep old nodes alive during operation lifetime.
- Readers traverse without tree lock.
- Writers still take tree write lock for structural changes.
- This needs safe memory reclamation, so it is more invasive.

Recommendation:

Start with Option A. It is simpler and safer.

Acceptance criteria:

- Read-only QPS should improve and scale at least from 1 to 8 and 16 threads.
- No point-read mismatches in `bpt_compressed_mixed_concurrency` with `BTREE_SCAN_PCT=0`.
- Existing split/delete tests still pass.

## Phase 2: Shorten Leaf Write Lock Hold Time

Purpose:

Avoid doing expensive decompress/recompress while holding `simple_leaf_node::rwlock`.

Current problem:

When landing buffer is full, `insert_into_leaf` does full leaf merge and compression under leaf write lock.

Proposed design:

- Under leaf write lock, copy:
  - landing buffer
  - compressed metadata
  - compressed_data bytes needed for rebuild
  - a leaf generation/version
- Release leaf lock.
- Decompress/merge/recompress outside the lock.
- Reacquire leaf write lock.
- If generation unchanged, install new compressed_data/subpage_index and clear landing buffer.
- If generation changed, discard work and retry or fall back to current locked path.

Key design detail:

Do not expose partially rebuilt leaf state. Build new buffers separately, then atomically install while holding leaf write lock.

Acceptance criteria:

- Mixed workload QPS should improve at 16 and 32 threads.
- No mismatches in:
  - `bpt_compressed_throughput_bench`
  - `bpt_compressed_mixed_concurrency`
  - `bpt_compressed_split_payload_stats`

## Phase 3: Make Range Scan Less Blocking

Purpose:

Prevent scans from holding global tree read lock across many leaves.

Current problem:

`bplus_tree_compressed_get_range` holds `ct_tree->rwlock` while walking linked leaves and reading each leaf.

Proposed design:

- Use tree lock only to find the starting leaf.
- Pin or validate the current leaf.
- Release tree lock.
- Read current leaf under leaf read lock.
- Move to next leaf using a safe next pointer with validation.
- If structural version changed, retry from last key.

Simpler intermediate step:

- Add a benchmark mode with scan disabled to isolate this.
- Keep current scan semantics initially, but avoid using range scans in throughput claims until this is fixed.

Acceptance criteria:

- Mixed workload with 5% scans should not collapse under 16/32 threads.
- Range correctness issue should remain tracked separately if not fixed in the same phase.

## Phase 4: QPL/IAA-Specific Submission Path

Purpose:

Avoid global QPL job pool contention and map better to IAA hardware queues.

Current problem:

`init_qpl` creates a global pool capped at 16 jobs. `compress_subpage` and `decompress_subpage` acquire jobs through one mutex/cond.

Proposed design:

- Replace or supplement global pool with thread-local QPL jobs:
  - one `qpl_job` per thread for compression
  - one `qpl_job` per thread for decompression
- Initialize lazily on first use per thread.
- Keep global pool as fallback if TLS init fails.
- For hardware path, expose config for number of queues and thread-to-queue mapping if QPL supports it directly in the chosen API.

Benchmark knobs:

- `BTREE_QPL_PATH=software|hardware|auto`
- `BTREE_QPL_MODE=fixed|dynamic`
- Add optional `BTREE_QPL_POOL_SIZE` if global pool remains.

Acceptance criteria:

- QPL 16/32-thread QPS should not fall sharply because of job pool mutex contention.
- Hardware-only mode must fail fast if IAA is not configured.

## Phase 5: Sharding for Write Scalability

Purpose:

Scale writes when a single B+Tree root/global structure becomes the bottleneck.

Proposed design:

- Partition key space into N independent compressed B+Trees.
- Choose shard by hash/range partition.
- Each shard has its own tree rwlock and leaf locks.
- Range scan across shards needs either:
  - range partitioning, or
  - fan-out/merge across hash shards.

Recommendation:

Use range partitioning if `SCAN` is a core ZipCache operation. Use hash sharding if point lookup/update throughput is the primary goal.

Acceptance criteria:

- Mixed write-heavy workload should scale beyond 8 threads.
- Range scan semantics remain defined and tested.

## Phase 6: Landing Buffer Policy Tuning

Purpose:

Use landing buffer to reduce codec/IAA submissions without growing stale/uncompressed data too much.

Current landing buffer:

- Maximum 2KB per leaf.
- Effective default is 512B per leaf.
- Runtime override: `BTREE_LANDING_BUFFER_BYTES=256|512|1024|2048`.
- Shared by LZ4, QPL, and zlib-accel.
- With 128B inline values, 2048B landing is half of a 4KB leaf and can significantly reduce effective compression.

Experiments:

- Sweep landing buffer size: 256B, 512B, 1KB, 2KB.
- Track:
  - mixed QPS
  - memory overhead
  - effective compression ratio / saved memory percent
  - compressed bytes / total bytes
  - number of compression operations
  - read hit rate in landing buffer if instrumented

IAA-specific expectation:

- Larger landing buffer should reduce IAA/QPL submissions and improve write-heavy workloads.
- Too large a landing buffer can reduce effective compression and increase scan/read work.

Acceptance criteria:

- Find a default that improves 16/32-thread mixed QPS without causing large memory overhead.

## Phase 7: Variable-Length Key/Value DRAM-Tier API

Purpose:

Make the DRAM-tier compressed B+Tree accept the actual key and value bytes supplied by a workload, instead of forcing tests into `int key_t` plus fixed `COMPRESSED_VALUE_BYTES` payload slots.

Target API:

```c
int bplus_tree_compressed_put_kv(struct bplus_tree_compressed *tree,
                                 const void *key, size_t key_len,
                                 const void *value, size_t value_len);

int bplus_tree_compressed_get_kv(struct bplus_tree_compressed *tree,
                                 const void *key, size_t key_len,
                                 void *out_value, size_t out_capacity,
                                 size_t *out_value_len);

int bplus_tree_compressed_delete_kv(struct bplus_tree_compressed *tree,
                                    const void *key, size_t key_len);
```

Design requirement:

- Keep the current integer API as a compatibility wrapper until existing tests and benchmarks are migrated.
- Do not implement arbitrary values by simply increasing `COMPRESSED_VALUE_BYTES`.
- Store variable-length records inside a 4KB leaf/subpage using a slotted-page layout:
  - page header: record count, free-space offsets, generation/version
  - slot directory: offsets and record lengths
  - record body: key length, value length, optional metadata, key bytes, value bytes
- Landing buffer must become byte-capacity based, not `struct kv_pair` slot-count based.
- Compression stats must count real resident key/value bytes and compressed bytes, not `sizeof(struct kv_pair)`.

Key design choice:

- Correct range scans require byte-key ordering in internal B+Tree separator keys. This means replacing `key_t` internal separators with variable-length separator keys and a comparator.
- A faster intermediate point-only mode can route arbitrary keys by hash and keep full key bytes in leaves for equality checks, but it is not correct for range scan semantics and must not be presented as final ZipCache behavior.

Recommended implementation sequence:

1. Add an internal `kv_record` abstraction and helpers for byte-key comparison, serialization, deserialization, and copy-out.
2. Add a slotted-page encoder/decoder for one leaf subpage and test it independently without compression.
3. Replace compressed leaf rebuild/collect logic to operate on variable-length records.
4. Convert landing buffer to an append-only byte buffer with duplicate-key overwrite semantics during merge.
5. Add `put_kv/get_kv/delete_kv` public APIs while retaining the current integer wrappers.
6. Add tests with variable key/value distributions:
   - fixed 16B keys + variable 32B to 512B values
   - variable 8B to 64B keys + 128B Silesia slices
   - duplicate updates with changing value lengths
   - delete/reinsert after split
   - range scan over lexicographic byte keys
7. Only after point correctness is stable, revisit concurrency scaling and IAA/QPL submission behavior with the new record layout.

Acceptance criteria:

- Tests can insert and retrieve arbitrary byte values without truncation.
- Tests can use variable key lengths without first converting keys to `int`.
- Existing integer-key smoke tests still pass through compatibility wrappers.
- Whole-tree compression stats reflect actual key/value bytes.
- Range scan behavior is explicitly correct for byte-key ordering, or the test is labeled point-only if using the temporary hash-routed mode.

## Phase 8: Background Landing-Buffer Compaction

Purpose:

Move expensive landing-buffer merge/recompress work out of foreground `put()` when possible. This should reduce write-path p99/p999 latency and improve write-heavy throughput stability, especially for QPL hardware/IAA and zlib-style codecs where compression cost is much higher than a simple landing-buffer insert.

Current foreground behavior:

- `put()` first inserts into the per-leaf landing buffer when a free slot exists.
- When the landing buffer is full, the foreground writer decompresses existing compressed subpages, merges landing entries, inserts the new entry, recompresses subpages, clears landing, and returns.
- This creates latency spikes and makes foreground user threads directly pay for LZ4/QPL/zlib work.

Implemented direction:

- First implementation uses a conservative low-rate scan-only worker rather than a queued leaf pointer design.
- This intentionally avoids queued-leaf lifetime hazards while delete/split/rebalance can free leaves.
- The worker holds the tree read lock while scanning the leaf list, then uses leaf `trywrlock` by default.
- If landing occupancy exceeds the high-watermark, it merges compressed data with landing entries, rebuilds compressed subpages, clears landing, and updates the leaf generation/stat fields.
- Foreground still keeps the existing synchronous fallback when a landing buffer is full and the background worker has not compacted it yet. Correctness does not depend on the background thread running.

Proposed runtime knobs:

```text
BTREE_BG_COMPACTION=0|1
BTREE_BG_THREADS=N
BTREE_BG_SCAN_INTERVAL_US=N
BTREE_BG_LANDING_HIGH_WATERMARK_PCT=N
BTREE_BG_LANDING_LOW_WATERMARK_PCT=N
BTREE_BG_MAX_LEAVES_PER_PASS=N
BTREE_BG_MAX_COMPACTIONS_PER_SEC=N
BTREE_BG_TRYLOCK_ONLY=0|1
```

Initial default recommendation:

- `BTREE_BG_COMPACTION=0` by default until correctness and IAA queue behavior are validated.
- `BTREE_BG_THREADS=1` for first implementation.
- `BTREE_BG_LANDING_HIGH_WATERMARK_PCT=75`.
- `BTREE_BG_TRYLOCK_ONLY=1` to avoid background workers blocking foreground operations during early experiments.

Implemented code:

1. Added tree-level background state in `struct bplus_tree_compressed`:
   - worker threads
   - shutdown flag
   - scan serialization lock
   - tuning knobs
   - `bg_passes`, `bg_compactions`, `bg_trylock_misses`, `bg_skipped`, and `bg_errors`.
2. Added `compressed_leaf_flush_landing_locked()`:
   - collects compressed and landing pairs using existing newest-landing-wins semantics.
   - hashes all resident pairs into subpages.
   - compresses subpages through the selected codec.
   - clears landing buffer after installing the compressed image.
3. Added background lifecycle:
   - configured at tree init from `BTREE_BG_*`.
   - starts after QPL initialization.
   - stops before leaf/free cleanup in `bplus_tree_compressed_deinit()`.
4. Added `bplus_tree_compressed_bg_stats()` and mixed-concurrency benchmark output:
   - `bg_passes`
   - `bg_compactions`
   - `bg_trylock_misses`
   - `bg_skipped`
   - `bg_errors`.
5. Sharded trees inherit the same behavior per shard because each shard initializes its own background worker when enabled. Aggregated background counters are returned by the top-level sharded tree.
6. Added key-based dirty queue:
   - foreground enqueues the written key when landing occupancy crosses high watermark.
   - background workers pop keys, re-find the current owning leaf, and compact if still above watermark.
   - this avoids storing raw leaf pointers and therefore avoids queued leaf lifetime hazards.
7. Added foreground and queue counters:
   - `fg_landing_full`
   - `fg_sync_compactions`
   - `fg_sync_compaction_errors`
   - `fg_split_fallbacks`
   - `bg_enqueue_attempts`
   - `bg_enqueued`
   - `bg_enqueue_duplicates`
   - `bg_queue_full`
   - `bg_queue_pops`.
8. Added `bplus_tree_compressed_compaction_stats()` and exported the new counters through `bpt_compressed_mixed_concurrency` and `run_iaa_eval.sh`.
9. Added `tail_latency_summary.tsv` generation in `run_iaa_eval.sh` when `RUN_TAIL_LATENCY=1`.
10. Extended `tail_latency_compare` output to include p999 for both get and put latency.

Important correctness constraints:

- Background compaction must preserve "landing entries are newer than compressed entries" duplicate-key semantics.
- It must not race with split/delete/rebalance. If structural operations can free a leaf, the first version should either require the tree read lock while compacting or use a safe queued-leaf lifetime rule.
- Do not use background compaction with unsafe lock-free traversal or default-on out-of-lock rebuild until reclamation/lifetime is solved.
- Sharded trees currently have one key-based dirty queue per shard.
- Current scan-only implementation is lifetime-safe because it does not retain leaf pointers outside the tree read-locked scan.
- Current key-based dirty queue is also lifetime-safe because it stores keys, not leaf pointers. The worker re-finds the leaf under tree read lock before compaction.

Codec-specific expectations:

| Codec | Expected benefit | Main risk | Suggested initial strength |
|---|---|---|---|
| LZ4 | Small to medium; mainly smoother write p99 | Background lock contention may erase gains | Low or off |
| QPL software | Medium; avoids foreground dynamic/fixed deflate stalls | CPU contention | 1 worker, throttled |
| QPL hardware/IAA | Potentially high for write-heavy workloads | IAA work-queue contention and foreground read-decompress interference | 1 worker first, then queue-aware tuning |
| zlib/zlib-accel | Potentially high when compression is slow | CPU/runtime contention | 1 worker, throttled |

Instrumentation required before making claims:

- foreground synchronous compactions
- background compactions
- compaction failures / trylock misses
- average and max landing occupancy
- foreground put p50/p99/p999
- foreground get p50/p99/p999
- leaf lock wait time if practical
- QPL submit/execute failures if practical
- `ratio`, `saved_pct`, `compressed_bytes`, `total_bytes`

Benchmark matrix:

```bash
for bg in 0 1; do
  BTREE_BG_COMPACTION=$bg \
  BTREE_BG_THREADS=1 \
  BTREE_BG_LANDING_HIGH_WATERMARK_PCT=75 \
  BTREE_BG_LANDING_LOW_WATERMARK_PCT=25 \
  BTREE_SHARDS=8 \
  BTREE_LANDING_BUFFER_BYTES=512 \
  THREADS_LIST='4 8 16 32' \
  DRAM-tier/tests/btree/run_iaa_eval.sh
done
```

IAA-specific matrix:

```bash
RUN_IAA=1 \
BTREE_BG_COMPACTION=1 \
BTREE_BG_THREADS=<1|2|4> \
BTREE_QPL_PATH=hardware \
BTREE_QPL_MODE=<fixed|dynamic> \
DRAM-tier/tests/btree/run_iaa_eval.sh
```

Acceptance criteria:

- `mismatches=0` across LZ4/QPL/zlib-accel.
- Foreground synchronous compactions decrease under write-heavy workloads.
- `put` p99/p999 improves or stays flat.
- `get` p99 does not regress materially.
- Write-heavy and point-mixed QPS improve or stay flat.
- Whole-tree `saved_pct` does not fall unexpectedly relative to the same landing-buffer size.
- On IAA hardware, background compaction must not reduce QPL hardware throughput by saturating or contending on work queues.

Local validation on non-IAA machine:

Build and default correctness:

```text
cmake --build DRAM-tier/build_check -j$(nproc): passed
bpt_compressed_crud_fuzz: passed
bpt_compressed_split_payload_stats: passed
bpt_compressed_mixed_concurrency: passed, mismatches=0, bg counters all 0 by default
```

Write-heavy synthetic short run, 4 threads, 2 seconds, 2048-key space, mix 20/70/10/0:

| Background | LZ4 QPS | QPL software QPS | zlib QPS | Mismatches | Notes |
|---|---:|---:|---:|---:|---|
| off | 1,047,811 | 660,798 | 265,455 | 0 | Lower ratio because more data remains in landing buffers. |
| on, 75% watermark | 1,031,066 | 713,116 | 280,292 | 0 | QPL/zlib improved; LZ4 slightly lower due to background CPU/lock contention. |
| on, 50% watermark | 979,074 | 594,590 | 222,772 | 0 | Too aggressive for this CPU-only run; better ratio but lower QPS. |

Handoff note:

Implement Phase 8 before revisiting Phase 7 variable-length KV if the immediate goal is IAA promotion. Phase 8 directly targets the foreground compression stalls and queue-submission behavior Intel cares about; Phase 7 is required for final ZipCache API fidelity but is a larger storage-layout change.

## Phase 8 TODO Checklist

Use this checklist as the current handoff queue. The existing Phase 8 implementation now has a safe scan fallback plus a key-based dirty queue. Remaining work should be done incrementally with correctness tests after each item.

### P8.1 Instrument Foreground Compaction - Done

- Add counters for foreground synchronous landing-buffer compactions.
- Add counters for foreground compaction failures that lead to leaf split.
- Add counters for landing-buffer full events.
- Expose these through a stats API similar to `bplus_tree_compressed_bg_stats()`.
- Print these counters from `bpt_compressed_mixed_concurrency`.
- Add them to `run_iaa_eval.sh` `summary.tsv`.

Implemented:

- `bplus_tree_compressed_compaction_stats()`
- `fg_landing_full`
- `fg_sync_compactions`
- `fg_sync_compaction_errors`
- `fg_split_fallbacks`
- mixed-concurrency and `summary.tsv` output.

Why this matters:

Without foreground compaction counters, we can only see that background compaction happened. We cannot prove it reduced foreground stalls, which is the main Phase 8 claim.

### P8.2 Add Tail-Latency Summary For Phase 8 - Mostly Done

- Extend `run_iaa_eval.sh` so `RUN_TAIL_LATENCY=1` produces a compact TSV summary, not only logs/CSV files.
- Include `get_p50/get_p99/get_p999` if available.
- Include `put_p50/put_p99/put_p999` if available.
- Add a `bg_compaction` column so results with `BTREE_BG_COMPACTION=0` and `1` are directly comparable.
- Keep raw CSV/log files for deeper inspection.

Implemented:

- `tail_latency_summary.tsv`
- get/put p50/p90/p99/p999/max/avg
- throughput in Mops/s
- raw CSV/log retention.

Remaining:

- Add explicit `bg_compaction`, `bg_threads`, and `bg_watermark` columns to `tail_latency_summary.tsv`.

Why this matters:

Background compaction may not always improve average QPS. Its strongest expected benefit is lower write tail latency, especially for QPL hardware/IAA and zlib-accel.

### P8.3 Add Dirty Queue - Done With Key-Based Queue

- Original leaf-pointer plan:
  - `dirty_for_compaction`
  - `compaction_queued`
  - `compaction_in_progress`
  - `last_compaction_generation`
- Add per-shard dirty queue:
  - bounded ring buffer or linked queue
  - queue lock
  - condition variable
  - shutdown handling
- Foreground `insert_into_leaf()` should enqueue the leaf once when landing occupancy crosses `BTREE_BG_LANDING_HIGH_WATERMARK_PCT`.
- Background worker should prefer queued dirty leaves before falling back to low-rate scan.
- Add `BTREE_BG_QUEUE_CAPACITY`.
- Add queue stats:
  - enqueue attempts
  - successful enqueues
  - duplicate enqueue skips
  - queue full drops
  - queue pops.

Correctness requirement:

Do not store raw leaf pointers in the queue unless leaf lifetime is protected. Start with either tree-read-lock protected queue processing, a leaf refcount, or an epoch/hazard scheme.

Implemented:

- Key-based dirty queue, not raw leaf-pointer queue.
- `BTREE_BG_QUEUE_CAPACITY`.
- Queue lock/condition variable/shutdown handling.
- Background worker prefers queued dirty keys, then falls back to scan.
- The worker re-finds the current leaf for the queued key under tree read lock.
- Queue stats are exported in mixed benchmark and `summary.tsv`.

Deferred:

- Per-leaf queued/in-progress state is not required for key-based queue correctness but may still be useful later to reduce duplicate enqueue attempts.

### P8.4 Protect Queued Leaf Lifetime - Avoided For Current Queue

Choose one safe lifetime model before making dirty queue the default:

| Option | Pros | Cons |
|---|---|---|
| Tree read lock held while popping/processing queued leaf | Simple and safe | Reduces concurrency and can block structural changes |
| Leaf refcount | Good performance | More invasive; delete/split/free must respect refcount |
| Epoch/RCU-style reclamation | Best long-term read scalability | Most complex; requires deferred free |

Initial recommendation:

Implement leaf refcount only for queued background compaction. Keep scan-only mode as fallback. Do not combine queued leaf pointers with unsafe free paths.

Current implementation:

- Avoids the problem by storing queued keys instead of leaf pointers.
- Background worker finds the leaf under tree read lock and does not retain it after unlocking.
- If a future implementation switches to leaf-pointer queue, this item becomes required again.

### P8.5 Add Low-Watermark / Hysteresis

- Implement `BTREE_BG_LANDING_LOW_WATERMARK_PCT`.
- A leaf should not be requeued until occupancy rises above high watermark after being compacted below low watermark.
- Keep high default at `75`, low default at `25`.

Why this matters:

Without hysteresis, hot leaves can be compacted too frequently, reducing QPS and increasing IAA queue pressure.

### P8.6 Add Codec-Aware Budgeting

- Implement `BTREE_BG_CODEC_BUDGET`.
- Add separate budget for QPL hardware if possible:
  - max background QPL operations per second
  - max background QPL in-flight jobs if async path exists later
- Keep LZ4 background compaction low/off by default.
- Let QPL hardware/zlib-accel use more aggressive defaults only after IAA tests prove it helps.

Why this matters:

IAA is a shared accelerator. Background compaction can help by moving compression off foreground, but it can also hurt by saturating the same work queues used by foreground reads/writes.

### P8.7 Add IAA Queue-Aware Scheduling

- Stop treating all QPL calls as equal.
- Separate foreground and background QPL job pools if QPL/IAA supports enough queues.
- Add optional background-only QPL path/queue selection if available.
- Add queue affinity or per-thread assignment so many threads do not submit to the same queue.
- Track QPL execution failures and fallbacks.

Why this matters:

Intel's observed regression likely comes from submission-path or work-queue contention rather than raw IAA capacity. Phase 8 should not worsen that contention.

### P8.8 Evaluate Sharded Background Thread Count

- Current behavior: `BTREE_SHARDS=8` and `BTREE_BG_THREADS=1` creates one worker per shard, so 8 background workers total.
- Add an option for global background worker budget across all shards.
- Candidate env:
  - `BTREE_BG_GLOBAL_THREADS=N`
  - `BTREE_BG_THREADS_PER_SHARD=N`
- Compare:
  - one worker per shard
  - one global worker
  - fixed global pool of 2/4 workers.

Why this matters:

Per-shard workers scale well for CPU but may be too aggressive for IAA hardware queues.

### P8.9 Integrate Background Compaction Into `run_iaa_eval.sh` - Partial

- Add script-level matrix for `BTREE_BG_COMPACTION=0|1`.
- Add `bg_compaction`, `bg_threads`, `bg_watermark`, and `bg_scan_interval_us` columns to `summary.tsv`.
- Add a `RUN_BG_SWEEP=1` option that compares off vs on for all workloads.
- Keep default off unless the user explicitly requests Phase 8.

Implemented:

- `summary.tsv` includes Phase 8 counters from mixed-concurrency output.
- `tail_latency_summary.tsv` is generated when `RUN_TAIL_LATENCY=1`.

Remaining:

- Add explicit config columns: `bg_compaction`, `bg_threads`, `bg_watermark`, `bg_scan_interval_us`, and `bg_queue_capacity`.
- Add `RUN_BG_SWEEP=1`.

### P8.10 Acceptance Test Matrix

Run this after each implementation step:

```bash
cmake --build DRAM-tier/build_check -j$(nproc)

DRAM-tier/build_check/bin/bpt_compressed_crud_fuzz
DRAM-tier/build_check/bin/test_compression_concurrency
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
DRAM-tier/build_check/bin/bpt_compressed_split_payload_stats

BTREE_BG_COMPACTION=1 \
BTREE_BG_THREADS=1 \
BTREE_BG_LANDING_HIGH_WATERMARK_PCT=75 \
BTREE_THREADS=4 \
BTREE_DURATION_SEC=2 \
BTREE_KEY_SPACE=2048 \
BTREE_READ_PCT=20 \
BTREE_WRITE_PCT=70 \
BTREE_DELETE_PCT=10 \
BTREE_SCAN_PCT=0 \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

Expected:

- all tests pass
- all mixed-concurrency rows show `mismatches=0`
- `bg_errors` should be explained if nonzero
- QPS should not collapse relative to `BTREE_BG_COMPACTION=0`
- `saved_pct` should improve or stay within expected landing-buffer trade-off.

## Phase 9: IAA Performance Sprint For QPL/zlib-accel API Paths

Goal:

Maximize throughput and tail latency for ZipCache DRAM-tier B+Tree when Intel has already configured QPL or zlib-accel to use IAA. The target is to make QPL/zlib-accel competitive with, and ideally faster than, LZ4 in write-heavy and mixed workloads. LZ4 remains the CPU baseline and must not be slowed down by QPL/zlib-specific changes.

Assumptions:

- Intel owns IAA enablement and runtime setup outside this repo.
- QPL and zlib-accel are external codec APIs. B+Tree only controls how often and where it calls those APIs.
- IAA benefit should appear mostly in compression-heavy paths: write-only, R:W 5:5, and R:W 2:8.
- Read-only may still favor LZ4 because LZ4 decode is cheap and IAA/QPL/zlib have API overhead.

Non-goals:

- Do not remove or slow down LZ4.
- Do not add B+Tree-owned IAA device setup, `accel-config`, queue provisioning, or low-level DSA/IAA queue management.
- Do not build a generic codec runtime abstraction beyond what is needed for this performance sprint.
- Do not require IAA for correctness tests.
- Do not implement arbitrary variable-length KV layout in this phase.

Implemented in the current Phase 9 pass:

- Added B+Tree-side codec API counters:
  - `qpl_compress_calls`
  - `qpl_decompress_calls`
  - `qpl_tls_jobs`
  - `qpl_pool_jobs`
  - `qpl_errors`
  - `zlib_compress_calls`
  - `zlib_decompress_calls`
  - `zlib_stream_reuses`
  - `zlib_stream_inits`
  - `zlib_errors`
- Added `bplus_tree_compressed_codec_stats()` with shard aggregation.
- Added `BTREE_QPL_JOB_CACHE=thread|pool`.
  - `thread` is the default and uses the existing thread-local QPL job reuse path.
  - `pool` disables TLS jobs and uses the retained global QPL job pool for comparison/debugging.
- Added optional `BTREE_ZLIB_STREAM_CACHE=none|thread`.
  - `none` is the default and preserves the previous `compress2()`/`uncompress()` behavior.
  - `thread` reuses per-thread `z_stream` objects for independent leaf blocks.
- Extended `bpt_compressed_mixed_concurrency` output with codec API counters.
- Extended `run_iaa_eval.sh summary.tsv` with codec cache labels and counters.
- Added `BTREE_BG_CODEC=all|lz4|qpl|zlib_accel` so background compaction can target QPL/zlib paths without perturbing LZ4.
- Added `RUN_BG_SWEEP=1`, `BG_SWEEP_VALUES`, and `LANDING_LIST` to `run_iaa_eval.sh` so Intel can sweep background compaction and landing-buffer size with one command.
- Verified that LZ4 rows report zero QPL/zlib API calls, so the LZ4 fast path is not routed through Phase 9 runtime logic.
- Added `BTREE_CODEC_FILTER=all|lz4|qpl|zlib|zlib_accel` to `bpt_compressed_mixed_concurrency`.
- Updated `run_iaa_eval.sh` to run each codec in a separate process instead of timing all codecs together. This makes CPU usage meaningful for LZ4 vs QPL vs zlib-accel comparisons.
- Added benchmark-side timed-window CPU metrics with `getrusage()`.
- Added default `COLLECT_CPU=1` process-level RSS/context-switch metrics through `/usr/bin/time`.
- Extended `summary.tsv` with `wall_sec`, `user_sec`, `sys_sec`, `cpu_pct`, `cpu_cores`, `qps_per_cpu_core`, `max_rss_kb`, `voluntary_cs`, and `involuntary_cs`.
- Added `SOFTWARE_CODECS`, `IAA_CODECS`, and `ZLIB_ACCEL_CODECS` filters to keep hardware runs focused while preserving the software baseline matrix.

### P9.1 QPL Job Reuse

Status: implemented for the main B+Tree path.

Purpose:

- Avoid repeated QPL job init/allocation on the foreground path.
- Compare `BTREE_QPL_JOB_CACHE=thread` against `pool` if Intel sees unexpected regressions.

Acceptance:

- QPL rows should show `qpl_tls_jobs>0` and `qpl_pool_jobs=0` in the default configuration.
- LZ4 rows should show all QPL counters as zero.
- QPL hardware/software selection remains external to B+Tree except for the existing `BTREE_QPL_PATH` test knob.

### P9.2 zlib Stream Reuse

Status: implemented behind `BTREE_ZLIB_STREAM_CACHE=thread`, default off.

Purpose:

- Avoid repeated zlib stream setup if Intel zlib-accel benefits from stream API reuse.
- Keep `none` as default until Intel validates that `thread` is faster with zlib-accel preload.

Acceptance:

- zlib rows should show `zlib_stream_reuses>0` and `zlib_errors=0` when enabled.
- LZ4 and QPL rows should show zlib counters as zero.

### P9.3 QPL/zlib-Only Background Compaction

Status: implemented with `BTREE_BG_CODEC`.

Purpose:

- Let foreground writes append to landing buffers.
- Move expensive QPL/zlib compaction work into background workers.
- Avoid perturbing LZ4 unless explicitly requested.

Acceptance:

- `BTREE_BG_CODEC=qpl` should only enqueue/compact QPL leaves.
- `BTREE_BG_CODEC=zlib_accel` should only enqueue/compact zlib leaves.
- `BTREE_BG_CODEC=all` preserves old behavior.
- QPL/zlib write p99/p999 should improve or QPS should increase on IAA machines when background compaction is tuned correctly.

### P9.4 Landing Buffer And Background Sweep

Status: implemented in `run_iaa_eval.sh`.

Purpose:

- Find the practical throughput/latency point for IAA by sweeping landing buffer size and background on/off.
- The most important knobs are `LANDING_LIST`, `RUN_BG_SWEEP`, `BTREE_BG_CODEC`, `BTREE_BG_THREADS`, and `BTREE_BG_LANDING_HIGH_WATERMARK_PCT`.

Acceptance:

- `summary.tsv` must include landing bytes, background config, throughput, latency, correctness, compression ratio, and codec API counters.
- No manual log parsing is required.

### P9.5 Host CPU Offload Measurement

Status: implemented in `run_iaa_eval.sh`.

Purpose:

- Show whether QPL/zlib-accel on IAA reduces host CPU usage, even when throughput is similar to LZ4.
- Make the IAA claim stronger than throughput-only: lower `cpu_cores` or higher `qps_per_cpu_core` is a useful offload result.

Acceptance:

- Each `summary.tsv` row must represent one codec process, not a mixed LZ4/QPL/zlib process.
- `cpu_pct=800%` should be interpreted as roughly eight host logical CPUs consumed during the timed workload window.
- `cpu_cores` and `qps_per_cpu_core` must be available for direct comparison.
- Hardware IAA rows should ideally show either higher QPS/lower latency than software rows or lower host CPU cores at comparable QPS.

### P9.6 Intel IAA Sprint Matrix

After Intel configures IAA outside this repo, use this as the primary matrix:

```bash
RUN_IAA=1 \
RUN_ZLIB_ACCEL=1 \
RUN_BG_SWEEP=1 \
ZLIB_ACCEL_SO=<zlib-accel-build>/libzlib_accel.so \
THREADS_LIST='8 32' \
LANDING_LIST='512 1024 2048' \
BTREE_DURATION_SEC=5 \
BTREE_KEY_SPACE=50000 \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_BG_CODEC=qpl \
BTREE_BG_THREADS=1 \
BTREE_BG_LANDING_HIGH_WATERMARK_PCT=75 \
COLLECT_CPU=1 \
BTREE_QPL_JOB_CACHE=thread \
BTREE_ZLIB_STREAM_CACHE=thread \
BTREE_MEASURE_LATENCY=1 \
BTREE_LATENCY_SAMPLES_PER_THREAD=4096 \
DRAM-tier/tests/btree/run_iaa_eval.sh
```

If zlib-accel is the main target, rerun with:

```bash
RUN_IAA=1 \
RUN_ZLIB_ACCEL=1 \
RUN_BG_SWEEP=1 \
ZLIB_ACCEL_SO=<zlib-accel-build>/libzlib_accel.so \
THREADS_LIST='8 32' \
LANDING_LIST='512 1024 2048' \
BTREE_DURATION_SEC=5 \
BTREE_KEY_SPACE=50000 \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_SHARDS=8 \
BTREE_BG_CODEC=zlib_accel \
BTREE_BG_THREADS=1 \
BTREE_BG_LANDING_HIGH_WATERMARK_PCT=75 \
COLLECT_CPU=1 \
BTREE_QPL_JOB_CACHE=thread \
BTREE_ZLIB_STREAM_CACHE=thread \
DRAM-tier/tests/btree/run_iaa_eval.sh
```

Expected hardware claims:

- QPL with Intel-configured IAA should beat QPL software fixed in write-heavy and mixed workloads.
- QPL dynamic may improve compression ratio; QPS may be lower than fixed and must be reported separately.
- zlib API with Intel zlib-accel preload should be reported only as `engine=zlib_accel_preload`; regular system zlib is not a headline comparison target.
- LZ4 may still win read-only latency. The primary success target is write-only, R:W 5:5, and R:W 2:8.
- If QPL/zlib-accel does not beat LZ4 on QPS, it can still be valuable if `cpu_cores` is significantly lower and `qps_per_cpu_core` is higher.

### P9.7 Remaining Work

Short list only:

1. Run the IAA sprint matrix on Intel hardware.
2. Pick the best landing buffer and background settings for QPL fixed, QPL dynamic, and zlib-accel.
3. If background compaction hurts IAA, add a simple codec-op rate limit using existing `BTREE_BG_MAX_COMPACTIONS_PER_SEC`.
4. Add a small post-processing helper if Intel wants a compact table of `QPS`, `p99`, `cpu_cores`, and `qps_per_cpu_core` from `summary.tsv`.
5. Keep defaults conservative until Intel results prove a better default.

## DRAM-Tier Optimization Backlog

This backlog is broader than Phase 8 and should be prioritized based on whether the next goal is IAA promotion or paper-complete ZipCache.

### D1 IAA/QPL Async Path

- Replace synchronous `qpl_execute_job()` usage with an async submit/poll path where practical.
- Reuse QPL job objects and buffers per thread.
- Avoid repeated init/fini in hot paths.
- Track submit latency, completion latency, and queue wait.

Priority:

High for IAA hardware claims.

### D2 Work-Queue Distribution

- Detect enabled IAA devices/work queues.
- Assign foreground threads across available queues.
- Keep background compaction on separate queues or throttle it aggressively.
- Fail fast for `BTREE_QPL_PATH=hardware` if hardware queues are unavailable.

Priority:

High. This directly targets Intel's concern that performance regresses at higher thread counts due to queue submission contention.

### D3 Adaptive Compression Bypassing

- Track per-leaf hotness.
- Keep hot leaves uncompressed or less aggressively compacted.
- Recompress cooled-down leaves in background.
- Add env knobs for hotness threshold and aging period.

Priority:

Medium-high. This is one of the paper's DRAM-tier design techniques and should reduce read/write amplification on hot pages.

### D4 Locking / Read Scalability

- Continue reducing tree-level lock overhead.
- Consider safe optimistic read traversal.
- Consider RCU-style read traversal only after leaf/node reclamation is designed.
- Keep structural changes under conservative locks until correctness is proven.

Priority:

Medium. Useful for multi-thread scaling, but easy to break correctness.

### D5 Landing-Buffer Policy

- Make landing-buffer size adaptive per codec and workload.
- Larger landing buffer improves QPS but reduces effective compression.
- Smaller landing buffer improves memory savings but increases foreground/background compression pressure.
- Track average/max landing occupancy in stats.

Priority:

Medium. Needed to present throughput vs memory-saving trade-off clearly.

### D6 Stats Fast Path

- Rebuild correct incremental stats.
- Keep `bplus_tree_compressed_calculate_stats()` as exact validation.
- Add tests comparing incremental stats to exact leaf walk after split/delete/reinsert/background compaction.

Priority:

Medium. Current exact stats are correct but expensive.

### D7 Variable-Length KV Layout

- Replace fixed `int key_t` and `payload[COMPRESSED_VALUE_BYTES]` with arbitrary byte keys and values.
- Introduce a slotted leaf-page format.
- Preserve subpage hashing and partial decompression.
- Revisit split/range/delete semantics after layout change.

Priority:

Medium-low for immediate IAA evaluation, high for final ZipCache API fidelity.

### D8 Production Range Scan Semantics

- Audit range scan behavior under hashed subpages.
- Ensure returned values are strictly within requested range.
- Add concurrent range-scan correctness tests with deletes/reinserts.

Priority:

Medium-low for IAA point-operation promotion, higher for full B+Tree feature completeness.

## Required Correctness Tests

Run after each phase:

```bash
cmake --build DRAM-tier/build_check -j$(nproc)

DRAM-tier/build_check/bin/bpt_compressed_lz4_smoke
DRAM-tier/build_check/bin/bpt_compressed_qpl_smoke
DRAM-tier/build_check/bin/bpt_compressed_zlib_accel_smoke
DRAM-tier/build_check/bin/bpt_compressed_crud_fuzz
DRAM-tier/build_check/bin/test_compression_concurrency
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
DRAM-tier/build_check/bin/bpt_compressed_split_payload_stats
DRAM-tier/build_check/bin/bpt_compressed_throughput_bench
```

Run point-operation concurrency without scans:

```bash
BTREE_READ_PCT=55 \
BTREE_WRITE_PCT=35 \
BTREE_DELETE_PCT=10 \
BTREE_SCAN_PCT=0 \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

Landing-buffer sweep:

```bash
LANDING_LIST='256 512 1024 2048' \
SHARDS_LIST='1 8' \
THREADS_LIST='8 16 32' \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
BTREE_DURATION_SEC=2 \
BENCH_DURATION_SEC=2 \
DRAM-tier/tests/btree/run_scaling_sweep.sh
```

Run QPL software and hardware modes:

```bash
BTREE_QPL_PATH=software BTREE_QPL_MODE=fixed \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency

BTREE_QPL_PATH=hardware BTREE_QPL_MODE=fixed \
DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
```

The hardware command should fail fast if IAA is not configured. It should not silently fall back to software in hardware-only mode.

## Required Performance Tests

Mixed throughput sweep:

```bash
for t in 1 2 4 8 16 32; do
  BENCH_DURATION_SEC=5 \
  BENCH_WARMUP_KEYS=10000 \
  BENCH_KEY_SPACE=50000 \
  BENCH_THREADS=$t \
  DRAM-tier/build_check/bin/bpt_compressed_throughput_bench
done
```

Read-only isolation:

```bash
for t in 1 2 4 8 16 32; do
  BTREE_DURATION_SEC=2 \
  BTREE_KEY_SPACE=50000 \
  BTREE_HOT_PCT=100 \
  BTREE_READ_PCT=100 \
  BTREE_WRITE_PCT=0 \
  BTREE_DELETE_PCT=0 \
  BTREE_SCAN_PCT=0 \
  BTREE_THREADS=$t \
  DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
done
```

Write-heavy isolation:

```bash
for t in 1 2 4 8 16 32; do
  BTREE_DURATION_SEC=2 \
  BTREE_KEY_SPACE=50000 \
  BTREE_HOT_PCT=100 \
  BTREE_READ_PCT=20 \
  BTREE_WRITE_PCT=70 \
  BTREE_DELETE_PCT=10 \
  BTREE_SCAN_PCT=0 \
  BTREE_THREADS=$t \
  DRAM-tier/build_check/bin/bpt_compressed_mixed_concurrency
done
```

## Notes for Future Agents

- Do not remove the landing buffer. It is central to ZipCache's design and benefits all three codec paths.
- Do not make QPL hardware mode silently fall back; it hides IAA queue/submission issues.
- Be careful with `get_range`: range behavior has historically had filtering/semantics issues. Keep range correctness separate from point lookup correctness.
- Avoid adding a larger global lock to "fix" correctness. It may pass tests but will make the scaling problem worse.
- Prefer narrow changes with before/after QPS tables.
- Keep build artifacts and unrelated dirty files separate from source changes when preparing commits.
