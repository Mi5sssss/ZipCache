# Real B+Tree Tests

These tests exercise the actual B+Tree or compressed B+Tree APIs.

## Current coverage

- `bplustree_test.c`: original uncompressed B+Tree get/put/delete behavior and structural dump checks.
- `bplustree_coverage.c`: coverage-oriented original B+Tree workload.
- `bplustree_demo.c`: interactive/demo original B+Tree target.
- `compressed_lz4_smoke.c`: compressed B+Tree smoke test using the LZ4 backend.
- `compressed_qpl_smoke.c`: compressed B+Tree smoke test using the QPL backend, with fallback behavior if QPL is unavailable.
- `compressed_zlib_accel_smoke.c`: compressed B+Tree smoke test using the zlib API backend; `LD_PRELOAD` can route this through Intel zlib-accel.
- `compressed_crud_fuzz.c`: randomized compressed B+Tree CRUD and range membership checks for LZ4/QPL/zlib-accel.
- `compressed_synthetic_test.c`: synthetic payload scenarios for compressed B+Tree behavior.
- `compressed_tail_latency.c`: end-to-end compressed B+Tree latency benchmark, including tree traversal and leaf compression/decompression for LZ4/QPL/zlib-accel.
- `compressed_dcperf_workload.c`: compressed B+Tree workload benchmark inspired by DCPerf-style distributions for LZ4/QPL/zlib-accel.
- `test_compression_concurrency.c`: compressed B+Tree single-thread and multi-thread correctness checks for LZ4/QPL/zlib-accel.
- `compressed_mixed_concurrency.c`: real B+Tree mixed read/write/delete/scan stress test; target name is `bpt_compressed_mixed_concurrency`.
- `compressed_split_payload_stats.c`: split, inline payload, delete/reinsert, range membership, and stats consistency stress test; target name is `bpt_compressed_split_payload_stats`.
- `compressed_throughput_bench.c`: per-operation and mixed workload throughput benchmark across LZ4/QPL/zlib-accel.
- `run_scaling_sweep.sh`: reproducible scaling harness for read-only, read-heavy, point-mixed, mixed-with-scan, write-heavy, and benchmark-mixed cases.

## Runtime controls

- QPL path: `BTREE_QPL_PATH=auto|software|hardware`. Use `auto` for normal runs so the QPL runtime auto-detects the execution path. `software` and `hardware` are for forced-path debugging or strict diagnostics.
- QPL Huffman mode: `BTREE_QPL_MODE=fixed|dynamic`.
- zlib-accel: use `COMPRESS_ZLIB_ACCEL` tests as the zlib API baseline; enable Intel zlib-accel with `LD_PRELOAD=/path/to/libzlib_accel.so`.
- Concurrency smoke: `BTREE_THREADS` and `BTREE_KEYS_PER_THREAD` control `test_compression_concurrency`.
- Mixed workload: `BTREE_THREADS`, `BTREE_DURATION_SEC`, `BTREE_KEY_SPACE`, `BTREE_HOT_PCT`, `BTREE_READ_PCT`, `BTREE_WRITE_PCT`, `BTREE_DELETE_PCT`, and `BTREE_SCAN_PCT` control `bpt_compressed_mixed_concurrency`.
- Fixed-width real value payloads: `BTREE_USE_SILESIA=1` loads `SilesiaCorpus/samba.zip`, extracts `samba`, and maps continuous integer keys to continuous value slices. `BTREE_VALUE_BYTES=128` is the default payload size for sweep runs. Set `SAMBA_ZIP_PATH` or `SILESIA_CORPUS_DIR` if running outside the source tree.
- QPL fallback pool size: `BTREE_QPL_POOL_SIZE` controls the retained global QPL job pool cap. The main QPL path now prefers thread-local jobs.
- Optional sharding: `BTREE_SHARDS=N` creates `N` independent compressed B+Tree shards behind one coordinator object. The default is `1`, preserving the original single-tree behavior.
- Reference oracle lock striping: `BTREE_REF_LOCKS=N` controls the striped mutex count used by `bpt_compressed_mixed_concurrency`. The default is `1024`.
- Experimental out-of-lock rebuild: `BTREE_OUT_OF_LOCK_REBUILD=1` enables a leaf rebuild prototype. It is default-off because the current single-tree split path can still lose keys under concurrent inserts. Use only for sharded experiments until the structural race is fixed.
- Small range scan point lookup: `BTREE_RANGE_POINT_LOOKUP_THRESHOLD=N` routes ranges with width `<=N` through descending point lookups instead of holding the tree read lock while walking leaves. The default is `256`; set `0` to force the legacy leaf-walk range path.
- Landing buffer tuning: `BTREE_LANDING_BUFFER_BYTES=N` controls the effective per-leaf software landing buffer. The default is `512`, the current max allocation is `2048`, and values are rounded down to `struct kv_pair` slot boundaries.
- Scaling sweep: `BTREE_USE_SILESIA`, `BTREE_VALUE_BYTES`, `LANDING_LIST`, `SHARDS_LIST`, `THREADS_LIST`, `BIN_DIR`, `RESULTS_DIR`, `BTREE_DURATION_SEC`, `BTREE_KEY_SPACE`, `BTREE_HOT_PCT`, `BENCH_DURATION_SEC`, `BENCH_WARMUP_KEYS`, and `BENCH_KEY_SPACE` control `run_scaling_sweep.sh`. The generated `summary.tsv` includes `value_source`, `value_bytes`, `ratio`, `saved_pct`, `compressed_bytes`, and `total_bytes`.

## Sharding status

`BTREE_SHARDS` is a correctness-preserving concurrency optimization for point-heavy workloads. The current implementation uses hash sharding by key and routes `put/get/delete` directly to the owning shard, which removes a large amount of global tree-lock contention.

Range scan support is intentionally conservative: the sharded coordinator currently scans from `max_key` down to `min_key` and performs point lookups on the owning shard. This preserves the existing "return the last resident value in the requested key range" semantics used by the tests, but it is not the final range-scan performance design. A production range-scan optimized version should use range-partitioned shards or per-shard range iterators plus merge logic.

For non-sharded trees, small ranges now use the same descending point-lookup strategy by default. This avoids holding the global tree read lock during decompression-heavy range scans. Large ranges still use the legacy leaf walk unless the threshold is raised.

## Landing buffer status

Each compressed leaf now allocates a 2KB maximum landing buffer, while the effective usable size defaults to 512B and can be tuned with `BTREE_LANDING_BUFFER_BYTES`. The inline value payload width is currently 128B, so a 4KB leaf holds far fewer KV pairs than the earlier 64B payload setup. The landing buffer is shared by LZ4, QPL, and zlib-accel; it is not an IAA work queue. It is a ZipCache software write buffer that reduces recompression frequency, reduces QPL/IAA submission count on write-heavy paths, and lets recent writes be served before touching compressed subpages.

Short 32-thread sharded write-heavy tests showed larger landing buffers can substantially improve write-heavy QPS, especially for QPL/zlib-style codecs. Treat this as a tuning result, not a final default change, because larger landing buffers increase uncompressed per-leaf memory and can increase scan/read work.

The main B+Tree benchmarks now report effective whole-tree memory compression:

- `ratio = total_bytes / compressed_bytes`.
- `saved_pct = (1 - compressed_bytes / total_bytes) * 100`.
- `total_bytes` counts resident KV payload bytes in compressed subpages plus live landing-buffer entries.
- `compressed_bytes` counts actual compressed subpage bytes plus live landing-buffer entries, because landing entries are stored uncompressed.

This means larger landing buffers should show the throughput/compressibility trade-off directly: QPS may improve while `saved_pct` can fall if more data remains in landing buffers.

The current scaling sweep defaults to real Silesia samba values:

```bash
BTREE_USE_SILESIA=1
BTREE_VALUE_BYTES=128
```

Set `BTREE_USE_SILESIA=0` to return to synthetic integer payloads.

## Key/value format limitation

The current compressed B+Tree tests do not yet prove arbitrary ZipCache key/value support. The implementation still uses a fixed integer key and fixed inline payload slot:

```c
typedef int key_t;

struct kv_pair {
    key_t key;
    int stored_value;
    uint8_t payload[COMPRESSED_VALUE_BYTES];
};
```

Current consequences:

- Arbitrary key bytes are not supported by the B+Tree comparator, internal separator keys, sharding router, or range-scan path.
- `put_with_payload()` stores at most `COMPRESSED_VALUE_BYTES`; larger values are truncated.
- `get()` returns the compatibility `int stored_value`, not arbitrary value bytes.
- Landing-buffer capacity is still rounded to `struct kv_pair` slots, not actual key/value byte usage.

The Silesia mode is therefore a fixed-width real-data benchmark for compression and landing-buffer trade-offs. It is not the final variable-length ZipCache DRAM-tier API. The next design step is a variable-length byte-key/byte-value path with a slotted leaf-page format and new `put_kv/get_kv/delete_kv` style APIs, while keeping the integer API as a compatibility wrapper.

## Current known failures exposed by these tests

- `test_compression_concurrency` now passes the default 4-thread short run across LZ4/QPL/zlib-accel.
- `bpt_compressed_mixed_concurrency` now passes the default short mixed run and the 2026-04-14 scaling sweep with 0 mismatches.
- `BTREE_OUT_OF_LOCK_REBUILD=1` is experimental: it passes the short concurrency smoke with `BTREE_SHARDS=8`, but fails the single-tree concurrency smoke. Keep it disabled for baseline claims.
- `bpt_compressed_crud_fuzz` now passes across LZ4/QPL/zlib-accel after fixing duplicate-key stale values between compressed data and landing buffers.
- `bpt_compressed_split_payload_stats` now passes across LZ4/QPL/zlib-accel. `bplus_tree_compressed_stats()` currently returns exact leaf-walk stats rather than incomplete incremental counters.
- `compressed_synthetic_test.c` was already present and can fail on dense inserts around split pressure; keep it as a known stress signal rather than a smoke baseline.

## Scaling sweep

Short local run:

```bash
SHARDS_LIST='1 8' \
LANDING_LIST='512' \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
THREADS_LIST='1 8 16 32' \
BTREE_DURATION_SEC=1 \
BENCH_DURATION_SEC=1 \
DRAM-tier/tests/btree/run_scaling_sweep.sh
```

Longer run:

```bash
SHARDS_LIST='1 8' \
LANDING_LIST='256 512 1024 2048' \
BTREE_USE_SILESIA=1 \
BTREE_VALUE_BYTES=128 \
THREADS_LIST='1 2 4 8 16 32' \
BTREE_DURATION_SEC=2 \
BENCH_DURATION_SEC=5 \
DRAM-tier/tests/btree/run_scaling_sweep.sh
```
