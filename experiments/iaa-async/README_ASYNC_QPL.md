# ZipCache: bounded cross-leaf QPL qualification

## Status and purpose

This is an isolated research branch snapshot, not a production replacement.
The production checkout and previously supplied hardware baseline are unchanged.
The new real QPL adapter has been syntax-checked against the original QPL 1.9.0
headers. **It has not yet been linked or executed on an IAA machine.**

The question is whether removing cross-leaf serialization improves useful small-KV
performance after accounting for job memory, software polling, and tail latency.
Higher hardware utilization is not the acceptance criterion.

The package contains no correspondence, recordings, historical private reports,
machine credentials, or prebuilt Mac binaries. Nothing uploads results or changes
work queues, NUMA policy, CPU configuration, or device permissions.

## Run

Requirements: Linux x86-64 for QPL/IAA; C/C++ toolchain, CMake >=3.20, Python >=3.9,
pkg-config, unzip, LZ4, Zstd, zlib development packages, and real QPL **1.9.0**.
IAA must already be configured and accessible. Use the existing QPL installation;
both static and shared linkage are supported. If it is outside the default library
search path, set `QPL_ROOT` to its install prefix. Shared libraries may also need
the corresponding library directory on the loader search path.

From the extracted package root:

```sh
python3 DRAM-tier/tests/btree_iaa/run_async.py check --out results-check
python3 DRAM-tier/tests/btree_iaa/run_async.py screen --out results-screen
```

The first command configures, builds, and runs correctness tests. Skipped QPL tests
are explicitly reported, not accepted as hardware validation. The second command
requires strict-hardware software/hardware cross-decoding to succeed before it
runs any performance cases. Missing QPL or IAA stops this preflight.

For controlled measurements, choose a CPU list in an IAA-local NUMA domain first:

```sh
numactl --hardware
accel-config list
python3 DRAM-tier/tests/btree_iaa/run_async.py screen \
  --cpu-list "$ZIPCACHE_CPU_LIST" --numa-node "$ZIPCACHE_NUMA_NODE" \
  --layouts 3 --clients 1 4 --windows 1 4 --out results-local-numa
```

Set the two task-specific variables to the selected CPU IDs and NUMA node before
this command. They are intentionally not guessed. Affinity applies to the entire
process, including coordinator and client threads. The unbound default run is a
diagnostic screen, not a fair fixed-CPU-budget product comparison.

Return **summary.txt** first. If possible, also return `results.json`, `manifest.json`,
`resources-*.log`, `qpl-format.log`, and any failing case logs. No large attachment
is required to inspect the initial results. The runner preserves every failure.

### Small follow-ups, only after the default screen

- `--layouts 2 3`: whole 16 KiB leaf versus up to four independent 4 KiB blocks.
- `--huffman fixed`: fixed-Huffman comparison; default is dynamic.
- `--reads 100` or `--reads 20`: read-only and write-heavy controls.
- `--order sequential`: increasing keys within each client's assigned key set;
  concurrent interleaving is not globally sequential. Default is random access.
- `--windows 1 2 4 8`: up to one accepted background job per independent leaf slot.
- `--poll-us 1`: optional no-progress polling-backoff sensitivity, not a default.

Each command requires a fresh output directory. Adjacent/permuted describes the
**key-to-payload mapping**, not access order. Both mappings are run by default.
Client ownership is deterministically shuffled, with one owner per key, to share
leaves/shards without introducing an ambiguous correctness oracle.

## What changed

The same real B+Tree retains 128-byte payloads, three active updates, one pending
task per leaf, immutable encoded bases, and a single root-owned background worker.
The existing 16 KiB layouts and their atomic whole-leaf publication are unchanged.
No hash index, frontend queue, adaptive routing, or decompression cache was added.

```text
PUT -> active / pending -> bounded software queue
                              |
                     one coordinator, 1..8 leaf slots
                              |
       per leaf: snapshot -> decode -> merge -> encode -> atomic commit
                    |                     |
                    +---- reusable QPL job ----+
                          submit / check
                              |
                     strict hardware execution

GET -> existing short-lock snapshot -> existing synchronous codec path
```

The coordinator polls every live slot and advances whichever job completed.
It does not wait for an entire batch. Each slot owns one reusable QPL job and its
buffers; no QPL job is operated by two threads. Busy submission does not transfer
buffer ownership. Accepted jobs retain their buffers until a terminal status.

Jobs use strict `qpl_path_hardware`, default compression level, raw Deflate, and
the selected fixed/dynamic Huffman mode. Compression and maintenance decoding must
both use hardware in the new coordinator. Frontend GET remains the existing
synchronous path. This package deliberately does **not** evaluate hybrid routing.

The optional block-reuse treatment still decodes/validates the whole old leaf,
merges updates, compresses changed blocks, copies unaffected encoded blocks, and
installs the complete new frame atomically. It never skips correctness checks.

Variable compressed lengths, per-block RAW bypass, allocation-rounded whole-frame
RAW bypass, CRC/order checks, retries, failed pending, and structural split fallback
are preserved. Splits pause fresh background submissions and finish accepted jobs
before entering the existing structural path. Structural paths may still perform
synchronous codec work under their existing locks and are reported separately.

Default comparison cases:

| Case | Background execution | Unchanged-block reuse |
|---|---|---|
| RAW, LZ4, Zstd, zlib | Existing serial worker | On where applicable |
| QPL software | Existing serial worker, explicit software routes | On |
| QPL hardware serial | Existing serial worker, explicit hardware routes | Off/on |
| QPL hardware cross-leaf | One worker, reusable strict-hardware jobs, window 1/4 | Off/on |

Use serial versus cross-leaf with the **same reuse setting** to attribute scheduling.
Use reuse off versus on with the same execution window to attribute reduced work.
There is no automatic winner selection or claim of IAA saturation.

## Bounded resources and failure behavior

- Existing software queue: 32 leaf tasks; one pending per leaf.
- Window: 1..8, chosen explicitly. No preallocation for unused larger windows.
- `BTREE_CROSS_BYTES`: requested-byte reservation ceiling, 16 MiB in the runner.
  This is a ceiling, not an allocation; initialization rejects an unfitting window.
- `BTREE_CROSS_JOB_BYTES`: 1 MiB of accepted input-plus-output capacity. These are
  borrowed portions of already-owned buffers and must not be counted twice.
- Job sizes are queried from the real QPL library. Requested and allocator-usable
  bytes are reported, along with coordinator snapshots, scratch and descriptors.
- No `qpl_wait_job` inside the coordinator. A no-progress pass yields by default;
  polling CPU is included in process CPU/op. Optional sleep is explicitly recorded.
- No silent software fallback in hardware cases. Hard errors invalidate a run,
  even if the tree's one retry subsequently succeeds.
- There is no safe arbitrary cancellation of in-flight DMA. A stuck configuration
  is terminated by the runner's process watchdog and recorded as failed; buffers
  are not reclaimed while an accepted job is still outstanding.
- Callers must stop concurrent public operations before closing the tree.

## Measurements and interpretation

`MEMORY_V3` reports raw valid record bytes, encoded payload bytes, frame/descriptor
bytes, RAW blocks and actual raw-block size histograms. Histograms use 1 KiB buckets
with the final bucket above 16 KiB. A record is **136 bytes**, including the 128-byte
payload, key and compatibility integer. Thus a 4 KiB block holds at most 30 records
(4080 bytes), and balanced partially filled leaves often produce smaller blocks.

`CODEC_WORK` separately reports compression, foreground GET decoding and maintenance
decoding, including real input/output bytes and histograms. Reused blocks and RAW
copies do not become fictitious codec jobs. Existing origin counters distinguish
background from structural operations. Cumulative job wall time is not CPU time.

`CROSS_V3` reports accepted/terminal/failed jobs, busy, peak background jobs, submit
and poll cost, dependency-ready-to-accept wait, acceptance-to-observed-completion
wall time, workspace, QPL job bytes and borrowed-byte high water. The completion
interval includes polling delay; it is not hardware-only service time. Peak
background jobs does not include frontend jobs and is **not device WQ occupancy**.

`TIMING_V2` separates actual final drain from completion-detection delay, software
queue wait and serial-batch wait. Legacy JOBS/PHASES are not applicable to aggregate
layouts; do not use their zeros as evidence of no work.

The audit process accounts for owned tree/leaf/active/pending/base allocations,
allocator rounding, coordinator scratch, QPL job allocations, process-wide TLS
workspace, new/old overlapping representations, and reserved thread stacks.
Its high water measures simultaneous owned allocations, not a sum of category
peaks. Fixture, trace, oracle, measurement buffers and auditor overhead are excluded
from cache-owned capacity and remain visible in whole-process RSS.

**QPL private/shared-library memory is still an explicit open item.** The independent
resource probe samples process allocator/anonymous/RSS changes before init, after
init, after execution and after cleanup for 1/4/8 jobs. These process deltas are not
precise attribution. QPL's dispatcher and libaccel-config may retain shared state.
The runner therefore labels QPL capacity **partial**, never a capacity PASS.
Missing allocator introspection appears as zero in the diagnostic probe on unsupported
platforms; it is not evidence of zero allocation (the probe targets glibc >=2.33 Linux).

The base compression ratio is raw base record bytes / encoded block bytes, including
RAW blocks. Capacity is actual RAW-owned memory / compressed-owned memory, using
the least-memory RAW candidate among tested layouts. These are different numbers.
RSS is reported separately, not presented as per-object allocation.

Timing excludes preload and final full validation but includes background work and
drain. Audit verifies each returned payload; performance returns full payloads and
validates the final state outside timing. The manifest identifies synthetic access
order and the real Samba bytes; this is not a production AI/agent workload.

## Qualification gates

The default is a short screen (seed 11), not a >=30-second acceptance run. For
final candidates, select common trace lengths long enough for every case to run
>=30 seconds and >=10 dataset-equivalent update rounds, then use
`--seeds 11 29 47 --repeats 3`. All candidates replay the same trace; do not shorten
fast cases individually. Repeat order alternates. Zero errors and stable final drain
are mandatory. Compare within the same CPU budget, NUMA domain, Huffman mode,
mapping, layout and client count.

Product qualification still requires steady **and peak** complete capacity >=1.25x;
QPS +15% or CPU/op -20%; and GET and PUT p99 <=1.10x of the comparison. Report both
same-layout synchronous QPL and the best tested CPU codec. Unresolved memory,
missing hardware, short runs or unstable thresholds remain pending/inconclusive.

## Local verification

```sh
python3 DRAM-tier/tests/btree_iaa/run_async.py check --local --out local-check
python3 DRAM-tier/tests/btree_iaa/run_async.py screen --local \
  --keys 10000 --ops 10000 --clients 4 --out local-screen
cmake -S DRAM-tier/tests/async_device -B work/device -DCMAKE_BUILD_TYPE=Release
cmake --build work/device -j4
ctest --test-dir work/device --output-on-failure
```

CPU-contract cross cases use real LZ4/zlib bytes and complete synchronously inside
the adapter. They validate variable-size frames and lifecycle, not asynchronous CPU
speedup. The separate COPY surrogate validates late/overlapping/failed completion
and buffer lifetime. Neither predicts DMA, NUMA, hardware throughput or IAA p99.
`check --sanitizer address` and `check --sanitizer thread` build separate checks when
given separate build paths. On the local macOS 27 / AppleClang 17 environment, even
a trivial program failed before main under ASan/TSan; those results are unavailable,
not passed. UBSan and ordinary local results are recorded in the implementation report.

## Source and dependencies

QPL: https://github.com/intel/qpl, v1.9.0,
commit `1813ccedb90b6468d47cc4ea87ae8b754a9151e1`.
The `build_qpl.sh` helper is optional and only builds that pinned static library
under the package directory when explicitly invoked. It does not configure IAA.
Samba is bundled unmodified with its archive hash in the manifest; traces are
deterministically generated with their own checksums. Retain the included license
and source headers. `SOURCE_SHA256.json` inventories every packaged input.
