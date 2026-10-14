# WIP: small-KV B+Tree cross-leaf QPL evaluation

## Purpose and status

Test whether overlapping independent leaf maintenance jobs improves useful cache
performance without losing memory savings or foreground latency. This is not a
claim that IAA must beat LZ4 on a synchronous, small-block point read.

This branch adds an isolated experiment under `experiments/iaa-async/`. Existing
root-level library sources and older evaluation commands remain unchanged from
baseline `f2ef0b0b46c782c8d8bdf047884047d7659d7f8c`.

**Implemented:** actual QPL submit/check integration and versioned measurements.
**Locally tested:** CPU data paths, delayed-device contracts and resource bounds.
**Pending:** real QPL linking/execution, hardware performance and complete QPL
private/shared-library memory attribution. Missing hardware never counts as a pass.

The branch name is `wip/iaa-hybrid-evaluate`, but this revision isolates background
asynchrony. It does not yet evaluate IAA compression with CPU point-read decoding.

## Run from a fresh checkout

Requirements: Linux x86-64, configured and accessible IAA, C/C++ toolchain,
CMake >=3.20, Python >=3.9, pkg-config, unzip, LZ4/Zstd/zlib development libraries,
and real QPL **1.9.0**. Set `QPL_ROOT` to the installation prefix for a nonstandard
QPL installation. Static and shared linking are supported by the build setup;
actual target-machine linking is part of this qualification.

```sh
git clone --single-branch --branch wip/iaa-hybrid-evaluate \
  https://github.com/Mi5sssss/ZipCache.git ZipCache-iaa-evaluate
cd ZipCache-iaa-evaluate/experiments/iaa-async
python3 DRAM-tier/tests/btree_iaa/run_async.py check --out results-check
python3 DRAM-tier/tests/btree_iaa/run_async.py screen --out results-screen
```

No submodule download is needed for this experiment; the checked Samba archive is
included. `build_qpl.sh` is an optional explicit helper for building the pinned
static QPL release. See [the detailed README](experiments/iaa-async/README_ASYNC_QPL.md).
Neither the runner nor helper configures work queues or device permissions.

The unbound screen is diagnostic. For comparable CPU and latency results, select
a common CPU list in an IAA-local NUMA domain, then run:

```sh
python3 DRAM-tier/tests/btree_iaa/run_async.py screen \
  --cpu-list "$ZIPCACHE_CPU_LIST" --numa-node "$ZIPCACHE_NUMA_NODE" \
  --layouts 3 --clients 1 4 --windows 1 4 --out results-local-numa
```

Set those two variables to the chosen CPU IDs and NUMA node first. Affinity covers
the whole process, including the coordinator. Use a fresh output directory for
each command. Return `summary.txt` first; `results.json`, `manifest.json`, resource
logs, format checks and any failure logs provide details when needed. Nothing
uploads results automatically.

## What the experiment changes

| Area | Implementation | What it can establish |
|---|---|---|
| Measurement | Actual block-size histograms; separate compression, GET decoding and maintenance decoding work | Whether tasks are really near 2 KiB or 4 KiB, and where codec work occurs |
| Memory | Encoded bytes, framing, metadata, active/pending, allocator rounding, scratch and QPL job allocations | Representation ratio versus audited owned-memory capacity; QPL private/shared costs remain partial |
| Background execution | One coordinator, 1..8 independent leaf slots, reusable jobs, `qpl_submit_job` / `qpl_check_job`, strict hardware | Whether cross-leaf overlap removes serial waiting at acceptable CPU/memory cost |
| Unchanged-block reuse | Decode/validate the old leaf; merge; encode changed blocks; copy unchanged encoded bytes; atomically install the whole result | Benefit from avoiding redundant compression, tested independently of scheduling |
| Failure handling | Busy retry, bounded credits, accepted-buffer lifetime, terminal accounting and safe split fallback | Correctness and resource safety under pressure; real DMA behavior still requires hardware |

Each leaf still obeys decode -> merge -> encode -> commit. Different leaves can
advance independently without a whole-batch completion barrier. Frontend GET
retains its existing synchronous path. There is no new hash index, frontend
scheduler, decompression cache, extra pending buffer or adaptive codec routing.

The layouts are:

- **Whole-leaf compression (`--layouts 2`):** a 16 KiB logical leaf encoded as one
  valid-record compression unit.
- **Independent-block compression (`--layouts 3`):** a 16 KiB logical leaf with up
  to four independently encoded blocks, each at most 4 KiB of valid records.

The record is 136 bytes including its 128-byte payload, key and compatibility
integer. A 4 KiB block holds at most 30 records (4080 bytes). Partial occupancy
and balanced partitioning make actual input smaller; 4 KiB is not a promised size.

## Small initial comparison

All cases replay the same Samba payloads and logical trace. Defaults: 150k keys,
8 shards, 80% GET / 20% PUT, seed 11, clients 1/4, both adjacent and permuted
key-to-payload mappings. These mappings are not sequential/random access order.

| Comparison | Hold constant | Question |
|---|---|---|
| Serial QPL hardware vs cross-leaf windows 1/4 | Layout, Huffman mode, reuse setting, trace and CPU budget | Does asynchronous overlap help? |
| Block reuse off/on | Execution mode and window | Does avoiding redundant compression help? |
| QPL hardware vs QPL software | Layout, trace and CPU budget | What does offload contribute relative to the software codec path? |
| Best hardware candidate vs LZ4, Zstd and zlib | Workload and total resource budget | Is hardware competitive with the best measured CPU choice? |

Default layout is independent-block compression; default Huffman mode is dynamic.
Only after the initial screen, add whole-leaf compression (`--layouts 2 3`), fixed
Huffman (`--huffman fixed`), read-only (`--reads 100`) or write-heavy (`--reads 20`)
controls. Access order can separately use `--order sequential` or `random`.
Increasing keys within each client does not imply globally sequential concurrent
execution.

Measure QPS, full-process CPU/op, GET/PUT p99, final drain, block sizes, encoded and
owned-memory sizes, accepted/terminal/failed jobs, busy, submit/poll costs and job
workspace. Software in-flight counts are **not hardware WQ occupancy**. Cumulative
codec wall time is **not CPU time**.

## Verification already completed

The source snapshot passed 96 aggregation/API/measurement tests and 28 delayed
device tests. Ten QPL-dependent tests were skipped, not passed. UBSan passed;
ASan/TSan were unavailable because trivial programs failed before main in the
local toolchain/runtime. The actual QPL adapter was syntax-checked against the
original QPL 1.9.0 public headers, not executed on QPL hardware.

The local 150k-key software screen covered both layouts and mappings in 48 audit
and performance processes, with 9.6 million operations and 772,065 accepted
cross-leaf jobs. It found no payload mismatch, failed job, unmatched accepted
task or audited owned allocation remaining after teardown. These short software
screens do not establish hardware speedup, capacity acceptance or saturation.

The bundled `SOURCE_SHA256.json` preserves the 78 input hashes of the previously
validated r2 delivery. Source archive SHA-256:
`2a4a42f25f78470111895cfe899edc81dd2732c1c4e55ab75851c51f82a6e45d`.
The large software screen preceded a final COPY short-output failure guard; that
guard did not alter its measured LZ4/zlib paths. The final source was separately
rebuilt and tested. Branch navigation and ignore files are outside that manifest.

## Decision after hardware results

First establish strict-hardware execution, correct output, bounded resources and
successful drain. Then compare serial versus cross-leaf execution before changing
foreground GET. If GET still dominates, evaluate CPU decoding of the same QPL
bytes as a separate follow-up, not as an unexplained simultaneous change.

Do not expand concurrency merely to raise device activity. Expand only if ready
work is waiting to submit and memory/tail latency remain acceptable. Same-leaf
dependencies, polling CPU and structural operations can still limit progress.

The short default screen is not final qualification. Promising configurations
need three seeds, three repeats, >=30-second windows and >=10 dataset-equivalent
update rounds under common CPU/NUMA budgets. Product gates remain: complete
steady and peak capacity >=1.25x versus actual RAW; QPS +15% or CPU/op -20%; GET
and PUT p99 <=1.10x of the comparison. Report both synchronous QPL and the best
CPU baseline. Unresolved QPL memory or noisy thresholds remain pending/inconclusive.

Fixed-DRAM end-to-end cache/backing-store benefit, eviction and application
integration are not implemented here. Compression ratio alone does not prove
fewer misses, less I/O, or a product advantage.
