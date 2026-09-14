# ZipCache: Intel IAA evaluation

Branch: `wip/iaa-hybrid-evaluation`. This is an experimental evaluation branch,
not a production release or a claim of verified IAA acceleration.

## Goal

Determine whether the revised small-KV B+Tree supplies useful work to IAA and
whether offload improves throughput or process CPU cost without unacceptable
GET/PUT tail latency or memory cost. Device utilization alone is not success.

Changes include compact leaf metadata, exact-size compressed allocations,
readable pending updates during background compaction, full 128-byte payload
GET/verification, and codec/queue/memory instrumentation. Production background
compaction remains opt-in; the experiment enables it explicitly.

## 1. Get the source and fixed corpus

```sh
git clone --depth 1 --branch wip/iaa-hybrid-evaluation https://github.com/Mi5sssss/ZipCache.git ZipCache-iaa
cd ZipCache-iaa
git submodule update --init SilesiaCorpus
git rev-parse HEAD
git submodule status SilesiaCorpus
```

The corpus submodule is pinned to `3f3fa2cdbbb3795c903b74e774acb309e1360337`.
The runner uses the complete `samba.zip` member, with real 128-byte slices and
recorded source/trace checksums. Arrival order and KV boundaries are synthetic,
not a production AI/agent workload.

## 2. Prerequisites and host selection

- Linux x86-64; CMake >=3.20; C/C++ toolchain; Python >=3.9; pkg-config; unzip.
- Development libraries for LZ4, Zstd and zlib. On Debian/Ubuntu these normally
  correspond to `liblz4-dev`, `libzstd-dev`, and `zlib1g-dev`.
- `taskset`, `numactl`, and `ldd`.
- Real QPL **v1.9.0**, source commit
  `1813ccedb90b6468d47cc4ea87ae8b754a9151e1`, installed in a private prefix
  containing `include/qpl/qpl.h` and `lib/libqpl.so` or `lib64/libqpl.so`.
- An already configured, accessible IAA work queue. The scripts do not use sudo,
  install packages, configure work queues, or change system settings.

Use the [official QPL installation instructions](https://intel.github.io/qpl/documentation/get_started_docs/installation.html)
for compiler, assembler and Linux dependencies. If QPL is not installed, after
those prerequisites are available it can be built locally as follows:

```sh
git clone --depth 1 --branch v1.9.0 https://github.com/intel/qpl.git qpl-source
test "$(git -C qpl-source rev-parse HEAD)" = 1813ccedb90b6468d47cc4ea87ae8b754a9151e1
cmake -S qpl-source -B qpl-build -DCMAKE_BUILD_TYPE=Release \
  -DQPL_BUILD_TESTS=OFF -DQPL_BUILD_EXAMPLES=OFF \
  -DCMAKE_INSTALL_PREFIX="$PWD/qpl-install"
cmake --build qpl-build -j4
cmake --install qpl-build
```

Choose **two distinct physical cores**, represented by two logical CPU IDs, on
the same IAA-local NUMA node for the first comparison. Do not select two SMT
siblings as a two-core budget. The runner also accepts eight explicit IDs for
a subsequent eight-core comparison. It enforces affinity for the entire
benchmark, including its background worker.

Inspect the existing host configuration without changing it:

```sh
lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE
numactl --hardware
accel-config list
```

The examples below use CPUs `2,4`, NUMA node `0`, and QPL prefix
`/opt/qpl-1.9.0`. **Replace these with verified host values**; if using the local
build above, replace the prefix with `"$PWD/qpl-install"`. Memory binding does
not select a specific IAA work queue. Keep the host load and CPU topology
identical across comparisons. A system-wide compression interposer is rejected.

## 3. Run the hardware smoke test first

From the repository root:

```sh
python3 DRAM-tier/tests/btree_iaa/intel_eval.py \
  --out intel-runs/smoke --profile smoke \
  --qpl-root /opt/qpl-1.9.0 --cpu-list 2,4 --numa-node 0
```

One command configures a fresh Release build, runs CTest and strict QPL
software/hardware cross-decoding, generates fixed traces, executes comparisons,
and writes the return archive. Missing hardware/library, skipped required QPL
tests, wrong payloads, or failed jobs fail the run rather than silently falling
back. Smoke uses 1,024 keys and 256 operations per client; it is not performance
evidence. If it fails, return its archive before attempting the screen.

## 4. Run the focused pre-meeting screen

```sh
python3 DRAM-tier/tests/btree_iaa/intel_eval.py \
  --out intel-runs/screen --profile screen \
  --qpl-root /opt/qpl-1.9.0 --cpu-list 2,4 --numa-node 0 \
  --clients 1 4 --reads 80
```

This uses 150k keys, eight shards, 128-byte payloads, 100k operations per client,
seed 11, random point reads/updates, and both adjacent and permuted initial
key-to-payload mappings. Mappings are not sequential/random access modes;
updates replace the whole payload using a version-dependent source slice.
Shuffled client ownership shares shards/leaves, but each key has one client
owner. Same-key overlapping correctness histories are separate tests.

The initial layout is a **16 KiB logical leaf with up to four independent
4 KiB compression blocks**. In raw JSON/CLI this is layout `3`, not a CPU cache
level. Layout `2` is **16 KiB whole-leaf compression**; add `--layouts 3 2` for
that additional layout control, but do not change layout when attributing a
CPU/IAA execution difference.

If time permits, run separate read-only and write-heavy cases:

```sh
python3 DRAM-tier/tests/btree_iaa/intel_eval.py \
  --out intel-runs/read-write-controls --profile screen \
  --qpl-root /opt/qpl-1.9.0 --cpu-list 2,4 --numa-node 0 \
  --clients 1 4 --reads 100 20
```

The existing runner filename `intel_hybrid.py` remains a compatible alias. Both
entry points execute the same code, checks and matrix:

| Result label | Compression | GET decompression | Maintenance decompression |
|---|---|---|---|
| E-raw / E-lz4 / E-zstd / E-zlib | CPU reference | Matching representation | Matching representation |
| A | QPL software | QPL software | QPL software |
| D | Strict IAA | Strict IAA | Strict IAA |
| B | Strict IAA | QPL software | QPL software |
| C | Strict IAA | Strict IAA | QPL software |

Prioritize D versus A and the best measured CPU reference. B/C and the separate
byte-identical GET comparison diagnose where costs arise; they do not prescribe
a final deployment policy. Software and hardware QPL paths are cross-decoded;
LZ4 is not a decoder for QPL Deflate bytes. The optional zlib-accel shim is not
loaded into these CPU/strict-QPL comparisons.

Each matrix configuration has separate correctness/memory-audit and performance
processes. Performance returns full payloads and counts background drain, but
does not time a per-read value oracle. Final data are verified after timing.
Add `--dry-run` to inspect the command plan without building or executing.
Output directories must be new; previous results are never overwritten.

## 5. Return the results

Send the generated `intel-results.tar.gz` from each output directory, including
failed smoke runs. For the required commands these are:

```text
intel-runs/smoke/intel-results.tar.gz
intel-runs/screen/intel-results.tar.gz
```

The archive includes `SUMMARY.md`, `metrics.csv`, complete JSON rows, source and
trace checksums, environment information, and logs. Metrics include QPS through
final drain, whole-process CPU/op, GET/PUT p50/p99/p999, owned steady/peak memory,
RSS, codec calls/bytes by origin, pending/lock waits, software queue observations,
strict hardware calls/completions, busy/failures and synchronous fallbacks.

Please also attach available IAA/WQ telemetry and a CPU profile, with collection
times and the existing NUMA/WQ configuration. Device occupancy is **not** inferred
from software queue depth. QPL call duration includes submission, wait and
service; it is not isolated hardware service latency. Recorded process timestamps
include preload and validation, not only the timed window. Return machine details
privately; do not commit result archives or host telemetry to this public branch.

## Interpretation boundaries

- The 16 KiB candidates still execute QPL synchronously inside one root-owned
  background worker. They do not have a continuously refilled asynchronous
  executor. This screen diagnoses the current implementation, not IAA peak
  capacity or a causal before/after proof for the old batcher.
- Point reads are random; a sequential workload is not implemented in this
  entry point. Read/write proportions and concurrency are independent strata.
- One-seed, unpaced screens are not sustained-QPS, p99 SLO, or product acceptance.
  `--profile qualification` requires positive common `--rates`, `--get-slo-us`
  and `--put-slo-us`, then uses three seeds and three repetitions. Select those
  rates/SLOs from a CPU pilot before judging candidates, not retrospectively.
- Memory accounting still needs hardware-private QPL allocations reconciled.
  Product acceptance remains pending even when a screen succeeds.
- The joint target remains zero correctness errors, >=1.25x steady and peak
  capacity, >=15% throughput improvement or >=20% CPU/op reduction, and GET/PUT
  p99 <=1.10x versus the matched QPL reference and best measured CPU scheme.
- This is not an eviction-enabled, fixed-DRAM application or a real AI workload.

For a software-only local release check, with the corpus initialized:

```sh
python3 DRAM-tier/tests/btree_iaa/intel_eval.py \
  --out intel-runs/local-smoke --profile smoke --cpu-only
```

This explicitly disables QPL and cannot establish hardware performance.
