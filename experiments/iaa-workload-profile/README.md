# ZipCache: bounded hot leaves and IAA host-CPU evaluation

This isolated snapshot compares CPU LZ4 with strict-hardware QPL in the same
small-KV B+Tree. It combines bounded whole-leaf RAW residency, cold-leaf updates,
four cross-leaf background slots, unchanged-block reuse and host-CPU profiling.
Existing library code and the older `experiments/iaa-async` experiment are unchanged.

The purpose is to determine whether the revised cache gains throughput or CPU
efficiency at acceptable GET/PUT tail latency, explain host CPU cost, and measure
real QPL compression separately from total memory cost. Local tests do not
establish IAA performance or a complete product result.

## Run on the IAA host

To look for a change in the throughput lead as the GET percentage rises,
run the four-leaf workload with the same CPU and NUMA placement for both codecs.
The earlier four-leaf hardware screen used CPU list 0-32 and memory node 0.
Use those settings on the same host to reproduce its endpoints:

```sh
export QPL_ROOT=/opt/qpl-1.9.0/qpl_build
python3 run_intel.py --cpu-list 0-32 --numa-node 0 \
  --reads $(seq 0 10 100) --out results-read-sweep
```

Supply any distinct integer percentages from 0 to 100. Each backend replays
the same one-million-request uniform samba trace at each point, with four
clients, four leaf slots and three paired runs in alternating backend order.
The sweep runs the checks and smoke once, then the unpaced performance pairs.
Return the top-level `summary.txt`. It shows median QPS including final drain,
the range of paired gains and the tested intervals where the observed leader
changes. Mixed signs are inconclusive. Refine only an interval of interest by
passing additional percentages and a fresh output directory. If every point
favors one backend, that is a valid result. To generate and verify just the
inputs on any host, use `--reads 80 85 90 95 --prepare-only`.

### Confirm the two sides of a change in the throughput lead

On the same host, keep the sweep output directory and run:

```sh
python3 run_intel.py --confirm-from results-read-sweep \
  --arrival-rate 80000 --out results-confirm
```

The command selects the two measured endpoints if the sweep found one change
in the lead. For multiple changes, choose a reported interval with
`--reads LOW HIGH`. If no change was resolved, return the sweep summary or
refine the percentages. CPU placement, memory node and trace checksums are
matched to the sweep. The rate is configurable and is the aggregate across
four clients. Use a rate both backends can deliver, and check delivered QPS.

The latency pass runs three pairs with alternating codec order. GET and PUT
p99 run from scheduled arrival to return, including time queued at the client.
A separate instrumented replay at the same offered rate measures RAW, LZ4 and
strict QPL with identical inputs and full byte checks. The report separates
cold stored-data ratio, tracked retained and peak cache bytes, and whole-process
RSS. RSS includes the test harness and its lifetime peak includes preload.
The cache ledger excludes unverified QPL-private allocations. A single
`results-confirm/summary.txt` contains both the original throughput comparison
and the confirmation results. On failure send the top-level summary from the
affected stage. The detailed JSON and logs remain in the run directories.

The older synchronous diagnostic at `DRAM-tier/tests/btree_iaa/intel_eval.py`
also accepts arbitrary integer percentages through its existing `--reads`
option. Compare results within each runner separately.

Requirements: Linux x86-64, QPL **1.9.0**, configured and accessible IAA devices,
CMake >=3.20, C/C++ compiler, Python >=3.9, pkg-config, LZ4/Zstd/zlib development
libraries, unzip and numactl. Static QPL linking includes the C++ runtime.
Set `QPL_ROOT` to an existing nonstandard install prefix if needed, and the
loader library path for shared QPL if required. No driver, device or WQ settings
are changed by these scripts.

Choose the same explicit CPU mask and IAA-local memory node for all runs. Inspect
`lscpu`, `numactl --hardware` and `accel-config list` to select them. The mask
applies to the whole process, including the background coordinator. Set
`ZIPCACHE_CPU_LIST` and `ZIPCACHE_NUMA_NODE` to those values, then run from here:

```sh
python3 run_intel.py \
  --cpu-list "$ZIPCACHE_CPU_LIST" --numa-node "$ZIPCACHE_NUMA_NODE" \
  --out results-20261102
```

Use a new output directory each time. This single command:

1. Generates the fixed traces and verifies them against the reviewed checksums.
2. Builds the experiment and runs CTest. Skipped QPL tests are not passes.
3. Runs a 10,000-request LZ4/IAA smoke after strict software/hardware
   cross-decoding preflight. A missing library, wrong version or failed device
   stops the run; no silent software fallback is permitted.
4. Runs the three-point performance screen, the separate CPU profile and the
   separate RAW/LZ4/QPL memory audit.
5. Combines the results into **`results-20261102/summary.txt`**.

The check stage uses the same CPU/NUMA binding as the workloads; QPL jobs must
not migrate to a node without accessible userspace IAA workqueues. This worktree
includes local check-stage corrections documented in [VALIDATION.md](VALIDATION.md)
and no longer matches the original published source checksums.

Please return that summary.txt and run-status.json. On failure, also return the
named stage log. Detailed results.json, machine.json and build-provenance.json
are retained in the stage directories for follow-up.

If permitted perf is available, add `--perf`. It affects only the diagnostic
profile and records CPU-clock stacks from request start through final drain,
excluding preload and final verification. A perf error is explicit; there is no
automatic permission change. DWARF recording explicitly requests the standard
x86-64 integer, instruction-pointer, flags and CS/SS registers to avoid
unsupported bits in vendor perf defaults. It remains process-scoped, not
system-wide.

## Fixed comparisons

All points use 150,000 keys, complete 128-byte samba payloads, eight shards, four
clients, seed 11 and one million logical requests. Performance alternates backend
order over three repetitions. The CPU profile uses one diagnostic repetition.

| Point | Zipf alpha | GET / PUT | Popularity mapping | Question |
|---|---:|---:|---|---|
| `uniform-r80` | 0 | 80% / 20% | Uniform keys | Does maintenance throughput improve without an excessive GET tail? |
| `clustered-r20` | 1.1 | 20% / 80% | Popularity rank follows key order | Does offloading remaining maintenance help with bounded hot leaves? |
| `uniform-r95` | 0 | 95% / 5% | Uniform keys | What is the cost or benefit of foreground hardware decode? |

Payload mapping is adjacent samba slices in every point. Updates select another
real slice using the fixed version rule. These access orders are synthetic, not
a production application trace. Single-writer key ownership preserves exact
per-key histories but can produce unequal client lengths; manifests record them.

The logical leaf is 16 KiB with up to four independent approximately 4 KiB blocks.
Hot RAW leaf allocations have a global 2 MiB ceiling; the delta budget is separate
at 2,040,000 bytes. Cold leaves use three active updates and at most one pending
task. One coordinator owns four cross-leaf jobs. These limits are identical for
the compared backends; they are not a hard total DRAM cap.

QPL compression, foreground decode and maintenance decode all use strict hardware
with dynamic Huffman compression. Foreground GET remains synchronous. This
screen does not evaluate hardware-compression / CPU-decompression hybrid routing,
despite the branch name.

## Read the results

- Performance: useful QPS including final drain, whole-process CPU/op, GET/PUT
  p50/p99/p999, foreground return cost, backend/path/version, jobs/bytes, failures,
  hot hits, lock-held codec work, waits and fallbacks. CRC and per-read oracle are
  off during performance; full final payload verification is outside timing.
- CPU profile: OS thread CPU and exclusive stage CPU for submit, completion
  checks, waiting, merge, commit, preparation and hot policy. QPL execute is
  inclusive; separating library setup from polling requires native perf stacks.
  First-use client TLS initialization is included and counted. Diagnostic timing
  must not replace normal performance numbers.
- Memory audit: CRC and per-read verification enabled; current cold-data and
  framed compression ratios; actual packed-RAW comparison; metadata, buffers,
  allocator-usable bytes, visible codec workspaces, execution stacks and observed
  peak allocations. RSS is separate and includes harness/runtime costs.
- **QPL-private allocations remain unverified.** `tracked_saved_*` is a partial
  ledger comparison. `complete_saved_*` and capacity qualification remain
  unavailable for QPL. Do not call tracked savings whole-system savings.
- A run is valid only when run-status.json is complete and all row checks pass.
  Exit-zero but malformed results, trace mismatches, incomplete tasks, wrong
  routes and exceeded budgets are failures. Unknown fields are not zero.

The software outstanding count is not hardware occupancy. Finite closed-loop
results are not maximum sustainable QPS or matched-arrival-rate tail latency.
The one-coordinator LZ4 path is a controlled snapshot baseline, not a search for
the best multicore CPU implementation. Hot on/off, old/new architecture,
synchronous QPL, hybrid routing, complete memory and sustained-load comparisons
remain separate follow-up work. See [VALIDATION.md](VALIDATION.md).

## Local checks and individual stages

```sh
python3 run_intel.py --prepare-only
cmake -S source/DRAM-tier/tests/btree_iaa -B build \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo -DAGG_DISABLE_QPL=ON
cmake --build build -j4
python3 run.py check --build build --out results-check
python3 test_handoff.py
python3 test_entrypoint.py
```

For individual hardware stages use `run.py intel`, `run.py intel-profile` or
`run.py intel-audit`, each with `--build build-intel --cpu-list ... --numa-node ...
--out NEW_DIRECTORY`. Their defaults select the three points above. The local
`screen`, `diagnose` and `audit` actions retain the reviewed software behavior.

`data/samba.zip` reuses the identical corpus blob already in this repository.
No new official-source verification is claimed. `expected-inputs.json` freezes
the original trace manifests; `source-sha256.json` binds the 97 originally published source
files; current deviations are documented above. Only the public entrypoint,
trace preparation and test-fixture paths were adapted for Git distribution.
Private correspondence, full historical logs,
local binaries and recordings are excluded.
