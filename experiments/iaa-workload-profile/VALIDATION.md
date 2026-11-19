# Local verification and hardware boundary

The original Git distribution preserved all 97 source files from the reviewed
2026-11-02 workload/profile snapshot. The existing runner, CPU profiler, layout,
hot/cold policy and backend execution strategy are unchanged. Distribution adds
one orchestration entrypoint, frozen-input verification and minimal public test
fixtures; it does not introduce a new scheduler or hybrid routing policy.

## Local corrections (2026-11-04)

The current worktree deviates from the published snapshot. Original
`source-sha256.json` and `PUBLICATION_SHA256.json` remain unchanged; new results
record the actual source hashes and build provenance.

- The Intel entrypoint selects its active Python interpreter and passes CPU/NUMA
  binding to the check stage. Bound checks enable hardware cross-decoding with
  the bundled corpus. Unbound local checks retain their previous behavior.
- Real-QPL cross-fuzz tests request hardware routing, dynamic Huffman and a
  16 MiB cross-job reservation. This test-only reservation does not change the
  performance or audit budgets. CPU-only builds retain their existing fallback.
- The test-only rebuild equivalence helper verifies both decoded payloads.
  Byte-identical output is not required because allocator-usable rounding can
  legitimately choose RAW in only one of the two encodings.
- Strict legacy QPL encode/decode errors now log status and buffer sizes.
  No codec fallback, hot/cold policy, storage format or device setting changed.

Validated on this host: all 114 CTests passed with no skips through the bound
check runner; all 14 Python entrypoint/handoff tests passed; the 10,000-request
LZ4/strict-IAA smoke completed with both rows validated. The three previously
failing checks also passed in a CPU-only build. Full performance, profiling
and memory-audit passes were not rerun for these corrections.

### Perf compatibility correction

The host's vendor perf 6.6 build rejects default DWARF sampling against its
7.3 kernel with EINVAL and a misleading per-thread `cpu-clock` error. Explicit
standard x86-64 registers work with the same event, DWARF stack size and FIFO
measurement gate. The runner now specifies that register set, without
system-wide sampling, fallback or permission changes.

A native 10k LZ4/QPL diagnostic run completed with both rows validated and
nonempty, symbolized perf call stacks (including QPL submit/execute paths).
This small probe validates recording, not statistically useful profiling.

The exact three-point, one-million-operation profile was subsequently rerun:
all six LZ4/QPL rows validated, all six perf reports contained samples, and
none reported lost samples. All 15 Python entrypoint/handoff tests passed.
The original failed output directory was preserved; corrected profile results
are in `results-profile-full-20261104-fixed`.

## Reviewed snapshot evidence

- 221 historical local runs passed full final payload, trace, task-conservation
  and resource checks. They were revalidated against the hardened runner.
- Independent clean build: 104 CTest passes, 10 real-QPL tests unavailable/skipped.
- Unchanged source previously passed the same executed tests under UBSan.
  ASan and TSan probes failed in the local host runtime, so neither is claimed
  as passed.
- The handoff runner rejects invalid exit-zero results before labeling a row
  successful, retains early failures and records binary/library provenance.
- Performance CRC is disabled; independent audit CRC and per-read oracle are
  enabled. Final correctness verification is outside measured performance.
- Cold stored-data ratio and tracked memory savings are distinct. QPL-private
  allocation coverage is unverified; complete QPL capacity results stay
  unavailable rather than treating unknown allocations as zero.

## Git distribution checks

- All four published input manifests, including the 10k smoke, regenerate with
  identical trace, rank-map and corpus checksums.
- A clean RelWithDebInfo build and CTest pass in the isolated publication directory:
  114 registered, 104 executed successfully and 10 QPL tests skipped.
- Nine handoff test methods cover malformed results, mismatches, unfinished work,
  wrong modes/routes, budget overflow, unavailable CPU clocks, leaks, absent QPL,
  preflight failure and failure-summary retention. Eleven invalid-exit-zero
  subcases are included. Python -O does not disable the runner's validation.
- Four entrypoint tests check the fixed three-point plan, perf scope, rejection
  of changed input manifests, fail-stop execution and combined summaries.
- Independent software smoke runs cover normal LZ4/surrogate performance,
  instrumented CPU diagnostics and RAW/LZ4/zlib memory audit. Their purpose is
  distribution/integrity validation, not a new performance claim.
- No private correspondence, recordings, full historical logs, local binaries,
  Git metadata or local user paths are included in the new experiment.

## What the hardware screen can decide

The first three-point screen can reveal whether this cache snapshot gains
useful throughput or CPU efficiency from real IAA, whether foreground decode
dominates GET p99, which host code consumes CPU, and how actual QPL data size
compares with LZ4. It records software outstanding work, not physical device
occupancy. Native Linux perf and strict QPL execution remain unverified locally.

The screen does not establish sustained-load performance, matched-arrival-rate
p99, complete QPL memory savings, equal-total-DRAM superiority, or application
value. It compares the current one-coordinator LZ4 path, not the best possible
multicore CPU implementation. It does not isolate hot-leaf or async speedups
without on/off or synchronous-QPL controls. It does not test hardware compression
with CPU decode, fixed Huffman, zlib-accel or QPL software performance.

A promising result should trigger only the next relevant comparison. Full
product claims still require correctness, >=1.25x complete retained/peak capacity,
>=15% sustainable throughput gain or >=20% CPU/op reduction, and GET/PUT p99
<=1.10x the appropriate CPU baseline under matched resources. No local surrogate
result qualifies those hardware gates.
