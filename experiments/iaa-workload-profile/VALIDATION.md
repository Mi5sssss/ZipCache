# Local verification and hardware boundary

This Git distribution preserves all 97 source files from the reviewed
2026-11-02 workload/profile snapshot. The existing runner, CPU profiler, layout,
hot/cold policy and backend execution strategy are unchanged. Distribution adds
one orchestration entrypoint, frozen-input verification and minimal public test
fixtures; it does not introduce a new scheduler or hybrid routing policy.

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
