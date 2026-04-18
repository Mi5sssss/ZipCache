---
description: Read this first — current project status, blockers, and what to work on next
---

# ZipCache B+Tree — Project Status

**Last updated**: 2026-04-13

## What's Working ✅

- LZ4, QPL (software), zlib-accel compression backends all functional
- QPL job pool (replaces single-mutex bottleneck)
- Smoke tests pass for all three codecs
- Single-thread B+Tree operations (put/get/delete/get_range/put_with_payload/stats)
- Block-level codec benchmarks (KV bench, mixed workload, async batch)
- Tree-level benchmarks (tail latency, DCPerf workload)
- Test organization: `btree/` (real tree) vs `codec/` (block-level) vs `legacy/`
- QPL path (auto/software/hardware) and Huffman mode (fixed/dynamic) configurable via env

## Current Blockers 🔴

1. ~~**Multi-writer deadlock**~~: **FIXED** (2026-04-13). Removed per-leaf locking, serialize writers under tree wrlock. All 3 codecs pass 8-thread concurrency.

2. **Range-scan correctness**: `bpt_compressed_crud_fuzz` shows `get_range` returning values outside the requested live range. Also causes ~75 mismatches in `bpt_compressed_mixed_concurrency`.

3. **Stats drift**: `bpt_compressed_split_payload_stats` shows incremental stats exceeding calculated leaf-walk stats after split/delete/reinsert.

## Intel Collaboration Status 🤝

- **Partner**: Binuraj (Intel)
- **Focus**: QPL hardware performance on IAA machines
- **Open issue**: Thread scaling degradation at high thread counts — Intel investigating work queue distribution
- **Our deliverables**: Reproducible benchmarks with env-controlled QPL path/mode, documented build instructions

## What Needs Work Next 📋

### High Priority
- [ ] Fix multi-writer deadlock (unblocks all concurrency tests)
- [ ] Fix range-scan correctness (fixes CRUD fuzz)
- [ ] Fix stats drift after structural changes

### Medium Priority
- [ ] Explicit zlib compression-level controls
- [ ] CSV/JSON report mode for codec benchmarks (Intel parsing)
- [ ] QPL hardware queue scaling tests
- [ ] Robust bucket overflow handling (currently NUM_SUBPAGES=1 workaround)

### Low Priority
- [ ] Full ZipCache multi-tier integration
- [ ] SSD-tier and LO-tier activation
- [ ] Background compaction thread testing

## Key Conversations

- `bc910cbb`: QPL job pool + dedup bugfixes (Nov 2025)
- `7e7320ed`: Mixed workload bugfix + Intel benchmarks (Dec 2025 – Mar 2026)
- `282a1867`: Concurrency deadlock fix + workflow setup (Apr 2026, current)
