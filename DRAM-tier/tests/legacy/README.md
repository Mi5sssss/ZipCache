# Legacy Tests

These files are kept for reference but are not active CMake targets.

## Current files

- `benchmark_legacy_compression.c`: old compression benchmark using removed legacy APIs.
- `minimal_test.c`: old minimal test using removed legacy APIs.
- `simple_test_legacy.c`: old simple compression test using removed legacy APIs.
- `test_legacy_simple_compression.c`: old legacy simple-compression test; also ignored by the top-level `test_*` ignore pattern unless explicitly tracked.

## What to do next

- Do not use these for current ZipCache/DRAM-tier correctness claims.
- If any scenario is still valuable, port it into `btree/` using `bplus_tree_compressed_init_with_config`.
- Remove or archive these once the active B+Tree tests cover the same cases.
