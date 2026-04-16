set(TEST_NAME ${PROJECT_NAME}_test)
set(COVR_NAME ${PROJECT_NAME}_coverage)
set(DEMO_NAME ${PROJECT_NAME}_demo)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)

set(INC_DIR ${PROJECT_SOURCE_DIR}/lib)
include_directories(${INC_DIR})

if(${CMAKE_BUILD_TYPE} MATCHES "Test")
        set(SRC_LIST btree/bplustree_test.c)
        add_executable(${TEST_NAME} ${SRC_LIST})
        set(CMAKE_C_FLAGS "-O2 -Wall -Werror -Wextra")
        target_link_libraries(${TEST_NAME} ${LIB_BPLUSTREE_NAME})
elseif(${CMAKE_BUILD_TYPE} MATCHES "Coverage")
        set(LIB_DIR ../lib)
        set(SRC_LIST ${LIB_DIR}/bplustree.c btree/bplustree_coverage.c)
        add_executable(${COVR_NAME} ${SRC_LIST})
        set(CMAKE_C_FLAGS "-O2 -Wall --coverage")
        include(CodeCoverage)
        setup_target_for_coverage(coverage ${COVR_NAME} coverage)
else()
        set(SRC_LIST btree/bplustree_demo.c)
        add_executable(${DEMO_NAME} ${SRC_LIST})
        set(CMAKE_C_FLAGS "-O2 -Wall -Werror -Wextra")
        target_link_libraries(${DEMO_NAME} ${LIB_BPLUSTREE_NAME})
endif()

# Always build minimal compressed DRAM-tier smoke tests
add_executable(bpt_compressed_lz4_smoke btree/compressed_lz4_smoke.c)
target_link_libraries(bpt_compressed_lz4_smoke ${LIB_BPLUSTREE_COMPRESSED_NAME})

add_executable(bpt_compressed_qpl_smoke btree/compressed_qpl_smoke.c)
target_link_libraries(bpt_compressed_qpl_smoke ${LIB_BPLUSTREE_COMPRESSED_NAME})

add_executable(bpt_compressed_zlib_accel_smoke btree/compressed_zlib_accel_smoke.c)
target_link_libraries(bpt_compressed_zlib_accel_smoke ${LIB_BPLUSTREE_COMPRESSED_NAME})
if(ZLIB_FOUND)
    target_link_libraries(bpt_compressed_zlib_accel_smoke ZLIB::ZLIB)
    target_compile_definitions(bpt_compressed_zlib_accel_smoke PRIVATE HAVE_ZLIB=1)
endif()

add_executable(bpt_compressed_crud_fuzz btree/compressed_crud_fuzz.c)
target_link_libraries(bpt_compressed_crud_fuzz ${LIB_BPLUSTREE_COMPRESSED_NAME})

add_executable(bpt_compressed_synthetic_test btree/compressed_synthetic_test.c)
target_link_libraries(bpt_compressed_synthetic_test ${LIB_BPLUSTREE_COMPRESSED_NAME})

add_executable(test_compression_concurrency btree/test_compression_concurrency.c)
target_link_libraries(test_compression_concurrency ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads)

add_executable(bpt_compressed_mixed_concurrency btree/compressed_mixed_concurrency.c)
target_link_libraries(bpt_compressed_mixed_concurrency ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads)

add_executable(bpt_compressed_split_payload_stats btree/compressed_split_payload_stats.c)
target_link_libraries(bpt_compressed_split_payload_stats ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads)
if(ZLIB_FOUND)
    target_link_libraries(bpt_compressed_split_payload_stats ZLIB::ZLIB)
    target_compile_definitions(bpt_compressed_split_payload_stats PRIVATE HAVE_ZLIB=1)
endif()

# Comprehensive test for legacy 1D design with dual compression
# Disabled - uses removed API
# add_executable(test_legacy_simple_compression legacy/test_legacy_simple_compression.c)
# target_link_libraries(test_legacy_simple_compression ${LIB_BPLUSTREE_COMPRESSED_NAME})

# Performance benchmark for legacy compression
# Disabled - uses removed API
# add_executable(benchmark_legacy_compression legacy/benchmark_legacy_compression.c)
# target_link_libraries(benchmark_legacy_compression ${LIB_BPLUSTREE_COMPRESSED_NAME})

# Simple basic test for legacy compression
# Disabled - uses removed API
# add_executable(simple_test_legacy legacy/simple_test_legacy.c)
# target_link_libraries(simple_test_legacy ${LIB_BPLUSTREE_COMPRESSED_NAME})

# Minimal test for basic functions
# Disabled - uses removed API
# add_executable(minimal_test legacy/minimal_test.c)
# target_link_libraries(minimal_test ${LIB_BPLUSTREE_COMPRESSED_NAME})

add_executable(tail_latency_compare btree/compressed_tail_latency.c)
target_link_libraries(tail_latency_compare ${LIB_BPLUSTREE_COMPRESSED_NAME} m)
if(ZLIB_FOUND)
    target_link_libraries(tail_latency_compare ZLIB::ZLIB)
    target_compile_definitions(tail_latency_compare PRIVATE HAVE_ZLIB=1)
endif()

add_executable(dcperf_workload_benchmark btree/compressed_dcperf_workload.c)
target_link_libraries(dcperf_workload_benchmark ${LIB_BPLUSTREE_COMPRESSED_NAME} m)

# Micro-benchmark LZ4 vs QPL on fixed 4KB blocks
add_executable(qpl_lz4_block_bench codec/qpl_lz4_block_bench.c)
target_link_libraries(qpl_lz4_block_bench ${LIB_BPLUSTREE_COMPRESSED_NAME})

# Simple block-size sweep benchmark (LZ4 vs QPL, Silesia payload)
add_executable(qpl_lz4_block_sizes codec/qpl_lz4_block_sizes.c)
target_link_libraries(qpl_lz4_block_sizes ${LIB_BPLUSTREE_COMPRESSED_NAME})

# Micro-benchmark LZ4 vs QPL on kv-pair shaped 8KB pages (tree-like layout)
add_executable(qpl_lz4_kv_bench codec/qpl_lz4_kv_bench.c)
target_link_libraries(qpl_lz4_kv_bench ${LIB_BPLUSTREE_COMPRESSED_NAME})

# Multi-threaded benchmark (C++)
add_executable(qpl_lz4_kv_bench_mt codec/qpl_lz4_kv_bench_mt.cpp)
target_link_libraries(qpl_lz4_kv_bench_mt ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads)

# Async batch benchmark (C++)
add_executable(qpl_lz4_kv_bench_async codec/qpl_lz4_kv_bench_async.cpp)
target_link_libraries(qpl_lz4_kv_bench_async ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads)

# Mixed workload benchmark (read + write + compaction)
add_executable(qpl_lz4_mixed_workload codec/qpl_lz4_mixed_workload.cpp)
target_link_libraries(qpl_lz4_mixed_workload ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads)
if(ZLIB_FOUND)
    target_link_libraries(qpl_lz4_mixed_workload ZLIB::ZLIB)
    target_compile_definitions(qpl_lz4_mixed_workload PRIVATE HAVE_ZLIB=1)
endif()

# Per-operation throughput benchmark with warmup (all 3 codecs)
add_executable(bpt_compressed_throughput_bench btree/compressed_throughput_bench.c)
target_link_libraries(bpt_compressed_throughput_bench ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads m)
if(ZLIB_FOUND)
    target_link_libraries(bpt_compressed_throughput_bench ZLIB::ZLIB)
    target_compile_definitions(bpt_compressed_throughput_bench PRIVATE HAVE_ZLIB=1)
endif()
