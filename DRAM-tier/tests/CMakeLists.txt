set(TEST_NAME ${PROJECT_NAME}_test)
set(COVR_NAME ${PROJECT_NAME}_coverage)
set(DEMO_NAME ${PROJECT_NAME}_demo)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)

set(INC_DIR ${PROJECT_SOURCE_DIR}/lib)
include_directories(${INC_DIR})

if(${CMAKE_BUILD_TYPE} MATCHES "Test")
        set(SRC_LIST btree/bplustree_test.c)
        add_executable(${TEST_NAME} ${SRC_LIST})
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Werror -Wextra")
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
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Werror -Wextra")
        target_link_libraries(${DEMO_NAME} ${LIB_BPLUSTREE_NAME})
endif()

# Always build minimal compressed DRAM-tier smoke tests
add_executable(bpt_compressed_lz4_smoke btree/compressed_lz4_smoke.c)
target_link_libraries(bpt_compressed_lz4_smoke ${LIB_BPLUSTREE_COMPRESSED_NAME})
add_executable(bpt_compressed_record_api btree/compressed_record_api.c)
target_link_libraries(bpt_compressed_record_api ${LIB_BPLUSTREE_COMPRESSED_NAME})
add_test(NAME compressed_record_api COMMAND bpt_compressed_record_api)
set_tests_properties(compressed_record_api PROPERTIES TIMEOUT 60)

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

# Producer-side request trace for Virtual IAA replay on non-IAA hosts.
add_executable(bpt_submission_shape btree/submission_shape_bench.c)
target_link_libraries(bpt_submission_shape ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads)

# Opt-in architecture probes on real payloads; not an IAA performance emulator.
add_executable(bpt_architecture_dry_run btree/architecture_dry_run.c)
target_link_libraries(bpt_architecture_dry_run ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads)

add_executable(bpt_compressed_split_payload_stats btree/compressed_split_payload_stats.c)
target_link_libraries(bpt_compressed_split_payload_stats ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads)
if(ZLIB_FOUND)
    target_link_libraries(bpt_compressed_split_payload_stats ZLIB::ZLIB)
    target_compile_definitions(bpt_compressed_split_payload_stats PRIVATE HAVE_ZLIB=1)
endif()

add_executable(bpt_compressed_async_compaction btree/compressed_async_compaction.c)
target_link_libraries(bpt_compressed_async_compaction ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads)

add_executable(bpt_compressed_failure_paths btree/compressed_failure_paths.c)
target_link_libraries(bpt_compressed_failure_paths ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads)

add_executable(bpt_product_capacity btree/product_capacity_probe.c)
target_link_libraries(bpt_product_capacity ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads)
add_executable(bpt_product_contract btree/compressed_product_contract.c)
target_link_libraries(bpt_product_contract ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads)
if(ZLIB_FOUND)
    target_compile_definitions(bpt_product_contract PRIVATE HAVE_ZLIB=1)
endif()
add_executable(bpt_product_qpl_crosspath btree/product_qpl_crosspath.c)
target_link_libraries(bpt_product_qpl_crosspath ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads)
add_test(NAME product_qpl_crosspath COMMAND bpt_product_qpl_crosspath)
set_tests_properties(product_qpl_crosspath PROPERTIES TIMEOUT 120 SKIP_RETURN_CODE 77)
add_test(NAME product_contract COMMAND bpt_product_contract)
set_tests_properties(product_contract PROPERTIES TIMEOUT 60 ENVIRONMENT "BTREE_MEMORY_AUDIT=1")
foreach(codec raw lz4 zlib)
    add_test(NAME product_capacity_${codec} COMMAND bpt_product_capacity ${codec})
    set_tests_properties(product_capacity_${codec} PROPERTIES TIMEOUT 60 SKIP_RETURN_CODE 77
        ENVIRONMENT "BTREE_BG_COMPACTION=1;BTREE_MEMORY_AUDIT=1;PRODUCT_RANDOM=1;PRODUCT_KEYS=128;PRODUCT_OPS=200")
endforeach()

add_test(NAME compressed_lz4_smoke COMMAND bpt_compressed_lz4_smoke)
add_test(NAME compressed_qpl_smoke COMMAND bpt_compressed_qpl_smoke)
set_tests_properties(compressed_qpl_smoke PROPERTIES SKIP_RETURN_CODE 77)
add_test(NAME compressed_zlib_smoke COMMAND bpt_compressed_zlib_accel_smoke)
add_test(NAME compressed_crud_fuzz COMMAND bpt_compressed_crud_fuzz)
add_test(NAME compressed_split_payload COMMAND bpt_compressed_split_payload_stats)
add_test(NAME compressed_thread_smoke COMMAND test_compression_concurrency)
add_test(NAME compressed_mixed_concurrency COMMAND bpt_compressed_mixed_concurrency)
add_test(NAME compressed_submission_shape COMMAND bpt_submission_shape)
add_test(NAME compressed_async_compaction COMMAND bpt_compressed_async_compaction)
add_test(NAME compressed_failure_paths COMMAND bpt_compressed_failure_paths)

set_tests_properties(
    compressed_lz4_smoke
    compressed_qpl_smoke
    compressed_zlib_smoke
    compressed_crud_fuzz
    compressed_split_payload
    compressed_thread_smoke
    PROPERTIES ENVIRONMENT "BTREE_BG_COMPACTION=1" TIMEOUT 30)
set_tests_properties(
    compressed_mixed_concurrency
    PROPERTIES ENVIRONMENT "BTREE_BG_COMPACTION=1;BTREE_CODEC_FILTER=lz4;BTREE_DURATION_SEC=1;BTREE_SCAN_PCT=0;BTREE_READ_PCT=55;BTREE_WRITE_PCT=35;BTREE_DELETE_PCT=10" TIMEOUT 30)
set_tests_properties(
    compressed_submission_shape
    PROPERTIES ENVIRONMENT "BTREE_BG_COMPACTION=1;BTREE_SHARDS=4;BTREE_SUBMISSION_THREADS=4;BTREE_SUBMISSION_OPS_PER_THREAD=5000;BTREE_SUBMISSION_KEYS_PER_THREAD=64" TIMEOUT 30)
set_tests_properties(compressed_async_compaction compressed_failure_paths PROPERTIES TIMEOUT 30)

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

if(QPL_FOUND)
    # Direct-QPL microbenchmarks require the real QPL headers/library.
    add_executable(qpl_lz4_block_bench codec/qpl_lz4_block_bench.c)
    target_link_libraries(qpl_lz4_block_bench ${LIB_BPLUSTREE_COMPRESSED_NAME})

    add_executable(qpl_lz4_block_sizes codec/qpl_lz4_block_sizes.c)
    target_link_libraries(qpl_lz4_block_sizes ${LIB_BPLUSTREE_COMPRESSED_NAME})

    add_executable(qpl_lz4_kv_bench codec/qpl_lz4_kv_bench.c)
    target_link_libraries(qpl_lz4_kv_bench ${LIB_BPLUSTREE_COMPRESSED_NAME})

    add_executable(qpl_lz4_kv_bench_mt codec/qpl_lz4_kv_bench_mt.cpp)
    target_link_libraries(qpl_lz4_kv_bench_mt ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads)

    add_executable(qpl_lz4_kv_bench_async codec/qpl_lz4_kv_bench_async.cpp)
    target_link_libraries(qpl_lz4_kv_bench_async ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads)

    add_executable(qpl_lz4_mixed_workload codec/qpl_lz4_mixed_workload.cpp)
    target_link_libraries(qpl_lz4_mixed_workload ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads)
    if(ZLIB_FOUND)
        target_link_libraries(qpl_lz4_mixed_workload ZLIB::ZLIB)
        target_compile_definitions(qpl_lz4_mixed_workload PRIVATE HAVE_ZLIB=1)
    endif()
endif()

# Per-operation throughput benchmark with warmup (all 3 codecs)
add_executable(bpt_compressed_throughput_bench btree/compressed_throughput_bench.c)
target_link_libraries(bpt_compressed_throughput_bench ${LIB_BPLUSTREE_COMPRESSED_NAME} Threads::Threads m)
if(ZLIB_FOUND)
    target_link_libraries(bpt_compressed_throughput_bench ZLIB::ZLIB)
    target_compile_definitions(bpt_compressed_throughput_bench PRIVATE HAVE_ZLIB=1)
endif()
