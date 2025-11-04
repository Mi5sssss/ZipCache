set(TEST_NAME ${PROJECT_NAME}_test)
set(COVR_NAME ${PROJECT_NAME}_coverage)
set(DEMO_NAME ${PROJECT_NAME}_demo)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)

set(INC_DIR ${PROJECT_SOURCE_DIR}/lib)
include_directories(${INC_DIR})

if(${CMAKE_BUILD_TYPE} MATCHES "Test")
        set(SRC_LIST bplustree_test.c)
        add_executable(${TEST_NAME} ${SRC_LIST})
        set(CMAKE_C_FLAGS "-O2 -Wall -Werror -Wextra")
        target_link_libraries(${TEST_NAME} ${LIB_BPLUSTREE_NAME})
elseif(${CMAKE_BUILD_TYPE} MATCHES "Coverage")
        set(LIB_DIR ../lib)
        set(SRC_LIST ${LIB_DIR}/bplustree.c bplustree_coverage.c)
        add_executable(${COVR_NAME} ${SRC_LIST})
        set(CMAKE_C_FLAGS "-O2 -Wall --coverage")
        include(CodeCoverage)
        setup_target_for_coverage(coverage ${COVR_NAME} coverage)
else()
        set(SRC_LIST bplustree_demo.c)
        add_executable(${DEMO_NAME} ${SRC_LIST})
        set(CMAKE_C_FLAGS "-O2 -Wall -Werror -Wextra")
        target_link_libraries(${DEMO_NAME} ${LIB_BPLUSTREE_NAME})
endif()

# Always build minimal compressed DRAM-tier smoke tests
add_executable(bpt_compressed_lz4_smoke compressed_lz4_smoke.c)
target_link_libraries(bpt_compressed_lz4_smoke ${LIB_BPLUSTREE_COMPRESSED_NAME})

add_executable(bpt_compressed_qpl_smoke compressed_qpl_smoke.c)
target_link_libraries(bpt_compressed_qpl_smoke ${LIB_BPLUSTREE_COMPRESSED_NAME})

add_executable(bpt_compressed_crud_fuzz compressed_crud_fuzz.c)
target_link_libraries(bpt_compressed_crud_fuzz ${LIB_BPLUSTREE_COMPRESSED_NAME})

add_executable(bpt_compressed_synthetic_test compressed_synthetic_test.c)
target_link_libraries(bpt_compressed_synthetic_test ${LIB_BPLUSTREE_COMPRESSED_NAME})

# Comprehensive test for legacy 1D design with dual compression
# Disabled - uses removed API
# add_executable(test_legacy_simple_compression test_legacy_simple_compression.c)
# target_link_libraries(test_legacy_simple_compression ${LIB_BPLUSTREE_COMPRESSED_NAME})

# Performance benchmark for legacy compression
# Disabled - uses removed API
# add_executable(benchmark_legacy_compression benchmark_legacy_compression.c)
# target_link_libraries(benchmark_legacy_compression ${LIB_BPLUSTREE_COMPRESSED_NAME})

# Simple basic test for legacy compression
# Disabled - uses removed API
# add_executable(simple_test_legacy simple_test_legacy.c)
# target_link_libraries(simple_test_legacy ${LIB_BPLUSTREE_COMPRESSED_NAME})

# Minimal test for basic functions
# Disabled - uses removed API
# add_executable(minimal_test minimal_test.c)
# target_link_libraries(minimal_test ${LIB_BPLUSTREE_COMPRESSED_NAME})

add_executable(tail_latency_compare compressed_tail_latency.c)
target_link_libraries(tail_latency_compare ${LIB_BPLUSTREE_COMPRESSED_NAME} m)

add_executable(dcperf_workload_benchmark compressed_dcperf_workload.c)
target_link_libraries(dcperf_workload_benchmark ${LIB_BPLUSTREE_COMPRESSED_NAME} m)
