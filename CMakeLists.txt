cmake_minimum_required(VERSION 3.10)
project(bplustree_multi_tier VERSION 2.0)

# Set C standard
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Add debug information
set(CMAKE_C_FLAGS_DEBUG "-g -O0 -Wall -Wextra")
set(CMAKE_C_FLAGS_RELEASE "-O3 -DNDEBUG")

# Set default build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

option(BUILD_DRAM_TIER "Build DRAM-tier B+Tree" ON)
option(BUILD_SSD_TIER "Build SSD-tier B+Tree" ON)

# Build options
if(BUILD_DRAM_TIER)
    message(STATUS "Building DRAM-tier B+Tree with lazy compression")
    add_subdirectory(DRAM-tier)
endif()

if(BUILD_SSD_TIER)
    message(STATUS "Building SSD-tier B+Tree with disk persistence")
    add_subdirectory(SSD-tier)
endif()

# Create comparison tools directory
add_subdirectory(comparison)

# Focused ZipCache thresholds test (built via CMake to avoid header conflicts)
add_executable(zipcache_thresholds_test
    zipcache.c
    tests/zipcache_thresholds_test.c
)
target_compile_definitions(zipcache_thresholds_test PRIVATE _GNU_SOURCE)
target_link_libraries(zipcache_thresholds_test pthread)
