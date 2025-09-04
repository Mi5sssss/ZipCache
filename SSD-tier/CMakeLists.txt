cmake_minimum_required(VERSION 3.10)
project(HybridBPlusTree)

# Set C standard
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -g")

# Include directories
include_directories(lib)

# Library
add_library(bplustree_hybrid STATIC
    lib/bplustree.c
)

# Test executables
add_executable(hybrid_btree_test
    tests/hybrid_btree_test.c
)

add_executable(super_leaf_test
    tests/super_leaf_test.c
)

add_executable(hashed_io_test
    tests/hashed_io_test.c
)

add_executable(zero_padding_test
    tests/zero_padding_test.c
)

target_link_libraries(hybrid_btree_test bplustree_hybrid)
target_link_libraries(super_leaf_test bplustree_hybrid)
target_link_libraries(hashed_io_test bplustree_hybrid)
target_link_libraries(zero_padding_test bplustree_hybrid)

# Create build directory if it doesn't exist
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR})
