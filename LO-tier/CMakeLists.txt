cmake_minimum_required(VERSION 3.10)
project(BPlusTreeLO)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -g")

# Large Object B+Tree library
add_library(bplustree_lo STATIC
    lib/bplustree_lo.c
)

target_include_directories(bplustree_lo PUBLIC lib)

# Test executable
add_executable(btlo_test
    tests/btlo_test.c
)

# Example executable
add_executable(btlo_example
    examples/btlo_example.c
)

add_executable(splitting_test
    tests/splitting_test.c
)

target_link_libraries(btlo_test bplustree_lo)
target_link_libraries(btlo_example bplustree_lo)
target_link_libraries(splitting_test bplustree_lo)

# Create build directory if it doesn't exist
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR})

# Add custom target for easy testing
add_custom_target(test_btlo
    COMMAND btlo_test
    DEPENDS btlo_test
    COMMENT "Running Large Object B+Tree tests"
)
