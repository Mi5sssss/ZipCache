cmake_minimum_required(VERSION 3.10)

project(bplustree VERSION 2.0.0 LANGUAGES C)

# Set C standard
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Set default build type to Release
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Set compiler flags
set(CMAKE_C_FLAGS_DEBUG "-g -O0 -Wall -Wextra")
set(CMAKE_C_FLAGS_RELEASE "-O3 -DNDEBUG")

# Find required packages
find_package(PkgConfig REQUIRED)
pkg_check_modules(LZ4 REQUIRED liblz4)
find_package(Threads REQUIRED)

# Find Intel QPL library (portable)
# Allow users to specify via environment variable QPL_ROOT or CMAKE_PREFIX_PATH
find_library(QPL_LIBRARY qpl
    HINTS $ENV{QPL_ROOT}/lib
)
find_path(QPL_INCLUDE_DIR qpl/qpl.h
    HINTS $ENV{QPL_ROOT}/include
)

if(QPL_LIBRARY AND QPL_INCLUDE_DIR)
    message(STATUS "Found Intel QPL: ${QPL_LIBRARY}")
    set(QPL_FOUND TRUE)
else()
    message(WARNING "Intel QPL not found - compressed B+Tree will use LZ4 only")
    set(QPL_FOUND FALSE)
endif()

# Set library names
set(LIB_BPLUSTREE_NAME bplustree)
set(LIB_BPLUSTREE_COMPRESSED_NAME bplustree_compressed)

# Configure output directories
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib) 
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Add subdirectories
add_subdirectory(lib)
add_subdirectory(tests)

# Add examples and compression tests
add_subdirectory(examples)

# Create install targets
install(TARGETS ${LIB_BPLUSTREE_NAME} ${LIB_BPLUSTREE_COMPRESSED_NAME}
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)

install(DIRECTORY lib/ DESTINATION include/bplustree
    FILES_MATCHING PATTERN "*.h"
)

# Package configuration
include(CMakePackageConfigHelpers)
set(INCLUDE_INSTALL_DIR include/)
configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/bplustreeConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/bplustreeConfig.cmake
    INSTALL_DESTINATION lib/cmake/bplustree
    PATH_VARS INCLUDE_INSTALL_DIR
)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/bplustreeConfig.cmake
    DESTINATION lib/cmake/bplustree
)
