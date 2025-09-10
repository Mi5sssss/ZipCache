# Original B+Tree library
set(LIB_BPLUSTREE_SRC bplustree.c)

# Compressed B+Tree library  
set(LIB_BPLUSTREE_COMPRESSED_SRC 
    bplustree.c
    bplustree_compressed.c
)

# Debug definitions
add_definitions(-D_BPLUS_TREE_DEBUG)

# Create shared library for original B+Tree
add_library(${LIB_BPLUSTREE_NAME} SHARED ${LIB_BPLUSTREE_SRC})
set_target_properties(${LIB_BPLUSTREE_NAME} PROPERTIES 
    VERSION 1.0 
    SOVERSION 1
    CLEAN_DIRECT_OUTPUT 1
)

# Create static library for original B+Tree  
add_library(${LIB_BPLUSTREE_NAME}_static STATIC ${LIB_BPLUSTREE_SRC})
set_target_properties(${LIB_BPLUSTREE_NAME}_static PROPERTIES 
    OUTPUT_NAME "${LIB_BPLUSTREE_NAME}"
    CLEAN_DIRECT_OUTPUT 1
)

# Create shared library for compressed B+Tree
add_library(${LIB_BPLUSTREE_COMPRESSED_NAME} SHARED ${LIB_BPLUSTREE_COMPRESSED_SRC})
set_target_properties(${LIB_BPLUSTREE_COMPRESSED_NAME} PROPERTIES
    VERSION 2.0
    SOVERSION 2  
    CLEAN_DIRECT_OUTPUT 1
)

# Link compressed library with dependencies
target_link_libraries(${LIB_BPLUSTREE_COMPRESSED_NAME} 
    ${LZ4_LIBRARIES} 
    Threads::Threads
)
target_include_directories(${LIB_BPLUSTREE_COMPRESSED_NAME} PRIVATE ${LZ4_INCLUDE_DIRS})
target_compile_options(${LIB_BPLUSTREE_COMPRESSED_NAME} PRIVATE ${LZ4_CFLAGS_OTHER})

# Add Intel QPL support if available
if(QPL_FOUND)
    target_link_libraries(${LIB_BPLUSTREE_COMPRESSED_NAME} ${QPL_LIBRARY})
    target_include_directories(${LIB_BPLUSTREE_COMPRESSED_NAME} PRIVATE ${QPL_INCLUDE_DIR})
    target_compile_definitions(${LIB_BPLUSTREE_COMPRESSED_NAME} PRIVATE HAVE_QPL=1)
    # Some QPL builds (static) pull C++ stdlib symbols; link libstdc++ explicitly
    if (CMAKE_C_COMPILER_ID STREQUAL "GNU")
        target_link_libraries(${LIB_BPLUSTREE_COMPRESSED_NAME} stdc++)
    endif()
endif()

# Create static library for compressed B+Tree
add_library(${LIB_BPLUSTREE_COMPRESSED_NAME}_static STATIC ${LIB_BPLUSTREE_COMPRESSED_SRC})
set_target_properties(${LIB_BPLUSTREE_COMPRESSED_NAME}_static PROPERTIES
    OUTPUT_NAME "${LIB_BPLUSTREE_COMPRESSED_NAME}"
    CLEAN_DIRECT_OUTPUT 1
)
target_link_libraries(${LIB_BPLUSTREE_COMPRESSED_NAME}_static 
    ${LZ4_LIBRARIES} 
    Threads::Threads
)
target_include_directories(${LIB_BPLUSTREE_COMPRESSED_NAME}_static PRIVATE ${LZ4_INCLUDE_DIRS})
target_compile_options(${LIB_BPLUSTREE_COMPRESSED_NAME}_static PRIVATE ${LZ4_CFLAGS_OTHER})

# Add Intel QPL support for static library if available
if(QPL_FOUND)
    target_link_libraries(${LIB_BPLUSTREE_COMPRESSED_NAME}_static ${QPL_LIBRARY})
    target_include_directories(${LIB_BPLUSTREE_COMPRESSED_NAME}_static PRIVATE ${QPL_INCLUDE_DIR})
    target_compile_definitions(${LIB_BPLUSTREE_COMPRESSED_NAME}_static PRIVATE HAVE_QPL=1)
    if (CMAKE_C_COMPILER_ID STREQUAL "GNU")
        target_link_libraries(${LIB_BPLUSTREE_COMPRESSED_NAME}_static stdc++)
    endif()
endif()
