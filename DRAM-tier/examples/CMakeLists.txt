# Compression examples and tests

# Simple compressed test 
add_executable(simple_compressed_test simple_compressed_test.c)
target_link_libraries(simple_compressed_test ${LIB_BPLUSTREE_COMPRESSED_NAME})

# Performance test
add_executable(compressed_performance_test compressed_performance_test.c) 
target_link_libraries(compressed_performance_test ${LIB_BPLUSTREE_COMPRESSED_NAME})

# Silesia corpus compression test
add_executable(silesia_compression_test silesia_compression_test.c)
target_link_libraries(silesia_compression_test ${LIB_BPLUSTREE_COMPRESSED_NAME})

# Samba chunk B+tree test  
add_executable(samba_chunk_btree_test samba_chunk_btree_test.c)
target_link_libraries(samba_chunk_btree_test ${LIB_BPLUSTREE_COMPRESSED_NAME})

# Samba chunk key-value test
add_executable(samba_chunk_kv_test samba_chunk_kv_test.c)
target_link_libraries(samba_chunk_kv_test ${LIB_BPLUSTREE_COMPRESSED_NAME})

# Lazy flush behavior test
add_executable(lazy_flush_test lazy_flush_test.c)
target_link_libraries(lazy_flush_test ${LIB_BPLUSTREE_COMPRESSED_NAME})

# Compression benchmark test
add_executable(compression_benchmark compression_benchmark.c)
target_link_libraries(compression_benchmark ${LIB_BPLUSTREE_COMPRESSED_NAME})

# Focused compression benchmark
add_executable(focused_compression_benchmark focused_compression_benchmark.c)
target_link_libraries(focused_compression_benchmark ${LIB_BPLUSTREE_COMPRESSED_NAME})

# Synthetic compression benchmark
add_executable(synthetic_compression_benchmark synthetic_compression_benchmark.c)
target_link_libraries(synthetic_compression_benchmark ${LIB_BPLUSTREE_COMPRESSED_NAME})
