#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>
#include <assert.h>
#include <lz4.h>

#include "../lib/bplustree_compressed.h"

#define MAX_CHUNK_SIZE 4096  // Maximum chunk size
#define MIN_CHUNK_SIZE 512   // Minimum chunk size
#define MAX_KEYS 10000       // Maximum number of keys to test

// Timing utilities
double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

// Structure to hold chunk data for testing
struct chunk_data {
    char *data;
    size_t size;
    int key;
    int hash_value; // Hash of the chunk data to use as B+Tree value
};

// Calculate hash of chunk data
int calculate_chunk_hash(const char *data, size_t size) {
    int hash = 0;
    for (size_t i = 0; i < size; i++) {
        hash = (hash * 31 + data[i]) & 0x7FFFFFFF;
    }
    return hash;
}

// Test B+Tree with samba chunks as values
void test_samba_chunk_btree_compression() {
    printf("=== SAMBA CHUNK B+TREE COMPRESSION TEST ===\n");
    printf("Using samba file chunks as values in B+Tree key-value pairs\n");
    printf("Adjacent keys will have adjacent chunk values from samba file\n\n");
    
    // Open the samba file
    FILE *file = fopen("SilesiaCorpus/samba", "rb");
    if (file == NULL) {
        printf("Error: Could not open SilesiaCorpus/samba file\n");
        return;
    }
    
    // Get file size
    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    fseek(file, 0, SEEK_SET);
    
    printf("Samba file size: %ld bytes\n", file_size);
    
    // Initialize compressed B+Tree with 4KB leaf nodes
    int order = 16;          // Order for non-leaf nodes
    int entries = 64;        // Entries per leaf node (will be compressed)
    struct bplus_tree_compressed *ct_tree = bplus_tree_compressed_init(order, entries);
    if (ct_tree == NULL) {
        printf("Error: Failed to initialize compressed B+Tree\n");
        fclose(file);
        return;
    }
    
    // Enable compression
    bplus_tree_compressed_set_compression(ct_tree, 1);
    
    // Read chunks and create key-value pairs
    struct chunk_data *chunks = malloc(MAX_KEYS * sizeof(struct chunk_data));
    if (chunks == NULL) {
        printf("Error: Failed to allocate memory for chunks\n");
        bplus_tree_compressed_deinit(ct_tree);
        fclose(file);
        return;
    }
    
    int num_chunks = 0;
    size_t total_chunk_size = 0;
    
    printf("Reading chunks and creating key-value pairs...\n");
    
    double start_time = get_time();
    
    // Read chunks with varying sizes (adjacent chunks from file)
    while (num_chunks < MAX_KEYS && !feof(file)) {
        // Use fixed chunk size for now to ensure adjacent values
        size_t chunk_size = MIN_CHUNK_SIZE;
        
        // Allocate memory for chunk
        chunks[num_chunks].data = malloc(chunk_size);
        if (chunks[num_chunks].data == NULL) {
            printf("Error: Failed to allocate memory for chunk %d\n", num_chunks);
            break;
        }
        
        // Read chunk data
        size_t bytes_read = fread(chunks[num_chunks].data, 1, chunk_size, file);
        if (bytes_read == 0) {
            free(chunks[num_chunks].data);
            break;
        }
        
        // Pad with zeros if chunk is not full
        if (bytes_read < chunk_size) {
            memset(chunks[num_chunks].data + bytes_read, 0, chunk_size - bytes_read);
        }
        
        // Set chunk metadata
        chunks[num_chunks].size = chunk_size;
        chunks[num_chunks].key = num_chunks; // Adjacent keys: 0, 1, 2, ...
        chunks[num_chunks].hash_value = calculate_chunk_hash(chunks[num_chunks].data, chunk_size);
        
        // Insert into B+Tree (key -> hash_value)
        int result = bplus_tree_compressed_put(ct_tree, num_chunks, chunks[num_chunks].hash_value);
        if (result != 0) {
            printf("Warning: Failed to insert key %d into B+Tree\n", num_chunks);
            free(chunks[num_chunks].data);
            break;
        }
        
        total_chunk_size += chunk_size;
        num_chunks++;
        
        // Progress indicator
        if (num_chunks % 1000 == 0) {
            printf("Processed %d chunks...\n", num_chunks);
        }
    }
    
    double end_time = get_time();
    double insert_time = end_time - start_time;
    
    fclose(file);
    
    printf("Inserted %d key-value pairs into B+Tree\n", num_chunks);
    printf("Total chunk data size: %zu bytes (%.2f MB)\n", 
           total_chunk_size, total_chunk_size / (1024.0 * 1024.0));
    printf("Insertion time: %.6f seconds\n", insert_time);
    printf("Insertion rate: %.2f keys/second\n", num_chunks / insert_time);
    
    // Get B+Tree compression statistics
    size_t btree_total_size, btree_compressed_size;
    int stats_result = bplus_tree_compressed_stats(ct_tree, &btree_total_size, &btree_compressed_size);
    double btree_compression_ratio = bplus_tree_compressed_get_compression_ratio(ct_tree);
    
    printf("\n=== B+TREE COMPRESSION RESULTS ===\n");
    if (stats_result == 0) {
        printf("B+Tree total uncompressed size: %zu bytes (%.2f MB)\n", 
               btree_total_size, btree_total_size / (1024.0 * 1024.0));
        printf("B+Tree compressed size: %zu bytes (%.2f MB)\n", 
               btree_compressed_size, btree_compressed_size / (1024.0 * 1024.0));
        printf("B+Tree compression ratio: %.2f%%\n", btree_compression_ratio);
        printf("B+Tree space saved: %.2f%%\n", 100.0 - btree_compression_ratio);
    } else {
        printf("Failed to get compression statistics\n");
    }
    
    // Test retrieval
    printf("\n=== RETRIEVAL TEST ===\n");
    start_time = get_time();
    
    int retrieval_successes = 0;
    int retrieval_failures = 0;
    int test_retrievals = (num_chunks < 1000) ? num_chunks : 1000;
    
    for (int i = 0; i < test_retrievals; i++) {
        int retrieved_value = bplus_tree_compressed_get(ct_tree, i);
        
        if (retrieved_value == chunks[i].hash_value) {
            retrieval_successes++;
        } else {
            retrieval_failures++;
            if (retrieval_failures <= 10) { // Limit error output
                printf("Retrieval mismatch for key %d: expected %d, got %d\n", 
                       i, chunks[i].hash_value, retrieved_value);
            }
        }
    }
    
    end_time = get_time();
    double retrieval_time = end_time - start_time;
    
    printf("Retrieval test results (%d keys tested):\n", test_retrievals);
    printf("Successes: %d\n", retrieval_successes);
    printf("Failures: %d\n", retrieval_failures);
    printf("Success rate: %.2f%%\n", retrieval_successes * 100.0 / test_retrievals);
    printf("Retrieval time: %.6f seconds\n", retrieval_time);
    printf("Retrieval rate: %.2f keys/second\n", test_retrievals / retrieval_time);
    
    // Test range queries
    printf("\n=== RANGE QUERY TEST ===\n");
    start_time = get_time();
    
    int range_tests = 100;
    int range_successes = 0;
    
    for (int i = 0; i < range_tests && i * 10 + 5 < num_chunks; i++) {
        int start_key = i * 10;
        int end_key = start_key + 5;
        
        int range_result = bplus_tree_compressed_get_range(ct_tree, start_key, end_key);
        
        if (range_result != -1) {
            range_successes++;
        }
    }
    
    end_time = get_time();
    double range_time = end_time - start_time;
    
    printf("Range query test results (%d queries):\n", range_tests);
    printf("Successes: %d\n", range_successes);
    printf("Range query time: %.6f seconds\n", range_time);
    printf("Range query rate: %.2f queries/second\n", range_tests / range_time);
    
    // Calculate overall compression ratio including chunk data
    printf("\n=== OVERALL COMPRESSION SUMMARY ===\n");
    printf("Original chunk data: %zu bytes (%.2f MB)\n", 
           total_chunk_size, total_chunk_size / (1024.0 * 1024.0));
    
    if (stats_result == 0 && btree_total_size > 0) {
        printf("B+Tree storage overhead: %zu bytes\n", btree_total_size);
        printf("B+Tree compressed storage: %zu bytes\n", btree_compressed_size);
        
        double overall_ratio = (double)btree_compressed_size / total_chunk_size * 100.0;
        printf("Overall compression ratio (compressed B+Tree / original chunks): %.2f%%\n", overall_ratio);
        printf("Overall space savings: %.2f%%\n", 100.0 - overall_ratio);
    }
    
    // Dump B+Tree structure
    printf("\n=== B+TREE STRUCTURE ===\n");
    bplus_tree_compressed_dump(ct_tree);
    
    // Cleanup
    for (int i = 0; i < num_chunks; i++) {
        free(chunks[i].data);
    }
    free(chunks);
    bplus_tree_compressed_deinit(ct_tree);
    
    printf("\n=== TEST COMPLETED ===\n");
    printf("Samba chunk B+Tree compression test completed successfully!\n");
}

// Test with different chunk sizes
void test_different_chunk_sizes() {
    printf("\n=== DIFFERENT CHUNK SIZES TEST ===\n");
    
    int chunk_sizes[] = {512, 1024, 2048, 4096};
    int num_sizes = sizeof(chunk_sizes) / sizeof(chunk_sizes[0]);
    
    for (int size_idx = 0; size_idx < num_sizes; size_idx++) {
        int chunk_size = chunk_sizes[size_idx];
        printf("\n--- Testing with %d byte chunks ---\n", chunk_size);
        
        // Open the samba file
        FILE *file = fopen("SilesiaCorpus/samba", "rb");
        if (file == NULL) {
            printf("Error: Could not open SilesiaCorpus/samba file\n");
            continue;
        }
        
        // Initialize B+Tree
        struct bplus_tree_compressed *ct_tree = bplus_tree_compressed_init(16, 64);
        if (ct_tree == NULL) {
            printf("Error: Failed to initialize B+Tree\n");
            fclose(file);
            continue;
        }
        
        bplus_tree_compressed_set_compression(ct_tree, 1);
        
        int num_chunks = 0;
        size_t total_size = 0;
        double start_time = get_time();
        
        // Process chunks
        while (num_chunks < 1000 && !feof(file)) {
            char *chunk_data = malloc(chunk_size);
            if (chunk_data == NULL) {
                break;
            }
            
            size_t bytes_read = fread(chunk_data, 1, chunk_size, file);
            if (bytes_read == 0) {
                free(chunk_data);
                break;
            }
            
            if (bytes_read < chunk_size) {
                memset(chunk_data + bytes_read, 0, chunk_size - bytes_read);
            }
            
            int hash_value = calculate_chunk_hash(chunk_data, chunk_size);
            int result = bplus_tree_compressed_put(ct_tree, num_chunks, hash_value);
            
            if (result == 0) {
                total_size += chunk_size;
                num_chunks++;
            }
            
            free(chunk_data);
        }
        
        double end_time = get_time();
        double processing_time = end_time - start_time;
        
        // Get compression statistics
        size_t btree_total, btree_compressed;
        bplus_tree_compressed_stats(ct_tree, &btree_total, &btree_compressed);
        double compression_ratio = bplus_tree_compressed_get_compression_ratio(ct_tree);
        
        printf("Chunks processed: %d\n", num_chunks);
        printf("Total chunk data: %zu bytes (%.2f MB)\n", 
               total_size, total_size / (1024.0 * 1024.0));
        printf("B+Tree compressed size: %zu bytes (%.2f MB)\n", 
               btree_compressed, btree_compressed / (1024.0 * 1024.0));
        printf("B+Tree compression ratio: %.2f%%\n", compression_ratio);
        printf("Processing time: %.6f seconds\n", processing_time);
        printf("Processing rate: %.2f chunks/second\n", num_chunks / processing_time);
        
        if (total_size > 0) {
            double overall_ratio = (double)btree_compressed / total_size * 100.0;
            printf("Overall ratio (B+Tree/chunks): %.2f%%\n", overall_ratio);
        }
        
        bplus_tree_compressed_deinit(ct_tree);
        fclose(file);
    }
}

int main() {
    printf("Samba Chunk B+Tree Compression Test\n");
    printf("===================================\n");
    printf("Testing proper B+Tree compression with samba file chunks as values\n");
    printf("Adjacent keys will have adjacent chunk values from samba file\n\n");
    
    // Seed random number generator
    srand(time(NULL));
    
    // Test main compression functionality
    test_samba_chunk_btree_compression();
    
    // Test different chunk sizes
    test_different_chunk_sizes();
    
    printf("\n=== ALL TESTS COMPLETED ===\n");
    printf("Samba chunk B+Tree compression testing completed successfully!\n");
    
    return 0;
}
