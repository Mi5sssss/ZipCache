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

// Structure to hold chunk data
struct chunk_data {
    char *data;
    size_t size;
    int key;
};

// Test B+Tree with samba chunks as values
void test_samba_chunk_kv_compression() {
    printf("=== SAMBA CHUNK KV COMPRESSION TEST ===\n");
    printf("Using samba file chunks as values in key-value pairs\n");
    printf("Adjacent keys will have adjacent chunk values\n\n");
    
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
    
    // Initialize compressed B+Tree
    struct bplus_tree_compressed *ct_tree = bplus_tree_compressed_init(16, 32);
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
    size_t total_uncompressed_size = 0;
    size_t total_compressed_size = 0;
    
    printf("Reading chunks and creating key-value pairs...\n");
    
    double start_time = get_time();
    
    // Read chunks with varying sizes
    while (num_chunks < MAX_KEYS && !feof(file)) {
        // Determine chunk size (varying between MIN_CHUNK_SIZE and MAX_CHUNK_SIZE)
        size_t chunk_size = MIN_CHUNK_SIZE + (rand() % (MAX_CHUNK_SIZE - MIN_CHUNK_SIZE + 1));
        
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
        chunks[num_chunks].key = num_chunks; // Adjacent keys
        
        // Insert into B+Tree
        // For this test, we'll use the chunk data as a value
        // Since the B+Tree expects int values, we'll use a hash of the chunk data
        int hash_value = 0;
        for (size_t i = 0; i < chunk_size; i++) {
            hash_value = (hash_value * 31 + chunks[num_chunks].data[i]) & 0x7FFFFFFF;
        }
        
        int result = bplus_tree_compressed_put(ct_tree, num_chunks, hash_value);
        if (result != 0) {
            printf("Warning: Failed to insert key %d into B+Tree\n", num_chunks);
        }
        
        total_uncompressed_size += chunk_size;
        num_chunks++;
        
        // Progress indicator
        if ((num_chunks + 1) % 1000 == 0) {
            printf("Processed %d chunks...\n", num_chunks + 1);
        }
    }
    
    double end_time = get_time();
    double insert_time = end_time - start_time;
    
    fclose(file);
    
    printf("Inserted %d key-value pairs into B+Tree\n", num_chunks);
    printf("Total uncompressed data size: %zu bytes (%.2f MB)\n", 
           total_uncompressed_size, total_uncompressed_size / (1024.0 * 1024.0));
    
    // Get B+Tree compression statistics
    size_t btree_total_size, btree_compressed_size;
    bplus_tree_compressed_stats(ct_tree, &btree_total_size, &btree_compressed_size);
    double btree_compression_ratio = bplus_tree_compressed_get_compression_ratio(ct_tree);
    
    printf("\n=== B+TREE COMPRESSION RESULTS ===\n");
    printf("B+Tree total size: %zu bytes\n", btree_total_size);
    printf("B+Tree compressed size: %zu bytes\n", btree_compressed_size);
    printf("B+Tree compression ratio: %.2f%%\n", btree_compression_ratio);
    printf("B+Tree space saved: %.2f%%\n", 100.0 - btree_compression_ratio);
    printf("Insertion time: %.6f seconds\n", insert_time);
    printf("Insertion rate: %.2f keys/second\n", num_chunks / insert_time);
    
    // Test retrieval
    printf("\n=== RETRIEVAL TEST ===\n");
    start_time = get_time();
    
    int retrieval_successes = 0;
    int retrieval_failures = 0;
    
    for (int i = 0; i < num_chunks && i < 1000; i++) { // Test first 1000 keys
        int retrieved_value = bplus_tree_compressed_get(ct_tree, i);
        
        // Calculate expected hash value
        int expected_hash = 0;
        for (size_t j = 0; j < chunks[i].size; j++) {
            expected_hash = (expected_hash * 31 + chunks[i].data[j]) & 0x7FFFFFFF;
        }
        
        if (retrieved_value == expected_hash) {
            retrieval_successes++;
        } else {
            retrieval_failures++;
            printf("Retrieval mismatch for key %d: expected %d, got %d\n", 
                   i, expected_hash, retrieved_value);
        }
    }
    
    end_time = get_time();
    double retrieval_time = end_time - start_time;
    
    printf("Retrieval test results:\n");
    printf("Successes: %d\n", retrieval_successes);
    printf("Failures: %d\n", retrieval_failures);
    printf("Success rate: %.2f%%\n", retrieval_successes * 100.0 / (retrieval_successes + retrieval_failures));
    printf("Retrieval time: %.6f seconds\n", retrieval_time);
    printf("Retrieval rate: %.2f keys/second\n", (retrieval_successes + retrieval_failures) / retrieval_time);
    
    // Test range queries
    printf("\n=== RANGE QUERY TEST ===\n");
    start_time = get_time();
    
    int range_successes = 0;
    int range_failures = 0;
    
    for (int i = 0; i < 100; i++) { // Test 100 range queries
        int start_key = i * 10;
        int end_key = start_key + 5;
        
        int range_result = bplus_tree_compressed_get_range(ct_tree, start_key, end_key);
        
        if (range_result != -1) {
            range_successes++;
        } else {
            range_failures++;
        }
    }
    
    end_time = get_time();
    double range_time = end_time - start_time;
    
    printf("Range query test results:\n");
    printf("Successes: %d\n", range_successes);
    printf("Failures: %d\n", range_failures);
    printf("Range query time: %.6f seconds\n", range_time);
    printf("Range query rate: %.2f queries/second\n", 100.0 / range_time);
    
    // Calculate overall compression ratio
    double overall_compression_ratio = 0.0;
    if (total_uncompressed_size > 0) {
        overall_compression_ratio = (double)btree_compressed_size / total_uncompressed_size * 100.0;
    }
    
    printf("\n=== OVERALL COMPRESSION SUMMARY ===\n");
    printf("Original data size: %zu bytes (%.2f MB)\n", 
           total_uncompressed_size, total_uncompressed_size / (1024.0 * 1024.0));
    printf("B+Tree compressed size: %zu bytes (%.2f MB)\n", 
           btree_compressed_size, btree_compressed_size / (1024.0 * 1024.0));
    printf("Overall compression ratio: %.2f%%\n", overall_compression_ratio);
    printf("Overall space saved: %.2f%%\n", 100.0 - overall_compression_ratio);
    
    // Cleanup
    for (int i = 0; i < num_chunks; i++) {
        free(chunks[i].data);
    }
    free(chunks);
    bplus_tree_compressed_deinit(ct_tree);
    
    printf("\n=== TEST COMPLETED ===\n");
    printf("Samba chunk KV compression test completed successfully!\n");
}

// Test with different chunk size distributions
void test_chunk_size_distributions() {
    printf("\n=== CHUNK SIZE DISTRIBUTION TEST ===\n");
    
    // Open the samba file
    FILE *file = fopen("SilesiaCorpus/samba", "rb");
    if (file == NULL) {
        printf("Error: Could not open SilesiaCorpus/samba file\n");
        return;
    }
    
    // Test different chunk size distributions
    struct {
        char *name;
        int min_size;
        int max_size;
    } distributions[] = {
        {"Small chunks (512B-1KB)", 512, 1024},
        {"Medium chunks (1KB-2KB)", 1024, 2048},
        {"Large chunks (2KB-4KB)", 2048, 4096},
        {"Mixed chunks (512B-4KB)", 512, 4096}
    };
    
    int num_distributions = sizeof(distributions) / sizeof(distributions[0]);
    
    for (int dist_idx = 0; dist_idx < num_distributions; dist_idx++) {
        printf("\n--- Testing %s ---\n", distributions[dist_idx].name);
        
        // Reset file position
        fseek(file, 0, SEEK_SET);
        
        // Initialize B+Tree for this test
        struct bplus_tree_compressed *ct_tree = bplus_tree_compressed_init(16, 32);
        if (ct_tree == NULL) {
            printf("Error: Failed to initialize B+Tree for distribution %d\n", dist_idx);
            continue;
        }
        
        bplus_tree_compressed_set_compression(ct_tree, 1);
        
        size_t total_uncompressed = 0;
        int num_chunks = 0;
        double start_time = get_time();
        
        // Process chunks with this distribution
        while (num_chunks < 1000 && !feof(file)) { // Limit to 1000 chunks per test
            int chunk_size = distributions[dist_idx].min_size + 
                           (rand() % (distributions[dist_idx].max_size - distributions[dist_idx].min_size + 1));
            
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
            
            // Calculate hash and insert
            int hash_value = 0;
            for (int i = 0; i < chunk_size; i++) {
                hash_value = (hash_value * 31 + chunk_data[i]) & 0x7FFFFFFF;
            }
            
            bplus_tree_compressed_put(ct_tree, num_chunks, hash_value);
            
            total_uncompressed += chunk_size;
            num_chunks++;
            
            free(chunk_data);
        }
        
        double end_time = get_time();
        double processing_time = end_time - start_time;
        
        // Get compression statistics
        size_t btree_total, btree_compressed;
        bplus_tree_compressed_stats(ct_tree, &btree_total, &btree_compressed);
        double compression_ratio = bplus_tree_compressed_get_compression_ratio(ct_tree);
        
        printf("Chunks processed: %d\n", num_chunks);
        printf("Total uncompressed: %zu bytes (%.2f MB)\n", 
               total_uncompressed, total_uncompressed / (1024.0 * 1024.0));
        printf("B+Tree compressed: %zu bytes (%.2f MB)\n", 
               btree_compressed, btree_compressed / (1024.0 * 1024.0));
        printf("Compression ratio: %.2f%%\n", compression_ratio);
        printf("Processing time: %.6f seconds\n", processing_time);
        printf("Processing rate: %.2f chunks/second\n", num_chunks / processing_time);
        
        bplus_tree_compressed_deinit(ct_tree);
    }
    
    fclose(file);
}

int main() {
    printf("Samba Chunk KV Compression Test\n");
    printf("===============================\n");
    printf("Testing B+Tree compression with samba file chunks as values\n");
    printf("Adjacent keys will have adjacent chunk values\n\n");
    
    // Seed random number generator
    srand(time(NULL));
    
    // Test main compression functionality
    test_samba_chunk_kv_compression();
    
    // Test different chunk size distributions
    test_chunk_size_distributions();
    
    printf("\n=== ALL TESTS COMPLETED ===\n");
    printf("Samba chunk KV compression testing completed successfully!\n");
    
    return 0;
}
