#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>
#include <assert.h>
#include <lz4.h>

#include "../lib/bplustree_compressed.h"

#define CHUNK_SIZE 4096  // 4KB chunks to match leaf node size
#define MAX_CHUNKS 10000 // Maximum number of chunks to process

// Timing utilities
double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

// Test compression with Silesia corpus samba file chunks
void test_silesia_compression() {
    printf("=== SILESIA CORPUS COMPRESSION TEST ===\n");
    printf("Testing LZ4 compression with samba file chunks\n");
    printf("Chunk size: %d bytes (4KB)\n", CHUNK_SIZE);
    
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
    
    // Calculate number of chunks
    int num_chunks = file_size / CHUNK_SIZE;
    if (num_chunks > MAX_CHUNKS) {
        num_chunks = MAX_CHUNKS;
        printf("Limiting to %d chunks for testing\n", MAX_CHUNKS);
    }
    
    printf("Processing %d chunks...\n", num_chunks);
    
    // Statistics
    size_t total_uncompressed_size = 0;
    size_t total_compressed_size = 0;
    int chunks_processed = 0;
    int compression_successes = 0;
    int compression_failures = 0;
    
    // Process chunks
    char chunk_buffer[CHUNK_SIZE];
    char compressed_buffer[LZ4_compressBound(CHUNK_SIZE)];
    
    double start_time = get_time();
    
    for (int i = 0; i < num_chunks; i++) {
        // Read chunk
        size_t bytes_read = fread(chunk_buffer, 1, CHUNK_SIZE, file);
        if (bytes_read == 0) {
            break;
        }
        
        // Pad with zeros if chunk is not full
        if (bytes_read < CHUNK_SIZE) {
            memset(chunk_buffer + bytes_read, 0, CHUNK_SIZE - bytes_read);
        }
        
        // Compress chunk using LZ4
        int compressed_size = LZ4_compress_default(
            chunk_buffer,
            compressed_buffer,
            CHUNK_SIZE,
            LZ4_compressBound(CHUNK_SIZE)
        );
        
        if (compressed_size > 0) {
            // Compression successful
            total_uncompressed_size += CHUNK_SIZE;
            total_compressed_size += compressed_size;
            compression_successes++;
            
            // Verify decompression
            char decompressed_buffer[CHUNK_SIZE];
            int decompressed_size = LZ4_decompress_safe(
                compressed_buffer,
                decompressed_buffer,
                compressed_size,
                CHUNK_SIZE
            );
            
            if (decompressed_size != CHUNK_SIZE) {
                printf("Error: Decompression failed for chunk %d\n", i);
                compression_failures++;
            } else if (memcmp(chunk_buffer, decompressed_buffer, CHUNK_SIZE) != 0) {
                printf("Error: Decompression data mismatch for chunk %d\n", i);
                compression_failures++;
            }
        } else {
            // Compression failed
            compression_failures++;
        }
        
        chunks_processed++;
        
        // Progress indicator
        if ((i + 1) % 1000 == 0) {
            printf("Processed %d chunks...\n", i + 1);
        }
    }
    
    double end_time = get_time();
    double total_time = end_time - start_time;
    
    fclose(file);
    
    // Calculate compression ratio
    double compression_ratio = 0.0;
    if (total_uncompressed_size > 0) {
        compression_ratio = (double)total_compressed_size / total_uncompressed_size * 100.0;
    }
    
    // Print results
    printf("\n=== COMPRESSION RESULTS ===\n");
    printf("Chunks processed: %d\n", chunks_processed);
    printf("Compression successes: %d\n", compression_successes);
    printf("Compression failures: %d\n", compression_failures);
    printf("Total uncompressed size: %zu bytes (%.2f MB)\n", 
           total_uncompressed_size, total_uncompressed_size / (1024.0 * 1024.0));
    printf("Total compressed size: %zu bytes (%.2f MB)\n", 
           total_compressed_size, total_compressed_size / (1024.0 * 1024.0));
    printf("Compression ratio: %.2f%%\n", compression_ratio);
    printf("Space saved: %.2f%%\n", 100.0 - compression_ratio);
    printf("Processing time: %.6f seconds\n", total_time);
    printf("Processing rate: %.2f chunks/second\n", chunks_processed / total_time);
    
    // Test with compressed B+Tree
    printf("\n=== COMPRESSED B+TREE TEST ===\n");
    
    struct bplus_tree_compressed *ct_tree = bplus_tree_compressed_init(16, 32);
    if (ct_tree == NULL) {
        printf("Error: Failed to initialize compressed B+Tree\n");
        return;
    }
    
    // Reopen file for B+Tree test
    file = fopen("SilesiaCorpus/samba", "rb");
    if (file == NULL) {
        printf("Error: Could not reopen SilesiaCorpus/samba file\n");
        bplus_tree_compressed_deinit(ct_tree);
        return;
    }
    
    start_time = get_time();
    int btree_chunks = 0;
    
    for (int i = 0; i < num_chunks && i < 1000; i++) { // Limit for B+Tree test
        // Read chunk
        size_t bytes_read = fread(chunk_buffer, 1, CHUNK_SIZE, file);
        if (bytes_read == 0) {
            break;
        }
        
        // Insert chunk data into B+Tree (simplified - just insert first few bytes as key-value pairs)
        for (int j = 0; j < 64 && j < bytes_read / 8; j++) {
            int key = i * 1000 + j;
            int value = *(int*)(chunk_buffer + j * 8);
            bplus_tree_compressed_put(ct_tree, key, value);
        }
        
        btree_chunks++;
    }
    
    end_time = get_time();
    double btree_time = end_time - start_time;
    
    fclose(file);
    
    // Get B+Tree compression statistics
    size_t btree_total_size, btree_compressed_size;
    bplus_tree_compressed_stats(ct_tree, &btree_total_size, &btree_compressed_size);
    double btree_compression_ratio = bplus_tree_compressed_get_compression_ratio(ct_tree);
    
    printf("B+Tree chunks processed: %d\n", btree_chunks);
    printf("B+Tree total size: %zu bytes\n", btree_total_size);
    printf("B+Tree compressed size: %zu bytes\n", btree_compressed_size);
    printf("B+Tree compression ratio: %.2f%%\n", btree_compression_ratio);
    printf("B+Tree processing time: %.6f seconds\n", btree_time);
    
    bplus_tree_compressed_deinit(ct_tree);
    
    printf("\n=== SUMMARY ===\n");
    printf("LZ4 compression with Silesia samba file chunks:\n");
    printf("- Average compression ratio: %.2f%%\n", compression_ratio);
    printf("- Space savings: %.2f%%\n", 100.0 - compression_ratio);
    printf("- Processing rate: %.2f chunks/second\n", chunks_processed / total_time);
    printf("- Compression success rate: %.2f%%\n", 
           compression_successes * 100.0 / chunks_processed);
}

// Test different compression levels
void test_compression_levels() {
    printf("\n=== COMPRESSION LEVELS TEST ===\n");
    
    // Open the samba file
    FILE *file = fopen("SilesiaCorpus/samba", "rb");
    if (file == NULL) {
        printf("Error: Could not open SilesiaCorpus/samba file\n");
        return;
    }
    
    char chunk_buffer[CHUNK_SIZE];
    char compressed_buffer[LZ4_compressBound(CHUNK_SIZE)];
    
    // Test different compression levels
    int compression_levels[] = {1, 4, 8, 12, 16};
    int num_levels = sizeof(compression_levels) / sizeof(compression_levels[0]);
    
    printf("Testing different LZ4 compression levels with samba chunks:\n");
    printf("Chunk size: %d bytes\n", CHUNK_SIZE);
    
    for (int level_idx = 0; level_idx < num_levels; level_idx++) {
        int level = compression_levels[level_idx];
        
        // Reset file position
        fseek(file, 0, SEEK_SET);
        
        size_t total_uncompressed = 0;
        size_t total_compressed = 0;
        int chunks_processed = 0;
        double start_time = get_time();
        
        // Process first 100 chunks for each level
        for (int i = 0; i < 100; i++) {
            size_t bytes_read = fread(chunk_buffer, 1, CHUNK_SIZE, file);
            if (bytes_read == 0) {
                break;
            }
            
            if (bytes_read < CHUNK_SIZE) {
                memset(chunk_buffer + bytes_read, 0, CHUNK_SIZE - bytes_read);
            }
            
            // Compress with specific level
            int compressed_size = LZ4_compress_fast(
                chunk_buffer,
                compressed_buffer,
                CHUNK_SIZE,
                LZ4_compressBound(CHUNK_SIZE),
                level
            );
            
            if (compressed_size > 0) {
                total_uncompressed += CHUNK_SIZE;
                total_compressed += compressed_size;
                chunks_processed++;
            }
        }
        
        double end_time = get_time();
        double processing_time = end_time - start_time;
        
        double compression_ratio = 0.0;
        if (total_uncompressed > 0) {
            compression_ratio = (double)total_compressed / total_uncompressed * 100.0;
        }
        
        printf("Level %2d: %.2f%% compression, %.6f seconds, %d chunks\n", 
               level, compression_ratio, processing_time, chunks_processed);
    }
    
    fclose(file);
}

int main() {
    printf("Silesia Corpus Compression Test\n");
    printf("================================\n");
    printf("Testing LZ4 compression with real data from Silesia corpus\n");
    printf("Source: https://github.com/MiloszKrajewski/SilesiaCorpus.git\n\n");
    
    // Test basic compression with samba file
    test_silesia_compression();
    
    // Test different compression levels
    test_compression_levels();
    
    printf("\n=== TEST COMPLETED ===\n");
    printf("Real compression testing with Silesia corpus completed successfully!\n");
    
    return 0;
}
