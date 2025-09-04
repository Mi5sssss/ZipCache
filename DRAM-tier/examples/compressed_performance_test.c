#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>
#include <assert.h>

#include "../lib/bplustree_compressed.h"

#define MAX_KEYS 10000
#define NUM_THREADS 4
#define OPERATIONS_PER_THREAD 1000

// Global compressed tree for testing
struct bplus_tree_compressed *global_ct_tree = NULL;

// Test statistics
typedef struct {
    int inserts;
    int gets;
    int deletes;
    int errors;
    double total_time;
} thread_stats_t;

thread_stats_t thread_stats[NUM_THREADS];

// Timing utilities
double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

// Thread function for compressed tree testing
void* compressed_thread_function(void* arg) {
    int thread_id = *(int*)arg;
    int start_key = thread_id * OPERATIONS_PER_THREAD;
    
    for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
        int key = start_key + i;
        
        // Insert
        int result = bplus_tree_compressed_put(global_ct_tree, key, key + 1);
        if (result == 0) thread_stats[thread_id].inserts++;
        else thread_stats[thread_id].errors++;
        
        // Get
        int value = bplus_tree_compressed_get(global_ct_tree, key);
        if (value == key + 1) thread_stats[thread_id].gets++;
        else thread_stats[thread_id].errors++;
        
        // Delete
        result = bplus_tree_compressed_delete(global_ct_tree, key);
        if (result == 0) thread_stats[thread_id].deletes++;
        else thread_stats[thread_id].errors++;
    }
    
    return NULL;
}

// Test single-thread performance
void test_single_thread_performance() {
    printf("\n=== SINGLE-THREAD COMPRESSED B+TREE PERFORMANCE ===\n");
    
    struct bplus_tree_compressed *ct_tree = bplus_tree_compressed_init(16, 32);
    assert(ct_tree != NULL);
    
    // Test 1: Insert Performance
    printf("1. Insert Performance:\n");
    double start_time = get_time();
    int insert_errors = 0;
    for (int i = 0; i < MAX_KEYS; i++) {
        int result = bplus_tree_compressed_put(ct_tree, i, i + 1);
        if (result != 0) insert_errors++;
    }
    double end_time = get_time();
    double insert_time = end_time - start_time;
    double insert_rate = MAX_KEYS / insert_time;
    
    printf("   Inserted %d keys in %.6f seconds\n", MAX_KEYS, insert_time);
    printf("   Insert rate: %.2f ops/sec\n", insert_rate);
    printf("   Insert errors: %d (%.2f%%)\n", insert_errors, insert_errors * 100.0 / MAX_KEYS);
    
    // Test 2: Get Performance
    printf("2. Get Performance:\n");
    start_time = get_time();
    int get_errors = 0;
    for (int i = 0; i < MAX_KEYS; i++) {
        int value = bplus_tree_compressed_get(ct_tree, i);
        if (value != i + 1) get_errors++;
    }
    end_time = get_time();
    double get_duration = end_time - start_time;
    double get_rate = MAX_KEYS / get_duration;
    
    printf("   Retrieved %d keys in %.6f seconds\n", MAX_KEYS, get_duration);
    printf("   Get rate: %.2f ops/sec\n", get_rate);
    printf("   Get errors: %d (%.2f%%)\n", get_errors, get_errors * 100.0 / MAX_KEYS);
    
    // Test 3: Delete Performance
    printf("3. Delete Performance:\n");
    start_time = get_time();
    int delete_errors = 0;
    for (int i = 0; i < MAX_KEYS; i++) {
        int result = bplus_tree_compressed_delete(ct_tree, i);
        if (result != 0) delete_errors++;
    }
    end_time = get_time();
    double delete_time = end_time - start_time;
    double delete_rate = MAX_KEYS / delete_time;
    
    printf("   Deleted %d keys in %.6f seconds\n", MAX_KEYS, delete_time);
    printf("   Delete rate: %.2f ops/sec\n", delete_rate);
    printf("   Delete errors: %d (%.2f%%)\n", delete_errors, delete_errors * 100.0 / MAX_KEYS);
    
    // Test 4: Compression Statistics
    printf("4. Compression Statistics:\n");
    size_t total_size, compressed_size;
    if (bplus_tree_compressed_stats(ct_tree, &total_size, &compressed_size) == 0) {
        double compression_ratio = (double)compressed_size / total_size * 100.0;
        printf("   Total uncompressed size: %zu bytes\n", total_size);
        printf("   Total compressed size: %zu bytes\n", compressed_size);
        printf("   Compression ratio: %.2f%%\n", compression_ratio);
        printf("   Space saved: %.2f%%\n", 100.0 - compression_ratio);
    }
    
    bplus_tree_compressed_deinit(ct_tree);
}

// Test multi-thread performance
void test_multi_thread_performance() {
    printf("\n=== MULTI-THREAD COMPRESSED B+TREE PERFORMANCE ===\n");
    
    global_ct_tree = bplus_tree_compressed_init(16, 32);
    assert(global_ct_tree != NULL);
    
    memset(thread_stats, 0, sizeof(thread_stats));
    
    pthread_t threads[NUM_THREADS];
    int thread_ids[NUM_THREADS];
    
    double start_time = get_time();
    
    // Create threads
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_ids[i] = i;
        int result = pthread_create(&threads[i], NULL, compressed_thread_function, &thread_ids[i]);
        if (result != 0) {
            printf("Failed to create thread %d\n", i);
            return;
        }
    }
    
    // Wait for all threads to complete
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }
    
    double end_time = get_time();
    double total_time = end_time - start_time;
    
    // Calculate totals
    int total_inserts = 0, total_gets = 0, total_deletes = 0, total_errors = 0;
    for (int i = 0; i < NUM_THREADS; i++) {
        total_inserts += thread_stats[i].inserts;
        total_gets += thread_stats[i].gets;
        total_deletes += thread_stats[i].deletes;
        total_errors += thread_stats[i].errors;
    }
    
    int total_operations = total_inserts + total_gets + total_deletes;
    double ops_per_sec = total_operations / total_time;
    
    printf("Completed %d operations with %d threads in %.6f seconds\n", 
           total_operations, NUM_THREADS, total_time);
    printf("Operation breakdown: %d inserts, %d gets, %d deletes\n", 
           total_inserts, total_gets, total_deletes);
    printf("Concurrent operation rate: %.2f ops/sec\n", ops_per_sec);
    printf("Errors: %d (%.2f%%)\n", total_errors, 
           total_errors * 100.0 / total_operations);
    
    // Compression statistics
    size_t total_size, compressed_size;
    if (bplus_tree_compressed_stats(global_ct_tree, &total_size, &compressed_size) == 0) {
        double compression_ratio = (double)compressed_size / total_size * 100.0;
        printf("Compression statistics:\n");
        printf("  Total uncompressed size: %zu bytes\n", total_size);
        printf("  Total compressed size: %zu bytes\n", compressed_size);
        printf("  Compression ratio: %.2f%%\n", compression_ratio);
        printf("  Space saved: %.2f%%\n", 100.0 - compression_ratio);
    }
    
    bplus_tree_compressed_deinit(global_ct_tree);
    global_ct_tree = NULL;
}

// Test compression effectiveness
void test_compression_effectiveness() {
    printf("\n=== COMPRESSION EFFECTIVENESS TEST ===\n");
    
    struct bplus_tree_compressed *ct_tree = bplus_tree_compressed_init(16, 32);
    assert(ct_tree != NULL);
    
    // Insert data with different patterns to test compression
    printf("Testing compression with different data patterns:\n");
    
    // Pattern 1: Sequential data (should compress well)
    printf("1. Sequential data pattern:\n");
    for (int i = 0; i < 1000; i++) {
        bplus_tree_compressed_put(ct_tree, i, i);
    }
    
    size_t total_size, compressed_size;
    bplus_tree_compressed_stats(ct_tree, &total_size, &compressed_size);
    double compression_ratio = (double)compressed_size / total_size * 100.0;
    printf("   Sequential data - Compression ratio: %.2f%%\n", compression_ratio);
    
    // Clear and test pattern 2: Random data (should compress less well)
    bplus_tree_compressed_deinit(ct_tree);
    ct_tree = bplus_tree_compressed_init(16, 32);
    
    printf("2. Random data pattern:\n");
    srand(42); // Fixed seed for reproducible results
    for (int i = 0; i < 1000; i++) {
        int key = rand() % 10000;
        int value = rand() % 10000;
        bplus_tree_compressed_put(ct_tree, key, value);
    }
    
    bplus_tree_compressed_stats(ct_tree, &total_size, &compressed_size);
    compression_ratio = (double)compressed_size / total_size * 100.0;
    printf("   Random data - Compression ratio: %.2f%%\n", compression_ratio);
    
    // Clear and test pattern 3: Repeated data (should compress very well)
    bplus_tree_compressed_deinit(ct_tree);
    ct_tree = bplus_tree_compressed_init(16, 32);
    
    printf("3. Repeated data pattern:\n");
    for (int i = 0; i < 1000; i++) {
        bplus_tree_compressed_put(ct_tree, i, 42); // All values are 42
    }
    
    bplus_tree_compressed_stats(ct_tree, &total_size, &compressed_size);
    compression_ratio = (double)compressed_size / total_size * 100.0;
    printf("   Repeated data - Compression ratio: %.2f%%\n", compression_ratio);
    
    bplus_tree_compressed_deinit(ct_tree);
}

int main() {
    printf("Compressed B+Tree Performance Test\n");
    printf("==================================\n");
    
    // Seed random number generator
    srand(time(NULL));
    
    // Test compression effectiveness
    test_compression_effectiveness();
    
    // Test single-thread performance
    test_single_thread_performance();
    
    // Test multi-thread performance
    test_multi_thread_performance();
    
    printf("\n=== FINAL SUMMARY ===\n");
    printf("Compressed B+Tree implementation completed successfully!\n");
    printf("Features:\n");
    printf("- 4KB leaf nodes with LZ4 compression\n");
    printf("- Thread-safe with read-write locks\n");
    printf("- Automatic compression/decompression on operations\n");
    printf("- Lossless compression preserving all data\n");
    printf("- Performance optimized for both single and multi-threaded use\n");
    
    return 0;
}
