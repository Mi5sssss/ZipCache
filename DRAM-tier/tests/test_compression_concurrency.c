// Comprehensive test for compression/decompression and concurrency
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include "bplustree_compressed.h"

// Test configuration
#define NUM_KEYS_PER_THREAD 1000
#define NUM_THREADS 4
#define RANDOM_BYTES 64   // First 64 bytes random (low compression)
#define ZERO_BYTES 448    // Rest zeros (high compression)
#define TOTAL_VALUE_SIZE (RANDOM_BYTES + ZERO_BYTES)

// Thread argument structure
struct thread_args {
    struct bplus_tree_compressed *tree;
    int thread_id;
    int start_key;
    int num_keys;
    int *success_count;
    pthread_mutex_t *stats_lock;
};

// Generate synthetic value with controlled compression ratio
// First RANDOM_BYTES are random, rest are zeros
void generate_synthetic_value(int seed, char *buffer, int size) {
    srand(seed);

    // Random bytes at the beginning (incompressible)
    for (int i = 0; i < RANDOM_BYTES && i < size; i++) {
        buffer[i] = (char)(rand() % 256);
    }

    // Zeros for the rest (highly compressible)
    for (int i = RANDOM_BYTES; i < size; i++) {
        buffer[i] = 0;
    }
}

// Verify synthetic value matches what we generated
int verify_synthetic_value(int seed, const char *buffer, int size) {
    char expected[TOTAL_VALUE_SIZE];
    generate_synthetic_value(seed, expected, size);
    return memcmp(buffer, expected, size) == 0;
}

// Worker thread function
void *worker_thread(void *arg) {
    struct thread_args *args = (struct thread_args *)arg;
    int local_success = 0;

    printf("Thread %d: Starting insertion of %d keys from key %d\n",
           args->thread_id, args->num_keys, args->start_key);

    // Insert phase
    for (int i = 0; i < args->num_keys; i++) {
        key_t key = args->start_key + i;

        // For testing, we'll use the key as the seed for value generation
        // In real scenario, value would be the synthetic data, but since our
        // API only supports int values, we'll just store the seed/key
        int ret = bplus_tree_compressed_put(args->tree, key, key + 1000);
        if (ret == 0) {
            local_success++;
        }
    }

    printf("Thread %d: Completed insertion, %d/%d succeeded\n",
           args->thread_id, local_success, args->num_keys);

    // Read verification phase
    int read_success = 0;
    for (int i = 0; i < args->num_keys; i++) {
        key_t key = args->start_key + i;
        int value = bplus_tree_compressed_get(args->tree, key);
        if (value == key + 1000) {
            read_success++;
        } else {
            printf("Thread %d: Read mismatch for key %d: got %d, expected %d\n",
                   args->thread_id, key, value, key + 1000);
        }
    }

    printf("Thread %d: Completed verification, %d/%d reads succeeded\n",
           args->thread_id, read_success, args->num_keys);

    pthread_mutex_lock(args->stats_lock);
    *args->success_count += (local_success == args->num_keys &&
                             read_success == args->num_keys) ? 1 : 0;
    pthread_mutex_unlock(args->stats_lock);

    return NULL;
}

// Test single-threaded operations
int test_single_thread(compression_algo_t algo, const char *algo_name) {
    printf("\n=== Testing Single-Thread %s ===\n", algo_name);

    struct compression_config cfg = bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    cfg.algo = algo;
    cfg.default_sub_pages = 8;

    struct bplus_tree_compressed *tree = bplus_tree_compressed_init_with_config(16, 64, &cfg);
    if (!tree) {
        fprintf(stderr, "Failed to initialize tree\n");
        return -1;
    }

    int num_keys = 500;
    printf("Inserting %d keys...\n", num_keys);

    // Insert keys
    for (int i = 0; i < num_keys; i++) {
        int ret = bplus_tree_compressed_put(tree, i, i + 1000);
        if (ret != 0) {
            fprintf(stderr, "Insert failed for key %d\n", i);
            bplus_tree_compressed_deinit(tree);
            return -1;
        }

        if ((i + 1) % 100 == 0) {
            printf("  Inserted %d keys\n", i + 1);
        }
    }

    printf("Verifying %d keys...\n", num_keys);

    // Verify all keys
    int verify_success = 0;
    for (int i = 0; i < num_keys; i++) {
        int value = bplus_tree_compressed_get(tree, i);
        if (value == i + 1000) {
            verify_success++;
        } else {
            fprintf(stderr, "Verification failed for key %d: got %d, expected %d\n",
                    i, value, i + 1000);
        }
    }

    printf("Verification: %d/%d keys correct\n", verify_success, num_keys);

    // Test range scan
    printf("Testing range scan [100, 200]...\n");
    int range_value = bplus_tree_compressed_get_range(tree, 100, 200);
    printf("  Range scan returned: %d\n", range_value);

    // Get compression statistics
    size_t total_size, compressed_size;
    bplus_tree_compressed_stats(tree, &total_size, &compressed_size);
    if (total_size > 0) {
        double ratio = (double)compressed_size / (double)total_size * 100.0;
        printf("Compression stats: %zu -> %zu bytes (%.2f%%)\n",
               total_size, compressed_size, ratio);
    }

    bplus_tree_compressed_deinit(tree);

    if (verify_success == num_keys) {
        printf("✓ Single-thread %s test PASSED\n", algo_name);
        return 0;
    } else {
        printf("✗ Single-thread %s test FAILED\n", algo_name);
        return -1;
    }
}

// Test multi-threaded concurrent operations
int test_multi_thread(compression_algo_t algo, const char *algo_name) {
    printf("\n=== Testing Multi-Thread %s (%d threads) ===\n", algo_name, NUM_THREADS);

    struct compression_config cfg = bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    cfg.algo = algo;
    cfg.default_sub_pages = 16;

    struct bplus_tree_compressed *tree = bplus_tree_compressed_init_with_config(32, 128, &cfg);
    if (!tree) {
        fprintf(stderr, "Failed to initialize tree\n");
        return -1;
    }

    pthread_t threads[NUM_THREADS];
    struct thread_args args[NUM_THREADS];
    int success_count = 0;
    pthread_mutex_t stats_lock;
    pthread_mutex_init(&stats_lock, NULL);

    printf("Launching %d threads, each inserting %d keys...\n",
           NUM_THREADS, NUM_KEYS_PER_THREAD);

    // Launch worker threads
    for (int i = 0; i < NUM_THREADS; i++) {
        args[i].tree = tree;
        args[i].thread_id = i;
        args[i].start_key = i * NUM_KEYS_PER_THREAD;
        args[i].num_keys = NUM_KEYS_PER_THREAD;
        args[i].success_count = &success_count;
        args[i].stats_lock = &stats_lock;

        if (pthread_create(&threads[i], NULL, worker_thread, &args[i]) != 0) {
            fprintf(stderr, "Failed to create thread %d\n", i);
            bplus_tree_compressed_deinit(tree);
            return -1;
        }
    }

    // Wait for all threads to complete
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    printf("\nAll threads completed. Success: %d/%d threads\n",
           success_count, NUM_THREADS);

    // Additional verification: random reads from all ranges
    printf("Performing additional random verification...\n");
    int random_checks = 100;
    int random_success = 0;

    for (int i = 0; i < random_checks; i++) {
        key_t key = rand() % (NUM_THREADS * NUM_KEYS_PER_THREAD);
        int value = bplus_tree_compressed_get(tree, key);
        if (value == key + 1000) {
            random_success++;
        }
    }

    printf("Random verification: %d/%d correct\n", random_success, random_checks);

    pthread_mutex_destroy(&stats_lock);
    bplus_tree_compressed_deinit(tree);

    if (success_count == NUM_THREADS && random_success >= random_checks * 0.95) {
        printf("✓ Multi-thread %s test PASSED\n", algo_name);
        return 0;
    } else {
        printf("✗ Multi-thread %s test FAILED\n", algo_name);
        return -1;
    }
}

// Test compression effectiveness with synthetic data
void test_compression_ratio() {
    printf("\n=== Testing Compression Effectiveness ===\n");

    printf("Synthetic value structure:\n");
    printf("  - First %d bytes: Random (incompressible)\n", RANDOM_BYTES);
    printf("  - Next %d bytes: Zeros (highly compressible)\n", ZERO_BYTES);
    printf("  - Total: %d bytes\n", TOTAL_VALUE_SIZE);
    printf("  - Expected compression ratio: ~%d%%\n",
           (RANDOM_BYTES * 100) / TOTAL_VALUE_SIZE + 10);

    // Generate sample
    char sample[TOTAL_VALUE_SIZE];
    generate_synthetic_value(12345, sample, TOTAL_VALUE_SIZE);

    printf("\nSample value (first 32 bytes):\n  ");
    for (int i = 0; i < 32 && i < TOTAL_VALUE_SIZE; i++) {
        printf("%02x ", (unsigned char)sample[i]);
    }
    printf("\n");
}

int main(void) {
    srand(time(NULL));

    printf("==================================================\n");
    printf("Compression & Concurrency Test Suite\n");
    printf("==================================================\n");

    // Test compression ratio information
    test_compression_ratio();

    int failed = 0;

    // Test single-threaded LZ4
    if (test_single_thread(COMPRESS_LZ4, "LZ4") != 0) {
        failed++;
    }

    // Test single-threaded QPL
    if (test_single_thread(COMPRESS_QPL, "QPL") != 0) {
        failed++;
    }

    // Test multi-threaded LZ4
    if (test_multi_thread(COMPRESS_LZ4, "LZ4") != 0) {
        failed++;
    }

    // Test multi-threaded QPL
    if (test_multi_thread(COMPRESS_QPL, "QPL") != 0) {
        failed++;
    }

    printf("\n==================================================\n");
    if (failed == 0) {
        printf("✓ ALL TESTS PASSED\n");
        printf("==================================================\n");
        return 0;
    } else {
        printf("✗ %d TEST(S) FAILED\n", failed);
        printf("==================================================\n");
        return 1;
    }
}
