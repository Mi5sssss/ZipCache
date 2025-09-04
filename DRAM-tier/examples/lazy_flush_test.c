#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>

#include "../lib/bplustree_compressed.h"

static void sleep_ms(int ms) {
    usleep(ms * 1000);
}

int main(void) {
    printf("Lazy Flush Test (DRAM-tier)\n");
    printf("===========================\n");

    // Initialize with defaults (LZ4 + lazy compression enabled)
    struct bplus_tree_compressed *ct = bplus_tree_compressed_init(16, 32);
    assert(ct);

    // Lower the flush threshold to trigger quickly
    struct compression_config cfg;
    int rc = bplus_tree_compressed_get_config(ct, &cfg);
    assert(rc == 0);
    cfg.flush_threshold = 2; // flush when 2 buffered entries
    rc = bplus_tree_compressed_set_config(ct, &cfg);
    assert(rc == 0);

    // 1) Buffer two inserts to trigger background flush
    printf("1) Buffering inserts to trigger background flush...\n");
    rc = bplus_tree_compressed_put(ct, 10, 100);
    assert(rc == 0);
    rc = bplus_tree_compressed_put(ct, 11, 110);
    assert(rc == 0);

    // Give the background thread a moment, then force flush to be deterministic
    sleep_ms(50);
    rc = bplus_tree_compressed_flush_all_buffers(ct);
    assert(rc == 0);

    // Verify values are visible from leaf (post-flush)
    int v1 = bplus_tree_compressed_get(ct, 10);
    int v2 = bplus_tree_compressed_get(ct, 11);
    printf("   GET 10=%d (expect 100), GET 11=%d (expect 110)\n", v1, v2);
    assert(v1 == 100 && v2 == 110);

    // 2) Update a key via buffer, then flush and verify
    printf("2) Buffering update then flushing...\n");
    rc = bplus_tree_compressed_put(ct, 10, 101); // update
    assert(rc == 0);
    rc = bplus_tree_compressed_put(ct, 12, 120); // another insert to hit threshold
    assert(rc == 0);
    sleep_ms(50);
    rc = bplus_tree_compressed_flush_all_buffers(ct);
    assert(rc == 0);
    v1 = bplus_tree_compressed_get(ct, 10);
    int v3 = bplus_tree_compressed_get(ct, 12);
    printf("   GET 10=%d (expect 101), GET 12=%d (expect 120)\n", v1, v3);
    assert(v1 == 101 && v3 == 120);

    // 3) Delete a key through the buffer path, flush and verify deletion
    printf("3) Buffering delete then flushing...\n");
    rc = bplus_tree_compressed_delete(ct, 11);
    assert(rc == 0);
    rc = bplus_tree_compressed_put(ct, 13, 130); // hit threshold to enqueue
    assert(rc == 0);
    sleep_ms(50);
    rc = bplus_tree_compressed_flush_all_buffers(ct);
    assert(rc == 0);
    int d = bplus_tree_compressed_get(ct, 11);
    int k13 = bplus_tree_compressed_get(ct, 13);
    printf("   GET 11=%d (expect -1), GET 13=%d (expect 130)\n", d, k13);
    assert(d == -1 && k13 == 130);

    // Final stats
    size_t total, compressed;
    rc = bplus_tree_compressed_stats(ct, &total, &compressed);
    assert(rc == 0);
    printf("Stats: total=%zu, compressed=%zu\n", total, compressed);

    bplus_tree_compressed_deinit(ct);
    printf("\nAll lazy flush checks passed.\n");
    return 0;
}

