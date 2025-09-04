#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>

#define ZIPCACHE_DISABLE_CLASSIFY_SIZE_COMPAT 1
#include "../zipcache.h"

static void ensure_tmp_dir(void) {
    mkdir("/tmp/zipcache_test", 0755);
}

int main(void) {
    ensure_tmp_dir();
    const char *ssd_path = "/tmp/zipcache_test/thresholds_ssd";

    printf("ZipCache Thresholds Focused Test\n");
    printf("================================\n");

    // Init with defaults
    zipcache_t *cache = zipcache_init(32, ssd_path);
    assert(cache);

    // 1) Default classification boundaries (instance-aware call)
    assert(zipcache_classify_object(cache, 64) == ZIPCACHE_OBJ_TINY);
    assert(zipcache_classify_object(cache, 128) == ZIPCACHE_OBJ_TINY);
    assert(zipcache_classify_object(cache, 129) == ZIPCACHE_OBJ_MEDIUM);
    assert(zipcache_classify_object(cache, 2048) == ZIPCACHE_OBJ_MEDIUM);
    assert(zipcache_classify_object(cache, 2049) == ZIPCACHE_OBJ_LARGE);
    printf("✓ Default classification boundaries OK\n");

    // 2) Change thresholds at runtime
    int rc = zipcache_set_thresholds(cache, 64, 1024);
    assert(rc == ZIPCACHE_OK);

    size_t tiny_cur = 0, med_cur = 0;
    zipcache_get_thresholds(cache, &tiny_cur, &med_cur);
    assert(tiny_cur == 64 && med_cur == 1024);

    // Re-check boundaries with instance thresholds
    assert(zipcache_classify_object(cache, 64) == ZIPCACHE_OBJ_TINY);
    assert(zipcache_classify_object(cache, 65) == ZIPCACHE_OBJ_MEDIUM);
    assert(zipcache_classify_object(cache, 1024) == ZIPCACHE_OBJ_MEDIUM);
    assert(zipcache_classify_object(cache, 1025) == ZIPCACHE_OBJ_LARGE);
    printf("✓ Runtime classification boundaries OK (64/1024)\n");

    // 3) Routing by size: update stats to verify puts_* counters
    zipcache_stats_t stats_before = {0};
    zipcache_get_stats(cache, &stats_before);

    char buf_tiny[32] = {0};
    char buf_med[512] = {0};
    char buf_large[4096] = {0};

    zipcache_result_t r1 = zipcache_put(cache, "k_tiny", buf_tiny, sizeof(buf_tiny));
    assert(r1 == ZIPCACHE_OK);
    zipcache_result_t r2 = zipcache_put(cache, "k_medium", buf_med, sizeof(buf_med));
    assert(r2 == ZIPCACHE_OK);
    zipcache_result_t r3 = zipcache_put(cache, "k_large", buf_large, sizeof(buf_large));
    assert(r3 == ZIPCACHE_OK);

    zipcache_stats_t stats_after = {0};
    zipcache_get_stats(cache, &stats_after);

    assert(stats_after.puts_tiny == stats_before.puts_tiny + 1);
    assert(stats_after.puts_medium == stats_before.puts_medium + 1);
    assert(stats_after.puts_large == stats_before.puts_large + 1);
    printf("✓ Routing counters updated (tiny/medium/large)\n");

    zipcache_destroy(cache);
    printf("All threshold tests passed.\n");
    return 0;
}
