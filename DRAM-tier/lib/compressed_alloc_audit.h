/* Optional process-wide allocation audit for this translation unit.
 * Diagnostic records are out-of-band: object allocation sizes are unchanged.
 * Enable before creating any trees. Never use audit timings as performance data.
 */
#ifndef COMPRESSED_ALLOC_AUDIT_H
#define COMPRESSED_ALLOC_AUDIT_H
#ifdef ZIPCACHE_AGG_EXPERIMENT
static __thread long allocation_failure_countdown = -1;
void bplus_tree_compressed_test_fail_allocation(long after)
{ allocation_failure_countdown = after; }
static int fail_this_allocation(void)
{
    if (allocation_failure_countdown < 0) return 0;
    if (allocation_failure_countdown-- == 0) { errno = ENOMEM; return 1; }
    return 0;
}
#else
#define fail_this_allocation() 0
#endif
struct audit_record {
    void *ptr;
    size_t requested, usable;
    struct audit_record *next;
};
static struct audit_record *audit_table[8192];
static pthread_mutex_t audit_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_once_t audit_once = PTHREAD_ONCE_INIT;
static int audit_enabled;
static struct bplus_tree_allocation_audit audit_totals;
static size_t allocation_usable_size(const void *ptr, size_t requested);

static void audit_init(void)
{
    const char *env = getenv("BTREE_MEMORY_AUDIT");
    audit_enabled = env && !strcmp(env, "1");
}

static void audit_add(void *ptr, size_t size)
{
    pthread_once(&audit_once, audit_init);
    if (!audit_enabled || !ptr) return;
    struct audit_record *record = malloc(sizeof(*record));
    if (!record) abort(); /* Do not silently return an incomplete audit. */
    *record = (struct audit_record){ptr, size, allocation_usable_size(ptr, size), NULL};
    size_t bucket = ((uintptr_t)ptr >> 4) % 8192;
    pthread_mutex_lock(&audit_lock);
    record->next = audit_table[bucket];
    audit_table[bucket] = record;
    audit_totals.requested += record->requested;
    audit_totals.usable += record->usable;
    audit_totals.live_allocations++;
    audit_totals.tracker_bytes += allocation_usable_size(record, sizeof(*record));
    if (audit_totals.requested > audit_totals.peak_requested)
        audit_totals.peak_requested = audit_totals.requested;
    if (audit_totals.usable > audit_totals.peak_usable)
        audit_totals.peak_usable = audit_totals.usable;
    pthread_mutex_unlock(&audit_lock);
}

/* Also used when the generic tree destructor will perform the actual free. */
static void audit_forget(void *ptr)
{
    if (!audit_enabled || !ptr) return;
    size_t bucket = ((uintptr_t)ptr >> 4) % 8192;
    pthread_mutex_lock(&audit_lock);
    struct audit_record **link = &audit_table[bucket];
    while (*link && (*link)->ptr != ptr) link = &(*link)->next;
    if (*link) {
        struct audit_record *record = *link;
        *link = record->next;
        audit_totals.requested -= record->requested;
        audit_totals.usable -= record->usable;
        audit_totals.live_allocations--;
        audit_totals.tracker_bytes -= allocation_usable_size(record, sizeof(*record));
        free(record);
    }
    pthread_mutex_unlock(&audit_lock);
}

static void *audit_malloc(size_t size)
{
    if (fail_this_allocation()) return NULL;
    void *ptr = malloc(size);
    audit_add(ptr, size);
    return ptr;
}
static void *audit_calloc(size_t count, size_t size)
{
    if (fail_this_allocation()) return NULL;
    void *ptr = calloc(count, size);
    if (ptr) audit_add(ptr, count * size);
    return ptr;
}
static void audit_free(void *ptr) { audit_forget(ptr); free(ptr); }
static void *audit_realloc(void *old, size_t size)
{
    if (fail_this_allocation()) return NULL;
    pthread_once(&audit_once, audit_init);
    if (!audit_enabled) return realloc(old, size);
    if (!old) return audit_malloc(size);
    if (!size) { audit_free(old); return NULL; }
    pthread_mutex_lock(&audit_lock);
    size_t bucket = ((uintptr_t)old >> 4) % 8192;
    struct audit_record **link = &audit_table[bucket];
    while (*link && (*link)->ptr != old) link = &(*link)->next;
    assert(*link); /* All realloc callers in this module own audited pointers. */
    struct audit_record *record = *link;
    void *ptr = realloc(old, size);
    if (ptr) {
        *link = record->next;
        audit_totals.requested -= record->requested;
        audit_totals.usable -= record->usable;
        record->ptr = ptr; record->requested = size;
        record->usable = allocation_usable_size(ptr, size);
        bucket = ((uintptr_t)ptr >> 4) % 8192;
        record->next = audit_table[bucket]; audit_table[bucket] = record;
        audit_totals.requested += record->requested;
        audit_totals.usable += record->usable;
        if (audit_totals.requested > audit_totals.peak_requested)
            audit_totals.peak_requested = audit_totals.requested;
        if (audit_totals.usable > audit_totals.peak_usable)
            audit_totals.peak_usable = audit_totals.usable;
    }
    pthread_mutex_unlock(&audit_lock);
    return ptr;
}
void bplus_tree_compressed_allocation_audit(struct bplus_tree_allocation_audit *out,
                                           int reset_peak)
{
    pthread_once(&audit_once, audit_init);
    pthread_mutex_lock(&audit_lock);
    if (reset_peak) {
        audit_totals.peak_requested = audit_totals.requested;
        audit_totals.peak_usable = audit_totals.usable;
    }
    if (out) {
        *out = audit_totals;
        out->enabled = audit_enabled;
        out->tracker_bytes += sizeof(audit_table);
    }
    pthread_mutex_unlock(&audit_lock);
}
#define malloc audit_malloc
#define calloc audit_calloc
#define realloc audit_realloc
#define free audit_free
#endif
