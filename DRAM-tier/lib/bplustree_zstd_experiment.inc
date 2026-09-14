/* CPU comparator: reusable, audited TLS workspaces, counted once per process. */
#ifdef HAVE_ZSTD
static size_t zstd_live_bytes;
struct zstd_state { ZSTD_CCtx *compress; ZSTD_DCtx *decompress; };
static pthread_key_t zstd_key;
static pthread_once_t zstd_once = PTHREAD_ONCE_INIT;
static int zstd_key_ready;
static void *zstd_allocate(void *opaque, size_t n)
{
    (void)opaque; void *p = malloc(n);
    if (p) __atomic_add_fetch(&zstd_live_bytes, allocation_usable_size(p,n), __ATOMIC_RELAXED);
    return p;
}
static void zstd_release(void *opaque, void *p)
{
    (void)opaque;
    if (p) __atomic_sub_fetch(&zstd_live_bytes, allocation_usable_size(p,0), __ATOMIC_RELAXED);
    free(p);
}
static void zstd_destroy(void *p)
{
    struct zstd_state *s = p; if (!s) return;
    ZSTD_freeCCtx(s->compress); ZSTD_freeDCtx(s->decompress); zstd_release(NULL,s);
}
static void zstd_key_init(void) { zstd_key_ready = pthread_key_create(&zstd_key,zstd_destroy)==0; }
static struct zstd_state *zstd_get(void)
{
    pthread_once(&zstd_once,zstd_key_init); if (!zstd_key_ready) return NULL;
    struct zstd_state *s=pthread_getspecific(zstd_key);
    if (!s) {
        s=zstd_allocate(NULL,sizeof(*s)); if (!s) return NULL;
        memset(s,0,sizeof(*s));
        if (pthread_setspecific(zstd_key,s)) { zstd_release(NULL,s); return NULL; }
    }
    return s;
}
static int zstd_transform(int compressing, const uint8_t *src, uint32_t n, uint8_t *dst, uint32_t cap)
{
    struct zstd_state *s=zstd_get(); if (!s) { errno=ENOMEM; return -1; }
    ZSTD_customMem mem={zstd_allocate,zstd_release,NULL}; size_t result;
    if (compressing) {
        if (!s->compress) s->compress=ZSTD_createCCtx_advanced(mem);
        if (!s->compress) { errno=ENOMEM; return -1; }
        result=ZSTD_compressCCtx(s->compress,dst,cap,src,n,1);
    } else {
        if (!s->decompress) s->decompress=ZSTD_createDCtx_advanced(mem);
        if (!s->decompress) { errno=ENOMEM; return -1; }
        result=ZSTD_decompressDCtx(s->decompress,dst,cap,src,n);
    }
    return ZSTD_isError(result)?-1:(int)result;
}
#endif
