/* Real-tree capacity and closed-loop latency probe. No model/cache substitute. */
#define _POSIX_C_SOURCE 200809L
#define _DARWIN_C_SOURCE 1
#include <inttypes.h>
#include <sys/resource.h>
#include <time.h>
#ifdef __APPLE__
#include <mach/mach.h>
#endif
#include "compressed_test_utils.h"

static void check(int ok, const char *why)
{ if (!ok) { fprintf(stderr, "product_capacity: %s\n", why); exit(1); } }
static uint32_t rng(uint32_t *s)
{ *s ^= *s << 13; *s ^= *s >> 17; *s ^= *s << 5; return *s; }
static uint64_t ns(void)
{ struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return (uint64_t)t.tv_sec*1000000000+t.tv_nsec; }
static uint64_t cpu(void)
{ struct timespec t; clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t); return (uint64_t)t.tv_sec*1000000000+t.tv_nsec; }
static size_t rss(void)
{
#ifdef __APPLE__
    struct mach_task_basic_info info;
    mach_msg_type_number_t n = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &n) == KERN_SUCCESS)
        return info.resident_size;
    return 0;
#else
    FILE *f = fopen("/proc/self/statm", "r");
    size_t total = 0, resident = 0;
    if (f) { if (fscanf(f, "%zu %zu", &total, &resident) != 2) resident = 0; fclose(f); }
    return resident * (size_t)sysconf(_SC_PAGESIZE);
#endif
}
static int cmp_u64(const void *a, const void *b)
{ uint64_t x=*(const uint64_t*)a, y=*(const uint64_t*)b; return (x>y)-(x<y); }
static uint64_t percentile(uint64_t *v, size_t n, double p)
{ if (!n) return 0; qsort(v,n,sizeof(*v),cmp_u64); return v[(size_t)((n-1)*p)]; }

struct fixture {
    struct bplus_tree_compressed *tree;
    struct btree_silesia_dataset data;
    int keys, threads, ops, read_pct, hot, random;
    unsigned seed;
    int *version, *live;
    size_t live_count;
    uint8_t *seen;
    pthread_mutex_t gate;
    pthread_cond_t ready;
    int started, finished, release;
};
static void payload(struct fixture *f, int key, int version, uint8_t out[128])
{
    if (f->random) {
        uint32_t s = (uint32_t)key * 2654435761u + (uint32_t)version * 104729u + f->seed;
        if (!s) s = 1;
        for (int i=0;i<128;i++) out[i]=(uint8_t)rng(&s);
    } else {
        size_t index = ((size_t)(key-1) + (size_t)version*7919) % f->data.chunk_count;
        memcpy(out, f->data.data + index*128, 128);
    }
}
struct worker { struct fixture *f; int id; uint64_t *reads,*writes; size_t nr,nw; uint64_t hash; };
static void *run(void *arg)
{
    struct worker *w=arg; struct fixture *f=w->f;
    uint32_t state=f->seed+(uint32_t)w->id*104729u;
    size_t domain=(f->live_count-(size_t)w->id+(size_t)f->threads-1)/(size_t)f->threads;
    pthread_mutex_lock(&f->gate);
    while (!f->started) pthread_cond_wait(&f->ready,&f->gate);
    pthread_mutex_unlock(&f->gate);
    for (int i=0;i<f->ops;i++) {
        size_t extent=domain;
        if (f->hot && rng(&state)%100<80) { extent=domain/5; if (!extent) extent=1; }
        int key=f->live[(rng(&state)%extent)*(size_t)f->threads+(size_t)w->id];
        int read=(int)(rng(&state)%100)<f->read_pct;
        w->hash=(w->hash^(uint64_t)(key*2+read))*1099511628211ULL;
        uint8_t bytes[128];
        int version=i*f->threads+w->id+1;
        if (!read) payload(f,key,version,bytes);
        uint64_t start=ns();
        int rc=read ? bplus_tree_compressed_get(f->tree,key) :
            bplus_tree_compressed_put_with_payload(f->tree,key,bytes,128,version+1);
        uint64_t elapsed=ns()-start;
        /* Each worker owns a disjoint key set. No permissive version oracle. */
        if (read) { check(rc==f->version[key]+1,"GET exact-version mismatch"); w->reads[w->nr++]=elapsed; }
        else { check(rc==0,"PUT failed"); f->version[key]=version; w->writes[w->nw++]=elapsed; }
    }
    /* Keep worker TLS/workspaces alive through drain and the memory snapshot. */
    pthread_mutex_lock(&f->gate);
    f->finished++;
    pthread_cond_broadcast(&f->ready);
    while (!f->release) pthread_cond_wait(&f->ready,&f->gate);
    pthread_mutex_unlock(&f->gate);
    return NULL;
}
static int verify_pair(const struct kv_pair *p, void *context)
{
    struct fixture *f=context; uint8_t bytes[128];
    if (p->key<1 || p->key>f->keys || f->version[p->key]<0 || f->seen[p->key] ||
        p->stored_value!=f->version[p->key]+1) return -1;
    payload(f,p->key,f->version[p->key],bytes);
    if (memcmp(bytes,p->payload,128)) return -1;
    f->seen[p->key]=1; return 0;
}
static size_t resident_ledger(const struct bplus_tree_memory_stats *m)
{
    return m->tree_metadata_usable_bytes+m->leaf_metadata_usable_bytes+
        m->active_allocated_bytes+m->pending_allocated_bytes+m->compressed_usable_bytes+
        m->scheduler_and_qpl_usable_bytes+m->process_qpl_tls_bytes+m->process_zlib_workspace_bytes;
}
int main(int argc,char **argv)
{
    const char *codec=argc>1?argv[1]:"lz4";
    struct fixture f={0};
    f.keys=btree_env_int("PRODUCT_KEYS",10000,32);
    f.threads=btree_env_int("PRODUCT_THREADS",4,1);
    f.ops=btree_env_int("PRODUCT_OPS",5000,1);
    f.seed=(unsigned)btree_env_int("PRODUCT_SEED",11,1);
    f.read_pct=btree_env_int("PRODUCT_READ_PCT",80,0);
    f.hot=btree_env_bool("PRODUCT_HOT",0);
    f.random=btree_env_bool("PRODUCT_RANDOM",0);
    int sparse=btree_env_bool("PRODUCT_SPARSE",0);
    check(f.threads<=64 && f.read_pct<=100,"invalid configuration");
    if (!f.random) check(!btree_load_silesia_samba(&f.data,128,0),"Silesia samba unavailable");
    f.version=calloc((size_t)f.keys+1,sizeof(int));
    f.seen=calloc((size_t)f.keys+1,1);
    f.live=malloc((size_t)f.keys*sizeof(int));
    check(f.version&&f.seen&&f.live,"fixture allocation");
    struct compression_config cfg=bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    cfg.algo=!strcmp(codec,"raw")?COMPRESS_RAW_PACKED:!strcmp(codec,"lz4")?COMPRESS_LZ4:
             !strcmp(codec,"zlib")?COMPRESS_ZLIB_ACCEL:COMPRESS_QPL;
    cfg.qpl_path=btree_parse_qpl_path(); cfg.qpl_huffman_mode=QPL_HUFFMAN_DYNAMIC;
    compression_algo_t requested=cfg.algo;
    if (cfg.algo==COMPRESS_QPL) {
#ifndef HAVE_QPL
        printf("PRODUCT {\"status\":\"unavailable\",\"requested_backend\":\"%s\",\"reason\":\"real QPL not linked\"}\n",codec);
        return 77;
#endif
    }
    setenv("BTREE_PROFILE_SUBMISSION","1",1);
    f.tree=bplus_tree_compressed_init_with_config(32,64,&cfg);
    if (!f.tree) { printf("PRODUCT {\"status\":\"unavailable\",\"requested_backend\":\"%s\"}\n",codec); return 77; }
    check(f.tree->config.algo==requested,"unexpected backend fallback");
    for (int k=1;k<=f.keys;k++) {
        uint8_t bytes[128]; payload(&f,k,0,bytes);
        check(!bplus_tree_compressed_put_with_payload(f.tree,k,bytes,128,1),"preload failed");
    }
    check(!bplus_tree_compressed_drain_background(f.tree),"preload drain failed");
    if (sparse) for (int k=1;k<=f.keys;k++) if (k%5) {
        check(!bplus_tree_compressed_delete(f.tree,k),"DELETE failed"); f.version[k]=-1;
    }
    for (int k=1;k<=f.keys;k++) if (f.version[k]>=0) f.live[f.live_count++]=k;
    check(f.live_count>=(size_t)f.threads,"not enough live keys");
    struct bplus_tree_memory_stats before,after;
    check(!bplus_tree_compressed_memory_stats(f.tree,&before),"preload memory walk failed");
    bplus_tree_compressed_submission_reset(f.tree);
    struct bplus_tree_scheduler_stats sched_before;
    bplus_tree_compressed_scheduler_stats(f.tree,&sched_before);
    struct bplus_tree_allocation_audit audit;
    bplus_tree_compressed_allocation_audit(&audit,0);
    size_t preload_peak=audit.peak_usable;
    bplus_tree_compressed_allocation_audit(&audit,1);
    struct worker workers[64]={0}; pthread_t threads[64];
    pthread_mutex_init(&f.gate,NULL); pthread_cond_init(&f.ready,NULL);
    for (int t=0;t<f.threads;t++) {
        workers[t].f=&f; workers[t].id=t; workers[t].hash=14695981039346656037ULL;
        workers[t].reads=malloc((size_t)f.ops*sizeof(uint64_t));
        workers[t].writes=malloc((size_t)f.ops*sizeof(uint64_t));
        check(workers[t].reads&&workers[t].writes,"latency allocation");
        check(!pthread_create(&threads[t],NULL,run,&workers[t]),"pthread_create failed");
    }
    uint64_t cpu_start=cpu(),start=ns();
    pthread_mutex_lock(&f.gate); f.started=1; pthread_cond_broadcast(&f.ready); pthread_mutex_unlock(&f.gate);
    pthread_mutex_lock(&f.gate);
    while (f.finished<f.threads) pthread_cond_wait(&f.ready,&f.gate);
    pthread_mutex_unlock(&f.gate);
    uint64_t foreground=ns()-start;
    check(!bplus_tree_compressed_drain_background(f.tree),"final drain has failed pending");
    uint64_t completed=ns()-start,used_cpu=cpu()-cpu_start;
    size_t steady_rss=rss(); struct rusage ru; getrusage(RUSAGE_SELF,&ru);
    bplus_tree_compressed_allocation_audit(&audit,0); /* before validation allocations */
    struct bplus_tree_submission_stats submission;
    bplus_tree_compressed_submission_stats(f.tree,&submission);
    check(!bplus_tree_compressed_memory_stats(f.tree,&after),"memory walk failed");
    check(!after.failed_pending_count,"failed pending hidden from measurement");
    size_t retained=resident_ledger(&after);
    if (audit.enabled && cfg.algo!=COMPRESS_QPL)
        check(audit.usable==retained,"allocation audit disagrees with resident ledger");
    pthread_mutex_lock(&f.gate); f.release=1; pthread_cond_broadcast(&f.ready); pthread_mutex_unlock(&f.gate);
    for (int t=0;t<f.threads;t++) check(!pthread_join(threads[t],NULL),"join failed");
    check(!bplus_tree_compressed_verify(f.tree,verify_pair,&f),"full-payload validation failed");
    for (int k=1;k<=f.keys;k++) {
        check(f.seen[k]==(f.version[k]>=0),"missing or resurrected key");
        check(bplus_tree_compressed_get(f.tree,k)==(f.version[k]<0?-1:f.version[k]+1),"final GET mismatch");
    }
    size_t n=(size_t)f.threads*f.ops,nr=0,nw=0;
    uint64_t *reads=malloc(n*sizeof(uint64_t)),*writes=malloc(n*sizeof(uint64_t)),hash=0;
    check(reads&&writes,"aggregation allocation");
    for (int t=0;t<f.threads;t++) {
        memcpy(reads+nr,workers[t].reads,workers[t].nr*sizeof(uint64_t)); nr+=workers[t].nr;
        memcpy(writes+nw,workers[t].writes,workers[t].nw*sizeof(uint64_t)); nw+=workers[t].nw;
        hash^=workers[t].hash; free(workers[t].reads); free(workers[t].writes);
    }
    struct bplus_tree_scheduler_stats sched; bplus_tree_compressed_scheduler_stats(f.tree,&sched);
    struct bplus_tree_backend_info backend; bplus_tree_compressed_backend_info(f.tree,&backend);
    sched.failed_tasks-=sched_before.failed_tasks;
    sched.synchronous_fallbacks-=sched_before.synchronous_fallbacks;
    sched.split_fallbacks-=sched_before.split_fallbacks;
    sched.submitted_jobs-=sched_before.submitted_jobs;
    sched.terminal_jobs-=sched_before.terminal_jobs;
    sched.failed_jobs-=sched_before.failed_jobs;
    sched.busy_jobs-=sched_before.busy_jobs;
    check(!sched.outstanding_jobs && sched.submitted_jobs==sched.terminal_jobs,"unfinished accepted codec jobs");
    printf("PRODUCT {\"status\":\"ok\",\"requested_backend\":\"%s\",\"actual_backend\":\"%s\","
      "\"lz4_version\":\"%s\",\"keys\":%d,\"live_keys\":%zu,\"seed\":%u,\"shards\":%d,\"threads\":%d,"
      "\"library_version\":\"%s\",\"execution_path\":\"%s\",\"explicit_fallbacks\":%" PRIu64 ",\"include_trigger\":%d,"
      "\"read_pct\":%d,\"hot\":%d,\"sparse\":%d,\"random\":%d,\"async\":%d,\"audit\":%d,"
      "\"trace_hash\":\"%016" PRIx64 "\",\"operations\":%zu,\"reads\":%zu,\"writes\":%zu,\"foreground_ns\":%" PRIu64 ","
      "\"completed_ns\":%" PRIu64 ",\"cpu_ns\":%" PRIu64 ",\"get_p99_ns\":%" PRIu64 ",\"put_p99_ns\":%" PRIu64 ","
      "\"get_p50_ns\":%" PRIu64 ",\"put_p50_ns\":%" PRIu64 ",\"get_p999_ns\":%" PRIu64 ",\"put_p999_ns\":%" PRIu64 ","
      "\"live_kv_bytes\":%zu,\"preload_resident\":%zu,\"resident_usable\":%zu,\"audit_requested\":%zu,"
      "\"audit_usable\":%zu,\"audit_peak_usable\":%zu,\"preload_peak_usable\":%zu,\"audit_tracker_bytes\":%zu,\"rss\":%zu,\"ru_maxrss_native\":%ld,"
      "\"wrapper_size\":%zu,\"leaf_metadata_size\":%zu,\"leaf_count\":%zu,\"tree_metadata\":%zu,\"tree_metadata_usable\":%zu,"
      "\"leaf_metadata\":%zu,\"leaf_metadata_usable\":%zu,\"active\":%zu,\"pending\":%zu,\"base_requested\":%zu,\"base_usable\":%zu,"
      "\"scheduler_requested\":%zu,\"scheduler_usable\":%zu,\"process_codec_workspace\":%zu,"
      "\"compress_calls\":%" PRIu64 ",\"decompress_calls\":%" PRIu64 ",\"write_lock_codec_calls\":%" PRIu64 ","
      "\"queue_peak\":%" PRIu64 ",\"failed_tasks\":%" PRIu64 ",\"sync_fallbacks\":%" PRIu64 ",\"split_fallbacks\":%" PRIu64 ",\"mismatches\":0,\"fill_histogram\":[",
      codec,codec,LZ4_versionString(),f.keys,f.live_count,f.seed,f.tree->shard_count>1?f.tree->shard_count:1,f.threads,
      backend.library_version,backend.execution_path,backend.explicit_fallbacks,btree_env_bool("BTREE_BG_INCLUDE_TRIGGER",0),f.read_pct,f.hot,sparse,f.random,
      f.tree->bg_compaction_enabled,audit.enabled,hash,n,nr,nw,foreground,completed,used_cpu,percentile(reads,nr,.99),percentile(writes,nw,.99),
      percentile(reads,nr,.50),percentile(writes,nw,.50),percentile(reads,nr,.999),percentile(writes,nw,.999),after.live_kv_bytes,
      resident_ledger(&before),retained,audit.requested,audit.usable,audit.peak_usable,preload_peak,audit.tracker_bytes,steady_rss,ru.ru_maxrss,
      sizeof(struct compressed_leaf_ref),sizeof(struct compressed_leaf_ref)+sizeof(struct simple_leaf_node)-sizeof(((struct simple_leaf_node*)0)->active),
      after.leaf_count,after.tree_metadata_bytes,after.tree_metadata_usable_bytes,after.leaf_metadata_bytes,after.leaf_metadata_usable_bytes,
      after.active_allocated_bytes,after.pending_allocated_bytes,after.compressed_requested_bytes,after.compressed_usable_bytes,
      after.scheduler_and_qpl_bytes,after.scheduler_and_qpl_usable_bytes,after.process_qpl_tls_bytes+after.process_zlib_workspace_bytes,
      submission.compress_calls,submission.decompress_calls,submission.calls_under_write_lock,sched.queue_peak,sched.failed_tasks,sched.synchronous_fallbacks,sched.split_fallbacks);
    for (int i=0;i<65;i++) printf("%s%zu",i?",":"",after.fill_histogram[i]);
    printf("]}\n");
    printf("JOBS {\"submitted\":%" PRIu64 ",\"terminal\":%" PRIu64 ",\"failed\":%" PRIu64 ",\"busy\":%" PRIu64 ",\"outstanding\":%" PRIu64 ",\"queue_wait_ns\":%" PRIu64 "}\n",
        sched.submitted_jobs,sched.terminal_jobs,sched.failed_jobs,sched.busy_jobs,sched.outstanding_jobs,
        sched.total_queue_wait_ns-sched_before.total_queue_wait_ns);
    printf("PHASES {\"decompress_ns\":%" PRIu64 ",\"merge_ns\":%" PRIu64 ",\"compress_and_allocate_ns\":%" PRIu64 ",\"commit_and_split_ns\":%" PRIu64 ",\"ready_at_wait_samples\":%" PRIu64 "}\n",
        sched.decompress_phase_ns-sched_before.decompress_phase_ns,
        sched.merge_phase_ns-sched_before.merge_phase_ns,
        sched.compress_phase_ns-sched_before.compress_phase_ns,
        sched.commit_phase_ns-sched_before.commit_phase_ns,
        sched.ready_at_wait_samples-sched_before.ready_at_wait_samples);
    bplus_tree_compressed_deinit(f.tree);
    bplus_tree_compressed_allocation_audit(&audit,0);
    size_t tls_retained=audit.usable;
    bplus_tree_compressed_release_thread_resources();
    bplus_tree_compressed_allocation_audit(&audit,0);
    printf("TEARDOWN {\"before_thread_release\":%zu,\"after_thread_release\":%zu,\"allocations\":%zu}\n",tls_retained,audit.usable,audit.live_allocations);
    check(!audit.enabled || !audit.live_allocations,"shutdown allocation leak");
    free(reads); free(writes); free(f.data.data); free(f.version); free(f.live); free(f.seen);
    pthread_mutex_destroy(&f.gate); pthread_cond_destroy(&f.ready);
    return 0;
}
