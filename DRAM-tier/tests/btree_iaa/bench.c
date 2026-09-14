/* Real-tree aggregation probe. Derived from product_capacity_probe.c. */
#define _POSIX_C_SOURCE 200809L
#define _DARWIN_C_SOURCE 1
#define _GNU_SOURCE
#include <inttypes.h>
#include <sys/resource.h>
#include <time.h>
#ifdef __APPLE__
#include <mach/mach.h>
#endif
#include "compressed_test_utils.h"
#include <zstd.h>
#ifdef __APPLE__
#include <malloc/malloc.h>
#else
#include <malloc.h>
#endif
static size_t external_stacks;
static size_t usable(void *p) {
#ifdef __APPLE__
    return malloc_size(p);
#else
    return malloc_usable_size(p);
#endif
}
static void *start_budgeted(pthread_t *id,void *(*fn)(void*),void *arg) {
    size_t page=(size_t)sysconf(_SC_PAGESIZE),n=512*1024+page;
    void *p=malloc(n); if(!p) abort(); memset(p,0,n);
    uintptr_t base=((uintptr_t)p+page-1)&~(uintptr_t)(page-1);
    pthread_attr_t attr;pthread_attr_init(&attr);
    int rc=pthread_attr_setstack(&attr,(void*)base,512*1024);
    __atomic_add_fetch(&external_stacks,usable(p),__ATOMIC_RELAXED);
    if(!rc)rc=pthread_create(id,&attr,fn,arg);pthread_attr_destroy(&attr);
    if(rc)abort();return p;
}

static void check(int ok, const char *why)
{ if (!ok) { fprintf(stderr, "product_capacity: %s\n", why); exit(1); } }
static uint32_t rng(uint32_t *s)
{ *s ^= *s << 13; *s ^= *s >> 17; *s ^= *s << 5; return *s; }
static uint64_t ns(void)
{ struct timespec t; clock_gettime(CLOCK_MONOTONIC_RAW, &t); return (uint64_t)t.tv_sec*1000000000+t.tv_nsec; }
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
    int keys, threads, ops, read_pct, hot, random, batch, mapping, legacy, sparse, oracle;
    uint64_t epoch_ns, arrival_rate, last_return_ns;
    uint32_t *map, *events;
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
        size_t index = ((size_t)f->map[key-1] + (size_t)version*7919) % f->data.chunk_count;
        memcpy(out, f->data.data + index*128, 128);
    }
}
#include "bench_split.inc"
struct worker { struct fixture *f; int id; uint64_t *reads,*writes; size_t nr,nw; uint64_t hash, sink; };
static void *run(void *arg)
{
    struct worker *w=arg; struct fixture *f=w->f;
    pthread_mutex_lock(&f->gate);
    while (!f->started) pthread_cond_wait(&f->ready,&f->gate);
    pthread_mutex_unlock(&f->gate);
    for (int i=0;i<f->ops;i++) {
        uint32_t *event=f->events+((size_t)w->id*f->ops+(size_t)i)*2;
        int key=(int)event[0],read=(int)event[1];
        w->hash=(w->hash^(uint64_t)(key*2+read))*1099511628211ULL;
        uint8_t bytes[128]; struct kv_pair records[32]; int statuses[32]; key_t keys[32];
        for(int j=0;j<f->batch;j++) keys[j]=key; /* overwritten below for batch sensitivity */
        if(f->batch>1) {
            size_t domain=(f->live_count-(size_t)w->id+(size_t)f->threads-1)/(size_t)f->threads;
            size_t index=(size_t)(f->sparse?key/5-1:key-1)/(size_t)f->threads;
            for(int j=0;j<f->batch;j++)keys[j]=f->live[((index+(size_t)j)%domain)*f->threads+w->id];
            keys[f->batch-1]=key; /* explicit duplicate, version chosen once */
        }
        int version=i*f->threads+w->id+1;
        uint64_t scheduled = f->arrival_rate ? f->epoch_ns +
            ((uint64_t)i*f->threads+(unsigned)w->id)*UINT64_C(1000000000)/f->arrival_rate : 0;
        if (scheduled) wait_until(scheduled);
        if(!read)payload(f,key,version,bytes);
        uint64_t start=ns();int rc;
        if(!read)rc=bplus_tree_compressed_put_with_payload(f->tree,key,bytes,128,version+1);
        else if(f->legacy)rc=bplus_tree_compressed_get(f->tree,key);
        else if(f->batch==1)rc=bplus_tree_compressed_get_record(f->tree,key,&records[0]);
        else rc=bplus_tree_compressed_get_many_records(f->tree,keys,(size_t)f->batch,records,statuses);
        uint64_t elapsed=ns()-(scheduled?scheduled:start);
        /* The audit process checks every byte. Performance consumes the actual
         * return and verifies the entire final state after CPU/wall timing. */
        if(read) {
            check(rc==(f->legacy?f->version[key]+1:BPLUS_RECORD_OK),"GET status/version");
            if(!f->legacy && f->oracle)for(int j=0;j<f->batch;j++){
                int k=keys[j];payload(f,k,f->version[k],bytes);
                check(f->batch==1 || statuses[j]==BPLUS_RECORD_OK,"GET_MANY status");
                check(records[j].key==k && records[j].stored_value==f->version[k]+1 &&
                      !memcmp(records[j].payload,bytes,128),"complete returned payload/order");
            }
            if(!f->legacy) for(int j=0;j<f->batch;j++) {
                check(f->batch==1 || statuses[j]==BPLUS_RECORD_OK,"GET_MANY status");
                w->sink += (uint64_t)records[j].stored_value + records[j].payload[127];
            }
            w->reads[w->nr++]=elapsed;
        }else{check(!rc,"PUT failed");f->version[key]=version;w->writes[w->nw++]=elapsed;}
    }
    /* Keep worker TLS/workspaces alive through drain and the memory snapshot. */
    pthread_mutex_lock(&f->gate);
    uint64_t returned=ns();if(returned>f->last_return_ns)f->last_return_ns=returned;
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
        m->scheduler_and_qpl_usable_bytes+m->process_qpl_tls_bytes+m->process_zlib_workspace_bytes+m->process_zstd_workspace_bytes;
}
static int probe_main(int argc,char **argv)
{
    const char *codec=argc>1?argv[1]:"lz4";
    struct fixture f={0};
    check(argc==3,"usage: agg_bench_lN CODEC TRACE");
    FILE *trace=fopen(argv[2],"rb");check(trace!=NULL,"trace open");
    char magic[8];uint32_t header[10];
    check(fread(magic,1,8,trace)==8 && !memcmp(magic,"ZCAGG001",8),"trace magic");
    check(fread(header,4,10,trace)==10 && header[0]==1,"trace header");
    f.keys=header[1];f.threads=header[2];f.ops=header[3];f.read_pct=header[4];
    f.seed=header[5];f.mapping=header[6];f.hot=header[7];f.sparse=header[8];f.batch=header[9];
    int sparse=f.sparse;f.random=btree_env_bool("AGG_RANDOM",0);
    f.legacy=btree_env_bool("AGG_LEGACY_GET",0);
    f.oracle=btree_env_bool("AGG_VERIFY_EACH",1);
    const char *rate=getenv("AGG_ARRIVAL_RATE");
    if(rate){char *end; f.arrival_rate=strtoull(rate,&end,10);
        check(*rate && !*end && *rate!='-' && f.arrival_rate<=100000000,"invalid arrival rate");}
    check(f.keys>=32 && f.keys<=150000 && f.threads>=1 && f.threads<=64 &&
          f.ops>0 && f.ops<=100000000 && f.read_pct<=100 &&
          (uint64_t)f.ops*f.threads<INT32_MAX &&
          (f.batch==1 || f.batch==8 || f.batch==32) && (!f.legacy || f.batch==1),"trace limits");
    f.map=malloc((size_t)f.keys*4);f.events=malloc((size_t)f.threads*f.ops*8);
    check(f.map&&f.events,"trace allocation");
    check(fread(f.map,4,f.keys,trace)==(size_t)f.keys &&
          fread(f.events,8,(size_t)f.threads*f.ops,trace)==(size_t)f.threads*f.ops &&
          fgetc(trace)==EOF,"trace length");fclose(trace);
    for(int k=0;k<f.keys;k++)check(f.map[k]<(uint32_t)f.keys,"mapping bounds");
    for(size_t e=0;e<(size_t)f.threads*f.ops;e++)
        check(f.events[e*2]>=1 && f.events[e*2]<=(uint32_t)f.keys &&
              f.events[e*2+1]<=1,"event bounds");
    if (!f.random) check(!btree_load_silesia_samba(&f.data,128,0),"Silesia samba unavailable");
    f.version=calloc((size_t)f.keys+1,sizeof(int));
    f.seen=calloc((size_t)f.keys+1,1);
    f.live=malloc((size_t)f.keys*sizeof(int));
    check(f.version&&f.seen&&f.live,"fixture allocation");
    struct compression_config cfg=bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    cfg.algo=!strcmp(codec,"raw")?COMPRESS_RAW_PACKED:!strcmp(codec,"lz4")?COMPRESS_LZ4:
             (!strcmp(codec,"zlib") || !strcmp(codec,"zlib-accel"))?COMPRESS_ZLIB_ACCEL:!strcmp(codec,"zstd")?COMPRESS_ZSTD_EXPERIMENT:COMPRESS_QPL;
    cfg.qpl_path=btree_parse_qpl_path(); cfg.qpl_huffman_mode=QPL_HUFFMAN_DYNAMIC;
    compression_algo_t requested=cfg.algo;
    const char *accel_library = NULL;
    if (!strcmp(codec,"zlib-accel") && bplus_tree_compressed_accel_configure(
        btree_env_bool("AGG_ACCEL_COMPRESS",1),btree_env_bool("AGG_ACCEL_DECOMPRESS",0),&accel_library)) {
        printf("PRODUCT {\"status\":\"unavailable\",\"reason\":\"verified frozen IAA shim/observer not loaded\"}\n"); return 77;
    }
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
    const char *snapshot=getenv("AGG_BASE_IMAGE");
    if(snapshot){FILE *image=fopen(snapshot,"wb");check(image!=NULL,"snapshot open");
        snapshot_tree(f.tree,image);check(!fclose(image),"snapshot close");}
    struct bplus_tree_memory_stats before,after;
    check(!bplus_tree_compressed_memory_stats(f.tree,&before),"preload memory walk failed");
    bplus_tree_compressed_submission_reset(f.tree);
    bplus_tree_compressed_aggregation_stats(NULL,1);
    struct bplus_tree_scheduler_stats sched_before;
    bplus_tree_compressed_scheduler_stats(f.tree,&sched_before);
    struct bplus_tree_allocation_audit audit;
    bplus_tree_compressed_allocation_audit(&audit,0);
    size_t preload_peak=audit.peak_usable;
    bplus_tree_compressed_allocation_audit(&audit,1);
    struct worker workers[64]={0}; pthread_t threads[64]; void *stacks[64];
    pthread_mutex_init(&f.gate,NULL); pthread_cond_init(&f.ready,NULL);
    for (int t=0;t<f.threads;t++) {
        workers[t].f=&f; workers[t].id=t; workers[t].hash=14695981039346656037ULL;
        workers[t].reads=malloc((size_t)f.ops*sizeof(uint64_t));
        workers[t].writes=malloc((size_t)f.ops*sizeof(uint64_t));
        check(workers[t].reads&&workers[t].writes,"latency allocation");
        stacks[t]=start_budgeted(&threads[t],run,&workers[t]);
    }
    uint64_t cpu_start=cpu(),start=ns();
    pthread_mutex_lock(&f.gate); f.epoch_ns=start; f.started=1; pthread_cond_broadcast(&f.ready); pthread_mutex_unlock(&f.gate);
    uint64_t queue_samples=0, queue_first=0, queue_last=0, queue_max=0;
    for (;;) {
        pthread_mutex_lock(&f.gate); int finished=f.finished==f.threads; pthread_mutex_unlock(&f.gate);
        if(finished)break;
        struct bplus_tree_scheduler_stats sample;bplus_tree_compressed_scheduler_stats(f.tree,&sample);
        if(!queue_samples)queue_first=sample.queue_depth;
        queue_last=sample.queue_depth;if(queue_last>queue_max)queue_max=queue_last;queue_samples++;
        struct timespec interval={0,1000000};nanosleep(&interval,NULL);
    }
    uint64_t foreground=f.last_return_ns-start;
    check(!bplus_tree_compressed_drain_background(f.tree),"final drain has failed pending");
    uint64_t completed=ns()-start,used_cpu=cpu()-cpu_start;
    size_t steady_rss=rss(); struct rusage ru; getrusage(RUSAGE_SELF,&ru);
    bplus_tree_compressed_allocation_audit(&audit,0); /* before validation allocations */
    struct bplus_aggregation_stats origins;bplus_tree_compressed_aggregation_stats(&origins,0);
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
    print_split(&f,&origins,accel_library,queue_samples,queue_first,queue_last,queue_max);
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
    for (int i=0;i<129;i++) printf("%s%zu",i?",":"",after.fill_histogram[i]);
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
    printf("WAITS {\"admission_waits\":%" PRIu64 ",\"admission_ns\":%" PRIu64 ",\"pending_or_resource_ns\":%" PRIu64 "}\n",
        origins.admission_waits,origins.admission_wait_ns,origins.pending_wait_ns);
    printf("AGGREGATION {\"layout\":%d,\"mapping\":%d,\"batch\":%d,\"legacy_get\":%d,"
        "\"stack_allocated_usable\":%zu,\"zstd_workspace\":%zu,\"zstd_version\":\"%s\","
        "\"active_hits\":%" PRIu64 ",\"pending_hits\":%" PRIu64 ",\"base_hits\":%" PRIu64 ","
        "\"pending_wait_ns\":%" PRIu64 ",\"read_lock_wait_ns\":%" PRIu64 ",\"bypass_blocks\":%" PRIu64 ","
        "\"merged_updates\":%" PRIu64 ",\"merge_count\":%" PRIu64 ",\"origins\":[",
        ZIPCACHE_AGG_LAYOUT,f.mapping,f.batch,f.legacy,external_stacks,after.process_zstd_workspace_bytes,ZSTD_versionString(),
        origins.active_hits,origins.pending_hits,origins.base_hits,origins.pending_wait_ns,origins.read_lock_wait_ns,
        origins.bypass_blocks,origins.merged_updates,origins.merge_count);
    for(int x=0;x<BPLUS_ORIGIN_COUNT;x++)printf("%s{\"compress\":%" PRIu64 ",\"decompress\":%" PRIu64
        ",\"encode_bytes\":%" PRIu64 ",\"decode_bytes\":%" PRIu64
        ",\"encode_ns\":%" PRIu64 ",\"decode_ns\":%" PRIu64 ",\"encode_failed\":%" PRIu64 ",\"decode_failed\":%" PRIu64 "}",x?",":"",
        origins.compress[x],origins.decompress[x],origins.encode_bytes[x],origins.decode_bytes[x],
        origins.codec_ns[x][1],origins.codec_ns[x][0],origins.codec_failures[x][1],origins.codec_failures[x][0]);
    printf("]}\n");
    bplus_tree_compressed_deinit(f.tree);
    bplus_tree_compressed_allocation_audit(&audit,0);
    size_t tls_retained=audit.usable;
    bplus_tree_compressed_release_thread_resources();
    bplus_tree_compressed_allocation_audit(&audit,0);
    printf("TEARDOWN {\"before_thread_release\":%zu,\"after_thread_release\":%zu,\"allocations\":%zu}\n",tls_retained,audit.usable,audit.live_allocations);
    check(!audit.enabled || !audit.live_allocations,"shutdown allocation leak");
    for(int t=0;t<f.threads;t++){ external_stacks-=usable(stacks[t]);free(stacks[t]); }
    free(f.map);free(f.events);
    free(reads); free(writes); free(f.data.data); free(f.version); free(f.live); free(f.seen);
    pthread_mutex_destroy(&f.gate); pthread_cond_destroy(&f.ready);
    return 0;
}

struct driver { int argc;char **argv;int result; };
static void *drive(void *p){struct driver *d=p;d->result=probe_main(d->argc,d->argv);return NULL;}
int main(int argc,char **argv){
    struct driver d={argc,argv,0};pthread_t id;void *stack=start_budgeted(&id,drive,&d);
    pthread_join(id,NULL);external_stacks-=usable(stack);free(stack);return d.result;
}
