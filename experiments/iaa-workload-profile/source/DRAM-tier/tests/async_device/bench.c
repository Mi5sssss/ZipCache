#define _POSIX_C_SOURCE 200809L
#include "compressed_test_utils.h"
#include "async_copy_device.h"
#include <inttypes.h>
#include <time.h>
struct fixture {struct bplus_tree_compressed *tree;int keys,clients,reads,hot,sequential,*versions,*keymap;uint64_t fixed_ops;};
/* Portable start gate (macOS does not provide pthread_barrier_t). */
struct run_gate {pthread_mutex_t m;pthread_cond_t cv;int ready,start,done;uint64_t deadline,fixed_ops;};
struct client {struct fixture *f;struct run_gate *g;int id;uint64_t reads,writes,cpu,last,hash;uint64_t rh[1024],wh[1024];};
#define need(b) do {if(!(b)){fprintf(stderr,"surrogate benchmark check failed line %d: %s\n",__LINE__,#b);abort();}}while(0)
static uint64_t cpu(clockid_t id){struct timespec t;clock_gettime(id,&t);return (uint64_t)t.tv_sec*1000000000+t.tv_nsec;}
static uint32_t next(uint32_t *s){*s^=*s<<13;*s^=*s>>17;*s^=*s<<5;return *s;}
static void payload(int k,int v,uint8_t p[128]){for(int i=0;i<128;i++)p[i]=(uint8_t)(k*31+v*7+i);memcpy(p,&v,4);}
static void hist(uint64_t *h,uint64_t n){if(!n)n=1;unsigned e=63-__builtin_clzll(n);uint64_t b=UINT64_C(1)<<e;unsigned sub=(unsigned)((n-b)*16/b);h[e*16+sub]++;}
static uint64_t percentile(uint64_t *h,uint64_t count){if(!count)return 0;uint64_t seen=0,target=(count*99+99)/100;for(int i=0;i<1024;i++){seen+=h[i];if(seen>=target){uint64_t b=UINT64_C(1)<<(i/16);return b+(uint64_t)(i%16+1)*b/16;}}return 0;}
static void *client_run(void *p){struct client *c=p;struct fixture *f=c->f;uint32_t rng=11u+(uint32_t)c->id*991u;c->hash=14695981039346656037ULL;
    pthread_mutex_lock(&c->g->m);c->g->ready++;pthread_cond_broadcast(&c->g->cv);while(!c->g->start)pthread_cond_wait(&c->g->cv,&c->g->m);pthread_mutex_unlock(&c->g->m);
    uint64_t cpu_start=cpu(CLOCK_THREAD_CPUTIME_ID);
    while(c->g->fixed_ops?c->reads+c->writes<c->g->fixed_ops:sd_now()<c->g->deadline){int ord=c->id+(int)(next(&rng)%(unsigned)(f->keys/f->clients))*f->clients;
        if(f->sequential)ord=c->id+(int)((c->reads+c->writes)%(unsigned)(f->keys/f->clients))*f->clients;
        int k=f->hot?(int)(next(&rng)%4)+1:f->keymap[ord];int read=(int)(next(&rng)%100)<f->reads;
        uint8_t expected[128];struct kv_pair value;int rc;uint64_t start=sd_now();
        if(read){rc=bplus_tree_compressed_get_record(f->tree,k,&value);uint64_t end=sd_now();hist(c->rh,end-start);c->reads++;need(!rc);payload(k,f->versions[k],expected);need(value.stored_value==f->versions[k]&&!memcmp(value.payload,expected,128));}
        else{int v=f->versions[k]+1;payload(k,v,expected);rc=bplus_tree_compressed_put_with_payload(f->tree,k,expected,128,v);uint64_t end=sd_now();hist(c->wh,end-start);c->writes++;need(!rc);f->versions[k]=v;}
        c->hash=(c->hash^(uint64_t)(k*2+read))*1099511628211ULL;
    }
    c->last=sd_now();c->cpu=cpu(CLOCK_THREAD_CPUTIME_ID)-cpu_start;
    bplus_tree_compressed_release_thread_resources();pthread_mutex_lock(&c->g->m);c->g->done++;pthread_cond_broadcast(&c->g->cv);pthread_mutex_unlock(&c->g->m);return NULL;
}
static void phase(struct fixture *f,double seconds,int report,unsigned slots){
    struct run_gate g={0};g.fixed_ops=report?f->fixed_ops:0;pthread_mutex_init(&g.m,NULL);pthread_cond_init(&g.cv,NULL);
    struct client *cs=calloc(f->clients,sizeof(*cs));pthread_t ids[16];need(cs!=NULL);
    for(int i=0;i<f->clients;i++){cs[i].f=f;cs[i].g=&g;cs[i].id=i;need(!pthread_create(ids+i,NULL,client_run,cs+i));}
    pthread_mutex_lock(&g.m);while(g.ready!=f->clients)pthread_cond_wait(&g.cv,&g.m);
    struct sd_stats before={0};if(f->tree->test_device)sd_stats(f->tree->test_device,&before);
    uint64_t begin=sd_now(),pcpu=cpu(CLOCK_PROCESS_CPUTIME_ID);g.deadline=begin+(uint64_t)(seconds*1e9);g.start=1;pthread_cond_broadcast(&g.cv);pthread_mutex_unlock(&g.m);
    uint64_t samples=0,idle_backlog=0;
    for(;;){pthread_mutex_lock(&g.m);int done=g.done==f->clients;pthread_mutex_unlock(&g.m);if(done)break;
        struct bplus_tree_scheduler_stats q;bplus_tree_compressed_scheduler_stats(f->tree,&q);struct sd_stats s={0};if(f->tree->test_device)sd_stats(f->tree->test_device,&s);
        samples++;if(q.queue_depth && s.active<slots)idle_backlog++;
        struct timespec nap={0,1000000};nanosleep(&nap,NULL);
    }
    uint64_t foreground=0,client_cpu=0,nr=0,nw=0,rh[1024]={0},wh[1024]={0},hash=0;
    for(int i=0;i<f->clients;i++){pthread_join(ids[i],NULL);if(cs[i].last>foreground)foreground=cs[i].last;client_cpu+=cs[i].cpu;nr+=cs[i].reads;nw+=cs[i].writes;hash^=cs[i].hash;for(int j=0;j<1024;j++){rh[j]+=cs[i].rh[j];wh[j]+=cs[i].wh[j];}}
    uint64_t drain_start=sd_now();need(!bplus_tree_compressed_drain_background(f->tree));if(f->tree->test_device)need(sd_drain(f->tree->test_device)==SD_OK);
    uint64_t end=sd_now(),process_cpu=cpu(CLOCK_PROCESS_CPUTIME_ID)-pcpu;
    struct sd_stats s={0};if(f->tree->test_device)sd_stats(f->tree->test_device,&s);
    struct bplus_aggregation_stats a;bplus_tree_compressed_aggregation_stats(&a,0);
    struct bplus_tree_scheduler_stats q;bplus_tree_compressed_scheduler_stats(f->tree,&q);
    struct bplus_tree_submission_stats sub;bplus_tree_compressed_submission_stats(f->tree,&sub);
    need(!s.active&&!s.queued && s.accepted==s.terminal && !s.failed);
    int real_codec=f->tree->config.algo==COMPRESS_LZ4||f->tree->config.algo==COMPRESS_ZLIB_ACCEL;
    if(!real_codec)need(!a.real_codec_calls);
    const char *backend=f->tree->config.algo==COMPRESS_LZ4?"LZ4":f->tree->config.algo==COMPRESS_ZLIB_ACCEL?"ZLIB_CPU":f->tree->test_device?"ASYNC_COPY_TEST":"DIRECT_COPY_TEST";
    if(report){
#define U(x) (unsigned long long)(x)
        struct bplus_cross_stats cross={0};need(!bplus_tree_cross_stats(f->tree,&cross,0));
        need(cross.accepted==cross.completed && cross.reservation_peak<=cross.reservation_limit);
        printf("{\"schema_version\":4,\"fixed_ops_per_client\":%llu,\"cross\":{\"window\":%llu,\"workspace_requested\":%llu,\"reservation_limit\":%llu,\"reservation_peak\":%llu,\"peak_leaves\":%llu,\"started_attempts\":%llu,\"finished_attempts\":%llu,\"accepted\":%llu,\"completed\":%llu,\"busy\":%llu,\"device_wait_ns\":%llu,\"host_step_ns\":%llu,\"snapshot_ns\":%llu,\"merge_ns\":%llu,\"commit_ns\":%llu,\"split_ns\":%llu,\"window_full_backlog\":%llu},\"legacy_stage_metrics_applicable\":%s,\"admission_wait_ns\":%llu,\"scheduler_sync_fallbacks_lifetime\":%llu,",U(f->fixed_ops),U(cross.window),U(cross.workspace_requested),U(cross.reservation_limit),U(cross.reservation_peak),U(cross.peak_leaves),U(cross.started),U(cross.finished),U(cross.accepted),U(cross.completed),U(cross.busy),U(cross.device_wait_ns),U(cross.host_step_ns),U(cross.prepare_ns),U(cross.merge_ns),U(cross.commit_ns),U(cross.split_ns),U(cross.window_full_backlog),cross.window?"false":"true",U(a.admission_wait_ns),U(q.synchronous_fallbacks));
        printf("\"progress_metrics\":{\"device_event_clock\":\"CLOCK_MONOTONIC\",\"order\":\"%s\",\"diagnostics\":%s,\"workspace_usable\":%llu,\"compact_workspace\":%llu,\"split_drains\":%llu,\"split_paused_steps\":%llu,\"split_drain_wait_ns\":%llu,\"observed\":%llu,\"ready_to_accept_ns\":%llu,\"discovery_ns\":%llu,\"max_discovery_ns\":%llu,\"queue_busy\":%llu,\"byte_busy\":%llu,\"injected_busy\":%llu,\"advertised_ready_ns\":%llu,\"eligible_idle_slot_ns\":%llu,\"credit_blocked_ns\":%llu,\"advertised_ready_peak\":%llu,\"trace_count\":%llu,\"trace_dropped\":%llu,\"roles\":[",f->sequential?"sequential":"random",getenv("SD_DIAGNOSTICS")&&atoi(getenv("SD_DIAGNOSTICS"))?"true":"false",U(cross.workspace_usable),U(cross.compact_workspace),U(cross.split_drains),U(cross.split_paused_steps),U(cross.split_drain_wait_ns),U(s.observed),U(s.ready_to_accept_ns),U(s.discovery_ns),U(s.max_discovery_ns),U(s.queue_busy),U(s.byte_busy),U(s.injected_busy),U(s.advertised_ready_ns),U(s.eligible_idle_slot_ns),U(s.credit_blocked_ns),U(s.advertised_ready_peak),U(s.trace_count),U(s.trace_dropped));
        for(int role=0;role<3;role++)printf("%s{\"role\":%d,\"queue_ns\":%llu,\"service_ns\":%llu,\"discovery_ns\":%llu}",role?",":"",role,U(s.role_queue_ns[role]),U(s.role_service_ns[role]),U(s.role_discovery_ns[role]));
        printf("],\"origin_codec_calls\":[");
        for(int origin=0;origin<BPLUS_ORIGIN_COUNT;origin++)printf("%s{\"origin\":%d,\"encode_calls\":%llu,\"decode_calls\":%llu,\"encode_bytes\":%llu,\"decode_bytes\":%llu}",origin?",":"",origin,U(a.compress[origin]),U(a.decompress[origin]),U(a.encode_bytes[origin]),U(a.decode_bytes[origin]));
        printf("],\"reused_encode_blocks\":%llu,\"reused_encoded_bytes\":%llu},\"old_metrics\":",U(a.reused_encode_blocks),U(a.reused_encoded_bytes));
        printf("{\"busy_wait_ns\":%llu,\"background_adapter_ns\":%llu,\"device_stack_reserved_bytes\":%llu,\"collect_host_ns\":%llu,\"encode_host_ns\":%llu,\"client_prefixes\":[",U(s.busy_wait_ns),U(s.adapter_ns[1]),U(s.stack_reserved_bytes),U(a.preparation_collect_ns-a.collect_device_ns),U(a.preparation_encode_ns-a.encode_device_ns));
        for(int i=0;i<f->clients;i++)printf("%s{\"operations\":%llu,\"hash\":\"%llx\"}",i?",":"",U(cs[i].reads+cs[i].writes),U(cs[i].hash));
        printf("],\"measurement\":");
        printf("{\"layout\":%d,\"backend\":\"%s\",\"keys\":%d,\"clients\":%d,\"read_pct\":%d,\"hot\":%d,\"operations\":%llu,\"reads\":%llu,\"writes\":%llu,\"trace_prefix_hash\":\"%llx\",\"foreground_ns\":%llu,\"completed_ns\":%llu,\"drain_ns\":%llu,\"get_p99_upper_ns\":%llu,\"put_p99_upper_ns\":%llu,\"process_cpu_ns\":%llu,\"client_cpu_ns\":%llu,\"device_cpu_ns\":%llu,",ZIPCACHE_AGG_LAYOUT,backend,f->keys,f->clients,f->reads,f->hot,U(nr+nw),U(nr),U(nw),U(hash),U(foreground-begin),U(end-begin),U(end-drain_start),U(percentile(rh,nr)),U(percentile(wh,nw)),U(process_cpu),U(client_cpu),U(s.device_cpu_ns-before.device_cpu_ns));
        printf("\"accepted\":%llu,\"terminal\":%llu,\"failed\":%llu,\"busy\":%llu,\"peak_active\":%llu,\"peak_inflight\":%llu,\"background_peak_active\":%llu,\"background_accepted\":%llu,\"foreground_get_accepted\":%llu,\"foreground_maintenance_accepted\":%llu,\"simulated_slot_utilization\":%.6f,\"queue_ns\":%llu,\"configured_service_ns\":%llu,\"late_ns\":%llu,\"max_late_ns\":%llu,\"copy_ns\":%llu,\"caller_wait_ns\":%llu,",U(s.accepted),U(s.terminal),U(s.failed),U(s.busy),U(s.peak_active),U(s.peak_inflight),U(s.role_peak[1]),U(s.role_accepted[1]),U(s.role_accepted[0]),U(s.role_accepted[2]),s.elapsed_ns?(double)s.active_slot_ns/(slots*s.elapsed_ns):0,U(s.queue_ns),U(s.configured_service_ns),U(s.late_ns),U(s.max_late_ns),U(s.copy_ns),U(s.wait_ns));
        printf("\"borrowed_peak_bytes\":%llu,\"descriptor_peak_bytes\":%llu,\"device_owned_requested\":%llu,\"device_owned_usable\":%llu,\"tree_queue_peak\":%llu,\"tree_queue_wait_ns\":%llu,\"batch_wait_ns\":%llu,\"pending_wait_ns\":%llu,\"read_lock_wait_ns\":%llu,\"write_lock_device_calls\":%llu,\"collect_ns_including_device\":%llu,\"merge_ns\":%llu,\"encode_ns_including_device\":%llu,\"commit_ns\":%llu,\"samples\":%llu,\"idle_with_tree_backlog_samples\":%llu,\"real_codec_calls\":%llu,\"mismatches\":0}\n",U(s.peak_borrowed_bytes),U(s.descriptor_peak_bytes),U(s.owned_requested),U(s.owned_usable),U(q.queue_peak),U(a.queue_wait_ns),U(a.batch_wait_ns),U(a.pending_wait_ns),U(a.read_lock_wait_ns),U(sub.calls_under_write_lock),U(a.preparation_collect_ns),U(a.preparation_merge_ns),U(a.preparation_encode_ns),U(a.preparation_commit_ns),U(samples),U(idle_backlog),U(a.real_codec_calls));
    }
    if(report)puts("}}");
    if(report&&f->tree->test_device&&getenv("SD_TRACE_FILE")){
        struct sd_trace *events=calloc(s.trace_count?s.trace_count:1,sizeof(*events));need(events!=NULL);
        size_t count=sd_trace_copy(f->tree->test_device,events,s.trace_count);FILE *out=fopen(getenv("SD_TRACE_FILE"),"w");need(out!=NULL);
        fputs("id,role,encode,input_bytes,output_capacity,ready_ns,accepted_ns,started_ns,terminal_ns,observed_ns,configured_service_ns,status\n",out);
        for(size_t i=0;i<count;i++){struct sd_trace *t=events+i;need(t->ready_ns<=t->accepted_ns&&t->accepted_ns<=t->started_ns&&t->started_ns<=t->terminal_ns&&t->terminal_ns<=t->observed_ns);
            fprintf(out,"%llu,%d,%d,%zu,%zu,%llu,%llu,%llu,%llu,%llu,%llu,%d\n",U(t->id),t->role,t->encode,t->input_bytes,t->output_capacity,U(t->ready_ns),U(t->accepted_ns),U(t->started_ns),U(t->terminal_ns),U(t->observed_ns),U(t->service_ns),t->status);}
        fclose(out);free(events);
    }
    free(cs);pthread_cond_destroy(&g.cv);pthread_mutex_destroy(&g.m);
}
int main(int argc,char **argv){need(argc==8);int async=!strcmp(argv[1],"async");double seconds=atof(argv[2]);struct fixture f={0};f.clients=atoi(argv[3]);f.keys=atoi(argv[4]);f.reads=atoi(argv[5]);f.hot=atoi(argv[6]);double warm=atof(argv[7]);need(seconds>0&&f.clients>=1&&f.clients<=16&&f.keys>=32&&f.keys%f.clients==0&&(!f.hot||f.clients==1));
    setenv("BTREE_BG_COMPACTION","1",1);setenv("BTREE_SHARDS",f.hot?"1":"8",1);setenv("BTREE_PROFILE_SUBMISSION","1",1);
    struct compression_config config=bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    if(async)config.algo=COMPRESS_ASYNC_COPY_TEST;
    else if(!strcmp(argv[1],"direct"))config.algo=COMPRESS_DIRECT_COPY_TEST;
    else if(!strcmp(argv[1],"lz4"))config.algo=COMPRESS_LZ4;
    else if(!strcmp(argv[1],"zlib"))config.algo=COMPRESS_ZLIB_ACCEL;
    else need(0);
    f.tree=bplus_tree_compressed_init_with_config(32,64,&config);need(f.tree!=NULL&&f.tree->config.algo==config.algo);f.versions=calloc(f.keys+1,sizeof(int));need(f.versions!=NULL);
    f.fixed_ops=getenv("BTREE_TRACE_OPS")?strtoull(getenv("BTREE_TRACE_OPS"),NULL,10):0;
    f.keymap=malloc(f.keys*sizeof(int));need(f.keymap!=NULL);
    f.sequential=getenv("BTREE_TRACE_ORDER")&&!strcmp(getenv("BTREE_TRACE_ORDER"),"sequential");
    for(int i=0;i<f.keys;i++)f.keymap[i]=i+1;
    uint32_t map_rng=11;if(!f.sequential)for(int i=f.keys-1;i>0;i--){int j=(int)(next(&map_rng)%(unsigned)(i+1));int x=f.keymap[i];f.keymap[i]=f.keymap[j];f.keymap[j]=x;}
    if(async)need(sd_configure_idle(f.tree->test_device,0,0,0)==SD_OK);
    for(int k=1;k<=f.keys;k++){uint8_t b[128];f.versions[k]=1;payload(k,1,b);need(!bplus_tree_compressed_put_with_payload(f.tree,k,b,128,1));}need(!bplus_tree_compressed_drain_background(f.tree));
    unsigned slots=getenv("SD_SLOTS")?(unsigned)atoi(getenv("SD_SLOTS")):4;
    if(async){uint64_t en=getenv("SD_ENCODE_US")?strtoull(getenv("SD_ENCODE_US"),NULL,10):1000,de=getenv("SD_DECODE_US")?strtoull(getenv("SD_DECODE_US"),NULL,10):1000;need(sd_configure_idle(f.tree->test_device,en*1000,de*1000,0)==SD_OK);}
    if(warm>0)phase(&f,warm,0,slots);
    bplus_tree_compressed_aggregation_stats(NULL,1);bplus_tree_compressed_submission_reset(f.tree);if(async)need(sd_reset_stats(f.tree->test_device)==SD_OK);
    if(async){unsigned every=getenv("SD_TAIL_EVERY")?(unsigned)atoi(getenv("SD_TAIL_EVERY")):0,mult=getenv("SD_TAIL_MULTIPLIER")?(unsigned)atoi(getenv("SD_TAIL_MULTIPLIER")):10;
        need(sd_tail_profile(f.tree->test_device,every,mult)==SD_OK);if(getenv("SD_BUSY_NEXT"))sd_busy_next(f.tree->test_device,(unsigned)atoi(getenv("SD_BUSY_NEXT")));}
    need(!bplus_tree_cross_stats(f.tree,NULL,1));
    phase(&f,seconds,1,slots);
    if(async)need(sd_configure_idle(f.tree->test_device,0,0,0)==SD_OK);
    for(int k=1;k<=f.keys;k++){struct kv_pair p;uint8_t b[128];payload(k,f.versions[k],b);need(!bplus_tree_compressed_get_record(f.tree,k,&p)&&p.stored_value==f.versions[k]&&!memcmp(b,p.payload,128));}
    bplus_tree_compressed_deinit(f.tree);free(f.keymap);free(f.versions);bplus_tree_compressed_release_thread_resources();return 0;
}
