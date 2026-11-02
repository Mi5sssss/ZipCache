#include "compressed_test_utils.h"
#include "async_copy_device.h"
#include <sched.h>
#define need(x) do {if(!(x)){fprintf(stderr,"cross check line %d: %s\n",__LINE__,#x);abort();}}while(0)
static void bytes(int k,int v,uint8_t p[128]){memset(p,k,128);memcpy(p,&v,sizeof(v));}
static int put(struct bplus_tree_compressed *t,int k,int v){uint8_t p[128];bytes(k,v,p);return bplus_tree_compressed_put_with_payload(t,k,p,128,v);}
static void get(struct bplus_tree_compressed *t,int k,int v){uint8_t p[128];struct kv_pair out;bytes(k,v,p);need(!bplus_tree_compressed_get_record(t,k,&out)&&out.stored_value==v&&!memcmp(p,out.payload,128));}
static struct bplus_tree_compressed *make(void){struct compression_config c=bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);c.algo=COMPRESS_ASYNC_COPY_TEST;return bplus_tree_compressed_init_with_config(4,64,&c);}
static void timeline(struct sd_device *d)
{
    struct sd_trace events[8192];size_t n=sd_trace_copy(d,events,8192);
    for(size_t i=0;i<n;i++){struct sd_trace *t=events+i;
        need(t->ready_ns<=t->accepted_ns&&t->accepted_ns<=t->started_ns&&
             t->started_ns<=t->terminal_ns&&t->terminal_ns<=t->observed_ns);}
}
static void gated_parallel(int failure)
{
    struct bplus_tree_compressed *t=make();need(t!=NULL);
    need(sd_configure_idle(t->test_device,100,10,1)==SD_OK);
    sd_fault_next(t->test_device,-failure);
    /* Four independent shards: each gets exactly one pending plus active. */
    for(int shard=0;shard<4;shard++)for(int i=0;i<4;i++)need(!put(t,1+shard+8*i,1));
    uint64_t limit=sd_now()+10000000000ULL;struct sd_stats d;
    do {sd_stats(t->test_device,&d);need(sd_now()<limit);sched_yield();}while(d.role_active[1]<4);
    need(d.role_peak[1]==4);
    for(int shard=0;shard<4;shard++){get(t,1+shard,1);need(!put(t,1+shard,9));get(t,1+shard,9);}
    struct bplus_tree_scheduler_stats q;
    if(!failure) {
        need(sd_release_id(t->test_device,4)==SD_OK);
        do {bplus_tree_compressed_scheduler_stats(t,&q);need(sd_now()<limit);sched_yield();}while(q.completed_tasks!=1);
        /* Refill the free leaf slot before any of the first three finish. */
        for(int i=0;i<4;i++)need(!put(t,5+8*i,1));
        do {sd_stats(t->test_device,&d);need(sd_now()<limit);sched_yield();}while(d.accepted!=5);
        need(d.active==4&&d.terminal==1);
        need(sd_release_id(t->test_device,5)==SD_OK);
        need(sd_release_id(t->test_device,2)==SD_OK);
        need(sd_release_id(t->test_device,3)==SD_OK);
        need(sd_release_id(t->test_device,1)==SD_OK);
    }
    for(;;){sd_advance(t->test_device,100);sched_yield();need(sd_now()<limit);
        bplus_tree_compressed_scheduler_stats(t,&q);
        if(failure?q.failed_tasks==4:q.completed_tasks==5)break;
    }
    need(bplus_tree_compressed_drain_background(t)==(failure?-1:0));
    if(!failure)need(sd_configure_idle(t->test_device,0,0,0)==SD_OK);
    for(int shard=0;shard<4;shard++){get(t,1+shard,9);get(t,9+shard,1);}
    struct bplus_cross_stats c;need(!bplus_tree_cross_stats(t,&c,0));
    need(c.peak_leaves==4&&c.accepted==c.completed&&c.reservation_peak<=c.reservation_limit);
    sd_stats(t->test_device,&d);need(d.accepted==d.terminal&&!d.active&&!d.queued);
    timeline(t->test_device);
    bplus_tree_compressed_deinit(t);
}
static void split_drain(void)
{
    struct bplus_tree_compressed *t=make();need(t!=NULL);
    for(int i=0;i<8;i++)need(!put(t,1+8*i,1));
    need(!bplus_tree_compressed_drain_background(t));
    struct sd_stats d;sd_stats(t->test_device,&d);uint64_t id_base=d.accepted;
    need(sd_reset_stats(t->test_device)==SD_OK);
    need(!bplus_tree_cross_stats(t,NULL,1));
    struct bplus_tree_scheduler_stats q;bplus_tree_compressed_scheduler_stats(t,&q);
    uint64_t expected_completed=q.completed_tasks+5;
    /* Controlled intent at the real split fallback. Natural overflow is also
     * covered by tree_contract; this gate isolates its drain protocol. */
    setenv("BTREE_TEST_CROSS_FORCE_SPLIT_KEY","49",1);
    need(sd_configure_idle(t->test_device,100,100,1)==SD_OK);
    for(int shard=1;shard<=3;shard++)for(int i=0;i<4;i++)need(!put(t,1+shard+8*i,1));
    uint64_t limit=sd_now()+10000000000ULL;struct bplus_cross_stats c;
    do {sd_stats(t->test_device,&d);need(sd_now()<limit);sched_yield();}while(d.accepted<3);
    /* Preload left keys 49,57 in active; one new key seals those with key 65. */
    need(!put(t,65,1));need(!put(t,73,1));
    do {sd_stats(t->test_device,&d);need(sd_now()<limit);sched_yield();}while(d.accepted<4);
    need(sd_release_id(t->test_device,id_base+4)==SD_OK);
    do {need(!bplus_tree_cross_stats(t,&c,0));need(sd_now()<limit);sched_yield();}while(!c.split_latched);
    for(int i=0;i<4;i++)need(!put(t,5+8*i,1));
    /* A completed other slot has a successor but cannot refill the drain set. */
    need(sd_release_id(t->test_device,id_base+1)==SD_OK);
    do {need(!bplus_tree_cross_stats(t,&c,0));need(sd_now()<limit);sched_yield();}while(!c.split_paused_steps);
    sd_stats(t->test_device,&d);need(d.accepted==4&&d.active==2);
    need(sd_release_id(t->test_device,id_base+3)==SD_OK);need(sd_release_id(t->test_device,id_base+2)==SD_OK);
    for(;;){sd_advance(t->test_device,100);sched_yield();need(sd_now()<limit);
        bplus_tree_compressed_scheduler_stats(t,&q);if(q.completed_tasks==expected_completed)break;}
    need(!bplus_tree_compressed_drain_background(t));
    need(!bplus_tree_cross_stats(t,&c,0));need(c.split_drains==1&&!c.split_latched&&c.accepted==c.completed);
    need(sd_configure_idle(t->test_device,0,0,0)==SD_OK);
    for(int i=0;i<10;i++)get(t,1+8*i,1);
    timeline(t->test_device);
    bplus_tree_compressed_deinit(t);unsetenv("BTREE_TEST_CROSS_FORCE_SPLIT_KEY");
}
int main(void)
{
    setenv("BTREE_BG_COMPACTION","1",1);setenv("BTREE_CROSS_LEAF","1",1);
    setenv("BTREE_CROSS_OUTSTANDING","4",1);setenv("BTREE_AGG_BACKPRESSURE","1",1);
    setenv("BTREE_SHARDS","8",1);setenv("BTREE_BG_INCLUDE_TRIGGER","0",1);
    setenv("SD_SLOTS","4",1);setenv("SD_ENCODE_US","0",1);setenv("SD_DECODE_US","0",1);
    gated_parallel(0);gated_parallel(1);gated_parallel(2);gated_parallel(3);split_drain();
    setenv("BTREE_CROSS_BYTES","1",1);need(make()==NULL);unsetenv("BTREE_CROSS_BYTES");
    setenv("BTREE_TEST_FAIL_CROSS_ALLOC","1",1);need(make()==NULL);unsetenv("BTREE_TEST_FAIL_CROSS_ALLOC");
    for(int stage=1;stage<=4;stage++) {
        unsetenv("BTREE_TEST_CROSS_FAIL_STAGE");
        struct bplus_tree_compressed *t=make();need(t!=NULL);
        for(int i=0;i<8;i++)need(!put(t,1+8*i,1));
        need(!bplus_tree_compressed_drain_background(t));
        char value[8];snprintf(value,sizeof(value),"%d",stage);setenv("BTREE_TEST_CROSS_FAIL_STAGE",value,1);
        need(!put(t,1,2));need(!put(t,9,2));
        need(bplus_tree_compressed_drain_background(t)==-1);
        for(int i=0;i<8;i++)get(t,1+8*i,i<2?2:1);
        bplus_tree_compressed_deinit(t);
    }
    unsetenv("BTREE_TEST_CROSS_FAIL_STAGE");
    bplus_tree_compressed_release_thread_resources();
    struct bplus_tree_allocation_audit a;bplus_tree_compressed_allocation_audit(&a,0);need(!a.enabled||!a.usable);
    puts("cross-leaf deterministic contracts passed");return 0;
}
