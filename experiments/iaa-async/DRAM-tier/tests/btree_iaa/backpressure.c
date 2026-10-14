#include "compressed_test_utils.h"
static void require(int ok,const char *why){if(!ok){fprintf(stderr,"backpressure: %s\n",why);abort();}}
static struct bplus_tree_compressed *make(void){
    struct compression_config c=bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    struct bplus_tree_compressed *t=bplus_tree_compressed_init_with_config(4,64,&c);
    require(t!=NULL,"init");return t;
}
static void fill(struct bplus_tree_compressed *t,int first,int last){
    for(int k=first;k<=last;k++){uint8_t bytes[128];memset(bytes,k,128);
        require(!bplus_tree_compressed_put_with_payload(t,k,bytes,128,k),"put");}
}
int main(void){
    setenv("BTREE_AGG_BACKPRESSURE","1",1);setenv("BTREE_BG_COMPACTION","1",1);
    setenv("BTREE_SHARDS","8",1);setenv("BTREE_BG_BATCH_SIZE","1",1);
    setenv("BTREE_BG_QUEUE_CAPACITY","1",1);setenv("BTREE_PROFILE_SUBMISSION","1",1);
    setenv("BTREE_TEST_WORKER_DELAY_US","1000",1);
    struct bplus_tree_compressed *t=make();fill(t,1,96);
    require(!bplus_tree_compressed_drain_background(t),"drain");
    for(int k=1;k<=96;k++){struct kv_pair r;uint8_t p[128];memset(p,k,128);
        require(!bplus_tree_compressed_get_record(t,k,&r)&&!memcmp(r.payload,p,128),"read after waiting");}
    struct bplus_tree_scheduler_stats scheduler;bplus_tree_compressed_scheduler_stats(t,&scheduler);
    struct bplus_tree_submission_stats profile;bplus_tree_compressed_submission_stats(t,&profile);
    require(!scheduler.synchronous_fallbacks&&!scheduler.split_fallbacks,"unexpected fallback");
    require(!profile.calls_under_write_lock,"codec under write lock");
    require(scheduler.completed_tasks==scheduler.submitted_tasks,"missing task");
    bplus_tree_compressed_deinit(t);
    setenv("BTREE_SHARDS","1",1);setenv("BTREE_AGG_PENDING_BYTES","0",1);
    t=make();fill(t,1,3);uint8_t bytes[128]={0};
    require(bplus_tree_compressed_put_with_payload(t,4,bytes,128,4)==-1,"permanent no-space must reject");
    for(int k=1;k<=3;k++)require(bplus_tree_compressed_get(t,k)==k,"old active lost on no-space");
    require(bplus_tree_compressed_get(t,4)==-1,"rejected write became visible");bplus_tree_compressed_deinit(t);
    unsetenv("BTREE_AGG_PENDING_BYTES");t=make();fill(t,1,3);
    setenv("BTREE_TEST_FAIL_PENDING_ALLOC","1",1);
    require(bplus_tree_compressed_put_with_payload(t,4,bytes,128,4)==-1,"allocation failure silently rebuilt");
    unsetenv("BTREE_TEST_FAIL_PENDING_ALLOC");fill(t,4,8);
    require(!bplus_tree_compressed_drain_background(t),"reservation leaked on allocation failure");
    bplus_tree_compressed_deinit(t);bplus_tree_compressed_release_thread_resources();
    struct bplus_tree_allocation_audit audit;bplus_tree_compressed_allocation_audit(&audit,0);
    require(!audit.enabled||!audit.usable,"teardown leak");return 0;
}
