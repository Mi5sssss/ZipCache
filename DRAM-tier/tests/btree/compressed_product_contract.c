#define _POSIX_C_SOURCE 200809L
#include <sched.h>
#include <time.h>
#include "compressed_test_utils.h"

static void require(int ok, const char *why)
{ if (!ok) { fprintf(stderr,"product_contract: %s\n",why); exit(1); } }
static compression_algo_t contract_codec=COMPRESS_LZ4;
static struct bplus_tree_compressed *make_tree(int order, int subpages)
{
    struct compression_config config=bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    config.algo=contract_codec;
    config.default_sub_pages=subpages;
    struct bplus_tree_compressed *tree=bplus_tree_compressed_init_with_config(order,64,&config);
    require(tree!=NULL,"init failed"); return tree;
}
static int versions[2048];
static void bytes_for(int key,int value,uint8_t out[128])
{ for (int i=0;i<128;i++) out[i]=(uint8_t)(key*31+value*7+i); }
static int put(struct bplus_tree_compressed *tree,int key,int value)
{
    uint8_t bytes[128]; bytes_for(key,value,bytes);
    int rc=bplus_tree_compressed_put_with_payload(tree,key,bytes,128,value);
    if (!rc) versions[key]=value; return rc;
}
static int verify(const struct kv_pair *pair,void *context)
{
    (void)context; uint8_t bytes[128];
    if (pair->key<1 || pair->key>=2048 || pair->stored_value!=versions[pair->key]) return -1;
    bytes_for(pair->key,pair->stored_value,bytes);
    return memcmp(bytes,pair->payload,128)!=0;
}
static void verify_keys(struct bplus_tree_compressed *tree,int count)
{
    require(!bplus_tree_compressed_verify(tree,verify,NULL),"payload mismatch");
    for (int k=1;k<=count;k++) require(bplus_tree_compressed_get(tree,k)==
        (versions[k]?versions[k]:-1),"missing/incorrect visible key");
}
static void split_failures(void)
{
    setenv("BTREE_BG_COMPACTION","0",1);
    setenv("BTREE_SHARDS","1",1);
    for (int pages=1;pages<=4;pages*=4) for (int stage=1;stage<=3;stage++) {
        memset(versions,0,sizeof(versions));
        struct bplus_tree_compressed *tree=make_tree(4,pages);
        for (int k=1;k<=200;k++) require(!put(tree,k,k+1),"split preload");
        char s[8]; snprintf(s,sizeof(s),"%d",stage);
        setenv("BTREE_TEST_FAIL_SPLIT_STAGE",s,1);
        int k;
        for (k=201;k<300;k++) if (put(tree,k,k+1)) break;
        require(k<300,"split fault was not reached");
        verify_keys(tree,k-1);
        unsetenv("BTREE_TEST_FAIL_SPLIT_STAGE");
        require(!put(tree,k,k+1),"split did not recover");
        verify_keys(tree,k);
        for (int d=1;d<=k;d+=2) {
            require(!bplus_tree_compressed_delete(tree,d),"rebalance delete"); versions[d]=0;
        }
        verify_keys(tree,k);
        bplus_tree_compressed_deinit(tree);
    }
}
static void wait_accepted(struct bplus_tree_compressed *tree)
{
    struct timespec start,now; clock_gettime(CLOCK_MONOTONIC,&start);
    for (;;) {
        struct bplus_tree_contract_stats stats;
        require(!bplus_tree_compressed_test_contract(tree,-1,&stats),"contract unavailable");
        if (stats.accepted) return;
        clock_gettime(CLOCK_MONOTONIC,&now);
        require(now.tv_sec-start.tv_sec<10,"contract acceptance timeout");
        sched_yield();
    }
}
struct delete_arg { struct bplus_tree_compressed *tree; int rc; };
static void *delete_pending(void *p)
{ struct delete_arg *arg=p; arg->rc=bplus_tree_compressed_delete(arg->tree,2); return NULL; }
static void contracts(void)
{
    setenv("BTREE_BG_COMPACTION","1",1);
    for (int mode=1;mode<=5;mode++) {
        char s[8]; snprintf(s,sizeof(s),"%d",mode);
        setenv("BTREE_TEST_CONTRACT_MODE",s,1);
        setenv("BTREE_SHARDS",mode==2?"8":"1",1);
        setenv("BTREE_TEST_CONTRACT_CLOSED",mode==2?"1":"0",1);
        memset(versions,0,sizeof(versions));
        struct bplus_tree_compressed *tree=make_tree(16,1);
        int count=mode==2?32:4;
        for (int k=1;k<=count;k++) require(!put(tree,k,k+1),"contract setup PUT");
        if (mode==2) {
            wait_accepted(tree);
            require(!put(tree,1,901),"active overwrite of pending");
            require(bplus_tree_compressed_get(tree,1)==901,"active > pending precedence");
            struct delete_arg arg={tree,-2}; pthread_t thread;
            require(!pthread_create(&thread,NULL,delete_pending,&arg),"DELETE thread create");
            require(!bplus_tree_compressed_test_contract(tree,1,NULL),"release gate");
            pthread_join(thread,NULL); require(!arg.rc,"DELETE after pending failed"); versions[2]=0;
        }
        int rc=bplus_tree_compressed_drain_background(tree);
        require(rc==(mode>=4?-1:0),"wrong failure/drain contract");
        verify_keys(tree,count);
        struct bplus_tree_contract_stats stats;
        bplus_tree_compressed_test_contract(tree,-1,&stats);
        require(stats.accepted==stats.terminal && !stats.in_flight,"accepted job lost/duplicated");
        if (mode==2) require(stats.out_of_order>0,"reverse completion was not exercised");
        if (mode==3) require(stats.busy==1,"busy-once not exercised");
        if (mode>=4) {
            require(stats.failures>0,"hard failure not exercised");
            require(!put(tree,5,6)&&!put(tree,6,7),"remaining active slots unusable");
            require(put(tree,7,8)==-1,"failed pending incorrectly discarded");
        }
        bplus_tree_compressed_deinit(tree);
    }
    /* Allocation failure before submission must not dereference NULL arrays. */
    setenv("BTREE_TEST_CONTRACT_MODE","2",1);
    setenv("BTREE_TEST_CONTRACT_CLOSED","0",1);
    setenv("BTREE_SHARDS","1",1);
    setenv("BTREE_TEST_FAIL_BATCH_ALLOC","1",1);
    struct bplus_tree_compressed *tree=make_tree(16,1);
    for (int k=1;k<=4;k++) require(!put(tree,k,k+1),"batch allocation setup");
    require(bplus_tree_compressed_drain_background(tree)==-1,"batch allocation not surfaced");
    verify_keys(tree,4); bplus_tree_compressed_deinit(tree);
    unsetenv("BTREE_TEST_FAIL_BATCH_ALLOC");
    /* Shutdown itself releases the deterministic completion gate, then joins. */
    setenv("BTREE_TEST_CONTRACT_CLOSED","1",1);
    tree=make_tree(16,1);
    for (int k=1;k<=4;k++) require(!put(tree,k,k+1),"shutdown setup");
    wait_accepted(tree); bplus_tree_compressed_deinit(tree);
    unsetenv("BTREE_TEST_CONTRACT_MODE"); unsetenv("BTREE_TEST_CONTRACT_CLOSED");
}
static void reject_short_decode(void)
{
    setenv("BTREE_BG_COMPACTION","0",1); setenv("BTREE_SHARDS","1",1);
    memset(versions,0,sizeof(versions));
    struct bplus_tree_compressed *tree=make_tree(16,1);
    for (int k=1;k<=12;k++) require(!put(tree,k,k+1),"corrupt-input setup");
    struct compressed_leaf_ref *ref=(struct compressed_leaf_ref *)tree->tree->root;
    struct simple_leaf_node *leaf=(struct simple_leaf_node *)ref->payload;
    require(leaf->is_compressed,"missing base for corrupt-input test");
    uint8_t *saved=malloc(leaf->compressed_capacity);
    require(saved!=NULL,"corrupt snapshot allocation");
    memcpy(saved,leaf->compressed_data,leaf->compressed_capacity);
    struct subpage_index_entry index=leaf->subpage_index[0];
    int target=0;
    for (int k=1;k<=12;k++) {
        int active=0;
        for (int j=0;j<3;j++) active|=leaf->active[j].key==k;
        if (!active) { target=k; break; }
    }
    require(target>0,"no base-only key");
    /* Valid LZ4 encoding of one byte is not a valid complete logical leaf. */
    char one=0;
    int size=LZ4_compress_default(&one,leaf->compressed_data,1,(int)leaf->compressed_capacity);
    require(size>0,"short encoding failed");
    leaf->subpage_index[0].offset=0; leaf->subpage_index[0].length=(uint32_t)size;
    require(bplus_tree_compressed_get(tree,target)==-1,"short decode exposed tail bytes");
    require(bplus_tree_compressed_verify(tree,verify,NULL)==-1,"corrupt audit succeeded");
    memcpy(leaf->compressed_data,saved,leaf->compressed_capacity); leaf->subpage_index[0]=index;
    free(saved); verify_keys(tree,12); bplus_tree_compressed_deinit(tree);
}
static void include_trigger(void)
{
    setenv("BTREE_BG_INCLUDE_TRIGGER","1",1);
    setenv("BTREE_BG_COMPACTION","1",1);
    setenv("BTREE_SHARDS","1",1);
    setenv("BTREE_TEST_CONTRACT_MODE","2",1);
    setenv("BTREE_TEST_CONTRACT_CLOSED","1",1);
    memset(versions,0,sizeof(versions));
    struct bplus_tree_compressed *tree=make_tree(16,1);
    for (int k=1;k<=4;k++) require(!put(tree,k,k+1),"include-trigger setup");
    wait_accepted(tree);
    struct compressed_leaf_ref *ref=(struct compressed_leaf_ref *)tree->tree->root;
    struct simple_leaf_node *leaf=(struct simple_leaf_node *)ref->payload;
    require(!leaf->active[0].key && leaf->pending,"trigger did not join pending");
    require(!put(tree,4,904),"active override of included trigger failed");
    verify_keys(tree,4);
    bplus_tree_compressed_test_contract(tree,1,NULL);
    require(!bplus_tree_compressed_drain_background(tree),"include-trigger drain");
    verify_keys(tree,4);
    bplus_tree_compressed_deinit(tree);
    unsetenv("BTREE_TEST_CONTRACT_MODE"); unsetenv("BTREE_TEST_CONTRACT_CLOSED");
    unsetenv("BTREE_BG_INCLUDE_TRIGGER");
}
int main(void)
{
    require(sizeof(struct bplus_leaf)==808 || sizeof(void*)!=8,"ordinary ABI changed unexpectedly");
#ifdef __APPLE__
    require(sizeof(struct compressed_leaf_ref)+sizeof(struct simple_leaf_node)-
            sizeof(((struct simple_leaf_node*)0)->active)<=384,"compact metadata budget exceeded");
#endif
    split_failures(); contracts();
#ifdef HAVE_ZLIB
    contract_codec=COMPRESS_ZLIB_ACCEL;
    contracts();
    contract_codec=COMPRESS_LZ4;
#endif
    reject_short_decode(); include_trigger();
    bplus_tree_compressed_release_thread_resources();
    struct bplus_tree_allocation_audit audit;
    bplus_tree_compressed_allocation_audit(&audit,0);
    require(!audit.enabled || !audit.live_allocations,"contract/split allocation leak");
    puts("product_contract: OK (atomic split, multi-subpage ownership, completion order, busy, failure, length, DELETE, shutdown)");
    return 0;
}
