#include "compressed_test_utils.h"
static void require(int x,const char *s){if(!x){fprintf(stderr,"failure test: %s\n",s);abort();}}
static void payload(int k,int v,uint8_t p[128]){memset(p,k%13,128);memcpy(p,&v,4);}
static struct bplus_tree_compressed *make(void){
    struct compression_config c=bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    struct bplus_tree_compressed *t=bplus_tree_compressed_init_with_config(4,64,&c);
    require(t!=NULL,"init");for(int k=1;k<=256;k++){uint8_t p[128];payload(k,1,p);
        require(!bplus_tree_compressed_put_with_payload(t,k,p,128,1),"preload");}
    require(!bplus_tree_compressed_drain_background(t),"preload drain");return t;
}
static void expect(struct bplus_tree_compressed *t,int k,int v){
    struct kv_pair r;uint8_t p[128];payload(k,v,p);
    require(!bplus_tree_compressed_get_record(t,k,&r)&&r.stored_value==v&&!memcmp(r.payload,p,128),"old/new value exact");
}
int main(void){
    setenv("BTREE_BG_COMPACTION","0",1);setenv("BTREE_SHARDS","1",1);
    struct bplus_tree_compressed *t=make();
    for(int nth=0;nth<2;nth++){
        struct kv_pair r,before;memset(&r,0xa5,sizeof(r));before=r;
        bplus_tree_compressed_test_fail_allocation(nth);
        int rc=bplus_tree_compressed_get_record(t,7,&r);
        bplus_tree_compressed_test_fail_allocation(-1);
        require(rc==BPLUS_RECORD_NO_SPACE&&!memcmp(&r,&before,sizeof(r)),"allocation failure is not a miss/partial result");expect(t,7,1);
    }
    struct compressed_leaf_ref *ref=list_entry(t->tree->list[0].next,struct compressed_leaf_ref,link);
    struct simple_leaf_node *leaf=(void*)ref->payload;
    int saved=leaf->compressed_size;leaf->compressed_size=0;
    struct kv_pair record;require(bplus_tree_compressed_get_record(t,7,&record)==BPLUS_RECORD_CODEC_ERROR,"truncated representation");
    leaf->compressed_size=saved;
#if ZIPCACHE_AGG_LAYOUT > 0
    uint8_t header[4];memcpy(header,leaf->compressed_data,4);memset(leaf->compressed_data,0,4);
    require(bplus_tree_compressed_get_record(t,7,&record)==BPLUS_RECORD_CODEC_ERROR,"format tag validation");
    memcpy(leaf->compressed_data,header,4);
#endif
    expect(t,1,1);bplus_tree_compressed_deinit(t);
    /* Sweep fallible synchronous replacement preparation, not just init. */
    for(int nth=0;nth<32;nth++){
        t=make();for(int k=1;k<=3;k++){uint8_t p[128];payload(k,2,p);require(!bplus_tree_compressed_put_with_payload(t,k,p,128,2),"active update");}
        uint8_t p[128];payload(4,3,p);bplus_tree_compressed_test_fail_allocation(nth);
        int rc=bplus_tree_compressed_put_with_payload(t,4,p,128,3);
        bplus_tree_compressed_test_fail_allocation(-1);
        expect(t,4,rc?1:3);for(int k=1;k<=3;k++)expect(t,k,2);
        bplus_tree_compressed_deinit(t);
    }
    bplus_tree_compressed_release_thread_resources();struct bplus_tree_allocation_audit audit;
    bplus_tree_compressed_allocation_audit(&audit,0);require(!audit.enabled||!audit.usable,"failed-path leak");puts("failure preservation passed");return 0;
}
