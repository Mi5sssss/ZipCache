#include "compressed_test_utils.h"
static void require(int ok){if(!ok){fprintf(stderr,"complete record API failure\n");abort();}}
int main(void){
    setenv("BTREE_BG_COMPACTION","0",1);setenv("BTREE_SHARDS","1",1);
    for(int sub=1;sub<=4;sub*=4){
        struct compression_config c=bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
        c.default_sub_pages=sub;
        struct bplus_tree_compressed *t=bplus_tree_compressed_init_with_config(4,64,&c);require(t!=NULL);
        struct kv_pair out,before;memset(&out,0xa5,sizeof(out));before=out;
        require(bplus_tree_compressed_get_record(t,7,&out)==BPLUS_RECORD_NOT_FOUND);
        require(!memcmp(&out,&before,sizeof(out)));
        require(bplus_tree_compressed_get_record(NULL,7,&out)==BPLUS_RECORD_INVALID);
        require(bplus_tree_compressed_get_record(t,7,NULL)==BPLUS_RECORD_INVALID);
        for(int k=1;k<=1000;k++){uint8_t p[128];for(int j=0;j<128;j++)p[j]=(uint8_t)(k+j);
            require(!bplus_tree_compressed_put_with_payload(t,k,p,128,k+1));}
        for(int k=1;k<=1000;k++){uint8_t p[128];for(int j=0;j<128;j++)p[j]=(uint8_t)(k+j);
            require(bplus_tree_compressed_get_record(t,k,&out)==BPLUS_RECORD_OK);
            require(out.key==k&&out.stored_value==k+1&&!memcmp(p,out.payload,128));
            require(bplus_tree_compressed_get(t,k)==out.stored_value);}
        require(bplus_tree_compressed_get_range(t,995,1000)==1001);
        bplus_tree_compressed_deinit(t);
    }
    bplus_tree_compressed_release_thread_resources();return 0;
}
