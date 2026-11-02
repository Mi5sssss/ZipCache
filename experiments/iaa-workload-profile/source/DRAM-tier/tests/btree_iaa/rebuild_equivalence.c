#include "compressed_test_utils.h"
static void need(int x){if(!x)abort();}
int main(void){
    setenv("BTREE_BG_COMPACTION","0",1);setenv("BTREE_SHARDS","1",1);
    for(int codec=0;codec<2;codec++)for(int random=0;random<2;random++){
        struct compression_config cfg=bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
        if(codec)cfg.algo=COMPRESS_ZLIB_ACCEL;cfg.compression_level=1;
        struct bplus_tree_compressed *t=bplus_tree_compressed_init_with_config(4,64,&cfg);need(t!=NULL);
        struct kv_pair pairs[COMPRESSED_LEAF_SIZE/sizeof(struct kv_pair)+1];
        uint32_t seed=11;
        for(size_t i=0;i<sizeof(pairs)/sizeof(*pairs);i++){
            pairs[i].key=(int)i+1;pairs[i].stored_value=(int)i;
            for(size_t j=0;j<sizeof(pairs[i].payload);j++){
                seed^=seed<<13;seed^=seed>>17;seed^=seed<<5;
                pairs[i].payload[j]=random?(uint8_t)seed:(uint8_t)(i%3);
            }
        }
        for(size_t n=0;n<sizeof(pairs)/sizeof(*pairs);n++)
            need(!bplus_tree_compressed_test_rebuild_equivalence(t,pairs,n));
        need(bplus_tree_compressed_test_rebuild_equivalence(t,pairs,sizeof(pairs)/sizeof(*pairs))==-2);
        for(int nth=0;nth<20;nth++){
            bplus_tree_compressed_release_thread_resources();
            struct bplus_tree_allocation_audit before,after;
            bplus_tree_compressed_allocation_audit(&before,0);
            bplus_tree_compressed_test_fail_allocation(nth);
            int rc=bplus_tree_compressed_test_rebuild_equivalence(t,pairs,COMPRESSED_LEAF_SIZE/sizeof(*pairs));
            bplus_tree_compressed_test_fail_allocation(-1);
            need(rc==0||rc==-1);
            bplus_tree_compressed_release_thread_resources();
            bplus_tree_compressed_allocation_audit(&after,0);
            need(!before.enabled||before.usable==after.usable);
            need(!bplus_tree_compressed_test_rebuild_equivalence(t,pairs,10));
        }
        bplus_tree_compressed_deinit(t);bplus_tree_compressed_release_thread_resources();
    }
    return 0;
}
