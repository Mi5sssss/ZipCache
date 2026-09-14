#include "compressed_test_utils.h"
#include <pthread.h>
static void require(int ok, const char *message) { if (!ok) { fprintf(stderr,"FAIL L%d: %s\n",ZIPCACHE_AGG_LAYOUT,message); abort(); } }
static void bytes(int key,int v,uint8_t p[128]) {
    for(int i=0;i<128;i++) p[i]=(uint8_t)((key/17+i/13)%7+'a');
    memcpy(p,&key,sizeof(key)); memcpy(p+4,&v,sizeof(v));
}
static void check_key(struct bplus_tree_compressed *t,int k,int v) {
    struct kv_pair r; memset(&r,0xa5,sizeof(r)); uint8_t p[128]; bytes(k,v,p);
    require(bplus_tree_compressed_get_record(t,k,&r)==BPLUS_RECORD_OK,"get_record status");
    require(r.key==k && r.stored_value==v+1 && !memcmp(r.payload,p,128),"full GET value");
    require(bplus_tree_compressed_get(t,k)==v+1,"legacy GET");
}
int main(int argc,char **argv) {
    const char *codec=argc>1?argv[1]:"lz4";
    struct compression_config c=bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    c.algo=!strcmp(codec,"raw")?COMPRESS_RAW_PACKED:!strcmp(codec,"zstd")?COMPRESS_ZSTD_EXPERIMENT:
        !strcmp(codec,"zlib")?COMPRESS_ZLIB_ACCEL:COMPRESS_LZ4;
    struct bplus_tree_compressed *t=bplus_tree_compressed_init_with_config(4,64,&c);
    require(t!=NULL,"init");require(t->config.algo==c.algo,"actual codec");int version[1001]={0};
    for(int k=1;k<=1000;k++){ uint8_t p[128];bytes(k,0,p);require(!bplus_tree_compressed_put_with_payload(t,k,p,128,1),"preload"); }
    require(!bplus_tree_compressed_drain_background(t),"preload drain");
    for(int k=1;k<=1000;k++)check_key(t,k,0);
    uint32_t state=11;
    for(int i=0;i<6000;i++) {
        state=state*1664525u+1013904223u; int k=1+(int)(state%1000);
        int v=++version[k];uint8_t p[128];bytes(k,v,p);
        require(!bplus_tree_compressed_put_with_payload(t,k,p,128,v+1),"update");check_key(t,k,v);
        if(i%127==0) {
            key_t keys[32];struct kv_pair out[32];int statuses[32];
            for(int j=0;j<32;j++)keys[j]=k+(j%3);keys[31]=2001;
            require(!bplus_tree_compressed_get_many_records(t,keys,32,out,statuses),"many status");
            for(int j=0;j<32;j++) {
                int key=keys[j];require(statuses[j]==(key<=1000?BPLUS_RECORD_OK:BPLUS_RECORD_NOT_FOUND),"many miss");
                if(key<=1000) { bytes(key,version[key],p);require(!memcmp(out[j].payload,p,128),"many bytes/order"); }
            }
        }
    }
    require(!bplus_tree_compressed_drain_background(t),"update drain");
    for(int k=1;k<=1000;k++)check_key(t,k,version[k]);
    for(int k=1;k<=1000;k+=2)require(!bplus_tree_compressed_delete(t,k),"delete/rebalance");
    for(int k=1;k<=1000;k++) {
        if(k%2) {struct kv_pair r;require(bplus_tree_compressed_get_record(t,k,&r)==BPLUS_RECORD_NOT_FOUND,"deleted key");}
        else check_key(t,k,version[k]);
    }
    for(int k=1;k<=1000;k+=2){uint8_t p[128];bytes(k,9,p);require(!bplus_tree_compressed_put_with_payload(t,k,p,128,10),"reinsert");}
    require(!bplus_tree_compressed_drain_background(t),"final drain");
    struct bplus_tree_memory_stats m;require(!bplus_tree_compressed_memory_stats(t,&m),"memory walk");
    require(m.live_kv_bytes==1000*sizeof(struct kv_pair),"live byte accounting");
    bplus_tree_compressed_deinit(t);bplus_tree_compressed_release_thread_resources();
    struct bplus_tree_allocation_audit audit;bplus_tree_compressed_allocation_audit(&audit,0);
    require(!audit.enabled || audit.usable==0,"teardown allocation leak");
    printf("PASS L%d %s\n",ZIPCACHE_AGG_LAYOUT,codec);return 0;
}
