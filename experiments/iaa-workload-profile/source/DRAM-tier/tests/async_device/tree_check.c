#include "compressed_test_utils.h"
#include "async_copy_device.h"
#include <sched.h>
static void need(int x){if(!x)abort();}
static void bytes(int k,int v,uint8_t p[128]){for(int i=0;i<128;i++)p[i]=(uint8_t)(k*31+v*7+i);memcpy(p,&v,sizeof(v));}
static int put(struct bplus_tree_compressed *t,int k,int v){uint8_t p[128];bytes(k,v,p);return bplus_tree_compressed_put_with_payload(t,k,p,128,v);}
static void get(struct bplus_tree_compressed *t,int k,int v){struct kv_pair p;uint8_t b[128];bytes(k,v,b);need(!bplus_tree_compressed_get_record(t,k,&p)&&p.stored_value==v&&!memcmp(b,p.payload,128));}
static struct bplus_tree_compressed *make(void){struct compression_config c=bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);c.algo=COMPRESS_ASYNC_COPY_TEST;struct bplus_tree_compressed *t=bplus_tree_compressed_init_with_config(4,64,&c);need(t!=NULL);return t;}
static void accepted(struct sd_device *d){uint64_t end=sd_now()+10000000000ULL;struct sd_stats s;do{sd_stats(d,&s);need(sd_now()<end);sched_yield();}while(!s.active);}
struct erase_arg{struct bplus_tree_compressed *t;int done,rc;};
static void *erase(void *p){struct erase_arg *a=p;a->rc=bplus_tree_compressed_delete(a->t,2);__atomic_store_n(&a->done,1,__ATOMIC_RELEASE);return NULL;}
int main(void){
    setenv("BTREE_BG_COMPACTION","1",1);setenv("SD_ENCODE_US","0",1);setenv("SD_DECODE_US","0",1);
    for(int trigger=0;trigger<2;trigger++){
        setenv("BTREE_SHARDS","8",1);setenv("BTREE_BG_INCLUDE_TRIGGER",trigger?"1":"0",1);
        struct bplus_tree_compressed *t=make();for(int i=0;i<8;i++)need(t->shards[i]->test_device==t->test_device);
        /* Exceed one 16 KiB leaf per shard, including under sanitizers. */
        for(int k=1;k<=2048;k++)need(!put(t,k,1));need(!bplus_tree_compressed_drain_background(t));
        for(int k=1;k<=2048;k++)get(t,k,1);
        for(int k=1;k<=2048;k++)need(!put(t,k,2));need(!bplus_tree_compressed_drain_background(t));
        for(int k=1;k<=2048;k++){get(t,k,2);if(k%3==0)need(!bplus_tree_compressed_delete(t,k));}
        for(int k=1;k<=2048;k++)if(k%3==0){struct kv_pair p;need(bplus_tree_compressed_get_record(t,k,&p)==BPLUS_RECORD_NOT_FOUND);need(!put(t,k,3));get(t,k,3);}
        need(!bplus_tree_compressed_drain_background(t));struct sd_stats s;sd_stats(t->test_device,&s);need(s.accepted==s.terminal&&!s.failed&&s.role_accepted[0]);
        bplus_tree_compressed_deinit(t);
    }
    setenv("BTREE_SHARDS","1",1);setenv("BTREE_BG_INCLUDE_TRIGGER","0",1);
    for(int fault=0;fault<=2;fault++){
        struct bplus_tree_compressed *t=make();need(sd_configure_idle(t->test_device,100,100,1)==SD_OK);sd_fault_next(t->test_device,-fault);
        for(int k=1;k<=4;k++)need(!put(t,k,k));accepted(t->test_device);
        for(int k=1;k<=4;k++)get(t,k,k);need(!put(t,1,99));get(t,1,99);
        struct erase_arg a={.t=t};pthread_t thread;
        if(!fault)need(!pthread_create(&thread,NULL,erase,&a));
        uint64_t end=sd_now()+10000000000ULL;
        for(;;){sd_advance(t->test_device,100);struct sd_stats s;sd_stats(t->test_device,&s);sched_yield();need(sd_now()<end);
            if(!fault && __atomic_load_n(&a.done,__ATOMIC_ACQUIRE))break;
            if(fault && s.failed>=2)break;
        }
        if(!fault){pthread_join(thread,NULL);need(!a.rc);}else {need(bplus_tree_compressed_drain_background(t)==-1);get(t,2,2);}
        get(t,1,99);get(t,3,3);sd_fault_next(t->test_device,0);
        need(sd_configure_idle(t->test_device,0,0,0)==SD_OK);bplus_tree_compressed_deinit(t);
    }
    /* Destroy with a real background task in flight, without a gated clock. */
    setenv("SD_ENCODE_US","1000",1);struct bplus_tree_compressed *t=make();for(int k=1;k<=4;k++)need(!put(t,k,k));bplus_tree_compressed_deinit(t);
    t=make();need(sd_configure_idle(t->test_device,100,100,1)==SD_OK);for(int k=1;k<=4;k++)need(!put(t,k,k));accepted(t->test_device);bplus_tree_compressed_deinit(t);
    setenv("SD_BYTES","1",1);t=make();for(int k=1;k<=4;k++)need(!put(t,k,k));need(bplus_tree_compressed_drain_background(t)==-1);for(int k=1;k<=4;k++)get(t,k,k);bplus_tree_compressed_deinit(t);unsetenv("SD_BYTES");
    bplus_tree_compressed_release_thread_resources();struct bplus_tree_allocation_audit a;bplus_tree_compressed_allocation_audit(&a,0);need(!a.enabled||!a.usable);
    puts("real-tree surrogate contracts passed");return 0;
}
