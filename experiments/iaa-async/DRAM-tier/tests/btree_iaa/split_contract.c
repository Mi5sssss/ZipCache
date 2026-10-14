#include "compressed_test_utils.h"
#include <time.h>
static void require(int ok,const char *why){if(!ok){fprintf(stderr,"split contract: %s\n",why);abort();}}
static struct bplus_tree_compressed *make(compression_algo_t algo) {
    struct compression_config c=bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);c.algo=algo;
    struct bplus_tree_compressed *t=bplus_tree_compressed_init_with_config(4,64,&c);require(t!=NULL,"init");return t;
}
static void put(struct bplus_tree_compressed *t,int k,int v){uint8_t b[128];memset(b,v,128);
    require(!bplus_tree_compressed_put_with_payload(t,k,b,128,v),"put");}
static void get(struct bplus_tree_compressed *t,int k,int v){struct kv_pair p;uint8_t b[128];memset(b,v,128);
    require(!bplus_tree_compressed_get_record(t,k,&p)&&p.stored_value==v&&!memcmp(b,p.payload,128),"visible full value");}
static void accepted(struct bplus_tree_compressed *t){for(int i=0;i<10000;i++){
    struct bplus_tree_contract_stats s;require(!bplus_tree_compressed_test_contract(t,-1,&s),"gate stats");
    if(s.in_flight)return;struct timespec d={0,1000000};nanosleep(&d,NULL);
}require(0,"worker never reached real codec completion");}
struct deletion{struct bplus_tree_compressed *tree;int started,done,rc;};
static void *erase(void *p){struct deletion *a=p;__atomic_store_n(&a->started,1,__ATOMIC_RELEASE);
    a->rc=bplus_tree_compressed_delete(a->tree,2);__atomic_store_n(&a->done,1,__ATOMIC_RELEASE);return NULL;}
int main(void){
    setenv("BTREE_BG_COMPACTION","1",1);setenv("BTREE_SHARDS","1",1);
    setenv("BTREE_BG_BATCH_SIZE","1",1);setenv("BTREE_BG_QUEUE_CAPACITY","1",1);
    setenv("BTREE_TEST_CONTRACT_CLOSED","1",1);
    for(int codec=0;codec<2;codec++)for(int mode=1;mode<=5;mode++){
        if(mode==2||mode==3)continue;char s[4];snprintf(s,sizeof(s),"%d",mode);setenv("BTREE_TEST_SPLIT_CODEC_MODE",s,1);
        struct bplus_tree_compressed *t=make(codec?COMPRESS_ZLIB_ACCEL:COMPRESS_LZ4);
        for(int k=1;k<=4;k++)put(t,k,k);accepted(t);
        for(int k=1;k<=4;k++)get(t,k,k);
        put(t,1,99);get(t,1,99); /* newer active wins over the completed-but-unpublished result */
        struct deletion a={.tree=t};pthread_t id;
        if(mode==1){require(!pthread_create(&id,NULL,erase,&a),"delete thread");
            while(!__atomic_load_n(&a.started,__ATOMIC_ACQUIRE))sched_yield();
            require(!__atomic_load_n(&a.done,__ATOMIC_ACQUIRE),"delete ignored pending");}
        require(!bplus_tree_compressed_test_contract(t,1,NULL),"release");
        if(mode==1){pthread_join(id,NULL);require(!a.rc,"delete result");
            struct kv_pair p;require(bplus_tree_compressed_get_record(t,2,&p)==BPLUS_RECORD_NOT_FOUND,"resurrected delete");}
        require(bplus_tree_compressed_drain_background(t)==(mode==1?0:-1),"drain failure semantics");
        get(t,1,99);get(t,3,3);get(t,4,4);
        if(mode!=1){get(t,2,2);put(t,5,5);uint8_t b[128]={0};
            require(bplus_tree_compressed_put_with_payload(t,6,b,128,6)==-1,"failed pending discarded");}
        struct bplus_tree_contract_stats stats;bplus_tree_compressed_test_contract(t,-1,&stats);
        require(stats.accepted&&stats.accepted==stats.terminal&&!stats.in_flight,"terminal ownership");
        bplus_tree_compressed_deinit(t);
    }
    setenv("BTREE_TEST_SPLIT_CODEC_MODE","1",1);
    struct bplus_tree_compressed *t=make(COMPRESS_LZ4);
    for(int k=1;k<=4;k++)put(t,k,k);accepted(t);
    bplus_tree_compressed_deinit(t); /* shutdown must release gate, finish, join, then free */
    bplus_tree_compressed_release_thread_resources();
    struct bplus_tree_allocation_audit a;bplus_tree_compressed_allocation_audit(&a,0);
    require(!a.enabled||!a.usable,"leak after in-flight close");return 0;
}
