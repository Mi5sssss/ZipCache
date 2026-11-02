#include "compressed_test_utils.h"
#include <time.h>
static void need(int ok){if(!ok)abort();}
struct args {struct bplus_tree_compressed *t;uint64_t sequence;int done;};
static void *waiter(void *ptr){struct args *a=ptr;
    bplus_tree_compressed_test_wait_event(a->t,5,a->sequence);
    __atomic_store_n(&a->done,1,__ATOMIC_RELEASE);return NULL;}
static void until(struct bplus_tree_compressed *t,int counter,uint64_t minimum){
    struct timespec start,now;clock_gettime(CLOCK_MONOTONIC,&start);
    while(bplus_tree_compressed_test_wait_event(t,counter,0)<minimum){
        sched_yield();clock_gettime(CLOCK_MONOTONIC,&now);need(now.tv_sec-start.tv_sec<10);
    }
}
int main(void){
    setenv("BTREE_BG_COMPACTION","1",1);setenv("BTREE_SHARDS","1",1);
    struct compression_config c=bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    struct bplus_tree_compressed *t=bplus_tree_compressed_init_with_config(4,64,&c);need(t!=NULL);
    uint64_t sequence=bplus_tree_compressed_test_wait_event(t,0,0);
    bplus_tree_compressed_test_wait_event(t,1,0); /* completion before waiting */
    bplus_tree_compressed_test_wait_event(t,5,sequence);
    need(bplus_tree_compressed_test_wait_event(t,3,0)==0);
    struct args a={.t=t,.sequence=bplus_tree_compressed_test_wait_event(t,0,0)};
    pthread_t thread;need(!pthread_create(&thread,NULL,waiter,&a));until(t,3,1);
    bplus_tree_compressed_test_wait_event(t,2,0); /* spurious wake cannot complete wait */
    until(t,4,1);until(t,3,2);need(!__atomic_load_n(&a.done,__ATOMIC_ACQUIRE));
    bplus_tree_compressed_test_wait_event(t,1,0);need(!pthread_join(thread,NULL));need(a.done);
    bplus_tree_compressed_deinit(t);
    size_t requested[2];
    for(int i=0;i<2;i++){
        setenv("BTREE_BG_BATCH_SIZE",i?"8":"1",1);setenv("BTREE_BG_QUEUE_CAPACITY","32",1);
        t=bplus_tree_compressed_init_with_config(4,64,&c);need(t!=NULL);
        struct bplus_tree_memory_stats m;need(!bplus_tree_compressed_memory_stats(t,&m));
        requested[i]=m.scheduler_and_qpl_bytes;bplus_tree_compressed_deinit(t);
    }
    need(requested[1]-requested[0]==(ZIPCACHE_AGG_LAYOUT?0:7*(COMPRESSED_LEAF_SIZE+MAX_COMPRESSED_SIZE)));
    bplus_tree_compressed_release_thread_resources();return 0;
}
