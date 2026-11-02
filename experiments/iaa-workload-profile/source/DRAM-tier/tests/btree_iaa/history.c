/* Exhaustive linearizability check of bounded, overlapping single-key histories.
 * Rejects a merely plausible version if no legal real-time ordering exists. */
#include "compressed_test_utils.h"
#include <sched.h>
struct event { int kind,value,rc,observed; unsigned begin,end; };
struct test { struct bplus_tree_compressed *tree; struct event events[8]; unsigned clock; int gate; };
struct worker { struct test *test; int id; };
static void require(int x,const char *s){if(!x){fprintf(stderr,"history: %s\n",s);abort();}}
static void value(int n,uint8_t out[128]){memset(out,n,128);}
static void *run(void *arg){
    struct worker *w=arg;struct test *t=w->test;
    while(!__atomic_load_n(&t->gate,__ATOMIC_ACQUIRE))sched_yield();
    for(int i=0;i<4;i++){
        struct event *e=t->events+w->id*4+i;uint8_t p[128];struct kv_pair record;
        value(e->value,p);e->begin=__atomic_add_fetch(&t->clock,1,__ATOMIC_SEQ_CST);
        if(e->kind==0)e->rc=bplus_tree_compressed_put_with_payload(t->tree,7,p,128,e->value);
        else if(e->kind==1){
            e->rc=bplus_tree_compressed_get_record(t->tree,7,&record);
            if(e->rc==BPLUS_RECORD_OK){
                e->observed=record.stored_value;value(e->observed,p);
                require(record.key==7&&!memcmp(p,record.payload,128),"torn payload");
            }
        }else e->rc=bplus_tree_compressed_delete(t->tree,7);
        e->end=__atomic_add_fetch(&t->clock,1,__ATOMIC_SEQ_CST);sched_yield();
    }return NULL;
}
static int linearize(struct test *t,unsigned mask,int state){
    if(mask==255)return 1;
    for(int i=0;i<8;i++)if(!(mask&(1u<<i))){
        struct event *e=t->events+i;int ready=1;
        for(int j=0;j<8;j++)if(!(mask&(1u<<j))&&t->events[j].end<e->begin)ready=0;
        if(!ready)continue;int next=state;
        if(e->kind==0){if(e->rc)continue;next=e->value;}
        else if(e->kind==1){
            if(state){if(e->rc!=BPLUS_RECORD_OK||e->observed!=state)continue;}
            else if(e->rc!=BPLUS_RECORD_NOT_FOUND)continue;
        }else{if(e->rc && state)continue;next=0;}
        if(linearize(t,mask|(1u<<i),next))return 1;
    }return 0;
}
int main(void){
    for(int round=0;round<64;round++){
        struct test t={0};struct compression_config c=bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
        t.tree=bplus_tree_compressed_init_with_config(4,64,&c);require(t.tree!=NULL,"init");
        for(int k=1;k<=256;k++){uint8_t p[128];value(1,p);require(!bplus_tree_compressed_put_with_payload(t.tree,k,p,128,1),"preload");}
        int kinds[8]={0,1,0,1,1,2,0,1};
        for(int i=0;i<8;i++){t.events[i].kind=kinds[(i+round)%8];t.events[i].value=i+2;}
        pthread_t ids[2];struct worker workers[2]={{&t,0},{&t,1}};
        for(int i=0;i<2;i++)require(!pthread_create(ids+i,NULL,run,workers+i),"thread");
        __atomic_store_n(&t.gate,1,__ATOMIC_RELEASE);
        for(int i=0;i<2;i++)pthread_join(ids[i],NULL);
        require(linearize(&t,0,1),"no legal linearization");
        require(!bplus_tree_compressed_drain_background(t.tree),"drain");bplus_tree_compressed_deinit(t.tree);
    }
    bplus_tree_compressed_release_thread_resources();puts("controlled histories passed");return 0;
}
