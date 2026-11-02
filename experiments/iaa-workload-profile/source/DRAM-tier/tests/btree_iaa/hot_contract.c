#include "compressed_test_utils.h"
#include "async_copy_device.h"
#include <sched.h>
#include <pthread.h>
#define need(x) do{if(!(x)){fprintf(stderr,"hot contract line %d: %s\n",__LINE__,#x);abort();}}while(0)
static void bytes(int k,int v,uint8_t p[128]){for(int i=0;i<128;i++)p[i]=(uint8_t)(k+i);memcpy(p,&v,4);}
static int put(struct bplus_tree_compressed *t,int k,int v){uint8_t p[128];bytes(k,v,p);return bplus_tree_compressed_put_with_payload(t,k,p,128,v);}
static void get(struct bplus_tree_compressed *t,int k,int v){uint8_t p[128];struct kv_pair out;bytes(k,v,p);need(!bplus_tree_compressed_get_record(t,k,&out));need(out.stored_value==v&&!memcmp(out.payload,p,128));}
static struct bplus_tree_compressed *make(int codec){struct compression_config c=bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);c.algo=codec;struct bplus_tree_compressed *t=bplus_tree_compressed_init_with_config(4,64,&c);need(t);return t;}
static void accepted(struct bplus_tree_compressed *t){uint64_t end=sd_now()+10000000000ULL;struct sd_stats s;do{sd_stats(t->test_device,&s);need(sd_now()<end);sched_yield();}while(!s.active);}
static void finish(struct bplus_tree_compressed *t,uint64_t target){uint64_t end=sd_now()+10000000000ULL;struct bplus_hot_stats s;do{sd_advance(t->test_device,1000);sched_yield();bplus_tree_hot_stats(t,&s);need(sd_now()<end);}while(s.task_finished<target);}
static void teardown(struct bplus_tree_compressed *t){bplus_tree_compressed_deinit(t);bplus_tree_compressed_release_thread_resources();struct bplus_tree_allocation_audit a;bplus_tree_compressed_allocation_audit(&a,0);need(!a.enabled||!a.usable);}
struct del_arg{struct bplus_tree_compressed *t;int result,done;};
static void *del(void *p){struct del_arg *a=p;a->result=bplus_tree_compressed_delete(a->t,1);__atomic_store_n(&a->done,1,__ATOMIC_RELEASE);return NULL;}
static void transitions(void){
 struct bplus_tree_compressed *t=make(COMPRESS_ASYNC_COPY_TEST);
 for(int k=1;k<=1000;k++)need(!put(t,k,1));need(!bplus_tree_compressed_drain_background(t));
 need(!put(t,1,2));need(!bplus_tree_compressed_drain_background(t));
 need(sd_configure_idle(t->test_device,100,100,1)==SD_OK);
 need(!bplus_tree_hot_request(t,1,1));accepted(t);need(!put(t,1,3));get(t,1,3);finish(t,1);
 need(sd_configure_idle(t->test_device,0,0,0)==SD_OK);need(!bplus_tree_compressed_drain_background(t));
 struct bplus_hot_stats h;bplus_tree_hot_stats(t,&h);need(h.promotions==1&&h.hot_used>0&&h.hot_used<=h.hot_limit);
 size_t logical=0,encoded=0;need(!bplus_tree_compressed_stats(t,&logical,&encoded));need(logical==1000*sizeof(struct kv_pair)&&encoded>0);
 struct sd_stats before,after;sd_stats(t->test_device,&before);get(t,1,3);get(t,2,1);sd_stats(t->test_device,&after);need(before.accepted==after.accepted);
 key_t keys[]={2,1,2,10001};struct kv_pair out[4];int statuses[4];need(!bplus_tree_compressed_get_many_records(t,keys,4,out,statuses));need(!memcmp(out,out+2,sizeof(*out))&&statuses[3]==BPLUS_RECORD_NOT_FOUND);
 need(sd_configure_idle(t->test_device,100,100,1)==SD_OK);need(!bplus_tree_hot_request(t,1,2));accepted(t);need(!put(t,1,4));get(t,1,4);finish(t,2);
 bplus_tree_hot_stats(t,&h);need(h.conflicts==1&&h.demotions==0&&h.hot_used>0);
 need(!bplus_tree_hot_request(t,1,2));accepted(t);finish(t,3);need(sd_configure_idle(t->test_device,0,0,0)==SD_OK);get(t,1,4);
 bplus_tree_hot_stats(t,&h);need(h.demotions==1&&!h.hot_used);
 // DELETE waits for conversion and then uses real structural path.
 need(sd_configure_idle(t->test_device,100,100,1)==SD_OK);need(!bplus_tree_hot_request(t,1,1));accepted(t);
 struct del_arg a={t,-99};pthread_t th;need(!pthread_create(&th,NULL,del,&a));finish(t,4);
 /* DELETE may start more device jobs after conversion completes. Keep
  * advancing the manual clock until the structural operation itself is done. */
 uint64_t end=sd_now()+10000000000ULL;
 while(!__atomic_load_n(&a.done,__ATOMIC_ACQUIRE)){sd_advance(t->test_device,1000);need(sd_now()<end);sched_yield();}
 pthread_join(th,NULL);need(!a.result);need(sd_configure_idle(t->test_device,0,0,0)==SD_OK);
 need(bplus_tree_compressed_get_record(t,1,out)==BPLUS_RECORD_NOT_FOUND);
 for(int k=2;k<=1000;k++)get(t,k,1);
 // Fill a promoted last leaf; overflow must preserve all records via real split.
 need(!bplus_tree_hot_request(t,1000,1));need(!bplus_tree_compressed_drain_background(t));
 for(int k=1001;k<=1300;k++)need(!put(t,k,1));need(!bplus_tree_compressed_drain_background(t));
 for(int k=2;k<=1300;k++)get(t,k,1);
 sd_stats(t->test_device,&after);need(after.accepted==after.terminal&&!after.active&&!after.queued);
 teardown(t);
}
static void failure(int fault){
 struct bplus_tree_compressed *t=make(COMPRESS_ASYNC_COPY_TEST);for(int k=1;k<=12;k++)need(!put(t,k,1));need(!bplus_tree_compressed_drain_background(t));
 need(sd_configure_idle(t->test_device,100,100,1)==SD_OK);sd_fault_next(t->test_device,fault);
 need(!bplus_tree_hot_request(t,1,1));accepted(t);finish(t,1);need(sd_configure_idle(t->test_device,0,0,0)==SD_OK);
 struct bplus_hot_stats h;bplus_tree_hot_stats(t,&h);need(h.errors==1&&!h.hot_used);need(bplus_tree_compressed_drain_background(t)==-1);
 for(int k=1;k<=12;k++)get(t,k,1);teardown(t);
}
static void budgets(void){
 setenv("BTREE_HOT_BYTES","1",1);struct bplus_tree_compressed *t=make(COMPRESS_LZ4);
 for(int k=1;k<=16;k++)need(!put(t,k,1));need(!bplus_tree_compressed_drain_background(t));need(bplus_tree_hot_request(t,1,1)==-1);
 for(int k=1;k<=16;k++)get(t,k,1);teardown(t);unsetenv("BTREE_HOT_BYTES");
 setenv("BTREE_DELTA_BYTES","1",1);t=make(COMPRESS_LZ4);need(put(t,1,1)==-1);need(bplus_tree_hot_request(t,1,1)==-1);struct kv_pair r;need(bplus_tree_compressed_get_record(t,1,&r)==BPLUS_RECORD_NOT_FOUND);teardown(t);unsetenv("BTREE_DELTA_BYTES");
}
static void allocation_failures(void){
 struct bplus_tree_compressed *t=make(COMPRESS_LZ4);for(int k=1;k<=50;k++)need(!put(t,k,1));need(!bplus_tree_compressed_drain_background(t));
 for(int n=0;n<2;n++){bplus_tree_compressed_test_fail_allocation(n);need(bplus_tree_hot_request(t,1,1)==-1);bplus_tree_compressed_test_fail_allocation(-1);for(int k=1;k<=50;k++)get(t,k,1);}
 need(!bplus_tree_hot_request(t,1,1));need(!bplus_tree_compressed_drain_background(t));
 setenv("BTREE_TEST_CROSS_FAIL_STAGE","3",1);need(!bplus_tree_hot_request(t,1,2));need(bplus_tree_compressed_drain_background(t)==-1);unsetenv("BTREE_TEST_CROSS_FAIL_STAGE");
 for(int k=1;k<=50;k++)get(t,k,1);teardown(t);
}
static void automatic_policy(void){
 setenv("BTREE_HOT_MANUAL","0",1);struct bplus_tree_compressed *t=make(COMPRESS_LZ4);
 for(int k=1;k<=1000;k++)need(!put(t,k,1));need(!bplus_tree_compressed_drain_background(t));bplus_tree_hot_pause(t,0);
 struct bplus_hot_stats h;uint64_t end=sd_now()+10000000000ULL;
 do{for(int n=0;n<100;n++)get(t,1,1);bplus_tree_hot_stats(t,&h);sched_yield();need(sd_now()<end);}while(!h.promotions);
 // Real accesses to a different leaf age the old one without a timer oracle.
 do{for(int n=0;n<100;n++)get(t,1000,1);bplus_tree_hot_stats(t,&h);sched_yield();need(sd_now()<end);}while(!h.demotions);
 bplus_tree_hot_pause(t,1);need(!bplus_tree_compressed_drain_background(t));for(int k=1;k<=1000;k++)get(t,k,1);teardown(t);setenv("BTREE_HOT_MANUAL","1",1);
}
static void delete_failure_sweep(void){
 for(int fail=0;fail<36;fail++) {
    struct bplus_tree_compressed *t=make(COMPRESS_LZ4);
    for(int k=1;k<=160;k++)need(!put(t,k,1));need(!bplus_tree_compressed_drain_background(t));
    need(t->tree->level==1);struct bplus_non_leaf *parent=(void*)t->tree->root;int bound=parent->key[0];need(bound>2&&bound<160);
    for(int k=2;k<bound;k++)need(!bplus_tree_compressed_delete(t,k));
    bplus_tree_compressed_test_fail_allocation(fail);int rc=bplus_tree_compressed_delete(t,1);bplus_tree_compressed_test_fail_allocation(-1);
    struct kv_pair r;if(rc) get(t,1,1);else need(bplus_tree_compressed_get_record(t,1,&r)==BPLUS_RECORD_NOT_FOUND);
    for(int k=bound;k<=160;k++)get(t,k,1);teardown(t);
 }
}
struct grow_arg{struct bplus_tree_compressed *t;int done;};
static void *grow(void *p){struct grow_arg *a=p;for(int k=97;k<=400;k++)need(!put(a->t,k,1));__atomic_store_n(&a->done,1,__ATOMIC_RELEASE);return NULL;}
static void overflow_pin_and_close(void){
 struct bplus_tree_compressed *t=make(COMPRESS_ASYNC_COPY_TEST);
 for(int k=1;k<=96;k++)need(!put(t,k,1));need(!bplus_tree_compressed_drain_background(t));
 need(!bplus_tree_hot_request(t,1,1));need(!bplus_tree_compressed_drain_background(t));
 need(sd_configure_idle(t->test_device,100,100,1)==SD_OK);need(!bplus_tree_hot_request(t,1,2));accepted(t);
 uint64_t waits=bplus_tree_compressed_test_wait_event(t,3,0),end=sd_now()+10000000000ULL;
 struct grow_arg a={t,0};pthread_t th;need(!pthread_create(&th,NULL,grow,&a));
 while(bplus_tree_compressed_test_wait_event(t,3,0)==waits){need(sd_now()<end);sched_yield();}
 get(t,1,1);need(!__atomic_load_n(&a.done,__ATOMIC_ACQUIRE));
 while(!__atomic_load_n(&a.done,__ATOMIC_ACQUIRE)){sd_advance(t->test_device,1000);need(sd_now()<end);sched_yield();}
 pthread_join(th,NULL);
 while(sd_configure_idle(t->test_device,0,0,0)!=SD_OK){sd_advance(t->test_device,1000);need(sd_now()<end);sched_yield();}
 need(!bplus_tree_compressed_drain_background(t));for(int k=1;k<=400;k++)get(t,k,1);teardown(t);
 t=make(COMPRESS_ASYNC_COPY_TEST);for(int k=1;k<=32;k++)need(!put(t,k,1));need(!bplus_tree_compressed_drain_background(t));
 need(sd_configure_idle(t->test_device,100,100,1)==SD_OK);need(!bplus_tree_hot_request(t,1,1));accepted(t);teardown(t);
}
static void packed_raw_control(void){
 struct bplus_tree_compressed *t=make(COMPRESS_RAW_PACKED);
 for(int k=1;k<=400;k++)need(!put(t,k,1));need(!bplus_tree_compressed_drain_background(t));
 need(!bplus_tree_hot_request(t,1,1));need(!bplus_tree_compressed_drain_background(t));
 need(!put(t,1,2));get(t,1,2);
 need(!bplus_tree_hot_request(t,1,2));need(!bplus_tree_compressed_drain_background(t));
 for(int k=1;k<=400;k++)get(t,k,k==1?2:1);
 struct bplus_hot_stats h;bplus_tree_hot_stats(t,&h);need(h.promotions==1&&h.demotions==1&&!h.errors);
 struct bplus_cross_stats s;bplus_tree_cross_stats(t,&s,0);need(!s.accepted&&!s.completed&&!s.failed);
 struct bplus_tree_memory_stats m;need(!bplus_tree_compressed_memory_stats(t,&m));
 need(m.base_raw_bytes==m.base_encoded_bytes&&m.raw_blocks==m.base_blocks);
 need(!bplus_tree_compressed_delete(t,1));for(int k=2;k<=400;k++)get(t,k,1);teardown(t);
}
static void work_signal_and_total_guard(void){
 setenv("BTREE_HOT_WORK_SIGNAL","1",1);
 struct bplus_tree_compressed *t=make(COMPRESS_LZ4);
 for(int k=1;k<=40;k++)need(!put(t,k,1));need(!bplus_tree_compressed_drain_background(t));
 need(t->tree->level==0);struct compressed_leaf_ref *r=(void*)t->tree->root;
 struct simple_leaf_node *l=(void*)r->payload;bplus_tree_hot_pause(t,0);
 need(!put(t,1,2));for(int i=0;i<20;i++)get(t,1,2);
 need((uint32_t)__atomic_load_n(&l->hot_access,__ATOMIC_RELAXED)==0);
 get(t,2,1);need((uint32_t)__atomic_load_n(&l->hot_access,__ATOMIC_RELAXED)>0);
 bplus_tree_hot_pause(t,1);need(!bplus_tree_compressed_drain_background(t));
 struct bplus_hot_stats h;bplus_tree_hot_stats(t,&h);
 struct bplus_tree_allocation_audit a;bplus_tree_compressed_allocation_audit(&a,0);
 need(h.owned_census==a.usable);teardown(t);
 setenv("BTREE_HOT_TOTAL_BYTES","1",1);t=make(COMPRESS_LZ4);
 for(int k=1;k<=40;k++)need(!put(t,k,1));need(!bplus_tree_compressed_drain_background(t));
 need(bplus_tree_hot_request(t,1,1)==-1);bplus_tree_hot_stats(t,&h);
 need(h.total_budget_denied==1&&!h.hot_used);
 for(int k=1;k<=40;k++)get(t,k,1);teardown(t);
 setenv("BTREE_HOT_TOTAL_BYTES","1073741824",1);unsetenv("BTREE_HOT_WORK_SIGNAL");
}
int main(void){setenv("BTREE_HOT_TOTAL_BYTES","1073741824",1);setenv("BTREE_BG_COMPACTION","1",1);setenv("BTREE_AGG_BACKPRESSURE","1",1);setenv("BTREE_CROSS_LEAF","1",1);setenv("BTREE_CROSS_CPU_TEST","1",1);setenv("BTREE_SHARDS","1",1);setenv("BTREE_HOT_ENABLE","1",1);setenv("BTREE_HOT_MANUAL","1",1);setenv("SD_ENCODE_US","0",1);setenv("SD_DECODE_US","0",1);transitions();failure(1);failure(2);failure(3);budgets();allocation_failures();automatic_policy();delete_failure_sweep();overflow_pin_and_close();packed_raw_control();work_signal_and_total_guard();puts("PASS hot promotion/demotion, late update, DELETE, split, failure, budgets, ownership and packed RAW without codec jobs");}
