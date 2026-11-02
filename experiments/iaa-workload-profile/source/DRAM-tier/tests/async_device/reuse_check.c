#include "compressed_test_utils.h"
#include "async_copy_device.h"
#include <zlib.h>
#include <sched.h>
#define need(x) do { if(!(x)){fprintf(stderr,"reuse check line %d: %s\n",__LINE__,#x);abort();} } while(0)
/* Inspect the existing version-1 frame, including exact encoded bytes. */
struct header { uint32_t magic,version,blocks,records; };
struct block { int32_t upper;uint32_t offset,length,raw_length,codec,crc; };
struct frame { uint8_t *bytes;size_t length;struct header h;struct block b[4]; };
static int key_offset;
static void payload(int k,int v,uint8_t p[128]){memset(p,(uint8_t)k,128);memcpy(p,&v,4);}
static void put(struct bplus_tree_compressed *t,int ordinal,int v){uint8_t p[128];int k=ordinal+key_offset;payload(k,v,p);need(!bplus_tree_compressed_put_with_payload(t,k,p,128,v));}
static void get(struct bplus_tree_compressed *t,int ordinal,int v){uint8_t p[128];struct kv_pair out;int k=ordinal+key_offset;payload(k,v,p);need(!bplus_tree_compressed_get_record(t,k,&out)&&out.key==k&&out.stored_value==v&&!memcmp(p,out.payload,128));}
static struct simple_leaf_node *leaf(struct bplus_tree_compressed *t){need(t->tree->root&&t->tree->level==0);return (struct simple_leaf_node *)((struct compressed_leaf_ref *)t->tree->root)->payload;}
static struct frame snapshot(struct bplus_tree_compressed *t){
    struct frame f={0};pthread_rwlock_rdlock(&t->rwlock);struct simple_leaf_node *l=leaf(t);pthread_rwlock_rdlock(&l->rwlock);
    need(l->num_subpages==1&&l->is_compressed&&l->subpage_index[0].offset==0);
    f.length=l->subpage_index[0].length;f.bytes=malloc(f.length);need(f.bytes);memcpy(f.bytes,l->compressed_data,f.length);
    pthread_rwlock_unlock(&l->rwlock);pthread_rwlock_unlock(&t->rwlock);
    memcpy(&f.h,f.bytes,sizeof(f.h));need(f.h.version==1&&f.h.blocks<=4);memcpy(f.b,f.bytes+sizeof(f.h),f.h.blocks*sizeof(*f.b));return f;
}
static struct bplus_tree_compressed *make(compression_algo_t codec,int reuse,int trigger){
    setenv("BTREE_REUSE_UNCHANGED_BLOCKS",reuse?"1":"0",1);setenv("BTREE_BG_INCLUDE_TRIGGER",trigger?"1":"0",1);
    struct compression_config c=bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);c.algo=codec;
    struct bplus_tree_compressed *t=bplus_tree_compressed_init_with_config(4,64,&c);need(t);
    for(int i=1;i<=96;i++)put(t,i,1);
    put(t,1,1);need(!bplus_tree_compressed_drain_background(t));
    struct frame f=snapshot(t);need(f.h.records==96&&f.h.blocks==(ZIPCACHE_AGG_LAYOUT==3?4:1));free(f.bytes);
    return t;
}
/* A valid noncanonical partition must not be mistaken for the encoder's
 * balanced partition. COPY bytes remain in key order; adjust descriptors/CRC. */
static void repartition(struct bplus_tree_compressed *t){
    struct simple_leaf_node *l=leaf(t);pthread_rwlock_wrlock(&l->rwlock);
    struct header h;memcpy(&h,l->compressed_data,sizeof(h));need(h.blocks==4);
    struct block b[4];memcpy(b,l->compressed_data+sizeof(h),sizeof(b));
    unsigned counts[4]={23,25,24,24};uint32_t offset=sizeof(h)+sizeof(b);
    for(int i=0;i<4;i++){b[i].offset=offset;b[i].length=b[i].raw_length=counts[i]*sizeof(struct kv_pair);
        struct kv_pair last;memcpy(&last,l->compressed_data+offset+b[i].raw_length-sizeof(last),sizeof(last));b[i].upper=last.key;
        b[i].crc=(uint32_t)crc32(0,(uint8_t *)l->compressed_data+offset,b[i].raw_length);offset+=b[i].length;}
    memcpy(l->compressed_data+sizeof(h),b,sizeof(b));pthread_rwlock_unlock(&l->rwlock);
}
struct scenario {const char *name;int keys[4],n,dirty,trigger,offset,noncanonical;};
static struct frame run(compression_algo_t codec,const struct scenario *s,int reuse){
    key_offset=s->offset;struct bplus_tree_compressed *t=make(codec,reuse,s->trigger);
    if(s->noncanonical)repartition(t);
    struct frame before=snapshot(t);int versions[100];for(int i=0;i<100;i++)versions[i]=1;
    bplus_tree_compressed_aggregation_stats(NULL,1);
    for(int i=0;i<s->n;i++){put(t,s->keys[i],2);versions[s->keys[i]]=2;}
    if(!s->trigger){put(t,4,2);versions[4]=2;}
    need(!bplus_tree_compressed_drain_background(t));struct bplus_aggregation_stats a;bplus_tree_compressed_aggregation_stats(&a,0);
    unsigned blocks=before.h.blocks,kept=reuse&&ZIPCACHE_AGG_LAYOUT==3&&!s->noncanonical?blocks-s->dirty:0;
    need(a.decompress[BPLUS_ORIGIN_COMPACTION]==blocks);
    need(a.compress[BPLUS_ORIGIN_COMPACTION]==blocks-kept);
    need(a.reused_encode_blocks==kept);
    struct frame after=snapshot(t);
    unsigned matches=0;
    if(kept)for(unsigned i=0;i<blocks;i++)if(before.b[i].length==after.b[i].length &&
        !memcmp(before.bytes+before.b[i].offset,after.bytes+after.b[i].offset,before.b[i].length))matches++;
    need(!kept||matches>=kept);
    for(int i=1;i<=96;i++)get(t,i,versions[i]);if(after.h.records==97)get(t,97,2);
    printf("%s reuse=%d encode=%llu decode=%llu reused=%llu frame=%zu\n",s->name,reuse,
        (unsigned long long)a.compress[1],(unsigned long long)a.decompress[1],(unsigned long long)a.reused_encode_blocks,after.length);
    free(before.bytes);bplus_tree_compressed_deinit(t);return after;
}
static void failed_publish(int corrupt){
    key_offset=0;struct bplus_tree_compressed *t=make(COMPRESS_ASYNC_COPY_TEST,1,0);
    if(corrupt){struct simple_leaf_node *l=leaf(t);pthread_rwlock_wrlock(&l->rwlock);
        struct block b;size_t p=sizeof(struct header)+3*sizeof(b);memcpy(&b,l->compressed_data+p,sizeof(b));b.crc^=1;memcpy(l->compressed_data+p,&b,sizeof(b));pthread_rwlock_unlock(&l->rwlock);
    }else setenv("BTREE_TEST_CROSS_FAIL_STAGE","3",1);
    struct frame before=snapshot(t);bplus_tree_compressed_aggregation_stats(NULL,1);
    for(int i=1;i<=4;i++)put(t,i,2);need(bplus_tree_compressed_drain_background(t)==-1);
    struct frame after=snapshot(t);need(before.length==after.length&&!memcmp(before.bytes,after.bytes,before.length));
    for(int i=1;i<=4;i++)get(t,i,2); /* Successful writes remain in pending/active. */
    if(corrupt){struct kv_pair p;need(bplus_tree_compressed_get_record(t,96,&p)==BPLUS_RECORD_CODEC_ERROR);
        struct bplus_aggregation_stats a;bplus_tree_compressed_aggregation_stats(&a,0);need(!a.reused_encode_blocks&&!a.compress[1]);}
    free(before.bytes);free(after.bytes);bplus_tree_compressed_deinit(t);unsetenv("BTREE_TEST_CROSS_FAIL_STAGE");
}
static void gated_reuse(int close_inflight){
    key_offset=0;struct bplus_tree_compressed *t=make(COMPRESS_ASYNC_COPY_TEST,1,0);
    struct bplus_tree_scheduler_stats initial,q;bplus_tree_compressed_scheduler_stats(t,&initial);
    need(sd_configure_idle(t->test_device,100,100,1)==SD_OK);
    for(int i=1;i<=4;i++)put(t,i,2);
    uint64_t end=sd_now()+10000000000ULL;struct sd_stats d;
    do {sd_stats(t->test_device,&d);need(sd_now()<end);sched_yield();}while(!d.active);
    if(close_inflight){bplus_tree_compressed_deinit(t);return;}
    /* The old-base decode is gated. A newer active value must survive the
     * later commit that reuses three untouched encoded blocks. */
    put(t,1,3);get(t,1,3);get(t,2,2);
    do {sd_advance(t->test_device,100);sched_yield();need(sd_now()<end);
        bplus_tree_compressed_scheduler_stats(t,&q);
    }while(q.completed_tasks==initial.completed_tasks);
    need(!bplus_tree_compressed_drain_background(t));need(sd_configure_idle(t->test_device,0,0,0)==SD_OK);
    for(int i=1;i<=96;i++)get(t,i,i==1?3:i<=4?2:1);
    bplus_tree_compressed_deinit(t);
}
struct gated_reader {struct bplus_tree_compressed *tree;};
static void *read_untouched(void *arg){
    get(((struct gated_reader *)arg)->tree,96,1);
    bplus_tree_compressed_release_thread_resources();return NULL;
}
static struct sd_stats wait_active(struct sd_device *d,uint64_t accepted,uint64_t deadline){
    struct sd_stats s;
    do{sd_stats(d,&s);need(sd_now()<deadline);sched_yield();}while(s.accepted<accepted||!s.active);
    return s;
}
/* Hold the replacement's encode after all four old blocks were validated.
 * A GET of an untouched block completes out of order while the new base is
 * incomplete. No wall-clock latency threshold determines correctness. */
static void gated_encode(int fault){
    key_offset=0;struct bplus_tree_compressed *t=make(COMPRESS_ASYNC_COPY_TEST,1,0);
    struct frame before=snapshot(t);struct sd_stats s;sd_stats(t->test_device,&s);
    uint64_t first=s.accepted,deadline=sd_now()+10000000000ULL;
    struct bplus_tree_scheduler_stats initial;bplus_tree_compressed_scheduler_stats(t,&initial);
    need(sd_reset_stats(t->test_device)==SD_OK);
    bplus_tree_compressed_aggregation_stats(NULL,1);
    need(sd_configure_idle(t->test_device,100,100,1)==SD_OK);
    for(int i=1;i<=4;i++)put(t,i,2);
    for(unsigned i=1;i<=4;i++){
        wait_active(t->test_device,i,deadline);
        /* The next accepted job, after the final decode, is the dirty encode.
         * A negative fault also rejects retries, exposing permanent failure. */
        if(i==4&&fault)sd_fault_next(t->test_device,-fault);
        need(sd_release_id(t->test_device,first+i)==SD_OK);
    }
    s=wait_active(t->test_device,5,deadline);need(s.accepted==5&&s.terminal==4);
    struct bplus_aggregation_stats a;
    do{bplus_tree_compressed_aggregation_stats(&a,0);need(sd_now()<deadline);sched_yield();}while(a.compress[1]!=1);
    need(a.decompress[1]==4&&a.compress[1]==1);
    struct frame during=snapshot(t);need(before.length==during.length&&!memcmp(before.bytes,during.bytes,before.length));free(during.bytes);
    put(t,1,3);get(t,1,3);get(t,2,2);get(t,4,2);
    if(!fault){
        struct gated_reader arg={t};pthread_t reader;need(!pthread_create(&reader,NULL,read_untouched,&arg));
        s=wait_active(t->test_device,6,deadline);need(s.accepted==6&&s.active==2);
        need(sd_release_id(t->test_device,first+6)==SD_OK);pthread_join(reader,NULL);
        sd_stats(t->test_device,&s);need(s.terminal==5&&s.active==1);
        during=snapshot(t);need(before.length==during.length&&!memcmp(before.bytes,during.bytes,before.length));free(during.bytes);
    }
    need(sd_release_id(t->test_device,first+5)==SD_OK);
    struct bplus_tree_scheduler_stats q;
    for(;;){
        bplus_tree_compressed_scheduler_stats(t,&q);
        if(q.completed_tasks>initial.completed_tasks||q.failed_tasks>initial.failed_tasks)break;
        need(sd_now()<deadline);sd_advance(t->test_device,100);sched_yield();
    }
    need(bplus_tree_compressed_drain_background(t)==(fault?-1:0));
    sd_stats(t->test_device,&s);need(s.accepted==s.terminal&&s.terminal==s.observed&&!s.active&&!s.queued);
    sd_fault_next(t->test_device,0);need(sd_configure_idle(t->test_device,0,0,0)==SD_OK);
    struct frame after=snapshot(t);bplus_tree_compressed_aggregation_stats(&a,0);
    if(fault)need(before.length==after.length&&!memcmp(before.bytes,after.bytes,before.length));
    else{
        need(a.decompress[1]==4&&a.compress[1]==1&&a.reused_encode_blocks==3);
        for(int i=1;i<4;i++)need(before.b[i].length==after.b[i].length&&
            !memcmp(before.bytes+before.b[i].offset,after.bytes+after.b[i].offset,before.b[i].length));
    }
    for(int i=1;i<=96;i++)get(t,i,i==1?3:i<=4?2:1);
    printf("encode-gate fault=%d accepted=%llu terminal=%llu: old frame, newer active and untouched reads preserved\n",
        fault,(unsigned long long)s.accepted,(unsigned long long)s.terminal);
    free(before.bytes);free(after.bytes);bplus_tree_compressed_deinit(t);
}
int main(int argc,char **argv){
    need(argc==2);compression_algo_t codec=!strcmp(argv[1],"lz4")?COMPRESS_LZ4:!strcmp(argv[1],"zlib")?COMPRESS_ZLIB_ACCEL:COMPRESS_ASYNC_COPY_TEST;
    setenv("BTREE_BG_COMPACTION","1",1);setenv("BTREE_SHARDS","1",1);setenv("BTREE_AGG_BACKPRESSURE","1",1);
    setenv("BTREE_CROSS_LEAF",codec==COMPRESS_ASYNC_COPY_TEST?"1":"0",1);setenv("SD_ENCODE_US","0",1);setenv("SD_DECODE_US","0",1);
    const struct scenario cases[]={
        {"one-block",{1,2,3},3,1,0,0,0},
        {"boundary",{1,24,25},3,2,0,0,0},
        {"three-blocks",{1,25,49},3,3,0,0,0},
        {"include-trigger",{1,25,49,73},4,4,1,0,0},
        {"insertion",{1,2,97},3,4,0,0,0},
        {"negative-keys",{1,24,25},3,2,0,-97,0},
        {"noncanonical",{1,2,3},3,4,0,0,1}};
    for(unsigned i=0;i<sizeof(cases)/sizeof(*cases);i++){
        if(cases[i].noncanonical&&(codec!=COMPRESS_ASYNC_COPY_TEST||ZIPCACHE_AGG_LAYOUT!=3))continue;
        struct frame a=run(codec,cases+i,0),b=run(codec,cases+i,1);
        need(a.length==b.length&&!memcmp(a.bytes,b.bytes,a.length));free(a.bytes);free(b.bytes);
    }
    if(codec==COMPRESS_ASYNC_COPY_TEST&&ZIPCACHE_AGG_LAYOUT==3){failed_publish(0);failed_publish(1);gated_reuse(0);gated_reuse(1);for(int fault=0;fault<=2;fault++)gated_encode(fault);}
    bplus_tree_compressed_release_thread_resources();struct bplus_tree_allocation_audit a;bplus_tree_compressed_allocation_audit(&a,0);need(!a.enabled||!a.usable);
    puts("reuse contracts passed");return 0;
}
