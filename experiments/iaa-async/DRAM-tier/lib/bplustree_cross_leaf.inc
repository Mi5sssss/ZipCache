/* Experimental single-coordinator, cross-leaf pipeline. A slot owns one
 * pending task and at most one accepted device job. No coroutine or new thread.
 * Pending pins the leaf until commit/failure; structural operations drain it.
 * Backend completion never acquires tree/leaf locks. */
enum cross_stage { CROSS_DECODE, CROSS_MERGE, CROSS_ENCODE, CROSS_COMMIT, CROSS_SPLIT };
struct cross_slot {
    struct compressed_pending_task *task;
    enum cross_stage stage;
    struct sd_job job;
    int inflight, profiled;
    uint32_t block, offset;
    size_t count, position, take;
    struct kv_pair *pairs;
    struct agg_header header;
    struct agg_block blocks[4];
    struct agg_reuse_view reuse;
    uint64_t ready_ns, split_intent_ns;
    uint8_t *snapshot, *frame, *temporary;
    size_t frame_capacity, temporary_capacity;
    qpl_job *qpl;
    int qpl_initialized;
    uint64_t accepted_ns;
};

static size_t cross_frame_capacity(void)
{
    if(!env_bool_enabled("BTREE_CROSS_COMPACT_WORKSPACE",1))return MAX_COMPRESSED_SIZE;
    return COMPRESSED_LEAF_SIZE+sizeof(struct agg_header)+4*sizeof(struct agg_block);
}
static size_t cross_temporary_capacity(void)
{
    if(!env_bool_enabled("BTREE_CROSS_COMPACT_WORKSPACE",1))return MAX_COMPRESSED_SIZE;
    return ZIPCACHE_AGG_LAYOUT==3?(4096/sizeof(struct kv_pair))*sizeof(struct kv_pair):COMPRESSED_LEAF_SIZE;
}
static size_t cross_workspace_per_slot(void)
{
    return sizeof(struct cross_slot)+2*cross_frame_capacity()+cross_temporary_capacity();
}
static void cross_destroy(struct compaction_scheduler *s)
{
    for(int i=0;i<s->cross_window;i++) {
#ifdef HAVE_QPL
        if(s->cross_slots[i].qpl_initialized)qpl_fini_job(s->cross_slots[i].qpl);
#endif
        free(s->cross_slots[i].qpl);
        free(s->cross_slots[i].snapshot);free(s->cross_slots[i].frame);free(s->cross_slots[i].temporary);
    }
    free(s->cross_slots);s->cross_slots=NULL;s->cross_window=0;
}
/* Conservatively reserves all overlapping buffers, merge allocations and final
 * image. This is requested-byte credit, not allocator-usable or RSS accounting. */
static size_t cross_charge(void)
{
    return cross_workspace_per_slot()+2*(COMPRESSED_LEAF_SIZE+4*sizeof(struct kv_pair))+
        cross_frame_capacity()+sizeof(struct subpage_index_entry);
}

static int cross_init(struct compaction_scheduler *s)
{
    int qpl=s->root->config.algo==COMPRESS_QPL;
    if(s->root->config.algo!=COMPRESS_ASYNC_COPY_TEST && !qpl &&
       !(env_bool_enabled("BTREE_CROSS_CPU_TEST",0) &&
         (s->root->config.algo==COMPRESS_LZ4 || s->root->config.algo==COMPRESS_ZLIB_ACCEL)))return -1;
    uint32_t job_size=0;
    size_t temp=cross_temporary_capacity(),frame=cross_frame_capacity();
    if(qpl) {
#ifdef HAVE_QPL
        if(s->root->split_routes[0]!=qpl_path_hardware ||
           s->root->split_routes[2]!=qpl_path_hardware ||
           qpl_get_job_size(qpl_path_hardware,&job_size)!=QPL_STS_OK)return -1;
        temp=qpl_get_safe_deflate_compression_buffer_size((uint32_t)temp);
        if(!temp)return -1;
#else
        return -1;
#endif
    } else if(s->root->config.algo!=COMPRESS_ASYNC_COPY_TEST)temp=MAX_COMPRESSED_SIZE;
    int n=env_int_clamped("BTREE_CROSS_OUTSTANDING",4,1,8);
    size_t bytes=(size_t)env_int_clamped("BTREE_CROSS_BYTES",2*1024*1024,0,INT_MAX);
    size_t extra=temp-cross_temporary_capacity()+job_size;
    if(bytes/(cross_charge()+extra)<(size_t)n)return -1;
    if(env_bool_enabled("BTREE_TEST_FAIL_CROSS_ALLOC",0)) return -1;
    s->cross_slots=calloc((size_t)n,sizeof(*s->cross_slots));
    if(!s->cross_slots)return -1;
    s->cross_window=n;
    size_t usable=allocation_usable_size(s->cross_slots,n*sizeof(*s->cross_slots));
    for(int i=0;i<n;i++) {
        struct cross_slot *c=s->cross_slots+i;
        c->frame_capacity=frame;c->temporary_capacity=temp;
        c->snapshot=malloc(frame);c->frame=malloc(frame);c->temporary=malloc(temp);
        if(!c->snapshot||!c->frame||!c->temporary){cross_destroy(s);return -1;}
        usable+=allocation_usable_size(c->snapshot,frame)+allocation_usable_size(c->frame,frame)+allocation_usable_size(c->temporary,temp);
#ifdef HAVE_QPL
        if(qpl) {
            c->qpl=malloc(job_size);
            if(!c->qpl || qpl_init_job(qpl_path_hardware,c->qpl)!=QPL_STS_OK){cross_destroy(s);return -1;}
            c->qpl_initialized=1;
            size_t u=allocation_usable_size(c->qpl,job_size);
            usable+=u;s->cross_stats.qpl_job_requested+=job_size;s->cross_stats.qpl_job_usable+=u;
        }
#endif
    }
    s->cross_stats.window=n;
    s->cross_stats.workspace_requested=n*(cross_workspace_per_slot()+extra);
    s->cross_stats.workspace_usable=usable;
    s->cross_stats.compact_workspace=env_bool_enabled("BTREE_CROSS_COMPACT_WORKSPACE",1);
    s->cross_stats.reservation_limit=bytes;
    s->cross_stats.reservation_peak=s->cross_stats.workspace_requested;
    s->cross_stats.byte_limit=(size_t)env_int_clamped("BTREE_CROSS_JOB_BYTES",1048576,0,INT_MAX);
    if(s->cross_stats.byte_limit<COMPRESSED_LEAF_SIZE+temp){cross_destroy(s);return -1;}
    return 0;
}

int bplus_tree_cross_stats(struct bplus_tree_compressed *t,struct bplus_cross_stats *out,int reset)
{
    struct compaction_scheduler *s=t?t->scheduler:NULL;
    if(!s)return -1;
    pthread_mutex_lock(&s->lock);
    if(out)*out=s->cross_stats;
    if(reset) {
        if(s->running_count||s->queue_count){pthread_mutex_unlock(&s->lock);return -1;}
        uint64_t n=s->cross_stats.window,w=s->cross_stats.workspace_requested,b=s->cross_stats.reservation_limit;
        uint64_t u=s->cross_stats.workspace_usable,compact=s->cross_stats.compact_workspace;
        uint64_t jr=s->cross_stats.qpl_job_requested,ju=s->cross_stats.qpl_job_usable,bl=s->cross_stats.byte_limit;
        memset(&s->cross_stats,0,sizeof(s->cross_stats));
        s->cross_stats.window=n;s->cross_stats.workspace_requested=w;s->cross_stats.reservation_limit=b;
        s->cross_stats.workspace_usable=u;s->cross_stats.compact_workspace=compact;
        s->cross_stats.reservation_peak=w;
        s->cross_stats.qpl_job_requested=jr;s->cross_stats.qpl_job_usable=ju;s->cross_stats.byte_limit=bl;
    }
    pthread_mutex_unlock(&s->lock);return 0;
}

#define CROSS_ADD(s,field,value) do { pthread_mutex_lock(&(s)->lock); \
    (s)->cross_stats.field+=(value);pthread_mutex_unlock(&(s)->lock); } while(0)

static int cross_snapshot(struct cross_slot *c)
{
    struct simple_leaf_node *leaf=c->task->leaf;
    int result=-1;
    if(env_int_clamped("BTREE_TEST_CROSS_FAIL_STAGE",0,0,4)==1)return -1;
    pthread_rwlock_rdlock(&leaf->rwlock);
    if(leaf->pending!=c->task || leaf->base_version!=c->task->base_version ||
       leaf->pending_token!=c->task->token || leaf->num_subpages!=1)goto done;
    if(!leaf->is_compressed) {c->stage=CROSS_MERGE;result=0;goto done;}
    if(!leaf->subpage_index||!leaf->compressed_data||leaf->compressed_size<0||
        (size_t)leaf->compressed_size>leaf->compressed_capacity)goto done;
    struct subpage_index_entry b=leaf->subpage_index[0];
    if(b.offset>(uint32_t)leaf->compressed_size || !b.length ||
       b.length>(uint32_t)leaf->compressed_size-b.offset || b.length>c->frame_capacity)goto done;
    memcpy(c->snapshot,(uint8_t *)leaf->compressed_data+b.offset,b.length);
    if(agg_parse(c->snapshot,b.length,&c->header,c->blocks))goto done;
    for(uint32_t i=0;i<c->header.blocks;i++)
        if(c->blocks[i].codec!=COMPRESS_RAW_PACKED && c->blocks[i].codec!=(uint32_t)leaf->compression_algo)goto done;
    c->pairs=malloc(c->header.records*sizeof(*c->pairs));
    if(!c->pairs)goto done;
    c->stage=CROSS_DECODE;result=0;
done:
    pthread_rwlock_unlock(&leaf->rwlock);return result;
}

static int cross_validate_block(struct cross_slot *c)
{
    struct agg_block *b=c->blocks+c->block;
    struct kv_pair *p=c->pairs+c->count;
    size_t n=b->raw_length/sizeof(*p);
    if((uint32_t)crc32(0,(uint8_t *)p,b->raw_length)!=b->crc)return -1;
    for(size_t i=0;i<n;i++)
        if(!p[i].key || (i&&p[i-1].key>=p[i].key))return -1;
    if(p[n-1].key!=b->upper_key || (c->count&&p[-1].key>=p[0].key))return -1;
    c->count+=n;c->block++;
    if(c->block==c->header.blocks) {
        if(c->count!=c->header.records)return -1;
        c->stage=CROSS_MERGE;
    }
    return 0;
}

/* Advertise only dependency-ready device work. A sealed leaf or a host merge
 * is not a device job. Suppressed split-drain successors are not advertised. */
static void cross_ready(struct compaction_scheduler *s,struct cross_slot *c)
{
    if(c->inflight||(c->stage!=CROSS_DECODE&&c->stage!=CROSS_ENCODE))return;
    if(c->stage==CROSS_ENCODE && (c->reuse.mask&(1u<<c->block)))return;
    if(c->stage==CROSS_DECODE && c->blocks[c->block].codec==COMPRESS_RAW_PACKED)return;
    if(!c->ready_ns)c->ready_ns=sd_now(); /* Same event clock as the device. */
    size_t input,output;
    if(c->stage==CROSS_DECODE){input=c->blocks[c->block].length;output=c->blocks[c->block].raw_length;}
    else {c->take=(c->count-c->position+c->header.blocks-c->block-1)/(c->header.blocks-c->block);
        input=c->take*sizeof(*c->pairs);output=c->temporary_capacity;}
    if(s->root->test_device)sd_background_ready(s->root->test_device,(unsigned)(c-s->cross_slots),1,input,output);
}

/* Each job is owned by this coordinator. QPL software is deliberately not
 * accepted here: it may complete inside submit, not on an asynchronous device.
 * CPU_TEST is a separate byte-format/RAW-bypass test, never a hardware result. */
static int cross_submit_backend(struct compaction_scheduler *s,struct cross_slot *c)
{
    if(s->root->test_device)return sd_submit(s->root->test_device,&c->job);
#ifdef HAVE_QPL
    if(c->qpl) {
        qpl_job *j=c->qpl;
        j->op=c->job.encode?qpl_op_compress:qpl_op_decompress;
        j->next_in_ptr=(uint8_t *)c->job.input;j->available_in=(uint32_t)c->job.length;j->total_in=0;
        j->next_out_ptr=c->job.output;j->available_out=(uint32_t)c->job.capacity;j->total_out=0;
        j->flags=QPL_FLAG_FIRST|QPL_FLAG_LAST;j->level=qpl_default_level;
        if(c->job.encode && s->root->config.qpl_huffman_mode==QPL_HUFFMAN_DYNAMIC)j->flags|=QPL_FLAG_DYNAMIC_HUFFMAN;
        qpl_status rc=qpl_submit_job(j);
        int role=c->job.encode?0:2;
        if(rc==QPL_STS_QUEUES_ARE_BUSY_ERR){AGG_INC(route_busy[role],1);return SD_BUSY;}
        if(rc!=QPL_STS_OK){
            __atomic_store_n(&agg_stats.route_last_status[role],rc,__ATOMIC_RELAXED);
            AGG_INC(route_calls[role],1);AGG_INC(route_failed[role],1);return SD_ERROR;
        }
        return SD_OK;
    }
#endif
    int n=c->job.encode?codec_compress_bytes(c->task->tree,c->task->leaf,c->job.input,
        c->job.length,c->job.output,c->job.capacity):codec_decompress_bytes(c->task->tree,
        c->task->leaf,c->job.input,c->job.length,c->job.output,c->job.capacity);
    c->job.status=n>0?SD_OK:SD_ERROR;c->job.result=n>0?(size_t)n:0;return SD_OK;
}
static int cross_poll_backend(struct compaction_scheduler *s,struct cross_slot *c)
{
    if(s->root->test_device)return sd_poll(s->root->test_device,&c->job);
#ifdef HAVE_QPL
    if(c->qpl) {
        qpl_status rc=qpl_check_job(c->qpl);
        if(rc==QPL_STS_BEING_PROCESSED)return SD_BUSY;
        c->job.status=rc==QPL_STS_OK && c->qpl->total_in==c->job.length?SD_OK:SD_ERROR;
        c->job.result=c->qpl->total_out;
        if(rc!=QPL_STS_OK)__atomic_store_n(&agg_stats.route_last_status[c->job.encode?0:2],rc,__ATOMIC_RELAXED);
    }
#endif
    return SD_OK;
}
/* Return -1 failure, 0 blocked, 1 progressed, 2 committed/task freed, 3 split. */
static int cross_step(struct compaction_scheduler *s,struct cross_slot *c,int split_drain)
{
    if(c->inflight) {
        CROSS_ADD(s,completion_checks,1);
        uint64_t poll_start=submission_now_ns();
        int polled=cross_poll_backend(s,c);
        CROSS_ADD(s,poll_ns,submission_now_ns()-poll_start);
        if(polled!=SD_OK){CROSS_ADD(s,not_ready,1);return 0;}
        /* Poll synchronizes publication; this job is terminal before reuse. */
        c->inflight=0;c->ready_ns=0;
        submission_profile_codec_end(c->task->tree,c->profiled);c->profiled=0;
        CROSS_ADD(s,completed,1);
        uint64_t elapsed=submission_now_ns()-c->accepted_ns;
        CROSS_ADD(s,accepted_to_observed_ns,elapsed);
        pthread_mutex_lock(&s->lock);s->cross_stats.current_jobs--;
        s->cross_stats.borrowed_bytes-=c->job.length+c->job.capacity;pthread_mutex_unlock(&s->lock);
        int valid=c->job.status==SD_OK && c->job.result>0 && c->job.result<=c->job.capacity &&
            (c->job.encode || c->job.result==c->job.capacity) &&
            (!agg_test_copy(c->task->leaf->compression_algo) || c->job.result==c->job.length);
        AGG_INC(codec_ns[BPLUS_ORIGIN_COMPACTION][c->job.encode],elapsed);
        agg_observe_codec(c->job.encode,c->job.length,valid?(int)c->job.result:-1);
        if(c->qpl) {
            int role=c->job.encode?0:2;
            AGG_INC(route_ns[role],elapsed);
            if(valid){AGG_INC(route_completed[role],1);AGG_INC(route_output_bytes[role],c->job.result);}
            else AGG_INC(route_failed[role],1);
        }
        if(!valid){CROSS_ADD(s,failed,1);AGG_INC(codec_failures[BPLUS_ORIGIN_COMPACTION][c->job.encode],1);return -1;}
        if(c->stage==CROSS_DECODE) {
            if(cross_validate_block(c))return -1;
        } else {
            uint32_t bytes=(uint32_t)c->job.result;
            uint32_t raw=c->take*sizeof(*c->pairs),codec=c->task->leaf->compression_algo;
            const uint8_t *output=c->temporary;
            if(!agg_test_copy(codec) && bytes>=raw) {
                bytes=raw;codec=COMPRESS_RAW_PACKED;output=(uint8_t *)(c->pairs+c->position);AGG_INC(bypass_blocks,1);
            }
            if(c->offset>c->frame_capacity||bytes>c->frame_capacity-c->offset)return -1;
            c->blocks[c->block]=(struct agg_block){c->pairs[c->position+c->take-1].key,
                c->offset,bytes,raw,codec,
                (uint32_t)crc32(0,(uint8_t *)(c->pairs+c->position),raw)};
            memcpy(c->frame+c->offset,output,bytes);
            c->offset+=bytes;c->position+=c->take;c->block++;
            if(c->block==c->header.blocks)c->stage=CROSS_COMMIT;
        }
        if(c->stage==CROSS_DECODE || (c->stage==CROSS_ENCODE &&
            !(c->reuse.mask&(1u<<c->block))))c->ready_ns=sd_now();
        if(!split_drain)cross_ready(s,c);return 1;
    }
    if(c->stage==CROSS_MERGE) {
        if(env_int_clamped("BTREE_TEST_CROSS_FAIL_STAGE",0,0,4)==2)return -1;
        uint64_t start=submission_now_ns();
        int rc=compact_merge_ordered(&c->pairs,&c->count,c->task->records,c->task->count);
        CROSS_ADD(s,merge_ns,submission_now_ns()-start);
        if(rc)return -1;
        AGG_INC(merged_updates,c->task->count);AGG_INC(merge_count,1);
        int force=env_int_clamped("BTREE_TEST_CROSS_FORCE_SPLIT_KEY",0,0,INT_MAX);
        if(c->count>COMPRESSED_LEAF_SIZE/sizeof(*c->pairs) || (force&&c->task->records[0].key==force)) {
            c->stage=CROSS_SPLIT;c->split_intent_ns=submission_now_ns();return 3;
        }
        if(!c->count)return -1;
        uint32_t old_length=c->header.blocks?
            c->blocks[c->header.blocks-1].offset+c->blocks[c->header.blocks-1].length:0;
        if(agg_prepare_reuse(&c->reuse,old_length?c->snapshot:NULL,old_length,
                             c->pairs,c->count,c->task->records,c->task->count))return -1;
        size_t per=4096/sizeof(*c->pairs);
        c->header=(struct agg_header){AGG_MAGIC,1,ZIPCACHE_AGG_LAYOUT==3?(uint32_t)((c->count+per-1)/per):1,(uint32_t)c->count};
        c->block=0;c->position=0;c->offset=sizeof(c->header)+c->header.blocks*sizeof(*c->blocks);
        c->stage=CROSS_ENCODE;cross_ready(s,c);return 1;
    }
    if(c->stage==CROSS_COMMIT) {
        struct compressed_page_image image={0};
        if(!agg_test_copy(c->task->leaf->compression_algo)) {
            size_t raw_size=sizeof(c->header)+c->header.blocks*sizeof(*c->blocks)+c->count*sizeof(*c->pairs);
            void *a=malloc(c->offset),*b=malloc(raw_size);
            if(!a||!b){free(a);free(b);return -1;}
            int raw=allocation_usable_size(a,c->offset)>=allocation_usable_size(b,raw_size);
            free(a);free(b);
            if(raw) {
                size_t p=0;c->offset=sizeof(c->header)+c->header.blocks*sizeof(*c->blocks);
                for(unsigned i=0;i<c->header.blocks;i++) {
                    struct agg_block *b=c->blocks+i;
                    if(b->codec!=COMPRESS_RAW_PACKED)AGG_INC(bypass_blocks,1);
                    b->codec=COMPRESS_RAW_PACKED;b->offset=c->offset;b->length=b->raw_length;
                    memcpy(c->frame+c->offset,(uint8_t *)c->pairs+p,b->length);c->offset+=b->length;p+=b->length;
                }
            }
        }
        memcpy(c->frame,&c->header,sizeof(c->header));
        memcpy(c->frame+sizeof(c->header),c->blocks,c->header.blocks*sizeof(*c->blocks));
        int fail=env_int_clamped("BTREE_TEST_CROSS_FAIL_STAGE",0,0,4);
        image.data=fail==3?NULL:malloc(c->offset);
        image.index=fail==4?NULL:calloc(1,sizeof(*image.index));
        if(!image.data||!image.index){compressed_page_image_destroy(&image);return -1;}
        memcpy(image.data,c->frame,c->offset);image.size=c->offset;
        image.usable=allocation_usable_size(image.data,image.size);image.num_subpages=1;
        image.uncompressed_bytes=c->count*sizeof(*c->pairs);
        image.index[0].length=c->offset;image.index[0].uncompressed_bytes=image.uncompressed_bytes;
        uint64_t start=submission_now_ns();
        int rc=compaction_commit_image(s,c->task,&image);
        CROSS_ADD(s,commit_ns,submission_now_ns()-start);
        compressed_page_image_destroy(&image);return rc?-1:2;
    }
    if(c->stage==CROSS_SPLIT)return 3;
    if(c->stage==CROSS_DECODE && c->blocks[c->block].codec==COMPRESS_RAW_PACKED) {
        struct agg_block *b=c->blocks+c->block;
        if(b->length!=b->raw_length)return -1;
        memcpy(c->pairs+c->count,c->snapshot+b->offset,b->raw_length);
        return cross_validate_block(c)?-1:1;
    }
    if(c->stage==CROSS_ENCODE && (c->reuse.mask&(1u<<c->block))) {
        int copied=agg_copy_reused_block(&c->reuse,c->block,c->frame,c->offset,
                                         (uint32_t)c->frame_capacity,c->blocks+c->block);
        if(copied<0)return -1;
        c->take=(c->count-c->position+c->header.blocks-c->block-1)/(c->header.blocks-c->block);
        c->offset+=(uint32_t)copied;c->position+=c->take;c->block++;c->ready_ns=0;
        if(c->block==c->header.blocks)c->stage=CROSS_COMMIT;
        else cross_ready(s,c);
        return 1; /* Host progress only: do not advertise or submit a fake job. */
    }
    cross_ready(s,c);
    c->job=(struct sd_job){.encode=c->stage==CROSS_ENCODE,.role=1,
        .producer_slot=(int)(c-s->cross_slots),.ready_ns=c->ready_ns};
    if(c->stage==CROSS_DECODE) {
        struct agg_block *b=c->blocks+c->block;
        c->job.input=c->snapshot+b->offset;c->job.output=c->pairs+c->count;
        c->job.length=b->length;c->job.capacity=b->raw_length;
    } else {
        c->take=(c->count-c->position+c->header.blocks-c->block-1)/(c->header.blocks-c->block);
        c->job.input=c->pairs+c->position;c->job.output=c->temporary;
        c->job.length=c->take*sizeof(*c->pairs);c->job.capacity=c->temporary_capacity;
    }
    size_t charge=c->job.length+c->job.capacity;
    if(charge>s->cross_stats.byte_limit-s->cross_stats.borrowed_bytes){CROSS_ADD(s,byte_busy,1);return 0;}
    uint64_t submit_start=submission_now_ns();
    int rc=cross_submit_backend(s,c);
    CROSS_ADD(s,submit_ns,submission_now_ns()-submit_start);
    if(rc==SD_BUSY){CROSS_ADD(s,busy,1);return 0;}
    if(rc!=SD_OK){CROSS_ADD(s,failed,1);return -1;}
    c->inflight=1;
    c->accepted_ns=submission_now_ns();
    CROSS_ADD(s,ready_to_accept_ns,sd_now()-c->ready_ns);
    pthread_mutex_lock(&s->lock);s->cross_stats.current_jobs++;
    if(s->cross_stats.current_jobs>s->cross_stats.peak_jobs)s->cross_stats.peak_jobs=s->cross_stats.current_jobs;
    s->cross_stats.borrowed_bytes+=charge;
    if(s->cross_stats.borrowed_bytes>s->cross_stats.peak_borrowed_bytes)s->cross_stats.peak_borrowed_bytes=s->cross_stats.borrowed_bytes;
    pthread_mutex_unlock(&s->lock);
    if(c->qpl) {
        int role=c->job.encode?0:2;AGG_INC(route_calls[role],1);AGG_INC(route_input_bytes[role],c->job.length);
        AGG_INC(real_codec_calls,1);
        if(c->job.encode)__atomic_add_fetch(&c->task->tree->qpl_compress_calls,1,__ATOMIC_RELAXED);
        else __atomic_add_fetch(&c->task->tree->qpl_decompress_calls,1,__ATOMIC_RELAXED);
    }
    c->profiled=submission_profile_codec_begin(c->task->tree,c->job.encode,
        (uint32_t)c->job.length,(uint32_t)c->job.capacity,c->task->leaf);
    if(c->job.encode){AGG_INC(compress[BPLUS_ORIGIN_COMPACTION],1);AGG_INC(encode_bytes[BPLUS_ORIGIN_COMPACTION],c->job.length);}
    else{AGG_INC(decompress[BPLUS_ORIGIN_COMPACTION],1);AGG_INC(decode_bytes[BPLUS_ORIGIN_COMPACTION],c->job.capacity);}
    CROSS_ADD(s,accepted,1);return 1;
}

static void cross_retire(struct compaction_scheduler *s,struct cross_slot *c,int retry)
{
    assert(!c->inflight);
    if(retry)compaction_fail_or_retry(s,c->task);
    if(s->root->test_device)sd_background_ready(s->root->test_device,(unsigned)(c-s->cross_slots),0,0,0);
    free(c->pairs);
    uint8_t *snapshot=c->snapshot,*frame=c->frame,*temporary=c->temporary;
    size_t fc=c->frame_capacity,tc=c->temporary_capacity;
    qpl_job *qpl=c->qpl;int initialized=c->qpl_initialized;
    memset(c,0,sizeof(*c));c->snapshot=snapshot;c->frame=frame;c->temporary=temporary;
    c->frame_capacity=fc;c->temporary_capacity=tc;
    c->qpl=qpl;c->qpl_initialized=initialized;
    pthread_mutex_lock(&s->lock);s->running_count--;s->cross_stats.finished++;
    compaction_scheduler_changed_locked(s);pthread_mutex_unlock(&s->lock);
}

static void *cross_worker(struct compaction_scheduler *s)
{
    struct sd_device *d=s->root->test_device;
    submission_tls_write_critical=0;agg_origin=BPLUS_ORIGIN_COMPACTION;
    unsigned turn=0;
    for(;;) {
        uint64_t observed=d?sd_sequence(d):0;
        int progress=0,live=0,split_drain=0;
        for(int i=0;i<s->cross_window;i++)if(s->cross_slots[i].task&&s->cross_slots[i].stage==CROSS_SPLIT)split_drain=1;
        for(int k=0;k<s->cross_window;k++) {
            struct cross_slot *c=s->cross_slots+(turn+k)%s->cross_window;
            if(!c->task&&!split_drain) {
                pthread_mutex_lock(&s->lock);
                if(s->queue_count) {
                    c->task=s->queue[s->queue_head];s->queue[s->queue_head]=NULL;
                    s->queue_head=(s->queue_head+1)%s->queue_capacity;s->queue_count--;s->running_count++;
                    pending_task_state_store(c->task,PENDING_TASK_RUNNING);
                    c->task->dequeued_ns=submission_now_ns();
                    uint64_t waited=c->task->dequeued_ns-c->task->enqueued_ns;
                    s->total_queue_wait_ns+=waited;AGG_INC(queue_wait_ns,waited);
                    if(waited>s->max_queue_wait_ns)s->max_queue_wait_ns=waited;
                    s->cross_stats.started++;
                    if((uint64_t)s->running_count>s->cross_stats.peak_leaves)s->cross_stats.peak_leaves=s->running_count;
                    s->cross_stats.reservation_peak=s->cross_stats.workspace_requested+
                        s->cross_stats.peak_leaves*(cross_charge()-cross_workspace_per_slot());
                    compaction_scheduler_changed_locked(s);
                }
                pthread_mutex_unlock(&s->lock);
                if(c->task) {
                    uint64_t start=submission_now_ns();int rc=cross_snapshot(c);
                    CROSS_ADD(s,prepare_ns,submission_now_ns()-start);progress=1;
                    if(rc){cross_retire(s,c,1);continue;}
                }
            }
            if(!c->task)continue;
            if(split_drain&&!c->inflight&&c->stage!=CROSS_SPLIT) {
                if(d)sd_background_ready(d,(unsigned)(c-s->cross_slots),0,0,0);
                CROSS_ADD(s,split_paused_steps,1);live++;continue;
            }
            uint64_t step_start=submission_now_ns();
            int rc=cross_step(s,c,split_drain);
            CROSS_ADD(s,host_step_ns,submission_now_ns()-step_start);
            if(rc==2) {cross_retire(s,c,0);progress=1;}
            else if(rc<0) {cross_retire(s,c,1);progress=1;}
            else if(rc==3) {
                if(!split_drain){CROSS_ADD(s,split_drains,1);if(d)sd_background_pause(d);}
                split_drain=1;
                pthread_mutex_lock(&s->lock);s->cross_stats.split_latched=1;pthread_mutex_unlock(&s->lock);
                /* Structural fallback must not block while another slot's
                 * accepted buffers could be needed by a concurrent drain. */
                int others=0;for(int j=0;j<s->cross_window;j++)others+=s->cross_slots[j].inflight;
                if(!others) {
                    uint64_t waited=submission_now_ns()-c->split_intent_ns;
                    pthread_mutex_lock(&s->lock);s->cross_stats.split_drain_wait_ns+=waited;
                    if(waited>s->cross_stats.max_split_drain_wait_ns)s->cross_stats.max_split_drain_wait_ns=waited;
                    pthread_mutex_unlock(&s->lock);
                    uint64_t start=submission_now_ns();compaction_split_overflow(s,c->task);
                    CROSS_ADD(s,split_ns,submission_now_ns()-start);
                    cross_retire(s,c,0);progress=1;
                }
            } else if(rc)progress=1;
            live+=c->task!=NULL;
        }
        pthread_mutex_lock(&s->lock);
        s->cross_stats.split_latched=0;
        for(int i=0;i<s->cross_window;i++)if(s->cross_slots[i].task&&s->cross_slots[i].stage==CROSS_SPLIT)s->cross_stats.split_latched=1;
        pthread_mutex_unlock(&s->lock);
        turn=(turn+1)%(unsigned)s->cross_window;
        pthread_mutex_lock(&s->lock);
        if(s->queue_count&&s->running_count==s->cross_window)s->cross_stats.window_full_backlog++;
        pthread_mutex_unlock(&s->lock);
        if(progress)continue;
        pthread_mutex_lock(&s->lock);
        if(!live) {
            while(!s->queue_count&&!s->shutdown)pthread_cond_wait(&s->work_available,&s->lock);
            int stop=!s->queue_count&&s->shutdown;
            pthread_mutex_unlock(&s->lock);if(stop)break;
        } else {
            pthread_mutex_unlock(&s->lock);
            uint64_t start=submission_now_ns();
            if(d)sd_wait_change(d,observed);
            else {
                /* No blocking qpl_wait_job: every live slot is polled again.
                 * Bounded backoff is explicit and included in CPU/latency. */
                int us=env_int_clamped("BTREE_CROSS_POLL_US",0,0,1000);
                if(us){struct timespec pause={0,us*1000};nanosleep(&pause,NULL);}
                else sched_yield();
            }
            CROSS_ADD(s,device_wait_ns,submission_now_ns()-start);
        }
    }
    return NULL;
}
#undef CROSS_ADD
