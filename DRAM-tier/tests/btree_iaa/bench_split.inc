/* Benchmark-only controls. No persistent storage API is added to ZipCache. */
static void wait_until(uint64_t deadline)
{
    for (;;) {
        uint64_t now=ns();if(now>=deadline)return;
        uint64_t left=deadline-now;
        struct timespec t={(time_t)(left/1000000000),(long)(left%1000000000)};
        nanosleep(&t,NULL);
    }
}
static void image_write(FILE *file,const void *p,size_t bytes)
{check(fwrite(p,1,bytes,file)==bytes,"snapshot write");}
static void snapshot_tree(struct bplus_tree_compressed *tree,FILE *file)
{
    /* Caller has drained and has no producers. Serialize only representation
     * bytes, not pointers, tokens, timestamps, locks or allocator padding. */
    if(tree->shard_count>1){for(int i=0;i<tree->shard_count;i++)snapshot_tree(tree->shards[i],file);return;}
    struct list_head *p,*head=&tree->tree->list[0];
    for(p=head->next;p!=head;p=p->next){
        struct compressed_leaf_ref *ref=list_entry(p,struct compressed_leaf_ref,link);
        struct simple_leaf_node *leaf=(void*)ref->payload;
        if(!leaf)continue;
        check(!leaf->pending,"snapshot with pending");
        uint64_t length=leaf->compressed_bytes;image_write(file,&length,sizeof(length));
        if(length)image_write(file,leaf->compressed_data,(size_t)length);
        image_write(file,leaf->active,sizeof(leaf->active));
    }
}
static const char *path_name(qpl_path_t path)
{return path==qpl_path_hardware?"hardware":path==qpl_path_software?"software":"auto-unobserved";}
static void print_split(struct fixture *f,const struct bplus_aggregation_stats *s,
    const char *accel_library,uint64_t samples,uint64_t first,uint64_t last,uint64_t peak)
{
    qpl_path_t routes[3];int enabled=bplus_tree_compressed_execution_routes(f->tree,routes);
    printf("EXECUTION {\"split_enabled\":%d,\"verify_each\":%d,\"arrival_rate\":%" PRIu64
        ",\"latency_basis\":\"%s\",\"queue_samples\":%" PRIu64 ",\"queue_first\":%" PRIu64
        ",\"queue_last\":%" PRIu64 ",\"queue_observed_peak\":%" PRIu64 ",\"routes\":[",
        enabled,f->oracle,f->arrival_rate,f->arrival_rate?"scheduled-arrival-to-return":"API-call-to-return",
        samples,first,last,peak);
    for(int i=0;i<3;i++)printf("%s{\"requested\":\"%s\",\"calls\":%" PRIu64
        ",\"completed\":%" PRIu64 ",\"failed\":%" PRIu64 ",\"input_bytes\":%" PRIu64
        ",\"output_bytes\":%" PRIu64 ",\"ns\":%" PRIu64 ",\"busy\":%" PRIu64
        ",\"last_error_status\":%" PRId64 "}",i?",":"",f->tree->config.algo==COMPRESS_QPL?path_name(routes[i]):"not-applicable",
        s->route_calls[i],s->route_completed[i],s->route_failed[i],s->route_input_bytes[i],s->route_output_bytes[i],s->route_ns[i],
        s->route_busy[i],(int64_t)s->route_last_status[i]);
    printf("],\"accel_observed\":%s,\"accel_counters\":[",accel_library?"true":"false");
    for(int i=0;i<20;i++)printf("%s%" PRIu64,i?",":"",s->accel_counters[i]);
    printf("]}\n");
    /* Library location is printed separately, not interpolated into JSON. */
    if(accel_library)printf("ACCEL_LIBRARY %s\n",accel_library);
}
