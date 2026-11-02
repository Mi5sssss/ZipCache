/* Separate diagnostic process only. No leaf fields, no retained leaf pointer.
 * The logger never takes tree/leaf/scheduler locks. Callers snapshot fields
 * while holding the leaf lock; async completions log only pointer identity.
 * All successful primary traces are update-only. Any structural fallback
 * invalidates the offline identity-based analysis rather than guessing IDs. */
static pthread_mutex_t hd_lock = PTHREAD_MUTEX_INITIALIZER;
static FILE *hd_file;
static uint64_t hd_sequence, hd_requests;
static size_t hd_raw_usable;
int bplus_tree_hot_diagnostic_open(const char *path) {
    if (!path || hd_file) return -1;
    void *probe=malloc(sizeof(struct hot_raw));
    if(!probe)return -1;
    hd_raw_usable=allocation_usable_size(probe,sizeof(struct hot_raw));free(probe);
    hd_file=fopen(path,"w");if(!hd_file)return -1;
    hd_sequence=hd_requests=0;
    if(fputs("sequence,requests,kind,leaf,hot,base_usable,active_usable,raw_usable,units,bytes,encoded_blocks\n",hd_file)<0)abort();
    return 0;
}
int bplus_tree_hot_diagnostic_close(void) {
    pthread_mutex_lock(&hd_lock);
    FILE *f=hd_file;hd_file=NULL;
    int rc=f?fclose(f):0;
    pthread_mutex_unlock(&hd_lock);return rc;
}
static void hot_diag_emit(char kind,uintptr_t identity,int hot,size_t base,
                          size_t active,size_t units,size_t bytes,unsigned blocks) {
    if(!hd_file)return; /* open/close occur while callers and worker are idle */
    pthread_mutex_lock(&hd_lock);
    if(kind=='G'||kind=='H'||kind=='A'||kind=='B'||kind=='N'||kind=='W')hd_requests++;
    if(fprintf(hd_file,"%llu,%llu,%c,%llx,%d,%zu,%zu,%zu,%zu,%zu,%u\n",
        (unsigned long long)++hd_sequence,(unsigned long long)hd_requests,kind,
        (unsigned long long)identity,hot,base,active,hd_raw_usable,units,bytes,blocks)<0)abort();
    pthread_mutex_unlock(&hd_lock);
}
/* Called only under leaf read/write lock. G means selected encoded block,
 * not CPU time or a claimed successful job. Aggregate codec counts reconcile
 * these selections after a zero-error run. */
static void hot_diag_leaf(struct simple_leaf_node *l,char kind,size_t units,size_t bytes) {
    if(!hd_file)return;
    unsigned n=0;
    if(l->is_compressed && l->compressed_data && l->compressed_size>0) {
        struct agg_header h;struct agg_block b[4];
        if(agg_parse((uint8_t*)l->compressed_data,l->compressed_size,&h,b))abort();
        for(unsigned i=0;i<h.blocks;i++)n+=b[i].codec!=COMPRESS_RAW_PACKED;
    }
    hot_diag_emit(kind,(uintptr_t)l,l->hot_raw!=NULL,
        l->compressed_data?allocation_usable_size(l->compressed_data,l->compressed_capacity):0,
        active_allocation(l),units,bytes,n);
}
