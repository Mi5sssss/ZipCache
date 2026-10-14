/* Experiment-only routing. Encoding tags remain unchanged. No device model,
 * alternate cache, asynchronous executor or runtime route mutation lives here. */
#ifdef ZIPCACHE_AGG_EXPERIMENT
static uint64_t (*split_accel_counter)(unsigned);
static __thread uint64_t split_accel_before[20];
int bplus_tree_compressed_accel_configure(int compress, int decompress, const char **library)
{
#ifdef HAVE_ZLIB
    if ((compress != 0 && compress != 1) || (decompress != 0 && decompress != 1)) return -1;
    const char *(*source)(void) = dlsym(RTLD_DEFAULT, "zipcache_accel_source");
    int (*enabled)(void) = dlsym(RTLD_DEFAULT, "zipcache_accel_statistics_enabled");
    int (*configure)(int,int) = dlsym(RTLD_DEFAULT, "zipcache_accel_split_configure");
    uint64_t (*counter)(unsigned) = dlsym(RTLD_DEFAULT, "zipcache_accel_counter");
    Dl_info codec_info, observer_info;
    if (!source || !enabled || !configure || !counter || !enabled() ||
        strcmp(source(), "b437553304ca765be38c7352d5a3086efd40cd09") ||
        !dladdr(dlsym(RTLD_DEFAULT, "deflate"), &codec_info) || !dladdr((void *)source, &observer_info) ||
        codec_info.dli_fbase != observer_info.dli_fbase || configure(compress, decompress)) return -1;
    split_accel_counter = counter;
    split_zlib_window_bits = 12; /* shim-off and shim-on use compatible streams */
    if (library) *library = observer_info.dli_fname;
    return 0;
#else
    (void)compress; (void)decompress; (void)library; return -1;
#endif
}

static uint64_t split_codec_start(void)
{
    if (split_accel_counter) for (unsigned i = 0; i < 20; i++) split_accel_before[i] = split_accel_counter(i);
    return submission_now_ns();
}

static int split_routes_init(struct bplus_tree_compressed *tree,
                              const struct compression_config *config)
{
    static const char *names[] = {"BTREE_QPL_COMPRESS_PATH", "BTREE_QPL_GET_PATH",
                                  "BTREE_QPL_MAINTENANCE_PATH"};
    for (int i = 0; i < 3; i++) {
        tree->split_routes[i] = config->qpl_path;
        const char *s = getenv(names[i]);
        if (!s || !*s) continue;
        tree->split_routes_enabled = 1;
        if (!strcmp(s, "software")) tree->split_routes[i] = qpl_path_software;
        else if (!strcmp(s, "hardware")) tree->split_routes[i] = qpl_path_hardware;
        else { fprintf(stderr, "Invalid %s: use software or hardware\n", names[i]); return -1; }
    }
    if (tree->split_routes_enabled) {
#if !defined(HAVE_QPL) || ZIPCACHE_AGG_LAYOUT == 0
        fprintf(stderr, "Split QPL routes unavailable: require real QPL and framed L1-L3\n");
        return -1;
#endif
        if (config->algo != COMPRESS_QPL) return -1;
    }
    return 0;
}

int bplus_tree_compressed_execution_routes(struct bplus_tree_compressed *tree,
                                           qpl_path_t paths[3])
{
    if (!tree || !paths) return -1;
    memcpy(paths, tree->split_routes, sizeof(tree->split_routes));
    return tree->split_routes_enabled;
}

static int split_qpl_transform(struct bplus_tree_compressed *tree, int encode,
    const uint8_t *src, uint32_t n, uint8_t *dst, uint32_t cap)
{
    int role = encode ? 0 : agg_origin == BPLUS_ORIGIN_GET ? 1 : 2;
    uint64_t start = submission_now_ns();
    AGG_INC(route_calls[role], 1); AGG_INC(route_input_bytes[role], n);
    if (encode) __atomic_add_fetch(&tree->qpl_compress_calls, 1, __ATOMIC_RELAXED);
    else __atomic_add_fetch(&tree->qpl_decompress_calls, 1, __ATOMIC_RELAXED);
    qpl_job *job = acquire_qpl_tls_job(tree, encode);
    int produced = -1;
    int status = -1; /* allocation/init failure before an execution call */
    if (job) {
        __atomic_add_fetch(&tree->qpl_tls_jobs, 1, __ATOMIC_RELAXED);
        job->op = encode ? qpl_op_compress : qpl_op_decompress;
        job->next_in_ptr = (uint8_t *)src; job->available_in = n; job->total_in = 0;
        job->next_out_ptr = dst; job->available_out = cap; job->total_out = 0;
        job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;
        job->level = qpl_default_level;
        if (encode && tree->config.qpl_huffman_mode == QPL_HUFFMAN_DYNAMIC)
            job->flags |= QPL_FLAG_DYNAMIC_HUFFMAN;
        qpl_status rc = qpl_execute_job(job);
        status = (int)rc;
#ifdef HAVE_QPL
        if (rc == QPL_STS_QUEUES_ARE_BUSY_ERR) AGG_INC(route_busy[role], 1);
#endif
        if (rc == QPL_STS_OK && job->total_in == n && job->total_out && job->total_out <= cap)
            produced = (int)job->total_out;
    }
    /* Explicit routes never retry through the old differently initialized pool
     * and never silently change codec/path. Auto inheritance stays observable. */
    AGG_INC(route_ns[role], submission_now_ns()-start);
    if (produced > 0) {
        AGG_INC(route_completed[role], 1); AGG_INC(route_output_bytes[role], produced);
    } else {
        AGG_INC(route_failed[role], 1);
        __atomic_store_n(&agg_stats.route_last_status[role], (uint64_t)(int64_t)status, __ATOMIC_RELAXED);
        fprintf(stderr,"QPL route=%d path=%d status=%d input=%u capacity=%u failed\n",
                role,(int)tree->split_routes[role],status,n,cap);
        __atomic_add_fetch(&tree->qpl_errors, 1, __ATOMIC_RELAXED);
    }
    return produced;
}

/* Gate the actual worker's completed byte transform before publication. Tests
 * use the real pending/version/DELETE/shutdown paths, never hardware timings. */
static int split_codec_finish(struct bplus_tree_compressed *tree, int encode,
                              int result, uint64_t started)
{
    if (split_accel_counter) for (unsigned i = 0; i < 20; i++)
        AGG_INC(accel_counters[i], split_accel_counter(i)-split_accel_before[i]);
    struct compaction_scheduler *s = tree->scheduler;
    if (encode && s && s->split_test_mode && pthread_equal(pthread_self(), s->worker)) {
        pthread_mutex_lock(&s->lock);
        s->contract_stats.accepted++; s->contract_stats.in_flight++;
        pthread_cond_broadcast(&s->state_changed);
        while (!s->contract_gate_open && !s->shutdown)
            pthread_cond_wait(&s->state_changed, &s->lock);
        if (s->split_test_mode == 4) result = -1;
        if (s->split_test_mode == 5 && result > 0) result--; /* invalid encoded frame */
        s->contract_stats.terminal++; s->contract_stats.in_flight--;
        if (result <= 0 || s->split_test_mode == 5) s->contract_stats.failures++;
        pthread_cond_broadcast(&s->state_changed);
        pthread_mutex_unlock(&s->lock);
        /* Reject abnormal output at the codec boundary, not after installing it. */
        if (s->split_test_mode == 5) result = -1;
    }
    AGG_INC(codec_ns[agg_origin][encode], submission_now_ns()-started);
    if (result <= 0) AGG_INC(codec_failures[agg_origin][encode], 1);
    return result;
}
#endif
