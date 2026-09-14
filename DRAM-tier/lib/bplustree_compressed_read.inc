/* Shared implementation of legacy integer GET, complete record GET and the
 * experimental bounded GET_MANY. Only compressed bytes cross the leaf lock. */
struct record_read_block {
    uint8_t *input;
    uint32_t length, raw_length;
    int block_id;
    compression_algo_t codec;
#if ZIPCACHE_AGG_LAYOUT > 0
    struct agg_block descriptor;
#endif
};

static int read_records(struct bplus_tree_compressed *root, const key_t *keys,
                        size_t count, struct kv_pair *out, int *statuses)
{
    if (!root || !keys || !out || !statuses || !count || count > 32) return BPLUS_RECORD_INVALID;
    if (!root->initialized) return BPLUS_RECORD_CLOSED;
    struct kv_pair values[32] = {{0}};
    int states[32], done[32] = {0}, duplicate[32];
    for (size_t i = 0; i < count; i++) {
        states[i] = BPLUS_RECORD_NOT_FOUND; duplicate[i] = -1;
        if (!keys[i]) { states[i] = BPLUS_RECORD_INVALID; done[i] = 1; }
        for (size_t j = 0; j < i; j++) if (keys[i] == keys[j]) { duplicate[i] = (int)j; break; }
    }
#ifdef ZIPCACHE_AGG_EXPERIMENT
    int previous_origin = agg_origin; agg_origin = BPLUS_ORIGIN_GET;
#endif
    int error = BPLUS_RECORD_OK;
    for (size_t first = 0; first < count && !error; first++) {
        if (done[first] || duplicate[first] >= 0) continue;
        struct bplus_tree_compressed *tree = compressed_tree_is_sharded(root)
            ? compressed_tree_shard_for_key(root, keys[first]) : root;
        submission_profile_begin_logical(tree, SUBMISSION_LOGICAL_READ);
        struct record_read_block blocks[32] = {{0}};
        int group[32], target[32]; size_t members = 0, block_count = 0;
        uint64_t before = submission_now_ns();
        pthread_rwlock_rdlock(&tree->rwlock);
        AGG_INC(read_lock_wait_ns, submission_now_ns()-before);
        struct compressed_leaf_ref *ref = find_leaf_for_key(tree->tree, keys[first]);
        if (!ref || !ref->payload) { pthread_rwlock_unlock(&tree->rwlock); done[first] = 1; continue; }
        struct simple_leaf_node *leaf = (struct simple_leaf_node *)ref->payload;
        /* No retained raw leaf pointer after the read lock is released. */
        for (size_t i = first; i < count; i++) {
            if (done[i] || duplicate[i] >= 0) continue;
            struct bplus_tree_compressed *child = compressed_tree_is_sharded(root)
                ? compressed_tree_shard_for_key(root, keys[i]) : root;
            if (child == tree && find_leaf_for_key(tree->tree, keys[i]) == ref) {
                group[members] = (int)i; target[members++] = -1; done[i] = 1;
            }
        }
        before = submission_now_ns();
        pthread_rwlock_rdlock(&leaf->rwlock);
        AGG_INC(read_lock_wait_ns, submission_now_ns()-before);
        pthread_rwlock_unlock(&tree->rwlock);
        for (size_t m = 0; m < members && !error; m++) {
            int i = group[m];
            for (int a = 0; a < landing_buffer_capacity_for_tree(tree); a++) {
                if (leaf->active[a].key == keys[i]) {
                    values[i] = leaf->active[a]; states[i] = BPLUS_RECORD_OK; AGG_INC(active_hits, 1); break;
                }
            }
            if (states[i] == BPLUS_RECORD_OK) continue;
            if (leaf->pending) for (size_t p = 0; p < leaf->pending->count; p++) {
                if (leaf->pending->records[p].key == keys[i]) {
                    values[i] = leaf->pending->records[p]; states[i] = BPLUS_RECORD_OK; AGG_INC(pending_hits, 1); break;
                }
            }
            if (states[i] == BPLUS_RECORD_OK || !leaf->is_compressed) continue;
            if (!leaf->subpage_index || leaf->num_subpages <= 0 || !leaf->compressed_data ||
                leaf->compressed_size < 0 || (size_t)leaf->compressed_size > leaf->compressed_capacity) {
                error = BPLUS_RECORD_CODEC_ERROR; break;
            }
            int bucket = positive_mod_i32(keys[i], leaf->num_subpages);
            struct subpage_index_entry e = leaf->subpage_index[bucket];
            if (!e.length) continue;
            if (e.offset > (uint32_t)leaf->compressed_size || e.length > (uint32_t)leaf->compressed_size-e.offset) {
                error = BPLUS_RECORD_CODEC_ERROR; break;
            }
            uint32_t offset = e.offset, length = e.length;
            uint32_t raw_length = COMPRESSED_LEAF_SIZE / leaf->num_subpages;
            int id = bucket;
#if ZIPCACHE_AGG_LAYOUT > 0
            struct agg_header h; struct agg_block descriptors[4];
            if (agg_parse((uint8_t *)leaf->compressed_data+offset, length, &h, descriptors)) {
                error = BPLUS_RECORD_CODEC_ERROR; break;
            }
            uint32_t selected = 0; while (selected < h.blocks && keys[i] > descriptors[selected].upper_key) selected++;
            if (selected == h.blocks) continue;
            id = (int)selected; offset += descriptors[selected].offset; length = descriptors[selected].length;
            raw_length = descriptors[selected].raw_length;
#endif
            size_t b = 0; while (b < block_count && blocks[b].block_id != id) b++;
            if (b == block_count) {
                blocks[b].input = malloc(length);
                if (!blocks[b].input) { error = BPLUS_RECORD_NO_SPACE; break; }
                blocks[b].block_id = id; blocks[b].length = length; blocks[b].raw_length = raw_length;
                blocks[b].codec = leaf->compression_algo;
#if ZIPCACHE_AGG_LAYOUT > 0
                blocks[b].descriptor = descriptors[id];
#endif
                memcpy(blocks[b].input, leaf->compressed_data+offset, length);
                AGG_INC(copied_compressed_bytes, length); block_count++;
            }
            target[m] = (int)b;
        }
        pthread_rwlock_unlock(&leaf->rwlock);
        /* Decode only copied blocks. Do not touch leaf/ref here, including when
         * a concurrent DELETE/split frees them. */
        for (size_t b = 0; b < block_count && !error; b++) {
            uint8_t *raw = malloc(blocks[b].raw_length);
            if (!raw) { error = BPLUS_RECORD_NO_SPACE; break; }
            int n;
            errno = 0;
#if ZIPCACHE_AGG_LAYOUT > 0
            n = agg_decode_block(tree, &blocks[b].descriptor, blocks[b].input, raw);
#else
            struct simple_leaf_node snapshot = {0}; snapshot.compression_algo = blocks[b].codec;
            n = decompress_subpage(tree, &snapshot, blocks[b].input, blocks[b].length, raw, blocks[b].raw_length);
#endif
            if (n < 0) error = errno == ENOMEM ? BPLUS_RECORD_NO_SPACE : BPLUS_RECORD_CODEC_ERROR;
            else for (size_t m = 0; m < members; m++) if (target[m] == (int)b) {
                int i = group[m];
                for (size_t p = 0; p + sizeof(struct kv_pair) <= (size_t)n; p += sizeof(struct kv_pair)) {
                    struct kv_pair pair; memcpy(&pair, raw+p, sizeof(pair));
                    if (pair.key == keys[i]) { values[i] = pair; states[i] = BPLUS_RECORD_OK; AGG_INC(base_hits, 1); break; }
                }
            }
            free(raw);
        }
        for (size_t b = 0; b < block_count; b++) free(blocks[b].input);
    }
#ifdef ZIPCACHE_AGG_EXPERIMENT
    agg_origin = previous_origin;
#endif
    if (error) return error;
    for (size_t i = 0; i < count; i++) {
        if (duplicate[i] >= 0) { states[i] = states[duplicate[i]]; values[i] = values[duplicate[i]]; }
        statuses[i] = states[i];
        if (states[i] == BPLUS_RECORD_OK) out[i] = values[i];
        else if (states[i] == BPLUS_RECORD_NOT_FOUND) AGG_INC(misses, 1);
    }
    return BPLUS_RECORD_OK;
}

int bplus_tree_compressed_get_record(struct bplus_tree_compressed *tree, key_t key, struct kv_pair *out)
{
    int status = BPLUS_RECORD_NOT_FOUND;
    int error = read_records(tree, &key, 1, out, &status);
    return error ? error : status;
}
int bplus_tree_compressed_get(struct bplus_tree_compressed *tree, key_t key)
{
    struct kv_pair record;
    return bplus_tree_compressed_get_record(tree, key, &record) == BPLUS_RECORD_OK ? record.stored_value : -1;
}
#ifdef ZIPCACHE_AGG_EXPERIMENT
int bplus_tree_compressed_get_many_records(struct bplus_tree_compressed *tree,
    const key_t *keys, size_t count, struct kv_pair *out, int *statuses)
{
    return read_records(tree, keys, count, out, statuses);
}
#endif
