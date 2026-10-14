/* Experimental on-leaf framing. Included in the real tree translation unit so
 * allocations, profiling, locking and failures use exactly the tree's paths.
 * No index or tree is implemented here. Native-endian, process-local format. */
#if ZIPCACHE_AGG_LAYOUT > 0
#define AGG_MAGIC UINT32_C(0x5a414731)
struct agg_header { uint32_t magic, version, blocks, records; };
struct agg_block { int32_t upper_key; uint32_t offset, length, raw_length, codec, crc; };
static void agg_observe_codec(int encode, uint32_t input, int output)
{
    int role=encode?0:agg_origin==BPLUS_ORIGIN_GET?1:2;
    if(output<=0){AGG_INC(work_failed[role],1);return;}
    AGG_INC(work_calls[role],1);AGG_INC(work_input[role],input);
    AGG_INC(work_output[role],output);
    unsigned a=input?(input-1)/1024:0,b=((unsigned)output-1)/1024;
    AGG_INC(work_histogram[role][0][a<16?a:16],1);
    AGG_INC(work_histogram[role][1][b<16?b:16],1);
}
static int agg_test_copy(uint32_t codec) {
#ifdef ZIPCACHE_DEVICE_SURROGATE
    return codec==COMPRESS_ASYNC_COPY_TEST || codec==COMPRESS_DIRECT_COPY_TEST;
#else
    (void)codec;return 0;
#endif
}
_Static_assert(sizeof(struct agg_header) == 16 && sizeof(struct agg_block) == 24,
               "aggregation framing ABI");

static int agg_parse(const uint8_t *src, uint32_t length, struct agg_header *h,
                     struct agg_block blocks[4])
{
    if (length < sizeof(*h)) return -1;
    memcpy(h, src, sizeof(*h));
    if (h->magic != AGG_MAGIC || h->version != 1 || !h->blocks || h->blocks > 4 ||
        !h->records || h->records > COMPRESSED_LEAF_SIZE / sizeof(struct kv_pair)) return -1;
    uint32_t offset = sizeof(*h) + h->blocks * sizeof(*blocks), records = 0;
    if (offset > length) return -1;
    memcpy(blocks, src + sizeof(*h), h->blocks * sizeof(*blocks));
    for (uint32_t i = 0; i < h->blocks; i++) {
        struct agg_block *b = blocks + i;
        uint32_t max_raw = ZIPCACHE_AGG_LAYOUT == 3 ? 4096 : COMPRESSED_LEAF_SIZE;
        if (b->offset != offset || !b->length || b->length > length-offset ||
            !b->raw_length || b->raw_length > max_raw || b->raw_length % sizeof(struct kv_pair) ||
            !b->upper_key || (i && b->upper_key <= blocks[i-1].upper_key) ||
            (b->codec != COMPRESS_RAW_PACKED && b->codec != COMPRESS_LZ4 &&
             b->codec != COMPRESS_QPL && b->codec != COMPRESS_ZLIB_ACCEL &&
             b->codec != COMPRESS_ZSTD_EXPERIMENT && !agg_test_copy(b->codec))) return -1;
        offset += b->length; records += b->raw_length / sizeof(struct kv_pair);
    }
    return offset == length && records == h->records ? 0 : -1;
}

static int agg_decode_block(struct bplus_tree_compressed *tree,
                            const struct agg_block *b, const uint8_t *src, uint8_t *dst)
{
    struct simple_leaf_node codec_leaf = {0};
    codec_leaf.compression_algo = (compression_algo_t)b->codec;
    int n;
    if (b->codec == COMPRESS_RAW_PACKED) {
        if (b->length != b->raw_length) return -1;
        memcpy(dst, src, b->raw_length); n = (int)b->raw_length;
    } else {
        int profiled = submission_profile_codec_begin(tree, 0, b->length, b->raw_length, &codec_leaf);
        uint64_t started = split_codec_start();
        n = codec_decompress_bytes(tree, &codec_leaf, src, b->length, dst, b->raw_length);
        n = split_codec_finish(tree, 0, n, started);
        agg_observe_codec(0,b->length,n);
        submission_profile_codec_end(tree, profiled);
        AGG_INC(decompress[agg_origin], 1); AGG_INC(decode_bytes[agg_origin], b->raw_length);
    }
    if (n != (int)b->raw_length || (uint32_t)crc32(0, dst, b->raw_length) != b->crc) return -1;
    struct kv_pair *pairs = (struct kv_pair *)dst;
    size_t count = b->raw_length / sizeof(*pairs);
    for (size_t i = 0; i < count; i++)
        if (!pairs[i].key || (i && pairs[i].key <= pairs[i-1].key)) return -1;
    return pairs[count-1].key == b->upper_key ? n : -1;
}

/* Borrowed immutable frame, held by pending or the coordinator snapshot. This
 * view is created only after every old block has passed decode/CRC/order checks.
 * It never outlives the task and adds no resident leaf metadata. */
struct agg_reuse_view { const uint8_t *frame; uint32_t length, mask; };

static int agg_prepare_reuse(struct agg_reuse_view *view, const uint8_t *frame,
                             uint32_t length, const struct kv_pair *merged, size_t count,
                             const struct kv_pair *updates, size_t n)
{
    memset(view,0,sizeof(*view));
    if (ZIPCACHE_AGG_LAYOUT != 3 || !frame ||
        !env_bool_enabled("BTREE_REUSE_UNCHANGED_BLOCKS",0)) return 0;
    struct agg_header h; struct agg_block old[4];
    if (agg_parse(frame,length,&h,old) || n>ACTIVE_DELTA_ENTRIES+1) return -1;
    /* A PUT-only merge cannot remove keys. Equal counts prove no insertion. */
    if (h.records!=count) return 0;
    size_t per=4096/sizeof(*merged), blocks=(count+per-1)/per, position=0;
    if (h.blocks!=blocks) return 0;
    for (uint32_t i=0;i<h.blocks;i++) {
        size_t take=(count-position+h.blocks-i-1)/(h.blocks-i);
        if (old[i].raw_length!=take*sizeof(*merged) ||
            old[i].upper_key!=merged[position+take-1].key) return 0;
        position+=take;
    }
    uint32_t mask=(1u<<h.blocks)-1u;
    for (size_t j=0;j<n;j++) {
        if (!updates[j].key) return -1;
        uint32_t i=0;
        while(i<h.blocks && updates[j].key>old[i].upper_key)i++;
        if (i==h.blocks) return -1;
        mask&=~(1u<<i); /* First block includes all keys below its upper bound. */
    }
    view->frame=frame;view->length=length;view->mask=mask;
    return 0;
}

static int agg_copy_reused_block(const struct agg_reuse_view *view, uint32_t i,
                                 uint8_t *dst, uint32_t offset, uint32_t cap,
                                 struct agg_block *out)
{
    if (!view || !(view->mask&(1u<<i))) return -1;
    struct agg_block old;
    memcpy(&old,view->frame+sizeof(struct agg_header)+i*sizeof(old),sizeof(old));
    if (old.offset>view->length || old.length>view->length-old.offset ||
        offset>cap || old.length>cap-offset) return -1;
    memcpy(dst+offset,view->frame+old.offset,old.length);
    *out=old;out->offset=offset;
    AGG_INC(reused_encode_blocks,1);AGG_INC(reused_encoded_bytes,old.length);
    return (int)old.length;
}

static int aggregate_encode_records(struct bplus_tree_compressed *tree, struct simple_leaf_node *leaf,
                                     const struct kv_pair *packed, size_t count, uint8_t *dst, uint32_t cap,
                                     const struct agg_reuse_view *reuse)
{
    if (!count || count > COMPRESSED_LEAF_SIZE / sizeof(*packed)) return -1;
    for (size_t i = 0; i < count; i++) {
        WORK_INC(ordered_checks, 1);
        if (!packed[i].key || (i && packed[i-1].key >= packed[i].key)) return -1;
    }
    uint8_t *temporary = malloc(MAX_COMPRESSED_SIZE);
    if (!temporary) return -1;
    int result = -1;
    const size_t per_block = 4096 / sizeof(*packed);
    struct agg_header h = { AGG_MAGIC, 1,
        ZIPCACHE_AGG_LAYOUT == 3 ? (uint32_t)((count+per_block-1)/per_block) : 1,
        (uint32_t)count };
    struct agg_block blocks[4] = {{0}};
    uint32_t offset = sizeof(h) + h.blocks*sizeof(*blocks);
    if (h.blocks > 4 || offset > cap) goto done;
    size_t position = 0;
    for (uint32_t i = 0; i < h.blocks; i++) {
        size_t take = (count-position + h.blocks-i-1)/(h.blocks-i);
        uint32_t bytes = (uint32_t)(take*sizeof(*packed));
        const uint8_t *input = (const uint8_t *)(packed+position);
        if (reuse && (reuse->mask&(1u<<i))) {
            int copied=agg_copy_reused_block(reuse,i,dst,offset,cap,blocks+i);
            if (copied<0) goto done;
            offset+=(uint32_t)copied;position+=take;continue;
        }
        int produced;
        uint32_t codec = leaf->compression_algo;
        if (codec == COMPRESS_RAW_PACKED || codec == COMPRESS_COPY) {
            produced = (int)bytes; memcpy(temporary, input, bytes); codec = COMPRESS_RAW_PACKED;
        } else {
            int profiled = submission_profile_codec_begin(tree, 1, bytes, MAX_COMPRESSED_SIZE, leaf);
            uint64_t started = split_codec_start();
            produced = codec_compress_bytes(tree, leaf, input, bytes, temporary, MAX_COMPRESSED_SIZE);
            produced = split_codec_finish(tree, 1, produced, started);
            agg_observe_codec(1,bytes,produced);
            submission_profile_codec_end(tree, profiled);
            AGG_INC(compress[agg_origin], 1); AGG_INC(encode_bytes[agg_origin], bytes);
            if (produced <= 0) goto done; // Failed encoding is not RAW bypass.
            /* Aggregate allocation rounding is rechecked after all blocks. */
            if (!agg_test_copy(codec) && (uint32_t)produced >= bytes) {
                produced = (int)bytes; memcpy(temporary, input, bytes); codec = COMPRESS_RAW_PACKED;
                AGG_INC(bypass_blocks, 1);
            }
        }
        if ((uint32_t)produced > cap-offset) goto done;
        blocks[i] = (struct agg_block){packed[position+take-1].key, offset, (uint32_t)produced,
            bytes, codec, (uint32_t)crc32(0, input, bytes)};
        memcpy(dst+offset, temporary, (size_t)produced); offset += (uint32_t)produced;
        position += take;
    }
    /* One allocation owns the frame. Compare its usable bytes against the same
     * framed RAW representation; descriptor/format bytes are never free. */
    if (!agg_test_copy(leaf->compression_algo)) {
        size_t raw_size = sizeof(h) + h.blocks*sizeof(*blocks) + count*sizeof(*packed);
        uint8_t *encoded_probe = malloc(offset), *raw_probe = malloc(raw_size);
        if (!encoded_probe || !raw_probe) { free(encoded_probe); free(raw_probe); goto done; }
        int use_raw = allocation_usable_size(encoded_probe, offset) >= allocation_usable_size(raw_probe, raw_size);
        free(encoded_probe); free(raw_probe);
        if (use_raw) {
            offset = sizeof(h) + h.blocks*sizeof(*blocks); position = 0;
            for (uint32_t i = 0; i < h.blocks; i++) {
                if (blocks[i].codec != COMPRESS_RAW_PACKED) AGG_INC(bypass_blocks, 1);
                blocks[i].codec = COMPRESS_RAW_PACKED; blocks[i].offset = offset;
                blocks[i].length = blocks[i].raw_length;
                if (blocks[i].length > cap-offset) goto done;
                memcpy(dst+offset, (uint8_t *)packed+position, blocks[i].length);
                offset += blocks[i].length; position += blocks[i].length;
            }
        }
    }
    memcpy(dst, &h, sizeof(h)); memcpy(dst+sizeof(h), blocks, h.blocks*sizeof(*blocks));
    result = (int)offset;
done:
    free(temporary); return result;
}

static int aggregate_compress(struct bplus_tree_compressed *tree, struct simple_leaf_node *leaf,
                              const uint8_t *src, uint32_t n, uint8_t *dst, uint32_t cap)
{
    struct kv_pair *packed = malloc(n);
    if (!packed) return -1;
    size_t count = 0;
    for (size_t i = 0; i + sizeof(*packed) <= n; i += sizeof(*packed)) {
        WORK_INC(pack_scans, 1);
        struct kv_pair pair; memcpy(&pair, src+i, sizeof(pair));
        if (pair.key) packed[count++] = pair;
    }
    WORK_INC(sort_calls, 1);
    qsort(packed, count, sizeof(*packed), compare_kv_pairs);
    int result = aggregate_encode_records(tree, leaf, packed, count, dst, cap, NULL);
    free(packed); return result;
}

static int aggregate_decompress(struct bplus_tree_compressed *tree, struct simple_leaf_node *leaf,
                                const uint8_t *src, uint32_t n, uint8_t *dst, uint32_t cap)
{
    (void)leaf;
    struct agg_header h; struct agg_block blocks[4];
    if (agg_parse(src, n, &h, blocks) || h.records*sizeof(struct kv_pair) > cap) return -1;
    memset(dst, 0, cap); size_t offset = 0;
    for (uint32_t i = 0; i < h.blocks; i++) {
        if (agg_decode_block(tree, blocks+i, src+blocks[i].offset, dst+offset) < 0) return -1;
        offset += blocks[i].raw_length;
    }
    return (int)cap;
}
#endif
