/* Experimental on-leaf framing. Included in the real tree translation unit so
 * allocations, profiling, locking and failures use exactly the tree's paths.
 * No index or tree is implemented here. Native-endian, process-local format. */
#if ZIPCACHE_AGG_LAYOUT > 0
#define AGG_MAGIC UINT32_C(0x5a414731)
struct agg_header { uint32_t magic, version, blocks, records; };
struct agg_block { int32_t upper_key; uint32_t offset, length, raw_length, codec, crc; };
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
             b->codec != COMPRESS_ZSTD_EXPERIMENT)) return -1;
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

static int aggregate_compress(struct bplus_tree_compressed *tree, struct simple_leaf_node *leaf,
                              const uint8_t *src, uint32_t n, uint8_t *dst, uint32_t cap)
{
    struct kv_pair *packed = malloc(n);
    uint8_t *temporary = malloc(MAX_COMPRESSED_SIZE);
    if (!packed || !temporary) { free(packed); free(temporary); return -1; }
    size_t count = 0;
    for (size_t i = 0; i + sizeof(*packed) <= n; i += sizeof(*packed)) {
        struct kv_pair pair; memcpy(&pair, src+i, sizeof(pair));
        if (pair.key) packed[count++] = pair;
    }
    qsort(packed, count, sizeof(*packed), compare_kv_pairs);
    int result = -1;
    if (!count) goto done;
    for (size_t i = 1; i < count; i++) if (packed[i-1].key == packed[i].key) goto done;
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
        int produced;
        uint32_t codec = leaf->compression_algo;
        if (codec == COMPRESS_RAW_PACKED || codec == COMPRESS_COPY) {
            produced = (int)bytes; memcpy(temporary, input, bytes); codec = COMPRESS_RAW_PACKED;
        } else {
            int profiled = submission_profile_codec_begin(tree, 1, bytes, MAX_COMPRESSED_SIZE, leaf);
            uint64_t started = split_codec_start();
            produced = codec_compress_bytes(tree, leaf, input, bytes, temporary, MAX_COMPRESSED_SIZE);
            produced = split_codec_finish(tree, 1, produced, started);
            submission_profile_codec_end(tree, profiled);
            AGG_INC(compress[agg_origin], 1); AGG_INC(encode_bytes[agg_origin], bytes);
            if (produced <= 0) goto done; // Failed encoding is not RAW bypass.
            /* Aggregate allocation rounding is rechecked after all blocks. */
            if ((uint32_t)produced >= bytes) {
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
    {
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
    free(temporary); free(packed); return result;
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
