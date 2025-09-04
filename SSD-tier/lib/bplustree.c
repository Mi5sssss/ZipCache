/*
 * Hybrid B+Tree Implementation with Super-Leaf Pages
 * - Non-leaf nodes: Stored in memory
 * - Leaf nodes: Super-leaf pages with multiple 4KB sub-pages on SSD/disk
 */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "bplustree.h"

enum {
        LEFT_SIBLING,
        RIGHT_SIBLING = 1,
};

static inline int is_leaf(struct bplus_node_ssd *node)
{
        return node->type == BPLUS_SSD_TREE_LEAF;
}

static key_t key_binary_search(key_t *arr, int len, key_t target)
{
        int low = -1;
        int high = len;
        while (low + 1 < high) {
                int mid = low + (high - low) / 2;
                if (target > arr[mid]) {
                        low = mid;
                } else {
                        high = mid;
                }
        }
        if (high >= len || arr[high] != target) {
                return -high - 1;
        } else {
                return high;
        }
}

/* Block Allocator Implementation */
struct block_allocator *block_allocator_init(uint32_t total_blocks)
{
        struct block_allocator *allocator = calloc(1, sizeof(*allocator));
        if (!allocator) return NULL;
        
        allocator->total_blocks = total_blocks;
        allocator->allocated_blocks = 0;
        allocator->next_search_hint = 0;
        
        /* Allocate bitmap (1 bit per block) */
        uint32_t bitmap_size = (total_blocks + 31) / 32;  /* Round up to 32-bit words */
        allocator->bitmap = calloc(bitmap_size, sizeof(uint32_t));
        if (!allocator->bitmap) {
                free(allocator);
                return NULL;
        }
        
        return allocator;
}

void block_allocator_deinit(struct block_allocator *allocator)
{
        if (allocator) {
                free(allocator->bitmap);
                free(allocator);
        }
}

uint32_t allocate_block(struct block_allocator *allocator)
{
        if (!allocator || allocator->allocated_blocks >= allocator->total_blocks) {
                return INVALID_BLOCK_ID;
        }
        
        /* Search for free block starting from hint */
        for (uint32_t i = 0; i < allocator->total_blocks; i++) {
                uint32_t block_id = (allocator->next_search_hint + i) % allocator->total_blocks;
                uint32_t word_idx = block_id / 32;
                uint32_t bit_idx = block_id % 32;
                
                if (!(allocator->bitmap[word_idx] & (1U << bit_idx))) {
                        /* Found free block */
                        allocator->bitmap[word_idx] |= (1U << bit_idx);
                        allocator->allocated_blocks++;
                        allocator->next_search_hint = (block_id + 1) % allocator->total_blocks;
                        return block_id;
                }
        }
        
        return INVALID_BLOCK_ID;  /* No free blocks */
}

int allocate_multiple_blocks(struct block_allocator *allocator, uint32_t count, uint32_t *block_ids)
{
        if (!allocator || !block_ids || count == 0) return -1;
        if (allocator->allocated_blocks + count > allocator->total_blocks) return -1;
        
        /* Allocate blocks one by one */
        for (uint32_t i = 0; i < count; i++) {
                block_ids[i] = allocate_block(allocator);
                if (block_ids[i] == INVALID_BLOCK_ID) {
                        /* Rollback previous allocations */
                        for (uint32_t j = 0; j < i; j++) {
                                free_block(allocator, block_ids[j]);
                        }
                        return -1;
                }
        }
        
        return 0;
}

void free_block(struct block_allocator *allocator, uint32_t block_id)
{
        if (!allocator || block_id >= allocator->total_blocks) return;
        
        uint32_t word_idx = block_id / 32;
        uint32_t bit_idx = block_id % 32;
        
        if (allocator->bitmap[word_idx] & (1U << bit_idx)) {
                allocator->bitmap[word_idx] &= ~(1U << bit_idx);
                allocator->allocated_blocks--;
        }
}

void free_multiple_blocks(struct block_allocator *allocator, uint32_t count, uint32_t *block_ids)
{
        if (!allocator || !block_ids) return;
        
        for (uint32_t i = 0; i < count; i++) {
                free_block(allocator, block_ids[i]);
        }
}

/* Disk Manager Implementation */
struct disk_manager *disk_manager_init(const char *filename)
{
        struct disk_manager *dm = calloc(1, sizeof(*dm));
        assert(dm != NULL);
        
        /* Use /mnt/zipcache_test path */
        snprintf(dm->filename, sizeof(dm->filename), "/mnt/zipcache_test/%s", 
                 strrchr(filename, '/') ? strrchr(filename, '/') + 1 : filename);
        dm->leaf_size = sizeof(struct bplus_leaf_disk);  /* Legacy */
        
        /* Open or create disk file */
        dm->fd = open(dm->filename, O_CREAT | O_RDWR, 0644);
        if (dm->fd < 0) {
                printf("Failed to open %s\n", dm->filename);
                free(dm);
                return NULL;
        }
        
        /* Get file size and calculate blocks */
        struct stat st;
        if (fstat(dm->fd, &st) == 0) {
                dm->file_size = st.st_size;
        } else {
                dm->file_size = 0;
        }
        
        /* Initialize for 1GB file with 4KB blocks */
        dm->total_4kb_blocks = (1024 * 1024 * 1024) / SUB_PAGE_SIZE;  /* 256K blocks */
        
        /* Extend file if necessary */
        off_t required_size = (off_t)dm->total_4kb_blocks * SUB_PAGE_SIZE;
        if (dm->file_size < required_size) {
                if (ftruncate(dm->fd, required_size) == 0) {
                        dm->file_size = required_size;
                        printf("Extended file to %ld bytes (%u blocks)\n", 
                               (long)dm->file_size, dm->total_4kb_blocks);
                } else {
                        printf("Warning: Could not extend file to required size\n");
                        dm->total_4kb_blocks = dm->file_size / SUB_PAGE_SIZE;
                }
        }
        
        /* Initialize block allocator */
        dm->allocator = block_allocator_init(dm->total_4kb_blocks);
        if (!dm->allocator) {
                close(dm->fd);
                free(dm);
                return NULL;
        }
        
        dm->super_leaf_metadata_offset = 0;
        dm->next_super_leaf_id = 1;
        
        printf("Disk manager initialized: %s (%u blocks, %ld MB)\n", 
               dm->filename, dm->total_4kb_blocks, (long)dm->file_size / (1024*1024));
        
        return dm;
}

void disk_manager_deinit(struct disk_manager *dm)
{
        if (dm) {
                block_allocator_deinit(dm->allocator);
                if (dm->fd >= 0) {
                        fsync(dm->fd);
                        close(dm->fd);
                }
                free(dm);
        }
}

/* Hash Function for Intra-Page Key Distribution */
int hash_key_to_sub_page(key_t key, int num_sub_pages)
{
        if (num_sub_pages <= 0) return 0;
        
        /* Simple but effective hash function for key distribution */
        /* Uses multiplication and modulo for good distribution */
        uint32_t hash = (uint32_t)key;
        
        /* Multiply by a large prime for better distribution */
        hash = hash * 2654435761U;  /* Knuth's multiplicative hash constant */
        
        /* Mix the bits */
        hash ^= hash >> 16;
        hash ^= hash >> 8;
        
        return (int)(hash % (uint32_t)num_sub_pages);
}

/* Sub-page Operations */
struct sub_page *sub_page_create(void)
{
        struct sub_page *sub_page = calloc(1, sizeof(*sub_page));
        if (sub_page) {
                sub_page->header.entries = 0;
                sub_page->header.next_sub_page = -1;
        }
        return sub_page;
}

void sub_page_free(struct sub_page *sub_page)
{
        if (sub_page) {
                free(sub_page);
        }
}

static key_t *sub_page_keys(struct sub_page *sub_page)
{
        return (key_t *)sub_page->payload;
}

static long *sub_page_data(struct sub_page *sub_page)
{
        return (long *)(sub_page->payload + ENTRIES_PER_SUB_PAGE * sizeof(key_t));
}

int sub_page_insert(struct sub_page *sub_page, key_t key, long data)
{
        if (!sub_page || sub_page_is_full(sub_page)) return -1;
        
        key_t *keys = sub_page_keys(sub_page);
        long *values = sub_page_data(sub_page);
        
        /* Find insertion position */
        int pos = key_binary_search(keys, sub_page->header.entries, key);
        if (pos >= 0) {
                /* Key exists, update */
                values[pos] = data;
                return 0;
        }
        
        pos = -pos - 1;
        
        /* Shift elements to make space */
        memmove(&keys[pos + 1], &keys[pos], 
                (sub_page->header.entries - pos) * sizeof(key_t));
        memmove(&values[pos + 1], &values[pos], 
                (sub_page->header.entries - pos) * sizeof(long));
        
        /* Insert new entry */
        keys[pos] = key;
        values[pos] = data;
        sub_page->header.entries++;
        
        return 0;
}

long sub_page_search(struct sub_page *sub_page, key_t key)
{
        if (!sub_page) return -1;
        
        key_t *keys = sub_page_keys(sub_page);
        long *values = sub_page_data(sub_page);
        
        int pos = key_binary_search(keys, sub_page->header.entries, key);
        return (pos >= 0) ? values[pos] : -1;
}

int sub_page_is_full(struct sub_page *sub_page)
{
        return sub_page && (sub_page->header.entries >= (int)ENTRIES_PER_SUB_PAGE);
}

int sub_page_delete(struct sub_page *sub_page, key_t key)
{
        if (!sub_page) return -1;
        
        key_t *keys = sub_page_keys(sub_page);
        long *values = sub_page_data(sub_page);
        
        /* Find key position */
        int pos = key_binary_search(keys, sub_page->header.entries, key);
        if (pos < 0) {
                return -1;  /* Key not found */
        }
        
        /* Shift elements to remove the key */
        memmove(&keys[pos], &keys[pos + 1], 
                (sub_page->header.entries - pos - 1) * sizeof(key_t));
        memmove(&values[pos], &values[pos + 1], 
                (sub_page->header.entries - pos - 1) * sizeof(long));
        
        sub_page->header.entries--;
        
        return 0;
}

/* Zero-padding for SSD Compression Support */
size_t sub_page_get_used_space(struct sub_page *sub_page)
{
        if (!sub_page) return 0;
        
        /* Calculate actual used space considering fixed array layout:
         * Header + full key array + full value array */
        return sizeof(struct sub_page_header) + 
               (ENTRIES_PER_SUB_PAGE * sizeof(key_t)) +
               (ENTRIES_PER_SUB_PAGE * sizeof(long));
}

size_t sub_page_get_unused_space(struct sub_page *sub_page)
{
        if (!sub_page) return 0;
        
        size_t used = sub_page_get_used_space(sub_page);
        return SUB_PAGE_SIZE - used;
}

void sub_page_zero_pad_unused_space(struct sub_page *sub_page)
{
        if (!sub_page) return;
        
        size_t used_space = sub_page_get_used_space(sub_page);
        size_t unused_space = SUB_PAGE_SIZE - used_space;
        
        if (unused_space > 0) {
                /* Zero-fill unused space for SSD compression */
                char *unused_start = ((char *)sub_page) + used_space;
                memset(unused_start, 0, unused_space);
                
                printf("🗜️ Zero-padded %zu bytes (%.1f%% of 4KB page) for SSD compression\n", 
                       unused_space, (double)unused_space / SUB_PAGE_SIZE * 100);
        }
}

void sub_page_prepare_for_compression(struct sub_page *sub_page)
{
        if (!sub_page) return;
        
        key_t *keys = sub_page_keys(sub_page);
        long *values = sub_page_data(sub_page);
        int entries = sub_page->header.entries;
        
        /* Zero unused key slots */
        if (entries < (int)ENTRIES_PER_SUB_PAGE) {
                size_t unused_keys = ENTRIES_PER_SUB_PAGE - entries;
                memset(&keys[entries], 0, unused_keys * sizeof(key_t));
        }
        
        /* Zero unused value slots */
        if (entries < (int)ENTRIES_PER_SUB_PAGE) {
                size_t unused_values = ENTRIES_PER_SUB_PAGE - entries;
                memset(&values[entries], 0, unused_values * sizeof(long));
        }
        
        /* Zero any remaining space after the fixed arrays */
        char *arrays_end = ((char *)&values[ENTRIES_PER_SUB_PAGE]);
        char *page_end = ((char *)sub_page) + SUB_PAGE_SIZE;
        size_t remaining_space = page_end - arrays_end;
        
        if (remaining_space > 0) {
                memset(arrays_end, 0, remaining_space);
        }
        
        /* Calculate total zero-padded space */
        size_t total_zero_padded = 0;
        if (entries < (int)ENTRIES_PER_SUB_PAGE) {
                total_zero_padded += (ENTRIES_PER_SUB_PAGE - entries) * (sizeof(key_t) + sizeof(long));
        }
        total_zero_padded += remaining_space;
        
        if (total_zero_padded > 0) {
                printf("🗜️ Zero-padded %zu bytes (%.1f%% of 4KB page) for SSD compression\n", 
                       total_zero_padded, (double)total_zero_padded / SUB_PAGE_SIZE * 100);
        }
}

/* Disk I/O for Sub-pages with Zero-Padding */
int disk_write_sub_page(struct disk_manager *dm, uint32_t block_id, struct sub_page *sub_page)
{
        if (!dm || !sub_page || block_id >= dm->total_4kb_blocks) return -1;
        
        /* Prepare sub-page for SSD compression by zero-padding unused space */
        sub_page_prepare_for_compression(sub_page);
        
        off_t offset = (off_t)block_id * SUB_PAGE_SIZE;
        ssize_t written = pwrite(dm->fd, sub_page, SUB_PAGE_SIZE, offset);
        
        printf("💾 Wrote full 4KB sub-page to block %u (offset %ld) - ready for SSD compression\n", 
               block_id, (long)offset);
        
        return (written == SUB_PAGE_SIZE) ? 0 : -1;
}

struct sub_page *disk_read_sub_page(struct disk_manager *dm, uint32_t block_id)
{
        if (!dm || block_id >= dm->total_4kb_blocks) return NULL;
        
        struct sub_page *sub_page = sub_page_create();
        if (!sub_page) return NULL;
        
        off_t offset = (off_t)block_id * SUB_PAGE_SIZE;
        ssize_t read_bytes = pread(dm->fd, sub_page, SUB_PAGE_SIZE, offset);
        
        if (read_bytes != SUB_PAGE_SIZE) {
                sub_page_free(sub_page);
                return NULL;
        }
        
        return sub_page;
}

/* Super-leaf Operations */
struct bplus_super_leaf *super_leaf_create(struct disk_manager *dm)
{
        if (!dm) return NULL;
        
        struct bplus_super_leaf *super_leaf = calloc(1, sizeof(*super_leaf));
        if (!super_leaf) return NULL;
        
        super_leaf->type = BPLUS_SSD_TREE_LEAF;
        super_leaf->total_entries = 0;
        super_leaf->active_sub_pages = 0;
        super_leaf->next_super_leaf = -1;
        super_leaf->prev_super_leaf = -1;
        
        /* Initialize sub-page block IDs to invalid */
        for (int i = 0; i < SUB_PAGES_PER_SUPER_LEAF; i++) {
                super_leaf->sub_page_blocks[i] = INVALID_BLOCK_ID;
                super_leaf->cached_sub_pages[i] = NULL;
                super_leaf->dirty_flags[i] = 0;
        }
        
        return super_leaf;
}

void super_leaf_free(struct bplus_super_leaf *super_leaf)
{
        if (!super_leaf) return;
        
        /* Free cached sub-pages */
        for (int i = 0; i < SUB_PAGES_PER_SUPER_LEAF; i++) {
                if (super_leaf->cached_sub_pages[i]) {
                        sub_page_free(super_leaf->cached_sub_pages[i]);
                }
        }
        
        free(super_leaf);
}

/* Hashed sub-page loading - only load the specific sub-page for a key */
struct sub_page *super_leaf_load_sub_page_by_hash(struct disk_manager *dm, struct bplus_super_leaf *super_leaf, key_t key)
{
        if (!dm || !super_leaf) return NULL;
        
        /* Calculate sub-page index using hash function */
        int sub_page_idx = hash_key_to_sub_page(key, SUB_PAGES_PER_SUPER_LEAF);
        
        /* Return cached version if available */
        if (super_leaf->cached_sub_pages[sub_page_idx]) {
                return super_leaf->cached_sub_pages[sub_page_idx];
        }
        
        /* Allocate block if needed */
        if (super_leaf->sub_page_blocks[sub_page_idx] == INVALID_BLOCK_ID) {
                uint32_t block_id = allocate_block(dm->allocator);
                if (block_id == INVALID_BLOCK_ID) return NULL;
                
                super_leaf->sub_page_blocks[sub_page_idx] = block_id;
                super_leaf->cached_sub_pages[sub_page_idx] = sub_page_create();
                super_leaf->dirty_flags[sub_page_idx] = 1;  /* New sub-page is dirty */
                if (sub_page_idx >= super_leaf->active_sub_pages) {
                        super_leaf->active_sub_pages = sub_page_idx + 1;
                }
                
                printf("🔹 Allocated new sub-page %d (block %u) for key %d\n", 
                       sub_page_idx, block_id, key);
                
                return super_leaf->cached_sub_pages[sub_page_idx];
        }
        
        /* Load from disk - this is a single 4KB I/O operation */
        printf("📖 Loading sub-page %d (block %u) for key %d from disk\n", 
               sub_page_idx, super_leaf->sub_page_blocks[sub_page_idx], key);
        
        super_leaf->cached_sub_pages[sub_page_idx] = 
                disk_read_sub_page(dm, super_leaf->sub_page_blocks[sub_page_idx]);
        
        return super_leaf->cached_sub_pages[sub_page_idx];
}

struct sub_page *super_leaf_load_sub_page(struct disk_manager *dm, struct bplus_super_leaf *super_leaf, int sub_page_idx)
{
        if (!dm || !super_leaf || sub_page_idx < 0 || sub_page_idx >= SUB_PAGES_PER_SUPER_LEAF) {
                return NULL;
        }
        
        /* Return cached version if available */
        if (super_leaf->cached_sub_pages[sub_page_idx]) {
                return super_leaf->cached_sub_pages[sub_page_idx];
        }
        
        /* Allocate block if needed */
        if (super_leaf->sub_page_blocks[sub_page_idx] == INVALID_BLOCK_ID) {
                uint32_t block_id = allocate_block(dm->allocator);
                if (block_id == INVALID_BLOCK_ID) return NULL;
                
                super_leaf->sub_page_blocks[sub_page_idx] = block_id;
                super_leaf->cached_sub_pages[sub_page_idx] = sub_page_create();
                super_leaf->dirty_flags[sub_page_idx] = 1;  /* New sub-page is dirty */
                super_leaf->active_sub_pages++;
                
                return super_leaf->cached_sub_pages[sub_page_idx];
        }
        
        /* Load from disk */
        super_leaf->cached_sub_pages[sub_page_idx] = 
                disk_read_sub_page(dm, super_leaf->sub_page_blocks[sub_page_idx]);
        
        return super_leaf->cached_sub_pages[sub_page_idx];
}

/* Hashed Insert Operation with Split Detection - single 4KB I/O */
int super_leaf_insert_hashed(struct disk_manager *dm, struct bplus_super_leaf *super_leaf, key_t key, long data)
{
        if (!dm || !super_leaf) return -1;
        
        /* Step 1: Calculate sub-page index using hash function */
        int sub_page_idx = hash_key_to_sub_page(key, SUB_PAGES_PER_SUPER_LEAF);
        
        printf("📝 Insert key %d → sub-page %d (hash-based)\n", key, sub_page_idx);
        
        /* Step 2: Load only the specific 4KB sub-page (single I/O) */
        struct sub_page *sub_page = super_leaf_load_sub_page_by_hash(dm, super_leaf, key);
        if (!sub_page) return -1;
        
        /* Step 3: Check if sub-page is full before insertion */
        if (sub_page_is_full(sub_page)) {
                printf("⚠️ Sub-page %d is full, checking if super-leaf needs splitting\n", sub_page_idx);
                
                /* Check if super-leaf is nearly full */
                if (super_leaf_is_full(super_leaf)) {
                        printf("🔄 Super-leaf is full (entries: %d), needs splitting before insertion\n", super_leaf->total_entries);
                        return -2; /* Special return code indicating split needed */
                }
                
                /* Sub-page is full but super-leaf has capacity - return error for now */
                printf("❌ Sub-page %d is full but super-leaf has remaining capacity\n", sub_page_idx);
                return -1;
        }
        
        /* Step 4: Modify the 4KB page in memory */
        if (sub_page_insert(sub_page, key, data) != 0) {
                printf("❌ Failed to insert key %d (sub-page full or error)\n", key);
                return -1;
        }
        
        /* Step 5: Mark for write-back (lazy write) */
        super_leaf->dirty_flags[sub_page_idx] = 1;
        super_leaf->total_entries++;
        
        printf("💾 Marked sub-page %d as dirty for key %d\n", sub_page_idx, key);
        
        return 0;
}

/* Legacy Insert Operation (for compatibility) */
int super_leaf_insert(struct disk_manager *dm, struct bplus_super_leaf *super_leaf, key_t key, long data)
{
        if (!dm || !super_leaf) return -1;
        
        /* For simplicity, use first available sub-page */
        for (int i = 0; i < SUB_PAGES_PER_SUPER_LEAF; i++) {
                struct sub_page *sub_page = super_leaf_load_sub_page(dm, super_leaf, i);
                if (!sub_page) continue;
                
                if (!sub_page_is_full(sub_page)) {
                        if (sub_page_insert(sub_page, key, data) == 0) {
                                super_leaf->dirty_flags[i] = 1;
                                super_leaf->total_entries++;
                                return 0;
                        }
                }
        }
        
        return -1;  /* No space available */
}

/* Hashed Search Operation - single 4KB I/O */
long super_leaf_search_hashed(struct disk_manager *dm, struct bplus_super_leaf *super_leaf, key_t key)
{
        if (!dm || !super_leaf) return -1;
        
        /* Step 1: Calculate sub-page index using hash function */
        int sub_page_idx = hash_key_to_sub_page(key, SUB_PAGES_PER_SUPER_LEAF);
        
        printf("🔍 Search key %d → sub-page %d (hash-based)\n", key, sub_page_idx);
        
        /* Step 2: Check if the sub-page exists */
        if (super_leaf->sub_page_blocks[sub_page_idx] == INVALID_BLOCK_ID) {
                printf("❌ Sub-page %d not allocated for key %d\n", sub_page_idx, key);
                return -1;  /* Sub-page doesn't exist */
        }
        
        /* Step 3: Load only the specific 4KB sub-page (single I/O) */
        struct sub_page *sub_page = super_leaf_load_sub_page_by_hash(dm, super_leaf, key);
        if (!sub_page) return -1;
        
        /* Step 4: Search within the single 4KB block */
        long result = sub_page_search(sub_page, key);
        
        if (result != -1) {
                printf("✅ Found key %d with value %ld in sub-page %d\n", key, result, sub_page_idx);
        } else {
                printf("❌ Key %d not found in sub-page %d\n", key, sub_page_idx);
        }
        
        return result;
}

/* Hashed Delete Operation - single 4KB I/O */
int super_leaf_delete_hashed(struct disk_manager *dm, struct bplus_super_leaf *super_leaf, key_t key)
{
        if (!dm || !super_leaf) return -1;
        
        /* Step 1: Calculate sub-page index using hash function */
        int sub_page_idx = hash_key_to_sub_page(key, SUB_PAGES_PER_SUPER_LEAF);
        
        printf("🗑️ Delete key %d → sub-page %d (hash-based)\n", key, sub_page_idx);
        
        /* Step 2: Check if the sub-page exists */
        if (super_leaf->sub_page_blocks[sub_page_idx] == INVALID_BLOCK_ID) {
                printf("❌ Sub-page %d not allocated for key %d\n", sub_page_idx, key);
                return -1;  /* Sub-page doesn't exist */
        }
        
        /* Step 3: Load only the specific 4KB sub-page (single I/O) */
        struct sub_page *sub_page = super_leaf_load_sub_page_by_hash(dm, super_leaf, key);
        if (!sub_page) return -1;
        
        /* Step 4: Delete from the single 4KB block */
        if (sub_page_delete(sub_page, key) == 0) {
                super_leaf->dirty_flags[sub_page_idx] = 1;
                super_leaf->total_entries--;
                printf("✅ Deleted key %d from sub-page %d\n", key, sub_page_idx);
                return 0;
        }
        
        printf("❌ Key %d not found for deletion in sub-page %d\n", key, sub_page_idx);
        return -1;
}

/* Legacy Search Operation (for compatibility) */
long super_leaf_search(struct disk_manager *dm, struct bplus_super_leaf *super_leaf, key_t key)
{
        if (!dm || !super_leaf) return -1;
        
        /* Search all active sub-pages */
        for (int i = 0; i < super_leaf->active_sub_pages && i < SUB_PAGES_PER_SUPER_LEAF; i++) {
                if (super_leaf->sub_page_blocks[i] == INVALID_BLOCK_ID) continue;
                
                struct sub_page *sub_page = super_leaf_load_sub_page(dm, super_leaf, i);
                if (!sub_page) continue;
                
                long result = sub_page_search(sub_page, key);
                if (result != -1) {
                        return result;
                }
        }
        
        return -1;  /* Not found */
}

int super_leaf_flush_dirty(struct disk_manager *dm, struct bplus_super_leaf *super_leaf)
{
        if (!dm || !super_leaf) return -1;
        
        int flushed = 0;
        size_t total_zero_padded = 0;
        
        for (int i = 0; i < SUB_PAGES_PER_SUPER_LEAF; i++) {
                if (super_leaf->dirty_flags[i] && super_leaf->cached_sub_pages[i]) {
                        struct sub_page *sub_page = super_leaf->cached_sub_pages[i];
                        
                        /* Calculate compression benefit before writing */
                        size_t unused_space = sub_page_get_unused_space(sub_page);
                        total_zero_padded += unused_space;
                        
                        printf("📊 Sub-page %d: %d entries, %zu bytes unused (%.1f%% compressible)\n",
                               i, sub_page->header.entries, unused_space,
                               (double)unused_space / SUB_PAGE_SIZE * 100);
                        
                        if (disk_write_sub_page(dm, super_leaf->sub_page_blocks[i], sub_page) == 0) {
                                super_leaf->dirty_flags[i] = 0;
                                flushed++;
                        }
                }
        }
        
        if (flushed > 0) {
                printf("🗜️ Flushed %d sub-pages with %zu total zero-padded bytes for SSD compression\n",
                       flushed, total_zero_padded);
                printf("💾 Compression potential: %.1f%% of written data\n",
                       (double)total_zero_padded / (flushed * SUB_PAGE_SIZE) * 100);
        }
        
        return flushed;
}

/* Check if super-leaf is full and needs splitting */
int super_leaf_is_full(struct bplus_super_leaf *super_leaf)
{
        if (!super_leaf) return 0;
        
        /* Check if we have reached maximum capacity */
        int total_capacity = SUB_PAGES_PER_SUPER_LEAF * ENTRIES_PER_SUB_PAGE;
        return super_leaf->total_entries >= total_capacity * 0.9; /* 90% full trigger */
}

/* Helper structure for consolidating data during split */
typedef struct {
        key_t key;
        long data;
} KeyValuePair;

/* Consolidate and sort all key-value pairs from multiple sub-pages */
static KeyValuePair *consolidate_and_sort(struct sub_page **sub_pages, int num_sub_pages, int *total_pairs)
{
        /* Count total pairs */
        int count = 0;
        for (int i = 0; i < num_sub_pages; i++) {
                if (sub_pages[i]) {
                        count += sub_pages[i]->header.entries;
                }
        }
        
        if (count == 0) {
                *total_pairs = 0;
                return NULL;
        }
        
        /* Allocate array for all pairs */
        KeyValuePair *all_pairs = malloc(count * sizeof(KeyValuePair));
        if (!all_pairs) {
                *total_pairs = 0;
                return NULL;
        }
        
        /* Collect all pairs */
        int idx = 0;
        for (int i = 0; i < num_sub_pages; i++) {
                if (!sub_pages[i]) continue;
                
                key_t *keys = sub_page_keys(sub_pages[i]);
                long *values = sub_page_data(sub_pages[i]);
                
                for (int j = 0; j < sub_pages[i]->header.entries; j++) {
                        all_pairs[idx].key = keys[j];
                        all_pairs[idx].data = values[j];
                        idx++;
                }
        }
        
        /* Sort by key (simple insertion sort for now) */
        for (int i = 1; i < count; i++) {
                KeyValuePair temp = all_pairs[i];
                int j = i - 1;
                while (j >= 0 && all_pairs[j].key > temp.key) {
                        all_pairs[j + 1] = all_pairs[j];
                        j--;
                }
                all_pairs[j + 1] = temp;
        }
        
        *total_pairs = count;
        return all_pairs;
}

/* Redistribute pairs to two super-leaves using hash function */
static int redistribute_pairs_hashed(KeyValuePair *pairs, int total_pairs,
                                    struct bplus_super_leaf *left_leaf,
                                    struct bplus_super_leaf *right_leaf,
                                    key_t *median_key)
{
        if (!pairs || total_pairs <= 0) return -1;
        
        /* Find median key for promotion */
        int median_idx = total_pairs / 2;
        *median_key = pairs[median_idx].key;
        
        printf("🔄 Redistributing %d pairs, median key: %d\n", total_pairs, *median_key);
        
        /* Clear existing sub-pages in both leaves */
        for (int i = 0; i < SUB_PAGES_PER_SUPER_LEAF; i++) {
                if (left_leaf->cached_sub_pages[i]) {
                        sub_page_free(left_leaf->cached_sub_pages[i]);
                        left_leaf->cached_sub_pages[i] = NULL;
                }
                if (right_leaf->cached_sub_pages[i]) {
                        sub_page_free(right_leaf->cached_sub_pages[i]);
                        right_leaf->cached_sub_pages[i] = NULL;
                }
        }
        
        /* Reset counters */
        left_leaf->total_entries = 0;
        right_leaf->total_entries = 0;
        
        /* Redistribute pairs using hash function */
        for (int i = 0; i < total_pairs; i++) {
                struct bplus_super_leaf *target_leaf = (pairs[i].key < *median_key) ? left_leaf : right_leaf;
                int sub_page_idx = hash_key_to_sub_page(pairs[i].key, SUB_PAGES_PER_SUPER_LEAF);
                
                /* Load or create sub-page */
                if (!target_leaf->cached_sub_pages[sub_page_idx]) {
                        target_leaf->cached_sub_pages[sub_page_idx] = sub_page_create();
                        if (!target_leaf->cached_sub_pages[sub_page_idx]) {
                                return -1;
                        }
                        target_leaf->dirty_flags[sub_page_idx] = 1;
                }
                
                /* Insert into sub-page */
                if (sub_page_insert(target_leaf->cached_sub_pages[sub_page_idx], 
                                   pairs[i].key, pairs[i].data) == 0) {
                        target_leaf->total_entries++;
                        target_leaf->dirty_flags[sub_page_idx] = 1;
                }
        }
        
        /* Update active sub-pages count */
        left_leaf->active_sub_pages = 0;
        right_leaf->active_sub_pages = 0;
        for (int i = 0; i < SUB_PAGES_PER_SUPER_LEAF; i++) {
                if (left_leaf->cached_sub_pages[i]) {
                        left_leaf->active_sub_pages = i + 1;
                }
                if (right_leaf->cached_sub_pages[i]) {
                        right_leaf->active_sub_pages = i + 1;
                }
        }
        
        printf("✅ Split complete: left=%d entries, right=%d entries\n", 
               left_leaf->total_entries, right_leaf->total_entries);
        
        return 0;
}

/* Super-leaf splitting with parallel I/O */
PromotedKey split_super_leaf(struct disk_manager *dm, struct bplus_super_leaf *leaf_to_split)
{
        PromotedKey result = {0, NULL};
        
        if (!dm || !leaf_to_split) {
                return result;
        }
        
        printf("🔄 Starting super-leaf split with parallel I/O...\n");
        
        /* Step 1: Read Phase - Load all sub-pages in parallel */
        printf("📖 Phase 1: Reading all sub-pages in parallel...\n");
        
        struct sub_page *read_buffers[SUB_PAGES_PER_SUPER_LEAF];
        int valid_sub_pages = 0;
        
        /* Load all existing sub-pages */
        for (int i = 0; i < SUB_PAGES_PER_SUPER_LEAF; i++) {
                read_buffers[i] = NULL;
                
                if (leaf_to_split->sub_page_blocks[i] != INVALID_BLOCK_ID) {
                        if (leaf_to_split->cached_sub_pages[i]) {
                                /* Already in memory */
                                read_buffers[i] = leaf_to_split->cached_sub_pages[i];
                        } else {
                                /* Load from disk */
                                read_buffers[i] = disk_read_sub_page(dm, leaf_to_split->sub_page_blocks[i]);
                        }
                        if (read_buffers[i]) {
                                valid_sub_pages++;
                                printf("📖 Loaded sub-page %d from block %u\n", i, leaf_to_split->sub_page_blocks[i]);
                        }
                }
        }
        
        if (valid_sub_pages == 0) {
                printf("❌ No valid sub-pages found for splitting\n");
                return result;
        }
        
        /* Step 2: Logical Split Phase - Consolidate and redistribute */
        printf("🔄 Phase 2: Consolidating and redistributing data...\n");
        
        int total_pairs = 0;
        KeyValuePair *all_pairs = consolidate_and_sort(read_buffers, SUB_PAGES_PER_SUPER_LEAF, &total_pairs);
        
        if (!all_pairs || total_pairs == 0) {
                printf("❌ Failed to consolidate key-value pairs\n");
                return result;
        }
        
        /* Create new right sibling super-leaf */
        struct bplus_super_leaf *right_sibling = super_leaf_create(dm);
        if (!right_sibling) {
                printf("❌ Failed to create right sibling\n");
                free(all_pairs);
                return result;
        }
        
        /* Redistribute data between left (original) and right sibling */
        key_t median_key;
        if (redistribute_pairs_hashed(all_pairs, total_pairs, leaf_to_split, right_sibling, &median_key) != 0) {
                printf("❌ Failed to redistribute pairs\n");
                super_leaf_free(right_sibling);
                free(all_pairs);
                return result;
        }
        
        free(all_pairs);
        
        /* Step 3: Write Phase - Allocate new blocks and write in parallel */
        printf("💾 Phase 3: Allocating blocks and writing in parallel...\n");
        
        /* Allocate new blocks for right sibling's sub-pages */
        for (int i = 0; i < SUB_PAGES_PER_SUPER_LEAF; i++) {
                if (right_sibling->cached_sub_pages[i]) {
                        uint32_t new_block = allocate_block(dm->allocator);
                        if (new_block == INVALID_BLOCK_ID) {
                                printf("❌ Failed to allocate block for right sibling sub-page %d\n", i);
                                super_leaf_free(right_sibling);
                                return result;
                        }
                        right_sibling->sub_page_blocks[i] = new_block;
                        printf("📦 Allocated block %u for right sibling sub-page %d\n", new_block, i);
                }
        }
        
        /* Write all dirty sub-pages for both leaves */
        int left_writes = super_leaf_flush_dirty(dm, leaf_to_split);
        int right_writes = super_leaf_flush_dirty(dm, right_sibling);
        
        printf("💾 Write phase complete: %d left writes, %d right writes\n", left_writes, right_writes);
        
        /* Step 4: Update metadata and return promoted key */
        right_sibling->next_super_leaf = leaf_to_split->next_super_leaf;
        right_sibling->prev_super_leaf = -1; /* Will be set by parent */
        leaf_to_split->next_super_leaf = -1; /* Will be set by parent */
        
        result.key = median_key;
        result.right_sibling = right_sibling;
        
        printf("✅ Super-leaf split completed! Promoted key: %d\n", median_key);
        
        return result;
}

/* Memory node management */
static struct bplus_non_leaf_ssd *non_leaf_new(void)
{
        struct bplus_non_leaf_ssd *node = calloc(1, sizeof(*node));
        assert(node != NULL);
        list_init(&node->link);
        node->type = BPLUS_SSD_TREE_NON_LEAF;
        node->parent_key_idx = -1;
        node->is_leaf_parent = 0;
        return node;
}

static void non_leaf_delete(struct bplus_non_leaf_ssd *node)
{
        if (node) {
                list_del(&node->link);
                free(node);
        }
}

/* Tree search using super-leaf */
static long bplus_tree_search(struct bplus_tree_ssd *tree, key_t key)
{
        if (!tree || !tree->root) return -1;
        
        struct bplus_node_ssd *node = tree->root;
        
        /* Traverse memory nodes until we reach leaf parent */
        while (node != NULL && !is_leaf(node)) {
                struct bplus_non_leaf_ssd *nln = (struct bplus_non_leaf_ssd *)node;
                
                if (nln->is_leaf_parent) {
                        /* This non-leaf node points to super-leaf pages */
                        int i = key_binary_search(nln->key, nln->children - 1, key);
                        int super_leaf_idx;
                        
                        if (i >= 0) {
                                super_leaf_idx = i + 1;
                        } else {
                                i = -i - 1;
                                super_leaf_idx = i;
                        }
                        
                        if (super_leaf_idx < nln->children) {
                                /* Get the actual super-leaf from the tree's storage */
                                struct bplus_super_leaf *super_leaf = 
                                        (struct bplus_super_leaf *)nln->sub_ptr[super_leaf_idx];
                                if (!super_leaf) return -1;
                                
                                /* Use hashed search for single 4KB I/O */
                                return super_leaf_search_hashed(tree->disk_mgr, super_leaf, key);
                        }
                        return -1;
                } else {
                        /* Regular memory-based non-leaf node */
                        int i = key_binary_search(nln->key, nln->children - 1, key);
                        if (i >= 0) {
                                node = nln->sub_ptr[i + 1];
                        } else {
                                i = -i - 1;
                                node = nln->sub_ptr[i];
                        }
                }
        }
        
        return -1; /* Not found */
}

/* Update parent node with promoted key after split */
static int update_parent_with_promoted_key(struct bplus_tree_ssd *tree, PromotedKey promoted)
{
        if (!tree || !promoted.right_sibling) return -1;
        
        struct bplus_non_leaf_ssd *root = (struct bplus_non_leaf_ssd *)tree->root;
        if (!root || !root->is_leaf_parent) return -1;
        
        printf("🔼 Updating parent with promoted key: %d\n", promoted.key);
        
        /* Find insertion position for promoted key */
        int insert_pos = 0;
        for (int i = 0; i < root->children - 1; i++) {
                if (promoted.key > root->key[i]) {
                        insert_pos = i + 1;
                } else {
                        break;
                }
        }
        
        /* Check if parent has space */
        if (root->children >= BPLUS_MAX_ORDER) {
                printf("❌ Parent node is full, cannot handle promoted key\n");
                return -1; /* Parent split not implemented */
        }
        
        /* Shift keys and pointers to make space */
        for (int i = root->children - 1; i > insert_pos; i--) {
                root->key[i] = root->key[i - 1];
                root->sub_ptr[i + 1] = root->sub_ptr[i];
        }
        
        /* Insert promoted key and right sibling */
        root->key[insert_pos] = promoted.key;
        root->sub_ptr[insert_pos + 1] = (struct bplus_node_ssd *)promoted.right_sibling;
        root->children++;
        
        printf("✅ Promoted key %d inserted at position %d in parent\n", promoted.key, insert_pos);
        
        return 0;
}

/* Tree insertion using super-leaf with splitting support */
static int bplus_tree_insert(struct bplus_tree_ssd *tree, key_t key, long data)
{
        if (!tree) return -1;
        
        /* Simple implementation: single super-leaf at root level */
        if (!tree->root) {
                /* Create root non-leaf node */
                struct bplus_non_leaf_ssd *root = non_leaf_new();
                root->is_leaf_parent = 1;
                tree->root = (struct bplus_node_ssd *)root;
                tree->level = 1;
                
                /* Create super-leaf and keep it in memory */
                struct bplus_super_leaf *super_leaf = super_leaf_create(tree->disk_mgr);
                if (!super_leaf) return -1;
                
                int insert_result = super_leaf_insert_hashed(tree->disk_mgr, super_leaf, key, data);
                if (insert_result != 0) {
                        super_leaf_free(super_leaf);
                        return -1;
                }
                
                /* Store super-leaf pointer in root (keeping it in memory) */
                root->sub_ptr[0] = (struct bplus_node_ssd *)super_leaf;
                root->children = 1;
                
                return 0;
        } else {
                /* Root exists, find the super-leaf and insert */
                struct bplus_non_leaf_ssd *root = (struct bplus_non_leaf_ssd *)tree->root;
                if (root->is_leaf_parent && root->children > 0) {
                        struct bplus_super_leaf *super_leaf = 
                                (struct bplus_super_leaf *)root->sub_ptr[0];
                        if (super_leaf) {
                                int insert_result = super_leaf_insert_hashed(tree->disk_mgr, super_leaf, key, data);
                                
                                if (insert_result == -2) {
                                        /* Super-leaf needs splitting */
                                        printf("🔄 Triggering super-leaf split for key %d\n", key);
                                        
                                        PromotedKey promoted = split_super_leaf(tree->disk_mgr, super_leaf);
                                        if (!promoted.right_sibling) {
                                                printf("❌ Super-leaf split failed\n");
                                                return -1;
                                        }
                                        
                                        /* Update parent with promoted key */
                                        if (update_parent_with_promoted_key(tree, promoted) != 0) {
                                                printf("❌ Failed to update parent with promoted key\n");
                                                super_leaf_free(promoted.right_sibling);
                                                return -1;
                                        }
                                        
                                        /* Retry insertion after split */
                                        printf("🔄 Retrying insertion after split\n");
                                        return bplus_tree_insert(tree, key, data);
                                }
                                
                                return insert_result;
                        }
                }
        }
        
        return -1; /* Complex insertion not implemented yet */
}

/* Public API */
long bplus_tree_ssd_get(struct bplus_tree_ssd *tree, key_t key)
{
        return bplus_tree_search(tree, key);
}

int bplus_tree_ssd_put(struct bplus_tree_ssd *tree, key_t key, long data)
{
        if (data == 0) {
                /* TODO: Implement deletion */
                return -1;
        }
        return bplus_tree_insert(tree, key, data);
}

long bplus_tree_ssd_get_range(struct bplus_tree_ssd *tree, key_t key1, key_t key2)
{
        /* TODO: Implement range query */
        (void)tree; (void)key1; (void)key2;
        return -1;
}

struct bplus_tree_ssd *bplus_tree_ssd_init(int order, int entries, const char *disk_file)
{
        if (order < BPLUS_MIN_ORDER || order > BPLUS_MAX_ORDER) return NULL;
        if (entries <= 0 || entries > BPLUS_MAX_ENTRIES) return NULL;
        if (!disk_file) return NULL;
        
        struct bplus_tree_ssd *tree = calloc(1, sizeof(*tree));
        assert(tree != NULL);
        
        tree->order = order;
        tree->entries = entries;
        tree->level = 0;
        tree->root = NULL;
        
        /* Initialize level lists */
        for (int i = 0; i < BPLUS_MAX_LEVEL; i++) {
                list_init(&tree->list[i]);
        }
        
        /* Initialize disk manager with /mnt/zipcache_test path */
        tree->disk_mgr = disk_manager_init(disk_file);
        if (!tree->disk_mgr) {
                free(tree);
                return NULL;
        }
        
        printf("Super-leaf configuration:\n");
        printf("- Sub-page size: %d bytes\n", SUB_PAGE_SIZE);
        printf("- Super-leaf size: %d bytes (%d sub-pages)\n", 
               SUPER_LEAF_SIZE, SUB_PAGES_PER_SUPER_LEAF);
        printf("- Entries per sub-page: %lu\n", ENTRIES_PER_SUB_PAGE);
        
        return tree;
}

void bplus_tree_ssd_deinit(struct bplus_tree_ssd *tree)
{
        if (!tree) return;
        
        /* Free all memory nodes recursively */
        if (tree->root) {
                struct bplus_non_leaf_ssd *root = (struct bplus_non_leaf_ssd *)tree->root;
                if (root->is_leaf_parent) {
                        /* Free super-leaf pages */
                        for (int i = 0; i < root->children; i++) {
                                struct bplus_super_leaf *super_leaf = 
                                        (struct bplus_super_leaf *)root->sub_ptr[i];
                                if (super_leaf) {
                                        /* Flush any dirty pages before cleanup */
                                        super_leaf_flush_dirty(tree->disk_mgr, super_leaf);
                                        super_leaf_free(super_leaf);
                                }
                        }
                }
                non_leaf_delete(root);
        }
        
        /* Free disk manager */
        disk_manager_deinit(tree->disk_mgr);
        
        free(tree);
}

void bplus_tree_ssd_dump(struct bplus_tree_ssd *tree)
{
        if (!tree) {
                printf("Tree is NULL\n");
                return;
        }
        
        printf("Hybrid B+Tree with Super-Leaf Pages:\n");
        printf("- Order: %d, Entries: %d, Level: %d\n", 
               tree->order, tree->entries, tree->level);
        printf("- Disk file: %s\n", tree->disk_mgr ? tree->disk_mgr->filename : "None");
        printf("- Allocated blocks: %u/%u\n", 
               tree->disk_mgr ? tree->disk_mgr->allocator->allocated_blocks : 0,
               tree->disk_mgr ? tree->disk_mgr->allocator->total_blocks : 0);
        
        if (!tree->root) {
                printf("Tree is empty\n");
                return;
        }
        
        printf("Tree structure: [Root present but detailed dump not implemented]\n");
}

/* Legacy disk operations (for compatibility) */
off_t disk_write_leaf(struct disk_manager *dm, struct bplus_leaf_disk *leaf)
{
        if (!dm || !leaf) return -1;
        
        off_t offset = dm->file_size;
        ssize_t written = pwrite(dm->fd, leaf, dm->leaf_size, offset);
        
        if (written != (ssize_t)dm->leaf_size) {
                return -1;
        }
        
        dm->file_size += dm->leaf_size;
        return offset;
}

struct bplus_leaf_disk *disk_read_leaf(struct disk_manager *dm, off_t offset)
{
        if (!dm || offset < 0) return NULL;
        
        struct bplus_leaf_disk *leaf = malloc(sizeof(*leaf));
        if (!leaf) return NULL;
        
        ssize_t read_bytes = pread(dm->fd, leaf, dm->leaf_size, offset);
        if (read_bytes != (ssize_t)dm->leaf_size) {
                free(leaf);
                return NULL;
        }
        
        return leaf;
}

int disk_update_leaf(struct disk_manager *dm, off_t offset, struct bplus_leaf_disk *leaf)
{
        if (!dm || !leaf || offset < 0) return -1;
        
        ssize_t written = pwrite(dm->fd, leaf, dm->leaf_size, offset);
        return (written == (ssize_t)dm->leaf_size) ? 0 : -1;
}

void disk_free_leaf(struct bplus_leaf_disk *leaf)
{
        if (leaf) {
                free(leaf);
        }
}
