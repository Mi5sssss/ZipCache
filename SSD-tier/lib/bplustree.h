/*
 * Hybrid B+Tree Implementation
 * - Non-leaf nodes: Stored in memory
 * - Leaf nodes: Stored on SSD/disk
 * Based on DRAM-tier implementation
 */

#ifndef _BPLUS_TREE_SSD_H
#define _BPLUS_TREE_SSD_H

#include <sys/types.h>
#include <stdint.h>

#define BPLUS_MIN_ORDER     3
#define BPLUS_MAX_ORDER     64
#define BPLUS_MAX_ENTRIES   64
#define BPLUS_MAX_LEVEL     10

/* Super-Leaf Page Configuration */
#define SUB_PAGE_SIZE       4096        /* 4KB sub-page size */
#define SUPER_LEAF_SIZE     65536       /* 64KB logical super-leaf size */
#define SUB_PAGES_PER_SUPER_LEAF (SUPER_LEAF_SIZE / SUB_PAGE_SIZE)  /* 16 sub-pages */
#define ENTRIES_PER_SUB_PAGE ((SUB_PAGE_SIZE - sizeof(struct sub_page_header)) / (sizeof(key_t) + sizeof(long)))

/* Block allocation constants */
#define INVALID_BLOCK_ID    0xFFFFFFFF

typedef int key_t;

#ifndef BPLUSTREE_LIST_ONCE
#define BPLUSTREE_LIST_ONCE
struct list_head {
        struct list_head *prev, *next;
};

static inline void list_init(struct list_head *link)
{
        link->prev = link;
        link->next = link;
}

static inline void
__list_add(struct list_head *link, struct list_head *prev, struct list_head *next)
{
        link->next = next;
        link->prev = prev;
        next->prev = link;
        prev->next = link;
}

static inline void __list_del(struct list_head *prev, struct list_head *next)
{
        prev->next = next;
        next->prev = prev;
}

static inline void list_add(struct list_head *link, struct list_head *prev)
{
        __list_add(link, prev, prev->next);
}

static inline void list_add_tail(struct list_head *link, struct list_head *head)
{
	__list_add(link, head->prev, head);
}

static inline void list_del(struct list_head *link)
{
        __list_del(link->prev, link->next);
        list_init(link);
}

static inline int list_is_first(struct list_head *link, struct list_head *head)
{
	return link->prev == head;
}

static inline int list_is_last(struct list_head *link, struct list_head *head)
{
	return link->next == head;
}

#define list_entry(ptr, type, member) \
        ((type *)((char *)(ptr) - (size_t)(&((type *)0)->member)))

#define list_next_entry(pos, member) \
	list_entry((pos)->member.next, __typeof__(*(pos)), member)

#define list_prev_entry(pos, member) \
	list_entry((pos)->member.prev, __typeof__(*(pos)), member)

#define list_for_each_safe(pos, n, head) \
        for (pos = (head)->next, n = pos->next; pos != (head); \
                pos = n, n = pos->next)
#endif /* BPLUSTREE_LIST_ONCE */

/**
 * Hybrid B+tree basic node (base structure)
 */ 
struct bplus_node_ssd {
        int type;               /* BPLUS_TREE_LEAF or BPLUS_TREE_NON_LEAF */
        int parent_key_idx;     /* index in parent node */
        struct bplus_non_leaf_ssd *parent;
        struct list_head link;
        int count;
};

/**
 * Non-leaf (internal) node - stored in memory
 * Contains keys and pointers to child nodes or disk offsets to leaf nodes
 */
struct bplus_non_leaf_ssd {
        int type;               /* BPLUS_TREE_NON_LEAF */
        int parent_key_idx;
        struct bplus_non_leaf_ssd *parent;
        struct list_head link;
        int children;           /* number of child pointers */
        int key[BPLUS_MAX_ORDER - 1];
        union {
                struct bplus_node_ssd *sub_ptr[BPLUS_MAX_ORDER];    /* pointers to child nodes */
                off_t disk_offset[BPLUS_MAX_ORDER];             /* disk offsets to leaf nodes */
        };
        int is_leaf_parent;     /* 1 if children are leaf nodes on disk, 0 if memory nodes */
};

/**
 * Sub-page header (part of each 4KB sub-page)
 */
struct sub_page_header {
        int entries;            /* number of entries in this sub-page */
        int next_sub_page;      /* index of next sub-page in super-leaf (or -1) */
        int reserved[2];        /* padding for alignment */
};

/**
 * Sub-page structure (4KB each)
 * Layout: [header][keys...][data...]
 */
struct sub_page {
        struct sub_page_header header;
        char payload[SUB_PAGE_SIZE - sizeof(struct sub_page_header)];
};

/**
 * Super-Leaf Page - logical container for multiple 4KB sub-pages
 * This is NOT stored as a single contiguous block
 */
struct bplus_super_leaf {
        int type;               /* BPLUS_TREE_LEAF */
        int total_entries;      /* total entries across all sub-pages */
        int active_sub_pages;   /* number of active sub-pages */
        off_t next_super_leaf;  /* offset to next super-leaf */
        off_t prev_super_leaf;  /* offset to previous super-leaf */
        
        /* Array of block IDs for non-contiguous 4KB sub-pages */
        uint32_t sub_page_blocks[SUB_PAGES_PER_SUPER_LEAF];
        
        /* In-memory cache of sub-pages (loaded on demand) */
        struct sub_page *cached_sub_pages[SUB_PAGES_PER_SUPER_LEAF];
        int dirty_flags[SUB_PAGES_PER_SUPER_LEAF];  /* track which sub-pages need writing */
};

/**
 * Legacy leaf node structure (for compatibility)
 */
struct bplus_leaf_disk {
        int type;               /* BPLUS_TREE_LEAF */
        int entries;            /* number of key-value pairs */
        off_t next_leaf;        /* offset to next leaf node on disk */
        off_t prev_leaf;        /* offset to previous leaf node on disk */
        int key[BPLUS_MAX_ENTRIES];
        long data[BPLUS_MAX_ENTRIES];
};

/**
 * Block allocation bitmap for managing 4KB blocks
 */
struct block_allocator {
        uint32_t *bitmap;           /* bitmap for allocated blocks */
        uint32_t total_blocks;      /* total number of 4KB blocks */
        uint32_t allocated_blocks;  /* number of allocated blocks */
        uint32_t next_search_hint;  /* hint for next allocation search */
};

/**
 * Disk management structure with block allocation
 */
struct disk_manager {
        int fd;                 /* file descriptor */
        char filename[256];     /* disk file name */
        off_t file_size;        /* current file size */
        size_t leaf_size;       /* size of each leaf node on disk (legacy) */
        
        /* Block-based allocation for super-leaf pages */
        struct block_allocator *allocator;
        uint32_t total_4kb_blocks;      /* total 4KB blocks in file */
        
        /* Super-leaf management */
        off_t super_leaf_metadata_offset;  /* offset to super-leaf metadata area */
        uint32_t next_super_leaf_id;       /* next available super-leaf ID */
};

/**
 * Hybrid B+tree structure
 */
struct bplus_tree_ssd {
        int order;              /* max children per non-leaf node */
        int entries;            /* max entries per leaf node */
        int level;              /* height of tree */
        struct bplus_node_ssd *root;
        struct list_head list[BPLUS_MAX_LEVEL]; /* list of nodes at each level */
        struct disk_manager *disk_mgr;          /* disk management */
};

/* Function prototypes */
void bplus_tree_ssd_dump(struct bplus_tree_ssd *tree);
long bplus_tree_ssd_get(struct bplus_tree_ssd *tree, key_t key);
int bplus_tree_ssd_put(struct bplus_tree_ssd *tree, key_t key, long data);
long bplus_tree_ssd_get_range(struct bplus_tree_ssd *tree, key_t key1, key_t key2);
struct bplus_tree_ssd *bplus_tree_ssd_init(int order, int entries, const char *disk_file);
void bplus_tree_ssd_deinit(struct bplus_tree_ssd *tree);

/* Constants */
enum {
        BPLUS_SSD_TREE_LEAF = 0,
        BPLUS_SSD_TREE_NON_LEAF = 1,
};

/* Block allocation operations */
struct block_allocator *block_allocator_init(uint32_t total_blocks);
void block_allocator_deinit(struct block_allocator *allocator);
uint32_t allocate_block(struct block_allocator *allocator);
int allocate_multiple_blocks(struct block_allocator *allocator, uint32_t count, uint32_t *block_ids);
void free_block(struct block_allocator *allocator, uint32_t block_id);
void free_multiple_blocks(struct block_allocator *allocator, uint32_t count, uint32_t *block_ids);

/* Hash function for intra-page key distribution */
int hash_key_to_sub_page(key_t key, int num_sub_pages);

/* Sub-page operations */
struct sub_page *sub_page_create(void);
void sub_page_free(struct sub_page *sub_page);
int sub_page_insert(struct sub_page *sub_page, key_t key, long data);
long sub_page_search(struct sub_page *sub_page, key_t key);
int sub_page_is_full(struct sub_page *sub_page);
int sub_page_delete(struct sub_page *sub_page, key_t key);

/* Zero-padding for SSD compression */
void sub_page_zero_pad_unused_space(struct sub_page *sub_page);
size_t sub_page_get_used_space(struct sub_page *sub_page);
size_t sub_page_get_unused_space(struct sub_page *sub_page);
void sub_page_prepare_for_compression(struct sub_page *sub_page);

/* Super-leaf operations with hashed I/O */
struct bplus_super_leaf *super_leaf_create(struct disk_manager *dm);
void super_leaf_free(struct bplus_super_leaf *super_leaf);
int super_leaf_insert_hashed(struct disk_manager *dm, struct bplus_super_leaf *super_leaf, key_t key, long data);
long super_leaf_search_hashed(struct disk_manager *dm, struct bplus_super_leaf *super_leaf, key_t key);
int super_leaf_delete_hashed(struct disk_manager *dm, struct bplus_super_leaf *super_leaf, key_t key);
int super_leaf_flush_dirty(struct disk_manager *dm, struct bplus_super_leaf *super_leaf);
struct sub_page *super_leaf_load_sub_page_by_hash(struct disk_manager *dm, struct bplus_super_leaf *super_leaf, key_t key);
struct sub_page *super_leaf_load_sub_page(struct disk_manager *dm, struct bplus_super_leaf *super_leaf, int sub_page_idx);
int super_leaf_is_full(struct bplus_super_leaf *super_leaf);

/* Super-leaf splitting with parallel I/O */
typedef struct {
    key_t key;
    struct bplus_super_leaf *right_sibling;
} PromotedKey;

PromotedKey split_super_leaf(struct disk_manager *dm, struct bplus_super_leaf *leaf_to_split);

/* Legacy super-leaf operations (for compatibility) */
int super_leaf_insert(struct disk_manager *dm, struct bplus_super_leaf *super_leaf, key_t key, long data);
long super_leaf_search(struct disk_manager *dm, struct bplus_super_leaf *super_leaf, key_t key);

/* Disk operations */
struct disk_manager *disk_manager_init(const char *filename);
void disk_manager_deinit(struct disk_manager *dm);
int disk_write_sub_page(struct disk_manager *dm, uint32_t block_id, struct sub_page *sub_page);
struct sub_page *disk_read_sub_page(struct disk_manager *dm, uint32_t block_id);

/* Legacy disk operations (for compatibility) */
off_t disk_write_leaf(struct disk_manager *dm, struct bplus_leaf_disk *leaf);
struct bplus_leaf_disk *disk_read_leaf(struct disk_manager *dm, off_t offset);
int disk_update_leaf(struct disk_manager *dm, off_t offset, struct bplus_leaf_disk *leaf);
void disk_free_leaf(struct bplus_leaf_disk *leaf);

#endif  /* _BPLUS_TREE_SSD_H */
