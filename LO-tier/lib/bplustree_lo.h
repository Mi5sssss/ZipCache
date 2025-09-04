/*
 * Large Object B+Tree (BT_LO) Implementation
 * Stores pointers to large objects on SSD instead of values directly
 * Based on DRAM B+Tree but with object pointer semantics
 */

#ifndef _BPLUS_TREE_LO_H
#define _BPLUS_TREE_LO_H

#include <stdint.h>
#include <stddef.h>

#define BPLUS_MIN_ORDER     3
#define BPLUS_MAX_ORDER     64
#define BPLUS_MAX_ENTRIES   64
#define BPLUS_MAX_LEVEL     10

typedef int key_t;

/* Object Pointer Structure for Large Objects on SSD */
struct object_pointer {
        uint64_t lba;           /* Logical Block Address on SSD */
        uint32_t size;          /* Size of the object in bytes */
        uint32_t checksum;      /* Optional checksum for integrity */
};

/* Invalid object pointer marker */
#define INVALID_OBJECT_POINTER  ((struct object_pointer){0, 0, 0})

/* Object pointer operations */
static inline int object_pointer_is_valid(struct object_pointer *ptr) {
        return ptr && (ptr->lba != 0 || ptr->size != 0);
}

static inline int object_pointer_equals(struct object_pointer *a, struct object_pointer *b) {
        return a && b && a->lba == b->lba && a->size == b->size;
}

/* Linked list for B+tree nodes */
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

static inline int list_empty(struct list_head *head)
{
        return head->next == head;
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

/* B+tree node types */
enum {
        BPLUS_TREE_LEAF,
        BPLUS_TREE_NON_LEAF = 1,
};

/* B+tree basic node structure */
struct bplus_node_lo {
        int type;                               /* BPLUS_TREE_LEAF or BPLUS_TREE_NON_LEAF */
        int parent_key_idx;                     /* Index in parent node */
        struct bplus_non_leaf_lo *parent;       /* Pointer to parent node */
        struct list_head link;                  /* Linked list for leaf traversal */
        int count;                              /* Number of elements */
};

/* B+tree non-leaf (internal) node */
struct bplus_non_leaf_lo {
        int type;                               /* Always BPLUS_TREE_NON_LEAF */
        int parent_key_idx;                     /* Index in parent node */
        struct bplus_non_leaf_lo *parent;       /* Pointer to parent node */
        struct list_head link;                  /* Linked list for traversal */
        int children;                           /* Number of child nodes */
        key_t key[BPLUS_MAX_ORDER - 1];         /* Keys for navigation */
        struct bplus_node_lo *sub_ptr[BPLUS_MAX_ORDER]; /* Child pointers */
};

/* B+tree leaf node - stores object pointers instead of direct values */
struct bplus_leaf_lo {
        int type;                               /* Always BPLUS_TREE_LEAF */
        int parent_key_idx;                     /* Index in parent node */
        struct bplus_non_leaf_lo *parent;       /* Pointer to parent node */
        struct list_head link;                  /* Linked list for leaf traversal */
        int entries;                            /* Number of key-pointer pairs */
        key_t key[BPLUS_MAX_ENTRIES];           /* Keys */
        struct object_pointer data[BPLUS_MAX_ENTRIES]; /* Object pointers to SSD */
};

/* B+tree structure for Large Objects */
struct bplus_tree_lo {
        int order;                              /* Tree order */
        int entries;                            /* Total number of entries */
        int level;                              /* Tree height */
        struct bplus_node_lo *root;             /* Root node */
        struct list_head list[BPLUS_MAX_LEVEL]; /* Level-wise linked lists */
        
        /* Large object management */
        uint64_t next_lba;                      /* Next available LBA for allocation */
        uint64_t total_objects;                 /* Total number of large objects */
        uint64_t total_size;                    /* Total size of all objects */
};

/* BT_LO API Functions */

/* Tree management */
struct bplus_tree_lo *bplus_tree_lo_init(int order);
void bplus_tree_lo_deinit(struct bplus_tree_lo *tree);

/* Object pointer operations */
int bplus_tree_lo_put(struct bplus_tree_lo *tree, key_t key, struct object_pointer obj_ptr);
struct object_pointer bplus_tree_lo_get(struct bplus_tree_lo *tree, key_t key);
int bplus_tree_lo_delete(struct bplus_tree_lo *tree, key_t key);

/* Object allocation and management */
struct object_pointer bplus_tree_lo_allocate_object(struct bplus_tree_lo *tree, uint32_t size);
int bplus_tree_lo_free_object(struct bplus_tree_lo *tree, struct object_pointer obj_ptr);

/* Tree statistics and utilities */
int bplus_tree_lo_get_range(struct bplus_tree_lo *tree, key_t key1, key_t key2, 
                            key_t *keys, struct object_pointer *obj_ptrs, int max_count);
void bplus_tree_lo_dump(struct bplus_tree_lo *tree);
void bplus_tree_lo_print_stats(struct bplus_tree_lo *tree);

/* Object pointer utilities */
uint32_t object_pointer_checksum(const void *data, uint32_t size);
int object_pointer_verify(struct object_pointer obj_ptr, const void *data);

#endif /* _BPLUS_TREE_LO_H */
