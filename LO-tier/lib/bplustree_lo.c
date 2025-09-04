/*
 * Large Object B+Tree (BT_LO) Implementation
 * Stores pointers to large objects on SSD instead of values directly
 * Based on DRAM B+Tree but with object pointer semantics
 */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "bplustree_lo.h"

enum {
        LEFT_SIBLING,
        RIGHT_SIBLING = 1,
};

static inline int is_leaf(struct bplus_node_lo *node)
{
        return node->type == BPLUS_TREE_LEAF;
}

/* Binary search for keys */
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

/* Node creation functions */
static struct bplus_non_leaf_lo *non_leaf_new(void)
{
        struct bplus_non_leaf_lo *node = calloc(1, sizeof(*node));
        assert(node != NULL);
        list_init(&node->link);
        node->type = BPLUS_TREE_NON_LEAF;
        node->parent_key_idx = -1;
        return node;
}

static struct bplus_leaf_lo *leaf_new(void)
{
        struct bplus_leaf_lo *node = calloc(1, sizeof(*node));
        assert(node != NULL);
        list_init(&node->link);
        node->type = BPLUS_TREE_LEAF;
        node->parent_key_idx = -1;
        return node;
}

/* Node deletion functions */
static void non_leaf_delete(struct bplus_non_leaf_lo *node)
{
        list_del(&node->link);
        free(node);
}

static void leaf_delete(struct bplus_leaf_lo *node)
{
        list_del(&node->link);
        free(node);
}

/* Forward declarations for splitting functions */
static int parent_node_build(struct bplus_tree_lo *tree, struct bplus_node_lo *left,
                            struct bplus_node_lo *right, key_t key, int level);
static int non_leaf_insert(struct bplus_tree_lo *tree, struct bplus_non_leaf_lo *node,
                          struct bplus_node_lo *l_ch, struct bplus_node_lo *r_ch, key_t key, int level);
static void leaf_split_left(struct bplus_leaf_lo *leaf, struct bplus_leaf_lo *left,
                           key_t key, struct object_pointer obj_ptr, int insert);
static void leaf_split_right(struct bplus_leaf_lo *leaf, struct bplus_leaf_lo *right,
                            key_t key, struct object_pointer obj_ptr, int insert);
static void leaf_simple_insert(struct bplus_leaf_lo *leaf, key_t key, 
                              struct object_pointer obj_ptr, int insert);

/* Object pointer search in BT_LO */
struct object_pointer bplus_tree_lo_get(struct bplus_tree_lo *tree, key_t key)
{
        int i;
        struct object_pointer result = INVALID_OBJECT_POINTER;
        struct bplus_node_lo *node = tree->root;
        
        while (node != NULL) {
                if (is_leaf(node)) {
                        struct bplus_leaf_lo *ln = (struct bplus_leaf_lo *)node;
                        i = key_binary_search(ln->key, ln->entries, key);
                        if (i >= 0) {
                                result = ln->data[i];
                        }
                        break;
                } else {
                        struct bplus_non_leaf_lo *nln = (struct bplus_non_leaf_lo *)node;
                        i = key_binary_search(nln->key, nln->children - 1, key);
                        if (i >= 0) {
                                node = nln->sub_ptr[i + 1];
                        } else {
                                i = -i - 1;
                                node = nln->sub_ptr[i];
                        }
                }
        }
        return result;
}

/* Find leaf node for insertion/deletion */
static struct bplus_leaf_lo *leaf_locate(struct bplus_tree_lo *tree, key_t key)
{
        int i;
        struct bplus_node_lo *node = tree->root;
        
        while (node != NULL) {
                if (is_leaf(node)) {
                        return (struct bplus_leaf_lo *)node;
                } else {
                        struct bplus_non_leaf_lo *nln = (struct bplus_non_leaf_lo *)node;
                        i = key_binary_search(nln->key, nln->children - 1, key);
                        if (i >= 0) {
                                node = nln->sub_ptr[i + 1];
                        } else {
                                i = -i - 1;
                                node = nln->sub_ptr[i];
                        }
                }
        }
        return NULL;
}

/* Leaf node insertion with splitting support */
static int leaf_insert(struct bplus_tree_lo *tree, struct bplus_leaf_lo *leaf, 
                      key_t key, struct object_pointer obj_ptr)
{
        /* search key location */
        int insert = key_binary_search(leaf->key, leaf->entries, key);
        if (insert >= 0) {
                /* Key already exists - update object pointer */
                leaf->data[insert] = obj_ptr;
                return 0;
        }
        insert = -insert - 1;

        /* node full */
        if (leaf->entries == BPLUS_MAX_ENTRIES) {
                /* split = [m/2] */
                int split = (BPLUS_MAX_ENTRIES + 1) / 2;
                /* splited sibling node */
                struct bplus_leaf_lo *sibling = leaf_new();
                /* sibling leaf replication due to location of insertion */
                if (insert < split) {
                        leaf_split_left(leaf, sibling, key, obj_ptr, insert);
                } else {
                        leaf_split_right(leaf, sibling, key, obj_ptr, insert);
                }
                tree->entries++; /* New entry was added during split */
                
                /* build new parent */
                if (insert < split) {
                        return parent_node_build(tree, (struct bplus_node_lo *)sibling,
                                (struct bplus_node_lo *)leaf, leaf->key[0], 0);
                } else {
                        return parent_node_build(tree, (struct bplus_node_lo *)leaf,
                                (struct bplus_node_lo *)sibling, sibling->key[0], 0);
                }
        } else {
                leaf_simple_insert(leaf, key, obj_ptr, insert);
                tree->entries++;
        }

        return 0;
}

static int leaf_remove(struct bplus_tree_lo *tree, struct bplus_leaf_lo *leaf, key_t key)
{
        int pos = key_binary_search(leaf->key, leaf->entries, key);
        if (pos < 0) {
                return -1; /* Key not found */
        }
        
        /* Shift elements to fill the gap */
        memmove(&leaf->key[pos], &leaf->key[pos + 1], 
                (leaf->entries - pos - 1) * sizeof(key_t));
        memmove(&leaf->data[pos], &leaf->data[pos + 1], 
                (leaf->entries - pos - 1) * sizeof(struct object_pointer));
        
        leaf->entries--;
        tree->entries--;
        
        return 0;
}

/* Leaf node splitting functions adapted for object pointers */
static void leaf_split_left(struct bplus_leaf_lo *leaf, struct bplus_leaf_lo *left,
                           key_t key, struct object_pointer obj_ptr, int insert)
{
        int i, j;
        /* split = [m/2] */
        int split = (leaf->entries + 1) / 2;
        /* split as left sibling */
        __list_add(&left->link, leaf->link.prev, &leaf->link);
        /* replicate from 0 to key[split - 2] */
        for (i = 0, j = 0; i < split - 1; j++) {
                if (j == insert) {
                        left->key[j] = key;
                        left->data[j] = obj_ptr;
                } else {
                        left->key[j] = leaf->key[i];
                        left->data[j] = leaf->data[i];
                        i++;
                }
        }
        if (j == insert) {
                left->key[j] = key;
                left->data[j] = obj_ptr;
                j++;
        }
        left->entries = j;
        /* left shift for right node */
        for (j = 0; i < leaf->entries; i++, j++) {
                leaf->key[j] = leaf->key[i];
                leaf->data[j] = leaf->data[i];
        }
        leaf->entries = j;
}

static void leaf_split_right(struct bplus_leaf_lo *leaf, struct bplus_leaf_lo *right,
                            key_t key, struct object_pointer obj_ptr, int insert)
{
        int i, j;
        /* split = [m/2] */
        int split = (leaf->entries + 1) / 2;
        /* split as right sibling */
        list_add(&right->link, &leaf->link);
        /* replicate from key[split] */
        for (i = split, j = 0; i < leaf->entries; j++) {
                if (j != insert - split) {
                        right->key[j] = leaf->key[i];
                        right->data[j] = leaf->data[i];
                        i++;
                }
        }
        /* reserve a hole for insertion */
        if (j > insert - split) {
                right->entries = j;
        } else {
                assert(j == insert - split);
                right->entries = j + 1;
        }
        /* insert new key */
        j = insert - split;
        right->key[j] = key;
        right->data[j] = obj_ptr;
        /* left leaf number */
        leaf->entries = split;
}

static void leaf_simple_insert(struct bplus_leaf_lo *leaf, key_t key, 
                              struct object_pointer obj_ptr, int insert)
{
        int i;
        for (i = leaf->entries; i > insert; i--) {
                leaf->key[i] = leaf->key[i - 1];
                leaf->data[i] = leaf->data[i - 1];
        }
        leaf->key[i] = key;
        leaf->data[i] = obj_ptr;
        leaf->entries++;
}

/* Parent node building for splits */
static int parent_node_build(struct bplus_tree_lo *tree, struct bplus_node_lo *left,
                            struct bplus_node_lo *right, key_t key, int level)
{
        if (left->parent == NULL && right->parent == NULL) {
                /* new parent */
                struct bplus_non_leaf_lo *parent = non_leaf_new();
                parent->key[0] = key;
                parent->sub_ptr[0] = left;
                parent->sub_ptr[0]->parent = parent;
                parent->sub_ptr[0]->parent_key_idx = -1;
                parent->sub_ptr[1] = right;
                parent->sub_ptr[1]->parent = parent;
                parent->sub_ptr[1]->parent_key_idx = 0;
                parent->children = 2;
                /* update root */
                tree->root = (struct bplus_node_lo *)parent;
                list_add(&parent->link, &tree->list[++tree->level]);
                return 0;
        } else if (right->parent == NULL) {
                /* trace upwards */
                right->parent = left->parent;
                return non_leaf_insert(tree, left->parent, left, right, key, level + 1);
        } else {
                /* trace upwards */
                left->parent = right->parent;
                return non_leaf_insert(tree, right->parent, left, right, key, level + 1);
        }
}

/* Non-leaf node simple insertion */
static void non_leaf_simple_insert(struct bplus_non_leaf_lo *node, struct bplus_node_lo *l_ch, 
                                  struct bplus_node_lo *r_ch, key_t key, int insert)
{
        int i;
        /* shift key array */
        for (i = node->children - 1; i > insert; i--) {
                node->key[i] = node->key[i - 1];
        }
        /* shift pointer array */
        for (i = node->children; i > insert + 1; i--) {
                node->sub_ptr[i] = node->sub_ptr[i - 1];
                node->sub_ptr[i]->parent_key_idx++;
        }
        /* insert new key and sub-node */
        node->key[insert] = key;
        node->sub_ptr[insert + 1] = r_ch;
        node->children++;
        
        /* update parent info */
        r_ch->parent = node;
        r_ch->parent_key_idx = insert;
        l_ch->parent = node;
}

/* Non-leaf node insertion with potential splitting */
static int non_leaf_insert(struct bplus_tree_lo *tree, struct bplus_non_leaf_lo *node,
                          struct bplus_node_lo *l_ch, struct bplus_node_lo *r_ch, key_t key, int level)
{
        /* search key location */
        int insert = key_binary_search(node->key, node->children - 1, key);
        assert(insert < 0);
        insert = -insert - 1;

        /* node is full */
        if (node->children == tree->order) {
                /* split = [m/2] */
                key_t split_key;
                int split = node->children / 2;
                struct bplus_non_leaf_lo *sibling = non_leaf_new();
                
                /* For simplicity, implement basic splitting */
                if (insert < split) {
                        /* Insert in left node, create right sibling */
                        split_key = node->key[split - 1];
                        
                        /* Move right half to sibling */
                        sibling->children = node->children - split;
                        memcpy(sibling->key, &node->key[split], 
                               (sibling->children - 1) * sizeof(key_t));
                        memcpy(sibling->sub_ptr, &node->sub_ptr[split], 
                               sibling->children * sizeof(struct bplus_node_lo *));
                        
                        /* Update parent pointers */
                        for (int i = 0; i < sibling->children; i++) {
                                sibling->sub_ptr[i]->parent = sibling;
                                sibling->sub_ptr[i]->parent_key_idx = i - 1;
                        }
                        
                        /* Truncate original node */
                        node->children = split;
                        
                        /* Insert in left node */
                        non_leaf_simple_insert(node, l_ch, r_ch, key, insert);
                        
                        /* Add sibling to list */
                        list_add(&sibling->link, &node->link);
                        
                } else {
                        /* Insert in right node */
                        split_key = node->key[split];
                        
                        /* Move right half to sibling */
                        sibling->children = node->children - split - 1;
                        if (sibling->children > 0) {
                                memcpy(sibling->key, &node->key[split + 1], 
                                       (sibling->children - 1) * sizeof(key_t));
                                memcpy(sibling->sub_ptr, &node->sub_ptr[split + 1], 
                                       sibling->children * sizeof(struct bplus_node_lo *));
                        }
                        
                        /* Insert new key in appropriate position */
                        int new_insert = insert - split - 1;
                        if (new_insert >= 0) {
                                /* Insert in sibling */
                                memmove(&sibling->key[new_insert + 1], &sibling->key[new_insert],
                                       (sibling->children - 1 - new_insert) * sizeof(key_t));
                                memmove(&sibling->sub_ptr[new_insert + 2], &sibling->sub_ptr[new_insert + 1],
                                       (sibling->children - 1 - new_insert) * sizeof(struct bplus_node_lo *));
                                sibling->key[new_insert] = key;
                                sibling->sub_ptr[new_insert + 1] = r_ch;
                                sibling->children++;
                                
                                r_ch->parent = sibling;
                                r_ch->parent_key_idx = new_insert;
                                l_ch->parent = sibling;
                        }
                        
                        /* Update parent pointers */
                        for (int i = 0; i < sibling->children; i++) {
                                sibling->sub_ptr[i]->parent = sibling;
                                sibling->sub_ptr[i]->parent_key_idx = i - 1;
                        }
                        
                        /* Truncate original node */
                        node->children = split + 1;
                        
                        /* Add sibling to list */
                        list_add(&sibling->link, &node->link);
                }
                
                /* build new parent */
                return parent_node_build(tree, (struct bplus_node_lo *)node,
                                       (struct bplus_node_lo *)sibling, split_key, level);
        } else {
                non_leaf_simple_insert(node, l_ch, r_ch, key, insert);
        }

        return 0;
}

/* Tree initialization */
struct bplus_tree_lo *bplus_tree_lo_init(int order)
{
        if (order < BPLUS_MIN_ORDER || order > BPLUS_MAX_ORDER) {
                return NULL;
        }
        
        struct bplus_tree_lo *tree = calloc(1, sizeof(*tree));
        assert(tree != NULL);
        
        tree->order = order;
        tree->entries = 0;
        tree->level = 1;
        tree->next_lba = 1; /* Start from LBA 1, 0 is reserved for invalid */
        tree->total_objects = 0;
        tree->total_size = 0;
        
        /* Initialize level-wise linked lists */
        for (int i = 0; i < BPLUS_MAX_LEVEL; i++) {
                list_init(&tree->list[i]);
        }
        
        /* Create initial leaf node */
        struct bplus_leaf_lo *root = leaf_new();
        tree->root = (struct bplus_node_lo *)root;
        list_add_tail(&root->link, &tree->list[tree->level - 1]);
        
        return tree;
}

/* Tree cleanup */
static void bplus_tree_lo_free_node(struct bplus_node_lo *node)
{
        if (is_leaf(node)) {
                leaf_delete((struct bplus_leaf_lo *)node);
        } else {
                struct bplus_non_leaf_lo *nln = (struct bplus_non_leaf_lo *)node;
                for (int i = 0; i < nln->children; i++) {
                        bplus_tree_lo_free_node(nln->sub_ptr[i]);
                }
                non_leaf_delete(nln);
        }
}

void bplus_tree_lo_deinit(struct bplus_tree_lo *tree)
{
        if (tree && tree->root) {
                bplus_tree_lo_free_node(tree->root);
        }
        free(tree);
}

/* Object allocation */
struct object_pointer bplus_tree_lo_allocate_object(struct bplus_tree_lo *tree, uint32_t size)
{
        if (!tree || size == 0) {
                return INVALID_OBJECT_POINTER;
        }
        
        struct object_pointer obj_ptr;
        obj_ptr.lba = tree->next_lba++;
        obj_ptr.size = size;
        obj_ptr.checksum = 0; /* Will be calculated when data is written */
        
        tree->total_objects++;
        tree->total_size += size;
        
        return obj_ptr;
}

/* Object pointer insertion with full splitting support */
int bplus_tree_lo_put(struct bplus_tree_lo *tree, key_t key, struct object_pointer obj_ptr)
{
        if (!tree || !object_pointer_is_valid(&obj_ptr)) {
                return -1;
        }
        
        struct bplus_node_lo *node = tree->root;
        while (node != NULL) {
                if (is_leaf(node)) {
                        struct bplus_leaf_lo *ln = (struct bplus_leaf_lo *)node;
                        return leaf_insert(tree, ln, key, obj_ptr);
                } else {
                        struct bplus_non_leaf_lo *nln = (struct bplus_non_leaf_lo *)node;
                        int i = key_binary_search(nln->key, nln->children - 1, key);
                        if (i >= 0) {
                                node = nln->sub_ptr[i + 1];
                        } else {
                                i = -i - 1;
                                node = nln->sub_ptr[i];
                        }
                }
        }

        /* new root */
        struct bplus_leaf_lo *root = leaf_new();
        root->key[0] = key;
        root->data[0] = obj_ptr;
        root->entries = 1;
        tree->root = (struct bplus_node_lo *)root;
        tree->entries++;
        list_add(&root->link, &tree->list[tree->level]);
        return 0;
}

/* Object deletion */
int bplus_tree_lo_delete(struct bplus_tree_lo *tree, key_t key)
{
        if (!tree) {
                return -1;
        }
        
        struct bplus_leaf_lo *leaf = leaf_locate(tree, key);
        if (!leaf) {
                return -1;
        }
        
        return leaf_remove(tree, leaf, key);
}

/* Get range of objects */
int bplus_tree_lo_get_range(struct bplus_tree_lo *tree, key_t key1, key_t key2, 
                           key_t *keys, struct object_pointer *obj_ptrs, int max_count)
{
        if (!tree || !keys || !obj_ptrs || key1 > key2 || max_count <= 0) {
                return 0;
        }
        
        int count = 0;
        struct list_head *pos, *n;
        
        /* Traverse leaf nodes */
        list_for_each_safe(pos, n, &tree->list[0]) {
                struct bplus_leaf_lo *leaf = list_entry(pos, struct bplus_leaf_lo, link);
                
                for (int i = 0; i < leaf->entries && count < max_count; i++) {
                        if (leaf->key[i] >= key1 && leaf->key[i] <= key2) {
                                keys[count] = leaf->key[i];
                                obj_ptrs[count] = leaf->data[i];
                                count++;
                        }
                }
        }
        
        return count;
}

/* Tree statistics */
void bplus_tree_lo_print_stats(struct bplus_tree_lo *tree)
{
        if (!tree) {
                printf("BT_LO: Tree is NULL\n");
                return;
        }
        
        printf("🏗️  Large Object B+Tree (BT_LO) Statistics:\n");
        printf("   Tree order: %d\n", tree->order);
        printf("   Tree level: %d\n", tree->level);
        printf("   Total entries: %d\n", tree->entries);
        printf("   Total objects: %lu\n", tree->total_objects);
        printf("   Total size: %lu bytes (%.2f MB)\n", 
               tree->total_size, (double)tree->total_size / (1024 * 1024));
        printf("   Next LBA: %lu\n", tree->next_lba);
        printf("   Avg object size: %.2f bytes\n", 
               tree->total_objects > 0 ? (double)tree->total_size / tree->total_objects : 0.0);
}

/* Tree dump for debugging */
static void bplus_tree_lo_dump_node(struct bplus_node_lo *node, int level)
{
        if (is_leaf(node)) {
                struct bplus_leaf_lo *leaf = (struct bplus_leaf_lo *)node;
                printf("Level %d Leaf: ", level);
                for (int i = 0; i < leaf->entries; i++) {
                        printf("(%d:LBA%lu)", leaf->key[i], leaf->data[i].lba);
                        if (i < leaf->entries - 1) printf(" ");
                }
                printf("\n");
        } else {
                struct bplus_non_leaf_lo *non_leaf = (struct bplus_non_leaf_lo *)node;
                printf("Level %d Non-leaf: ", level);
                for (int i = 0; i < non_leaf->children - 1; i++) {
                        printf("%d ", non_leaf->key[i]);
                }
                printf("\n");
                
                for (int i = 0; i < non_leaf->children; i++) {
                        bplus_tree_lo_dump_node(non_leaf->sub_ptr[i], level + 1);
                }
        }
}

void bplus_tree_lo_dump(struct bplus_tree_lo *tree)
{
        if (!tree || !tree->root) {
                printf("BT_LO: Empty tree\n");
                return;
        }
        
        printf("🌳 Large Object B+Tree Structure:\n");
        bplus_tree_lo_dump_node(tree->root, 0);
        printf("\n");
}

/* Object pointer utilities */
uint32_t object_pointer_checksum(const void *data, uint32_t size)
{
        if (!data || size == 0) {
                return 0;
        }
        
        /* Simple checksum calculation */
        uint32_t checksum = 0;
        const unsigned char *bytes = (const unsigned char *)data;
        for (uint32_t i = 0; i < size; i++) {
                checksum = ((checksum << 1) | (checksum >> 31)) ^ bytes[i];
        }
        return checksum;
}

int object_pointer_verify(struct object_pointer obj_ptr, const void *data)
{
        if (!object_pointer_is_valid(&obj_ptr) || !data) {
                return 0;
        }
        
        uint32_t calculated_checksum = object_pointer_checksum(data, obj_ptr.size);
        return calculated_checksum == obj_ptr.checksum;
}
