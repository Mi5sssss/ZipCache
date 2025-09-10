# Hybrid B+Tree Implementation

## Overview

This is a hybrid B+tree implementation that optimizes memory usage by storing:
- **Non-leaf nodes**: In memory for fast traversal
- **Leaf nodes**: On SSD/disk for memory efficiency

This design is ideal for scenarios where you need fast range queries but want to minimize memory footprint for large datasets.

## Architecture

### Memory Structure (Non-leaf nodes)
```
┌─────────────────┐
│   Root Node     │ ← In Memory
│ [10|20|30]      │
│ [ptr|ptr|ptr]   │
└─────────────────┘
         │
         ▼
┌─────────────────┐
│ Internal Node   │ ← In Memory  
│ [5|15|25]       │
│ [off|off|off]   │ ← Disk offsets to leaf nodes
└─────────────────┘
```

### Disk Structure (Leaf nodes)
```
Disk File: /path/to/data.dat
┌─────────────────┐ ← Offset 0
│   Leaf Node 1   │
│ Keys: [1,2,3,4] │
│ Data: [10,20,30,40] │
│ Next: Offset 512│
└─────────────────┘
┌─────────────────┐ ← Offset 512
│   Leaf Node 2   │
│ Keys: [5,6,7,8] │
│ Data: [50,60,70,80] │
│ Next: Offset 1024│
└─────────────────┘
```

## Key Features

1. **Memory Efficiency**: Only non-leaf nodes stored in memory
2. **Fast Traversal**: Memory-based navigation to leaf nodes
3. **Persistent Storage**: Leaf nodes persist on disk
4. **Hybrid Design**: Best of both memory and disk-based approaches

## Data Structures

### Non-leaf Node (Memory)
```c
struct bplus_non_leaf {
    int type;                           // BPLUS_TREE_NON_LEAF
    int children;                       // Number of child pointers
    int key[BPLUS_MAX_ORDER - 1];      // Keys for navigation
    union {
        struct bplus_node *sub_ptr[BPLUS_MAX_ORDER];     // Memory child pointers
        off_t disk_offset[BPLUS_MAX_ORDER];              // Disk offsets to leaves
    };
    int is_leaf_parent;                 // 1 if children are disk leaves
};
```

### Leaf Node (Disk)
```c
struct bplus_leaf_disk {
    int type;                           // BPLUS_TREE_LEAF
    int entries;                        // Number of key-value pairs
    off_t next_leaf;                    // Next leaf offset
    off_t prev_leaf;                    // Previous leaf offset
    int key[BPLUS_MAX_ENTRIES];         // Keys
    long data[BPLUS_MAX_ENTRIES];       // Values
};
```

## Usage

### Basic Operations

```c
#include "bplustree.h"

// Initialize tree
struct bplus_tree *tree = bplus_tree_init(16, 64, "/path/to/data.dat");

// Insert key-value pairs
bplus_tree_put(tree, 42, 4200);

// Retrieve values
long value = bplus_tree_get(tree, 42);

// Cleanup
bplus_tree_deinit(tree);
```

### Parameters

- **order**: Maximum children per non-leaf node (typically 16-64)
- **entries**: Maximum entries per leaf node (typically 64-256) 
- **disk_file**: Path to disk file for storing leaf nodes

## Building

```bash
cd bplustree/SSD-tier
mkdir build && cd build
cmake ..
make
```

## Testing

```bash
# Run basic tests
./hybrid_btree_test

# Run performance example  
./hybrid_btree_example
```

## Performance Characteristics

### Memory Usage
- **Memory**: O(log N) for non-leaf nodes
- **Disk**: O(N) for leaf nodes
- **Total Memory**: Significantly reduced compared to full in-memory trees

### Time Complexity
- **Search**: O(log N) - memory traversal + 1 disk read
- **Insert**: O(log N) - memory traversal + 1 disk write
- **Range Query**: O(log N + K) - where K is result size

### Advantages
1. **Low Memory Footprint**: Only tree structure in memory
2. **Fast Navigation**: Memory-based tree traversal
3. **Scalability**: Can handle datasets larger than memory
4. **Persistence**: Data survives restarts

### Trade-offs
1. **Disk I/O**: Each leaf access requires disk read/write
2. **Complexity**: More complex than pure memory/disk solutions
3. **Leaf Operations**: Limited leaf manipulation capabilities

## Implementation Status

### Completed Features
- ✅ Basic tree structure (memory non-leaf, disk leaf)
- ✅ Key-value insertion
- ✅ Key lookup
- ✅ Disk-based leaf storage
- ✅ Memory-based navigation

### TODO Features
- ⏳ Leaf node splitting
- ⏳ Node deletion
- ⏳ Range queries
- ⏳ Tree balancing
- ⏳ Concurrent access
- ⏳ Crash recovery

## Use Cases

This hybrid B+tree is ideal for:

1. **Large Datasets**: When dataset > available memory
2. **Read-Heavy Workloads**: Fast key lookups with minimal memory
3. **Embedded Systems**: Memory-constrained environments
4. **Database Indexes**: Where index structure should fit in memory
5. **Analytics**: Range queries over large datasets

## Comparison with Other Approaches

| Approach | Memory Usage | Search Speed | Scalability |
|----------|--------------|--------------|-------------|
| Pure Memory | O(N) | O(log N) | Limited by RAM |
| Pure Disk | O(1) | O(log N) × disk_access | Unlimited |
| **Hybrid** | **O(log N)** | **O(log N) + 1 disk** | **High** |

The hybrid approach provides the best balance of memory efficiency and performance.
