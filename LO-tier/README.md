# Large Object B+Tree (BT_LO)

## Overview

The Large Object B+Tree (BT_LO) is the third core data structure in the hybrid caching system. It provides efficient management of pointers to large objects stored on SSD while keeping the tree structure entirely in DRAM for fast lookups.

## Architecture

### Key Design Principles

1. **Memory-Resident Tree**: The entire B+tree structure resides in DRAM for fast traversal
2. **Object Pointer Storage**: Leaf nodes store object pointers (LBA + size + checksum) instead of actual data
3. **SSD Object Storage**: Large objects remain on SSD, referenced by their logical block addresses
4. **DRAM-Based Implementation**: Built on the proven DRAM B+tree codebase

### Object Pointer Structure

```c
struct object_pointer {
    uint64_t lba;           /* Logical Block Address on SSD */
    uint32_t size;          /* Size of the object in bytes */
    uint32_t checksum;      /* Optional checksum for integrity */
};
```

## Features

### Core Operations

- **Object Allocation**: Automatic LBA assignment for new objects
- **Pointer Management**: Insert, retrieve, update, and delete object pointers
- **Range Queries**: Batch retrieval of object pointers within key ranges
- **Memory Efficiency**: Stores only 16-byte pointers vs. full object data

### Advanced Features

- **Integrity Checking**: Checksum support for object verification
- **Statistics Tracking**: Monitor object count, total size, and memory usage
- **Tree Debugging**: Dump tree structure and analyze performance
- **Automatic Cleanup**: Proper memory management and tree deinitialization

## Memory Efficiency

### Real-World Performance

For 1000 large objects (1-10 MB each):
- **Total object data**: 5.37 GB (stored on SSD)
- **Pointer memory**: 15.62 KB (stored in DRAM)
- **Memory efficiency**: 360,448x reduction in DRAM usage
- **Per-pointer overhead**: 16 bytes (LBA + size + checksum)

## API Reference

### Tree Management

```c
struct bplus_tree_lo *bplus_tree_lo_init(int order);
void bplus_tree_lo_deinit(struct bplus_tree_lo *tree);
```

### Object Operations

```c
/* Allocate space for new object on SSD */
struct object_pointer bplus_tree_lo_allocate_object(struct bplus_tree_lo *tree, uint32_t size);

/* Insert object pointer */
int bplus_tree_lo_put(struct bplus_tree_lo *tree, key_t key, struct object_pointer obj_ptr);

/* Retrieve object pointer */
struct object_pointer bplus_tree_lo_get(struct bplus_tree_lo *tree, key_t key);

/* Delete object pointer */
int bplus_tree_lo_delete(struct bplus_tree_lo *tree, key_t key);
```

### Range Operations

```c
/* Get multiple object pointers in key range */
int bplus_tree_lo_get_range(struct bplus_tree_lo *tree, key_t key1, key_t key2, 
                            key_t *keys, struct object_pointer *obj_ptrs, int max_count);
```

### Utilities

```c
/* Print tree statistics */
void bplus_tree_lo_print_stats(struct bplus_tree_lo *tree);

/* Debug tree structure */
void bplus_tree_lo_dump(struct bplus_tree_lo *tree);

/* Object integrity functions */
uint32_t object_pointer_checksum(const void *data, uint32_t size);
int object_pointer_verify(struct object_pointer obj_ptr, const void *data);
```

## Building and Testing

### Build Instructions

```bash
cd LO-tier
mkdir build && cd build
cmake ..
make
```

### Running Tests

```bash
# Comprehensive test suite
./btlo_test

# Interactive demonstration
./btlo_example
```

## Use Cases

### Ideal Applications

1. **Large Media Storage**: Images, videos, audio files
2. **Database Backups**: Large database dumps and archives
3. **Machine Learning**: Model weights and training datasets
4. **Document Storage**: Large documents and file attachments
5. **Scientific Data**: Research datasets and simulation results

### Performance Characteristics

- **Lookup Time**: O(log n) for pointer retrieval
- **Memory Usage**: O(n) for pointer storage (16 bytes per object)
- **Storage Scalability**: Limited only by SSD capacity
- **Range Query**: Efficient batch operations

## Integration with Hybrid System

The BT_LO works alongside:

1. **DRAM B+Tree**: For small, frequently accessed data
2. **Hybrid B+Tree**: For medium-sized data with SSD leaf nodes
3. **Zero-Padding**: For optimal SSD compression

## Limitations

- **No Persistence**: Tree structure rebuilds on restart  
- **Single-Threaded**: No concurrent access protection

## Future Enhancements

- **Persistence**: Save/restore tree structure
- **Concurrency**: Thread-safe operations
- **Compression**: Object-level compression support
- **Tiered Storage**: Automatic migration between storage tiers

## Benefits Summary

✅ **Memory Efficiency**: 360,000x reduction in DRAM usage vs. storing full objects  
✅ **Fast Lookups**: O(log n) pointer retrieval from DRAM  
✅ **Scalable Storage**: Objects limited only by SSD capacity  
✅ **Full B+Tree Operations**: Complete splitting and growth support  
✅ **Range Operations**: Efficient batch processing  
✅ **Integrity Support**: Checksum-based verification  
✅ **Simple API**: Easy integration with existing systems
