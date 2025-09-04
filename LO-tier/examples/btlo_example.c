#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../lib/bplustree_lo.h"

void demonstrate_large_object_storage() {
    printf("🏗️  Large Object B+Tree (BT_LO) Demonstration\n");
    printf("==============================================\n\n");
    
    /* Initialize the Large Object B+Tree */
    struct bplus_tree_lo *btlo = bplus_tree_lo_init(16);
    if (!btlo) {
        printf("❌ Failed to initialize BT_LO\n");
        return;
    }
    
    printf("✅ BT_LO initialized with order 16\n\n");
    
    /* Simulate large object allocation and storage */
    printf("📦 Allocating large objects on SSD...\n");
    
    /* Large image file */
    struct object_pointer img_ptr = bplus_tree_lo_allocate_object(btlo, 5 * 1024 * 1024); /* 5MB */
    bplus_tree_lo_put(btlo, 1001, img_ptr);
    printf("  🖼️  Image file: key=1001, LBA=%lu, size=%u bytes (%.1f MB)\n", 
           img_ptr.lba, img_ptr.size, (double)img_ptr.size / (1024 * 1024));
    
    /* Large video file */
    struct object_pointer video_ptr = bplus_tree_lo_allocate_object(btlo, 50 * 1024 * 1024); /* 50MB */
    bplus_tree_lo_put(btlo, 2001, video_ptr);
    printf("  🎥 Video file: key=2001, LBA=%lu, size=%u bytes (%.1f MB)\n", 
           video_ptr.lba, video_ptr.size, (double)video_ptr.size / (1024 * 1024));
    
    /* Large database backup */
    struct object_pointer db_ptr = bplus_tree_lo_allocate_object(btlo, 100 * 1024 * 1024); /* 100MB */
    bplus_tree_lo_put(btlo, 3001, db_ptr);
    printf("  🗄️  Database backup: key=3001, LBA=%lu, size=%u bytes (%.1f MB)\n", 
           db_ptr.lba, db_ptr.size, (double)db_ptr.size / (1024 * 1024));
    
    /* Large log archive */
    struct object_pointer log_ptr = bplus_tree_lo_allocate_object(btlo, 25 * 1024 * 1024); /* 25MB */
    bplus_tree_lo_put(btlo, 4001, log_ptr);
    printf("  📝 Log archive: key=4001, LBA=%lu, size=%u bytes (%.1f MB)\n", 
           log_ptr.lba, log_ptr.size, (double)log_ptr.size / (1024 * 1024));
    
    /* Large AI model weights */
    struct object_pointer model_ptr = bplus_tree_lo_allocate_object(btlo, 200 * 1024 * 1024); /* 200MB */
    bplus_tree_lo_put(btlo, 5001, model_ptr);
    printf("  🤖 AI model: key=5001, LBA=%lu, size=%u bytes (%.1f MB)\n", 
           model_ptr.lba, model_ptr.size, (double)model_ptr.size / (1024 * 1024));
    
    printf("\n📊 BT_LO Statistics:\n");
    bplus_tree_lo_print_stats(btlo);
    
    /* Demonstrate object retrieval */
    printf("\n🔍 Retrieving large objects by key...\n");
    
    struct object_pointer retrieved = bplus_tree_lo_get(btlo, 2001);
    if (object_pointer_is_valid(&retrieved)) {
        printf("  Video file found: LBA=%lu, size=%.1f MB\n", 
               retrieved.lba, (double)retrieved.size / (1024 * 1024));
    }
    
    retrieved = bplus_tree_lo_get(btlo, 5001);
    if (object_pointer_is_valid(&retrieved)) {
        printf("  AI model found: LBA=%lu, size=%.1f MB\n", 
               retrieved.lba, (double)retrieved.size / (1024 * 1024));
    }
    
    /* Demonstrate range queries */
    printf("\n📋 Range query for objects with keys 2000-4000...\n");
    key_t range_keys[10];
    struct object_pointer range_objects[10];
    
    int found = bplus_tree_lo_get_range(btlo, 2000, 4000, range_keys, range_objects, 10);
    printf("  Found %d objects in range:\n", found);
    
    for (int i = 0; i < found; i++) {
        printf("    Key %d: LBA=%lu, size=%.1f MB\n", 
               range_keys[i], range_objects[i].lba, 
               (double)range_objects[i].size / (1024 * 1024));
    }
    
    /* Show tree structure */
    printf("\n🌳 BT_LO Tree Structure:\n");
    bplus_tree_lo_dump(btlo);
    
    /* Simulate object updates */
    printf("🔄 Updating large object (new version)...\n");
    struct object_pointer new_model_ptr = bplus_tree_lo_allocate_object(btlo, 250 * 1024 * 1024); /* 250MB */
    bplus_tree_lo_put(btlo, 5001, new_model_ptr);
    printf("  AI model updated: key=5001, new LBA=%lu, new size=%.1f MB\n", 
           new_model_ptr.lba, (double)new_model_ptr.size / (1024 * 1024));
    
    printf("\n📊 Final BT_LO Statistics:\n");
    bplus_tree_lo_print_stats(btlo);
    
    /* Cleanup */
    bplus_tree_lo_deinit(btlo);
    printf("\n✅ BT_LO demonstration completed\n");
}

void show_memory_efficiency() {
    printf("\n💾 Memory Efficiency Demonstration\n");
    printf("==================================\n");
    
    struct bplus_tree_lo *btlo = bplus_tree_lo_init(16);
    
    /* Calculate memory usage for storing pointers vs. actual data */
    printf("📏 Memory footprint comparison:\n");
    
    const int num_objects = 1000;
    uint64_t total_object_size = 0;
    uint64_t pointer_memory = 0;
    
    printf("\n📦 Storing %d large objects...\n", num_objects);
    
    for (int i = 0; i < num_objects; i++) {
        uint32_t obj_size = (1 + i % 10) * 1024 * 1024; /* 1-10 MB objects */
        struct object_pointer obj_ptr = bplus_tree_lo_allocate_object(btlo, obj_size);
        bplus_tree_lo_put(btlo, i + 1000, obj_ptr);
        
        total_object_size += obj_size;
        pointer_memory += sizeof(struct object_pointer);
    }
    
    printf("\n📊 Memory Usage Analysis:\n");
    printf("  Total object data: %.2f GB (on SSD)\n", 
           (double)total_object_size / (1024 * 1024 * 1024));
    printf("  Pointer memory: %.2f KB (in DRAM)\n", 
           (double)pointer_memory / 1024);
    printf("  Memory efficiency: %.0fx reduction\n", 
           (double)total_object_size / pointer_memory);
    printf("  Each pointer: %zu bytes (LBA + size + checksum)\n", 
           sizeof(struct object_pointer));
    
    bplus_tree_lo_print_stats(btlo);
    bplus_tree_lo_deinit(btlo);
}

int main() {
    demonstrate_large_object_storage();
    show_memory_efficiency();
    
    printf("\n🎯 Key BT_LO Benefits:\n");
    printf("• Memory-resident pointers for fast lookup\n");
    printf("• Large objects stored efficiently on SSD\n");
    printf("• Scalable to millions of large objects\n");
    printf("• Range queries for batch operations\n");
    printf("• Object integrity with checksums\n");
    printf("• Automatic LBA allocation\n");
    
    return 0;
}
