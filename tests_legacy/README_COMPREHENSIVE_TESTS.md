# ZipCache Comprehensive Test Suite

This test suite verifies ZipCache's main operations with deliberate object placement across all 3 B+Trees, multi-threading support, and eviction/promotion logic.

## 🎯 **Test Coverage**

### Core Operations Tested
- ✅ **GET** - Coordinated search across BT_DRAM → BT_LO → BT_SSD
- ✅ **PUT** - Size-based routing to appropriate tiers  
- ✅ **REMOVE** - Cross-tier deletion with consistency
- 🚧 **SCAN** - Framework ready (implementation pending)

### Multi-Tier B+Tree Testing
- ✅ **BT_DRAM** - Tiny objects (≤128B) and Medium objects (129-2048B)
- ✅ **BT_LO** - Large objects (>2048B) with SSD storage pointers
- ✅ **BT_SSD** - Super-leaf pages with 4KB sub-page structure

### Advanced Features
- ✅ **Multi-threading** - 8 concurrent threads with 100 objects each
- ✅ **Eviction Logic** - DRAM → SSD when memory >90% full
- ✅ **Promotion Logic** - SSD → DRAM for hot objects
- ✅ **Tombstone Invalidation** - Cross-tier consistency management
- ✅ **Thread Safety** - Read-write locks and mutex protection

## 🚀 **Quick Start**

### Prerequisites
```bash
# Ensure SSD is mounted
sudo mount /dev/nvme2n1p3 /mnt/zipcache_test

# Setup test environment
make setup-ssd

# Install dependencies (if needed)
make install-deps
```

### Running Tests
```bash
# Run comprehensive test suite
make comprehensive

# Run all tests (basic + comprehensive)
make test-all

# Performance benchmark with timing
make benchmark

# Memory leak detection
make memcheck

# Thread safety analysis  
make threadcheck
```

## 📋 **Test Cases**

### 1. Deliberate B+Tree Tier Placement
**Purpose:** Verify objects are placed in correct tiers based on size
- **Tiny Objects (64B)** → BT_DRAM
- **Medium Objects (1KB)** → BT_DRAM  
- **Large Objects (4KB)** → BT_LO
- **Verification:** Statistics confirm tier-specific hits

### 2. Cross-Tier Operations
**Purpose:** Test operations that span multiple tiers
- Small → Large object replacement (BT_DRAM → BT_LO)
- Tombstone creation and invalidation logic
- Cross-tier DELETE operations
- **Key Test:** Same key transitions between tiers correctly

### 3. SCAN Operation Framework
**Purpose:** Prepare for range query implementation
- Setup: 30 objects across all tiers (0-9: tiny, 10-19: medium, 20-29: large)
- **Status:** Framework ready, awaiting SCAN implementation
- **TODO:** `zipcache_scan(cache, key1, key2, results, max_count)`

### 4. Multi-Threading Test  
**Purpose:** Verify thread safety under concurrent access
- **Configuration:** 8 threads × 100 objects = 800 total operations
- **Workload:** Mixed PUT/GET/DELETE operations
- **Verification:** Cache consistency maintained, no race conditions

### 5. Eviction and Promotion Logic
**Purpose:** Test memory management policies
- **Setup:** Small DRAM (4MB) to force eviction
- **Test Eviction:** Fill beyond 90% threshold, verify background eviction
- **Test Promotion:** Access evicted objects, verify SSD → DRAM promotion
- **Verification:** Statistics show eviction/promotion counts

### 6. Stress Test with Mixed Workload
**Purpose:** Validate system under heavy mixed operations
- **Workload:** 1000 operations (60% PUT, 30% GET, 10% DELETE)
- **Objects:** Mixed sizes across all tiers
- **Metrics:** Success rates, hit rates, consistency validation

## 📊 **Expected Results**

### Performance Metrics
- **PUT Success Rate:** >95% for all object types
- **GET Success Rate:** >90% with proper cache hits
- **DELETE Success Rate:** >95% with proper cleanup
- **Thread Safety:** No race conditions or consistency violations

### Cache Statistics Verification
```
DRAM hits: >0 (tiny/medium object access)
LO hits: >0 (large object access)  
SSD hits: >0 (promoted objects)
Evictions: >0 (when DRAM full)
Promotions: >0 (SSD → DRAM movement)
Tombstones: >0 (invalidation operations)
```

### Memory Management
- **Eviction Trigger:** When DRAM usage >90%
- **Eviction Target:** 10% of DRAM capacity per cycle
- **Promotion Policy:** Tiny/medium objects only
- **Consistency:** Cache validation passes throughout

## 🔧 **Test Configuration**

### Environment Settings
```c
#define TEST_DRAM_SIZE_MB       32      /* Small to trigger eviction */
#define TEST_SSD_PATH           "/mnt/zipcache_test/zipcache_test"
#define TEST_THREADS            8       /* Multi-threading test */
#define TEST_OBJECTS_PER_THREAD 100
```

### Object Size Definitions
```c
#define TINY_OBJECT_SIZE        64      /* Forces BT_DRAM */
#define MEDIUM_OBJECT_SIZE      1024    /* Forces BT_DRAM */
#define LARGE_OBJECT_SIZE       4096    /* Forces BT_LO */
```

## 🐛 **Debugging**

### Debug Output
Tests run with `zipcache_set_debug(1)` for detailed logging:
```
[ZipCache] PUT operation: key='test_key', size=64
[ZipCache] Object classified as: TINY
[ZipCache] → Routing to DRAM tier  
[ZipCache] ✓ DRAM tier PUT successful
```

### Common Issues

**SSD Mount Issues:**
```bash
# Check if SSD is mounted
mountpoint /mnt/zipcache_test

# Mount SSD if needed
sudo mount /dev/nvme2n1p3 /mnt/zipcache_test
```

**Permission Issues:**
```bash
# Fix write permissions
sudo chmod 777 /mnt/zipcache_test

# Test write access
touch /mnt/zipcache_test/test_file
```

**Memory Issues:**
```bash
# Check available memory
free -h

# Run with smaller DRAM size if needed
# Edit TEST_DRAM_SIZE_MB in test file
```

## 🔍 **Validation Tools**

### Memory Leak Detection
```bash
make memcheck
# Uses valgrind to detect memory leaks
# Should show "All heap blocks were freed"
```

### Thread Safety Analysis
```bash
make threadcheck  
# Uses helgrind to detect race conditions
# Should show no threading errors
```

### Performance Profiling
```bash
make benchmark
# Shows timing for each test phase
# Useful for performance regression testing
```

## 📈 **Success Criteria**

### Functional Requirements
- [ ] All objects placed in correct B+Tree tiers
- [ ] Cross-tier operations maintain data consistency
- [ ] Multi-threading produces no race conditions
- [ ] Eviction reduces DRAM usage when threshold exceeded
- [ ] Promotion works for SSD → DRAM object movement
- [ ] Statistics accurately reflect system state

### Performance Requirements
- [ ] GET operations complete within reasonable time
- [ ] PUT operations succeed >95% of the time
- [ ] Multi-threaded access maintains system stability
- [ ] Background eviction doesn't block foreground operations
- [ ] Cache hit rate >60% for mixed workloads

### System Requirements
- [ ] Cache consistency validation passes
- [ ] No memory leaks detected
- [ ] Thread safety validation passes
- [ ] SSD storage operations complete successfully
- [ ] Error handling works for invalid operations

## 🚧 **Next Steps**

### Immediate TODOs
1. **Implement SCAN Operation**
   - Add `zipcache_scan()` function to `zipcache.c`
   - Test range queries across all tiers
   - Verify sorted result merging

2. **Enhance Size Tracking**
   - Improve object size tracking in GET operations
   - Better verification of retrieved object sizes
   - Size-based promotion decisions

3. **Optimize Eviction Logic**
   - Replace simulation with actual page migration
   - Implement page-based DRAM → SSD eviction
   - Better integration with super-leaf pages

### Future Enhancements
- Hash-based leaf page organization testing
- Decompression early termination verification  
- CSD-3310 hardware integration tests
- Compression ratio measurement and validation
- Write amplification reduction verification

---

**Test Suite Status:** ✅ Ready to run  
**Target Platform:** Linux with NVMe SSD support  
**Dependencies:** pthread, valgrind (optional)  
**Estimated Runtime:** 30-60 seconds for full suite