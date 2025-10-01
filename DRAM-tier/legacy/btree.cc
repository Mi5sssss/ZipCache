#include "btree.h"
// #include "helper.h"
// test

using namespace std;

#include <iostream>
#include <sys/resource.h>

void print_memory_usage() {
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    std::cout << "Memory usage: " << usage.ru_maxrss << " KB" << std::endl;
}

bplustree::bplustree() {
    root = new bplustree_leaf_node();
    memset(root, 0, sizeof(struct bplustree_leaf_node));
    root -> next = NULL;
    root -> is_leaf = true;
    root -> key_count = 0;
    root -> rwlatch_ = new RWMutex();
}

void * bplustree::search(struct bplustree * t,
    const unsigned char * key, int key_len) {
    LOG("Get key=" << std::string((const char * ) key, key_len));
    std::stack < struct bplustree_node * > ancestors;
    struct bplustree_leaf_node * pleaf =
        (struct bplustree_leaf_node * ) LeafSearch(t, key, key_len, ancestors);
    // cout << "search key is " << key << endl;
    // printLeaf(pleaf);
    
    // cout << "pleaf is " << reinterpret_cast<uintptr_t>(pleaf) << endl;
   
   while(!ancestors.empty()){
        cout << ancestors.top() << endl;
        ancestors.pop();
    }
    // second chance 
    pleaf -> second_chance = 1;
    char * value = NULL;
    int idx = 0;
    volatile uint64_t v = 0;
    int ret = -1;
    if (!pleaf) {
        LOG("could not find key");
        return NULL;
    }

    int keycount = pleaf -> key_count;

    // RX
    char* kv_address = nullptr;
    kv_address = isInLeaf(pleaf, key);
    // cout << "kv_address is " << reinterpret_cast<uintptr_t>(kv_address) << endl;
    struct kv_metadata metadata = {0};

    // cout << "kv_address is " << (void*)kv_address << endl;
    // cout << "kv_address == nullptr is " << (kv_address == nullptr) << endl;


    // if (kv_address == nullptr) {
    //     // cout << "could not find key in landing buffer or subpages" << endl;
        
    //     #ifdef USE_READ_LOCK
    //     pleaf -> rwlatch_ -> RUnlock(); // 2020-02-19
    //     #endif

    //     return nullptr;

    // } else {
        
    //     struct kv_metadata metadata = {0};
    //     memcpy(&metadata, kv_address, sizeof(metadata));
    //     value = new char[metadata.value_size+1];
    //     memcpy(value, kv_address + sizeof(metadata), metadata.value_size);
    //     value[metadata.value_size] = '\0';
    //     char* key = new char[KEY_SIZE+1];
    //     key[KEY_SIZE] = '\0';
    //     memcpy(key, metadata.key, KEY_SIZE);
    //     // cout << "value is " << (char*)value << endl;
    //     // cout << "key is " << (char*)key << endl;
    //     // cout << "find the value"<< value << endl;

    //     #ifdef USE_READ_LOCK
    //     pleaf -> rwlatch_ -> RUnlock(); // 2020-02-19
    //     #endif

    //     return value;
    // }

    // 侧滑=
    if (kv_address == nullptr) {
        struct bplustree_leaf_node * next = (struct bplustree_leaf_node * ) pleaf -> next;
        bool retry = false;
        // cout << "ret < 0 in search() show in single thread" << endl;
        while (next && (memcmp((void * ) key, (void * ) next -> minimum_key, KEY_SIZE) > 0)) {
            retry = true;
            pleaf = next;
            cout << "next ret < 0 in search() not show in single thread" << endl;
            next = (struct bplustree_leaf_node * ) pleaf -> next;
        }
    }
    else {
        metadata = {0};
        memcpy(&metadata, kv_address, sizeof(metadata));
        value = new char[metadata.value_size+1];
        // value[metadata.value_size+1] = {0};
        memcpy(value, kv_address + sizeof(metadata), metadata.value_size);
        // value[metadata.value_size] = '\0';
        // char* key = new char[KEY_SIZE+1];
        // char key[KEY_SIZE + 1] = {0};
        // key[KEY_SIZE] = '\0';
        // memcpy(key, metadata.key, KEY_SIZE);
        // return value;
        // cout << "value is " << value << endl;

    }

    #ifdef USE_READ_LOCK
    // cout << "unlock read lock here " <<this_thread::get_id()<< endl;
    pleaf -> rwlatch_ -> RUnlock(); // 2020-02-19
    #endif

    if (nullptr != kv_address) {
    // if (nullptr != value) {
        return value;
    }
    // if(kv_address != nullptr){
    //     return kv_address;
    // }

    LOG("cannot find");
    // cout << "cannot find" << endl;
    return NULL;
}


bool bplustree::FindNext(const unsigned char * key, int key_len,
    struct bplustree_node * node,
    struct bplustree_node ** next) {

    const int keycount = node -> key_count;
    int idx = 0;
    LOG("FindNext keycount = " << keycount);
    for (idx = 0; idx < keycount; idx++) {
        unsigned char * router_key =
            (unsigned char * )((struct bplustree_index_node * ) node) -> slots[idx].key;

        if (memcmp((void * ) router_key, (void * ) key, KEY_SIZE) >= 0) {
            break;
        }
    }

    if (idx < keycount) {
        LOG("ptr = " << (void * )((struct bplustree_index_node * ) node) -> slots[idx].ptr);
        * next = ((struct bplustree_index_node * ) node) -> slots[idx].ptr;
        return true;
    }

    struct bplustree_node * sibling = node -> next;

    if (sibling && (memcmp((void * ) key,
            (void * )((struct bplustree_index_node * ) sibling) -> slots[0].key,
            KEY_SIZE) >= 0)) {
        // cout << "sibling not show in single thread" << endl;
        * next = sibling;
        return false;
    }

    * next = ((struct bplustree_index_node * ) node) -> slots[keycount].ptr;
    return true;

}

bplustree::bplustree_node * bplustree::LeafSearch(struct bplustree * t,
    const unsigned char * key, int key_len,
        std::stack < struct bplustree_node * > & ancestors)

{

    #ifdef USE_READ_LOCK
    t -> root -> rwlatch_ -> RLock(); // 2020-02-19
    #endif

    struct bplustree_node * pnode = t -> root;
    bool move_downwards = false;
    struct bplustree_node * next = NULL;
    if (!pnode) {
        return NULL;
    }
    #ifdef INDEX_STATS
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    #endif

    #ifdef USE_PREFETCH
    bplustree_node_prefetch(pnode);
    #endif
    while (!pnode -> is_leaf) {

        LOG("searchleaf access " << pnode);
        move_downwards = FindNext(key, key_len, pnode, & next);
        assert(next);
        // search() don't need this opt
        // if(move_downwards) ancestors.push(pnode);
        #ifdef USE_READ_LOCK
        next -> rwlatch_ -> RLock(); // 2020-02-19
        pnode -> rwlatch_ -> RUnlock(); // 2020-02-19
        #endif
        pnode = next;
        #ifdef USE_PPREFETCH
        bplustree_node_prefetch(pnode);
        #endif
        assert(pnode);
    }

    #ifdef INDEX_STATS
    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast < std::chrono::nanoseconds > (t2 - t1).count();
    search_time += duration;
    #endif
    return pnode;
}

bplustree::bplustree_node * bplustree::LeafSearchForInsert(struct bplustree * t,
    const unsigned char * key, int key_len,
        std::stack < struct bplustree_node * > & ancestors)

{
    #ifdef USE_WRITE_LOCK
    // cout << "*************************" << endl;
    // cout << "Wlock root " << t->root << endl;
    t -> root -> rwlatch_ -> WLock(); // 2020-02-20
    #endif
    // int a = sizeof(struct kv_metadata);
    // uint8_t kv_size = sizeof(struct kv_metadata) + strlen(value);
    
    struct bplustree_node * pnode = t -> root;
    bool move_downwards = false;
    struct bplustree_node * next = NULL;
    if (!pnode) {
        return NULL;
    }
    // cout << "**************" << endl;

    while (!pnode -> is_leaf) {
        // cout << "find pnode is not leaf "<< endl;
        move_downwards = FindNext(key, KEY_SIZE, pnode, & next);
        // cout << "pnode -> is_leaf " << pnode -> is_leaf << endl;
        
        assert(next);
        if (move_downwards) {
            // 检查该节点是否安全，如果安全, 释放该节点所有祖先节点的写锁
            // 节点安全定义如下：如果对于插入操作，如果再插入一个元素，不会产生分裂
            // need to modify here
            // cout << " pnode -> key_count " << pnode -> key_count << endl;
            // cout << "ancestors.size() " << ancestors.size() << endl;
            
            // need to modify here
            if (pnode -> key_count < NUM_LEAF_NODE_KEYS) {
                // cout << "debug: release the ancestors lock "<< endl;
                // cout << "ancestors.size() " << ancestors.size() << endl;
                
                while (ancestors.size()) {
                    auto parent = ancestors.top();
                    ancestors.pop();

                    #ifdef USE_WRITE_LOCK
                    // cout << "WUnlock in the ancestors " << parent << endl;
                    parent -> rwlatch_ -> WUnlock();
                    #endif

                }
            }
            // cout << "the pushed node into ancestors is " << pnode << endl;
            ancestors.push(pnode);
            // cout << "ancestors.size() " << ancestors.size() << endl;
        }
        // Modified and Added WUnlock here
        // else{
        //     #ifdef USE_WRITE_LOCK
        //         pnode -> rwlatch_ -> WUnlock(); // 2020-02-20
        //     #endif
        // }
        #ifdef USE_WRITE_LOCK
        // cout << "WLock next " << next << endl;
        next -> rwlatch_ -> WLock(); // 2020-02-20
        
        #endif
        pnode = next;
        assert(pnode);
    }
    // cout << "pnode now is leaf node now " << endl;
    return pnode;
}

bplustree::bplustree_node * bplustree::LeafSearchForDelete(struct bplustree * t, 
    const unsigned char * key, int key_len,
    std::stack < struct bplustree_node * > & ancestors_for_lock, std::stack < struct bplustree_node * > & ancestors_full){
    
    #ifdef USE_WRITE_LOCK
    t -> root -> rwlatch_ -> WLock();
    #endif
    
    struct bplustree_node * pnode = t -> root;
    bool move_downwards = false;
    struct bplustree_node * next = NULL;
    if (!pnode) {
        return NULL;
    }

    while (!pnode -> is_leaf) {
        move_downwards = FindNext(key, KEY_SIZE, pnode, & next);
        
        assert(next);
        if (move_downwards) {
            
            if (pnode -> key_count >= NUM_LEAF_NODE_KEYS / 2) {
                
                while (ancestors_for_lock.size()) {
                    auto parent = ancestors_for_lock.top();
                    ancestors_for_lock.pop();

                    #ifdef USE_WRITE_LOCK
                    parent -> rwlatch_ -> WUnlock();
                    #endif

                }
            }
            ancestors_for_lock.push(pnode);
            ancestors_full.push(pnode);
        }

        #ifdef USE_WRITE_LOCK
        next -> rwlatch_ -> WLock();
        
        #endif
        pnode = next;
        assert(pnode);
    }
    return pnode;
}

// Find the left node get the total key count
int bplustree::findTotalKeyCount(struct bplustree * t) {
    t -> total_keys = 0;
    t -> total_nodes = 0;
    t -> total_compressed_bytes = 0;
    t -> total_btree_size = 0;

    if (t -> root -> is_leaf == false) {
        bplustree_index_node * node = (bplustree_index_node * ) t -> root;
        while (node -> is_leaf == false) {
            node = (bplustree_index_node * ) node -> slots[0].ptr;
        }
        while (node -> next != 0) {
            bplustree_leaf_node * node_ = (bplustree_leaf_node * ) node;
            if(node_ -> compress_size != 0){
                t -> total_keys += node_ -> key_count;
                t -> total_nodes += 1;
                t -> total_compressed_bytes += node_ -> compress_size;
                t -> total_btree_size += sizeof( * node_);
            }
            node = (bplustree_index_node * ) node -> next;
        }
    }
    cout << "total count of key is " << t -> total_keys << endl;
    cout << "total count of leaf node is " << t -> total_nodes << endl;
    cout << "total count of compressed bytes is " << t -> total_compressed_bytes << endl;
    cout << "total count of btree size is " << t -> total_btree_size << endl;
    cout << "Averange number of key value pairs inside per leaf node " << t -> total_keys / t -> total_nodes << endl;
    return 0;
}

// randomly get the evict node in buffer tree
struct bplustree::bplustree_node * bplustree::getEvictNode_(struct bplustree * t) {
    if (t -> root -> is_leaf == false) {
        bplustree_index_node * node = (bplustree_index_node * ) t -> root;
        while (node -> is_leaf == false) {
            int random_access = rand() % node->key_count;
            while (node -> slots[random_access].ptr == 0) {
                random_access = rand() % node->key_count;
            }
            node = (bplustree_index_node * ) node -> slots[random_access].ptr;
        }
        if (node -> is_leaf == true) {
            bplustree_leaf_node * leaf = (bplustree_leaf_node * ) node;
            while (leaf -> second_chance == 1 || leaf->compress_size == 0) {
                leaf = (bplustree_leaf_node * ) leaf -> next;
                // cout << "next here " << reinterpret_cast<uintptr_t>(leaf)<< endl;
            }
            return leaf;
        }
    } else {
        cout << "the root is leaf node" << endl;
    }
}

void bplustree::redistributeSubpages(bplustree_leaf_node *p_leaf, char subpages[]){

    cout << "********** Start Redistribute **********" << endl;

    if(p_leaf->num_subpages > 2){
        int new_num_subpages = p_leaf->num_subpages / 2;
        cout << "debug: new_num_subpages "<< new_num_subpages << endl;
        // int new_subpage_bytes = TOTAL_SUBPAGES_BYTES / new_num_subpages;
        p_leaf->num_subpages = new_num_subpages;
        
        // Record bounds of subpages
        int16_t subpage_bound[p_leaf->num_subpages];
        for(int i = 0; i < p_leaf->num_subpages; i++){
            subpage_bound[i] = TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages * i;
        }
        
        // Fetch key value pairs in each subpage, store them into sort buffer.
        std::vector<string> sort_buffer;
        struct kv_metadata metadata = {0};
        char zeros[sizeof(metadata)] = {0};
        for(int i = 0; i < p_leaf->num_subpages; i++){
            // Initilize p
            char* p = subpages + subpage_bound[i];
            while(p < subpages + subpage_bound[i] + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages){
                metadata = {0};
                if(p + sizeof(metadata) <= subpages + subpage_bound[i] + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages){
                    memcpy(&metadata, p, sizeof(metadata));
                }
                
                // Check bound
                if(p + sizeof(metadata) + metadata.value_size <= subpages + subpage_bound[i] + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages){
                    if(memcmp(&metadata, zeros, sizeof(metadata)) != 0){
                        string buffer(p, p + sizeof(metadata) + metadata.value_size);
                        sort_buffer.push_back(buffer);
                    }
                    else{
                        // p += sizeof(metadata) + metadata.value_size;
                        break;
                    }
                }
                p += sizeof(metadata) + metadata.value_size;
            }
        }
        // printLeaf(p_leaf);

        memset(subpages, 0, TOTAL_SUBPAGES_BYTES);

        // sort(sort_buffer.begin(), sort_buffer.end(), compareMetadata);
        
        // char zeros[sizeof(metadata)] = {0};
        // cout << "sort_buffer.size() " << sort_buffer.size() << endl;
        for(int i = 0; i < sort_buffer.size(); i++){
            int hash_result = (hash < string > {}((sort_buffer[i]))) % p_leaf->num_subpages;
            cout << "sort_buffer["<<i<<"] " << sort_buffer[i] << endl;
            char* p = subpages + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages * hash_result;
            while(p < subpages + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages * (hash_result + 1)){
                metadata = {0};
                if(memcmp(p, zeros, sizeof(metadata)) == 0){
                    memcpy(&metadata, sort_buffer[i].data(), sizeof(metadata));
                    memcpy(p, sort_buffer[i].data(), sizeof(metadata) + metadata.value_size);
                    p += sizeof(metadata) + metadata.value_size;
                    break;
                }
                p += sizeof(metadata) + metadata.value_size;
                // printLeaf(p_leaf);
            }
        }
        // printLeaf(p_leaf);
    }
    else{
        cout << "num_subpages = " << p_leaf->num_subpages << ", need to split" << endl;
    }
    cout << "********** End Redistribute **********" << endl;
    
}

void bplustree::compressSubpagesToLeaf(bplustree_leaf_node *p_leaf, char subpages[]){
    // cout << "***compress:***" << endl;
    // std::cout << "p_leaf for compress: " << reinterpret_cast<uintptr_t>(p_leaf) << std::endl;
    int max_compress_size = LZ4_compressBound(TOTAL_SUBPAGES_BYTES);
    char buffer1[max_compress_size];
    memset(buffer1, 0, max_compress_size);
    int compress_size = LZ4_compress_default((char*)subpages, buffer1, TOTAL_SUBPAGES_BYTES, max_compress_size);
    // cout << "debug: compress_size " << compress_size << endl;
    // cout << "p_leaf->compress_data old address: " << reinterpret_cast<uintptr_t>(p_leaf->compress_data) << endl;
    if(compress_size > 0){
        // if(p_leaf->compress_size != 0){delete[] p_leaf->compress_data;}
        if(p_leaf->compress_data != NULL && p_leaf->compress_size != 0){
            // cout << "p_leaf->compress_size is " << p_leaf->compress_size << endl;
            // cout << "p_leaf->compress_data old address: " << reinterpret_cast<uintptr_t>(p_leaf->compress_data) << endl;
            delete[] p_leaf->compress_data;
            // p_leaf->compress_data = nullptr;
            // cout << "delete sucessfully " << endl;
        }
        p_leaf->compress_data = new char[compress_size];
        // cout << "p_leaf->compress_data new initialized address: " << reinterpret_cast<uintptr_t>(p_leaf->compress_data) << endl;
        memcpy(p_leaf->compress_data, buffer1, compress_size);
        p_leaf->compress_size = compress_size;
    }
    else{
        cerr << "Error: Compression failed!" << endl;
    }
    // cout << "***compress end***" << endl;
}



void bplustree::decompressLeafToSubpages(bplustree_leaf_node *p_leaf, char subpages[]){
    // cout << "***decompress:***" << endl;
    // std::cout << "p_leaf for decompress: " << reinterpret_cast<uintptr_t>(p_leaf) << std::endl;
    // std::cout << "p_leaf->compress_data: " << reinterpret_cast<uintptr_t>(p_leaf->compress_data) << std::endl;
    // std::cout << "p_leaf->compress_size: " << p_leaf->compress_size << std::endl;
    // std::cout << "Total subpages bytes: " << TOTAL_SUBPAGES_BYTES << std::endl;
    int decompress_size = LZ4_decompress_safe(p_leaf->compress_data, (char *) subpages, p_leaf -> compress_size, TOTAL_SUBPAGES_BYTES);
    // int decompress_size = LZ4_decompress_safe(p_leaf->compress_data + sizeof(uint64_t), (char *) subpages, p_leaf->compress_size - sizeof(uint64_t), TOTAL_SUBPAGES_BYTES);

    if (decompress_size <= 0) {
        std::cerr << "Error: Decompression failed!" << std::endl;
    }
    // cout << "***decompress end***" << endl;
}

// Deprecated
// Insert Key into Landing Buffer
// If Landing Buffer is Full, Hash Keys to Subpages
// If Subpages are Full, Insertion Fails
// No Split in this Function
int bplustree::insertHashNode(bplustree_leaf_node * p_leaf, char * value, kv_metadata metadata) {
    // cout << "****************************************************" << endl;
    // cout << "p_leaf is " << reinterpret_cast<uintptr_t>(p_leaf) << endl;
    // cout << "key inserted is " << metadata.key << endl;
    // cout << "value is "<< value << endl;
    // cout << "value length is " << strlen(value) << endl;
    // printf("Size of struct kv_metadata: %lu bytes\n", sizeof(metadata));
    // printf("key: %s\n", metadata.key);
    // printf("value_type: %u\n", metadata.value_type);
    // printf("value_size: %u\n", metadata.value_size);
    // std::cout << "start the insertHashNode p_leaf->compress_data: " << reinterpret_cast<uintptr_t>(p_leaf->compress_data) << std::endl;
    if ((memcmp((char * ) metadata.key, p_leaf -> minimum_key, KEY_SIZE) < 0) &&
        p_leaf -> minimum_key[0] != '\0') {
        memcpy((char * ) p_leaf -> minimum_key, (char * ) metadata.key, KEY_SIZE);
    } else if (p_leaf -> minimum_key[0] == '\0') {
        memcpy((char * ) p_leaf -> minimum_key, (char * ) metadata.key, KEY_SIZE);
    }

    if ((memcmp((char * ) metadata.key, p_leaf -> maximum_key, KEY_SIZE) > 0) &&
        p_leaf -> maximum_key[0] != '\0') {
        memcpy((char * ) p_leaf -> maximum_key, (char * ) metadata.key, KEY_SIZE);
    } else if (p_leaf -> maximum_key[0] == '\0') {
        memcpy((char * ) p_leaf -> maximum_key, (char * ) metadata.key, KEY_SIZE);
    }
        
    char* p = p_leaf->landing_buffer;

    // if(memcmp(p_leaf -> minimum_key, "tcbxdxez", KEY_SIZE)==0) {
    //     cout << p_leaf->minimum_key<<endl;
    //     cout << p_leaf->maximum_key<<endl;
    // }
    
    unsigned char zeros[KEY_SIZE] = {0};
    struct kv_metadata metadata_buffer = {0};
    // cout << "debug in start: p_leaf -> num_subpages "<< p_leaf -> num_subpages << endl;
    while(p < p_leaf->landing_buffer + LANDING_BUFFER_BYTES){
        memset(&metadata_buffer, 0, sizeof(metadata_buffer));
        if(p + sizeof(metadata_buffer) <= p_leaf->landing_buffer + LANDING_BUFFER_BYTES){
            memcpy(&metadata_buffer, p, sizeof(metadata_buffer));
        }
        
        if(p + sizeof(metadata) + metadata.value_size <= p_leaf->landing_buffer + LANDING_BUFFER_BYTES){
            // Update the exist value, need to modify, consider the case of different size value update
            
            // std::cout.write(p_leaf->landing_buffer, sizeof(p_leaf->landing_buffer));
            // cout << "\nmetadata_buffer.key is " << metadata_buffer.key << endl;
            // cout << "metadata.key is " << metadata.key << endl;
            
            
            if(memcmp(metadata_buffer.key, metadata.key, KEY_SIZE) == 0){
                // cout << "Update landing buffer " << endl;
                // cout << "metadata_buffer.key is "<<metadata_buffer.key<<" and metadata.key is "<< metadata.key<<endl;
                // metadata_buffer.value_size = strlen(value);
                total_updates++;
                memcpy(p + sizeof(metadata_buffer), value, metadata_buffer.value_size);
                // memcpy(p, &metadata_buffer, sizeof(metadata_buffer));
                p += sizeof(metadata_buffer) + metadata_buffer.value_size;
                break;
            } 
            // Append to the vacant position
            else if(memcmp(metadata_buffer.key, zeros, KEY_SIZE) == 0){
                metadata_buffer.value_size = strlen(value);
                memcpy(metadata_buffer.key, metadata.key, KEY_SIZE);

                memcpy(p, &metadata_buffer, sizeof(metadata_buffer));
                // cout << "debug in starting append landing buffer: p_leaf -> num_subpages "<< p_leaf -> num_subpages << endl;
                memcpy(p + sizeof(metadata_buffer), value, metadata_buffer.value_size);
                
                p += sizeof(metadata_buffer) + metadata_buffer.value_size;
                // cout << "p address: " << reinterpret_cast<uintptr_t>(p) << endl;
                // cout << "p_leaf->landing_buffer + LANDING_BUFFER_BYTES " << reinterpret_cast<uintptr_t>(p_leaf->landing_buffer + LANDING_BUFFER_BYTES) << endl;
                p_leaf->key_count += 1;
                // cout << "debug in appending landing buffer: p_leaf -> num_subpages "<< p_leaf -> num_subpages << endl;
                // std::cout << "*** p_leaf->compress_data in landing buffer found vacant: " << reinterpret_cast<uintptr_t>(p_leaf->compress_data) << std::endl;
                break;
            }
            // else{
            //     cout << "hey! "<< endl;
            //     cout << "p is " << p << endl;
            // }
            
        }
        p += sizeof(metadata_buffer) + metadata_buffer.value_size;
        // std::cout << "p in landing buffer: " << reinterpret_cast<uintptr_t>(p) << std::endl;
        // cout << "debug in inserting landing buffer: p_leaf -> num_subpages "<< p_leaf -> num_subpages << endl;
        
    }
    // std::cout << "--- p_leaf->compress_data after landing buffer: " << reinterpret_cast<uintptr_t>(p_leaf->compress_data) << std::endl;
    // cout << "p_leaf address: " << reinterpret_cast<uintptr_t>(p_leaf) << endl;
    // cout << "p_leaf->num_subpages address: " << reinterpret_cast<uintptr_t>(&p_leaf->num_subpages) << endl;
    // cout << "p_leaf->key_count " << p_leaf->key_count << endl;
    // cout << "p address: " << reinterpret_cast<uintptr_t>(p) << endl;
    // cout << "metadata_buffer address: " << reinterpret_cast<uintptr_t>(&metadata_buffer) << endl;
    // cout << "p_leaf->landing_buffer + LANDING_BUFFER_BYTES: " << reinterpret_cast<uintptr_t>(p_leaf->landing_buffer + LANDING_BUFFER_BYTES) << endl;



    // cout << "debug in insertHashNode: p_leaf -> num_subpages "<< p_leaf -> num_subpages << endl;
    // std::cout << "p_leaf->compress_data: " << reinterpret_cast<uintptr_t>(p_leaf->compress_data) << std::endl;
    // Landing buffer is full, start hashing
    if(p + sizeof(metadata_buffer) + metadata_buffer.value_size >= p_leaf->landing_buffer + LANDING_BUFFER_BYTES){
        // cout << "hashing start !!!!!!!!!!! "<< endl;
        // std::cout << "p_leaf->compress_data: " << reinterpret_cast<uintptr_t>(p_leaf->compress_data) << std::endl;
        // cout << "hashing start..." << endl;

        // Record bounds of subpages
        // cout << "pleaf is " << p_leaf << endl;
        // cout << "p_leaf->num_subpages is " << p_leaf->num_subpages << endl;
        // printLeaf(p_leaf);

        int16_t subpage_bound[p_leaf->num_subpages] = {0};
        for(int i = 0; i < p_leaf->num_subpages; i++){
            subpage_bound[i] = TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages * i;
        }

        char subpages[TOTAL_SUBPAGES_BYTES] = {0};

        //********************************************************************//
        const int COMPRESSED_SIZE = LZ4_compressBound(TOTAL_SUBPAGES_BYTES);
        // cout << "debug: p_leaf -> compress_data " << p_leaf -> compress_data << endl;
        if (p_leaf->compress_data != nullptr) {
        // if (p_leaf->compress_size != 0) {
        // if (p_leaf->compress_data != NULL && p_leaf->compress_size != 0) {
            // cout << "p_leaf -> compress_data " << p_leaf -> compress_data << endl;
            // cout << "debug: p_leaf -> compress_size " << p_leaf -> compress_size << endl;
            // cout << "*** decompress in the inserting ***" << endl;
            // cout << "p_leaf->compress_data in decompress address: " << reinterpret_cast<uintptr_t>(p_leaf->compress_data) << endl;
            // cout << "key and value are " << metadata.key << " " << value << endl; 
            // cout << "Lets decompress " << endl;
            // cout << "p_leaf is " << p_leaf << endl;
            decompressLeafToSubpages(p_leaf, subpages);
        } 
        //********************************************************************//        
    
        char* p1 = p_leaf->landing_buffer;
        // cout << "p1 initilized " << reinterpret_cast<uintptr_t>(p1) << endl;
        struct kv_metadata metadata1 = {0};
        struct kv_metadata metadata2 = {0};
        while(p1 < p_leaf->landing_buffer + LANDING_BUFFER_BYTES){
// cout << "debug p1 is " << reinterpret_cast<uintptr_t>(p1) << endl;
// cout << "debug p_leaf->landing_buffer + LANDING_BUFFER_BYTES is " << reinterpret_cast<uintptr_t>(p_leaf->landing_buffer + LANDING_BUFFER_BYTES) << endl;
            // Check landing buffer bound
            memset(&metadata1, 0, sizeof(metadata));
            if(p1 + sizeof(metadata1) < p_leaf->landing_buffer + LANDING_BUFFER_BYTES){
                memcpy(&metadata1, p1, sizeof(metadata1));
            }

            if(p1 + sizeof(metadata1) + metadata1.value_size < p_leaf->landing_buffer + LANDING_BUFFER_BYTES){
                // Hash key and get which subpage to insert
                string str(metadata1.key, metadata1.key + KEY_SIZE);

                int hash_result = (hash < string > {}((str))) % p_leaf->num_subpages;
                // cout << "str " << str << "hash result "<< hash_result << endl;
                // cout << "hashing..." <<endl;
                // cout <<"metadata1.key is " << metadata1.key << endl;
                // cout << "sizeof value is " << metadata1.value_size << endl;
                // cout << "hashing value is " << p1+sizeof(metadata1) << endl;
                // printChars(p1, sizeof(metadata1)+metadata1.value_size);
                
                char* p2 = &subpages[subpage_bound[hash_result]];
                unsigned char zeros[KEY_SIZE] = {0};
                // cout << "p_leaf->num_subpages 1 " << p_leaf->num_subpages << endl; 
                while(p2 < &subpages[subpage_bound[hash_result]] + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages){
                    // struct kv_metadata metadata2 = {0};
                    memset(&metadata2, 0, sizeof(metadata2));

                    if(p2 + sizeof(metadata1) < &subpages[subpage_bound[hash_result]] + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages){
                        memcpy(&metadata2, p2, sizeof(metadata2));
                        // cout << "metadata2.value_size is " << metadata2.value_size << endl;
                        // printChars(p2, sizeof(metadata2)+metadata2.value_size);
                    }
                    
                    if(p2 + sizeof(metadata1) + metadata1.value_size < &subpages[subpage_bound[hash_result]] + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages){
                        
                        // Update
                        if(memcmp(metadata2.key, metadata1.key, KEY_SIZE) == 0){
                            // cout << "Update subpages " << endl;
                            // memcpy(p, &metadata1, sizeof(metadata1));
                            // memcpy(&metadata2, &metadata1, sizeof(metadata2));
                            
                            // Need to modify here

                            // Omitting the rest chars of the updating value if updated value was longer than previous value
                            // memcpy(p2 + sizeof(metadata1), value, metadata2.value_size);
                            memcpy(p2 + sizeof(metadata1), value, metadata2.value_size);
                            // cout << "p_leaf->num_subpages 2 " << p_leaf->num_subpages << endl;
                            p2 += sizeof(metadata1) + metadata2.value_size;
                            break;
                        }
                        
                        // Find the vacant position and append KV pair
                        if(memcmp(metadata2.key, zeros, KEY_SIZE) == 0){
                            // cout << "got vacant position!" << endl;
                            // cout << "copy " << metadata1.key <<endl;
                            // cout << "str is " << str << " key " << metadata.key << " " << hash_result << endl;
                            memcpy(p2, &metadata1, sizeof(metadata1));
                            memcpy(p2 + sizeof(metadata1), value, metadata1.value_size);
                            // strcpy(p2 + sizeof(metadata1), value);
                            // cout << "hash_result is " << hash_result << endl;
                            // cout << "debug here key: " << p2  << endl;
                            // cout << "debug here value: " << p2 + sizeof(metadata1) << endl;
                            // cout << "key is " << metadata.key << endl;
                            // cout << "value is " << value << endl;
                            // cout << "sizeof metadata " << sizeof(metadata) << endl;

                            // cout << "metadata2.value_size " << metadata2.value_size << endl;
                            // cout << "p2 is " << reinterpret_cast<uintptr_t>(p2) << endl;
                            // cout << reinterpret_cast<uintptr_t>(p2 + sizeof(metadata2)) << endl;
                            // cout << "bound is " << reinterpret_cast<uintptr_t>(&subpages[subpage_bound[hash_result]] + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages) << endl;

                            // cout <<"debug subpages ";
                            // for(int i = 0; i<sizeof(subpages)/sizeof(subpages[0]);i++){
                            //     cout << subpages[i];
                            // }
                            // cout << endl;
                            p2 += sizeof(metadata1) + metadata1.value_size;
                            // cout << "p2 after increment " << reinterpret_cast<uintptr_t>(p2) << endl;
                            break;
                        }
                    }
                    // else{break;}
                    // cout << "p2 is " << reinterpret_cast<uintptr_t>(p2) << endl;
                    p2 += sizeof(metadata2) + metadata2.value_size;
                    // cout << "p2 after increment " << reinterpret_cast<uintptr_t>(p2) << endl;
                    // cout << "subpage bound is " << reinterpret_cast<uintptr_t>(&subpages + TOTAL_SUBPAGES_BYTES) << endl;
                    // cout << "hi " << endl;
                    
                }
                    // cout << "debug p2 is " << std::hex << std::showbase << reinterpret_cast<uintptr_t>(p2) << endl;
                    // cout << "debug subpage boundry is " << std::hex << std::showbase << reinterpret_cast<uintptr_t>(&subpages[subpage_bound[hash_result]] + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages) << endl;
                    // cout << "debug p2 is " << reinterpret_cast<uintptr_t>(p2) << endl;
                    // cout << "debug subpage boundry is " << reinterpret_cast<uintptr_t>(&subpages[subpage_bound[hash_result]] + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages) << endl;
                    
                if(p2 >= &subpages[subpage_bound[hash_result]] + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages){
                    // Redistribute the subpages, suppose redistribution will not cause redistribution again.
                    // cout << "*** Start redistribute ***" << endl;
                    // redistributeSubpages(p_leaf, subpages);
                    // memset(subpage_bound, 0, p_leaf->num_subpages * 2);
                    // for(int i = 0; i < p_leaf->num_subpages; i++){
                    //     subpage_bound[i] = TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages * i;
                    // }
                    // printLeaf(p_leaf);
                    // cout << "set flag 1 " << endl;
                    // Move content forword to overwrite the first few positions containing zeros (seems no influnce to the hit ratio)
                    // int num_zeros_to_rm = 0;
                    // while(p_leaf->landing_buffer[num_zeros_to_rm] == NULL) {
                    //     if(num_zeros_to_rm < LANDING_BUFFER_BYTES){
                    //         num_zeros_to_rm++;
                    //     }
                    //     else{
                    //         break;
                    //     }
                        
                    // }
                    // cout << "num_zeros_to_rm is " << num_zeros_to_rm << endl;
                    // std::memmove(p_leaf->landing_buffer, p1, num_zeros_to_rm);
                    // memset(p1, 0, p_leaf->landing_buffer + LANDING_BUFFER_BYTES - p1);
                    // printChars(p_leaf->landing_buffer, LANDING_BUFFER_BYTES);
                    p_leaf->is_subpage_full = 1;
                    break;
                }
                memset(p1, 0, sizeof(metadata1) + metadata1.value_size);
                
            }
            
            p1 += sizeof(metadata1) + metadata1.value_size;

            // cout <<"debug subpages ";
            // for(int i = 0; i<sizeof(subpages)/sizeof(subpages[0]);i++){
            //     cout << subpages[i];
            // }
            // cout << endl;
            
            // cout << "debug p1 is " << reinterpret_cast<uintptr_t>(p1) << endl;
            // cout << "debug p_leaf->landing_buffer + LANDING_BUFFER_BYTES is " << reinterpret_cast<uintptr_t>(p_leaf->landing_buffer + LANDING_BUFFER_BYTES) << endl;
        }
            // cout << "//********************************Start************************************//" << endl;
            // printLeaf(p_leaf);
            // cout << "//**********************************END**********************************//" << endl;

            // cout <<"debug subpages ";
            // for(int i = 0; i<sizeof(subpages)/sizeof(subpages[0]);i++){
            //     // cout << subpages[i];
            //     printf("%02X ", static_cast<unsigned char>(subpages[i]));
            // }
            // cout << endl;
        //********************************************************************//
        // cout << "here " << endl;
        compressSubpagesToLeaf(p_leaf, subpages);
        // printf("this is the test\n");
        // decompressLeafToSubpages(p_leaf, subpages);
        // std::cout << "p_leaf->compress_data: " << reinterpret_cast<uintptr_t>(p_leaf->compress_data) << std::endl;
        //********************************************************************//

        // cout <<"debug subpages ";
        // for(int i = 0; i<sizeof(subpages)/sizeof(subpages[0]);i++){
        //     cout << subpages[i];
        // }
        // cout << endl;
        // printLeaf(p_leaf);
    
    }
    // std::cout << "p_leaf->compress_data: " << reinterpret_cast<uintptr_t>(p_leaf->compress_data) << std::endl;
    return 0;
}

char* bplustree::isInLeaf(bplustree_leaf_node * p_leaf, const unsigned char * key) {

    // Check landing buffer
    char *p = p_leaf->landing_buffer;
    kv_metadata metadata = {0};
    while(p < p_leaf->landing_buffer + LANDING_BUFFER_BYTES){
        if(p + sizeof(metadata) <= p_leaf->landing_buffer + LANDING_BUFFER_BYTES){
            memcpy(&metadata, p, sizeof(metadata));
        }
        
        if(p + sizeof(metadata) + metadata.value_size <= p_leaf->landing_buffer + LANDING_BUFFER_BYTES){
            if(memcmp(metadata.key, key, KEY_SIZE) == 0){
                return p;
            }
        }
        p += sizeof(metadata) + metadata.value_size;
    }

    // Check subpages
    string str(key, key + KEY_SIZE);
    int hash_result = hash < string > {}(str) % p_leaf->num_subpages;

    char subpages_partial[TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages * (hash_result + 1)] = {0};
    // memset(subpages_partial, 0, TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages * (hash_result + 1));
    int bytes_written = LZ4_decompress_safe_partial(p_leaf->compress_data, subpages_partial, p_leaf->compress_size, TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages * (hash_result + 1), TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages * (hash_result + 1));
    // printChars(subpages_partial, TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages * (hash_result + 1));
    if(bytes_written > 0){
        char* p1 = subpages_partial + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages * (hash_result);
        while(p1 < subpages_partial + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages * (hash_result + 1)){
            // metadata = {0};
            memset(&metadata, 0, sizeof(metadata));
            if(p1 + sizeof(metadata) <= subpages_partial + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages * (hash_result + 1)){
                memcpy(&metadata, p1, sizeof(metadata));
            }
            if(p1 + sizeof(metadata) + metadata.value_size <= subpages_partial + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages * (hash_result + 1)){
                // cout << "metadata.key is " << metadata.key << endl;
                if(memcmp(metadata.key, key, KEY_SIZE) == 0){
                    // cout << "key found" << endl;
                    return p1;
                }
            }
            p1 += sizeof(metadata) + metadata.value_size;
        }
    }
    else{
        // cerr << "Faild to partially decompress" << endl;
    }

    return nullptr;

}

// RX
// split the hash node, copy the right half key value pair to new leaf
string bplustree::splitHashNode(bplustree_leaf_node * p_leaf, bplustree_leaf_node * p_new_leaf) {
    
    // const unsigned char * split_key;
    // printf("splitting!!!!!!\n");
    std::vector<string> sort_buffer;
    char* p = p_leaf->landing_buffer;
    kv_metadata metadata;
    std::memset(&metadata, 0, sizeof(metadata));
    char zeros[sizeof(metadata)] = {0};
    // std::memset(zeros, 0, sizeof(zeros));

        // problem: it cant go into this statement (because the first several bytes are zeros)
    // while(p < p_leaf->landing_buffer + LANDING_BUFFER_BYTES){
    //     memcpy(&metadata, p, sizeof(metadata));
    //     cout << "metadata.key is " << metadata.key << endl;
    //     if(memcmp(&metadata, zeros, sizeof(metadata)) != 0){
    //         string buffer(p, p + sizeof(metadata) + metadata.value_size);
    //         // cout << "buffer is " << buffer << endl;
    //         sort_buffer.push_back(buffer);
    //     }
    //     else{
    //         // cout << "------------------"<<endl;
    //         // cout <<"p is "<<p<<endl;
    //         break;
    //     }
    //     p += sizeof(metadata) + metadata.value_size;
    // }
    while(p < p_leaf->landing_buffer + LANDING_BUFFER_BYTES){
        memcpy(&metadata, p, sizeof(metadata));
        if(metadata.value_size>MEDIUM_SIZE){
            // cout << "metadata.value_size is "<<metadata.value_size<<endl;
            // printChars(p_leaf->landing_buffer, LANDING_BUFFER_BYTES);
            // cout << "break here "<<endl;
            break;
        }

        if(memcmp(&metadata, zeros, sizeof(metadata)) == 0){
            // Search for the first non-zero byte in the buffer
            char *first_non_zero = p;
            while (first_non_zero < p_leaf->landing_buffer + LANDING_BUFFER_BYTES && *first_non_zero == 0) {
                first_non_zero++;
            }

            // If a non-zero byte is found, set p to its address
            
            if (first_non_zero < p_leaf->landing_buffer + LANDING_BUFFER_BYTES) {
            // cout << "first_non_zero is " << reinterpret_cast<uintptr_t>(first_non_zero)<<endl;
            // cout << "p_leaf->landing_buffer + LANDING_BUFFER_BYTES is " << reinterpret_cast<uintptr_t>(p_leaf->landing_buffer + LANDING_BUFFER_BYTES)<<endl;
                p = first_non_zero;
                memcpy(&metadata, p, sizeof(metadata));

                
                string buffer(p, p + sizeof(metadata) + metadata.value_size);
                // printChars(p, sizeof(metadata) + metadata.value_size);
                // cout << "buffer is " << buffer << endl;
                // printChars(buffer.data(), sizeof(metadata) + metadata.value_size);
                sort_buffer.push_back(buffer);
            }

            // need to modify here, here should not be cont. or break.
            // else{
            //     continue;
            // }
        }
        else{
            // cout << "------------------"<<endl;
            // cout <<"p is "<<p<<endl;
            string buffer(p, p + sizeof(metadata) + metadata.value_size);
            // cout << "buffer is " << buffer << endl;
            sort_buffer.push_back(buffer);
            // break;
        }
        p += sizeof(metadata) + metadata.value_size;
    }
    memset(p_leaf->landing_buffer, 0, LANDING_BUFFER_BYTES);
    memset(&metadata, 0, sizeof(metadata));

    char subpages[TOTAL_SUBPAGES_BYTES] = {0};
    // cout << "subpages address: " << reinterpret_cast<uintptr_t>(subpages) << endl;
    // cout <<"debug subpages ";
    // for(int i = 0; i<sizeof(subpages)/sizeof(subpages[0]);i++){
    //     cout << subpages[i];
    // }
    // cout << endl;
    // cout << "*** decompress in splitting ***" << endl;
    decompressLeafToSubpages(p_leaf, subpages);

    // Record bounds of subpages
    int16_t subpage_bound[p_leaf->num_subpages];
    for(int i = 0; i < p_leaf->num_subpages; i++){
        subpage_bound[i] = TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages * i;
    }

    for(int i = 0; i < p_leaf->num_subpages; i++){
        p = subpages + subpage_bound[i];
        while(p < subpages + subpage_bound[i] + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages){
            
            // Check bound
            if(p + sizeof(metadata) < subpages + subpage_bound[i] + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages){
                memcpy(&metadata, p, sizeof(metadata));
            }
            // else{
            //     break;
            // }

            if(p + sizeof(metadata) + metadata.value_size < subpages + subpage_bound[i] + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages){
                if(memcmp(&metadata, zeros, sizeof(metadata)) != 0){
                    // cout << "debug i is " << i << endl;
                    // cout << "p + sizeof(metadata) + metadata.value_size " << (p + sizeof(metadata) + metadata.value_size) << endl;
                    // cout << "debug LHS " << std::hex << std::showbase << reinterpret_cast<uintptr_t>(p + sizeof(metadata) + metadata.value_size) << endl;
                    // cout << "debug RHS " << std::hex << std::showbase << reinterpret_cast<uintptr_t>(subpages + subpage_bound[i] + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages) << endl;
                    // cout << "debug p is " << std::hex << std::showbase << reinterpret_cast<uintptr_t>(p) << endl;
                    string buffer(p, p + sizeof(metadata) + metadata.value_size);
                    // cout << "buffer is " << buffer << endl;
                    sort_buffer.push_back(buffer);
                    // for(int a = 0; a < sort_buffer.size() ; a++){cout << sort_buffer[a]<<endl;}
                }
                else{
                    break;
                }
            }
            // else{
            //     break;
            // }
            p += sizeof(metadata) + metadata.value_size;
        }
    }
    
    sort(sort_buffer.begin(), sort_buffer.end(), compareMetadata);
    // // Print sorted keys
    // std::cout << "Sorted keys:" << std::endl;
    // for (const auto &metadata_str : sort_buffer) {
    //     kv_metadata metadata;
    //     std::memcpy(&metadata, metadata_str.data(), sizeof(kv_metadata));
    //     std::string key(reinterpret_cast<char*>(metadata.key), KEY_SIZE);
    //     std::cout << key << std::endl;
    // }


    memset(p_leaf->minimum_key, 0, KEY_SIZE);
    memset(p_leaf->maximum_key, 0, KEY_SIZE);
    memset(p_leaf->landing_buffer, 0, LANDING_BUFFER_BYTES);

    delete[] p_leaf->compress_data;
    p_leaf->compress_data = NULL;
    p_leaf->num_subpages = NUM_SUBPAGES;
    p_leaf->is_subpage_full = 0;
    p_leaf->compress_size = 0;
    p_leaf->second_chance = 0;
    p_leaf->key_count = 0;
    
    // cout <<"next is " << p_leaf->next<<endl;

    // split_key = (const unsigned char*) const_cast<char*>(sort_buffer[sort_buffer.size() / 2].c_str());
    // split_key = reinterpret_cast<const unsigned char*>(sort_buffer.at(sort_buffer.size() / 2).c_str());

    string split_key_str = sort_buffer.at(sort_buffer.size() / 2 - 1).substr(0,KEY_SIZE);

    // cout << "split_key_str " << split_key_str << endl;
    // const unsigned char* split_key = reinterpret_cast<const unsigned char*>(split_key_str.c_str());

    // printf("--------insert left half\n");
    // cout << "reinsert in splitting" << endl;
    // cout << "--------------------might have update left " << endl;
    for(int i = 0; i < sort_buffer.size() / 2; i++){
        // std::cout << "p_leaf->compress_data in the inserting: " << reinterpret_cast<uintptr_t>(p_leaf->compress_data) << std::endl;
        kv_metadata metadata1={0};
        memcpy(&metadata1, const_cast<char*>(sort_buffer[i].data()), sizeof(metadata1));
        string value = sort_buffer[i].substr(sizeof(metadata1), sort_buffer[i].size() - sizeof(metadata1));
        char* val = const_cast<char*>(value.data());
        // cout << "debug in left: " << p_leaf->num_subpages << endl;
        
        insertHashNode(p_leaf, val, metadata1);
        // cout << "Insert left leaf " << endl;
        // printLeaf(p_leaf);
    }
    // memcpy((void*)split_key_str.data(), p_leaf->maximum_key, KEY_SIZE);

    // printf("--------insert right half\n");
    // cout << "--------------------might have update right " << endl;
    for(int i = sort_buffer.size() / 2; i < sort_buffer.size(); i++){
        kv_metadata metadata2={0};
        memcpy(&metadata2, const_cast<char*>(sort_buffer[i].data()), sizeof(metadata2));
        string value = sort_buffer[i].substr(sizeof(metadata2), sort_buffer[i].size() - sizeof(metadata2));
        char* val = const_cast<char*>(value.data());
        // cout << "debug in right: " << p_new_leaf->num_subpages << endl;
        
        insertHashNode(p_new_leaf, val, metadata2);
        // cout << "Insert right leaf " << endl;
        // printLeaf(p_new_leaf);
    }

    // Delete the vector elements and free its memory
    sort_buffer.clear();
    sort_buffer.shrink_to_fit();
    return split_key_str;
}

void bplustree::printLeaf(bplustree_leaf_node * p_leaf) {
    char subpages[TOTAL_SUBPAGES_BYTES] = {0};
    cout << "p_leaf is " << p_leaf << endl;
    cout << "p_leaf->minimum_key.key is " << p_leaf -> minimum_key << endl;
    cout << "p_leaf->maximum_key.key is " << p_leaf -> maximum_key << endl;
    cout << "p_leaf->key_count is " << p_leaf -> key_count << endl;
    cout << "p_leaf -> num_subpages is " << p_leaf -> num_subpages << endl;
    cout << "p_leaf->next is " << p_leaf -> next << endl;
    cout << "size of p_leaf is " << sizeof(*p_leaf) << " Bytes" << endl;
    cout << "size of all subpages is " << sizeof(subpages) << " Bytes" << endl;
    cout << "size of compressed data is " << p_leaf -> compress_size << " Bytes" << endl;
    cout << "key size is " << KEY_SIZE << endl;

    if(p_leaf->compress_data != NULL){
        decompressLeafToSubpages(p_leaf, subpages);
    }

    // int landing_buffer_size = sizeof(p_leaf -> landing_buffer) / sizeof(p_leaf -> landing_buffer[0]);


    char* p = NULL;
    p = p_leaf->landing_buffer;
    kv_metadata metadata = {0};
    char zeros[sizeof(metadata)] = {0};
    int j = 0;
    while(p <= p_leaf->landing_buffer + LANDING_BUFFER_BYTES){
        if(p + sizeof(metadata) <= p_leaf->landing_buffer + LANDING_BUFFER_BYTES){
            memcpy(&metadata, p, sizeof(metadata));
        }
        // Problem will appear if the value sizes are different, since not obtaining the metadata of the specific KV pair
        if(p + sizeof(metadata) + metadata.value_size <= p_leaf->landing_buffer + LANDING_BUFFER_BYTES){
            if(memcmp(&metadata, zeros, sizeof(metadata)) != 0){
                string val(p + sizeof(metadata), p + sizeof(metadata) + metadata.value_size);
                cout << "landing buffer["<< j <<"] key and value is " << metadata.key << "------" << val.data() << endl;

            }
            else{
                cout << "Rest bytes of landing buffer is " << p_leaf->landing_buffer + LANDING_BUFFER_BYTES - p << endl;
                break;
            }
            // p += sizeof(metadata) + metadata.value_size;
            // j++;

        }

        p += sizeof(metadata) + metadata.value_size;
        j++;
    }

    // Record bounds of subpages
    int16_t subpage_bound[p_leaf->num_subpages];
    for(int i = 0; i < p_leaf->num_subpages; i++){
        subpage_bound[i] = TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages * i;
    }
    for(int i = 0; i < p_leaf->num_subpages; i++){
        p = subpages + subpage_bound[i];
        // cout << "p is " << reinterpret_cast<uintptr_t>(p) << endl;
        j = 0;
        while(p <= subpages + subpage_bound[i] + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages){
            if(p + sizeof(metadata) <= subpages + subpage_bound[i] + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages){
                memcpy(&metadata, p, sizeof(metadata));
            }

            if(p + sizeof(metadata) + metadata.value_size <= subpages + subpage_bound[i] + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages){
                if(memcmp(&metadata, zeros, sizeof(metadata)) != 0){
                    string val(p + sizeof(metadata), p + sizeof(metadata) + metadata.value_size);
                    cout << "subpage["<< i <<"]["<<j<<"]key and value is " << metadata.key << "------" << val << endl;
                    
                }
                else{
                    // cout << "subpage["<< i <<"]["<<j<<"] is empty, stop printing the rest of this subpages " << endl;
                    cout << "Rest bytes of subpage["<<i<<"] is " <<subpages + subpage_bound[i] + TOTAL_SUBPAGES_BYTES / p_leaf->num_subpages-p << endl;
                    break;
                    // cout << (char*) p << endl;
                }
                
            }
            j++;
            p += sizeof(metadata) + metadata.value_size;
        }
    }

    // cout << "**********************************************************************" << endl;
}

int bplustree::insert(struct bplustree * t, const unsigned char * key, int key_len, char * value) {
    // cout << "key and value is " << key << " " <<value << endl;
    LOG("Put key=" << std::string((const char * ) key, key_len));

    std::stack < struct bplustree_node * > ancestors;
    // bool need_split = false;
    const unsigned char * split_key;
    bplustree_leaf_node * p_new_leaf = NULL;

    struct bplustree_leaf_node * p_leaf = (struct bplustree_leaf_node * ) LeafSearchForInsert(t, key, key_len, ancestors);
    // cout << "p_leaf is " << p_leaf << endl;
    // cout << "p_leaf -> next is " << p_leaf -> next << endl;
    // cout << "p_leaf address: " << reinterpret_cast<uintptr_t>(p_leaf) << endl;
    // cout << "p_new_leaf address: " << reinterpret_cast<uintptr_t>(p_new_leaf) << endl;

    if(p_leaf == root){
        p_leaf->num_subpages = NUM_SUBPAGES;
    }

    p_leaf -> second_chance = 0;

    if (!p_leaf) {
        assert(false);
        return 0;
    }

    #ifdef INDEX_STATS
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    #endif

    struct bplustree_leaf_node * next = (struct bplustree_leaf_node * ) p_leaf -> next;
    if(next){
        // cout << "key is " << key << " next -> minimum_key is " << next -> minimum_key << endl;
        // cout << "p_leaf -> minimum_key is " << p_leaf -> minimum_key << endl;
        // cout << "p_leaf -> maximum_key is " << p_leaf -> maximum_key << endl;
    }
    // cout << "key is " << key << " p_leaf -> maximum_key is " << p_leaf -> maximum_key << endl;
    while (next && (memcmp((void * ) key, (void * ) next -> minimum_key, KEY_SIZE) >= 0)) {
        cout << "Leaf update next here " << endl;

        // Modified and Added Unlock here
        // #ifdef USE_WRITE_LOCK
        //     // cout << "Lock modification here: unlock " << p_leaf << endl;
        //     // cout << "Lock Next is " << next << endl;
        //     p_leaf -> rwlatch_ -> WUnlock();
        //     next -> rwlatch_ ->WLock();
        // #endif
        cout << "key is " << key << " next -> minimum_key is " << next -> minimum_key << endl;
        cout << "p_leaf -> minimum_key is " << p_leaf -> minimum_key << endl;
        cout << "p_leaf -> maximum_key is " << p_leaf -> maximum_key << endl;

        p_leaf = next;
        next = (struct bplustree_leaf_node * ) p_leaf -> next;


        
        LOG("Leaf update next " << (void * ) next);
    }

    int idx = 0;
    bool exist = false;
    int ret;
    int keycount = p_leaf -> key_count;

    struct kv_metadata metadata = {0};

    // Copy key to metadata
    memcpy(metadata.key, key, KEY_SIZE);
    // cout << "insert value is " << value << endl;

    if (strlen(value) <= MIN_SIZE) {

        metadata.value_size = strlen(value);
        setValueType(metadata, VALUE_TYPE_TINY);
        // cout << "metadata size is " << sizeof(metadata) << endl;
        int is_inserted = insertHashNode(p_leaf, value, metadata);

    } else if (strlen(value) > MIN_SIZE && strlen(value) <= MEDIUM_SIZE) {
        auto it = medium_size_values.end();

        // Compress each midium size value
        int max_compress_size = LZ4_COMPRESSBOUND(strlen(value));
        char buffer[max_compress_size];
        int compress_size = LZ4_compress_default(value, buffer, strlen(value), max_compress_size);

        it = medium_size_values.insert(it, string(buffer, buffer + compress_size));
        int offset = it - medium_size_values.begin();

        // Create a vector<char> with a variable size
        std::vector<char> buffer1(20); // Change the size as needed
        int length = sprintf(buffer1.data(), "%d", offset);
        buffer1.resize(length); // Resize the vector to the actual length
        // cout << "buffer1 is " << buffer1.data() << endl;
        metadata.value_size = length;
        setValueType(metadata, VALUE_TYPE_MEDIUM);
        // cout << "the length inserted into btree is " << length << endl;
        // cout << "metadata size is " << sizeof(metadata) << endl;

        int is_inserted = insertHashNode(p_leaf, buffer1.data(), metadata);

    } else if (strlen(value) > MEDIUM_SIZE) {
        // append
        FILE * file = fopen("/home/xier2/CompressedCache_test_data/val_large_on_ssd.txt", "ab");
        size_t offset = ftell(file);
        size_t bytes_written = fwrite(value, sizeof(char), strlen(value), file);
        
        if (bytes_written != strlen(value)) {
            std::cerr << "Failed to copy the entire string to val_large_on_ssd.txt" << std::endl;
        }
        // char buffer[32];
        
        // Create a vector<char> with a variable size
        std::vector<char> buffer(32); // Change the size as needed
        int length = sprintf(buffer.data(), "%ld-%ld", offset, bytes_written);
        buffer.resize(length); // Resize the vector to the actual length

        // int length = sprintf(buffer, "%ld-%ld", offset, bytes_written);

        metadata.value_size = length;
        setValueType(metadata, VALUE_TYPE_LARGE);
        
        int is_inserted = insertHashNode(p_leaf, buffer.data(), metadata);
        fclose(file);
    }
    // cout << "p_leaf after insertHashNode "<< p_leaf << endl;

    // cout << "************ Start *************" << endl;
    // printLeaf(p_leaf);
    // cout << "************ End *************" << endl;
    // split
    // need modify here
    // if((p_leaf->key_count >= NUM_LEAF_NODE_KEYS) || (p_leaf->is_subpage_full == 1)) {
    // if ((p_leaf -> key_count >= 8)) {
    if ((p_leaf->is_subpage_full == 1)) {
    // if ((p_leaf->num_subpages == 2)) {
        // cout << "Start split " << endl;
        #ifdef INDEX_STATS
        split_nums++;
        #endif
        p_new_leaf = new bplustree_leaf_node;
        // cout << "split, keycount = " << p_leaf->key_count << endl;
        LOG("split, keycount = " << p_leaf -> key_count);

        // need_split = true;

        memset(p_new_leaf, 0, sizeof(struct bplustree_leaf_node));
        p_new_leaf -> is_leaf = true;
        p_new_leaf -> rwlatch_ = new RWMutex();
        p_new_leaf -> num_subpages = NUM_SUBPAGES;
        p_new_leaf -> is_subpage_full = 0;
        p_new_leaf -> compress_data = NULL;
        p_new_leaf -> compress_size = 0;
        memset(p_new_leaf->landing_buffer, 0, LANDING_BUFFER_BYTES);
        // cout << "debug: p_new_leaf -> num_subpages "<< p_new_leaf -> num_subpages << endl;
        // cout << "debug: p_leaf -> num_subpages "<< p_leaf -> num_subpages << endl;

        keycount = p_leaf -> key_count;

        // RX
        string split_key_str = splitHashNode(p_leaf, p_new_leaf);
        // cout << "p_leaf after split is " << p_leaf << endl;
        split_key = reinterpret_cast<const unsigned char*>(split_key_str.c_str());

        if(memcmp(metadata.key, split_key, KEY_SIZE)>0){
            total_ignores++;
        }
        else{
            total_ignores++;
        }
        // cout << "split_key is " << split_key << endl;
        LOG("new_leaf keycount " << p_new_leaf -> key_count);
        LOG("old_leaf keycount " << p_leaf -> key_count);

        // cout << "p_leaf -> next " << p_leaf -> next << endl;

        p_new_leaf -> next = (struct bplustree_node * ) p_leaf -> next;
        p_leaf -> next = (struct bplustree_node * ) p_new_leaf;
        // creat new root node
        if ((struct bplustree_node * ) p_leaf == t -> root) {
            assert(ancestors.empty());

            struct bplustree_index_node * p_root;
            p_root = (struct bplustree_index_node * )
            malloc(sizeof(struct bplustree_index_node));

            memset(p_root, 0, sizeof(struct bplustree_index_node));
            memcpy((void * ) p_root -> slots[0].key,
                GET_KEY_STR(split_key),
                GET_KEY_LEN(split_key));

            p_root -> slots[0].ptr = (struct bplustree_node * ) p_leaf;
            p_root -> slots[1].ptr = (struct bplustree_node * ) p_new_leaf;

            // cout << "hello p_new_leaf = " << p_new_leaf << endl;
            p_root -> key_count = 1;
            p_root -> is_leaf = false;
            p_root -> rwlatch_ = new RWMutex();

            t -> root = (struct bplustree_node * ) p_root;

        }
                // cout << "split_key " << split_key << endl;
        InsertInternal(t, (const unsigned char * ) GET_KEY_STR(split_key),
            GET_KEY_LEN(split_key),
            (struct bplustree_node * ) p_new_leaf,
            ancestors);
    }

    // if (need_split) {
    //     InsertInternal(t, (const unsigned char * ) GET_KEY_STR(split_key),
    //         GET_KEY_LEN(split_key),
    //         (struct bplustree_node * ) p_new_leaf,
    //         ancestors);
    // }

    #ifdef INDEX_STATS
    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast < std::chrono::nanoseconds > (t2 - t1).count();
    update_time += duration;
    #endif

    #ifdef USE_WRITE_LOCK
    // cout << "WUnlock after insert " << p_leaf << endl;
    p_leaf -> rwlatch_ -> WUnlock();
    // remove the stack's node's lock
    while (ancestors.size()) {
        auto parent = ancestors.top();
        ancestors.pop();
        // cout << "WUnlock parents after insert "<< parent << endl;
        parent -> rwlatch_ -> WUnlock();
    }
    #endif

    return 1;
}

void bplustree::InsertInternal(struct bplustree * t,
    const unsigned char * key, int key_len,
        struct bplustree_node * right,
        std::stack < struct bplustree_node * > ancestors) {

    // cout << "start insertInternal >>>" << endl;

    if (ancestors.empty()) {
        //root split
        //assert(left.oid.off == D_RW(bplustree)->root.oid.off);
        return;
    }

    struct bplustree_index_node * p_parent =
        (struct bplustree_index_node * ) ancestors.top();
    ancestors.pop();

    struct bplustree_index_node * next =
        (struct bplustree_index_node * ) p_parent -> next;

    while (next && (memcmp((void * ) key,
            (void * ) next -> slots[0].key, KEY_SIZE) >= 0)) {

        p_parent = next;
        next = (struct bplustree_index_node * ) p_parent -> next;
    }

    int keycount = p_parent -> key_count;
    int idx = 0;
    for (idx = 0; idx < keycount; idx++) {
        const unsigned char * router_key = p_parent -> slots[idx].key;
        if (memcmp((void * ) router_key, (void * ) key, KEY_SIZE) >= 0) {
            break;
        }
    }

    for (int i = keycount; i >= idx; i--) {
        memcpy( & p_parent -> slots[i + 1], & p_parent -> slots[i],
            sizeof(struct bplustree_kpslot));
    }

    memcpy((void * ) p_parent -> slots[idx].key,
        key, KEY_SIZE);

    p_parent -> slots[idx + 1].ptr = right;

    p_parent -> key_count += 1;

    if (p_parent -> key_count <= BPLUSTREE_NODE_KEYS) {
        return;
    }

    LOG("internal split, keycount = " << p_parent -> key_count);

    struct bplustree_index_node * p_new_parent;
    p_new_parent = (struct bplustree_index_node * )
    malloc(sizeof(struct bplustree_index_node));
    memset(p_new_parent, 0, sizeof(struct bplustree_index_node));

    p_new_parent -> is_leaf = false;
    p_new_parent -> rwlatch_ = new RWMutex();
    unsigned char * split_key = p_parent -> slots[BPLUSTREE_NODE_MIDPOINT].key;
    keycount = p_parent -> key_count;

    LOG("split_key = " << std::string((const char * ) GET_KEY_STR(split_key),
        GET_KEY_LEN(split_key)));
    for (int i = BPLUSTREE_NODE_UPPER; i <= keycount; i++) {
        memcpy( & p_new_parent -> slots[i - BPLUSTREE_NODE_UPPER], &
            p_parent -> slots[i],
            sizeof(struct bplustree_kpslot));
    }

    p_new_parent -> key_count = keycount - BPLUSTREE_NODE_MIDPOINT - 1;
    p_new_parent -> next = (struct bplustree_node * ) p_parent -> next;

    //update old parent
    p_parent -> next = (struct bplustree_node * ) p_new_parent;

    p_parent -> key_count = BPLUSTREE_NODE_MIDPOINT;

    LOG("new_parent keycount " << p_new_parent -> key_count);
    LOG("old_parent keycount " << p_parent -> key_count);
    if ((struct bplustree_node * ) p_parent == t -> root) {
        assert(ancestors.empty());
        struct bplustree_index_node * p_root;
        p_root = (struct bplustree_index_node * )
        malloc(sizeof(struct bplustree_index_node));
        memset(p_root, 0, sizeof(struct bplustree_index_node));
        memcpy((void * ) p_root -> slots[0].key,
            GET_KEY_STR(split_key),
            GET_KEY_LEN(split_key));

        p_root -> slots[0].ptr = (struct bplustree_node * ) p_parent;
        p_root -> slots[1].ptr = (struct bplustree_node * ) p_new_parent;
        p_root -> key_count = 1;
        p_root -> is_leaf = false;
        p_root -> rwlatch_ = new RWMutex();
        t -> root = (struct bplustree_node * ) p_root;
    }

    InsertInternal(t, (const unsigned char * ) GET_KEY_STR(split_key),
        GET_KEY_LEN(split_key),
        (struct bplustree_node * ) p_new_parent,
        ancestors);
    return;
}

void bplustree::UpdatePreviousLeafNextPointer(bplustree_leaf_node* leaf, std::stack<struct bplustree_node*>& ancestors_full) {
    struct bplustree_node* parent = ancestors_full.top();
    ancestors_full.pop();
    int index = -1;

    for (int i = 0; i < parent->key_count; i++) {
        if (((bplustree_index_node*)parent)->slots[i].ptr == leaf) {
            index = i;
            break;
        }
    }
    assert(index != -1);

    bplustree_node* previous_leaf = nullptr;

    if (index != 0) {
        bplustree_node* sibling = ((bplustree_index_node*)parent)->slots[index - 1].ptr;
        while (!sibling->is_leaf) {
            sibling = ((bplustree_index_node*)sibling)->slots[sibling->key_count].ptr;
        }
        previous_leaf = sibling;
    } else {
        while (!ancestors_full.empty()) {
            parent = ancestors_full.top();
            ancestors_full.pop();

            for (int i = 0; i < parent->key_count; i++) {
                if (((bplustree_index_node*)parent)->slots[i].ptr == leaf) {
                    index = i;
                    break;
                }
            }

            if (index != 0) {
                bplustree_node* sibling = ((bplustree_index_node*)parent)->slots[index - 1].ptr;
                while (!sibling->is_leaf) {
                    sibling = ((bplustree_index_node*)sibling)->slots[sibling->key_count].ptr;
                }
                previous_leaf = sibling;
                break;
            }
        }
    }

    if (previous_leaf) {
        ((bplustree_leaf_node*)previous_leaf)->next = ((bplustree_leaf_node*)leaf)->next;
    }
}


void bplustree::deleteLeaf(struct bplustree * t, bplustree_leaf_node * leaf) {
    cout << "leaf to be deleted is " << reinterpret_cast<uintptr_t>(leaf) << endl;
    // 93825240858272
    sleep(2);
    std::stack < struct bplustree_node * > ancestors_for_lock;
    std::stack < struct bplustree_node * > ancestors_full;
    // bool need_split = false;
    const unsigned char * split_key = 0;
    // bplustree_leaf_node * p_new_leaf = new bplustree_leaf_node;
    bplustree_index_node * parent;
    unsigned char key[KEY_SIZE];
    memcpy(key, leaf -> minimum_key, KEY_SIZE);

    // // Find the parents node
    LeafSearchForDelete(t, key, KEY_SIZE, ancestors_for_lock, ancestors_full);
    UpdatePreviousLeafNextPointer(leaf, ancestors_full);

    // print_memory_usage();

    if(leaf->compress_data != nullptr){
        delete[] leaf -> compress_data;
    }
    delete leaf;

    // print_memory_usage();

    cout << "Leaf deleted in bufferTree " << endl;

    while(ancestors_full.size()){
        ancestors_full.pop();
    }

    #ifdef USE_WRITE_LOCK
        while (ancestors_for_lock.size()) {
        auto parent = ancestors_for_lock.top();
        ancestors_for_lock.pop();
        // cout << "WUnlock parents after insert "<< parent << endl;
        parent -> rwlatch_ -> WUnlock();
    }
    #endif

}

void bplustree::resetLeafNode(bplustree_leaf_node* leaf) {
    leaf->is_leaf = 1;
    leaf->key_count = 0;
    // leaf->next = nullptr;

    // memset(leaf->minimum_key, 0, sizeof(leaf->minimum_key));
    // memset(leaf->maximum_key, 0, sizeof(leaf->maximum_key));
    memset(leaf->landing_buffer, 0, sizeof(leaf->landing_buffer));

    if (leaf->compress_data) {
        delete[] leaf->compress_data;
        leaf->compress_data = nullptr;
    }

    leaf->num_subpages = NUM_SUBPAGES;
    leaf->is_subpage_full = 0;
    leaf->compress_size = 0;
    leaf->second_chance = 0;
}


void bplustree::deleteCompressedData(bplustree_leaf_node* leaf) {
    if (leaf->compress_data) {
        delete[] leaf->compress_data;
        leaf->compress_data = nullptr;
        leaf->compress_size = 0;
        leaf->key_count = 0;
    }
}




int bplustree::remove(struct bplustree * t,
    const unsigned char * key, int key_len) {
    return 0;
}

void bplustree::countNodesAndMemoryUsage(bplustree_node *node, uint64_t &internal_nodes, uint64_t &leaf_nodes, uint64_t &total_memory) {
    if (node->is_leaf) {
        // Increment leaf node counter
        leaf_nodes++;

        // Calculate memory usage for the leaf node
        total_memory += sizeof(bplustree_leaf_node);

        bplustree_leaf_node* leaf = static_cast<bplustree_leaf_node*>(node);
        total_memory += leaf->compress_size; // Add the compressed data size to total_memory
    } else {
        // Increment internal node counter
        internal_nodes++;

        // Calculate memory usage for the internal node
        total_memory += sizeof(bplustree_index_node);

        // Traverse the child nodes
        bplustree_index_node *index_node = static_cast<bplustree_index_node *>(node);
        for (int i = 0; i <= index_node->key_count; i++) {
            countNodesAndMemoryUsage(index_node->slots[i].ptr, internal_nodes, leaf_nodes, total_memory);
        }
    }
}

