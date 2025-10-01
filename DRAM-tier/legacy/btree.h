#ifndef BTREE_H
#define BTREE_H

#include <iostream>
#include <fstream>
#include <algorithm>
#include <cassert>
#include <cstring>
#include <list>
#include <unistd.h>
#include <vector>
#include <variant>
#include <unordered_map>
#include <queue>
#include <mutex>
#include <thread>
#include <memory>
#include <atomic>

#include <stdlib.h>
#include <stdint.h>
#include <stack>
#include <math.h>
#include "rwmutex.h"
#include <chrono>
#include <openssl/sha.h>
#include "../external/lz4.h"


#define KEY_SIZE 8 // key size
#define VALUE_SIZE 55 //55 23
#define MAXLARGESIZEVALUE 1024

// value size thresholds
#define MIN_SIZE 64
#define MEDIUM_SIZE 512
// #define LARGE_SIZE 1024

#define TOTAL_SUBPAGES_BYTES 4096
#define LANDING_BUFFER_BYTES 512

#define NUM_SUBPAGES 8 // 16, 32
#define NUM_LANDING_BUFFER 4 // 32, 128 is not working
#define NUM_SUBPAGES_KEYS TOTAL_SUBPAGES_BYTES/(((VALUE_SIZE+KEY_SIZE+1+3)/4)*4)
#define NUM_LEAF_NODE_KEYS NUM_LANDING_BUFFER + NUM_SUBPAGES_KEYS // 65.1343283582

#define BPLUSTREE_NODE_KEYS 128 // for internal node
#define BPLUSTREE_NODE_MIDPOINT (BPLUSTREE_NODE_KEYS / 2)
#define BPLUSTREE_NODE_UPPER (BPLUSTREE_NODE_MIDPOINT + 1)

#define KEY_IDENTIFIER_SIZE 8

#define USE_READ_LOCK 1
#define USE_WRITE_LOCK 1
// #define USE_REDISTRIBUTE 1

// Constants for request and value types
const uint8_t REQUEST_TYPE_READ = 0x01;
const uint8_t REQUEST_TYPE_WRITE = 0x02;
const uint8_t REQUEST_TYPE_DELETE = 0x03;

const uint8_t VALUE_TYPE_TINY = 0x01;
const uint8_t VALUE_TYPE_MEDIUM = 0x02;
const uint8_t VALUE_TYPE_LARGE = 0x03;
inline std::atomic<uint64_t> total_updates{0};
inline std::atomic<uint64_t> total_ignores{0};

using namespace std;
using namespace cmudb;

class bplustree {

public:
    struct bplustree_node {
        bool is_leaf = 0;
        uint16_t key_count = 0;
        struct bplustree_node* next;
        RWMutex *rwlatch_;       // read and write lock.
    };


    struct bplustree_kpslot {
        unsigned char key[KEY_SIZE+1]={'\0'};
        struct bplustree_node *ptr; // child pointer or value pointer
    };

    // Deprecated
    struct bplustree_kvslot {
        unsigned char key[KEY_SIZE+1]={'\0'};

        unsigned char val[VALUE_SIZE]={'\0'};
    };

    // Structure for the key-value pair metadata
    struct kv_metadata {
        unsigned char key[KEY_SIZE];
        uint8_t value_type; // first four bits: value type, last four bits: request type
        uint16_t value_size;

    };

    struct bplustree_leaf_node : public bplustree_node {
        uint64_t pad;
        char minimum_key[KEY_SIZE]={'\0'};
        char maximum_key[KEY_SIZE]={'\0'};
        char landing_buffer[LANDING_BUFFER_BYTES];
        char * compress_data = NULL;
        int num_subpages = NUM_SUBPAGES;
        bool is_subpage_full = 0;
        int compress_size = 0;
        bool second_chance = 0;
    };

    struct bplustree_index_node  : public bplustree_node {
        uint64_t pad;
        struct bplustree_kpslot slots[BPLUSTREE_NODE_KEYS + 2];
    };

    struct Request {
        kv_metadata metadata;
        char* value;
    };

    std::vector<string> medium_size_values;

    std::queue<Request> request_queue;
    std::mutex queue_mutex;

    bplustree_node *root;

public:
    bplustree();
    ~bplustree();

    int insert(struct bplustree *t, const unsigned char *key, int key_len, char *value);
    
    int insertHashNode(bplustree_leaf_node *p_leaf, char * value, kv_metadata metadata); // Deprecated

    void insertIntoLandingBUffer(bplustree_leaf_node *p_leaf, Request *request);

    void performHashing(bplustree_leaf_node *p_leaf);
   
    char* isInLeaf(bplustree_leaf_node *p_leaf, const unsigned char *key);
    
    string splitHashNode(bplustree_leaf_node *p_leaf, bplustree_leaf_node *p_new_leaf);
    
    int remove(struct bplustree *t, const unsigned char *key, int key_len);

    void* search(struct bplustree *t, const unsigned char *key, int key_len);

    void printLeaf(bplustree_leaf_node *p_leaf);
    
    int total_keys = 0;
    int total_nodes = 0;
    uint64_t total_compressed_bytes = 0;
    uint64_t total_btree_size = 0;
    uint64_t total_memory = 0;
    uint64_t leaf_nodes = 0;
    uint64_t internal_nodes = 0;

    int findTotalKeyCount(struct bplustree *t);

    struct bplustree_node* getEvictNode_(struct bplustree *t);

    // Delete the leaf node in bufferTree and its keys in ancestor/internal nodes
    void deleteLeaf(struct bplustree *t, bplustree_leaf_node* leaf);

    /**
     * @brief Updates the next pointer of the previous leaf node when deleting a leaf node from the B+ tree.
     * 
     * @param leaf The target leaf node to be removed from the B+ tree.
     * @param ancestors_full The stack of ancestors of the target leaf node (from root to the parent node).
     *
     * This function first finds the index of the child pointer in the parent node that points to the target leaf node.
     * Then, it follows the procedure to find the previous leaf node in the B+ tree.
     * If the previous leaf node is found, it updates the next pointer of the previous leaf node to point
     * to the leaf node that comes after the target leaf node (if there is one).
     * This function is called before removing the target leaf node and updating the tree as needed.
     */
    void UpdatePreviousLeafNextPointer(bplustree_leaf_node* leaf, std::stack<struct bplustree_node*>& ancestors_full);

    void deleteCompressedData(bplustree_leaf_node* leaf);

    void resetLeafNode(bplustree_leaf_node* leaf);

    void countNodesAndMemoryUsage(bplustree_node *node, uint64_t &internal_nodes, uint64_t &leaf_nodes, uint64_t &total_memory);
    
    void redistributeSubpages(bplustree_leaf_node *p_leaf, char subpages[]);

    void compressSubpagesToLeaf(bplustree_leaf_node *p_leaf, char subpages[]);

    void decompressLeafToSubpages(bplustree_leaf_node *p_leaf, char subpages[]);

    void backgroundTask(bplustree_leaf_node *p_leaf);

    void printChars(const char* start, size_t size) {
        std::string str(start, size);
        for (char c : str) {
            if (c == '\0') {
                std::cout << "<NUL>";
            } else {
                std::cout << c;
            }
        }
        std::cout << std::endl;
    }

    char* findPointerInArray(char* array, char* key, int array_size){
        char* p = array;
        while(p < array + array_size){
            struct kv_metadata metadata;
            memcpy(&metadata, p, sizeof(metadata));
            if(memcmp(metadata.key, key, KEY_SIZE) == 0){
                return p;
            }
            p += sizeof(metadata) + metadata.value_size;
        }
        return nullptr;
    }

    // // Deprecated
    // char* findVacantPositionPointerInArray(char* array, int array_size){
    //     for (int i = 0; i < array_size; i++) {
    //         if (array[i] == '\0') {
    //             return &array[i];
    //         }
    //     }
    //     return nullptr;
    // }

    static bool compareMetadata(const string &s1, const string& s2) {
        // Convert the strings to kv_metadata structs
        kv_metadata m1, m2;
        std::memcpy(&m1, s1.data(), sizeof(kv_metadata));
        std::memcpy(&m2, s2.data(), sizeof(kv_metadata));

        // Extract the key field from each metadata struct
        std::string key1(reinterpret_cast<char*>(m1.key), KEY_SIZE);
        std::string key2(reinterpret_cast<char*>(m2.key), KEY_SIZE);
        
        // Compare the key fields using the less-than operator
        return key1 < key2;
    }

    // Utility functions
    void setRequestType(kv_metadata &metadata, uint8_t request_type) {
        metadata.value_type = (metadata.value_type & 0x0F) | (request_type << 4);
    }

    void setValueType(kv_metadata &metadata, uint8_t value_type) {
        metadata.value_type = (metadata.value_type & 0xF0) | value_type;
    }

    uint8_t getRequestType(const kv_metadata &metadata) {
        return metadata.value_type >> 4;
    }

    uint8_t getValueType(const kv_metadata &metadata) {
        return metadata.value_type & 0x0F;
    }

    void requestPush(const Request& request) {
        request_queue.push(request);
    }

    bool requestPop(Request& request) {
        if (!request_queue.empty()) {
            request = request_queue.front();
            request_queue.pop();
            return true;
        }
        return false;
    }

private:

    struct bplustree_node* LeafSearchForInsert(struct bplustree *t,
                                      const unsigned char *key, int key_len,
                                      std::stack <struct bplustree_node *> &ancestors);

    struct bplustree_node* LeafSearchForDelete(struct bplustree *t,
                                      const unsigned char *key, int key_len,
                                      std::stack < struct bplustree_node * > & ancestors_for_lock, 
                                      std::stack < struct bplustree_node * > & ancestors_full);
    
    struct bplustree_node* LeafSearch(struct bplustree *t,
                                      const unsigned char *key, int key_len,
                                      std::stack <struct bplustree_node *> &ancestors);

    void InsertInternal(struct bplustree *t, const unsigned char *key, int key_len,
                        struct bplustree_node *right,
                        std::stack <struct bplustree_node *> ancestors);


    bool FindNext(const unsigned char *key, int key_len,
                  struct bplustree_node *node,
                  struct bplustree_node **next);


#define DO_LOG 0
#define LOG(msg, ...)   \
    do {                \
        if (DO_LOG)     \
            std::cout << "[bplustree]" << msg << "\n";  \
    }while(0)

    static inline int min(int a, int b) {
        return (a < b) ? a : b;
    }




#define GET_KEY_LEN(key) KEY_SIZE

#define GET_KEY_STR(key) key


    static int bplustree_compare(const void* p_slot1, const void* p_slot2)
    {
        struct bplustree_kvslot *slot1 = (struct bplustree_kvslot *) p_slot1;
        struct bplustree_kvslot *slot2 = (struct bplustree_kvslot *) p_slot2;
        return memcmp((void *)slot1->key,
                      (void *)slot2->key, KEY_SIZE);
    }


    static int bplustree_slot_compare(const void* pp_key1, const void* pp_key2)
    {
        unsigned char *p_key1 = *((unsigned char **)pp_key1);
        unsigned char *p_key2 = *((unsigned char **)pp_key2);
        return memcmp((void *)p_key1, (void *)p_key2, KEY_SIZE);
    }

    static void bplustree_node_prefetch(struct bplustree_node *n)
    {
        __builtin_prefetch((char *)n +   0, 0 /* rw */, 3 /* locality */);
        __builtin_prefetch((char *)n +  64, 0 /* rw */, 3 /* locality */);
        __builtin_prefetch((char *)n + 128, 0 /* rw */, 3 /* locality */);
        __builtin_prefetch((char *)n + 192, 0 /* rw */, 3 /* locality */);
        __builtin_prefetch((char *)n + 256, 0 /* rw */, 3 /* locality */);
        __builtin_prefetch((char *)n + 320, 0 /* rw */, 3 /* locality */);

    }

};
#endif
