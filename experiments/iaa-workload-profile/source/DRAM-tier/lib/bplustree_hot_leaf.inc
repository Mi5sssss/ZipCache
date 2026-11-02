/* Experimental whole-leaf RAW representation. No new thread or per-key index.
 * Leaf state is protected by its existing rwlock. Heat is an atomic hint only.
 * Quotas cover allocator-usable RAW/delta allocations, including transitions.
 * They are NOT a cap on total tree/process memory. */
#ifdef ZIPCACHE_HOT_LEAF
#define HOT_CAP (COMPRESSED_LEAF_SIZE/sizeof(struct kv_pair))
#define HL_LOAD(p) __atomic_load_n((p),__ATOMIC_RELAXED)
#define HOT_INC(t,f,n) __atomic_add_fetch(&(t)->hot_budget->stats.f,(n),__ATOMIC_RELAXED)
struct hot_budget {
    struct bplus_hot_stats stats;
    pthread_mutex_t policy_lock;
    uint64_t accesses;
    uint32_t epoch,epoch_ops;
    int enabled,paused,manual,draining,work_signal;
    uint64_t total_limit,reserve;
    unsigned promote,cold,min_epochs;
    uint64_t next_scan;
    unsigned shard;
    key_t cursors[64];
};
struct hot_raw { struct hot_budget *owner; size_t count; struct kv_pair pairs[HOT_CAP]; };
struct hot_delta { struct hot_budget *owner; struct kv_pair pairs[ACTIVE_DELTA_ENTRIES]; };
static void compaction_scheduler_note_completion(struct compaction_scheduler *,int,int);
static size_t hot_find(const struct kv_pair *p,size_t n,key_t key) {
    size_t l=0,r=n;while(l<r){size_t m=l+(r-l)/2;if(p[m].key<key)l=m+1;else r=m;}return l;
}
static int hot_reserve(uint64_t *used,uint64_t *peak,uint64_t limit,size_t n) {
    uint64_t old=HL_LOAD(used);
    do{if(old>limit||n>limit-old)return -1;}while(!__atomic_compare_exchange_n(used,&old,old+n,0,__ATOMIC_RELAXED,__ATOMIC_RELAXED));
    uint64_t p=HL_LOAD(peak);while(p<old+n&&!__atomic_compare_exchange_n(peak,&p,old+n,0,__ATOMIC_RELAXED,__ATOMIC_RELAXED)){}return 0;
}
static void hot_raw_free(struct hot_raw *r) {
    if(!r)return;__atomic_sub_fetch(&r->owner->stats.hot_used,allocation_usable_size(r,sizeof(*r)),__ATOMIC_RELAXED);free(r);
}
static void hot_raw_drop(struct simple_leaf_node *l){hot_raw_free(l->hot_raw);l->hot_raw=NULL;}
static size_t active_allocation(struct simple_leaf_node *l) {
    return l->active?allocation_usable_size((char*)l->active - offsetof(struct hot_delta,pairs),sizeof(struct hot_delta)):0;
}
static int active_ensure(struct bplus_tree_compressed *t,struct simple_leaf_node *l) {
    if(l->active)return 0;
    struct hot_delta *d=calloc(1,sizeof(*d));if(!d){errno=ENOMEM;return -1;}
    struct hot_budget *b=t->hot_budget;size_t n=allocation_usable_size(d,sizeof(*d));
    if(!b||hot_reserve(&b->stats.delta_used,&b->stats.delta_peak,b->stats.delta_limit,n)) {
        free(d);if(b)HOT_INC(t,budget_denied,1);errno=ENOMEM;return -1;
    }
    d->owner=b;l->active=d->pairs;return 0;
}
static void active_drop(struct simple_leaf_node *l) {
    if(!l->active)return;
    struct hot_delta *d=(void*)((char*)l->active - offsetof(struct hot_delta,pairs));
    __atomic_sub_fetch(&d->owner->stats.delta_used,allocation_usable_size(d,sizeof(*d)),__ATOMIC_RELAXED);
    l->active=NULL;free(d);
}
#include "bplustree_hot_diagnostic.inc"

static uint32_t hot_epoch(struct hot_budget *b){return HL_LOAD(&b->epoch)+(uint32_t)(HL_LOAD(&b->accesses)/b->epoch_ops);}
static unsigned hot_heat(struct hot_budget *b,struct simple_leaf_node *l) {
    uint64_t v=HL_LOAD(&l->hot_access);uint32_t delta=hot_epoch(b)-(uint32_t)(v>>32);
    return delta>=32?0:(uint32_t)v>>delta;
}
static void hot_score(struct bplus_tree_compressed *t,struct simple_leaf_node *l) {
    struct hot_budget *b=t->hot_budget;if(!b||!b->enabled||HL_LOAD(&b->paused))return;
    uint64_t old=HL_LOAD(&l->hot_access),next;
    do {uint32_t e=hot_epoch(b);if(e<(uint32_t)(old>>32))e=(uint32_t)(old>>32);uint32_t d=e-(uint32_t)(old>>32),n=d>=32?0:(uint32_t)old>>d;
        if(n<UINT32_MAX)n++;next=((uint64_t)e<<32)|n;
    }while(!__atomic_compare_exchange_n(&l->hot_access,&old,next,0,__ATOMIC_RELAXED,__ATOMIC_RELAXED));
}
static void hot_touch(struct bplus_tree_compressed *t,struct simple_leaf_node *l) {
    struct hot_budget *b=t->hot_budget;if(!b||!b->enabled||HL_LOAD(&b->paused))return;
    __atomic_add_fetch(&b->accesses,1,__ATOMIC_RELAXED);
    if(!b->work_signal||l->hot_raw)hot_score(t,l);
}
static void hot_work(struct bplus_tree_compressed *t,struct simple_leaf_node *l) {
    if(t->hot_budget&&t->hot_budget->work_signal)hot_score(t,l);
}
static int hot_insert(struct hot_raw *r,const struct kv_pair *p) {
    size_t i=hot_find(r->pairs,r->count,p->key);
    if(i<r->count&&r->pairs[i].key==p->key){r->pairs[i]=*p;return 0;}
    if(r->count==HOT_CAP)return -1;
    memmove(r->pairs+i+1,r->pairs+i,(r->count-i)*sizeof(*p));r->pairs[i]=*p;r->count++;return 0;
}
static int hot_raw_put(struct bplus_tree_compressed *t,struct simple_leaf_node *l,key_t k,int value,const uint8_t *p,size_t n) {
    struct kv_pair pair={0};pair.key=k;pair.stored_value=value;
    if(p){if(n>COMPRESSED_VALUE_BYTES)n=COMPRESSED_VALUE_BYTES;memcpy(pair.payload,p,n);}
    else memcpy(pair.payload,&value,sizeof(value));
    if(hot_insert(l->hot_raw,&pair))return -2;
    l->generation++;l->uncompressed_bytes=l->hot_raw->count*sizeof(pair);HOT_INC(t,raw_puts,1);return 0;
}
static void hot_attach(struct bplus_tree_compressed *t,struct hot_budget *b) {
    t->hot_budget=b;if(compressed_tree_is_sharded(t))for(int i=0;i<t->shard_count;i++)hot_attach(t->shards[i],b);
}
static int hot_budget_init(struct bplus_tree_compressed *t) {
    struct hot_budget *b=calloc(1,sizeof(*b));if(!b)return -1;
    b->enabled=env_bool_enabled("BTREE_HOT_ENABLE",0);b->paused=1;
    b->work_signal=env_bool_enabled("BTREE_HOT_WORK_SIGNAL",0);
    b->total_limit=env_int_clamped("BTREE_HOT_TOTAL_BYTES",0,0,INT_MAX);
    b->reserve=env_int_clamped("BTREE_HOT_RESERVE_BYTES",4*1024*1024,0,INT_MAX);
    b->manual=env_bool_enabled("BTREE_HOT_MANUAL",0);
    b->stats.hot_limit=env_int_clamped("BTREE_HOT_BYTES",2*1024*1024,0,INT_MAX);
    b->stats.delta_limit=env_int_clamped("BTREE_DELTA_BYTES",4*1024*1024,0,INT_MAX);
    b->promote=env_int_clamped("BTREE_HOT_THRESHOLD",64,1,INT_MAX);
    b->cold=env_int_clamped("BTREE_COLD_THRESHOLD",8,0,b->promote-1);
    b->min_epochs=env_int_clamped("BTREE_HOT_RESIDENCE",4,1,10000);
    pthread_mutex_init(&b->policy_lock,NULL);b->epoch_ops=env_int_clamped("BTREE_HOT_EPOCH_OPS",8192,128,INT_MAX);
    b->epoch=b->min_epochs;
    hot_attach(t,b);t->owns_hot_budget=1;return 0;
}
static void hot_budget_destroy(struct bplus_tree_compressed *t) {
    if(t->owns_hot_budget){assert(!HL_LOAD(&t->hot_budget->stats.hot_used));assert(!HL_LOAD(&t->hot_budget->stats.delta_used));pthread_mutex_destroy(&t->hot_budget->policy_lock);free(t->hot_budget);}
    t->hot_budget=NULL;t->owns_hot_budget=0;
}
int bplus_tree_hot_stats(struct bplus_tree_compressed *t,struct bplus_hot_stats *out) {
    if(!t||!t->hot_budget||!out)return -1;
    /* Atomic loads, never race an ordinary struct copy with counter updates. */
#define HS(f) out->f=HL_LOAD(&t->hot_budget->stats.f)
    HS(raw_gets);HS(raw_puts);HS(promotions);HS(demotions);HS(conflicts);HS(errors);
    HS(budget_denied);HS(partial_flushes);HS(policy_scans);HS(policy_ns);
    HS(hot_used);HS(hot_peak);HS(hot_limit);HS(delta_used);HS(delta_peak);HS(delta_limit);
    HS(promotion_overflow);HS(task_started);HS(task_finished);
    HS(total_budget_denied);out->owned_census=owned_current();
    out->total_limit=t->hot_budget->total_limit;out->reserve=t->hot_budget->reserve;
    out->work_signal=t->hot_budget->work_signal;
#undef HS
    return 0;
}
void bplus_tree_hot_pause(struct bplus_tree_compressed *t,int p){pthread_mutex_lock(&t->hot_budget->policy_lock);__atomic_store_n(&t->hot_budget->paused,!!p,__ATOMIC_RELAXED);pthread_mutex_unlock(&t->hot_budget->policy_lock);}
void bplus_tree_hot_tick(struct bplus_tree_compressed *t,unsigned n){__atomic_add_fetch(&t->hot_budget->epoch,n,__ATOMIC_RELAXED);}
/* Called under leaf write lock. Optional tasks never steal the last queue slot
 * or the last pending-task credit from required maintenance. */
static int hot_schedule(struct bplus_tree_compressed *t,struct simple_leaf_node *l,int kind) {
    struct compaction_scheduler *s=t->scheduler;
    if(!s||!s->cross_window||l->pending||(kind==1&&l->hot_raw)||(kind==2&&!l->hot_raw))return -1;
    if(kind==1&&t->hot_budget->total_limit) {
        struct hot_budget *b=t->hot_budget;uint64_t live=owned_current();
        uint64_t need=b->reserve+sizeof(struct hot_raw)+sizeof(struct compressed_pending_task)+4096;
        if(live>b->total_limit||need>b->total_limit-live){HOT_INC(t,total_budget_denied,1);return -1;}
    }
    if(kind==1&&!l->is_compressed&&!active_view(l)[0].key&&!active_view(l)[1].key&&!active_view(l)[2].key)return -1;
    struct compressed_pending_task *task=calloc(1,sizeof(*task));if(!task)return -1;
    if(kind==1) {
        task->raw_result=malloc(sizeof(struct hot_raw));if(!task->raw_result){free(task);return -1;}
        struct hot_budget *b=t->hot_budget;
        if(hot_reserve(&b->stats.hot_used,&b->stats.hot_peak,b->stats.hot_limit,allocation_usable_size(task->raw_result,sizeof(struct hot_raw)))) {
            free(task->raw_result);free(task);HOT_INC(t,budget_denied,1);return -1;
        }
        task->raw_result->owner=b;task->raw_result->count=0;
    }
    pthread_mutex_lock(&s->lock);
    size_t credits=s->pending_byte_limit/s->pending_charge,live=s->submitted_tasks-s->completed_tasks;
    int ok=s->accepting&&!s->shutdown&&s->queue_count+s->admission_reserved+1<(size_t)s->queue_capacity&&live+s->admission_reserved+1<credits;
    if(ok&&s->admission_control)s->admission_reserved++;
    pthread_mutex_unlock(&s->lock);
    if(!ok){hot_raw_free(task->raw_result);free(task);return -1;}
    task->kind=kind;task->tree=t;task->leaf=l;task->base_version=l->base_version;task->generation=l->generation;
    task->token=++l->pending_token;task->enqueued_ns=submission_now_ns();l->pending=task;
    if(compaction_scheduler_enqueue_locked(t,l,task)){l->pending=NULL;hot_raw_free(task->raw_result);free(task);return -1;}
    HOT_INC(t,task_started,1);return 0;
}
int bplus_tree_hot_request(struct bplus_tree_compressed *root,key_t key,int kind) {
    if(kind!=1&&kind!=2)return -1;
    struct bplus_tree_compressed *t=compressed_tree_is_sharded(root)?compressed_tree_shard_for_key(root,key):root;
    pthread_rwlock_rdlock(&t->rwlock);struct compressed_leaf_ref *r=find_leaf_for_key(t->tree,key);
    if(!r||!r->payload){pthread_rwlock_unlock(&t->rwlock);return -1;}
    struct simple_leaf_node *l=(void*)r->payload;pthread_rwlock_wrlock(&l->rwlock);
    pthread_rwlock_unlock(&t->rwlock);int rc=hot_schedule(t,l,kind);pthread_rwlock_unlock(&l->rwlock);return rc;
}
static key_t hot_lower_bound(struct compressed_leaf_ref *r) {
    struct bplus_node *n=(void*)r;
    while(n->parent){if(n->parent_key_idx>=0)return n->parent->key[n->parent_key_idx];n=(void*)n->parent;}return 0;
}
static void hot_policy_tick(struct compaction_scheduler *s) {
    struct hot_budget *b=s->root->hot_budget;
    if(!b||!b->enabled||b->manual||HL_LOAD(&b->paused)||HL_LOAD(&b->draining))return;
    uint64_t now=submission_now_ns();if(now<b->next_scan)return;b->next_scan=now+1000000;
    uint64_t start=now;
    pthread_mutex_lock(&b->policy_lock);
    if(HL_LOAD(&b->paused)||HL_LOAD(&b->draining)){pthread_mutex_unlock(&b->policy_lock);return;}
    unsigned shards=compressed_tree_is_sharded(s->root)?s->root->shard_count:1;
    unsigned si=b->shard++%shards;if(si>=64){pthread_mutex_unlock(&b->policy_lock);return;}
    struct bplus_tree_compressed *t=shards>1?s->root->shards[si]:s->root;
    pthread_rwlock_rdlock(&t->rwlock);
    struct compressed_leaf_ref *r=find_leaf_for_key(t->tree,b->cursors[si]);
    for(int k=0;r&&k<64;k++) {
        struct simple_leaf_node *l=(void*)r->payload;
        if(l&&!pthread_rwlock_trywrlock(&l->rwlock)) {
            unsigned heat=hot_heat(b,l);HOT_INC(t,policy_scans,1);
            if(!l->pending&&hot_epoch(b)-l->hot_changed_epoch>=b->min_epochs) {
                if(l->hot_raw&&heat<=b->cold)hot_schedule(t,l,2);
                else if(!l->hot_raw&&heat>=b->promote)hot_schedule(t,l,1);
            }
            pthread_rwlock_unlock(&l->rwlock);
        }
        struct list_head *next=r->link.next;
        if(next==&t->tree->list[0]){b->cursors[si]=0;r=NULL;break;}
        r=list_entry(next,struct compressed_leaf_ref,link);b->cursors[si]=hot_lower_bound(r);
    }
    pthread_rwlock_unlock(&t->rwlock);HOT_INC(t,policy_ns,submission_now_ns()-start);pthread_mutex_unlock(&b->policy_lock);
}
/* Explicit drain seals every remaining cold delta. The caller has stopped
 * mutating APIs. This closes the lifetime of small, infrequently updated deltas
 * without another timer, thread, dirty list or leaf metadata field. */
static int hot_seal(struct bplus_tree_compressed *t,struct simple_leaf_node *l) {
    size_t n=0;for(int i=0;i<ACTIVE_DELTA_ENTRIES;i++)n+=active_view(l)[i].key!=0;
    if(!n){active_drop(l);return 0;}
    if(l->pending)return pending_task_state_load(l->pending)==PENDING_TASK_FAILED?-1:1;
    struct compaction_scheduler *q=t->scheduler;
    pthread_mutex_lock(&q->lock);
    size_t credits=q->pending_byte_limit/q->pending_charge,live=q->submitted_tasks-q->completed_tasks;
    if(!q->accepting||q->shutdown||credits<=q->failed_tasks){pthread_mutex_unlock(&q->lock);return -1;}
    if(q->queue_count+q->admission_reserved>=(size_t)q->queue_capacity||live+q->admission_reserved>=credits){pthread_mutex_unlock(&q->lock);return 1;}
    if(q->admission_control)q->admission_reserved++;
    pthread_mutex_unlock(&q->lock);
    struct compressed_pending_task *task=calloc(1,sizeof(*task)+n*sizeof(struct kv_pair));
    if(!task){pthread_mutex_lock(&q->lock);if(q->admission_control)q->admission_reserved--;compaction_scheduler_changed_locked(q);pthread_mutex_unlock(&q->lock);return -1;}
    task->tree=t;task->leaf=l;task->token=++l->pending_token;task->base_version=l->base_version;task->enqueued_ns=submission_now_ns();
    for(int i=0;i<ACTIVE_DELTA_ENTRIES;i++)if(active_view(l)[i].key)task->records[task->count++]=active_view(l)[i];
    l->pending=task;
    if(compaction_scheduler_enqueue_locked(t,l,task)){l->pending=NULL;free(task);return -1;}
    active_drop(l);HOT_INC(t,partial_flushes,1);return 0;
}
static int hot_seal_all(struct bplus_tree_compressed *root) {
    if(compressed_tree_is_sharded(root)){for(int i=0;i<root->shard_count;i++)if(hot_seal_all(root->shards[i]))return -1;return 0;}
    key_t cursor=0;
    for(;;) {
        uint64_t sequence=compaction_scheduler_sequence(root->scheduler);
        pthread_rwlock_rdlock(&root->rwlock);
        struct compressed_leaf_ref *r=find_leaf_for_key(root->tree,cursor);
        if(!r){pthread_rwlock_unlock(&root->rwlock);return 0;}
        struct simple_leaf_node *l=(void*)r->payload;int rc=0;
        if(l){pthread_rwlock_wrlock(&l->rwlock);rc=hot_seal(root,l);pthread_rwlock_unlock(&l->rwlock);}
        int last=r->link.next==&root->tree->list[0];
        key_t next=last?0:hot_lower_bound(list_entry(r->link.next,struct compressed_leaf_ref,link));
        pthread_rwlock_unlock(&root->rwlock);
        if(rc<0)return -1;
        if(rc>0){compaction_scheduler_wait_for_change(root->scheduler,sequence);continue;}
        if(last)return 0;cursor=next;
    }
}
/* Task terminal: old representation is still authoritative on abort. */
static void hot_finish(struct compaction_scheduler *s,struct compressed_pending_task *task,int error) {
    hot_raw_free(task->raw_result);HOT_INC(task->tree,task_finished,1);
    if(error)HOT_INC(task->tree,errors,1);
    free(task);compaction_scheduler_note_completion(s,1,0);
}
static void hot_abort(struct compaction_scheduler *s,struct compressed_pending_task *task,int error) {
    pthread_rwlock_wrlock(&task->leaf->rwlock);
    if(task->leaf->pending==task){task->leaf->pending=NULL;task->leaf->hot_changed_epoch=hot_epoch(task->tree->hot_budget);}
    pthread_rwlock_unlock(&task->leaf->rwlock);hot_finish(s,task,error);
}
static int hot_publish_raw(struct compaction_scheduler *s,struct compressed_pending_task *task,const struct kv_pair *p,size_t n) {
    struct simple_leaf_node *l=task->leaf;struct hot_raw *r=task->raw_result;int ok=0;
    if(n>HOT_CAP){hot_abort(s,task,1);return 2;}
    if(n)memcpy(r->pairs,p,n*sizeof(*p));r->count=n;
    pthread_rwlock_wrlock(&l->rwlock);
    if(l->pending==task&&l->base_version==task->base_version&&l->pending_token==task->token&&!l->hot_raw) {
        ok=1;for(int i=0;i<ACTIVE_DELTA_ENTRIES;i++)if(active_view(l)[i].key&&hot_insert(r,&active_view(l)[i]))ok=0;
        if(ok){free(l->compressed_data);l->compressed_data=NULL;l->compressed_size=0;l->compressed_capacity=0;l->compressed_usable=0;l->compressed_bytes=0;
            leaf_index_release(l);active_drop(l);l->hot_raw=r;task->raw_result=NULL;l->is_compressed=false;
            l->uncompressed_bytes=r->count*sizeof(*p);l->base_version++;l->generation++;HOT_INC(task->tree,promotions,1);
        }else HOT_INC(task->tree,promotion_overflow,1);
        l->pending=NULL;l->hot_changed_epoch=hot_epoch(task->tree->hot_budget);
    }
    pthread_rwlock_unlock(&l->rwlock);hot_finish(s,task,0);return 2;
}
static int hot_publish_cold(struct compaction_scheduler *s,struct compressed_pending_task *task,struct compressed_page_image *image) {
    struct simple_leaf_node *l=task->leaf;int ok=0;
    pthread_rwlock_wrlock(&l->rwlock);
    if(l->pending==task&&l->base_version==task->base_version&&l->pending_token==task->token&&l->generation==task->generation&&l->hot_raw&&(task->tree->hot_budget->manual||hot_heat(task->tree->hot_budget,l)<=task->tree->hot_budget->cold)) {
        leaf_index_adopt(l,image->index,image->num_subpages);image->index=NULL;
        l->num_subpages=l->num_subpage_entries=image->num_subpages;l->uncompressed_bytes=image->uncompressed_bytes;
        compressed_leaf_adopt_base(l,image->data,image->size,image->usable);image->data=NULL;
        l->generation++;HOT_INC(task->tree,demotions,1);ok=1;
    }
    if(!ok)HOT_INC(task->tree,conflicts,1);
    if(l->pending==task)l->pending=NULL;
    l->hot_changed_epoch=hot_epoch(task->tree->hot_budget);
    pthread_rwlock_unlock(&l->rwlock);hot_finish(s,task,0);return 0;
}
#endif
