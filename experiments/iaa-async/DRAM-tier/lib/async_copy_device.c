#define _GNU_SOURCE
#define _DARWIN_C_SOURCE
#include "async_copy_device.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <time.h>
#ifdef __APPLE__
#include <malloc/malloc.h>
#else
#include <malloc.h>
#endif
struct sd_device {
    pthread_mutex_t mutex; pthread_cond_t changed; pthread_t thread;
    struct sd_config c; struct sd_stats s; struct sd_job **active, **queue;
    unsigned head, count; int closing, fault; uint64_t sequence, next_id, manual_now, last, epoch;
    unsigned tail_every, tail_multiplier, reject;
    int diagnostics;
    struct { int ready; size_t charge; } ready[8];
    struct sd_trace *trace;
    unsigned trace_capacity;
};
static __thread uint64_t thread_adapter_ns;
uint64_t sd_thread_adapter_ns(void){return thread_adapter_ns;}
uint64_t sd_now(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return (uint64_t)t.tv_sec*1000000000+t.tv_nsec; }
static uint64_t cpu_now(void) { struct timespec t; clock_gettime(CLOCK_THREAD_CPUTIME_ID,&t);return (uint64_t)t.tv_sec*1000000000+t.tv_nsec; }
static size_t usable(void *p) {
#ifdef __APPLE__
    return malloc_size(p);
#else
    return malloc_usable_size(p);
#endif
}
static uint64_t now(struct sd_device *d) { return d->c.manual?d->manual_now:sd_now(); }
static void account(struct sd_device *d, uint64_t t) {
    if(d->diagnostics) {
        size_t charges[8];unsigned n=0;
        for(unsigned i=0;i<8;i++)if(d->ready[i].ready)charges[n++]=d->ready[i].charge;
        for(unsigned i=1;i<n;i++){size_t x=charges[i];unsigned j=i;while(j&&charges[j-1]>x){charges[j]=charges[j-1];j--;}charges[j]=x;}
        size_t bytes=d->c.byte_limit-d->s.borrowed_bytes;
        unsigned eligible=0, room=d->c.slots-(unsigned)d->s.active;
        /* Already accepted FIFO work has priority over unaccepted BG jobs. */
        room=room>d->count?room-d->count:0;
        /* Immediate sd_submit also requires a free queue-ring entry. */
        if(d->count<d->c.queue)for(unsigned i=0;i<n&&eligible<room;i++)
            if(charges[i]<=bytes){bytes-=charges[i];eligible++;}
        uint64_t delta=t-d->last;
        d->s.advertised_ready_ns+=delta*n;
        d->s.eligible_idle_slot_ns+=delta*eligible;
        if(n&&!eligible&&(d->count==d->c.queue||charges[0]>d->c.byte_limit-d->s.borrowed_bytes))d->s.credit_blocked_ns+=delta;
        if(n>d->s.advertised_ready_peak)d->s.advertised_ready_peak=n;
    }
    d->s.active_slot_ns+=(t-d->last)*d->s.active; d->last=t; d->s.elapsed_ns=t-d->epoch;
}
static void changed(struct sd_device *d) { d->sequence++;pthread_cond_broadcast(&d->changed); }
static void dispatch(struct sd_device *d, uint64_t t) {
    for(unsigned i=0;i<d->c.slots && d->count;i++) if(!d->active[i]) {
        struct sd_job *j=d->queue[d->head]; d->head=(d->head+1)%d->c.queue;d->count--;
        j->state=2;j->start_ns=t;j->due_ns=t+j->service_ns;j->started_wall_ns=sd_now();
        d->s.queue_ns+=t-j->enqueue_ns;d->s.configured_service_ns+=j->due_ns-t;
        d->s.role_queue_ns[j->role]+=t-j->enqueue_ns;d->s.role_service_ns[j->role]+=j->service_ns;
        if(j->trace_index<d->s.trace_count)d->trace[j->trace_index].started_ns=j->started_wall_ns;
        d->active[i]=j;d->s.active++;d->s.role_active[j->role]++;
        if(d->s.role_active[j->role]>d->s.role_peak[j->role])d->s.role_peak[j->role]=d->s.role_active[j->role];
        if(d->s.active>d->s.peak_active)d->s.peak_active=d->s.active;
    }
    d->s.queued=d->count;
}
static void finish(struct sd_device *d, unsigned i, uint64_t t, int aborting) {
    struct sd_job *j=d->active[i];
    j->status=(aborting||j->fault==1)?SD_ERROR:SD_OK;j->result=0;
    if(j->status==SD_OK) { uint64_t start=sd_now();memcpy(j->output,j->input,j->length);d->s.copy_ns+=sd_now()-start;
        j->result=j->fault==2?j->length+1:j->fault==3?j->length-1:j->length;
        if(j->fault==2)j->status=SD_ERROR; }
    t=now(d);account(d,t);uint64_t late=t>j->due_ns?t-j->due_ns:0;
    d->s.late_ns+=late;if(late>d->s.max_late_ns)d->s.max_late_ns=late;
    unsigned bin=0;for(uint64_t x=late/1000;x && bin<31;x>>=1)bin++;d->s.late_hist[bin]++;
    j->terminal_wall_ns=sd_now();j->state=3;d->s.terminal++; if(j->status==SD_OK)d->s.completed++;else d->s.failed++;
    if(j->trace_index<d->s.trace_count){d->trace[j->trace_index].terminal_ns=j->terminal_wall_ns;d->trace[j->trace_index].status=j->status;}
    d->s.active--;d->s.role_active[j->role]--;d->s.borrowed_bytes-=j->length+j->capacity;d->active[i]=NULL;
}
static void pump(struct sd_device *d, uint64_t t) {
    account(d,t);
    for(unsigned i=0;i<d->c.slots;i++)if(d->active[i] && d->active[i]->due_ns<=t)finish(d,i,t,0);
    t=now(d);account(d,t);dispatch(d,t);changed(d);
}
static void *engine(void *p) {
    struct sd_device *d=p;uint64_t cpu=cpu_now();pthread_mutex_lock(&d->mutex);
    for(;;) {
        uint64_t t=now(d); if(!d->c.manual)pump(d,t);
        d->s.device_cpu_ns=cpu_now()-cpu;
        if(d->closing && !d->s.active && !d->count)break;
        uint64_t due=UINT64_MAX;
        if(!d->c.manual)for(unsigned i=0;i<d->c.slots;i++)if(d->active[i] && d->active[i]->due_ns<due)due=d->active[i]->due_ns;
        if(due==UINT64_MAX)pthread_cond_wait(&d->changed,&d->mutex);
        else {
            uint64_t n=sd_now(), delta=due>n?due-n:0;
            if(!delta)continue;
#ifdef __APPLE__
            struct timespec ts={(time_t)(delta/1000000000),(long)(delta%1000000000)};
            pthread_cond_timedwait_relative_np(&d->changed,&d->mutex,&ts);
#else
            struct timespec ts={(time_t)(due/1000000000),(long)(due%1000000000)};
            pthread_cond_timedwait(&d->changed,&d->mutex,&ts);
#endif
        }
    }
    pthread_mutex_unlock(&d->mutex);return NULL;
}
struct sd_device *sd_create(struct sd_config c) {
    if(!c.slots||c.slots>128||!c.queue||c.queue>4096||!c.byte_limit||c.encode_ns>1000000000||c.decode_ns>1000000000)return NULL;
    struct sd_device *d=calloc(1,sizeof(*d));if(!d)return NULL;d->c=c;
    d->diagnostics=getenv("SD_DIAGNOSTICS")&&atoi(getenv("SD_DIAGNOSTICS"));
    unsigned cap=getenv("SD_TRACE_CAPACITY")?(unsigned)strtoul(getenv("SD_TRACE_CAPACITY"),NULL,10):0;
    if(cap>65536){free(d);return NULL;}d->trace_capacity=cap;
    if(cap){d->trace=calloc(cap,sizeof(*d->trace));if(!d->trace){free(d);return NULL;}}
    d->active=calloc(c.slots,sizeof(*d->active));d->queue=calloc(c.queue,sizeof(*d->queue));
    if(!d->active||!d->queue){free(d->trace);free(d->active);free(d->queue);free(d);return NULL;}
    pthread_mutex_init(&d->mutex,NULL);pthread_condattr_t attr;pthread_condattr_init(&attr);
#ifndef __APPLE__
    pthread_condattr_setclock(&attr,CLOCK_MONOTONIC);
#endif
    pthread_cond_init(&d->changed,&attr);pthread_condattr_destroy(&attr);
    d->epoch=d->last=now(d);
    d->s.owned_requested=sizeof(*d)+(c.slots+c.queue)*sizeof(void *)+cap*sizeof(*d->trace);
    d->s.owned_usable=usable(d)+usable(d->active)+usable(d->queue)+(d->trace?usable(d->trace):0);
    pthread_attr_t thread_attr;pthread_attr_init(&thread_attr);pthread_attr_setstacksize(&thread_attr,256*1024);
    int created=pthread_create(&d->thread,&thread_attr,engine,d);pthread_attr_destroy(&thread_attr);d->s.stack_reserved_bytes=256*1024;
    if(created){pthread_cond_destroy(&d->changed);pthread_mutex_destroy(&d->mutex);free(d->trace);free(d->active);free(d->queue);free(d);return NULL;}
    return d;
}
int sd_submit(struct sd_device *d, struct sd_job *j) {
    if(!d||!j||!j->input||!j->output||!j->length||j->length>j->capacity||j->length>INT_MAX||j->role<0||j->role>2)return SD_INVALID;
    if(j->ready_ns>sd_now())return SD_INVALID; /* Reject a mismatched/future clock. */
    pthread_mutex_lock(&d->mutex);d->s.attempts++;
    if(j->state==1||j->state==2){pthread_mutex_unlock(&d->mutex);return SD_INVALID;}
    if(d->closing){pthread_mutex_unlock(&d->mutex);return SD_CLOSED;}
    size_t charge=j->length+j->capacity;
    if(charge<j->length||charge>d->c.byte_limit){pthread_mutex_unlock(&d->mutex);return SD_NO_SPACE;}
    if(d->reject){d->reject--;d->s.busy++;d->s.injected_busy++;changed(d);pthread_mutex_unlock(&d->mutex);return SD_BUSY;}
    if(d->count==d->c.queue || charge>d->c.byte_limit-d->s.borrowed_bytes) {
        if(d->count==d->c.queue)d->s.queue_busy++;else d->s.byte_busy++;
        d->s.busy++;pthread_mutex_unlock(&d->mutex);return SD_BUSY;
    }
    uint64_t t=now(d);account(d,t);j->state=1;j->status=SD_OK;j->result=0;j->enqueue_ns=t;j->id=++d->next_id;
    j->accepted_wall_ns=sd_now();j->started_wall_ns=j->terminal_wall_ns=j->observed_wall_ns=0;
    if(!j->ready_ns)j->ready_ns=j->accepted_wall_ns;
    d->s.ready_to_accept_ns+=j->accepted_wall_ns-j->ready_ns;
    j->service_ns=(j->encode?d->c.encode_ns:d->c.decode_ns);
    if(d->tail_every&&j->id%d->tail_every==0)j->service_ns*=d->tail_multiplier;
    if(j->role==1&&j->producer_slot>=0&&j->producer_slot<8)d->ready[j->producer_slot].ready=0;
    j->trace_index=UINT_MAX;
    if(d->trace_capacity){if(d->s.trace_count<d->trace_capacity){j->trace_index=(unsigned)d->s.trace_count++;
        d->trace[j->trace_index]=(struct sd_trace){.id=j->id,.ready_ns=j->ready_ns,.accepted_ns=j->accepted_wall_ns,.service_ns=j->service_ns,.input_bytes=j->length,.output_capacity=j->capacity,.role=j->role,.encode=j->encode};
    }else d->s.trace_dropped++;}
    j->fault=d->fault<0?-d->fault:d->fault;if(d->fault>0)d->fault=0;d->queue[(d->head+d->count)%d->c.queue]=j;d->count++;
    d->s.accepted++;d->s.role_accepted[j->role]++;d->s.borrowed_bytes+=charge;
    if(d->s.borrowed_bytes>d->s.peak_borrowed_bytes)d->s.peak_borrowed_bytes=d->s.borrowed_bytes;
    if(d->s.active+d->count>d->s.peak_inflight)d->s.peak_inflight=d->s.active+d->count;
    d->s.descriptor_peak_bytes=d->s.peak_inflight*sizeof(*j);dispatch(d,t);changed(d);
    pthread_mutex_unlock(&d->mutex);return SD_OK;
}
static void observed(struct sd_device *d,struct sd_job *j) {
    if(j->observed_wall_ns)return;
    j->observed_wall_ns=sd_now();uint64_t delta=j->observed_wall_ns-j->terminal_wall_ns;
    d->s.observed++;d->s.discovery_ns+=delta;d->s.role_discovery_ns[j->role]+=delta;
    if(delta>d->s.max_discovery_ns)d->s.max_discovery_ns=delta;
    if(j->trace_index<d->s.trace_count)d->trace[j->trace_index].observed_ns=j->observed_wall_ns;
}
int sd_wait(struct sd_device *d, struct sd_job *j) {
    uint64_t start=sd_now();pthread_mutex_lock(&d->mutex);
    while(j->state!=3){d->s.wait_entries++;pthread_cond_wait(&d->changed,&d->mutex);d->s.wait_wakes++;}
    observed(d,j);
    uint64_t waited=sd_now()-start;d->s.wait_ns+=waited;d->s.role_wait_ns[j->role]+=waited;
    int status=j->status;pthread_mutex_unlock(&d->mutex);return status;
}
uint64_t sd_sequence(struct sd_device *d){pthread_mutex_lock(&d->mutex);uint64_t s=d->sequence;pthread_mutex_unlock(&d->mutex);return s;}
int sd_poll(struct sd_device *d,struct sd_job *j){pthread_mutex_lock(&d->mutex);int ready=j->state==3;if(ready)observed(d,j);pthread_mutex_unlock(&d->mutex);return ready?SD_OK:SD_BUSY;}
void sd_notify(struct sd_device *d){if(!d)return;pthread_mutex_lock(&d->mutex);changed(d);pthread_mutex_unlock(&d->mutex);}
int sd_wait_change(struct sd_device *d,uint64_t s){pthread_mutex_lock(&d->mutex);while(s==d->sequence&&!d->closing)pthread_cond_wait(&d->changed,&d->mutex);int rc=d->closing?SD_CLOSED:SD_OK;pthread_mutex_unlock(&d->mutex);return rc;}
int sd_execute(struct sd_device *d,int encode,int role,const void *src,size_t n,void *dst,size_t cap){
    struct sd_job j={.input=src,.output=dst,.length=n,.capacity=cap,.encode=encode,.role=role,.producer_slot=-1,.ready_ns=sd_now()};
    uint64_t start=sd_now(),busy=0;int result=-1;
    for(;;){uint64_t s=sd_sequence(d);int rc=sd_submit(d,&j);if(rc==SD_OK)break;if(rc!=SD_BUSY)goto done;
        uint64_t b=sd_now();rc=sd_wait_change(d,s);busy+=sd_now()-b;if(rc!=SD_OK)goto done;}
    result=sd_wait(d,&j)==SD_OK && j.result==n?(int)n:-1;
done:;
    uint64_t elapsed=sd_now()-start;thread_adapter_ns+=elapsed;
    pthread_mutex_lock(&d->mutex);d->s.busy_wait_ns+=busy;d->s.adapter_ns[role]+=elapsed;pthread_mutex_unlock(&d->mutex);return result;
}
int sd_drain(struct sd_device *d){pthread_mutex_lock(&d->mutex);while(d->s.active||d->count)pthread_cond_wait(&d->changed,&d->mutex);int rc=d->s.failed?SD_ERROR:SD_OK;pthread_mutex_unlock(&d->mutex);return rc;}
int sd_configure_idle(struct sd_device *d,uint64_t en,uint64_t de,int manual){
    pthread_mutex_lock(&d->mutex);if(d->s.active||d->count||en>1000000000||de>1000000000){pthread_mutex_unlock(&d->mutex);return SD_BUSY;}
    if(d->tail_every&&(en>1000000000/d->tail_multiplier||de>1000000000/d->tail_multiplier)){pthread_mutex_unlock(&d->mutex);return SD_INVALID;}
    d->c.encode_ns=en;d->c.decode_ns=de;d->c.manual=manual;d->epoch=d->last=now(d);changed(d);pthread_mutex_unlock(&d->mutex);return SD_OK;
}
int sd_reset_stats(struct sd_device *d){pthread_mutex_lock(&d->mutex);if(d->s.active||d->count){pthread_mutex_unlock(&d->mutex);return SD_BUSY;}uint64_t req=d->s.owned_requested,use=d->s.owned_usable,cpu=d->s.device_cpu_ns;memset(&d->s,0,sizeof(d->s));d->s.owned_requested=req;d->s.owned_usable=use;d->s.device_cpu_ns=cpu;d->s.stack_reserved_bytes=256*1024;d->epoch=d->last=now(d);pthread_mutex_unlock(&d->mutex);return SD_OK;}
void sd_background_ready(struct sd_device *d,unsigned slot,int ready,size_t input,size_t output){
    if(!d->diagnostics||slot>=8||input+output<input)return;pthread_mutex_lock(&d->mutex);account(d,now(d));
    d->ready[slot].ready=ready;d->ready[slot].charge=input+output;pthread_mutex_unlock(&d->mutex);
}
void sd_background_pause(struct sd_device *d){
    if(!d->diagnostics)return;pthread_mutex_lock(&d->mutex);account(d,now(d));
    for(unsigned i=0;i<8;i++)d->ready[i].ready=0;
    pthread_mutex_unlock(&d->mutex);
}
size_t sd_trace_copy(struct sd_device *d,struct sd_trace *out,size_t cap){pthread_mutex_lock(&d->mutex);size_t n=d->s.trace_count;if(n>cap)n=cap;if(n&&out)memcpy(out,d->trace,n*sizeof(*out));pthread_mutex_unlock(&d->mutex);return n;}
int sd_tail_profile(struct sd_device *d,unsigned every,unsigned multiplier){pthread_mutex_lock(&d->mutex);
    int rc=SD_BUSY;if(!d->s.active&&!d->count){rc=SD_INVALID;
        if(!every||(multiplier&&multiplier<=1000&&d->c.encode_ns<=1000000000/multiplier&&d->c.decode_ns<=1000000000/multiplier)){d->tail_every=every;d->tail_multiplier=multiplier;rc=SD_OK;}}
    pthread_mutex_unlock(&d->mutex);return rc;
}
void sd_busy_next(struct sd_device *d,unsigned attempts){pthread_mutex_lock(&d->mutex);d->reject=attempts;changed(d);pthread_mutex_unlock(&d->mutex);}
void sd_stats(struct sd_device *d,struct sd_stats *s){pthread_mutex_lock(&d->mutex);account(d,now(d));*s=d->s;pthread_mutex_unlock(&d->mutex);}
void sd_advance(struct sd_device *d,uint64_t delta){pthread_mutex_lock(&d->mutex);if(d->c.manual){d->manual_now+=delta;pump(d,d->manual_now);}pthread_mutex_unlock(&d->mutex);}
int sd_release(struct sd_device *d,struct sd_job *j){pthread_mutex_lock(&d->mutex);int rc=SD_INVALID;for(unsigned i=0;i<d->c.slots;i++)if(d->active[i]==j){uint64_t t=now(d);account(d,t);finish(d,i,t,0);dispatch(d,t);changed(d);rc=SD_OK;break;}pthread_mutex_unlock(&d->mutex);return rc;}
void sd_fault_next(struct sd_device *d,int f){pthread_mutex_lock(&d->mutex);d->fault=f;pthread_mutex_unlock(&d->mutex);}
int sd_release_id(struct sd_device *d,uint64_t id){pthread_mutex_lock(&d->mutex);int rc=SD_INVALID;
    if(d->c.manual)for(unsigned i=0;i<d->c.slots;i++)if(d->active[i]&&d->active[i]->id==id){
        uint64_t t=now(d);account(d,t);finish(d,i,t,0);dispatch(d,t);changed(d);rc=SD_OK;break;}
    pthread_mutex_unlock(&d->mutex);return rc;
}
void sd_spurious(struct sd_device *d){pthread_mutex_lock(&d->mutex);pthread_cond_broadcast(&d->changed);pthread_mutex_unlock(&d->mutex);}
void sd_unblock_for_shutdown(struct sd_device *d){if(!d)return;pthread_mutex_lock(&d->mutex);if(d->c.manual){d->c.manual=0;d->c.encode_ns=d->c.decode_ns=0;d->epoch=d->last=now(d);for(unsigned i=0;i<d->c.slots;i++)if(d->active[i])d->active[i]->due_ns=d->last;changed(d);}pthread_mutex_unlock(&d->mutex);}
void sd_close(struct sd_device *d){if(!d)return;pthread_mutex_lock(&d->mutex);d->closing=1;
    if(d->c.manual) {uint64_t t=now(d);account(d,t);while(d->s.active||d->count){for(unsigned i=0;i<d->c.slots;i++)if(d->active[i])finish(d,i,t,1);dispatch(d,t);}}
    changed(d);pthread_mutex_unlock(&d->mutex);pthread_join(d->thread,NULL);
    pthread_cond_destroy(&d->changed);pthread_mutex_destroy(&d->mutex);free(d->trace);free(d->active);free(d->queue);free(d);
}
