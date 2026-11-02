#define _GNU_SOURCE
#define _DARWIN_C_SOURCE
#include "async_copy_device.h"
#include "cpu_profile.h"
#include <pthread.h>
#include <sched.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <time.h>
#ifdef __APPLE__
#include <malloc/malloc.h>
#include <mach/mach.h>
#else
#include <malloc.h>
#endif
struct sd_device {
    pthread_mutex_t mutex; pthread_cond_t changed; pthread_t thread;
    struct sd_config c; struct sd_stats s; struct sd_job **active, **queue;
    unsigned head, count; int closing, fault; uint64_t sequence, next_id, manual_now, last, epoch;
    unsigned tail_every, tail_multiplier, reject;
    int diagnostics, sleep_wait, sleep_timer, force_legacy;
    unsigned sleepers;
    /* Optional calibration: per-byte device time and host CPU per submit/poll. */
    uint64_t encode_ps_per_byte, decode_ps_per_byte, submit_host_ns, poll_host_ns;
    uint64_t dispatched;
    /* Wall-clock fast path: an intrusive MPSC submission queue (Vyukov) and an
     * accepted-but-not-terminal count. `fast_active` is engine-private, so a
     * mode switch can never let one path complete the other path's jobs. */
    struct sd_job stub, *mpsc_head, *mpsc_tail, **fast_active;
    uint64_t inflight;
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
static uint64_t now(struct sd_device *d) { return __atomic_load_n(&d->c.manual,__ATOMIC_ACQUIRE)?d->manual_now:sd_now(); }
/* Wall-clock jobs are polled like hardware completion records; manual-clock
 * tests keep condition-variable waits so their wait/wake counters stay exact. */
#define SD_SPIN_LIMIT_NS 2000000ULL
#define SD_IDLE_SPIN_NS 20000000ULL
static int polled_wait(struct sd_device *d) { return !__atomic_load_n(&d->c.manual,__ATOMIC_ACQUIRE) && !d->sleep_wait; }
/* Lock-free submission/completion applies to wall-clock devices; manual tests,
 * the legacy A/B switches and shutdown-unblocked devices keep the locked path. */
static int fast(struct sd_device *d) {
    return polled_wait(d) && !d->sleep_timer && !__atomic_load_n(&d->force_legacy,__ATOMIC_ACQUIRE);
}
static void cpu_relax(void) {
#if defined(__aarch64__)
    __asm__ __volatile__("yield");
#elif defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#endif
}
static void burn(uint64_t ns) { if(!ns)return; uint64_t end=sd_now()+ns; while(sd_now()<end)cpu_relax(); }
static void stat_add(uint64_t *field,uint64_t value) { __atomic_add_fetch(field,value,__ATOMIC_RELAXED); }
static void stat_max(uint64_t *field,uint64_t value) {
    uint64_t old=__atomic_load_n(field,__ATOMIC_RELAXED);
    while(value>old && !__atomic_compare_exchange_n(field,&old,value,1,__ATOMIC_RELAXED,__ATOMIC_RELAXED));
}
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
/* Every wait on `changed` and every broadcast happen under the device mutex,
 * so broadcasting only when a sleeper is registered cannot lose a wakeup. */
static void changed(struct sd_device *d) { __atomic_add_fetch(&d->sequence,1,__ATOMIC_RELEASE);if(d->sleepers)pthread_cond_broadcast(&d->changed); }
static void sleep_on(struct sd_device *d) { d->sleepers++;pthread_cond_wait(&d->changed,&d->mutex);d->sleepers--; }
static uint64_t service_for(struct sd_device *d,const struct sd_job *j) {
    uint64_t s=(j->encode?d->c.encode_ns:d->c.decode_ns)+
        (uint64_t)j->length*(j->encode?d->encode_ps_per_byte:d->decode_ps_per_byte)/1000;
    if(d->tail_every&&j->id%d->tail_every==0)s*=d->tail_multiplier;
    return s;
}
static void late_record(struct sd_device *d,uint64_t late) {
    stat_add(&d->s.late_ns,late);stat_max(&d->s.max_late_ns,late);
    unsigned bin=0;for(uint64_t x=late/1000;x && bin<31;x>>=1)bin++;stat_add(&d->s.late_hist[bin],1);
}
/* ---------------- locked path: manual clock and legacy switches ---------------- */
static void dispatch(struct sd_device *d, uint64_t t) {
    for(unsigned i=0;i<d->c.slots && d->count;i++) if(!d->active[i]) {
        struct sd_job *j=d->queue[d->head]; d->head=(d->head+1)%d->c.queue;d->count--;
        __atomic_store_n(&j->state,2,__ATOMIC_RELAXED);j->start_ns=t;j->due_ns=t+j->service_ns;j->started_wall_ns=sd_now();
        d->s.queue_ns+=t-j->enqueue_ns;d->s.configured_service_ns+=j->due_ns-t;
        d->s.role_queue_ns[j->role]+=t-j->enqueue_ns;d->s.role_service_ns[j->role]+=j->service_ns;
        if(j->trace_index<d->s.trace_count)d->trace[j->trace_index].started_ns=j->started_wall_ns;
        d->active[i]=j;d->s.active++;d->s.role_active[j->role]++;__atomic_add_fetch(&d->dispatched,1,__ATOMIC_RELEASE);
        if(d->s.role_active[j->role]>d->s.role_peak[j->role])d->s.role_peak[j->role]=d->s.role_active[j->role];
        if(d->s.active>d->s.peak_active)d->s.peak_active=d->s.active;
    }
    d->s.queued=d->count;
}
static void finish(struct sd_device *d, unsigned i, uint64_t t, int aborting) {
    struct sd_job *j=d->active[i];
    j->status=(aborting||j->fault==1)?SD_ERROR:SD_OK;j->result=0;
    if(j->status==SD_OK) { uint64_t start=sd_now();{ ZP_SCOPE(ZP_COPY); memcpy(j->output,j->input,j->length); }d->s.copy_ns+=sd_now()-start;
        j->result=j->fault==2?j->length+1:j->fault==3?j->length-1:j->length;
        if(j->fault==2)j->status=SD_ERROR; }
    t=now(d);account(d,t);uint64_t late=t>j->due_ns?t-j->due_ns:0;
    late_record(d,late);
    j->terminal_wall_ns=sd_now();d->s.terminal++; if(j->status==SD_OK)d->s.completed++;else d->s.failed++;
    if(j->trace_index<d->s.trace_count){d->trace[j->trace_index].terminal_ns=j->terminal_wall_ns;d->trace[j->trace_index].status=j->status;}
    d->s.active--;d->s.role_active[j->role]--;d->s.borrowed_bytes-=j->length+j->capacity;d->active[i]=NULL;
    /* Lock-free pollers may reuse the job immediately: publish terminal last. */
    __atomic_store_n(&j->state,3,__ATOMIC_RELEASE);
}
static void pump(struct sd_device *d, uint64_t t) {
    account(d,t);
    for(unsigned i=0;i<d->c.slots;i++)if(d->active[i] && d->active[i]->due_ns<=t)finish(d,i,t,0);
    t=now(d);account(d,t);dispatch(d,t);changed(d);
}
/* One locked engine iteration; called and returns with the mutex held. */
static int legacy_step(struct sd_device *d) {
    uint64_t t=now(d); if(!d->c.manual)pump(d,t);
    if(d->closing && !d->s.active && !d->count)return 1;
    uint64_t due=UINT64_MAX;
    if(!d->c.manual)for(unsigned i=0;i<d->c.slots;i++)if(d->active[i] && d->active[i]->due_ns<due)due=d->active[i]->due_ns;
    if(due==UINT64_MAX) {
        if(!d->c.manual && !d->sleep_timer) {
            uint64_t seen=__atomic_load_n(&d->dispatched,__ATOMIC_ACQUIRE), idle_end=sd_now()+SD_IDLE_SPIN_NS;
            pthread_mutex_unlock(&d->mutex);
            while(__atomic_load_n(&d->dispatched,__ATOMIC_ACQUIRE)==seen &&
                  !__atomic_load_n(&d->closing,__ATOMIC_ACQUIRE) && sd_now()<idle_end)cpu_relax();
            pthread_mutex_lock(&d->mutex);
            if(__atomic_load_n(&d->dispatched,__ATOMIC_ACQUIRE)!=seen || d->closing)return 0;
        }
        if(!fast(d))sleep_on(d);
        return 0;
    }
    uint64_t n=sd_now(), delta=due>n?due-n:0;
    if(!delta)return 0;
    if(!d->sleep_timer && delta<=SD_SPIN_LIMIT_NS) {
        /* OS timed waits overshoot microsecond service times; a newly
         * dispatched job may complete earlier, so stop spinning on dispatch. */
        uint64_t seen=__atomic_load_n(&d->dispatched,__ATOMIC_ACQUIRE);
        pthread_mutex_unlock(&d->mutex);
        while(sd_now()<due && __atomic_load_n(&d->dispatched,__ATOMIC_ACQUIRE)==seen &&
              !__atomic_load_n(&d->closing,__ATOMIC_ACQUIRE))cpu_relax();
        pthread_mutex_lock(&d->mutex);
        return 0;
    }
#ifdef __APPLE__
    struct timespec ts={(time_t)(delta/1000000000),(long)(delta%1000000000)};
    d->sleepers++;pthread_cond_timedwait_relative_np(&d->changed,&d->mutex,&ts);d->sleepers--;
#else
    struct timespec ts={(time_t)(due/1000000000),(long)(due%1000000000)};
    d->sleepers++;pthread_cond_timedwait(&d->changed,&d->mutex,&ts);d->sleepers--;
#endif
    return 0;
}
/* ---------------- lock-free path: wall-clock device ---------------- */
static void mpsc_push(struct sd_device *d,struct sd_job *n) {
    __atomic_store_n(&n->next,NULL,__ATOMIC_RELAXED);
    struct sd_job *prev=__atomic_exchange_n(&d->mpsc_head,n,__ATOMIC_ACQ_REL);
    __atomic_store_n(&prev->next,n,__ATOMIC_RELEASE);
}
/* Single consumer (engine). NULL may also mean a producer is mid-push. */
static struct sd_job *mpsc_pop(struct sd_device *d) {
    struct sd_job *tail=d->mpsc_tail,*next=__atomic_load_n(&tail->next,__ATOMIC_ACQUIRE);
    if(tail==&d->stub) {
        if(!next)return NULL;
        d->mpsc_tail=next;tail=next;next=__atomic_load_n(&next->next,__ATOMIC_ACQUIRE);
    }
    if(next){d->mpsc_tail=next;return tail;}
    if(tail!=__atomic_load_n(&d->mpsc_head,__ATOMIC_ACQUIRE))return NULL;
    mpsc_push(d,&d->stub);
    next=__atomic_load_n(&tail->next,__ATOMIC_ACQUIRE);
    if(next){d->mpsc_tail=next;return tail;}
    return NULL;
}
static void fast_start(struct sd_device *d,unsigned i,struct sd_job *j,uint64_t t) {
    __atomic_store_n(&j->state,2,__ATOMIC_RELAXED);j->start_ns=t;j->due_ns=t+j->service_ns;j->started_wall_ns=t;
    stat_add(&d->s.queue_ns,t-j->enqueue_ns);stat_add(&d->s.configured_service_ns,j->service_ns);
    stat_add(&d->s.role_queue_ns[j->role],t-j->enqueue_ns);stat_add(&d->s.role_service_ns[j->role],j->service_ns);
    if(j->trace_index<d->trace_capacity)__atomic_store_n(&d->trace[j->trace_index].started_ns,t,__ATOMIC_RELAXED);
    d->fast_active[i]=j;
    stat_max(&d->s.peak_active,__atomic_add_fetch(&d->s.active,1,__ATOMIC_RELAXED));
    stat_max(&d->s.role_peak[j->role],__atomic_add_fetch(&d->s.role_active[j->role],1,__ATOMIC_RELAXED));
    __atomic_add_fetch(&d->dispatched,1,__ATOMIC_RELEASE);
}
static void fast_finish(struct sd_device *d,unsigned i) {
    struct sd_job *j=d->fast_active[i];
    int status=j->fault==1?SD_ERROR:SD_OK;size_t result=0;
    if(status==SD_OK) { uint64_t start=sd_now();{ ZP_SCOPE(ZP_COPY); memcpy(j->output,j->input,j->length); }stat_add(&d->s.copy_ns,sd_now()-start);
        result=j->fault==2?j->length+1:j->fault==3?j->length-1:j->length;
        if(j->fault==2)status=SD_ERROR; }
    uint64_t t=sd_now();late_record(d,t>j->due_ns?t-j->due_ns:0);
    j->status=status;j->result=result;j->terminal_wall_ns=t;
    if(j->trace_index<d->trace_capacity){__atomic_store_n(&d->trace[j->trace_index].terminal_ns,t,__ATOMIC_RELAXED);
        __atomic_store_n(&d->trace[j->trace_index].status,status,__ATOMIC_RELAXED);}
    stat_add(&d->s.terminal,1);stat_add(status==SD_OK?&d->s.completed:&d->s.failed,1);
    int role=j->role;size_t charge=j->length+j->capacity;d->fast_active[i]=NULL;
    __atomic_sub_fetch(&d->s.active,1,__ATOMIC_RELAXED);__atomic_sub_fetch(&d->s.role_active[role],1,__ATOMIC_RELAXED);
    __atomic_sub_fetch(&d->s.borrowed_bytes,charge,__ATOMIC_RELEASE);
    /* The caller may reuse the job as soon as it observes terminal. */
    __atomic_store_n(&j->state,3,__ATOMIC_RELEASE);
    __atomic_sub_fetch(&d->inflight,1,__ATOMIC_ACQ_REL);
    __atomic_add_fetch(&d->sequence,1,__ATOMIC_RELEASE);
}
static void fast_step(struct sd_device *d,uint64_t *idle_since,uint64_t *last) {
    uint64_t t=sd_now();unsigned busy=0;
    for(unsigned i=0;i<d->c.slots;i++)if(d->fast_active[i])busy++;
    stat_add(&d->s.active_slot_ns,(t-*last)*busy);*last=t;
    uint64_t epoch=__atomic_load_n(&d->epoch,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.elapsed_ns,t>=epoch?t-epoch:0,__ATOMIC_RELAXED);
    for(unsigned i=0;i<d->c.slots;i++)if(d->fast_active[i] && d->fast_active[i]->due_ns<=t)fast_finish(d,i);
    /* FIFO admission into free engines, like a hardware work queue. */
    /* Jobs may have been accepted after `t` was sampled: start them at a fresh time. */
    for(unsigned i=0;i<d->c.slots;i++)if(!d->fast_active[i]){struct sd_job *j=mpsc_pop(d);if(!j)break;fast_start(d,i,j,sd_now());}
    busy=0;for(unsigned i=0;i<d->c.slots;i++)if(d->fast_active[i])busy++;
    if(busy || __atomic_load_n(&d->inflight,__ATOMIC_ACQUIRE)){*idle_since=0;cpu_relax();return;}
    if(!*idle_since)*idle_since=t;
    if(t-*idle_since>SD_IDLE_SPIN_NS){struct timespec ts={0,50000};nanosleep(&ts,NULL);}
    else cpu_relax();
}
static void *engine(void *p) {
    zp_register("surrogate_engine");
    struct sd_device *d=p;uint64_t cpu=cpu_now(),idle_since=0,last=sd_now();unsigned tick=0;
    for(;;) {
        /* A submitter that passed fast() just before a mode switch may still
         * enqueue; keep serving the lock-free queue until it is empty. */
        if(fast(d) || __atomic_load_n(&d->inflight,__ATOMIC_ACQUIRE)) {
            if(__atomic_load_n(&d->closing,__ATOMIC_ACQUIRE) && !__atomic_load_n(&d->inflight,__ATOMIC_ACQUIRE))break;
            fast_step(d,&idle_since,&last);
            if(!(++tick&1023))__atomic_store_n(&d->s.device_cpu_ns,cpu_now()-cpu,__ATOMIC_RELAXED);
            continue;
        }
        pthread_mutex_lock(&d->mutex);
        int stop=legacy_step(d);
        __atomic_store_n(&d->s.device_cpu_ns,cpu_now()-cpu,__ATOMIC_RELAXED);
        pthread_mutex_unlock(&d->mutex);
        last=sd_now();
        if(stop)break;
    }
    __atomic_store_n(&d->s.device_cpu_ns,cpu_now()-cpu,__ATOMIC_RELAXED);
    return NULL;
}
struct sd_device *sd_create(struct sd_config c) {
    if(!c.slots||c.slots>128||!c.queue||c.queue>4096||!c.byte_limit||c.encode_ns>1000000000||c.decode_ns>1000000000)return NULL;
    struct sd_device *d=calloc(1,sizeof(*d));if(!d)return NULL;d->c=c;
    d->diagnostics=getenv("SD_DIAGNOSTICS")&&atoi(getenv("SD_DIAGNOSTICS"));
    d->sleep_wait=getenv("SD_SLEEP_WAIT")&&atoi(getenv("SD_SLEEP_WAIT"));
    d->sleep_timer=getenv("SD_SLEEP_TIMER")&&atoi(getenv("SD_SLEEP_TIMER"));
    d->encode_ps_per_byte=getenv("SD_ENCODE_PS_PER_BYTE")?strtoull(getenv("SD_ENCODE_PS_PER_BYTE"),NULL,10):0;
    d->decode_ps_per_byte=getenv("SD_DECODE_PS_PER_BYTE")?strtoull(getenv("SD_DECODE_PS_PER_BYTE"),NULL,10):0;
    d->submit_host_ns=getenv("SD_SUBMIT_HOST_NS")?strtoull(getenv("SD_SUBMIT_HOST_NS"),NULL,10):0;
    d->poll_host_ns=getenv("SD_POLL_HOST_NS")?strtoull(getenv("SD_POLL_HOST_NS"),NULL,10):0;
    d->stub.next=NULL;d->mpsc_head=d->mpsc_tail=&d->stub;
    unsigned cap=getenv("SD_TRACE_CAPACITY")?(unsigned)strtoul(getenv("SD_TRACE_CAPACITY"),NULL,10):0;
    if(cap>65536){free(d);return NULL;}d->trace_capacity=cap;
    if(cap){d->trace=calloc(cap,sizeof(*d->trace));if(!d->trace){free(d);return NULL;}}
    d->active=calloc(c.slots,sizeof(*d->active));d->fast_active=calloc(c.slots,sizeof(*d->fast_active));d->queue=calloc(c.queue,sizeof(*d->queue));
    if(!d->active||!d->fast_active||!d->queue){free(d->trace);free(d->active);free(d->fast_active);free(d->queue);free(d);return NULL;}
    pthread_mutex_init(&d->mutex,NULL);pthread_condattr_t attr;pthread_condattr_init(&attr);
#ifndef __APPLE__
    pthread_condattr_setclock(&attr,CLOCK_MONOTONIC);
#endif
    pthread_cond_init(&d->changed,&attr);pthread_condattr_destroy(&attr);
    d->last=now(d);__atomic_store_n(&d->epoch,d->last,__ATOMIC_RELAXED);
    d->s.owned_requested=sizeof(*d)+(2*c.slots+c.queue)*sizeof(void *)+cap*sizeof(*d->trace);
    d->s.owned_usable=usable(d)+usable(d->active)+usable(d->fast_active)+usable(d->queue)+(d->trace?usable(d->trace):0);
    pthread_attr_t thread_attr;pthread_attr_init(&thread_attr);pthread_attr_setstacksize(&thread_attr,256*1024);
    int created=pthread_create(&d->thread,&thread_attr,engine,d);pthread_attr_destroy(&thread_attr);d->s.stack_reserved_bytes=256*1024;
    if(created){pthread_cond_destroy(&d->changed);pthread_mutex_destroy(&d->mutex);free(d->trace);free(d->active);free(d->fast_active);free(d->queue);free(d);return NULL;}
    return d;
}
static int submit_fast(struct sd_device *d,struct sd_job *j) {
    stat_add(&d->s.attempts,1);
    int state=__atomic_load_n(&j->state,__ATOMIC_ACQUIRE);
    if(state==1||state==2)return SD_INVALID;
    if(__atomic_load_n(&d->closing,__ATOMIC_ACQUIRE))return SD_CLOSED;
    size_t charge=j->length+j->capacity;
    if(charge<j->length||charge>d->c.byte_limit)return SD_NO_SPACE;
    unsigned reject=__atomic_load_n(&d->reject,__ATOMIC_ACQUIRE);
    while(reject)if(__atomic_compare_exchange_n(&d->reject,&reject,reject-1,1,__ATOMIC_ACQ_REL,__ATOMIC_ACQUIRE)) {
        stat_add(&d->s.busy,1);stat_add(&d->s.injected_busy,1);__atomic_add_fetch(&d->sequence,1,__ATOMIC_RELEASE);return SD_BUSY;
    }
    /* Engines plus work-queue entries bound accepted jobs, as in the locked path. */
    uint64_t limit=(uint64_t)d->c.slots+d->c.queue,n=__atomic_load_n(&d->inflight,__ATOMIC_ACQUIRE);
    do { if(n>=limit){stat_add(&d->s.queue_busy,1);stat_add(&d->s.busy,1);return SD_BUSY;} }
    while(!__atomic_compare_exchange_n(&d->inflight,&n,n+1,1,__ATOMIC_ACQ_REL,__ATOMIC_ACQUIRE));
    uint64_t b=__atomic_load_n(&d->s.borrowed_bytes,__ATOMIC_ACQUIRE);
    do { if(charge>d->c.byte_limit-b){__atomic_sub_fetch(&d->inflight,1,__ATOMIC_ACQ_REL);
            stat_add(&d->s.byte_busy,1);stat_add(&d->s.busy,1);return SD_BUSY;} }
    while(!__atomic_compare_exchange_n(&d->s.borrowed_bytes,&b,b+charge,1,__ATOMIC_ACQ_REL,__ATOMIC_ACQUIRE));
    stat_max(&d->s.peak_borrowed_bytes,b+charge);stat_max(&d->s.peak_inflight,n+1);
    stat_max(&d->s.descriptor_peak_bytes,(n+1)*sizeof(*j));
    uint64_t t=sd_now();
    __atomic_store_n(&j->state,1,__ATOMIC_RELAXED);j->status=SD_OK;j->result=0;j->enqueue_ns=t;
    j->id=__atomic_add_fetch(&d->next_id,1,__ATOMIC_RELAXED);
    j->accepted_wall_ns=t;j->started_wall_ns=j->terminal_wall_ns=j->observed_wall_ns=0;
    if(!j->ready_ns)j->ready_ns=t;
    stat_add(&d->s.ready_to_accept_ns,t-j->ready_ns);
    j->service_ns=service_for(d,j);
    j->trace_index=UINT_MAX;
    if(d->trace_capacity) {
        uint64_t idx=__atomic_load_n(&d->s.trace_count,__ATOMIC_ACQUIRE);
        do { if(idx>=d->trace_capacity){stat_add(&d->s.trace_dropped,1);break;} }
        while(!__atomic_compare_exchange_n(&d->s.trace_count,&idx,idx+1,1,__ATOMIC_ACQ_REL,__ATOMIC_ACQUIRE));
        if(idx<d->trace_capacity){j->trace_index=(unsigned)idx;
            d->trace[idx]=(struct sd_trace){.id=j->id,.ready_ns=j->ready_ns,.accepted_ns=t,.service_ns=j->service_ns,.input_bytes=j->length,.output_capacity=j->capacity,.role=j->role,.encode=j->encode};}
    }
    int fault=__atomic_load_n(&d->fault,__ATOMIC_ACQUIRE);j->fault=fault<0?-fault:fault;
    if(fault>0)__atomic_compare_exchange_n(&d->fault,&fault,0,0,__ATOMIC_ACQ_REL,__ATOMIC_RELAXED);
    stat_add(&d->s.accepted,1);stat_add(&d->s.role_accepted[j->role],1);
    mpsc_push(d,j);
    __atomic_add_fetch(&d->sequence,1,__ATOMIC_RELEASE);
    return SD_OK;
}
int sd_submit(struct sd_device *d, struct sd_job *j) {
    ZP_SCOPE(ZP_SUBMIT);
    if(!d||!j||!j->input||!j->output||!j->length||j->length>j->capacity||j->length>INT_MAX||j->role<0||j->role>2)return SD_INVALID;
    if(j->ready_ns>sd_now())return SD_INVALID; /* Reject a mismatched/future clock. */
    burn(d->submit_host_ns);
    if(fast(d))return submit_fast(d,j);
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
    uint64_t t=now(d);account(d,t);__atomic_store_n(&j->state,1,__ATOMIC_RELAXED);j->status=SD_OK;j->result=0;j->enqueue_ns=t;j->id=++d->next_id;
    j->accepted_wall_ns=sd_now();j->started_wall_ns=j->terminal_wall_ns=j->observed_wall_ns=0;
    if(!j->ready_ns)j->ready_ns=j->accepted_wall_ns;
    d->s.ready_to_accept_ns+=j->accepted_wall_ns-j->ready_ns;
    j->service_ns=service_for(d,j);
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
/* Completion-side statistics are atomic so that observing a finished job, like
 * reading a hardware completion record, never takes the device mutex. */
static void observed(struct sd_device *d,struct sd_job *j) {
    if(j->observed_wall_ns)return;
    j->observed_wall_ns=sd_now();uint64_t delta=j->observed_wall_ns-j->terminal_wall_ns;
    stat_add(&d->s.observed,1);stat_add(&d->s.discovery_ns,delta);stat_add(&d->s.role_discovery_ns[j->role],delta);
    stat_max(&d->s.max_discovery_ns,delta);
    if(j->trace_index<d->trace_capacity)
        __atomic_store_n(&d->trace[j->trace_index].observed_ns,j->observed_wall_ns,__ATOMIC_RELAXED);
}
int sd_wait(struct sd_device *d, struct sd_job *j) {
    ZP_SCOPE(ZP_WAIT);
    uint64_t start=sd_now();int locked=0;
    if(polled_wait(d)) {
        /* Synchronous QPL waits spin on the completion record; yield only
         * after a long wait so an oversubscribed host can still make progress. */
        for(unsigned spins=1;__atomic_load_n(&j->state,__ATOMIC_ACQUIRE)!=3;spins++) {
            cpu_relax();
            if(!(spins%1024) && sd_now()-start>50000)sched_yield();
        }
    } else {
        pthread_mutex_lock(&d->mutex);locked=1;
        while(j->state!=3){d->s.wait_entries++;sleep_on(d);d->s.wait_wakes++;}
    }
    observed(d,j);
    uint64_t waited=sd_now()-start;stat_add(&d->s.wait_ns,waited);stat_add(&d->s.role_wait_ns[j->role],waited);
    int status=j->status;if(locked)pthread_mutex_unlock(&d->mutex);return status;
}
uint64_t sd_sequence(struct sd_device *d){return __atomic_load_n(&d->sequence,__ATOMIC_ACQUIRE);}
int sd_poll(struct sd_device *d,struct sd_job *j){
    ZP_SCOPE(ZP_POLL);
    burn(d->poll_host_ns);
    if(__atomic_load_n(&j->state,__ATOMIC_ACQUIRE)!=3)return SD_BUSY;
    observed(d,j);return SD_OK;
}
void sd_notify(struct sd_device *d){if(!d)return;pthread_mutex_lock(&d->mutex);changed(d);pthread_mutex_unlock(&d->mutex);}
int sd_wait_change(struct sd_device *d,uint64_t s){
    ZP_SCOPE(ZP_WAIT);
    if(polled_wait(d)) {
        while(__atomic_load_n(&d->sequence,__ATOMIC_ACQUIRE)==s && !__atomic_load_n(&d->closing,__ATOMIC_ACQUIRE))sched_yield();
        return __atomic_load_n(&d->closing,__ATOMIC_ACQUIRE)?SD_CLOSED:SD_OK;
    }
    pthread_mutex_lock(&d->mutex);while(s==d->sequence&&!d->closing)sleep_on(d);int rc=d->closing?SD_CLOSED:SD_OK;pthread_mutex_unlock(&d->mutex);return rc;}
int sd_execute(struct sd_device *d,int encode,int role,const void *src,size_t n,void *dst,size_t cap){
    struct sd_job j={.input=src,.output=dst,.length=n,.capacity=cap,.encode=encode,.role=role,.producer_slot=-1,.ready_ns=sd_now()};
    uint64_t start=sd_now(),busy=0;int result=-1;
    for(;;){uint64_t s=sd_sequence(d);int rc=sd_submit(d,&j);if(rc==SD_OK)break;if(rc!=SD_BUSY)goto done;
        uint64_t b=sd_now();rc=sd_wait_change(d,s);busy+=sd_now()-b;if(rc!=SD_OK)goto done;}
    result=sd_wait(d,&j)==SD_OK && j.result==n?(int)n:-1;
done:;
    uint64_t elapsed=sd_now()-start;thread_adapter_ns+=elapsed;
    stat_add(&d->s.busy_wait_ns,busy);stat_add(&d->s.adapter_ns[role],elapsed);return result;
}
static int idle(struct sd_device *d){return !__atomic_load_n(&d->s.active,__ATOMIC_ACQUIRE) && !d->count && !__atomic_load_n(&d->inflight,__ATOMIC_ACQUIRE);}
int sd_drain(struct sd_device *d){
    if(fast(d)) {
        while(__atomic_load_n(&d->inflight,__ATOMIC_ACQUIRE)){struct timespec ts={0,10000};nanosleep(&ts,NULL);}
        return __atomic_load_n(&d->s.failed,__ATOMIC_ACQUIRE)?SD_ERROR:SD_OK;
    }
    pthread_mutex_lock(&d->mutex);while(d->s.active||d->count)sleep_on(d);int rc=d->s.failed?SD_ERROR:SD_OK;pthread_mutex_unlock(&d->mutex);return rc;}
int sd_configure_idle(struct sd_device *d,uint64_t en,uint64_t de,int manual){
    pthread_mutex_lock(&d->mutex);if(!idle(d)||en>1000000000||de>1000000000){pthread_mutex_unlock(&d->mutex);return SD_BUSY;}
    if(d->tail_every&&(en>1000000000/d->tail_multiplier||de>1000000000/d->tail_multiplier)){pthread_mutex_unlock(&d->mutex);return SD_INVALID;}
    d->c.encode_ns=en;d->c.decode_ns=de;__atomic_store_n(&d->c.manual,manual,__ATOMIC_RELEASE);d->last=now(d);__atomic_store_n(&d->epoch,d->last,__ATOMIC_RELAXED);changed(d);pthread_mutex_unlock(&d->mutex);return SD_OK;
}
/* The v2 engine updates counters atomically without taking the mutex.
 * Administrative snapshots/reset must use atomics too. A snapshot during work
 * is approximate; conservation is checked only after drain. */
static void snapshot_stats(struct sd_device *d,struct sd_stats *s) {
    s->attempts=__atomic_load_n(&d->s.attempts,__ATOMIC_RELAXED);
    s->accepted=__atomic_load_n(&d->s.accepted,__ATOMIC_RELAXED);
    s->busy=__atomic_load_n(&d->s.busy,__ATOMIC_RELAXED);
    s->completed=__atomic_load_n(&d->s.completed,__ATOMIC_RELAXED);
    s->failed=__atomic_load_n(&d->s.failed,__ATOMIC_RELAXED);
    s->terminal=__atomic_load_n(&d->s.terminal,__ATOMIC_RELAXED);
    s->active=__atomic_load_n(&d->s.active,__ATOMIC_RELAXED);
    s->queued=__atomic_load_n(&d->s.queued,__ATOMIC_RELAXED);
    s->peak_active=__atomic_load_n(&d->s.peak_active,__ATOMIC_RELAXED);
    s->peak_inflight=__atomic_load_n(&d->s.peak_inflight,__ATOMIC_RELAXED);
    s->borrowed_bytes=__atomic_load_n(&d->s.borrowed_bytes,__ATOMIC_RELAXED);
    s->peak_borrowed_bytes=__atomic_load_n(&d->s.peak_borrowed_bytes,__ATOMIC_RELAXED);
    s->queue_ns=__atomic_load_n(&d->s.queue_ns,__ATOMIC_RELAXED);
    s->configured_service_ns=__atomic_load_n(&d->s.configured_service_ns,__ATOMIC_RELAXED);
    s->late_ns=__atomic_load_n(&d->s.late_ns,__ATOMIC_RELAXED);
    s->max_late_ns=__atomic_load_n(&d->s.max_late_ns,__ATOMIC_RELAXED);
    s->copy_ns=__atomic_load_n(&d->s.copy_ns,__ATOMIC_RELAXED);
    s->wait_ns=__atomic_load_n(&d->s.wait_ns,__ATOMIC_RELAXED);
    s->active_slot_ns=__atomic_load_n(&d->s.active_slot_ns,__ATOMIC_RELAXED);
    s->elapsed_ns=__atomic_load_n(&d->s.elapsed_ns,__ATOMIC_RELAXED);
    s->device_cpu_ns=__atomic_load_n(&d->s.device_cpu_ns,__ATOMIC_RELAXED);
    s->wait_entries=__atomic_load_n(&d->s.wait_entries,__ATOMIC_RELAXED);
    s->wait_wakes=__atomic_load_n(&d->s.wait_wakes,__ATOMIC_RELAXED);
    s->busy_wait_ns=__atomic_load_n(&d->s.busy_wait_ns,__ATOMIC_RELAXED);
    s->adapter_ns[0]=__atomic_load_n(&d->s.adapter_ns[0],__ATOMIC_RELAXED);
    s->adapter_ns[1]=__atomic_load_n(&d->s.adapter_ns[1],__ATOMIC_RELAXED);
    s->adapter_ns[2]=__atomic_load_n(&d->s.adapter_ns[2],__ATOMIC_RELAXED);
    s->stack_reserved_bytes=__atomic_load_n(&d->s.stack_reserved_bytes,__ATOMIC_RELAXED);
    s->role_active[0]=__atomic_load_n(&d->s.role_active[0],__ATOMIC_RELAXED);
    s->role_active[1]=__atomic_load_n(&d->s.role_active[1],__ATOMIC_RELAXED);
    s->role_active[2]=__atomic_load_n(&d->s.role_active[2],__ATOMIC_RELAXED);
    s->role_peak[0]=__atomic_load_n(&d->s.role_peak[0],__ATOMIC_RELAXED);
    s->role_peak[1]=__atomic_load_n(&d->s.role_peak[1],__ATOMIC_RELAXED);
    s->role_peak[2]=__atomic_load_n(&d->s.role_peak[2],__ATOMIC_RELAXED);
    s->role_accepted[0]=__atomic_load_n(&d->s.role_accepted[0],__ATOMIC_RELAXED);
    s->role_accepted[1]=__atomic_load_n(&d->s.role_accepted[1],__ATOMIC_RELAXED);
    s->role_accepted[2]=__atomic_load_n(&d->s.role_accepted[2],__ATOMIC_RELAXED);
    s->role_wait_ns[0]=__atomic_load_n(&d->s.role_wait_ns[0],__ATOMIC_RELAXED);
    s->role_wait_ns[1]=__atomic_load_n(&d->s.role_wait_ns[1],__ATOMIC_RELAXED);
    s->role_wait_ns[2]=__atomic_load_n(&d->s.role_wait_ns[2],__ATOMIC_RELAXED);
    s->descriptor_peak_bytes=__atomic_load_n(&d->s.descriptor_peak_bytes,__ATOMIC_RELAXED);
    s->owned_requested=__atomic_load_n(&d->s.owned_requested,__ATOMIC_RELAXED);
    s->owned_usable=__atomic_load_n(&d->s.owned_usable,__ATOMIC_RELAXED);
    s->late_hist[0]=__atomic_load_n(&d->s.late_hist[0],__ATOMIC_RELAXED);
    s->late_hist[1]=__atomic_load_n(&d->s.late_hist[1],__ATOMIC_RELAXED);
    s->late_hist[2]=__atomic_load_n(&d->s.late_hist[2],__ATOMIC_RELAXED);
    s->late_hist[3]=__atomic_load_n(&d->s.late_hist[3],__ATOMIC_RELAXED);
    s->late_hist[4]=__atomic_load_n(&d->s.late_hist[4],__ATOMIC_RELAXED);
    s->late_hist[5]=__atomic_load_n(&d->s.late_hist[5],__ATOMIC_RELAXED);
    s->late_hist[6]=__atomic_load_n(&d->s.late_hist[6],__ATOMIC_RELAXED);
    s->late_hist[7]=__atomic_load_n(&d->s.late_hist[7],__ATOMIC_RELAXED);
    s->late_hist[8]=__atomic_load_n(&d->s.late_hist[8],__ATOMIC_RELAXED);
    s->late_hist[9]=__atomic_load_n(&d->s.late_hist[9],__ATOMIC_RELAXED);
    s->late_hist[10]=__atomic_load_n(&d->s.late_hist[10],__ATOMIC_RELAXED);
    s->late_hist[11]=__atomic_load_n(&d->s.late_hist[11],__ATOMIC_RELAXED);
    s->late_hist[12]=__atomic_load_n(&d->s.late_hist[12],__ATOMIC_RELAXED);
    s->late_hist[13]=__atomic_load_n(&d->s.late_hist[13],__ATOMIC_RELAXED);
    s->late_hist[14]=__atomic_load_n(&d->s.late_hist[14],__ATOMIC_RELAXED);
    s->late_hist[15]=__atomic_load_n(&d->s.late_hist[15],__ATOMIC_RELAXED);
    s->late_hist[16]=__atomic_load_n(&d->s.late_hist[16],__ATOMIC_RELAXED);
    s->late_hist[17]=__atomic_load_n(&d->s.late_hist[17],__ATOMIC_RELAXED);
    s->late_hist[18]=__atomic_load_n(&d->s.late_hist[18],__ATOMIC_RELAXED);
    s->late_hist[19]=__atomic_load_n(&d->s.late_hist[19],__ATOMIC_RELAXED);
    s->late_hist[20]=__atomic_load_n(&d->s.late_hist[20],__ATOMIC_RELAXED);
    s->late_hist[21]=__atomic_load_n(&d->s.late_hist[21],__ATOMIC_RELAXED);
    s->late_hist[22]=__atomic_load_n(&d->s.late_hist[22],__ATOMIC_RELAXED);
    s->late_hist[23]=__atomic_load_n(&d->s.late_hist[23],__ATOMIC_RELAXED);
    s->late_hist[24]=__atomic_load_n(&d->s.late_hist[24],__ATOMIC_RELAXED);
    s->late_hist[25]=__atomic_load_n(&d->s.late_hist[25],__ATOMIC_RELAXED);
    s->late_hist[26]=__atomic_load_n(&d->s.late_hist[26],__ATOMIC_RELAXED);
    s->late_hist[27]=__atomic_load_n(&d->s.late_hist[27],__ATOMIC_RELAXED);
    s->late_hist[28]=__atomic_load_n(&d->s.late_hist[28],__ATOMIC_RELAXED);
    s->late_hist[29]=__atomic_load_n(&d->s.late_hist[29],__ATOMIC_RELAXED);
    s->late_hist[30]=__atomic_load_n(&d->s.late_hist[30],__ATOMIC_RELAXED);
    s->late_hist[31]=__atomic_load_n(&d->s.late_hist[31],__ATOMIC_RELAXED);
    s->observed=__atomic_load_n(&d->s.observed,__ATOMIC_RELAXED);
    s->discovery_ns=__atomic_load_n(&d->s.discovery_ns,__ATOMIC_RELAXED);
    s->max_discovery_ns=__atomic_load_n(&d->s.max_discovery_ns,__ATOMIC_RELAXED);
    s->ready_to_accept_ns=__atomic_load_n(&d->s.ready_to_accept_ns,__ATOMIC_RELAXED);
    s->role_queue_ns[0]=__atomic_load_n(&d->s.role_queue_ns[0],__ATOMIC_RELAXED);
    s->role_queue_ns[1]=__atomic_load_n(&d->s.role_queue_ns[1],__ATOMIC_RELAXED);
    s->role_queue_ns[2]=__atomic_load_n(&d->s.role_queue_ns[2],__ATOMIC_RELAXED);
    s->role_service_ns[0]=__atomic_load_n(&d->s.role_service_ns[0],__ATOMIC_RELAXED);
    s->role_service_ns[1]=__atomic_load_n(&d->s.role_service_ns[1],__ATOMIC_RELAXED);
    s->role_service_ns[2]=__atomic_load_n(&d->s.role_service_ns[2],__ATOMIC_RELAXED);
    s->role_discovery_ns[0]=__atomic_load_n(&d->s.role_discovery_ns[0],__ATOMIC_RELAXED);
    s->role_discovery_ns[1]=__atomic_load_n(&d->s.role_discovery_ns[1],__ATOMIC_RELAXED);
    s->role_discovery_ns[2]=__atomic_load_n(&d->s.role_discovery_ns[2],__ATOMIC_RELAXED);
    s->queue_busy=__atomic_load_n(&d->s.queue_busy,__ATOMIC_RELAXED);
    s->byte_busy=__atomic_load_n(&d->s.byte_busy,__ATOMIC_RELAXED);
    s->injected_busy=__atomic_load_n(&d->s.injected_busy,__ATOMIC_RELAXED);
    s->trace_count=__atomic_load_n(&d->s.trace_count,__ATOMIC_RELAXED);
    s->trace_dropped=__atomic_load_n(&d->s.trace_dropped,__ATOMIC_RELAXED);
    s->advertised_ready_ns=__atomic_load_n(&d->s.advertised_ready_ns,__ATOMIC_RELAXED);
    s->eligible_idle_slot_ns=__atomic_load_n(&d->s.eligible_idle_slot_ns,__ATOMIC_RELAXED);
    s->credit_blocked_ns=__atomic_load_n(&d->s.credit_blocked_ns,__ATOMIC_RELAXED);
    s->advertised_ready_peak=__atomic_load_n(&d->s.advertised_ready_peak,__ATOMIC_RELAXED);
}
static void zero_stats(struct sd_device *d) {
    __atomic_store_n(&d->s.attempts,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.accepted,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.busy,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.completed,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.failed,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.terminal,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.active,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.queued,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.peak_active,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.peak_inflight,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.borrowed_bytes,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.peak_borrowed_bytes,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.queue_ns,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.configured_service_ns,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_ns,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.max_late_ns,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.copy_ns,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.wait_ns,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.active_slot_ns,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.elapsed_ns,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.device_cpu_ns,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.wait_entries,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.wait_wakes,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.busy_wait_ns,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.adapter_ns[0],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.adapter_ns[1],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.adapter_ns[2],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.stack_reserved_bytes,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_active[0],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_active[1],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_active[2],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_peak[0],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_peak[1],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_peak[2],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_accepted[0],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_accepted[1],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_accepted[2],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_wait_ns[0],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_wait_ns[1],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_wait_ns[2],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.descriptor_peak_bytes,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.owned_requested,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.owned_usable,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[0],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[1],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[2],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[3],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[4],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[5],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[6],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[7],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[8],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[9],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[10],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[11],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[12],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[13],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[14],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[15],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[16],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[17],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[18],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[19],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[20],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[21],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[22],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[23],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[24],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[25],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[26],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[27],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[28],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[29],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[30],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.late_hist[31],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.observed,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.discovery_ns,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.max_discovery_ns,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.ready_to_accept_ns,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_queue_ns[0],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_queue_ns[1],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_queue_ns[2],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_service_ns[0],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_service_ns[1],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_service_ns[2],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_discovery_ns[0],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_discovery_ns[1],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.role_discovery_ns[2],0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.queue_busy,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.byte_busy,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.injected_busy,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.trace_count,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.trace_dropped,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.advertised_ready_ns,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.eligible_idle_slot_ns,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.credit_blocked_ns,0,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.advertised_ready_peak,0,__ATOMIC_RELAXED);
}
static uint64_t device_thread_cpu(struct sd_device *d,uint64_t fallback) {
#ifdef __APPLE__
    thread_basic_info_data_t info;mach_msg_type_number_t n=THREAD_BASIC_INFO_COUNT;
    if(thread_info(pthread_mach_thread_np(d->thread),THREAD_BASIC_INFO,(thread_info_t)&info,&n)==KERN_SUCCESS)
        return ((uint64_t)info.user_time.seconds+info.system_time.seconds)*1000000000ULL+
               ((uint64_t)info.user_time.microseconds+info.system_time.microseconds)*1000;
#else
    clockid_t id;struct timespec t;
    if(!pthread_getcpuclockid(d->thread,&id)&&!clock_gettime(id,&t))
        return (uint64_t)t.tv_sec*1000000000ULL+(uint64_t)t.tv_nsec;
#endif
    return fallback;
}
int sd_reset_stats(struct sd_device *d){
    pthread_mutex_lock(&d->mutex);if(!idle(d)){pthread_mutex_unlock(&d->mutex);return SD_BUSY;}
    struct sd_stats old;snapshot_stats(d,&old);zero_stats(d);
    __atomic_store_n(&d->s.owned_requested,old.owned_requested,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.owned_usable,old.owned_usable,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.device_cpu_ns,old.device_cpu_ns,__ATOMIC_RELAXED);
    __atomic_store_n(&d->s.stack_reserved_bytes,256*1024,__ATOMIC_RELAXED);
    d->last=now(d);__atomic_store_n(&d->epoch,d->last,__ATOMIC_RELAXED);
    pthread_mutex_unlock(&d->mutex);return SD_OK;
}
void sd_background_ready(struct sd_device *d,unsigned slot,int ready,size_t input,size_t output){
    if(!d->diagnostics||slot>=8||input+output<input||fast(d)||__atomic_load_n(&d->inflight,__ATOMIC_ACQUIRE))return;pthread_mutex_lock(&d->mutex);account(d,now(d));
    d->ready[slot].ready=ready;d->ready[slot].charge=input+output;pthread_mutex_unlock(&d->mutex);
}
void sd_background_pause(struct sd_device *d){
    if(!d->diagnostics||fast(d)||__atomic_load_n(&d->inflight,__ATOMIC_ACQUIRE))return;pthread_mutex_lock(&d->mutex);account(d,now(d));
    for(unsigned i=0;i<8;i++)d->ready[i].ready=0;
    pthread_mutex_unlock(&d->mutex);
}
size_t sd_trace_copy(struct sd_device *d,struct sd_trace *out,size_t cap){pthread_mutex_lock(&d->mutex);size_t n=d->s.trace_count;if(n>cap)n=cap;if(n&&out)memcpy(out,d->trace,n*sizeof(*out));pthread_mutex_unlock(&d->mutex);return n;}
int sd_tail_profile(struct sd_device *d,unsigned every,unsigned multiplier){pthread_mutex_lock(&d->mutex);
    int rc=SD_BUSY;if(idle(d)){rc=SD_INVALID;
        if(!every||(multiplier&&multiplier<=1000&&d->c.encode_ns<=1000000000/multiplier&&d->c.decode_ns<=1000000000/multiplier)){d->tail_every=every;d->tail_multiplier=multiplier;rc=SD_OK;}}
    pthread_mutex_unlock(&d->mutex);return rc;
}
void sd_busy_next(struct sd_device *d,unsigned attempts){pthread_mutex_lock(&d->mutex);__atomic_store_n(&d->reject,attempts,__ATOMIC_RELEASE);changed(d);pthread_mutex_unlock(&d->mutex);}
void sd_stats(struct sd_device *d,struct sd_stats *s){
    pthread_mutex_lock(&d->mutex);if(!fast(d)&&!__atomic_load_n(&d->inflight,__ATOMIC_ACQUIRE))account(d,now(d));snapshot_stats(d,s);
    s->device_cpu_ns=device_thread_cpu(d,s->device_cpu_ns);
    if(fast(d)){uint64_t inflight=__atomic_load_n(&d->inflight,__ATOMIC_ACQUIRE);s->queued=inflight>s->active?inflight-s->active:0;}
    pthread_mutex_unlock(&d->mutex);
}
void sd_advance(struct sd_device *d,uint64_t delta){pthread_mutex_lock(&d->mutex);if(d->c.manual){d->manual_now+=delta;pump(d,d->manual_now);}pthread_mutex_unlock(&d->mutex);}
int sd_release(struct sd_device *d,struct sd_job *j){pthread_mutex_lock(&d->mutex);int rc=SD_INVALID;for(unsigned i=0;i<d->c.slots;i++)if(d->active[i]==j){uint64_t t=now(d);account(d,t);finish(d,i,t,0);dispatch(d,t);changed(d);rc=SD_OK;break;}pthread_mutex_unlock(&d->mutex);return rc;}
void sd_fault_next(struct sd_device *d,int f){pthread_mutex_lock(&d->mutex);__atomic_store_n(&d->fault,f,__ATOMIC_RELEASE);pthread_mutex_unlock(&d->mutex);}
int sd_release_id(struct sd_device *d,uint64_t id){pthread_mutex_lock(&d->mutex);int rc=SD_INVALID;
    if(d->c.manual)for(unsigned i=0;i<d->c.slots;i++)if(d->active[i]&&d->active[i]->id==id){
        uint64_t t=now(d);account(d,t);finish(d,i,t,0);dispatch(d,t);changed(d);rc=SD_OK;break;}
    pthread_mutex_unlock(&d->mutex);return rc;
}
void sd_spurious(struct sd_device *d){pthread_mutex_lock(&d->mutex);pthread_cond_broadcast(&d->changed);pthread_mutex_unlock(&d->mutex);}
/* Gated manual jobs finish through the locked path; the device never re-enters
 * the lock-free path afterwards, so the two structures never mix. */
void sd_unblock_for_shutdown(struct sd_device *d){if(!d)return;pthread_mutex_lock(&d->mutex);if(d->c.manual){__atomic_store_n(&d->force_legacy,1,__ATOMIC_RELEASE);__atomic_store_n(&d->c.manual,0,__ATOMIC_RELEASE);d->c.encode_ns=d->c.decode_ns=0;d->last=now(d);__atomic_store_n(&d->epoch,d->last,__ATOMIC_RELAXED);for(unsigned i=0;i<d->c.slots;i++)if(d->active[i])d->active[i]->due_ns=d->last;changed(d);}pthread_mutex_unlock(&d->mutex);}
void sd_close(struct sd_device *d){if(!d)return;pthread_mutex_lock(&d->mutex);__atomic_store_n(&d->closing,1,__ATOMIC_RELEASE);
    if(d->c.manual) {uint64_t t=now(d);account(d,t);while(d->s.active||d->count){for(unsigned i=0;i<d->c.slots;i++)if(d->active[i])finish(d,i,t,1);dispatch(d,t);}}
    changed(d);pthread_mutex_unlock(&d->mutex);pthread_join(d->thread,NULL);
    pthread_cond_destroy(&d->changed);pthread_mutex_destroy(&d->mutex);free(d->trace);free(d->active);free(d->fast_active);free(d->queue);free(d);
}
