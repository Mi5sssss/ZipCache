#include "async_copy_device.h"
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static void need(int ok){if(!ok)abort();}
struct waiter {struct sd_device *d;struct sd_job *j;int done;};
static void *wait_job(void *p){struct waiter *w=p;need(sd_wait(w->d,w->j)==SD_OK);__atomic_store_n(&w->done,1,__ATOMIC_RELEASE);return NULL;}
static struct sd_job job(char *a,char *b,int encode){return (struct sd_job){.input=a,.output=b,.length=32,.capacity=32,.encode=encode,.role=encode?1:0};}
int main(int argc,char **argv){
    char input[32];memset(input,42,32);char output[128][32];
    if(argc>1){unsigned slots=(unsigned)atoi(argv[1]);need(slots && slots<=128);
        struct sd_device *d=sd_create((struct sd_config){slots,2*slots,8*1024*1024,1000000,1000000,0});need(d!=NULL);
        uint64_t start=sd_now();
        do {struct sd_job jobs[128];for(unsigned i=0;i<slots;i++){jobs[i]=job(input,output[i],1);need(sd_submit(d,jobs+i)==SD_OK);}
            for(unsigned i=0;i<slots;i++)need(sd_wait(d,jobs+i)==SD_OK && !memcmp(input,output[i],32));
        }while(sd_now()-start<250000000);
        struct sd_stats s;sd_stats(d,&s);need(s.peak_active==slots && s.accepted==s.terminal && !s.failed);
        printf("{\"kind\":\"device-only\",\"slots\":%u,\"peak_active\":%llu,\"accepted\":%llu,\"simulated_slot_utilization\":%.6f,\"max_completion_late_ns\":%llu}\n",slots,(unsigned long long)s.peak_active,(unsigned long long)s.accepted,(double)s.active_slot_ns/(slots*s.elapsed_ns),(unsigned long long)s.max_late_ns);
        sd_close(d);return 0;
    }
    setenv("SD_DIAGNOSTICS","1",1);setenv("SD_TRACE_CAPACITY","32",1);
    struct sd_device *d=sd_create((struct sd_config){2,1,192,100,10,1});need(d!=NULL);
    struct sd_job a=job(input,output[0],1),b=job(input,output[1],0),c=job(input,output[2],1),e=job(input,output[3],1);
    a.ready_ns=sd_now()+1000000000;need(sd_submit(d,&a)==SD_INVALID&&!a.state);a.ready_ns=0;
    need(sd_submit(d,&a)==SD_OK && sd_submit(d,&a)==SD_INVALID);
    need(sd_submit(d,&b)==SD_OK && sd_submit(d,&c)==SD_OK && sd_submit(d,&e)==SD_BUSY);
    sd_advance(d,10);need(sd_wait(d,&b)==SD_OK); /* completion precedes wait, out of order */
    struct sd_stats s;sd_stats(d,&s);need(s.terminal==1 && s.active==2 && s.peak_inflight==3);
    struct waiter w={d,&a,0};pthread_t t;need(!pthread_create(&t,NULL,wait_job,&w));
    uint64_t limit=sd_now()+10000000000ULL;
    do {sd_stats(d,&s);sched_yield();need(sd_now()<limit);}while(s.wait_entries<1);
    sd_spurious(d);
    do {sd_stats(d,&s);sched_yield();need(sd_now()<limit);}while(s.wait_wakes<1 || s.wait_entries<2);
    need(!__atomic_load_n(&w.done,__ATOMIC_ACQUIRE));
    need(sd_release(d,&a)==SD_OK);pthread_join(t,NULL);need(w.done);
    sd_advance(d,100);need(sd_wait(d,&c)==SD_OK && sd_drain(d)==SD_OK);sd_stats(d,&s);
    need(s.accepted==s.terminal && s.borrowed_bytes==0 && s.peak_borrowed_bytes==192);sd_close(d);
    d=sd_create((struct sd_config){2,4,64,0,0,1});a=job(input,output[0],1);b=job(input,output[1],1);
    need(sd_submit(d,&a)==SD_OK && sd_submit(d,&b)==SD_BUSY);sd_advance(d,0);need(sd_wait(d,&a)==SD_OK);
    b.capacity=65;need(sd_submit(d,&b)==SD_NO_SPACE);b.capacity=32;
    for(int fault=1;fault<=2;fault++){b.state=0;sd_fault_next(d,fault);need(sd_submit(d,&b)==SD_OK);sd_advance(d,0);need(sd_wait(d,&b)==SD_ERROR);}
    need(sd_drain(d)==SD_ERROR);sd_close(d);
    d=sd_create((struct sd_config){1,2,1024,100,100,1});a=job(input,output[0],1);b=job(input,output[1],1);
    need(sd_submit(d,&a)==SD_OK && sd_submit(d,&b)==SD_OK);sd_close(d);need(a.state==3 && b.state==3 && a.status==SD_ERROR && b.status==SD_ERROR);
    d=sd_create((struct sd_config){2,2,256,100,100,1});need(d!=NULL);
    need(sd_tail_profile(d,2,10)==SD_OK);sd_busy_next(d,2);
    a=job(input,output[0],1);b=job(input,output[1],1);
    uint64_t seq=sd_sequence(d);need(sd_submit(d,&a)==SD_BUSY);
    /* Injected transient busy changes sequence, so retry cannot sleep forever. */
    need(sd_wait_change(d,seq)==SD_OK && sd_submit(d,&a)==SD_BUSY);
    need(sd_submit(d,&a)==SD_OK && sd_submit(d,&b)==SD_OK);
    sd_advance(d,100);need(sd_poll(d,&a)==SD_OK && sd_poll(d,&b)==SD_BUSY);
    sd_advance(d,900);need(sd_wait(d,&b)==SD_OK);need(sd_wait(d,&a)==SD_OK);
    struct sd_trace tr[32];need(sd_trace_copy(d,tr,32)==2);
    for(int i=0;i<2;i++)need(tr[i].ready_ns<=tr[i].accepted_ns&&tr[i].accepted_ns<=tr[i].started_ns&&tr[i].started_ns<=tr[i].terminal_ns&&tr[i].terminal_ns<=tr[i].observed_ns);
    sd_stats(d,&s);need(s.accepted==2&&s.terminal==2&&s.observed==2&&s.injected_busy==2&&!s.trace_dropped);
    need(tr[0].service_ns==100 && tr[1].service_ns==1000);
    need(sd_reset_stats(d)==SD_OK);
    sd_background_ready(d,0,1,32,32);sd_advance(d,100);
    sd_stats(d,&s);need(s.eligible_idle_slot_ns==100&&s.advertised_ready_ns==100);
    sd_background_pause(d);sd_advance(d,100);sd_stats(d,&s);
    need(s.eligible_idle_slot_ns==100&&s.advertised_ready_ns==100);sd_close(d);
    /* Trace loss is explicit, not silently treated as a complete history. */
    setenv("SD_TRACE_CAPACITY","1",1);d=sd_create((struct sd_config){2,2,256,0,0,1});
    a=job(input,output[0],1);b=job(input,output[1],1);need(sd_submit(d,&a)==SD_OK&&sd_submit(d,&b)==SD_OK);
    sd_advance(d,0);need(sd_wait(d,&a)==SD_OK&&sd_wait(d,&b)==SD_OK);
    sd_stats(d,&s);need(s.trace_count==1&&s.trace_dropped==1);sd_close(d);
    puts("device deterministic contracts passed");return 0;
}
