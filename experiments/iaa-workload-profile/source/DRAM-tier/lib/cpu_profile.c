#define _GNU_SOURCE
#define _DARWIN_C_SOURCE
#include "cpu_profile.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>
#include <assert.h>
#ifdef __APPLE__
#include <mach/mach.h>
#endif
#define ZP_THREADS 80
struct row { pthread_t thread; const char *role; uint64_t before,after;
 uint64_t stage[ZP_STAGES],calls[ZP_STAGES]; int valid; };
static struct row rows[ZP_THREADS];
static unsigned count;
static pthread_mutex_t lock=PTHREAD_MUTEX_INITIALIZER;
static pthread_once_t once=PTHREAD_ONCE_INIT;
static int mode,active;
static uint64_t process_start,process_end;
static uint64_t init_attempts,init_ok,init_before,ok_before,init_after,ok_after;
static __thread struct row *self;
static __thread struct zp_scope *top;
static const char *names[ZP_STAGES]={"get_other","put_other","encode_or_dispatch",
 "decode_or_dispatch","submit","poll","wait_backoff","qpl_execute_inclusive",
 "background_step_other","merge","commit","prepare","hot_policy","device_copy"};
static uint64_t clock_ns(clockid_t c){struct timespec t;if(clock_gettime(c,&t))abort();return (uint64_t)t.tv_sec*1000000000ULL+t.tv_nsec;}
static void init(void){const char *p=getenv("BTREE_CPU_PROFILE");__atomic_store_n(&mode,p&&!strcmp(p,"stages")?2:p&&!strcmp(p,"threads")?1:0,__ATOMIC_RELAXED);}
static int thread_ns(pthread_t p,uint64_t *out){
#ifdef __APPLE__
 thread_basic_info_data_t i;mach_msg_type_number_t n=THREAD_BASIC_INFO_COUNT;
 if(thread_info(pthread_mach_thread_np(p),THREAD_BASIC_INFO,(thread_info_t)&i,&n)!=KERN_SUCCESS)return -1;
 *out=((uint64_t)i.user_time.seconds+i.system_time.seconds)*1000000000ULL+((uint64_t)i.user_time.microseconds+i.system_time.microseconds)*1000;
#else
 clockid_t c;if(pthread_getcpuclockid(p,&c))return -1;*out=clock_ns(c);
#endif
 return 0;
}
void zp_register(const char *role){pthread_once(&once,init);if(!mode||self)return;
 pthread_mutex_lock(&lock);assert(count<ZP_THREADS);self=&rows[count++];self->thread=pthread_self();self->role=role;
 pthread_mutex_unlock(&lock);
}
void zp_begin(void){pthread_once(&once,init);if(!mode)return;
 init_before=__atomic_load_n(&init_attempts,__ATOMIC_RELAXED);ok_before=__atomic_load_n(&init_ok,__ATOMIC_RELAXED);
 process_start=clock_ns(CLOCK_PROCESS_CPUTIME_ID);pthread_mutex_lock(&lock);
 for(unsigned i=0;i<count;i++){struct row *r=&rows[i];r->valid=!thread_ns(r->thread,&r->before);
  for(int s=0;s<ZP_STAGES;s++){__atomic_store_n(&r->stage[s],0,__ATOMIC_RELAXED);__atomic_store_n(&r->calls[s],0,__ATOMIC_RELAXED);}}
 pthread_mutex_unlock(&lock);__atomic_store_n(&active,1,__ATOMIC_RELEASE);
}
void zp_end(void){if(!mode)return;__atomic_store_n(&active,0,__ATOMIC_RELEASE);
 init_after=__atomic_load_n(&init_attempts,__ATOMIC_RELAXED);ok_after=__atomic_load_n(&init_ok,__ATOMIC_RELAXED);
 pthread_mutex_lock(&lock);for(unsigned i=0;i<count;i++)rows[i].valid &= !thread_ns(rows[i].thread,&rows[i].after);
 pthread_mutex_unlock(&lock);process_end=clock_ns(CLOCK_PROCESS_CPUTIME_ID);
}
void zp_qpl_init_event(int ok){if(!__atomic_load_n(&mode,__ATOMIC_RELAXED))return;
 __atomic_add_fetch(&init_attempts,1,__ATOMIC_RELAXED);if(ok)__atomic_add_fetch(&init_ok,1,__ATOMIC_RELAXED);}
void zp_enter(struct zp_scope *s,enum zp_stage stage){if(__atomic_load_n(&mode,__ATOMIC_RELAXED)!=2||!__atomic_load_n(&active,__ATOMIC_ACQUIRE)||!self)return;
 s->stage=stage;s->enabled=1;s->parent=top;top=s;s->start=clock_ns(CLOCK_THREAD_CPUTIME_ID);
}
void zp_leave(struct zp_scope *s){if(!s->enabled)return;uint64_t inclusive=clock_ns(CLOCK_THREAD_CPUTIME_ID)-s->start;
 assert(top==s);top=s->parent;if(top)top->children+=inclusive;
 if(__atomic_load_n(&active,__ATOMIC_ACQUIRE)){
  __atomic_add_fetch(&self->stage[s->stage],inclusive-s->children,__ATOMIC_RELAXED);
  __atomic_add_fetch(&self->calls[s->stage],1,__ATOMIC_RELAXED);}
}
void zp_print(FILE *f){if(!mode){fprintf(f,"CPU_PROFILE {\"version\":1,\"mode\":\"off\"}\n");return;}
 uint64_t sum=0;int complete=1;
 fprintf(f,"CPU_PROFILE {\"version\":1,\"mode\":\"%s\",\"epoch\":\"preload-drained through final-drain, OS snapshots bracket API timing\",\"process_cpu_ns\":%" PRIu64 ",\"instrumentation_static_bytes\":%zu,\"threads\":[",mode==2?"stages":"threads",process_end-process_start,sizeof(rows));
 for(unsigned i=0;i<count;i++){struct row *r=&rows[i];uint64_t n=r->valid?r->after-r->before:0;sum+=n;complete &= r->valid;
  fprintf(f,"%s{\"role\":\"%s\",\"valid\":%s,\"cpu_ns\":%" PRIu64 ",\"stages\":{",i?",":"",r->role,r->valid?"true":"false",n);
  uint64_t total=0;for(int s=0;s<ZP_STAGES;s++){uint64_t v=__atomic_load_n(&r->stage[s],__ATOMIC_RELAXED);total+=v;
   fprintf(f,"%s\"%s\":{\"cpu_ns\":%" PRIu64 ",\"calls\":%" PRIu64 "}",s?",":"",names[s],v,__atomic_load_n(&r->calls[s],__ATOMIC_RELAXED));}
  fprintf(f,"},\"unattributed_cpu_ns\":%" PRId64 "}",(int64_t)n-(int64_t)total);
 }
 fprintf(f,"],\"thread_clock_complete\":%s,\"process_minus_threads_ns\":%" PRId64 ",\"qpl_init_attempts\":%" PRIu64 ",\"qpl_init_successes\":%" PRIu64 ",\"stage_cpu_exclusive\":true,\"qpl_execute_internal_split\":\"requires external stack profile\"}\n",complete?"true":"false",(int64_t)(process_end-process_start)-(int64_t)sum,init_after-init_before,ok_after-ok_before);
}
