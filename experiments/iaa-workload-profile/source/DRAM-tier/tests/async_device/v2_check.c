#include "async_copy_device.h"
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define REQUIRE(x) do { if(!(x)){fprintf(stderr,"v2 contract line %d: %s\n",__LINE__,#x);abort();} } while(0)
struct producer {struct sd_device *d; unsigned id;};
static void *produce(void *arg) {
    struct producer *p=arg;unsigned char input[4096],output[4096];
    for(unsigned n=0;n<1000;n++) {
        memset(input,(n+p->id)%251,sizeof(input));
        struct sd_job j={.input=input,.output=output,.length=1+(n*127)%4096,
            .capacity=sizeof(output),.encode=n%2,.role=p->id%3};
        int rc;do {uint64_t seq=sd_sequence(p->d);rc=sd_submit(p->d,&j);
            if(rc==SD_BUSY)REQUIRE(sd_wait_change(p->d,seq)==SD_OK);
        }while(rc==SD_BUSY);
        REQUIRE(rc==SD_OK);REQUIRE(sd_wait(p->d,&j)==SD_OK);
        REQUIRE(j.result==j.length&&!memcmp(input,output,j.length));
        REQUIRE(j.service_ns==(j.encode?3100:900)+j.length*(j.encode?1800:330)/1000);
        struct sd_stats s;sd_stats(p->d,&s); /* Concurrent, atomic telemetry snapshot. */
        REQUIRE(s.peak_active<=4&&s.peak_inflight<=5&&s.peak_borrowed_bytes<=65536);
    }
    return NULL;
}
int main(void) {
    setenv("SD_ENCODE_PS_PER_BYTE","1800",1);setenv("SD_DECODE_PS_PER_BYTE","330",1);
    /* Manual time verifies the size term and exact terminal boundary, no sleep. */
    unsigned char a[4096]={1},b[4096];
    struct sd_device *d=sd_create((struct sd_config){2,2,65536,3100,900,1});REQUIRE(d);
    struct sd_job j={.input=a,.output=b,.length=4096,.capacity=4096,.encode=1,.role=1};
    REQUIRE(sd_submit(d,&j)==SD_OK&&j.service_ns==10472);
    sd_advance(d,j.service_ns-1);REQUIRE(sd_poll(d,&j)==SD_BUSY);
    sd_advance(d,1);REQUIRE(sd_wait(d,&j)==SD_OK&&!memcmp(a,b,4096));sd_close(d);
    /* Multiple real producers exercise v2's existing bounded MPSC engine. */
    d=sd_create((struct sd_config){4,1,65536,3100,900,0});REQUIRE(d);
    struct sd_stats before,after;sd_stats(d,&before);
    pthread_t threads[8];struct producer p[8];
    for(unsigned i=0;i<8;i++){p[i]=(struct producer){d,i};REQUIRE(!pthread_create(&threads[i],NULL,produce,p+i));}
    for(unsigned i=0;i<8;i++)REQUIRE(!pthread_join(threads[i],NULL));
    REQUIRE(sd_drain(d)==SD_OK);sd_stats(d,&after);
    REQUIRE(after.accepted==8000&&after.terminal==8000&&!after.failed&&!after.borrowed_bytes&&!after.active&&!after.queued);
    REQUIRE(after.device_cpu_ns>=before.device_cpu_ns);
    REQUIRE(sd_reset_stats(d)==SD_OK);sd_stats(d,&after);
    REQUIRE(!after.accepted&&!after.terminal&&!after.borrowed_bytes&&after.owned_usable);
    sd_close(d);puts("v2 exact service, concurrent content, bounded credits and telemetry passed");return 0;
}
