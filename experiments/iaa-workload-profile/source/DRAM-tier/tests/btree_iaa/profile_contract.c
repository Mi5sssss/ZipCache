#include "cpu_profile.h"
#include <time.h>
#include <stdlib.h>
static void work(void){volatile unsigned long v=1;for(unsigned i=0;i<800000;i++)v=v*33+i;}
int main(void){setenv("BTREE_CPU_PROFILE","stages",1);zp_register("contract");zp_begin();
 {ZP_SCOPE(ZP_GET);work();{ZP_SCOPE(ZP_DECODE);work();}}
 zp_end();zp_print(stdout);return 0;}
