/* A separate diagnostic process, NOT a precise per-cache RSS attribution.
 * Record heap/anonymous mappings around actual QPL init, execution and cleanup.
 * Shared dispatcher/libaccel-config resources can outlive jobs. Never hide them
 * by claiming get_job_size alone is the complete library memory footprint. */
#include "compressed_test_utils.h"
#ifdef __linux__
#include <malloc.h>
#endif
static void sample(const char *phase,size_t requested,size_t usable)
{
    size_t heap=0,rss=0,anonymous=0;
#ifdef __GLIBC__
#if __GLIBC_PREREQ(2,33)
    struct mallinfo2 m=mallinfo2();heap=m.uordblks+m.hblkhd;
#endif
#endif
#ifdef __linux__
    FILE *f=fopen("/proc/self/smaps_rollup","r");char line[256];
    if(f){while(fgets(line,sizeof(line),f)){size_t n;
        if(sscanf(line,"Rss: %zu kB",&n)==1)rss=n*1024;
        if(sscanf(line,"Anonymous: %zu kB",&n)==1)anonymous=n*1024;
    }fclose(f);}
#endif
    printf("QPL_MEMORY {\"phase\":\"%s\",\"job_requested\":%zu,\"job_usable\":%zu,"
        "\"process_allocator_bytes\":%zu,\"rss_bytes\":%zu,\"anonymous_bytes\":%zu,"
        "\"private_ledger_complete\":false}\n",phase,requested,usable,heap,rss,anonymous);
}
int main(int argc,char **argv)
{
#ifndef HAVE_QPL
    puts("unavailable: real QPL not linked");return 77;
#else
    if(argc!=3 || (strcmp(argv[1],"hardware")&&strcmp(argv[1],"software")))return 2;
    int n=atoi(argv[2]);if(n<1||n>8)return 2;
    if(strcmp(qpl_get_library_version(),"1.9.0"))return 1;
    qpl_path_t path=!strcmp(argv[1],"hardware")?qpl_path_hardware:qpl_path_software;
    qpl_job *jobs[8]={0};int initialized=0,result=0;size_t usable=0;uint32_t size=0;
    uint8_t input[16320],output[8][32768];
    for(size_t i=0;i<sizeof(input);i++)input[i]=(uint8_t)(i/13);
    sample("before",0,0);
    if(qpl_get_job_size(path,&size)!=QPL_STS_OK)return 77;
    for(int i=0;i<n;i++) {
        jobs[i]=malloc(size);if(!jobs[i]){result=1;goto done;}
        if(qpl_init_job(path,jobs[i])!=QPL_STS_OK){free(jobs[i]);jobs[i]=NULL;result=77;goto done;}
        initialized++;
#ifdef __linux__
        usable+=malloc_usable_size(jobs[i]);
#else
        usable+=size;
#endif
    }
    sample("initialized",(size_t)n*size,usable);
    for(int i=0;i<n;i++) {
        qpl_job *j=jobs[i];j->op=qpl_op_compress;j->level=qpl_default_level;
        j->flags=QPL_FLAG_FIRST|QPL_FLAG_LAST|QPL_FLAG_DYNAMIC_HUFFMAN;
        j->next_in_ptr=input;j->available_in=sizeof(input);j->next_out_ptr=output[i];j->available_out=sizeof(output[i]);
        if(qpl_execute_job(j)!=QPL_STS_OK){result=1;goto done;}
    }
    sample("executed",(size_t)n*size,usable);
done:
    for(int i=0;i<initialized;i++){qpl_fini_job(jobs[i]);free(jobs[i]);}
    sample("released",0,0);return result;
#endif
}
