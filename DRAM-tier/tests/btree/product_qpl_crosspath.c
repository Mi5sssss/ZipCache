/* Real software/hardware cross decoding. No compatibility-shim execution. */
#include "compressed_test_utils.h"
#ifdef HAVE_QPL
static qpl_job *make_job(qpl_path_t path)
{
    uint32_t size=0;
    if (qpl_get_job_size(path,&size)!=QPL_STS_OK || !size) return NULL;
    qpl_job *job=malloc(size);
    if (!job) return NULL;
    if (qpl_init_job(path,job)!=QPL_STS_OK) { free(job); return NULL; }
    return job;
}
static int convert(qpl_job *job,int compressing,const uint8_t *src,uint32_t size,
                   uint8_t *dst,uint32_t capacity)
{
    job->op=compressing?qpl_op_compress:qpl_op_decompress;
    job->next_in_ptr=(uint8_t*)src; job->available_in=size; job->total_in=0;
    job->next_out_ptr=dst; job->available_out=capacity; job->total_out=0;
    job->flags=QPL_FLAG_FIRST|QPL_FLAG_LAST;
    if (compressing) job->flags|=QPL_FLAG_DYNAMIC_HUFFMAN;
    job->level=qpl_default_level;
    qpl_status status=qpl_execute_job(job);
    if (status!=QPL_STS_OK || !job->total_out || job->total_out>capacity) return -1;
    return (int)job->total_out;
}
#endif
int main(void)
{
#ifndef HAVE_QPL
    puts("QPL cross-path unavailable: real library not linked."); return 77;
#else
    if (!btree_env_bool("BTREE_QPL_CROSS_HARDWARE",0)) {
        puts("QPL cross-path unavailable: set BTREE_QPL_CROSS_HARDWARE=1 on an IAA host."); return 77;
    }
    struct btree_silesia_dataset data={0};
    if (btree_load_silesia_samba(&data,128,0)) return 1;
    qpl_job *sw=make_job(qpl_path_software), *hw=make_job(qpl_path_hardware);
    if (!sw || !hw) {
        if (sw) { qpl_fini_job(sw); free(sw); }
        if (hw) { qpl_fini_job(hw); free(hw); }
        free(data.data); fprintf(stderr,"strict QPL path initialization failed\n"); return 1;
    }
    uint8_t compressed[8192],decoded[4096];
    int failed=0;
    size_t blocks=data.chunk_count*128/4096;
    for (size_t i=0;i<blocks && !failed;i++) for (int direction=0;direction<2;direction++) {
        const uint8_t *raw=data.data+i*4096;
        int size=convert(direction?hw:sw,1,raw,4096,compressed,sizeof(compressed));
        int restored=size>0?convert(direction?sw:hw,0,compressed,(uint32_t)size,decoded,sizeof(decoded)):-1;
        if (restored!=4096 || memcmp(raw,decoded,4096)) { failed=1; break; }
    }
    qpl_fini_job(sw); qpl_fini_job(hw); free(sw); free(hw); free(data.data);
    printf("CROSS {\"qpl_version\":\"%s\",\"samba_blocks\":%zu,\"directions\":2,\"failed\":%d,\"fallbacks\":0}\n",
           qpl_get_library_version(),blocks,failed);
    return failed;
#endif
}
