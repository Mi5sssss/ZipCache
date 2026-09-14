/* Hardware qualification, not a portable QPL simulator. No tree performance. */
#include "compressed_test_utils.h"
#ifdef HAVE_QPL
static qpl_job *job(qpl_path_t path){
    uint32_t n=0;if(qpl_get_job_size(path,&n)!=QPL_STS_OK||!n)return NULL;
    qpl_job *j=malloc(n);if(!j)return NULL;
    if(qpl_init_job(path,j)!=QPL_STS_OK){free(j);return NULL;}return j;
}
static int transform(qpl_job *j,int compress,const uint8_t *src,uint32_t n,uint8_t *dst,uint32_t cap){
    j->op=compress?qpl_op_compress:qpl_op_decompress;j->next_in_ptr=(uint8_t*)src;
    j->available_in=n;j->total_in=0;j->next_out_ptr=dst;j->available_out=cap;j->total_out=0;
    j->flags=QPL_FLAG_FIRST|QPL_FLAG_LAST|(compress?QPL_FLAG_DYNAMIC_HUFFMAN:0);j->level=qpl_default_level;
    qpl_status status=qpl_execute_job(j);
    return status==QPL_STS_OK&&j->total_out<=cap?(int)j->total_out:-1;
}
#endif
int main(void){
#ifndef HAVE_QPL
    puts("unavailable: real QPL not linked");return 77;
#else
    if(strcmp(qpl_get_library_version(),"1.9.0")){fprintf(stderr,"require QPL 1.9.0, found %s\n",qpl_get_library_version());return 1;}
    if(!btree_env_bool("BTREE_QPL_CROSS_HARDWARE",0)){puts("pending: explicitly enable BTREE_QPL_CROSS_HARDWARE on IAA");return 77;}
    qpl_job *sw=job(qpl_path_software),*hw=job(qpl_path_hardware);int error=0;
    struct btree_silesia_dataset data={0};
    if(!sw||!hw||btree_load_silesia_samba(&data,128,0))error=1;
    uint8_t encoded[32768],decoded[16384];uint32_t sizes[]={136,512,1024,4080,8192,16320};
    for(size_t s=0;s<sizeof(sizes)/sizeof(sizes[0])&&!error;s++)
        for(size_t pos=0;pos+sizes[s]<=data.chunk_count*128&&!error;pos+=65536)
            for(int direction=0;direction<2;direction++){
                int n=transform(direction?hw:sw,1,data.data+pos,sizes[s],encoded,sizeof(encoded));
                int got=n>0?transform(direction?sw:hw,0,encoded,n,decoded,sizeof(decoded)):-1;
                if(got!=(int)sizes[s]||memcmp(data.data+pos,decoded,sizes[s])){error=1;break;}
            }
    if(sw){qpl_fini_job(sw);free(sw);}if(hw){qpl_fini_job(hw);free(hw);}free(data.data);
    printf("QPL cross-format: %s; strict hardware, two directions, zero fallback\n",error?"FAIL":"PASS");return error;
#endif
}
