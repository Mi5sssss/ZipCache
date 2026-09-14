#include "compressed_test_utils.h"
static void require(int ok,const char *why){if(!ok){fprintf(stderr,"split routes: %s\n",why);abort();}}
int main(int argc,char **argv){
    const char *names[]={"BTREE_QPL_COMPRESS_PATH","BTREE_QPL_GET_PATH","BTREE_QPL_MAINTENANCE_PATH"};
    struct compression_config c=bplus_tree_create_default_leaf_config(LEAF_TYPE_LZ4_HASHED);
    c.qpl_path=qpl_path_software;
    if(argc>1){
#ifndef HAVE_QPL
        puts("real QPL unavailable");return 77;
#else
        if(!strstr(qpl_get_library_version(),"1.9.0"))return 77;
        if(!strcmp(argv[1],"hardware")&&!btree_env_bool("BTREE_QPL_CROSS_HARDWARE",0))return 77;
        c.algo=COMPRESS_QPL;
        for(int i=0;i<3;i++)setenv(names[i],"software",1);
        if(!strcmp(argv[1],"hardware"))setenv(names[0],"hardware",1);
#endif
    }
    struct bplus_tree_compressed *t=bplus_tree_compressed_init_with_config(4,64,&c);
    require(t!=NULL,"init");qpl_path_t paths[3];
    require(bplus_tree_compressed_execution_routes(t,paths)==(argc>1),"enabled/inheritance");
    for(int i=1;i<3;i++)require(paths[i]==qpl_path_software,"inherited path");
    setenv(names[1],"hardware",1);qpl_path_t immutable[3];
    bplus_tree_compressed_execution_routes(t,immutable);require(!memcmp(paths,immutable,sizeof(paths)),"mutable policy");
    for(int round=0;round<3;round++)for(int k=1;k<=300;k++){
        uint8_t b[128];memset(b,k%7,128);b[0]=round;
        require(!bplus_tree_compressed_put_with_payload(t,k,b,128,round+1),"put");
        struct kv_pair p;require(!bplus_tree_compressed_get_record(t,k,&p)&&!memcmp(b,p.payload,128),"get bytes");}
    require(!bplus_tree_compressed_drain_background(t),"drain");
    for(int k=1;k<=300;k++){struct kv_pair p;require(!bplus_tree_compressed_get_record(t,k,&p)&&p.payload[0]==2,"base get");}
    for(int k=1;k<=100;k++)require(!bplus_tree_compressed_delete(t,k),"delete");
    struct bplus_aggregation_stats s;bplus_tree_compressed_aggregation_stats(&s,0);
    if(argc>1)for(int i=0;i<3;i++)require(s.route_completed[i]&&!s.route_failed[i]&&s.route_calls[i]==s.route_completed[i],"actual route work");
    bplus_tree_compressed_deinit(t);bplus_tree_compressed_release_thread_resources();
    for(int i=0;i<3;i++)unsetenv(names[i]);
    setenv(names[0],"invalid",1);require(!bplus_tree_compressed_init_with_config(4,64,&c),"invalid accepted");
    unsetenv(names[0]);return 0;
}
