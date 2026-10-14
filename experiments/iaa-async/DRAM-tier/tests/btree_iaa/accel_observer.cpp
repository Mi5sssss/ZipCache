// Frozen official shim extension. Counter observation and process-local test
// configuration only; no changes to codec/eligibility/device behavior.
#include "statistics.h"
#include "config/config.h"
#include <cstdint>
extern "C" {
__attribute__((visibility("default"))) const char *zipcache_accel_source() {
    return "b437553304ca765be38c7352d5a3086efd40cd09";
}
__attribute__((visibility("default"))) int zipcache_accel_statistics_enabled() {
    return AreStatsEnabled();
}
__attribute__((visibility("default"))) uint64_t zipcache_accel_counter(unsigned i) {
    return i<STATS_COUNT?GetStat(static_cast<Statistic>(i)):0;
}
__attribute__((visibility("default"))) int zipcache_accel_split_configure(int c,int d) {
    if((c!=0&&c!=1)||(d!=0&&d!=1)||!AreStatsEnabled())return -1;
    using namespace config;
    SetConfig(USE_QAT_COMPRESS,0);SetConfig(USE_QAT_UNCOMPRESS,0);
    SetConfig(USE_IGZIP_COMPRESS,0);SetConfig(USE_IGZIP_UNCOMPRESS,0);
    SetConfig(USE_IAA_COMPRESS,c);SetConfig(USE_IAA_UNCOMPRESS,d);
    SetConfig(USE_ZLIB_COMPRESS,!c);SetConfig(USE_ZLIB_UNCOMPRESS,!d);
    SetConfig(IGZIP_FALLBACK,0);SetConfig(IGNORE_ZLIB_DICTIONARY,0);
    SetConfig(IAA_COMPRESS_PERCENTAGE,100);SetConfig(IAA_UNCOMPRESS_PERCENTAGE,100);
    return 0;
}
}
