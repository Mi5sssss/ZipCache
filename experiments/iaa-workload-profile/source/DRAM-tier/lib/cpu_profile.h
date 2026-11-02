#ifndef ZIPCACHE_CPU_PROFILE_H
#define ZIPCACHE_CPU_PROFILE_H
#include <stdint.h>
#include <stdio.h>
/* Experiment-only. Thread totals are OS CPU clocks, stages are EXCLUSIVE CPU.
 * A scope spanning the end of the epoch stays in the unattributed remainder.
 * No allocations, tree locks or per-leaf state in the measurement path. */
enum zp_stage { ZP_GET, ZP_PUT, ZP_ENCODE, ZP_DECODE, ZP_SUBMIT, ZP_POLL,
 ZP_WAIT, ZP_QPL_SYNC, ZP_BG_STEP, ZP_MERGE, ZP_COMMIT, ZP_PREPARE,
 ZP_POLICY, ZP_COPY, ZP_STAGES };
struct zp_scope { uint64_t start, children; int stage, enabled; struct zp_scope *parent; };
void zp_register(const char *role);
void zp_begin(void);
void zp_end(void);
void zp_print(FILE *out);
void zp_qpl_init_event(int ok);
void zp_enter(struct zp_scope *s, enum zp_stage stage);
void zp_leave(struct zp_scope *s);
#define ZP_JOIN_(a,b) a##b
#define ZP_JOIN(a,b) ZP_JOIN_(a,b)
#define ZP_SCOPE(stage) struct zp_scope ZP_JOIN(zp_scope_,__LINE__) __attribute__((cleanup(zp_leave))) = {0}; zp_enter(&ZP_JOIN(zp_scope_,__LINE__),stage)
#endif
