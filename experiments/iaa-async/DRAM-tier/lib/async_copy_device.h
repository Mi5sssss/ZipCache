#ifndef ASYNC_COPY_DEVICE_H
#define ASYNC_COPY_DEVICE_H
#include <stddef.h>
#include <stdint.h>
/* Experimental device, not a codec or a hardware performance model.
 * Accepted jobs borrow buffers until terminal. A job may be reused only after wait.
 * close requires producers to stop; it drains wall-clock jobs and aborts gated jobs. */
enum sd_status { SD_OK=0, SD_BUSY=1, SD_CLOSED=2, SD_NO_SPACE=3, SD_INVALID=4, SD_ERROR=5 };
struct sd_config { unsigned slots, queue; size_t byte_limit; uint64_t encode_ns, decode_ns; int manual; };
struct sd_job {
    const void *input; void *output; size_t length, capacity, result;
    int encode, role, state, status, fault;
    uint64_t id, enqueue_ns, start_ns, due_ns, service_ns;
    uint64_t ready_ns, accepted_wall_ns, started_wall_ns, terminal_wall_ns, observed_wall_ns;
    unsigned trace_index;
    int producer_slot;
};
struct sd_trace {
    uint64_t id, ready_ns, accepted_ns, started_ns, terminal_ns, observed_ns, service_ns;
    size_t input_bytes, output_capacity;
    int role, encode, status;
};
struct sd_stats {
    uint64_t attempts, accepted, busy, completed, failed, terminal, active, queued, peak_active, peak_inflight;
    uint64_t borrowed_bytes, peak_borrowed_bytes, queue_ns, configured_service_ns, late_ns, max_late_ns;
    uint64_t copy_ns, wait_ns, active_slot_ns, elapsed_ns, device_cpu_ns;
    uint64_t wait_entries, wait_wakes;
    uint64_t busy_wait_ns, adapter_ns[3], stack_reserved_bytes;
    uint64_t role_active[3], role_peak[3], role_accepted[3], role_wait_ns[3];
    uint64_t descriptor_peak_bytes, owned_requested, owned_usable, late_hist[32];
    uint64_t observed, discovery_ns, max_discovery_ns, ready_to_accept_ns;
    uint64_t role_queue_ns[3], role_service_ns[3], role_discovery_ns[3];
    uint64_t queue_busy, byte_busy, injected_busy, trace_count, trace_dropped;
    uint64_t advertised_ready_ns, eligible_idle_slot_ns, credit_blocked_ns, advertised_ready_peak;
};
struct sd_device;
uint64_t sd_now(void);
struct sd_device *sd_create(struct sd_config config);
int sd_submit(struct sd_device *, struct sd_job *);
int sd_wait(struct sd_device *, struct sd_job *);
uint64_t sd_sequence(struct sd_device *);
int sd_wait_change(struct sd_device *, uint64_t sequence);
int sd_execute(struct sd_device *, int encode, int role, const void *, size_t, void *, size_t);
int sd_drain(struct sd_device *);
int sd_configure_idle(struct sd_device *, uint64_t encode_ns, uint64_t decode_ns, int manual);
int sd_reset_stats(struct sd_device *);
void sd_stats(struct sd_device *, struct sd_stats *);
/* Deterministic test controls, never used to predict hardware performance. */
void sd_advance(struct sd_device *, uint64_t delta_ns);
int sd_release(struct sd_device *, struct sd_job *);
void sd_fault_next(struct sd_device *, int fault); /* 1=hard error, 2=bad length/error, 3=short length/OK */
void sd_spurious(struct sd_device *);
void sd_unblock_for_shutdown(struct sd_device *);
uint64_t sd_thread_adapter_ns(void);
void sd_close(struct sd_device *);
/* Nonblocking completion check; job fields may be consumed only after OK. */
int sd_poll(struct sd_device *, struct sd_job *);
void sd_notify(struct sd_device *);
int sd_release_id(struct sd_device *, uint64_t); /* Manual-clock testing only. */
/* Diagnostic-only readiness advertisement: no tree/leaf pointer is retained. */
void sd_background_ready(struct sd_device *, unsigned slot, int ready, size_t input, size_t output);
void sd_background_pause(struct sd_device *);
size_t sd_trace_copy(struct sd_device *, struct sd_trace *, size_t capacity);
int sd_tail_profile(struct sd_device *, unsigned every, unsigned multiplier);
void sd_busy_next(struct sd_device *, unsigned attempts);
#endif
