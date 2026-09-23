"""Email-sized diagnostic record. Missing data stay unavailable, never zero."""
import json


def write_summary(a, plan, rows, flat):
    lines = ["ZipCache diagnostic summary", "",
             f"Status: {plan['status']}; product/hardware performance qualification: PENDING",
             f"Source commit: {plan.get('source_commit', 'unavailable')}",
             f"Tracked local changes: {plan.get('tracked_changes') or 'none recorded'}",
             f"Command: {json.dumps(plan.get('invocation', []))}",
             f"Profile={a.profile}; CPU IDs={a.cpu_list}; NUMA={a.numa_node}; backpressure={a.backpressure}",
             "QPL library: " + json.dumps(getattr(a, 'qpl_library', 'unavailable')),
             "Layouts: 3=16 KiB leaf / up to four 4 KiB blocks; 2=16 KiB whole-leaf compression.",
             "A=QPL software; D=strict IAA; B=IAA compression/CPU reads+maintenance; C=IAA compression+reads/CPU maintenance.",
             "Mapping is key-to-payload mapping, NOT sequential versus random access.", "",
             "PERFORMANCE (independent audit runs excluded; QPS and CPU include final drain)",
             "case | policy | layout | rep | QPS | CPU ns/op | GET p99 us | PUT p99 us | drain ms"]
    for r in flat:
        lines.append(f"{r['experiment']} | {r['policy']} | {r['layout']} | {r['repetition']} | "
                     f"{r['qps']:.0f} | {r['cpu_ns_per_op']:.1f} | {r['get_p99_us']:.3f} | "
                     f"{r['put_p99_us']:.3f} | {r['drain_ms']:.3f}")
    if not flat:
        lines.append("No completed performance measurements available.")
    lines += ["", "DIAGNOSTICS (per performance row; waits/route ns are accumulated wall time, NOT CPU time)"]
    for r in rows:
        if r.get('mode') != 'performance' or r.get('status') != 'ok':
            continue
        p, agg, e = r.get('product', {}), r.get('aggregation', {}), r.get('execution', {})
        lines.append(f"{r['experiment']} / {r.get('policy')} / layout {r.get('layout')} / rep {r.get('rep')}")
        lines.append("  " + json.dumps({k:p.get(k, 'unavailable') for k in
            ('operations', 'writes', 'mismatches', 'sync_fallbacks', 'split_fallbacks', 'write_lock_codec_calls',
             'foreground_ns', 'completed_ns', 'cpu_ns')}))
        lines.append("  " + json.dumps({k:agg.get(k, 'unavailable') for k in
            ('active_hits', 'pending_hits', 'base_hits', 'pending_wait_ns', 'read_lock_wait_ns',
             'merged_updates', 'merge_count')}))
        for origin, work in zip(('GET', 'compaction', 'split', 'DELETE', 'other'), agg.get('origins', [])):
            if any(work.values()):
                lines.append(f"  {origin}: " + json.dumps(work))
        lines.append("  " + json.dumps({k:e.get(k, 'unavailable') for k in
            ('queue_first', 'queue_last', 'queue_observed_peak', 'latency_basis', 'routes')}))
        lines.append("  Routes order: compression, GET decompression, maintenance decompression.")
        lines.append("  waits=" + json.dumps(r.get('waits', 'unavailable')))
        lines.append("  backend=" + json.dumps({k:p.get(k, 'unavailable') for k in
            ('requested_backend', 'actual_backend', 'library_version', 'execution_path', 'explicit_fallbacks')}))
    lines += ["", "MEMORY (separate audit process, not timed performance; ratios are NOT codec ratios)"]
    for r in flat:
        lines.append(f"{r['experiment']} / {r['policy']} / layout {r['layout']} / rep {r['repetition']}: " +
            json.dumps({k:r[k] for k in ('audited_steady_owned_bytes', 'audited_peak_owned_bytes',
                'tracked_steady_capacity', 'tracked_peak_capacity', 'memory_ledger_complete', 'performance_rss_bytes')}))
    lines.append("Capacity ratios: matched RAW owned bytes / compressed owned bytes; RAW is restricted to selected layouts.")
    lines += ["", "FAILURES / INCOMPLETE WORK"]
    problems = []
    if plan.get('error'):
        problems.append(plan['error'])
    for step in plan['steps']:
        if step.get('status') not in (None, 'ok'):
            problems.append(f"{step['name']}: {step.get('status')}; log={step.get('log', 'unavailable')}")
    for r in rows:
        if r.get('status') != 'ok':
            problems.append(json.dumps({k:r.get(k) for k in ('experiment', 'policy', 'mode', 'status', 'error')}))
    lines += problems or ["None recorded. Missing/unexecuted cases are not passes."]
    lines += ["", "INTERPRETATION",
        "Same-byte GET attribution is separate from mixed writes. Its hardware row uses software-produced compressed bytes.",
        "Software queue depth is NOT IAA WQ occupancy. Legacy async JOBS/PHASES counters do not cover this synchronous layout.",
        "Queue observation is sampled; it may miss peaks. QPL duration includes preparation/submission/wait/service.",
        "Tracked memory excludes unreconciled QPL-private allocations; no product capacity claim follows.",
        "Screen results are one-seed diagnostics, not sustained-QPS/SLO qualification.",
        "Review paths/host details before sharing. Email this file or paste its text; archive optional."]
    (a.out / 'summary.txt').write_text('\n'.join(lines) + '\n')
