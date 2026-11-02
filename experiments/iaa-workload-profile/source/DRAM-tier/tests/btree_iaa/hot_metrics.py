"""Storage ratios and matched-RAW memory accounting. No codec timing inference."""
import hashlib
import json

SCHEMA = 1


def attach_inputs(out, run):
    """Bind rows to literal inputs and effective settings, including old reports."""
    if 'INPUT' not in run:
        directory = out / f"trace-{run['seed']}-{run['mapping']}"
        manifest = json.loads((directory / 'manifest.json').read_text())
        run['INPUT'] = {k: manifest[k] for k in (
            'trace_sha256', 'rank_map_sha256', 'archive_sha256', 'samba_sha256',
            'keys', 'operations', 'clients', 'seed', 'zipf_alpha', 'read_pct',
            'rank_mapping', 'payload_mapping', 'update_rule')}
    if 'CONFIG' not in run:
        command = json.loads((out / (run['case'] + '.command.json')).read_text())
        env = command['env']
        names = ('BTREE_HOT_BYTES', 'BTREE_DELTA_BYTES', 'BTREE_HOT_THRESHOLD',
                 'BTREE_COLD_THRESHOLD', 'BTREE_HOT_RESIDENCE', 'BTREE_HOT_EPOCH_OPS',
                 'BTREE_CROSS_OUTSTANDING', 'BTREE_CROSS_BYTES', 'BTREE_SHARDS',
                 'BTREE_REUSE_UNCHANGED_BLOCKS', 'BTREE_BG_INCLUDE_TRIGGER',
                 'BTREE_VERIFY_CRC', 'BTREE_MEMORY_AUDIT', 'HOT_WARMUP')
        run['CONFIG'] = {k: env.get(k) for k in names}
        cmd = command['cmd']
        run['CONFIG']['cpu_list'] = cmd[cmd.index('--cpu-list')+1] if '--cpu-list' in cmd else None


def ratio(numerator, denominator):
    return numerator / denominator if numerator is not None and denominator and denominator > 0 else None


def match_key(run):
    p = run.get('PRODUCT', {})
    if not run.get('INPUT') or not run.get('CONFIG'):
        return None
    return json.dumps([run['INPUT'], run['CONFIG'], run.get('mode'), run.get('repeat'),
                       p.get('live_keys'), p.get('live_kv_bytes'), p.get('trace_hash'),
                       run.get('AGGREGATION', {}).get('layout')], sort_keys=True)


def storage_metrics(run):
    p = run.get('PRODUCT', {})
    m = run.get('MEMORY_V3', {})
    a = run.get('AGGREGATION', {})
    valid = run.get('status') == 'ok' and p.get('status') == 'ok' and p.get('mismatches') == 0
    simulated = run.get('codec') == 'surrogate'
    complete = valid and m.get('qpl_private_memory_verified') is True and not simulated
    raw, encoded, framing = (m.get(k) for k in ('base_raw_record_bytes', 'base_encoded_bytes', 'base_format_bytes'))
    stacks, retained = a.get('stack_allocated_usable'), p.get('resident_usable')
    total = retained + stacks if valid and retained is not None and stacks is not None else None
    peak = p.get('audit_peak_usable')
    peak_total = peak + stacks if valid and p.get('audit') and peak and stacks is not None else None
    work = next((x for x in run.get('CODEC_WORK', []) if x.get('role') == 'compress'), {})
    metric = {
        'version': SCHEMA,
        'cold_data_ratio': ratio(raw, encoded) if valid and not simulated else None,
        'cold_framed_ratio': ratio(raw, encoded + framing) if valid and not simulated and encoded is not None and framing is not None else None,
        'codec_work_ratio': ratio(work.get('input_bytes'), work.get('output_bytes')) if valid and not simulated and work.get('failed') == 0 else None,
        'live_record_bytes': p.get('live_kv_bytes'),
        'cache_codec_retained_bytes': retained,
        'execution_stack_bytes': stacks,
        'total_owned_retained_bytes': total,
        'total_owned_peak_bytes': peak_total,
        'memory_ledger_complete_for_scope': complete,
        'memory_scope': 'cache, metadata, buffers, allocator usable bytes, codec/scheduler workspaces and benchmark execution stacks; excludes fixture, trace, timing arrays, audit tracker, shared libraries and OS runtime; RSS separate',
        'retained_basis': 'after final drain, not sustained-write steady state',
        'peak_basis': 'observed allocation high-water during measurement including drain; preload excluded',
        'cold_data_basis': 'current valid cold records / stored block payload including RAW bypass, excludes framing and hot RAW leaves; not cumulative codec traffic',
        'raw_reference_retained_case': None, 'raw_reference_peak_case': None,
        'raw_retained_bytes': None, 'raw_peak_bytes': None,
        'tracked_memory_saved_retained_pct': None, 'tracked_memory_saved_peak_pct': None,
        'memory_saved_retained_pct': None, 'memory_saved_peak_pct': None,
        'capacity_retained_x': None, 'capacity_peak_x': None,
        'capacity_gate': 'unavailable', 'reason': 'matched RAW unavailable',
    }
    if not valid:
        metric['reason'] = 'run failed or unavailable'
    elif simulated:
        metric['reason'] = 'surrogate does not model compression or capacity'
    elif not complete:
        metric['reason'] = 'memory ledger incomplete, including unverified QPL-private allocations'
    return metric


def enrich(runs):
    """Use the smallest measured RAW on/off candidate, never an estimated RAW size."""
    for run in runs:
        run['CAPACITY_METRICS'] = storage_metrics(run)
    for run in runs:
        target = run['CAPACITY_METRICS']
        if run.get('status') != 'ok' or run.get('codec') == 'surrogate':
            continue
        key = match_key(run)
        candidates = [r for r in runs if key is not None and match_key(r) == key and
                      r.get('codec') == 'raw' and r['CAPACITY_METRICS']['memory_ledger_complete_for_scope']]
        reasons = []
        for kind in ('retained', 'peak'):
            field = f'total_owned_{kind}_bytes'
            eligible = [r for r in candidates if r['CAPACITY_METRICS'][field]]
            value = target[field]
            if not eligible or not value:
                reasons.append(f'{kind}: matched RAW or measured bytes unavailable')
                continue
            reference = min(eligible, key=lambda r: r['CAPACITY_METRICS'][field])
            raw_bytes = reference['CAPACITY_METRICS'][field]
            target[f'raw_reference_{kind}_case'] = reference['case']
            target[f'raw_{kind}_bytes'] = raw_bytes
            saved = 100 * (1 - value / raw_bytes)
            target[f'tracked_memory_saved_{kind}_pct'] = saved
            if target['memory_ledger_complete_for_scope']:
                target[f'memory_saved_{kind}_pct'] = saved
                target[f'capacity_{kind}_x'] = raw_bytes / value
        if not target['memory_ledger_complete_for_scope']:
            continue
        if reasons:
            target['reason'] = '; '.join(reasons)
        else:
            target['reason'] = 'matched RAW and tracked allocation audit available; limited to stated memory scope'
            target['capacity_gate'] = 'pass-tracked-scope' if min(target['capacity_retained_x'], target['capacity_peak_x']) >= 1.25 else 'fail-tracked-scope'
    return runs


def fmt(value, digits=3):
    return 'unavailable' if value is None else f'{value:.{digits}f}'
