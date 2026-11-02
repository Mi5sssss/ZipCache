"""Fail-closed result checks for the handoff runner, not hardware simulation."""
import hashlib
import json
from pathlib import Path


def require(ok, message):
    if not ok:
        raise ValueError(message)


def validate_result(row):
    p, m, c, h = (row[k] for k in ('PRODUCT', 'INPUT', 'CROSS_V3', 'HOT'))
    require(p['status'] == 'ok', 'benchmark did not report success')
    require(p['actual_backend'] == row['codec'], 'actual backend differs from requested codec')
    require(p['operations'] == m['operations'] and p['writes'] == m['puts'], 'operation count differs from trace')
    require(p['reads'] + p['writes'] == p['operations'], 'read/write counts do not reconcile')
    require(p['live_keys'] == m['keys'], 'live object count differs from trace')
    require(p['mismatches'] == p['failed_tasks'] == p['explicit_fallbacks'] == 0, 'data, task or backend fallback failure')
    expected = 0
    for value in m['worker_hashes']:
        expected ^= int(value, 16)
    require(p['trace_hash'] == f'{expected:016x}', 'replayed trace hash differs from manifest')
    require(c['enabled'] and c['window'] == 4, 'expected four-slot cross-leaf executor is not enabled')
    require(c['accepted'] == c['terminal'] and c['failed'] == 0, 'background tasks are not conserved successfully')
    require(c['peak_jobs'] <= c['window'] and c['borrowed_peak_bytes'] <= c['byte_limit'], 'executor resource bound exceeded')
    require(h['hot_peak'] <= h['hot_limit'] and h['delta_peak'] <= h['delta_limit'], 'hot/delta memory limit exceeded')
    require(h['errors'] == 0, 'hot/cold conversion reported errors')
    require(h['hot_limit'] == 2 * 1024 * 1024, 'unexpected hot-leaf limit')
    audit = row['mode'] == 'audit'
    require(bool(h['crc']) == audit and bool(p['audit']) == audit, 'CRC or allocation-audit mode differs from requested mode')
    execution = row['EXECUTION']
    require(bool(execution['verify_each']) == audit, 'per-read oracle enabled in wrong mode')
    require(execution['arrival_rate'] == 0, 'unexpected offered-load mode')
    a = row['AGGREGATION']
    require(a['layout'] == 3 and a['batch'] == 1 and not a['legacy_get'], 'wrong layout or payload-read API')
    require(p['threads'] == 4 and p['shards'] == 8, 'unexpected client/shard count')
    for role in ('compress', 'get_decompress', 'maintenance_decompress'):
        work = row['CODEC_WORK'][role]
        require(work['failed'] == 0, f'{role} reported codec failures')
        require(sum(work['input_histogram_1KiB']) == work['calls'], f'{role} input histogram mismatch')
        require(sum(work['output_histogram_1KiB']) == work['calls'], f'{role} output histogram mismatch')
    require(row['TEARDOWN']['allocations'] == 0, 'tracked allocation leak after close')
    if row['profile'] != 'off':
        prof = row['CPU_PROFILE']
        require(prof['mode'] == row['profile'] and prof['thread_clock_complete'], 'CPU profile unavailable or incomplete')
        roles = [t['role'] for t in prof['threads']]
        require(roles.count('client') == 4 and roles.count('background') == 1, 'CPU thread roles incomplete')
        require(abs(prof['process_minus_threads_ns']) <= max(1000000, prof['process_cpu_ns'] * .05), 'CPU thread/process clocks do not reconcile within 5% or 1ms')
    if row['codec'] == 'qpl':
        require(p['library_version'] == '1.9.0', 'wrong QPL version')
        require(c['backend'] == 'qpl-strict-hardware', 'background is not strict hardware')
        require(execution['split_enabled'] == 1, 'explicit QPL routes not enabled')
        require(len(execution['routes']) == 3, 'missing QPL routes')
        for route in execution['routes']:
            require(route['requested'] == 'hardware', 'QPL route is not strict hardware')
            require(route['failed'] == 0 and route['calls'] == route['completed'], 'QPL route failed or incomplete')
    if row['codec'] == 'surrogate':
        d = row['DEVICE']
        require(d['accepted'] == d['terminal'] and d['failed'] == d['trace_dropped'] == 0, 'surrogate terminal/trace failure')


def qualify(row):
    """Never leave an exit-zero but invalid row labeled ok."""
    row['validation_errors'] = []
    if row['returncode'] != 0:
        row['status'] = 'unavailable' if row['returncode'] == 77 else 'failed'
        row['validation_errors'].append(row.get('execution_error') or f"process exit {row['returncode']}")
        return
    try:
        validate_result(row)
    except (KeyError, TypeError, ValueError) as exc:
        row['status'] = 'failed'
        row['validation_errors'].append(f'{type(exc).__name__}: {exc}')
    else:
        row['status'] = 'ok'


def build_provenance(build):
    """Bind returned results to actual binaries and configured libraries."""
    files = {}
    paths = [build / 'hot_bench', build / 'agg_qpl_format', build / 'CMakeCache.txt',
             build / 'CMakeFiles/hot_bench.dir/link.txt']
    cache = {}
    cache_path = build / 'CMakeCache.txt'
    if cache_path.exists():
        for line in cache_path.read_text().splitlines():
            if '=' not in line or line.startswith(('#', '//')):
                continue
            name, value = line.split('=', 1)
            key = name.split(':', 1)[0]
            if key.startswith(('CMAKE_C_COMPILER', 'CMAKE_CXX_COMPILER', 'CMAKE_C_FLAGS', 'CMAKE_CXX_FLAGS',
                               'CMAKE_BUILD_TYPE', 'QPL_', 'AGG_', 'pkgcfg_lib_LZ4', 'pkgcfg_lib_ZSTD', 'ZLIB_LIBRARY')):
                cache[key] = value
                if 'LIBRARY' in key or key.startswith('pkgcfg_lib_'):
                    paths.append(Path(value))
    for p in paths:
        if p.is_file():
            files[str(p)] = {'bytes': p.stat().st_size, 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}
    return {'files': files, 'cmake': cache,
            'scope': 'Configured files, not proof of every runtime loader dependency; runtime codec version is in results.json.'}
