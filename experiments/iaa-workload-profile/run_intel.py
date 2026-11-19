#!/usr/bin/env python3
"""Build and run the reviewed hardware screen; no device changes or uploads."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
from statistics import median

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from run import trace, INTEL_POINTS
from confirmation import confirmation_summary


def prepare_inputs(reads=None):
    expected = json.loads((ROOT / 'expected-inputs.json').read_text())
    names = list(expected) if reads is None else ['uniform-r80-n10000'] + [
        f'uniform-r{pct}-n1000000' for pct in reads]
    verified = {}
    for name in names:
        point, ops = name.rsplit('-n', 1)
        binary, actual = trace(point, int(ops))
        if name in expected and actual != expected[name]:
            raise ValueError(f'{name}: generated input differs from the reviewed manifest')
        if reads is not None:
            reference = expected['uniform-r80-n10000']
            fixed = ('archive_sha256', 'samba_sha256', 'clients', 'keys', 'seed', 'zipf_alpha',
                     'rank_mapping', 'payload_mapping', 'rank_map_sha256')
            if (any(actual[k] != reference[k] for k in fixed) or actual['operations'] != int(ops)
                    or actual['read_pct'] != int(point.split('-r')[1])):
                raise ValueError(f'{name}: input does not match the requested uniform sweep')
        rank_hash = hashlib.sha256((binary.parent / 'rank_to_key.bin').read_bytes()).hexdigest()
        if rank_hash != actual['rank_map_sha256']:
            raise ValueError(f'{name}: rank map checksum mismatch')
        verified[name] = actual['trace_sha256']
    return verified


def commands(args, out):
    build = args.build.resolve()
    common = ['--build', str(build), '--cpu-list', args.cpu_list, '--numa-node', str(args.numa_node)]
    points = ['--points', ','.join(INTEL_POINTS)]
    runner = [sys.executable, str(ROOT / 'run.py')]
    plan = [
        ('configure', ['cmake', '-S', str(ROOT / 'source/DRAM-tier/tests/btree_iaa'), '-B', str(build),
                       '-DCMAKE_BUILD_TYPE=RelWithDebInfo', '-DAGG_DISABLE_QPL=OFF',
                       f'-DPython3_EXECUTABLE={sys.executable}']),
        ('build', ['cmake', '--build', str(build), '-j4']),
        ('check', runner + ['check'] + common + ['--out', str(out / 'check')]),
        ('smoke', runner + ['intel'] + common + ['--points', 'uniform-r80', '--operations', '10000',
                    '--repeats', '1', '--out', str(out / 'smoke')]),
        ('performance', runner + ['intel'] + common + points + ['--out', str(out / 'performance')]),
        ('profile', runner + ['intel-profile'] + common + points + ['--repeats', '1', '--out', str(out / 'profile')]
                    + (['--perf'] if args.perf else [])),
        ('memory', runner + ['intel-audit'] + common + points + ['--out', str(out / 'memory')]),
    ]
    reads = getattr(args, 'reads', None)
    if reads is not None:
        sweep_points = ','.join(f'uniform-r{pct}' for pct in reads)
        if getattr(args, 'confirm_from', None):
            paced = ['--points', sweep_points, '--arrival-rate', str(args.arrival_rate)]
            return plan[:4] + [
                ('matched-rate', runner + ['intel'] + common + paced + ['--repeats', '3', '--out', str(out / 'matched-rate')]),
                ('memory', runner + ['intel-audit'] + common + paced + ['--out', str(out / 'memory')])]
        return plan[:4] + [('performance', runner + ['intel'] + common +
                            ['--points', sweep_points, '--repeats', '3', '--out', str(out / 'performance')])]
    return plan


def crossover_summary(rows, reads, hashes):
    """Summarize only complete, validated hardware/software pairs from this run."""
    lines = ['Throughput through final drain, matched unpaced runs (three pairs per point).',
             'GET target | LZ4 median QPS | IAA median QPS | paired IAA gain median [min, max] | observed lead']
    previous = None
    brackets = []
    leaders = {}
    for pct in sorted(reads):
        name = f'uniform-r{pct}'
        expected_hash = hashes[name + '-n1000000']
        point = [r for r in rows if r['point'] == name]
        speeds = {'lz4': [], 'qpl': []}
        for rep in range(3):
            for codec in speeds:
                pair = [r for r in point if r['codec'] == codec and r['repeat'] == rep]
                if len(pair) != 1:
                    raise ValueError(f'{name}/{rep}/{codec}: missing or duplicate measurement')
                r = pair[0]
                if (r['status'] != 'ok' or r['mode'] != 'performance' or
                        r['INPUT']['trace_sha256'] != expected_hash):
                    raise ValueError(f'{name}/{rep}/{codec}: invalid result or mismatched trace')
                p = r['PRODUCT']
                speeds[codec].append(p['operations'] * 1e9 / p['completed_ns'])
        gains = [100 * (hw / cpu - 1) for hw, cpu in zip(speeds['qpl'], speeds['lz4'])]
        lead = 'IAA' if min(gains) > 0 else 'LZ4' if max(gains) < 0 else 'inconclusive'
        leaders[pct] = lead
        lines.append(f"{pct}% | {median(speeds['lz4']):,.0f} | {median(speeds['qpl']):,.0f} | "
                     f"{median(gains):+.2f}% [{min(gains):+.2f}%, {max(gains):+.2f}%] | {lead}")
        if lead != 'inconclusive':
            if previous and previous[1] != lead:
                brackets.append((previous[0], pct, previous[1], lead))
            previous = (pct, lead)
    intervals = [f'{lo}% to {hi}% GET ({first} to {second})' for lo, hi, first, second in brackets]
    lines.append('Observed lead changes: ' + (', '.join(intervals) or 'none resolved among the tested points'))
    lines.append('Agreement of three paired runs is descriptive. No exact crossover or statistical significance is inferred.')
    return lines, [(lo, hi) for lo, hi, _, _ in brackets], leaders


def select_confirmation(args):
    source = args.confirm_from
    prior = json.loads((source / 'run-status.json').read_text())
    if prior['status'] != 'complete' or not prior.get('reads') or prior.get('confirm_from'):
        raise ValueError('--confirm-from requires a completed throughput sweep')
    if prior.get('host') and prior['host'] != platform.node():
        raise ValueError('Run confirmation on the same host as the throughput sweep')
    hashes = json.loads((source / 'inputs.json').read_text())
    lines, brackets, leaders = crossover_summary(json.loads((source / 'performance/results.json').read_text()),
                                                prior['reads'], hashes)
    if not brackets:
        raise ValueError('No change in throughput lead was resolved. Return the sweep summary or refine the sweep.')
    if args.reads is None and len(brackets) == 1:
        args.reads = list(brackets[0])
    if (args.reads is None or len(args.reads) != 2 or
            {leaders.get(pct) for pct in args.reads} != {'IAA', 'LZ4'}):
        raise ValueError(f'Select two tested percentages with opposite leaders using --reads LOW HIGH. Observed brackets: {brackets}')
    for field in ('cpu_list', 'numa_node'):
        if getattr(args, field) not in (None, prior[field]):
            raise ValueError(f'{field} must match the original sweep ({prior[field]})')
        setattr(args, field, prior[field])
    return lines, hashes


def report(out, status):
    header = {key: value for key, value in status.items() if key not in ('confirmation', 'crossover', 'host')}
    lines = ['ZipCache IAA workload screen', json.dumps(header, sort_keys=True),
             'Performance, instrumented CPU diagnostics and memory audits are separate runs.',
             'The 10k smoke verifies setup only. QPL-private memory is not fully accounted.',
             'This is a finite screen, not sustained or equal-total-DRAM product acceptance.']
    if status.get('crossover'):
        lines += ['', *status['crossover']]
    if status.get('confirmation'):
        lines += ['', *status['confirmation']]
    stages = () if status.get('confirmation') and status['status'] == 'complete' else (
        'check', 'smoke', 'performance', 'matched-rate', 'profile', 'memory')
    for name in stages:
        part = out / name
        if (part / 'summary.txt').exists():
            lines += ['', '=== ' + name + ' ===', (part / 'summary.txt').read_text()]
        if (part / 'run-status.json').exists():
            lines.append(name + ' status: ' + (part / 'run-status.json').read_text().strip())
    (out / 'summary.txt').write_text('\n'.join(lines) + '\n')
    (out / 'run-status.json').write_text(json.dumps(status, indent=2) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cpu-list', default=os.getenv('ZIPCACHE_CPU_LIST'))
    parser.add_argument('--numa-node', type=int, default=os.getenv('ZIPCACHE_NUMA_NODE'))
    parser.add_argument('--out', type=Path)
    parser.add_argument('--confirm-from', type=Path, metavar='SWEEP_DIR',
                        help='Confirm one change in throughput lead. Use --reads LOW HIGH to select a bracket.')
    parser.add_argument('--arrival-rate', type=int, metavar='QPS', help='Total requests/s for confirmation (default 80000)')
    parser.add_argument('--reads', type=int, nargs='+', metavar='PCT',
                        help='Sweep uniform GET percentages (0..100), LZ4 versus strict IAA throughput only')
    parser.add_argument('--build', type=Path, default=ROOT / 'build-intel')
    parser.add_argument('--perf', action='store_true', help='Enable permitted Linux perf for the profile pass only')
    parser.add_argument('--prepare-only', action='store_true', help='Generate/check frozen inputs on any host; no build or hardware execution')
    args = parser.parse_args()
    prior_lines, prior_hashes = None, None
    if args.confirm_from:
        try:
            prior_lines, prior_hashes = select_confirmation(args)
        except (OSError, KeyError, ValueError) as exc:
            parser.error(str(exc))
        if args.arrival_rate is None:
            args.arrival_rate = 80000
    if args.arrival_rate is not None and (not args.confirm_from or not 0 < args.arrival_rate <= 100000000):
        parser.error('A positive --arrival-rate up to 100000000 requires --confirm-from')
    if args.reads is not None and (len(set(args.reads)) != len(args.reads)
                                  or any(not 0 <= pct <= 100 for pct in args.reads)):
        parser.error('--reads requires distinct integer percentages from 0 to 100')
    if args.reads and args.perf:
        parser.error('Use the separate default profile run for --perf')
    def checked_inputs():
        hashes = prepare_inputs(args.reads)
        if prior_hashes and any(prior_hashes.get(name) != digest for name, digest in hashes.items()):
            raise ValueError('Confirmation input differs from the saved sweep')
        return hashes
    if args.prepare_only:
        print(json.dumps(checked_inputs(), indent=2))
        return
    if platform.system() != 'Linux' or not args.cpu_list or args.numa_node is None or args.numa_node < 0:
        parser.error('Linux and explicit IAA-local --cpu-list / --numa-node are required')
    if args.out is None:
        parser.error('--out must be a new result directory')
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    status = {'status': 'running', 'host': platform.node(), 'cpu_list': args.cpu_list, 'numa_node': args.numa_node,
              'perf': args.perf, 'reads': args.reads, 'completed': [],
              'confirm_from': str(args.confirm_from) if args.confirm_from else None,
              'arrival_rate': args.arrival_rate, 'crossover': prior_lines}
    report(out, status)
    try:
        hashes = checked_inputs()
        (out / 'inputs.json').write_text(json.dumps(hashes, indent=2) + '\n')
        plan = commands(args, out)
        (out / 'commands.json').write_text(json.dumps(plan, indent=2) + '\n')
        for name, cmd in plan:
            status['current'] = name
            report(out, status)
            print(name, flush=True)
            with (out / (name + '.log')).open('w') as log:
                subprocess.run(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
            status['completed'].append(name)
            report(out, status)
        if args.confirm_from:
            status['confirmation'] = confirmation_summary(
                json.loads((out / 'matched-rate/results.json').read_text()),
                json.loads((out / 'memory/results.json').read_text()), args.reads, args.arrival_rate, hashes)
        elif args.reads:
            rows = json.loads((out / 'performance/results.json').read_text())
            status['crossover'], _, _ = crossover_summary(rows, args.reads, hashes)
        status['status'] = 'complete'
    except BaseException as exc:
        status.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        report(out, status)
        print(out / 'summary.txt', flush=True)


if __name__ == '__main__':
    main()
