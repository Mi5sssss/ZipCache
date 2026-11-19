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

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from run import trace, INTEL_POINTS


def prepare_inputs():
    expected = json.loads((ROOT / 'expected-inputs.json').read_text())
    verified = {}
    for name, frozen in expected.items():
        point, ops = name.rsplit('-n', 1)
        binary, actual = trace(point, int(ops))
        if actual != frozen:
            raise ValueError(f'{name}: generated input differs from the reviewed manifest')
        rank_hash = hashlib.sha256((binary.parent / 'rank_to_key.bin').read_bytes()).hexdigest()
        if rank_hash != frozen['rank_map_sha256']:
            raise ValueError(f'{name}: rank map checksum mismatch')
        verified[name] = frozen['trace_sha256']
    return verified


def commands(args, out):
    build = args.build.resolve()
    common = ['--build', str(build), '--cpu-list', args.cpu_list, '--numa-node', str(args.numa_node)]
    points = ['--points', ','.join(INTEL_POINTS)]
    runner = [sys.executable, str(ROOT / 'run.py')]
    return [
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


def report(out, status):
    lines = ['ZipCache IAA workload screen', json.dumps(status, sort_keys=True),
             'Performance, instrumented CPU diagnostics and memory audits are separate runs.',
             'The 10k smoke verifies setup only. QPL-private memory is not fully accounted.',
             'This is a finite screen, not sustained or equal-total-DRAM product acceptance.']
    for name in ('check', 'smoke', 'performance', 'profile', 'memory'):
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
    parser.add_argument('--build', type=Path, default=ROOT / 'build-intel')
    parser.add_argument('--perf', action='store_true', help='Enable permitted Linux perf for the profile pass only')
    parser.add_argument('--prepare-only', action='store_true', help='Generate/check frozen inputs on any host; no build or hardware execution')
    args = parser.parse_args()
    if args.prepare_only:
        print(json.dumps(prepare_inputs(), indent=2))
        return
    if platform.system() != 'Linux' or not args.cpu_list or args.numa_node is None or args.numa_node < 0:
        parser.error('Linux and explicit IAA-local --cpu-list / --numa-node are required')
    if args.out is None:
        parser.error('--out must be a new result directory')
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    status = {'status': 'running', 'cpu_list': args.cpu_list, 'numa_node': args.numa_node, 'perf': args.perf, 'completed': []}
    report(out, status)
    try:
        (out / 'inputs.json').write_text(json.dumps(prepare_inputs(), indent=2) + '\n')
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
        status['status'] = 'complete'
    except BaseException as exc:
        status.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        report(out, status)
        print(out / 'summary.txt', flush=True)


if __name__ == '__main__':
    main()
