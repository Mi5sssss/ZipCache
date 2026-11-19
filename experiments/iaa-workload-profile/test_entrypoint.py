"""Git entrypoint tests; no fabricated hardware success results."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import run_intel as runner
import run as stages


class EntryPoint(unittest.TestCase):
    def test_fixed_plan_and_optional_perf(self):
        args = argparse.Namespace(build=Path('build-intel'), cpu_list='0-3', numa_node=0, perf=False)
        plan = runner.commands(args, Path('/tmp/example-results'))
        self.assertEqual([n for n, _ in plan], ['configure', 'build', 'check', 'smoke', 'performance', 'profile', 'memory'])
        self.assertEqual(sum('--perf' in c for _, c in plan), 0)
        self.assertIn('10000', dict(plan)['smoke'])
        self.assertIn(f'-DPython3_EXECUTABLE={sys.executable}', dict(plan)['configure'])
        self.assertIn('--cpu-list', dict(plan)['check'])
        self.assertIn('0-3', dict(plan)['check'])
        self.assertIn('--numa-node', dict(plan)['check'])
        for n in ('performance', 'profile', 'memory'):
            self.assertIn('uniform-r80,clustered-r20,uniform-r95', dict(plan)[n])
            self.assertIn('0-3', dict(plan)[n])
        args.perf = True
        self.assertEqual([n for n, c in runner.commands(args, Path('/tmp/results')) if '--perf' in c], ['profile'])

    def test_changed_frozen_input_is_rejected(self):
        with patch.object(runner, 'trace', return_value=(Path('/unused'), {'wrong': 'manifest'})):
            with self.assertRaises(ValueError):
                runner.prepare_inputs()

    def test_check_binding_and_preflight_environment(self):
        for bound in (False, True):
            with self.subTest(bound=bound), tempfile.TemporaryDirectory() as temp:
                argv = ['run.py', 'check', '--build', temp, '--out', str(Path(temp) / 'result')]
                if bound:
                    argv += ['--cpu-list', '0-3', '--numa-node', '0']
                with patch.object(sys, 'argv', argv), \
                        patch.object(stages.platform, 'system', return_value='Linux'), \
                        patch.object(stages, 'build_provenance', return_value={}), \
                        patch.object(stages, 'source_manifest', return_value={}), \
                        patch.object(stages.shutil, 'which', return_value=None), \
                        patch.object(stages.os, 'environ', {}), \
                        patch.object(stages.subprocess, 'run', side_effect=subprocess.CalledProcessError(8, ['ctest'])) as launch:
                    with self.assertRaises(subprocess.CalledProcessError):
                        stages.main()
                cmd = launch.call_args.args[0]
                env = launch.call_args.kwargs['env']
                self.assertTrue(launch.call_args.kwargs['check'])
                if bound:
                    self.assertEqual(cmd[:4], ['numactl', '--physcpubind=0-3', '--membind=0', 'ctest'])
                    self.assertEqual(env['BTREE_QPL_CROSS_HARDWARE'], '1')
                    self.assertEqual(env['SAMBA_ZIP_PATH'], str(stages.ROOT / 'data/samba.zip'))
                else:
                    self.assertEqual(cmd[0], 'ctest')
                    self.assertEqual(env, {})

    def test_failed_stage_stops_and_retains_summary(self):
        with tempfile.TemporaryDirectory() as temp:
            out = Path(temp) / 'result'
            plan = [('injected_failure', [sys.executable, '-c', 'raise SystemExit(3)']),
                    ('must_not_run', [sys.executable, '-c', 'raise SystemExit(0)'])]
            argv = ['run_intel.py', '--cpu-list', '0', '--numa-node', '0', '--out', str(out)]
            with patch.object(sys, 'argv', argv), patch.object(runner.platform, 'system', return_value='Linux'), \
                    patch.object(runner, 'prepare_inputs', return_value={}), patch.object(runner, 'commands', return_value=plan):
                with self.assertRaises(Exception):
                    runner.main()
            status = json.loads((out / 'run-status.json').read_text())
            self.assertEqual(status['status'], 'failed')
            self.assertEqual(status['completed'], [])
            self.assertFalse((out / 'must_not_run.log').exists())
            self.assertIn('failed', (out / 'summary.txt').read_text())

    def test_perf_registers_scope_and_fifo_cleanup_on_launch_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            build = Path(temp) / 'build'
            build.mkdir()
            (build / 'hot_bench').touch()
            out = Path(temp) / 'result'
            argv = ['run.py', 'intel-profile', '--build', str(build), '--out', str(out),
                    '--cpu-list', '0-3', '--numa-node', '0', '--points', 'uniform-r80',
                    '--operations', '10000', '--repeats', '1', '--perf']
            with patch.object(sys, 'argv', argv), \
                    patch.object(stages.platform, 'system', return_value='Linux'), \
                    patch.object(stages, 'build_provenance', return_value={}), \
                    patch.object(stages, 'source_manifest', return_value={}), \
                    patch.object(stages, 'trace', return_value=(Path('/unused/trace.bin'), {})), \
                    patch.object(stages.shutil, 'which', return_value='/unused/tool'), \
                    patch.object(stages.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, '', '')), \
                    patch.object(stages.subprocess, 'Popen', side_effect=RuntimeError('injected launch failure')) as launch:
                with self.assertRaisesRegex(RuntimeError, 'injected launch failure'):
                    stages.main()
            cmd = launch.call_args.args[0]
            self.assertEqual(cmd[:3], ['perf', 'record', '--delay=-1'])
            self.assertIn('--user-regs=ax,bx,cx,dx,si,di,bp,sp,ip,flags,cs,ss,r8,r9,r10,r11,r12,r13,r14,r15', cmd)
            self.assertIn('dwarf,16384', cmd)
            self.assertIn('cpu-clock', cmd)
            self.assertNotIn('-a', cmd)
            self.assertEqual(cmd[cmd.index('--') + 1:cmd.index('--') + 4],
                             ['numactl', '--physcpubind=0-3', '--membind=0'])
            self.assertFalse(list(out.glob('*.control')))
            self.assertFalse(list(out.glob('*.ack')))

    def test_combined_summary_keeps_distinct_passes(self):
        with tempfile.TemporaryDirectory() as temp:
            out = Path(temp)
            for n in ('performance', 'profile', 'memory'):
                (out / n).mkdir()
                (out / n / 'summary.txt').write_text(n + ' sample\n')
            runner.report(out, {'status': 'complete'})
            text = (out / 'summary.txt').read_text()
            for n in ('performance', 'profile', 'memory'):
                self.assertIn('=== ' + n + ' ===', text)


if __name__ == '__main__':
    unittest.main(verbosity=2)
