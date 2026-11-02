"""Git entrypoint tests; no fabricated hardware success results."""
import argparse
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import run_intel as runner


class EntryPoint(unittest.TestCase):
    def test_fixed_plan_and_optional_perf(self):
        args = argparse.Namespace(build=Path('build-intel'), cpu_list='0-3', numa_node=0, perf=False)
        plan = runner.commands(args, Path('/tmp/example-results'))
        self.assertEqual([n for n, _ in plan], ['configure', 'build', 'check', 'smoke', 'performance', 'profile', 'memory'])
        self.assertEqual(sum('--perf' in c for _, c in plan), 0)
        self.assertIn('10000', dict(plan)['smoke'])
        for n in ('performance', 'profile', 'memory'):
            self.assertIn('uniform-r80,clustered-r20,uniform-r95', dict(plan)[n])
            self.assertIn('0-3', dict(plan)[n])
        args.perf = True
        self.assertEqual([n for n, c in runner.commands(args, Path('/tmp/results')) if '--perf' in c], ['profile'])

    def test_changed_frozen_input_is_rejected(self):
        with patch.object(runner, 'trace', return_value=(Path('/unused'), {'wrong': 'manifest'})):
            with self.assertRaises(ValueError):
                runner.prepare_inputs()

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
