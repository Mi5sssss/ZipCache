"""Runner/report fault tests. Synthetic rows test validation, never emulate IAA."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from handoff_checks import qualify
from run import summarize


class HandoffChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.row = json.loads((ROOT / 'test-fixtures/screen/results.json').read_text())[0]

    def checked(self, change):
        row = copy.deepcopy(self.row)
        change(row)
        qualify(row)
        self.assertEqual(row['status'], 'failed', row)
        self.assertTrue(row['validation_errors'])
        with tempfile.TemporaryDirectory() as tmp:
            summarize(Path(tmp), [row])
            self.assertIn('failed errors=', (Path(tmp) / 'summary.txt').read_text())

    def test_measured_success(self):
        row = copy.deepcopy(self.row)
        qualify(row)
        self.assertEqual(row['status'], 'ok')

    def test_invalid_exit_zero_results(self):
        cases = {
            'trace': lambda r: r['PRODUCT'].update(trace_hash='0' * 16),
            'count': lambda r: r['PRODUCT'].update(operations=3),
            'mismatch': lambda r: r['PRODUCT'].update(mismatches=1),
            'terminal': lambda r: r['CROSS_V3'].update(terminal=0),
            'hot_budget': lambda r: r['HOT'].update(hot_peak=1 << 30),
            'job_budget': lambda r: r['CROSS_V3'].update(peak_jobs=5),
            'codec_error': lambda r: r['CODEC_WORK']['compress'].update(failed=1),
            'crc_timing': lambda r: r['HOT'].update(crc=1),
            'cpu_missing': lambda r: r['CPU_PROFILE'].update(thread_clock_complete=False),
            'truncated_log': lambda r: r.pop('PRODUCT'),
            'leak': lambda r: r['TEARDOWN'].update(allocations=1),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name):
                self.checked(mutate)

    def test_missing_real_qpl_stays_unavailable(self):
        row = {'returncode': 77}
        qualify(row)
        self.assertEqual(row['status'], 'unavailable')

    def test_timeout_and_nonzero_exit_stay_failed(self):
        for code in (-9, 1):
            row = {'returncode': code, 'execution_error': 'injected execution failure'}
            qualify(row)
            self.assertEqual(row['status'], 'failed')

    def test_hardware_label_alone_is_not_evidence(self):
        # Deliberately mislabeled measured software must be rejected.
        self.checked(lambda r: r.update(codec='qpl', evidence='strict-hardware'))
        def fake(r):
            r.update(codec='qpl', evidence='strict-hardware')
            r['PRODUCT'].update(actual_backend='qpl', library_version='1.9.0')
        self.checked(fake)

    def test_qpl_incomplete_memory_stays_unqualified(self):
        rows = copy.deepcopy(json.loads((ROOT / 'test-fixtures/memory/results.json').read_text())[:3])
        target = rows[2]
        target['codec'] = 'qpl'
        target['STORAGE']['memory_ledger_complete_for_scope'] = False
        target['STORAGE']['reason'] = 'unverified QPL-private allocations'
        with tempfile.TemporaryDirectory() as tmp:
            summarize(Path(tmp), rows)
            self.assertIsNone(target['STORAGE']['capacity_retained_x'])
            self.assertIsNone(target['STORAGE']['memory_saved_retained_pct'])
            self.assertIsNotNone(target['STORAGE']['tracked_memory_saved_retained_pct'])
            self.assertIn('NOT whole-system savings', (Path(tmp) / 'summary.txt').read_text())

    def test_cli_rejects_truncated_success_under_python_optimized(self):
        with tempfile.TemporaryDirectory() as tmp:
            t = Path(tmp); build = t / 'build'; build.mkdir()
            binary = build / 'hot_bench'
            binary.write_text('#!' + sys.executable + '\nprint(\'PRODUCT {"status":"ok"}\')\n')
            binary.chmod(0o755)
            out = t / 'result'
            r = subprocess.run([sys.executable, '-O', str(ROOT / 'run.py'), 'screen',
                                '--points', 'uniform-r80', '--operations', '10000', '--repeats', '1',
                                '--build', str(build), '--out', str(out)], capture_output=True, text=True, timeout=60)
            self.assertNotEqual(r.returncode, 0)
            self.assertEqual(json.loads((out / 'run-status.json').read_text())['status'], 'failed')
            result = json.loads((out / 'results.json').read_text())
            self.assertEqual(result[0]['status'], 'failed')
            self.assertIn('RUN_FAILED', (out / 'summary.txt').read_text())

    def test_cli_build_missing_retains_failure_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / 'result'
            r = subprocess.run([sys.executable, str(ROOT / 'run.py'), 'screen', '--build', tmp,
                                '--out', str(out)], capture_output=True, text=True, timeout=30)
            self.assertNotEqual(r.returncode, 0)
            self.assertEqual(json.loads((out / 'run-status.json').read_text())['status'], 'failed')
            self.assertIn('RUN_FAILED', (out / 'summary.txt').read_text())

    def test_hardware_preflight_failure_retains_summary(self):
        # Mock only host detection and a failing launcher; no fake QPL success.
        with tempfile.TemporaryDirectory() as tmp:
            t = Path(tmp); build = t / 'build'; build.mkdir()
            (build / 'hot_bench').write_text('not reached')
            launcher = t / 'numactl'
            launcher.write_text('#!' + sys.executable + '\nraise SystemExit(77)\n')
            launcher.chmod(0o755)
            env = dict(os.environ, PATH=str(t) + os.pathsep + os.environ['PATH'])
            out = t / 'result'
            bootstrap = 'import platform,runpy,sys; from pathlib import Path; platform.system=lambda:"Linux"; sys.argv=sys.argv[1:]; sys.path.insert(0,str(Path(sys.argv[0]).parent)); runpy.run_path(sys.argv[0],run_name="__main__")'
            cmd = [sys.executable, '-c', bootstrap, str(ROOT / 'run.py'), 'intel',
                   '--build', str(build), '--cpu-list', '0', '--numa-node', '0', '--out', str(out)]
            r = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=30)
            self.assertNotEqual(r.returncode, 0)
            self.assertTrue((out / 'run-status.json').exists(), r.stdout + r.stderr)
            self.assertEqual(json.loads((out / 'run-status.json').read_text())['status'], 'failed')
            self.assertIn('preflight unavailable/failed', (out / 'summary.txt').read_text())


if __name__ == '__main__':
    unittest.main(verbosity=2)
