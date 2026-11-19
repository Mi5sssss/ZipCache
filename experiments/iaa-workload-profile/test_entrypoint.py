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
    def test_sweep_plan_and_validation(self):
        args = argparse.Namespace(build=Path('build-intel'), cpu_list='2,4', numa_node=0,
                                  perf=False, reads=[0, 87, 100])
        plan = dict(runner.commands(args, Path('/tmp/example-results')))
        self.assertEqual(set(plan), {'configure', 'build', 'check', 'smoke', 'performance'})
        self.assertIn('uniform-r0,uniform-r87,uniform-r100', plan['performance'])
        for values in (['-1'], ['101'], ['90', '90'], ['2.5']):
            proc = subprocess.run([sys.executable, runner.__file__, '--prepare-only', '--reads', *values],
                                  capture_output=True)
            self.assertEqual(proc.returncode, 2)

    def test_only_complete_matched_pairs_can_show_a_change_in_lead(self):
        percentages = [80, 87, 90, 95]
        hashes = {f'uniform-r{pct}-n1000000': str(pct) for pct in percentages}
        rows = []
        for pct, ns in zip(percentages, ([800, 900, 950], [900, 1000, 1100],
                                          [1100, 1200, 1300], [900, 950, 975])):
            for rep in range(3):
                for codec, elapsed in [('lz4', 1000), ('qpl', ns[rep])]:
                    rows.append(dict(point=f'uniform-r{pct}', repeat=rep, codec=codec, mode='performance',
                                     status='ok', INPUT={'trace_sha256': str(pct)},
                                     PRODUCT={'operations': 1, 'completed_ns': elapsed}))
        lines, _, _ = runner.crossover_summary(rows, percentages, hashes)
        self.assertTrue(any('87% |' in line and 'inconclusive' in line for line in lines))
        self.assertIn('80% to 90% GET (IAA to LZ4)', lines[-2])
        self.assertIn('90% to 95% GET (LZ4 to IAA)', lines[-2])
        self.assertIn('none resolved', runner.crossover_summary(rows[:6], [80], hashes)[0][-2])
        for bad in (rows[:-1], rows + rows[:1], [dict(rows[0], status='failed')] + rows[1:],
                    [dict(rows[0], INPUT={'trace_sha256':'wrong'})] + rows[1:]):
            with self.assertRaises(ValueError):
                runner.crossover_summary(bad, percentages, hashes)

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

    def test_confirm_uses_one_sweep_bracket_and_one_rate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'performance').mkdir()
            reads = [20, 71, 98]
            hashes = {f'uniform-r{p}-n1000000': str(p) for p in reads}
            (root / 'inputs.json').write_text(json.dumps(hashes))
            prior = dict(status='complete', reads=reads, cpu_list='2,4', numa_node=0)
            (root / 'run-status.json').write_text(json.dumps(prior))
            rows = [dict(point=f'uniform-r{p}', repeat=i, codec=codec, mode='performance', status='ok',
                         INPUT={'trace_sha256':str(p)}, PRODUCT={'operations':100, 'completed_ns':elapsed})
                    for p in reads for i in range(3)
                    for codec,elapsed in [('lz4',1000), ('qpl',1500 if p==98 else 750)]]
            (root / 'performance/results.json').write_text(json.dumps(rows))
            args = argparse.Namespace(confirm_from=root, reads=None, cpu_list=None, numa_node=None,
                                      build=root, perf=False, arrival_rate=80000)
            lines, verified = runner.select_confirmation(args)
            self.assertEqual(verified,hashes)
            self.assertEqual((args.reads,args.cpu_list,args.numa_node),([71,98],'2,4',0))
            plan=dict(runner.commands(args,root/'out'))
            self.assertEqual(list(plan), ['configure','build','check','smoke','matched-rate','memory'])
            for stage,action in [('matched-rate','intel'),('memory','intel-audit')]:
                cmd=plan[stage]
                self.assertIn(action,cmd)
                self.assertEqual(cmd[cmd.index('--points')+1], 'uniform-r71,uniform-r98')
                self.assertEqual(cmd[cmd.index('--arrival-rate')+1], '80000')
            args.cpu_list='0-32'
            with self.assertRaisesRegex(ValueError,'cpu_list'):
                runner.select_confirmation(args)
            args.cpu_list=None
            args.reads=[20,98]
            runner.select_confirmation(args)
            args.reads=[20,71]
            with self.assertRaisesRegex(ValueError,'bracket'):
                runner.select_confirmation(args)
            for row in rows:
                if row['point']=='uniform-r20' and row['codec']=='qpl':
                    row['PRODUCT']['completed_ns']=1200
            (root / 'performance/results.json').write_text(json.dumps(rows))
            args.reads=None
            with self.assertRaisesRegex(ValueError,'bracket'):
                runner.select_confirmation(args)
            args.reads=[20,71]
            runner.select_confirmation(args)
            for row in rows:
                if row['codec']=='qpl':
                    row['PRODUCT']['completed_ns']=1200
            (root / 'performance/results.json').write_text(json.dumps(rows))
            with self.assertRaisesRegex(ValueError,'No change'):
                runner.select_confirmation(args)

    def test_matched_summary_separates_latency_and_memory(self):
        from confirmation import confirmation_summary
        hashes={'uniform-r100-n100':'fixture'}
        def sample(codec, mode, rep):
            return dict(codec=codec,point='uniform-r100',repeat=rep,status='ok',mode=mode,
                        INPUT={'trace_sha256':'fixture','operations':100},
                        EXECUTION=dict(arrival_rate=80000,latency_basis='scheduled-arrival-to-return'),
                        PRODUCT=dict(operations=100,completed_ns=1250000,foreground_ns=1250000,cpu_ns=100000,reads=100,writes=0,
                                     get_p99_ns=2500,put_p99_ns=0,rss=2**20,ru_maxrss_native=0),
                        STORAGE=dict(cold_data_ratio=1.2,total_owned_retained_bytes=100,
                                     total_owned_peak_bytes=200,tracked_memory_saved_retained_pct=2,
                                     tracked_memory_saved_peak_pct=1,memory_ledger_complete_for_scope=True))
        perf=[sample(codec,'performance',i) for codec in ('lz4','zlib') for i in range(3)]
        memory=[sample(codec,'audit',0) for codec in ('raw','lz4','zlib')]
        def summary():
            return confirmation_summary(perf,memory,[100],80000,hashes,device='zlib')
        lines=summary()
        self.assertTrue(any('80000 | 1000 | 2.50 | n/a | 1.00 / n/a' in l for l in lines))
        for key,bad in [('arrival_rate',0),('latency_basis','API-call-to-return')]:
            with patch.dict(memory[0]['EXECUTION'],{key:bad}), self.assertRaises(ValueError):
                summary()
        with patch.dict(perf[0]['INPUT'],trace_sha256='mismatch'), self.assertRaises(ValueError):
            summary()
        memory.pop()
        with self.assertRaisesRegex(ValueError,'missing'):
            summary()

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
