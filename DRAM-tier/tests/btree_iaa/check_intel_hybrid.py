#!/usr/bin/env python3
"""Offline runner-contract checks. No fake QPL or IAA performance results."""
import json
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import intel_hybrid as runner
from prepare import prepare


class RunnerTests(unittest.TestCase):
    def test_public_entry_point(self):
        entry = Path(__file__).with_name("intel_eval.py")
        result = subprocess.run([sys.executable, str(entry), "--help"],
                                text=True, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--qpl-root", result.stdout)
        self.assertIn("--cpu-only", result.stdout)

    def test_missing_submodule_is_actionable(self):
        with tempfile.TemporaryDirectory() as d:
            with mock.patch.object(runner, "ROOT", Path(d)):
                with self.assertRaisesRegex(ValueError, "git submodule update --init SilesiaCorpus"):
                    runner.require_corpus()

    def options(self, root, *extra):
        args = runner.parser().parse_args(["--out", str(root / "output"), "--qpl-root", str(root / "qpl"),
            "--cpu-list", "2,4", "--numa-node", "0", *extra])
        runner.validate(args)
        return args

    def test_plan_and_isolation(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); a = self.options(root)
            plan = runner.build_plan(a)
            self.assertFalse(a.out.exists())
            matrices = [s for s in plan["steps"] if s.get("matrix")]
            self.assertEqual(len(matrices), 3)  # both mappings + same-byte GET
            self.assertEqual(matrices[0]["policies"], runner.POLICIES)
            self.assertTrue(all("--ownership" in s["command"] and "shuffled" in s["command"]
                for s in plan["steps"] if "prepare.py" in " ".join(s["command"])))
            self.assertTrue(any(s.get("reject_skips") for s in plan["steps"]))
            with mock.patch.dict("os.environ", {"BTREE_TEST_FAIL_CODEC":"1", "AGG_BASE_IMAGE":"old", "LD_PRELOAD":"bad.so"}):
                env = runner.child_environment(a)
            self.assertNotIn("LD_PRELOAD", env)
            self.assertNotIn("BTREE_TEST_FAIL_CODEC", env)
            self.assertNotIn("AGG_BASE_IMAGE", env)
            self.assertEqual(env["BTREE_QPL_CROSS_HARDWARE"], "1")

    def test_qualification_and_validation(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            with self.assertRaises(ValueError):
                self.options(root, "--profile", "qualification")
            with self.assertRaises(ValueError):
                self.options(root, "--cpu-list", "2,02")
            with self.assertRaises(ValueError):
                self.options(root, "--cpu-only")
            a = self.options(root, "--profile", "qualification", "--rates", "1000", "100000",
                             "--get-slo-us", "20", "--put-slo-us", "100")
            self.assertGreaterEqual(runner.operation_count(a, 4, 80)*4*.2/a.keys, 12)
            plan = runner.build_plan(a)
            matrices = [s for s in plan["steps"] if s.get("group")]
            self.assertEqual(len(matrices), 12)  # two mappings x three seeds x two rates
            self.assertEqual({s["seed"] for s in matrices}, {11, 29, 47})

    def test_unavailable_and_missing_cases_are_failures(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); a = self.options(root, "--layouts", "3")
            (root / "rows.json").write_text("[]")
            with self.assertRaises(RuntimeError):
                runner.check_matrix(dict(matrix=str(root), policies=runner.POLICIES), a)
            (root / "comparison.json").write_text(json.dumps({"comparisons":[{"read_attribution":"pending"}]*2}))
            with self.assertRaises(RuntimeError):
                runner.check_matrix(dict(matrix=str(root), attribution=True), a)

    def test_shuffled_history_reproducible_and_shared_shards(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            first = prepare(root / "a", keys=512, threads=4, ops=1000, ownership="shuffled")
            second = prepare(root / "b", keys=512, threads=4, ops=1000, ownership="shuffled")
            self.assertEqual(first["trace_sha256"], second["trace_sha256"])
            blob = (root / "a/trace.bin").read_bytes()
            events = struct.unpack("<8000I", blob[48+512*4:])
            domains = [set(events[t*2000:(t+1)*2000:2]) for t in range(4)]
            for i, keys in enumerate(domains):
                self.assertEqual({k % 8 for k in keys}, set(range(8)))
                self.assertTrue(all(keys.isdisjoint(other) for other in domains[i+1:]))
            self.assertEqual(len(set.union(*domains)), 512)
            with self.assertRaises(ValueError):
                prepare(root / "bad", ownership="shuffled", batch=8)

    def test_idle_hardware_is_not_a_failure_or_offload_evidence(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); a = self.options(root, "--layouts", "3")
            rows = [dict(policy="B", matrix_layout=3, rep=1, mode=mode, status="ok",
                execution=dict(routes=[dict(requested="hardware", calls=0, completed=0, failed=0)]))
                for mode in ("audit", "performance")]
            (root / "rows.json").write_text(json.dumps(rows))
            step = dict(matrix=str(root), policies=["B"])
            runner.check_matrix(step, a)
            self.assertEqual(len(step["no_timed_hardware_work"]), 2)
            rows[0]["execution"]["routes"][0]["calls"] = 1
            (root / "rows.json").write_text(json.dumps(rows))
            with self.assertRaises(RuntimeError):
                runner.check_matrix(dict(matrix=str(root), policies=["B"]), a)

    def test_legacy_trace_is_unchanged(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            legacy = prepare(root / "old", keys=150000, threads=4, ops=100000, seed=11, reads=80)
            self.assertEqual(legacy["trace_sha256"], "9af29e178c21ff968d45e0582f3db3574e9905b329ec53e8709ca79866425b11")

    def test_failed_report_remains_deliverable(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); a = self.options(root, "--cpu-only", "--profile", "smoke")
            a.out.mkdir()
            matrix = a.out / "broken"; matrix.mkdir(); (matrix / "rows.json").write_text("[")
            plan = dict(status="failed", steps=[dict(name="broken", matrix=str(matrix))], limitations=[])
            runner.summarize(a, plan); runner.archive_results(a)
            self.assertTrue((a.out / "intel-results.tar.gz").is_file())
            self.assertEqual(json.loads((a.out / "summary.json").read_text())["product_gate"], "pending")

    def test_real_tree_shared_shard_trace(self):
        # CTest supplies a real L3 binary; this is not a queue/cache substitute.
        if len(sys.argv) < 2 or not Path(sys.argv[1]).is_file():
            self.skipTest("Run under CTest to execute the real tree fixture")
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            manifest = prepare(root / "trace", keys=1024, threads=4, ops=1000, ownership="shuffled")
            a = self.options(root, "--cpu-only", "--profile", "smoke")
            env = runner.child_environment(a)
            env.update(AGG_RANDOM="1", AGG_VERIFY_EACH="1", BTREE_MEMORY_AUDIT="1", BTREE_SHARDS="8")
            for bg in ("0", "1"):
                env["BTREE_BG_COMPACTION"] = bg
                p = subprocess.run([sys.argv[1], "lz4", str(root / "trace/trace.bin")], env=env,
                                   text=True, capture_output=True, timeout=60)
                self.assertEqual(p.returncode, 0, p.stdout+p.stderr)
                records = {line.split(" ", 1)[0]:json.loads(line.split(" ", 1)[1])
                    for line in p.stdout.splitlines() if " {" in line}
                self.assertEqual(records["PRODUCT"]["mismatches"], 0)
                self.assertEqual(records["TEARDOWN"]["after_thread_release"], 0)
                self.assertEqual(records["PRODUCT"]["writes"], manifest["updates"])


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
