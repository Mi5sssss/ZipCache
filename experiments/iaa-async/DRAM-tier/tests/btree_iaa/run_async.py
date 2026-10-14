#!/usr/bin/env python3
"""Isolated async-QPL qualification. No downloads, WQ changes, upload or fallback.

Screen is a mechanism screen, not automatic product acceptance. Audit and timed
runs are separate processes; failed/unavailable cases stay in results.json.
"""
import argparse
import functools
import hashlib
import json
import operator
import os
from pathlib import Path
import platform
import subprocess
import time

from prepare import prepare

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def capture(cmd, env=None, timeout=1800):
    try:
        p = subprocess.run([str(x) for x in cmd], env=env, text=True,
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
        return p.returncode, p.stdout
    except subprocess.TimeoutExpired as e:
        out = e.stdout or b""
        return 124, (out.decode(errors="replace") if isinstance(out, bytes) else out) + "\nTIMEOUT\n"
    except OSError as e:
        return 127, str(e)


def parse(output):
    rows = {}
    for line in output.splitlines():
        tag, _, payload = line.partition(" ")
        if not payload.startswith("{"):
            continue
        value = json.loads(payload)
        if tag in ("CODEC_WORK", "QPL_MEMORY"):
            rows.setdefault(tag, []).append(value)
        else:
            rows[tag] = value
    return rows


def clean_env():
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("BTREE_", "AGG_", "SD_")) and
           k not in ("LD_PRELOAD", "DYLD_INSERT_LIBRARIES")}
    return env


def bound(cmd, a):
    if a.cpu_list:
        cmd = ["taskset", "-c", a.cpu_list] + cmd
    if a.numa_node is not None:
        cmd = ["numactl", "--membind=" + str(a.numa_node)] + cmd
    return cmd


def summary(out, rows):
    lines = ["ZipCache cross-leaf QPL screen, metrics schema 3",
             "Hardware/product acceptance: PENDING. Software queue/jobs are not IAA WQ occupancy.",
             "No capacity PASS for QPL until private-library accounting is resolved.",
             "Times include final drain; GET/PUT p99 are API/arrival-to-return, not codec p99.",
             "Audit oracle timings are excluded from performance comparisons.", "",
             "case | status | QPS | CPU ns/op | GET/PUT p99 us | base ratio | owned steady/peak capacity | peak background jobs"]
    for r in rows:
        if r["mode"] != "performance":
            continue
        if r["status"] != "ok":
            lines.append(f'{r["name"]} | {r["status"]} | {r.get("reason", "see case log")}')
            continue
        p = r["metrics"]["PRODUCT"]
        m = r["metrics"]["MEMORY_V3"]
        c = r["metrics"].get("CROSS_V3", {})
        ratio = m["base_raw_record_bytes"] / max(1, m["base_encoded_bytes"])
        audit = next((x for x in rows if x["group"] == r["group"] and
                      x["case"] == r["case"] and x["mode"] == "audit" and x["status"] == "ok"), None)
        raw = [x for x in rows if x["group"][:-1] == r["group"][:-1] and
               x["codec"] == "raw" and x["mode"] == "audit" and x["status"] == "ok"]
        cap = "unavailable"
        if audit and raw:
            cs = min(x["owned_steady"] for x in raw) / audit["owned_steady"]
            cp = min(x["owned_peak"] for x in raw) / audit["owned_peak"]
            cap = f"{cs:.3f}/{cp:.3f}x" + (" [partial]" if r["codec"] == "qpl" else "")
        lines.append(f'{r["name"]} | ok | {p["operations"]*1e9/p["completed_ns"]:.0f} | '
                     f'{p["cpu_ns"]/p["operations"]:.1f} | {p["get_p99_ns"]/1000:.2f}/{p["put_p99_ns"]/1000:.2f} | '
                     f'{ratio:.3f}x | {cap} | {c.get("peak_jobs", "unavailable")}')
        timing = r["metrics"].get("TIMING_V2", {})
        lines.append(f'  actual drain={timing.get("drain_call_ns", "unavailable")} ns; '
                     f'30s eligible={r["duration_eligible"]}; ten update rounds={r["update_eligible"]}; '
                     f'accepted/terminal/failed={c.get("accepted", "n/a")}/{c.get("terminal", "n/a")}/{c.get("failed", "n/a")}')
    lines += ["", "Capacity excludes fixture/auditor and shared library/OS overhead; RSS is separate in JSON.",
              "Base ratio includes RAW blocks, excludes active/pending/metadata/workspace. It is NOT capacity gain.",
              "CPU-contract cross runs are synchronous software byte-transform tests, NOT emulated IAA performance."]
    (out / "summary.txt").write_text("\n".join(lines) + "\n")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("action", choices=("check", "resources", "screen"))
    p.add_argument("--local", action="store_true", help="no QPL; CPU and contract tests only")
    p.add_argument("--build", type=Path, default=ROOT / "work/async-build")
    p.add_argument("--out", type=Path, default=ROOT / ("results-" + time.strftime("%Y%m%d-%H%M%S")))
    p.add_argument("--corpus", type=Path, default=ROOT / "SilesiaCorpus/samba.zip")
    p.add_argument("--layouts", nargs="+", type=int, choices=(2, 3), default=[3])
    p.add_argument("--clients", nargs="+", type=int, choices=(1, 4, 16), default=[1, 4])
    p.add_argument("--mappings", nargs="+", choices=("adjacent", "permuted"), default=["adjacent", "permuted"])
    p.add_argument("--windows", nargs="+", type=int, choices=(1, 2, 4, 8), default=[1, 4])
    p.add_argument("--seeds", nargs="+", type=int, default=[11])
    p.add_argument("--repeats", type=int, default=1)
    p.add_argument("--keys", type=int, default=150000)
    p.add_argument("--ops", type=int, default=100000, help="operations per client, same trace for all candidates")
    p.add_argument("--reads", type=int, default=80)
    p.add_argument("--order", choices=("random", "sequential"), default="random")
    p.add_argument("--huffman", choices=("fixed", "dynamic"), default="dynamic")
    p.add_argument("--cpu-list")
    p.add_argument("--numa-node", type=int)
    p.add_argument("--timeout", type=int, default=1800)
    p.add_argument("--poll-us", type=int, default=0)
    p.add_argument("--sanitizer", choices=("none", "address", "thread"), default="none")
    a = p.parse_args()
    if a.repeats < 1 or not 0 <= a.poll_us <= 1000:
        p.error("invalid repeats or poll interval")
    if a.action == "screen" and a.sanitizer != "none":
        p.error("sanitizers are for check, not performance")
    a.build = a.build.resolve(); a.out = a.out.resolve(); a.corpus = a.corpus.resolve()
    a.out.mkdir(parents=True, exist_ok=False)
    env = clean_env()
    files = [x for directory in (ROOT / "DRAM-tier/lib", HERE, HERE.parent / "async_device", HERE.parent / "btree")
             for x in directory.iterdir() if x.is_file() and x.suffix in (".c", ".h", ".inc", ".py", ".txt")]
    manifest = {"schema": 3, "platform": platform.platform(), "settings": {k: str(v) if isinstance(v, Path) else v for k, v in vars(a).items()},
                "source_sha256": {str(x.relative_to(ROOT)): sha(x) for x in sorted(files)},
                "qpl_version_required": "1.9.0", "qpl_commit": "1813ccedb90b6468d47cc4ea87ae8b754a9151e1",
                "qpl_root": env.get("QPL_ROOT"), "cpu_budget_controlled": bool(a.cpu_list),
                "hardware_gate": "pending", "machine": []}
    for cmd in (["uname", "-a"], ["lscpu"], ["numactl", "--hardware"], ["accel-config", "list"],
                ["pkg-config", "--modversion", "liblz4", "libzstd"], ["cmake", "--version"]):
        rc, text = capture(cmd, env, 20); manifest["machine"].append({"command": cmd, "returncode": rc, "output": text})
    (a.out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    for name, cmd in (("configure", ["cmake", "-S", HERE, "-B", a.build, "-DCMAKE_BUILD_TYPE=Release",
                        "-DAGG_DISABLE_QPL=" + ("ON" if a.local else "OFF"),
                        "-DAGG_ASAN=" + ("ON" if a.sanitizer == "address" else "OFF"),
                        "-DAGG_TSAN=" + ("ON" if a.sanitizer == "thread" else "OFF")]),
                      ("build", ["cmake", "--build", a.build, "-j4"])):
        rc, text = capture(cmd, env, a.timeout); (a.out / (name + ".log")).write_text(text)
        if rc:
            (a.out / "summary.txt").write_text(f"FAILED: {name}; see {name}.log\n"); return 1
    if a.action == "check":
        rc, text = capture(["ctest", "--test-dir", a.build, "--output-on-failure", "-j4"], env, a.timeout)
        (a.out / "summary.txt").write_text(text);return int(bool(rc))
    for path in ("software", "hardware"):
        for n in (1, 4, 8):
            cmd = bound([str(a.build / "agg_qpl_resources"), path, str(n)], a)
            rc, text = capture(cmd, env, a.timeout)
            (a.out / f"resources-{path}-{n}.log").write_text(text + f"\nreturncode={rc}\n")
    if a.action == "resources":
        (a.out / "summary.txt").write_text("See resources-*.log. Missing QPL/hardware is unavailable, not a pass. Process heap/RSS deltas are diagnostic, not precise private-memory attribution.\n")
        return 0
    if sha(a.corpus) != "285c06096c0e24b71e28705f489932482b23d823b306dddc1cd8d0a8145121a1":
        raise ValueError("unexpected Silesia samba archive")
    if not a.local:
        pre = dict(env, BTREE_QPL_CROSS_HARDWARE="1", SAMBA_ZIP_PATH=str(a.corpus))
        rc, text = capture(bound([str(a.build / "agg_qpl_format")], a), pre, a.timeout)
        (a.out / "qpl-format.log").write_text(text)
        if rc:
            (a.out / "summary.txt").write_text("Hardware preflight unavailable/failed. No hardware result is claimed. See qpl-format.log.\n")
            return 1
    cases = [("raw", "raw", 0, 0, "software"), ("lz4", "lz4", 0, 1, "software"),
             ("zstd", "zstd", 0, 1, "software"), ("zlib", "zlib", 0, 1, "software")]
    if a.local:
        cases += [(f"cpu-contract-{codec}-w{w}", codec, w, 1, "software") for codec in ("lz4", "zlib") for w in a.windows]
    else:
        cases += [("qpl-software", "qpl", 0, 1, "software"),
                  ("qpl-hardware-serial", "qpl", 0, 0, "hardware"),
                  ("qpl-hardware-serial-reuse", "qpl", 0, 1, "hardware")]
        cases += [(f"qpl-hardware-cross-w{w}-r{r}", "qpl", w, r, "hardware") for w in a.windows for r in (0, 1)]
    rows = []
    for seed in a.seeds:
      for clients in a.clients:
       for mapping in a.mappings:
        trace_dir = a.out / f"trace-{seed}-{clients}-{mapping}"
        trace = prepare(trace_dir, keys=a.keys, threads=clients, ops=a.ops, seed=seed, reads=a.reads,
                        mapping=mapping, corpus=a.corpus, ownership="shuffled", order=a.order)
        expected = functools.reduce(operator.xor, (int(h, 16) for h in trace["worker_hashes"]))
        for repeat in range(a.repeats):
         for layout in a.layouts:
          for case, codec, window, reuse, path in (cases if repeat % 2 == 0 else reversed(cases)):
           for mode in ("audit", "performance"):
            name = f"l{layout}-{mapping}-c{clients}-s{seed}-r{repeat}-{case}-{mode}"
            e = dict(env, BTREE_MEMORY_AUDIT=str(int(mode == "audit")), AGG_VERIFY_EACH=str(int(mode == "audit")),
                     AGG_RANDOM="0", SAMBA_ZIP_PATH=str(a.corpus), BTREE_BG_COMPACTION="1", BTREE_SHARDS="8",
                     BTREE_AGG_BACKPRESSURE="1", BTREE_BG_BATCH_SIZE="8", BTREE_BG_QUEUE_CAPACITY="32",
                     BTREE_CROSS_LEAF=str(int(window > 0)), BTREE_CROSS_CPU_TEST=str(int(a.local)),
                     BTREE_CROSS_OUTSTANDING=str(window or 1), BTREE_CROSS_BYTES="16777216",
                     BTREE_CROSS_JOB_BYTES="1048576", BTREE_CROSS_POLL_US=str(a.poll_us),
                     BTREE_REUSE_UNCHANGED_BLOCKS=str(reuse), BTREE_QPL_PATH=path, AGG_QPL_HUFFMAN=a.huffman)
            if codec == "qpl":
                e.update({"BTREE_QPL_" + role + "_PATH": path for role in ("COMPRESS", "GET", "MAINTENANCE")})
                e["BTREE_QPL_JOB_CACHE"] = "thread"
            cmd = bound([str(a.build / f"agg_bench_l{layout}"), codec, str(trace_dir / "trace.bin")], a)
            print(name, flush=True)
            rc, text = capture(cmd, e, a.timeout); (a.out / (name + ".log")).write_text(text)
            row = {"name": name, "case": case, "codec": codec, "layout": layout, "mode": mode,
                   "group": [seed, clients, mapping, repeat, layout], "command": cmd, "returncode": rc,
                   "trace_sha256": trace["trace_sha256"], "status": "ok" if rc == 0 else "unavailable" if rc == 77 else "failed"}
            try:
                row["metrics"] = parse(text)
                if not rc:
                    m = row["metrics"]; p = m["PRODUCT"]; c = m.get("CROSS_V3", {})
                    assert p["status"] == "ok" and not p["mismatches"] and p["trace_hash"] == f"{expected:016x}"
                    assert not m["TEARDOWN"]["after_thread_release"] and m["EXECUTION"]["verify_each"] == int(mode == "audit")
                    if window:
                        assert c["enabled"] and c["window"] == window and c["accepted"] == c["terminal"] and not c["failed"]
                    if codec == "qpl":
                        assert p["library_version"] == "1.9.0" and not p["explicit_fallbacks"]
                        assert not p["failed_tasks"] and all(not x["failed"] for x in m["EXECUTION"]["routes"])
                        assert all(x["requested"] == path for x in m["EXECUTION"]["routes"])
                    row["owned_steady"] = p["resident_usable"] + m["AGGREGATION"]["stack_allocated_usable"]
                    row["owned_peak"] = max(p["audit_peak_usable"], p["preload_peak_usable"]) + m["AGGREGATION"]["stack_allocated_usable"]
                    row["duration_eligible"] = p["foreground_ns"] >= 30_000_000_000
                    row["update_eligible"] = p["writes"] == 0 or p["writes"] >= 10*p["live_keys"]
            except (AssertionError, ValueError, KeyError) as error:
                row["status"] = "failed"; row["reason"] = "output validation: " + repr(error)
            rows.append(row)
            (a.out / "results.json").write_text(json.dumps(rows, indent=2) + "\n")
            summary(a.out, rows)
    return int(any(r["status"] == "failed" for r in rows))


if __name__ == "__main__":
    raise SystemExit(main())
