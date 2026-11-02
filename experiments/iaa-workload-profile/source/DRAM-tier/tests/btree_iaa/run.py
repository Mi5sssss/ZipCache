#!/usr/bin/env python3
"""Process-isolated real-tree layout screen. All failures remain in the result.

Audit and performance are different processes. Only audit includes the per-read
byte oracle. Both validate the final state and include background drain.
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


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def command(args):
    try:
        p = subprocess.run(args, capture_output=True, text=True, timeout=15)
        return {"command": args, "returncode": p.returncode, "stdout": p.stdout, "stderr": p.stderr}
    except (OSError, subprocess.TimeoutExpired) as e:
        return {"command": args, "unavailable": str(e)}


def run(a):
    a.out.mkdir(parents=True, exist_ok=False)
    manifest = json.loads((a.trace / "manifest.json").read_text())
    trace = a.trace / "trace.bin"
    if digest(trace) != manifest["trace_sha256"]:
        raise ValueError("trace checksum mismatch")
    expected = functools.reduce(operator.xor, (int(h, 16) for h in manifest["worker_hashes"]))
    root = Path(__file__).resolve().parents[3]
    corpus = root / "SilesiaCorpus/samba.zip"
    real = manifest["source"]["status"] == "real-silesia-samba"
    if real and digest(corpus) != manifest["source"]["zip_sha256"]:
        raise ValueError("payload source checksum mismatch")
    files = [*sorted((root / "DRAM-tier/lib").glob("*")), *sorted(Path(__file__).parent.glob("*")),
             *sorted((root / "DRAM-tier/tests/btree").glob("*"))]
    source_hashes = {str(p.relative_to(root)): digest(p) for p in files if p.is_file()}
    metadata = {"platform": platform.platform(), "machine": platform.machine(), "python": platform.python_version(),
                "trace": manifest, "source_sha256": source_hashes,
                "commands": [command(c) for c in (["uname", "-a"], ["cmake", "--version"],
                ["cc", "--version"], ["pkg-config", "--modversion", "liblz4", "libzstd"],
                ["sysctl", "-n", "machdep.cpu.brand_string"], ["lscpu"], ["numactl", "--hardware"],
                ["accel-config", "list"])],
                "hardware_gate": "pending", "performance_gate": "pending",
                "accounting": "audited tree/codec heap plus fully reserved, pre-touched caller stacks; RSS separate; fixture and auditor excluded",
                "timing": "audit includes per-read oracle; performance excludes it; both include final drain; scheduled-arrival latency when rate > 0",
                "warning": "screen only, not a >=30s three-seed three-repeat product acceptance"}
    (a.out / "manifest.json").write_text(json.dumps(metadata, indent=2) + "\n")
    # Capture dirty SOURCE bytes, not just HEAD or a digest that cannot be replayed.
    for source in files:
        if source.is_file():
            target = a.out / "source" / source.relative_to(root)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(source.read_bytes())
    rows = []
    cases = [(layout, codec) for layout in a.layouts for codec in a.codecs]
    if a.reverse:
        cases.reverse()
    for layout, codec in cases:
        for mode in (["performance"] if a.performance_only else ["audit", "performance"] if a.performance else ["audit"]):
            name = f"l{layout}-{codec}-{mode}"
            env = os.environ.copy()
            # Do not accidentally measure an inherited zlib shim as the CPU
            # baseline. Interposition is explicit and scoped to the shim case.
            env.pop("LD_PRELOAD", None)
            env.pop("DYLD_INSERT_LIBRARIES", None)
            # Fault injection and stale attribution controls are never inherited
            # into measurements. Their dedicated tests set them explicitly.
            for key in list(env):
                if key.startswith("BTREE_TEST_") or key in ("AGG_BASE_IMAGE", "AGG_ACCEL_COMPRESS", "AGG_ACCEL_DECOMPRESS"):
                    env.pop(key)
            # Explicit experiment values; retain user QPL path/NUMA binding.
            env.update(BTREE_MEMORY_AUDIT=str(int(mode == "audit")), BTREE_BG_COMPACTION=str(a.bg),
                       BTREE_SHARDS=str(a.shards), BTREE_BG_BATCH_SIZE=str(a.batch_size), BTREE_BG_QUEUE_CAPACITY=str(a.queue_capacity),
                       BTREE_AGG_BACKPRESSURE=str(a.backpressure), BTREE_BG_INCLUDE_TRIGGER=str(a.include_trigger),
                       AGG_RANDOM=str(int(not real)), AGG_LEGACY_GET=str(int(a.legacy)),
                       AGG_VERIFY_EACH=str(int(mode == "audit")), AGG_ARRIVAL_RATE=str(a.arrival_rate),
                       SAMBA_ZIP_PATH=str(corpus), BTREE_QPL_PATH=a.qpl_path)
            for role in ("COMPRESS", "GET", "MAINTENANCE"):
                env.pop("BTREE_QPL_" + role + "_PATH", None)
            if codec == "qpl" and a.routes:
                for role, path in zip(("COMPRESS", "GET", "MAINTENANCE"), a.routes):
                    env["BTREE_QPL_" + role + "_PATH"] = path
                env["BTREE_QPL_JOB_CACHE"] = "thread"
            if a.snapshot:
                if a.bg or manifest["read_pct"] != 100:
                    raise ValueError("byte-identical read attribution requires bg=0 and a pure-read trace")
                env["AGG_BASE_IMAGE"] = str(a.out / (name + ".bases"))
            if codec == "zlib-accel":
                if a.accel_library:
                    env["LD_PRELOAD"] = str(a.accel_library)
                env.update(AGG_ACCEL_COMPRESS=str(a.accel_compress), AGG_ACCEL_DECOMPRESS=str(a.accel_decompress))
            binary = str(a.build / f"agg_bench_l{layout}")
            cmd = [binary, codec, str(trace)]
            if a.cpu_list:
                cmd = ["taskset", "-c", a.cpu_list] + cmd
            if a.numa_node is not None:
                cmd = ["numactl", "--membind=" + str(a.numa_node)] + cmd
            started = time.time()
            output = ""
            row = {"case": name, "mode": mode, "layout": layout, "codec": codec, "command": cmd,
                   "binary_sha256": digest(binary), "cpu_list": a.cpu_list, "numa_node": a.numa_node,
                   "capacity_ledger_complete": codec not in ("qpl", "zlib-accel"),
                   "host": platform.node(), "started_unix": started,
                   "env": {k: v for k, v in env.items() if k.startswith(("BTREE_", "AGG_", "SAMBA_"))}}
            try:
                proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=a.timeout)
                output = proc.stdout + proc.stderr
                row["returncode"] = proc.returncode
                row["status"] = "ok" if not proc.returncode else "unavailable" if proc.returncode == 77 else "failed"
                for line in proc.stdout.splitlines():
                    tag, _, payload = line.partition(" ")
                    if payload.startswith("{"):
                        row[tag.lower()] = json.loads(payload)
                if row["status"] == "ok":
                    p, agg = row["product"], row["aggregation"]
                    if p["trace_hash"] != f"{expected:016x}" or p["mismatches"] or row["teardown"]["after_thread_release"]:
                        raise ValueError("incorrect history, payload, or teardown")
                    if p["async"] != a.bg or p["actual_backend"] != codec:
                        raise ValueError("requested configuration was not executed")
                    row["steady_bytes"] = p["resident_usable"] + agg["stack_allocated_usable"]
                    row["peak_bytes"] = max(p["audit_peak_usable"], p["preload_peak_usable"]) + agg["stack_allocated_usable"] if mode == "audit" else None
                    suffix = "_with_oracle" if mode == "audit" else ""
                    row["completed_ops_per_second" + suffix] = p["operations"] * 1e9 / p["completed_ns"]
                    row["cpu_ns_per_op" + suffix] = p["cpu_ns"] / p["operations"]
                    row["duration_eligible"] = p["foreground_ns"] >= 30_000_000_000
                    row["update_rounds"] = p["writes"] / p["live_keys"]
                    row["update_eligible"] = not p["writes"] or row["update_rounds"] >= 10
                    row["offered_load_eligible"] = a.arrival_rate > 0 and p["foreground_ns"] <= 1.05*p["operations"]*1e9/a.arrival_rate
                    execution = row["execution"]
                    if execution["verify_each"] != int(mode == "audit"):
                        raise ValueError("oracle mode mismatch")
                    if a.snapshot:
                        row["base_image_sha256"] = digest(env["AGG_BASE_IMAGE"])
                    if codec == "qpl":
                        if "1.9.0" not in p["library_version"]:
                            raise ValueError("QPL version is not pinned v1.9.0")
                        if a.routes and [r["requested"] for r in execution["routes"]] != a.routes:
                            raise ValueError("effective routes differ from requested routes")
                        if p["explicit_fallbacks"] or any(r["failed"] for r in execution["routes"]):
                            raise ValueError("QPL execution failed or silently fell back")
                        if any(r["calls"] != r["completed"]+r["failed"] for r in execution["routes"]):
                            raise ValueError("unexplained synchronous route call loss")
                    if codec == "zlib-accel":
                        counters=execution["accel_counters"]
                        row["accel_iaa_compress"] = counters[4]-counters[5]
                        row["accel_iaa_decompress"] = counters[14]-counters[15]
                        row["accel_cpu_fallback_calls"] = (counters[8] if a.accel_compress else 0)+(counters[18] if a.accel_decompress else 0)
                        row["capacity_ledger_complete"] = False  # private shim allocations not exposed by zlib
                        row["hardware_observed"] = (not a.accel_compress or counters[4]>0) and (not a.accel_decompress or counters[14]>0)
                        if any(counters[i] for i in (1,2,3,5,6,7,11,12,13,15,16,17)):
                            raise ValueError("shim errors or unexpected QAT/IGZIP execution")
                        if (not a.accel_compress and counters[4]) or (not a.accel_decompress and counters[14]):
                            raise ValueError("shim executed a disabled IAA direction")
                        if not execution["accel_observed"]:
                            raise ValueError("shim observer unavailable")
                        for line in proc.stdout.splitlines():
                            if line.startswith("ACCEL_LIBRARY "):
                                row["loaded_accel_library"] = line[len("ACCEL_LIBRARY "):]
                                row["loaded_accel_sha256"] = digest(row["loaded_accel_library"])
            except (subprocess.TimeoutExpired, OSError, ValueError) as e:
                row.update(status="failed", error=str(e))
                output += "\n" + str(e)
                if isinstance(e, subprocess.TimeoutExpired):
                    output += "\n" + str(e.stdout or "") + "\n" + str(e.stderr or "")
            row["finished_unix"] = time.time()
            row["elapsed_seconds"] = row["finished_unix"] - started
            (a.out / (name + ".log")).write_text(output)
            rows.append(row)
            (a.out / "results.json").write_text(json.dumps(rows, indent=2) + "\n")
            print(name, row["status"], row.get("steady_bytes"), flush=True)
    raw = [r for r in rows if r["codec"] == "raw" and r["mode"] == "audit" and r["status"] == "ok"]
    best = min(raw, key=lambda r: r["steady_bytes"]) if len(raw) == len(a.layouts) else None
    # One actual RAW configuration supplies BOTH denominators. Do not combine
    # independently minimized steady and peak values into an imaginary baseline.
    for row in rows:
        if best and row["status"] == "ok" and row["mode"] == "audit":
            row["raw_reference"] = best["case"]
            row["steady_capacity"] = best["steady_bytes"] / row["steady_bytes"]
            row["peak_capacity"] = best["peak_bytes"] / row["peak_bytes"]
            row["capacity_screen"] = "pass" if min(row["steady_capacity"], row["peak_capacity"]) >= 1.25 else "fail"
            if row.get("capacity_ledger_complete") is False:row["capacity_screen"]="pending-incomplete-private-workspace"
            same = next(r for r in raw if r["layout"] == row["layout"])
            row["same_layout_steady_capacity"] = same["steady_bytes"] / row["steady_bytes"]
        else:
            row["capacity_screen"] = "pending"
    (a.out / "results.json").write_text(json.dumps(rows, indent=2) + "\n")
    if any(r["status"] == "failed" for r in rows):
        raise SystemExit("At least one configuration failed; inspect preserved logs.")
    return rows


def parser():
    p = argparse.ArgumentParser()
    p.add_argument("trace", type=lambda s: Path(s).resolve())
    p.add_argument("out", type=lambda s: Path(s).resolve())
    p.add_argument("--build", type=lambda s: Path(s).resolve(), default=Path(__file__).parent / "work/build")
    p.add_argument("--layouts", nargs="+", type=int, choices=range(4), default=list(range(4)))
    p.add_argument("--codecs", nargs="+", choices=("raw", "lz4", "zstd", "zlib", "qpl", "zlib-accel"), default=["raw", "lz4", "zstd", "zlib"])
    p.add_argument("--shards", type=int, choices=(1, 8), default=8)
    p.add_argument("--bg", type=int, choices=(0, 1), default=1)
    p.add_argument("--backpressure", type=int, choices=(0, 1), default=0)
    p.add_argument("--include-trigger", type=int, choices=(0, 1), default=0)
    p.add_argument("--batch-size", type=int, choices=(1, 2, 4, 8, 16, 32), default=8)
    p.add_argument("--queue-capacity", type=int, default=32)
    p.add_argument("--qpl-path", choices=("hardware", "software"), default="hardware")
    p.add_argument("--routes", nargs=3, choices=("hardware", "software"), metavar=("COMPRESS", "GET", "MAINTENANCE"))
    p.add_argument("--arrival-rate", type=int, default=0)
    p.add_argument("--cpu-list", help="Explicit Linux logical CPUs, e.g. 2,4; applied to the whole process")
    p.add_argument("--numa-node", type=int)
    p.add_argument("--accel-library", type=lambda s: Path(s).resolve())
    p.add_argument("--accel-compress", type=int, choices=(0,1), default=1)
    p.add_argument("--accel-decompress", type=int, choices=(0,1), default=0)
    p.add_argument("--timeout", type=int, default=1800)
    for flag in ("performance", "performance-only", "legacy", "reverse", "snapshot"):
        p.add_argument("--" + flag, action="store_true")
    return p

if __name__ == "__main__":
    run(parser().parse_args())
