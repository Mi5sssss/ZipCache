#!/usr/bin/env python3
"""One entry point for CPU/IAA placement experiments, not device configuration.

No network, sudo, automatic WQ changes, publication, or hardware simulation.
Archive/results always retain pending hardware-private memory qualification.
"""
import argparse
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import tarfile
import time
from intel_summary import write_summary

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
CPU_POLICIES = ["E-raw", "E-lz4", "E-zstd", "E-zlib"]
POLICIES = CPU_POLICIES + ["A", "B", "C", "D"]


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--qpl-root", type=Path)
    p.add_argument("--qpl-linkage", choices=("shared", "static"), default="shared")
    p.add_argument("--diagnostic", action="store_true", help="RAW/LZ4/QPL software/IAA plus same-byte GET; no hybrid policy sweep")
    p.add_argument("--backpressure", type=int, choices=(0, 1), default=0,
                   help="Explicit queue-admission control, unchanged default 0")
    p.add_argument("--cpu-list", help="Exactly 2 or 8 explicit logical CPU IDs")
    p.add_argument("--numa-node", type=int)
    p.add_argument("--profile", choices=("smoke", "screen", "qualification"), default="screen")
    p.add_argument("--clients", nargs="+", type=int, choices=(1, 4, 16), default=[4])
    p.add_argument("--layouts", nargs="+", type=int, choices=(2, 3), default=[3])
    p.add_argument("--mappings", nargs="+", choices=("adjacent", "permuted"), default=["adjacent", "permuted"])
    p.add_argument("--ownership", choices=("strided", "shuffled"), default="shuffled")
    p.add_argument("--reads", nargs="+", type=int, choices=(0, 20, 50, 80, 95, 100), default=[80])
    p.add_argument("--rates", nargs="+", type=int, default=[0], help="0 is unpaced screening only")
    p.add_argument("--get-slo-us", type=float)
    p.add_argument("--put-slo-us", type=float)
    p.add_argument("--keys", type=int)
    p.add_argument("--ops-per-client", type=int)
    p.add_argument("--timeout", type=int, default=1800, help="Seconds per benchmark process")
    p.add_argument("--cpu-only", action="store_true", help="Local smoke only; never hardware evidence")
    p.add_argument("--dry-run", action="store_true", help="Print the complete plan; do not create files or run commands")
    return p


def validate(a):
    a.out = a.out.resolve()
    if a.out.exists():
        raise ValueError("Output already exists; choose a fresh directory")
    if a.cpu_only and a.profile != "smoke":
        raise ValueError("--cpu-only is restricted to smoke, not a qualification substitute")
    if a.diagnostic and a.profile == "qualification":
        raise ValueError("--diagnostic is a focused screen, not qualification")
    if not a.cpu_only:
        ids = (a.cpu_list or "").split(",")
        if len(ids) not in (2, 8) or not all(x.isdigit() for x in ids) or len(set(map(int, ids))) != len(ids):
            raise ValueError("Specify exactly 2 or 8 distinct CPU IDs with --cpu-list")
        if a.qpl_root is None or a.numa_node is None or a.numa_node < 0:
            raise ValueError("Specify --qpl-root and a nonnegative --numa-node")
        a.qpl_root = a.qpl_root.resolve()
    if a.keys is None:
        a.keys = 1024 if a.profile == "smoke" else 150000
    if not 32 <= a.keys <= 150000 or a.timeout < 1 or any(x < 0 or x > 100000000 for x in a.rates):
        raise ValueError("Invalid keys, rates or timeout")
    if any(len(x) != len(set(x)) for x in (a.clients, a.layouts, a.mappings, a.reads, a.rates)):
        raise ValueError("Duplicate experiment dimensions would overwrite results")
    if a.ops_per_client is not None and not 1 <= a.ops_per_client <= 100000000:
        raise ValueError("Invalid --ops-per-client")
    if (a.get_slo_us is None) != (a.put_slo_us is None):
        raise ValueError("Supply both GET and PUT SLO limits")
    if a.get_slo_us is not None and not all(math.isfinite(x) and x > 0 for x in (a.get_slo_us, a.put_slo_us)):
        raise ValueError("SLO limits must be finite and positive")
    if a.profile == "qualification" and (0 in a.rates or a.get_slo_us is None):
        raise ValueError("Qualification requires positive --rates and predeclared GET/PUT SLOs")


def operation_count(a, clients, reads):
    default = 256 if a.profile == "smoke" else 100000
    count = a.ops_per_client or default
    if a.profile == "qualification":
        # A single history per stratum is reused at every offered rate. Twenty
        # percent safety margin for the random number of updates; verify actuals.
        write_count = math.ceil(12 * a.keys / ((1 - reads / 100) * clients)) if reads < 100 else 0
        count = max(count, write_count, math.ceil(35 * max(a.rates) / clients))
    if count > 100000000 or count * clients >= 2147483647:
        raise ValueError("Requested history exceeds the benchmark's bounded format")
    return count


def build_plan(a):
    build = a.out / "build"
    steps, matrices = [], []
    def add(name, command, **extra):
        steps.append(dict(name=name, command=[str(x) for x in command], **extra))
    if (ROOT / "PACKAGE_MANIFEST.json").exists():
        add("verify-package", [sys.executable, HERE / "verify_package.py", ROOT])
    configure = ["cmake", "-S", HERE, "-B", build, "-DCMAKE_BUILD_TYPE=Release"]
    if a.cpu_only:
        configure += ["-DAGG_DISABLE_QPL=ON"]
    else:
        configure += ["-DAGG_DISABLE_QPL=OFF", f"-DQPL_INCLUDE_DIR={a.qpl_root / 'include'}",
                      f"-DQPL_LIBRARY={a.qpl_root / ('lib/libqpl.a' if a.qpl_linkage == 'static' else 'lib/libqpl.so')}"]
    add("configure", configure)
    add("build", ["cmake", "--build", build, "-j4"])
    add("correctness", ["ctest", "--test-dir", build, "--output-on-failure", "--no-tests=error"],
        reject_skips=not a.cpu_only, bound=not a.cpu_only)
    if not a.cpu_only:
        add("loaded-libraries", ["ldd", build / f"agg_bench_l{a.layouts[0]}"], check_qpl=True)
        add("cross-format", [build / "agg_qpl_format"], bound=True)
    seeds = [11, 29, 47] if a.profile == "qualification" else [11]
    repetitions = 3 if a.profile == "qualification" else 1
    for clients in a.clients:
        for mapping in a.mappings:
            for reads in a.reads:
                group = f"c{clients}-{mapping}-r{reads}-{a.ownership}"
                for seed in seeds:
                    trace = a.out / "traces" / f"{group}-s{seed}"
                    add(f"trace-{group}-s{seed}", [sys.executable, HERE / "prepare.py", trace,
                        "--corpus", ROOT / "SilesiaCorpus/samba.zip", "--keys", a.keys, "--threads", clients,
                        "--ops", operation_count(a, clients, reads), "--reads", reads, "--seed", seed,
                        "--mapping", mapping, "--ownership", a.ownership])
                    for rate in a.rates:
                        dest = a.out / "matrices" / f"{group}-s{seed}-rate{rate}"
                        cmd = [sys.executable, HERE / "split_matrix.py", trace, dest, "--build", build,
                               "--layouts", *a.layouts, "--repetitions", repetitions, "--arrival-rate", rate,
                               "--timeout", a.timeout, "--backpressure", a.backpressure]
                        policies = CPU_POLICIES if a.cpu_only else (["E-raw", "E-lz4", "A", "D"] if a.diagnostic else POLICIES)
                        cmd += ["--policies", *policies]
                        if not a.cpu_only:
                            cmd += ["--cpu-list", a.cpu_list, "--numa-node", a.numa_node]
                        add(dest.name, cmd, matrix=str(dest), group=group, rate=rate,
                            policies=policies, seed=seed,
                            timeout=a.timeout * 2 * len(a.layouts) * repetitions * len(POLICIES) + 120)
                        matrices.append(steps[-1])
    # Same-byte GET is attribution only. It does not change the mixed-write data
    # or pretend hardware and software produce identical compressed output.
    if not a.cpu_only:
        for clients in a.clients:
            trace = a.out / "traces" / f"read-attribution-c{clients}"
            add(trace.name, [sys.executable, HERE / "prepare.py", trace, "--corpus", ROOT / "SilesiaCorpus/samba.zip",
                "--keys", a.keys, "--threads", clients, "--ops", operation_count(a, clients, 100),
                "--reads", 100, "--mapping", "permuted", "--ownership", a.ownership])
            for rate in a.rates:
                dest = a.out / "matrices" / f"read-attribution-c{clients}-rate{rate}"
                add(dest.name, [sys.executable, HERE / "split_matrix.py", trace, dest, "--build", build,
                    "--suite", "read-attribution", "--layouts", *a.layouts, "--cpu-list", a.cpu_list,
                    "--numa-node", a.numa_node, "--arrival-rate", rate, "--timeout", a.timeout],
                    matrix=str(dest), attribution=True,
                    timeout=a.timeout * 4 * len(a.layouts) + 120)
    return dict(profile=a.profile, steps=steps, hardware_claim="pending", product_gate="pending",
                limitations=["Synthetic arrivals; shuffled ownership shares shards/leaves, not keys",
                             "L2/L3 use synchronous QPL calls inside one background worker",
                             "Device occupancy and private QPL memory are not inferred from software counters",
                             "Same workload/CPU budget, not a fixed-DRAM cache with real misses"])


def child_environment(a):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("BTREE_", "AGG_", "SAMBA_"))}
    env.pop("LD_PRELOAD", None)
    env.pop("DYLD_INSERT_LIBRARIES", None)
    env["SAMBA_ZIP_PATH"] = str(ROOT / "SilesiaCorpus/samba.zip")
    if not a.cpu_only:
        env["QPL_ROOT"] = str(a.qpl_root)
        dirs = [str(a.qpl_root / "lib"), str(a.qpl_root / "lib64")]
        env["LD_LIBRARY_PATH"] = ":".join(dirs + ([env["LD_LIBRARY_PATH"]] if env.get("LD_LIBRARY_PATH") else []))
        env["BTREE_QPL_CROSS_HARDWARE"] = "1"
    return env


def require_corpus():
    if not (ROOT / "SilesiaCorpus/samba.zip").is_file():
        raise ValueError("Missing SilesiaCorpus/samba.zip. From the repository root, run: "
                         "git submodule update --init SilesiaCorpus")


def execute(step, a, env):
    command = list(step["command"])
    if step["name"] == "configure" and not a.cpu_only:
        filename = "libqpl.a" if a.qpl_linkage == "static" else "libqpl.so"
        library = next((a.qpl_root / d / filename for d in ("lib", "lib64") if (a.qpl_root / d / filename).is_file()), None)
        if library is None or not (a.qpl_root / "include/qpl/qpl.h").is_file():
            raise ValueError(f"QPL prefix must contain include/qpl/qpl.h and lib[64]/{filename}")
        a.qpl_library = dict(path=str(library), linkage=a.qpl_linkage,
                             sha256=hashlib.sha256(library.read_bytes()).hexdigest())
        command = [f"-DQPL_LIBRARY={library}" if x.startswith("-DQPL_LIBRARY=") else x for x in command]
    if step.get("bound"):
        command = ["numactl", f"--membind={a.numa_node}", "taskset", "-c", a.cpu_list] + command
    step["executed_command"] = command
    print(step["name"], flush=True)
    path = a.out / "logs" / (step["name"] + ".log")
    started = time.monotonic()
    step["started_unix"] = time.time()
    with path.open("w") as log:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait(timeout=step.get("timeout", max(3600, a.timeout)))
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            step.update(status="interrupted-or-timeout", returncode=process.returncode)
            raise
    step.update(returncode=code, elapsed_seconds=time.monotonic() - started, finished_unix=time.time(),
                log=str(path), status="ok" if code == 0 else "failed")
    if code:
        raise RuntimeError(f"{step['name']} failed ({code}); see {path}")
    if step.get("reject_skips") and any(x in path.read_text() for x in ("***Skipped", "Not Run", "(Disabled)")):
        raise RuntimeError("A required hardware test was skipped; this is not a successful hardware run")
    if step.get("check_qpl"):
        lines = [line for line in path.read_text().splitlines() if "libqpl.so" in line]
        if a.qpl_linkage == "static" and lines:
            raise RuntimeError("Static QPL requested but executable also loads shared QPL")
        if a.qpl_linkage == "shared" and (len(lines) != 1 or str(a.qpl_root) + "/" not in lines[0]):
            raise RuntimeError("ldd did not resolve QPL from the explicitly supplied prefix")
    if step.get("matrix"):
        check_matrix(step, a)


def check_matrix(step, a):
    root = Path(step["matrix"])
    rows = json.loads((root / "rows.json").read_text())
    if step.get("attribution"):
        comparisons = json.loads((root / "comparison.json").read_text())["comparisons"]
        if len(comparisons) != 2 * len(a.layouts) or any(c["read_attribution"] != "same-encoded-bytes-verified" for c in comparisons):
            raise RuntimeError("Same-byte read attribution failed or had no measured encoded reads")
        return
    reps = 3 if a.profile == "qualification" else 1
    expected = {(policy, layout, rep, mode) for policy in step["policies"] for layout in a.layouts
                for rep in range(1, reps + 1) for mode in ("audit", "performance")}
    observed = [(r["policy"], r["matrix_layout"], r["rep"], r.get("mode")) for r in rows]
    if len(observed) != len(expected) or set(observed) != expected or any(r["status"] != "ok" for r in rows):
        raise RuntimeError("Required matrix cases missing, duplicated, failed or unavailable; no hardware success")
    for r in rows:
        hardware = [x for x in r["execution"]["routes"] if x["requested"] == "hardware"]
        if any(x["calls"] != x["completed"] or x.get("failed", 0) for x in hardware):
            raise RuntimeError("Measured strict hardware calls did not all complete successfully")
        # Pure-read B legitimately has no compression work after preload. A
        # configured but idle route is neither an error nor offload evidence.
        if r["policy"] in ("B", "C", "D") and not any(x["completed"] for x in hardware):
            step.setdefault("no_timed_hardware_work", []).append(
                dict(policy=r["policy"], layout=r["matrix_layout"], rep=r["rep"], mode=r["mode"]))


def compare_groups(a, plan, env):
    if a.cpu_only:
        return
    groups = {}
    for step in plan["steps"]:
        if step.get("matrix") and not step.get("attribution"):
            groups.setdefault((step["group"], step["rate"]), []).append(step["matrix"])
    (a.out / "comparisons").mkdir()
    for (group, rate), directories in groups.items():
        for layout in a.layouts:
            for candidate, reference in [("B", "A"), ("B", "D"), ("C", "B"), ("D", "C")]+[
                    (candidate, reference) for candidate in ("B", "D") for reference in CPU_POLICIES[1:]] + [("D", "A")]:
                available = next(s["policies"] for s in plan["steps"] if s.get("group") == group and s.get("rate") == rate)
                if candidate not in available or reference not in available:
                    continue
                name = f"{group}-rate{rate}-L{layout}-{candidate}-vs-{reference}"
                step = dict(name="compare-"+name, command=[sys.executable, str(HERE / "split_compare.py"),
                    str(a.out / "comparisons" / (name+".json")), *directories, "--candidate", candidate,
                    "--reference", reference, "--layout", str(layout), "--reference-layout", str(layout)])
                plan["steps"].append(step)
                execute(step, a, env)
                save(a.out / "run.json", plan)


def summarize(a, plan):
    rows = []
    for step in plan["steps"]:
        if not step.get("matrix"):
            continue
        path = Path(step["matrix"]) / "rows.json"
        if path.exists():
            try:
                loaded = json.loads(path.read_text())
            except ValueError:
                rows.append(dict(status="failed", error="Incomplete matrix JSON", source=str(path)))
                continue
            for r in loaded:
                r.update(experiment=step["name"], source=str(path), attribution=step.get("attribution", False))
                rows.append(r)
    flat = []
    audits = {(r.get("experiment"), r.get("policy"), r.get("layout"), r.get("rep")):r
              for r in rows if r.get("status") == "ok" and r.get("mode") == "audit"}
    for r in rows:
        if r.get("status") != "ok" or r.get("mode") != "performance":
            continue
        p, e, agg = r["product"], r["execution"], r["aggregation"]
        n = p["operations"]
        audit = audits.get((r["experiment"], r["policy"], r["layout"], r["rep"]), {})
        cores = p["cpu_ns"] / p["completed_ns"]
        hardware_completed = sum(x["completed"] for x in e["routes"] if x["requested"] == "hardware")
        flat.append(dict(experiment=r["experiment"], policy=r["policy"], layout=r["layout"],
            repetition=r["rep"], qps=r["completed_ops_per_second"], cpu_ns_per_op=r["cpu_ns_per_op"],
            average_process_cpu_cores=cores,
            cpu_budget_utilization_pct=100*cores/len(a.cpu_list.split(",")) if a.cpu_list else None,
            get_p50_us=p["get_p50_ns"]/1000, get_p99_us=p["get_p99_ns"]/1000,
            get_p999_us=p["get_p999_ns"]/1000, put_p99_us=p["put_p99_ns"]/1000,
            put_p50_us=p["put_p50_ns"]/1000, put_p999_us=p["put_p999_ns"]/1000,
            payload_bytes_per_second=128*r["completed_ops_per_second"],
            drain_ms=max(0, p["completed_ns"]-p["foreground_ns"])/1e6,
            active_hits=agg["active_hits"], pending_hits=agg["pending_hits"], base_hits=agg["base_hits"],
            pending_wait_ns_per_op=agg["pending_wait_ns"]/n, read_lock_wait_ns_per_op=agg["read_lock_wait_ns"]/n,
            software_queue_peak=e["queue_observed_peak"], write_lock_codec_calls=p["write_lock_codec_calls"],
            strict_hardware_completed_calls=hardware_completed,
            hardware_work_observed=hardware_completed > 0,
            process_started_unix=r.get("started_unix"), process_finished_unix=r.get("finished_unix"),
            audited_steady_owned_bytes=audit.get("steady_bytes"), audited_peak_owned_bytes=audit.get("peak_bytes"),
            tracked_steady_capacity=audit.get("steady_capacity"), tracked_peak_capacity=audit.get("peak_capacity"),
            performance_rss_bytes=p.get("rss"), memory_ledger_complete=audit.get("capacity_ledger_complete", False),
            sync_fallbacks=p["sync_fallbacks"], split_fallbacks=p["split_fallbacks"],
            compress_calls_per_op=sum(x["compress"] for x in agg["origins"])/n,
            decompress_calls_per_op=sum(x["decompress"] for x in agg["origins"])/n,
            duration_eligible=r["duration_eligible"], updates_eligible=r["update_eligible"],
            offered_load_eligible=r["offered_load_eligible"], latency_basis=e["latency_basis"]))
    if flat:
        with (a.out / "metrics.csv").open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(flat[0])); writer.writeheader(); writer.writerows(flat)
    save(a.out / "all-rows.json", rows)
    result = dict(run_status=plan["status"], profile=a.profile, product_gate="pending",
        policies={"A":"QPL SW/SW/SW", "B":"IAA/SW/SW", "C":"IAA/IAA/SW", "D":"IAA/IAA/IAA"},
        cases=len(rows), software_only=a.cpu_only, device_occupancy="not collected; software queue is not WQ occupancy",
        memory_gate="pending: reconcile QPL-private resident workspace; RAW reference is restricted to selected layouts",
        best_measured_under_common_slo={}, limitations=plan["limitations"])
    result["completed_paired_comparisons"] = len(list((a.out / "comparisons").glob("*.json")))
    # Do not choose a fast but unstable repetition or quietly pool different
    # concurrency/mapping/ownership strata. All 3x3 samples must meet the SLO.
    if a.profile == "qualification":
        groups = {}
        for step in plan["steps"]:
            if step.get("matrix") and not step.get("attribution"):
                groups.setdefault((step["group"], step["rate"]), []).append(step["matrix"])
        for (group, rate), directories in sorted(groups.items()):
            for layout in a.layouts:
                for policy in POLICIES:
                    samples = [r for r in rows if r["source"] in [str(Path(d)/"rows.json") for d in directories]
                               and r.get("mode") == "performance" and r["policy"] == policy and r["layout"] == layout]
                    good = len(samples) == 9 and all(r.get("status") == "ok" and r["duration_eligible"] and
                        r["update_eligible"] and r["offered_load_eligible"] and
                        r["product"]["get_p99_ns"] <= a.get_slo_us*1000 and r["product"]["put_p99_ns"] <= a.put_slo_us*1000 and
                        r["product"]["completed_ns"] <= 1.05*r["product"]["operations"]*1e9/rate for r in samples)
                    if good:
                        result["best_measured_under_common_slo"][f"{group}-L{layout}-{policy}"] = rate
    save(a.out / "summary.json", result)
    lines = ["# Intel CPU / IAA experiment", "", f"Run status: **{plan['status']}**. Product acceptance: **pending**.", "",
             "A = QPL CPU; B = IAA compression + CPU GET/maintenance; C = IAA compression/GET + CPU maintenance; D = all IAA.",
             "CPU comparators: RAW, LZ4, Zstd level 1 and zlib level 1. Compare B with A/D and every eligible CPU comparator, not just the weakest.", "",
             "## Interpretation", "", *["- " + s for s in plan["limitations"]],
             "- Same-byte GET attribution and mixed writes are separate experiments.",
             "- Unpaced/smoke/screen rows cannot establish sustained QPS or product qualification.",
             "- metrics.csv is performance-only; all-rows.json retains audit memory, RSS, origins and failures.",
             "- QPL call time combines submission/wait/service; it is not isolated device latency.",
             "- Actual WQ occupancy/IAA activity requires Intel's device telemetry; unmeasured is not zero.", "",
             "- Configured hardware routes with zero measured work (for example pure-read B) are not offload evidence.",
             "- comparisons/ contains paired B-vs-A/D/CPU and C-vs-B, D-vs-C results. Screen intervals are not qualification.",
             "- Choose the best CPU reference at the same load/SLO; no CPU codec is assumed to be the winner.", "",
             "## Measured performance", "", "| Case | QPS incl. drain | CPU ns/op | GET p99 us | PUT p99 us |",
             "|---|---:|---:|---:|---:|"]
    for r in flat:
        lines.append(f"| {r['experiment']} / L{r['layout']} {r['policy']} rep {r['repetition']} | {r['qps']:.0f} | {r['cpu_ns_per_op']:.1f} | {r['get_p99_us']:.3f} | {r['put_p99_us']:.3f} |")
    (a.out / "SUMMARY.md").write_text("\n".join(lines) + "\n")
    write_summary(a, plan, rows, flat)


def archive_results(a):
    # Logs/traces and source snapshots are recoverable; build products and huge
    # duplicate .bases images stay local. Their hashes remain in matrix records.
    hashes = {}
    with tarfile.open(a.out / "intel-results.tar.gz", "x:gz") as tar:
        for f in sorted(a.out.rglob("*")):
            name = f.relative_to(a.out)
            if not f.is_file() or f.is_symlink() or name.parts[0] == "build" or f.name == "intel-results.tar.gz" or f.suffix == ".bases":
                continue
            hashes[str(name)] = hashlib.sha256(f.read_bytes()).hexdigest()
            tar.add(f, arcname=str(name), recursive=False)
        data = (json.dumps(hashes, indent=2) + "\n").encode()
        info = tarfile.TarInfo("RESULT_CHECKSUMS.json"); info.size = len(data)
        tar.addfile(info, io.BytesIO(data))


def main():
    p = parser(); a = p.parse_args()
    try:
        validate(a); plan = build_plan(a)
    except ValueError as e:
        p.error(str(e))
    if a.dry_run:
        print(json.dumps(plan, indent=2)); return
    a.out.mkdir(parents=True); (a.out / "logs").mkdir()
    revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, capture_output=True)
    dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=no"], cwd=ROOT, text=True, capture_output=True)
    plan.update(status="running", started_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                source_commit=revision.stdout.strip() or "unavailable", tracked_changes=dirty.stdout.strip(),
                invocation=sys.argv)
    save(a.out / "run.json", plan)
    env = child_environment(a)
    try:
        require_corpus()
        if not a.cpu_only:
            if platform.system() != "Linux" or platform.machine() not in ("x86_64", "amd64"):
                raise ValueError("Real QPL/IAA requires this runner's Linux x86-64 path; Mac can only run --cpu-only --profile smoke")
            ids = set(map(int, a.cpu_list.split(",")))
            if not ids <= os.sched_getaffinity(0):
                raise ValueError("CPU IDs are outside the process's permitted affinity")
            preload = Path("/etc/ld.so.preload")
            if preload.exists() and any(line.split("#", 1)[0].strip() for line in preload.read_text().splitlines()):
                raise ValueError("System-wide LD_PRELOAD is active; ask the operator for a controlled host")
        for step in plan["steps"]:
            execute(step, a, env); save(a.out / "run.json", plan)
        compare_groups(a, plan, env)
        plan["status"] = "completed-software-smoke" if a.cpu_only else "completed-measurements-not-product-acceptance"
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired, KeyboardInterrupt) as e:
        plan.update(status="failed", error=str(e)); print(str(e), file=sys.stderr)
    finally:
        save(a.out / "run.json", plan)
        summarize(a, plan)
        archive_results(a)
    print(f"Email or paste: {a.out / 'summary.txt'}\nOptional archive: {a.out / 'intel-results.tar.gz'}")
    if plan["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
