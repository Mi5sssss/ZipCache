#!/usr/bin/env python3
"""Explicit offered-load grid under predeclared SLOs; no device auto tuning.
Reports maximum measured stable rate, not an interpolated device limit.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys

p=argparse.ArgumentParser()
p.add_argument("trace",type=Path);p.add_argument("out",type=Path)
p.add_argument("--build",type=Path,required=True)
p.add_argument("--rates",nargs="+",type=int,required=True)
p.add_argument("--get-slo-us",type=float,required=True)
p.add_argument("--put-slo-us",type=float,required=True)
p.add_argument("--cpu-list",required=True)
p.add_argument("--numa-node",type=int,required=True)
p.add_argument("--repetitions",type=int,default=3)
p.add_argument("--layouts",nargs="+",choices=(2,3),type=int,default=[3,2])
a=p.parse_args()
if min(a.rates)<=0 or a.get_slo_us<=0 or a.put_slo_us<=0:p.error("positive rates and limits required")
ids=a.cpu_list.split(",")
if len(ids) not in (2,8) or len(set(ids))!=len(ids) or not all(x.isdigit() for x in ids):
    p.error("give exactly 2 or 8 distinct individual CPU IDs, not a range")
a.out.mkdir(parents=True,exist_ok=False)
report={"limits_us":{"get":a.get_slo_us,"put":a.put_slo_us},"runs":[],"best_measured":{},"product_gate":"pending"}
for rate in sorted(set(a.rates)):
    dest=a.out/f"rate-{rate}"
    cmd=[sys.executable,str(Path(__file__).with_name("split_matrix.py")),str(a.trace),str(dest),
         "--build",str(a.build),"--layouts",*[str(x) for x in a.layouts],"--repetitions",str(a.repetitions),
         "--cpu-list",a.cpu_list,"--numa-node",str(a.numa_node),"--arrival-rate",str(rate)]
    result=subprocess.run(cmd)
    report["runs"].append({"rate":rate,"command":cmd,"returncode":result.returncode})
    path=dest/"rows.json"
    if path.exists():
        rows=json.loads(path.read_text())
        for layout in a.layouts:
            for policy in sorted({r["policy"] for r in rows}):
                samples=[r for r in rows if r["matrix_layout"]==layout and r["policy"]==policy and r.get("mode")=="performance"]
                eligible=len(samples)==a.repetitions and all(r["status"]=="ok" and r["duration_eligible"] and
                    r["update_eligible"] and r["offered_load_eligible"] and
                    r["product"]["get_p99_ns"]<=a.get_slo_us*1000 and r["product"]["put_p99_ns"]<=a.put_slo_us*1000 and
                    r["product"]["completed_ns"]<=1.05*r["product"]["operations"]*1e9/rate for r in samples)
                if eligible:
                    report["best_measured"][f"L{layout}-{policy}"]={"offered_rate":rate,
                        "minimum_completed_qps":min(r["completed_ops_per_second"] for r in samples),"source":str(path)}
    (a.out/"slo-sweep.json").write_text(json.dumps(report,indent=2)+"\n")
if any(r["returncode"] for r in report["runs"]):raise SystemExit("Failures retained; inspect slo-sweep.json")
