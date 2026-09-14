#!/usr/bin/env python3
"""Fixed-trace route ablation, serial processes, no device setup or auto tuning."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
from run import digest

POLICIES = {"A": ("software",)*3, "B": ("hardware","software","software"),
            "C": ("hardware","hardware","software"), "D": ("hardware",)*3}

def main():
    p=argparse.ArgumentParser()
    p.add_argument("trace",type=Path);p.add_argument("out",type=Path)
    p.add_argument("--build",type=Path,default=Path(__file__).parent/"work/split-build")
    p.add_argument("--suite",choices=("routes","read-attribution","accel"),default="routes")
    p.add_argument("--layouts",nargs="+",type=int,choices=(2,3),default=[3,2])
    p.add_argument("--repetitions",type=int,default=1)
    p.add_argument("--arrival-rate",type=int,default=0)
    p.add_argument("--cpu-list");p.add_argument("--numa-node",type=int)
    p.add_argument("--accel-library",type=Path)
    p.add_argument("--timeout",type=int,default=3600)
    p.add_argument("--policies",nargs="+",choices=["E-raw","E-lz4","E-zstd","E-zlib",*POLICIES])
    p.add_argument("--dry-run",action="store_true")
    a=p.parse_args();m=json.loads((a.trace/"manifest.json").read_text())
    if a.repetitions<1 or a.repetitions>9 or a.arrival_rate<0:p.error("invalid bounded repetition/rate")
    if a.policies and a.suite!="routes":p.error("--policies applies only to the routes suite")
    if a.suite=="read-attribution" and (m["read_pct"]!=100 or m["batch"]!=1):p.error("read attribution requires pure point reads")
    cases=[]
    if a.suite=="routes":
        cases=[("E-"+c,c,None,None) for c in ("raw","lz4","zstd","zlib")]
        cases += [(k,"qpl",list(v),None) for k,v in POLICIES.items()]
        if a.policies:cases=[case for case in cases if case[0] in a.policies]
    elif a.suite=="read-attribution":
        cases=[("get-"+s,"qpl",["software",s,"software"],None) for s in ("software","hardware")]
    else:
        cases=[(f"accel-{c}{d}","zlib-accel",None,(c,d)) for c,d in ((0,0),(1,0),(0,1),(1,1))]
    commands=[]
    for rep in range(a.repetitions):
        # Alternate both layout and policy order, never run competitors concurrently.
        ordered=[(layout,case) for layout in a.layouts for case in cases]
        if rep%2:ordered.reverse()
        for layout,(name,codec,routes,accel) in ordered:
            dest=a.out/f"rep-{rep+1}-l{layout}-{name}"
            cmd=[sys.executable,str(Path(__file__).with_name("run.py")),str(a.trace.resolve()),str(dest.resolve()),
                 "--build",str(a.build.resolve()),"--layouts",str(layout),"--codecs",codec,"--performance",
                 "--arrival-rate",str(a.arrival_rate),"--timeout",str(a.timeout)]
            if routes:cmd += ["--routes",*routes]
            if a.cpu_list:cmd += ["--cpu-list",a.cpu_list]
            if a.numa_node is not None:cmd += ["--numa-node",str(a.numa_node)]
            if a.suite=="read-attribution":cmd += ["--bg","0","--snapshot"]
            if accel:
                cmd += ["--accel-compress",str(accel[0]),"--accel-decompress",str(accel[1])]
                if a.accel_library:cmd += ["--accel-library",str(a.accel_library.resolve())]
            commands.append({"rep":rep+1,"layout":layout,"policy":name,"directory":str(dest.resolve()),"command":cmd})
    if a.dry_run:
        print(json.dumps(commands,indent=2));return
    a.out.mkdir(parents=True,exist_ok=False)
    manifest={"suite":a.suite,"trace":m,"trace_sha256":digest(a.trace/"trace.bin"),
              "commands":commands,"claim":"synthetic small-KV history; no hardware-performance simulator",
              "hardware_gate":"pending","product_gate":"pending"}
    (a.out/"matrix.json").write_text(json.dumps(manifest,indent=2)+"\n")
    rows=[];failed=False
    for item in commands:
        proc=subprocess.run(item["command"],text=True,capture_output=True)
        directory=Path(item["directory"])
        (a.out/(directory.name+".driver.log")).write_text(proc.stdout+proc.stderr)
        path=directory/"results.json"
        result=json.loads(path.read_text()) if path.exists() else [{"status":"failed","error":proc.stderr}]
        for row in result:row.update(policy=item["policy"],rep=item["rep"],matrix_layout=item["layout"])
        rows.extend(result);failed |= proc.returncode!=0
        (a.out/"rows.json").write_text(json.dumps(rows,indent=2)+"\n")
        print(directory.name,proc.returncode,flush=True)
    comparisons=[]
    for rep in range(1,a.repetitions+1):
        for layout in a.layouts:
            group=[r for r in rows if r["rep"]==rep and r["matrix_layout"]==layout]
            if a.suite=="read-attribution":
                for mode in ("audit","performance"):
                    pair=[r for r in group if r.get("mode")==mode and r["status"]=="ok"]
                    status="pending"
                    if len(pair)==2:
                        if pair[0]["base_image_sha256"]!=pair[1]["base_image_sha256"]:
                            status="failed-different-encoded-bytes";failed=True
                        elif not all(r["execution"]["routes"][1]["completed"] for r in pair):status="inconclusive-no-encoded-reads"
                        else:status="same-encoded-bytes-verified"
                    comparisons.append({"rep":rep,"layout":layout,"mode":mode,"read_attribution":status})
    # One actual RAW configuration supplies both denominators at this CPU/trace budget.
    for rep in range(1,a.repetitions+1):
        raw=[r for r in rows if r["rep"]==rep and r.get("codec")=="raw" and r.get("mode")=="audit" and r["status"]=="ok"]
        if len(raw)!=len(a.layouts):continue
        best=min(raw,key=lambda r:r["steady_bytes"])
        for row in rows:
            if row["rep"]==rep and row.get("mode")=="audit" and row["status"]=="ok":
                row.update(raw_reference=f"L{best['layout']}-RAW",steady_capacity=best["steady_bytes"]/row["steady_bytes"],
                           peak_capacity=best["peak_bytes"]/row["peak_bytes"])
                row["capacity_screen"] = ("pass" if min(row["steady_capacity"],row["peak_capacity"])>=1.25 else "fail") if row.get("capacity_ledger_complete") else "pending-private-workspace-reconciliation"
    (a.out/"rows.json").write_text(json.dumps(rows,indent=2)+"\n")
    (a.out/"comparison.json").write_text(json.dumps({"comparisons":comparisons,"product_gate":"pending",
        "reason":"requires matched CPU/SLO, complete hardware memory ledger and three-seed repeated qualification"},indent=2)+"\n")
    if failed:raise SystemExit("matrix contains failures; inspect preserved logs")

if __name__=="__main__":main()
