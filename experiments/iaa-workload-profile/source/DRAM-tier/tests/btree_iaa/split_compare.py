#!/usr/bin/env python3
"""Paired fixed-load comparison. Statistics are not an IAA performance model."""
import argparse
import json
import math
from pathlib import Path
import random

def interval(values):
    if not values:return None
    groups={}
    for seed,value in values:
        if value<=0 or not math.isfinite(value):raise ValueError("invalid ratio")
        groups.setdefault(seed,[]).append(math.log(value))
    rng=random.Random(11);seeds=sorted(groups);samples=[]
    for _ in range(4000):
        logs=[]
        for seed in rng.choices(seeds,k=len(seeds)):
            logs.extend(rng.choices(groups[seed],k=len(groups[seed])))
        samples.append(math.exp(sum(logs)/len(logs)))
    samples.sort()
    return {"estimate":math.exp(sum(math.log(v) for _,v in values)/len(values)),
            "low":samples[99],"high":samples[3899],"pairs":len(values),"seeds":seeds}

def verdict(cpu,qps,get,put):
    tails=[x for x in (get,put) if x]
    if any(x["low"]>1.10 for x in tails) or (cpu["low"]>0.80 and qps["high"]<1.15):return "fail"
    if all(x["high"]<=1.10 for x in tails) and (cpu["high"]<=0.80 or qps["low"]>=1.15):return "pass"
    return "inconclusive"

def main():
    p=argparse.ArgumentParser();p.add_argument("out",type=Path);p.add_argument("matrices",nargs="+",type=Path)
    p.add_argument("--candidate",default="B");p.add_argument("--reference",default="A")
    p.add_argument("--layout",type=int,choices=(2,3),default=3)
    p.add_argument("--reference-layout",type=int,choices=(2,3),default=3)
    a=p.parse_args();groups={};audits={};strata=set();binaries={}
    for directory in a.matrices:
        matrix=json.loads((directory/"matrix.json").read_text());trace=matrix["trace"]
        for r in json.loads((directory/"rows.json").read_text()):
            role="candidate" if (r["policy"],r["matrix_layout"])==(a.candidate,a.layout) else "reference" if (r["policy"],r["matrix_layout"])==(a.reference,a.reference_layout) else None
            if not role:continue
            key=(matrix["trace_sha256"],r["rep"],r.get("cpu_list"),r.get("numa_node"),r.get("execution",{}).get("arrival_rate"))
            if r["status"]=="ok":
                strata.add(tuple(trace[x] for x in ("keys","threads","read_pct","mapping","hot","sparse","batch"))+(trace.get("ownership","strided"),)+key[2:]+(r.get("host"),))
                binaries.setdefault(role,set()).add((r["binary_sha256"],r["product"]["library_version"]))
            target=groups if r.get("mode")=="performance" else audits
            if role in target.setdefault(key,{}):raise ValueError("ambiguous duplicate pair")
            target[key][role]=r
    if len(strata)>1:raise ValueError("do not pool workloads, budgets or arrival rates")
    if any(len(v)>1 for v in binaries.values()):raise ValueError("source binary/library changed within a paired series")
    ratios={k:[] for k in ("cpu","qps","get","put")};reasons=[];memory=True;hardware=True;receipts=[]
    for key,pair in groups.items():
        if set(pair)!={"candidate","reference"} or any(r["status"]!="ok" for r in pair.values()):
            reasons.append("missing/failed/unavailable paired run");continue
        c,b=pair["candidate"],pair["reference"];seed=c["product"]["seed"]
        if c["product"]["trace_hash"]!=b["product"]["trace_hash"]:raise ValueError("different history")
        for r in (c,b):
            if not (r["duration_eligible"] and r["update_eligible"] and r["offered_load_eligible"]):reasons.append("duration/write-volume/offered-load unqualified")
            if not r.get("cpu_list") or len(r["cpu_list"].split(",")) not in (2,8):reasons.append("unverified CPU budget")
        ratios["cpu"].append((seed,c["cpu_ns_per_op"]/b["cpu_ns_per_op"]))
        ratios["qps"].append((seed,c["completed_ops_per_second"]/b["completed_ops_per_second"]))
        for label in ("get","put"):
            denom=b["product"][label+"_p99_ns"]
            if denom:ratios[label].append((seed,c["product"][label+"_p99_ns"]/denom))
        audit=audits.get(key,{})
        if len(audit)!=2 or any(not r.get("capacity_ledger_complete") for r in audit.values()):memory=False
        elif any(min(r.get("steady_capacity",0),r.get("peak_capacity",0))<1.25 for r in audit.values()):reasons.append("capacity gate failed")
        hardware &= any(r["requested"]=="hardware" and r["completed"]>0 for r in c["execution"]["routes"])
        receipts.append({"trace":key[0],"rep":key[1],"candidate_binary":c["binary_sha256"],"reference_binary":b["binary_sha256"]})
    if any(sum(s==seed for s,_ in ratios["cpu"])<3 for seed in (11,29,47)):reasons.append("need three repetitions of each seed 11/29/47")
    estimates={k:interval(v) for k,v in ratios.items()}
    statistical=verdict(**estimates) if estimates["cpu"] and estimates["qps"] else "pending"
    result={"candidate":a.candidate,"reference":a.reference,"layout":a.layout,"reference_layout":a.reference_layout,
        "ratio_intervals":estimates,"statistical_screen":statistical,"eligibility_reasons":sorted(set(reasons)),
        "comparison_gate":statistical if not reasons else "pending","hardware_observed":bool(receipts) and hardware,
        "memory_reconciled":bool(receipts) and memory,
        "joint_gate":statistical if receipts and not reasons and hardware and memory else "pending",
        "method":"paired geometric ratios; 4000 seed/repetition hierarchical bootstrap samples; percentile 95% interval",
        "warning":"Does not select the best CPU baseline or replace the common-SLO throughput sweep.","receipts":receipts}
    if a.out.exists():raise ValueError("refusing to overwrite comparison")
    a.out.write_text(json.dumps(result,indent=2)+"\n");print(json.dumps(result,indent=2))

if __name__=="__main__":main()
