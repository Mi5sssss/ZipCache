#!/usr/bin/env python3
"""Causal opportunity accounting over diagnostic events; never a QPS model."""
import argparse, collections, csv, hashlib, json, os, subprocess
from pathlib import Path

ROOT=Path(__file__).resolve().parent
BASE=ROOT.parent/'hot-leaf-v2-20261101'
KINDS=set('GHABNW')

def parse_log(path):
    result={};work={}
    for line in path.read_text().splitlines():
        try:
            key,body=line.split(' ',1);value=json.loads(body)
        except (ValueError,json.JSONDecodeError):continue
        if key=='CODEC_WORK':work[value['role']]=value
        else:result[key]=value
    result['CODEC_WORK']=work;return result

def analyze(path, window=100000, allowance=1024*1024):
    bins=[];current=None;states={};last=0;observed=collections.Counter()
    with path.open() as f:
        for event in csv.DictReader(f):
            e={k:(v if k in ('kind','leaf') else int(v)) for k,v in event.items()}
            assert e['sequence']==last+1;last=e['sequence'];observed[e['kind']]+=1
            ix=max(0,(e['requests']-1)//window)
            while len(bins)<=ix:
                bins.append(dict(gets=collections.Counter(),reads=0,writes=0,
                                 jobs=collections.Counter(),rebuilds=collections.Counter(),
                                 end={},begin=dict(states)))
            b=bins[ix];leaf=e['leaf'];kind=e['kind']
            if kind in KINDS:
                states[leaf]=e;b['end'][leaf]=e
                if kind=='W':b['writes']+=1
                else:b['reads']+=1
            if kind=='G':b['gets'][leaf]+=1
            if kind in ('D','E'):b['jobs'][leaf]+=1
            if kind=='M':b['rebuilds'][leaf]+=1
    windows=[];state={}
    for i in range(len(bins)-1):
        train,test=bins[i:i+2];state.update(train['end'])
        choices=[]
        for leaf,hits in train['gets'].items():
            e=state[leaf]
            if e['hot'] or not e['base_usable']:continue
            cost=max(1,e['raw_usable']-e['base_usable']) # no free credit for mutable deltas
            choices.append((hits/cost,leaf,cost,e))
        # Ties do not use validation hits. IDs are diagnostic identities only.
        choices.sort(key=lambda x:(-x[0],x[1]))
        selected={};used=0;conversion=0;raw_peak=0
        for _,leaf,cost,e in choices:
            if used+cost>allowance:continue
            selected[leaf]=cost;used+=cost;conversion+=e['encoded_blocks']
            raw_peak=max(raw_peak,e['raw_usable'])
        avoided=sum(test['gets'][k] for k in selected)
        total=sum(test['gets'].values())
        windows.append(dict(train_window=i,test_window=i+1,selected_leaves=len(selected),
            retained_increment_bytes=used,one_conversion_destination_bytes=raw_peak,
            four_running_destinations_bytes=4*raw_peak,
            queued_plus_running_destinations_bound_bytes=36*raw_peak,
            training_cold_gets=sum(train['gets'][k] for k in selected),
            heldout_cold_gets=total,heldout_avoidable_cold_gets=avoided,
            heldout_avoidable_fraction=avoided/total if total else 0,
            initial_conversion_decode_jobs=conversion,
            heldout_existing_maintenance_jobs=sum(test['jobs'][k] for k in selected),
            heldout_existing_rebuilds=sum(test['rebuilds'][k] for k in selected),
            selected_leaf_ids=sorted(selected)))
    avoided=sum(x['heldout_avoidable_cold_gets'] for x in windows)
    total=sum(x['heldout_cold_gets'] for x in windows)
    conversion=sum(x['initial_conversion_decode_jobs'] for x in windows)
    return dict(schema=1,evidence='causal-counterfactual-work-opportunity-not-performance',
        selection='previous-window cold GET count / additional retained allocator-usable byte',
        window_requests=window,incremental_allowance_bytes=allowance,
        observed=dict(observed),heldout_avoidable_fraction=avoided/total if total else 0,
        heldout_avoidable_cold_gets=avoided,heldout_cold_gets=total,
        conversion_decode_jobs=conversion,conversion_to_avoided=conversion/avoided if avoided else None,
        gate_pass=total>0 and avoided/total>=.10 and conversion<avoided*.5,
        boundaries=['No latency, CPU saving, hardware ratio or memory-capacity prediction.',
          'Identities valid only without split/delete/rebalance in this fixed update trace.',
          'Current cold selections only: does not invent reads already served by RAW/delta.',
          'Representation sizes vary after selection. Listed cost is decision-time estimate.',
          'Transition bound includes 32 queued plus 4 running full RAW destinations, not just executing slots.',
          'Promotion costs count one decode of every encoded old block; repeated selections are charged again.',
          'No claim of optimal knapsack packing, executable policy or globally enforced memory cap.'],
        windows=windows)

def run(mapping,output=None,build=None):
    out=Path(output) if output else ROOT/'diagnostics-replay'/mapping;out=out.resolve();out.mkdir(parents=True,exist_ok=False)
    cfg={'env':json.loads((ROOT/'base-env.json').read_text())};env={k:v for k,v in os.environ.items()
                                     if not k.startswith(('BTREE_','HOT_','AGG_','SD_'))}
    env.update(cfg['env']);env.update(SAMBA_ZIP_PATH=str(ROOT/'data/samba.zip'),BTREE_HOT_WORK_SIGNAL='0',BTREE_HOT_TOTAL_BYTES='0',HOT_DIAGNOSTIC_FILE=str(out/'events.csv'),
        BTREE_VERIFY_CRC='1',AGG_VERIFY_EACH='1',BTREE_MEMORY_AUDIT='1')
    trace=ROOT/'traces'/f'trace-11-{mapping}'/'trace.bin'
    command=[str((Path(build) if build else ROOT/'build').resolve()/'hot_bench'),'lz4',str(trace)]
    (out/'command.json').write_text(json.dumps(dict(command=command,env={k:v for k,v in env.items()
        if k.startswith(('BTREE_','HOT_','AGG_','SD_','SAMBA_'))}),indent=2)+'\n')
    with (out/'run.log').open('w') as f:
        subprocess.run(command,env=env,stdout=f,stderr=subprocess.STDOUT,check=True,timeout=600)
    data=parse_log(out/'run.log');p=data['PRODUCT'];assert p['mismatches']==0
    assert not p['split_fallbacks'] and not p['failed_tasks']
    result=analyze(out/'events.csv');obs=result['observed'];work=data['CODEC_WORK']
    assert sum(obs.get(k,0) for k in KINDS)==p['operations']
    assert obs.get('G',0)==work['get_decompress']['calls']
    assert obs.get('D',0)==work['maintenance_decompress']['calls']
    assert obs.get('E',0)==work['compress']['calls']
    assert obs.get('M',0)==data['AGGREGATION']['merge_count']
    result.update(mapping=mapping,trace_sha256=hashlib.sha256(trace.read_bytes()).hexdigest(),
                  reconciliation='exact',diagnostic_timing_excluded=True)
    (out/'analysis.json').write_text(json.dumps(result,indent=2)+'\n')
    print(mapping,{k:result[k] for k in ('heldout_avoidable_fraction','conversion_to_avoided','gate_pass')},flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('mapping',choices=['clustered','dispersed']);p.add_argument('--out',type=Path);p.add_argument('--build',type=Path);a=p.parse_args();run(a.mapping,a.out,a.build)
