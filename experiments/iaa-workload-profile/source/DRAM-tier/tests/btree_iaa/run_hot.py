#!/usr/bin/env python3
"""Local, reviewable whole-hot-leaf experiment. Never changes WQ/system config."""
import argparse,hashlib,json,os,platform,shutil,subprocess,sys,time
from pathlib import Path
from prepare_zipf import prepare
from hot_metrics import attach_inputs, enrich, fmt
HERE=Path(__file__).resolve().parent
SOURCE=HERE.parents[2]

def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def records(text):
    result={}
    for line in text.splitlines():
        if ' {' in line:
            k,j=line.split(' ',1)
            try:v=json.loads(j)
            except ValueError:continue
            if k=='CODEC_WORK':result.setdefault(k,[]).append(v)
            else:result[k]=v
    return result

def summary(out,runs):
    for run in runs:
        attach_inputs(out, run)
    enrich(runs)
    lines=['Whole-hot-leaf ZipCache evaluation','Finite Zipf access, original samba 128B slices, no zero padding.',
      'Surrogate results are mechanism observations, NOT IAA predictions.',
      'Performance CRC OFF; exact-byte audit runs use CRC ON. All final values checked.',
      'RAW quota is a representation allowance, not a total-process memory cap.',
      'Separate audit runs include allocator peak; QPL private memory remains unverified.',
      'Global finite-Zipf request multiset. Fixed key ownership can imbalance clients; inspect manifests.',
      '', 'codec policy mode mapping seed QPS CPU_ns/op GET_p99_us PUT_p99_us GET_codec/request raw_GET_fraction compactions jobs/PUT resident_MiB audit_peak_MiB']
    for run in runs:
        p=run.get('PRODUCT',{});h=run.get('HOT',{});a=run.get('AGGREGATION',{});tag=run['case']
        if p.get('status')!='ok':lines.append(f"{tag}: {run['status']}");continue
        o=a.get('origins',[{},{}]);n=p['operations'];writes=p['writes'];reads=p['reads']
        jobs=sum(x.get('compress',0)+x.get('decompress',0) for x in o[1:])
        lines.append(f"{run['codec']} {run['policy']} {run['mode']} {run['mapping']} {run['seed']} {n*1e9/p['completed_ns']:.0f} {p['cpu_ns']/n:.0f} {p['get_p99_ns']/1000:.2f} {p['put_p99_ns']/1000:.2f} {o[0].get('decompress',0)/max(1,reads):.4f} {h.get('raw_gets',0)/max(1,reads):.4f} {a.get('merge_count',0)} {jobs/max(1,writes):.4f} {p['resident_usable']/2**20:.3f} {p['audit_peak_usable']/2**20 if p['audit'] else float('nan'):.3f}")
    lines += ['', 'COMPRESSION AND TOTAL OWNED MEMORY (schema 1)',
      'Cold data ratio = current valid cold record bytes / stored block payload bytes, including RAW bypass.',
      'Framed ratio also includes block headers. Neither ratio includes hot RAW leaves or metadata.',
      'Memory saved = 100 * (1 - total owned / matched packed-RAW total owned). Negative means MORE memory.',
      'Total owned includes cache, metadata, hot RAW, deltas, pending, allocator usable bytes, codec/scheduler workspaces and execution stacks.',
      'Retained is after final drain. Peak is measured in audit runs including drain, excluding preload.',
      'Fixture/trace/timing arrays/auditor/shared libraries/OS runtime excluded; RSS is separate in JSON.',
      'Choose the smaller measured RAW on/off candidate for each retained/peak metric. Exact inputs/settings must match.',
      'Unverified QPL-private memory or missing RAW/peak => unavailable, never a capacity pass.',
      'case | cold data x | cold framed x | total retained MiB | total peak MiB | retained saved % | peak saved % | capacity status']
    for run in runs:
        m=run['CAPACITY_METRICS']
        lines.append(' | '.join([run['case'],fmt(m['cold_data_ratio']),fmt(m['cold_framed_ratio']),
          fmt(m['total_owned_retained_bytes']/2**20 if m['total_owned_retained_bytes'] is not None else None),
          fmt(m['total_owned_peak_bytes']/2**20 if m['total_owned_peak_bytes'] is not None else None),
          fmt(m['memory_saved_retained_pct'],2),fmt(m['memory_saved_peak_pct'],2),m['capacity_gate']]))
        lines.append('  RAW retained='+str(m['raw_reference_retained_case'])+'; peak='+str(m['raw_reference_peak_case'])+'; '+m['reason'])
    lines += ['', 'SURROGATE V2 ONLY: observed simulation, not hardware performance',
      'Device CPU is the timed-window delta, not lifetime CPU. Host remainder is NOT predicted IAA CPU.',
      'Slots are model execution slots. They are not physical IAA devices or work queues.',
      'COPY input sizes are uncompressed. No compressed I/O, DMA, NUMA or hardware bandwidth is modeled.',
      'case | configured service us/job | completion overshoot us/job | queue us/job | peak active | average active | process CPU ns/op | device CPU ns/op | host remainder ns/op']
    for run in runs:
        if run.get('codec')!='surrogate' or run.get('status')!='ok':continue
        d=run['DEVICE'];p=run['PRODUCT'];n=p['operations'];jobs=max(1,d['accepted'])
        lines.append(' | '.join([run['case'],fmt(d['configured_service_ns']/jobs/1000),fmt(d['late_ns']/jobs/1000),
          fmt(d['queue_ns']/jobs/1000),str(d['peak_active']),fmt(d['active_slot_ns']/p['completed_ns']),
          fmt(p['cpu_ns']/n),fmt(d['device_cpu_ns']/n),fmt((p['cpu_ns']-d['device_cpu_ns'])/n)]))
    (out/'summary.txt').write_text('\n'.join(lines)+'\n');(out/'results.json').write_text(json.dumps(runs,indent=2,allow_nan=False)+'\n')

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['check','screen','intel']);p.add_argument('--out',type=Path,required=True);p.add_argument('--corpus',type=Path)
    p.add_argument('--traces',type=Path,help='Replay frozen trace directories after verifying hashes/configuration');p.add_argument('--build',type=Path);p.add_argument('--keys',type=int,default=150000);p.add_argument('--operations',type=int,default=1000000);p.add_argument('--clients',type=int,default=4)
    p.add_argument('--seeds',default='11,29,47');p.add_argument('--codecs',default='lz4,zlib');p.add_argument('--mappings',default='clustered,dispersed');p.add_argument('--payload-mapping',choices=['adjacent','permuted'],default='adjacent');p.add_argument('--alpha',type=float,default=1.1);p.add_argument('--reads',type=int,default=80);p.add_argument('--repeats',type=int,default=1);p.add_argument('--audit',action='store_true');p.add_argument('--device-us',type=int,default=100);p.add_argument('--surrogate-profile',choices=['v2-reference','fixed'],default='v2-reference');p.add_argument('--service-scale',type=float,default=1.0);p.add_argument('--device-slots',type=int,default=32);p.add_argument('--cpu-list',help='Intel Linux: explicit CPU affinity, e.g. 0,1,2,3');p.add_argument('--hot-fraction',type=float,default=.25)
    a=p.parse_args();out=a.out.resolve();out.mkdir(parents=True,exist_ok=False)
    if not 0<=a.hot_fraction<=1:p.error('hot fraction outside 0..1')
    if not 0<a.service_scale<=100 or not 1<=a.device_slots<=128:p.error('invalid device scale/slots')
    files={str(f.relative_to(SOURCE)):digest(f) for f in SOURCE.rglob('*') if f.is_file() and f.suffix in ['.c','.h','.inc','.py','.txt']}
    (out/'source-sha256.json').write_text(json.dumps(files,indent=2)+'\n')
    build=a.build.resolve() if a.build else out/'build';cmake=shutil.which('cmake') or '/opt/homebrew/bin/cmake'
    commands=[[cmake,'-S',str(HERE),'-B',str(build),'-DCMAKE_BUILD_TYPE=Release','-DAGG_DISABLE_QPL='+('OFF' if a.action=='intel' else 'ON')], [cmake,'--build',str(build),'--target','hot_bench','hot_contract','-j4']]
    with (out/'build.log').open('w') as log:
        for cmd in commands:subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT,check=True)
    machine={'platform':platform.platform(),'processor':platform.processor(),'python':sys.version,'argv':sys.argv,'source_manifest_sha256':digest(out/'source-sha256.json')}
    for cmd in [['uname','-a'],['lscpu'],['numactl','--hardware'],['accel-config','list']]:
        if shutil.which(cmd[0]):
            result=subprocess.run(cmd,capture_output=True,text=True);machine[' '.join(cmd)]={'rc':result.returncode,'stdout':result.stdout,'stderr':result.stderr}
    (out/'machine.json').write_text(json.dumps(machine,indent=2)+'\n')
    if a.action=='check':
        subprocess.run([sys.executable,str(HERE/'check_hot_metrics.py')],check=True)
        env={**os.environ,'BTREE_MEMORY_AUDIT':'1','BTREE_VERIFY_CRC':'1'}
        r=subprocess.run([str(build/'hot_contract')],env=env,capture_output=True,text=True);(out/'check.log').write_text(r.stdout+r.stderr);print(r.stdout);sys.exit(r.returncode)
    if not a.corpus or not a.corpus.is_file():p.error('--corpus samba.zip required')
    if a.action=='intel' and not a.cpu_list:p.error('Specify --cpu-list for equal whole-process CPU affinity. No CPU/NUMA/WQ guessed.')
    codecs=list(dict.fromkeys(['raw']+(['lz4','qpl'] if a.action=='intel' else a.codecs.split(','))));runs=[]
    env={k:v for k,v in os.environ.items() if not k.startswith(('BTREE_','AGG_','HOT_','SD_'))}
    env.update(BTREE_CROSS_LEAF='1',BTREE_CROSS_CPU_TEST='1',BTREE_CROSS_OUTSTANDING='4',BTREE_AGG_BACKPRESSURE='1',BTREE_BG_COMPACTION='1',BTREE_SHARDS='8',BTREE_BG_INCLUDE_TRIGGER='0',BTREE_REUSE_UNCHANGED_BLOCKS='1',BTREE_CROSS_BYTES='16777216',BTREE_DELTA_BYTES=str(max(65536,a.keys*136//10)),BTREE_HOT_BYTES=str(int(a.keys*136*a.hot_fraction)),BTREE_HOT_THRESHOLD='64',BTREE_COLD_THRESHOLD='8',BTREE_HOT_RESIDENCE='4',BTREE_HOT_EPOCH_OPS='8192',BTREE_QPL_PATH='hardware',BTREE_QPL_COMPRESS_PATH='hardware',BTREE_QPL_GET_PATH='hardware',BTREE_QPL_MAINTENANCE_PATH='hardware',SD_ENCODE_US='0',SD_DECODE_US='0',SD_SLOTS='4',HOT_DEVICE_US=str(a.device_us),SAMBA_ZIP_PATH=str(a.corpus.resolve()),HOT_WARMUP='2000')
    # Reuse v2's historical timing hypothesis, not a fresh hardware calibration.
    model=dict(profile=a.surrogate_profile,scale=a.service_scale,slots=a.device_slots,
      encode_fixed_ns=round((3100 if a.surrogate_profile=='v2-reference' else a.device_us*1000)*a.service_scale),
      decode_fixed_ns=round((900 if a.surrogate_profile=='v2-reference' else a.device_us*1000)*a.service_scale),
      encode_ps_per_byte=round((1800 if a.surrogate_profile=='v2-reference' else 0)*a.service_scale),
      decode_ps_per_byte=round((330 if a.surrogate_profile=='v2-reference' else 0)*a.service_scale),
      submit_host_ns=0,poll_host_ns=0,copy_only=True,hardware_prediction=False)
    env.update(SD_SLOTS=str(a.device_slots),SD_ENCODE_PS_PER_BYTE=str(model['encode_ps_per_byte']),
      SD_DECODE_PS_PER_BYTE=str(model['decode_ps_per_byte']),SD_SUBMIT_HOST_NS='0',SD_POLL_HOST_NS='0',
      HOT_ENCODE_NS=str(model['encode_fixed_ns']),HOT_DECODE_NS=str(model['decode_fixed_ns']))
    (out/'simulation-model.json').write_text(json.dumps(model,indent=2)+'\n')
    try:
      for seed_index,seed in enumerate(map(int,a.seeds.split(','))):
       for mapping in a.mappings.split(','):
        trace=out/f'trace-{seed}-{mapping}'
        if a.traces:
          frozen=a.traces/f'trace-{seed}-{mapping}'
          m=json.loads((frozen/'manifest.json').read_text())
          expected=dict(keys=a.keys,operations=a.operations,clients=a.clients,seed=seed,zipf_alpha=a.alpha,read_pct=a.reads,rank_mapping=mapping,payload_mapping=a.payload_mapping)
          for k,v in expected.items():
            if m[k]!=v:raise ValueError(f'frozen trace mismatch {k}: {m[k]} != {v}')
          if digest(a.corpus)!=m['archive_sha256'] or digest(frozen/'trace.bin')!=m['trace_sha256'] or digest(frozen/'rank_to_key.bin')!=m['rank_map_sha256']:
            raise ValueError('frozen input checksum mismatch')
          shutil.copytree(frozen,trace)
        else:m=prepare(trace,a.corpus,a.keys,a.operations,a.clients,seed,a.alpha,a.reads,mapping,a.payload_mapping)
        for rep in range(a.repeats):
         choices=[(c,h) for c in codecs for h in [0,1]]
         if (seed_index+rep)%2:choices.reverse()
         for codec,on in choices:
          case=f'{seed}-{mapping}-{rep}-{codec}-hot{on}';mode='audit' if a.audit else 'performance'
          e={**env,'BTREE_HOT_ENABLE':str(on),'BTREE_VERIFY_CRC':'1' if a.audit else '0','BTREE_MEMORY_AUDIT':'1' if a.audit else '0','AGG_VERIFY_EACH':'1' if a.audit else '0'}
          if codec!='qpl':
            for k in ['BTREE_QPL_COMPRESS_PATH','BTREE_QPL_GET_PATH','BTREE_QPL_MAINTENANCE_PATH']:e.pop(k,None)
          cmd=[str(build/'hot_bench'),codec,str(trace/'trace.bin')]
          if a.cpu_list:cmd=['taskset','--cpu-list',a.cpu_list]+cmd
          (out/f'{case}.command.json').write_text(json.dumps({'cmd':cmd,'env':{k:v for k,v in e.items() if k.startswith(('BTREE_','AGG_','SD_','HOT_','SAMBA_'))}},indent=2)+'\n')
          print('RUN',case,mode,flush=True);started=time.time();r=subprocess.run(cmd,env=e,capture_output=True,text=True,timeout=1800)
          (out/f'{case}.log').write_text(r.stdout+r.stderr)
          data=records(r.stdout);data.update(case=case,codec=codec,policy=on,mode=mode,mapping=mapping,seed=seed,repeat=rep,status='ok' if r.returncode==0 else 'unavailable' if r.returncode==77 else 'failed',returncode=r.returncode,elapsed_seconds=time.time()-started)
          data['EVIDENCE']='simulation-with-copy' if codec=='surrogate' else 'measured-software'
          if codec=='surrogate':data['SIMULATION_MODEL']=model
          runs.append(data);summary(out,runs)
          if r.returncode not in (0,77) or (r.returncode==77 and codec!='qpl'):raise RuntimeError(f'{case} failed: see preserved log')
          if r.returncode==0:
            prod=data['PRODUCT'];assert prod['operations']==a.operations and prod['mismatches']==0 and prod['explicit_fallbacks']==0
            expected=0
            for h in m['worker_hashes']:expected^=int(h,16)
            assert prod['trace_hash']==f'{expected:016x}', 'trace mismatch'
            if codec=='qpl' and not prod['library_version'].startswith('1.9.0'):raise RuntimeError('QPL version differs from frozen v1.9.0')
    finally:summary(out,runs)
    print(out/'summary.txt')
if __name__=='__main__':main()
