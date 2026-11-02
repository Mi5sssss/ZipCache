#!/usr/bin/env python3
"""Fixed-input workload screen and matching real-codec memory audits. No uploads."""
import argparse, hashlib, json, os, platform, shutil, signal, subprocess, sys, time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'source/DRAM-tier/tests/btree_iaa'))
from prepare_zipf import prepare
from hot_metrics import storage_metrics
from diagnose import parse_log
from handoff_checks import qualify, build_provenance
POINTS=[f'{locality}-r{reads}' for locality in ['uniform','clustered','dispersed'] for reads in [95,80,50,20]]
INTEL_POINTS=['uniform-r80','clustered-r20','uniform-r95']
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def source_manifest():return {str(p.relative_to(ROOT/'source')):sha(p) for p in sorted((ROOT/'source').rglob('*')) if p.is_file() and '__pycache__' not in p.parts}
def trace(point,ops):
    locality,reads=point.split('-r');path=ROOT/'traces'/f'{point}-n{ops}'
    if not path.exists():prepare(path,ROOT/'data/samba.zip',150000,ops,4,11,0 if locality=='uniform' else 1.1,int(reads),'dispersed' if locality=='dispersed' else 'clustered','adjacent')
    m=json.loads((path/'manifest.json').read_text())
    if sha(path/'trace.bin')!=m['trace_sha256'] or sha(ROOT/'data/samba.zip')!=m['archive_sha256']:
        raise ValueError('Input checksum differs from trace manifest')
    return path/'trace.bin',m
def environment(codec,mode,profile,enc=1.,dec=1.):
    e={k:v for k,v in os.environ.items() if not k.startswith(('BTREE_','HOT_','AGG_','SD_','ZIPCACHE_PERF_'))}
    e.update(json.loads((ROOT/'base-env.json').read_text()))
    e.update(SAMBA_ZIP_PATH=str(ROOT/'data/samba.zip'),BTREE_HOT_BYTES=str(2*1024*1024),BTREE_HOT_TOTAL_BYTES='0',
      BTREE_QPL_CROSS_HARDWARE='1',BTREE_HOT_WORK_SIGNAL='0',BTREE_HOT_THRESHOLD='64',BTREE_COLD_THRESHOLD='8',BTREE_CPU_PROFILE=profile,
      BTREE_VERIFY_CRC=str(int(mode=='audit')),AGG_VERIFY_EACH=str(int(mode=='audit')),BTREE_MEMORY_AUDIT=str(int(mode=='audit')),
      HOT_ENCODE_NS=str(round(3100*enc)),HOT_DECODE_NS=str(round(900*dec)),
      SD_ENCODE_PS_PER_BYTE=str(round(1800*enc)),SD_DECODE_PS_PER_BYTE=str(round(330*dec)))
    for role in ['COMPRESS','GET','MAINTENANCE']:
        if codec=='qpl':e[f'BTREE_QPL_{role}_PATH']='hardware'
        else:e.pop(f'BTREE_QPL_{role}_PATH',None)
    return e
def summarize(out,rows):
    lines=['ZipCache workload / host CPU screen, schema 2',
      'LZ4: measured software. Surrogate: uncalibrated delays + COPY, NOT IAA performance or capacity.',
      '150k samba 128B values, 8 shards, 4 clients, window 4, 2MiB hot RAW ceiling.',
      'No total-storage admission guard in either performance path. Not equal-total-DRAM performance.',
      'Finite closed-loop screen includes final drain; not sustained QPS, production locality, or hardware acceptance.',
      'Audit has CRC/oracle/allocation tracking on; its timing is NOT compared with performance.',
      'zlib rows measure software Deflate; qpl rows measure actual QPL output after strict hardware preflight. Stored ratio covers cold bases only.',
      'mode point codec rep QPS CPU_ns/op GET_p99_us PUT_p99_us hot_GET_pct GET_jobs/read comp_jobs/PUT status']
    for r in rows:
        if r['status']!='ok':lines.append(f"{r['case']} {r['status']} errors={r.get('validation_errors',[])}");continue
        p=r['PRODUCT'];n=p['operations'];writes=p['writes'];reads=n-writes;h=r['HOT'];work=r['CODEC_WORK']
        def v(x):return f'{x:.3f}'
        perf=r['mode']!='audit'
        lines.append(' '.join(map(str,[r['mode'],r['point'],r['codec'],r['repeat'],v(n*1e9/p['completed_ns']) if perf else 'not-timed',
          v(p['cpu_ns']/n) if perf else 'not-timed',v(p['get_p99_ns']/1000) if perf else 'not-timed',v(p['put_p99_ns']/1000) if perf else 'not-timed',
          v(100*h['raw_gets']/reads),v(work['get_decompress']['calls']/reads),v(work['compress']['calls']/writes),r['status']])))
    lines+=['','INPUTS AND EXECUTION (software outstanding is not hardware occupancy):']
    for r in rows:
        if r['status']!='ok':continue
        p=r['PRODUCT'];m=r['INPUT'];w=r['CODEC_WORK'];a=r['AGGREGATION'];e=r['EXECUTION']
        lines.append(f"INPUT {r['case']} zipf_alpha={m['zipf_alpha']} GET_pct={m['read_pct']} rank_mapping={m['rank_mapping']} payload_mapping={m['payload_mapping']} trace_sha256={m['trace_sha256']}")
        lines.append(f"RESOURCES {r['case']} {json.dumps(r.get('RESOURCES',{'status':'not recorded in older result'}),sort_keys=True)}")
        lines.append(f"EXEC {r['case']} actual_backend={p['actual_backend']} library={p['library_version']} lz4_version={p['lz4_version']} execution_path={p['execution_path']} crc={r['HOT']['crc']} calls_under_write_lock={p['write_lock_codec_calls']} sync_fallbacks={p['sync_fallbacks']} split_fallbacks={p['split_fallbacks']} pending_wait_wall_ns={a['pending_wait_ns']} read_lock_wait_wall_ns={a['read_lock_wait_ns']} foreground_QPS={p['operations']*1e9/p['foreground_ns'] if r['mode']!='audit' else 'not-timed'}")
        for role,work in w.items():
            lines.append(f"WORK {r['case']} role={role} calls={work['calls']} failed={work['failed']} input_bytes={work['input_bytes']} output_bytes={work['output_bytes']}")
        if r['codec']=='qpl':
            for role,route in zip(('compress','GET_decompress','maintenance_decompress'),e['routes']):
                lines.append(f"QPL_ROUTE {r['case']} role={role} requested={route['requested']} calls={route['calls']} completed={route['completed']} failed={route['failed']} busy={route['busy']} last_error_status={route['last_error_status']}")
    lines+=['','CPU LEDGER (ns/logical op; stages are instrumented diagnostic costs):']
    for r in rows:
        prof=r.get('CPU_PROFILE',{})
        if r['status']!='ok' or prof.get('mode','off')=='off':continue
        n=r['PRODUCT']['operations'];roles={};stages={}
        for t in prof['threads']:
            roles[t['role']]=roles.get(t['role'],0)+t['cpu_ns']/n
            for k,v in t['stages'].items():stages[k]=stages.get(k,0)+v['cpu_ns']/n
            stages['unattributed']=stages.get('unattributed',0)+t['unattributed_cpu_ns']/n
        lines.append('CPU '+r['case']+' '+' '.join(f'{k}={v:.1f}' for k,v in roles.items())+f" process_minus_threads_ns={prof['process_minus_threads_ns']} qpl_init_attempts={prof.get('qpl_init_attempts','unavailable')} qpl_init_successes={prof.get('qpl_init_successes','unavailable')}")
        if prof['mode']=='stages':lines.append('CPU_STAGE '+r['case']+' '+' '.join(f'{k}={v:.1f}' for k,v in stages.items() if v))
        c=r['CROSS_V3'];checks=r.get('CROSS_CHECKS',{});timing=r['TIMING_V2']
        lines.append(f"WAIT {r['case']} accepted={c['accepted']} terminal={c['terminal']} failed={c['failed']} busy={c['busy']} bg_peak_jobs={c['peak_jobs']} checks={checks.get('completion_checks','unavailable')} not_ready={checks.get('not_ready','unavailable')} queue_wait_wall_ns={timing['enqueue_to_dequeue_ns']} ready_to_accept_wall_ns={c['ready_to_accept_ns']} drain_wall_ns={timing['drain_call_ns']}")
    for r in rows:
        if r['mode']!='audit' or r['status']!='ok':continue
        refs=[x for x in rows if x['mode']=='audit' and x['codec']=='raw' and x['point']==r['point'] and x['status']=='ok']
        if not refs:continue
        s=r['STORAGE'];ref=refs[0]['STORAGE']
        for kind in ['retained','peak']:
            a=ref[f'total_owned_{kind}_bytes'];b=s[f'total_owned_{kind}_bytes']
            s[f'raw_{kind}_reference_bytes']=a;s[f'capacity_{kind}_x']=a/b if a and b and s['memory_ledger_complete_for_scope'] else None
            s[f'memory_saved_{kind}_pct']=100*(1-b/a) if a and b and s['memory_ledger_complete_for_scope'] else None
            s[f'tracked_memory_saved_{kind}_pct']=100*(1-b/a) if a and b else None
        if s['memory_ledger_complete_for_scope']:
            s['reason']='matched same-trace RAW; limited to stated owned-memory scope'
            s['capacity_gate']='pass-tracked-scope' if min(s['capacity_retained_x'],s['capacity_peak_x'])>=1.25 else 'fail-tracked-scope'
        lines.append(f"MEMORY {r['point']} {r['codec']} cold_data_ratio={s['cold_data_ratio']} cold_framed_ratio={s['cold_framed_ratio']} codec_work_ratio={s['codec_work_ratio']} retained_bytes={s['total_owned_retained_bytes']} peak_bytes={s['total_owned_peak_bytes']} tracked_saved_retained_pct={s['tracked_memory_saved_retained_pct']} tracked_saved_peak_pct={s['tracked_memory_saved_peak_pct']} complete_saved_retained_pct={s['memory_saved_retained_pct']} complete_saved_peak_pct={s['memory_saved_peak_pct']} complete={s['memory_ledger_complete_for_scope']} reason={s['reason']}")
    lines+=['','Memory: tracked_saved excludes any unverified QPL-private allocations; it is NOT whole-system savings. Complete savings remain unavailable when complete=False. RSS includes benchmark fixtures/runtime and is not a substitute for this ledger.']
    (out/'results.json').write_text(json.dumps(rows,indent=2)+'\n');(out/'summary.txt').write_text('\n'.join(lines)+'\n')
def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['check','screen','diagnose','audit','sensitivity','overhead','intel','intel-profile','intel-audit'])
    p.add_argument('--out',type=Path,required=True);p.add_argument('--points');p.add_argument('--operations',type=int,default=1000000)
    p.add_argument('--repeats',type=int,default=3);p.add_argument('--build',type=Path,default=ROOT/'build');p.add_argument('--cpu-list');p.add_argument('--numa-node',type=int)
    p.add_argument('--perf',action='store_true',help='Linux intel-profile only; epoch-gated CPU call stacks, separate from performance')
    a=p.parse_args();points=a.points.split(',') if a.points else (INTEL_POINTS if a.action.startswith('intel') else POINTS)
    if not set(points)<=set(POINTS) or len(set(points))!=len(points):p.error('Unknown or duplicate workload point')
    if a.operations<=0 or a.repeats<=0:p.error('Operations and repeats must be positive')
    if a.perf and (a.action!='intel-profile' or not shutil.which('perf')):p.error('--perf requires intel-profile and an already permitted perf installation')
    if a.action.startswith('intel') and (platform.system()!='Linux' or not a.cpu_list or a.numa_node is None):p.error('Intel requires Linux, explicit --cpu-list and --numa-node; no WQ/system changes are made.')
    out=a.out.resolve();out.mkdir(parents=True,exist_ok=False);build=a.build.resolve()
    global CURRENT_OUT
    CURRENT_OUT=out
    (out/'run-status.json').write_text(json.dumps({'status':'running','action':a.action})+'\n')
    (out/'build-provenance.json').write_text(json.dumps(build_provenance(build),indent=2)+'\n')
    (out/'source-sha256.json').write_text(json.dumps(source_manifest(),indent=2)+'\n')
    machine={'platform':platform.platform(),'machine':platform.machine(),'cpu_count':os.cpu_count(),'argv':sys.argv}
    for cmd in [['sysctl','-n','machdep.cpu.brand_string'],['lscpu'],['numactl','--hardware'],['accel-config','list']]:
        if shutil.which(cmd[0]):
            r=subprocess.run(cmd,capture_output=True,text=True);machine[' '.join(cmd)]={'returncode':r.returncode,'stdout':r.stdout,'stderr':r.stderr}
    (out/'machine.json').write_text(json.dumps(machine,indent=2)+'\n')
    if a.action=='check':
        with (out/'ctest.log').open('w') as f:subprocess.run(['ctest','--test-dir',str(build),'--output-on-failure','-j4'],stdout=f,stderr=subprocess.STDOUT,check=True)
        (out/'run-status.json').write_text(json.dumps({'status':'complete','action':a.action,'scope':'CTest; inspect log for skipped hardware tests'})+'\n')
        return
    if not (build/'hot_bench').exists():p.error('Build hot_bench first; see README.md')
    if a.action.startswith('intel'):
        with (out/'qpl-preflight.log').open('w') as f:
            r=subprocess.run(['numactl',f'--physcpubind={a.cpu_list}',f'--membind={a.numa_node}',str(build/'agg_qpl_format')],stdout=f,stderr=subprocess.STDOUT,env=environment('qpl','audit','off'),timeout=120)
        if r.returncode:raise RuntimeError('Strict hardware cross-decoding preflight unavailable/failed; see log. No surrogate substitution.')
    rows=[]
    try:
      for repeat in range(1 if a.action in ('audit','intel-audit') else a.repeats):
       for point in (points if repeat%2==0 else list(reversed(points))):
        path,m=trace(point,a.operations)
        choices=[('raw',1.,1.,'off'),('lz4',1.,1.,'off'),('zlib',1.,1.,'off')] if a.action in ('audit','intel-audit') else [(c,1.,1.,'stages' if a.action in ('diagnose','intel-profile') else 'threads') for c in (['lz4','qpl'] if a.action.startswith('intel') else ['lz4','surrogate'])]
        if a.action=='intel-audit':choices=[('raw',1.,1.,'off'),('lz4',1.,1.,'off'),('qpl',1.,1.,'off')]
        if a.action=='sensitivity':
            # Re-measure nominal service in every repetition. A historical
            # screen from another thermal/scheduling interval is not its control.
            choices=[('surrogate',e,d,'threads') for e,d in [(0.5,1),(2,1),(1,1),(1,0.5),(1,2)]]
            shift=(repeat*2)%len(choices);choices=choices[shift:]+choices[:shift]
        if a.action=='overhead':choices=[(c,1.,1.,prof) for c in ['lz4','surrogate'] for prof in ['off','threads']]
        if repeat%2:choices.reverse()
        for codec,enc,dec,profile in choices:
          case=f'{point}-{codec}-e{enc:g}-d{dec:g}-{profile}-{repeat}';mode='audit' if a.action in ('audit','intel-audit') else 'diagnostic' if a.action in ('diagnose','intel-profile') else 'performance'
          env=environment(codec,mode,profile,enc,dec);cmd=[str(build/'hot_bench'),codec,str(path)]
          if a.action.startswith('intel'):cmd=['numactl',f'--physcpubind={a.cpu_list}',f'--membind={a.numa_node}']+cmd
          if a.perf:
            ctl=out/f'{case}.control';ack=out/f'{case}.ack';os.mkfifo(ctl);os.mkfifo(ack)
            fifo_fds=[os.open(ctl,os.O_RDWR),os.open(ack,os.O_RDWR)]
            env.update(ZIPCACHE_PERF_CONTROL=str(ctl),ZIPCACHE_PERF_ACK=str(ack))
            cmd=['perf','record','--delay=-1','--control',f'fifo:{ctl},{ack}','-e','cpu-clock','-F','99','--call-graph','dwarf,16384','-o',str(out/f'{case}.perf.data'),'--']+cmd
          (out/f'{case}.command.json').write_text(json.dumps({'cmd':cmd,'env':{k:v for k,v in env.items() if k.startswith(('BTREE_','HOT_','AGG_','SD_','SAMBA_','ZIPCACHE_PERF_'))}},indent=2)+'\n')
          print(a.action,case,flush=True);start=time.time()
          execution_error=None
          try:
            with (out/f'{case}.log').open('w') as f:
              r=subprocess.Popen(cmd,env=env,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
              try:r.wait(timeout=1200)
              except subprocess.TimeoutExpired:
                os.killpg(r.pid,signal.SIGKILL);r.wait();execution_error='benchmark timeout after 1200 seconds'
          finally:
            if a.perf:
              for fd in fifo_fds:os.close(fd)
              ctl.unlink();ack.unlink()
          d=parse_log(out/f'{case}.log');d.update(case=case,point=point,mode=mode,codec=codec,repeat=repeat,profile=profile,encode_scale=enc,decode_scale=dec,
            INPUT=m,CONFIG={k:v for k,v in env.items() if k.startswith(('BTREE_','HOT_','AGG_','SD_'))},elapsed_seconds=time.time()-start,returncode=r.returncode,
            RESOURCES={'cpu_list':a.cpu_list,'numa_node':a.numa_node,'scope':'whole process','background_window':4,'hot_bytes':2097152,'total_dram_cap_enforced':False},
            status='ok' if r.returncode==0 else 'unavailable' if r.returncode==77 else 'failed',evidence='conditional-simulation-COPY' if codec=='surrogate' else 'strict-hardware' if codec=='qpl' else 'measured-software')
          if execution_error:d['execution_error']=execution_error
          qualify(d)
          tmp=dict(d);tmp['CODEC_WORK']=list(d['CODEC_WORK'].values());d['STORAGE']=storage_metrics(tmp);rows.append(d);summarize(out,rows)
          if d['status']!='ok':raise RuntimeError(f"{case} failed/unavailable: {d['validation_errors']}; retained log, no fallback")
          if a.perf:
            with (out/f'{case}.perf.txt').open('w') as f:subprocess.run(['perf','report','--stdio','--no-children','-i',str(out/f'{case}.perf.data')],stdout=f,stderr=subprocess.STDOUT,check=True)
    finally:summarize(out,rows)
    (out/'run-status.json').write_text(json.dumps({'status':'complete','action':a.action,'validated_rows':len(rows)})+'\n')
    print(out/'summary.txt')
if __name__=='__main__':
    CURRENT_OUT=None
    try:main()
    except BaseException as exc:
        if CURRENT_OUT is not None and not (isinstance(exc,SystemExit) and exc.code==0):
            message=f'{type(exc).__name__}: {exc}'
            (CURRENT_OUT/'run-status.json').write_text(json.dumps({'status':'failed','error':message},indent=2)+'\n')
            with (CURRENT_OUT/'summary.txt').open('a') as f:f.write('\nRUN_FAILED '+message+'\n')
        raise
