import json, os, subprocess, sys, tempfile
from pathlib import Path
from prepare import prepare
def parse(s):
    out={}
    for line in s.splitlines():
        try:k,v=line.split(' ',1);out[k]=json.loads(v)
        except (ValueError,json.JSONDecodeError):pass
    return out
def verify(p):
    assert p['thread_clock_complete'],p
    total=sum(t['cpu_ns'] for t in p['threads'])
    assert abs(p['process_minus_threads_ns'])<max(1000000,p['process_cpu_ns']*.05),p
    assert total>0
    for t in p['threads']:
        assert t['unattributed_cpu_ns']>=-10000,t
    return total
p=parse(subprocess.check_output([sys.argv[1]],text=True))['CPU_PROFILE'];verify(p)
s=p['threads'][0]['stages'];assert s['get_other']['calls']==s['decode_or_dispatch']['calls']==1
assert s['get_other']['cpu_ns']>0 and s['decode_or_dispatch']['cpu_ns']>0
with tempfile.TemporaryDirectory() as t:
    path=Path(t)/'trace';prepare(path,keys=512,threads=4,ops=3000,reads=80,ownership='shuffled')
    for codec in ['lz4','surrogate']:
        env={k:v for k,v in os.environ.items() if not k.startswith(('BTREE_','HOT_','AGG_','SD_'))}
        env.update(AGG_RANDOM='1',AGG_VERIFY_EACH='1',BTREE_CPU_PROFILE='stages',BTREE_MEMORY_AUDIT='1',
          BTREE_BG_COMPACTION='1',BTREE_SHARDS='8',BTREE_AGG_BACKPRESSURE='1',BTREE_CROSS_LEAF='1',
          BTREE_CROSS_CPU_TEST='1',BTREE_CROSS_OUTSTANDING='4',BTREE_REUSE_UNCHANGED_BLOCKS='1',
          BTREE_HOT_ENABLE='1',BTREE_HOT_BYTES='65536',HOT_DEVICE_US='0',HOT_WARMUP='0',SD_SLOTS='4')
        r=subprocess.run([sys.argv[2],codec,str(path/'trace.bin')],env=env,capture_output=True,text=True,timeout=90)
        assert r.returncode==0,r.stdout+r.stderr
        d=parse(r.stdout);verify(d['CPU_PROFILE']);assert not d['PRODUCT']['mismatches']
        roles=[x['role'] for x in d['CPU_PROFILE']['threads']]
        assert roles.count('client')==4 and roles.count('background')==1,roles
        assert roles.count('surrogate_engine')==(codec=='surrogate'),roles
        c=d['CROSS_V3'];assert c['accepted']==c['terminal'] and not c['failed']
        assert d['CROSS_CHECKS']['completion_checks']>=c['terminal']
print('CPU clocks, exclusive nested stages, real-tree full bytes and terminal conservation PASS')
