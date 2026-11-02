#!/usr/bin/env python3
"""Finite Zipf requests, real unmodified samba slices, independently frozen mappings.
Global sampled requests are routed to deterministic single-writer clients. The
inter-client order is unspecified, the per-key sequence is identical in every run.
"""
import argparse,bisect,hashlib,json,random,struct,zipfile
from pathlib import Path

def prepare(out,corpus,keys=10000,operations=200000,clients=4,seed=11,alpha=1.1,reads=80,rank_mapping='clustered',payload_mapping='adjacent'):
    out=Path(out);out.mkdir(parents=True,exist_ok=False)
    archive=Path(corpus).read_bytes()
    with zipfile.ZipFile(corpus) as z:data=z.read('samba')
    assert len(data)%128==0 and len(data)//128>=keys
    ranks=list(range(1,keys+1));payloads=list(range(keys));owners=list(range(keys))
    if rank_mapping=='dispersed':random.Random(seed^0x127acf).shuffle(ranks)
    if payload_mapping=='permuted':random.Random(seed^0x903c12).shuffle(payloads)
    random.Random(seed^0x5a17c9e3).shuffle(owners)
    owner=[0]*(keys+1)
    for i,k in enumerate(owners):owner[k+1]=i%clients
    cdf=[];total=0.
    for r in range(1,keys+1):total+=r**(-alpha);cdf.append(total)
    rng=random.Random(seed);events=[[] for _ in range(clients)];freq=[0]*keys;puts=0
    for _ in range(operations):
        r=bisect.bisect_left(cdf,rng.random()*total);k=ranks[r];read=int(rng.randrange(100)<reads)
        events[owner[k]].append((k,read));freq[r]+=1;puts+=not read
    width=max(map(len,events));header=struct.pack('<10I',1,keys,clients,width,reads,seed,payload_mapping=='permuted',0,0,1)
    blob=bytearray(b'ZCAGG001'+header+struct.pack(f'<{keys}I',*payloads));hashes=[]
    for row in events:
        h=14695981039346656037
        for key,read in row:blob+=struct.pack('<II',key,read);h=((h^(key*2+read))*1099511628211)&((1<<64)-1)
        blob+=bytes(8*(width-len(row)));hashes.append(f'{h:016x}')
    sha=lambda b:hashlib.sha256(b).hexdigest()
    (out/'trace.bin').write_bytes(blob)
    (out/'rank_to_key.bin').write_bytes(struct.pack(f'<{keys}I',*ranks))
    m=dict(schema=1,source='real Silesia samba, complete 128B slices',archive_sha256=sha(archive),samba_sha256=sha(data),samba_bytes=len(data),keys=keys,operations=operations,clients=clients,seed=seed,zipf_alpha=alpha,read_pct=reads,rank_mapping=rank_mapping,payload_mapping=payload_mapping,trace_sha256=sha(blob),rank_map_sha256=sha((out/'rank_to_key.bin').read_bytes()),client_events=list(map(len,events)),worker_hashes=hashes,puts=puts,updates_per_key=puts/keys,top_1pct_request_share=sum(freq[:max(1,keys//100)])/operations,top_10pct_request_share=sum(freq[:max(1,keys//10)])/operations,assumptions=['Synthetic finite Zipf arrival pattern, not a production trace','Single client owns each key for an exact latest-value oracle','Client load imbalance is reported, no padding counted as operations','Clustered/dispersed refers to popularity rank mapping, independent of payload mapping','Updates select another existing 128B samba slice, never mutate or pad the bytes','Fixed prefix read warmup, no future hot-set oracle'],update_rule='slice=(payload_map[key-1]+version*7919)%samba_chunks; version=local_event_index*clients+client+1')
    (out/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');return m
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('out');p.add_argument('--corpus',required=True)
    for k,v in [('keys',10000),('operations',200000),('clients',4),('seed',11),('reads',80)]:p.add_argument('--'+k,type=int,default=v)
    p.add_argument('--alpha',type=float,default=1.1);p.add_argument('--rank-mapping',choices=['clustered','dispersed'],default='clustered');p.add_argument('--payload-mapping',choices=['adjacent','permuted'],default='adjacent')
    print(json.dumps(prepare(**vars(p.parse_args())),indent=2))
