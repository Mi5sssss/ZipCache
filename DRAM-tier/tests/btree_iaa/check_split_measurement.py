#!/usr/bin/env python3
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from prepare import prepare

with tempfile.TemporaryDirectory(prefix="zipcache-split-measurement-") as d:
    root=Path(d);prepare(root/"trace",keys=128,threads=2,ops=40,reads=100)
    images=[]
    for oracle in (1,0):
        image=root/f"{oracle}.bases"
        env=dict(os.environ,AGG_RANDOM="1",AGG_VERIFY_EACH=str(oracle),AGG_ARRIVAL_RATE="1000",
                 AGG_BASE_IMAGE=str(image),BTREE_BG_COMPACTION="0",BTREE_MEMORY_AUDIT=str(oracle),BTREE_SHARDS="8")
        result=subprocess.run([sys.argv[1],"lz4",str(root/"trace/trace.bin")],env=env,text=True,capture_output=True,timeout=30)
        if result.returncode:raise RuntimeError(result.stdout+result.stderr)
        records={tag:json.loads(data) for tag,sep,data in (line.partition(" ") for line in result.stdout.splitlines()) if data.startswith("{")}
        execution,product=records["EXECUTION"],records["PRODUCT"]
        assert execution["verify_each"]==oracle and execution["arrival_rate"]==1000
        assert product["operations"]==80 and product["reads"]==80 and product["foreground_ns"]>=79_000_000
        assert not product["mismatches"] and not records["TEARDOWN"]["after_thread_release"]
        images.append(image.read_bytes())
    assert images[0]==images[1],"encoded representation changed between paired reads"
print("oracle separation, scheduled arrival, no drops and identical representation passed")
