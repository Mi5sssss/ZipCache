#!/usr/bin/env python3
import functools
import json
import operator
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from prepare import prepare

with tempfile.TemporaryDirectory(prefix="zipcache-aggregation-") as d:
    for batch in (1, 8, 32):
        trace = Path(d) / str(batch)
        m = prepare(trace, keys=512, threads=4, ops=500, seed=29, batch=batch)
        expected = functools.reduce(operator.xor, (int(h, 16) for h in m["worker_hashes"]))
        env = dict(os.environ, AGG_RANDOM="1", BTREE_MEMORY_AUDIT="1", BTREE_BG_COMPACTION="1",
                   BTREE_SHARDS="8", BTREE_AGG_BACKPRESSURE="1")
        for codec in ("raw", "lz4", "zstd", "zlib"):
            p = subprocess.run([sys.argv[1], codec, str(trace / "trace.bin")], env=env,
                               text=True, capture_output=True, timeout=60)
            if p.returncode:
                raise RuntimeError(p.stdout + p.stderr)
            lines = dict(line.split(" ", 1) for line in p.stdout.splitlines() if " {" in line)
            result = json.loads(lines["PRODUCT"])
            if result["trace_hash"] != f"{expected:016x}" or result["mismatches"]:
                raise RuntimeError("history mismatch")
    print("fixed history, full bytes and GET_MANY ordering passed")
