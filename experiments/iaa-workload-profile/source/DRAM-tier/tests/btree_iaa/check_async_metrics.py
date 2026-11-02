import os
from pathlib import Path
import subprocess
import sys
import tempfile
from prepare import prepare
from run_async import parse, clean_env

with tempfile.TemporaryDirectory(prefix="zipcache-metrics-") as temporary:
    trace = Path(temporary) / "trace"
    prepare(trace, keys=512, threads=4, ops=2000, reads=80, ownership="shuffled")
    for codec, window in (("raw", 0), ("lz4", 0), ("lz4", 4), ("zlib", 4)):
        env = dict(clean_env(), AGG_RANDOM="1", AGG_VERIFY_EACH="1", BTREE_MEMORY_AUDIT="1",
                   BTREE_BG_COMPACTION="1", BTREE_SHARDS="8", BTREE_AGG_BACKPRESSURE="1",
                   BTREE_CROSS_LEAF=str(int(window > 0)), BTREE_CROSS_CPU_TEST="1", BTREE_CROSS_OUTSTANDING="4",
                   BTREE_REUSE_UNCHANGED_BLOCKS="1")
        p = subprocess.run([sys.argv[1], codec, str(trace / "trace.bin")], env=env,
                           text=True, capture_output=True, timeout=60)
        assert p.returncode == 0, p.stdout + p.stderr
        m = parse(p.stdout); base = m["MEMORY_V3"]; tree = m["PRODUCT"]; c = m["CROSS_V3"]
        assert base["base_encoded_bytes"] + base["base_format_bytes"] == tree["base_requested"]
        assert sum(base["block_raw_histogram_1KiB"]) == base["base_blocks"]
        assert base["base_raw_record_bytes"] >= base["base_encoded_bytes"]
        assert tree["audit_usable"] == tree["resident_usable"]
        assert not m["TEARDOWN"]["after_thread_release"]
        assert not tree["mismatches"] and not c["failed"]
        for work in m["CODEC_WORK"]:
            assert sum(work["input_histogram_1KiB"]) == work["calls"]
            assert sum(work["output_histogram_1KiB"]) == work["calls"]
            if codec == "raw":
                assert work["calls"] == 0
        if window:
            assert c["accepted"] == c["terminal"] > 0 and c["peak_jobs"] <= window
            assert c["borrowed_peak_bytes"] <= c["byte_limit"]
            assert c["workspace_usable"] <= tree["scheduler_usable"]
print("format bytes, histogram conservation, complete owned ledger and job conservation PASS")
