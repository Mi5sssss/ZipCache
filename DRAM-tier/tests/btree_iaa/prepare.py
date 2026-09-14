#!/usr/bin/env python3
"""Freeze source-derived payload mapping and synthetic per-worker history.

No network, retrieval, padding, codec-dependent ordering, or production-trace claim.
Binary integers are little endian (the supported Mac/Intel experiment platforms).
"""
import argparse
import hashlib
import json
from pathlib import Path
import struct
import zipfile


def sha(data):
    return hashlib.sha256(data).hexdigest()


def randoms(seed):
    s = seed or 1
    while True:
        s ^= (s << 13) & 0xffffffff
        s ^= s >> 17
        s ^= (s << 5) & 0xffffffff
        yield s & 0xffffffff


def prepare(out, keys=150000, threads=4, ops=100000, seed=11, reads=80,
            mapping="adjacent", hot=False, sparse=False, batch=1, corpus=None,
            ownership="strided"):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=False)
    if keys < 32 or keys > 150000 or threads < 1 or threads > 64 or ops < 1:
        raise ValueError("invalid bounded trace configuration")
    if reads not in range(101) or batch not in (1, 8, 32):
        raise ValueError("invalid read percentage or batch")
    if ownership not in ("strided", "shuffled") or (ownership == "shuffled" and batch != 1):
        raise ValueError("shuffled ownership supports point operations only")
    source = {"status": "synthetic-test-only"}
    if corpus:
        archive = Path(corpus).read_bytes()
        with zipfile.ZipFile(corpus) as z:
            data = z.read("samba")
        if len(data) % 128 or len(data) // 128 < keys:
            raise ValueError("the whole corpus must contain enough complete 128B slices")
        (out / "samba.bin").write_bytes(data)
        source = {"status": "real-silesia-samba", "source": "https://sun.aei.polsl.pl/~sdeor/index.php?page=silesia",
                  "zip_sha256": sha(archive), "raw_sha256": sha(data), "raw_bytes": len(data),
                  "chunk_bytes": 128, "chunks": len(data) // 128}
    indices = list(range(keys))
    if mapping == "permuted":
        rng = randoms(seed)
        for i in range(keys - 1, 0, -1):
            j = next(rng) % (i + 1)
            indices[i], indices[j] = indices[j], indices[i]
    elif mapping != "adjacent":
        raise ValueError("unknown mapping")
    live = [k for k in range(1, keys + 1) if not sparse or k % 5 == 0]
    if len(live) < threads:
        raise ValueError("not enough live keys")
    assignment = list(live)
    if ownership == "shuffled":
        # Decouple client ownership from key % shard_count while preserving a
        # single reader/writer owner per key and the existing exact oracle.
        rng = randoms(seed ^ 0x5a17c9e3)
        for i in range(len(assignment) - 1, 0, -1):
            j = next(rng) % (i + 1)
            assignment[i], assignment[j] = assignment[j], assignment[i]
    header = struct.pack("<10I", 1, keys, threads, ops, reads, seed,
                         mapping == "permuted", hot, sparse, batch)
    events = bytearray()
    hashes, updates = [], 0
    for t in range(threads):
        owned = sorted(assignment[t::threads])
        rng, h = randoms(seed + t * 104729), 14695981039346656037
        for _ in range(ops):
            extent = len(owned)
            if hot and next(rng) % 100 < 80:
                extent = max(1, extent // 5)
            key = owned[next(rng) % extent]
            read = next(rng) % 100 < reads
            events += struct.pack("<II", key, read)
            h = ((h ^ (key * 2 + read)) * 1099511628211) & ((1 << 64) - 1)
            updates += not read
        hashes.append(h)
    trace = b"ZCAGG001" + header + struct.pack(f"<{keys}I", *indices) + events
    (out / "trace.bin").write_bytes(trace)
    manifest = {"schema": 1, "keys": keys, "threads": threads, "ops_per_thread": ops,
                "seed": seed, "read_pct": reads, "mapping": mapping, "hot": hot, "sparse": sparse,
                "batch": batch, "trace_sha256": sha(trace), "mapping_sha256": sha(trace[48:48+keys*4]),
                "worker_hashes": [f"{h:016x}" for h in hashes], "updates": updates,
                "update_rounds": updates / len(live), "source": source,
                "ownership": ownership,
                "ownership_sha256": sha(struct.pack(f"<{len(assignment)}I", *assignment)),
                "history": "Synthetic arrival order, disjoint per-worker key ownership; not an AI/agent trace.",
                "locality": ("shuffled ownership shares shards/leaves, not keys" if ownership == "shuffled" else
                             "strided ownership may isolate shards across workers") +
                            "; hot uses 80% hot-prefix plus 20% whole-domain sampling (about 84% on the first 20%)",
                "mapping_rule": "key k initially uses slice mapping[k-1]; no payload truncation or zero padding",
                "update_rule": "version=event_index*threads+worker+1; slice=(mapping[k-1]+version*7919)%corpus_chunks",
                "batch_rule": "synthetic next owned keys, last repeats first; never counted as single GET",
                "verification": "returned full bytes outside API latency, included in process CPU and wall time"}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("out", type=Path)
    p.add_argument("--corpus", type=Path)
    for name, default in (("keys", 150000), ("threads", 4), ("ops", 100000), ("seed", 11), ("reads", 80), ("batch", 1)):
        p.add_argument("--" + name, type=int, default=default)
    p.add_argument("--mapping", choices=("adjacent", "permuted"), default="adjacent")
    p.add_argument("--ownership", choices=("strided", "shuffled"), default="strided")
    p.add_argument("--hot", action="store_true")
    p.add_argument("--sparse", action="store_true")
    a = vars(p.parse_args())
    print(json.dumps(prepare(**a), indent=2))
