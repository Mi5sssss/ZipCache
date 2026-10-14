#!/usr/bin/env python3
"""Verify an extracted packet before building; no filesystem writes."""
import argparse
import hashlib
import json
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument("root",type=Path);a=p.parse_args();root=a.root.resolve()
m=json.loads((root/"PACKAGE_MANIFEST.json").read_text())
for name,expected in m["files"].items():
    target=(root/name).resolve()
    if not target.is_relative_to(root) or not target.is_file():raise SystemExit("invalid/missing file: "+name)
    if hashlib.sha256(target.read_bytes()).hexdigest()!=expected:raise SystemExit("checksum mismatch: "+name)
print(f"Verified {len(m['files'])} source/data/evidence files; hardware remains pending.")
