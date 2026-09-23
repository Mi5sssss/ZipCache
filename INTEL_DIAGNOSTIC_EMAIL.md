Subject: ZipCache diagnostic update and text summary

Hi Binuraj,

Thanks for flagging the upload restriction. I have updated `wip/iaa-hybrid-evaluation` to generate a `summary.txt` that you can email or paste directly, and added an explicit static QPL linking option.

This update keeps the tree and scheduling behavior unchanged. The focused run compares LZ4, QPL software and IAA on the same 80/20 workload, retains RAW for the memory comparison, and adds a same-compressed-data GET comparison. The aim is to distinguish foreground decompression cost from maintenance pressure and synchronous rebuilds, rather than assume a work queue issue.

From your checkout:

```sh
git switch wip/iaa-hybrid-evaluation
git pull --ff-only origin wip/iaa-hybrid-evaluation
git submodule update --init SilesiaCorpus

python3 DRAM-tier/tests/btree_iaa/intel_eval.py \
  --out intel-runs/diagnostic-smoke --profile smoke --diagnostic \
  --qpl-root /opt/qpl-1.9.0 --qpl-linkage static \
  --cpu-list 2,4 --numa-node 0
```

If smoke passes:

```sh
python3 DRAM-tier/tests/btree_iaa/intel_eval.py \
  --out intel-runs/diagnostic-screen --profile screen --diagnostic \
  --qpl-root /opt/qpl-1.9.0 --qpl-linkage static \
  --cpu-list 2,4 --numa-node 0 --clients 1 4 --reads 80
```

Please substitute your QPL path, two distinct physical-core CPU IDs and local NUMA node. If your previous linking changes conflict with the pull, please preserve them and use a fresh checkout rather than discard them. The branch and instructions are here:
https://github.com/Mi5sssss/ZipCache/tree/wip/iaa-hybrid-evaluation

Please send `intel-runs/diagnostic-screen/summary.txt`. It includes QPS, CPU/op, GET/PUT p99, memory ratios, queue and lock waits, synchronous fallbacks, and codec work by operation. If a run fails, its `summary.txt` and the error excerpt are sufficient initially; no archive upload is needed.

Best,
Rui
