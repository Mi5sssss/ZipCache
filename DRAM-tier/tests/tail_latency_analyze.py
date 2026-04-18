#!/usr/bin/env python3
"""
Simple helper to compute latency percentiles from tail_latency_compare CSV output.
"""

import argparse
import csv
import math
from collections import defaultdict


def percentile(values, fraction):
    if not values:
        return float("nan")
    data = sorted(values)
    pos = (len(data) - 1) * fraction
    low = math.floor(pos)
    high = math.ceil(pos)
    if low == high:
        return data[int(pos)]
    return data[low] + (data[high] - data[low]) * (pos - low)


def format_stats(values):
    if not values:
        return "n/a"
    avg = sum(values) / len(values)
    return (
        f"count={len(values)} "
        f"avg={avg:.0f}ns "
        f"p50={percentile(values, 0.5):.0f}ns "
        f"p90={percentile(values, 0.9):.0f}ns "
        f"p99={percentile(values, 0.99):.0f}ns "
        f"max={max(values):.0f}ns"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Summarize latency CSV emitted by tail_latency_compare."
    )
    parser.add_argument("csv_path", help="CSV file produced by tail_latency_compare")
    parser.add_argument(
        "--op",
        choices=["get", "put", "landing"],
        help="Filter to one op type",
    )
    args = parser.parse_args()

    data = defaultdict(list)

    with open(args.csv_path, newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            op = row["op"]
            if args.op and op != args.op:
                continue
            key = (row["algorithm"], op)
            try:
                latency = float(row["latency_ns"])
            except ValueError:
                continue
            data[key].append(latency)

    if not data:
        print("No matching samples found.")
        return

    for (algo, op), values in sorted(data.items()):
        print(f"{algo} {op}: {format_stats(values)}")


if __name__ == "__main__":
    main()
