#!/usr/bin/env python3
"""
Split a CSV into 9 disjoint partition files (A..I).

Usage:
  python scripts/split_data.py --input data/test_sample.csv --output-dir data
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, List


NODE_IDS = ["A", "B", "C", "D", "E", "F", "G", "H", "I"]


def split_rows_evenly(rows: List[List[str]], n: int) -> List[List[List[str]]]:
    base = len(rows) // n
    rem = len(rows) % n
    partitions: List[List[List[str]]] = []
    start = 0
    for i in range(n):
        end = start + base + (1 if i < rem else 0)
        partitions.append(rows[start:end])
        start = end
    return partitions


def write_partition(path: Path, header: List[str], rows: List[List[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Split input CSV into 9 disjoint node partitions.")
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--output-dir", default="data", help="Output directory for partition files")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    with input_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)

    partitions = split_rows_evenly(rows, len(NODE_IDS))
    counts: Dict[str, int] = {}

    for node_id, part_rows in zip(NODE_IDS, partitions):
        out_path = output_dir / f"partition_{node_id}.csv"
        write_partition(out_path, header, part_rows)
        counts[node_id] = len(part_rows)

    total_partitioned = sum(counts.values())

    print(f"Input rows: {len(rows)}")
    print(f"Total partitioned rows: {total_partitioned}")
    for node_id in NODE_IDS:
        print(f"partition_{node_id}.csv: {counts[node_id]}")

    if total_partitioned != len(rows):
        raise RuntimeError("Row count mismatch: partitions are not disjoint/comprehensive")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
