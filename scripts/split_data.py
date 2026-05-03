#!/usr/bin/env python3
"""
Split a CSV into 9 disjoint partition files (A..I).

Usage:
  python scripts/split_data.py --input data/test_sample.csv --output-dir data
"""

from __future__ import annotations

import argparse
import contextlib
import csv
from pathlib import Path
from typing import Dict, List


NODE_IDS = ["A", "B", "C", "D", "E", "F", "G", "H", "I"]


def partition_sizes(total_rows: int, n: int) -> List[int]:
    base = total_rows // n
    rem = total_rows % n
    return [base + (1 if i < rem else 0) for i in range(n)]


def count_data_rows(input_path: Path) -> int:
    count = 0
    with input_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # header
        for _ in reader:
            count += 1
    return count


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
    total_rows = count_data_rows(input_path)
    sizes = partition_sizes(total_rows, len(NODE_IDS))

    counts: Dict[str, int] = {node_id: 0 for node_id in NODE_IDS}

    with contextlib.ExitStack() as stack:
        writers: List[csv.writer] = []
        for node_id in NODE_IDS:
            out_path = output_dir / f"partition_{node_id}.csv"
            out_file = stack.enter_context(out_path.open("w", newline="", encoding="utf-8"))
            writer = csv.writer(out_file)
            writer.writerow(header)
            writers.append(writer)

        with input_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # header

            partition_index = 0
            rows_left_in_partition = sizes[partition_index] if sizes else 0

            for row in reader:
                while rows_left_in_partition == 0 and partition_index < len(NODE_IDS) - 1:
                    partition_index += 1
                    rows_left_in_partition = sizes[partition_index]

                writers[partition_index].writerow(row)
                counts[NODE_IDS[partition_index]] += 1
                rows_left_in_partition -= 1

    total_partitioned = sum(counts.values())

    print(f"Input rows: {total_rows}")
    print(f"Total partitioned rows: {total_partitioned}")
    for node_id in NODE_IDS:
        print(f"partition_{node_id}.csv: {counts[node_id]}")

    if total_partitioned != total_rows:
        raise RuntimeError("Row count mismatch: partitions are not disjoint/comprehensive")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
