#!/usr/bin/env python3
"""
Combine multiple CSV files into one output CSV with a single header row.

Example:
  python scripts/combine_csv.py \
    --output combined/all_years.csv \
    --inputs 2020/2020.csv 2021/2021.csv 2022/2022.csv 2023/2023.csv
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Combine multiple CSV files (same schema) into one CSV."
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output CSV path (parent directories are created if needed).",
    )
    parser.add_argument(
        "--inputs",
        nargs="+",
        required=True,
        help="Input CSV files in the desired order.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_path = Path(args.output)
    input_paths = [Path(p) for p in args.inputs]

    for path in input_paths:
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    expected_header: list[str] | None = None
    total_rows = 0

    with output_path.open("w", newline="", encoding="utf-8") as out_f:
        writer = csv.writer(out_f)

        for input_path in input_paths:
            with input_path.open("r", newline="", encoding="utf-8") as in_f:
                reader = csv.reader(in_f)
                try:
                    header = next(reader)
                except StopIteration:
                    continue

                if expected_header is None:
                    expected_header = header
                    writer.writerow(header)
                elif header != expected_header:
                    raise ValueError(
                        f"Header mismatch in {input_path}. "
                        "All input CSVs must have identical headers."
                    )

                file_rows = 0
                for row in reader:
                    writer.writerow(row)
                    file_rows += 1
                total_rows += file_rows
                print(f"{input_path}: {file_rows} rows")

    if expected_header is None:
        raise RuntimeError("No headers found. Inputs may be empty.")

    print(f"Output: {output_path}")
    print(f"Total combined rows: {total_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
