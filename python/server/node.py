#!/usr/bin/env python3
"""
Placeholder runtime for Python nodes (B-I).

Part 2 implementation should replace this file with the full gRPC server.
"""

from __future__ import annotations

import argparse
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Mini2 Python node placeholder")
    parser.add_argument("--id", required=True, help="Node identifier, e.g. B")
    parser.add_argument(
        "--config",
        default="config/topology.json",
        help="Path to topology configuration JSON",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    config_path = Path(args.config)
    print(
        f"Mini2 Python node placeholder (id={args.id}, config={config_path.as_posix()})"
    )
    if not config_path.exists():
        print("Config file not found. Verify --config path.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
