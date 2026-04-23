#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[1/4] Splitting sample data into A..I partitions..."
python3 scripts/split_data.py --input data/test_sample.csv --output-dir data

echo "[2/4] Configuring C++ build..."
cmake -S cpp -B cpp/build

echo "[3/4] Building C++ targets..."
cmake --build cpp/build

echo "[4/4] Running smoke binaries..."
./cpp/build/node_server
./cpp/build/client

echo
echo "Foundation demo completed successfully."
echo "Next: open docs/HANDOFF_SETH.md for Seth's implementation handoff."
