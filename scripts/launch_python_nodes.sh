#!/usr/bin/env bash
# Start Python gRPC nodes B..I (topology). Run from repo root; uses .venv/bin/python.
# Start C++ entry separately: ./cpp/build/node_server --id A
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PY="${ROOT}/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  echo "error: ${PY} not found or not executable" >&2
  echo "Create a venv and install deps: python3 -m venv .venv && .venv/bin/python -m pip install -r requirements.txt" >&2
  exit 1
fi

mkdir -p logs
PIDFILE="${ROOT}/logs/python_node_pids.txt"
: >"$PIDFILE"

for id in B C D E F G H I; do
  "${PY}" python/server/node.py --id "${id}" >>"logs/node_${id}.log" 2>&1 &
  echo $! >>"$PIDFILE"
  echo "started node ${id} pid=$!"
done

echo
echo "PIDs written to ${PIDFILE}"
echo "Logs: ${ROOT}/logs/node_*.log"
echo "Stop: bash scripts/stop_python_nodes.sh"
