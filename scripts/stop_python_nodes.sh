#!/usr/bin/env bash
# Stop processes recorded by scripts/launch_python_nodes.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PIDFILE="${ROOT}/logs/python_node_pids.txt"

if [[ ! -f "$PIDFILE" ]]; then
  echo "no pid file at ${PIDFILE} (nothing to stop)" >&2
  exit 0
fi

while read -r pid; do
  [[ -n "${pid}" ]] || continue
  if kill "${pid}" 2>/dev/null; then
    echo "stopped pid ${pid}"
  fi
done <"$PIDFILE"

rm -f "$PIDFILE"
echo "done"
