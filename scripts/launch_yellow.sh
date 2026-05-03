#!/bin/bash
# Machine 2: nodes C, E, F, G, I (all Python)
# Update config/topology.json with real IPs before running:
#   Machine 1 nodes: A, B, D, H
#   Machine 2 nodes: C, E, F, G, I
set -euo pipefail

CONFIG="${1:-config/topology.json}"
PY=""
WAIT_SECONDS="${WAIT_SECONDS:-1800}"

if [ ! -f "$CONFIG" ]; then
    echo "Config not found: $CONFIG"
    exit 1
fi

if [ -x ".venv/bin/python" ]; then
    PY=".venv/bin/python"
elif [ -x ".venv/Scripts/python.exe" ]; then
    PY=".venv/Scripts/python.exe"
fi

if [ -z "$PY" ]; then
    echo "Python venv interpreter not found."
    echo "Tried:"
    echo "  .venv/bin/python"
    echo "  .venv/Scripts/python.exe"
    echo "Create/install with:"
    echo "  python3 -m venv .venv"
    echo "  .venv/bin/python -m pip install -r requirements.txt  # macOS/Linux"
    echo "  .venv/Scripts/python.exe -m pip install -r requirements.txt  # Windows"
    exit 1
fi

mkdir -p logs

node_endpoint() {
    local node_id="$1"
    "$PY" - "$CONFIG" "$node_id" <<'PYCODE'
import json
import sys
from pathlib import Path

cfg_path = Path(sys.argv[1])
node_id = sys.argv[2]
cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
node = cfg["nodes"][node_id]
print(f"{node['host']}:{int(node['port'])}")
PYCODE
}

endpoint_is_ready() {
    local host="$1"
    local port="$2"
    if command -v nc >/dev/null 2>&1; then
        nc -z "$host" "$port" >/dev/null 2>&1
        return $?
    fi

    "$PY" - "$host" "$port" <<'PYCODE' >/dev/null 2>&1
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(0.5)
try:
    s.connect((host, port))
except Exception:
    raise SystemExit(1)
finally:
    s.close()
PYCODE
}

cleanup() {
    echo "Stopping yellow team nodes..."
    kill "${PIDS[@]}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

PIDS=()

for NODE in C E F G I; do
    "$PY" python/server/node.py --id "$NODE" --config "$CONFIG" \
        > "logs/node_${NODE}.log" 2>&1 &
    pid=$!
    PIDS+=("$pid")
    echo "Started node $NODE (pid=$pid)"
done

echo "Yellow team running: C E F G I"
echo "Logs in logs/node_*.log"

# Wait for all yellow nodes to bind
for NODE in C E F G I; do
    endpoint="$(node_endpoint "$NODE")"
    host="${endpoint%:*}"
    port="${endpoint##*:}"
    WAITED=0
    while ! endpoint_is_ready "$host" "$port"; do
        sleep 1
        WAITED=$((WAITED + 1))
        if [ "$WAITED" -ge "$WAIT_SECONDS" ]; then
            echo "Timeout waiting for $NODE at $host:$port"
            exit 1
        fi
    done
    echo "  $NODE ready at $host:$port"
done

echo "All yellow nodes ready."
wait
