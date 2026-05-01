#!/bin/bash
# Machine 2: nodes C, E, F, G, I (all Python)
# Update config/topology.json with real IPs before running:
#   Machine 1 nodes: A, B, D, H
#   Machine 2 nodes: C, E, F, G, I
set -euo pipefail

CONFIG="${1:-config/topology.json}"

if [ ! -f "$CONFIG" ]; then
    echo "Config not found: $CONFIG"
    exit 1
fi

mkdir -p logs

cleanup() {
    echo "Stopping yellow team nodes..."
    kill "${PIDS[@]}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

PIDS=()

for NODE in C E F G I; do
    python3 python/server/node.py --id "$NODE" --config "$CONFIG" \
        > "logs/node_${NODE}.log" 2>&1 &
    PIDS+=($!)
    echo "Started node $NODE (pid=${PIDS[-1]})"
done

echo "Yellow team running: C E F G I"
echo "Logs in logs/node_*.log"

# Wait for all yellow nodes to bind
for PORT in 50053 50055 50056 50057 50059; do
    WAITED=0
    while ! nc -z 127.0.0.1 "$PORT" 2>/dev/null; do
        sleep 1
        WAITED=$((WAITED + 1))
        if [ "$WAITED" -ge 30 ]; then
            echo "Timeout waiting for port $PORT"
            exit 1
        fi
    done
    echo "  port $PORT ready"
done

echo "All yellow nodes ready."
wait
