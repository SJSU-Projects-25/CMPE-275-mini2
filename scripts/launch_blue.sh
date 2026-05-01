#!/bin/bash
# Machine 1: nodes A (C++), B, D, H (Python)
# Update config/topology.json with real IPs before running:
#   Machine 1 nodes: A, B, D, H
#   Machine 2 nodes: C, E, F, G, I
set -euo pipefail

CONFIG="${1:-config/topology.json}"
BUILD_DIR="cpp/build"

if [ ! -f "$CONFIG" ]; then
    echo "Config not found: $CONFIG"
    exit 1
fi

mkdir -p logs

cleanup() {
    echo "Stopping blue team nodes..."
    kill "${PIDS[@]}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

PIDS=()

# Start Python nodes B, D, H
for NODE in B D H; do
    python3 python/server/node.py --id "$NODE" --config "$CONFIG" \
        > "logs/node_${NODE}.log" 2>&1 &
    PIDS+=($!)
    echo "Started node $NODE (pid=${PIDS[-1]})"
done

# Wait for Python nodes to be ready
echo "Waiting for Python nodes to bind..."
for PORT in 50052 50054 50058; do
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

# Start C++ node A
if [ ! -f "$BUILD_DIR/node_server" ]; then
    echo "C++ server not found at $BUILD_DIR/node_server. Build with: cmake --build $BUILD_DIR"
    exit 1
fi

"$BUILD_DIR/node_server" --id A --config "$CONFIG" > logs/node_A.log 2>&1 &
PIDS+=($!)
echo "Started node A (pid=${PIDS[-1]})"

echo "Blue team running: A B D H"
echo "Logs in logs/node_*.log"
wait
