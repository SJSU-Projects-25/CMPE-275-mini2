#!/bin/bash
set -euo pipefail

CONFIG="config/topology.json"
CLIENT="./cpp/build/client"

if [ ! -f "$CLIENT" ]; then
    echo "Client binary not found at $CLIENT. Build first with: cmake --build cpp/build"
    exit 1
fi

if [ ! -f "$CONFIG" ]; then
    echo "Config not found at $CONFIG"
    exit 1
fi

LOG_DIR="logs/concurrent_test"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "Launching 3 concurrent clients..."

"$CLIENT" --config "$CONFIG" --query-type fare --min 10 --max 50 \
    > "$LOG_DIR/client_fare_$TIMESTAMP.log" 2>&1 &
PID1=$!

"$CLIENT" --config "$CONFIG" --query-type distance --min 1 --max 5 \
    > "$LOG_DIR/client_distance_$TIMESTAMP.log" 2>&1 &
PID2=$!

"$CLIENT" --config "$CONFIG" --query-type time --start-time 1577836800 --end-time 1580515200 \
    > "$LOG_DIR/client_time_$TIMESTAMP.log" 2>&1 &
PID3=$!

echo "  fare query    pid=$PID1"
echo "  distance query pid=$PID2"
echo "  time query    pid=$PID3"

FAILED=0
wait $PID1 || { echo "fare query failed"; FAILED=1; }
wait $PID2 || { echo "distance query failed"; FAILED=1; }
wait $PID3 || { echo "time query failed"; FAILED=1; }

echo ""
echo "Results:"
for LOG in "$LOG_DIR"/client_*_"$TIMESTAMP".log; do
    echo "--- $LOG ---"
    grep -E "^(query_type|total_records_received|elapsed_ms|total_chunks):" "$LOG" || true
done

if [ "$FAILED" -eq 0 ]; then
    echo ""
    echo "All 3 clients completed successfully."
else
    echo ""
    echo "One or more clients failed. Check logs in $LOG_DIR/"
    exit 1
fi
