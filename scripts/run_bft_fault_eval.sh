#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CONFIG="$ROOT/config/topology.json"
BACKUP="$ROOT/logs/topology.bak.$$.json"
OUTDIR="$ROOT/logs/bft_eval"
mkdir -p "$ROOT/logs" "$OUTDIR"
cp "$CONFIG" "$BACKUP"

cleanup() {
  cp "$BACKUP" "$CONFIG" 2>/dev/null || true
  rm -f "$BACKUP"
}
trap cleanup EXIT INT TERM

set_config() {
  local mode="$1"
  local fi_enabled="$2"
  local fi_mode="$3"
  local fi_delay="$4"
  local py_bool="False"
  if [[ "$fi_enabled" == "true" ]]; then
    py_bool="True"
  fi
  .venv/bin/python - <<PY
import json
p="$CONFIG"
obj=json.load(open(p))
obj["bft_mode"]="$mode"
fi=obj.setdefault("fault_injection", {})
fi["enabled"]=$py_bool
nodes=fi.setdefault("nodes", {})
g=nodes.setdefault("G", {})
g["mode"]="$fi_mode"
g["probability"]=1.0
g["delay_ms"]=$fi_delay
json.dump(obj, open(p,"w"), indent=2)
PY
}

run_case() {
  local label="$1"
  local mode="$2"
  local fi_enabled="$3"
  local fi_mode="$4"
  local fi_delay="$5"

  set_config "$mode" "$fi_enabled" "$fi_mode" "$fi_delay"
  rm -f logs/node_*.log
  local run_out="$OUTDIR/${label}.txt"
  bash scripts/launch_all.sh >"$run_out" 2>&1 || true

  local request_id total_records total_chunks elapsed bft_rejects failed_forwards fi_hits
  request_id="$( (rg "request_id:" "$run_out" -n || true) | awk '{print $2}' | tail -1 )"
  total_records="$( (rg "total_records_received:" "$run_out" -n || true) | awk '{print $2}' | tail -1 )"
  total_chunks="$( (rg "total_chunks:" "$run_out" -n || true) | awk '{print $2}' | tail -1 )"
  elapsed="$( (rg "elapsed_ms:" "$run_out" -n || true) | awk '{print $2}' | tail -1 )"
  bft_rejects="$( (rg -c "\\[A\\]\\[BFT\\].*rejected|\\[A\\]\\[BFT\\].*excluded" logs/node_A.log 2>/dev/null || true) | awk -F: '{if (NF==1) s+=$1; else s+=$2} END{print s+0}' )"
  failed_forwards="$( (rg -c "forward failed" logs/node_A.log 2>/dev/null || true) | awk -F: '{if (NF==1) s+=$1; else s+=$2} END{print s+0}' )"
  fi_hits="$( (rg -c "fault_injection mode=" logs/node_G.log 2>/dev/null || true) | awk -F: '{if (NF==1) s+=$1; else s+=$2} END{print s+0}' )"
  bft_rejects="${bft_rejects:-0}"
  failed_forwards="${failed_forwards:-0}"
  fi_hits="${fi_hits:-0}"

  printf "%-18s mode=%-4s fi=%-18s chunks=%-4s records=%-6s elapsed_ms=%-10s bft_signals=%-3s forward_fail=%-3s fi_hits=%-3s request=%s\n" \
    "$label" "$mode" "$fi_mode" "${total_chunks:-NA}" "${total_records:-NA}" "${elapsed:-NA}" \
    "$bft_rejects" "$failed_forwards" "$fi_hits" "${request_id:-NA}"
}

echo "Running BFT fault-eval scenarios..."
echo "Summary:"
run_case baseline off false none 0
run_case bft_lite_clean lite false none 0
run_case bft_lite_mutation lite true mutation 0
run_case bft_lite_delay lite true delay 250
run_case bft_lite_drop lite true drop 0
run_case bft_lite_wrongagg lite true wrong_aggregation 0
