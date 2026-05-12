#!/usr/bin/env bash
# Measure end-to-end client elapsed_ms with a long-lived cluster.
#
# Usage (from repo root):
#   bash scripts/bench_cluster_elapsed.sh [iterations] [-- extra client args...]
#
# Modes:
#   Default: temp topology (stream_up=false, large chunk_size) — stable micro-benchmarks.
#   MINI2_BENCH_USE_REPO_CONFIG=1 — use config/topology.json unchanged (production-like:
#     stream_up, chunk_size, fairness). Adds pacing between client runs to avoid chunk races.
#
# Examples:
#   bash scripts/bench_cluster_elapsed.sh 25
#   MINI2_BENCH_USE_REPO_CONFIG=1 bash scripts/bench_cluster_elapsed.sh 12 -- --query-type distance --min 1 --max 5
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ITER=20
CLIENT_EXTRA=()
if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
  ITER="$1"
  shift
fi
if [[ "${1:-}" == "--" ]]; then
  shift
  CLIENT_EXTRA+=("$@")
fi

if [[ ! -x "${ROOT}/cpp/build/client" ]] || [[ ! -x "${ROOT}/cpp/build/node_server" ]]; then
  echo "Build first: cmake -S cpp -B cpp/build && cmake --build cpp/build" >&2
  exit 1
fi

PY="${ROOT}/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  echo "Need ${PY}" >&2
  exit 1
fi

USE_REPO="${MINI2_BENCH_USE_REPO_CONFIG:-}"
WAIT_PORT_SEC="${MINI2_BENCH_WAIT_PORT_SEC:-600}"
TMP_CFG=""

if [[ "$USE_REPO" == "1" ]]; then
  INTER_SLEEP="${MINI2_BENCH_INTER_REQUEST_SLEEP:-0.9}"
else
  INTER_SLEEP=0
fi

# Clean slate: stale node_server or Python workers break chunk / config state.
pkill -f "${ROOT}/cpp/build/node_server" 2>/dev/null || true
bash "${ROOT}/scripts/stop_python_nodes.sh" 2>/dev/null || true
sleep 0.5

if [[ "$USE_REPO" == "1" ]]; then
  CFG_FILE="${ROOT}/config/topology.json"
  echo "bench: MINI2_BENCH_USE_REPO_CONFIG=1 (production-like topology from ${CFG_FILE})"
  echo "bench: inter_request_sleep=${INTER_SLEEP}s wait_port_sec=${WAIT_PORT_SEC}"
else
  TMP_CFG="$(mktemp "${TMPDIR:-/tmp}/mini2-bench.XXXXXX")"
  CFG_FILE="$TMP_CFG"
  "${PY}" - "$ROOT/config/topology.json" "$TMP_CFG" <<'PYCODE'
import json
import sys

src, dst = sys.argv[1], sys.argv[2]
cfg = json.load(open(src))
cfg["stream_up"] = False
cfg["chunk_size"] = max(int(cfg.get("chunk_size", 500)), 50_000)
json.dump(cfg, open(dst, "w"), indent=2)
PYCODE
  echo "bench: tuned temp config (stream_up=false, chunk_size>=50000)"
fi

cleanup() {
  [[ -n "${TMP_CFG}" ]] && rm -f "${TMP_CFG}"
  kill "${NODE_A_PID:-}" 2>/dev/null || true
  wait "${NODE_A_PID:-}" 2>/dev/null || true
  bash "${ROOT}/scripts/stop_python_nodes.sh" || true
  pkill -f "${ROOT}/cpp/build/node_server" 2>/dev/null || true
  sleep 0.3
}
trap cleanup EXIT INT TERM

wait_tcp() {
  local host="$1" port="$2"
  local deadline=$((SECONDS + WAIT_PORT_SEC))
  while (( SECONDS < deadline )); do
    if { echo >/dev/tcp/${host}/${port}; } >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  echo "timeout waiting for ${host}:${port} after ${WAIT_PORT_SEC}s" >&2
  return 1
}

mkdir -p "${ROOT}/logs"
PIDFILE="${ROOT}/logs/python_node_pids.txt"
: >"$PIDFILE"
for id in B C D E F G H I; do
  "${PY}" "${ROOT}/python/server/node.py" --id "${id}" --config "${CFG_FILE}" \
    >>"${ROOT}/logs/node_${id}.log" 2>&1 &
  echo $! >>"$PIDFILE"
done

echo "bench: waiting for Python nodes to bind (large partitions may take minutes)..."
for port in 50052 50053 50054 50055 50056 50057 50058 50059; do
  wait_tcp 127.0.0.1 "${port}"
  echo "  port ${port} ready"
done

"${ROOT}/cpp/build/node_server" --id A --config "${CFG_FILE}" >>"${ROOT}/logs/node_A.log" 2>&1 &
NODE_A_PID=$!
echo "bench: waiting for entry node A..."
wait_tcp 127.0.0.1 50051
echo "bench: cluster ready, running client iterations..."

CFG=(--config "${CFG_FILE}")
if [[ ${#CLIENT_EXTRA[@]} -eq 0 ]]; then
  CLIENT_EXTRA=(--query-type distance --min 0 --max 10000)
fi

SUM=0
MIN="999999"
MAX=0
OK=0
for ((i = 1; i <= ITER; i++)); do
  if [[ "$i" -gt 1 && "$INTER_SLEEP" != "0" ]]; then
    sleep "$INTER_SLEEP"
  fi
  OUT="$("${ROOT}/cpp/build/client" "${CFG[@]}" "${CLIENT_EXTRA[@]}" 2>&1)" || true
  ELAPSED="$(echo "$OUT" | awk '/elapsed_ms:/{print $2; exit}')"
  if [[ -z "${ELAPSED}" ]]; then
    echo "run $i: parse failed" >&2
    echo "$OUT" >&2
    continue
  fi
  OK=$((OK + 1))
  SUM="$(awk -v s="$SUM" -v e="$ELAPSED" 'BEGIN{printf "%.6f", s+e}')"
  MIN="$(awk -v m="$MIN" -v e="$ELAPSED" 'BEGIN{if (e<m) print e; else print m}')"
  MAX="$(awk -v m="$MAX" -v e="$ELAPSED" 'BEGIN{if (e>m) print e; else print m}')"
done

if [[ "$OK" -eq 0 ]]; then
  echo "No successful samples." >&2
  exit 1
fi

AVG="$(awk -v s="$SUM" -v n="$OK" 'BEGIN{printf "%.4f", s/n}')"
echo "bench_cluster_elapsed: iterations_requested=${ITER} ok_samples=${OK}"
if [[ "$USE_REPO" == "1" ]]; then
  echo "  bench_config: repo topology.json (stream_up / chunk_size as in repo)"
else
  echo "  bench_config: temp (stream_up=false chunk_size>=50000)"
fi
echo "  elapsed_ms avg=${AVG} min=${MIN} max=${MAX}"
