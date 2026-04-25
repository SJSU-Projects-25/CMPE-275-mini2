#!/usr/bin/env bash
# Full tree on one host — Python B..I, C++ A, then client; cleanup on exit.
#
# Usage:
#   bash scripts/launch_all.sh              # default client query (wide distance)
#   bash scripts/launch_all.sh --build      # cmake build node_server + client first
#   bash scripts/launch_all.sh -- --config config/topology.json --query-type aggregate
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

NODE_A_PID=""

cleanup() {
  if [[ -n "${NODE_A_PID}" ]]; then
    kill "${NODE_A_PID}" 2>/dev/null || true
    wait "${NODE_A_PID}" 2>/dev/null || true
    NODE_A_PID=""
  fi
  if [[ -x "${ROOT}/scripts/stop_python_nodes.sh" ]]; then
    bash "${ROOT}/scripts/stop_python_nodes.sh" || true
  fi
}

trap 'cleanup' EXIT INT TERM

wait_tcp() {
  local host="$1"
  local port="$2"
  local label="${3:-${host}:${port}}"
  local deadline=$((SECONDS + 45))
  while (( SECONDS < deadline )); do
    if { echo >/dev/tcp/${host}/${port}; } >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.15
  done
  echo "error: timeout waiting for ${label}" >&2
  return 1
}

BUILD=0
CLIENT_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --build)
      BUILD=1
      shift
      ;;
    --help | -h)
      echo "usage: bash scripts/launch_all.sh [--build] [-- <client args...>]"
      echo "  default client: --config config/topology.json --query-type distance --min 0 --max 10000"
      exit 0
      ;;
    --)
      shift
      CLIENT_ARGS+=("$@")
      break
      ;;
    *)
      CLIENT_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ ${#CLIENT_ARGS[@]} -eq 0 ]]; then
  CLIENT_ARGS=(--config config/topology.json --query-type distance --min 0 --max 10000)
fi

if [[ "${BUILD}" -eq 1 ]]; then
  echo "[launch_all] configuring and building C++ (node_server, client)..."
  cmake -S cpp -B cpp/build
  cmake --build cpp/build --target node_server client
fi

if [[ ! -x "${ROOT}/cpp/build/node_server" ]] || [[ ! -x "${ROOT}/cpp/build/client" ]]; then
  echo "error: missing cpp/build/node_server or cpp/build/client" >&2
  echo "Run: bash scripts/launch_all.sh --build   or   cmake -S cpp -B cpp/build && cmake --build cpp/build" >&2
  exit 1
fi

PY="${ROOT}/.venv/bin/python"
if [[ ! -x "${PY}" ]]; then
  echo "error: ${PY} not found. Create venv and: .venv/bin/python -m pip install -r requirements.txt" >&2
  exit 1
fi

echo "[launch_all] starting Python nodes B..I..."
bash "${ROOT}/scripts/launch_python_nodes.sh"

for port in 50052 50053 50054 50055 50056 50057 50058 50059; do
  wait_tcp 127.0.0.1 "${port}" "python node port ${port}"
done

mkdir -p logs
echo "[launch_all] starting C++ node A..."
"${ROOT}/cpp/build/node_server" --id A >>"${ROOT}/logs/node_A.log" 2>&1 &
NODE_A_PID=$!

wait_tcp 127.0.0.1 50051 "C++ entry node A"

echo "[launch_all] running client: ${CLIENT_ARGS[*]}"
echo "---"
"${ROOT}/cpp/build/client" "${CLIENT_ARGS[@]}"
echo "---"
echo "[launch_all] client finished; stopping cluster..."
