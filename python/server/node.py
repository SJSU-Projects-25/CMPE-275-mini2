#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PYTHON_DIR = Path(__file__).resolve().parents[1]
if str(PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(PYTHON_DIR))

from trip_record import TripRecord


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Mini2 Python node runtime (Step 3)")
    parser.add_argument("--id", required=True, help="Node identifier, e.g. B")
    parser.add_argument(
        "--config",
        default="config/topology.json",
        help="Path to topology configuration JSON",
    )
    return parser


@dataclass(frozen=True)
class NodeContext:
    node_id: str
    host: str
    port: int
    data_path: Path
    children: list[str]
    records: list[TripRecord]


def _resolve_path(raw_path: str, config_path: Path) -> Path:
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return candidate
    config_relative = (config_path.parent / candidate).resolve()
    if config_relative.exists():
        return config_relative
    return (Path.cwd() / candidate).resolve()


def _build_graph(edges: list[str]) -> dict[str, set[str]]:
    graph: dict[str, set[str]] = {}
    for edge in edges:
        if len(edge) < 2:
            continue
        left, right = edge[0], edge[1]
        graph.setdefault(left, set()).add(right)
        graph.setdefault(right, set()).add(left)
    return graph


def _build_parent_map(graph: dict[str, set[str]], root: str = "A") -> dict[str, str | None]:
    if root not in graph:
        return {}
    parent: dict[str, str | None] = {root: None}
    queue: deque[str] = deque([root])
    while queue:
        current = queue.popleft()
        for neighbor in sorted(graph[current]):
            if neighbor in parent:
                continue
            parent[neighbor] = current
            queue.append(neighbor)
    return parent


def _derive_children(
    node_id: str, edges: list[str], root: str = "A"
) -> tuple[list[str], str | None]:
    graph = _build_graph(edges)
    parent_map = _build_parent_map(graph, root=root)
    neighbors = set(graph.get(node_id, set()))
    parent = parent_map.get(node_id)
    if parent is not None and parent in neighbors:
        neighbors.remove(parent)
    return sorted(neighbors), parent


def _to_int(raw_value: str) -> int:
    text = raw_value.strip()
    return int(text) if text else 0


def _to_float(raw_value: str) -> float:
    text = raw_value.strip()
    return float(text) if text else 0.0


def _to_bool(raw_value: str) -> bool:
    text = raw_value.strip().upper()
    return text in {"Y", "YES", "TRUE", "1"}


def _parse_timestamp(raw_value: str) -> int:
    # Match Mini 1 format like: "2018 Dec 31 11:47:38 PM"
    from datetime import datetime, timezone

    text = raw_value.strip()
    if not text:
        return 0
    try:
        dt = datetime.strptime(text, "%Y %b %d %I:%M:%S %p")
        return int(dt.replace(tzinfo=timezone.utc).timestamp())
    except ValueError:
        pass
    try:
        dt = datetime.strptime(text, "%m/%d/%Y %I:%M:%S %p")
        return int(dt.replace(tzinfo=timezone.utc).timestamp())
    except ValueError:
        pass
    try:
        dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        return int(dt.replace(tzinfo=timezone.utc).timestamp())
    except ValueError:
        return 0


def load_records(data_path: Path) -> list[TripRecord]:
    records: list[TripRecord] = []
    with data_path.open("r", newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            pickup_ts = _parse_timestamp(row.get("tpep_pickup_datetime", ""))
            dropoff_ts = _parse_timestamp(row.get("tpep_dropoff_datetime", ""))
            if pickup_ts <= 0 or dropoff_ts <= pickup_ts:
                continue
            records.append(
                TripRecord(
                    vendor_id=_to_int(row.get("VendorID", "")),
                    pickup_timestamp=pickup_ts,
                    dropoff_timestamp=dropoff_ts,
                    passenger_count=_to_int(row.get("passenger_count", "")),
                    trip_distance=_to_float(row.get("trip_distance", "")),
                    rate_code_id=_to_int(row.get("RatecodeID", "")),
                    store_and_fwd_flag=_to_bool(row.get("store_and_fwd_flag", "")),
                    pu_location_id=_to_int(row.get("PULocationID", "")),
                    do_location_id=_to_int(row.get("DOLocationID", "")),
                    payment_type=_to_int(row.get("payment_type", "")),
                    fare_amount=_to_float(row.get("fare_amount", "")),
                    extra=_to_float(row.get("extra", "")),
                    mta_tax=_to_float(row.get("mta_tax", "")),
                    tip_amount=_to_float(row.get("tip_amount", "")),
                    tolls_amount=_to_float(row.get("tolls_amount", "")),
                    improvement_surcharge=_to_float(row.get("improvement_surcharge", "")),
                    total_amount=_to_float(row.get("total_amount", "")),
                )
            )
    return records


def load_node_context(node_id: str, config_path: Path) -> NodeContext:
    with config_path.open("r", encoding="utf-8") as config_file:
        config: dict[str, Any] = json.load(config_file)

    nodes = config.get("nodes", {})
    if node_id not in nodes:
        raise ValueError(f"Node id '{node_id}' not found in config: {config_path}")

    node_cfg = nodes[node_id]
    host = node_cfg["host"]
    port = int(node_cfg["port"])
    raw_data_path = node_cfg["data"]
    data_path = _resolve_path(raw_data_path, config_path)

    if not data_path.exists():
        raise FileNotFoundError(f"Data file not found for node {node_id}: {data_path}")

    edges = config.get("edges", [])
    children, _ = _derive_children(node_id=node_id, edges=edges, root="A")
    records = load_records(data_path)

    return NodeContext(
        node_id=node_id,
        host=host,
        port=port,
        data_path=data_path,
        children=children,
        records=records,
    )


def main() -> int:
    args = build_parser().parse_args()
    config_path = Path(args.config).resolve()
    if not config_path.exists():
        print("Config file not found. Verify --config path.")
        return 1

    try:
        context = load_node_context(node_id=args.id, config_path=config_path)
    except Exception as exc:
        print(f"Node startup failed: {exc}")
        return 1

    children_text = ", ".join(context.children) if context.children else "(none)"
    print(f"Node {context.node_id} startup complete")
    print(f"  host: {context.host}")
    print(f"  port: {context.port}")
    print(f"  children: {children_text}")
    print(f"  data_file: {context.data_path.as_posix()}")
    print(f"  loaded_records: {len(context.records)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
