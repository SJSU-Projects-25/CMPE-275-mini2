#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PYTHON_DIR = Path(__file__).resolve().parents[1]
if str(PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(PYTHON_DIR))

import grpc
import mini2_pb2
import mini2_pb2_grpc
from query_engine import QueryEngine
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
    neighbors: list[str]
    children: list[str]
    node_addresses: dict[str, str]
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
    parent = parent_map.get(node_id)
    children = sorted(
        candidate for candidate, candidate_parent in parent_map.items() if candidate_parent == node_id
    )
    return children, parent


def _derive_neighbors(node_id: str, edges: list[str]) -> list[str]:
    graph = _build_graph(edges)
    return sorted(graph.get(node_id, set()))


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
    neighbors = _derive_neighbors(node_id=node_id, edges=edges)
    children, _ = _derive_children(node_id=node_id, edges=edges, root="A")
    records = load_records(data_path)
    node_addresses: dict[str, str] = {}
    for name, cfg in nodes.items():
        node_addresses[name] = f"{cfg['host']}:{int(cfg['port'])}"

    return NodeContext(
        node_id=node_id,
        host=host,
        port=port,
        data_path=data_path,
        neighbors=neighbors,
        children=children,
        node_addresses=node_addresses,
        records=records,
    )


def _trip_record_to_proto(record: TripRecord) -> mini2_pb2.TripRecordMsg:
    return mini2_pb2.TripRecordMsg(
        vendor_id=record.vendor_id,
        pickup_timestamp=record.pickup_timestamp,
        dropoff_timestamp=record.dropoff_timestamp,
        passenger_count=record.passenger_count,
        trip_distance=record.trip_distance,
        rate_code_id=record.rate_code_id,
        store_and_fwd_flag=record.store_and_fwd_flag,
        pu_location_id=record.pu_location_id,
        do_location_id=record.do_location_id,
        payment_type=record.payment_type,
        fare_amount=record.fare_amount,
        extra=record.extra,
        mta_tax=record.mta_tax,
        tip_amount=record.tip_amount,
        tolls_amount=record.tolls_amount,
        improvement_surcharge=record.improvement_surcharge,
        total_amount=record.total_amount,
    )


class NodeService(mini2_pb2_grpc.NodeServiceServicer):
    def __init__(self, context: NodeContext) -> None:
        self.context = context
        self.engine = QueryEngine(context.records)

    def _forward_to_child(
        self, child_id: str, request: mini2_pb2.ForwardRequest
    ) -> mini2_pb2.ForwardResponse:
        target = self.context.node_addresses[child_id]
        with grpc.insecure_channel(target) as channel:
            stub = mini2_pb2_grpc.NodeServiceStub(channel)
            child_request = mini2_pb2.ForwardRequest(
                request_id=request.request_id,
                origin_node=self.context.node_id,
                query=request.query,
            )
            return stub.ForwardQuery(child_request, timeout=10.0)

    def ForwardQuery(
        self, request: mini2_pb2.ForwardRequest, rpc_context: grpc.ServicerContext
    ) -> mini2_pb2.ForwardResponse:
        local_result = self.engine.execute_request(request.query)
        merged_records = [_trip_record_to_proto(record) for record in local_result.records]
        merged_sum = local_result.aggregation_sum
        merged_count = local_result.aggregation_count

        forward_targets = [
            child
            for child in self.context.children
            if child != request.origin_node
        ]
        if forward_targets:
            print(
                f"[{self.context.node_id}] request={request.request_id} "
                f"fan-out -> {', '.join(forward_targets)}"
            , flush=True)

        with ThreadPoolExecutor(max_workers=max(1, len(forward_targets))) as pool:
            futures = {
                pool.submit(self._forward_to_child, child_id, request): child_id
                for child_id in forward_targets
            }
            for future in as_completed(futures):
                child_id = futures[future]
                try:
                    child_response = future.result()
                except Exception as exc:  # pragma: no cover - runtime/network branch
                    print(
                        f"[{self.context.node_id}] request={request.request_id} "
                        f"child={child_id} forward failed: {exc}"
                    , flush=True)
                    continue
                if child_response.request_id != request.request_id:
                    print(
                        f"[{self.context.node_id}] request={request.request_id} "
                        f"child={child_id} returned mismatched request_id="
                        f"{child_response.request_id}"
                    , flush=True)
                    continue
                merged_records.extend(child_response.records)
                merged_sum += child_response.aggregation_sum
                merged_count += child_response.aggregation_count

        merged_avg = merged_sum / merged_count if merged_count else 0.0
        print(
            f"[{self.context.node_id}] request={request.request_id} merged_count="
            f"{merged_count} (local={local_result.aggregation_count})"
        , flush=True)
        return mini2_pb2.ForwardResponse(
            request_id=request.request_id,
            source_node=self.context.node_id,
            records=merged_records,
            aggregation_sum=merged_sum,
            aggregation_avg=merged_avg,
            aggregation_count=merged_count,
        )

    def SubmitQuery(
        self, request: mini2_pb2.QueryRequest, rpc_context: grpc.ServicerContext
    ) -> mini2_pb2.ChunkResponse:
        rpc_context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        rpc_context.set_details("SubmitQuery is implemented in Step 5.")
        return mini2_pb2.ChunkResponse()

    def FetchChunk(
        self, request: mini2_pb2.ChunkRequest, rpc_context: grpc.ServicerContext
    ) -> mini2_pb2.ChunkResponse:
        rpc_context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        rpc_context.set_details("FetchChunk is implemented in later steps.")
        return mini2_pb2.ChunkResponse()

    def CancelQuery(
        self, request: mini2_pb2.CancelRequest, rpc_context: grpc.ServicerContext
    ) -> mini2_pb2.CancelResponse:
        rpc_context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        rpc_context.set_details("CancelQuery is implemented in later steps.")
        return mini2_pb2.CancelResponse(acknowledged=False)


def serve(context: NodeContext) -> None:
    server = grpc.server(ThreadPoolExecutor(max_workers=16))
    mini2_pb2_grpc.add_NodeServiceServicer_to_server(NodeService(context), server)
    bind_target = f"{context.host}:{context.port}"
    server.add_insecure_port(bind_target)
    server.start()
    print(f"Node {context.node_id} gRPC server listening on {bind_target}", flush=True)
    server.wait_for_termination()


def main() -> int:
    args = build_parser().parse_args()
    config_path = Path(args.config).resolve()
    if not config_path.exists():
        print("Config file not found. Verify --config path.", flush=True)
        return 1

    try:
        context = load_node_context(node_id=args.id, config_path=config_path)
    except Exception as exc:
        print(f"Node startup failed: {exc}", flush=True)
        return 1

    children_text = ", ".join(context.children) if context.children else "(none)"
    print(f"Node {context.node_id} startup complete", flush=True)
    print(f"  host: {context.host}", flush=True)
    print(f"  port: {context.port}", flush=True)
    print(f"  children: {children_text}", flush=True)
    print(f"  data_file: {context.data_path.as_posix()}", flush=True)
    print(f"  loaded_records: {len(context.records)}", flush=True)
    serve(context)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
