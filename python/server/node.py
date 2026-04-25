#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
import uuid
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import hashlib

PYTHON_DIR = Path(__file__).resolve().parents[1]
if str(PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(PYTHON_DIR))

import grpc
import mini2_pb2
import mini2_pb2_grpc
from query_engine import QueryEngine
from trip_record import TripRecord


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Mini2 Python node runtime")
    parser.add_argument("--id", required=True, help="Node identifier, e.g. B")
    parser.add_argument(
        "--config",
        default="config/topology.json",
        help="Path to topology configuration JSON",
    )
    parser.add_argument(
        "--submit-local-only",
        action="store_true",
        help="Handle SubmitQuery with local data only (no child forwarding).",
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
    submit_local_only: bool
    chunk_size: int
    bft_mode: str
    bft_auth_type: str
    bft_key: str
    fault_injection_enabled: bool
    fault_injection_nodes: dict[str, dict[str, Any]]


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


def load_node_context(
    node_id: str, config_path: Path, submit_local_only: bool = False
) -> NodeContext:
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

    raw_chunk = config.get("chunk_size", 500)
    chunk_size = int(raw_chunk) if isinstance(raw_chunk, (int, float)) else 500
    if chunk_size < 1:
        chunk_size = 500

    bft_mode = str(config.get("bft_mode", "off"))
    bft_cfg = config.get("bft", {}) if isinstance(config.get("bft", {}), dict) else {}
    auth_cfg = bft_cfg.get("auth", {}) if isinstance(bft_cfg.get("auth", {}), dict) else {}
    bft_auth_type = str(auth_cfg.get("type", "off"))
    keys_cfg = auth_cfg.get("keys", {}) if isinstance(auth_cfg.get("keys", {}), dict) else {}
    bft_key = str(keys_cfg.get(node_id, ""))
    fi_cfg = config.get("fault_injection", {}) if isinstance(config.get("fault_injection", {}), dict) else {}
    fi_enabled = bool(fi_cfg.get("enabled", False))
    fi_nodes = fi_cfg.get("nodes", {}) if isinstance(fi_cfg.get("nodes", {}), dict) else {}

    return NodeContext(
        node_id=node_id,
        host=host,
        port=port,
        data_path=data_path,
        neighbors=neighbors,
        children=children,
        node_addresses=node_addresses,
        records=records,
        submit_local_only=submit_local_only,
        chunk_size=chunk_size,
        bft_mode=bft_mode,
        bft_auth_type=bft_auth_type,
        bft_key=bft_key,
        fault_injection_enabled=fi_enabled,
        fault_injection_nodes=fi_nodes,
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
                bft_meta=request.bft_meta,
            )
            return stub.ForwardQuery(child_request, timeout=10.0)

    @staticmethod
    def _build_payload_hash(response: mini2_pb2.ForwardResponse) -> str:
        payload = [
            response.request_id,
            response.source_node,
            f"{response.aggregation_sum}",
            f"{response.aggregation_avg}",
            f"{response.aggregation_count}",
            f"{len(response.records)}",
        ]
        for rec in response.records:
            payload.extend(
                [
                    str(rec.vendor_id),
                    str(rec.pickup_timestamp),
                    str(rec.dropoff_timestamp),
                    str(rec.passenger_count),
                    str(rec.trip_distance),
                    str(rec.rate_code_id),
                    str(int(rec.store_and_fwd_flag)),
                    str(rec.pu_location_id),
                    str(rec.do_location_id),
                    str(rec.payment_type),
                    str(rec.fare_amount),
                    str(rec.extra),
                    str(rec.mta_tax),
                    str(rec.tip_amount),
                    str(rec.tolls_amount),
                    str(rec.improvement_surcharge),
                    str(rec.total_amount),
                ]
            )
        return hashlib.sha256("|".join(payload).encode("utf-8")).hexdigest()

    def _attach_bft_meta(self, response: mini2_pb2.ForwardResponse) -> mini2_pb2.ForwardResponse:
        if self.context.bft_mode != "lite":
            return response
        payload_hash = self._build_payload_hash(response)
        auth_source = (
            f"{self.context.node_id}|{response.request_id}|{payload_hash}|{self.context.bft_key}"
        )
        auth_tag = hashlib.sha256(auth_source.encode("utf-8")).hexdigest()
        response.bft_meta.node_id = self.context.node_id
        response.bft_meta.payload_hash = payload_hash
        response.bft_meta.auth_tag = auth_tag
        response.bft_meta.nonce = int(time.time_ns() & 0xFFFFFFFFFFFFFFFF)
        response.bft_meta.timestamp_ms = int(time.time() * 1000)
        response.bft_meta.algorithm = "hmac-sha256"
        return response

    def _apply_fault_injection(
        self, response: mini2_pb2.ForwardResponse, rpc_context: grpc.ServicerContext | None
    ) -> mini2_pb2.ForwardResponse | None:
        if not self.context.fault_injection_enabled:
            return response
        node_cfg = self.context.fault_injection_nodes.get(self.context.node_id)
        if not isinstance(node_cfg, dict):
            return response
        probability = float(node_cfg.get("probability", 1.0))
        if probability < 1.0 and random.random() > probability:
            return response
        mode = str(node_cfg.get("mode", "none"))
        delay_ms = int(node_cfg.get("delay_ms", 0))
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)
        if mode == "drop":
            if rpc_context is not None:
                rpc_context.set_code(grpc.StatusCode.UNAVAILABLE)
                rpc_context.set_details("fault injection drop")
            print(f"[{self.context.node_id}] fault_injection mode=drop", flush=True)
            return None
        if mode == "mutation":
            response.bft_meta.node_id = f"{self.context.node_id}-MUTATED"
            print(f"[{self.context.node_id}] fault_injection mode=mutation", flush=True)
            return response
        if mode == "wrong_aggregation":
            response.aggregation_sum = response.aggregation_sum + 1000.0
            if response.aggregation_count > 0:
                response.aggregation_avg = response.aggregation_sum / response.aggregation_count
            print(f"[{self.context.node_id}] fault_injection mode=wrong_aggregation", flush=True)
            return response
        if mode == "delay":
            print(
                f"[{self.context.node_id}] fault_injection mode=delay delay_ms={delay_ms}",
                flush=True,
            )
            return response
        return response

    def _run_query_and_maybe_forward(
        self,
        request_id: str,
        query: mini2_pb2.QueryRequest,
        origin_node: str,
        allow_forwarding: bool,
    ) -> mini2_pb2.ForwardResponse:
        local_result = self.engine.execute_request(query)
        merged_records = [_trip_record_to_proto(record) for record in local_result.records]
        merged_sum = local_result.aggregation_sum
        merged_count = local_result.aggregation_count

        forward_targets = [
            child for child in self.context.children if child != origin_node
        ] if allow_forwarding else []
        if forward_targets:
            print(
                f"[{self.context.node_id}] request={request_id} "
                f"fan-out -> {', '.join(forward_targets)}",
                flush=True,
            )

        with ThreadPoolExecutor(max_workers=max(1, len(forward_targets))) as pool:
            futures = {
                pool.submit(
                    self._forward_to_child,
                    child_id,
                    mini2_pb2.ForwardRequest(
                        request_id=request_id,
                        origin_node=self.context.node_id,
                        query=query,
                    ),
                ): child_id
                for child_id in forward_targets
            }
            for future in as_completed(futures):
                child_id = futures[future]
                try:
                    child_response = future.result()
                except Exception as exc:  # pragma: no cover - runtime/network branch
                    print(
                        f"[{self.context.node_id}] request={request_id} "
                        f"child={child_id} forward failed: {exc}",
                        flush=True,
                    )
                    continue
                if child_response.request_id != request_id:
                    print(
                        f"[{self.context.node_id}] request={request_id} "
                        f"child={child_id} returned mismatched request_id="
                        f"{child_response.request_id}",
                        flush=True,
                    )
                    continue
                merged_records.extend(child_response.records)
                merged_sum += child_response.aggregation_sum
                merged_count += child_response.aggregation_count

        merged_avg = merged_sum / merged_count if merged_count else 0.0
        print(
            f"[{self.context.node_id}] request={request_id} merged_count="
            f"{merged_count} (local={local_result.aggregation_count})",
            flush=True,
        )
        response = mini2_pb2.ForwardResponse(
            request_id=request_id,
            source_node=self.context.node_id,
            records=merged_records,
            aggregation_sum=merged_sum,
            aggregation_avg=merged_avg,
            aggregation_count=merged_count,
        )
        return self._attach_bft_meta(response)

    def ForwardQuery(
        self, request: mini2_pb2.ForwardRequest, rpc_context: grpc.ServicerContext
    ) -> mini2_pb2.ForwardResponse:
        response = self._run_query_and_maybe_forward(
            request_id=request.request_id,
            query=request.query,
            origin_node=request.origin_node,
            allow_forwarding=True,
        )
        maybe_faulted = self._apply_fault_injection(response, rpc_context)
        if maybe_faulted is None:
            return mini2_pb2.ForwardResponse()
        return maybe_faulted

    def SubmitQuery(
        self, request: mini2_pb2.QueryRequest, rpc_context: grpc.ServicerContext
    ) -> mini2_pb2.ChunkResponse:
        effective_request_id = request.request_id or f"py-{uuid.uuid4().hex}"
        if request.request_id != effective_request_id:
            request = mini2_pb2.QueryRequest(
                request_id=effective_request_id,
                query_type=request.query_type,
                time_query=request.time_query,
                numeric_query=request.numeric_query,
                int_query=request.int_query,
                combined_query=request.combined_query,
            )

        forward_result = self._run_query_and_maybe_forward(
            request_id=effective_request_id,
            query=request,
            origin_node=self.context.node_id,
            allow_forwarding=not self.context.submit_local_only,
        )
        return mini2_pb2.ChunkResponse(
            request_id=effective_request_id,
            chunk_index=0,
            total_chunks=1,
            is_last=True,
            records=forward_result.records,
            aggregation_sum=forward_result.aggregation_sum,
            aggregation_avg=forward_result.aggregation_avg,
            aggregation_count=forward_result.aggregation_count,
            effective_chunk_size=self.context.chunk_size,
        )

    def FetchChunk(
        self, request: mini2_pb2.ChunkRequest, rpc_context: grpc.ServicerContext
    ) -> mini2_pb2.ChunkResponse:
        rpc_context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        rpc_context.set_details("FetchChunk is not supported on this node; use the C++ entry node.")
        return mini2_pb2.ChunkResponse()

    def CancelQuery(
        self, request: mini2_pb2.CancelRequest, rpc_context: grpc.ServicerContext
    ) -> mini2_pb2.CancelResponse:
        rpc_context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        rpc_context.set_details("CancelQuery is not supported on this node; use the C++ entry node.")
        return mini2_pb2.CancelResponse(acknowledged=False)


def serve(context: NodeContext) -> None:
    server = grpc.server(
        ThreadPoolExecutor(max_workers=16),
        options=(("grpc.so_reuseport", 0),),
    )
    mini2_pb2_grpc.add_NodeServiceServicer_to_server(NodeService(context), server)
    bind_target = f"{context.host}:{context.port}"
    bound_port = server.add_insecure_port(bind_target)
    if bound_port == 0:
        raise RuntimeError(f"Failed to bind gRPC server to {bind_target}")
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
        context = load_node_context(
            node_id=args.id,
            config_path=config_path,
            submit_local_only=args.submit_local_only,
        )
    except Exception as exc:
        print(f"Node startup failed: {exc}", flush=True)
        return 1

    children_text = ", ".join(context.children) if context.children else "(none)"
    print(f"Node {context.node_id} startup complete", flush=True)
    print(f"  host: {context.host}", flush=True)
    print(f"  port: {context.port}", flush=True)
    print(f"  children: {children_text}", flush=True)
    print(f"  submit_local_only: {context.submit_local_only}", flush=True)
    print(f"  bft_mode: {context.bft_mode}", flush=True)
    print(f"  fault_injection_enabled: {context.fault_injection_enabled}", flush=True)
    print(f"  data_file: {context.data_path.as_posix()}", flush=True)
    print(f"  loaded_records: {len(context.records)}", flush=True)
    serve(context)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
