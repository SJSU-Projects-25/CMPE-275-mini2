# Mini2 Handoff to Seth (Part 2 Start Point)

## Status: Ready to hand off

Ashish Part 1 foundation is complete and verified locally.

Implemented foundation:

- Repository scaffold (`proto`, `config`, `cpp`, `python`, `data`, `scripts`)
- Mini1 core import (typed records/parser/query infra files)
- `TripRecord` and `TripDataSoA` type-tightening + field reordering
- Record layout report with `sizeof` and `offsetof`:
  - See `docs/TRIPRECORD_LAYOUT.md`
- Mini2 protobuf + gRPC service contract:
  - `proto/mini2.proto`
- Generated stubs:
  - Python: `python/mini2_pb2.py`, `python/mini2_pb2_grpc.py`
  - C++: `cpp/mini2.pb.{h,cc}`, `cpp/mini2.grpc.pb.{h,cc}`
- Tree topology config:
  - `config/topology.json`
- Data partitioning script + generated A..I sample partitions:
  - `scripts/split_data.py`
  - `data/partition_A.csv` … `data/partition_I.csv`
- C++ build setup:
  - `cpp/CMakeLists.txt`
  - Builds `node_server` and `client` placeholders

---

## Verified on local machine

Executed successfully:

```bash
python3 scripts/split_data.py --input data/test_sample.csv --output-dir data
cmake -S cpp -B cpp/build
cmake --build cpp/build
./cpp/build/node_server
./cpp/build/client
```

Observed output:

- Partition split sums correctly to 10,000 sample rows
- C++ targets compile and link
- Binaries run:
  - `Mini2 node server placeholder`
  - `Mini2 client placeholder`

---

## What Seth should implement next (Part 2)

### 1) Python node runtime (B–I)

- `python/server/node.py`:
  - parse `--id` and `--config`
  - load `config/topology.json`
  - derive neighbors/children from edge list
  - load this node’s CSV partition from config
  - implement `ForwardQuery`
    - local query
    - concurrent child forwards
    - merge and return

### 2) Python query execution

- Expand `python/query_engine.py` with Q1–Q6 equivalents.
- Keep output schema compatible with protobuf response messages.

### 3) C++ gateway (A)

- Replace placeholder in `cpp/server/node_server.cpp`:
  - load config
  - load local partition
  - implement `SubmitQuery` / `FetchChunk` / `ForwardQuery` / `CancelQuery`
  - scatter-gather to children (B/H/G/I in current tree)
  - chunk cache keyed by `request_id`

### 4) C++ client

- Replace placeholder in `cpp/client/client.cpp`:
  - load config
  - submit query to A
  - repeatedly call `FetchChunk` until last chunk

---

## Important constraints (keep)

- No hardcoded node identity/host/port in code
- All runtime topology from `config/topology.json`
- Keep request IDs end-to-end for tracing/cancel
- Start with fixed chunking first; dynamic chunk tuning later
- Avoid gRPC async streaming APIs for this mini

---

## Notes on current assumptions

- Edge list is tree-shaped and currently directional strings (e.g., `AB`).
- If needed, convert to undirected neighbor map in Seth’s implementation to make parent/child derivation robust.

---

## Quick health checks before coding

```bash
# protobuf regeneration (optional)
./.venv/bin/python -m grpc_tools.protoc -I proto/ --python_out=python/ --grpc_python_out=python/ proto/mini2.proto
protoc -I proto/ --cpp_out=cpp/ --grpc_out=cpp/ --plugin=protoc-gen-grpc=$(which grpc_cpp_plugin) proto/mini2.proto

# build
cmake -S cpp -B cpp/build && cmake --build cpp/build
```

If these pass, Seth can start implementing Part 2 directly.
