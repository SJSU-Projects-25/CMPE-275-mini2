# CMPE-275-mini2

Multi-processes using request/cache controls.

## Foundation Status (Ashish Part 1)

Implemented:

- Project scaffold (`proto/`, `config/`, `cpp/`, `python/`, `data/`, `scripts/`)
- Mini1 asset import for data model + parser + query core
- Tightened `TripRecord` and `TripDataSoA` field types
- `sizeof` / `offsetof` layout report in `docs/TRIPRECORD_LAYOUT.md`
- Mini2 gRPC contract in `proto/mini2.proto`
- Generated Python/C++ protobuf and gRPC stubs
- Tree overlay config in `config/topology.json`
- CSV partition utility `scripts/split_data.py` (A..I disjoint splits)
- C++ build system (`cpp/CMakeLists.txt`) with gRPC/protobuf/json linkage

## Repo Layout

```
CMPE-275-mini2/
├── proto/
│   └── mini2.proto
├── config/
│   └── topology.json
├── cpp/
│   ├── CMakeLists.txt
│   ├── mini2.pb.{h,cc}
│   ├── mini2.grpc.pb.{h,cc}
│   ├── server/node_server.cpp
│   ├── client/client.cpp
│   ├── include/taxi/
│   └── src/
├── python/
│   ├── mini2_pb2.py
│   ├── mini2_pb2_grpc.py
│   ├── query_engine.py
│   ├── trip_record.py
│   └── server/
├── data/
│   ├── test_sample.csv
│   └── partition_[A-I].csv
├── scripts/
│   └── split_data.py
└── docs/
    └── TRIPRECORD_LAYOUT.md
```

## Dependencies

### Python

Recommended project-local environment:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install grpcio grpcio-tools
```

### C++ (macOS)

```bash
brew install grpc protobuf nlohmann-json
```

### C++ (Linux)

```bash
sudo apt install -y libgrpc++-dev protobuf-compiler-grpc nlohmann-json3-dev
```

## Regenerate gRPC Stubs

### Python

```bash
. .venv/bin/activate
python -m grpc_tools.protoc \
  -I proto/ \
  --python_out=python/ \
  --grpc_python_out=python/ \
  proto/mini2.proto
```

### C++

```bash
protoc -I proto/ \
  --cpp_out=cpp/ \
  --grpc_out=cpp/ \
  --plugin=protoc-gen-grpc=$(which grpc_cpp_plugin) \
  proto/mini2.proto
```

## Partition Test Data

```bash
python3 scripts/split_data.py --input data/test_sample.csv --output-dir data
```

This writes 9 disjoint files:

- `data/partition_A.csv` ... `data/partition_I.csv`

## Build C++ Targets

```bash
cmake -S cpp -B cpp/build
cmake --build cpp/build
```

Outputs:

- `cpp/build/node_server`
- `cpp/build/client`

## Smoke Test

```bash
./cpp/build/node_server
./cpp/build/client
```

Expected:

```
Mini2 node server placeholder
Mini2 client placeholder
```
