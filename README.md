# CMPE-275 Mini 2

A 9-node distributed scatter-gather query system over NYC TLC yellow taxi trip data. Clients submit range queries to a C++ entry node (A); A fans out to its subtree over gRPC, each node filters its local CSV partition, and results are merged and returned to the client in chunks.

## Topology

```
A (C++) -- entry node
├── B (Python)
│   ├── C (Python)
│   └── D (Python)
│       ├── E (Python)
│       └── F (Python)
├── G (Python)
├── H (Python)
└── I (Python)
```

Edges and port assignments are defined in `config/topology.json`. Each node loads its own `data/partition_<ID>.csv`.

## Query types

| Type | Filter |
|------|--------|
| `distance` | trip_distance in [min, max] miles |
| `fare` | fare_amount in [min, max] dollars |
| `time` | pickup_timestamp in [start, end] Unix seconds |
| `location` | PULocationID in [min, max] |
| `combined` | time + distance + passenger count ranges |
| `aggregate` | time range, returns sum/count/avg of fare_amount |

## Dependencies

### macOS

```bash
brew install grpc protobuf nlohmann-json cmake
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

### Linux

```bash
sudo apt install -y libgrpc++-dev protobuf-compiler-grpc nlohmann-json3-dev cmake
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

Python 3.12 is required (grpcio wheels are not available for 3.13+).

## Build

```bash
cmake -S cpp -B cpp/build
cmake --build cpp/build
```

Produces `cpp/build/node_server` and `cpp/build/client`.

## Data

Split the CSV into 9 node partitions:

```bash
.venv/bin/python scripts/split_data.py --input data/test_sample.csv --output-dir data
```

## Run

### Single machine

```bash
bash scripts/launch_all.sh
# with build step:
bash scripts/launch_all.sh --build
# custom query:
bash scripts/launch_all.sh -- --config config/topology.json --query-type aggregate --start-time 1577836800 --end-time 1580515200
```

This starts all Python nodes (B-I), then C++ node A, runs one client query, and shuts everything down.

### Two machines

Update `config/topology.json` with the real IP addresses of each machine, then:

Machine 1 (nodes A, B, D, H):
```bash
bash scripts/launch_blue.sh
```

Machine 2 (nodes C, E, F, G, I):
```bash
bash scripts/launch_yellow.sh
```

### Manual

```bash
# Terminal 1
bash scripts/launch_python_nodes.sh

# Terminal 2
./cpp/build/node_server --id A --config config/topology.json

# Terminal 3
./cpp/build/client --config config/topology.json --query-type distance --min 0 --max 10000

# Teardown
bash scripts/stop_python_nodes.sh
```

## Concurrency test

```bash
bash scripts/test_concurrent.sh
```

Fires 3 parallel queries (fare, distance, time) and reports results from each.

## Configuration reference

Key fields in `config/topology.json` per node:

| Field | Description |
|-------|-------------|
| `chunk_size` | Records per chunk delivered to client |
| `max_concurrent_requests` | Concurrency cap at entry node |
| `chunk_timeout_seconds` | Idle chunk eviction timeout |
| `stream_up` | Return chunk 0 immediately before child results arrive |
| `bft_mode` | `"lite"` enables payload hash + quorum verification |
