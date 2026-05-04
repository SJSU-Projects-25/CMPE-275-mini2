# Installation Notes

## Requirements

- Python 3.12 (grpcio has no pre-built wheels for 3.13+)
- C++17 compiler (clang++ or g++)
- CMake 3.15+

---

## macOS

### System dependencies

```bash
brew install grpc protobuf nlohmann-json cmake
```

### Python environment

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

---

## Linux (Ubuntu/Debian)

### System dependencies

```bash
sudo apt update
sudo apt install -y \
    libgrpc++-dev \
    protobuf-compiler \
    protobuf-compiler-grpc \
    nlohmann-json3-dev \
    cmake \
    build-essential
```

### Python environment

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

---

## Build C++ binaries

```bash
cmake -S cpp -B cpp/build
cmake --build cpp/build
```

Produces:
- `cpp/build/node_server` — gRPC server (runs as any node A–I)
- `cpp/build/client` — query client

---

## Regenerate gRPC stubs (only if proto changes)

### Python

```bash
.venv/bin/python -m grpc_tools.protoc \
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

---

## Prepare data

### Download

NYC TLC Yellow Taxi trip records are available at:
https://data.cityofnewyork.us/browse?q=taxi&sortBy=relevance&page=1&pageSize=20

Download one or more monthly CSV files (Yellow Taxi Trip Records). The full
dataset is ~12 GB; a single month is sufficient for testing.

Place the downloaded CSV in the `data/` directory:

```
data/
└── your_tlc_file.csv
```

The repository includes `data/test_sample.csv` (10,000 rows) for quick runs
without downloading the full dataset.

### Partition into node shards

Split the CSV into 9 disjoint partitions (one per node A–I):

```bash
.venv/bin/python scripts/split_data.py \
    --input data/your_tlc_file.csv \
    --output-dir data
```

This writes `data/partition_A.csv` through `data/partition_I.csv`. Each node
loads only its own partition at startup.

---

## Run (single machine)

```bash
bash scripts/launch_all.sh
```

Starts Python nodes B–I, C++ node A, runs a default distance query, then shuts everything down. See `README.md` for other query types and two-machine setup.
