# Mini2 Handoff to Shamathmika

## Current project status

The distributed query flow is implemented and runnable:

- Python worker nodes: `B..I` (`python/server/node.py`)
- C++ entry/gateway node: `A` (`cpp/server/node_server.cpp`)
- C++ client: `cpp/client/client.cpp`
- Topology and runtime settings: `config/topology.json`
- End-to-end launcher: `scripts/launch_all.sh`
- BFT scenario runner: `scripts/run_bft_fault_eval.sh`

The current `topology.json` keeps `bft_mode: "lite"` and `fault_threshold: 1`.

## Important BFT behavior in current code

`cpp/server/node_server.cpp` now enforces strict `f=1` math in BFT-lite merge:

- requires `n >= 3f + 1` for each logical child replica group
- requires quorum `2f + 1` matching payloads

For `f=1`, this means:

- minimum replicas per logical group: `4`
- minimum matching replies: `3`

Because `config/topology.json` currently has one replica per group (`n=1`), BFT-lite rejects those child contributions with:

`rejected: requires n>=3f+1 (have=1, need=4, f=1)`

So this is strict enforcement, not full PBFT consensus.

## How to test (with and without BFT)

### 1) Build and run scenario suite

From repo root:

```bash
bash scripts/run_bft_fault_eval.sh
```

This runs:

- `baseline` (BFT off)
- `bft_lite_clean`
- `bft_lite_mutation`
- `bft_lite_delay`
- `bft_lite_drop`
- `bft_lite_wrongagg`

### 2) What to expect

- `baseline` (off): high merged record count (full tree merge)
- `bft_lite_*` with current config: much lower record count (mostly local A data) because children are rejected for not meeting `3f+1`

### 3) Inspect logs

```bash
rg "\\[A\\]\\[BFT\\]|requires n>=3f\\+1|quorum not met" logs/node_A.log
```

You should see one rejection per logical child (`B`, `G`, `H`, `I`) in current topology.

## If you want true `f=1` tolerance demo

Current topology is not sufficient for practical `f=1` Byzantine tolerance. To demo valid `f=1` behavior:

- create replica groups with size `4` per protected logical child
- ensure replicas hold equivalent shard/state
- keep quorum at `3` matching results
- then inject one faulty replica and show that 3 honest replicas still win

## Notes on requested references

I checked for `docs/Part_2.md` and `docs/BUILD_GUIDE` in this repository, but those files are not present here. This handoff is based on the current codebase and existing docs (`README.md`, `docs/HANDOFF_SETH.md`).
