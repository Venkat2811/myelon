# competitive-bench Setup

This file is the operator guide for `competitive-bench`. The crate README explains scope; this file explains what you need installed and how the peers are built.

## Prerequisites

### Submodules

```bash
git submodule sync --recursive
git submodule update --init --recursive crates/competitive-bench/third_party/crossbar
```

### System packages

Debian / Ubuntu example:

```bash
sudo apt-get update
sudo apt-get install -y build-essential pkg-config libzmq3-dev libboost-all-dev openmpi-bin libopenmpi-dev
```

What they are for:

| Package | Why |
|---|---|
| `build-essential` | C/C++ compilation for local third-party adapters |
| `pkg-config`, `libzmq3-dev` | build and link the Rust `zmq` crate |
| `libboost-all-dev` | build the Boost message-queue adapter |
| `openmpi-bin`, `libopenmpi-dev` | `mpicc`, `mpirun`, and Open MPI headers/libs for the OMPI adapter |

Optional by peer:

- only `zeromq-*`: `pkg-config`, `libzmq3-dev`
- only `boost-message-queue`: `build-essential`, `libboost-all-dev`
- only `ompi-vader-self`: `build-essential`, `openmpi-bin`, `libopenmpi-dev`
- `crossbar`, `shmipc-rs`, `iceoryx2-shm`, `rusteron-aeron-ipc`: no extra system packages beyond the Rust toolchain in the normal Linux setup

## Build and run

Typical flow:

```bash
make help
make build-all
make simple-smoke
make super-tiny
make quick
```

Larger sweeps:

```bash
make extensive
make headon-extensive
```

Direct runner invocation:

```bash
cargo run -p competitive-bench --profile competitive \
  --bin competitive_bench_runner -- --help
```

`make build-all` compiles the workspace crate plus the local Boost and OMPI peer helpers, so expect it to take longer than a normal Rust-only build.

## Adapter build contract

| Adapter family | Build path |
|---|---|
| internal baselines | built through workspace Rust code |
| `crossbar` | source under `third_party/crossbar`, built through Cargo |
| `shmipc-rs` | Cargo dependency |
| `iceoryx2-shm` | Cargo dependency |
| `boost-message-queue` | local source under `third_party/boost_pingpong` |
| `ompi-vader-self` | local source under `third_party/ompi_pingpong` |
| `rusteron-aeron-ipc` | Cargo dependency, launches its own local Aeron media driver |
| `zeromq-*` | Rust adapter binary plus system `libzmq` |

Notes:

- `OMPI_MPICC` can override the MPI compiler used for the OMPI adapter
- payloads larger than the Aeron IPC max message length are skipped by the runner for `rusteron`

## Durable outputs

Default output roots:

- `output/results`
- `output/headon`
- `output/simple-smoke`
- `output/super-tiny`

These outputs are crate-local and durable enough for later graphing or post-processing.

## Cleanup and process hygiene

Kill leftover peer processes between runs:

```bash
make kill-pingpong
```

Shared-memory artifacts under `/dev/shm/` are cleaned up by the runner between runs. If a run is interrupted hard, `make kill-pingpong` followed by another run is usually enough.

## Output contract

The output schema is intentionally stable across peers:

- internal raw baselines cover both `shm` and `mmap`
- throughput and fixed-rate CO-aware sections are separate
- latency exports include the full percentile ladder used by the runner
- Pareto/frontier graphs are generated from the durable JSON bundle, not from terminal output

## `third_party/` policy

Only a few peers live under `third_party/`:

- `crossbar`
- `boost_pingpong`
- `ompi_pingpong`

The rule is simple:

- pin source locally when it materially helps reproducibility
- keep Cargo-managed peers Cargo-managed unless that changes
- keep system-managed dependencies system-managed unless that changes

## Fast lanes

```bash
make simple-smoke
make super-tiny
```

- `simple-smoke`: fast exact-size sanity lane
- `super-tiny`: broader CI-style lane with signal, ping-pong, and broadcast coverage
