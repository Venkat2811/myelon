# competitive-bench Setup

## Scope

`competitive-bench` is the narrow external-comparison orchestrator.
It does not run the exhaustive internal `perf-bench` matrices.

Current families:

- signal
  - internal raw-ring only
  - `shm` + `mmap`
  - exposed via `make super-tiny`
- `1p1c` ping-pong
  - max-throughput mode
  - fixed-rate coordinated-omission-aware mode
- broadcast
  - `1p4c`
  - `1p8c`
  - max-throughput mode
  - fixed-rate coordinated-omission-aware mode

Current adapter surface:

- internal baselines
  - `disruptor-shm`
  - `disruptor-mmap`
  - `myelon-raw-shm`
  - `myelon-raw-mmap`
- external peers
  - `crossbar-channel`
  - `crossbar-pubsub`
  - `shmipc-rs`
  - `iceoryx2-shm`
  - `boost-message-queue`
  - `ompi-vader-self`
  - `rusteron-aeron-ipc`
  - `zeromq-ipc`
  - `zeromq-ipc-abs`
  - `zeromq-tcp`

## Why `third_party/` only has three peers

Only these peers are stored as crate-local source trees:

- `crossbar`
- `boost_pingpong`
- `ompi_pingpong`

Reason:

- `crossbar` is pinned here as a local source dependency used directly by the crate.
- `boost_pingpong` and `ompi_pingpong` are checked-in reference adapters whose source and build contract belong with the harness.
- `shmipc-rs`, `iceoryx2`, `rusteron`, and the Rust-side `zeromq` adapter are built from Cargo in the workspace and do not currently need local source vendoring.
- `zeromq` still depends on system `libzmq`; the transport library is system-managed even though the adapter binary is ours.

## Durable outputs

Default output roots:

- `output/results`
- `output/headon`
- `output/simple-smoke`
- `output/super-tiny`

These are local to the crate, not `/tmp`.

## Prerequisites

### Submodules

```bash
git submodule update --init --recursive crates/competitive-bench/third_party/crossbar
```

### System packages

Debian/Ubuntu example:

```bash
sudo apt-get update
sudo apt-get install -y build-essential pkg-config libzmq3-dev libboost-all-dev openmpi-bin libopenmpi-dev
```

What each package is for:

- `build-essential`
  - C/C++ compilation for local third-party adapters
- `pkg-config` + `libzmq3-dev`
  - Rust `zmq` crate build and link
- `libboost-all-dev`
  - `boost::interprocess message_queue` adapter build
- `openmpi-bin` + `libopenmpi-dev`
  - `mpicc`, `mpirun`, and Open MPI headers/libs for the `ompi` adapter

## Build and run

Typical flow from `crates/competitive-bench`:

```bash
make help
make build-all
make simple-smoke
make super-tiny
make quick
make headon-smoke
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

The runner is the source of truth for adapter dispatch. The Makefile is only a thin wrapper for build prerequisites, quick tier aliases, and signal integration.

## Adapter build contract

### Internal baselines

Built through `perf-bench` benches invoked by `competitive-bench`:

- `disruptor-shm`
- `disruptor-mmap`
- `myelon-raw-shm`
- `myelon-raw-mmap`
- internal broadcast via `internal_broadcast`

### `crossbar`

- source: `third_party/crossbar`
- built by Cargo through `make build-all`
- participates in ping-pong and broadcast

### `shmipc-rs`

- source: Cargo dependency only
- built by Cargo through `make build-all`
- participates in ping-pong

### `iceoryx2-shm`

- source: Cargo dependency only
- built by Cargo through `make build-all`
- participates in ping-pong

### `boost::interprocess message_queue`

- source: `third_party/boost_pingpong`
- built by `make build-all`
- participates in ping-pong

### `ompi`

- source: `third_party/ompi_pingpong`
- built by `make build-all`
- participates in ping-pong

Notes:

- `OMPI_MPICC` can override the local MPI compiler.

### `rusteron` / Aeron IPC

- source: Cargo dependency only
- built by Cargo through `make build-all`
- participates in ping-pong

Notes:

- the adapter launches and tears down its own embedded Aeron media driver
- payloads larger than Aeron IPC's max message length are skipped by the runner

### `zeromq`

- adapter is our Rust binary
- transport library is system `libzmq`
- built by Cargo through `make build-all`
- participates in ping-pong over:
  - `ipc`
  - `ipc-abs`
  - `tcp`

## Cleanup and process hygiene

Kill leftover benchmark peer processes between runs:

```bash
make kill-pingpong
```

Shared-memory artifacts under `/dev/shm/` are cleaned up by the runner between runs. If a run is interrupted hard, `make kill-pingpong` followed by another run is usually enough.

## Output contract

Current output/reporting contract:

- internal raw baselines cover both `shm` and `mmap`
- benchmark families are rendered separately:
  - `Signal`
  - `Ping-Pong`
  - `Broadcast`
- aggregate tables are separated by protocol
  - `SHM`
  - `MMAP`
  - `IPC`
  - `TCP`
  - `MPI`
  - `Message Queue`
- max-throughput and fixed-rate CO-aware render in separate sections
- latency exports include:
  - `P1`
  - `P10`
  - `P25`
  - `P50`
  - `P90`
  - `P95`
  - `P99`
  - `P99.9`
  - `P99.99`
  - `P99.999`
  - `P99.9999`
- Pareto frontier SVGs are generated from the same durable output bundle

## Fast lanes

```bash
make simple-smoke
make super-tiny
```

`simple-smoke` is the exact-size fast sanity lane.

`super-tiny` is the broader CI-style lane:

- `100` warmup
- `1000` measured messages/events
- one representative fixed-rate CO lane at `50K/s` per scenario, rather than the full parity rate ladder
- signal over `shm` + `mmap`
- ping-pong for all currently wired peers
- broadcast for the wired broadcast peers
