# competitive-bench Setup

## Scope

`competitive-bench` is the narrow external-comparison orchestrator.
It does not run the exhaustive internal `perf-bench` matrices.

Internal parity baselines currently wired:

- raw `disruptor-mp` SHM ping-pong
- raw `disruptor-mp` mmap ping-pong
- raw curated `myelon` SHM ping-pong
- raw curated `myelon` mmap ping-pong
- raw `disruptor-mp` SHM broadcast (`1p4c`, `1p8c`)
- raw `disruptor-mp` mmap broadcast (`1p4c`, `1p8c`)
- raw curated `myelon` SHM broadcast (`1p4c`, `1p8c`)
- raw curated `myelon` mmap broadcast (`1p4c`, `1p8c`)

External peer parity surface currently wired:

- `crossbar-channel` via `third_party/crossbar`
- `crossbar-pubsub` via `third_party/crossbar`
- `shmipc-rs` via Cargo
- `iceoryx2-shm` via Cargo
- `boost::interprocess message_queue` via `third_party/boost_pingpong`
- `ompi` via `third_party/ompi_pingpong`
- `rusteron` / Aeron IPC via Cargo
- `zeromq` via Cargo + system `libzmq`
- `zeromq-ipc-abs` via Cargo + system `libzmq`
- `zeromq-tcp` via Cargo + system `libzmq`
- `iggy-tcp` via Docker-managed Apache Iggy
- `redpanda-kafka` via Docker-managed Redpanda

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
- `iggy` and `redpanda` are broker peers. We pin the container image in the Makefile and keep the runtime ephemeral instead of vendoring full broker source trees under `third_party/`.

## Durable outputs

Default output roots:

- `output/results`
- `output/headon`
- `output/simple-smoke`

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

### Docker

`iggy-tcp` and `redpanda-kafka` are Docker-managed peers.

Required:

```bash
docker --version
docker pull apache/iggy:latest
docker pull docker.redpanda.com/redpandadata/redpanda:v26.1.5
```

The harness starts and tears down these brokers automatically:

- `make iggy-run-quick`
- `make iggy-run-fixed-rate-quick`
- `make redpanda-run-quick`
- `make redpanda-run-fixed-rate-quick`

They are intentionally quick-only peers:

- sizes: `64B`, `1KB`
- modes:
  - max throughput
  - fixed-rate coordinated-omission-aware at `1K/s`, `3K/s`, and `5K/s`
- overload is still available explicitly via:
  - `BROKER_RATES=400000 make iggy-run-fixed-rate-quick`
  - `BROKER_RATES=400000 make redpanda-run-fixed-rate-quick`

## Build contracts per peer

### Internal baselines

Built through `perf-bench` benches invoked by `competitive-bench`:

- `competitive_shm`
- `competitive_mmap`
- `competitive_raw_myelon_shm`
- `competitive_raw_myelon_mmap`

Commands are driven by `make internal-run-quick` and `make internal-run-fixed-rate-quick`.
Broadcast baselines are driven by `make broadcast-run-quick` and
`make broadcast-run-fixed-rate-quick`.

### `crossbar`

Source:

- `third_party/crossbar` submodule

Build:

```bash
make crossbar-build
```

Run:

```bash
make crossbar-run-quick
make crossbar-run-fixed-rate-quick
make broadcast-run-quick
make broadcast-run-fixed-rate-quick
```

### `shmipc-rs`

Source:

- Cargo dependency only

Build:

```bash
make shmipc-build
```

Run:

```bash
make shmipc-run-quick
make shmipc-run-fixed-rate-quick
```

### `iceoryx2-shm`

Source:

- Cargo dependency only

Build:

```bash
make iceoryx2-build
```

Run:

```bash
make iceoryx2-run-quick
make iceoryx2-run-fixed-rate-quick
```

Notes:

- currently wired as a true multiprocess local IPC ping-pong peer
- included in the normal quick parity surface
- not part of the extensive ladder yet
- expected to work on Unix targets where `iceoryx2` itself is supported

### `boost::interprocess message_queue`

Source:

- `third_party/boost_pingpong`

Build:

```bash
make boost-build
```

Run:

```bash
make boost-run-quick
make boost-run-fixed-rate-quick
```

### `ompi`

Source:

- `third_party/ompi_pingpong`

Build:

```bash
make ompi-build
```

Run:

```bash
make ompi-run-quick
make ompi-run-fixed-rate-quick
```

Notes:

- `OMPI_MPICC` and `OMPI_MPIRUN` can override the local toolchain.
- `OMPI_TIMEOUT_SEC` can override runtime timeout separately from Cargo-backed peers.

### `rusteron` / Aeron IPC

Source:

- Cargo dependency only

Build:

```bash
make rusteron-build
```

Run:

```bash
make rusteron-run-quick
make rusteron-run-fixed-rate-quick
```

Notes:

- the adapter launches and tears down its own embedded Aeron media driver
- `RUSTERON_MAX_MESSAGE_SIZE` can cap the largest exercised payload size

### `zeromq`

Source:

- adapter is our Rust binary
- transport library is system `libzmq`

Build:

```bash
make zmq-build
```

Run:

```bash
make zmq-run-quick
make zmq-run-fixed-rate-quick
make zmq-ipc-abs-run-quick
make zmq-ipc-abs-run-fixed-rate-quick
make zmq-tcp-run-quick
make zmq-tcp-run-fixed-rate-quick
```

### `iggy-tcp`

Source:

- Docker-managed broker peer

Build:

```bash
make broker-build
```

Run:

```bash
make iggy-run-quick
make iggy-run-fixed-rate-quick
```

Notes:

- container image defaults to `apache/iggy:latest`
- the harness starts Iggy with default root credentials only for the ephemeral test container
- fixed-rate runs use `BROKER_RATES`, default `1000 3000 5000`
- set `BROKER_RATES=400000` only when you intentionally want an overload run
- current broker parity surface is intentionally limited to `64B` and `1KB`

### `redpanda-kafka`

Source:

- Docker-managed broker peer

Build:

```bash
make broker-build
```

Run:

```bash
make redpanda-run-quick
make redpanda-run-fixed-rate-quick
```

Notes:

- container image defaults to `docker.redpanda.com/redpandadata/redpanda:v26.1.5`
- the ephemeral container runs as `root` so the tmpfs-backed data directory is writable
- fixed-rate runs use `BROKER_RATES`, default `1000 3000 5000`
- set `BROKER_RATES=400000` only when you intentionally want an overload run
- the harness runs a single-broker `dev-container` instance with an ephemeral data directory
- current broker parity surface is intentionally limited to `64B` and `1KB`

## Typical usage

```bash
cd crates/competitive-bench
make help
make simple-smoke
make build-all
make simple-smoke
make quick
make headon-smoke
```

Extensive large-object flow:

```bash
cd crates/competitive-bench
make build-all
make extensive
make headon-extensive
```

For the rusteron alignment standalone check (RFC 0029 Test 6):

```bash
cargo run -p competitive-bench --profile competitive \
    --bin rusteron_pingpong -- --verify-align --message-size 64
```

## Cleanup and process hygiene

Kill leftover benchmark peer processes between runs:

```bash
make kill-pingpong
```

Shared-memory artifacts under `/dev/shm/` are cleaned up by the runner
between runs; if a run is interrupted hard (SIGKILL), `make
kill-pingpong` followed by another run is usually enough. For manual
inspection, `ls /dev/shm/` shows what is left.

## Output contract

Current output/reporting contract:

- internal raw baselines cover both `shm` and `mmap`
- benchmark families are rendered separately:
  - `Signal`
  - `Ping-Pong`
  - `Broadcast`
- aggregate tables are separated by protocol
- broker peers render under:
  - `TCP / Brokered`
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

## Ultra-quick exact-size lane

```bash
make simple-smoke
```

This lane is intentionally narrower than `ubermensh-smoke`.

It runs:

- internal signal over `shm` + `mmap`
- ping-pong over `32B`, `64B`, `128B`, `1KB`, `2KB`, `4KB`
- broadcast over the same size ladder
- all currently wired external peers over the same size ladder
- one fixed-rate lane per peer family
