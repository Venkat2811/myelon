# competitive-bench

`competitive-bench` is the narrow external-comparison harness for `myelon`.
It owns the apples-to-apples transport comparison surface. It does not own the broad
internal sweep universe in `crates/perf-bench`.

## Scope

Current competitive families:

- signal
  - internal raw-ring only
  - SHM + mmap
  - exposed through `simple-smoke`
- `1p1c` ping-pong
  - max-throughput mode
  - fixed-rate coordinated-omission-aware mode
- broadcast
  - `1p4c`
  - `1p8c`
  - max-throughput mode
  - fixed-rate coordinated-omission-aware mode
- raw internal baselines
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
  - `iggy-tcp`
  - `redpanda-kafka`

Size ladders:

- simple-smoke ladder:
  - `32`, `64`, `128`, `1KB`, `2KB`, `4KB`
- default parity ladder:
  - `64`, `512`, `1024`, `2048`, `2MB`
- extensive ladder:
  - `16KB`, `32KB`, `64KB`, `128KB`, `512KB`, `1MB`, `8MB`, `16MB`, `32MB`, `64MB`

The default run stays narrow and fast enough for regular parity work. The extensive ladder is
opt-in and targets large-object analysis.

## What This Crate Does Not Do

- It does not run `perf-bench results`.
- It does not run framed / typed / zero-copy internal matrices.
- It does not mix competitive ping-pong with broader internal sweep reporting.

Use `crates/perf-bench` when you want the full internal benchmark platform.

## Layout

- `config/parity.mk`
  - default and extensive size/rate/count policy
- `src/bin/*.rs`
  - per-peer benchmark adapters
- `scripts/aggregate_results.py`
  - family-separated, protocol-separated throughput / fixed-rate tables
- `scripts/aggregate_headon.py`
  - head-on summaries
- `scripts/pareto_frontier.py`
  - SVG frontier plots
- `scripts/cleanup_shm.py`
  - removes competitive-bench-owned `/dev/shm` artifacts after interrupted or heavy runs
- `third_party/`
  - pinned local source trees that must stay scoped to this crate
- `output/`
  - durable result bundles, graphs, and aggregate text

## Why `third_party/` Only Has Three Peers

Current crate-local source trees:

- `third_party/crossbar`
- `third_party/boost_pingpong`
- `third_party/ompi_pingpong`

The rest are intentionally not vendored here:

- `shmipc-rs`
  - built from Cargo crates
- `iceoryx2`
  - built from Cargo crates
- `rusteron`
  - built from Cargo crates
- `zeromq`
  - adapter is our Rust code, transport library comes from system `libzmq`
- `iggy`
  - Docker-managed broker peer
- `redpanda`
  - Docker-managed broker peer

Broker peers are intentionally minimal:

- sizes:
  - `64B`
  - `1KB`
- modes:
  - max throughput
  - fixed-rate CO at `1K/s`, `3K/s`, and `5K/s`

Override `BROKER_RATES` if you explicitly want an overload run instead of the default below-saturation ladder.

Rule:

- vendor or pin source only when exact local source materially affects reproducibility
- keep Cargo-managed peers Cargo-managed unless source pinning becomes necessary
- keep system-managed dependencies system-managed when that is the simpler and more honest setup

## Build and Run

Typical flow:

```bash
cd crates/competitive-bench
make help
make simple-smoke
make build-all
make cleanup-shm
make ubermensh-smoke
make run-all-quick
make run-all-fixed-rate-quick
make aggregate
make graphs
make headon-smoke
make verify-align
```

Extensive large-object flow:

```bash
cd crates/competitive-bench
make build-all
make cleanup-shm
OUTDIR=output/results_extensive_full make run-all-extensive-quick
OUTDIR=output/results_extensive_full make run-all-extensive-fixed-rate-quick
OUTDIR=output/results_extensive_full make aggregate
make headon-extensive HEADON_DIR=output/headon_extensive
make verify-align-extensive
```

## Result Layout

Outputs are durable and crate-local:

- `output/results*`
- `output/headon*`

Aggregate reports are split by:

- family
  - `Signal`
  - `Ping-Pong`
  - `Broadcast`
- protocol
  - `SHM`, `MMAP`, `IPC`, `TCP`, `TCP / Brokered`, `MPI`, `Message Queue`
- measurement mode
  - max throughput
  - fixed-rate CO-aware

Broker peers appear only under:

- `TCP / Brokered`

Latency columns include:

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

## Setup

See `SETUP.md` for:

- submodule initialization
- system package prerequisites
- how each peer is built
- how each peer is executed
- output, cleanup, and large-object run behavior
