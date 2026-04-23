# third_party

Pinned external benchmark dependencies live here for `competitive-bench`.

This directory is intentionally crate-local rather than repo-root so benchmark-only
source pinning stays scoped to `competitive-bench`.

## What lives here now

- `crossbar`
  - real upstream source tree
  - pinned as a git submodule
- `boost_pingpong`
  - local reference adapter source checked into the repo
- `ompi_pingpong`
  - local reference adapter source checked into the repo

## Why not more

Only three peers currently live under `third_party/` because source pinning is not free.
We keep local source here only when it materially improves reproducibility or when the
adapter source itself is part of the benchmark contract.

Peers currently wired without local source vendoring:

- `shmipc-rs`
  - built from Cargo
- `iceoryx2`
  - built from Cargo
- `rusteron`
  - built from Cargo
- `zeromq`
  - Rust adapter in this crate, system `libzmq` underneath
- `iggy`
  - Docker-managed broker peer
- `redpanda`
  - Docker-managed broker peer

Rule:

- vendor or pin source only when exact local source materially affects reproducibility
- keep Cargo-managed peers Cargo-managed unless source pinning becomes necessary
- keep system-managed dependencies system-managed when that is the simpler and more honest setup

## Current output/reporting contract

- internal raw baselines cover both `shm` and `mmap`
- protocol groups render separately
  - `SHM`
  - `MMAP`
  - `IPC`
  - `TCP`
  - `TCP / Brokered`
  - `MPI`
  - `Message Queue`
- throughput and fixed-rate CO-aware sections render separately
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
- Pareto frontier SVGs are generated from the durable output bundle

## Future policy

If a future peer needs exact upstream source pinning for published benchmark tables,
it can move under `third_party/` too. Cargo-managed and system-managed peers should stay out
until that is actually necessary.
