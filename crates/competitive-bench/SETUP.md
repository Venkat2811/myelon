# competitive-bench Setup

## Scope

`competitive-bench` is the narrow external-comparison orchestrator.
It does not run the exhaustive internal `perf-bench` matrices.

Internal parity baselines currently wired:

- raw `disruptor-mp` SHM ping-pong
- raw `disruptor-mp` mmap ping-pong
- raw curated `myelon` SHM ping-pong
- raw curated `myelon` mmap ping-pong

External peer parity surface follows `mp_ipc_world_domination`.

Currently wired peers:

- `crossbar` via `third_party/crossbar`
- `shmipc-rs` via Cargo
- `boost::interprocess message_queue` via `third_party/boost_pingpong`
- `ompi` via `third_party/ompi_pingpong`
- `rusteron` / Aeron IPC via Cargo
- `zeromq` via Cargo
- `zeromq-ipc-abs` via Cargo
- `zeromq-tcp` via Cargo

## Durable outputs

Default output roots:

- `output/results`
- `output/headon`

These are local to the crate, not `/tmp`.

## Basic usage

```bash
git submodule update --init --recursive crates/competitive-bench/third_party/crossbar
make help
make build-all
make ubermensh-smoke
make run-all-quick
make run-all-fixed-rate-quick
make zmq-ipc-abs-run-quick
make zmq-tcp-run-quick
make headon-smoke
make verify-align
make aggregate
```

## Future peer wiring

Peer sources that need pinning should be checked out under `third_party/`.

Current policy:

- `crossbar` is pinned as a crate-local submodule
- `boost_pingpong` and `ompi_pingpong` are checked in as crate-local third-party adapters
- Cargo-backed Rust peers stay crate-managed unless source pinning becomes necessary
- system dependencies such as Boost and OpenMPI can remain externally installed as
  long as their versions are captured alongside results
