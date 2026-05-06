# perf-bench

> **Internal**. Not published to crates.io. Owns the broad internal
> performance sweep universe; the narrower external comparison surface
> lives in [`competitive-bench`](../competitive-bench/).

## Purpose

`perf-bench` consolidates the repository's performance benchmarks for
the [`disruptor-mp`](../disruptor-mp/) and
[`myelon`](../myelon/) transport stacks into a
small set of consolidated binaries.

## Binaries

| Binary | Scenarios |
|---|---|
| `perf-bench-pingpong` | 1p1c ping-pong. Layers: `raw_ring`, `raw_myelon`, `framed`, `codec`, `typed_zc`. Backends: `shm`, `mmap`. Modes: max-throughput, coordinated-omission-aware fixed-rate, low-overhead batch-timing. |
| `perf-bench-broadcast` | 1pNc broadcast for `1p4c` and `1p8c`. |
| `perf-bench-repeatability` | Repeat a configuration N times and emit canonical JSON for run-to-run variance analysis. |
| `perf-bench-signal` | Tiny signal-only ring scenarios. |

## Layers

- `raw_ring` — direct `disruptor_mp::SharedProducer` / `SharedConsumer`.
- `raw_myelon` — `myelon::MyelonTransport*` with no framing.
- `framed` — `myelon::FramedTransportProducer / Consumer`
  with multi-frame messages.
- `codec` — `framed` + a serialisation codec (bincode / rkyv /
  flatbuffers).
- `typed_zc` — typed zero-copy codec wrappers.

## Backends

- `shm` — POSIX shared-memory segment (Linux/macOS) or Win32
  shared-memory section (Windows).
- `mmap` — file-backed mmap.

## Observability

Pass `--enable-counters` to opt into the RFC-0040 observability
counters on the SHM-backed ring. Default-off so the standard bench
path stays counter-free; the flag is honored only by
`--layer raw_ring --backend shm` for now.

## Quick start

```sh
# Default ping-pong: raw_ring + shm + 64-byte payload + 100k messages.
cargo run -p perf-bench --release --bin perf-bench-pingpong -- \
    --layer raw_ring --backend shm

# Counters-on scenario.
cargo run -p perf-bench --release --bin perf-bench-pingpong -- \
    --layer raw_ring --backend shm --enable-counters

# Codec layer with rkyv.
cargo run -p perf-bench --release --bin perf-bench-pingpong -- \
    --layer codec --backend shm --codec rkyv
```

## License

MIT.
