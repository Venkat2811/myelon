# Changelog

All notable changes to `disruptor-mp` and `myelon` are documented here. Both
crates ship from this workspace and version in lockstep during the early-OSS
window; once API surfaces stabilise they may diverge.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the version numbers follow [Semantic Versioning](https://semver.org/) with
[pre-release tags](https://semver.org/#spec-item-9) (`-alpha.N`, `-beta.N`,
`-rc.N`) for the iteration window.

## [Unreleased]

## [0.1.0-alpha.1] — 2026-05-13

First public release of `disruptor-mp` and `myelon` on crates.io.

### Why `0.1.0-alpha.1` (and not `3.x.y`)

`disruptor-mp` carried `version = "3.7.1"` in its internal Cargo.toml because
this fork started from upstream `disruptor` v3.7.1. That number was never on
crates.io under the `disruptor-mp` name; using it on first publish would
have falsely implied a mature 3.x history. The reset to `0.1.0-alpha.1`
explicitly signals "first public release of this crate, API may evolve
during the early-iteration window."

`myelon` is similarly first-time-public at `0.1.0-alpha.1`.

The pre-release tag (Cargo equivalent of PyPI's `0.1.0a1`) means `cargo add` /
`cargo update` won't pick these versions by default — consumers opt in
explicitly via `disruptor-mp = "=0.1.0-alpha.1"` or similar.

### Added

#### `disruptor-mp` — Layer 0 substrate

- **Multiprocess SHM ring buffer**: `SharedProducer`, `SharedConsumer`, and
  builder surface for cross-process publication on POSIX shared memory
  segments. Auto-cleanup of stale segments, configurable wait strategies
  (busy-spin, spinloop, sleep, block), cache-line padded cursors.
- **mmap-backed transport**: `MmapProducer`, `MmapConsumer`, and builder
  surface for file-backed ring buffers on mmap'd regions. Survives reboots,
  no macOS `PSHMNAMLEN` 31-byte naming ceiling. Side-by-side with the SHM
  path; same builder shape, different segment backing.
- **Required-consumer liveness (RFC-0017.5)**:
  `RequiredConsumerLivenessConfig`, alert hook, failure-action policy
  (`GracefulShutdown` / `LogAndContinue`), startup-wait + progress-check
  timing budget, same-ID rejoin recovery, observable stall alerts.
  Producers configured with `enable_required_consumer_liveness` route
  publish calls through `publish_managed` to consult the liveness layer
  on the hot path.
- **Observability counters (RFC-0040)**: zero-cost (~2 ns/op relaxed atomic)
  `CountersFile` exposing `events_published`, `producer_full_events`,
  `events_consumed`, `consumer_empty_spins`, `consumer_lag_max`, etc. Three
  feature flags layer integrations on top:
  - `metrics` (default): wire counters into the `metrics`-rs facade.
  - `metrics-prometheus`: `metrics-exporter-prometheus` exporter glue.
  - `metrics-otel`: `opentelemetry` / `opentelemetry_sdk` /
    `opentelemetry-otlp` exporter glue (OTLP/HTTP + OTLP/grpc).
- **Aggregator thread**: `AggregatorHandle::spawn` background task that
  snapshots counter slots at a configurable cadence and forwards them
  through the active `metrics` exporter.
- **Cross-process segment-name helpers**: portable salted shared-memory
  segment names that respect the macOS `PSHMNAMLEN` cap, coordination
  cursors, and validation.
- **DST primitives (`#[cfg(dst)]` only)**: FoundationDB/TigerBeetle-style
  Antithesis assertions (`assert_always`, `assert_sometimes`,
  `assert_reachable`, `assert_unreachable`), `AssertionLog`, BUGGIFY
  probabilistic fault injection, deterministic-scenario profiles and
  runtime. Available to crates.io consumers who set
  `RUSTFLAGS="--cfg dst"` on their builds — same pattern as FoundationDB
  ships BUGGIFY with production code and TigerBeetle ships VOPR
  primitives with the database.

#### `myelon` — Layers 1, 2, 3 + topology + observability façade

- **Layer 1 — framed transport**: `FramedTransportProducer<F>`,
  `FramedTransportConsumer<F>`, mmap-backed variants. Variable-length
  byte messages with start/end frame flags + multi-frame fragmentation
  for payloads exceeding ring slot size. `FixedFrame<N>` /
  `AlignedFixedFrame<N>` const-generic frame shapes, `ReassemblyBuffer`,
  `MyelonTransportLayout`, `RunnerMyelonTransportConfig`.
- **Layer 2 — codec**: `Codec` trait with bincode, rkyv (feature-gated),
  and flatbuffers (feature-gated) implementations. Typed
  serialisation/deserialisation over framed transport.
- **Layer 3 — typed zero-copy**: `TypedProducer<F>`, `TypedConsumer<F>`,
  mmap variants, `ZeroCopyCodec` for in-place reads of serialised data
  without intermediate allocations.
- **Topology**: `FixedTopology`, `WorkerCount` (2..=8), discovery +
  rendezvous helpers for fixed scheduler / N-worker inference shapes.
- **Observability re-export**: full `observability::*` surface
  re-exported from `disruptor-mp` so users who depend only on `myelon`
  get the counters API without a second dep.
- **Per-layer managed-publish surface**: `enable_required_consumer_liveness`
  + `publish_managed` on every producer variant (framed, typed, mmap of
  each).

### Workspace

- Apache 2.0-style workspace lint configuration: `unsafe_op_in_unsafe_fn`,
  `rust_2018_idioms`, `nonstandard_style`, `broken_intra_doc_links = deny`,
  `doc_markdown`, plus a `cfg(dst)` `unexpected_cfgs` declaration.
- Workspace MSRV: Rust 1.87 (driven by `is_multiple_of` in `perf-bench`).
- Cargo dep matrix audited and bumped to current versions where safe:
  upstream `disruptor` 4.2.0, `thiserror` 2.0, `once_cell` 1.21, `memmap2`
  0.9.10, `core_affinity` 0.8.3, `nix` 0.31, `metrics-exporter-prometheus`
  0.18, `opentelemetry` family 0.32.

### Deferred

The following items are intentionally out of scope for `0.1.0-alpha.1`
and tracked for follow-up:

- DST-coverage report (assertion log review pipeline).
- Broadcast / fan-out integration tests beyond the existing demos.
- Multi-process liveness integration test.
- Workspace-wide `cargo-llvm-cov` workflow.
- Exporter feature smoke tests (`metrics-prometheus`, `metrics-otel`).
- Safe wrappers around the four `pub unsafe fn` in `observability`
  (`CountersFile::init`, `CountersFile::attach`, `CountersFile::from_ptr`,
  `AggregatorHandle::spawn`). Power-user escape hatches stay; idiomatic
  `boxed()` / `with_shm_segment()` builders are a v0.1.0-alpha.2 candidate.
- `flatbuffers` 24 → 25 (regenerate `bench_payload_generated.rs`).
- `rand` 0.8 → 0.10 (dev-dep, usage refactor).
- `criterion` 0.5 → 0.8 (dev-dep, API delta).

### Acknowledgements

- `disruptor` crate (`Nicholas-Schultz` and contributors) for the upstream
  single-process Disruptor ring-buffer surface this crate extends.
- The [FoundationDB BUGGIFY pattern](https://apple.github.io/foundationdb/testing.html)
  and [TigerBeetle VOPR](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md)
  for the DST primitives' shape.
- The [tokio loom integration pattern](https://github.com/tokio-rs/tokio/blob/master/tokio/Cargo.toml)
  for the `[target.'cfg(dst)'.dev-dependencies]` workspace test wiring.

[Unreleased]: https://github.com/Venkat2811/myelon/compare/v0.1.0-alpha.1...HEAD
[0.1.0-alpha.1]: https://github.com/Venkat2811/myelon/releases/tag/v0.1.0-alpha.1
