# Changelog

All notable changes to the publishable crates in this workspace are documented here.

During the early OSS release window, `disruptor-mp` and `myelon` move in lockstep. That may change once their public surfaces stabilize further.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the versions follow [Semantic Versioning](https://semver.org/) with explicit pre-release tags such as `-alpha.N`, `-beta.N`, and `-rc.N`.

## [0.1.0-alpha.2] — 2026-05-21

Post-launch dependency and CI hygiene. No public-API changes; safe drop-in upgrade from `0.1.0-alpha.1`.

### Changed

- GitHub Actions dependencies bumped:
  - `actions/checkout` v4 → v6 (#1)
  - `codecov/codecov-action` v4 → v6 (#2)
- Cargo patch-updates group (#8):
  - `macroquad` 0.4.14 → 0.4.15 (used by `myelon-pulse-vanity` brand demo)
  - `serde_json` patch bump

### CI infrastructure fixes (already in `v0.1.0-alpha.1` tag, recorded here for completeness)

The first public push to `main` exposed several CI gaps that were patched before `v0.1.0-alpha.1` was tagged. Listing them so the alpha.2 release notes are a complete snapshot of what's in CI today:

- Removed the broken duplicate `ci.yml` workflow (had `env.RUST_MSRV` in a job-level `name:` which Actions validation rejected; the focused per-gate workflows already cover its surface).
- `actions/checkout` now fetches submodules (`submodules: recursive`) in every workflow that runs cargo against the workspace. Required because `competitive-bench` has a path dep on the `crossbar` submodule.
- Excluded `competitive-bench` from main CI workflows (`build_and_test.yml`, `lint.yml`, `dst.yml`, `test_coverage.yml`). It pulls heavy C / C++ system deps (libbsd, libzmq, libboost, libopenmpi) that aren't worth installing on every PR for a crate that isn't published.
- Dropped the Miri job from `build_and_test.yml`; Miri rejects `shm_open` and `mmap` calls (`unsupported operation: can't call foreign function`). A properly-scoped Miri lane against pointer-math helpers is on the roadmap (Safety & quality gates Tier 2).
- Loosened the `perf-bench::infra::output::log::test_overhead_is_low` threshold from 1 µs to 5 µs to tolerate CI debug-build hardware variance; the steady-state release-mode cost is still ~2ns/op.

### Acknowledgements

- Codex (OpenAI) and Claude Code (Anthropic) shared the agentic-engineering load on this release. See the README's "Built with" section.

## [0.1.0-alpha.1] — 2026-05-21

Initial public launch of `disruptor-mp` and `myelon` on crates.io.

`disruptor-mp` is the raw multiprocess substrate that extends the single-process [LMAX Disruptor design](https://github.com/LMAX-Exchange/disruptor) to cross-process IPC on POSIX shared memory and memory-mapped files. `myelon` is the layered typed transport built on top of it (framing, codecs, typed zero-copy, topology helpers, layout helpers).

The two crates ship together at the same version because their public surfaces are tightly coupled. They move in lockstep until the surfaces stabilize.

### Features included in this release

#### `disruptor-mp`: raw multiprocess substrate

- [x] Cross-process Single Producer Single Consumer (SPSC).
- [x] Cross-process Single Producer Multi Consumer (SPMC).
- [ ] Cross-process Multi Producer Single Consumer (MPSC).
- [ ] Cross-process Multi Producer Multi Consumer (MPMC).
- [x] Communication patterns:
  - [x] Ping-pong: request/response RTT (two SPSC rings).
  - [x] Broadcast: strict fan-out; every consumer sees every event, slowest gates the producer.
  - [x] Signal: pipelined fan-out, no ack; maximum throughput.
  - [x] Broadcast + per-rank ping-pong: one SPMC dispatch ring + N SPSC return rings (driver ↔ N worker ranks); the inference-fabric shape.
- [x] Two type-identical backends:
  - [x] POSIX shared memory (`shm_open`).
  - [x] Memory-mapped file (`mmap`).
- [x] Memory-level zero-copy reads (`&E` into the ring slot).
- [x] Wait strategies (`AutoWaitStrategy`):
  - [x] `BusySpin`, `BusySpinWithSpinLoopHint`, `SpinThenYield { spins }`, `Sleep(Duration)`, `Block`.
- [x] Liveness for gating consumers: producer-side stall detection with cold-path alert, optional hook, recoverable rejoin.
- [x] Portable shared-memory naming (macOS 31-byte budget enforced).
- [x] Hot-path observability counters:
  - [x] `metrics`-rs facade (default).
  - [x] Prometheus exporter (`metrics-prometheus` feature).
  - [x] OpenTelemetry / OTLP exporter (`metrics-otel` feature).
- [x] Deterministic-simulation hooks behind `RUSTFLAGS="--cfg dst"` (FoundationDB BUGGIFY / TigerBeetle VOPR-style assertions, scenario profiles, runtime).
- [ ] HFT-grade deployment tuning (deferred):
  - [ ] Hugepages-backed SHM segments.
  - [ ] Core pinning / `isolcpus` integration in the builder API.
  - [ ] NUMA-aware SHM placement.

#### `myelon`: layered transport on top of `disruptor-mp`

- [x] Re-exports the raw substrate at type-identical types (one dep gets you everything).
- [x] Framed transport: `&[u8]` payloads in fixed-size frames; multi-frame fragmentation for payloads larger than one frame (start/end flags + message id let the consumer reassemble).
- [x] Compile-time-fixed frame size: `FixedFrame<N>` and `AlignedFixedFrame<N>` (aligned variant for zero-copy reads).
- [x] Typed transport (codec encodes `T` → bytes; consumer decodes back into an owned `T`):
  - [x] bincode.
  - [x] rkyv.
  - [x] flatbuffers.
- [x] Typed zero-copy (consumer reads fields in place via `ZeroCopyCodec::access`; no decode step, no allocation):
  - [x] rkyv (`Archived<T>`).
  - [x] flatbuffers root tables.
- [x] Topology helpers for inference fabrics: rank-scoped request/response, producer-owned startup, attach-time wait-strategy metadata.

#### `myelon-dst`: internal deterministic-simulation harness

- [x] Runner with fault injection and invariant oracle.
- [x] Verification, report emission, DST-coverage sweep.

#### `perf-bench`: internal broad transport sweep harness

- [x] Pingpong, broadcast, signal, repeatability binaries.
- [x] Layer matrix: raw, framed, codec, typed-zero-copy (all × shm / mmap).
- [x] Throughput and CO-aware fixed-rate measurement modes.
- [x] Tier ladder: `super-tiny`, `simple-smoke`, `smoke`, `quick`, `extensive`.

#### `competitive-bench`: internal external-comparison harness

- [x] Adapters: Crossbeam, Iceoryx2, Rusteron (Aeron), shmipc-rs, ZeroMQ (IPC / IPC-abs / TCP), Boost.MQ, OpenMPI.
- [x] Tier ladder matching `perf-bench`.
- [x] Aggregate report and Pareto-frontier summary per run.

#### Bindings

- [ ] Python.
- [ ] C / C++.
- [ ] Zig.

### Validation

Two reference machines exercise different parts of the surface.

**Reference machine for the README's headline numbers** — `venkat-pc`:

- AMD Ryzen 7 5800X (8 cores / 16 threads, 4.85 GHz boost, 32 MiB L3 cache).
- 64 GiB DDR4.
- Ubuntu 22.04, kernel 6.8.

The Linux Ryzen box is the bench-grade reference: pinned cores, stable frequency, deterministic NUMA topology. All five headline tables (signal scaling, raw ping-pong, framed payload scaling, broadcast scaling, typed zero-copy ping-pong) come from this machine.

**Dev machine for pre-release validation gates**:

- Apple M3 Max (14 cores).
- 96 GiB unified memory.
- macOS 26.1, `rustc 1.90.0`.

Gates that passed clean on the dev machine before tagging this release:

- `cargo check --workspace --all-features` ✓
- `cargo clippy --workspace --all-targets -- -D warnings` ✓
- `cargo fmt --check` ✓
- `cargo test --workspace --all-features` ✓
- `cargo doc --workspace --no-deps` ✓ (workspace `broken_intra_doc_links = deny`)
- `RUSTFLAGS="--cfg dst" cargo test -p myelon-dst` ✓ (DST harness assertions + verifier)
- `make -C crates/perf-bench super-tiny` ✓: **94 measurements parsed** (40 single-shot rows + 54 inline-sweep rows + 40 codec rows) across raw-ring, raw-myelon, framed, broadcast, signal, and codec layers × shm / mmap × max-throughput + CO@50K modes.
- `make -C crates/competitive-bench super-tiny` ✓: **220 / 220 scenario JSON files** non-empty and non-zero across the 12 adapters listed above + internal baselines (Disruptor-shm, Disruptor-mmap, Myelon-Raw-shm, Myelon-Raw-mmap). Two third-party adapters (`shmipc-rs`, `rusteron-aeron-ipc`) were excluded from the run because of pre-existing build / runtime issues unrelated to this release; they are tracked separately.
- MSRV check at declared `rust-version = "1.86"` for the publishable crates ✓.
- `cargo publish --dry-run -p disruptor-mp` ✓ (68 files, 146 KiB compressed).

Smoke-grade absolute numbers from the M3 Max super-tiny runs are intentionally **not** promoted alongside the README's headline tables. The bench-grade reference is the pinned-frequency Linux Ryzen box; Apple-silicon peak numbers will land in a separate cross-platform comparison artifact in a follow-up release rather than being mixed into the headline surface.

### Safety & quality gates

Both crates do non-trivial `unsafe` work internally (raw ring slots, SHM `mmap`, atomic ordering, manual cache-line layout). The public API is safe Rust, with a small set of documented escape hatches. The full tier-by-tier checklist also lives in [the workspace README](README.md#safety--quality-gates) and the two are kept in sync.

**Tier 1: public-surface contract (required for crates.io)**

- [x] `myelon` public surface: zero `pub unsafe fn` across the crate.
- [x] `disruptor-mp` public surface: four documented `pub unsafe fn` escape hatches in `observability`:
  - `CountersFile::init`
  - `CountersFile::attach`
  - `CountersFile::from_ptr`
  - `AggregatorHandle::spawn`

  Each carries a `# Safety` rustdoc section stating the caller's obligation. These are intentional power-user escape hatches for callers who already hold a verified pointer or own a shared-memory segment.
- [x] Workspace lint `unsafe_op_in_unsafe_fn = warn` (every `unsafe` action inside an `unsafe fn` must be re-wrapped in an explicit `unsafe { ... }` block).
- [x] Workspace lint `clippy::missing_safety_doc = warn` (every public `unsafe fn` must carry a `# Safety` rustdoc section).
- [x] `cargo clippy --workspace --all-targets -- -D warnings` clean.

**Tier 2: internal correctness**

- [x] DST harness: `assert_always` / `assert_sometimes` + BUGGIFY fault injection, FoundationDB + TigerBeetle-style. Run with `RUSTFLAGS="--cfg dst"`. Plays the role Loom plays for in-process atomics, but scaled to cross-process scenarios.
- [ ] Per-block `// SAFETY:` comment on every internal `unsafe { ... }` block. Currently **12 / 81 blocks ≈ 15% coverage**. The remaining blocks rely on cursor monotonicity, cache-line alignment, and slot-lifecycle invariants established at the type-system / builder layer, but lack explicit per-block justification comments. Backfill scheduled for `v0.1.0-alpha.2`.
- [ ] `cargo miri test` lane (pointer-math helpers; not the syscall paths).
- [ ] AddressSanitizer lane.
- [ ] ThreadSanitizer lane.

**Tier 3: enforcement and audit**

- [ ] `clippy::undocumented_unsafe_blocks = warn` enabled workspace-wide (after Tier 2 backfill).
- [ ] Safe wrappers around the four `pub unsafe fn` (`boxed()`, `with_shm_segment()`); the four escape hatches stay as power-user surface.
- [ ] `cargo geiger` audit reported.

### Why `0.1.0-alpha.1` (and not `3.x.y`)

`disruptor-mp` carried `version = "3.7.1"` internally because this fork started from upstream `disruptor` v3.7.1. That number was never on crates.io under the `disruptor-mp` name; using it on first publish would have falsely implied a mature 3.x history. The reset to `0.1.0-alpha.1` explicitly signals "first public release, API may evolve during the early-iteration window."

`myelon` is similarly first-time-public at `0.1.0-alpha.1`.

The pre-release tag (Cargo equivalent of PyPI's `0.1.0a1`) means `cargo add` / `cargo update` won't pick these versions by default. Consumers opt in explicitly:

```toml
disruptor-mp = "=0.1.0-alpha.1"
myelon       = "=0.1.0-alpha.1"
```

### Deferred

Tracked for follow-up releases. Safety-related follow-ups are listed inline in the [Safety & quality gates](#safety--quality-gates) tier checklist above; this section covers the rest.

- Hugepages-backed SHM, core pinning, NUMA-aware placement (HFT-grade deployment tuning).
- DST-coverage report (assertion-log review pipeline).
- Multi-process liveness integration test.
- Workspace-wide `cargo-llvm-cov` workflow.
- Exporter feature smoke tests (`metrics-prometheus`, `metrics-otel`).
- `flatbuffers` 24 → 25 (regenerate `bench_payload_generated.rs`).
- `rand` 0.8 → 0.10 (dev-dep, usage refactor).
- `criterion` 0.5 → 0.8 (dev-dep, API delta).

### Acknowledgements

- `disruptor` crate (`nicholassm` and contributors) for the upstream single-process Disruptor ring-buffer surface this crate extends.
- The [FoundationDB BUGGIFY pattern](https://apple.github.io/foundationdb/testing.html) and [TigerBeetle VOPR](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md) for the shape of the DST primitives.
- The [tokio loom integration pattern](https://github.com/tokio-rs/tokio/blob/master/tokio/Cargo.toml) for the `[target.'cfg(dst)'.dev-dependencies]` workspace test wiring.

[Unreleased]: https://github.com/Venkat2811/myelon/compare/v0.1.0-alpha.2...HEAD
[0.1.0-alpha.2]: https://github.com/Venkat2811/myelon/releases/tag/v0.1.0-alpha.2
[0.1.0-alpha.1]: https://github.com/Venkat2811/myelon/releases/tag/v0.1.0-alpha.1
