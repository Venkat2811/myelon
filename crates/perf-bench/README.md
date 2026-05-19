# perf-bench

> **Internal.** Not published to crates.io. Owns the broad internal performance sweep universe; the narrower external comparison surface lives in [`competitive-bench`](../competitive-bench/).

`perf-bench` consolidates the repository's performance benchmarks for the [`disruptor-mp`](../disruptor-mp/) and [`myelon`](../myelon/) transport stacks into a small set of binaries that share one set of event types, payload generators, latency recorders, coordination primitives, and reporting code.

## Binaries

| Binary | Scenarios |
|---|---|
| `perf-bench-pingpong` | 1p1c ping-pong. All four layers (raw_ring, framed, codec, typed_zc) × both backends (shm, mmap) × three modes (max-throughput, fixed-rate coordinated-omission-aware, low-overhead batch-timing). |
| `perf-bench-broadcast` | 1pNc broadcast for `1p4c` and `1p8c` across the raw, framed, codec, wait-strategy, sweep, and layout families. Typed zero-copy is ping-pong only today. |
| `perf-bench-signal` | Cache-line-sized signal events, no payload variation, raw layer only — pure throughput ceiling for "what can this hardware push through a disruptor ring?". |
| `perf-bench-repeatability` | Repeat a single configuration N times and emit canonical JSON for run-to-run variance analysis. |

## Capability matrix

| Dimension | Values |
|---|---|
| Layer | `raw_ring`, `raw_myelon`, `framed`, `codec`, `typed_zc` |
| Backend | `shm` (POSIX SHM), `mmap` (memory-mapped file) |
| Mode | `--throughput` (default), `--target-rate <ops/s>` (CO-aware fixed rate), `--batch-timing` (low-overhead) |
| Wait strategy | `--wait-strategy busyspin | spinloop | sleep | block` (RFC 0017.5 §8 cost model) |
| Codec | `--codec bincode | rkyv | flatbuffers` (Layer 2 / 3 only) |
| Fragmentation | `--frag` (64 KB fixed slots, multi-frame) / `--nofrag` (right-sized single-frame) |
| Coordination | Internal `UnifiedCoordination` (single SHM segment, 9 cache-line-padded atomics) for native lanes; external `BenchmarkCoordination` (5 separate cursor segments) for adapter lanes. Both styles run side-by-side. |
| Liveness | `--enable-counters` to attach RFC-0040 hot-path counters; required-consumer liveness wired for parity benches. |
| Output | `--json`, `--json-canonical`, `--json-out PATH`, `--csv-out`, `--md-out`, `--tree`. |

## Layer hierarchy (the onion exercised in concrete benches)

```
        consumer-side checksum / decode
                  ▲
                  │  Layer 3 — typed zero-copy via myelon::ZeroCopyCodec
                  │  Layer 2 — owned decode via myelon::Codec (bincode/rkyv/flatbuf)
                  │  Layer 1 — myelon::FramedTransport (multi-frame, msg_id, flags)
                  │  Layer 0 — disruptor_mp::SharedProducer/Consumer (raw ring)
                  ▼
        producer publishes raw struct or framed bytes
```

Methodology note:
- Layer 1 framed benches use `myelon`'s leased receive path where available, so the reported cost is framing / fragmentation / reassembly overhead, not an avoidable owned-copy artifact.
- Layer 2 codec benches intentionally include owned decode cost.
- Layer 3 typed-zero-copy benches intentionally exercise in-place access.

Each scenario in `perf-bench` is named after the layer + backend it exercises, e.g. `layers/raw/disruptor_mp/pingpong_shm.rs` is "Layer 0, SHM backend, ping-pong shape." The directory tree mirrors the layer hierarchy exactly so you can read the filesystem and understand what the bench measures.

## File / directory structure

Per the perf-bench structure design, the source tree is organized strictly by layer, not by crate of origin:

```
crates/perf-bench/
├── Cargo.toml
├── Makefile
├── output/                                  # Local bench output (gitignored)
├── src/
│   ├── lib.rs                               # Crate root: layered modules + bench_support re-export.
│   │
│   ├── bin/                                 # Consolidated bench binaries.
│   │   ├── pingpong.rs                      # `perf-bench-pingpong`
│   │   ├── broadcast.rs                     # `perf-bench-broadcast`
│   │   ├── signal.rs                        # `perf-bench-signal`
│   │   └── repeatability.rs                 # `perf-bench-repeatability`
│   │
│   ├── bench_support/                       # Shared event types, competitor reference data,
│   │   ├── common.rs                        # helper functions consumed by per-layer modules and
│   │   ├── table.rs                         # by competitive-bench's adapter binaries.
│   │   └── mod.rs
│   │
│   ├── cli/                                 # CLI arg parsing + scenario specs.
│   │   ├── pingpong.rs
│   │   ├── myelon_pingpong.rs
│   │   ├── raw_ring.rs
│   │   ├── codec.rs
│   │   ├── framed.rs
│   │   ├── wait_strategy.rs
│   │   ├── layout.rs
│   │   └── sweeps/
│   │       ├── common.rs
│   │       ├── framed.rs
│   │       ├── layers.rs
│   │       ├── monster.rs
│   │       └── typed_zero_copy.rs
│   │
│   ├── layers/                              # The onion, mirrored on disk.
│   │   │
│   │   ├── raw/                             # Layer 0: direct ring, no framing.
│   │   │   ├── disruptor_mp/                # disruptor-mp substrate.
│   │   │   │   ├── pingpong_{shm,mmap}.rs
│   │   │   │   ├── broadcast_{shm,mmap}.rs
│   │   │   │   └── wait_strategy_{shm,mmap}.rs
│   │   │   └── myelon/                      # myelon raw-transport (Layer 0 façade).
│   │   │       ├── pingpong_{shm,mmap}.rs
│   │   │       ├── broadcast_{shm,mmap}.rs
│   │   │       └── wait_strategy_{shm,mmap}.rs
│   │   │
│   │   ├── framed_myelon/                   # Layer 1+: FramedTransport on top of raw.
│   │   │   ├── frag/                        # 64 KB fixed slots, multi-frame messages.
│   │   │   │   ├── pingpong_{shm,mmap}.rs
│   │   │   │   └── broadcast_{shm,mmap}.rs
│   │   │   ├── nofrag/                      # Right-sized slots, no fragmentation.
│   │   │   │   └── pingpong_{shm,mmap}.rs
│   │   │   ├── codec/                       # Layer 2: Codec on top of Framed (owned decode).
│   │   │   │   ├── frag/{pingpong,broadcast}_{shm,mmap}.rs
│   │   │   │   ├── nofrag/{shm,mmap}.rs
│   │   │   │   └── payloads.rs              # TestPayload + encode/decode/access helpers.
│   │   │   └── typed_zc/                    # Layer 3: zero-copy on top of Framed.
│   │   │       ├── pingpong_{shm,mmap}.rs
│   │   │       └── support.rs               # AlignedFixedFrame alias, telemetry.
│   │   │
│   │   ├── sweeps/                          # Parametric matrices across layers.
│   │   │   ├── layers.rs                    # Layer-overhead comparison.
│   │   │   ├── monster_{shm,mmap}.rs        # All-layer × all-size sweep.
│   │   │   ├── framed.rs
│   │   │   ├── typed_zc.rs
│   │   │   └── nofrag.rs
│   │   │
│   │   └── layout.rs                        # Memory layout timing validation gate.
│   │
│   ├── infra/                               # Shared infrastructure.
│   │   ├── child_runner.rs                  # Child-process role dispatch.
│   │   ├── process.rs                       # Process spawning.
│   │   ├── naming.rs                        # Segment naming.
│   │   ├── launch.rs                        # launch_shm_group, launch_mmap_group.
│   │   ├── config.rs                        # Env-var helpers, timeout.
│   │   ├── backend.rs                       # SHM / mmap backend abstraction.
│   │   ├── events.rs                        # BenchEvent, PingPongEvent, SignalEvent.
│   │   ├── latency.rs                       # LatencyRecorder, LatencyStats.
│   │   ├── allocation.rs                    # AllocationMetrics + tracking allocator.
│   │   ├── competitors.rs                   # Static competitor reference data.
│   │   ├── repeatability.rs                 # CV% analysis for run-to-run variance.
│   │   │
│   │   ├── coordination/                    # Process synchronization (two patterns).
│   │   │   ├── external.rs                  # BenchmarkCoordination (5 SHM cursors).
│   │   │   └── native.rs                    # UnifiedCoordination (9 padded atomics, 1 segment).
│   │   │
│   │   └── output/                          # All output concerns.
│   │       ├── dir.rs                       # Timestamped run directories.
│   │       ├── log.rs                       # BenchLog JSONL lifecycle.
│   │       ├── producer.rs                  # ProducerOutput struct.
│   │       ├── consumer.rs                  # ConsumerOutput struct.
│   │       └── report/                      # Formatted output renderers.
│   │           ├── model.rs                 # ReportBundle, ScenarioReport.
│   │           ├── divan_tree.rs            # Divan-style tree renderer.
│   │           ├── emit.rs                  # emit_report dispatcher.
│   │           ├── adapt.rs                 # BenchResult → ReportBundle compat.
│   │           ├── csv.rs / json.rs / markdown.rs / table.rs
│   │           ├── views.rs                 # MonsterSweep views, layer comparison.
│   │           └── reporting.rs             # BenchResult / BenchReport structs.
│   │
│   └── generated/                           # Code generated by `flatc`.
│       └── bench_payload_generated.rs
│
└── tests/                                   # Compile-test surfaces, fragmentation regression.
```

### Why this layout

- **`layers/` mirrors the onion exactly.** Read the filesystem, understand the architecture. Each `layer-x/y/z` path corresponds to one cell in the layer × backend × shape matrix.
- **`infra/coordination/` splits into `native` and `external`** — two fundamentally different sync mechanisms. Native uses a single SHM segment with 9 cache-line-padded atomics; external uses 5 separate cursor segments (the legacy multi-process pattern). Mixing them in one file obscured which benches use which.
- **`infra/output/report/`** keeps formatting concerns one `infra::output` import away from the benches.
- **`bench_support/`** holds primitive types (`BenchmarkEvent<const SIZE>`, `LatencyStats`, `CompetitorBenchmarks` reference data, etc.) shared with `competitive-bench`. These started life under `disruptor-mp/benches/ipc/competitive/` and were moved here when that bench tree was deleted as redundant with `perf-bench`.

## Quick start

```bash
# Default 1p1c ping-pong: raw ring, SHM, 64-byte payload, 100k messages.
cargo run -p perf-bench --release --bin perf-bench-pingpong -- \
    --layer raw_ring --backend shm

# RFC-0040 observability counters on, runs in a separate scenario
# from the default counter-free path.
cargo run -p perf-bench --release --bin perf-bench-pingpong -- \
    --layer raw_ring --backend shm --enable-counters

# Layer 2 codec with rkyv, mmap backend.
cargo run -p perf-bench --release --bin perf-bench-pingpong -- \
    --layer codec --backend shm --codec rkyv

# Layer 3 typed zero-copy, flatbuffers, fragmentation mode.
cargo run -p perf-bench --release --bin perf-bench-pingpong -- \
    --layer typed_zc --backend shm --codec flatbuffers

# 1p4c broadcast with fixed-rate coordinated-omission-aware mode.
cargo run -p perf-bench --release --bin perf-bench-broadcast -- \
    --layer raw_ring --backend mmap --consumers 4 --target-rate 1000000

# Hardware ceiling — signal-only events.
cargo run -p perf-bench --release --bin perf-bench-signal -- \
    --backend shm --consumers 1 --events 10000000
```

## Fast lanes

```bash
make -C crates/perf-bench simple-smoke
make -C crates/perf-bench super-tiny
```

- `simple-smoke`
  - exact-size sanity lane
  - signal + raw ping-pong + raw broadcast
  - raw ping-pong starts at `64B` total event size because the ping-pong event layout carries a `64B` structural header
  - raw broadcast `--size` is the logical event request; reports expose the physical aligned slot bytes separately when `32B` or `144B` round up
- `super-tiny`
  - broader OSS/CI gate
  - `100` warmup + `1000` measured messages/events where the scenario family exposes explicit warmup/message-count controls
  - covers signal, exact-size raw lanes, representative higher-layer ping-pong, representative higher-layer broadcast, wait-strategy, sweeps, and layout

## Relationship to other crates

- Wraps Layer 0 ([`disruptor-mp`](../disruptor-mp/)) and Layers 1–3 ([`myelon`](../myelon/)) without modifying them.
- The `bench_support/` module is consumed by [`competitive-bench`](../competitive-bench/) so the same `BenchmarkEvent<SIZE>` flows through native and external lanes.
- Workspace-level runnable examples for users who don't need a full sweep live in [`../../examples/`](../../examples/).

## License

Licensed under either of [Apache License, Version 2.0](../../LICENSE-APACHE) or [MIT license](../../LICENSE-MIT) at your option.
