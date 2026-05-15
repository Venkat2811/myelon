<p align="center">
  <img src="assets/myelon-pulse-demo.gif" alt="myelon-pulse — angiogram-style brand demo: trunk, fractal branches, and pulses for myelon, the multiprocess shared-memory transport" width="900">
</p>

# myelon (workspace)

Repo for the [`myelon`](https://crates.io/crates/myelon) and [`disruptor-mp`](https://crates.io/crates/disruptor-mp) crates, plus their internal bench and test-runner support crates. The repo name predates the crate rename — `myelon` is the GitHub repo; the publishable crate is `myelon`.

`myelon` is multiprocess shared-memory transport for inference and other low-latency pipelines. It offers **simplified access to `disruptor-mp`'s core capabilities** plus framing, codecs, typed zero-copy, and topology layered on top — all behind one stable public surface.

> **Publishable crates.** `disruptor-mp` (the Layer 0 substrate) and `myelon` (a single façade over `disruptor-mp` + Layers 1–3 + orthogonal concerns). The other four crates in this repo are internal benches and test infrastructure (`publish = false`).

## The onion

```
┌──────────────────────────────────────────────────────────────┐
│ myelon                       ← full layered façade           │
│                                                              │
│   Layer 3 — typed zero-copy                                  │
│   Layer 2 — codec (bincode / rkyv / flatbuffers)             │
│   Layer 1 — framed transport                                 │
│                                                              │
│   ┌──────────────────────────────────────────────────────┐   │
│   │ Layer 0  (re-exported from disruptor-mp)             │   │
│   │   SharedProducer<E> / SharedConsumer<E>      (SHM)   │   │
│   │   MmapProducer<E>   / MmapConsumer<E>        (mmap)  │   │
│   │   builders, coordination, discovery, liveness,       │   │
│   │   observability counters                             │   │
│   └──────────────────────────────────────────────────────┘   │
│                                                              │
│   + FixedTopology / WorkerCount   (topology shape)           │
│   + MyelonTransportLayout         (macOS-safe SHM names)     │
│   + observability::*              (RFC-0040 re-export)       │
└────────────────────────────────┬─────────────────────────────┘
                                 │ depends on
                                 ▼
                          disruptor-mp
                          (also publishable on its own for users
                          who want only the Layer 0 substrate)
                                 │ depends on
                                 ▼
                          disruptor (crates.io, upstream)
                          single-process / threaded primitives
```

Both crates are first-class entry points — pick by what surface your code actually needs:

- **Depend on [`myelon`](crates/myelon/)** for the full layered stack. It re-exports every relevant `disruptor-mp` type, so one dep gives you Layer 0 plus framing / codec / typed-zero-copy / topology / layout.
- **Depend on [`disruptor-mp`](crates/disruptor-mp/) directly** for Layer 0 only — the raw cross-process ring plus its coordination, discovery, liveness, and observability primitives. Smaller dep footprint; suits substrate-only consumers and projects that publish their own wire format on top.

The type identity is preserved across the boundary — a `disruptor_mp::SharedConsumer<E>` *is* a `myelon::SharedConsumer<E>` — so helpers, rendezvous primitives, and patterns transfer unchanged between the two profiles.

## Where to go for what

Every row below is reachable from [`myelon`](crates/myelon/); the right-most column flags rows where depending on [`disruptor-mp`](crates/disruptor-mp/) directly is also a clean choice.

| You want to … | Layer | Type | Direct on `disruptor-mp`? |
|---|---|---|---|
| Cross-process publish/consume of a fixed-size `Copy` event over a ring buffer | 0 — raw | `SharedProducer<E>` / `SharedConsumer<E>` (SHM); `MmapProducer<E>` / `MmapConsumer<E>` (mmap). All re-exported by `myelon`. | Yes — substrate alone. |
| Variable-length byte messages with start/end flags + multi-frame fragmentation | 1 — framed | `FramedTransportProducer<F>` / `FramedTransportConsumer<F>` | No — only on `myelon`. |
| Typed messages with serialisation (bincode / rkyv / flatbuffers) | 2 — codec | `TypedProducer<F>` / `TypedConsumer<F>` + `Codec` impl | No. |
| Zero-copy in-place reads of serialised data | 3 — typed zero-copy | `ZeroCopyCodec` + the typed transport above | No. |
| Fixed scheduler / N-worker topology with discovery + rendezvous | orthogonal | `FixedTopology`, `WorkerCount` (2..=8) | No. |
| Per-process hot-path counters (events_published, consumer_lag_max, …) | orthogonal | `observability::*` | Yes — same surface lives on both crates. |

## Layout

```
crates/
├── disruptor-mp/        # Publishable. Layer 0: raw cross-process ring buffer.
├── myelon/              # Publishable. Layers 1, 2, 3 + topology + observability.
├── dst-fixtures/        # Internal. Deterministic-simulation test fixtures.
├── dst-runner/          # Internal. Multiprocess DST harness.
├── perf-bench/          # Internal. Performance benchmark consolidation.
└── competitive-bench/   # Internal. Apples-to-apples external transport comparison.

examples/
├── demos/                  # Workspace-level runnable examples (one place for them all).
│                           #   shm_disruptor.rs            — Layer 0, SHM, multiprocess quick start
│                           #   mmap_disruptor.rs           — Layer 0, mmap, multiprocess quick start
│                           #   pingpong.rs                 — multiprocess RTT request/response
│                           #   counters.rs                 — RFC-0040 observability end-to-end
│                           #   fixed_inference_topology.rs — myelon::FixedTopology demo
│                           #   required_consumer_liveness.rs — RFC-0017.5 same-ID rejoin recovery
└── myelon-pulse-vanity/    # Brand vanity demo (the video at the top of this README).

book/                    # mdBook source for the user-facing docs site.
                         # Build: `mdbook build` (output at book/build, gitignored).
```

Run any example with:

```bash
cargo run --release -p demos --example <name>
```

## Cargo features (high-impact)

`disruptor-mp`:

| Feature | Adds |
|---|---|
| `metrics` (default) | Wire `observability` counters into the `metrics`-rs façade. |
| `metrics-prometheus` | `metrics-exporter-prometheus`. |
| `metrics-otel` | `opentelemetry-otlp` for OTLP export. |
| `dst` | DST hooks against `dst-fixtures`. |

`myelon`:

| Feature | Adds |
|---|---|
| (default) | Layers 0, 1; Layer 2 with `bincode` only. |
| `rkyv` | Layer 2/3 with `rkyv`. |
| `flatbuffers` | Layer 2/3 with `flatbuffers`. |
| `dst` | Forwards to `disruptor_mp/dst`. |

## Bench harnesses

- `crates/perf-bench` — broad internal sweep universe across all layers (raw, framed, codec, typed_zc), both backends (`shm`, `mmap`), and three modes (throughput, fixed-rate coordinated-omission-aware, batch-timing). Runs against `disruptor-mp` and `myelon` natively.
- `crates/competitive-bench` — narrow apples-to-apples transport comparison harness against `crossbar`, `shmipc`, `rusteron`, `iceoryx2`, `zmq`, `iggy`, `redpanda`. Uses `disruptor-mp` + `myelon` raw layers as internal baselines.

## One-command workflows

- Competitive exact-size smoke: `make -C crates/competitive-bench simple-smoke`
- Internal exact-size smoke: `make -C crates/perf-bench simple-smoke`
- Fast benchmark smoke (~60s): `make smoke`
- Workspace wiring + crate boundary checks: `make workspace-smoke`
- Rust-tier orchestration (format/lint/tests/bench+example compile checks): `make orchestrate-rust`
- Full monorepo orchestration: `make orchestrate-all`

## Platform support

- **Linux** — supported.
- **macOS** — supported.
- **Windows** — experimental.

## Acknowledgements

- **[LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor)** (Java) — Martin Thompson, Mike Barker, Dave Farley, and the LMAX Exchange team — for the original lock-free ring-buffer design and the mechanical-sympathy thinking this whole lineage descends from.
- **[`disruptor-rs`](https://github.com/nicholassm/disruptor-rs)** — Nicholas Schultz-Møller and contributors — for the single-process Rust port (the [`disruptor`](https://crates.io/crates/disruptor) crate) that `disruptor-mp` extends to cross-process.
- Bill Dally (NVIDIA Chief Scientist) and Jeff Dean (Google), [_Advancing to AI's Next Frontier_](https://www.youtube.com/watch?v=g8BuAtM3fp4) (GTC 2026) — for framing nanosecond-scale chip-level optimization and the "latency is communication, not computation" insight that motivates `myelon`'s focus on shared-memory transport.

## License

MIT.
