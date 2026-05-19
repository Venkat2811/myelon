<p align="center">
  <img src="assets/myelon-pulse-demo.gif" alt="myelon-pulse — angiogram-style brand demo: trunk, fractal branches, and pulses for myelon, the multiprocess shared-memory transport" width="900">
</p>

# myelon (workspace)

Repo for the [`myelon`](https://crates.io/crates/myelon) and [`disruptor-mp`](https://crates.io/crates/disruptor-mp) crates, plus the internal harnesses, benches, and example crates that validate them.

`myelon` is multiprocess shared-memory transport for inference and other low-latency pipelines. It offers **simplified access to `disruptor-mp`'s core capabilities** plus framing, codecs, typed zero-copy, and topology layered on top — all behind one stable public surface.

> **Publishable crates.** `disruptor-mp` (the Layer 0 substrate) and `myelon` (a single façade over `disruptor-mp` + Layers 1–3 + orthogonal concerns). The rest of the workspace is internal benches, DST harnesses, and runnable examples (`publish = false`).

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
├── myelon-dst/          # Internal. Multiprocess DST harness, fixtures, oracle, child runner.
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
| `RUSTFLAGS="--cfg dst"` | Compile deterministic-simulation hooks used by the internal `myelon-dst` harness. |

`myelon`:

| Feature | Adds |
|---|---|
| (default) | Layers 0, 1; Layer 2 with `bincode` only. |
| `rkyv` | Layer 2/3 with `rkyv`. |
| `flatbuffers` | Layer 2/3 with `flatbuffers`. |
| `RUSTFLAGS="--cfg dst"` | Pull in the internal `myelon-dst` dev-dependency for DST-backed test lanes. |

## Bench harnesses

- `crates/perf-bench` — broad internal sweep universe across all layers (raw, framed, codec, typed_zc), both backends (`shm`, `mmap`), and three modes (throughput, fixed-rate coordinated-omission-aware, batch-timing). Runs against `disruptor-mp` and `myelon` natively.
- `crates/competitive-bench` — narrow apples-to-apples transport comparison harness against `crossbar`, `shmipc`, `rusteron`, `iceoryx2`, `zeromq`, `boost::interprocess message_queue`, and `ompi`. Uses `disruptor-mp` + `myelon` raw layers as internal baselines.

### Headline numbers — Apple M3 Max, single laptop

Built `--profile competitive`, measured with `perf-bench-pingpong`, busy-spin wait, single-producer single-consumer, HDR-histogram percentiles. Cross-process round-trip (real `fork()`-style multiprocess, not threaded), one warm-up run discarded.

| Layer              | Backend | Payload | Throughput   | p50    | p99    | Notes                                       |
| ---                | ---     | ---     | ---          | ---    | ---    | ---                                         |
| `raw_ring`         | mmap    | 64 B    | **4.84 M ops/s** | **125 ns** | 500 ns | Layer 0 substrate, file-backed              |
| `raw_ring`         | shm     | 64 B    | 4.69 M ops/s | 167 ns | 250 ns | Layer 0 substrate, POSIX SHM                |
| `typed_zc` (rkyv)  | shm     | 4 KB    | 113 K ops/s  | 8.0 μs | 13 μs  | Layer 3 zero-copy, **11.7× faster than parse** |
| `typed_zc` (rkyv)  | shm     | 36 KB   | 66 K ops/s   | 15 μs  | 21 μs  | 2.5 GB/s payload throughput                 |
| `typed_zc` (rkyv)  | shm     | 146 KB  | 20 K ops/s   | 49 μs  | 71 μs  | 3.0 GB/s payload throughput                 |

Reproduce the top row:

```bash
cargo run --profile competitive -p perf-bench --bin perf-bench-pingpong -- \
  --layer raw_ring --backend mmap -n 200000 -w 5000 --tree
```

The full sweep matrix (layer × backend × codec × payload × mode × consumer-count) is wrapped under `make` targets — see *One-command workflows* below. Comparison against external transports (`zmq`, `iceoryx2`, `crossbar`, `rusteron`, `shmipc`) lives in `crates/competitive-bench`.

## One-command workflows

- Competitive exact-size smoke: `make -C crates/competitive-bench simple-smoke`
- Competitive broad perf gate: `make -C crates/competitive-bench super-tiny`
- Internal exact-size smoke: `make -C crates/perf-bench simple-smoke`
- Internal broad perf gate: `make -C crates/perf-bench super-tiny`
- Fast benchmark smoke (~60s): `make smoke`
- Workspace wiring + crate boundary checks: `make workspace-smoke`
- Rust-tier orchestration (format/lint/tests/bench+example compile checks): `make orchestrate-rust`
- Full monorepo orchestration: `make orchestrate-all`

## Platform support

- **Linux** — officially supported.
- **macOS** — known to work for core multiprocess flows, but currently unsupported for official guarantees.
- **Windows** — unsupported.

## Acknowledgements

- **[LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor)** (Java) — Martin Thompson, Mike Barker, Dave Farley, and the LMAX Exchange team — for the original lock-free ring-buffer design and the mechanical-sympathy thinking this whole lineage descends from.
- **[`disruptor-rs`](https://github.com/nicholassm/disruptor-rs)** — Nicholas Schultz-Møller and contributors — for the single-process Rust port (the [`disruptor`](https://crates.io/crates/disruptor) crate) that `disruptor-mp` extends to cross-process.
- **vLLM** — the [`shm_broadcast.py`](https://github.com/vllm-project/vllm/blob/main/vllm/distributed/device_communicators/shm_broadcast.py) `ShmRingBuffer` (single-producer / multiple-consumer shared-memory ring for cross-worker broadcast) is the same pattern in the same problem space; we're indebted to it for showing the shape of the right answer in Python land.
- Bill Dally (NVIDIA Chief Scientist) and Jeff Dean (Google), [_Advancing to AI's Next Frontier_](https://www.youtube.com/watch?v=g8BuAtM3fp4) (GTC 2026) — for framing nanosecond-scale chip-level optimization and the "latency is communication, not computation" insight that motivates `myelon`'s focus on shared-memory transport.

## Citation

If you use `myelon` or `disruptor-mp` in research or downstream work, please cite the relevant crate.

**`myelon`** — the layered façade (framing, codecs, typed zero-copy, topology):

```
Venkat Raman (@venkat_systems). "myelon: Low-latency, high-throughput, zero-copy typed transport over multiprocess SHM and mmap ring buffers for inference and other low-latency pipelines". GitHub (2026). https://github.com/Venkat2811/myelon
```

```bibtex
@misc{venkat2026myelon,
  title        = {myelon: Low-latency, high-throughput, zero-copy typed transport over multiprocess SHM and mmap ring buffers --- framing, codecs, typed zero-copy, and topology for inference and other low-latency pipelines},
  author       = {Venkat Raman},
  year         = {2026},
  publisher    = {GitHub},
  url          = {https://github.com/Venkat2811/myelon},
  note         = {Twitter/X: \url{https://twitter.com/venkat_systems}}
}
```

**`disruptor-mp`** — the Layer 0 substrate (raw cross-process ring buffer):

```
Venkat Raman (@venkat_systems). "disruptor-mp: Low-latency, high-throughput multiprocess SHM and mmap ring buffers for Disruptor-style publication". GitHub (2026). https://github.com/Venkat2811/myelon
```

```bibtex
@misc{venkat2026disruptormp,
  title        = {disruptor-mp: Low-latency, high-throughput multiprocess SHM and mmap ring buffers for Disruptor-style publication, with cross-process producer/consumer coordination and observability counters},
  author       = {Venkat Raman},
  year         = {2026},
  publisher    = {GitHub},
  url          = {https://github.com/Venkat2811/myelon},
  note         = {Twitter/X: \url{https://twitter.com/venkat_systems}}
}
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
