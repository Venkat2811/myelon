# myelon (workspace)

Repo for the [`myelon`](https://crates.io/crates/myelon) and
[`disruptor-mp`](https://crates.io/crates/disruptor-mp) crates, plus
their internal bench and test-runner support crates. The repo name
predates the crate rename — `myelon` is the GitHub repo;
the publishable crate is `myelon`.

`myelon` is multiprocess shared-memory transport for inference and
other low-latency pipelines, built as concentric layers on top of
`disruptor-mp`.

> **Publishable crates.** `disruptor-mp` (Layer 0) and `myelon`
> (Layers 1–3 + orthogonal concerns). The other four crates in this
> repo are internal benches and test infrastructure
> (`publish = false`).

## The onion

```
┌──────────────────────────────────────────────────────────┐
│  myelon                                                  │
│  Layer 1 — framed transport                              │
│  Layer 2 — codec (bincode / rkyv / flatbuffers)          │
│  Layer 3 — typed zero-copy                               │
│  + topology, layout, observability re-exports            │
├──────────────────────────────────────────────────────────┤
│  disruptor-mp                                            │
│  Layer 0 — raw cross-process ring buffer                 │
│  + coordination, discovery, liveness, observability      │
├──────────────────────────────────────────────────────────┤
│  disruptor (crates.io, upstream)                         │
│  single-process / threaded primitives                    │
└──────────────────────────────────────────────────────────┘
```

You depend on the **outermost** layer that satisfies your needs and
the inner ones come along for the ride. Most users only need
`myelon`.

## Where to go for what

| You want to … | Crate | Notes |
|---|---|---|
| Cross-process publish/consume of a fixed-size `Copy` event over a ring buffer | [`disruptor-mp`](crates/disruptor-mp/) (Layer 0) | Or `myelon`, which re-exports the same types. |
| Variable-length byte messages with start/end flags + multi-frame fragmentation | [`myelon`](crates/myelon/) (Layer 1) | `FramedTransportProducer<F>` / `FramedTransportConsumer<F>`. |
| Typed messages with serialisation (bincode / rkyv / flatbuffers) | [`myelon`](crates/myelon/) (Layer 2) | `TypedProducer<F>` / `TypedConsumer<F>` + `Codec` impl. |
| Zero-copy in-place reads of serialised data | [`myelon`](crates/myelon/) (Layer 3) | `ZeroCopyCodec` + the typed transport above. |
| Fixed scheduler / N-worker topology with discovery + rendezvous | [`myelon`](crates/myelon/) | `FixedTopology`, `WorkerCount` (2..=8). |
| Per-process hot-path counters (events_published, consumer_lag_max, …) | [`disruptor-mp::observability`](crates/disruptor-mp/the workspace book) | Re-exported from `myelon::observability`. |

## Layout

```
crates/
├── disruptor-mp/        # Publishable. Layer 0: raw cross-process ring buffer.
├── myelon/              # Publishable. Layers 1, 2, 3 + topology + observability.
├── dst-fixtures/        # Internal. Deterministic-simulation test fixtures.
├── dst-runner/          # Internal. Multiprocess DST harness.
├── perf-bench/          # Internal. Performance benchmark consolidation.
└── competitive-bench/   # Internal. Apples-to-apples external transport comparison.
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

- `crates/perf-bench` — broad internal sweep universe across all
  layers (raw, framed, codec, typed_zc), both backends (`shm`,
  `mmap`), and three modes (throughput, fixed-rate
  coordinated-omission-aware, batch-timing). Runs against
  `disruptor-mp` and `myelon` natively.
- `crates/competitive-bench` — narrow apples-to-apples transport
  comparison harness against `crossbar`, `shmipc`, `rusteron`,
  `iceoryx2`, `zmq`, `iggy`, `redpanda`. Uses
  `disruptor-mp` + `myelon` raw layers as internal
  baselines.

## One-command workflows

- Competitive exact-size smoke:
  - `make -C crates/competitive-bench simple-smoke`
- Internal exact-size smoke:
  - `make -C crates/perf-bench simple-smoke`
- Fast benchmark smoke (~60s):
  - `make smoke`
- Workspace wiring + crate boundary checks:
  - `make workspace-smoke`
- Rust-tier orchestration (format/lint/tests/bench+example compile checks):
  - `make orchestrate-rust`
- Full monorepo orchestration:
  - `make orchestrate-all`

## Platform policy

- **Linux** — officially supported.
- **macOS** — exercised and expected to work for primary multiprocess
  paths, but not an officially supported target.
- **Windows** — unsupported.

## License

MIT.
