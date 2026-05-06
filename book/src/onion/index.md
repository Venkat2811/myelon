# Layered architecture

The central organising idea is a four-layer onion. Each layer wraps the layer below; you only pay for the layers you use.

```text
┌───────────────────────────────────────────────────────────┐
│ myelon                  ← single dep for most users       │
│                                                           │
│   Layer 3 — typed zero-copy (ZeroCopyCodec)               │
│   Layer 2 — codec (bincode / rkyv / flatbuffers)          │
│   Layer 1 — framed transport (multi-frame, msg_id, flags) │
│                                                           │
│   ┌───────────────────────────────────────────────────┐   │
│   │ Layer 0  — re-exported from disruptor-mp:         │   │
│   │   Shared{Producer,Consumer}<E>            (SHM)   │   │
│   │   Mmap{Producer,Consumer}<E>              (mmap)  │   │
│   │   build_shared_single_producer / attach_…         │   │
│   │   CoordinationMode, discovery,                    │   │
│   │   RequiredConsumerLivenessConfig (RFC 0017.5),    │   │
│   │   observability::* (RFC 0040)                     │   │
│   └───────────────────────────────────────────────────┘   │
│                                                           │
│   + FixedTopology / WorkerCount    (topology shape)       │
│   + MyelonTransportLayout          (macOS-safe SHM names) │
│   + observability::*               (RFC-0040 re-export)   │
└───────────────────────────────────────────────────────────┘
```

| Layer | Type | Reachable via |
|---|---|---|
| 0 | `SharedProducer<E>` / `SharedConsumer<E>` (SHM); `MmapProducer<E>` / `MmapConsumer<E>` (mmap) | `myelon::*` (re-export) **or** `disruptor-mp::*` directly |
| 1 | `FramedTransportProducer<F>` / `FramedTransportConsumer<F>` (and mmap counterparts) | `myelon::transport::*` |
| 2 | `TypedProducer<F>` / `TypedConsumer<F>` + a `Codec` impl | `myelon::typed_transport::*` |
| 3 | Same typed transport + a `ZeroCopyCodec` impl | `myelon::typed_transport::*` + `myelon::codec::ZeroCopyCodec` |

## Why one dependency reaches everything

`myelon` is intentionally a **simplified façade** for `disruptor-mp`'s core capabilities, not just an additional crate stacked on top. The Layer 0 types you'd reach for in a substrate-only project (`SharedProducer`, builders, `CoordinationMode`, `RequiredConsumerLivenessConfig`, `observability::*`) are all re-exported under `myelon::*`, so:

- You add `myelon = "..."` to your `Cargo.toml` once.
- You `use myelon::{SharedProducer, build_shared_single_producer, …};` for raw-ring work, or step up to `myelon::transport::*` / `myelon::typed_transport::*` when you need framing or typing.
- You never reach into `disruptor-mp` directly unless you specifically don't want the higher layers compiled into your binary.

Reach for `disruptor-mp` as a direct dependency only when you want the substrate alone — for example, if you publish your own wire-format crate on top of it.

## Orthogonal concerns

These wrap *across* layers — pick them by what your *system* needs, not what your *wire format* needs:

- **Coordination** — `CoordinationMode::{Immediate, WaitForConsumers, Discovery}`
- **Discovery** — explicit consumer ID or prefix-based
- **Liveness** — `RequiredConsumerLivenessConfig` (RFC 0017.5)
- **Topology** — `FixedTopology`, `WorkerCount`
- **Layout** — `MyelonTransportLayout` (macOS-safe SHM naming)
- **Observability** — `observability::*` (RFC 0040 hot-path counters)

See [When to use which layer](when-to-use-which.md) for the decision matrix and [Costs per layer](costs.md) for the trade-offs.
