# Layered architecture

The central organising idea is a four-layer onion. Each layer wraps the layer below; you only pay for the layers you use.

```text
┌───────────────────────────────────────────────────────────┐
│ myelon                  ← full layered façade             │
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

## Two ways to depend on it

`myelon` is intentionally a **simplified façade** for `disruptor-mp`'s core capabilities — the Layer 0 types you'd reach for in a substrate-only project (`SharedProducer`, builders, `CoordinationMode`, `RequiredConsumerLivenessConfig`, `observability::*`) are all re-exported under `myelon::*`. Both crates are first-class; pick by what surface your code actually needs:

- **Full layered stack** — depend on `myelon`. `use myelon::{SharedProducer, build_shared_single_producer, …};` for raw-ring work, or step up to `myelon::transport::*` / `myelon::typed_transport::*` when you need framing or typing. One dep covers everything.
- **Substrate only** — depend on `disruptor-mp` directly. The Layer 0 surface is identical to what `myelon` re-exports, but framing / codec / typed-zero-copy / topology / layout aren't compiled into your binary. Useful when you're publishing your own wire-format crate on top of the substrate, or you simply want the smaller dependency surface. See [`examples/demos/disruptor_mp_shm.rs`](https://github.com/Venkat2811/myelon/blob/main/examples/demos/disruptor_mp_shm.rs) and [`examples/demos/disruptor_mp_mmap.rs`](https://github.com/Venkat2811/myelon/blob/main/examples/demos/disruptor_mp_mmap.rs).

The type identity is preserved across the boundary — a `disruptor_mp::SharedConsumer<E>` *is* a `myelon::SharedConsumer<E>` — so helpers, rendezvous primitives, and patterns transfer unchanged between the two profiles.

## Orthogonal concerns

These wrap *across* layers — pick them by what your *system* needs, not what your *wire format* needs:

- **Coordination** — `CoordinationMode::{Immediate, WaitForConsumers, Discovery}`
- **Discovery** — explicit consumer ID or prefix-based
- **Liveness** — `RequiredConsumerLivenessConfig` (RFC 0017.5)
- **Topology** — `FixedTopology`, `WorkerCount`
- **Layout** — `MyelonTransportLayout` (macOS-safe SHM naming)
- **Observability** — `observability::*` (RFC 0040 hot-path counters)

See [When to use which layer](when-to-use-which.md) for the decision matrix and [Costs per layer](costs.md) for the trade-offs.
