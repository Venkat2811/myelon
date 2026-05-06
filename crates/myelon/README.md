# myelon

Multiprocess shared-memory transport for inference and other low-latency pipelines.

`myelon` is the **single, simplified façade** for the `disruptor-mp` substrate. It re-exports every relevant type from [`disruptor-mp`](../disruptor-mp/) (Layer 0 — the raw cross-process ring buffer plus its coordination, discovery, liveness, and observability primitives) and adds three more layers on top: framing, codec, and typed zero-copy. One dependency, one stable API surface, the whole stack.

## The onion

```
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

Each layer **wraps** the layer below: a `TypedProducer` wraps a `FramedTransportProducer` wraps a `SharedProducer`. You only pay for the layers you use. Pick the outermost layer that satisfies your needs — the inner ones (and Layer 0) are reachable through `myelon` without adding `disruptor-mp` as a separate dependency.

## When to use which layer

| Need | Layer | Type |
|---|---|---|
| Fixed-size, `Copy` event over a ring buffer. You own the wire format. | 0 — raw ring | `SharedProducer<E>` / `SharedConsumer<E>` (SHM) or `MmapProducer<E>` / `MmapConsumer<E>` (mmap) |
| Variable-length `&[u8]` payloads, possibly larger than one ring slot. Need start/end flags + a message id. | 1 — framed | `FramedTransportProducer<F>` / `FramedTransportConsumer<F>` (SHM) or `MmapFramedTransportProducer<F>` / `MmapFramedTransportConsumer<F>` (mmap) |
| Typed message with serialisation (bincode / rkyv / flatbuffers). Owned decode on the consumer side. | 2 — codec | `TypedProducer<F>` / `TypedConsumer<F>` + a `Codec` impl |
| Same as Layer 2 but consumer reads serialised data in-place — no `deserialize` allocation. | 3 — typed zero-copy | `TypedProducer<F>` / `TypedConsumer<F>` + a `ZeroCopyCodec` impl |

If you're not sure: start at the highest layer that matches your data, profile, and only step down if you see allocator or codec cost in the profile.

## What each layer adds, and what it costs

| Layer | Adds | Approximate added cost |
|---|---|---|
| 0 | Cross-process publish/consume of fixed-size events. | — (this is the floor: ~150ns SHM RTT pingpong on Apple Silicon) |
| 1 | Header (kind, flags, msg_id), multi-frame fragmentation, reassembly buffer. | Header copy + per-frame branch on flags. |
| 2 | Encode/decode through a `Codec` (`bincode`, `rkyv`, `flatbuffers`). | Whatever the codec does. `bincode` allocates; `rkyv` is roughly a pointer cast + validation; `flatbuffers` is a vtable lookup. |
| 3 | `ZeroCopyCodec::access(...)` returns a borrowed view into the ring slot. No deserialize allocation on the consumer side. | Layer 2 cost minus the deserialize allocation. Encoder cost is unchanged. |

Numbers are illustrative; measure on your hardware via `perf-bench`.

## Choosing a frame size (Layer 1 and up)

The framed transport's frame type is a **const generic** — you pick the per-frame payload size at compile time:

```rust
use myelon::transport::{FixedFrame, AlignedFixedFrame};

// 64 KB total slot, 12-byte header, 65,524-byte payload capacity.
type Frame64K = FixedFrame<{ 64 * 1024 - 12 }>;

// Tiny 256-byte payload — useful for high message density of small messages.
type FrameTiny = FixedFrame<256>;

// Right-sized to fit a 4 KB payload exactly, no fragmentation.
type Frame4K  = FixedFrame<{ 4 * 1024 - 12 }>;

// 16-byte-aligned variant for typed zero-copy (rkyv / flatbuffers).
type ZcFrame  = AlignedFixedFrame<{ 64 * 1024 - 16 }>;
```

Header size is fixed:

| Frame type | Header bytes | Use payload capacity |
|---|---|---|
| `FixedFrame<N>` | 12 | `N` |
| `AlignedFixedFrame<N>` | 16 (padded so payload is 16-byte aligned) | `N` |

There is **nothing special about 64 KB** — that's just the convention [`perf-bench`](../perf-bench/) uses for its "fragmentation" mode. You own the choice. The trade-off is straightforward:

| Frame size | Effect |
|---|---|
| Larger `DATA_BYTES` | Larger ring slot → bigger SHM segment for the same depth. Fewer messages fit in a fixed memory budget. Avoids fragmentation for messages that fit in one slot. |
| Smaller `DATA_BYTES` | Smaller ring slot → tighter memory. Multi-frame fragmentation kicks in when payload exceeds `DATA_BYTES`. Fragmentation has a header copy + reassembly cost per extra frame. |
| Right-sized to dominant payload | No fragmentation at all (single-frame messages). Lowest overhead at the cost of flexibility — bigger payloads still fragment, just less often. |

Two modes you can copy from `perf-bench`:

- **Fragmentation mode (`frag`)** — fixed slot size (e.g. 64 KB), rely on the framing layer's start/last flags + `msg_id` to reassemble payloads larger than one slot.
- **No-fragmentation mode (`nofrag`)** — pick a frame size that matches your dominant payload, accept that the rare oversized message will fragment.

The slot size must be a compile-time constant because the ring buffer's memory layout depends on it. To support a runtime-chosen size, instantiate one transport per size class (the `nofrag` benchmark in `perf-bench` does this with a generic `pingpong<const N: usize>` worker).

## Zero-copy at every layer — what it actually means

Two different things in this codebase get called "zero-copy", and they're not the same:

| Sense | Where it happens | How |
|---|---|---|
| **Memory-level zero-copy** | Layer 0 already provides this. | `try_consume_next_leased()` and `consume_next_leased()` return `&E` *straight into the ring slot*. No allocation, no copy. Use this for fixed-size `Copy + repr(C)` events. |
| **Typed-format zero-copy** | Layer 3. | The bytes on the wire are already a serialised graph (`rkyv`'s `Archived<T>` / a flatbuffers root table); the consumer reads fields in-place via [`codec::ZeroCopyCodec::access`]. |

There is no separate "raw + typed zero-copy" layer because it would be a degenerate combination:

- If your event is fixed-size and `Copy + repr(C)`, **just put the struct in the slot** — Layer 0 is already memory-zero-copy and is faster than archiving via `rkyv`.
- If you want serialisation flexibility (variable size, schema evolution, polymorphism), you also want framing (`msg_id`, start/end flags, multi-frame fragmentation). Skipping the frame header to save 12 bytes loses every protocol feature framing provides.

So the four-layer picture covers both senses without overlap.

## Required-consumer liveness (RFC 0017.5)

`disruptor-mp` enforces strict broadcast — the slowest consumer gates capacity. Out of the box, that means a stalled or crashed consumer backpressures the producer indefinitely. The optional liveness layer turns that silent stall into a producer-observable, time-bounded event.

It's **opt-in** via a parallel `*_managed` publish surface. Existing unmanaged calls (`publish`, `try_publish`, `publish_batch`) keep their current semantics; nothing changes for callers that don't opt in.

### When to enable it

| Situation | Use the managed surface? |
|---|---|
| Production system where a stalled required consumer should fail the topology rather than block forever. | Yes. |
| Crash-and-rejoin needed (consumer process restarts under the same stable ID). | Yes — same-ID rejoin is the supported recovery path. |
| Idle topologies that have long quiet periods. | Either way is safe — the producer only checks liveness while gating is blocking it; idle does not trigger alerts. |
| Soft-realtime path where you'd rather block than fail. | No — leave the unmanaged calls as-is. |

### How to enable it

| Layer | Type | Methods |
|---|---|---|
| 0 (SHM) | `SharedProducer<E>` | `enable_required_consumer_liveness(cfg)`, `publish_managed`, `publish_batch_managed` |
| 0 (mmap) | `MmapProducer<E>` | same |
| 1 (SHM) | `FramedTransportProducer<F>` | `enable_required_consumer_liveness(cfg)`, `publish_managed` |
| 1 (mmap) | `MmapFramedTransportProducer<F>` | same |
| 2/3 (SHM) | `TypedProducer<F>` | `enable_required_consumer_liveness(cfg)`, `publish_managed` |
| 2/3 (mmap) | `MmapTypedProducer<F>` | same |

```rust,no_run
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use myelon::{
    RequiredConsumerLivenessConfig, RequiredConsumerFailureAction,
};
use std::time::Duration;

# let mut producer: myelon::SharedProducer<()> = unimplemented!();
producer.enable_required_consumer_liveness(RequiredConsumerLivenessConfig {
    required_consumer_ids: vec!["worker_0".into(), "worker_1".into()],
    startup_wait_timeout:    Duration::from_secs(10),
    progress_timeout:        Duration::from_secs(5),
    progress_check_interval: Duration::from_millis(100),
    shutdown_grace_period:   Duration::from_secs(2),
    failure_action:          RequiredConsumerFailureAction::GracefulShutdown,
    alert_hook:              None,
});

// Now use publish_managed instead of publish.
producer.publish_managed(|slot| { /* ... */ })?;
# Ok(()) }
```

### Failure surface

| Case | Outcome |
|---|---|
| Required consumer doesn't appear within `startup_wait_timeout`. | `RequiredConsumerError::StartupTimeout { missing }` from the first managed publish. |
| Required consumer stalls past `progress_timeout` while gating the producer. | One stderr alert + optional `RequiredConsumerAlertHook` callback. |
| Stall persists past `shutdown_grace_period`. | `RequiredConsumerError::GracefulShutdownTriggered { consumer_id, last_sequence, stalled_for }` from the next managed publish. |
| Required consumer crashes, then a new process attaches under the **same** consumer ID before the grace window expires. | Producer recovers, alert state clears, publishing resumes. |
| Crashed consumer never reappears. | Graceful shutdown path above. |

### Cost

The check is **cold-path only** — it runs only while the producer is blocked on a gating consumer. Steady-state publish cost is unchanged (validated under perf-bench in RFC 0017.5 §9). There is no consumer-side heartbeat — progress is observed from the cursor data the producer already needs for gating.

### Not provided by this layer

By design, the liveness layer does **not** add:

- dead-consumer eviction
- quorum / degraded-broadcast modes
- consumer-side autonomous failure policy
- "healthy consumers continue without the dead one"

The system stays strict-broadcast. If the dead consumer was required, the topology fails gracefully — that's the contract.

## Orthogonal concerns

These wrap **across** layers — pick them by what your *system* needs, not by what your *wire format* needs.

| Concern | Type | What it does |
|---|---|---|
| Coordination | `producer::CoordinationMode::{Immediate, WaitForConsumers, Discovery}` | When does the producer consider its peers attached? |
| Discovery | `attach_shared_consumer(...).with_consumer_id(...)` / `.discover_consumer_with_prefix(...)` | How does the producer find consumers — explicit ID, or by prefix? |
| Liveness | `RequiredConsumerLivenessConfig`, `RequiredConsumerFailureAction` | Should the producer treat a stalled required consumer as a failure or just an alert? |
| Topology | `FixedTopology`, `WorkerCount` (2..=8) | Pre-baked one-scheduler / N-worker shape with discovery + rendezvous. |
| Layout | `MyelonTransportLayout`, `MyelonTransportConfig`, `RunnerMyelonTransportConfig` | macOS-safe SHM segment names for one-engine / N-runner sessions. |
| Observability | `observability::*` (re-export of `disruptor_mp::observability`) | RFC-0040 hot-path counters file. Optional `metrics`-rs / Prometheus / OTLP exporters via feature flags on `disruptor-mp`. |

## Cargo features

| Feature | Layer / surface | What it enables |
|---|---|---|
| (default) | Layers 0, 1; Layer 2 with `bincode` only | Raw + framed transport, plus the bincode codec. |
| `rkyv` | Layer 2/3 with `rkyv` | Re-exports `rkyv` and the rkyv codec wrappers (including `ZeroCopyCodec`). |
| `flatbuffers` | Layer 2/3 with `flatbuffers` | Re-exports `flatbuffers` and the flatbuffers codec wrappers. |
| `dst` | tests only | Forwards to `disruptor_mp/dst` for deterministic-simulation hooks. |

## Quick start — Layer 0 (raw ring)

Producer:

```rust,no_run
use myelon::{
    attach_shared_consumer, build_shared_single_producer,
};
use myelon::producer::CoordinationMode;
use disruptor_mp::portable_shm_segment_name;

#[derive(Copy, Clone, Default)]
#[repr(C)]
struct Tick { ts_ns: u64, price: u64 }

let segment = portable_shm_segment_name("ticks");
let mut producer = build_shared_single_producer::<Tick>(&segment, 4096)
    .discover_consumer_with_prefix(1, "cp")
    .with_coordination(CoordinationMode::Immediate)
    .build_producer(Tick::default)
    .expect("build producer");

producer.publish(|slot| { slot.ts_ns = 1; slot.price = 100; });
```

Consumer in a different process:

```rust,no_run
use myelon::attach_shared_consumer;
# #[derive(Copy, Clone, Default)]
# #[repr(C)]
# struct Tick { ts_ns: u64, price: u64 }
# let segment = "ticks";
let mut consumer = attach_shared_consumer::<Tick>(segment, 4096)
    .with_consumer_id("cp_0")
    .build_consumer()
    .expect("attach consumer");

while let Some(tick) = consumer.try_consume_next_leased() {
    let _ = (tick.ts_ns, tick.price);
}
```

## Quick start — Layer 1 (framed) and up

Layer 1 wraps Layer 0:

```rust,no_run
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use myelon::transport::{
    AlignedFixedFrame, FramedTransportProducer, FramedTransportConsumer, MyelonWaitStrategy,
};

type Frame = AlignedFixedFrame<1024>;

let mut producer = FramedTransportProducer::<Frame>::create("rpc", 4096)?;
producer.publish(b"hello", /* kind = */ 1);

let mut consumer = FramedTransportConsumer::<Frame>::attach(
    "rpc", 4096, MyelonWaitStrategy::BusySpin,
)?;
let (kind, payload) = consumer.recv_message_blocking_owned();
let _ = (kind, payload);
# Ok(()) }
```

Layer 2/3 wraps Layer 1: construct a `TypedProducer<F>` / `TypedConsumer<F>` and implement `Codec` (or `ZeroCopyCodec`) for your message type.

## Relationship to `disruptor-mp`

`disruptor-mp` provides Layer 0 (the raw shared-memory ring buffer with cross-process producer/consumer coordination). `myelon` re-exports every relevant `disruptor-mp` type and stacks Layers 1, 2, and 3 on top, plus the orthogonal concerns above. Downstream code should depend on `myelon` only — there's no scenario where both crates make sense as direct dependencies.

## License

MIT.
