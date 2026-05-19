# disruptor-mp

Multiprocess shared-memory ring buffers for Disruptor-style publication.

`disruptor-mp` extends the upstream single-process [`disruptor`](https://crates.io/crates/disruptor) crate with a cross-process data plane: producers and consumers in different OS processes coordinate through a shared-memory segment with cache-line-padded sequence cursors and a fixed-size ring buffer.

## Where this crate sits

This is the substrate — Layer 0 of the project's layered model. Higher-level transports (framing, codecs, typed zero-copy, topology) live in [`myelon`](../myelon/), and `myelon` is **also a simplified façade for everything in this crate**: it re-exports every relevant type here, so most users only need to depend on `myelon`. Reach for `disruptor-mp` directly only when you want the raw ring buffer with no framing / codec / topology surface compiled in.

```
                  ┌──────────────────────────────────────┐
                  │  myelon                              │
                  │  Layers 1, 2, 3 + topology           │ ← simplified façade for
                  │  + re-exports of everything below    │   most users
                  └──────────────────┬───────────────────┘
                                     │ depends on
                  ┌──────────────────▼───────────────────┐
                  │  disruptor-mp  (this crate)          │
                  │  Layer 0: raw ring buffer            │ ← what you get here
                  │  + coordination, discovery,          │
                  │    liveness, observability counters  │
                  └──────────────────┬───────────────────┘
                                     │ depends on
                  ┌──────────────────▼───────────────────┐
                  │  disruptor  (crates.io)              │
                  │  single-process / threaded           │
                  └──────────────────────────────────────┘
```

## What this crate provides

| Concern | Type | Purpose |
|---|---|---|
| Raw ring (SHM) | `SharedProducer<E>`, `SharedConsumer<E>` | Cross-process publish/consume of fixed-size events over a POSIX shared-memory segment. |
| Raw ring (mmap) | `MmapProducer<E>`, `MmapConsumer<E>` | Same, backed by a memory-mapped file. |
| Builders | `build_shared_single_producer(...)`, `attach_shared_consumer(...)` | Construct producer/consumer with discovery and coordination wired up. |
| Coordination | `CoordinationMode::{Immediate, WaitForConsumers, Discovery}` | When does the producer consider its peers attached? |
| Liveness | `RequiredConsumerLivenessConfig`, `RequiredConsumerFailureAction` | A stalled required consumer is treated as a failure or alert, not silent backpressure. |
| Naming | `portable_shm_segment_name(name)` | Derive a macOS-safe SHM segment name from an arbitrary label. |
| Observability | `disruptor_mp::observability::*` (RFC 0040) | Hot-path counters file (`events_published`, `events_consumed`, `producer_full_events`, `consumer_empty_spins`, `consumer_lag_max`) plus optional `metrics`-rs / Prometheus / OTLP exporters. See the Layer 0 observability chapter in the workspace book. |

`E` is your event type — anything `Copy + Default + 'static` with a stable layout. The crate stays out of the wire-format business; reach for [`myelon`](../myelon/) when you need framing or serialisation, since `myelon` re-exports everything in this table and adds those layers on top.

## Boundary with upstream `disruptor`

- `disruptor-mp` owns multiprocess concerns: shared-memory lifecycle, producer/consumer coordination, mmap layout, observability, and multiprocess benchmarks/tests. Stable public namespaces:
  - `disruptor_mp::shared_memory`
  - `disruptor_mp::lock_free`
  - `disruptor_mp::backend`
- The crates.io [`disruptor`](https://crates.io/crates/disruptor) crate owns single-process / threaded APIs (`build_single_producer`, `build_multi_producer`, wait strategies, pollers).

The workspace book covers the public layered model, backend choices, and Layer 0 observability. This crate's public compatibility promise is the exported API in `disruptor_mp::{...}` plus the README examples here.

## Dependency Model

Internally this crate depends on crates.io `disruptor` as:

```toml
[dependencies]
disruptor_core = { package = "disruptor", version = "4.2.0" }
```

## Required-consumer liveness (RFC 0017.5)

The base producer/consumer model is strict broadcast — the slowest consumer gates capacity. By default that means a stalled or crashed required consumer backpressures the producer indefinitely. The liveness layer turns that silent stall into a producer-observable, time-bounded event — opt-in, no consumer-side heartbeat, no cost on the steady-state hot path.

```rust,no_run
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use disruptor_mp::{
    RequiredConsumerLivenessConfig, RequiredConsumerFailureAction,
    build_shared_single_producer,
};
use std::time::Duration;

let mut producer = build_shared_single_producer::<u64>("ring", 4096)
    .build_producer(Default::default)?;

producer.enable_required_consumer_liveness(RequiredConsumerLivenessConfig {
    required_consumer_ids: vec!["worker_0".into(), "worker_1".into()],
    startup_wait_timeout:    Duration::from_secs(10),
    progress_timeout:        Duration::from_secs(5),
    progress_check_interval: Duration::from_millis(100),
    shutdown_grace_period:   Duration::from_secs(2),
    failure_action:          RequiredConsumerFailureAction::GracefulShutdown,
    alert_hook:              None,
});

// Use publish_managed instead of publish — same arguments, but the result
// surfaces RequiredConsumerError instead of blocking forever on a stalled
// required consumer.
producer.publish_managed(|slot| { *slot = 42; })?;
# Ok(()) }
```

| Behaviour | What happens |
|---|---|
| Required consumer doesn't appear within `startup_wait_timeout`. | First `publish_managed` returns `RequiredConsumerError::StartupTimeout { missing }`. |
| Required consumer stalls past `progress_timeout` while gating the producer. | One stderr alert + optional `RequiredConsumerAlertHook` callback. |
| Same consumer ID rejoins before `shutdown_grace_period` expires. | Producer recovers, alert state clears, publishing resumes. |
| Stall persists past `shutdown_grace_period`. | Next `publish_managed` returns `RequiredConsumerError::GracefulShutdownTriggered { consumer_id, last_sequence, stalled_for }`. |
| Idle topology (no publish work). | Liveness is not checked. No false positives. |

The liveness check is **cold-path only** — it runs only while the producer is blocked on a gating required consumer. Steady-state publish cost is unchanged. Existing unmanaged calls (`publish`, `try_publish`, `publish_batch`) keep their original semantics for callers that don't opt in. By design, the liveness layer does **not** add dead-consumer eviction, quorum, or degraded-broadcast modes — the system stays strict-broadcast.

## Quick Start (Multiprocess)

```toml
[dependencies]
disruptor = { package = "disruptor-mp", version = "0.1.0-alpha.1" }
```

```rust,no_run
use disruptor_mp::build_shared_single_producer;
use disruptor_mp::shared_memory::ShmRingBuffer;

#[derive(Copy, Clone, Default)]
struct Event {
    value: i64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _type_hint: Option<ShmRingBuffer<Event>> = None;

    let mut producer = build_shared_single_producer::<Event>("mp_ring", 1024)
        .build_producer(Event::default)?;

    producer.publish(|e| {
        e.value = 42;
    });

    Ok(())
}
```

## Public Namespaces

- `disruptor_mp::shared_memory::{SharedRingBuffer, ShmRingBuffer, SharedMemoryConfig}`
- `disruptor_mp::lock_free::{SharedCursor, ConsumerBarrier, ProducerBarrier, DiscoveryMode}`
- `disruptor_mp::{CoordinationMode, DiscoveryMode, SharedProducer, SharedConsumer}`
- `disruptor_mp::backend::shared_memory::*` (same API, backend-oriented path)

## API Stability

- Stable API is `disruptor_mp::{...}` namespaces and explicit re-exports documented above.
- Internal modules (`builder`, `producer`, `consumer`, `ringbuffer`, `cursor`, `wait`) are intentionally private.
- Breaking changes to public surface are tracked by kanban card and include migration notes.
- Internal refactors that do not change the public surface are not part of this compatibility policy.

## Platform Support

- Linux: officially supported.
- macOS: known to work for core multiprocess flows, but currently unsupported for official guarantees.
- Windows: explicitly unsupported.

## Discovery Contract

- Prefer `discover_consumer_with_prefix(...)` for fixed topologies that need deterministic naming.
- `enable_discovery(n)` now uses coordination-backed slot IDs (`ad_0`, `ad_1`, ...) when startup coordination is active.
- Legacy PID-based scanning remains available only as a best-effort fallback for non-coordinated or older flows and should not be treated as deterministic under child-process churn.

## Development

```bash
cargo test -p disruptor-mp --lib --tests
cargo test -p demos --examples --no-run
cargo test -p disruptor-mp --benches --no-run
make -C crates/perf-bench smoke
make -C crates/competitive-bench simple-smoke
```

## Canonical Test Lanes

- `cargo test -p disruptor-mp --lib --tests`: core crate coverage, integration tests, and true multiprocess regressions
- `cargo test -p demos --examples --no-run`: keep public runnable examples compiling
- `cargo test -p disruptor-mp --benches --no-run`: keep bench entry points compiling
- `make -C crates/perf-bench smoke`: workspace perf smoke lane for the raw / layered transport matrix
- `make -C crates/competitive-bench simple-smoke`: external-comparison smoke lane for the canonical small payload ladder

## Benchmarks

This crate intentionally does not host its own benchmark scenarios. Performance work lives in the dedicated bench crates so the substrate stays compact:

- **`crates/perf-bench`** — broad sweep across raw / framed / codec / typed-zero-copy layers and `shm` / `mmap` backends. Consolidated into `perf-bench-pingpong`, `perf-bench-broadcast`, `perf-bench-signal`, and `perf-bench-repeatability` binaries.
- **`crates/competitive-bench`** — apples-to-apples comparison against external transports (`crossbar`, `shmipc`, `iceoryx2`, `rusteron`, `zeromq`, `boost::interprocess message_queue`, `ompi`).

## License

Licensed under either of [Apache License, Version 2.0](../../LICENSE-APACHE) or [MIT license](../../LICENSE-MIT) at your option.
