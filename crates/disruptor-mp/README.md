# disruptor-mp

Multiprocess shared-memory ring buffer support for Disruptor-style publication.

This crate is intentionally **multiprocess-only**.
It no longer vendors/copypastes single-process internals from `disruptor-rs`.

## Boundary

- `disruptor-mp` owns:
  - shared-memory lifecycle, producer/consumer coordination, and multiprocess benchmarks/tests via stable namespaces:
    - `disruptor_mp::shared_memory`
    - `disruptor_mp::lock_free`
    - `disruptor_mp::backend`
  - high-level constructors and types:
    - `attach_shared_consumer`
    - `build_shared_single_producer`
    - `SharedProducer`, `SharedConsumer`, `SharedCursor`, `SharedRingBuffer`, `ShmRingBuffer`
    - `CoordinationMode`, `DiscoveryMode`, `ConsumerBarrier`, `ProducerBarrier`
- crates.io `disruptor` owns:
  - single-process/threaded disruptor APIs (`build_single_producer`, `build_multi_producer`, wait strategies, pollers).

See `the workspace book` for migration details.
See `the workspace book` for shared-memory layout versioning rules.
See `the workspace book` for Linux CPU affinity controls and benchmark usage.
See `the workspace book` for current Linux core-to-core optimization measurements.

## Dependency Model

Internally this crate depends on crates.io `disruptor` as:

```toml
[dependencies]
disruptor_core = { package = "disruptor", version = "3.7.1" }
```

## Quick Start (Multiprocess)

```toml
[dependencies]
disruptor = { package = "disruptor-mp", version = "3.7.1" }
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

## Development

```bash
make drift-check
make test
cargo test -p disruptor-mp --test true_multiprocess -- --nocapture
make test-shm-cleanup-stress ITERATIONS=50
make bench-multiprocess
make bench-affinity-matrix
make bench-r10-ab
```
