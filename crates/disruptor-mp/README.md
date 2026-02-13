# disruptor-mp

Multiprocess shared-memory ring buffer support for Disruptor-style publication.

This crate is intentionally **multiprocess-only**.
It no longer vendors/copypastes single-process internals from `disruptor-rs`.

## Boundary

- `disruptor-mp` owns:
  - `disruptor_mp::{shared_memory, lock_free, producer, consumer, builder}`
  - shared-memory lifecycle, producer/consumer coordination, and multiprocess benchmarks/tests.
- crates.io `disruptor` owns:
  - single-process/threaded disruptor APIs (`build_single_producer`, `build_multi_producer`, wait strategies, pollers).

See `the workspace book` for migration details.

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
use disruptor_mp::builder::build_shared_single_producer;
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
- `disruptor_mp::producer::{SharedProducer, CoordinationMode}`
- `disruptor_mp::consumer::SharedConsumer`
- `disruptor_mp::backend::shared_memory::*` (same API, backend-oriented path)

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
```
