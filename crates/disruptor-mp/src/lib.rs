#![warn(missing_docs)]

//! Multiprocess shared-memory ring buffers for Disruptor-style
//! publication.
//!
//! `disruptor-mp` extends the upstream single-process `disruptor`
//! crate with a cross-process data plane: producers and consumers in
//! different OS processes coordinate through a shared-memory segment
//! with cache-line-padded sequence cursors and a fixed-size ring
//! buffer. Single-process / threaded Disruptor APIs continue to come
//! from the upstream `disruptor` crate.
//!
//! # Where this crate sits
//!
//! This crate is the **substrate** — Layer 0 in the project's onion
//! model. Higher-level transports (framing, codecs, typed zero-copy,
//! topology) live in `myelon` and stack on top of the
//! types here.
//!
//! ```text
//!     ┌───────────────────────────────────────┐
//!     │  myelon                    │  ← framing / codec / typed
//!     │  Layers 1, 2, 3                       │  ← (optional, on top)
//!     ├───────────────────────────────────────┤
//!     │  disruptor-mp        (this crate)     │  ← Layer 0 — raw ring
//!     │  raw ring + coordination + obs        │
//!     ├───────────────────────────────────────┤
//!     │  disruptor           (crates.io)      │  ← single-process / threaded
//!     └───────────────────────────────────────┘
//! ```
//!
//! If you want a typed transport with serialisation and
//! fragmentation, depend on `myelon` instead — it
//! re-exports everything from this crate.
//!
//! # What this crate provides
//!
//! | Concern | Type | Purpose |
//! |---|---|---|
//! | Raw ring (SHM) | [`SharedProducer<E>`], [`SharedConsumer<E>`] | Cross-process publish/consume of fixed-size events over a POSIX shared-memory segment. |
//! | Raw ring (mmap) | [`MmapProducer<E>`], [`MmapConsumer<E>`] | Same, backed by a memory-mapped file. |
//! | Builders | [`build_shared_single_producer`], [`attach_shared_consumer`] | Construct a producer/consumer with discovery and coordination wired up. |
//! | Coordination | [`CoordinationMode`] | When does the producer consider its peers attached? |
//! | Liveness | [`RequiredConsumerLivenessConfig`], [`RequiredConsumerFailureAction`] | A stalled required consumer is treated as a failure or alert, not silent backpressure. |
//! | Naming | [`portable_shm_segment_name`] | Derive a macOS-safe SHM segment name from an arbitrary label. |
//! | Observability | [`observability`] (RFC-0040) | Hot-path counters file plus `metrics`-rs / Prometheus / OTLP exporters behind feature flags. |
//!
//! `E` is your event type — anything `Copy + Default + 'static` with
//! a stable layout.
//!
//! # Feature flags
//!
//! - `metrics` (default): wire the [`observability`] counters into
//!   the [`metrics`](https://crates.io/crates/metrics) façade so any
//!   `metrics`-rs recorder can ingest them.
//! - `metrics-prometheus`: pull in `metrics-exporter-prometheus`.
//! - `metrics-otel`: pull in `opentelemetry-otlp` for OTLP export.
//! - `RUSTFLAGS="--cfg dst"`: compile deterministic-simulation hooks
//!   used by the internal `myelon-dst` harness and DST integration tests.
//!
//! # Required-consumer liveness (RFC 0017.5)
//!
//! The base model is strict broadcast — the slowest consumer gates
//! capacity, so a stalled required consumer backpressures the
//! producer indefinitely. The liveness layer is opt-in and turns
//! that silent stall into a producer-observable, time-bounded event.
//!
//! Enable it via [`SharedProducer::enable_required_consumer_liveness`]
//! or [`MmapProducer::enable_required_consumer_liveness`], then use
//! the parallel `*_managed` publish methods (`publish_managed`,
//! `publish_batch_managed`) instead of the unmanaged ones. Existing
//! unmanaged calls keep their current semantics for callers that
//! don't opt in.
//!
//! | Case | Outcome |
//! |---|---|
//! | Required consumer doesn't appear within `startup_wait_timeout`. | [`RequiredConsumerError::StartupTimeout`] |
//! | Required consumer stalls past `progress_timeout` while gating the producer. | One stderr alert + optional [`RequiredConsumerAlertHook`] callback. |
//! | Same consumer ID rejoins before `shutdown_grace_period` expires. | Producer recovers, alert state clears, publishing resumes. |
//! | Stall persists past `shutdown_grace_period`. | [`RequiredConsumerError::GracefulShutdownTriggered`] |
//!
//! The check is **cold-path only** — it runs only while the producer
//! is blocked on a gating consumer, so steady-state publish cost is
//! unchanged. There is no consumer-side heartbeat; progress is
//! observed from the cursor data the producer already needs for
//! gating. By design the liveness layer does not add dead-consumer
//! eviction, quorum, or degraded broadcast — the system stays
//! strict-broadcast.
//!
//! # Quick start
//!
//! Producer side:
//!
//! ```no_run
//! use disruptor_mp::{
//!     build_shared_single_producer, portable_shm_segment_name, CoordinationMode,
//! };
//! # #[derive(Copy, Clone, Default)]
//! # #[repr(C)]
//! # struct Event { sequence: u64 }
//! let segment = portable_shm_segment_name("demo");
//! let mut producer = build_shared_single_producer::<Event>(&segment, 4096)
//!     .discover_consumer_with_prefix(1, "cp")
//!     .with_coordination(CoordinationMode::Immediate)
//!     .build_producer(Event::default)
//!     .expect("build producer");
//! producer.publish(|slot| slot.sequence = 42);
//! ```
//!
//! Consumer side, in a different process:
//!
//! ```no_run
//! use disruptor_mp::attach_shared_consumer;
//! # #[derive(Copy, Clone, Default)]
//! # #[repr(C)]
//! # struct Event { sequence: u64 }
//! # let segment = "demo";
//! let mut consumer = attach_shared_consumer::<Event>(segment, 4096)
//!     .with_consumer_id("cp_0")
//!     .build_consumer()
//!     .expect("attach consumer");
//! while let Some(event) = consumer.try_consume_next_leased() {
//!     // process &event
//!     # let _ = event;
//! }
//! ```
//!
//! See the README, the workspace book, and the `examples/` directory for
//! end-to-end programs and the observability surface.

pub use disruptor_core::{MissingFreeSlots, Producer, RingBufferFull, Sequence};

// `api` and `builder` are private but their public items are re-exported
// through `pub use api::*;` below. Their doctests reference re-exported
// types and are valid; suppressing the lint here keeps those examples
// in place without making the modules `pub`.
#[allow(rustdoc::private_doc_tests)]
mod api;
#[path = "backend/mmap/barrier.rs"]
mod mmap_barrier;
#[path = "backend/mmap/consumer.rs"]
mod mmap_consumer;
#[path = "backend/mmap/cursor.rs"]
mod mmap_cursor;
#[path = "backend/mmap/producer.rs"]
mod mmap_producer;
#[path = "backend/mmap/ringbuffer.rs"]
mod mmap_ringbuffer;
#[path = "backend/mmap/transport.rs"]
mod mmap_transport;
pub mod observability;
mod required_consumer;
mod segment_name;
mod shared_memory_layout;

// Deterministic-simulation testing primitives. Available when the
// build is invoked with `RUSTFLAGS="--cfg dst"`. See the module docs
// for the FoundationDB/TigerBeetle-style design.
#[cfg(dst)]
pub mod dst;
pub use api::*;
pub use required_consumer::{
    RequiredConsumerAlert, RequiredConsumerAlertHook, RequiredConsumerError,
    RequiredConsumerFailureAction, RequiredConsumerLivenessConfig,
};
pub use segment_name::*;

/// Returns true when `value` is an exact multiple of `divisor`.
///
/// This uses division/multiplication to avoid unstable and toolchain-dependent
/// integer helper APIs while staying explicit and compiler-stable.
#[inline]
pub fn is_multiple_of_u64(value: u64, divisor: u64) -> bool {
    divisor != 0 && value / divisor * divisor == value
}
