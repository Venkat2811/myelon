#![warn(missing_docs)]

//! Multiprocess shared-memory transport for inference and other
//! low-latency pipelines.
//!
//! `myelon` is a **simplified façade** for the [`disruptor_mp`]
//! substrate. It re-exports every relevant type from `disruptor-mp`
//! (Layer 0 — the raw cross-process ring buffer plus its
//! coordination, discovery, liveness, and observability primitives)
//! and adds three more layers on top: framing, codec, and typed
//! zero-copy.
//!
//! Both `myelon` and `disruptor-mp` are first-class entry points —
//! depend on whichever exposes only the surface your code actually
//! needs. The type identity is preserved across the re-export
//! boundary, so a `disruptor_mp::SharedConsumer<E>` *is* a
//! `myelon::SharedConsumer<E>`; helpers and patterns transfer
//! between the two profiles unchanged.
//!
//! Pick the outermost layer that matches your data; inner layers
//! (including Layer 0) are reachable through `myelon` without
//! adding `disruptor-mp` as a separate dependency.
//!
//! # The onion
//!
//! ```text
//! ┌───────────────────────────────────────────────────────────┐
//! │ myelon                  ← full layered façade             │
//! │                                                           │
//! │   Layer 3 — typed zero-copy (ZeroCopyCodec)               │
//! │   Layer 2 — codec (bincode / rkyv / flatbuffers)          │
//! │   Layer 1 — framed transport (multi-frame, msg_id, flags) │
//! │                                                           │
//! │   ┌───────────────────────────────────────────────────┐   │
//! │   │ Layer 0  — re-exported from disruptor-mp:         │   │
//! │   │   Shared{Producer,Consumer}<E>            (SHM)   │   │
//! │   │   Mmap{Producer,Consumer}<E>              (mmap)  │   │
//! │   │   build_shared_single_producer / attach_…         │   │
//! │   │   CoordinationMode, discovery,                    │   │
//! │   │   RequiredConsumerLivenessConfig (RFC 0017.5),    │   │
//! │   │   observability::* (RFC 0040)                     │   │
//! │   └───────────────────────────────────────────────────┘   │
//! │                                                           │
//! │   + FixedTopology / WorkerCount   (topology shape)        │
//! │   + MyelonTransportLayout         (macOS-safe SHM names)  │
//! │   + observability::*              (RFC-0040 re-export)    │
//! └───────────────────────────────────────────────────────────┘
//! ```
//!
//! # When to use which layer
//!
//! | Need | Layer | Type |
//! |---|---|---|
//! | Fixed-size, `Copy` event. You own the wire format. | 0 — raw | [`SharedProducer<E>`] / [`SharedConsumer<E>`] (SHM) or [`MmapProducer<E>`] / [`MmapConsumer<E>`] (mmap) |
//! | Variable-length `&[u8]` payloads, possibly larger than one ring slot. Need start/end flags + a message id. | 1 — framed | [`transport::FramedTransportProducer`] / [`transport::FramedTransportConsumer`] (SHM) or [`transport::MmapFramedTransportProducer`] / [`transport::MmapFramedTransportConsumer`] (mmap) |
//! | Typed message with serialisation (bincode / rkyv / flatbuffers). Owned decode on the consumer side. | 2 — codec | [`typed_transport::TypedProducer`] / [`typed_transport::TypedConsumer`] + a [`codec::Codec`] impl |
//! | Same as Layer 2 but consumer reads serialised data in-place — no `deserialize` allocation. | 3 — typed zero-copy | [`typed_transport::TypedProducer`] / [`typed_transport::TypedConsumer`] + a [`codec::ZeroCopyCodec`] impl |
//!
//! # Choosing a frame size (Layer 1 and up)
//!
//! The framed transport's frame type is a **const generic** —
//! callers pick the per-frame payload capacity at compile time:
//!
//! ```rust
//! use myelon::transport::{FixedFrame, AlignedFixedFrame};
//! type Frame64K = FixedFrame<{ 64 * 1024 - 12 }>;       // 12-byte header
//! type FrameTiny = FixedFrame<256>;
//! type Frame4K  = FixedFrame<{ 4 * 1024 - 12 }>;
//! type ZcFrame  = AlignedFixedFrame<{ 64 * 1024 - 16 }>; // 16-byte header
//! ```
//!
//! There is nothing special about 64 KB — that is just the convention
//! [`perf-bench`](https://github.com/Venkat2811/myelon/tree/main/crates/perf-bench)
//! uses. The trade-off:
//!
//! | Frame size | Effect |
//! |---|---|
//! | Larger `DATA_BYTES` | Bigger ring slot, bigger SHM footprint for the same depth, but messages that fit in one slot avoid fragmentation. |
//! | Smaller `DATA_BYTES` | Smaller slot, tighter memory, multi-frame fragmentation kicks in once payload exceeds `DATA_BYTES`. |
//! | Right-sized to dominant payload | Single-frame messages, lowest overhead, at the cost of flexibility for atypical sizes. |
//!
//! The slot size must be a compile-time constant so the ring layout
//! is known. To support a runtime-chosen size, instantiate one
//! transport per size class. `perf-bench`'s `nofrag` mode is a
//! worked example of this pattern.
//!
//! # Two senses of "zero-copy"
//!
//! Two different things in this stack get called "zero-copy", and
//! they're not the same:
//!
//! | Sense | Where | How |
//! |---|---|---|
//! | **Memory-level zero-copy** | Layer 0 already provides this. | `try_consume_next_leased()` returns `&E` straight into the ring slot. No allocation, no copy. Use this for fixed-size `Copy + repr(C)` events. |
//! | **Typed-format zero-copy** | Layer 3. | The bytes on the wire are already a serialised graph (rkyv `Archived<T>` or a flatbuffers root table); the consumer reads fields in-place via [`codec::ZeroCopyCodec::access`]. |
//!
//! There is no separate "raw + typed zero-copy" layer. If your event
//! is fixed-size and `Copy + repr(C)`, just put the struct in the
//! slot — Layer 0 is already memory-zero-copy and outperforms an
//! archived blob. If you want serialisation flexibility, you also
//! want framing. The four-layer picture covers both senses without
//! overlap.
//!
//! # Orthogonal concerns
//!
//! These wrap *across* layers — pick them by what your *system* needs,
//! not by what your *wire format* needs.
//!
//! | Concern | Type | Purpose |
//! |---|---|---|
//! | Coordination | [`producer::CoordinationMode`] | When does the producer consider its peers attached? |
//! | Discovery | builder methods on [`SharedDisruptorBuilder`] | Producer locates consumers by explicit ID or by prefix. |
//! | Liveness | [`RequiredConsumerLivenessConfig`], [`RequiredConsumerFailureAction`] (see below) | A stalled required consumer becomes a failure or alert, not silent backpressure. |
//! | Topology | [`inference::FixedTopology`], [`inference::WorkerCount`] | Pre-baked one-scheduler / N-worker shape with discovery + rendezvous. |
//! | Layout | [`transport::MyelonTransportLayout`], [`transport::MyelonTransportConfig`], [`transport::RunnerMyelonTransportConfig`] | macOS-safe SHM segment names for one-engine / N-runner sessions. |
//! | Observability | [`observability`] (re-export of [`disruptor_mp::observability`]) | RFC-0040 hot-path counters file. |
//!
//! # Required-consumer liveness (RFC 0017.5)
//!
//! Out of the box, the base model is strict broadcast — the slowest
//! consumer gates capacity. A stalled or crashed required consumer
//! therefore backpressures the producer indefinitely. The optional
//! liveness layer turns that silent stall into a producer-observable,
//! time-bounded event.
//!
//! It is **opt-in** through a parallel `*_managed` publish surface.
//! Existing unmanaged calls (`publish`, `try_publish`, `publish_batch`)
//! keep their original semantics; nothing changes for callers that
//! don't opt in.
//!
//! Available on every layer:
//!
//! | Layer / backend | Type | Methods |
//! |---|---|---|
//! | 0, SHM | [`SharedProducer<E>`] | `enable_required_consumer_liveness`, `publish_managed`, `publish_batch_managed` |
//! | 0, mmap | [`MmapProducer<E>`] | same |
//! | 1, SHM | [`transport::FramedTransportProducer`] | `enable_required_consumer_liveness`, `publish_managed` |
//! | 1, mmap | [`transport::MmapFramedTransportProducer`] | same |
//! | 2/3, SHM | [`typed_transport::TypedProducer`] | `enable_required_consumer_liveness`, `publish_managed` |
//! | 2/3, mmap | [`typed_transport::MmapTypedProducer`] | same |
//!
//! Failure surface (returned from `publish_managed`):
//!
//! | Case | Outcome |
//! |---|---|
//! | Required consumer doesn't appear within `startup_wait_timeout`. | [`RequiredConsumerError::StartupTimeout`] |
//! | Required consumer stalls past `progress_timeout` while gating the producer. | One stderr alert + optional `RequiredConsumerAlertHook` callback. |
//! | Same consumer ID rejoins before `shutdown_grace_period` expires. | Producer recovers, alert state clears, publishing resumes. |
//! | Stall persists past `shutdown_grace_period`. | [`RequiredConsumerError::GracefulShutdownTriggered`] |
//!
//! The liveness check is **cold-path only** — it runs only while the
//! producer is blocked on a gating required consumer. Steady-state
//! publish cost is unchanged. There is no consumer-side heartbeat;
//! progress is observed from the cursor data the producer already
//! needs for gating.
//!
//! By design the liveness layer does **not** add dead-consumer
//! eviction, quorum modes, or degraded broadcast — the system stays
//! strict-broadcast. If a required consumer dies and never returns
//! under the same ID, the topology fails gracefully.
//!
//! # Feature flags
//!
//! | Feature | Layer / surface | What it enables |
//! |---|---|---|
//! | (default) | Layers 0, 1; Layer 2 with `bincode` only | Raw + framed transport, plus the bincode codec. |
//! | `rkyv` | Layer 2/3 with `rkyv` | Re-exports `rkyv` and the rkyv codec wrappers (incl. [`codec::ZeroCopyCodec`]). |
//! | `flatbuffers` | Layer 2/3 with `flatbuffers` | Re-exports `flatbuffers` and the flatbuffers codec wrappers. |
//! | `dst` | tests only | Forwards to `disruptor_mp/dst` for deterministic-simulation hooks. |
//!
//! # Quick start (Layer 0)
//!
//! ```no_run
//! use myelon::{
//!     attach_shared_consumer, build_shared_single_producer,
//! };
//! use myelon::producer::CoordinationMode;
//! use disruptor_mp::portable_shm_segment_name;
//! # #[derive(Copy, Clone, Default)]
//! # #[repr(C)]
//! # struct Event { sequence: u64 }
//! let segment = portable_shm_segment_name("demo");
//! let mut producer = build_shared_single_producer::<Event>(&segment, 4096)
//!     .discover_consumer_with_prefix(1, "cp")
//!     .with_coordination(CoordinationMode::Immediate)
//!     .build_producer(Event::default)
//!     .expect("build producer");
//! producer.publish(|slot| slot.sequence = 1);
//! ```
//!
//! See the [README](https://github.com/Venkat2811/myelon/blob/main/crates/myelon/README.md)
//! for Layer 1 / Layer 2 / Layer 3 examples.

pub mod codec;
pub mod inference;
pub mod transport;
pub mod typed_transport;

// Re-export serialization crates so downstream consumers don't need direct deps.
// Derive macros (e.g. #[derive(rkyv::Archive)]) require the crate to be accessible
// by name. This re-export makes `use myelon::rkyv` work in derive paths.
#[cfg(feature = "rkyv")]
pub use rkyv;

#[cfg(feature = "flatbuffers")]
pub use flatbuffers;

pub use disruptor_mp::{
    attach_shared_consumer, build_shared_single_producer, default_block_strategy_duration,
    default_consume_sleep_duration, default_discovery_poll_duration, perform_default_block_wait,
    perform_default_consume_sleep_wait, perform_default_discovery_poll_wait, perform_sleep_wait,
    portable_shm_segment_name, AutoConsumer, AutoWaitStrategy, CoordinationMode, MmapConsumer,
    MmapProducer, MmapTransportLayout, MultiProcessError, MultiProcessResult,
    RequiredConsumerAlert, RequiredConsumerAlertHook, RequiredConsumerError,
    RequiredConsumerFailureAction, RequiredConsumerLivenessConfig, RingBufferFull, Sequence,
    SharedConsumer, SharedDisruptorBuilder, SharedProducer, DEFAULT_MAX_CONSUMERS,
    PORTABLE_SHM_SEGMENT_NAME_MAX_LEN,
};
pub use inference::{FixedTopology, InferenceTopologyError, InferenceTopologyResult, WorkerCount};
pub use transport::{
    compact_session_tag, coordination_cursor_name, ensure_coordination_cursor, frame_flags,
    is_last_frame, is_single_frame, publish_framed_payload, recv_framed_message,
    validate_segment_name, AlignedFixedFrame, FrameMeta, MmapFramedTransportConsumer,
    MmapFramedTransportProducer, MyelonTransportConfig, MyelonTransportLayout, MyelonWaitStrategy,
    RunnerMyelonTransportConfig, TransportError, TransportResult, DEFAULT_MYELON_RESPONSE_DEPTH,
    DEFAULT_MYELON_RPC_DEPTH, DEFAULT_TRANSPORT_PREFIX, SEGMENT_NAME_LIMIT_MACOS,
};

/// Shared-memory top level façade types.
pub mod shared_memory {
    pub use disruptor_mp::shared_memory::{SharedMemoryConfig, SharedRingBuffer, ShmRingBuffer};
}

/// Backend façade namespace for shared-memory implementation.
pub mod backend {
    /// Shared-memory backend re-exports.
    pub mod shared_memory {
        pub use disruptor_mp::backend::shared_memory::{
            SharedMemoryConfig, SharedRingBuffer, ShmRingBuffer,
        };
    }

    /// mmap-file backend re-exports.
    pub mod mmap {
        pub use disruptor_mp::backend::mmap::{
            MmapConsumer, MmapConsumerBarrier, MmapCursor, MmapCursorConfig, MmapFileConfig,
            MmapProducer, MmapRingBuffer, MmapTransportLayout,
        };
    }
}

/// Lock-free coordination façade types.
pub mod lock_free {
    pub use disruptor_mp::lock_free::{
        ConsumerBarrier, DiscoveryMode, ProducerBarrier, SharedCursor,
    };
    pub use disruptor_mp::SharedCursorTrait;
}

/// Producer façade namespace for coordinated construction and policy.
pub mod producer {
    pub use disruptor_mp::{CoordinationMode, SharedProducer};
}

/// Consumer façade namespace for shared-memory consumer behavior.
pub mod consumer {
    pub use disruptor_mp::SharedConsumer;
}

/// Re-export of `disruptor_mp::observability` for downstream users that
/// want to attach Aeron-style hot-path counters without taking a direct
/// dependency on `disruptor-mp`. RFC 0040.
///
/// Typical use:
///
/// ```ignore
/// let file = unsafe { myelon::observability::CountersFile::init(ptr) };
/// producer.attach_counters(&file);
/// consumer.attach_counters(&file);
/// // External reader (e.g. a future `myelon-stat` companion):
/// let reader = unsafe { myelon::observability::CountersFile::attach(ptr) }?;
/// for c in reader.snapshot() { println!("{} = {}", c.label, c.value); }
/// ```
pub mod observability {
    pub use disruptor_mp::observability::*;
}

pub use codec::{Codec, CodecError};
pub use typed_transport::{
    MmapTypedConsumer, MmapTypedProducer, TypedConsumer, TypedProducer, TypedPublishError,
};

/// Convenience re-exports for downstream code that wants the typical
/// transport, codec, and observability surface in one `use` line.
pub mod prelude {
    pub use super::{
        attach_shared_consumer, build_shared_single_producer, AutoConsumer, AutoWaitStrategy,
        FixedTopology, Sequence, SharedConsumer, SharedDisruptorBuilder, SharedProducer,
        WorkerCount,
    };
    pub use super::{
        backend, compact_session_tag, consumer, coordination_cursor_name,
        ensure_coordination_cursor, frame_flags, is_last_frame, is_single_frame, lock_free,
        publish_framed_payload, recv_framed_message, shared_memory, validate_segment_name,
        AlignedFixedFrame, FrameMeta, MmapConsumer, MmapFramedTransportConsumer,
        MmapFramedTransportProducer, MmapProducer, MmapTransportLayout, MyelonTransportConfig,
        MyelonTransportLayout, MyelonWaitStrategy, RunnerMyelonTransportConfig,
    };
    pub use super::{
        Codec, CodecError, MmapTypedConsumer, MmapTypedProducer, RequiredConsumerAlert,
        RequiredConsumerAlertHook, RequiredConsumerError, RequiredConsumerFailureAction,
        RequiredConsumerLivenessConfig, TypedConsumer, TypedProducer, TypedPublishError,
    };
}
