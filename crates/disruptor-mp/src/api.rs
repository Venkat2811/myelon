//! Multi-process support for Disruptor using shared memory.
//!
//! This module provides multi-process variants of the Disruptor pattern that allow
//! producers and consumers to run in separate processes while maintaining the same
//! high-performance, lock-free characteristics.
//!
//! # Key Features
//!
//! - **Platform policy**: Linux supported, macOS best effort, Windows unsupported
//! - **Automatic coordination**: Built-in consumer discovery and startup coordination
//! - **Automatic event handlers**: Background thread processing with `handle_events_with()`
//! - **Consumer discovery**: Automatic detection using process IDs or prefix matching
//! - **Multiple coordination modes**: Immediate, wait-for-consumers, and discovery-based startup
//! - **Resource management**: Automatic cleanup via `Drop` trait and shared memory cleanup
//! - **Platform-optimized naming**: Adapts shared memory names to platform constraints
//!
//! # Architecture
//!
//! The multiprocess implementation uses shared memory to create a ring buffer that can be
//! accessed by multiple processes. Key components include:
//!
//! - [`SharedRingBuffer`]: The core ring buffer stored in shared memory
//! - [`SharedProducer`]: Producer handle for publishing events across processes
//! - [`SharedConsumer`]: Consumer handle for processing events in separate processes
//! - [`SharedCursor`]: Atomic cursors for coordination between processes
//!
//! # Usage Patterns
//!
//! ## Automatic Coordination Pattern (Recommended)
//!
//! The recommended pattern for production systems with automatic
//! event handling and built-in coordination:
//!
//! ```rust,no_run
//! use disruptor_mp::*;
//! use disruptor_mp::Producer;
//!
//! #[derive(Copy, Clone, Default)]
//! struct Event { data: i64 }
//!
//! // Producer with automatic consumer discovery
//! let mut producer = build_shared_single_producer::<Event>("my_ring", 1024)
//!     .enable_discovery(1)  // Automatically discover 1 consumer
//!     .build_producer(Event::default).unwrap();
//!
//! // Consumer with automatic event processing
//! let _consumer = attach_shared_consumer::<Event>("my_ring", 1024)
//!     .handle_events_with(|event, sequence, end_of_batch| {
//!         // Process events automatically in background thread
//!         println!("Processing: {}", event.data);
//!     }).unwrap();
//!
//! // Coordination happens automatically - no manual coordination needed
//! // Consumer automatically cleans up when dropped
//! ```
//!
//! ## Manual Consumer Pattern
//!
//! For custom polling patterns and fine-grained control:
//!
//! ```rust,no_run
//! use disruptor_mp::*;
//! use disruptor_mp::Producer;
//!
//! #[derive(Copy, Clone, Default)]
//! struct Event { data: i64 }
//!
//! // Producer creates shared memory
//! let mut producer = build_shared_single_producer::<Event>("my_ring", 1024)
//!     .build_producer(Event::default).unwrap();
//!
//! // Consumer with manual polling
//! let mut consumer = attach_shared_consumer::<Event>("my_ring", 1024)
//!     .build_consumer().unwrap();
//!
//! // Custom polling loop with manual coordination
//! loop {
//!     let processed = consumer.process_available(|event, sequence| {
//!         println!("Processing: {}", event.data);
//!     });
//!     if processed == 0 {
//!         std::thread::sleep(std::time::Duration::from_millis(1));
//!     }
//! }
//! ```
//!
//! ## Coordination Modes
//!
//! The library supports multiple coordination strategies:
//!
//! ### Automatic Discovery (Recommended)
//! - **PID-based discovery**: `enable_discovery(max_consumers)`
//! - **Prefix-based discovery**: `discover_consumer_with_prefix(max_consumers, "PREFIX")`
//! - **Automatic coordination**: No manual signaling required
//! - **Background processing**: Events processed in separate threads
//!
//! ### Manual Coordination
//! - **Immediate mode**: Start publishing immediately
//! - **Wait-for-consumers**: `wait_for_consumers(count, timeout)`
//! - **Custom polling**: Manual `process_available()` loops
//!
//! This approach is ideal for:
//! - **Dynamic topologies** (consumers can join/leave)
//! - **High-frequency publishing** (millions of events per second)
//! - **Automatic resource management** (no manual cleanup needed)
//!
//! # Platform Considerations
//!
//! ## Shared Memory Naming
//!
//! The library supports both manual and automatic naming approaches:
//!
//! ### Manual Naming (Recommended for Production)
//! ```rust,ignore
//! use disruptor_mp::SharedMemoryConfig;
//!
//! let config = SharedMemoryConfig {
//!     name: "my_app".to_string(),  // Short name for cross-platform compatibility
//!     buffer_size: 1024,
//!     element_size: std::mem::size_of::<MyEvent>(),
//!     create: true,
//! };
//! ```
//!
//! ### Automatic Naming (Development/Testing)
//! ```rust,ignore
//! use disruptor_mp::{SharedRingBuffer, SharedCursor};
//!
//! // Producer creates with automatic naming
//! let (ring_buffer, ring_name) = SharedRingBuffer::new_auto(1024, || MyEvent::default())?;
//! let (cursor, cursor_name) = SharedCursor::new_auto(0)?;
//!
//! // Share the generated names with consumers (via serialization, IPC, etc.)
//! // Consumers use the names to attach to existing shared memory
//! ```
//!
//! ### Platform Constraints
//! Prefer builder-generated names or `portable_shm_segment_name(...)` so
//! shared-memory segment identifiers stay portable across Linux and macOS.
//!
//! This eliminates platform-specific naming constraints and is what
//! `portable_shm_segment_name` produces by default.
//!
//! ## Resource Management
//!
//! Shared memory segments are automatically cleaned up when:
//! - The creating process exits normally
//! - All processes detach from the segment
//! - The system is restarted
//!
//! For explicit cleanup, shared memory segments can be manually removed using
//! platform-specific tools if needed.
//!
//! # Performance Characteristics
//!
//! The multiprocess implementation provides high-performance inter-process communication
//! with both automatic and manual coordination modes:
//!
//! - **Low latency**: Microsecond-range latency for typical workloads
//! - **High throughput**: Millions of events per second
//! - **Lock-free**: No mutexes or locks in the critical path
//! - **Wait-free publishing**: Publishers never block on slow consumers
//! - **Cache-friendly**: Sequential memory access patterns
//! - **Automatic coordination**: Built-in discovery eliminates manual coordination
//! - **Background processing**: Automatic event handlers run in separate threads
//!
//! ## Benchmark Results
//!
//! From `examples/counters_auto.rs` (automatic coordination):
//! - **SPSC**: ~12-14M events/sec (automatic event delivery)
//! - **SPMC-2**: ~10-21M events/sec (broadcast to 2 consumers)
//! - **SPMC-5**: ~7-8M events/sec (broadcast to 5 consumers)
//!
//! From `examples/counters.rs` (manual coordination):
//! - **SPSC**: ~13-18M events/sec producer, ~16-20M events/sec consumer
//! - **SPMC-2**: ~14-15M events/sec producer, ~20-21M events/sec consumer
//! - **SPMC-5**: ~7-8M events/sec producer, ~17M events/sec consumer
//!
//! # Error Handling
//!
//! All multiprocess operations return [`MultiProcessResult<T>`] which can contain:
//!
//! - [`MultiProcessError::SharedMemoryError`]: Failed to create/access shared memory
//! - [`MultiProcessError::MemoryMapError`]: Failed to map memory into process space
//! - [`MultiProcessError::SegmentNotFound`]: Shared segment doesn't exist
//! - [`MultiProcessError::IncompatibleLayout`]: Data layout mismatch between processes
//! - [`MultiProcessError::PermissionDenied`]: Insufficient permissions
//! - [`MultiProcessError::CoordinationTimeout`]: Coordination timeout during startup
//!
//! # Thread Safety
//!
//! All multiprocess components are thread-safe and can be used from multiple threads
//! within the same process. However, each process should typically have only one
//! producer handle to maintain ordering guarantees.
//!
//! # Examples
//!
//! See the `examples/` directory for complete examples:
//! - `counters_auto.rs`: **Recommended** - Automatic coordination with event handlers
//! - `counters.rs`: Manual coordination with external `ProcessCoordination`
//! - `shared_disruptor.rs`: Basic producer-consumer setup
//!
//! ## Running Examples
//!
//! ```bash
//! # Automatic coordination showcase (recommended)
//! cargo run --release --example counters_auto showcase
//!
//! # Full automatic coordination test suite
//! cargo run --release --example counters_auto test
//!
//! # Manual coordination test suite
//! cargo run --release --example counters test
//!
//! # Performance benchmarks
//! cargo bench --bench ipc_shm
//! ```

// Doctests in `builder.rs` reference items that re-export through this
// module's public API; suppressing the lint here keeps those examples
// while not promoting the module to `pub`.
#[allow(rustdoc::private_doc_tests)]
#[path = "builder.rs"]
mod builder;
#[path = "consumer.rs"]
mod consumer;
#[path = "lock_free/consumer_barrier.rs"]
mod consumer_barrier;
#[path = "lock_free/cursor.rs"]
mod cursor;
#[path = "producer.rs"]
mod producer;
#[path = "backend/shared_memory/ringbuffer.rs"]
mod ringbuffer;
#[path = "runtime/wait.rs"]
mod wait;

/// Shared-memory data-plane primitives.
///
/// This is the recommended namespace for buffer/config types used by library consumers.
pub mod shared_memory {
    pub use super::ringbuffer::SharedRingBuffer;
    pub use super::SharedMemoryConfig;

    /// Short alias for shared-memory ring buffer.
    pub type ShmRingBuffer<E> = SharedRingBuffer<E>;
}

/// Backend namespaces for storage implementations.
///
/// Keeping this namespace now makes it easy to add `mmap`/`arena` backends later
/// without changing high-level imports.
pub mod backend {
    /// Shared-memory backend implementation.
    pub mod shared_memory {
        pub use super::super::ringbuffer::SharedRingBuffer;
        pub use super::super::SharedMemoryConfig;

        /// Short alias for shared-memory ring buffer.
        pub type ShmRingBuffer<E> = SharedRingBuffer<E>;
    }

    /// File-backed mmap backend implementation.
    pub mod mmap {
        pub use super::super::MmapConsumerBarrier;
        pub use super::super::MmapCursorConfig;
        pub use super::super::MmapFileConfig;
        pub use super::super::MmapTransportLayout;
        pub use crate::mmap_consumer::MmapConsumer;
        pub use crate::mmap_cursor::MmapCursor;
        pub use crate::mmap_producer::MmapProducer;
        pub use crate::mmap_ringbuffer::MmapRingBuffer;
    }
}

/// Lock-free coordination primitives.
pub mod lock_free {
    pub use super::consumer_barrier::{ConsumerBarrier, DiscoveryMode, SharedConsumerBarrier};
    pub use super::cursor::{SharedCursor, SharedCursorTrait};

    /// Producer-side sequencing barrier represented by a shared cursor.
    pub type ProducerBarrier = super::cursor::SharedCursor;
}

pub use crate::mmap_barrier::MmapConsumerBarrier;
pub use crate::mmap_consumer::MmapConsumer;
pub use crate::mmap_cursor::MmapCursor;
pub use crate::mmap_producer::MmapProducer;
pub use crate::mmap_ringbuffer::MmapRingBuffer;
pub use crate::mmap_transport::MmapTransportLayout;
pub use builder::{
    attach_shared_consumer, build_shared_single_producer, AutoConsumer, AutoWaitStrategy,
    SharedDisruptorBuilder,
};
pub use consumer::{ConsumerCounterSelection, SharedConsumer};
pub use consumer_barrier::{ConsumerBarrier, DiscoveryMode, SharedConsumerBarrier};
pub use cursor::{SharedCursor, SharedCursorTrait};
pub use producer::{CoordinationMode, ProducerCounterSelection, SharedProducer};
pub use ringbuffer::SharedRingBuffer;
pub use shared_memory::ShmRingBuffer;

// Re-exports for public API
use std::{fmt, path::PathBuf};

/// Default maximum number of consumers that can be registered with a single shared ring buffer.
///
/// This limit helps prevent unbounded memory usage in the consumer registry and ensures
/// reasonable performance when tracking minimum consumer sequences. Applications requiring
/// more consumers can implement custom coordination mechanisms.
pub const DEFAULT_MAX_CONSUMERS: usize = 64;

/// Errors that can occur during multi-process setup
#[derive(Debug, thiserror::Error)]
pub enum MultiProcessError {
    /// Failed to create shared memory
    #[error("Failed to create shared memory: {0}")]
    SharedMemoryError(String),

    /// Failed to map memory
    #[error("Failed to map memory: {0}")]
    MemoryMapError(String),

    /// Shared segment not found
    #[error("Shared segment not found: {0}")]
    SegmentNotFound(String),

    /// Incompatible data layout
    #[error("Incompatible data layout: {0}")]
    IncompatibleLayout(String),

    /// Permission denied
    #[error("Permission denied")]
    PermissionDenied,

    /// Coordination timeout during startup
    #[error("Coordination timeout: {0}")]
    CoordinationTimeout(String),
}

/// Result type for multi-process operations
pub type MultiProcessResult<T> = Result<T, MultiProcessError>;

/// Configuration for shared memory segments
#[derive(Debug, Clone)]
pub struct SharedMemoryConfig {
    /// Name of the shared memory segment
    pub name: String,
    /// Size of the ring buffer (must be power of 2)
    pub buffer_size: usize,
    /// Element size in bytes
    pub element_size: usize,
    /// Whether to create the segment (producer) or attach to existing (consumer)
    pub create: bool,
}

impl fmt::Display for SharedMemoryConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SharedMemory(name={}, size={}, element_size={})",
            self.name, self.buffer_size, self.element_size
        )
    }
}

/// Configuration for file-backed mmap segments.
#[derive(Debug, Clone)]
pub struct MmapFileConfig {
    /// Path to the backing file used for the shared mapping.
    pub path: PathBuf,
    /// Size of the ring buffer (must be power of 2).
    pub buffer_size: usize,
    /// Element size in bytes.
    pub element_size: usize,
    /// Whether to create the backing file or attach to an existing one.
    pub create: bool,
}

impl fmt::Display for MmapFileConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MmapFile(path={}, size={}, element_size={})",
            self.path.display(),
            self.buffer_size,
            self.element_size
        )
    }
}

/// Configuration for file-backed mmap cursor segments.
#[derive(Debug, Clone)]
pub struct MmapCursorConfig {
    /// Path to the backing file used for the shared mapping.
    pub path: PathBuf,
    /// Whether to create the backing file or attach to an existing one.
    pub create: bool,
}

impl fmt::Display for MmapCursorConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MmapCursor(path={})", self.path.display())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MissingFreeSlots;
    use crate::RequiredConsumerError;
    use crate::RequiredConsumerLivenessConfig;
    use crate::RingBufferFull;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    fn unique_test_segment(prefix: &str) -> String {
        let name = crate::portable_shm_segment_name(prefix);
        assert!(
            name.len() <= 14,
            "test segment name '{}' exceeds macOS-safe budget",
            name
        );
        name
    }

    #[test]
    fn test_unique_test_segment_stays_within_macos_budget() {
        assert!(unique_test_segment("process_available_blocking_batch").len() <= 14);
        assert!(unique_test_segment("race_condition_fix").len() <= 14);
    }

    #[derive(Debug, Copy, Clone, Default, PartialEq)]
    struct TestEvent {
        sequence: i64,
        data: i64,
    }

    fn attach_named_consumer(
        name: &str,
        buffer_size: usize,
        consumer_id: &str,
    ) -> SharedConsumer<TestEvent> {
        let config = SharedMemoryConfig {
            name: name.to_string(),
            buffer_size,
            element_size: std::mem::size_of::<TestEvent>(),
            create: false,
        };

        SharedDisruptorBuilder::new(config)
            .with_consumer_id(consumer_id)
            .build_consumer()
            .unwrap()
    }

    #[test]
    fn managed_publish_reports_missing_required_consumer_at_startup() {
        let name = unique_test_segment("req_cons_start");
        let buffer_size = 8;

        let mut producer = build_shared_single_producer::<TestEvent>(&name, buffer_size)
            .enable_discovery(2)
            .with_coordination(CoordinationMode::Immediate)
            .build_producer(TestEvent::default)
            .unwrap();
        producer.enable_required_consumer_liveness(
            RequiredConsumerLivenessConfig::new(vec!["c1".into(), "c2".into()])
                .with_startup_wait_timeout(Duration::from_millis(50))
                .with_progress_timeout(Duration::from_millis(20))
                .with_progress_check_interval(Duration::from_millis(1))
                .with_shutdown_grace_period(Duration::from_millis(10)),
        );

        let _consumer1 = attach_named_consumer(&name, buffer_size, "c1");

        let error = producer
            .publish_managed(|event| {
                event.sequence = 1;
                event.data = 100;
            })
            .expect_err("managed publish should fail when c2 never appears");

        match error {
            RequiredConsumerError::StartupTimeout { missing } => {
                assert_eq!(missing, vec!["c2".to_string()]);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn managed_batch_publish_reports_missing_required_consumer_at_startup() {
        let name = unique_test_segment("req_cons_batch_start");
        let buffer_size = 8;

        let mut producer = build_shared_single_producer::<TestEvent>(&name, buffer_size)
            .enable_discovery(2)
            .with_coordination(CoordinationMode::Immediate)
            .build_producer(TestEvent::default)
            .unwrap();
        producer.enable_required_consumer_liveness(
            RequiredConsumerLivenessConfig::new(vec!["c1".into(), "c2".into()])
                .with_startup_wait_timeout(Duration::from_millis(50))
                .with_progress_timeout(Duration::from_millis(20))
                .with_progress_check_interval(Duration::from_millis(1))
                .with_shutdown_grace_period(Duration::from_millis(10)),
        );

        let _consumer1 = attach_named_consumer(&name, buffer_size, "c1");

        let error = producer
            .publish_batch_managed(2, |event, index| {
                event.sequence = index as i64;
                event.data = (index as i64) * 10;
            })
            .expect_err("managed batch publish should fail when c2 never appears");

        match error {
            RequiredConsumerError::StartupTimeout { missing } => {
                assert_eq!(missing, vec!["c2".to_string()]);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn managed_publish_shuts_down_when_required_consumer_stalls() {
        let name = unique_test_segment("req_cons_fail");
        let buffer_size = 4;

        let mut producer = build_shared_single_producer::<TestEvent>(&name, buffer_size)
            .enable_discovery(2)
            .with_coordination(CoordinationMode::Immediate)
            .build_producer(TestEvent::default)
            .unwrap();
        producer.enable_required_consumer_liveness(
            RequiredConsumerLivenessConfig::new(vec!["c1".into(), "c2".into()])
                .with_startup_wait_timeout(Duration::from_millis(100))
                .with_progress_timeout(Duration::from_millis(20))
                .with_progress_check_interval(Duration::from_millis(1))
                .with_shutdown_grace_period(Duration::from_millis(20)),
        );

        let stop_consumer1 = Arc::new(AtomicBool::new(false));
        let stop_consumer1_thread = Arc::clone(&stop_consumer1);
        let name_for_thread = name.clone();
        let consumer1_thread = thread::spawn(move || {
            let mut consumer1 = attach_named_consumer(&name_for_thread, buffer_size, "c1");
            while !stop_consumer1_thread.load(Ordering::Acquire) {
                if consumer1.try_consume_next().is_none() {
                    thread::sleep(Duration::from_millis(1));
                }
            }
        });

        let mut consumer2 = attach_named_consumer(&name, buffer_size, "c2");

        producer
            .publish_managed(|event| {
                event.sequence = 0;
                event.data = 0;
            })
            .unwrap();
        let _ = consumer2.consume_next();
        drop(consumer2);

        for i in 1..=buffer_size {
            producer
                .publish_managed(|event| {
                    event.sequence = i as i64;
                    event.data = (i as i64) * 10;
                })
                .unwrap();
        }

        let error = producer
            .publish_managed(|event| {
                event.sequence = 99;
                event.data = 990;
            })
            .expect_err("managed publish should fail once c2 stops advancing");

        stop_consumer1.store(true, Ordering::Release);
        consumer1_thread.join().unwrap();

        match error {
            RequiredConsumerError::GracefulShutdownTriggered { consumer_id, .. } => {
                assert_eq!(consumer_id, "c2");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn managed_publish_recovers_when_same_consumer_id_rejoins() {
        let name = unique_test_segment("req_cons_rejn");
        let buffer_size = 4;

        let mut producer = build_shared_single_producer::<TestEvent>(&name, buffer_size)
            .enable_discovery(2)
            .with_coordination(CoordinationMode::Immediate)
            .build_producer(TestEvent::default)
            .unwrap();
        producer.enable_required_consumer_liveness(
            RequiredConsumerLivenessConfig::new(vec!["c1".into(), "c2".into()])
                .with_startup_wait_timeout(Duration::from_millis(100))
                .with_progress_timeout(Duration::from_millis(20))
                .with_progress_check_interval(Duration::from_millis(1))
                .with_shutdown_grace_period(Duration::from_millis(200)),
        );

        let stop_consumer1 = Arc::new(AtomicBool::new(false));
        let stop_consumer1_thread = Arc::clone(&stop_consumer1);
        let name_for_thread = name.clone();
        let consumer1_thread = thread::spawn(move || {
            let mut consumer1 = attach_named_consumer(&name_for_thread, buffer_size, "c1");
            while !stop_consumer1_thread.load(Ordering::Acquire) {
                if consumer1.try_consume_next().is_none() {
                    thread::sleep(Duration::from_millis(1));
                }
            }
        });

        let mut consumer2 = attach_named_consumer(&name, buffer_size, "c2");

        producer
            .publish_managed(|event| {
                event.sequence = 0;
                event.data = 0;
            })
            .unwrap();
        let _ = consumer2.consume_next();
        drop(consumer2);

        for i in 1..=buffer_size {
            producer
                .publish_managed(|event| {
                    event.sequence = i as i64;
                    event.data = (i as i64) * 10;
                })
                .unwrap();
        }

        let name_for_rejoin = name.clone();
        let rejoin_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(40));
            let mut rejoined = attach_named_consumer(&name_for_rejoin, buffer_size, "c2");
            let deadline = Instant::now() + Duration::from_millis(500);
            let mut consumed = 0usize;
            while Instant::now() < deadline && consumed < buffer_size + 2 {
                if rejoined.try_consume_next().is_some() {
                    consumed += 1;
                } else {
                    thread::sleep(Duration::from_millis(1));
                }
            }
            consumed
        });

        let sequence = producer
            .publish_managed(|event| {
                event.sequence = 99;
                event.data = 990;
            })
            .expect("same-id rejoin should recover before shutdown");

        stop_consumer1.store(true, Ordering::Release);
        consumer1_thread.join().unwrap();
        let rejoined_consumed = rejoin_thread.join().unwrap();

        assert!(sequence >= buffer_size as i64);
        assert!(rejoined_consumed > 0, "rejoined c2 should consume backlog");
    }

    #[test]
    fn managed_publish_rejects_different_consumer_id_rejoin() {
        let name = unique_test_segment("req_cons_diff");
        let buffer_size = 4;

        let mut producer = build_shared_single_producer::<TestEvent>(&name, buffer_size)
            .enable_discovery(2)
            .with_coordination(CoordinationMode::Immediate)
            .build_producer(TestEvent::default)
            .unwrap();
        producer.enable_required_consumer_liveness(
            RequiredConsumerLivenessConfig::new(vec!["c1".into(), "c2".into()])
                .with_startup_wait_timeout(Duration::from_millis(100))
                .with_progress_timeout(Duration::from_millis(20))
                .with_progress_check_interval(Duration::from_millis(1))
                .with_shutdown_grace_period(Duration::from_millis(200)),
        );

        let stop_consumer1 = Arc::new(AtomicBool::new(false));
        let stop_consumer1_thread = Arc::clone(&stop_consumer1);
        let name_for_thread = name.clone();
        let consumer1_thread = thread::spawn(move || {
            let mut consumer1 = attach_named_consumer(&name_for_thread, buffer_size, "c1");
            while !stop_consumer1_thread.load(Ordering::Acquire) {
                if consumer1.try_consume_next().is_none() {
                    thread::sleep(Duration::from_millis(1));
                }
            }
        });

        let mut consumer2 = attach_named_consumer(&name, buffer_size, "c2");
        producer
            .publish_managed(|event| {
                event.sequence = 0;
                event.data = 0;
            })
            .unwrap();
        let _ = consumer2.consume_next();
        drop(consumer2);

        for i in 1..=buffer_size {
            producer
                .publish_managed(|event| {
                    event.sequence = i as i64;
                    event.data = (i as i64) * 10;
                })
                .unwrap();
        }

        let name_for_wrong_rejoin = name.clone();
        let wrong_rejoin_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(40));
            let mut wrong_consumer =
                attach_named_consumer(&name_for_wrong_rejoin, buffer_size, "c3");
            let deadline = Instant::now() + Duration::from_millis(500);
            let mut consumed = 0usize;
            while Instant::now() < deadline && consumed < buffer_size + 2 {
                if wrong_consumer.try_consume_next().is_some() {
                    consumed += 1;
                } else {
                    thread::sleep(Duration::from_millis(1));
                }
            }
            consumed
        });

        let error = producer
            .publish_managed(|event| {
                event.sequence = 99;
                event.data = 990;
            })
            .expect_err("wrong-id rejoin must not clear the c2 stall");

        stop_consumer1.store(true, Ordering::Release);
        consumer1_thread.join().unwrap();
        let wrong_rejoin_consumed = wrong_rejoin_thread.join().unwrap();

        assert!(
            wrong_rejoin_consumed > 0,
            "a new consumer id may still read the broadcast stream"
        );
        match error {
            RequiredConsumerError::GracefulShutdownTriggered { consumer_id, .. } => {
                assert_eq!(consumer_id, "c2");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn managed_publish_rejoin_after_grace_period_still_fails() {
        let name = unique_test_segment("req_cons_late");
        let buffer_size = 4;

        let mut producer = build_shared_single_producer::<TestEvent>(&name, buffer_size)
            .enable_discovery(2)
            .with_coordination(CoordinationMode::Immediate)
            .build_producer(TestEvent::default)
            .unwrap();
        producer.enable_required_consumer_liveness(
            RequiredConsumerLivenessConfig::new(vec!["c1".into(), "c2".into()])
                .with_startup_wait_timeout(Duration::from_millis(100))
                .with_progress_timeout(Duration::from_millis(20))
                .with_progress_check_interval(Duration::from_millis(1))
                .with_shutdown_grace_period(Duration::from_millis(60)),
        );

        let stop_consumer1 = Arc::new(AtomicBool::new(false));
        let stop_consumer1_thread = Arc::clone(&stop_consumer1);
        let name_for_thread = name.clone();
        let consumer1_thread = thread::spawn(move || {
            let mut consumer1 = attach_named_consumer(&name_for_thread, buffer_size, "c1");
            while !stop_consumer1_thread.load(Ordering::Acquire) {
                if consumer1.try_consume_next().is_none() {
                    thread::sleep(Duration::from_millis(1));
                }
            }
        });

        let mut consumer2 = attach_named_consumer(&name, buffer_size, "c2");
        producer
            .publish_managed(|event| {
                event.sequence = 0;
                event.data = 0;
            })
            .unwrap();
        let _ = consumer2.consume_next();
        drop(consumer2);

        for i in 1..=buffer_size {
            producer
                .publish_managed(|event| {
                    event.sequence = i as i64;
                    event.data = (i as i64) * 10;
                })
                .unwrap();
        }

        let name_for_rejoin = name.clone();
        let rejoin_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(140));
            let mut rejoined = attach_named_consumer(&name_for_rejoin, buffer_size, "c2");
            let deadline = Instant::now() + Duration::from_millis(300);
            let mut consumed = 0usize;
            while Instant::now() < deadline && consumed < buffer_size + 2 {
                if rejoined.try_consume_next().is_some() {
                    consumed += 1;
                } else {
                    thread::sleep(Duration::from_millis(1));
                }
            }
            consumed
        });

        let error = producer
            .publish_managed(|event| {
                event.sequence = 99;
                event.data = 990;
            })
            .expect_err("late same-id rejoin must not rescue the topology after grace expires");

        stop_consumer1.store(true, Ordering::Release);
        consumer1_thread.join().unwrap();
        let rejoined_consumed = rejoin_thread.join().unwrap();

        assert!(
            rejoined_consumed > 0,
            "late rejoined consumer may still drain retained backlog"
        );
        match error {
            RequiredConsumerError::GracefulShutdownTriggered { consumer_id, .. } => {
                assert_eq!(consumer_id, "c2");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn managed_publish_does_not_fail_while_topology_is_idle() {
        let name = unique_test_segment("req_cons_idle");
        let buffer_size = 8;

        let mut producer = build_shared_single_producer::<TestEvent>(&name, buffer_size)
            .enable_discovery(2)
            .with_coordination(CoordinationMode::Immediate)
            .build_producer(TestEvent::default)
            .unwrap();
        producer.enable_required_consumer_liveness(
            RequiredConsumerLivenessConfig::new(vec!["c1".into(), "c2".into()])
                .with_startup_wait_timeout(Duration::from_millis(100))
                .with_progress_timeout(Duration::from_millis(20))
                .with_progress_check_interval(Duration::from_millis(1))
                .with_shutdown_grace_period(Duration::from_millis(50)),
        );

        let mut consumer1 = attach_named_consumer(&name, buffer_size, "c1");
        let mut consumer2 = attach_named_consumer(&name, buffer_size, "c2");

        producer
            .publish_managed(|event| {
                event.sequence = 1;
                event.data = 10;
            })
            .unwrap();
        assert_eq!(consumer1.consume_next().0, 0);
        assert_eq!(consumer2.consume_next().0, 0);

        thread::sleep(Duration::from_millis(75));

        producer
            .publish_managed(|event| {
                event.sequence = 2;
                event.data = 20;
            })
            .expect("idle topology must not trigger stall shutdown");

        assert_eq!(consumer1.consume_next().0, 1);
        assert_eq!(consumer2.consume_next().0, 1);
    }

    // ============================================================================
    // BASIC FUNCTIONALITY TESTS
    // ============================================================================

    #[test]
    fn test_shared_ring_buffer_creation_and_attachment() {
        let name = unique_test_segment("test_ring_basic");
        let buffer_size = 8;

        // Producer creates the ring buffer
        let config_create = SharedMemoryConfig {
            name: name.clone(),
            buffer_size,
            element_size: std::mem::size_of::<TestEvent>(),
            create: true,
        };

        let ring_buffer = SharedRingBuffer::new(config_create, TestEvent::default).unwrap();
        assert_eq!(ring_buffer.size(), buffer_size);

        // Consumer attaches to existing ring buffer
        let config_attach = SharedMemoryConfig {
            name,
            buffer_size,
            element_size: std::mem::size_of::<TestEvent>(),
            create: false,
        };

        let attached_buffer: SharedRingBuffer<TestEvent> =
            SharedRingBuffer::attach(config_attach).unwrap();
        assert_eq!(attached_buffer.size(), buffer_size);
        assert_eq!(ring_buffer.size(), attached_buffer.size());
    }

    #[test]
    fn test_basic_producer_consumer_coordination() {
        let name = unique_test_segment("test_basic_coord");
        let buffer_size = 8;

        // Create producer
        let mut producer = build_shared_single_producer::<TestEvent>(&name, buffer_size)
            .build_producer(TestEvent::default)
            .unwrap();

        // Create consumer
        let config = SharedMemoryConfig {
            name,
            buffer_size,
            element_size: std::mem::size_of::<TestEvent>(),
            create: false,
        };
        let mut consumer: SharedConsumer<TestEvent> = SharedDisruptorBuilder::new(config)
            .build_consumer()
            .unwrap();

        // Test single event publish/consume cycle
        producer.publish(|event| {
            event.sequence = 0;
            event.data = 42;
        });

        let mut consumed_events = Vec::new();

        // Use try_consume_next instead of process_available
        if let Some((seq, event)) = consumer.try_consume_next() {
            consumed_events.push((seq, event));
        }

        assert_eq!(consumed_events.len(), 1);
        assert_eq!(consumed_events[0].0, 0); // sequence
        assert_eq!(consumed_events[0].1.sequence, 0);
        assert_eq!(consumed_events[0].1.data, 42);
    }

    // ============================================================================
    // SINGLE PRODUCER SINGLE CONSUMER (SPSC) TESTS
    // ============================================================================

    #[test]
    fn test_spsc_ring_buffer_full_behavior() {
        let name = unique_test_segment("spsc_full");
        let buffer_size = 4;

        // Create consumer first in a thread
        let name_clone = name.clone();
        let consumer_handle = thread::spawn(move || {
            let config = SharedMemoryConfig {
                name: name_clone,
                buffer_size,
                element_size: std::mem::size_of::<TestEvent>(),
                create: false,
            };

            // Wait a bit for producer to create segment
            thread::sleep(Duration::from_millis(50));

            let mut consumer: SharedConsumer<TestEvent> = SharedDisruptorBuilder::new(config)
                .build_consumer()
                .unwrap();

            // Process events after a delay to test buffer full behavior
            thread::sleep(Duration::from_millis(200));

            let mut consumed = Vec::new();
            let processed = consumer.process_available(|event: &TestEvent, _| {
                consumed.push(event.data);
            });

            (processed, consumed)
        });

        // Create producer with discovery enabled
        let mut producer = build_shared_single_producer::<TestEvent>(&name, buffer_size)
            .enable_discovery(1) // Enable discovery to track consumer
            .build_producer(TestEvent::default)
            .unwrap();

        // Give time for consumer to attach and be discovered
        thread::sleep(Duration::from_millis(100));

        // Fill the ring buffer to capacity
        for i in 0..buffer_size {
            producer
                .try_publish(|e| {
                    e.sequence = i as i64;
                    e.data = i as i64 * 10;
                })
                .expect("Should be able to publish to non-full buffer");
        }

        // Next publish should fail - ring buffer is full (consumer hasn't processed yet)
        assert_eq!(
            producer
                .try_publish(|e| e.sequence = buffer_size as i64)
                .err()
                .unwrap(),
            RingBufferFull
        );

        // Wait for consumer thread to process events
        let (processed, consumed) = consumer_handle.join().unwrap();
        assert_eq!(processed, buffer_size); // Consumer processes all available events

        // Now should be able to publish again
        producer
            .try_publish(|e| {
                e.sequence = buffer_size as i64;
                e.data = 999;
            })
            .expect("Should be able to publish after consumer freed space");

        // Verify all events were consumed correctly
        let expected: Vec<i64> = (0..buffer_size).map(|i| i as i64 * 10).collect();
        assert_eq!(consumed, expected);
    }

    #[test]
    fn test_spsc_ordered_event_processing() {
        let name = unique_test_segment("spsc_ordered");
        let buffer_size = 16; // Large enough to hold all test events
        let num_events = 10;

        let mut producer = build_shared_single_producer::<TestEvent>(&name, buffer_size)
            .build_producer(TestEvent::default)
            .unwrap();

        let config = SharedMemoryConfig {
            name,
            buffer_size,
            element_size: std::mem::size_of::<TestEvent>(),
            create: false,
        };
        let mut consumer: SharedConsumer<TestEvent> = SharedDisruptorBuilder::new(config)
            .build_consumer()
            .unwrap();

        // Publish events
        for i in 0..num_events {
            producer.publish(|event| {
                event.sequence = i as i64;
                event.data = (i as i64) * (i as i64); // Square for easy verification
            });
        }

        // Consume all events
        let mut consumed_events = Vec::new();
        let mut total_processed = 0;

        // May need multiple calls to process_available to get all events
        while total_processed < num_events {
            let processed = consumer.process_available(|event: &TestEvent, seq| {
                consumed_events.push((seq, event.sequence, event.data));
            });
            total_processed += processed;

            if processed == 0 {
                // Small yield to allow any pending operations to complete
                thread::yield_now();
            }
        }

        // Verify all events were consumed in order
        assert_eq!(consumed_events.len(), num_events);
        for (i, &(seq, event_seq, data)) in consumed_events.iter().enumerate() {
            assert_eq!(seq, i as i64);
            assert_eq!(event_seq, i as i64);
            assert_eq!(data, (i as i64) * (i as i64));
        }
    }

    #[test]
    fn test_process_available_advances_consumer_sequence_after_batch() {
        let name = unique_test_segment("process_available_batch");
        let buffer_size = 16;
        let num_events = 6;

        let mut producer = build_shared_single_producer::<TestEvent>(&name, buffer_size)
            .build_producer(TestEvent::default)
            .unwrap();

        let config = SharedMemoryConfig {
            name,
            buffer_size,
            element_size: std::mem::size_of::<TestEvent>(),
            create: false,
        };
        let mut consumer: SharedConsumer<TestEvent> = SharedDisruptorBuilder::new(config)
            .build_consumer()
            .unwrap();

        for i in 0..num_events {
            producer.publish(|event| {
                event.sequence = i as i64;
                event.data = i as i64 * 10;
            });
        }

        let mut consumed = Vec::new();
        let processed = consumer.process_available(|event: &TestEvent, seq| {
            consumed.push((seq, event.sequence, event.data));
        });

        assert_eq!(processed, num_events);
        assert_eq!(consumed.len(), num_events);
        assert_eq!(consumer.current_sequence(), (num_events - 1) as i64);
        assert_eq!(consumer.producer_sequence(), (num_events - 1) as i64);
        assert_eq!(consumer.consumer_sequence(), (num_events - 1) as i64);
    }

    #[test]
    fn test_process_available_blocking_marks_only_final_event_as_end_of_batch() {
        let name = unique_test_segment("process_available_blocking_batch");
        let buffer_size = 16;
        let num_events = 4;

        let mut producer = build_shared_single_producer::<TestEvent>(&name, buffer_size)
            .build_producer(TestEvent::default)
            .unwrap();

        let config = SharedMemoryConfig {
            name,
            buffer_size,
            element_size: std::mem::size_of::<TestEvent>(),
            create: false,
        };
        let mut consumer: SharedConsumer<TestEvent> = SharedDisruptorBuilder::new(config)
            .build_consumer()
            .unwrap();

        for i in 0..num_events {
            producer.publish(|event| {
                event.sequence = i as i64;
                event.data = i as i64;
            });
        }

        let mut observed = Vec::new();
        let processed =
            consumer.process_available_blocking(|event: &TestEvent, seq, end_of_batch| {
                observed.push((seq, event.sequence, end_of_batch));
            });

        assert_eq!(processed, num_events);
        assert_eq!(
            observed,
            vec![(0, 0, false), (1, 1, false), (2, 2, false), (3, 3, true),]
        );
        assert_eq!(consumer.current_sequence(), (num_events - 1) as i64);
    }

    #[test]
    fn test_per_consumer_sequences_prevent_race_conditions() {
        let name = unique_test_segment("per_consumer_test");
        let buffer_size = 64;
        let num_events = 10; // Start with fewer events for debugging

        // Create producer
        let mut producer = build_shared_single_producer::<TestEvent>(&name, buffer_size)
            .build_producer(TestEvent::default)
            .unwrap();

        // Create two consumers
        let config = SharedMemoryConfig {
            name: name.clone(),
            buffer_size,
            element_size: std::mem::size_of::<TestEvent>(),
            create: false,
        };

        let mut consumer1: SharedConsumer<TestEvent> = SharedDisruptorBuilder::new(config.clone())
            .build_consumer()
            .unwrap();

        let mut consumer2: SharedConsumer<TestEvent> = SharedDisruptorBuilder::new(config)
            .build_consumer()
            .unwrap();

        println!("Created two consumers for broadcast test");

        // Publish some events
        for i in 0..num_events {
            producer.publish(|event| {
                event.sequence = i as i64;
                event.data = i as i64;
            });
            println!("Published event {}", i);
        }

        // Check initial state
        let (seq1, prod_seq1, consumer_seq1) = consumer1.debug_sequences();
        let (seq2, prod_seq2, consumer_seq2) = consumer2.debug_sequences();
        println!(
            "After publishing - Consumer 1: current={}, producer={}, consumer={}",
            seq1, prod_seq1, consumer_seq1
        );
        println!(
            "After publishing - Consumer 2: current={}, producer={}, consumer={}",
            seq2, prod_seq2, consumer_seq2
        );

        // Let each consumer process all events (broadcast semantics)
        let mut consumer1_events = Vec::new();
        let mut consumer2_events = Vec::new();

        // Consumer 1 processes all events
        let _processed1 = consumer1.process_available(|event: &TestEvent, _seq| {
            consumer1_events.push((event.sequence, event.data));
            println!(
                "Consumer 1 processed event: seq={}, data={}",
                event.sequence, event.data
            );
        });

        // Consumer 2 processes all events
        let _processed2 = consumer2.process_available(|event: &TestEvent, _seq| {
            consumer2_events.push((event.sequence, event.data));
            println!(
                "Consumer 2 processed event: seq={}, data={}",
                event.sequence, event.data
            );
        });

        println!(
            "Consumer 1 processed {} events: {:?}",
            consumer1_events.len(),
            consumer1_events
        );
        println!(
            "Consumer 2 processed {} events: {:?}",
            consumer2_events.len(),
            consumer2_events
        );

        // Check state after processing
        let (seq1, prod_seq1, consumer_seq1) = consumer1.debug_sequences();
        let (seq2, prod_seq2, consumer_seq2) = consumer2.debug_sequences();
        println!(
            "After processing - Consumer 1: current={}, producer={}, consumer={}",
            seq1, prod_seq1, consumer_seq1
        );
        println!(
            "After processing - Consumer 2: current={}, producer={}, consumer={}",
            seq2, prod_seq2, consumer_seq2
        );

        // Verify broadcast semantics: each consumer should see all events
        assert_eq!(
            consumer1_events.len(),
            num_events,
            "Consumer 1 should see all events"
        );
        assert_eq!(
            consumer2_events.len(),
            num_events,
            "Consumer 2 should see all events"
        );

        // Verify events are in order and complete
        for i in 0..num_events {
            assert_eq!(consumer1_events[i], (i as i64, i as i64));
            assert_eq!(consumer2_events[i], (i as i64, i as i64));
        }

        println!("SUCCESS: Both consumers saw all events (broadcast semantics)!");
    }

    #[test]
    fn test_broadcast_consumer_basic() {
        let name = unique_test_segment("broadcast_basic");
        let buffer_size = 64;
        let num_events = 5;

        // Create producer
        let mut producer = build_shared_single_producer::<TestEvent>(&name, buffer_size)
            .build_producer(TestEvent::default)
            .unwrap();

        // Create two consumers
        let config = SharedMemoryConfig {
            name: name.clone(),
            buffer_size,
            element_size: std::mem::size_of::<TestEvent>(),
            create: false,
        };

        let mut consumer1: SharedConsumer<TestEvent> = SharedDisruptorBuilder::new(config.clone())
            .build_consumer()
            .unwrap();

        let mut consumer2: SharedConsumer<TestEvent> = SharedDisruptorBuilder::new(config)
            .build_consumer()
            .unwrap();

        println!("Created two consumers for basic broadcast test");
        println!(
            "Consumer 1 ID: {}, Consumer 2 ID: {}",
            consumer1.consumer_id(),
            consumer2.consumer_id()
        );

        // Publish events
        for i in 0..num_events {
            producer.publish(|event| {
                event.sequence = i as i64;
                event.data = i as i64;
            });
        }

        // Each consumer processes all events
        let mut consumer1_events = Vec::new();
        let mut consumer2_events = Vec::new();

        consumer1.process_available(|event: &TestEvent, _seq| {
            consumer1_events.push(event.sequence);
        });

        consumer2.process_available(|event: &TestEvent, _seq| {
            consumer2_events.push(event.sequence);
        });

        println!("Consumer 1 processed: {:?}", consumer1_events);
        println!("Consumer 2 processed: {:?}", consumer2_events);

        // Both consumers should see all events
        assert_eq!(
            consumer1_events.len(),
            num_events,
            "Consumer 1 should see all events"
        );
        assert_eq!(
            consumer2_events.len(),
            num_events,
            "Consumer 2 should see all events"
        );

        // Events should be in order
        for i in 0..num_events {
            assert_eq!(consumer1_events[i], i as i64);
            assert_eq!(consumer2_events[i], i as i64);
        }

        println!("Both consumers saw all events in order!");
    }

    // ============================================================================
    // STRESS AND PERFORMANCE TESTS
    // ============================================================================

    // ============================================================================
    // ATOMIC OPERATIONS TESTS
    // ============================================================================

    #[test]
    fn test_shared_cursor_operations() {
        let name = unique_test_segment("atomic_ops");
        let cursor = SharedCursor::new(&name, 0).unwrap();

        // Basic operations
        assert_eq!(cursor.load(Ordering::Relaxed), 0);

        cursor.store(42, Ordering::Relaxed);
        assert_eq!(cursor.load(Ordering::Relaxed), 42);

        let old = cursor.fetch_add(8, Ordering::Relaxed);
        assert_eq!(old, 42);
        assert_eq!(cursor.load(Ordering::Relaxed), 50);

        // Compare and exchange
        let result = cursor.compare_exchange(50, 100, Ordering::Relaxed, Ordering::Relaxed);
        assert_eq!(result, Ok(50));
        assert_eq!(cursor.load(Ordering::Relaxed), 100);

        let result = cursor.compare_exchange(50, 200, Ordering::Relaxed, Ordering::Relaxed);
        assert_eq!(result, Err(100));
        assert_eq!(cursor.load(Ordering::Relaxed), 100);
    }

    // ============================================================================
    // ERROR HANDLING TESTS
    // ============================================================================

    #[test]
    fn test_consumer_attachment_to_nonexistent_segment() {
        let name = "nonexistent".to_string();

        let config = SharedMemoryConfig {
            name,
            buffer_size: 8,
            element_size: std::mem::size_of::<TestEvent>(),
            create: false,
        };

        let result: Result<SharedConsumer<TestEvent>, MultiProcessError> =
            SharedDisruptorBuilder::new(config).build_consumer();
        assert!(result.is_err());

        match result.err().unwrap() {
            MultiProcessError::SegmentNotFound(_) => {} // Expected
            other => panic!("Expected SegmentNotFound, got {:?}", other),
        }
    }

    #[test]
    fn test_feature_completeness_documentation() {
        // This test serves as living documentation of implemented features

        // Implemented Core Features
        // Basic producer/consumer coordination
        // Single event publish/consume
        // Ring buffer overflow protection
        // Shared memory creation and attachment
        // Atomic sequence coordination
        // Thread-safe operations
        // Proper error handling

        // Not Yet Implemented (Future Work)
        // - Foundation for multi-writer producer topologies
        // - Multiple consumers (SPMC pattern)
        // - Multiple producers (MPSC pattern)
        // - Consumer dependencies and barriers
        // - Wait strategies beyond busy spinning
        // - Consumer thread lifecycle management
    }

    #[test]
    fn test_batch_publish_writes_and_consumes_in_sequence() {
        let name = unique_test_segment("batch_sequence");
        let buffer_size = 8;

        let mut producer = build_shared_single_producer::<TestEvent>(&name, buffer_size)
            .build_producer(TestEvent::default)
            .unwrap();

        let upper = producer
            .try_batch_publish(4, |event, i| {
                event.sequence = i as i64;
                event.data = 100 + i as i64;
            })
            .unwrap();
        assert_eq!(upper, 3);
        assert_eq!(producer.last_published_sequence(), 3);

        let config = SharedMemoryConfig {
            name,
            buffer_size,
            element_size: std::mem::size_of::<TestEvent>(),
            create: false,
        };
        let mut consumer: SharedConsumer<TestEvent> = SharedDisruptorBuilder::new(config)
            .build_consumer()
            .unwrap();

        let mut consumed = Vec::new();
        while consumed.len() < 4 {
            let processed = consumer.process_available(|event: &TestEvent, seq| {
                consumed.push((seq, event.sequence, event.data));
            });
            if processed == 0 {
                std::thread::yield_now();
            }
        }

        assert_eq!(
            consumed,
            vec![(0, 0, 100), (1, 1, 101), (2, 2, 102), (3, 3, 103)]
        );
    }

    #[test]
    fn test_simple_batch_publish_is_noop_for_zero() {
        let name = unique_test_segment("batch_zero");
        let buffer_size = 8;

        let mut producer = build_shared_single_producer::<TestEvent>(&name, buffer_size)
            .build_producer(TestEvent::default)
            .unwrap();

        assert_eq!(
            producer.try_batch_publish(0, |_event, _| {
                panic!("indexed closure must not run for n=0")
            }),
            Ok(-1)
        );

        producer
            .simple_batch_publish(0, |_event, _| {
                panic!("simple_batch_publish no-op closure must not run for n=0")
            })
            .unwrap();
        assert_eq!(producer.last_published_sequence(), -1);
    }

    #[test]
    fn test_try_batch_publish_reports_missing_slots_when_full() {
        let name = unique_test_segment("batch_full");
        let buffer_size = 4;
        let start_consume = Arc::new(AtomicBool::new(false));
        let consumed = Arc::new(AtomicUsize::new(0));

        let mut producer = build_shared_single_producer::<TestEvent>(&name, buffer_size)
            .discover_consumer_with_prefix(1, "bchk")
            .build_producer(TestEvent::default)
            .unwrap();

        let consumer_handle = {
            let name = name.clone();
            let start_consume = start_consume.clone();
            let consumed = consumed.clone();

            std::thread::spawn(move || {
                let config = SharedMemoryConfig {
                    name,
                    buffer_size,
                    element_size: std::mem::size_of::<TestEvent>(),
                    create: false,
                };
                let mut consumer: SharedConsumer<TestEvent> = SharedDisruptorBuilder::new(config)
                    .discover_consumer_with_prefix(1, "bchk")
                    .with_consumer_id("bchk_0")
                    .build_consumer()
                    .unwrap();

                while !start_consume.load(Ordering::Acquire) {
                    std::thread::yield_now();
                }

                while consumed.load(Ordering::Acquire) < (buffer_size + 1) {
                    let processed = consumer.process_available(|_event, _| {
                        consumed.fetch_add(1, Ordering::AcqRel);
                    });
                    if processed == 0 {
                        std::thread::yield_now();
                    }
                }
            })
        };

        producer
            .try_batch_publish(4, |event, i| {
                event.sequence = i as i64;
                event.data = 10 + i as i64;
            })
            .expect("full buffer should accept exactly `buffer_size` slots");

        // Ensure producer sees the consumer cursor before evaluating full-capacity math.
        let producer_seq = producer.last_published_sequence();
        let discovery_deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if producer.min_gating_sequence() != producer_seq {
                break;
            }
            if Instant::now() > discovery_deadline {
                panic!("consumer discovery did not reduce gating sequence below producer cursor");
            }
            std::thread::yield_now();
        }

        let err = producer
            .try_batch_publish(1, |_event, _| {
                panic!("second batch must not run when capacity is exhausted")
            })
            .expect_err("producer must report missing free slots when full");
        assert_eq!(err, MissingFreeSlots(1));

        start_consume.store(true, Ordering::Release);

        let deadline = Instant::now() + Duration::from_secs(2);
        while consumed.load(Ordering::Acquire) == 0 {
            if Instant::now() > deadline {
                panic!("consumer did not start consuming after start signal");
            }
            std::thread::yield_now();
        }

        producer
            .try_batch_publish(1, |event, i| {
                event.sequence = 4 + i as i64;
                event.data = 14;
            })
            .expect("single-slot batch should succeed once one slot is released");

        consumer_handle.join().unwrap();
        assert_eq!(consumed.load(Ordering::Acquire), buffer_size + 1);
    }

    #[test]
    fn test_fast_slow_consumer_race_condition_fix() {
        let name = unique_test_segment("race_condition_fix");
        let buffer_size = 8; // Small buffer to force backpressure

        // Create producer with discovery to track both consumers
        let mut producer = build_shared_single_producer::<TestEvent>(&name, buffer_size)
            .enable_discovery(2) // Track 2 consumers
            .build_producer(TestEvent::default)
            .unwrap();

        // Create two consumers
        let config = SharedMemoryConfig {
            name: name.clone(),
            buffer_size,
            element_size: std::mem::size_of::<TestEvent>(),
            create: false,
        };

        let mut fast_consumer: SharedConsumer<TestEvent> =
            SharedDisruptorBuilder::new(config.clone())
                .build_consumer()
                .unwrap();

        let mut slow_consumer: SharedConsumer<TestEvent> = SharedDisruptorBuilder::new(config)
            .build_consumer()
            .unwrap();

        // Publish events to fill buffer
        for i in 0..buffer_size {
            producer.publish(|event| {
                event.sequence = i as i64;
                event.data = i as i64 * 100; // Use distinctive values
            });
        }

        println!("Published {} events to fill buffer", buffer_size);

        // Fast consumer processes all available events
        let mut fast_events = Vec::new();
        fast_consumer.process_available(|event: &TestEvent, seq| {
            fast_events.push((seq, event.sequence, event.data));
        });

        // Slow consumer processes only some events (simulating slow processing)
        let mut slow_events = Vec::new();
        let mut slow_processed = 0;
        slow_consumer.process_available(|event: &TestEvent, seq| {
            if slow_processed < 3 {
                // Only process first 3 events
                slow_events.push((seq, event.sequence, event.data));
                slow_processed += 1;
            }
        });

        println!("Fast consumer processed: {} events", fast_events.len());
        println!("Slow consumer processed: {} events", slow_events.len());
        println!("Fast consumer events: {:?}", fast_events);
        println!("Slow consumer events: {:?}", slow_events);

        // The key test: try to publish more events
        // With the race condition fix, producer should be blocked by slow consumer
        let mut successful_publishes = 0;
        for i in buffer_size..(buffer_size + 10) {
            match producer.try_publish(|event| {
                event.sequence = i as i64;
                event.data = i as i64 * 100;
            }) {
                Ok(_) => {
                    successful_publishes += 1;
                    println!("Successfully published event {} (data: {})", i, i * 100);
                }
                Err(_) => {
                    println!(
                        "Buffer full at event {} - producer correctly blocked by slow consumer",
                        i
                    );
                    break;
                }
            }
        }

        // Now let slow consumer process more events
        let slow_events_before_catchup = slow_events.len();
        slow_consumer.process_available(|event: &TestEvent, seq| {
            slow_events.push((seq, event.sequence, event.data));
        });

        println!("Slow consumer after catchup: {} events", slow_events.len());
        println!("Slow consumer all events: {:?}", slow_events);

        // Verify the first few events are identical between consumers
        // (this proves no data corruption occurred)
        let overlap_count = std::cmp::min(fast_events.len(), slow_events_before_catchup);
        for i in 0..overlap_count {
            assert_eq!(
                fast_events[i], slow_events[i],
                "Data corruption detected at index {}: fast consumer saw {:?}, slow consumer saw {:?}",
                i, fast_events[i], slow_events[i]
            );
        }

        // Verify that producer was properly constrained by slow consumer
        // It should not have been able to publish unlimited events
        assert!(
            successful_publishes < 10,
            "Producer should have been blocked by slow consumer, but published {} additional events",
            successful_publishes
        );

        println!("SUCCESS: Producer correctly respected slow consumer position!");
        println!(
            "   - Fast consumer processed {} events immediately",
            fast_events.len()
        );
        println!(
            "   - Slow consumer processed {} events initially",
            slow_events_before_catchup
        );
        println!(
            "   - Producer was blocked after {} additional publishes",
            successful_publishes
        );
    }

    /// Test that producer can publish many events without consumers (no discovery)
    /// This verifies the buffer wrapping fix - previously would deadlock at 64KB
    #[test]
    fn test_buffer_wrapping_without_consumers() {
        const BUFFER_SIZE: usize = 512; // 512 slots (64KB with 128-byte events)
        const NUM_EVENTS: u64 = 150_000; // Way more than buffer size

        println!(
            "Testing: Publishing {} events without consumers (no discovery)",
            NUM_EVENTS
        );

        let segment_name = unique_test_segment("wrap_no_disc");

        // Create producer WITHOUT discovery
        let mut producer = build_shared_single_producer::<TestEvent>(&segment_name, BUFFER_SIZE)
            // Explicitly NOT enabling discovery
            .build_producer(TestEvent::default)
            .expect("Failed to create producer");

        println!("Producer created without discovery");

        // Try to publish many events - should wrap the buffer correctly
        let start = Instant::now();
        for i in 0..NUM_EVENTS {
            if start.elapsed() > Duration::from_secs(5) {
                panic!("Timeout at event {} - buffer not wrapping correctly!", i);
            }

            producer.publish(|event| {
                event.sequence = i as i64;
                event.data = (i % 1000) as i64;
            });

            if i > 0 && (i % 10_000 == 0) {
                println!("Published {} events", i);
            }
        }

        println!(
            "✅ Successfully published {} events without consumers!",
            NUM_EVENTS
        );
        println!("Buffer wrapped {} times", NUM_EVENTS / BUFFER_SIZE as u64);
    }

    /// Test the exact 64KB boundary case that was failing before the fix
    #[test]
    fn test_exact_64kb_boundary_no_deadlock() {
        const BUFFER_SIZE: usize = 512; // 512 slots = 64KB with 128-byte events
        const NUM_EVENTS: u64 = 65_536; // Exactly where the old bug occurred

        println!(
            "Testing: Publishing exactly {} events (64KB boundary)",
            NUM_EVENTS
        );

        let segment_name = unique_test_segment("boundary");

        // Create producer without discovery
        let mut producer = build_shared_single_producer::<TestEvent>(&segment_name, BUFFER_SIZE)
            .build_producer(TestEvent::default)
            .expect("Failed to create producer");

        let start = Instant::now();
        for i in 0..NUM_EVENTS {
            if start.elapsed() > Duration::from_secs(2) {
                panic!("Deadlock at event {} - this is the old bug!", i);
            }

            producer.publish(|event| {
                event.sequence = i as i64;
            });
        }

        println!(
            "✅ Successfully published {} events - no deadlock at 64KB boundary!",
            NUM_EVENTS
        );
    }

    /// Test with various buffer sizes to ensure the fix works universally
    #[test]
    fn test_buffer_wrapping_various_sizes() {
        let buffer_sizes = vec![256, 512, 1024, 2048];

        for buffer_size in buffer_sizes {
            let num_events = (buffer_size * 100) as u64; // 100x the buffer size

            println!(
                "Testing buffer size {} with {} events",
                buffer_size, num_events
            );

            let segment_name = unique_test_segment(&format!("size_{}", buffer_size));

            let mut producer =
                build_shared_single_producer::<TestEvent>(&segment_name, buffer_size)
                    .build_producer(TestEvent::default)
                    .expect("Failed to create producer");

            let start = Instant::now();
            for i in 0..num_events {
                if start.elapsed() > Duration::from_secs(5) {
                    panic!("Timeout with buffer size {} at event {}", buffer_size, i);
                }

                producer.publish(|event| {
                    event.sequence = i as i64;
                });
            }

            println!(
                "✅ Buffer size {} handled {} events correctly",
                buffer_size, num_events
            );
        }
    }
}
