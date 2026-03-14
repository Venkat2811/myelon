#![deny(rustdoc::broken_intra_doc_links)]
#![warn(missing_docs)]

//! Multiprocess shared-memory data-plane for Disruptor-style publication.
//!
//! This crate is intentionally scoped to multiprocess concerns.
//! Single-process/threaded Disruptor APIs come from crates.io `disruptor`.

pub use disruptor_core::{MissingFreeSlots, Producer, RingBufferFull, Sequence};

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
mod shared_memory_layout;
pub use api::*;

/// Returns true when `value` is an exact multiple of `divisor`.
///
/// This uses division/multiplication to avoid unstable and toolchain-dependent
/// integer helper APIs while staying explicit and compiler-stable.
#[inline]
pub fn is_multiple_of_u64(value: u64, divisor: u64) -> bool {
    divisor != 0 && value / divisor * divisor == value
}
