#![deny(rustdoc::broken_intra_doc_links)]
#![warn(missing_docs)]

//! Multiprocess shared-memory data-plane for Disruptor-style publication.
//!
//! This crate is intentionally scoped to multiprocess concerns.
//! Single-process/threaded Disruptor APIs come from crates.io `disruptor`.

pub use disruptor_core::{MissingFreeSlots, Producer, RingBufferFull, Sequence};

mod api;
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
