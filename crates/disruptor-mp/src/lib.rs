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
