//! Cross-adapter primitives shared by every competitive benchmark binary.
//!
//! - [`adapter`] — registry of adapter ids, origins, and parity specs
//! - [`parity`] — payload-size ladders and per-size message-count tuning
//! - [`pingpong`] — shared protocol helpers (control bytes, pacing, payloads)
//! - [`result_json`] — JSON output schema written by every adapter binary

pub mod adapter;
pub mod parity;
pub mod pingpong;
pub mod result_json;
