//! Shared deterministic-simulation (DST) and failure-injection
//! fixtures for integration tests. **Internal — not published to
//! crates.io.**
//!
//! Tests in `disruptor-mp` and `myelon` consume these
//! fixtures through the two crates' `dst` Cargo features, so both
//! crates validate identical DST contracts off a single source of
//! truth.
//!
//! Modules:
//!
//! - [`dst_assertions`] — assertion enum + log used to record DST
//!   observations during a run.
//! - [`mod@dst_buggify`] — probabilistic fault injector (LMAX-buggify
//!   style).
//! - [`dst_contract`] — stable contract identifiers
//!   (`OrderingPreserved`, `NoLoss`, etc.).
//! - [`dst_mapping`] — convert raw bench events into DST-checkable
//!   observations.
//! - [`dst_profiles`] — named scenarios (probabilities, seeds).
//! - [`dst_runtime`] — test-time runtime context.

pub mod dst_assertions;
pub mod dst_buggify;
pub mod dst_contract;
pub mod dst_mapping;
pub mod dst_profiles;
pub mod dst_runtime;
