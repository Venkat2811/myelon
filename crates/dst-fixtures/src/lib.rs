//! Shared deterministic simulation and failure-injection fixtures for integration tests.
//!
//! The source files are intentionally mirrored from the test-support source of
//! `disruptor-mp` and shared through this crate so `disruptor-mp` and
//! `myelon` can validate identical DST contracts.

pub mod dst_assertions;
pub mod dst_buggify;
pub mod dst_contract;
pub mod dst_mapping;
pub mod dst_profiles;
pub mod dst_runtime;
