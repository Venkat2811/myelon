//! Shared benchmark infrastructure for the Myelon transport stack.
//!
//! This crate provides multiprocess coordination, event types, payload
//! generators, and reporting utilities used by all bench files.

pub mod cli;
pub mod infra;
pub mod layers;

pub mod generated {
    #[path = "bench_payload_generated.rs"]
    pub mod bench_payload_generated;
}
