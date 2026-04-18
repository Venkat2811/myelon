//! Shared benchmark infrastructure for the Myelon transport stack.
//!
//! This crate provides multiprocess coordination, event types, payload
//! generators, and reporting utilities used by all bench files.

pub mod bench_log;
pub mod codec_payloads;
pub mod competitors;
pub mod coordination;
pub mod events;
pub mod harness;
pub mod latency;
pub mod reporting;

pub mod generated {
    #[path = "bench_payload_generated.rs"]
    pub mod bench_payload_generated;
}
