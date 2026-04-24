//! Shared benchmark infrastructure for the Myelon transport stack.
//!
//! This crate provides multiprocess coordination, event types, payload
//! generators, and reporting utilities used by all bench files.

pub mod allocation;
pub mod bench_log;
pub mod codec_bench;
pub mod codec_payloads;
pub mod pingpong;
pub mod competitors;
pub mod coordination;
pub mod events;
pub mod executor_v2;
pub mod framed_bench;
pub mod harness;
pub mod latency;
pub mod raw_ring;
pub mod repeatability;
pub mod report_v2;
pub mod reporting;
pub mod scenario_v2;
pub mod sweep_specs;

pub mod generated {
    #[path = "bench_payload_generated.rs"]
    pub mod bench_payload_generated;
}
