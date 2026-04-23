//! Competitive transport benchmark orchestration with feature parity targets
//! aligned to `mp_ipc_world_domination`.
//!
//! This crate does not own the broad internal benchmark universe. It owns the
//! narrower apples-to-apples competitive comparison contract:
//!
//! - strict `1p1c` ping-pong
//! - `1p4c` / `1p8c` broadcast
//! - throughput and fixed-rate CO modes
//! - default and extensive payload ladders
//! - durable artifact paths
//! - internal raw SHM + mmap baselines
//! - external peer staging under `third_party/`

pub mod adapter;
pub mod orchestrator;
pub mod parity;
pub mod pingpong_support;

pub mod result_json;
