//! Competitive IPC benchmark orchestration with feature parity targets aligned to
//! `mp_ipc_world_domination`.
//!
//! This crate does not own the broad internal benchmark universe. It owns the
//! narrower apples-to-apples competitive comparison contract:
//!
//! - strict `1p1c` ping-pong
//! - throughput and fixed-rate CO modes
//! - small canonical size/rate grids
//! - durable artifact paths
//! - internal SHM baselines only
//! - external peer staging under `third_party/`

pub mod adapter;
pub mod orchestrator;
pub mod parity;
