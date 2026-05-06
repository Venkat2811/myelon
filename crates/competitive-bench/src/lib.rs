//! Competitive IPC benchmark orchestration. **Internal — not
//! published to crates.io.**
//!
//! `competitive-bench` does not own the broad internal performance
//! sweep universe (that lives in `perf-bench`). It owns the narrower
//! apples-to-apples comparison contract used to measure
//! `disruptor-mp` against external transports.
//!
//! Comparison contract:
//!
//! - strict `1p1c` ping-pong
//! - `1p4c` / `1p8c` broadcast
//! - throughput and fixed-rate coordinated-omission-aware modes
//! - default and extensive payload ladders
//! - durable artifact paths
//! - internal raw SHM + mmap baselines
//! - external peer staging under `third_party/`
//!
//! External transports compared:
//! `crossbar`, `shmipc`, `rusteron` (Aeron client), `iceoryx2`,
//! `zmq`, `iggy`, `redpanda`.
//!
//! Modules:
//!
//! - [`adapters`] — per-transport adapter binaries.
//! - [`infra`] — coordination, latency recording, JSON output schema.
//! - [`runner`] — orchestrator that drives a tier or single adapter.

pub mod adapters;
pub mod infra;
pub mod runner;
