#![cfg(dst)]
//!
//! Multiprocess deterministic-simulation (DST) harness for
//! `disruptor-mp` and `myelon`. **Internal — not published to
//! crates.io.**
//!
//! The DST *primitives* (BUGGIFY, Antithesis-style assertions,
//! contract types, scenario profiles, runtime context) ship inside
//! `disruptor-mp` itself under `disruptor_mp::dst::*` so that
//! production-path call sites resolve without needing this crate (see
//! `disruptor_mp::dst` for the design rationale).
//!
//! This crate provides the *orchestrator*: a multiprocess runner that
//! spawns producer/consumer children with controlled configs,
//! injects faults, collects per-child reports, and verifies
//! cross-child invariants.
//!
//! ## Workflow
//!
//! ```sh
//! RUSTFLAGS="--cfg dst" cargo test -p disruptor-mp -p myelon
//! ```
//!
//! Workspace tests pull this crate in via
//! `[target.'cfg(dst)'.dev-dependencies]`. Published `disruptor-mp` and
//! `myelon` manifests never reference it.
//!
//! ## Modules
//!
//! - [`runner_config`] — deterministic-run knobs: backends, codecs,
//!   wait strategies, coordination modes.
//! - [`runner_fault`] — buggify-style probabilistic fault injection
//!   driven by the runner.
//! - [`runner_oracle`] — message-level invariants checked across runs.
//! - [`runner_report`] — per-child and aggregate run reports.
//! - [`runner`] — the [`runner::DstRunner`] surface and built-in
//!   harnesses.
//! - [`runner_verify`] — cross-child verification.

pub mod runner;
pub mod runner_config;
pub mod runner_fault;
pub mod runner_oracle;
pub mod runner_report;
pub mod runner_verify;

// Convenience re-exports preserve the flat DST harness surface
// (`BackendKind`, `ChildReport`, etc.) for workspace tests.
pub use runner::{DstRunner, DstRunnerError, RawRingHarness, RequiredConsumerLivenessPolicy};
pub use runner_config::{BackendKind, CodecKind, DstConfig, WaitStrategyKind};
pub use runner_fault::FaultInjector;
pub use runner_oracle::{
    payload_bytes, stable_payload_hash, MessageOracle, OracleMessage, OracleViolation,
};
pub use runner_report::{ChildReport, DstProperty, DstRunReport, TransportKind};
