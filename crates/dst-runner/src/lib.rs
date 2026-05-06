//! Multiprocess deterministic-simulation harness for `disruptor-mp`
//! and `myelon`. **Internal — not published to crates.io.**
//!
//! A test launches a parent process that spawns producer and consumer
//! children with a controlled config, fault-injection profile, and
//! oracle. The runner collects per-child reports, runs the assertion
//! oracle, and verifies the DST contract.
//!
//! Modules:
//!
//! - [`config`] — deterministic-run knobs: backends, codecs, wait
//!   strategies, coordination modes.
//! - [`fault`] — buggify-style probabilistic fault injection.
//! - [`oracle`] — message-level invariants checked across runs.
//! - [`report`] — per-child and aggregate run reports.
//! - [`runner`] — the [`runner::DstRunner`] surface and built-in
//!   harnesses.
//! - [`verify`] — cross-child verification.

pub mod config;
pub mod fault;
pub mod oracle;
pub mod report;
pub mod runner;
pub mod verify;

pub use config::{BackendKind, CodecKind, CoordinationKind, DstConfig, WaitStrategyKind};
pub use dst_fixtures::dst_assertions::{AssertionKind, AssertionLog, AssertionViolation};
pub use fault::{FaultEvent, FaultInjector, FaultKind};
pub use oracle::{
    payload_bytes, stable_payload_hash, MessageOracle, OracleMessage, OracleViolation,
};
pub use report::{ChildReport, DstProperty, DstRunReport, TransportKind};
pub use runner::{DstRunner, DstRunnerError, RawRingHarness, RequiredConsumerLivenessPolicy};
