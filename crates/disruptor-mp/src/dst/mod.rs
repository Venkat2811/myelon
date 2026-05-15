//! Deterministic-simulation testing (DST) primitives, gated under
//! `#[cfg(dst)]`.
//!
//! Ships with `disruptor-mp` itself so that the production-path BUGGIFY
//! call sites (in `producer`, `builder`, `consumer`, `lock_free`,
//! `observability`) resolve into real fault-injection logic when
//! workspace tests run with `RUSTFLAGS="--cfg dst"`, and disappear at
//! compile time otherwise. Mirrors how TigerBeetle ships its VOPR
//! primitives alongside production code (just under a build-time
//! switch instead of a Cargo feature), and how FoundationDB ships
//! BUGGIFY with the production codebase rather than as an external
//! dependency.
//!
//! Available to crates.io consumers who set `RUSTFLAGS="--cfg dst"` on
//! their own builds — you get a usable BUGGIFY + Antithesis-style
//! assertion log + DST runtime out of the box, no extra crate.
//!
//! ## Modules
//!
//! - [`assertions`] — Antithesis-style `assert_always` /
//!   `assert_sometimes` / `assert_reachable` / `assert_unreachable` and
//!   the global `AssertionLog`.
//! - [`buggify`] — LMAX/FoundationDB-style probabilistic fault
//!   injection (`buggify(file!(), line!())` returns `true` with a
//!   per-site probability under the active scenario).
//! - [`contract`] — stable identifiers used by the multiprocess DST
//!   harness (`FailureClass`, `ProcessRole`, `TraceArtifact`,
//!   `TraceStatus`, `SchedulerAction`).
//! - [`mapping`] — translate raw bench events into DST-checkable
//!   observations.
//! - [`profiles`] — named DST scenarios (probabilities + seeds).
//! - [`runtime`] — test-time runtime context (active scenario, RNG
//!   handle, process role).
//!
//! ## Convenience re-exports
//!
//! The most-used items are re-exported at the `dst::*` level so call
//! sites can write `crate::dst::buggify(...)` /
//! `crate::dst::assert_sometimes(...)`.

pub mod assertions;
pub mod buggify;
pub mod contract;
pub mod mapping;
pub mod profiles;
pub mod runtime;

pub use assertions::{
    assert_always, assert_reachable, assert_sometimes, assert_unreachable, reset_global_assertions,
    snapshot_global_assertions, AssertionKind, AssertionLog,
};
pub use buggify::{buggify, ScopedBuggify};
pub use contract::{FailureClass, ProcessRole, SchedulerAction, TraceArtifact, TraceStatus};
