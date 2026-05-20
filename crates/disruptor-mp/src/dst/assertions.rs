//! Assertion primitives used by deterministic-simulation tests.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, OnceLock};

/// Classification for an assertion recorded by the DST harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AssertionKind {
    /// An invariant that must hold for every execution.
    Always,
    /// An outcome that must occur in at least one execution.
    Sometimes,
    /// A code path that must be reachable in at least one execution.
    Reachable,
    /// A code path that must never be reached.
    Unreachable,
}

/// Concrete violation captured by the DST assertion log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssertionViolation {
    /// The assertion class that failed.
    pub kind: AssertionKind,
    /// Stable assertion name or message.
    pub message: String,
    /// Extra diagnostic context attached at the call site.
    pub details: String,
}

/// Aggregated assertion state collected across one DST run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssertionLog {
    /// Violations of invariants that must hold on every run.
    pub always_violations: Vec<AssertionViolation>,
    /// Violations produced by code paths that should never execute.
    pub unreachable_violations: Vec<AssertionViolation>,
    /// Whether a named "sometimes" assertion has been satisfied at least once.
    pub sometimes: BTreeMap<String, bool>,
    /// Named code paths observed as reachable during the run.
    pub reachable: BTreeSet<String>,
}

impl AssertionLog {
    /// Record a mandatory invariant and store a violation when it fails.
    pub fn assert_always(
        &mut self,
        condition: bool,
        message: impl Into<String>,
        details: impl Into<String>,
    ) {
        if !condition {
            self.always_violations.push(AssertionViolation {
                kind: AssertionKind::Always,
                message: message.into(),
                details: details.into(),
            });
        }
    }

    /// Mark a "sometimes" assertion as satisfied if `condition` is true.
    pub fn assert_sometimes(
        &mut self,
        condition: bool,
        message: impl Into<String>,
        _details: impl Into<String>,
    ) {
        let message = message.into();
        let entry = self.sometimes.entry(message).or_insert(false);
        *entry |= condition;
    }

    /// Mark a named path as reachable in this run.
    pub fn assert_reachable(&mut self, message: impl Into<String>) {
        self.reachable.insert(message.into());
    }

    /// Record an unexpected code path with attached context.
    pub fn assert_unreachable(&mut self, message: impl Into<String>, details: impl Into<String>) {
        self.unreachable_violations.push(AssertionViolation {
            kind: AssertionKind::Unreachable,
            message: message.into(),
            details: details.into(),
        });
    }

    /// Returns whether a named "sometimes" assertion was satisfied.
    pub fn sometimes_satisfied(&self, message: &str) -> bool {
        self.sometimes.get(message).copied().unwrap_or(false)
    }
}

fn global_assertion_log() -> &'static Mutex<AssertionLog> {
    static GLOBAL_ASSERTION_LOG: OnceLock<Mutex<AssertionLog>> = OnceLock::new();
    GLOBAL_ASSERTION_LOG.get_or_init(|| Mutex::new(AssertionLog::default()))
}

fn with_global_log<F, R>(f: F) -> R
where
    F: FnOnce(&mut AssertionLog) -> R,
{
    let mut log = global_assertion_log()
        .lock()
        .expect("global dst assertion log should not be poisoned");
    f(&mut log)
}

/// Reset the global assertion log used by DST helpers.
pub fn reset_global_assertions() {
    with_global_log(|log| *log = AssertionLog::default());
}

/// Return a snapshot of the current global assertion log.
pub fn snapshot_global_assertions() -> AssertionLog {
    with_global_log(|log| log.clone())
}

/// Record a global invariant that must hold for every execution.
pub fn assert_always(condition: bool, message: impl Into<String>, details: impl Into<String>) {
    with_global_log(|log| log.assert_always(condition, message, details));
}

/// Record a global assertion that must be satisfied by at least one execution.
pub fn assert_sometimes(condition: bool, message: impl Into<String>, details: impl Into<String>) {
    with_global_log(|log| log.assert_sometimes(condition, message, details));
}

/// Mark a named global path as reachable.
pub fn assert_reachable(message: impl Into<String>) {
    with_global_log(|log| log.assert_reachable(message));
}

/// Record a global path that should never be reached.
pub fn assert_unreachable(message: impl Into<String>, details: impl Into<String>) {
    with_global_log(|log| log.assert_unreachable(message, details));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sometimes_assertions_stick_once_satisfied() {
        let mut log = AssertionLog::default();
        log.assert_sometimes(false, "ring wraps", "first pass");
        assert!(!log.sometimes_satisfied("ring wraps"));
        log.assert_sometimes(true, "ring wraps", "second pass");
        assert!(log.sometimes_satisfied("ring wraps"));
    }

    #[test]
    fn global_assertions_are_resettable() {
        reset_global_assertions();
        assert_sometimes(true, "zero-copy access used", "leased read path");
        let snapshot = snapshot_global_assertions();
        assert!(snapshot.sometimes_satisfied("zero-copy access used"));
        reset_global_assertions();
        let snapshot = snapshot_global_assertions();
        assert!(!snapshot.sometimes_satisfied("zero-copy access used"));
    }
}
