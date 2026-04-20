use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AssertionKind {
    Always,
    Sometimes,
    Reachable,
    Unreachable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssertionViolation {
    pub kind: AssertionKind,
    pub message: String,
    pub details: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssertionLog {
    pub always_violations: Vec<AssertionViolation>,
    pub unreachable_violations: Vec<AssertionViolation>,
    pub sometimes: BTreeMap<String, bool>,
    pub reachable: BTreeSet<String>,
}

impl AssertionLog {
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

    pub fn assert_reachable(&mut self, message: impl Into<String>) {
        self.reachable.insert(message.into());
    }

    pub fn assert_unreachable(&mut self, message: impl Into<String>, details: impl Into<String>) {
        self.unreachable_violations.push(AssertionViolation {
            kind: AssertionKind::Unreachable,
            message: message.into(),
            details: details.into(),
        });
    }

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

pub fn reset_global_assertions() {
    with_global_log(|log| *log = AssertionLog::default());
}

pub fn snapshot_global_assertions() -> AssertionLog {
    with_global_log(|log| log.clone())
}

pub fn assert_always(condition: bool, message: impl Into<String>, details: impl Into<String>) {
    with_global_log(|log| log.assert_always(condition, message, details));
}

pub fn assert_sometimes(condition: bool, message: impl Into<String>, details: impl Into<String>) {
    with_global_log(|log| log.assert_sometimes(condition, message, details));
}

pub fn assert_reachable(message: impl Into<String>) {
    with_global_log(|log| log.assert_reachable(message));
}

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
