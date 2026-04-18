//! Shared runner utilities for bench binaries.
//!
//! This centralizes the last common top-level boilerplate: child-role dispatch,
//! timeout-wrapped child collection, and structured child-metric parsing.

use super::process::{collect_output, parse_json_output, wait_timeout, ProcessOutput};
use serde::de::DeserializeOwned;
use std::process::Child;
use std::time::Duration;

pub type BenchError = Box<dyn std::error::Error>;
pub type BenchRunResult = Result<(), BenchError>;
pub type ChildHandler = fn() -> BenchRunResult;

#[derive(Clone, Copy)]
pub struct ChildRole {
    pub name: &'static str,
    pub handler: ChildHandler,
}

impl ChildRole {
    pub const fn new(name: &'static str, handler: ChildHandler) -> Self {
        Self { name, handler }
    }
}

pub fn maybe_run_child<'a>(
    args: &'a [String],
    roles: &[ChildRole],
) -> Option<(&'a str, BenchRunResult)> {
    let role = args.get(1)?;
    if role.starts_with("--") {
        return None;
    }

    let _log = crate::bench_log::BenchLog::default_capacity(role);
    let result = roles
        .iter()
        .find(|entry| entry.name == role)
        .map(|entry| (entry.handler)())
        .unwrap_or(Ok(()));
    Some((role.as_str(), result))
}

pub fn dispatch_child_or_exit(args: &[String], roles: &[ChildRole]) -> bool {
    let Some((role, result)) = maybe_run_child(args, roles) else {
        return false;
    };

    if let Err(error) = result {
        eprintln!("{role} failed: {error}");
        std::process::exit(1);
    }
    true
}

pub fn collect_child_output(label: &str, child: Child, timeout: Duration) -> ProcessOutput {
    collect_output(label, wait_timeout(child, timeout))
}

pub fn parse_child_metrics<T: DeserializeOwned>(label: &str, output: &ProcessOutput) -> T {
    assert!(
        output.success,
        "{label} child failed\nstderr:\n{}",
        output.stderr
    );
    parse_json_output::<T>(&output.stdout).unwrap_or_else(|| {
        panic!(
            "{label} child produced no structured JSON metrics\nstdout:\n{}\nstderr:\n{}",
            output.stdout, output.stderr
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn noop_child() -> BenchRunResult {
        Ok(())
    }

    #[test]
    fn test_maybe_run_child_ignores_flag_args() {
        let args = vec!["bench".to_string(), "--quick".to_string()];
        assert!(maybe_run_child(&args, &[]).is_none());
    }

    #[test]
    fn test_maybe_run_child_dispatches_registered_role() {
        let args = vec!["bench".to_string(), "child".to_string()];
        let roles = [ChildRole::new("child", noop_child)];
        let Some((role, result)) = maybe_run_child(&args, &roles) else {
            panic!("expected child dispatch");
        };
        assert_eq!(role, "child");
        assert!(result.is_ok());
    }
}
