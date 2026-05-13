//! Deterministic runtime harness primitives for DST-style multiprocess scenarios.
//!
//! This module stays in test support only and currently models orchestration state
//! transitions in-memory with seeded, replayable traces.

use super::contract::{ProcessRole, SchedulerAction, TraceArtifact, TraceStatus};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DstRuntimeState {
    Initialized,
    Running,
    Paused,
    Crashed,
    Restarting,
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DstRuntimeCommand {
    Start,
    Pause,
    Resume,
    Crash { reason: &'static str },
    Restart { reason: &'static str },
    Step,
    Replay,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DstRuntimeError {
    InvalidTransition {
        current: DstRuntimeState,
        command: DstRuntimeCommand,
    },
    BudgetExceeded {
        limit_ns: u64,
        observed_ns: u64,
    },
}

/// Deterministic step result for one command execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeStepEvent {
    pub index: u64,
    pub command: DstRuntimeCommand,
    pub role: ProcessRole,
    pub action: Option<SchedulerAction>,
    pub virtual_time_ns: u64,
    pub attempt: u32,
}

/// Seeded runtime state used by tests while building future true OS-process orchestrators.
#[derive(Debug)]
pub struct DstRuntime {
    pub run_id: String,
    pub profile_id: String,
    pub seed: u64,
    pub state: DstRuntimeState,
    pub step: u64,
    pub max_virtual_time_ns: u64,
    pub virtual_time_ns: u64,
    pub attempts: u32,
    pub timeline: Vec<RuntimeStepEvent>,
    pub trace: TraceArtifact,
}

impl DstRuntime {
    pub fn new(
        run_id: impl Into<String>,
        profile_id: impl Into<String>,
        seed: u64,
        max_virtual_time_ns: u64,
    ) -> Self {
        let run_id = run_id.into();
        let profile_id = profile_id.into();
        let trace = TraceArtifact::new(&run_id, &profile_id, seed);

        Self {
            run_id,
            profile_id,
            seed,
            state: DstRuntimeState::Initialized,
            step: 0,
            max_virtual_time_ns,
            virtual_time_ns: 0,
            attempts: 0,
            timeline: Vec::new(),
            trace,
        }
    }

    pub fn start(&mut self) -> Result<(), DstRuntimeError> {
        if self.state != DstRuntimeState::Initialized && self.state != DstRuntimeState::Restarting {
            return Err(DstRuntimeError::InvalidTransition {
                current: self.state,
                command: DstRuntimeCommand::Start,
            });
        }

        self.state = DstRuntimeState::Running;
        self.record(
            DstRuntimeCommand::Start,
            ProcessRole::Orchestrator,
            None,
            TraceStatus::Success,
        );
        Ok(())
    }

    pub fn pause(&mut self) -> Result<(), DstRuntimeError> {
        if self.state != DstRuntimeState::Running {
            return Err(DstRuntimeError::InvalidTransition {
                current: self.state,
                command: DstRuntimeCommand::Pause,
            });
        }

        self.state = DstRuntimeState::Paused;
        self.record(
            DstRuntimeCommand::Pause,
            ProcessRole::Orchestrator,
            None,
            TraceStatus::Success,
        );
        Ok(())
    }

    pub fn resume(&mut self) -> Result<(), DstRuntimeError> {
        if self.state != DstRuntimeState::Paused {
            return Err(DstRuntimeError::InvalidTransition {
                current: self.state,
                command: DstRuntimeCommand::Resume,
            });
        }

        self.state = DstRuntimeState::Running;
        self.record(
            DstRuntimeCommand::Resume,
            ProcessRole::Orchestrator,
            None,
            TraceStatus::Success,
        );
        Ok(())
    }

    pub fn crash(
        &mut self,
        role: ProcessRole,
        reason: &'static str,
    ) -> Result<(), DstRuntimeError> {
        if matches!(
            self.state,
            DstRuntimeState::Completed | DstRuntimeState::Initialized
        ) {
            return Err(DstRuntimeError::InvalidTransition {
                current: self.state,
                command: DstRuntimeCommand::Crash { reason },
            });
        }

        self.state = DstRuntimeState::Crashed;
        self.record(
            DstRuntimeCommand::Crash { reason },
            role,
            None,
            TraceStatus::Failed {
                reason: reason.to_string(),
            },
        );
        Ok(())
    }

    pub fn restart(
        &mut self,
        role: ProcessRole,
        reason: &'static str,
    ) -> Result<(), DstRuntimeError> {
        if self.state != DstRuntimeState::Crashed {
            return Err(DstRuntimeError::InvalidTransition {
                current: self.state,
                command: DstRuntimeCommand::Restart { reason },
            });
        }

        self.state = DstRuntimeState::Restarting;
        self.record(
            DstRuntimeCommand::Restart { reason },
            role,
            None,
            TraceStatus::Success,
        );
        Ok(())
    }

    /// Record one deterministic harness step for a role/action pair.
    pub fn step(
        &mut self,
        role: ProcessRole,
        action: SchedulerAction,
        attempt: u32,
    ) -> Result<(), DstRuntimeError> {
        if self.state != DstRuntimeState::Running {
            return Err(DstRuntimeError::InvalidTransition {
                current: self.state,
                command: DstRuntimeCommand::Step,
            });
        }

        self.attempts = self.attempts.saturating_add(attempt);
        self.record(
            DstRuntimeCommand::Step,
            role,
            Some(action),
            TraceStatus::Success,
        );
        Ok(())
    }

    pub fn complete(&mut self) {
        self.state = DstRuntimeState::Completed;
        self.record(
            DstRuntimeCommand::Replay,
            ProcessRole::Orchestrator,
            None,
            TraceStatus::Success,
        );
    }

    fn record(
        &mut self,
        command: DstRuntimeCommand,
        role: ProcessRole,
        action: Option<SchedulerAction>,
        status: TraceStatus,
    ) {
        let _ = action;
        let trace_role = role.clone();
        let trace_action = action.clone().unwrap_or(SchedulerAction::Spawn);
        self.virtual_time_ns = self
            .virtual_time_ns
            .saturating_add(self.step.saturating_add(1).saturating_mul(1_000));
        self.trace.push(
            trace_role,
            trace_action.clone(),
            status,
            format!("idx={}", self.step),
        );
        self.timeline.push(RuntimeStepEvent {
            index: self.step,
            command,
            role: role.clone(),
            action,
            virtual_time_ns: self.virtual_time_ns,
            attempt: self.attempts,
        });
        self.step = self.step.saturating_add(1);
    }

    pub fn enforce_budget(&self, observed_ns: u64) -> Result<(), DstRuntimeError> {
        if observed_ns > self.max_virtual_time_ns {
            return Err(DstRuntimeError::BudgetExceeded {
                limit_ns: self.max_virtual_time_ns,
                observed_ns,
            });
        }
        Ok(())
    }

    pub fn into_trace(mut self) -> TraceArtifact {
        self.virtual_time_ns = self.virtual_time_ns.min(self.max_virtual_time_ns);
        self.trace.set_metadata("run_id", self.run_id.clone());
        self.trace
            .set_metadata("profile_id", self.profile_id.clone());
        self.trace.set_metadata("seed", self.seed.to_string());
        self.trace.clone()
    }
}

impl From<&DstRuntime> for TraceArtifact {
    fn from(value: &DstRuntime) -> Self {
        value.trace.clone()
    }
}

pub fn replay_trace(expected: &TraceArtifact, actual: &TraceArtifact) -> Result<(), String> {
    if expected.events.len() != actual.events.len() {
        return Err(format!(
            "trace length mismatch: expected {} actual {}",
            expected.events.len(),
            actual.events.len()
        ));
    }

    for (index, (left, right)) in expected.events.iter().zip(actual.events.iter()).enumerate() {
        if left != right {
            return Err(format!(
                "trace event mismatch at step {index}: expected={left:?}, actual={right:?}"
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime() -> DstRuntime {
        DstRuntime::new("dst-runtime-test", "unit", 0x1700, 10_000)
    }

    #[test]
    fn rejects_pause_before_start() {
        let mut runtime = runtime();
        let err = runtime.pause().expect_err("pause before start must fail");
        assert_eq!(
            err,
            DstRuntimeError::InvalidTransition {
                current: DstRuntimeState::Initialized,
                command: DstRuntimeCommand::Pause,
            }
        );
    }

    #[test]
    fn rejects_resume_without_pause() {
        let mut runtime = runtime();
        runtime.start().expect("start should succeed");
        let err = runtime
            .resume()
            .expect_err("resume without pause must fail");
        assert_eq!(
            err,
            DstRuntimeError::InvalidTransition {
                current: DstRuntimeState::Running,
                command: DstRuntimeCommand::Resume,
            }
        );
    }

    #[test]
    fn rejects_crash_before_start() {
        let mut runtime = runtime();
        let err = runtime
            .crash(ProcessRole::Producer, "not-running")
            .expect_err("crash before start must fail");
        assert_eq!(
            err,
            DstRuntimeError::InvalidTransition {
                current: DstRuntimeState::Initialized,
                command: DstRuntimeCommand::Crash {
                    reason: "not-running",
                },
            }
        );
    }

    #[test]
    fn rejects_restart_without_crash() {
        let mut runtime = runtime();
        runtime.start().expect("start should succeed");
        let err = runtime
            .restart(ProcessRole::Consumer { index: 0 }, "still-running")
            .expect_err("restart without crash must fail");
        assert_eq!(
            err,
            DstRuntimeError::InvalidTransition {
                current: DstRuntimeState::Running,
                command: DstRuntimeCommand::Restart {
                    reason: "still-running",
                },
            }
        );
    }

    #[test]
    fn rejects_step_when_not_running() {
        let mut runtime = runtime();
        let err = runtime
            .step(ProcessRole::Producer, SchedulerAction::Spawn, 1)
            .expect_err("step before start must fail");
        assert_eq!(
            err,
            DstRuntimeError::InvalidTransition {
                current: DstRuntimeState::Initialized,
                command: DstRuntimeCommand::Step,
            }
        );
    }

    #[test]
    fn budget_check_reports_overflow() {
        let runtime = runtime();
        let err = runtime
            .enforce_budget(20_000)
            .expect_err("budget overflow must fail");
        assert_eq!(
            err,
            DstRuntimeError::BudgetExceeded {
                limit_ns: 10_000,
                observed_ns: 20_000,
            }
        );
    }
}
