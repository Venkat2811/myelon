//! Deterministic runtime harness primitives for DST-style multiprocess scenarios.
//!
//! This module stays in test support only and currently models orchestration state
//! transitions in-memory with seeded, replayable traces.

use super::contract::{ProcessRole, SchedulerAction, TraceArtifact, TraceStatus};
use serde::{Deserialize, Serialize};

/// Runtime state for the in-memory DST harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DstRuntimeState {
    /// Constructed but not yet started.
    Initialized,
    /// Actively executing scheduled steps.
    Running,
    /// Temporarily paused by the orchestrator.
    Paused,
    /// Stopped because of an injected crash or fault.
    Crashed,
    /// Re-entering the running state after a crash.
    Restarting,
    /// Finished and no longer accepting new steps.
    Completed,
}

/// Command applied to the runtime state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DstRuntimeCommand {
    /// Transition from initialized/restarting into running.
    Start,
    /// Pause an active run.
    Pause,
    /// Resume a paused run.
    Resume,
    /// Crash the targeted role for the supplied reason.
    Crash {
        /// Static reason string attached to the crash.
        reason: &'static str,
    },
    /// Restart the targeted role for the supplied reason.
    Restart {
        /// Static reason string attached to the restart.
        reason: &'static str,
    },
    /// Execute one scheduled step.
    Step,
    /// Mark the run as complete/replayed.
    Replay,
}

/// Error returned by invalid runtime transitions or timing budget breaches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DstRuntimeError {
    /// The requested command is not valid from the current state.
    InvalidTransition {
        /// State observed when the command was attempted.
        current: DstRuntimeState,
        /// Command that was rejected.
        command: DstRuntimeCommand,
    },
    /// Observed time exceeded the configured virtual-time budget.
    BudgetExceeded {
        /// Maximum allowed virtual-time budget in nanoseconds.
        limit_ns: u64,
        /// Observed time in nanoseconds.
        observed_ns: u64,
    },
}

/// Deterministic step result for one command execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeStepEvent {
    /// Monotonic step index.
    pub index: u64,
    /// Runtime command executed at this step.
    pub command: DstRuntimeCommand,
    /// Role associated with the step.
    pub role: ProcessRole,
    /// Optional scheduler action attached to the step.
    pub action: Option<SchedulerAction>,
    /// Virtual time after the step completed.
    pub virtual_time_ns: u64,
    /// Cumulative retry-attempt counter after the step.
    pub attempt: u32,
}

/// Seeded runtime state used by tests while building future true OS-process orchestrators.
#[derive(Debug)]
pub struct DstRuntime {
    /// Stable run identifier.
    pub run_id: String,
    /// Profile identifier selected by the caller.
    pub profile_id: String,
    /// Seed used to generate runtime behavior.
    pub seed: u64,
    /// Current runtime state.
    pub state: DstRuntimeState,
    /// Number of recorded steps.
    pub step: u64,
    /// Maximum virtual-time budget in nanoseconds.
    pub max_virtual_time_ns: u64,
    /// Current virtual-time counter in nanoseconds.
    pub virtual_time_ns: u64,
    /// Aggregate retry-attempt counter.
    pub attempts: u32,
    /// In-memory timeline of runtime steps.
    pub timeline: Vec<RuntimeStepEvent>,
    /// Serializable trace captured for replay and inspection.
    pub trace: TraceArtifact,
}

impl DstRuntime {
    /// Create a new runtime with an empty trace and zero virtual time.
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

    /// Transition the runtime into the running state.
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

    /// Transition the runtime from running to paused.
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

    /// Resume a paused runtime.
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

    /// Mark the runtime as crashed for the given role and reason.
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

    /// Move a crashed runtime into the restarting state.
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

    /// Mark the runtime as completed and append the terminal replay event.
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

    /// Fail if the observed time exceeds the configured virtual-time budget.
    pub fn enforce_budget(&self, observed_ns: u64) -> Result<(), DstRuntimeError> {
        if observed_ns > self.max_virtual_time_ns {
            return Err(DstRuntimeError::BudgetExceeded {
                limit_ns: self.max_virtual_time_ns,
                observed_ns,
            });
        }
        Ok(())
    }

    /// Finalize the runtime and return the accumulated trace artifact.
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

/// Compare two traces for exact event-by-event equality.
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
