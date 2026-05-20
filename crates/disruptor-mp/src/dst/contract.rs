//! Deterministic DST support primitives used by integration tests.
//!
//! This module intentionally stays in `tests/support` and is not part of the
//! public library API. It defines the minimum abstractions required to describe
//! reproducible multiprocess failure scenarios:
//! - seeded scheduling decisions,
//! - deterministic fault injection,
//! - structured trace recording,
//! - and replay comparison.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::{self, Display};

/// Unique role identity used in trace and scheduler decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessRole {
    /// Producer process.
    Producer,
    /// Consumer process by index.
    Consumer {
        /// Zero-based consumer index within the scenario topology.
        index: u32,
    },
    /// Harness/controller process.
    Orchestrator,
}

impl Display for ProcessRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Producer => write!(f, "Producer"),
            Self::Consumer { index } => write!(f, "Consumer({index})"),
            Self::Orchestrator => write!(f, "Orchestrator"),
        }
    }
}

/// Logical action emitted by the DST scheduler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchedulerAction {
    /// Create a process for the next role.
    Spawn,
    /// Delay before the next action (for stable jitter simulation).
    SleepMs(
        /// Delay duration in milliseconds.
        u64,
    ),
    /// Establish attachment to shared state.
    Attach,
    /// Start the role's runtime loop.
    Start,
    /// Simulate a producer publish batch.
    PublishBatch {
        /// Number of events represented by the batch.
        events: u32,
    },
    /// Simulate consumer progress milestone.
    ConsumeBatch {
        /// Number of events represented by the batch.
        events: u32,
    },
    /// Simulate process exit.
    Kill,
    /// Simulate restart after explicit fault.
    Restart,
    /// Finalization for one role.
    Shutdown,
}

/// Status attached to a trace event during record or replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraceStatus {
    /// The event was planned but not yet executed.
    Planned,
    /// The event completed successfully.
    Success,
    /// The event requested an explicit retry path.
    Retry,
    /// The event failed with the supplied reason.
    Failed {
        /// Human-readable failure reason captured by the harness.
        reason: String,
    },
}

/// One deterministic scheduler step generated from a startup plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduledStep {
    /// Monotonic step index in the generated plan.
    pub step: u64,
    /// Action the orchestrator should perform.
    pub action: SchedulerAction,
    /// Process role targeted by the action.
    pub role: ProcessRole,
    /// Retry attempt number for the targeted role/action.
    pub attempt: u32,
    /// Stable plan identifier shared across all steps in the run.
    pub plan_id: String,
    /// Human-readable profile name for this scheduling variant.
    pub profile: String,
}

/// Deterministic pseudo random generator for seeded scheduling.
#[derive(Debug, Clone)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        let init = if seed == 0 {
            0x9E37_79B1_85EB_CA87
        } else {
            seed
        };
        Self { state: init }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 7;
        x ^= x >> 9;
        x ^= x << 8;
        self.state = x;
        x
    }

    fn next_u32(&mut self, max_exclusive: u32) -> u32 {
        if max_exclusive == 0 {
            return 0;
        }

        let max = max_exclusive as u64;
        (self.next_u64() % max) as u32
    }
}

/// Stable startup schedule generated from a seed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StartupSchedule {
    /// Stable identifier for the schedule.
    pub plan_id: String,
    /// Seed used to generate the schedule.
    pub seed: u64,
    /// Maximum number of steps requested by the caller.
    pub step_cap: u64,
    /// Ordered scheduler steps emitted from the seed.
    pub steps: Vec<ScheduledStep>,
}

impl StartupSchedule {
    /// Build a deterministic startup schedule for the given process count and seed.
    pub fn from_seed(
        plan_id: impl Into<String>,
        seed: u64,
        consumers: u32,
        max_steps: u64,
    ) -> Self {
        let mut rng = XorShift64::new(seed);
        let plan_id = plan_id.into();
        let mut steps = Vec::new();

        let producer_first = rng.next_u32(2) == 0;
        let mut step = 0u64;
        let profile = if producer_first {
            "producer_first"
        } else {
            "consumer_first"
        };

        if producer_first {
            steps.push(ScheduledStep {
                step,
                action: SchedulerAction::Spawn,
                role: ProcessRole::Producer,
                attempt: 0,
                plan_id: plan_id.clone(),
                profile: profile.to_string(),
            });
            step += 1;
        }

        for index in 0..consumers {
            steps.push(ScheduledStep {
                step,
                action: SchedulerAction::Spawn,
                role: ProcessRole::Consumer { index },
                attempt: 0,
                plan_id: plan_id.clone(),
                profile: profile.to_string(),
            });
            step += 1;

            let delay = u64::from(rng.next_u32(4));
            if delay > 0 {
                steps.push(ScheduledStep {
                    step,
                    action: SchedulerAction::SleepMs(25 * delay),
                    role: ProcessRole::Orchestrator,
                    attempt: 0,
                    plan_id: plan_id.clone(),
                    profile: profile.to_string(),
                });
                step += 1;
            }
        }

        if !producer_first {
            steps.push(ScheduledStep {
                step,
                action: SchedulerAction::Spawn,
                role: ProcessRole::Producer,
                attempt: 0,
                plan_id: plan_id.clone(),
                profile: profile.to_string(),
            });
            step += 1;
        }

        while step < max_steps {
            steps.push(ScheduledStep {
                step,
                action: SchedulerAction::SleepMs(10),
                role: ProcessRole::Orchestrator,
                attempt: 0,
                plan_id: plan_id.clone(),
                profile: profile.to_string(),
            });
            step += 1;
        }

        Self {
            plan_id,
            seed,
            step_cap: max_steps,
            steps,
        }
    }

    /// Returns true when the schedule contains concrete replayable steps.
    pub fn is_deterministic(&self) -> bool {
        !self.steps.is_empty()
    }
}

/// Fault kind injected by a deterministic failure plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FailureKind {
    /// Delay attach or readiness progress by the supplied milliseconds.
    DelayAttachMs(
        /// Delay duration in milliseconds.
        u64,
    ),
    /// Kill the targeted process.
    KillProcess,
    /// Restart the targeted process after a fault.
    RestartProcess,
    /// Drop the initial attach for a specific consumer.
    DropConsumerAttach {
        /// Zero-based consumer index to perturb.
        consumer_index: u32,
    },
    /// Reserve a step without applying a real fault.
    Noop,
}

/// One injected failure scheduled at a concrete step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FailureStep {
    /// Scheduler step at which the failure should be applied.
    pub step: u64,
    /// Role affected by the failure.
    pub role: ProcessRole,
    /// Failure action to apply.
    pub kind: FailureKind,
}

/// Deterministic failure injector backed by seed-derived injection points.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeterministicFaultInjector {
    /// Seed used to generate the fault plan.
    pub seed: u64,
    /// Highest step index eligible for injection.
    pub max_steps: u64,
    /// Faults ordered by injection step.
    pub inject_at: Vec<FailureStep>,
}

impl DeterministicFaultInjector {
    /// Build a deterministic fault plan for the given topology.
    pub fn from_seed(seed: u64, consumers: u32, max_steps: u64) -> Self {
        let mut rng = XorShift64::new(seed ^ 0xA5A5_5A5A_77AA_11EE);
        let mut inject_at = Vec::new();
        let injector_count = rng.next_u32(3) as usize;

        for _idx in 0..injector_count {
            let step = u64::from(rng.next_u32(max_steps as u32));
            let role = if consumers == 0 || rng.next_u32(2) == 0 {
                ProcessRole::Producer
            } else {
                ProcessRole::Consumer {
                    index: rng.next_u32(consumers.max(1)),
                }
            };

            let kind = match rng.next_u32(4) {
                0 => FailureKind::DelayAttachMs(25 * u64::from(rng.next_u32(4) + 1)),
                1 => FailureKind::KillProcess,
                2 => FailureKind::RestartProcess,
                _ => FailureKind::Noop,
            };

            inject_at.push(FailureStep { step, role, kind });
        }

        inject_at.sort_by_key(|event| event.step);
        Self {
            seed,
            max_steps,
            inject_at,
        }
    }

    /// Return all failures scheduled for `step`.
    pub fn at_step(&self, step: u64) -> Vec<&FailureStep> {
        self.inject_at
            .iter()
            .filter(|event| event.step == step)
            .collect()
    }
}

/// One recorded event in a deterministic trace.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TraceEvent {
    /// Monotonic event index within the trace.
    pub step: u64,
    /// Role that executed the action.
    pub role: ProcessRole,
    /// Action that was attempted or completed.
    pub action: SchedulerAction,
    /// Result status recorded for the action.
    pub status: TraceStatus,
    /// Retry attempt index associated with the event.
    pub attempt: u32,
    /// Resource name or synthetic identifier attached to the event.
    pub resource: String,
    /// Free-form metadata emitted by the harness.
    pub metadata: BTreeMap<String, String>,
}

/// Serializable trace artifact captured from a DST run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TraceArtifact {
    /// Stable run identifier selected by the caller.
    pub run_id: String,
    /// Profile identifier used to generate the run.
    pub profile_id: String,
    /// Seed associated with the run.
    pub seed: u64,
    /// Ordered events recorded for the run.
    pub events: Vec<TraceEvent>,
}

impl TraceArtifact {
    /// Create an empty trace artifact for a new run.
    pub fn new(run_id: impl Into<String>, profile_id: impl Into<String>, seed: u64) -> Self {
        Self {
            run_id: run_id.into(),
            profile_id: profile_id.into(),
            seed,
            events: Vec::new(),
        }
    }

    /// Append a new trace event with the next step index.
    pub fn push(
        &mut self,
        role: ProcessRole,
        action: SchedulerAction,
        status: TraceStatus,
        resource: impl Into<String>,
    ) {
        let step = self.events.len() as u64;
        self.events.push(TraceEvent {
            step,
            role,
            action,
            status,
            attempt: 0,
            resource: resource.into(),
            metadata: BTreeMap::new(),
        });
    }

    /// Attach metadata to the most recently recorded event.
    pub fn set_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        if let Some(last) = self.events.last_mut() {
            last.metadata.insert(key.into(), value.into());
        }
    }

    /// Serialize the trace artifact to pretty-printed JSON.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Parse a trace artifact from JSON.
    pub fn from_json(raw: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(raw)
    }
}

/// Structured reason why two traces failed replay validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayMismatch {
    /// The compared traces do not refer to the same run identity.
    RunIdentity {
        /// Expected run identifier and seed pair.
        left: (String, u64),
        /// Actual run identifier and seed pair.
        right: (String, u64),
    },
    /// The compared traces recorded different numbers of events.
    EventCount {
        /// Expected event count.
        expected: usize,
        /// Actual event count.
        actual: usize,
    },
    /// A specific event differed between the traces.
    EventContent {
        /// Step index of the first mismatching event.
        step: u64,
        /// Expected event payload.
        expected: Box<TraceEvent>,
        /// Actual event payload.
        actual: Box<TraceEvent>,
    },
}

/// Exact replay validator for two trace artifacts.
pub struct ReplayValidator;

impl ReplayValidator {
    /// Validate that `actual` exactly matches `expected`.
    pub fn validate(
        expected: &TraceArtifact,
        actual: &TraceArtifact,
    ) -> Result<(), ReplayMismatch> {
        if (expected.run_id.as_str(), expected.seed) != (actual.run_id.as_str(), actual.seed) {
            return Err(ReplayMismatch::RunIdentity {
                left: (expected.run_id.clone(), expected.seed),
                right: (actual.run_id.clone(), actual.seed),
            });
        }

        if expected.events.len() != actual.events.len() {
            return Err(ReplayMismatch::EventCount {
                expected: expected.events.len(),
                actual: actual.events.len(),
            });
        }

        for (index, (expected_event, actual_event)) in
            expected.events.iter().zip(actual.events.iter()).enumerate()
        {
            if expected_event != actual_event {
                return Err(ReplayMismatch::EventContent {
                    step: index as u64,
                    expected: Box::new(expected_event.clone()),
                    actual: Box::new(actual_event.clone()),
                });
            }
        }

        Ok(())
    }
}

/// Reproducible failure classes for `disruptor-mp` multiprocess contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum FailureClass {
    /// Producer starts and publishes before any consumer is attached.
    ProducerBeforeConsumers,
    /// Consumer attaches late and must not observe stale sequences.
    LateConsumerAttach,
    /// Producer/consumer churn while lifecycle transitions are in-flight.
    CreateAttachChurn,
    /// Producer is killed and later restarted with recovery behavior.
    ProducerCrashAndRestart,
    /// Consumer is restarted while producer remains alive.
    ConsumerCrashAndRestart,
    /// Discovery/metadata window is delayed and must converge deterministically.
    DiscoveryVisibilityLag,
    /// Consumer readiness is required before first publish.
    ReadinessGateViolation,
}

impl FailureClass {
    /// Return all currently supported failure classes.
    pub fn all() -> &'static [FailureClass] {
        use FailureClass::*;
        &[
            ProducerBeforeConsumers,
            LateConsumerAttach,
            CreateAttachChurn,
            ProducerCrashAndRestart,
            ConsumerCrashAndRestart,
            DiscoveryVisibilityLag,
            ReadinessGateViolation,
        ]
    }

    /// Returns the conditions the harness must establish before injecting this class.
    pub fn preconditions(self) -> &'static [&'static str] {
        match self {
            Self::ProducerBeforeConsumers => &[
                "Producer path accepts explicit startup order.",
                "Segment is not yet attached by consumer.",
            ],
            Self::LateConsumerAttach => &[
                "Shared segment is already active.",
                "Consumer retry loop is enabled.",
            ],
            Self::CreateAttachChurn => &[
                "Name churn includes repeated attach/create attempts.",
                "At least one attach/detach cycle exists.",
            ],
            Self::ProducerCrashAndRestart => &[
                "Crash point occurs after create and before all producers are done.",
                "Recovery path allows stale-segment handling.",
            ],
            Self::ConsumerCrashAndRestart => &[
                "Consumer lifecycle allows detach/reattach.",
                "Producer remains valid after consumer restart.",
            ],
            Self::DiscoveryVisibilityLag => &[
                "Shared visibility is explicitly delayed by the harness.",
                "Reader observes metadata after bounded delay.",
            ],
            Self::ReadinessGateViolation => &[
                "Producer is configured with wait-for-consumers contract.",
                "Consumer readiness state is persisted in shared memory.",
            ],
        }
    }

    /// Returns the observable guarantees expected after running this class.
    pub fn postconditions(self) -> &'static [&'static str] {
        match self {
            Self::ProducerBeforeConsumers => &[
                "No silent segment overwrite occurs before attach.",
                "Producer eventually converges once consumer readiness is observed.",
            ],
            Self::LateConsumerAttach => &[
                "Consumer attaches to exact latest producer state.",
                "Startup race is reproducible with bounded timeout.",
            ],
            Self::CreateAttachChurn => &[
                "Each owner handles cleanup deterministically.",
                "Attach failures are explicit and never silently downgrade invariants.",
            ],
            Self::ProducerCrashAndRestart => &[
                "Restarted producer resumes from known ownership policy.",
                "No duplicate segment is created while stale owner exists.",
            ],
            Self::ConsumerCrashAndRestart => &[
                "Consumer cursor recovery is monotonic and non-decreasing.",
                "Producer gating remains bounded after restart.",
            ],
            Self::DiscoveryVisibilityLag => &[
                "Visibility transitions are logged and replayable.",
                "Delayed metadata never causes permanent readiness deadlock.",
            ],
            Self::ReadinessGateViolation => &[
                "Publisher blocks until minimum readiness threshold is met.",
                "Producer eventually transitions to ready state or deterministic timeout.",
            ],
        }
    }
}

/// Named lifecycle invariant that DST runs are expected to preserve.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleAssertion {
    /// Stable assertion identifier used in reports and docs.
    pub name: &'static str,
    /// Human-readable description of the lifecycle invariant.
    pub description: &'static str,
}

impl LifecycleAssertion {
    /// Return the canonical lifecycle assertions checked by the DST harness.
    pub fn all() -> &'static [LifecycleAssertion] {
        &[
            LifecycleAssertion {
                name: "shared_segment_ownership",
                description:
                    "Exactly one live owner process may create a segment; attachors read existing state.",
            },
            LifecycleAssertion {
                name: "cursor_owner_consistency",
                description:
                    "Owner and attach cursors observe the same underlying shared cursor state.",
            },
            LifecycleAssertion {
                name: "discovery_state_transition",
                description:
                    "Discovery counter and role readiness transitions are monotonic and shared-memory visible.",
            },
            LifecycleAssertion {
                name: "producer_consumer_sequence_monotonicity",
                description:
                    "Producer and consumer sequences are monotonic and never regress across process restarts.",
            },
            LifecycleAssertion {
                name: "graceful_recovery_timeout",
                description:
                    "Timeout and restart states are bounded; no silent infinite wait on lifecycle errors.",
            },
        ]
    }
}
