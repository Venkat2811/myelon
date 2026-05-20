//! Process and failure profile definitions for deterministic multiprocess disruption tests.
//!
//! Profiles are the bridge between `FailureClass` and concrete runtime orchestration
//! parameters. They keep high-value invariants explicit and testable.

use std::time::Duration;

use super::contract::FailureClass;

/// High-level assertions each profile must satisfy in one or more test variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileAssertion {
    /// The scenario must not deadlock.
    NoDeadlock,
    /// Shutdown or restart must finish within the configured budget.
    BoundedShutdown,
    /// Producer and consumer cursor values must never regress.
    MonotonicCursor,
    /// Readers must not observe stale or overwritten data.
    NoStaleOverwrite,
    /// Delayed metadata visibility must eventually converge.
    DelayedVisibilityConverged,
    /// Progress must resume after an injected restart.
    RecoveryProgressesAfterRestart,
}

impl ProfileAssertion {
    /// Human-readable explanation of the assertion.
    pub fn description(self) -> &'static str {
        match self {
            Self::NoDeadlock => "Profile run must not deadlock",
            Self::BoundedShutdown => "Shutdown and recovery paths must complete within budget",
            Self::MonotonicCursor => "Cursor sequence values are monotonic",
            Self::NoStaleOverwrite => "No stale data is consumed after ownership changes",
            Self::DelayedVisibilityConverged => "Delayed state visibility eventually converges",
            Self::RecoveryProgressesAfterRestart => "Restart transitions resume progress",
        }
    }
}

/// Seed envelope used when sampling a profile across multiple runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProfileSeedEnvelope {
    /// Lowest seed value included in the envelope.
    pub min_seed: u64,
    /// Highest seed value included in the envelope.
    pub max_seed: u64,
    /// Step size between sampled seeds.
    pub step: u64,
    /// Short note describing the purpose of the envelope.
    pub note: &'static str,
}

impl ProfileSeedEnvelope {
    /// Create a new seed envelope.
    pub const fn new(min_seed: u64, max_seed: u64, step: u64, note: &'static str) -> Self {
        Self {
            min_seed,
            max_seed,
            step,
            note,
        }
    }

    /// Return how many samples the envelope describes.
    pub const fn sample_count(&self) -> u64 {
        if self.max_seed < self.min_seed || self.step == 0 {
            0
        } else {
            ((self.max_seed - self.min_seed) / self.step) + 1
        }
    }
}

/// Runtime-level profile parameters used by custom DST test runners.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProfileSpec {
    /// Failure class represented by the profile.
    pub kind: FailureClass,
    /// Producer process count expected by the scenario.
    pub producers: u32,
    /// Consumer process count expected by the scenario.
    pub consumers: u32,
    /// End-to-end timeout budget for the profile.
    pub timeout: Duration,
    /// Assertions expected to hold during the primary run.
    pub expected_assertions: &'static [ProfileAssertion],
    /// Assertions expected to hold when replaying or reproducing the run.
    pub replay_assertions: &'static [ProfileAssertion],
    /// Existing tests that cover this profile today.
    pub coverage_tests: &'static [&'static str],
    /// Seed sampling policy for the profile.
    pub seed_envelope: ProfileSeedEnvelope,
}

impl ProfileSpec {
    /// Stable profile name used in reports and artifacts.
    pub fn name(&self) -> &'static str {
        match self.kind {
            FailureClass::ProducerBeforeConsumers => "producer_before_consumers",
            FailureClass::LateConsumerAttach => "late_consumer_attach",
            FailureClass::CreateAttachChurn => "create_attach_churn",
            FailureClass::ProducerCrashAndRestart => "producer_crash_and_restart",
            FailureClass::ConsumerCrashAndRestart => "consumer_crash_and_restart",
            FailureClass::DiscoveryVisibilityLag => "discovery_visibility_lag",
            FailureClass::ReadinessGateViolation => "readiness_gate_violation",
        }
    }

    /// Timeout budget expressed in milliseconds.
    pub fn timeout_ms(&self) -> u64 {
        self.timeout.as_millis().try_into().unwrap_or(u64::MAX)
    }
}

const PROD_BEFORE_ASSERTIONS: &[ProfileAssertion] = &[
    ProfileAssertion::NoDeadlock,
    ProfileAssertion::MonotonicCursor,
    ProfileAssertion::BoundedShutdown,
    ProfileAssertion::NoStaleOverwrite,
];

const LATE_CONSUMER_ASSERTIONS: &[ProfileAssertion] = &[
    ProfileAssertion::NoDeadlock,
    ProfileAssertion::MonotonicCursor,
    ProfileAssertion::NoStaleOverwrite,
];

const CREATE_ATTACH_ASSERTIONS: &[ProfileAssertion] = &[
    ProfileAssertion::NoDeadlock,
    ProfileAssertion::BoundedShutdown,
    ProfileAssertion::RecoveryProgressesAfterRestart,
];

const PRODUCER_RESTART_ASSERTIONS: &[ProfileAssertion] = &[
    ProfileAssertion::NoDeadlock,
    ProfileAssertion::RecoveryProgressesAfterRestart,
    ProfileAssertion::MonotonicCursor,
    ProfileAssertion::BoundedShutdown,
];

const CONSUMER_RESTART_ASSERTIONS: &[ProfileAssertion] = &[
    ProfileAssertion::NoDeadlock,
    ProfileAssertion::RecoveryProgressesAfterRestart,
    ProfileAssertion::MonotonicCursor,
    ProfileAssertion::NoStaleOverwrite,
];

const VISIBILITY_ASSERTIONS: &[ProfileAssertion] = &[
    ProfileAssertion::NoDeadlock,
    ProfileAssertion::DelayedVisibilityConverged,
    ProfileAssertion::BoundedShutdown,
];

const READINESS_ASSERTIONS: &[ProfileAssertion] = &[
    ProfileAssertion::NoDeadlock,
    ProfileAssertion::BoundedShutdown,
    ProfileAssertion::NoStaleOverwrite,
];

const PROD_BEFORE_TESTS: &[&str] = &[
    "dst_failure_class_producer_before_consumers_shm",
    "dst_failure_class_producer_before_consumers_mmap",
    "dst_failure_class_producer_before_consumers_shm_broadcast",
];
const LATE_CONSUMER_TESTS: &[&str] = &[
    "dst_failure_class_late_consumer_attach_mmap",
    "dst_failure_class_late_consumer_attach_shm",
    "dst_failure_class_late_consumer_attach_shm_broadcast",
];
const CREATE_ATTACH_TESTS: &[&str] = &[
    "true_multiprocess_startup_determinism_loop",
    "ring_buffer_layout_rejects_version_regression",
    "cursor_new_or_attach_must_attach_existing_segment",
];
const PRODUCER_RESTART_TESTS: &[&str] = &[
    "true_multiprocess_startup_determinism_loop",
    "true_multiprocess_deadlock_boundary_conditions",
    "true_multiprocess_64kb_buffer_deadlock_at_65536_events",
];
const CONSUMER_RESTART_TESTS: &[&str] = &[
    "true_multiprocess_spmc_two_consumers_with_slow_consumer",
    "cursor_recreate_recovers_after_simulated_crash",
    "dropping_attached_cursor_must_not_disrupt_owner",
];
const VISIBILITY_TESTS: &[&str] = &[
    "true_multiprocess_startup_determinism_loop",
    "true_multiprocess_concurrent_spsc",
    "ring_buffer_layout_rejects_element_size_mismatch",
];
const READINESS_TESTS: &[&str] = &[
    "true_multiprocess_startup_determinism_loop",
    "true_multiprocess_spsc_wraparound_and_startup_race",
    "true_multiprocess_backpressure_spsc",
];

const QUICK_SEEDS: ProfileSeedEnvelope =
    ProfileSeedEnvelope::new(0x1001, 0x1001, 1, "single run smoke envelope");
const STARTUP_STABILITY_SEEDS: ProfileSeedEnvelope = ProfileSeedEnvelope::new(
    0x2001,
    0x2010,
    3,
    "multi-variant startup and attach ordering",
);
const RESTART_SEEDS: ProfileSeedEnvelope =
    ProfileSeedEnvelope::new(0x3001, 0x3010, 3, "restart fault injection envelope");
const VISIBILITY_SEEDS: ProfileSeedEnvelope =
    ProfileSeedEnvelope::new(0x4001, 0x4010, 2, "visibility lag simulation envelope");
const READINESS_SEEDS: ProfileSeedEnvelope =
    ProfileSeedEnvelope::new(0x5001, 0x5008, 1, "readiness timeout envelope");

const PRODUCER_BEFORE_CONSUMER: ProfileSpec = ProfileSpec {
    kind: FailureClass::ProducerBeforeConsumers,
    producers: 1,
    consumers: 1,
    timeout: Duration::from_millis(800),
    expected_assertions: PROD_BEFORE_ASSERTIONS,
    replay_assertions: &[
        ProfileAssertion::NoDeadlock,
        ProfileAssertion::MonotonicCursor,
        ProfileAssertion::NoStaleOverwrite,
    ],
    coverage_tests: PROD_BEFORE_TESTS,
    seed_envelope: QUICK_SEEDS,
};

const LATE_CONSUMER_ATTACH: ProfileSpec = ProfileSpec {
    kind: FailureClass::LateConsumerAttach,
    producers: 1,
    consumers: 2,
    timeout: Duration::from_millis(1_200),
    expected_assertions: LATE_CONSUMER_ASSERTIONS,
    replay_assertions: &[
        ProfileAssertion::NoDeadlock,
        ProfileAssertion::MonotonicCursor,
    ],
    coverage_tests: LATE_CONSUMER_TESTS,
    seed_envelope: STARTUP_STABILITY_SEEDS,
};

const CREATE_ATTACH_CHURN: ProfileSpec = ProfileSpec {
    kind: FailureClass::CreateAttachChurn,
    producers: 1,
    consumers: 2,
    timeout: Duration::from_millis(1_200),
    expected_assertions: CREATE_ATTACH_ASSERTIONS,
    replay_assertions: &[
        ProfileAssertion::NoDeadlock,
        ProfileAssertion::BoundedShutdown,
        ProfileAssertion::RecoveryProgressesAfterRestart,
    ],
    coverage_tests: CREATE_ATTACH_TESTS,
    seed_envelope: STARTUP_STABILITY_SEEDS,
};

const PRODUCER_CRASH_RESTART: ProfileSpec = ProfileSpec {
    kind: FailureClass::ProducerCrashAndRestart,
    producers: 1,
    consumers: 1,
    timeout: Duration::from_millis(1_500),
    expected_assertions: PRODUCER_RESTART_ASSERTIONS,
    replay_assertions: &[
        ProfileAssertion::NoDeadlock,
        ProfileAssertion::RecoveryProgressesAfterRestart,
        ProfileAssertion::MonotonicCursor,
    ],
    coverage_tests: PRODUCER_RESTART_TESTS,
    seed_envelope: RESTART_SEEDS,
};

const CONSUMER_CRASH_RESTART: ProfileSpec = ProfileSpec {
    kind: FailureClass::ConsumerCrashAndRestart,
    producers: 1,
    consumers: 2,
    timeout: Duration::from_millis(1_500),
    expected_assertions: CONSUMER_RESTART_ASSERTIONS,
    replay_assertions: &[
        ProfileAssertion::NoDeadlock,
        ProfileAssertion::MonotonicCursor,
        ProfileAssertion::NoStaleOverwrite,
    ],
    coverage_tests: CONSUMER_RESTART_TESTS,
    seed_envelope: RESTART_SEEDS,
};

const DISCOVERY_VISIBILITY_LAG: ProfileSpec = ProfileSpec {
    kind: FailureClass::DiscoveryVisibilityLag,
    producers: 1,
    consumers: 3,
    timeout: Duration::from_millis(1_000),
    expected_assertions: VISIBILITY_ASSERTIONS,
    replay_assertions: &[
        ProfileAssertion::DelayedVisibilityConverged,
        ProfileAssertion::BoundedShutdown,
    ],
    coverage_tests: VISIBILITY_TESTS,
    seed_envelope: VISIBILITY_SEEDS,
};

const READINESS_GATE_VIOLATION: ProfileSpec = ProfileSpec {
    kind: FailureClass::ReadinessGateViolation,
    producers: 1,
    consumers: 2,
    timeout: Duration::from_millis(1_000),
    expected_assertions: READINESS_ASSERTIONS,
    replay_assertions: &[
        ProfileAssertion::BoundedShutdown,
        ProfileAssertion::NoStaleOverwrite,
    ],
    coverage_tests: READINESS_TESTS,
    seed_envelope: READINESS_SEEDS,
};

fn specs() -> &'static [ProfileSpec] {
    &[
        PRODUCER_BEFORE_CONSUMER,
        LATE_CONSUMER_ATTACH,
        CREATE_ATTACH_CHURN,
        PRODUCER_CRASH_RESTART,
        CONSUMER_CRASH_RESTART,
        DISCOVERY_VISIBILITY_LAG,
        READINESS_GATE_VIOLATION,
    ]
}

/// Returns the complete process-fault profile catalog.
pub fn profile_catalog() -> &'static [ProfileSpec] {
    specs()
}

/// Return profile spec for a class, when one is registered.
pub fn profile_for(class: FailureClass) -> Option<&'static ProfileSpec> {
    specs().iter().find(|spec| spec.kind == class)
}

/// Assert that every supported failure class has profile data and runtime budget.
pub fn validate_profile_contract() -> bool {
    let mut mapped = 0usize;
    for class in FailureClass::all() {
        if profile_for(*class).is_some() {
            mapped += 1;
        }
    }
    mapped == FailureClass::all().len()
}
