//! Mapping between high-value deterministic-surface failure classes and existing
//! multiprocess integration tests.
//!
//! This mapping is the first concrete artifact that ensures every DST class has
//! at least one executable test path in the current suite and gives a stable set
//! of anchors for future seed/replay expansion.

use crate::dst_contract::FailureClass;

/// Coverage metadata for a failure class in the existing `tests/*` suite.
#[derive(Debug, Clone, Copy)]
pub struct FailureClassCoverage {
    pub failure_class: FailureClass,
    pub test_names: &'static [&'static str],
    pub assertions: &'static [&'static str],
    pub class_description: &'static str,
}

impl FailureClassCoverage {
    pub const fn new(
        failure_class: FailureClass,
        test_names: &'static [&'static str],
        assertions: &'static [&'static str],
        class_description: &'static str,
    ) -> Self {
        Self {
            failure_class,
            test_names,
            assertions,
            class_description,
        }
    }
}

const PRODUCER_FIRST_CASES: &[&str] = &["true_multiprocess_spsc_wraparound_and_startup_race"];
const LATE_CONSUMER_ATTACH_CASES: &[&str] = &["true_multiprocess_spsc_wraparound_and_startup_race"];
const CREATE_ATTACH_CHURN_CASES: &[&str] = &[
    "true_multiprocess_startup_determinism_loop",
    "cursor_new_or_attach_must_attach_existing_segment",
    "ring_buffer_layout_rejects_version_regression",
];
const PRODUCER_CRASH_RECOVERY_CASES: &[&str] = &[
    "true_multiprocess_startup_determinism_loop",
    "true_multiprocess_64kb_buffer_deadlock_at_65536_events",
    "true_multiprocess_deadlock_boundary_conditions",
];
const CONSUMER_CRASH_RECOVERY_CASES: &[&str] = &[
    "true_multiprocess_spmc_two_consumers_with_slow_consumer",
    "dropping_attached_cursor_must_not_disrupt_owner",
    "cursor_recreate_recovers_after_simulated_crash",
];
const DISCOVERY_LAG_CASES: &[&str] = &[
    "true_multiprocess_startup_determinism_loop",
    "true_multiprocess_concurrent_spsc",
];
const READINESS_GATE_CASES: &[&str] = &[
    "true_multiprocess_startup_determinism_loop",
    "true_multiprocess_spsc_wraparound_and_startup_race",
    "true_multiprocess_backpressure_spsc",
];

/// Canonical mapping of failure classes to current executable test coverage.
pub static FAILURE_CLASS_COVERAGE: &[FailureClassCoverage] = &[
    FailureClassCoverage::new(
        FailureClass::ProducerBeforeConsumers,
        PRODUCER_FIRST_CASES,
        &["Consumer-not-attached startup does not drop producer publish guarantees"],
        "Producer starts before consumers and remains safe under readiness behavior.",
    ),
    FailureClassCoverage::new(
        FailureClass::LateConsumerAttach,
        LATE_CONSUMER_ATTACH_CASES,
        &["Late consumer attach loops should resolve within bounded timeout"],
        "Consumer attachment delayed until after producer activity must replay latest state.",
    ),
    FailureClassCoverage::new(
        FailureClass::CreateAttachChurn,
        CREATE_ATTACH_CHURN_CASES,
        &[
            "Segment creation/attach retries are explicit and bounded",
            "Stale ownership windows are observable",
        ],
        "Create/attach races are exercised by repeated startup and layout recovery runs.",
    ),
    FailureClassCoverage::new(
        FailureClass::ProducerCrashAndRestart,
        PRODUCER_CRASH_RECOVERY_CASES,
        &[
            "Producer-side lifecycle transitions remain bounded after interruption",
            "Recovery path can re-run deterministically from fresh attach",
        ],
        "Producer interruption class validated through deterministic startup/restart-oriented cases.",
    ),
    FailureClassCoverage::new(
        FailureClass::ConsumerCrashAndRestart,
        CONSUMER_CRASH_RECOVERY_CASES,
        &[
            "Consumer restart path should continue monotonic sequence consumption",
            "Attachment churn does not regress segment metadata",
        ],
        "Consumer interruption class covered by multi-consumer and cleanup drop scenarios.",
    ),
    FailureClassCoverage::new(
        FailureClass::DiscoveryVisibilityLag,
        DISCOVERY_LAG_CASES,
        &[
            "Readiness and discovery transitions are eventually consistent",
            "Visibility lag does not stall indefinitely",
        ],
        "Discovery/metadata lag is modelled via deterministic startup ordering and concurrent timing.",
    ),
    FailureClassCoverage::new(
        FailureClass::ReadinessGateViolation,
        READINESS_GATE_CASES,
        &[
            "Producer waits for required consumer readiness before unbounded overwrite",
            "Readiness timeout path is explicit and deterministic",
        ],
        "Producer gating behavior under mixed slow/ready consumers is covered.",
    ),
];

/// Returns all canonical mappings for a class if present.
pub fn coverage_for(class: FailureClass) -> Option<&'static FailureClassCoverage> {
    let mut index = 0;
    while index < FAILURE_CLASS_COVERAGE.len() {
        let entry = &FAILURE_CLASS_COVERAGE[index];
        if entry.failure_class == class {
            return Some(entry);
        }
        index += 1;
    }
    None
}
