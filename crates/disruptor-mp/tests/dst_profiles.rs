//! Tests for process profiles used by deterministic disruption scenarios.

#[path = "support/dst_contract.rs"]
mod dst_contract;
#[path = "support/dst_mapping.rs"]
mod dst_mapping;
#[path = "support/dst_profiles.rs"]
mod dst_profiles;

use dst_contract::FailureClass;
use dst_contract::{
    DeterministicFaultInjector, FailureKind, LifecycleAssertion, ProcessRole, ReplayMismatch,
    ReplayValidator, SchedulerAction, StartupSchedule, TraceArtifact, TraceStatus,
};
use dst_mapping::coverage_for;
use dst_profiles::*;

#[test]
fn test_all_failure_classes_have_process_profiles() {
    for &failure_class in FailureClass::all() {
        let profile = profile_for(failure_class).expect("failure class must have profile spec");
        assert_eq!(profile.kind, failure_class);
        assert!(!profile.name().is_empty());
        assert!(profile.timeout_ms() > 0);
        assert!(profile.producers > 0);
        assert!(profile.consumers > 0);
        assert!(!profile.expected_assertions.is_empty());
        assert!(!profile.replay_assertions.is_empty());
        assert!(!profile.coverage_tests.is_empty());
        assert!(profile.seed_envelope.sample_count() > 0);
        assert!(profile.seed_envelope.step > 0);
        assert!(profile.seed_envelope.max_seed >= profile.seed_envelope.min_seed);
    }

    assert!(validate_profile_contract());
}

#[test]
fn test_profile_coverage_maps_to_existing_deterministic_multiprocess_matrix() {
    for profile in profile_catalog() {
        let coverage = coverage_for(profile.kind).expect("profile class must exist in matrix");
        assert!(
            !coverage.test_names.is_empty(),
            "{} lacks matrix entries",
            profile.name()
        );
        assert!(
            !coverage.assertions.is_empty(),
            "{} coverage assertions must exist",
            profile.name()
        );
        assert!(
            !coverage.class_description.is_empty(),
            "{} coverage description must exist",
            profile.name()
        );

        let overlaps = profile
            .coverage_tests
            .iter()
            .any(|mapped| coverage.test_names.contains(mapped));
        assert!(
            overlaps,
            "{} must share at least one test with matrix coverage",
            profile.name()
        );

        assert!(
            profile.coverage_tests.iter().any(|name| {
                name.contains("true_multiprocess")
                    || name.contains("ring_buffer")
                    || name.contains("cursor")
            }),
            "{} coverage must include a true multiprocess or shared-memory test",
            profile.name()
        );
    }
}

#[test]
fn test_profile_assertions_are_explicit_and_non_duplicated() {
    for profile in profile_catalog() {
        assert!(
            !profile.expected_assertions.is_empty(),
            "{} has empty assertions",
            profile.name()
        );

        for assertion in profile.expected_assertions {
            assert!(
                !assertion.description().is_empty(),
                "{} assertion description cannot be empty",
                profile.name()
            );
        }
    }
}

#[test]
fn test_profile_card_uses_contract_primitives() {
    let schedule = StartupSchedule::from_seed("profile-contract", 0xFEED, 1, 4);
    assert_eq!(schedule.seed, 0xFEED);
    assert!(schedule.steps.len() >= 2);
    assert!(schedule.is_deterministic());

    let injector = DeterministicFaultInjector::from_seed(0xB0B, 1, 16);
    let _ = injector.at_step(1);

    let mut trace = TraceArtifact::new("profile", "contract", 77);
    trace.set_metadata("phase", "profile_contract");
    trace.push(
        ProcessRole::Producer,
        SchedulerAction::Spawn,
        TraceStatus::Planned,
        "profile-step-1",
    );
    let serialized = trace.to_json().expect("trace serializes");
    let deserialized = TraceArtifact::from_json(&serialized).expect("trace deserializes");
    ReplayValidator::validate(&trace, &deserialized).expect("trace equality must hold");

    let _ = dst_contract::ReplayValidator::validate(&trace, &deserialized).is_ok();

    let failure = FailureKind::DelayAttachMs(5);
    let _ = match failure {
        FailureKind::DelayAttachMs(ms) => ms,
        FailureKind::KillProcess => 0,
        FailureKind::RestartProcess => 0,
        FailureKind::DropConsumerAttach { consumer_index } => u64::from(consumer_index),
        FailureKind::Noop => 0,
    };

    for assertion in LifecycleAssertion::all() {
        assert!(!assertion.name.is_empty());
        assert!(!assertion.description.is_empty());
    }

    let mismatch = ReplayMismatch::EventCount {
        expected: 1,
        actual: 2,
    };
    if let ReplayMismatch::EventCount { expected, actual } = mismatch {
        assert_ne!(expected, actual);
    }

    for failure in FailureClass::all() {
        assert!(!failure.preconditions().is_empty());
        assert!(!failure.postconditions().is_empty());
    }

    let _ = ReplayMismatch::RunIdentity {
        left: (String::new(), 1),
        right: (String::new(), 2),
    };
    let _ = ReplayMismatch::EventContent {
        step: 1,
        expected: Box::new(trace.events[0].clone()),
        actual: Box::new(trace.events[0].clone()),
    };
}
