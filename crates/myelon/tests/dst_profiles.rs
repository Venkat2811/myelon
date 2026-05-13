#![cfg(dst)]
use disruptor_mp::dst::{
    contract::{
        DeterministicFaultInjector, FailureClass, FailureKind, LifecycleAssertion, ProcessRole,
        ReplayMismatch, ReplayValidator, SchedulerAction, StartupSchedule, TraceArtifact,
        TraceStatus,
    },
    mapping::coverage_for,
    profiles::{profile_catalog, profile_for, validate_profile_contract},
};

const KNOWN_DST_COVERAGE_TESTS: &[&str] = &[
    "true_multiprocess_spsc_wraparound_and_startup_race",
    "true_multiprocess_startup_determinism_loop",
    "true_multiprocess_spmc_two_consumers_with_slow_consumer",
    "true_multiprocess_concurrent_spsc",
    "true_multiprocess_backpressure_spsc",
    "dst_failure_class_producer_before_consumers_shm",
    "dst_failure_class_producer_before_consumers_mmap",
    "dst_failure_class_producer_before_consumers_shm_broadcast",
    "dst_failure_class_late_consumer_attach_mmap",
    "dst_failure_class_late_consumer_attach_shm",
    "dst_failure_class_late_consumer_attach_shm_broadcast",
];

fn has_myelon_test(name: &str) -> bool {
    KNOWN_DST_COVERAGE_TESTS.contains(&name)
}

#[test]
fn test_all_failure_classes_have_myelon_dst_profiles() {
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
fn test_myelon_dst_profiles_overlap_with_shared_coverage_and_local_matrix() {
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

        let overlaps_shared_matrix = profile
            .coverage_tests
            .iter()
            .any(|mapped| coverage.test_names.contains(mapped));
        assert!(
            overlaps_shared_matrix,
            "{} must share at least one test with the shared coverage matrix",
            profile.name()
        );

        let overlaps_myelon_matrix = profile
            .coverage_tests
            .iter()
            .any(|name| has_myelon_test(name));
        assert!(
            overlaps_myelon_matrix,
            "{} must overlap at least one known shared DST coverage test",
            profile.name()
        );
    }
}

#[test]
fn test_myelon_profile_assertions_are_explicit_and_non_empty() {
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
fn test_myelon_profile_contract_uses_shared_dst_primitives() {
    let schedule = StartupSchedule::from_seed("myelon-profile-contract", 0xFEED, 1, 4);
    assert_eq!(schedule.seed, 0xFEED);
    assert!(schedule.steps.len() >= 2);
    assert!(schedule.is_deterministic());

    let injector = DeterministicFaultInjector::from_seed(0xB0B, 1, 16);
    let _ = injector.at_step(1);

    let mut trace = TraceArtifact::new("myelon-profile", "contract", 77);
    trace.set_metadata("phase", "profile_contract");
    trace.push(
        ProcessRole::Producer,
        SchedulerAction::Spawn,
        TraceStatus::Planned,
        "myelon-profile-step-1",
    );
    let serialized = trace.to_json().expect("trace serializes");
    let deserialized = TraceArtifact::from_json(&serialized).expect("trace deserializes");
    ReplayValidator::validate(&trace, &deserialized).expect("trace equality must hold");

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
}
