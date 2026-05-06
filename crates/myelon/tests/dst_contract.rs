use dst_fixtures::{
    dst_contract::{
        DeterministicFaultInjector, FailureClass, ProcessRole, ReplayMismatch, ReplayValidator,
        SchedulerAction, StartupSchedule, TraceArtifact,
    },
    dst_mapping::coverage_for,
    dst_profiles::{profile_catalog, profile_for},
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
fn test_shared_dst_contract_definitions_are_reproducible() {
    let baseline = StartupSchedule::from_seed("myelon-mp", 0xCAFE_BABE_1234_0001, 2, 8);
    let repeat = StartupSchedule::from_seed("myelon-mp", 0xCAFE_BABE_1234_0001, 2, 8);

    assert_eq!(baseline.plan_id, repeat.plan_id);
    assert_eq!(baseline.seed, repeat.seed);
    assert_eq!(baseline.steps, repeat.steps);
    assert!(baseline.is_deterministic());
}

#[test]
fn test_myelon_failure_injector_and_replay_contract() {
    let left = DeterministicFaultInjector::from_seed(0xBEEF_DEAD_1111_0001, 3, 64);
    let right = DeterministicFaultInjector::from_seed(0xBEEF_DEAD_1111_0001, 3, 64);

    assert_eq!(left.seed, right.seed);
    assert_eq!(left.inject_at, right.inject_at);
    assert_eq!(left.at_step(0).len(), right.at_step(0).len());

    let mut trace = TraceArtifact::new("myelon-trace", "contract-smoke", 7);
    trace.push(
        ProcessRole::Producer,
        SchedulerAction::Spawn,
        dst_fixtures::dst_contract::TraceStatus::Success,
        "role=producer",
    );
    trace.push(
        ProcessRole::Producer,
        SchedulerAction::Start,
        dst_fixtures::dst_contract::TraceStatus::Success,
        "role=producer",
    );

    let serial = trace.to_json().expect("trace should serialize");
    let deserialized = TraceArtifact::from_json(&serial).expect("trace should deserialize");

    ReplayValidator::validate(&trace, &deserialized).expect("trace should replay");

    let mut mutated = deserialized;
    mutated.seed = 8;
    let mismatch = ReplayValidator::validate(&trace, &mutated)
        .expect_err("seed-only mutation should mismatch");
    match mismatch {
        ReplayMismatch::RunIdentity { left, right } => {
            assert_ne!(left.1, right.1);
        }
        _ => panic!("unexpected mismatch type for seed drift"),
    }
}

#[test]
fn test_myelon_dst_profiles_map_to_coverage_and_shared_tests() {
    for profile in profile_catalog() {
        let class = profile.kind;
        let coverage = coverage_for(class).unwrap_or_else(|| {
            panic!("failure class {class:?} has no mapped shared coverage entry");
        });

        assert!(
            profile_for(class).is_some(),
            "profile_catalog should include every covered failure class {class:?}"
        );
        assert!(
            !profile.expected_assertions.is_empty(),
            "profile for {class:?} must define expected assertions"
        );
        assert!(
            coverage.test_names.iter().any(|name| has_myelon_test(name)),
            "coverage for {class:?} must include an existing shared DST test name"
        );
    }
}

#[test]
fn test_myelon_disruptor_mapped_failure_classes_are_sane() {
    for class in FailureClass::all() {
        let coverage =
            coverage_for(*class).unwrap_or_else(|| panic!("coverage missing for {class:?}"));
        assert!(
            !coverage.class_description.is_empty(),
            "coverage description missing for {class:?}"
        );
        assert!(
            coverage
                .assertions
                .iter()
                .all(|assertion| !assertion.is_empty()),
            "coverage assertion labels must be non-empty for {class:?}"
        );
    }
}
