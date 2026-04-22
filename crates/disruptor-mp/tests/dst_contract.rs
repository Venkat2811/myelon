//! Tests for deterministic DST contract primitives used by multiprocess regression tests.

#[path = "support/dst_contract.rs"]
mod dst_contract;
#[path = "support/dst_mapping.rs"]
mod dst_mapping;

use dst_contract::*;
use dst_mapping::*;

#[test]
fn test_deterministic_startup_schedule_reproducible() {
    let left = StartupSchedule::from_seed("start-01", 0xDEAD_BEEF_0011_2233, 3, 10);
    let right = StartupSchedule::from_seed("start-01", 0xDEAD_BEEF_0011_2233, 3, 10);
    assert_eq!(left.plan_id, right.plan_id);
    assert_eq!(left.seed, right.seed);
    assert_eq!(left.steps, right.steps);
    assert!(left.is_deterministic());
}

#[test]
fn test_deterministic_startup_schedule_varies_with_seed_and_profile() {
    let left = StartupSchedule::from_seed("start-01", 0x1111_2222_3333_4444, 2, 8);
    let right = StartupSchedule::from_seed("start-01", 0x5555_6666_7777_8888, 2, 8);
    assert_ne!(left.steps, right.steps);
}

#[test]
fn test_fault_injector_is_seed_stable() {
    let left = DeterministicFaultInjector::from_seed(0xCAFE_1234_9876_0001, 2, 64);
    let right = DeterministicFaultInjector::from_seed(0xCAFE_1234_9876_0001, 2, 64);
    assert_eq!(left.seed, right.seed);
    assert_eq!(left.inject_at, right.inject_at);
    assert_eq!(left.at_step(0).len(), right.at_step(0).len());
}

#[test]
fn test_trace_artifact_roundtrip_and_replay_validation() {
    let mut trace = TraceArtifact::new("dst-trace-01", "seeded-startup", 42);
    trace.push(
        ProcessRole::Orchestrator,
        SchedulerAction::Spawn,
        TraceStatus::Planned,
        "segment=contract-seed-01",
    );
    trace.set_metadata("phase", "spawn");
    trace.push(
        ProcessRole::Producer,
        SchedulerAction::Attach,
        TraceStatus::Success,
        "segment=contract-seed-01",
    );
    trace.push(
        ProcessRole::Producer,
        SchedulerAction::PublishBatch { events: 4 },
        TraceStatus::Success,
        "contract-seed-01",
    );

    let wire = trace.to_json().expect("trace should serialize");
    let parsed = TraceArtifact::from_json(&wire).expect("trace should deserialize");
    assert_eq!(trace, parsed);

    let mut replay = parsed.clone();
    replay.seed = 43;
    let mismatch = ReplayValidator::validate(&trace, &replay).expect_err("seeds differ");
    match mismatch {
        ReplayMismatch::RunIdentity {
            left: (_, left_seed),
            right: (_, right_seed),
        } => {
            assert_ne!(left_seed, right_seed);
        }
        _ => panic!("unexpected mismatch type"),
    }
}

#[test]
fn test_replay_validator_flags_event_count_and_content_mismatch() {
    let mut expected = TraceArtifact::new("dst-replay-01", "seeded-startup", 7);
    expected.push(
        ProcessRole::Producer,
        SchedulerAction::Spawn,
        TraceStatus::Planned,
        "segment=seg-a",
    );
    expected.push(
        ProcessRole::Producer,
        SchedulerAction::Start,
        TraceStatus::Success,
        "segment=seg-a",
    );

    let mut actual = expected.clone();
    actual.push(
        ProcessRole::Consumer { index: 0 },
        SchedulerAction::Attach,
        TraceStatus::Success,
        "segment=seg-a",
    );

    let error =
        ReplayValidator::validate(&expected, &actual).expect_err("extra event should mismatch");
    match error {
        ReplayMismatch::EventCount { expected, actual } => {
            assert!(expected < actual);
        }
        other => panic!("unexpected mismatch type: {other:?}"),
    }
}

#[test]
fn test_failure_class_contracts_define_pre_and_post_conditions() {
    assert!(!FailureClass::all().is_empty());

    for failure in FailureClass::all() {
        assert!(
            !failure.preconditions().is_empty(),
            "failure class {failure:?} must define at least one precondition"
        );
        assert!(
            !failure.postconditions().is_empty(),
            "failure class {failure:?} must define at least one postcondition"
        );
    }
}

#[test]
fn test_lifecycle_assertion_catalog_is_complete() {
    let catalog = LifecycleAssertion::all();
    assert!(
        catalog.len() >= 4,
        "lifecycle assertions must include shared ownership + recovery classes"
    );

    for entry in catalog {
        assert!(!entry.name.is_empty());
        assert!(!entry.description.is_empty());
    }
}

#[test]
fn test_dst_failure_classes_are_mapped_to_multiprocess_tests() {
    for failure in FailureClass::all() {
        let coverage = coverage_for(*failure).unwrap_or_else(|| {
            panic!("failure class {failure:?} must be mapped to at least one multiprocess test");
        });

        assert!(
            !coverage.test_names.is_empty(),
            "failure class {failure:?} has no test names mapped"
        );

        for assertion in coverage.assertions {
            assert!(
                !assertion.is_empty(),
                "failure class {failure:?} has an empty assertion"
            );
        }
    }
}

#[test]
fn test_dst_multiprocess_coverage_matrix_contains_expected_entries() {
    let expected_classes = [
        FailureClass::ProducerBeforeConsumers,
        FailureClass::LateConsumerAttach,
        FailureClass::CreateAttachChurn,
        FailureClass::ProducerCrashAndRestart,
        FailureClass::ConsumerCrashAndRestart,
        FailureClass::DiscoveryVisibilityLag,
        FailureClass::ReadinessGateViolation,
    ];

    assert_eq!(expected_classes.len(), FailureClass::all().len());

    for class in expected_classes {
        let Some(entry) = coverage_for(class) else {
            panic!("expected class {class:?} missing from coverage map");
        };

        assert!(
            entry.test_names.iter().any(|name| {
                name.contains("true_multiprocess")
                    || name.contains("ring_buffer")
                    || name.contains("dst_failure_class_")
                    || name.contains("cursor_")
            }),
            "class {class:?} should map to an implemented multiprocess regression test"
        );

        assert!(
            !entry.class_description.is_empty(),
            "class {class:?} is missing coverage description"
        );
    }
}
