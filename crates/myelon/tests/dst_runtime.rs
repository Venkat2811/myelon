use dst_fixtures::{
    dst_contract::{
        DeterministicFaultInjector, FailureClass, FailureKind, FailureStep, LifecycleAssertion,
        ProcessRole, ReplayMismatch, ReplayValidator, SchedulerAction, StartupSchedule,
        TraceArtifact, TraceStatus,
    },
    dst_runtime::{replay_trace, DstRuntime, DstRuntimeState},
};
use myelon::inference::{FixedTopology, WorkerCount};

#[test]
fn test_myelon_seeded_runtime_supports_lifecycle_transitions() {
    let mut runtime = DstRuntime::new("myelon-runtime-01", "seeded-startup", 0xBEEF, 5_000);

    runtime
        .start()
        .expect("start should transition initialized -> running");
    runtime
        .step(ProcessRole::Producer, SchedulerAction::Spawn, 1)
        .expect("step should work in running state");

    runtime.pause().expect("pause should work while running");
    assert_eq!(runtime.state, DstRuntimeState::Paused);

    runtime.resume().expect("resume should work while paused");
    assert_eq!(runtime.state, DstRuntimeState::Running);

    runtime
        .crash(ProcessRole::Producer, "induced fault")
        .expect("crash should transition running -> crashed");
    assert_eq!(runtime.state, DstRuntimeState::Crashed);

    runtime
        .restart(ProcessRole::Producer, "warm restart")
        .expect("restart should transition crashed -> restarting");
    assert_eq!(runtime.state, DstRuntimeState::Restarting);
}

#[test]
fn test_myelon_runtime_replay_verifies_seeded_trace_determinism() {
    let mut left = DstRuntime::new("myelon-runtime-replay", "seeded-replay", 123, 10_000);
    left.start().expect("start");
    left.step(ProcessRole::Producer, SchedulerAction::Spawn, 1)
        .expect("step");
    left.step(
        ProcessRole::Producer,
        SchedulerAction::PublishBatch { events: 8 },
        2,
    )
    .expect("step");
    left.complete();

    let mut right = DstRuntime::new("myelon-runtime-replay", "seeded-replay", 123, 10_000);
    right.start().expect("start");
    right
        .step(ProcessRole::Producer, SchedulerAction::Spawn, 1)
        .expect("step");
    right
        .step(
            ProcessRole::Producer,
            SchedulerAction::PublishBatch { events: 8 },
            2,
        )
        .expect("step");
    right.complete();

    let left_trace = left.into_trace();
    let right_trace = right.into_trace();

    replay_trace(&left_trace, &right_trace).expect("identical seeded traces should replay");
}

#[test]
fn test_myelon_runtime_budget_is_enforced() {
    let runtime = DstRuntime::new("myelon-runtime-budget", "seeded-budget", 99, 500);
    assert_eq!(
        runtime.enforce_budget(500),
        Ok(()),
        "budget at limit should pass"
    );
    assert!(
        runtime.enforce_budget(501).is_err(),
        "budget over limit should fail"
    );
}

#[test]
fn test_myelon_topology_informs_runtime_steps() {
    let topology = FixedTopology::new("myelon-dst".to_string(), 64, WorkerCount::Two);
    let consumer_count = topology.worker_count().as_usize() as u32;
    let schedule = StartupSchedule::from_seed("myelon-runtime-smoke", 0x1234, consumer_count, 6);
    let mut runtime = DstRuntime::new("myelon-runtime-schedule", "runtime-smoke", 0x1234, 10_000);
    runtime.start().expect("start");

    for step in &schedule.steps {
        if let ProcessRole::Consumer { index } = step.role {
            assert!(
                (index as usize) < topology.worker_count().as_usize(),
                "consumer index must fit the fixed topology"
            );
            let expected_id = topology
                .worker_consumer_id(index as usize)
                .expect("consumer id should exist");
            assert!(expected_id.starts_with("myelon-dst_wk_"));
        }

        runtime
            .step(step.role.clone(), step.action.clone(), step.attempt)
            .expect("schedule-derived step should execute in deterministic runtime");
    }

    runtime.complete();
    assert_eq!(runtime.state, DstRuntimeState::Completed);
    assert!(runtime.trace.events.len() > 1);
    assert_eq!(runtime.timeline.len(), runtime.trace.events.len());
}

#[test]
fn test_myelon_runtime_target_exercises_shared_dst_api_surface() {
    let schedule = StartupSchedule::from_seed("myelon-runtime-contract", 0xD05A, 2, 5);
    assert_eq!(schedule.plan_id, "myelon-runtime-contract");
    assert_eq!(schedule.seed, 0xD05A);
    assert!(schedule.is_deterministic());

    let injector = DeterministicFaultInjector::from_seed(0xBEEF, 2, 12);
    assert_eq!(injector.seed, 0xBEEF);
    let _ = injector.at_step(0);

    let mut trace = TraceArtifact::new("myelon-runtime-api", "contract", 55);
    trace.push(
        ProcessRole::Producer,
        SchedulerAction::Attach,
        TraceStatus::Planned,
        "myelon-runtime-step-1",
    );
    trace.set_metadata("seed", "55");
    let wire = trace
        .to_json()
        .expect("runtime contract trace should serialize");
    let parsed =
        TraceArtifact::from_json(&wire).expect("runtime contract trace should deserialize");
    ReplayValidator::validate(&trace, &parsed).expect("traces should match");

    replay_trace(&trace, &parsed).expect("traces should replay exactly");

    let failure = FailureKind::KillProcess;
    match failure {
        FailureKind::DelayAttachMs(ms) => {
            assert!(ms > 0);
        }
        FailureKind::KillProcess | FailureKind::RestartProcess => {}
        FailureKind::DropConsumerAttach { consumer_index } => {
            assert_ne!(consumer_index, u32::MAX);
        }
        FailureKind::Noop => {}
    }

    let _ = FailureStep {
        step: 1,
        role: ProcessRole::Consumer { index: 0 },
        kind: failure,
    };

    assert!(!FailureClass::all().is_empty());
    for failure_class in FailureClass::all() {
        assert!(!failure_class.preconditions().is_empty());
        assert!(!failure_class.postconditions().is_empty());
    }

    assert!(!LifecycleAssertion::all().is_empty());
    for assertion in LifecycleAssertion::all() {
        assert!(!assertion.name.is_empty());
        assert!(!assertion.description.is_empty());
    }

    let mismatch = ReplayMismatch::EventCount {
        expected: 5,
        actual: 4,
    };
    match mismatch {
        ReplayMismatch::RunIdentity { left, right } => {
            assert_ne!(left.1, right.1);
        }
        ReplayMismatch::EventCount { expected, actual } => {
            assert!(actual < expected);
        }
        ReplayMismatch::EventContent {
            step: _,
            expected,
            actual,
        } => {
            assert_ne!(expected, actual);
        }
    }
}
