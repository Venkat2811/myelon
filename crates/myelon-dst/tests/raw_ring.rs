#![cfg(dst)]
//!
use disruptor_mp::dst::buggify::ScopedBuggify;
use disruptor_mp::dst::contract::FailureClass;
use myelon_dst::{
    BackendKind, DstConfig, DstProperty, DstRunner, DstRunnerError, OracleViolation,
    RawRingHarness, RequiredConsumerLivenessPolicy, TransportKind, WaitStrategyKind,
};
use std::ops::Deref;
use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

static RAW_RING_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct LockedHarness {
    _guard: MutexGuard<'static, ()>,
    inner: RawRingHarness,
}

impl Deref for LockedHarness {
    type Target = RawRingHarness;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

fn harness() -> LockedHarness {
    LockedHarness {
        _guard: RAW_RING_TEST_LOCK
            .lock()
            .expect("raw-ring dst test lock should not be poisoned"),
        inner: RawRingHarness::new(env!("CARGO_BIN_EXE_myelon-dst-runner-child"))
            .with_timeout(Duration::from_secs(75)),
    }
}

fn fuzz_config(seed: u64, nightly: bool) -> DstConfig {
    let config = DstConfig::raw_ring_from_seed(seed);
    let max_depth = if nightly { 4096 } else { 1024 };
    let max_messages = if nightly { 1024 } else { 256 };
    let min_messages = if nightly { 128 } else { 64 };
    let max_consumers = if nightly { 6 } else { 4 };
    let ring_depth = config.ring_depth.min(max_depth).max(256);
    let message_count = config.message_count.min(max_messages).max(min_messages);
    let consumer_count = config.consumer_count.min(max_consumers).max(1);
    config
        .with_ring_depth(ring_depth)
        .with_consumer_count(consumer_count)
        .with_message_count(message_count)
}

fn run_fuzz_seed(seed: u64, nightly: bool) {
    let _buggify = ScopedBuggify::new(seed);
    let config = fuzz_config(seed, nightly);
    let property = if config.consumer_count > 1 {
        DstProperty::BroadcastCompleteness
    } else {
        DstProperty::MessageIntegrity
    };
    let mut runner = DstRunner::with_config(config.clone());
    let report = runner
        .run_property(property, TransportKind::RawRing, &harness())
        .unwrap_or_else(|err| {
            panic!("raw-ring fuzz seed {seed:#x} failed with config {config:?}: {err:?}")
        });
    assert_eq!(
        report.producer.messages.len(),
        config.message_count as usize,
        "raw-ring fuzz seed {seed:#x} producer count mismatch"
    );
    for (index, consumer) in report.consumers.iter().enumerate() {
        assert_eq!(
            consumer.messages.len(),
            config.message_count as usize,
            "raw-ring fuzz seed {seed:#x} consumer {index} count mismatch"
        );
    }
}

fn run_failure_case(
    seed: u64,
    class: FailureClass,
    backend: BackendKind,
    consumer_count: usize,
    message_count: u64,
) -> myelon_dst::DstRunReport {
    let config = DstConfig::raw_ring_from_seed(seed)
        .with_backend(backend)
        .with_ring_depth(2048)
        .with_payload_size(128)
        .with_consumer_count(consumer_count)
        .with_message_count(message_count);
    let mut runner = DstRunner::with_config(config);
    runner
        .run_failure_class(class, TransportKind::RawRing, &harness())
        .unwrap_or_else(|err| {
            panic!(
                "{class:?} should pass for backend {backend:?} with {consumer_count} consumers: {err:?}"
            )
        })
}

fn run_wait_strategy_smoke(
    backend: BackendKind,
    wait_strategy: WaitStrategyKind,
) -> (myelon_dst::DstRunReport, Duration) {
    let config = DstConfig::raw_ring_from_seed(0x5EED_1705)
        .with_backend(backend)
        .with_ring_depth(2048)
        .with_payload_size(128)
        .with_consumer_count(1)
        .with_message_count(16384)
        .with_wait_strategy(wait_strategy);
    let mut runner = DstRunner::with_config(config);
    let started = Instant::now();
    let report = runner
        .run_wait_strategy_smoke(TransportKind::RawRing, &harness())
        .unwrap_or_else(|err| {
            panic!(
                "wait-strategy smoke should pass for backend {backend:?} strategy {wait_strategy:?}: {err:?}"
            )
        });
    (report, started.elapsed())
}

fn required_consumer_policy(shutdown_grace_ms: u64) -> RequiredConsumerLivenessPolicy {
    RequiredConsumerLivenessPolicy {
        startup_wait_ms: 200,
        progress_timeout_ms: 20,
        progress_check_interval_ms: 1,
        shutdown_grace_ms,
        consumer_kill_after_ms: 60,
        consumer_restart_delay_ms: 80,
        required_consumer_missing_slots: 0,
        restart_with_wrong_consumer_id: false,
    }
}

fn required_consumer_stress_config(seed: u64, backend: BackendKind) -> DstConfig {
    DstConfig::raw_ring_from_seed(seed)
        .with_backend(backend)
        .with_ring_depth(64)
        .with_payload_size(128)
        .with_consumer_count(2)
        .with_message_count(4096)
}

#[test]
fn dst_shm_raw_ring_message_integrity_1p1c() {
    let config = DstConfig::raw_ring_from_seed(0x1701)
        .with_backend(BackendKind::Shm)
        .with_ring_depth(2048)
        .with_payload_size(128)
        .with_consumer_count(1)
        .with_message_count(1024);
    let mut runner = DstRunner::with_config(config);
    let report = runner
        .run_property(
            DstProperty::MessageIntegrity,
            TransportKind::RawRing,
            &harness(),
        )
        .expect("shm raw ring property should pass");

    assert_eq!(report.producer.messages.len(), 1024);
    assert_eq!(report.consumers.len(), 1);
    assert_eq!(report.consumers[0].messages.len(), 1024);
}

#[test]
fn dst_mmap_raw_ring_message_integrity_1p1c() {
    let config = DstConfig::raw_ring_from_seed(0x1702)
        .with_backend(BackendKind::Mmap)
        .with_ring_depth(2048)
        .with_payload_size(256)
        .with_consumer_count(1)
        .with_message_count(1024);
    let mut runner = DstRunner::with_config(config);
    let report = runner
        .run_property(
            DstProperty::MessageIntegrity,
            TransportKind::RawRing,
            &harness(),
        )
        .expect("mmap raw ring property should pass");

    assert_eq!(report.producer.messages.len(), 1024);
    assert_eq!(report.consumers[0].messages.len(), 1024);
}

#[test]
fn dst_shm_raw_ring_broadcast_integrity_1p4c() {
    let config = DstConfig::raw_ring_from_seed(0x1703)
        .with_backend(BackendKind::Shm)
        .with_ring_depth(4096)
        .with_payload_size(128)
        .with_consumer_count(4)
        .with_message_count(2048);
    let mut runner = DstRunner::with_config(config);
    let report = runner
        .run_property(
            DstProperty::BroadcastCompleteness,
            TransportKind::RawRing,
            &harness(),
        )
        .expect("broadcast completeness should pass");

    assert_eq!(report.consumers.len(), 4);
    for consumer in &report.consumers {
        assert_eq!(consumer.messages.len(), 2048);
    }
}

#[test]
fn dst_raw_ring_same_seed_replays_same_oracle_state() {
    let config = DstConfig::raw_ring_from_seed(0x1704)
        .with_backend(BackendKind::Shm)
        .with_ring_depth(2048)
        .with_payload_size(512)
        .with_consumer_count(2)
        .with_message_count(1024);

    let mut first = DstRunner::with_config(config.clone());
    let first = first
        .run_property(
            DstProperty::MessageIntegrity,
            TransportKind::RawRing,
            &harness(),
        )
        .expect("first seeded run should pass");

    let mut second = DstRunner::with_config(config);
    let second = second
        .run_property(
            DstProperty::MessageIntegrity,
            TransportKind::RawRing,
            &harness(),
        )
        .expect("second seeded run should pass");

    assert_eq!(first.producer.messages, second.producer.messages);
    assert_eq!(
        first
            .consumers
            .iter()
            .map(|report| report.messages.clone())
            .collect::<Vec<_>>(),
        second
            .consumers
            .iter()
            .map(|report| report.messages.clone())
            .collect::<Vec<_>>()
    );
}

#[test]
fn dst_failure_class_producer_before_consumers_shm() {
    let report = run_failure_case(
        0x1705,
        FailureClass::ProducerBeforeConsumers,
        BackendKind::Shm,
        1,
        512,
    );
    assert_eq!(report.consumers[0].messages.len(), 512);
}

#[test]
fn dst_failure_class_producer_before_consumers_mmap() {
    let report = run_failure_case(
        0x0001_7051,
        FailureClass::ProducerBeforeConsumers,
        BackendKind::Mmap,
        1,
        512,
    );
    assert_eq!(report.consumers[0].messages.len(), 512);
    assert_eq!(
        report.failure_class,
        Some(FailureClass::ProducerBeforeConsumers)
    );
}

#[test]
fn dst_failure_class_producer_before_consumers_shm_broadcast() {
    let report = run_failure_case(
        0x0001_7052,
        FailureClass::ProducerBeforeConsumers,
        BackendKind::Shm,
        3,
        512,
    );
    assert_eq!(report.consumers.len(), 3);
    for consumer in &report.consumers {
        assert_eq!(consumer.messages.len(), 512);
    }
}

#[test]
fn dst_failure_class_late_consumer_attach_mmap() {
    let report = run_failure_case(
        0x1706,
        FailureClass::LateConsumerAttach,
        BackendKind::Mmap,
        1,
        512,
    );
    assert_eq!(report.consumers[0].messages.len(), 512);
    assert_eq!(report.failure_class, Some(FailureClass::LateConsumerAttach));
}

#[test]
fn dst_failure_class_late_consumer_attach_shm() {
    let report = run_failure_case(
        0x0001_7063,
        FailureClass::LateConsumerAttach,
        BackendKind::Shm,
        1,
        512,
    );
    assert_eq!(report.consumers[0].messages.len(), 512);
    assert_eq!(report.failure_class, Some(FailureClass::LateConsumerAttach));
}

#[test]
fn dst_failure_class_late_consumer_attach_shm_broadcast() {
    let report = run_failure_case(
        0x0001_7064,
        FailureClass::LateConsumerAttach,
        BackendKind::Shm,
        3,
        512,
    );
    assert_eq!(report.consumers.len(), 3);
    for consumer in &report.consumers {
        assert_eq!(consumer.messages.len(), 512);
    }
}

#[test]
fn dst_failure_class_create_attach_churn_shm() {
    let config = DstConfig::raw_ring_from_seed(0x0001_7061)
        .with_backend(BackendKind::Shm)
        .with_ring_depth(256)
        .with_payload_size(128)
        .with_consumer_count(2)
        .with_message_count(2048);
    let mut runner = DstRunner::with_config(config);
    let report = runner
        .run_failure_class(
            FailureClass::CreateAttachChurn,
            TransportKind::RawRing,
            &harness(),
        )
        .expect("create-attach-churn should pass");
    assert_eq!(report.failure_class, Some(FailureClass::CreateAttachChurn));
    assert_eq!(report.consumers.len(), 2);
    for consumer in &report.consumers {
        assert_eq!(consumer.messages.len(), 2048);
    }
}

#[test]
fn dst_failure_class_producer_crash_and_restart_shm() {
    let config = DstConfig::raw_ring_from_seed(0x0001_7062)
        .with_backend(BackendKind::Shm)
        .with_ring_depth(256)
        .with_payload_size(128)
        .with_consumer_count(1)
        .with_message_count(2048);
    let mut runner = DstRunner::with_config(config);
    let report = runner
        .run_failure_class(
            FailureClass::ProducerCrashAndRestart,
            TransportKind::RawRing,
            &harness(),
        )
        .expect("producer-crash-and-restart should pass");
    assert_eq!(
        report.failure_class,
        Some(FailureClass::ProducerCrashAndRestart)
    );
    assert_eq!(report.producer.messages.len(), 2048);
    assert_eq!(report.consumers[0].messages.len(), 2048);
    assert!(report
        .assertions
        .sometimes_satisfied("producer restart executed"));
    assert!(report
        .assertions
        .sometimes_satisfied("consumer restart executed"));
}

#[test]
fn dst_failure_class_discovery_visibility_lag_shm() {
    let config = DstConfig::raw_ring_from_seed(0x1707)
        .with_backend(BackendKind::Shm)
        .with_ring_depth(4096)
        .with_payload_size(128)
        .with_consumer_count(3)
        .with_message_count(1024);
    let mut runner = DstRunner::with_config(config);
    let report = runner
        .run_failure_class(
            FailureClass::DiscoveryVisibilityLag,
            TransportKind::RawRing,
            &harness(),
        )
        .expect("discovery-visibility-lag should pass");
    assert_eq!(report.consumers.len(), 3);
    for consumer in &report.consumers {
        assert_eq!(consumer.messages.len(), 1024);
        assert!(consumer.attached_after_ms <= 15_000);
    }
}

#[test]
fn dst_failure_class_consumer_crash_and_restart_shm() {
    let config = DstConfig::raw_ring_from_seed(0x1708)
        .with_backend(BackendKind::Shm)
        .with_ring_depth(256)
        .with_payload_size(128)
        .with_consumer_count(1)
        .with_message_count(2048);
    let mut runner = DstRunner::with_config(config);
    let report = runner
        .run_failure_class(
            FailureClass::ConsumerCrashAndRestart,
            TransportKind::RawRing,
            &harness(),
        )
        .expect("consumer-crash-and-restart should pass");
    assert_eq!(
        report.failure_class,
        Some(FailureClass::ConsumerCrashAndRestart)
    );
    assert_eq!(report.consumers[0].messages.len(), 2048);
    assert!(report
        .assertions
        .sometimes_satisfied("consumer restart executed"));
}

#[test]
fn dst_required_consumer_liveness_recovers_same_id_restart_shm() {
    let config = required_consumer_stress_config(0x0001_7082, BackendKind::Shm);
    let mut runner = DstRunner::with_config(config);
    let report = runner
        .run_failure_class_with_required_consumer_liveness(
            FailureClass::ConsumerCrashAndRestart,
            TransportKind::RawRing,
            &harness(),
            required_consumer_policy(1500),
        )
        .expect("same-id restart within grace should pass");
    assert_eq!(
        report.failure_class,
        Some(FailureClass::ConsumerCrashAndRestart)
    );
    for consumer in &report.consumers {
        assert_eq!(consumer.messages.len(), 4096);
    }
}

#[test]
fn dst_required_consumer_liveness_recovers_same_id_restart_mmap() {
    let config = required_consumer_stress_config(0x0001_7083, BackendKind::Mmap);
    let mut runner = DstRunner::with_config(config);
    let report = runner
        .run_failure_class_with_required_consumer_liveness(
            FailureClass::ConsumerCrashAndRestart,
            TransportKind::RawRing,
            &harness(),
            required_consumer_policy(1500),
        )
        .expect("same-id restart within grace should pass");
    assert_eq!(
        report.failure_class,
        Some(FailureClass::ConsumerCrashAndRestart)
    );
    for consumer in &report.consumers {
        assert_eq!(consumer.messages.len(), 4096);
    }
}

#[test]
fn dst_required_consumer_liveness_fails_late_restart_shm() {
    let config = required_consumer_stress_config(0x0001_7084, BackendKind::Shm);
    let mut runner = DstRunner::with_config(config);
    let mut policy = required_consumer_policy(20);
    policy.consumer_kill_after_ms = 200;
    policy.consumer_restart_delay_ms = 800;
    let error = runner
        .run_failure_class_with_required_consumer_liveness(
            FailureClass::ConsumerCrashAndRestart,
            TransportKind::RawRing,
            &harness(),
            policy,
        )
        .expect_err("late restart should fail after grace expires");

    match error {
        DstRunnerError::ChildFailed { stderr, .. } => {
            assert!(
                stderr.contains("Required consumer stall detected")
                    || stderr.contains("GracefulShutdownTriggered"),
                "stderr should include required-consumer shutdown context: {stderr}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn dst_required_consumer_liveness_fails_startup_when_required_consumer_never_appears() {
    let config = required_consumer_stress_config(0x0001_7085, BackendKind::Shm);
    let mut runner = DstRunner::with_config(config);
    let mut policy = required_consumer_policy(1500);
    policy.required_consumer_missing_slots = 1;
    let error = runner
        .run_property_with_required_consumer_liveness(
            DstProperty::MessageIntegrity,
            TransportKind::RawRing,
            &harness(),
            policy,
        )
        .expect_err("missing required consumer id should fail startup");

    match error {
        DstRunnerError::ChildFailed { stderr, .. } => {
            assert!(
                stderr.contains("StartupTimeout")
                    || stderr.contains("required consumers did not appear before startup timeout"),
                "stderr should include startup-timeout context: {stderr}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn dst_required_consumer_liveness_rejects_wrong_id_restart() {
    let config = required_consumer_stress_config(0x0001_7086, BackendKind::Shm);
    let mut runner = DstRunner::with_config(config);
    let mut policy = required_consumer_policy(1500);
    policy.restart_with_wrong_consumer_id = true;
    let error = runner
        .run_failure_class_with_required_consumer_liveness(
            FailureClass::ConsumerCrashAndRestart,
            TransportKind::RawRing,
            &harness(),
            policy,
        )
        .expect_err("restart under a different consumer id must not clear the stall");

    match error {
        DstRunnerError::ChildFailed { stderr, .. } => {
            assert!(
                stderr.contains("Required consumer stall detected")
                    || stderr.contains("GracefulShutdownTriggered"),
                "stderr should include required-consumer shutdown context: {stderr}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn dst_wait_strategy_sleep_shm_smoke_avoids_scheduler_floor() {
    let (report, elapsed) = run_wait_strategy_smoke(BackendKind::Shm, WaitStrategyKind::Sleep);
    assert_eq!(report.consumers[0].messages.len(), 16384);
    assert!(
        elapsed < Duration::from_secs(3),
        "sleep wait smoke regressed to scheduler-floor behavior: elapsed={elapsed:?}"
    );
}

#[test]
fn dst_wait_strategy_block_shm_smoke_avoids_scheduler_floor() {
    let (report, elapsed) = run_wait_strategy_smoke(BackendKind::Shm, WaitStrategyKind::Block);
    assert_eq!(report.consumers[0].messages.len(), 16384);
    assert!(
        elapsed < Duration::from_secs(3),
        "block wait smoke regressed to scheduler-floor behavior: elapsed={elapsed:?}"
    );
}

#[test]
fn dst_wait_strategy_sleep_mmap_smoke_avoids_scheduler_floor() {
    let (report, elapsed) = run_wait_strategy_smoke(BackendKind::Mmap, WaitStrategyKind::Sleep);
    assert_eq!(report.consumers[0].messages.len(), 16384);
    assert!(
        elapsed < Duration::from_secs(3),
        "mmap sleep wait smoke regressed to scheduler-floor behavior: elapsed={elapsed:?}"
    );
}

#[test]
fn dst_wait_strategy_block_mmap_smoke_avoids_scheduler_floor() {
    let (report, elapsed) = run_wait_strategy_smoke(BackendKind::Mmap, WaitStrategyKind::Block);
    assert_eq!(report.consumers[0].messages.len(), 16384);
    assert!(
        elapsed < Duration::from_secs(3),
        "mmap block wait smoke regressed to scheduler-floor behavior: elapsed={elapsed:?}"
    );
}

#[test]
fn dst_failure_class_readiness_gate_violation_shm() {
    let config = DstConfig::raw_ring_from_seed(0x0001_7081)
        .with_backend(BackendKind::Shm)
        .with_ring_depth(256)
        .with_payload_size(128)
        .with_consumer_count(1)
        .with_message_count(1024);
    let mut runner = DstRunner::with_config(config);
    let report = runner
        .run_failure_class(
            FailureClass::ReadinessGateViolation,
            TransportKind::RawRing,
            &harness(),
        )
        .expect("readiness-gate-violation should pass");
    assert_eq!(
        report.failure_class,
        Some(FailureClass::ReadinessGateViolation)
    );
    assert_eq!(report.consumers[0].messages.len(), 1024);
}

#[test]
fn dst_fuzz_raw_ring_seed_smoke_matrix() {
    for seed in 0x1800..0x1808 {
        let config = DstConfig::raw_ring_from_seed(seed)
            .with_backend(if seed % 2 == 0 {
                BackendKind::Shm
            } else {
                BackendKind::Mmap
            })
            .with_ring_depth(2048)
            .with_message_count(512);
        let mut runner = DstRunner::with_config(config);
        let report = runner
            .run_property(
                DstProperty::MessageIntegrity,
                TransportKind::RawRing,
                &harness(),
            )
            .expect("seed smoke run should pass");
        assert_eq!(report.producer.messages.len(), 512);
    }
}

#[test]
#[ignore]
fn dst_fuzz_raw_ring_ci_seed_matrix() {
    for seed in 0x1900..0x1964 {
        if seed % 10 == 0 {
            eprintln!("raw-ring ci fuzz seed={seed:#x}");
        }
        run_fuzz_seed(seed, false);
    }
}

#[test]
#[ignore]
fn dst_fuzz_raw_ring_nightly_seed_matrix() {
    for seed in 0x1a00..0x1de8 {
        if seed % 50 == 0 {
            eprintln!("raw-ring nightly fuzz seed={seed:#x}");
        }
        run_fuzz_seed(seed, true);
    }
}

#[test]
fn dst_assertions_record_ring_wrap_when_depth_is_small() {
    let config = DstConfig::raw_ring_from_seed(0x1711)
        .with_backend(BackendKind::Shm)
        .with_ring_depth(128)
        .with_payload_size(64)
        .with_consumer_count(1)
        .with_message_count(512);
    let mut runner = DstRunner::with_config(config);
    let report = runner
        .run_property(
            DstProperty::MessageIntegrity,
            TransportKind::RawRing,
            &harness(),
        )
        .expect("small-ring message-integrity run should pass");

    assert!(report
        .assertions
        .sometimes_satisfied("ring buffer wraps around"));
}

#[test]
fn dst_oracle_detects_injected_payload_corruption_shm() {
    let config = DstConfig::raw_ring_from_seed(0x1712)
        .with_backend(BackendKind::Shm)
        .with_ring_depth(256)
        .with_payload_size(128)
        .with_consumer_count(1)
        .with_message_count(256);
    let mut runner = DstRunner::with_config(config).with_corruption_probe(73);
    let err = runner
        .run_property(
            DstProperty::MessageIntegrity,
            TransportKind::RawRing,
            &harness(),
        )
        .expect_err("oracle should detect injected payload corruption");

    match err {
        DstRunnerError::Oracle(violations) => {
            assert!(violations
                .iter()
                .any(|violation| matches!(violation, OracleViolation::PayloadMismatch { .. })));
        }
        other => panic!("expected oracle violation, got {other:?}"),
    }
}
