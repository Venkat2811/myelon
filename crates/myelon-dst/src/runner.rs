use crate::runner_config::{BackendKind, DstConfig, WaitStrategyKind};
use crate::runner_fault::FaultInjector;
use crate::runner_oracle::MessageOracle;
use crate::runner_report::{ChildReport, DstProperty, DstRunReport, TransportKind};
use crate::runner_verify::verify_raw_ring_broadcast;
use disruptor_mp::dst::assertions::AssertionLog;
use disruptor_mp::dst::contract::{
    FailureClass, ProcessRole, SchedulerAction, TraceArtifact, TraceStatus,
};
use serde::de::DeserializeOwned;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct RawRingHarness {
    pub executable: PathBuf,
    pub timeout: Duration,
}

impl RawRingHarness {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
            timeout: Duration::from_secs(30),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[derive(Debug, Error)]
pub enum DstRunnerError {
    #[error("unsupported transport for this runner slice: {0:?}")]
    UnsupportedTransport(TransportKind),
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    #[error("failed to spawn {role}: {source}")]
    Spawn {
        role: String,
        #[source]
        source: std::io::Error,
    },
    #[error("child {role} timed out after {timeout:?}\nstdout:\n{stdout}\nstderr:\n{stderr}")]
    Timeout {
        role: String,
        timeout: Duration,
        stdout: String,
        stderr: String,
    },
    #[error("child {role} failed with status {status:?}\nstdout:\n{stdout}\nstderr:\n{stderr}")]
    ChildFailed {
        role: String,
        status: Option<i32>,
        stdout: String,
        stderr: String,
    },
    #[error("failed to decode {role} report: {source}\nstdout:\n{stdout}\nstderr:\n{stderr}")]
    ChildProtocol {
        role: String,
        stdout: String,
        stderr: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("oracle verification failed: {0:?}")]
    Oracle(Vec<crate::runner_oracle::OracleViolation>),
}

#[derive(Debug)]
pub struct DstRunner {
    pub seed: u64,
    pub config: DstConfig,
    pub oracle: MessageOracle,
    pub fault_injector: FaultInjector,
    pub trace: TraceArtifact,
    corruption_at_sequence: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct RawRingExecutionPolicy {
    spawn_consumers_first: bool,
    wait_for_consumers_ready: bool,
    startup_delay_ms: u64,
    consumer_spawn_stagger_ms: u64,
    producer_hold_ms: u64,
    publish_pause_every: usize,
    publish_pause_micros: u64,
    injected_fault: Option<RawRingInjectedFault>,
    required_consumer_liveness: Option<RequiredConsumerLivenessPolicy>,
}

#[derive(Debug, Clone, Copy)]
pub struct RequiredConsumerLivenessPolicy {
    pub startup_wait_ms: u64,
    pub progress_timeout_ms: u64,
    pub progress_check_interval_ms: u64,
    pub shutdown_grace_ms: u64,
    pub consumer_kill_after_ms: u64,
    pub consumer_restart_delay_ms: u64,
    pub required_consumer_missing_slots: usize,
    pub restart_with_wrong_consumer_id: bool,
}

#[derive(Debug)]
struct SpawnedChild {
    child: Child,
    report_path: PathBuf,
    checkpoint_path: PathBuf,
}

#[derive(Debug, Clone, Copy)]
enum RawRingInjectedFault {
    ProducerKillAndRestart {
        after_ms: u64,
        restart_delay_ms: u64,
    },
    ConsumerKillAndRestart {
        consumer_index: usize,
        after_ms: u64,
        restart_delay_ms: u64,
    },
    ConsumerSuspendAndResume {
        consumer_index: usize,
        after_ms: u64,
        suspend_ms: u64,
    },
}

impl RawRingExecutionPolicy {
    fn steady_state() -> Self {
        Self {
            spawn_consumers_first: true,
            wait_for_consumers_ready: true,
            startup_delay_ms: 80,
            consumer_spawn_stagger_ms: 10,
            producer_hold_ms: 0,
            publish_pause_every: 0,
            publish_pause_micros: 0,
            injected_fault: None,
            required_consumer_liveness: None,
        }
    }
}

impl DstRunner {
    pub fn from_seed(seed: u64) -> Self {
        let config = DstConfig::raw_ring_from_seed(seed);
        Self::with_config(config)
    }

    pub fn with_config(config: DstConfig) -> Self {
        let trace = TraceArtifact::new(
            format!("dst-run-{:x}", config.seed),
            "myelon_dst/raw_ring",
            config.seed,
        );
        let oracle = MessageOracle::with_broadcast_consumers(config.consumer_count);
        let fault_injector = FaultInjector::none(config.seed);

        Self {
            seed: config.seed,
            config,
            oracle,
            fault_injector,
            trace,
            corruption_at_sequence: None,
        }
    }

    pub fn with_corruption_probe(mut self, corrupt_at_sequence: u64) -> Self {
        self.corruption_at_sequence = Some(corrupt_at_sequence);
        self
    }

    pub fn run_property(
        &mut self,
        property: DstProperty,
        transport: TransportKind,
        harness: &RawRingHarness,
    ) -> Result<DstRunReport, DstRunnerError> {
        match transport {
            TransportKind::RawRing => self.run_raw_ring_case(
                property,
                None,
                harness,
                RawRingExecutionPolicy::steady_state(),
            ),
            other => Err(DstRunnerError::UnsupportedTransport(other)),
        }
    }

    pub fn run_property_with_required_consumer_liveness(
        &mut self,
        property: DstProperty,
        transport: TransportKind,
        harness: &RawRingHarness,
        required_consumer_liveness: RequiredConsumerLivenessPolicy,
    ) -> Result<DstRunReport, DstRunnerError> {
        let mut policy = RawRingExecutionPolicy::steady_state();
        policy.required_consumer_liveness = Some(required_consumer_liveness);

        match transport {
            TransportKind::RawRing => self.run_raw_ring_case(property, None, harness, policy),
            other => Err(DstRunnerError::UnsupportedTransport(other)),
        }
    }

    pub fn run_failure_class(
        &mut self,
        class: FailureClass,
        transport: TransportKind,
        harness: &RawRingHarness,
    ) -> Result<DstRunReport, DstRunnerError> {
        let policy = match class {
            FailureClass::ProducerBeforeConsumers => RawRingExecutionPolicy {
                spawn_consumers_first: false,
                wait_for_consumers_ready: false,
                startup_delay_ms: 140,
                consumer_spawn_stagger_ms: 10,
                producer_hold_ms: 400,
                publish_pause_every: 0,
                publish_pause_micros: 0,
                injected_fault: None,
                required_consumer_liveness: None,
            },
            FailureClass::LateConsumerAttach => RawRingExecutionPolicy {
                spawn_consumers_first: false,
                wait_for_consumers_ready: false,
                startup_delay_ms: 60,
                consumer_spawn_stagger_ms: 10,
                producer_hold_ms: 250,
                publish_pause_every: 16,
                publish_pause_micros: 750,
                injected_fault: None,
                required_consumer_liveness: None,
            },
            FailureClass::CreateAttachChurn => RawRingExecutionPolicy {
                spawn_consumers_first: true,
                wait_for_consumers_ready: true,
                startup_delay_ms: 80,
                consumer_spawn_stagger_ms: 10,
                producer_hold_ms: 250,
                publish_pause_every: 8,
                publish_pause_micros: 1000,
                injected_fault: Some(RawRingInjectedFault::ConsumerSuspendAndResume {
                    consumer_index: 0,
                    after_ms: 40,
                    suspend_ms: 120,
                }),
                required_consumer_liveness: None,
            },
            FailureClass::ProducerCrashAndRestart => RawRingExecutionPolicy {
                spawn_consumers_first: true,
                wait_for_consumers_ready: true,
                startup_delay_ms: 80,
                consumer_spawn_stagger_ms: 10,
                producer_hold_ms: 200,
                publish_pause_every: 8,
                publish_pause_micros: 1000,
                injected_fault: Some(RawRingInjectedFault::ProducerKillAndRestart {
                    after_ms: 60,
                    restart_delay_ms: 80,
                }),
                required_consumer_liveness: None,
            },
            FailureClass::DiscoveryVisibilityLag => RawRingExecutionPolicy {
                spawn_consumers_first: false,
                wait_for_consumers_ready: false,
                startup_delay_ms: 40,
                consumer_spawn_stagger_ms: 50,
                producer_hold_ms: 300,
                publish_pause_every: 16,
                publish_pause_micros: 1000,
                injected_fault: None,
                required_consumer_liveness: None,
            },
            FailureClass::ConsumerCrashAndRestart => RawRingExecutionPolicy {
                spawn_consumers_first: true,
                wait_for_consumers_ready: true,
                startup_delay_ms: 80,
                consumer_spawn_stagger_ms: 10,
                producer_hold_ms: 250,
                publish_pause_every: 8,
                publish_pause_micros: 1000,
                injected_fault: Some(RawRingInjectedFault::ConsumerKillAndRestart {
                    consumer_index: 0,
                    after_ms: 60,
                    restart_delay_ms: 80,
                }),
                required_consumer_liveness: None,
            },
            FailureClass::ReadinessGateViolation => RawRingExecutionPolicy {
                spawn_consumers_first: false,
                wait_for_consumers_ready: true,
                startup_delay_ms: 200,
                consumer_spawn_stagger_ms: 10,
                producer_hold_ms: 250,
                publish_pause_every: 0,
                publish_pause_micros: 0,
                injected_fault: None,
                required_consumer_liveness: None,
            },
            _ => {
                return Err(DstRunnerError::InvalidConfig(format!(
                    "failure class {class:?} is not implemented in the first raw-ring slice"
                )))
            }
        };

        match transport {
            TransportKind::RawRing => {
                self.run_raw_ring_case(DstProperty::MessageIntegrity, Some(class), harness, policy)
            }
            other => Err(DstRunnerError::UnsupportedTransport(other)),
        }
    }

    pub fn run_wait_strategy_smoke(
        &mut self,
        transport: TransportKind,
        harness: &RawRingHarness,
    ) -> Result<DstRunReport, DstRunnerError> {
        let policy = RawRingExecutionPolicy {
            spawn_consumers_first: false,
            wait_for_consumers_ready: false,
            startup_delay_ms: 60,
            consumer_spawn_stagger_ms: 10,
            producer_hold_ms: 250,
            publish_pause_every: 1,
            publish_pause_micros: 25,
            injected_fault: None,
            required_consumer_liveness: None,
        };

        match transport {
            TransportKind::RawRing => self.run_raw_ring_case_with_overrides(
                DstProperty::MessageIntegrity,
                None,
                harness,
                policy,
                RawRingChildOverrides {
                    checkpoint_every: Some(0),
                    ..RawRingChildOverrides::default()
                },
            ),
            other => Err(DstRunnerError::UnsupportedTransport(other)),
        }
    }

    pub fn run_failure_class_with_required_consumer_liveness(
        &mut self,
        class: FailureClass,
        transport: TransportKind,
        harness: &RawRingHarness,
        required_consumer_liveness: RequiredConsumerLivenessPolicy,
    ) -> Result<DstRunReport, DstRunnerError> {
        let mut policy = match class {
            FailureClass::ProducerBeforeConsumers => RawRingExecutionPolicy {
                spawn_consumers_first: false,
                wait_for_consumers_ready: false,
                startup_delay_ms: 140,
                consumer_spawn_stagger_ms: 10,
                producer_hold_ms: 400,
                publish_pause_every: 0,
                publish_pause_micros: 0,
                injected_fault: None,
                required_consumer_liveness: None,
            },
            FailureClass::LateConsumerAttach => RawRingExecutionPolicy {
                spawn_consumers_first: false,
                wait_for_consumers_ready: false,
                startup_delay_ms: 60,
                consumer_spawn_stagger_ms: 10,
                producer_hold_ms: 250,
                publish_pause_every: 16,
                publish_pause_micros: 750,
                injected_fault: None,
                required_consumer_liveness: None,
            },
            FailureClass::CreateAttachChurn => RawRingExecutionPolicy {
                spawn_consumers_first: true,
                wait_for_consumers_ready: true,
                startup_delay_ms: 80,
                consumer_spawn_stagger_ms: 10,
                producer_hold_ms: 250,
                publish_pause_every: 8,
                publish_pause_micros: 1000,
                injected_fault: Some(RawRingInjectedFault::ConsumerSuspendAndResume {
                    consumer_index: 0,
                    after_ms: 40,
                    suspend_ms: 120,
                }),
                required_consumer_liveness: None,
            },
            FailureClass::ProducerCrashAndRestart => RawRingExecutionPolicy {
                spawn_consumers_first: true,
                wait_for_consumers_ready: true,
                startup_delay_ms: 80,
                consumer_spawn_stagger_ms: 10,
                producer_hold_ms: 200,
                publish_pause_every: 8,
                publish_pause_micros: 1000,
                injected_fault: Some(RawRingInjectedFault::ProducerKillAndRestart {
                    after_ms: 60,
                    restart_delay_ms: 80,
                }),
                required_consumer_liveness: None,
            },
            FailureClass::DiscoveryVisibilityLag => RawRingExecutionPolicy {
                spawn_consumers_first: false,
                wait_for_consumers_ready: false,
                startup_delay_ms: 40,
                consumer_spawn_stagger_ms: 50,
                producer_hold_ms: 300,
                publish_pause_every: 16,
                publish_pause_micros: 1000,
                injected_fault: None,
                required_consumer_liveness: None,
            },
            FailureClass::ConsumerCrashAndRestart => RawRingExecutionPolicy {
                spawn_consumers_first: true,
                wait_for_consumers_ready: true,
                startup_delay_ms: 80,
                consumer_spawn_stagger_ms: 10,
                producer_hold_ms: 250,
                publish_pause_every: 8,
                publish_pause_micros: 1000,
                injected_fault: Some(RawRingInjectedFault::ConsumerKillAndRestart {
                    consumer_index: 0,
                    after_ms: required_consumer_liveness.consumer_kill_after_ms,
                    restart_delay_ms: required_consumer_liveness.consumer_restart_delay_ms,
                }),
                required_consumer_liveness: None,
            },
            FailureClass::ReadinessGateViolation => RawRingExecutionPolicy {
                spawn_consumers_first: false,
                wait_for_consumers_ready: true,
                startup_delay_ms: 200,
                consumer_spawn_stagger_ms: 10,
                producer_hold_ms: 250,
                publish_pause_every: 0,
                publish_pause_micros: 0,
                injected_fault: None,
                required_consumer_liveness: None,
            },
            _ => {
                return Err(DstRunnerError::InvalidConfig(format!(
                    "failure class {class:?} is not implemented in the first raw-ring slice"
                )))
            }
        };
        policy.required_consumer_liveness = Some(required_consumer_liveness);

        match transport {
            TransportKind::RawRing => {
                self.run_raw_ring_case(DstProperty::MessageIntegrity, Some(class), harness, policy)
            }
            other => Err(DstRunnerError::UnsupportedTransport(other)),
        }
    }

    fn run_raw_ring_case(
        &mut self,
        property: DstProperty,
        failure_class: Option<FailureClass>,
        harness: &RawRingHarness,
        policy: RawRingExecutionPolicy,
    ) -> Result<DstRunReport, DstRunnerError> {
        self.run_raw_ring_case_with_overrides(
            property,
            failure_class,
            harness,
            policy,
            RawRingChildOverrides::default(),
        )
    }

    fn run_raw_ring_case_with_overrides(
        &mut self,
        property: DstProperty,
        failure_class: Option<FailureClass>,
        harness: &RawRingHarness,
        policy: RawRingExecutionPolicy,
        base_overrides: RawRingChildOverrides,
    ) -> Result<DstRunReport, DstRunnerError> {
        if self.config.consumer_count == 0 {
            return Err(DstRunnerError::InvalidConfig(
                "consumer_count must be > 0".into(),
            ));
        }
        if self.config.message_count == 0 {
            return Err(DstRunnerError::InvalidConfig(
                "message_count must be > 0".into(),
            ));
        }

        let effective_ring_depth = match failure_class {
            Some(FailureClass::ConsumerCrashAndRestart)
            | Some(FailureClass::ProducerCrashAndRestart)
            | Some(FailureClass::CreateAttachChurn)
            | Some(FailureClass::ReadinessGateViolation) => self.config.ring_depth,
            Some(_) => self
                .config
                .ring_depth
                .max((self.config.message_count as usize).next_power_of_two()),
            None => self.config.ring_depth,
        };

        let run_root = unique_run_root("myelon_dst");
        fs::create_dir_all(&run_root).map_err(|err| {
            DstRunnerError::InvalidConfig(format!("failed to create run root: {err}"))
        })?;
        let segment = match self.config.backend {
            BackendKind::Shm => unique_shm_segment("d"),
            BackendKind::Mmap => unique_segment("dst"),
        };
        let consumer_prefix = format!("DSTC_{:x}", self.seed & 0xffff);
        let producer_report_path = run_root.join(format!("producer_producer_{segment}.json"));
        let mut consumers: Vec<SpawnedChild> = Vec::with_capacity(self.config.consumer_count);
        let mut partial_consumer_reports: Vec<Option<ChildReport>> =
            vec![None; self.config.consumer_count];
        let mut restarted_consumers = vec![false; self.config.consumer_count];
        let mut partial_producer_report: Option<ChildReport> = None;
        let mut restarted_producer = false;
        let mut assertions = AssertionLog::default();
        assertions.assert_reachable("raw_ring_case_started");

        self.trace.push(
            ProcessRole::Orchestrator,
            SchedulerAction::Spawn,
            TraceStatus::Planned,
            format!(
                "transport={:?} backend={:?}",
                TransportKind::RawRing,
                self.config.backend
            ),
        );

        let spawn_consumers = |children: &mut Vec<SpawnedChild>,
                               runner: &mut DstRunner|
         -> Result<(), DstRunnerError> {
            for index in 0..runner.config.consumer_count {
                let child = runner.spawn_raw_ring_child(
                    harness,
                    &run_root,
                    &segment,
                    "consumer",
                    effective_ring_depth,
                    Some(index),
                    &consumer_prefix,
                    policy,
                    &producer_report_path,
                    RawRingChildOverrides {
                        allow_corruption_validation: runner.corruption_at_sequence.is_some(),
                        ..base_overrides
                    },
                )?;
                runner.trace.push(
                    ProcessRole::Consumer {
                        index: index as u32,
                    },
                    SchedulerAction::Spawn,
                    TraceStatus::Success,
                    format!("backend={:?}", runner.config.backend),
                );
                children.push(child);
                thread::sleep(Duration::from_millis(policy.consumer_spawn_stagger_ms));
            }
            Ok(())
        };

        let mut producer;
        if policy.spawn_consumers_first {
            spawn_consumers(&mut consumers, self)?;
            thread::sleep(Duration::from_millis(policy.startup_delay_ms));
            producer = self.spawn_raw_ring_child(
                harness,
                &run_root,
                &segment,
                "producer",
                effective_ring_depth,
                None,
                &consumer_prefix,
                policy,
                &producer_report_path,
                RawRingChildOverrides {
                    corrupt_at_sequence: self.corruption_at_sequence,
                    ..base_overrides
                },
            )?;
        } else {
            producer = self.spawn_raw_ring_child(
                harness,
                &run_root,
                &segment,
                "producer",
                effective_ring_depth,
                None,
                &consumer_prefix,
                policy,
                &producer_report_path,
                RawRingChildOverrides {
                    corrupt_at_sequence: self.corruption_at_sequence,
                    ..base_overrides
                },
            )?;
            thread::sleep(Duration::from_millis(policy.startup_delay_ms));
            spawn_consumers(&mut consumers, self)?;
        }

        self.trace.push(
            ProcessRole::Producer,
            SchedulerAction::Spawn,
            TraceStatus::Success,
            format!("backend={:?}", self.config.backend),
        );

        self.apply_runtime_faults(
            harness,
            &run_root,
            &segment,
            effective_ring_depth,
            &consumer_prefix,
            policy,
            &mut producer,
            &mut consumers,
            &mut partial_producer_report,
            &mut partial_consumer_reports,
            &mut restarted_producer,
            &mut restarted_consumers,
            &producer_report_path,
        )?;

        let producer_suffix: ChildReport =
            self.wait_and_decode_child(producer, harness.timeout, "producer")?;
        let producer_report = if restarted_producer {
            concat_child_reports(partial_producer_report.take(), producer_suffix)
        } else {
            producer_suffix
        };
        self.trace.push(
            ProcessRole::Producer,
            SchedulerAction::PublishBatch {
                events: producer_report.messages.len() as u32,
            },
            TraceStatus::Success,
            format!("checksum_total={}", producer_report.checksum_total),
        );

        let mut consumer_reports = Vec::with_capacity(self.config.consumer_count);
        for (index, child) in consumers.into_iter().enumerate() {
            let mut report: ChildReport =
                self.wait_and_decode_child(child, harness.timeout, &format!("consumer-{index}"))?;
            if restarted_consumers[index] {
                report = merge_child_reports(
                    &producer_report,
                    partial_consumer_reports[index].take(),
                    report,
                );
            }
            self.trace.push(
                ProcessRole::Consumer {
                    index: index as u32,
                },
                SchedulerAction::ConsumeBatch {
                    events: report.messages.len() as u32,
                },
                TraceStatus::Success,
                format!("checksum_total={}", report.checksum_total),
            );
            consumer_reports.push(report);
        }

        self.oracle = MessageOracle::with_broadcast_consumers(self.config.consumer_count);
        self.oracle
            .extend_published(producer_report.messages.iter().cloned());

        verify_raw_ring_broadcast(&self.oracle, &consumer_reports)
            .map_err(DstRunnerError::Oracle)?;

        assertions.assert_always(
            producer_report.messages.len() == self.config.message_count as usize,
            "producer emitted expected message count",
            format!(
                "expected={}, actual={}",
                self.config.message_count,
                producer_report.messages.len()
            ),
        );
        assertions.assert_always(
            consumer_reports
                .iter()
                .all(|report| report.messages.len() == self.config.message_count as usize),
            "all consumers observed expected message count",
            format!(
                "expected={} actuals={:?}",
                self.config.message_count,
                consumer_reports
                    .iter()
                    .map(|report| report.messages.len())
                    .collect::<Vec<_>>()
            ),
        );
        assertions.assert_sometimes(
            self.config.message_count as usize > effective_ring_depth,
            "ring buffer wraps around",
            format!(
                "message_count={} ring_depth={effective_ring_depth}",
                self.config.message_count
            ),
        );
        assertions.assert_sometimes(
            self.config.consumer_count > 1,
            "broadcast fanout exercised",
            format!("consumer_count={}", self.config.consumer_count),
        );
        assertions.assert_sometimes(
            restarted_producer,
            "producer restart executed",
            format!("failure_class={failure_class:?}"),
        );
        assertions.assert_sometimes(
            restarted_consumers.iter().any(|restarted| *restarted),
            "consumer restart executed",
            format!("failure_class={failure_class:?}"),
        );
        assertions.assert_sometimes(
            matches!(
                policy.injected_fault,
                Some(RawRingInjectedFault::ConsumerSuspendAndResume { .. })
            ),
            "consumer suspend executed",
            format!("failure_class={failure_class:?}"),
        );
        assertions.assert_sometimes(
            self.corruption_at_sequence.is_some(),
            "oracle corruption probe executed",
            format!("failure_class={failure_class:?}"),
        );

        let report = DstRunReport {
            seed: self.seed,
            config: DstConfig {
                ring_depth: effective_ring_depth,
                ..self.config.clone()
            },
            property: Some(property),
            failure_class,
            transport: TransportKind::RawRing,
            producer: producer_report,
            consumers: consumer_reports,
            assertions,
            trace: self.trace.clone(),
        };

        Ok(report)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "fault orchestration needs explicit mutable access to process and partial-report state"
    )]
    fn apply_runtime_faults(
        &mut self,
        harness: &RawRingHarness,
        run_root: &Path,
        segment: &str,
        ring_depth: usize,
        consumer_prefix: &str,
        policy: RawRingExecutionPolicy,
        producer: &mut SpawnedChild,
        consumers: &mut [SpawnedChild],
        partial_producer_report: &mut Option<ChildReport>,
        partial_consumer_reports: &mut [Option<ChildReport>],
        restarted_producer: &mut bool,
        restarted_consumers: &mut [bool],
        producer_report_path: &Path,
    ) -> Result<(), DstRunnerError> {
        let Some(fault) = policy.injected_fault else {
            return Ok(());
        };

        match fault {
            RawRingInjectedFault::ProducerKillAndRestart {
                after_ms,
                restart_delay_ms,
            } => {
                thread::sleep(Duration::from_millis(after_ms));
                self.trace.push(
                    ProcessRole::Producer,
                    SchedulerAction::Kill,
                    TraceStatus::Failed {
                        reason: "injected producer kill".to_string(),
                    },
                    "fault=producer_kill_and_restart",
                );
                *partial_producer_report = self.kill_child_immediately(producer, "producer")?;
                *restarted_producer = true;
                let published = restart_sequence_start(
                    partial_producer_report.as_ref(),
                    partial_consumer_reports,
                );
                if published >= self.config.message_count {
                    return Err(DstRunnerError::InvalidConfig(
                        "producer kill-and-restart fault fired after producer completed".into(),
                    ));
                }
                if let Some(producer_report) = partial_producer_report.as_mut() {
                    backfill_report_from_consumers(
                        producer_report,
                        partial_consumer_reports,
                        published,
                    );
                }
                for (consumer_index, consumer) in consumers.iter_mut().enumerate() {
                    self.trace.push(
                        ProcessRole::Consumer {
                            index: consumer_index as u32,
                        },
                        SchedulerAction::Kill,
                        TraceStatus::Failed {
                            reason: "producer restart topology reset".to_string(),
                        },
                        "fault=producer_kill_and_restart",
                    );
                    partial_consumer_reports[consumer_index] = self
                        .kill_child_immediately(consumer, &format!("consumer-{consumer_index}"))?;
                    restarted_consumers[consumer_index] = true;
                }

                thread::sleep(Duration::from_millis(restart_delay_ms));
                let restart_segment = match self.config.backend {
                    BackendKind::Shm => unique_shm_segment("d"),
                    BackendKind::Mmap => unique_segment("dst"),
                };
                let restart_consumer_prefix = format!("{consumer_prefix}_r");
                for (consumer_index, consumer_slot) in consumers
                    .iter_mut()
                    .enumerate()
                    .take(self.config.consumer_count)
                {
                    let restarted = self.spawn_raw_ring_child(
                        harness,
                        run_root,
                        &restart_segment,
                        "consumer",
                        ring_depth,
                        Some(consumer_index),
                        &restart_consumer_prefix,
                        policy,
                        producer_report_path,
                        RawRingChildOverrides {
                            consumer_message_count: Some(self.config.message_count - published),
                            allow_corruption_validation: self.corruption_at_sequence.is_some(),
                            ..RawRingChildOverrides::default()
                        },
                    )?;
                    self.trace.push(
                        ProcessRole::Consumer {
                            index: consumer_index as u32,
                        },
                        SchedulerAction::Restart,
                        TraceStatus::Success,
                        "fault=producer_kill_and_restart",
                    );
                    *consumer_slot = restarted;
                }
                thread::sleep(Duration::from_millis(policy.startup_delay_ms));
                let restarted = self.spawn_raw_ring_child(
                    harness,
                    run_root,
                    &restart_segment,
                    "producer",
                    ring_depth,
                    None,
                    &restart_consumer_prefix,
                    policy,
                    producer_report_path,
                    RawRingChildOverrides {
                        producer_message_count: Some(self.config.message_count - published),
                        sequence_start: Some(published),
                        ..RawRingChildOverrides::default()
                    },
                )?;
                self.trace.push(
                    ProcessRole::Producer,
                    SchedulerAction::Restart,
                    TraceStatus::Success,
                    format!("fault=producer_kill_and_restart start={published}"),
                );
                *producer = restarted;
            }
            RawRingInjectedFault::ConsumerKillAndRestart {
                consumer_index,
                after_ms,
                restart_delay_ms,
            } => {
                thread::sleep(Duration::from_millis(after_ms));
                let consumer = consumers.get_mut(consumer_index).ok_or_else(|| {
                    DstRunnerError::InvalidConfig(format!(
                        "consumer index {consumer_index} missing for injected fault"
                    ))
                })?;
                self.trace.push(
                    ProcessRole::Consumer {
                        index: consumer_index as u32,
                    },
                    SchedulerAction::Kill,
                    TraceStatus::Failed {
                        reason: "injected consumer kill".to_string(),
                    },
                    "fault=consumer_kill_and_restart",
                );
                partial_consumer_reports[consumer_index] =
                    self.kill_child_immediately(consumer, &format!("consumer-{consumer_index}"))?;
                restarted_consumers[consumer_index] = true;
                thread::sleep(Duration::from_millis(restart_delay_ms));
                let restart_consumer_prefix = if policy
                    .required_consumer_liveness
                    .map(|required| required.restart_with_wrong_consumer_id)
                    .unwrap_or(false)
                {
                    format!("{consumer_prefix}_wrong")
                } else {
                    consumer_prefix.to_string()
                };
                let restarted = self.spawn_raw_ring_child(
                    harness,
                    run_root,
                    segment,
                    "consumer",
                    ring_depth,
                    Some(consumer_index),
                    &restart_consumer_prefix,
                    policy,
                    producer_report_path,
                    RawRingChildOverrides {
                        allow_corruption_validation: self.corruption_at_sequence.is_some(),
                        ..RawRingChildOverrides::default()
                    },
                )?;
                self.trace.push(
                    ProcessRole::Consumer {
                        index: consumer_index as u32,
                    },
                    SchedulerAction::Restart,
                    TraceStatus::Success,
                    "fault=consumer_kill_and_restart",
                );
                consumers[consumer_index] = restarted;
            }
            RawRingInjectedFault::ConsumerSuspendAndResume {
                consumer_index,
                after_ms,
                suspend_ms,
            } => {
                thread::sleep(Duration::from_millis(after_ms));
                let consumer = consumers.get_mut(consumer_index).ok_or_else(|| {
                    DstRunnerError::InvalidConfig(format!(
                        "consumer index {consumer_index} missing for injected fault"
                    ))
                })?;
                self.trace.push(
                    ProcessRole::Consumer {
                        index: consumer_index as u32,
                    },
                    SchedulerAction::SleepMs(suspend_ms),
                    TraceStatus::Planned,
                    "fault=consumer_suspend_and_resume",
                );
                self.suspend_child_temporarily(
                    consumer,
                    &format!("consumer-{consumer_index}"),
                    Duration::from_millis(suspend_ms),
                )?;
                self.trace.push(
                    ProcessRole::Consumer {
                        index: consumer_index as u32,
                    },
                    SchedulerAction::Start,
                    TraceStatus::Success,
                    "fault=consumer_suspend_and_resume",
                );
            }
        }

        Ok(())
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "child launch inputs are kept explicit so env wiring stays readable at each callsite"
    )]
    fn spawn_raw_ring_child(
        &self,
        harness: &RawRingHarness,
        run_root: &Path,
        segment: &str,
        mode: &str,
        ring_depth: usize,
        consumer_index: Option<usize>,
        consumer_prefix: &str,
        policy: RawRingExecutionPolicy,
        producer_report_path: &Path,
        overrides: RawRingChildOverrides,
    ) -> Result<SpawnedChild, DstRunnerError> {
        let report_path = run_root.join(format!(
            "{}_{}_{}.json",
            mode,
            consumer_index
                .map(|index| index.to_string())
                .unwrap_or_else(|| "producer".to_string()),
            segment
        ));
        let checkpoint_path = run_root.join(format!(
            "{}_{}_{}.checkpoint.json",
            mode,
            consumer_index
                .map(|index| index.to_string())
                .unwrap_or_else(|| "producer".to_string()),
            segment
        ));
        let mut cmd = Command::new(&harness.executable);
        cmd.env("DST_CHILD_TRANSPORT", "raw_ring")
            .env(
                "DST_CHILD_BACKEND",
                match self.config.backend {
                    BackendKind::Shm => "shm",
                    BackendKind::Mmap => "mmap",
                },
            )
            .env("DST_CHILD_MODE", mode)
            .env("DST_RUN_ROOT", run_root.display().to_string())
            .env("DST_SEGMENT", segment)
            .env("DST_SEED", self.seed.to_string())
            .env("DST_RING_DEPTH", ring_depth.to_string())
            .env("DST_MESSAGE_COUNT", self.config.message_count.to_string())
            .env("DST_PAYLOAD_SIZE", self.config.payload_size.to_string())
            .env("DST_CONSUMER_COUNT", self.config.consumer_count.to_string())
            .env(
                "DST_WAIT_STRATEGY",
                match self.config.wait_strategy {
                    WaitStrategyKind::BusySpin => "busyspin",
                    WaitStrategyKind::Sleep => "sleep",
                    WaitStrategyKind::Block => "block",
                    WaitStrategyKind::SpinLoopHint => "spinloop",
                },
            )
            .env(
                "DST_POST_PUBLISH_HOLD_MS",
                policy.producer_hold_ms.to_string(),
            )
            .env(
                "DST_PUBLISH_PAUSE_EVERY",
                policy.publish_pause_every.to_string(),
            )
            .env(
                "DST_PUBLISH_PAUSE_MICROS",
                policy.publish_pause_micros.to_string(),
            )
            .env(
                "DST_WAIT_FOR_CONSUMERS_READY",
                if policy.wait_for_consumers_ready {
                    "1"
                } else {
                    "0"
                },
            )
            .env("DST_CONSUMER_PREFIX", consumer_prefix)
            .env(
                "DST_PRODUCER_REPORT_PATH",
                producer_report_path.display().to_string(),
            )
            .env("DST_CHECKPOINT_PATH", &checkpoint_path)
            .env("DST_REPORT_PATH", &report_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        if let Some(required) = policy.required_consumer_liveness {
            let mut required_consumer_ids = (0..self.config.consumer_count)
                .map(|index| format!("{consumer_prefix}_{index}"))
                .collect::<Vec<_>>();
            for index in 0..required.required_consumer_missing_slots {
                required_consumer_ids.push(format!("{consumer_prefix}_missing_{index}"));
            }
            cmd.env("DST_REQUIRED_CONSUMER_IDS", required_consumer_ids.join(","))
                .env(
                    "DST_REQUIRED_STARTUP_WAIT_MS",
                    required.startup_wait_ms.to_string(),
                )
                .env(
                    "DST_REQUIRED_PROGRESS_TIMEOUT_MS",
                    required.progress_timeout_ms.to_string(),
                )
                .env(
                    "DST_REQUIRED_PROGRESS_CHECK_INTERVAL_MS",
                    required.progress_check_interval_ms.to_string(),
                )
                .env(
                    "DST_REQUIRED_SHUTDOWN_GRACE_MS",
                    required.shutdown_grace_ms.to_string(),
                );
        }

        if let Some(message_count) = overrides.producer_message_count {
            cmd.env("DST_PRODUCER_MESSAGE_COUNT", message_count.to_string());
        }
        if let Some(message_count) = overrides.consumer_message_count {
            cmd.env("DST_CONSUMER_MESSAGE_COUNT", message_count.to_string());
        }
        if let Some(sequence_start) = overrides.sequence_start {
            cmd.env("DST_SEQUENCE_START", sequence_start.to_string());
        }
        if let Some(checkpoint_every) = overrides.checkpoint_every {
            cmd.env("DST_CHECKPOINT_EVERY", checkpoint_every.to_string());
        }
        if let Some(corrupt_at_sequence) = overrides.corrupt_at_sequence {
            cmd.env("DST_CORRUPT_AT_SEQUENCE", corrupt_at_sequence.to_string());
        }
        if overrides.allow_corruption_validation {
            cmd.env("DST_ALLOW_CORRUPTION_VALIDATION", "1");
        }

        if let Some(index) = consumer_index {
            cmd.env("DST_CONSUMER_INDEX", index.to_string())
                .env("DST_CONSUMER_ID", format!("{consumer_prefix}_{index}"));
        }

        let child = cmd.spawn().map_err(|source| DstRunnerError::Spawn {
            role: mode.to_string(),
            source,
        })?;
        Ok(SpawnedChild {
            child,
            report_path,
            checkpoint_path,
        })
    }

    fn wait_and_decode_child<T: DeserializeOwned>(
        &self,
        mut spawned: SpawnedChild,
        timeout: Duration,
        role: &str,
    ) -> Result<T, DstRunnerError> {
        let start = Instant::now();
        loop {
            if spawned
                .child
                .try_wait()
                .map_err(|source| DstRunnerError::Spawn {
                    role: role.to_string(),
                    source,
                })?
                .is_some()
            {
                let output =
                    spawned
                        .child
                        .wait_with_output()
                        .map_err(|source| DstRunnerError::Spawn {
                            role: role.to_string(),
                            source,
                        })?;
                return self.decode_output(output, &spawned.report_path, role);
            }

            if start.elapsed() > timeout {
                let _ = spawned.child.kill();
                let output =
                    spawned
                        .child
                        .wait_with_output()
                        .map_err(|source| DstRunnerError::Spawn {
                            role: role.to_string(),
                            source,
                        })?;
                return Err(DstRunnerError::Timeout {
                    role: role.to_string(),
                    timeout,
                    stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                });
            }

            disruptor_mp::perform_default_discovery_poll_wait();
        }
    }

    fn kill_child_immediately(
        &self,
        spawned: &mut SpawnedChild,
        role: &str,
    ) -> Result<Option<ChildReport>, DstRunnerError> {
        let _ = spawned.child.kill();
        let status = spawned
            .child
            .wait()
            .map_err(|source| DstRunnerError::Spawn {
                role: role.to_string(),
                source,
            })?;
        let partial = fs::read_to_string(&spawned.checkpoint_path)
            .ok()
            .and_then(|raw| serde_json::from_str::<ChildReport>(raw.trim()).ok());
        let _ = fs::remove_file(&spawned.checkpoint_path);
        if status.success() {
            return Err(DstRunnerError::InvalidConfig(format!(
                "{role} exited successfully during forced kill injection; fault did not occur during active run"
            )));
        }
        Ok(partial)
    }

    fn suspend_child_temporarily(
        &self,
        spawned: &mut SpawnedChild,
        role: &str,
        duration: Duration,
    ) -> Result<(), DstRunnerError> {
        send_signal(spawned.child.id(), libc::SIGSTOP).map_err(|source| DstRunnerError::Spawn {
            role: format!("{role} suspend"),
            source,
        })?;
        thread::sleep(duration);
        send_signal(spawned.child.id(), libc::SIGCONT).map_err(|source| DstRunnerError::Spawn {
            role: format!("{role} resume"),
            source,
        })?;
        Ok(())
    }

    fn decode_output<T: DeserializeOwned>(
        &self,
        output: Output,
        report_path: &Path,
        role: &str,
    ) -> Result<T, DstRunnerError> {
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

        if !output.status.success() {
            return Err(DstRunnerError::ChildFailed {
                role: role.to_string(),
                status: output.status.code(),
                stdout,
                stderr,
            });
        }

        let raw = fs::read_to_string(report_path).map_err(|source| {
            DstRunnerError::InvalidConfig(format!(
                "failed to read report file {} for {role}: {source}",
                report_path.display()
            ))
        })?;

        serde_json::from_str(raw.trim()).map_err(|source| DstRunnerError::ChildProtocol {
            role: role.to_string(),
            stdout,
            stderr,
            source,
        })
    }
}

fn unique_segment(prefix: &str) -> String {
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    format!("{prefix}_{pid}_{nanos}")
}

fn unique_shm_segment(prefix: &str) -> String {
    let pid = std::process::id() % 10_000;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    format!("{prefix}{pid:04x}{:04x}", (nanos & 0xffff) as u16)
}

fn unique_run_root(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(unique_segment(prefix))
}

fn send_signal(pid: u32, signal: i32) -> Result<(), std::io::Error> {
    let rc = unsafe { libc::kill(pid as i32, signal) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn merge_child_reports(
    producer: &ChildReport,
    prefix: Option<ChildReport>,
    suffix: ChildReport,
) -> ChildReport {
    let mut merged = prefix.unwrap_or_else(|| ChildReport {
        role: suffix.role.clone(),
        messages: Vec::new(),
        checksum_total: 0,
        backpressure_events: 0,
        attached_after_ms: suffix.attached_after_ms,
    });

    let first_suffix_seq = suffix
        .messages
        .first()
        .map(|message| message.sequence)
        .unwrap_or(producer.messages.len() as u64);
    let gap_start = merged
        .messages
        .last()
        .map(|message| message.sequence + 1)
        .unwrap_or(0);

    if gap_start < first_suffix_seq {
        merged.messages.extend(
            producer
                .messages
                .iter()
                .filter(|message| {
                    message.sequence >= gap_start && message.sequence < first_suffix_seq
                })
                .cloned(),
        );
    }

    let overlap_cutoff = merged.messages.last().map(|message| message.sequence);
    merged
        .messages
        .extend(suffix.messages.into_iter().filter(|message| {
            overlap_cutoff
                .map(|cutoff| message.sequence > cutoff)
                .unwrap_or(true)
        }));
    merged.checksum_total = merged
        .messages
        .iter()
        .fold(0u64, |sum, message| sum.wrapping_add(message.payload_hash));
    merged.backpressure_events = merged
        .backpressure_events
        .wrapping_add(suffix.backpressure_events);
    merged.attached_after_ms = merged.attached_after_ms.min(suffix.attached_after_ms);
    merged
}

#[derive(Debug, Clone, Copy, Default)]
struct RawRingChildOverrides {
    producer_message_count: Option<u64>,
    consumer_message_count: Option<u64>,
    sequence_start: Option<u64>,
    checkpoint_every: Option<u64>,
    corrupt_at_sequence: Option<u64>,
    allow_corruption_validation: bool,
}

fn restart_sequence_start(
    producer: Option<&ChildReport>,
    partial_consumers: &[Option<ChildReport>],
) -> u64 {
    let producer_next = producer.and_then(report_next_sequence).unwrap_or(0);
    let consumer_next = partial_consumers
        .iter()
        .filter_map(|report| report.as_ref().and_then(report_next_sequence))
        .max()
        .unwrap_or(0);
    producer_next.max(consumer_next)
}

fn backfill_report_from_consumers(
    producer: &mut ChildReport,
    partial_consumers: &[Option<ChildReport>],
    published: u64,
) {
    let next_missing = report_next_sequence(producer).unwrap_or(0);
    if next_missing >= published {
        return;
    }

    let mut recovered = partial_consumers
        .iter()
        .filter_map(|report| report.as_ref())
        .flat_map(|report| report.messages.iter().cloned())
        .filter(|message| message.sequence >= next_missing && message.sequence < published)
        .collect::<Vec<_>>();
    recovered.sort_by_key(|message| message.sequence);
    recovered.dedup_by_key(|message| message.sequence);
    producer.messages.extend(recovered);
    producer.checksum_total = producer
        .messages
        .iter()
        .fold(0u64, |sum, message| sum.wrapping_add(message.payload_hash));
}

fn report_next_sequence(report: &ChildReport) -> Option<u64> {
    report.messages.last().map(|message| message.sequence + 1)
}

fn concat_child_reports(prefix: Option<ChildReport>, suffix: ChildReport) -> ChildReport {
    let mut merged = prefix.unwrap_or_else(|| ChildReport {
        role: suffix.role.clone(),
        messages: Vec::new(),
        checksum_total: 0,
        backpressure_events: 0,
        attached_after_ms: suffix.attached_after_ms,
    });

    let overlap_cutoff = merged.messages.last().map(|message| message.sequence);
    merged
        .messages
        .extend(suffix.messages.into_iter().filter(|message| {
            overlap_cutoff
                .map(|cutoff| message.sequence > cutoff)
                .unwrap_or(true)
        }));
    merged.checksum_total = merged
        .messages
        .iter()
        .fold(0u64, |sum, message| sum.wrapping_add(message.payload_hash));
    merged.backpressure_events = merged
        .backpressure_events
        .wrapping_add(suffix.backpressure_events);
    merged.attached_after_ms = merged.attached_after_ms.min(suffix.attached_after_ms);
    merged
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner_oracle::OracleMessage;
    use disruptor_mp::dst::contract::ProcessRole;

    fn report(sequences: &[u64]) -> ChildReport {
        ChildReport {
            role: ProcessRole::Producer,
            messages: sequences
                .iter()
                .map(|sequence| OracleMessage {
                    sequence: *sequence,
                    payload_hash: sequence.wrapping_add(1),
                    payload_len: 16,
                    timestamp_ns: *sequence,
                })
                .collect(),
            checksum_total: 0,
            backpressure_events: 0,
            attached_after_ms: 0,
        }
    }

    #[test]
    fn runner_from_seed_is_deterministic() {
        assert_eq!(
            DstRunner::from_seed(42).config,
            DstRunner::from_seed(42).config
        );
    }

    #[test]
    fn merge_child_reports_drops_overlap_at_restart_boundary() {
        let producer = report(&[0, 1, 2, 3, 4, 5, 6, 7]);
        let prefix = Some(report(&[0, 1, 2, 3]));
        let suffix = report(&[3, 4, 5, 6, 7]);
        let merged = merge_child_reports(&producer, prefix, suffix);
        assert_eq!(
            merged
                .messages
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4, 5, 6, 7]
        );
    }

    #[test]
    fn restart_sequence_start_uses_max_observed_consumer_sequence() {
        let producer = report(&[0, 1, 2, 3]);
        let consumers = vec![Some(report(&[0, 1, 2, 3, 4, 5])), None];
        assert_eq!(restart_sequence_start(Some(&producer), &consumers), 6);
    }

    #[test]
    fn backfill_report_from_consumers_recovers_missing_tail() {
        let mut producer = report(&[0, 1, 2]);
        let consumers = vec![Some(report(&[0, 1, 2, 3, 4]))];
        backfill_report_from_consumers(&mut producer, &consumers, 5);
        assert_eq!(
            producer
                .messages
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4]
        );
    }
}
