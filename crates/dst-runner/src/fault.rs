use dst_fixtures::dst_contract::{FailureClass, ProcessRole};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FaultKind {
    Kill,
    DelayStart(u64),
    Suspend(u64),
    SlowConsumer(u64),
    SkipConsumerAttach,
    KillAndRestart(u64),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FaultEvent {
    pub at_message: u64,
    pub target: ProcessRole,
    pub kind: FaultKind,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FaultInjector {
    pub seed: u64,
    pub schedule: Vec<FaultEvent>,
}

impl FaultInjector {
    pub fn none(seed: u64) -> Self {
        Self {
            seed,
            schedule: Vec::new(),
        }
    }

    pub fn from_seed(
        seed: u64,
        failure_class: FailureClass,
        consumers: usize,
        message_count: u64,
    ) -> Self {
        let mut schedule = Vec::new();
        match failure_class {
            FailureClass::ProducerBeforeConsumers => {
                schedule.push(FaultEvent {
                    at_message: 0,
                    target: ProcessRole::Consumer { index: 0 },
                    kind: FaultKind::DelayStart(150),
                });
            }
            FailureClass::LateConsumerAttach => {
                schedule.push(FaultEvent {
                    at_message: message_count / 3,
                    target: ProcessRole::Consumer { index: 0 },
                    kind: FaultKind::DelayStart(75),
                });
            }
            FailureClass::CreateAttachChurn => {
                schedule.push(FaultEvent {
                    at_message: message_count / 4,
                    target: ProcessRole::Consumer { index: 0 },
                    kind: FaultKind::Suspend(120),
                });
            }
            FailureClass::ProducerCrashAndRestart => {
                schedule.push(FaultEvent {
                    at_message: message_count / 3,
                    target: ProcessRole::Producer,
                    kind: FaultKind::KillAndRestart(80),
                });
            }
            FailureClass::ReadinessGateViolation => {
                schedule.push(FaultEvent {
                    at_message: 0,
                    target: ProcessRole::Consumer { index: 0 },
                    kind: FaultKind::DelayStart(200),
                });
            }
            FailureClass::DiscoveryVisibilityLag => {
                for index in 0..consumers.max(1) {
                    schedule.push(FaultEvent {
                        at_message: 0,
                        target: ProcessRole::Consumer {
                            index: index as u32,
                        },
                        kind: FaultKind::DelayStart(50 * index as u64),
                    });
                }
            }
            FailureClass::ConsumerCrashAndRestart => {
                schedule.push(FaultEvent {
                    at_message: message_count / 3,
                    target: ProcessRole::Consumer { index: 0 },
                    kind: FaultKind::KillAndRestart(80),
                });
            }
            _ => {}
        }

        Self { seed, schedule }
    }

    pub fn faults_at_message(&self, message_num: u64) -> Vec<&FaultEvent> {
        self.schedule
            .iter()
            .filter(|event| event.at_message == message_num)
            .collect()
    }
}
