use crate::runner_config::{CodecKind, DstConfig};
use crate::runner_oracle::OracleMessage;
use disruptor_mp::dst::assertions::AssertionLog;
use disruptor_mp::dst::contract::{FailureClass, ProcessRole, TraceArtifact};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DstProperty {
    MessageIntegrity,
    SequenceMonotonicity,
    ZeroCopyEquivalence,
    FragmentationCorrectness,
    BackpressureEnforced,
    BroadcastCompleteness,
    ConcurrentIntegrity,
    RestartSafety,
    DiscoveryConvergence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransportKind {
    RawRing,
    Framed { frame_size: usize },
    TypedCodec { codec: CodecKind },
    TypedZeroCopy { codec: CodecKind },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChildReport {
    pub role: ProcessRole,
    pub messages: Vec<OracleMessage>,
    pub checksum_total: u64,
    pub backpressure_events: u64,
    pub attached_after_ms: u64,
}

impl ChildReport {
    pub fn produced(&self) -> u64 {
        self.messages.len() as u64
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DstRunReport {
    pub seed: u64,
    pub config: DstConfig,
    pub property: Option<DstProperty>,
    pub failure_class: Option<FailureClass>,
    pub transport: TransportKind,
    pub producer: ChildReport,
    pub consumers: Vec<ChildReport>,
    pub assertions: AssertionLog,
    pub trace: TraceArtifact,
}
