use crate::harness::output::PhaseTiming;
use crate::latency::LatencyStats;
use serde::{Deserialize, Serialize};
use std::process::Command;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportBundle {
    pub metadata: RunMetadata,
    pub scenarios: Vec<ScenarioReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMetadata {
    pub timestamp: String,
    pub platform: String,
    pub cpu: String,
    pub git_commit: Option<String>,
    pub rust_version: String,
}

impl RunMetadata {
    pub fn capture() -> Self {
        Self {
            timestamp: chrono::Utc::now().to_rfc3339(),
            platform: format!("{} {}", std::env::consts::OS, std::env::consts::ARCH),
            cpu: detect_cpu(),
            git_commit: detect_git_commit(),
            rust_version: env!("CARGO_PKG_RUST_VERSION")
                .parse()
                .unwrap_or_else(|_| "unknown".to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioReport {
    pub identity: ScenarioIdentity,
    pub config: ScenarioConfig,
    pub outcome: ScenarioOutcome,
    pub metadata: RunMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioIdentity {
    pub benchmark_id: String,
    pub suite: String,
    pub benchmark: String,
    pub family: ScenarioFamily,
    pub scenario: String,
    pub backend: BackendKind,
    pub layer: String,
    pub codec: Option<CodecKind>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioConfig {
    pub measurement: MeasurementKind,
    pub transport: TransportSpec,
    pub workload: WorkloadConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransportSpec {
    pub wait_strategy: WaitStrategyKind,
    pub coordination: Option<CoordinationKind>,
    pub discovery: Option<DiscoveryKind>,
    pub framing: Option<FramingKind>,
    pub zero_copy: Option<ZeroCopyKind>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadConfig {
    pub message_size_bytes: usize,
    pub payload_bytes: usize,
    pub buffer_depth: usize,
    pub num_messages: u64,
    pub warmup_messages: u64,
    pub num_producers: usize,
    pub num_consumers: usize,
    pub batch_size: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScenarioOutcome {
    Throughput(ThroughputOutcome),
    Layout(LayoutOutcome),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputOutcome {
    pub producer: ProducerMetrics,
    pub consumers: ConsumerAggregate,
    pub per_consumer: Vec<ConsumerMetrics>,
    pub verification: VerificationMetrics,
    pub latency: Option<LatencyStats>,
    pub phase_timing: Option<PhaseTiming>,
    pub derived: DerivedMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerMetrics {
    pub throughput_ops_sec: f64,
    pub bandwidth_bytes_sec: Option<f64>,
    pub data_rate_gbps: Option<f64>,
    pub data_rate_mbps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerAggregate {
    pub average_throughput_ops_sec: f64,
    pub min_throughput_ops_sec: Option<f64>,
    pub max_throughput_ops_sec: Option<f64>,
    pub total_throughput_ops_sec: Option<f64>,
    pub average_bandwidth_bytes_sec: Option<f64>,
    pub average_data_rate_gbps: Option<f64>,
    pub checksum_total: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerMetrics {
    pub consumer_id: usize,
    pub throughput_ops_sec: f64,
    pub events_consumed: u64,
    pub bandwidth_bytes_sec: f64,
    pub data_rate_gbps: f64,
    pub checksum: u64,
    pub latency: Option<LatencyStats>,
    pub phase_timing: Option<PhaseTiming>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationMetrics {
    pub passed: bool,
    pub messages_processed: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DerivedMetrics {
    pub pct_of_raw_ring: Option<f64>,
    pub speedup_vs_bincode: Option<f64>,
    pub hw_bandwidth_limit_gbps: Option<f64>,
    pub hw_efficiency_pct: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayoutOutcome {
    pub avg_ns: u64,
    pub budget_ns: u64,
    pub iterations: usize,
    pub pass: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ScenarioFamily {
    RawRing,
    WaitStrategy,
    Competitive,
    Framed,
    CodecE2E,
    CodecNoFrag,
    MonsterSweep,
    MyelonLayerSweep,
    MyelonFramedSweep,
    NofragSweep,
    LayoutValidation,
    Unknown(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BackendKind {
    Shm,
    Mmap,
    Layout,
    Unknown(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CodecKind {
    Bincode,
    Rkyv,
    Flatbuf,
    Unknown(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WaitStrategyKind {
    BusySpin,
    BusySpinWithSpinLoopHint,
    Block,
    Sleep,
    Unknown(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CoordinationKind {
    BenchmarkCoordination,
    UnifiedCoordination,
    MmapBuiltin,
    Unknown(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DiscoveryKind {
    Disabled,
    Enabled { consumers: usize },
    Unknown(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FramingKind {
    None,
    Fixed64K,
    Fixed64KBatch,
    RightSized,
    Unknown(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ZeroCopyKind {
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MeasurementKind {
    MaxThroughput,
    CoAware { target_rate: u64 },
    BatchTiming,
    LayoutValidation,
    Unknown(String),
}

fn detect_cpu() -> String {
    #[cfg(target_os = "macos")]
    {
        Command::new("sysctl")
            .args(["-n", "machdep.cpu.brand_string"])
            .output()
            .ok()
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .map(|cpu| cpu.trim().to_string())
            .unwrap_or_else(|| "Apple Silicon".to_string())
    }
    #[cfg(not(target_os = "macos"))]
    {
        std::fs::read_to_string("/proc/cpuinfo")
            .ok()
            .and_then(|cpuinfo| {
                cpuinfo
                    .lines()
                    .find(|line| line.starts_with("model name"))
                    .map(|line| line.split(':').nth(1).unwrap_or("").trim().to_string())
            })
            .unwrap_or_else(|| "unknown".to_string())
    }
}

fn detect_git_commit() -> Option<String> {
    Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|commit| commit.trim().to_string())
}
