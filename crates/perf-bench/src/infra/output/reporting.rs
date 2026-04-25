//! Benchmark reporting compatibility layer.
//!
//! This module still owns the legacy `BenchResult` / `BenchReport` structs used by
//! bench binaries and shared harness code, but the live render / emit pipeline now
//! flows through `report`.

use crate::infra::latency;
use crate::infra::output::report::ReportBundleCompat;
use crate::infra::output::results::{ConsumerOutput, PhaseTiming, ProducerOutput};
use serde::{Deserialize, Serialize};
use std::io;
use std::process::Command;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchResult {
    pub benchmark_id: String,
    pub scenario: String,
    pub backend: String,
    pub layer: String,
    pub codec: Option<String>,
    pub measurement_mode: String,
    pub wait_strategy: String,
    pub config: BenchConfig,
    pub results: BenchResults,
    pub latency: Option<latency::LatencyStats>,
    pub metadata: BenchMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchConfig {
    pub message_size_bytes: usize,
    pub payload_bytes: usize,
    pub buffer_depth: usize,
    pub num_messages: u64,
    pub warmup_messages: u64,
    pub num_producers: usize,
    pub num_consumers: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coordination: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zero_copy: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub framing: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchResults {
    pub producer_throughput_ops_sec: f64,
    pub consumer_throughput_ops_sec: f64,
    pub data_rate_mbps: f64,
    pub messages_processed: u64,
    pub verification_passed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_bandwidth_bytes_sec: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_data_rate_gbps: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_avg_bandwidth_bytes_sec: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_avg_data_rate_gbps: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_min_throughput_ops_sec: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_max_throughput_ops_sec: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_total_throughput_ops_sec: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_checksum_total: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_timing: Option<PhaseTiming>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pct_of_raw_ring: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_vs_raw_ring_pct: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speedup_vs_bincode: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_vs_bincode_pct: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_avg_ns: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_vs_decode_speedup: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alloc_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alloc_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hw_bandwidth_limit_gbps: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hw_efficiency_pct: Option<f64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub per_consumer: Vec<BenchConsumerResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout_validation: Option<LayoutValidationMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchConsumerResult {
    pub consumer_id: usize,
    pub throughput_ops_sec: f64,
    pub events_consumed: u64,
    pub bandwidth_bytes_sec: f64,
    pub data_rate_gbps: f64,
    pub checksum: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency: Option<latency::LatencyStats>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_timing: Option<PhaseTiming>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchMetadata {
    pub timestamp: String,
    pub platform: String,
    pub cpu: String,
    pub git_commit: Option<String>,
    pub rust_version: String,
}

impl BenchMetadata {
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
pub struct BenchReport {
    pub metadata: BenchMetadata,
    pub results: Vec<BenchResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayoutValidationMetrics {
    pub avg_ns: u64,
    pub budget_ns: u64,
    pub pass: bool,
    pub iterations: usize,
}

#[derive(Debug, Clone, Default)]
pub struct BenchTransportSpec {
    pub coordination: Option<String>,
    pub discovery_mode: Option<String>,
    pub zero_copy: Option<bool>,
    pub framing: Option<String>,
}

impl BenchTransportSpec {
    pub fn benchmark_shm(consumers: usize) -> Self {
        Self {
            coordination: Some("BenchmarkCoordination".to_string()),
            discovery_mode: Some(format!("enabled({consumers})")),
            zero_copy: None,
            framing: None,
        }
    }

    pub fn mmap_builtin() -> Self {
        Self {
            coordination: Some("mmap_builtin".to_string()),
            discovery_mode: Some("disabled".to_string()),
            zero_copy: None,
            framing: None,
        }
    }

    pub fn unified_pingpong() -> Self {
        Self {
            coordination: Some("UnifiedCoordination".to_string()),
            discovery_mode: Some("disabled".to_string()),
            zero_copy: None,
            framing: None,
        }
    }

    pub fn with_zero_copy(mut self, zero_copy: bool) -> Self {
        self.zero_copy = Some(zero_copy);
        self
    }

    pub fn with_framing(mut self, framing: &str) -> Self {
        self.framing = Some(framing.to_string());
        self
    }
}

#[derive(Debug, Clone)]
pub struct BenchResultSpec {
    pub bench_name: String,
    pub scenario: String,
    pub backend: String,
    pub layer: String,
    pub codec: Option<String>,
    pub measurement_mode: String,
    pub wait_strategy: String,
    pub transport: BenchTransportSpec,
    pub message_size_bytes: usize,
    pub payload_bytes: usize,
    pub buffer_depth: usize,
    pub num_messages: u64,
    pub warmup_messages: u64,
    pub num_producers: usize,
    pub num_consumers: usize,
    pub producer_throughput_ops_sec: f64,
    pub consumer_throughput_ops_sec: f64,
    pub latency: Option<latency::LatencyStats>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportView {
    Summary,
    Tree,
}

#[derive(Debug, Clone, Default)]
pub struct ReportOutputArgs {
    pub json_mode: bool,
    pub quick_mode: bool,
    pub tree_mode: bool,
    pub json_out: Option<String>,
    pub csv_out: Option<String>,
    pub markdown_out: Option<String>,
}

impl ReportOutputArgs {
    pub fn from_args(args: &[String]) -> Self {
        Self {
            json_mode: args.iter().any(|arg| arg == "--json"),
            quick_mode: args.iter().any(|arg| arg == "--quick"),
            tree_mode: args.iter().any(|arg| arg == "--tree"),
            json_out: find_arg_value(args, "--json-out"),
            csv_out: find_arg_value(args, "--csv-out"),
            markdown_out: find_arg_value(args, "--md-out"),
        }
    }
}

const HW_BANDWIDTH_LIMIT_GBPS: f64 = 300.0;

impl BenchReport {
    pub fn new() -> Self {
        Self {
            metadata: BenchMetadata::capture(),
            results: Vec::new(),
        }
    }

    pub fn add(&mut self, result: BenchResult) {
        self.results.push(result);
    }

    pub fn finalized(&self) -> Self {
        let mut report = self.clone();
        report.populate_derived_metrics();
        report
    }

    fn populate_derived_metrics(&mut self) {
        use std::collections::BTreeMap;

        let raw_baselines: BTreeMap<(String, usize, usize, String), f64> = self
            .results
            .iter()
            .filter(|result| result.layer == "raw_ring")
            .map(|result| {
                (
                    (
                        result.backend.clone(),
                        result.config.payload_bytes,
                        result.config.num_consumers,
                        result.measurement_mode.clone(),
                    ),
                    result.results.consumer_throughput_ops_sec,
                )
            })
            .collect();

        let bincode_baselines: BTreeMap<String, f64> = self
            .results
            .iter()
            .filter(|result| result.codec.as_deref() == Some("bincode"))
            .filter_map(|result| {
                codec_speedup_key(result)
                    .map(|key| (key, result.results.consumer_throughput_ops_sec))
            })
            .collect();

        for result in &mut self.results {
            if result.results.layout_validation.is_some() {
                continue;
            }

            let consumer_bw = result
                .results
                .consumer_avg_data_rate_gbps
                .unwrap_or_else(|| {
                    result.results.consumer_throughput_ops_sec
                        * result.config.message_size_bytes as f64
                        / 1e9
                });

            result.results.hw_bandwidth_limit_gbps = Some(HW_BANDWIDTH_LIMIT_GBPS);
            result.results.hw_efficiency_pct =
                Some((consumer_bw / HW_BANDWIDTH_LIMIT_GBPS) * 100.0);

            result.results.pct_of_raw_ring = if result.layer == "raw_ring" {
                Some(100.0)
            } else {
                raw_baselines
                    .get(&(
                        result.backend.clone(),
                        result.config.payload_bytes,
                        result.config.num_consumers,
                        result.measurement_mode.clone(),
                    ))
                    .map(|baseline| result.results.consumer_throughput_ops_sec / baseline * 100.0)
            };
            result.results.delta_vs_raw_ring_pct =
                result.results.pct_of_raw_ring.map(|value| value - 100.0);

            result.results.speedup_vs_bincode = codec_speedup_key(result).and_then(|key| {
                bincode_baselines
                    .get(&key)
                    .map(|baseline| result.results.consumer_throughput_ops_sec / baseline)
            });
            result.results.delta_vs_bincode_pct = result
                .results
                .speedup_vs_bincode
                .map(|value| (value - 1.0) * 100.0);
        }
    }
}

impl Default for BenchReport {
    fn default() -> Self {
        Self::new()
    }
}

pub fn emit_report(
    report: &BenchReport,
    output_args: &ReportOutputArgs,
    default_view: Option<ReportView>,
    quick_view: Option<ReportView>,
    markdown_writer: Option<fn(&BenchReport, &str) -> io::Result<()>>,
) {
    emit_report_with_extra_json(
        report,
        output_args,
        default_view,
        quick_view,
        markdown_writer,
        None,
    );
}

pub fn emit_report_with_extra_json(
    report: &BenchReport,
    output_args: &ReportOutputArgs,
    default_view: Option<ReportView>,
    quick_view: Option<ReportView>,
    markdown_writer: Option<fn(&BenchReport, &str) -> io::Result<()>>,
    extra_json_out: Option<&str>,
) {
    let report = report.finalized();
    let bundle = report.to_report();
    let mut forwarded_output_args = output_args.clone();
    if markdown_writer.is_some() {
        forwarded_output_args.markdown_out = None;
    }

    crate::infra::output::report::emit_report_with_extra_json(
        &bundle,
        &forwarded_output_args,
        default_view,
        quick_view,
        None,
        extra_json_out,
    );

    if let (Some(writer), Some(path)) = (markdown_writer, output_args.markdown_out.as_deref()) {
        writer(&report, path).expect("write markdown");
        eprintln!("Markdown written to {path}");
    }
}

fn find_arg_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|window| window[0] == flag)
        .map(|window| window[1].clone())
}

fn infer_benchmark_name(result: &BenchResult) -> Option<&str> {
    result.benchmark_id.split('/').nth(1)
}

fn codec_speedup_key(result: &BenchResult) -> Option<String> {
    let codec = result.codec.as_ref()?;
    let benchmark = infer_benchmark_name(result).unwrap_or(&result.benchmark_id);
    let benchmark_family = codec_speedup_benchmark_family(benchmark);
    let layer_family = codec_speedup_layer_family(&result.layer);
    let scenario_key = if result.scenario.contains(codec) {
        result.scenario.replacen(codec, "__codec__", 1)
    } else {
        return None;
    };

    Some(format!(
        "{benchmark_family}|{}|{}|{}|{}|{scenario_key}",
        result.backend, layer_family, result.measurement_mode, result.config.num_consumers,
    ))
}

fn codec_speedup_benchmark_family(benchmark: &str) -> &str {
    match benchmark {
        "pingpong_codec_shm"
        | "pingpong_codec_mmap"
        | "pingpong_typed_zero_copy_shm"
        | "pingpong_typed_zero_copy_mmap" => "pingpong_codec_family",
        other => other,
    }
}

fn codec_speedup_layer_family(layer: &str) -> &str {
    match layer {
        "typed" | "typed_zero_copy" | "typed_zero_copy_flatbuf" => "typed_family",
        other => other,
    }
}

pub fn make_result(spec: BenchResultSpec) -> BenchResult {
    BenchResult {
        benchmark_id: format!("myelon-bench/{}/{}", spec.bench_name, spec.scenario),
        scenario: spec.scenario,
        backend: spec.backend,
        layer: spec.layer,
        codec: spec.codec,
        measurement_mode: spec.measurement_mode,
        wait_strategy: spec.wait_strategy,
        config: BenchConfig {
            message_size_bytes: spec.message_size_bytes,
            payload_bytes: spec.payload_bytes,
            buffer_depth: spec.buffer_depth,
            num_messages: spec.num_messages,
            warmup_messages: spec.warmup_messages,
            num_producers: spec.num_producers,
            num_consumers: spec.num_consumers,
            coordination: spec.transport.coordination,
            discovery_mode: spec.transport.discovery_mode,
            zero_copy: spec.transport.zero_copy,
            framing: spec.transport.framing,
        },
        results: BenchResults {
            producer_throughput_ops_sec: spec.producer_throughput_ops_sec,
            consumer_throughput_ops_sec: spec.consumer_throughput_ops_sec,
            data_rate_mbps: crate::infra::events::data_rate_mbps(
                spec.producer_throughput_ops_sec,
                spec.message_size_bytes,
            ),
            messages_processed: spec.num_messages,
            verification_passed: true,
            producer_bandwidth_bytes_sec: None,
            producer_data_rate_gbps: None,
            consumer_avg_bandwidth_bytes_sec: None,
            consumer_avg_data_rate_gbps: None,
            consumer_min_throughput_ops_sec: None,
            consumer_max_throughput_ops_sec: None,
            consumer_total_throughput_ops_sec: None,
            consumer_checksum_total: None,
            phase_timing: None,
            pct_of_raw_ring: None,
            delta_vs_raw_ring_pct: None,
            speedup_vs_bincode: None,
            delta_vs_bincode_pct: None,
            access_avg_ns: None,
            access_vs_decode_speedup: None,
            alloc_count: None,
            alloc_bytes: None,
            hw_bandwidth_limit_gbps: None,
            hw_efficiency_pct: None,
            per_consumer: Vec::new(),
            layout_validation: None,
        },
        latency: spec.latency,
        metadata: BenchMetadata::capture(),
    }
}

pub fn attach_child_metrics(
    result: &mut BenchResult,
    producer: &ProducerOutput,
    consumers: &[ConsumerOutput],
) {
    result.results.producer_bandwidth_bytes_sec = Some(producer.bandwidth_bytes_sec);
    result.results.producer_data_rate_gbps = Some(producer.data_rate_gbps);

    if !consumers.is_empty() {
        let total_tp: f64 = consumers.iter().map(|entry| entry.throughput_ops_sec).sum();
        let avg_bw: f64 = consumers
            .iter()
            .map(|entry| entry.bandwidth_bytes_sec)
            .sum::<f64>()
            / consumers.len() as f64;
        let avg_rate: f64 = consumers
            .iter()
            .map(|entry| entry.data_rate_gbps)
            .sum::<f64>()
            / consumers.len() as f64;

        result.results.consumer_min_throughput_ops_sec = consumers
            .iter()
            .map(|entry| entry.throughput_ops_sec)
            .min_by(f64::total_cmp);
        result.results.consumer_max_throughput_ops_sec = consumers
            .iter()
            .map(|entry| entry.throughput_ops_sec)
            .max_by(f64::total_cmp);
        result.results.consumer_total_throughput_ops_sec = Some(total_tp);
        result.results.consumer_avg_bandwidth_bytes_sec = Some(avg_bw);
        result.results.consumer_avg_data_rate_gbps = Some(avg_rate);
        result.results.consumer_checksum_total = Some(
            consumers
                .iter()
                .fold(0u64, |sum, entry| sum.wrapping_add(entry.checksum)),
        );
    }

    result.results.per_consumer = consumers
        .iter()
        .map(|entry| BenchConsumerResult {
            consumer_id: entry.consumer_id,
            throughput_ops_sec: entry.throughput_ops_sec,
            events_consumed: entry.events_consumed,
            bandwidth_bytes_sec: entry.bandwidth_bytes_sec,
            data_rate_gbps: entry.data_rate_gbps,
            checksum: entry.checksum,
            latency: entry.latency.clone(),
            phase_timing: entry.phase_timing.clone(),
        })
        .collect();

    let consumer_phase_count = consumers
        .iter()
        .filter(|entry| entry.phase_timing.is_some())
        .count();
    if producer.phase_timing.is_some() || consumer_phase_count > 0 {
        let mut phase = producer.phase_timing.clone().unwrap_or(PhaseTiming {
            encode_avg_ns: None,
            transport_write_avg_ns: None,
            transport_read_avg_ns: None,
            decode_avg_ns: None,
        });
        if consumer_phase_count > 0 {
            let (read_sum, decode_sum) = consumers
                .iter()
                .filter_map(|entry| entry.phase_timing.as_ref())
                .fold((0.0, 0.0), |(read_acc, decode_acc), timing| {
                    (
                        read_acc + timing.transport_read_avg_ns.unwrap_or(0.0),
                        decode_acc + timing.decode_avg_ns.unwrap_or(0.0),
                    )
                });
            let denom = consumer_phase_count as f64;
            phase.transport_read_avg_ns = Some(read_sum / denom);
            phase.decode_avg_ns = Some(decode_sum / denom);
        }
        result.results.phase_timing = Some(phase);
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[expect(
        clippy::too_many_arguments,
        reason = "reporting tests build explicit benchmark dimensions for table coverage"
    )]
    fn make_test_result(
        bench_name: &str,
        scenario: &str,
        backend: &str,
        layer: &str,
        codec: Option<&str>,
        measurement_mode: &str,
        transport: BenchTransportSpec,
        msg_bytes: usize,
        payload_bytes: usize,
        buffer_depth: usize,
        num_messages: u64,
        warmup_messages: u64,
        num_consumers: usize,
        prod_ops: f64,
        cons_ops: f64,
        latency: Option<latency::LatencyStats>,
    ) -> BenchResult {
        make_result(BenchResultSpec {
            bench_name: bench_name.to_string(),
            scenario: scenario.to_string(),
            backend: backend.to_string(),
            layer: layer.to_string(),
            codec: codec.map(|value| value.to_string()),
            measurement_mode: measurement_mode.to_string(),
            wait_strategy: "BusySpin".to_string(),
            transport,
            message_size_bytes: msg_bytes,
            payload_bytes,
            buffer_depth,
            num_messages,
            warmup_messages,
            num_producers: 1,
            num_consumers,
            producer_throughput_ops_sec: prod_ops,
            consumer_throughput_ops_sec: cons_ops,
            latency,
        })
    }

    #[test]
    fn test_canonical_json_schema() {
        let result = make_test_result(
            "raw_ring_shm",
            "signal_1p1c_64B",
            "shm",
            "raw_ring",
            None,
            "max_throughput",
            BenchTransportSpec::benchmark_shm(1)
                .with_zero_copy(false)
                .with_framing("none"),
            64,
            64,
            65_536,
            10_000_000,
            100_000,
            1,
            207_000_000.0,
            207_000_000.0,
            None,
        );

        let json = serde_json::to_string_pretty(&result).unwrap();
        assert!(json.contains("benchmark_id"));
        assert!(json.contains("measurement_mode"));
        assert!(json.contains("message_size_bytes"));
        assert!(json.contains("producer_throughput_ops_sec"));
        assert!(json.contains("platform"));

        let _: BenchResult = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn test_metadata_capture() {
        let meta = BenchMetadata::capture();
        assert!(!meta.timestamp.is_empty());
        assert!(!meta.platform.is_empty());
    }

    #[test]
    fn test_result_with_latency() {
        let mut recorder = crate::infra::latency::LatencyRecorder::default_range();
        for _ in 0..1000 {
            recorder.record(250);
        }

        let result = make_test_result(
            "test",
            "test",
            "shm",
            "raw_ring",
            None,
            "max_throughput",
            BenchTransportSpec::benchmark_shm(1)
                .with_zero_copy(false)
                .with_framing("none"),
            64,
            64,
            1024,
            1000,
            100,
            1,
            1_000_000.0,
            1_000_000.0,
            recorder.stats(),
        );

        assert!(result.latency.is_some());
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("p50_ns"));
        assert!(json.contains("p99_ns"));
    }

    #[test]
    fn test_attach_child_metrics_populates_consumer_rollups() {
        let mut result = make_test_result(
            "raw_ring_shm",
            "message_1p2c_144B",
            "shm",
            "raw_ring",
            None,
            "max_throughput",
            BenchTransportSpec::benchmark_shm(2)
                .with_zero_copy(false)
                .with_framing("none"),
            144,
            144,
            1024,
            100_000,
            1_000,
            2,
            20_000_000.0,
            19_000_000.0,
            None,
        );

        let producer = ProducerOutput {
            throughput_ops_sec: 20_000_000.0,
            elapsed_secs: 0.005,
            events_produced: 100_000,
            bandwidth_bytes_sec: 2_880_000_000.0,
            data_rate_gbps: 2.88,
            phase_timing: None,
        };
        let consumers = vec![
            ConsumerOutput {
                consumer_id: 0,
                throughput_ops_sec: 9_000_000.0,
                bandwidth_bytes_sec: 1_296_000_000.0,
                data_rate_gbps: 1.296,
                events_consumed: 50_000,
                checksum: 123,
                latency: None,
                phase_timing: None,
            },
            ConsumerOutput {
                consumer_id: 1,
                throughput_ops_sec: 10_000_000.0,
                bandwidth_bytes_sec: 1_440_000_000.0,
                data_rate_gbps: 1.44,
                events_consumed: 50_000,
                checksum: 456,
                latency: None,
                phase_timing: None,
            },
        ];

        attach_child_metrics(&mut result, &producer, &consumers);

        assert_eq!(
            result.results.consumer_total_throughput_ops_sec,
            Some(19_000_000.0)
        );
        assert_eq!(result.results.consumer_checksum_total, Some(579));
        assert_eq!(result.results.per_consumer.len(), 2);
    }

    #[test]
    fn test_finalized_report_populates_schema_and_derived_metrics() {
        let mut raw = make_test_result(
            "raw_ring_shm",
            "message_1p2c_144B",
            "shm",
            "raw_ring",
            None,
            "max_throughput",
            BenchTransportSpec::benchmark_shm(2)
                .with_zero_copy(false)
                .with_framing("none"),
            144,
            144,
            1024,
            100_000,
            1000,
            2,
            20_000_000.0,
            20_000_000.0,
            None,
        );
        raw.results.consumer_avg_data_rate_gbps = Some(2.88);

        let mut raw_single = make_test_result(
            "raw_ring_shm",
            "message_1p1c_144B",
            "shm",
            "raw_ring",
            None,
            "max_throughput",
            BenchTransportSpec::benchmark_shm(1)
                .with_zero_copy(false)
                .with_framing("none"),
            144,
            144,
            1024,
            100_000,
            1000,
            1,
            19_000_000.0,
            19_000_000.0,
            None,
        );
        raw_single.results.consumer_avg_data_rate_gbps = Some(2.736);

        let mut bincode = make_test_result(
            "codec_e2e_shm",
            "codec_e2e_1p2c_8seq_bincode",
            "shm",
            "typed",
            Some("bincode"),
            "max_throughput",
            BenchTransportSpec::benchmark_shm(2)
                .with_zero_copy(false)
                .with_framing("fixed_64k"),
            512,
            144,
            1024,
            100_000,
            0,
            2,
            150_000.0,
            15_000.0,
            None,
        );
        bincode.results.consumer_avg_data_rate_gbps = Some(0.00216);

        let mut raw_myelon = make_test_result(
            "pingpong_raw_myelon_shm",
            "pingpong_raw_myelon_1p2c_144B",
            "shm",
            "raw_myelon",
            None,
            "max_throughput",
            BenchTransportSpec::benchmark_shm(2)
                .with_zero_copy(false)
                .with_framing("none"),
            144,
            144,
            1024,
            100_000,
            1000,
            2,
            19_600_000.0,
            19_600_000.0,
            None,
        );
        raw_myelon.results.consumer_avg_data_rate_gbps = Some(2.8224);

        let mut rkyv = make_test_result(
            "codec_e2e_shm",
            "codec_e2e_1p2c_8seq_rkyv",
            "shm",
            "typed",
            Some("rkyv"),
            "max_throughput",
            BenchTransportSpec::benchmark_shm(2)
                .with_zero_copy(false)
                .with_framing("fixed_64k"),
            384,
            144,
            1024,
            100_000,
            0,
            2,
            160_000.0,
            18_000.0,
            None,
        );
        rkyv.results.consumer_avg_data_rate_gbps = Some(0.002592);

        let mut zero_copy_rkyv = make_test_result(
            "pingpong_typed_zero_copy_shm",
            "pingpong_codec_1p1c_rkyv_b8",
            "shm",
            "typed_zero_copy",
            Some("rkyv"),
            "max_throughput",
            BenchTransportSpec::unified_pingpong()
                .with_zero_copy(true)
                .with_framing("fixed_64k"),
            384,
            144,
            1024,
            100_000,
            10_000,
            1,
            18_500.0,
            18_500.0,
            None,
        );
        zero_copy_rkyv.results.consumer_avg_data_rate_gbps = Some(0.002664);

        let mut pingpong_bincode = make_test_result(
            "pingpong_codec_shm",
            "pingpong_codec_1p1c_bincode_b8",
            "shm",
            "typed",
            Some("bincode"),
            "max_throughput",
            BenchTransportSpec::unified_pingpong()
                .with_zero_copy(false)
                .with_framing("fixed_64k"),
            512,
            144,
            1024,
            100_000,
            10_000,
            1,
            16_500.0,
            16_500.0,
            None,
        );
        pingpong_bincode.results.consumer_avg_data_rate_gbps = Some(0.002376);

        let mut report = BenchReport::new();
        report.add(raw);
        report.add(raw_single);
        report.add(raw_myelon);
        report.add(bincode);
        report.add(pingpong_bincode);
        report.add(rkyv);
        report.add(zero_copy_rkyv);

        let finalized = report.finalized();
        let raw_myelon = finalized
            .results
            .iter()
            .find(|result| result.layer == "raw_myelon")
            .unwrap();
        let bincode = finalized
            .results
            .iter()
            .find(|result| result.codec.as_deref() == Some("bincode"))
            .unwrap();
        let rkyv = finalized
            .results
            .iter()
            .find(|result| result.codec.as_deref() == Some("rkyv"))
            .unwrap();
        let zero_copy_rkyv = finalized
            .results
            .iter()
            .find(|result| result.layer == "typed_zero_copy")
            .unwrap();

        assert_eq!(
            bincode.config.coordination.as_deref(),
            Some("BenchmarkCoordination")
        );
        assert_eq!(bincode.config.discovery_mode.as_deref(), Some("enabled(2)"));
        assert_eq!(bincode.config.zero_copy, Some(false));
        assert_eq!(bincode.config.framing.as_deref(), Some("fixed_64k"));
        assert_eq!(bincode.results.speedup_vs_bincode, Some(1.0));
        assert_eq!(bincode.results.delta_vs_bincode_pct, Some(0.0));
        assert!(raw_myelon.results.pct_of_raw_ring.unwrap() > 95.0);
        assert!(raw_myelon.results.delta_vs_raw_ring_pct.unwrap() > -5.0);
        assert!(raw_myelon.results.delta_vs_raw_ring_pct.unwrap() < 0.0);
        assert!(rkyv.results.speedup_vs_bincode.unwrap() > 1.0);
        assert!(rkyv.results.delta_vs_bincode_pct.unwrap() > 0.0);
        assert!(rkyv.results.pct_of_raw_ring.unwrap() > 0.0);
        assert!(rkyv.results.delta_vs_raw_ring_pct.unwrap() < 0.0);
        assert!(rkyv.results.hw_efficiency_pct.unwrap() > 0.0);
        assert!(zero_copy_rkyv.results.speedup_vs_bincode.unwrap() > 1.0);
        assert!(zero_copy_rkyv.results.delta_vs_bincode_pct.unwrap() > 0.0);
        assert!(zero_copy_rkyv.results.pct_of_raw_ring.unwrap() > 0.0);
    }

    #[test]
    fn test_report_output_args_parse_common_flags() {
        let args = vec![
            "bench".to_string(),
            "--quick".to_string(),
            "--tree".to_string(),
            "--json-out".to_string(),
            "/tmp/out.json".to_string(),
            "--csv-out".to_string(),
            "/tmp/out.csv".to_string(),
            "--md-out".to_string(),
            "/tmp/out.md".to_string(),
        ];

        let parsed = ReportOutputArgs::from_args(&args);
        assert!(!parsed.json_mode);
        assert!(parsed.quick_mode);
        assert!(parsed.tree_mode);
        assert_eq!(parsed.json_out.as_deref(), Some("/tmp/out.json"));
        assert_eq!(parsed.csv_out.as_deref(), Some("/tmp/out.csv"));
        assert_eq!(parsed.markdown_out.as_deref(), Some("/tmp/out.md"));
    }
}
