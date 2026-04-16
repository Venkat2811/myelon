//! Benchmark reporting — canonical JSON schema, tabled output, CSV.
//!
//! Every benchmark in myelon-bench produces `BenchResult` structs that
//! share a single canonical JSON schema. Latency comes from real HDR
//! histograms (via `latency::LatencyStats`) or is `None`.

use crate::events::format_throughput;
use crate::latency;
use serde::{Deserialize, Serialize};
use std::process::Command;

/// Canonical benchmark result. All benchmarks produce this same struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchResult {
    /// Benchmark identifier (e.g., "myelon-bench/raw_ring_shm/signal_1p1c_64B").
    pub benchmark_id: String,
    /// Scenario name (e.g., "signal_1p1c_64B").
    pub scenario: String,
    /// Transport backend: "shm" or "mmap".
    pub backend: String,
    /// Transport layer: "raw_ring", "framed", "typed".
    pub layer: String,
    /// Codec used, or null for raw transport.
    pub codec: Option<String>,
    /// Measurement mode: "max_throughput", "fixed_rate", "batch_timing".
    pub measurement_mode: String,
    /// Wait strategy used.
    pub wait_strategy: String,

    /// Configuration
    pub config: BenchConfig,

    /// Results
    pub results: BenchResults,

    /// Latency percentiles from real HDR histogram, or null if not measured.
    pub latency: Option<latency::LatencyStats>,

    /// Metadata
    pub metadata: BenchMetadata,
}

/// Benchmark configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchConfig {
    pub message_size_bytes: usize,
    pub buffer_depth: usize,
    pub num_messages: u64,
    pub warmup_messages: u64,
    pub num_producers: usize,
    pub num_consumers: usize,
}

/// Benchmark results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchResults {
    pub producer_throughput_ops_sec: f64,
    pub consumer_throughput_ops_sec: f64,
    pub data_rate_mbps: f64,
    pub messages_processed: u64,
    pub verification_passed: bool,
}

/// Benchmark metadata.
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

fn detect_cpu() -> String {
    #[cfg(target_os = "macos")]
    {
        Command::new("sysctl")
            .args(["-n", "machdep.cpu.brand_string"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| "Apple Silicon".to_string())
    }
    #[cfg(not(target_os = "macos"))]
    {
        std::fs::read_to_string("/proc/cpuinfo")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("model name"))
                    .map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string())
            })
            .unwrap_or_else(|| "unknown".to_string())
    }
}

fn detect_git_commit() -> Option<String> {
    Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
}

/// Collection of results from a benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchReport {
    pub metadata: BenchMetadata,
    pub results: Vec<BenchResult>,
}

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

    pub fn write_json(&self, path: &str) -> std::io::Result<()> {
        std::fs::write(path, serde_json::to_string_pretty(self)?)
    }

    pub fn write_csv(&self, path: &str) -> std::io::Result<()> {
        let mut csv = String::from(
            "scenario,backend,layer,codec,mode,strategy,msg_bytes,buffer,consumers,\
             prod_ops,cons_ops,mbps,p50_ns,p99_ns,p999_ns,p9999_ns,p99999_ns,verified\n",
        );
        for r in &self.results {
            let codec = r.codec.as_deref().unwrap_or("");
            let lat_field = |f: fn(&latency::LatencyStats) -> u64| -> String {
                r.latency.as_ref().map(|l| f(l).to_string()).unwrap_or_default()
            };
            csv.push_str(&format!(
                "{},{},{},{},{},{},{},{},{},{:.0},{:.0},{:.1},{},{},{},{},{},{}\n",
                r.scenario, r.backend, r.layer, codec, r.measurement_mode, r.wait_strategy,
                r.config.message_size_bytes, r.config.buffer_depth, r.config.num_consumers,
                r.results.producer_throughput_ops_sec, r.results.consumer_throughput_ops_sec,
                r.results.data_rate_mbps,
                lat_field(|l| l.p50_ns), lat_field(|l| l.p99_ns), lat_field(|l| l.p999_ns),
                lat_field(|l| l.p9999_ns), lat_field(|l| l.p99999_ns),
                r.results.verification_passed,
            ));
        }
        std::fs::write(path, csv)
    }

    /// Print a formatted summary table using `tabled`.
    pub fn print_summary(&self) {
        use tabled::{Table, Tabled};

        #[derive(Tabled)]
        struct Row {
            #[tabled(rename = "Scenario")]
            scenario: String,
            #[tabled(rename = "Bknd")]
            backend: String,
            #[tabled(rename = "Layer")]
            layer: String,
            #[tabled(rename = "Codec")]
            codec: String,
            #[tabled(rename = "Cons")]
            consumers: usize,
            #[tabled(rename = "Prod ops/s")]
            prod_ops: String,
            #[tabled(rename = "Cons ops/s")]
            cons_ops: String,
            #[tabled(rename = "P50")]
            p50: String,
            #[tabled(rename = "P99")]
            p99: String,
            #[tabled(rename = "P99.9")]
            p999: String,
            #[tabled(rename = "P99.99")]
            p9999: String,
            #[tabled(rename = "P99.999")]
            p99999: String,
        }

        let rows: Vec<Row> = self.results.iter().map(|r| {
            let lat = |f: fn(&latency::LatencyStats) -> u64| {
                r.latency.as_ref().map(|l| latency::format_ns(f(l))).unwrap_or_else(|| "-".into())
            };
            Row {
                scenario: r.scenario.clone(),
                backend: r.backend.clone(),
                layer: r.layer.clone(),
                codec: r.codec.as_deref().unwrap_or("-").to_string(),
                consumers: r.config.num_consumers,
                prod_ops: format_throughput(r.results.producer_throughput_ops_sec),
                cons_ops: format_throughput(r.results.consumer_throughput_ops_sec),
                p50: lat(|l| l.p50_ns),
                p99: lat(|l| l.p99_ns),
                p999: lat(|l| l.p999_ns),
                p9999: lat(|l| l.p9999_ns),
                p99999: lat(|l| l.p99999_ns),
            }
        }).collect();

        println!("\n{}\n", Table::new(rows));
    }

    /// Write a markdown report file using tabled with markdown style.
    pub fn write_markdown(&self, path: &str) -> std::io::Result<()> {
        use tabled::{Table, Tabled, settings::Style};

        #[derive(Tabled)]
        struct Row {
            #[tabled(rename = "Scenario")]
            scenario: String,
            #[tabled(rename = "Backend")]
            backend: String,
            #[tabled(rename = "Layer")]
            layer: String,
            #[tabled(rename = "Codec")]
            codec: String,
            #[tabled(rename = "Consumers")]
            consumers: usize,
            #[tabled(rename = "Producer ops/s")]
            prod_ops: String,
            #[tabled(rename = "Consumer ops/s")]
            cons_ops: String,
            #[tabled(rename = "P50")]
            p50: String,
            #[tabled(rename = "P99")]
            p99: String,
            #[tabled(rename = "P99.9")]
            p999: String,
            #[tabled(rename = "P99.99")]
            p9999: String,
            #[tabled(rename = "P99.999")]
            p99999: String,
        }

        let rows: Vec<Row> = self.results.iter().map(|r| {
            let lat = |f: fn(&latency::LatencyStats) -> u64| -> String {
                r.latency.as_ref().map(|l| latency::format_ns(f(l))).unwrap_or_else(|| "-".into())
            };
            Row {
                scenario: r.scenario.clone(),
                backend: r.backend.clone(),
                layer: r.layer.clone(),
                codec: r.codec.as_deref().unwrap_or("-").to_string(),
                consumers: r.config.num_consumers,
                prod_ops: format_throughput(r.results.producer_throughput_ops_sec),
                cons_ops: format_throughput(r.results.consumer_throughput_ops_sec),
                p50: lat(|l| l.p50_ns),
                p99: lat(|l| l.p99_ns),
                p999: lat(|l| l.p999_ns),
                p9999: lat(|l| l.p9999_ns),
                p99999: lat(|l| l.p99999_ns),
            }
        }).collect();

        #[derive(Tabled)]
        struct ConfigRow {
            #[tabled(rename = "Scenario")]
            scenario: String,
            #[tabled(rename = "Payload")]
            payload: String,
            #[tabled(rename = "Buffer Depth")]
            buffer: usize,
            #[tabled(rename = "Events")]
            events: u64,
            #[tabled(rename = "Warmup")]
            warmup: u64,
        }

        let config_rows: Vec<ConfigRow> = self.results.iter().map(|r| {
            let size = if r.config.message_size_bytes >= 1_048_576 {
                format!("{}MB", r.config.message_size_bytes / 1_048_576)
            } else if r.config.message_size_bytes >= 1024 {
                format!("{}KB", r.config.message_size_bytes / 1024)
            } else {
                format!("{}B", r.config.message_size_bytes)
            };
            ConfigRow {
                scenario: r.scenario.clone(),
                payload: size,
                buffer: r.config.buffer_depth,
                events: r.config.num_messages,
                warmup: r.config.warmup_messages,
            }
        }).collect();

        let results_table = Table::new(rows).with(Style::markdown()).to_string();
        let config_table = Table::new(config_rows).with(Style::markdown()).to_string();

        let mut md = String::new();
        md.push_str("# Benchmark Report\n\n");
        md.push_str(&format!("- **Platform**: {}\n", self.metadata.platform));
        md.push_str(&format!("- **CPU**: {}\n", self.metadata.cpu));
        md.push_str(&format!("- **Timestamp**: {}\n", self.metadata.timestamp));
        if let Some(ref commit) = self.metadata.git_commit {
            md.push_str(&format!("- **Git Commit**: `{}`\n", commit));
        }
        md.push_str("\n## Results\n\n");
        md.push_str(&results_table);
        md.push_str("\n\n## Configuration\n\n");
        md.push_str(&config_table);
        md.push('\n');

        std::fs::write(path, md)
    }
}

impl Default for BenchReport {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to build a BenchResult with common defaults.
pub fn make_result(
    bench_name: &str,
    scenario: &str,
    backend: &str,
    layer: &str,
    codec: Option<&str>,
    wait_strategy: &str,
    msg_bytes: usize,
    buffer_depth: usize,
    num_messages: u64,
    warmup: u64,
    consumers: usize,
    prod_ops: f64,
    cons_ops: f64,
    latency: Option<latency::LatencyStats>,
) -> BenchResult {
    BenchResult {
        benchmark_id: format!("myelon-bench/{bench_name}/{scenario}"),
        scenario: scenario.to_string(),
        backend: backend.to_string(),
        layer: layer.to_string(),
        codec: codec.map(|s| s.to_string()),
        measurement_mode: "max_throughput".to_string(),
        wait_strategy: wait_strategy.to_string(),
        config: BenchConfig {
            message_size_bytes: msg_bytes,
            buffer_depth,
            num_messages,
            warmup_messages: warmup,
            num_producers: 1,
            num_consumers: consumers,
        },
        results: BenchResults {
            producer_throughput_ops_sec: prod_ops,
            consumer_throughput_ops_sec: cons_ops,
            data_rate_mbps: crate::events::data_rate_mbps(prod_ops, msg_bytes),
            messages_processed: num_messages,
            verification_passed: true,
        },
        latency,
        metadata: BenchMetadata::capture(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonical_json_schema() {
        let result = make_result(
            "raw_ring_shm", "signal_1p1c_64B", "shm", "raw_ring",
            None, "BusySpin", 64, 65536, 10_000_000, 100_000, 1,
            207_000_000.0, 207_000_000.0, None,
        );
        let json = serde_json::to_string_pretty(&result).unwrap();
        // Verify schema fields exist
        assert!(json.contains("benchmark_id"));
        assert!(json.contains("measurement_mode"));
        assert!(json.contains("config"));
        assert!(json.contains("message_size_bytes"));
        assert!(json.contains("results"));
        assert!(json.contains("producer_throughput_ops_sec"));
        assert!(json.contains("metadata"));
        assert!(json.contains("platform"));
        // Round-trip
        let _: BenchResult = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn test_report_csv() {
        let mut report = BenchReport::new();
        report.add(make_result(
            "test", "test_scenario", "shm", "raw_ring",
            None, "BusySpin", 144, 1024, 100_000, 1_000, 1,
            25_000_000.0, 25_000_000.0, None,
        ));
        let dir = std::env::temp_dir();
        let path = dir.join("myelon_bench_test.csv");
        report.write_csv(path.to_str().unwrap()).unwrap();
        let csv = std::fs::read_to_string(&path).unwrap();
        assert!(csv.contains("test_scenario"));
        assert!(csv.contains("shm"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_metadata_capture() {
        let meta = BenchMetadata::capture();
        assert!(!meta.timestamp.is_empty());
        assert!(!meta.platform.is_empty());
    }

    #[test]
    fn test_result_with_latency() {
        let mut recorder = crate::latency::LatencyRecorder::default_range();
        for _ in 0..1000 {
            recorder.record(250);
        }
        let result = make_result(
            "test", "test", "shm", "raw_ring", None, "BusySpin",
            64, 1024, 1000, 100, 1, 1_000_000.0, 1_000_000.0,
            recorder.stats(),
        );
        assert!(result.latency.is_some());
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("p50_ns"));
        assert!(json.contains("p99_ns"));
    }
}
