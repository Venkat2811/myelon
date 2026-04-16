//! Benchmark result types and JSON output.

use serde::{Deserialize, Serialize};

/// Standard benchmark result with JSON serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchResult {
    /// Human-readable scenario name (e.g., "1p1c_144B").
    pub scenario: String,
    /// Transport backend: "shm" or "mmap".
    pub backend: String,
    /// Transport layer: "raw_ring", "framed", "typed".
    pub layer: String,
    /// Codec used: "bincode", "rkyv", "flatbuf", or null for raw.
    pub codec: Option<String>,
    /// Wait strategy used.
    pub wait_strategy: String,
    /// Number of consumers.
    pub num_consumers: usize,
    /// Payload size in bytes.
    pub payload_bytes: usize,
    /// Number of events in the measured phase.
    pub events: usize,
    /// Producer throughput (events/sec).
    pub producer_ops_sec: f64,
    /// Consumer throughput (events/sec, average across all consumers).
    pub consumer_ops_sec: f64,
    /// Data rate in MB/sec (producer side).
    pub data_rate_mbps: f64,
    /// Latency percentiles in nanoseconds (if measured).
    pub latency: Option<LatencyStats>,
}

/// Latency percentiles from an HDR histogram.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyStats {
    pub p50_ns: f64,
    pub p90_ns: f64,
    pub p95_ns: f64,
    pub p99_ns: f64,
    pub p999_ns: f64,
    pub p9999_ns: f64,
    pub min_ns: f64,
    pub max_ns: f64,
    pub mean_ns: f64,
}

/// A collection of results for a benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchReport {
    /// ISO 8601 timestamp.
    pub timestamp: String,
    /// OS/arch description.
    pub platform: String,
    /// Git commit hash (if available).
    pub git_commit: Option<String>,
    /// Individual results.
    pub results: Vec<BenchResult>,
}

impl BenchReport {
    /// Create a new report with current metadata.
    pub fn new() -> Self {
        Self {
            timestamp: chrono::Utc::now().to_rfc3339(),
            platform: format!(
                "{} {} {}",
                std::env::consts::OS,
                std::env::consts::ARCH,
                std::env::consts::FAMILY,
            ),
            git_commit: None,
            results: Vec::new(),
        }
    }

    /// Add a result.
    pub fn add(&mut self, result: BenchResult) {
        self.results.push(result);
    }

    /// Write JSON to a file path.
    pub fn write_json(&self, path: &str) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)
    }

    /// Print a summary table to stdout.
    pub fn print_summary(&self) {
        println!(
            "\n{:<30} {:>8} {:>8} {:>8} {:>12} {:>12} {:>10}",
            "Scenario", "Backend", "Layer", "Codec", "Prod ops/s", "Cons ops/s", "P50 (ns)"
        );
        println!("{}", "-".repeat(100));
        for r in &self.results {
            let codec = r.codec.as_deref().unwrap_or("-");
            let p50 = r
                .latency
                .as_ref()
                .map(|l| format!("{:.0}", l.p50_ns))
                .unwrap_or_else(|| "-".to_string());
            println!(
                "{:<30} {:>8} {:>8} {:>8} {:>12.0} {:>12.0} {:>10}",
                r.scenario, r.backend, r.layer, codec, r.producer_ops_sec, r.consumer_ops_sec, p50
            );
        }
        println!();
    }
}

impl Default for BenchReport {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bench_result_serializes() {
        let result = BenchResult {
            scenario: "1p1c_144B".to_string(),
            backend: "shm".to_string(),
            layer: "raw_ring".to_string(),
            codec: None,
            wait_strategy: "BusySpin".to_string(),
            num_consumers: 1,
            payload_bytes: 144,
            events: 1_000_000,
            producer_ops_sec: 14_000_000.0,
            consumer_ops_sec: 14_000_000.0,
            data_rate_mbps: 2016.0,
            latency: Some(LatencyStats {
                p50_ns: 70.0,
                p90_ns: 85.0,
                p95_ns: 95.0,
                p99_ns: 120.0,
                p999_ns: 200.0,
                p9999_ns: 500.0,
                min_ns: 50.0,
                max_ns: 1000.0,
                mean_ns: 75.0,
            }),
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("1p1c_144B"));
        assert!(json.contains("14000000"));

        let deserialized: BenchResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.scenario, "1p1c_144B");
    }

    #[test]
    fn test_bench_report_new() {
        let report = BenchReport::new();
        assert!(!report.timestamp.is_empty());
        assert!(report.results.is_empty());
    }
}
