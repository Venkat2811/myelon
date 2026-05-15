//! Common utilities for competitive benchmarks
//!
//! Provides shared structures and utilities for ping-pong, echo, and broadcast benchmarks.

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// Standard benchmark event with configurable payload size
#[repr(C, align(64))] // Cache line aligned to avoid false sharing
#[derive(Clone, Copy)]
pub struct BenchmarkEvent<const SIZE: usize> {
    pub sequence: u64,
    pub timestamp_ns: u64,
    pub intended_send_time_ns: u64,
    // Pad header to full cache line - remaining 40 bytes
    _pad: [u8; 40],
    pub payload: [u8; SIZE],
}

impl<const SIZE: usize> Default for BenchmarkEvent<SIZE> {
    fn default() -> Self {
        Self {
            sequence: 0,
            timestamp_ns: 0,
            intended_send_time_ns: 0,
            _pad: [0u8; 40],
            payload: [0u8; SIZE],
        }
    }
}

impl<const SIZE: usize> BenchmarkEvent<SIZE> {
    /// Create a new event with the given sequence number
    pub fn new(sequence: u64) -> Self {
        Self {
            sequence,
            timestamp_ns: nanos_now(),
            intended_send_time_ns: 0,
            _pad: [0u8; 40],
            payload: [0u8; SIZE],
        }
    }

    /// Set timestamp to current time
    pub fn set_timestamp(&mut self) {
        self.timestamp_ns = nanos_now();
    }
}

/// Get current time in nanoseconds since UNIX epoch
pub fn nanos_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// Configuration for benchmarks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    pub message_size: usize,
    pub num_messages: u64,
    pub warmup_messages: u64,
    pub buffer_size: usize,
    pub wait_strategy: String,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            message_size: 128,
            num_messages: 100_000,
            warmup_messages: 10_000,
            buffer_size: 65_536,
            wait_strategy: "Sleep".to_string(),
        }
    }
}

/// Results from a benchmark run
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResults {
    pub config: BenchmarkConfig,
    pub throughput: f64,
    pub messages_processed: u64,
    pub duration_secs: f64,
    pub latency_stats: LatencyStats,
    pub timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_passed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sent_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measurement_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_rate: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coordinated_omission_stats: Option<LatencyStats>,
}

/// Latency statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyStats {
    pub count: u64,
    pub min: u64,
    pub max: u64,
    pub mean: f64,
    pub stdev: f64,
    pub p50: u64,
    pub p90: u64,
    pub p95: u64,
    pub p99: u64,
    pub p999: u64,
    pub p9999: u64,
}

impl LatencyStats {
    /// Create stats from an `HdrHistogram`
    pub fn from_histogram(hist: &Histogram<u64>) -> Self {
        Self {
            count: hist.len(),
            min: hist.min(),
            max: hist.max(),
            mean: hist.mean(),
            stdev: hist.stdev(),
            p50: hist.value_at_percentile(50.0),
            p90: hist.value_at_percentile(90.0),
            p95: hist.value_at_percentile(95.0),
            p99: hist.value_at_percentile(99.0),
            p999: hist.value_at_percentile(99.9),
            p9999: hist.value_at_percentile(99.99),
        }
    }

    /// Print formatted latency report
    pub fn print_report(&self, unit: &str) {
        println!("Count:  {}", self.count);
        println!("Min:    {} {}", self.min, unit);
        println!("Max:    {} {}", self.max, unit);
        println!("Mean:   {:.2} {}", self.mean, unit);
        println!("StdDev: {:.2} {}", self.stdev, unit);
        println!("\nPercentiles:");
        println!("  50%:   {} {}", self.p50, unit);
        println!("  90%:   {} {}", self.p90, unit);
        println!("  95%:   {} {}", self.p95, unit);
        println!("  99%:   {} {}", self.p99, unit);
        println!("  99.9%: {} {}", self.p999, unit);
        println!("  99.99%: {} {}", self.p9999, unit);
    }
}

/// Format throughput for display
pub fn format_throughput(throughput: f64) -> String {
    if throughput >= 1_000_000.0 {
        format!("{:.2}M", throughput / 1_000_000.0)
    } else if throughput >= 1_000.0 {
        format!("{:.2}K", throughput / 1_000.0)
    } else {
        format!("{:.0}", throughput)
    }
}

/// Calculate data rate in GB/s
pub fn calculate_data_rate_gbps(throughput: f64, message_size: usize) -> f64 {
    (throughput * message_size as f64) / (1024.0 * 1024.0 * 1024.0)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_benchmark_event() {
        let event: super::BenchmarkEvent<64> = super::BenchmarkEvent::new(42);
        assert_eq!(event.sequence, 42);
        assert!(event.timestamp_ns > 0);
    }

    #[test]
    fn test_format_functions() {
        assert_eq!(super::format_throughput(1_500_000.0), "1.50M");
        assert_eq!(super::format_throughput(500.0), "500");
    }
}
