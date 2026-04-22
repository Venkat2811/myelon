//! Real per-event latency measurement via HDR histograms.
//!
//! Every latency percentile in myelon-bench comes from a real HDR histogram
//! populated by per-event timestamp deltas. Never estimated from averages.
//! If latency wasn't measured, it's `None` — never synthesized.

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

/// Latency percentiles from a real HDR histogram.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyStats {
    pub count: u64,
    pub min_ns: u64,
    pub max_ns: u64,
    pub mean_ns: f64,
    pub stdev_ns: f64,
    pub p1_ns: u64,
    pub p10_ns: u64,
    pub p25_ns: u64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub p9999_ns: u64,
    pub p99999_ns: u64,
    pub p999999_ns: u64,
}

/// Records per-event latency values and produces real percentiles.
///
/// Backed by an HDR histogram with 3 significant digits of precision
/// and a configurable maximum trackable value.
pub struct LatencyRecorder {
    histogram: Histogram<u64>,
}

impl LatencyRecorder {
    /// Create a new recorder.
    ///
    /// `max_value_ns` is the highest latency value that can be recorded
    /// without saturation. Typically 1-10 seconds (1_000_000_000 - 10_000_000_000).
    pub fn new(max_value_ns: u64) -> Self {
        Self {
            histogram: Histogram::new_with_max(max_value_ns, 3)
                .expect("HDR histogram creation should not fail"),
        }
    }

    /// Create a recorder suitable for most benchmarks (max 10 seconds).
    pub fn default_range() -> Self {
        Self::new(10_000_000_000) // 10 seconds
    }

    /// Record a single latency value in nanoseconds.
    #[inline]
    pub fn record(&mut self, nanos: u64) {
        // Saturate at max rather than panic on overflow
        let _ = self.histogram.record(nanos);
    }

    /// Record a latency computed from send and receive timestamps.
    #[inline]
    pub fn record_delta(&mut self, send_ns: u64, recv_ns: u64) {
        if recv_ns > send_ns {
            self.record(recv_ns - send_ns);
        }
    }

    /// Number of values recorded.
    pub fn count(&self) -> u64 {
        self.histogram.len()
    }

    /// Extract percentile statistics. Returns `None` if no values recorded.
    pub fn stats(&self) -> Option<LatencyStats> {
        if self.histogram.is_empty() {
            return None;
        }
        Some(LatencyStats {
            count: self.histogram.len(),
            min_ns: self.histogram.min(),
            max_ns: self.histogram.max(),
            mean_ns: self.histogram.mean(),
            stdev_ns: self.histogram.stdev(),
            p1_ns: self.histogram.value_at_percentile(1.0),
            p10_ns: self.histogram.value_at_percentile(10.0),
            p25_ns: self.histogram.value_at_percentile(25.0),
            p50_ns: self.histogram.value_at_percentile(50.0),
            p90_ns: self.histogram.value_at_percentile(90.0),
            p95_ns: self.histogram.value_at_percentile(95.0),
            p99_ns: self.histogram.value_at_percentile(99.0),
            p999_ns: self.histogram.value_at_percentile(99.9),
            p9999_ns: self.histogram.value_at_percentile(99.99),
            p99999_ns: self.histogram.value_at_percentile(99.999),
            p999999_ns: self.histogram.value_at_percentile(99.9999),
        })
    }

    /// Reset the histogram for reuse.
    pub fn reset(&mut self) {
        self.histogram.reset();
    }

    /// Merge another recorder's data into this one.
    pub fn merge(&mut self, other: &LatencyRecorder) {
        let _ = self.histogram.add(&other.histogram);
    }
}

impl LatencyStats {
    /// Format as a compact one-line summary.
    pub fn summary(&self) -> String {
        format!(
            "P1={} P10={} P25={} P50={} P90={} P95={} P99={} P99.9={} P99.99={} P99.999={} P99.9999={} mean={:.0} min={} max={} (n={})",
            format_ns(self.p1_ns),
            format_ns(self.p10_ns),
            format_ns(self.p25_ns),
            format_ns(self.p50_ns),
            format_ns(self.p90_ns),
            format_ns(self.p95_ns),
            format_ns(self.p99_ns),
            format_ns(self.p999_ns),
            format_ns(self.p9999_ns),
            format_ns(self.p99999_ns),
            format_ns(self.p999999_ns),
            self.mean_ns,
            format_ns(self.min_ns),
            format_ns(self.max_ns),
            self.count,
        )
    }
}

/// Format nanoseconds for display: ns, μs, or ms.
pub fn format_ns(ns: u64) -> String {
    if ns < 1_000 {
        format!("{}ns", ns)
    } else if ns < 1_000_000 {
        format!("{:.1}μs", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.2}s", ns as f64 / 1_000_000_000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_recording() {
        let mut recorder = LatencyRecorder::default_range();
        for i in 0..1000 {
            recorder.record(i * 100); // 0ns to 99.9μs
        }
        let stats = recorder.stats().unwrap();
        assert_eq!(stats.count, 1000);
        assert!(stats.p50_ns > 0);
        assert!(stats.p99_ns > stats.p50_ns);
        assert!(stats.max_ns >= stats.p99_ns);
    }

    #[test]
    fn test_empty_returns_none() {
        let recorder = LatencyRecorder::default_range();
        assert!(recorder.stats().is_none());
    }

    #[test]
    fn test_record_delta() {
        let mut recorder = LatencyRecorder::default_range();
        recorder.record_delta(1000, 2000); // 1000ns
        recorder.record_delta(1000, 3000); // 2000ns
        let stats = recorder.stats().unwrap();
        assert_eq!(stats.count, 2);
        assert!(stats.min_ns >= 1000);
        assert!(stats.max_ns <= 2100); // HDR precision allows slight rounding
    }

    #[test]
    fn test_merge() {
        let mut a = LatencyRecorder::default_range();
        let mut b = LatencyRecorder::default_range();
        for i in 0..500 {
            a.record(i * 100);
        }
        for i in 500..1000 {
            b.record(i * 100);
        }
        a.merge(&b);
        assert_eq!(a.count(), 1000);
    }

    #[test]
    fn test_format_ns() {
        assert_eq!(format_ns(42), "42ns");
        assert_eq!(format_ns(1500), "1.5μs");
        assert_eq!(format_ns(2_500_000), "2.50ms");
        assert_eq!(format_ns(1_500_000_000), "1.50s");
    }

    #[test]
    fn test_stats_summary() {
        let mut recorder = LatencyRecorder::default_range();
        for _ in 0..10000 {
            recorder.record(250); // 250ns each
        }
        let stats = recorder.stats().unwrap();
        let summary = stats.summary();
        assert!(summary.contains("P50="));
        assert!(summary.contains("P99="));
        assert!(summary.contains("n=10000"));
    }

    #[test]
    fn test_serialization() {
        let mut recorder = LatencyRecorder::default_range();
        recorder.record(100);
        recorder.record(200);
        let stats = recorder.stats().unwrap();
        let json = serde_json::to_string(&stats).unwrap();
        assert!(json.contains("p50_ns"));
        let _: LatencyStats = serde_json::from_str(&json).unwrap();
    }
}
