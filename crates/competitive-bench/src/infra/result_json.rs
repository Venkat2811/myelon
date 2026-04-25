use hdrhistogram::Histogram;
use perf_bench::infra::latency::LatencyStats;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfigOut {
    pub message_size: usize,
    pub num_messages: u64,
    pub warmup_messages: u64,
    pub buffer_size: usize,
    pub wait_strategy: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumers: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyStatsOut {
    pub count: u64,
    pub min: u64,
    pub max: u64,
    pub mean: f64,
    pub stdev: f64,
    pub p1: u64,
    pub p10: u64,
    pub p25: u64,
    pub p50: u64,
    pub p90: u64,
    pub p95: u64,
    pub p99: u64,
    pub p999: u64,
    pub p9999: u64,
    pub p99999: u64,
    pub p999999: u64,
}

impl From<&LatencyStats> for LatencyStatsOut {
    fn from(stats: &LatencyStats) -> Self {
        Self {
            count: stats.count,
            min: stats.min_ns,
            max: stats.max_ns,
            mean: stats.mean_ns,
            stdev: stats.stdev_ns,
            p1: stats.p1_ns,
            p10: stats.p10_ns,
            p25: stats.p25_ns,
            p50: stats.p50_ns,
            p90: stats.p90_ns,
            p95: stats.p95_ns,
            p99: stats.p99_ns,
            p999: stats.p999_ns,
            p9999: stats.p9999_ns,
            p99999: stats.p99999_ns,
            p999999: stats.p999999_ns,
        }
    }
}

impl From<&Histogram<u64>> for LatencyStatsOut {
    fn from(stats: &Histogram<u64>) -> Self {
        Self {
            count: stats.len(),
            min: stats.min(),
            max: stats.max(),
            mean: stats.mean(),
            stdev: stats.stdev(),
            p1: stats.value_at_percentile(1.0),
            p10: stats.value_at_percentile(10.0),
            p25: stats.value_at_percentile(25.0),
            p50: stats.value_at_percentile(50.0),
            p90: stats.value_at_percentile(90.0),
            p95: stats.value_at_percentile(95.0),
            p99: stats.value_at_percentile(99.0),
            p999: stats.value_at_percentile(99.9),
            p9999: stats.value_at_percentile(99.99),
            p99999: stats.value_at_percentile(99.999),
            p999999: stats.value_at_percentile(99.9999),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResultsOut {
    pub adapter: String,
    pub family: String,
    pub config: BenchmarkConfigOut,
    pub throughput: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fanout_throughput: Option<f64>,
    pub messages_processed: u64,
    pub duration_secs: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_duration_secs: Option<f64>,
    pub latency_stats: LatencyStatsOut,
    pub timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_passed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measurement_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_rate: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coordinated_omission_stats: Option<LatencyStatsOut>,
}
