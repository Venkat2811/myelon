//! Structured output types for producer/consumer child processes.
//!
//! Children serialize these as JSON to stdout. The orchestrator deserializes
//! them — no more fragile `extract_value("Throughput")` line parsing.

use crate::latency::LatencyStats;
use serde::{Deserialize, Serialize};

/// What a producer child process reports back.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerOutput {
    pub throughput_ops_sec: f64,
    pub elapsed_secs: f64,
    pub events_produced: u64,
    pub bandwidth_bytes_sec: f64,
    pub data_rate_gbps: f64,
    pub phase_timing: Option<PhaseTiming>,
}

impl ProducerOutput {
    /// Construct from measured elapsed time and event count.
    pub fn from_elapsed(events: u64, elapsed: std::time::Duration, payload_bytes: usize) -> Self {
        let secs = elapsed.as_secs_f64();
        let tp = events as f64 / secs;
        let bw = tp * payload_bytes as f64;
        Self {
            throughput_ops_sec: tp,
            elapsed_secs: secs,
            events_produced: events,
            bandwidth_bytes_sec: bw,
            data_rate_gbps: bw / 1e9,
            phase_timing: None,
        }
    }
}

/// What a consumer child process reports back.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerOutput {
    pub consumer_id: usize,
    pub throughput_ops_sec: f64,
    pub events_consumed: u64,
    pub bandwidth_bytes_sec: f64,
    pub data_rate_gbps: f64,
    pub latency: Option<LatencyStats>,
    pub phase_timing: Option<PhaseTiming>,
    pub checksum: u64,
}

impl ConsumerOutput {
    /// Construct from measured elapsed time, event count, and checksum.
    pub fn from_elapsed(
        consumer_id: usize,
        events: u64,
        elapsed: std::time::Duration,
        payload_bytes: usize,
        checksum: u64,
    ) -> Self {
        let secs = elapsed.as_secs_f64();
        let tp = events as f64 / secs;
        let bw = tp * payload_bytes as f64;
        Self {
            consumer_id,
            throughput_ops_sec: tp,
            events_consumed: events,
            bandwidth_bytes_sec: bw,
            data_rate_gbps: bw / 1e9,
            latency: None,
            phase_timing: None,
            checksum,
        }
    }

    pub fn with_latency(mut self, latency: LatencyStats) -> Self {
        self.latency = Some(latency);
        self
    }

    pub fn with_phase_timing(mut self, timing: PhaseTiming) -> Self {
        self.phase_timing = Some(timing);
        self
    }
}

/// Codec phase timing breakdown.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseTiming {
    pub encode_avg_ns: Option<f64>,
    pub transport_write_avg_ns: Option<f64>,
    pub transport_read_avg_ns: Option<f64>,
    pub decode_avg_ns: Option<f64>,
}

impl PhaseTiming {
    pub fn codec_total_ns(&self) -> f64 {
        self.encode_avg_ns.unwrap_or(0.0) + self.decode_avg_ns.unwrap_or(0.0)
    }

    pub fn transport_total_ns(&self) -> f64 {
        self.transport_write_avg_ns.unwrap_or(0.0) + self.transport_read_avg_ns.unwrap_or(0.0)
    }

    pub fn codec_pct(&self) -> f64 {
        let codec = self.codec_total_ns();
        let total = codec + self.transport_total_ns();
        if total > 0.0 { codec / total * 100.0 } else { 0.0 }
    }
}
