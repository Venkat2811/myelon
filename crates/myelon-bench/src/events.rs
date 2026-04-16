//! Benchmark event types and payload generators.

/// Cache-line-aligned event for raw ring benchmarks.
///
/// The alignment prevents false sharing between adjacent slots.
/// `SIZE` is the payload bytes (e.g., 64, 128, 256).
#[repr(C, align(64))]
#[derive(Clone, Copy)]
pub struct BenchEvent<const SIZE: usize> {
    /// Monotonic sequence number set by producer.
    pub sequence: u64,
    /// Timestamp in nanoseconds (for latency measurement).
    pub timestamp_ns: u64,
    /// Payload bytes.
    pub payload: [u8; SIZE],
}

impl<const SIZE: usize> Default for BenchEvent<SIZE> {
    fn default() -> Self {
        Self {
            sequence: 0,
            timestamp_ns: 0,
            payload: [0u8; SIZE],
        }
    }
}

/// Standard event sizes used across benchmarks.
pub type Event64 = BenchEvent<48>; // 8 + 8 + 48 = 64 bytes
pub type Event144 = BenchEvent<128>; // 8 + 8 + 128 = 144 bytes (matches existing disruptor-mp bench)
pub type Event256 = BenchEvent<240>; // 8 + 8 + 240 = 256 bytes
pub type Event1K = BenchEvent<1008>; // 8 + 8 + 1008 = 1024 bytes
pub type Event4K = BenchEvent<4080>; // 8 + 8 + 4080 = 4096 bytes

/// Current time in nanoseconds since UNIX epoch.
#[inline]
pub fn nanos_now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos() as u64
}

/// Format a throughput number for display.
pub fn format_throughput(ops_per_sec: f64) -> String {
    if ops_per_sec >= 1_000_000.0 {
        format!("{:.2}M", ops_per_sec / 1_000_000.0)
    } else if ops_per_sec >= 1_000.0 {
        format!("{:.1}K", ops_per_sec / 1_000.0)
    } else {
        format!("{:.0}", ops_per_sec)
    }
}

/// Calculate data rate in MB/sec.
pub fn data_rate_mbps(ops_per_sec: f64, payload_bytes: usize) -> f64 {
    ops_per_sec * payload_bytes as f64 / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_sizes() {
        assert_eq!(std::mem::size_of::<Event64>(), 64);
        assert_eq!(std::mem::size_of::<Event144>(), 192); // 144 rounded up to 64-byte alignment
        assert_eq!(std::mem::size_of::<Event256>(), 256);
    }

    #[test]
    fn test_format_throughput() {
        assert_eq!(format_throughput(1_500_000.0), "1.50M");
        assert_eq!(format_throughput(50_000.0), "50.0K");
        assert_eq!(format_throughput(500.0), "500");
    }

    #[test]
    fn test_nanos_now() {
        let t1 = nanos_now();
        let t2 = nanos_now();
        assert!(t2 >= t1);
    }
}
