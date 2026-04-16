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

// --- Competitor-scale payloads (matching disruptor-mp/benches/competitor/) ---

/// 8KB payload — basic batch scheduling metadata.
pub type CompetitorSmall = BenchEvent<{ 8 * 1024 - 16 }>;
/// 16KB payload — KV cache coordination.
pub type CompetitorMedium = BenchEvent<{ 16 * 1024 - 16 }>;
/// 32KB payload — complex batch metadata.
pub type CompetitorLarge = BenchEvent<{ 32 * 1024 - 16 }>;
/// 64KB payload — token embedding coordination.
pub type CompetitorXLarge = BenchEvent<{ 64 * 1024 - 16 }>;
/// 96KB payload — production batch coordination.
pub type CompetitorProduction = BenchEvent<{ 96 * 1024 - 16 }>;
/// 128KB payload — peak coordination scenarios.
pub type CompetitorPeak = BenchEvent<{ 128 * 1024 - 16 }>;

// --- Competitive ping-pong event (with intended_send_time for CO) ---

/// Ping-pong event for competitive benchmark with coordinated omission support.
///
/// Cache-line-aligned header (64 bytes) + payload.
/// `intended_send_time_ns` enables coordinated omission measurement:
/// latency = recv_time - intended_send_time (not actual_send_time).
#[repr(C, align(64))]
#[derive(Clone, Copy)]
pub struct CompetitiveEvent<const SIZE: usize> {
    pub sequence: u64,
    pub timestamp_ns: u64,
    pub intended_send_time_ns: u64,
    pub _pad: [u8; 40], // pad header to full cache line (64 bytes)
    pub payload: [u8; SIZE],
}

impl<const SIZE: usize> Default for CompetitiveEvent<SIZE> {
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

/// Standard competitive event sizes (matching disruptor-mp/benches/ipc/competitive/)
pub type Competitive64 = CompetitiveEvent<0>; // 64-byte header only, no payload
pub type Competitive512 = CompetitiveEvent<448>; // 64 + 448 = 512
pub type Competitive1K = CompetitiveEvent<960>; // 64 + 960 = 1024
pub type Competitive4K = CompetitiveEvent<{ 4096 - 64 }>;
pub type Competitive16K = CompetitiveEvent<{ 16384 - 64 }>;
pub type Competitive64K = CompetitiveEvent<{ 65536 - 64 }>;
pub type Competitive128K = CompetitiveEvent<{ 131072 - 64 }>;

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
    fn test_competitor_event_sizes() {
        // All Competitor events should be at their target size (rounded up to 64-byte alignment)
        assert!(std::mem::size_of::<CompetitorSmall>() >= 8 * 1024);
        assert!(std::mem::size_of::<CompetitorMedium>() >= 16 * 1024);
        assert!(std::mem::size_of::<CompetitorLarge>() >= 32 * 1024);
        assert!(std::mem::size_of::<CompetitorProduction>() >= 96 * 1024);
        assert!(std::mem::size_of::<CompetitorPeak>() >= 128 * 1024);
    }

    #[test]
    fn test_competitive_event_sizes() {
        assert_eq!(std::mem::size_of::<Competitive64>(), 64);
        assert_eq!(std::mem::size_of::<Competitive512>(), 512);
        assert_eq!(std::mem::size_of::<Competitive1K>(), 1024);
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
