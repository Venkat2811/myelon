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

// --- Large payload event aliases ---

/// 8KB payload.
pub type Event8K = BenchEvent<{ 8 * 1024 - 16 }>;
/// 16KB payload.
pub type Event16K = BenchEvent<{ 16 * 1024 - 16 }>;
/// 32KB payload.
pub type Event32K = BenchEvent<{ 32 * 1024 - 16 }>;
/// 64KB payload.
pub type Event64K = BenchEvent<{ 64 * 1024 - 16 }>;
/// 96KB payload.
pub type Event96K = BenchEvent<{ 96 * 1024 - 16 }>;
/// 128KB payload.
pub type Event128K = BenchEvent<{ 128 * 1024 - 16 }>;

// --- Ping-pong event (with intended_send_time for CO) ---

/// Ping-pong event with coordinated omission support.
///
/// Cache-line-aligned header (64 bytes) + payload.
/// `intended_send_time_ns` enables coordinated omission measurement:
/// latency = recv_time - intended_send_time (not actual_send_time).
#[repr(C, align(64))]
#[derive(Clone, Copy)]
pub struct PingPongEvent<const SIZE: usize> {
    pub sequence: u64,
    pub timestamp_ns: u64,
    pub intended_send_time_ns: u64,
    pub _pad: [u8; 40], // pad header to full cache line (64 bytes)
    pub payload: [u8; SIZE],
}

impl<const SIZE: usize> Default for PingPongEvent<SIZE> {
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

/// Standard ping-pong event sizes (matching perf-bench/src/layers/raw/)
pub type PingPong64 = PingPongEvent<0>; // 64-byte header only, no payload
pub type PingPong512 = PingPongEvent<448>; // 64 + 448 = 512
pub type PingPong1K = PingPongEvent<960>; // 64 + 960 = 1024
pub type PingPong4K = PingPongEvent<{ 4096 - 64 }>;
pub type PingPong16K = PingPongEvent<{ 16384 - 64 }>;
pub type PingPong64K = PingPongEvent<{ 65536 - 64 }>;
pub type PingPong128K = PingPongEvent<{ 131072 - 64 }>;

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
    fn test_large_event_sizes() {
        // All large events should be at their target size (rounded up to 64-byte alignment)
        assert!(std::mem::size_of::<Event8K>() >= 8 * 1024);
        assert!(std::mem::size_of::<Event16K>() >= 16 * 1024);
        assert!(std::mem::size_of::<Event32K>() >= 32 * 1024);
        assert!(std::mem::size_of::<Event96K>() >= 96 * 1024);
        assert!(std::mem::size_of::<Event128K>() >= 128 * 1024);
    }

    #[test]
    fn test_pingpong_event_sizes() {
        assert_eq!(std::mem::size_of::<PingPong64>(), 64);
        assert_eq!(std::mem::size_of::<PingPong512>(), 512);
        assert_eq!(std::mem::size_of::<PingPong1K>(), 1024);
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
