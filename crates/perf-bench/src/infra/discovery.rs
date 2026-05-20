//! Shared discovery warmup helpers for SHM benchmarks.
//!
//! Several raw and sweep benchmarks need the same post-attach discovery scan
//! loop before entering the measured phase. Keeping it here prevents the
//! benchmark suite from drifting into slightly different local copies.

use std::time::Duration;

/// Sleep interval between discovery scans.
pub const DISCOVERY_SCAN_SLEEP: Duration = Duration::from_millis(150);

/// Number of rounds to spend warming discovery state.
pub fn discovery_scan_rounds(num_consumers: usize) -> usize {
    if num_consumers > 1 {
        8 + num_consumers
    } else {
        8
    }
}

/// Run repeated discovery scans with a fixed sleep between attempts.
pub fn warm_discovery_scans<F>(mut scan: F, rounds: usize)
where
    F: FnMut() -> i64,
{
    for _ in 0..rounds {
        let _ = scan();
        std::thread::sleep(DISCOVERY_SCAN_SLEEP);
    }
}

#[cfg(test)]
mod tests {
    use super::discovery_scan_rounds;

    #[test]
    fn rounds_scale_with_multi_consumer_topologies() {
        assert_eq!(discovery_scan_rounds(1), 8);
        assert_eq!(discovery_scan_rounds(4), 12);
        assert_eq!(discovery_scan_rounds(8), 16);
    }
}
