//! Static reference data for competitor IPC libraries.
//!
//! All data here is from prior benchmarks, NOT live-measured.
//! When displayed alongside live results, it MUST be labeled
//! "reference (static)" to avoid implying a fair comparison.

/// Competitor P50 latency reference data (microseconds) by message size.
///
/// Source: perf-bench/src/bench_support/table.rs
/// Commit: original disruptor-mp bench tree
/// Hardware: varies (see source for details)
pub struct CompetitorData {
    pub name: &'static str,
    pub commit: &'static str,
    /// (message_size_bytes, p50_latency_us)
    pub latencies: &'static [(usize, f64)],
}

pub static SHMIPC_RS: CompetitorData = CompetitorData {
    name: "shmipc-rs",
    commit: "2f22b071",
    latencies: &[
        (64, 1.066),
        (512, 1.013),
        (1024, 1.051),
        (4096, 1.018),
        (16384, 1.226),
        (32768, 1.184),
        (65536, 1.254),
        (262144, 1.167),
        (524288, 1.260),
        (1048576, 2.387),
        (4194304, 4.818),
    ],
};

pub static SHMIPC_GO: CompetitorData = CompetitorData {
    name: "shmipc-go",
    commit: "a5e0aefb",
    latencies: &[
        (64, 1.970),
        (512, 1.990),
        (1024, 2.045),
        (4096, 2.063),
        (16384, 1.996),
        (32768, 1.937),
        (65536, 1.995),
        (262144, 1.793),
        (524288, 1.993),
        (1048576, 1.873),
        (4194304, 1.891),
    ],
};

impl CompetitorData {
    /// Get P50 latency for a given message size, or None if not available.
    pub fn p50_us(&self, message_size: usize) -> Option<f64> {
        self.latencies
            .iter()
            .find(|(size, _)| *size == message_size)
            .map(|(_, latency)| *latency)
    }

    /// Calculate speedup vs this competitor.
    /// Returns (multiplier, "Xfaster"/"Xslower") or None if no data.
    pub fn speedup(&self, message_size: usize, our_p50_us: f64) -> Option<(f64, String)> {
        let their = self.p50_us(message_size)?;
        let ratio = their / our_p50_us;
        if ratio >= 1.0 {
            Some((ratio, format!("{:.1}x faster", ratio)))
        } else {
            Some((ratio, format!("{:.1}x slower", 1.0 / ratio)))
        }
    }
}

/// Format a speedup comparison line.
/// Labels competitor data as "reference (static)" to be honest.
pub fn format_comparison(
    message_size: usize,
    our_p50_us: f64,
    competitor: &CompetitorData,
) -> String {
    match competitor.speedup(message_size, our_p50_us) {
        Some((_, desc)) => format!(
            "vs {} [reference, static, commit {}]: {}",
            competitor.name, competitor.commit, desc
        ),
        None => format!("vs {}: no data for {}B", competitor.name, message_size),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lookup() {
        assert_eq!(SHMIPC_RS.p50_us(64), Some(1.066));
        assert_eq!(SHMIPC_RS.p50_us(999), None);
    }

    #[test]
    fn test_speedup_faster() {
        // Our 0.25μs vs their 1.066μs = 4.3x faster
        let (ratio, desc) = SHMIPC_RS.speedup(64, 0.25).unwrap();
        assert!(ratio > 4.0);
        assert!(desc.contains("faster"));
    }

    #[test]
    fn test_speedup_slower() {
        // Our 5.0μs vs their 1.066μs = 4.7x slower
        let (ratio, desc) = SHMIPC_RS.speedup(64, 5.0).unwrap();
        assert!(ratio < 1.0);
        assert!(desc.contains("slower"));
    }

    #[test]
    fn test_format_comparison() {
        let s = format_comparison(64, 0.25, &SHMIPC_RS);
        assert!(s.contains("reference, static"));
        assert!(s.contains("shmipc-rs"));
        assert!(s.contains("faster"));
    }
}
