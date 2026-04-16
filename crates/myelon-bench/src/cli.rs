//! Common CLI argument parsing for myelon-bench benchmarks.
//!
//! Every benchmark binary can use `BenchArgs` for consistent configuration.
//! Parameters not relevant to a particular benchmark are simply ignored.

use clap::Parser;
use std::time::Duration;

/// Common benchmark configuration parsed from CLI arguments.
///
/// Benchmarks parse this with `BenchArgs::parse_bench()` which filters out
/// cargo-bench arguments (`--bench`, `--test`, etc.) before parsing.
#[derive(Parser, Debug, Clone)]
#[command(about = "Myelon benchmark")]
pub struct BenchArgs {
    /// Message/event payload size in bytes.
    #[arg(long, default_value_t = 144)]
    pub message_size: usize,

    /// Ring buffer depth (number of slots).
    #[arg(long, default_value_t = 1024)]
    pub buffer_depth: usize,

    /// Number of measured messages/events (after warmup).
    #[arg(long, default_value_t = 100_000)]
    pub num_messages: u64,

    /// Number of warmup messages/events.
    #[arg(long, default_value_t = 1_000)]
    pub warmup: u64,

    /// Number of consumers (for broadcast scenarios).
    #[arg(long, default_value_t = 1)]
    pub consumers: usize,

    /// Wait strategy: busyspin, block, sleep, spinloop.
    #[arg(long, default_value = "busyspin")]
    pub wait_strategy: String,

    /// Output JSON canonical format.
    #[arg(long)]
    pub json: bool,

    /// Output JSON canonical format to this file path.
    #[arg(long)]
    pub json_out: Option<String>,

    /// Benchmark class (benchmark-specific, e.g., "signal", "message", "all").
    #[arg(long, default_value = "all")]
    pub class: String,

    /// Quick mode — run minimal subset for smoke testing.
    #[arg(long)]
    pub quick: bool,

    /// Target send rate for fixed-rate / coordinated-omission mode (msgs/sec).
    /// When set, enables CO-aware latency measurement.
    #[arg(long)]
    pub target_rate: Option<u64>,

    /// Enable batch timing mode (low-overhead, no per-message histogram).
    #[arg(long)]
    pub batch_timing: bool,

    /// Batch size for batch timing mode.
    #[arg(long, default_value_t = 1)]
    pub batch_size: usize,

    /// Codec to test (bincode, rkyv, flatbuf).
    #[arg(long, default_value = "all")]
    pub codec: String,

    /// Suppress progress output (for JSON-only runs).
    #[arg(long)]
    pub quiet: bool,
}

impl BenchArgs {
    /// Parse CLI args, filtering out cargo-bench noise (`--bench`, etc.).
    pub fn parse_bench() -> Self {
        let filtered: Vec<String> = std::env::args()
            .filter(|arg| arg != "--bench" && !arg.starts_with("--test"))
            .collect();
        Self::parse_from(filtered)
    }

    /// Parse from specific args (for testing).
    pub fn parse_from_args(args: &[&str]) -> Self {
        Self::parse_from(args)
    }

    /// Get the JSON output path from args or env var.
    pub fn json_path(&self) -> Option<String> {
        self.json_out
            .clone()
            .or_else(|| std::env::var("MYELON_BENCH_JSON_OUT").ok())
    }

    /// Get wait strategy as MyelonWaitStrategy.
    pub fn myelon_wait_strategy(&self) -> myelon::MyelonWaitStrategy {
        match self.wait_strategy.to_lowercase().as_str() {
            "block" => myelon::MyelonWaitStrategy::Block,
            _ => myelon::MyelonWaitStrategy::BusySpin,
        }
    }

    /// Discovery timeout — longer for more consumers.
    pub fn discovery_timeout(&self) -> Duration {
        Duration::from_secs(match self.consumers {
            0..=1 => 3,
            2..=4 => 5,
            5..=8 => 10,
            _ => 15,
        })
    }

    /// Coordination timeout — longer for more consumers.
    pub fn coordination_timeout(&self) -> Duration {
        Duration::from_secs(match self.consumers {
            0..=1 => 15,
            2..=4 => 20,
            5..=8 => 30,
            _ => 45,
        })
    }

    /// Run a specific class?
    pub fn should_run(&self, class: &str) -> bool {
        self.class == "all" || self.class == class
    }

    /// Run a specific codec?
    pub fn should_run_codec(&self, codec: &str) -> bool {
        self.codec == "all" || self.codec == codec
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_args() {
        let args = BenchArgs::parse_from_args(&["bench"]);
        assert_eq!(args.message_size, 144);
        assert_eq!(args.buffer_depth, 1024);
        assert_eq!(args.num_messages, 100_000);
        assert_eq!(args.consumers, 1);
        assert_eq!(args.wait_strategy, "busyspin");
        assert_eq!(args.class, "all");
        assert!(!args.quick);
        assert!(!args.json);
    }

    #[test]
    fn test_custom_args() {
        let args = BenchArgs::parse_from_args(&[
            "bench",
            "--message-size", "64",
            "--buffer-depth", "65536",
            "--num-messages", "10000000",
            "--consumers", "12",
            "--class", "signal",
            "--quick",
        ]);
        assert_eq!(args.message_size, 64);
        assert_eq!(args.buffer_depth, 65536);
        assert_eq!(args.num_messages, 10_000_000);
        assert_eq!(args.consumers, 12);
        assert_eq!(args.class, "signal");
        assert!(args.quick);
    }

    #[test]
    fn test_should_run() {
        let all = BenchArgs::parse_from_args(&["bench"]);
        assert!(all.should_run("signal"));
        assert!(all.should_run("message"));

        let signal = BenchArgs::parse_from_args(&["bench", "--class", "signal"]);
        assert!(signal.should_run("signal"));
        assert!(!signal.should_run("message"));
    }

    #[test]
    fn test_co_mode() {
        let args = BenchArgs::parse_from_args(&["bench", "--target-rate", "1000000"]);
        assert_eq!(args.target_rate, Some(1_000_000));
    }

    #[test]
    fn test_json_path_from_env() {
        std::env::set_var("MYELON_BENCH_JSON_OUT", "/tmp/test.json");
        let args = BenchArgs::parse_from_args(&["bench"]);
        assert_eq!(args.json_path(), Some("/tmp/test.json".to_string()));
        std::env::remove_var("MYELON_BENCH_JSON_OUT");
    }
}
