use crate::latency;
use crate::report_v2::BackendKind;
use crate::reporting::{self, BenchReport};
use clap::Parser;
use std::env;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompetitiveBackend {
    Shm,
    Mmap,
}

impl CompetitiveBackend {
    pub fn title(self) -> &'static str {
        match self {
            Self::Shm => "Competitive SHM Ping-Pong",
            Self::Mmap => "Competitive MMAP Ping-Pong",
        }
    }

    pub fn bench_name(self) -> &'static str {
        match self {
            Self::Shm => "competitive_shm",
            Self::Mmap => "competitive_mmap",
        }
    }

    pub fn backend_name(self) -> &'static str {
        match self {
            Self::Shm => "shm",
            Self::Mmap => "mmap",
        }
    }

    pub fn backend_kind(self) -> BackendKind {
        match self {
            Self::Shm => BackendKind::Shm,
            Self::Mmap => BackendKind::Mmap,
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct CompetitiveArgs {
    /// Message size in bytes
    #[arg(short = 's', long, default_value = "64")]
    pub message_size: usize,

    /// Number of messages to send after warmup
    #[arg(short = 'n', long, default_value = "100000")]
    pub num_messages: u64,

    /// Number of warmup messages
    #[arg(short = 'w', long, default_value = "10000")]
    pub warmup: u64,

    /// Wait strategy: busyspin, spinloop, sleep, block
    #[arg(long, default_value = "busyspin")]
    pub wait_strategy: String,

    /// Buffer size in slots
    #[arg(short = 'b', long)]
    pub buffer_size: Option<usize>,

    /// Emit JSON report to stdout
    #[arg(long)]
    pub json: bool,

    /// Emit canonical JSON report to stdout and DISRUPTOR_MP_BENCHMARK_JSON_OUT
    #[arg(long)]
    pub json_canonical: bool,

    /// Write JSON report to this path
    #[arg(long)]
    pub json_out: Option<String>,

    /// Write CSV report to this path
    #[arg(long)]
    pub csv_out: Option<String>,

    /// Write Markdown report to this path
    #[arg(long = "md-out")]
    pub markdown_out: Option<String>,

    /// Render canonical tree view
    #[arg(long)]
    pub tree: bool,

    /// Skip competitor comparison output
    #[arg(long)]
    pub no_compare: bool,

    /// Target rate in messages/sec for coordinated-omission-aware mode
    #[arg(long)]
    pub target_rate: Option<u64>,

    /// Enable low-overhead batch timing mode
    #[arg(long)]
    pub batch_timing: bool,

    /// Number of messages to burst before draining replies in fixed-rate mode
    #[arg(long, default_value = "1")]
    pub batch_size: usize,

    /// Number of concurrent echo consumers / ping-pong lanes
    #[arg(long, default_value = "1")]
    pub consumers: usize,

    /// Internal child-role flag
    #[arg(long, hide = true)]
    pub process_two: bool,
}

pub fn benchmark_json_output_path() -> Option<String> {
    env::var("DISRUPTOR_MP_BENCHMARK_JSON_OUT")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

pub fn legacy_json_output_path() -> Option<String> {
    env::var("MYELON_BENCH_JSON_OUT")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

pub fn json_mode(args: &CompetitiveArgs) -> bool {
    args.json
        || args.json_canonical
        || args.json_out.is_some()
        || args.csv_out.is_some()
        || args.markdown_out.is_some()
        || benchmark_json_output_path().is_some()
}

pub fn should_emit_report(args: &CompetitiveArgs) -> bool {
    json_mode(args) || args.tree || legacy_json_output_path().is_some()
}

pub fn report_output_args(args: &CompetitiveArgs) -> reporting::ReportOutputArgs {
    reporting::ReportOutputArgs {
        json_mode: args.json || args.json_canonical,
        quick_mode: false,
        tree_mode: args.tree,
        json_out: args.json_out.clone(),
        csv_out: args.csv_out.clone(),
        markdown_out: args.markdown_out.clone(),
    }
}

pub fn measurement_mode(args: &CompetitiveArgs) -> String {
    if args.batch_timing {
        "batch_timing".to_string()
    } else if let Some(target_rate) = args.target_rate {
        format!("co_aware@{target_rate}")
    } else {
        "max_throughput".to_string()
    }
}

pub fn default_buffer_size(message_size: usize) -> usize {
    match message_size {
        0..=1024 => 4096,
        1025..=16384 => 2048,
        16385..=65536 => 1024,
        _ => 512,
    }
}

pub fn scenario_label(args: &CompetitiveArgs) -> String {
    format!(
        "competitive_pingpong_1p{}c_{}",
        args.consumers,
        human_size(args.message_size)
    )
}

pub fn apply_wait_strategy(wait_strategy: &str) {
    match wait_strategy.to_ascii_lowercase().as_str() {
        "sleep" => std::thread::sleep(Duration::from_micros(50)),
        "block" => std::thread::sleep(Duration::from_millis(1)),
        "spinloop" => std::hint::spin_loop(),
        _ => std::hint::spin_loop(),
    }
}

pub fn build_report(
    backend: CompetitiveBackend,
    args: &CompetitiveArgs,
    throughput: f64,
    buffer_size: usize,
    latency: Option<latency::LatencyStats>,
    verification_passed: bool,
    messages_processed: u64,
) -> BenchReport {
    let mut result = reporting::make_result(reporting::BenchResultSpec {
        bench_name: backend.bench_name().to_string(),
        scenario: scenario_label(args),
        backend: backend.backend_name().to_string(),
        layer: "competitive_pingpong".to_string(),
        codec: None,
        measurement_mode: measurement_mode(args),
        wait_strategy: args.wait_strategy.clone(),
        transport: reporting::BenchTransportSpec::unified_competitive()
            .with_zero_copy(false)
            .with_framing("none"),
        message_size_bytes: args.message_size,
        payload_bytes: args.message_size,
        buffer_depth: buffer_size,
        num_messages: args.num_messages,
        warmup_messages: args.warmup,
        num_producers: 1,
        num_consumers: args.consumers,
        producer_throughput_ops_sec: throughput,
        consumer_throughput_ops_sec: throughput,
        latency,
    });
    result.results.verification_passed = verification_passed;
    result.results.messages_processed = messages_processed;
    result.results.data_rate_mbps = throughput * args.message_size as f64 / 1_000_000.0;
    result.metadata.timestamp = chrono::Utc::now().to_rfc3339();
    result.metadata.git_commit = result.metadata.git_commit.clone();

    let mut report = BenchReport::new();
    report.add(result);
    report
}

pub fn human_size(bytes: usize) -> String {
    match bytes {
        b if b >= 1024 * 1024 => format!("{}MB", b / (1024 * 1024)),
        b if b >= 1024 => format!("{}KB", b / 1024),
        b => format!("{b}B"),
    }
}

pub fn print_header(backend: CompetitiveBackend, args: &CompetitiveArgs, buffer_size: usize) {
    println!("=== {} ===", backend.title());
    println!("Message size: {}", human_size(args.message_size));
    println!("Buffer size: {} slots", buffer_size);
    println!("Messages: {} (+ {} warmup)", args.num_messages, args.warmup);
    println!("Echo consumers: {}", args.consumers);
    println!("Wait strategy: {}", args.wait_strategy);
    println!("Mode: {}", measurement_mode(args));
    if let Some(target_rate) = args.target_rate {
        println!("Target rate: {} msgs/sec", target_rate);
    }
    println!();
}

pub fn average_rtt_ns(duration: Duration, messages: u64) -> Option<f64> {
    if messages == 0 {
        return None;
    }
    Some(duration.as_nanos() as f64 / messages as f64)
}

pub fn validate_args(args: &CompetitiveArgs) -> Result<(), String> {
    if args.batch_timing && args.target_rate.is_some() {
        return Err("--batch-timing cannot be combined with --target-rate".into());
    }
    if args.batch_size == 0 {
        return Err("--batch-size must be greater than zero".into());
    }
    if args.consumers == 0 {
        return Err("--consumers must be greater than zero".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measurement_mode_tracks_cli_mode() {
        let throughput = CompetitiveArgs::parse_from(["bench"]);
        assert_eq!(measurement_mode(&throughput), "max_throughput");

        let batch = CompetitiveArgs::parse_from(["bench", "--batch-timing"]);
        assert_eq!(measurement_mode(&batch), "batch_timing");

        let co = CompetitiveArgs::parse_from(["bench", "--target-rate", "20000"]);
        assert_eq!(measurement_mode(&co), "co_aware@20000");
    }

    #[test]
    fn validate_args_rejects_invalid_combinations() {
        let args =
            CompetitiveArgs::parse_from(["bench", "--batch-timing", "--target-rate", "20000"]);
        assert_eq!(
            validate_args(&args).unwrap_err(),
            "--batch-timing cannot be combined with --target-rate"
        );

        let args = CompetitiveArgs::parse_from(["bench", "--batch-size", "0"]);
        assert_eq!(
            validate_args(&args).unwrap_err(),
            "--batch-size must be greater than zero"
        );
    }

    #[test]
    fn scenario_label_tracks_size_and_consumers() {
        let args = CompetitiveArgs::parse_from(["bench", "--consumers", "4", "-s", "65536"]);
        assert_eq!(scenario_label(&args), "competitive_pingpong_1p4c_64KB");
    }

    #[test]
    fn should_emit_report_honors_legacy_json_env() {
        unsafe {
            env::set_var("MYELON_BENCH_JSON_OUT", "/tmp/out.json");
        }
        let args = CompetitiveArgs::parse_from(["bench"]);
        assert!(should_emit_report(&args));
        unsafe {
            env::remove_var("MYELON_BENCH_JSON_OUT");
        }
    }
}
