use crate::infra::latency;
use crate::infra::output::report::BackendKind;
use crate::infra::output::reporting::{self, BenchReport};
use clap::Parser;
use std::env;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PingPongBackend {
    Shm,
    Mmap,
}

impl PingPongBackend {
    pub fn title(self) -> &'static str {
        match self {
            Self::Shm => "PingPong SHM",
            Self::Mmap => "PingPong MMAP",
        }
    }

    pub fn bench_name(self) -> &'static str {
        match self {
            Self::Shm => "pingpong_shm",
            Self::Mmap => "pingpong_mmap",
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

pub const PINGPONG_HEADER_BYTES: usize = 64;
pub const SUPPORTED_MESSAGE_SIZES_DISPLAY: &str =
    "64, 128, 512, 1024, 2048, 4096, 16384, 32768, 65536, 131072, 524288, 1048576, 2097152, 8388608, 16777216, 33554432, 67108864";

pub const fn payload_bytes_for_message_size(message_size: usize) -> Option<usize> {
    match message_size {
        64 => Some(0),
        128 => Some(64),
        512 => Some(448),
        1024 => Some(960),
        2048 => Some(1984),
        4096 => Some(4032),
        16384 => Some(16320),
        32768 => Some(32704),
        65536 => Some(65472),
        131072 => Some(131008),
        524288 => Some(524224),
        1048576 => Some(1048512),
        2097152 => Some(2097088),
        8388608 => Some(8388544),
        16777216 => Some(16777152),
        33554432 => Some(33554368),
        67108864 => Some(67108800),
        _ => None,
    }
}

pub fn supported_message_size_error(message_size: usize) -> String {
    format!(
        "unsupported message size: {} (expected {})",
        message_size, SUPPORTED_MESSAGE_SIZES_DISPLAY
    )
}

#[macro_export]
macro_rules! dispatch_pingpong_event {
    ($message_size:expr, |<$N:ident>| $body:expr) => {
        match $message_size {
            64 => {
                const $N: usize = 0;
                $body
            }
            128 => {
                const $N: usize = 64;
                $body
            }
            512 => {
                const $N: usize = 448;
                $body
            }
            1024 => {
                const $N: usize = 960;
                $body
            }
            2048 => {
                const $N: usize = 1984;
                $body
            }
            4096 => {
                const $N: usize = 4032;
                $body
            }
            16384 => {
                const $N: usize = 16320;
                $body
            }
            32768 => {
                const $N: usize = 32704;
                $body
            }
            65536 => {
                const $N: usize = 65472;
                $body
            }
            131072 => {
                const $N: usize = 131008;
                $body
            }
            524288 => {
                const $N: usize = 524224;
                $body
            }
            1048576 => {
                const $N: usize = 1048512;
                $body
            }
            2097152 => {
                const $N: usize = 2097088;
                $body
            }
            8388608 => {
                const $N: usize = 8388544;
                $body
            }
            16777216 => {
                const $N: usize = 16777152;
                $body
            }
            33554432 => {
                const $N: usize = 33554368;
                $body
            }
            67108864 => {
                const $N: usize = 67108800;
                $body
            }
            _ => Err($crate::cli::pingpong::supported_message_size_error($message_size).into()),
        }
    };
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct PingPongArgs {
    /// Total event size in bytes (includes the 64B ping-pong header)
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

    /// Emit canonical JSON report to stdout and `DISRUPTOR_MP_BENCHMARK_JSON_OUT`
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

    /// Attach RFC-0040 observability counters (`events_published` / `events_consumed`
    /// / `producer_full_events` / `consumer_empty_spins`) on the hot path so a
    /// counters-enabled scenario can be exercised. Default-off so the default
    /// bench path stays counter-free. Currently honored by `--backend shm`;
    /// `--backend mmap` ignores it (mmap producer/consumer have no counter
    /// wiring).
    #[arg(long)]
    pub enable_counters: bool,

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
    env::var(crate::infra::env::JSON_OUT)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

pub fn json_mode(args: &PingPongArgs) -> bool {
    args.json
        || args.json_canonical
        || args.json_out.is_some()
        || args.csv_out.is_some()
        || args.markdown_out.is_some()
        || benchmark_json_output_path().is_some()
}

pub fn should_emit_report(args: &PingPongArgs) -> bool {
    json_mode(args) || args.tree || legacy_json_output_path().is_some()
}

pub fn report_output_args(args: &PingPongArgs) -> reporting::ReportOutputArgs {
    reporting::ReportOutputArgs {
        json_mode: args.json || args.json_canonical,
        quick_mode: false,
        tree_mode: args.tree,
        json_out: args.json_out.clone(),
        csv_out: args.csv_out.clone(),
        markdown_out: args.markdown_out.clone(),
    }
}

pub fn measurement_mode(args: &PingPongArgs) -> String {
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
        65537..=131072 => 512,
        131073..=524_288 => 256,
        524_289..=1_048_576 => 128,
        1_048_577..=2_097_152 => 64,
        2_097_153..=8_388_608 => 16,
        8_388_609..=16_777_216 => 8,
        _ => 4,
    }
}

pub fn scenario_label(args: &PingPongArgs) -> String {
    format!(
        "pingpong_1p{}c_{}",
        args.consumers,
        human_size(args.message_size)
    )
}

pub fn apply_wait_strategy(wait_strategy: &str) {
    match wait_strategy.to_ascii_lowercase().as_str() {
        "sleep" => disruptor_mp::perform_default_consume_sleep_wait(),
        "block" => disruptor_mp::perform_default_block_wait(),
        "spinloop" => std::hint::spin_loop(),
        _ => std::hint::spin_loop(),
    }
}

pub fn build_report(
    backend: PingPongBackend,
    args: &PingPongArgs,
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
        layer: "raw_ring".to_string(),
        codec: None,
        measurement_mode: measurement_mode(args),
        wait_strategy: args.wait_strategy.clone(),
        transport: reporting::BenchTransportSpec::unified_pingpong()
            .with_zero_copy(false)
            .with_framing("none"),
        message_size_bytes: args.message_size,
        payload_bytes: payload_bytes_for_message_size(args.message_size)
            .unwrap_or(args.message_size.saturating_sub(PINGPONG_HEADER_BYTES)),
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

pub fn stack_thread_size(message_size: usize) -> Option<usize> {
    if message_size < 8 * 1024 * 1024 {
        return None;
    }

    Some((message_size.saturating_mul(8)).clamp(64 * 1024 * 1024, 512 * 1024 * 1024))
}

pub fn run_with_large_stack_if_needed<T, F>(
    message_size: usize,
    thread_name: &str,
    f: F,
) -> Result<T, Box<dyn std::error::Error>>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, Box<dyn std::error::Error>> + Send + 'static,
{
    let Some(stack_size) = stack_thread_size(message_size) else {
        return f();
    };

    let handle = std::thread::Builder::new()
        .name(thread_name.to_string())
        .stack_size(stack_size)
        .spawn(move || f().map_err(|error| error.to_string()))
        .map_err(|error| format!("failed to spawn {thread_name} stack worker: {error}"))?;

    match handle.join() {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error.into()),
        Err(_) => Err(format!("{thread_name} panicked").into()),
    }
}

pub fn print_header(backend: PingPongBackend, args: &PingPongArgs, buffer_size: usize) {
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

pub fn validate_args(args: &PingPongArgs) -> Result<(), String> {
    if args.batch_timing && args.target_rate.is_some() {
        return Err("--batch-timing cannot be combined with --target-rate".into());
    }
    if args.batch_size == 0 {
        return Err("--batch-size must be greater than zero".into());
    }
    if args.consumers == 0 {
        return Err("--consumers must be greater than zero".into());
    }
    if payload_bytes_for_message_size(args.message_size).is_none() {
        return Err(supported_message_size_error(args.message_size));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_buffer_depth_scales_down_for_multi_megabyte_payloads() {
        assert_eq!(default_buffer_size(2 * 1024 * 1024), 64);
        assert_eq!(default_buffer_size(8 * 1024 * 1024), 16);
        assert_eq!(default_buffer_size(16 * 1024 * 1024), 8);
        assert_eq!(default_buffer_size(64 * 1024 * 1024), 4);
    }

    #[test]
    fn stack_thread_size_only_applies_to_huge_payloads() {
        assert_eq!(stack_thread_size(2 * 1024 * 1024), None);
        assert_eq!(stack_thread_size(8 * 1024 * 1024), Some(64 * 1024 * 1024));
        assert_eq!(stack_thread_size(64 * 1024 * 1024), Some(512 * 1024 * 1024));
    }

    #[test]
    fn default_buffer_depth_preserves_existing_small_payload_behavior() {
        assert_eq!(default_buffer_size(64), 4096);
        assert_eq!(default_buffer_size(2048), 2048);
        assert_eq!(default_buffer_size(131072), 512);
        assert_eq!(default_buffer_size(524288), 256);
        assert_eq!(default_buffer_size(1048576), 128);
    }

    #[test]
    fn measurement_mode_tracks_cli_mode() {
        let throughput = PingPongArgs::parse_from(["bench"]);
        assert_eq!(measurement_mode(&throughput), "max_throughput");

        let batch = PingPongArgs::parse_from(["bench", "--batch-timing"]);
        assert_eq!(measurement_mode(&batch), "batch_timing");

        let co = PingPongArgs::parse_from(["bench", "--target-rate", "20000"]);
        assert_eq!(measurement_mode(&co), "co_aware@20000");
    }

    #[test]
    fn validate_args_rejects_invalid_combinations() {
        let args = PingPongArgs::parse_from(["bench", "--batch-timing", "--target-rate", "20000"]);
        assert_eq!(
            validate_args(&args).unwrap_err(),
            "--batch-timing cannot be combined with --target-rate"
        );

        let args = PingPongArgs::parse_from(["bench", "--batch-size", "0"]);
        assert_eq!(
            validate_args(&args).unwrap_err(),
            "--batch-size must be greater than zero"
        );

        let args = PingPongArgs::parse_from(["bench", "--message-size", "32"]);
        assert_eq!(
            validate_args(&args).unwrap_err(),
            supported_message_size_error(32)
        );
    }

    #[test]
    fn scenario_label_tracks_size_and_consumers() {
        let args = PingPongArgs::parse_from(["bench", "--consumers", "4", "-s", "65536"]);
        assert_eq!(scenario_label(&args), "pingpong_1p4c_64KB");
    }

    #[test]
    fn payload_size_contract_uses_total_event_bytes() {
        assert_eq!(payload_bytes_for_message_size(64), Some(0));
        assert_eq!(payload_bytes_for_message_size(128), Some(64));
        assert_eq!(payload_bytes_for_message_size(1024), Some(960));
        assert_eq!(payload_bytes_for_message_size(4096), Some(4032));
        assert_eq!(payload_bytes_for_message_size(32), None);
    }

    #[test]
    fn should_emit_report_honors_legacy_json_env() {
        unsafe {
            env::set_var(crate::infra::env::JSON_OUT, "/tmp/out.json");
        }
        let args = PingPongArgs::parse_from(["bench"]);
        assert!(should_emit_report(&args));
        unsafe {
            env::remove_var(crate::infra::env::JSON_OUT);
        }
    }
}
