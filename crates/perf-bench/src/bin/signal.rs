//! Dedicated signal benchmark binary.
//!
//! Public surfaces:
//! - pure signal throughput ceiling
//! - timestamped signal latency with measured achieved throughput
//! - queue-delay estimation via existing producer/consumer sequence cursors
//!
//! This stays a thin wrapper around the raw-ring broadcast executor so the
//! signal hot path remains identical to the main benchmark surface.

use clap::Parser;
use perf_bench::infra::signal_counters::{SignalSequenceObserver, SIGNAL_SINGLE_CONSUMER_ID};
use perf_bench::infra::signal_latency::SignalLatencyMode;
use std::error::Error;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(
    name = "perf-bench-signal",
    about = "Signal benchmark: throughput ceiling, true timestamped latency, and sequence-observer queue delay"
)]
struct Args {
    /// Backend transport
    #[arg(long, value_parser = ["shm", "mmap"], default_value = "shm")]
    backend: String,

    /// Number of consumers
    #[arg(long, default_value_t = 1)]
    consumers: usize,

    /// Ring buffer depth in slots.
    #[arg(long, default_value_t = 65_536)]
    buffer_size: usize,

    /// Number of signal events to send after warmup
    #[arg(long, short = 'n', default_value_t = 10_000_000)]
    events: u64,

    /// Warmup signal events before measurement
    #[arg(long)]
    warmup: Option<u64>,

    /// Timeout in seconds
    #[arg(long, default_value_t = 300)]
    timeout: u64,

    /// Render canonical tree view
    #[arg(long)]
    tree: bool,

    /// Emit JSON report to stdout
    #[arg(long)]
    json: bool,

    /// Write JSON report to this path
    #[arg(long)]
    json_out: Option<String>,

    /// Record canonical true signal latency with timestamping and report achieved throughput.
    #[arg(long)]
    latency: bool,

    /// Observe queueing via existing producer/consumer sequence cursors instead of hot-path counters.
    /// SHM backend and single-consumer only.
    #[arg(long)]
    observe_sequences: bool,

    /// Parent observer poll interval in microseconds when `--observe-sequences` is set.
    #[arg(long, default_value_t = 100)]
    sequence_poll_us: u64,

    /// Optional JSON path for the sequence-derived queueing estimate.
    #[arg(long)]
    sequence_report_out: Option<String>,
}

fn main() -> Result<(), Box<dyn Error>> {
    // --- Child process dispatch ---
    //
    // When executor code spawns child processes via current_exe(), the child
    // re-enters this binary. We detect and dispatch child invocations here
    // before parsing the consolidated CLI.

    let raw_args: Vec<String> = std::env::args().collect();

    if let Some(role) = raw_args.get(1) {
        if !role.starts_with("--") {
            return dispatch_child(&raw_args);
        }
    }

    // --- Orchestrator mode ---
    let args = Args::parse();
    run_signal(&args)
}

/// Dispatch a `BenchHarness` child process.
fn dispatch_child(args: &[String]) -> Result<(), Box<dyn Error>> {
    use perf_bench::infra::BenchHarness;

    let harness_key = std::env::var("PERF_BENCH_BROADCAST_HARNESS").unwrap_or_default();

    let harness: Option<&dyn BenchHarness> = match harness_key.as_str() {
        "raw_ring_shm" => {
            Some(&perf_bench::layers::raw::disruptor_mp::broadcast_shm::RawRingShmBench)
        }
        "raw_ring_mmap" => {
            Some(&perf_bench::layers::raw::disruptor_mp::broadcast_mmap::RawRingMmapBench)
        }
        _ => None,
    };

    if let Some(h) = harness {
        if perf_bench::infra::dispatch_child_or_exit(args, h.child_roles()) {
            return Ok(());
        }
    }

    // Fallback: try both harnesses
    let all: &[&dyn BenchHarness] = &[
        &perf_bench::layers::raw::disruptor_mp::broadcast_shm::RawRingShmBench,
        &perf_bench::layers::raw::disruptor_mp::broadcast_mmap::RawRingMmapBench,
    ];

    let combined: Vec<perf_bench::infra::child_runner::ChildRole> = all
        .iter()
        .flat_map(|h| h.child_roles().iter().copied())
        .collect();

    if perf_bench::infra::dispatch_child_or_exit(args, &combined) {
        return Ok(());
    }

    let role = args.get(1).map(String::as_str).unwrap_or("unknown");
    Err(format!("unrecognized child role: {role}").into())
}

fn run_signal(args: &Args) -> Result<(), Box<dyn Error>> {
    use perf_bench::infra::BenchHarness;

    if args.latency && args.observe_sequences {
        return Err("--latency and --observe-sequences are mutually exclusive".into());
    }

    let latency_mode = if args.latency {
        SignalLatencyMode::canonical()
    } else {
        SignalLatencyMode::None
    };
    if args.observe_sequences {
        return run_signal_with_sequence_observer(args, latency_mode);
    }
    std::env::set_var("PERF_BENCH_SIGNAL_LATENCY_MODE", latency_mode.as_str());
    std::env::set_var("PERF_BENCH_SIGNAL_SAMPLE_EVERY", "1");
    std::env::remove_var("PERF_BENCH_SIGNAL_TARGET_RATE");
    if latency_mode.records_latency() {
        std::env::set_var("PERF_BENCH_SIGNAL_RECORD_LATENCY", "1");
    } else {
        std::env::remove_var("PERF_BENCH_SIGNAL_RECORD_LATENCY");
    }

    if args.timeout != 300 {
        std::env::set_var("PERF_BENCH_TIMEOUT", args.timeout.to_string());
    }

    // Build synthetic args for RawRingSelection::parse
    let mut synthetic = vec![
        "perf-bench-signal".to_string(),
        "--class".to_string(),
        "signal".to_string(),
        "--mode".to_string(),
        "throughput".to_string(),
        "--events".to_string(),
        args.events.to_string(),
    ];

    synthetic.push("--consumers".to_string());
    synthetic.push(args.consumers.to_string());
    synthetic.push("--buffer-size".to_string());
    synthetic.push(args.buffer_size.to_string());
    if let Some(warmup) = args.warmup {
        synthetic.push("--warmup".to_string());
        synthetic.push(warmup.to_string());
    }

    if args.json {
        synthetic.push("--json".to_string());
    }
    if args.tree {
        synthetic.push("--tree".to_string());
    }
    if let Some(ref path) = args.json_out {
        synthetic.push("--json-out".to_string());
        synthetic.push(path.clone());
    }

    let result = match args.backend.as_str() {
        "shm" => {
            std::env::set_var("PERF_BENCH_BROADCAST_HARNESS", "raw_ring_shm");
            let bench = perf_bench::layers::raw::disruptor_mp::broadcast_shm::RawRingShmBench;
            bench.run_orchestrator(&synthetic)
        }
        "mmap" => {
            std::env::set_var("PERF_BENCH_BROADCAST_HARNESS", "raw_ring_mmap");
            let bench = perf_bench::layers::raw::disruptor_mp::broadcast_mmap::RawRingMmapBench;
            bench.run_orchestrator(&synthetic)
        }
        _ => Err(format!("unknown backend: {}", args.backend).into()),
    };
    result
}

fn run_signal_with_sequence_observer(
    args: &Args,
    latency_mode: SignalLatencyMode,
) -> Result<(), Box<dyn Error>> {
    use perf_bench::infra::{
        collect_child_output, parse_child_metrics, spawn_child, unique_shm_segment, ConsumerOutput,
        ProducerOutput,
    };

    if args.backend != "shm" {
        return Err("--observe-sequences currently supports only --backend shm".into());
    }
    if args.consumers != 1 {
        return Err("--observe-sequences currently supports only --consumers 1".into());
    }

    std::env::set_var("PERF_BENCH_SIGNAL_LATENCY_MODE", latency_mode.as_str());
    std::env::set_var("PERF_BENCH_SIGNAL_SAMPLE_EVERY", "1");
    std::env::remove_var("PERF_BENCH_SIGNAL_TARGET_RATE");
    if latency_mode.records_latency() {
        std::env::set_var("PERF_BENCH_SIGNAL_RECORD_LATENCY", "1");
    } else {
        std::env::remove_var("PERF_BENCH_SIGNAL_RECORD_LATENCY");
    }

    let timeout = Duration::from_secs(args.timeout);
    let warmup = args.warmup.unwrap_or(100_000);
    let segment = unique_shm_segment("sig_seqobs");
    let exe = std::env::current_exe()?;
    let envs: Vec<(&str, String)> = vec![
        ("BENCHMARK_SEGMENT_NAME", segment.clone()),
        ("BENCH_NUM_CONSUMERS", "1".to_string()),
        ("BENCH_BUFFER", args.buffer_size.to_string()),
        ("BENCH_EVENTS", args.events.to_string()),
        ("BENCH_WARMUP", warmup.to_string()),
        (
            "PERF_BENCH_SIGNAL_LATENCY_MODE",
            latency_mode.as_str().to_string(),
        ),
        ("PERF_BENCH_SIGNAL_SAMPLE_EVERY", "1".to_string()),
    ];
    let mut consumer_envs = envs.clone();
    consumer_envs.push(("BENCH_CONSUMER_ID", "0".to_string()));
    let producer = spawn_child(&exe, "sig_producer", &envs);
    let consumer = spawn_child(&exe, "sig_consumer", &consumer_envs);

    let observer = SignalSequenceObserver::spawn(
        segment,
        SIGNAL_SINGLE_CONSUMER_ID.to_string(),
        warmup,
        args.events,
        Duration::from_micros(args.sequence_poll_us),
    );

    let producer_output = collect_child_output("signal_seqobs prod", producer, timeout);
    let consumer_output = collect_child_output("signal_seqobs cons0", consumer, timeout);
    let observer_report = observer
        .stop_and_join()
        .map_err(|error| format!("signal sequence observer: {error}"))?;

    if !producer_output.success {
        return Err(format!(
            "signal producer failed\nstderr:\n{}",
            producer_output.stderr
        )
        .into());
    }
    if !consumer_output.success {
        return Err(format!(
            "signal consumer failed\nstderr:\n{}",
            consumer_output.stderr
        )
        .into());
    }

    let producer_metrics: ProducerOutput = parse_child_metrics("producer", &producer_output);
    let consumer_metrics: ConsumerOutput = parse_child_metrics("consumer", &consumer_output);

    if let Some(path) = &args.sequence_report_out {
        std::fs::write(path, serde_json::to_vec_pretty(&observer_report)?)?;
    } else {
        eprintln!(
            "[signal sequences] backlog p50={} p95={} p99={} max={} est_mean_queue_latency={:.2}ns est_p99_queue_latency={:.2}ns publish_rate={:.2} consume_rate={:.2}",
            observer_report.p50_backlog,
            observer_report.p95_backlog,
            observer_report.p99_backlog,
            observer_report.max_backlog,
            observer_report.estimated_mean_queue_latency_ns,
            observer_report.estimated_p99_queue_latency_ns,
            observer_report.observed_publish_ops_sec,
            observer_report.observed_consume_ops_sec,
        );
    }

    if args.json {
        let payload = serde_json::json!({
            "mode": "signal_sequence_observer",
            "backend": args.backend,
            "events": args.events,
            "warmup": warmup,
            "latency_mode": latency_mode.as_str(),
            "producer": producer_metrics,
            "consumer": consumer_metrics,
            "sequence_observer": observer_report,
        });
        if let Some(path) = &args.json_out {
            std::fs::write(path, serde_json::to_vec_pretty(&payload)?)?;
        }
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        eprintln!(
            "signal_seqobs producer={:.2} ops/s consumer={:.2} ops/s",
            producer_metrics.throughput_ops_sec, consumer_metrics.throughput_ops_sec
        );
    }

    Ok(())
}
