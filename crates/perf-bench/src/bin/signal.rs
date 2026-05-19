//! Dedicated signal benchmark binary.
//!
//! Measures raw ring throughput ceiling with minimal 64-byte events.
//! This is a thin wrapper around the `raw_ring` broadcast executor that
//! hardcodes `--class signal` and forces latency recording via
//! the `PERF_BENCH_SIGNAL_RECORD_LATENCY` env var.

use clap::Parser;
use std::error::Error;

#[derive(Parser, Debug)]
#[command(
    name = "perf-bench-signal",
    about = "Signal benchmark: raw ring throughput ceiling with 64B events (latency enabled)"
)]
struct Args {
    /// Backend transport
    #[arg(long, value_parser = ["shm", "mmap"], default_value = "shm")]
    backend: String,

    /// Number of consumers
    #[arg(long, default_value_t = 1)]
    consumers: usize,

    /// Number of signal events to send after warmup
    #[arg(long, short = 'n', default_value_t = 10_000_000)]
    events: u64,

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

    // Force latency recording for signal scenarios
    std::env::set_var("PERF_BENCH_SIGNAL_RECORD_LATENCY", "1");

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

    match args.backend.as_str() {
        "shm" => {
            std::env::set_var("PERF_BENCH_BROADCAST_HARNESS", "raw_ring_shm");
            let bench = perf_bench::layers::raw::disruptor_mp::broadcast_shm::RawRingShmBench;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        "mmap" => {
            std::env::set_var("PERF_BENCH_BROADCAST_HARNESS", "raw_ring_mmap");
            let bench = perf_bench::layers::raw::disruptor_mp::broadcast_mmap::RawRingMmapBench;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        _ => Err(format!("unknown backend: {}", args.backend).into()),
    }
}
