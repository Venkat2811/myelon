//! Consolidated ping-pong benchmark binary.
//!
//! Replaces the 10 individual pingpong_* bench targets with a single binary
//! that accepts CLI flags for all dimensions (layer, backend, mode, etc.).
//!
//! Phase 3 Step 3 of RFC 0026: this coexists with the old bench targets.

use clap::Parser;
use std::error::Error;

#[derive(Parser, Debug)]
#[command(
    name = "perf-bench-pingpong",
    about = "Consolidated ping-pong benchmark for all transport layers and backends"
)]
struct Args {
    /// Transport layer to benchmark
    #[arg(long, value_parser = ["raw_ring", "raw_myelon", "framed", "codec", "typed_zc"])]
    layer: String,

    /// Backend transport
    #[arg(long, value_parser = ["shm", "mmap"], default_value = "shm")]
    backend: String,

    /// Message size in bytes (for raw_ring and raw_myelon layers)
    #[arg(long, short = 's', default_value_t = 64)]
    size: usize,

    /// Benchmark mode
    #[arg(long, value_parser = ["throughput", "co"], default_value = "throughput")]
    mode: String,

    /// Target rate in messages/sec for coordinated-omission-aware mode
    #[arg(long)]
    target_rate: Option<u64>,

    /// Wait strategy: busyspin, spinloop, sleep, block
    #[arg(long, default_value = "busyspin")]
    wait_strategy: String,

    /// Codec to use (for codec and typed_zc layers)
    #[arg(long, value_parser = ["bincode", "rkyv", "flatbuf"])]
    codec: Option<String>,

    /// Fragmentation mode (for codec layer)
    #[arg(long, value_parser = ["frag", "nofrag"], default_value = "frag")]
    frag: String,

    /// Batch size for fixed-rate mode
    #[arg(long, default_value_t = 1)]
    batch_size: usize,

    /// Number of messages to send after warmup
    #[arg(long, short = 'n', default_value_t = 100000)]
    num_messages: u64,

    /// Number of warmup messages
    #[arg(long, short = 'w', default_value_t = 10000)]
    warmup: u64,

    /// Buffer size in slots
    #[arg(long, short = 'b')]
    buffer_size: Option<usize>,

    /// Timeout in seconds
    #[arg(long, default_value_t = 300)]
    timeout: u64,

    /// Emit JSON report to stdout
    #[arg(long)]
    json: bool,

    /// Render canonical tree view
    #[arg(long)]
    tree: bool,

    /// Write JSON report to this path
    #[arg(long)]
    json_out: Option<String>,

    /// Enable required-consumer liveness checking (on/off)
    #[arg(long, value_parser = ["on", "off"], default_value = "off")]
    liveness: String,

    // --- hidden flags for child process re-invocation ---
    /// Internal: child process flag for raw_ring / raw_myelon layers
    #[arg(long, hide = true)]
    process_two: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    // --- Child process dispatch ---
    //
    // When executor code spawns child processes via current_exe(), the child
    // re-enters this binary. We detect and dispatch child invocations here
    // before parsing the consolidated CLI.

    let raw_args: Vec<String> = std::env::args().collect();

    // BenchHarness child roles: args[1] is a role name (not starting with --)
    if let Some(role) = raw_args.get(1) {
        if !role.starts_with("--") {
            return dispatch_bench_harness_child(&raw_args);
        }
    }

    // raw_ring / raw_myelon child process: --process-two flag with env var dispatch
    if raw_args.iter().any(|a| a == "--process-two") {
        return dispatch_raw_child();
    }

    // --- Orchestrator mode ---
    let args = Args::parse();

    // Forward --liveness flag as env var for executor integration
    if args.liveness == "on" {
        std::env::set_var("PERF_BENCH_LIVENESS", "on");
    }

    match args.layer.as_str() {
        "raw_ring" => run_raw_ring(&args),
        "raw_myelon" => run_raw_myelon(&args),
        "framed" => run_framed(&args),
        "codec" => run_codec(&args),
        "typed_zc" => run_typed_zc(&args),
        _ => Err(format!("unknown layer: {}", args.layer).into()),
    }
}

// ---------------------------------------------------------------------------
// Child process dispatch helpers
// ---------------------------------------------------------------------------

/// Dispatch a BenchHarness child process by checking all known child role tables.
///
/// Collects all child roles from all harnesses into a single flat list,
/// then dispatches once. This avoids the bug in `maybe_run_child` where
/// a role name present in args\[1\] but not matching the current harness
/// returns `Some(Ok(()))` — silently succeeding with no work done.
fn dispatch_bench_harness_child(args: &[String]) -> Result<(), Box<dyn Error>> {
    use perf_bench::infra::child_runner::ChildRole;
    use perf_bench::infra::BenchHarness;

    // Collect ALL child roles from all pingpong BenchHarness instances into one list.
    // This avoids the bug in maybe_run_child where a role name present in args[1]
    // but not matching the current harness returns Some(Ok(())) silently.
    let harnesses: [&dyn BenchHarness; 6] = [
        &perf_bench::layers::framed_myelon::frag::pingpong_shm::PingPongFramedShmBench,
        &perf_bench::layers::framed_myelon::frag::pingpong_mmap::PingPongFramedMmapBench,
        &perf_bench::layers::framed_myelon::codec::pingpong_shm::PingPongCodecShmBench,
        &perf_bench::layers::framed_myelon::codec::pingpong_mmap::PingPongCodecMmapBench,
        &perf_bench::layers::framed_myelon::typed_zc::pingpong_shm::PingPongTypedZeroCopyShmBench,
        &perf_bench::layers::framed_myelon::typed_zc::pingpong_mmap::PingPongTypedZeroCopyMmapBench,
    ];

    let combined: Vec<ChildRole> = harnesses
        .iter()
        .flat_map(|h| h.child_roles().iter().copied())
        .collect();

    if perf_bench::infra::dispatch_child_or_exit(args, &combined) {
        return Ok(());
    }

    // args[1] was a role name but no harness recognized it
    let role = args.get(1).map(String::as_str).unwrap_or("unknown");
    Err(format!("unrecognized child role: {role}").into())
}

/// Dispatch a raw_ring / raw_myelon child process (`--process-two`).
///
/// The orchestrator sets `PERF_BENCH_DISPATCH` so the child knows which
/// executor module to invoke.
fn dispatch_raw_child() -> Result<(), Box<dyn Error>> {
    let dispatch = std::env::var("PERF_BENCH_DISPATCH").map_err(|_| {
        "child invoked with --process-two but PERF_BENCH_DISPATCH env var is not set"
    })?;

    // Build args that the executor's run_main_with_args can parse.
    // The child only needs: <exe> --process-two
    let synthetic = vec![
        "perf-bench-pingpong".to_string(),
        "--process-two".to_string(),
    ];

    match dispatch.as_str() {
        "raw_ring_shm" => {
            perf_bench::layers::raw::disruptor_mp::pingpong_shm::run_main_with_args(synthetic)
        }
        "raw_ring_mmap" => {
            perf_bench::layers::raw::disruptor_mp::pingpong_mmap::run_main_with_args(synthetic)
        }
        "raw_myelon_shm" => {
            perf_bench::layers::raw::myelon::pingpong_shm::run_main_with_args(synthetic)
        }
        "raw_myelon_mmap" => {
            perf_bench::layers::raw::myelon::pingpong_mmap::run_main_with_args(synthetic)
        }
        _ => Err(format!("unknown PERF_BENCH_DISPATCH value: {dispatch}").into()),
    }
}

// ---------------------------------------------------------------------------
// Orchestrator dispatch functions
// ---------------------------------------------------------------------------

/// Build PingPongArgs-compatible CLI args from the consolidated Args.
fn build_raw_args(args: &Args) -> Vec<String> {
    let mut v = vec!["perf-bench-pingpong".to_string()];

    v.push("--message-size".to_string());
    v.push(args.size.to_string());

    v.push("--num-messages".to_string());
    v.push(args.num_messages.to_string());

    v.push("--warmup".to_string());
    v.push(args.warmup.to_string());

    v.push("--wait-strategy".to_string());
    v.push(args.wait_strategy.clone());

    if let Some(buf) = args.buffer_size {
        v.push("--buffer-size".to_string());
        v.push(buf.to_string());
    }

    if let Some(rate) = args.target_rate {
        v.push("--target-rate".to_string());
        v.push(rate.to_string());
    }

    v.push("--batch-size".to_string());
    v.push(args.batch_size.to_string());

    if args.json {
        v.push("--json".to_string());
    }
    if args.tree {
        v.push("--tree".to_string());
    }
    if let Some(ref path) = args.json_out {
        v.push("--json-out".to_string());
        v.push(path.clone());
    }

    v
}

/// Build FramedPingPongArgs / CodecPingPongArgs-compatible CLI args.
fn build_myelon_args(args: &Args) -> Vec<String> {
    let mut v = vec!["perf-bench-pingpong".to_string()];

    v.push("--mode".to_string());
    v.push(args.mode.clone());

    v.push("--num-messages".to_string());
    v.push(args.num_messages.to_string());

    v.push("--warmup".to_string());
    v.push(args.warmup.to_string());

    if let Some(buf) = args.buffer_size {
        v.push("--buffer-size".to_string());
        v.push(buf.to_string());
    }

    v.push("--wait-strategy".to_string());
    v.push(args.wait_strategy.clone());

    if let Some(rate) = args.target_rate {
        v.push("--target-rate".to_string());
        v.push(rate.to_string());
    }

    if args.json {
        v.push("--json".to_string());
    }
    if args.tree {
        v.push("--tree".to_string());
    }
    if let Some(ref path) = args.json_out {
        v.push("--json-out".to_string());
        v.push(path.clone());
    }

    v
}

/// Build codec-specific CLI args (extends myelon args).
fn build_codec_args(args: &Args) -> Vec<String> {
    let mut v = build_myelon_args(args);

    if let Some(ref codec) = args.codec {
        v.push("--codec".to_string());
        v.push(codec.clone());
    }

    if args.batch_size > 1 {
        v.push("--batch".to_string());
        v.push(args.batch_size.to_string());
    }

    v
}

fn run_raw_ring(args: &Args) -> Result<(), Box<dyn Error>> {
    let dispatch_key = format!("raw_ring_{}", args.backend);
    std::env::set_var("PERF_BENCH_DISPATCH", &dispatch_key);

    if args.timeout != 300 {
        std::env::set_var("PERF_BENCH_TIMEOUT", args.timeout.to_string());
    }

    let synthetic = build_raw_args(args);

    match args.backend.as_str() {
        "shm" => perf_bench::layers::raw::disruptor_mp::pingpong_shm::run_main_with_args(synthetic),
        "mmap" => {
            perf_bench::layers::raw::disruptor_mp::pingpong_mmap::run_main_with_args(synthetic)
        }
        _ => Err(format!("unknown backend: {}", args.backend).into()),
    }
}

fn run_raw_myelon(args: &Args) -> Result<(), Box<dyn Error>> {
    let dispatch_key = format!("raw_myelon_{}", args.backend);
    std::env::set_var("PERF_BENCH_DISPATCH", &dispatch_key);

    if args.timeout != 300 {
        std::env::set_var("PERF_BENCH_TIMEOUT", args.timeout.to_string());
    }

    let synthetic = build_raw_args(args);

    match args.backend.as_str() {
        "shm" => perf_bench::layers::raw::myelon::pingpong_shm::run_main_with_args(synthetic),
        "mmap" => perf_bench::layers::raw::myelon::pingpong_mmap::run_main_with_args(synthetic),
        _ => Err(format!("unknown backend: {}", args.backend).into()),
    }
}

fn run_framed(args: &Args) -> Result<(), Box<dyn Error>> {
    use perf_bench::infra::BenchHarness;

    if args.timeout != 300 {
        std::env::set_var("PERF_BENCH_TIMEOUT", args.timeout.to_string());
    }

    let synthetic = build_myelon_args(args);

    match args.backend.as_str() {
        "shm" => {
            let bench =
                perf_bench::layers::framed_myelon::frag::pingpong_shm::PingPongFramedShmBench;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        "mmap" => {
            let bench =
                perf_bench::layers::framed_myelon::frag::pingpong_mmap::PingPongFramedMmapBench;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        _ => Err(format!("unknown backend: {}", args.backend).into()),
    }
}

fn run_codec(args: &Args) -> Result<(), Box<dyn Error>> {
    use perf_bench::infra::BenchHarness;

    if args.timeout != 300 {
        std::env::set_var("PERF_BENCH_TIMEOUT", args.timeout.to_string());
    }

    let synthetic = build_codec_args(args);

    match args.backend.as_str() {
        "shm" => {
            let bench =
                perf_bench::layers::framed_myelon::codec::pingpong_shm::PingPongCodecShmBench;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        "mmap" => {
            let bench =
                perf_bench::layers::framed_myelon::codec::pingpong_mmap::PingPongCodecMmapBench;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        _ => Err(format!("unknown backend: {}", args.backend).into()),
    }
}

fn run_typed_zc(args: &Args) -> Result<(), Box<dyn Error>> {
    use perf_bench::infra::BenchHarness;

    if args.timeout != 300 {
        std::env::set_var("PERF_BENCH_TIMEOUT", args.timeout.to_string());
    }

    let synthetic = build_codec_args(args);

    match args.backend.as_str() {
        "shm" => {
            let bench = perf_bench::layers::framed_myelon::typed_zc::pingpong_shm::PingPongTypedZeroCopyShmBench;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        "mmap" => {
            let bench = perf_bench::layers::framed_myelon::typed_zc::pingpong_mmap::PingPongTypedZeroCopyMmapBench;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        _ => Err(format!("unknown backend: {}", args.backend).into()),
    }
}
