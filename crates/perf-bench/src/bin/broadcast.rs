//! Consolidated broadcast benchmark binary.
//!
//! Replaces the 16 individual broadcast bench targets with a single binary
//! that accepts CLI flags for all dimensions (layer, backend, mode, etc.).
//!
//! Covers: `raw_ring` e2e, framed e2e, codec e2e, wait strategy, sweeps,
//! monster sweep, nofrag, and layout validation.

use clap::Parser;
use std::error::Error;

#[derive(Parser, Debug)]
#[command(
    name = "perf-bench-broadcast",
    about = "Consolidated broadcast benchmark for all transport layers and backends"
)]
struct Args {
    /// Transport layer / benchmark to run
    #[arg(long, value_parser = ["raw_ring", "raw_myelon", "framed", "codec", "typed_zc",
                                 "wait_strategy", "myelon_layers", "monster_sweep",
                                 "framed_sweep", "typed_zc_sweep", "nofrag", "layout"])]
    layer: String,

    /// Backend transport
    #[arg(long, value_parser = ["shm", "mmap"], default_value = "shm")]
    backend: String,

    /// Message size in bytes (total event size including 16B header).
    /// For `raw_ring`: overrides the default 144B message events (e.g. --size 2048).
    /// For sweep layers: selects the event size to benchmark.
    #[arg(long, short = 's')]
    size: Option<usize>,

    /// Benchmark mode
    #[arg(long, value_parser = ["throughput", "co", "phase_timing"], default_value = "throughput")]
    mode: String,

    /// Target rate in messages/sec for coordinated-omission-aware mode
    #[arg(long)]
    target_rate: Option<u64>,

    /// Wait strategy: busyspin, spinloop, sleep, block
    #[arg(long, default_value = "busyspin")]
    wait_strategy: String,

    /// Codec to use (for codec and `typed_zc` layers)
    #[arg(long)]
    codec: Option<String>,

    /// Number of consumers
    #[arg(long, default_value_t = 1)]
    consumers: usize,

    /// Batch size
    #[arg(long, default_value_t = 1)]
    batch_size: usize,

    /// Number of messages to send after warmup
    #[arg(long, short = 'n', default_value_t = 100000)]
    num_messages: u64,

    /// Number of warmup messages
    #[arg(long, short = 'w', default_value_t = 10000)]
    warmup: u64,

    /// Timeout in seconds
    #[arg(long, default_value_t = 300)]
    timeout: u64,

    // --- Signal-specific (raw_ring) ---
    /// Event class filter for `raw_ring`: signal, message, or all
    #[arg(long, value_parser = ["signal", "message", "all"], default_value = "all")]
    class: String,

    /// Override signal event count
    #[arg(long)]
    events: Option<u64>,

    // --- Output ---
    /// Emit JSON report to stdout
    #[arg(long)]
    json: bool,

    /// Render canonical tree view
    #[arg(long)]
    tree: bool,

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

    // BenchHarness child roles: args[1] is a role name (not starting with --)
    if let Some(role) = raw_args.get(1) {
        if !role.starts_with("--") {
            return dispatch_bench_harness_child(&raw_args);
        }
    }

    // --- Orchestrator mode ---
    let args = Args::parse();

    match args.layer.as_str() {
        "raw_ring" => run_raw_ring(&args),
        "raw_myelon" => run_raw_ring(&args), // raw_myelon uses same raw_ring executor path
        "framed" => run_framed(&args),
        "codec" => run_codec(&args),
        "typed_zc" => run_typed_zc(&args),
        "wait_strategy" => run_wait_strategy(&args),
        "myelon_layers" => run_myelon_layers(&args),
        "monster_sweep" => run_monster_sweep(&args),
        "framed_sweep" => run_framed_sweep(&args),
        "typed_zc_sweep" => run_typed_zc_sweep(&args),
        "nofrag" => run_nofrag(&args),
        "layout" => run_layout(&args),
        _ => Err(format!("unknown layer: {}", args.layer).into()),
    }
}

// ---------------------------------------------------------------------------
// Child process dispatch helpers
// ---------------------------------------------------------------------------

/// Dispatch a `BenchHarness` child process.
///
/// Uses `PERF_BENCH_BROADCAST_HARNESS` env var (set by the orchestrator) to
/// select the correct harness, then dispatches against that harness's child
/// roles only. This avoids role name collisions (e.g., both codec/shm.rs and
/// codec/mmap.rs use "`codec_producer`" as a role name).
fn dispatch_bench_harness_child(args: &[String]) -> Result<(), Box<dyn Error>> {
    use perf_bench::infra::BenchHarness;

    let harness_key = std::env::var("PERF_BENCH_BROADCAST_HARNESS").unwrap_or_default();

    let harness: Option<&dyn BenchHarness> = match harness_key.as_str() {
        "raw_ring_shm" => {
            Some(&perf_bench::layers::raw::disruptor_mp::broadcast_shm::RawRingShmBench)
        }
        "raw_ring_mmap" => {
            Some(&perf_bench::layers::raw::disruptor_mp::broadcast_mmap::RawRingMmapBench)
        }
        "framed_shm" => {
            Some(&perf_bench::layers::framed_myelon::frag::broadcast_shm::FramedShmBench)
        }
        "framed_mmap" => {
            Some(&perf_bench::layers::framed_myelon::frag::broadcast_mmap::FramedMmapBench)
        }
        "codec_shm" => Some(&perf_bench::layers::framed_myelon::codec::shm::CodecE2eShmBench),
        "codec_mmap" => Some(&perf_bench::layers::framed_myelon::codec::mmap::CodecE2eMmapBench),
        "nofrag_shm" => {
            Some(&perf_bench::layers::framed_myelon::codec::nofrag_shm::CodecNoFragShmBench)
        }
        "nofrag_mmap" => {
            Some(&perf_bench::layers::framed_myelon::codec::nofrag_mmap::CodecNoFragMmapBench)
        }
        "wait_shm" => {
            Some(&perf_bench::layers::raw::disruptor_mp::wait_strategy_shm::WaitStrategyShmBench)
        }
        "wait_mmap" => {
            Some(&perf_bench::layers::raw::disruptor_mp::wait_strategy_mmap::WaitStrategyMmapBench)
        }
        "myelon_layers" => Some(&perf_bench::layers::sweeps::myelon_layers::MyelonLayersBench),
        "framed_sweep" => Some(&perf_bench::layers::sweeps::myelon_framed_sweep::MyelonFramedSweep),
        "typed_zc_sweep" => {
            Some(&perf_bench::layers::sweeps::typed_zero_copy_sweep::TypedZeroCopySweep)
        }
        "nofrag_all" => Some(&perf_bench::layers::sweeps::nofrag_all::NofragAllBench),
        "monster_shm" => Some(&perf_bench::layers::sweeps::monster_sweep_shm::MonsterSweepShm),
        "monster_mmap" => Some(&perf_bench::layers::sweeps::monster_sweep_mmap::MonsterSweepMmap),
        _ => None,
    };

    if let Some(h) = harness {
        if perf_bench::infra::dispatch_child_or_exit(args, h.child_roles()) {
            return Ok(());
        }
    }

    // No harness key set or role didn't match — try all harnesses as fallback
    // (works for harnesses with unique role names)
    let all_harnesses: &[&dyn BenchHarness] = &[
        &perf_bench::layers::raw::disruptor_mp::broadcast_shm::RawRingShmBench,
        &perf_bench::layers::raw::disruptor_mp::broadcast_mmap::RawRingMmapBench,
        &perf_bench::layers::framed_myelon::frag::broadcast_shm::FramedShmBench,
        &perf_bench::layers::framed_myelon::frag::broadcast_mmap::FramedMmapBench,
        &perf_bench::layers::raw::disruptor_mp::wait_strategy_shm::WaitStrategyShmBench,
        &perf_bench::layers::raw::disruptor_mp::wait_strategy_mmap::WaitStrategyMmapBench,
        &perf_bench::layers::sweeps::myelon_layers::MyelonLayersBench,
        &perf_bench::layers::sweeps::myelon_framed_sweep::MyelonFramedSweep,
        &perf_bench::layers::sweeps::typed_zero_copy_sweep::TypedZeroCopySweep,
        &perf_bench::layers::sweeps::nofrag_all::NofragAllBench,
        &perf_bench::layers::sweeps::monster_sweep_shm::MonsterSweepShm,
        &perf_bench::layers::sweeps::monster_sweep_mmap::MonsterSweepMmap,
    ];

    let combined: Vec<perf_bench::infra::child_runner::ChildRole> = all_harnesses
        .iter()
        .flat_map(|h| h.child_roles().iter().copied())
        .collect();

    if perf_bench::infra::dispatch_child_or_exit(args, &combined) {
        return Ok(());
    }

    let role = args.get(1).map(String::as_str).unwrap_or("unknown");
    Err(format!("unrecognized child role: {role}").into())
}

// ---------------------------------------------------------------------------
// Arg builder helpers
// ---------------------------------------------------------------------------

/// Build common output args (--json, --tree, --json-out) from consolidated Args.
fn append_output_args(v: &mut Vec<String>, args: &Args) {
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
}

/// Build args for `raw_ring` / `raw_myelon` broadcast scenarios.
///
/// These use `RawRingSelection::parse` which expects:
///   --class, --mode, --consumers, --target-rate, --events, --json, --tree, --json-out
fn build_raw_ring_args(args: &Args) -> Vec<String> {
    let mut v = vec!["perf-bench-broadcast".to_string()];

    v.push("--class".to_string());
    v.push(args.class.clone());

    v.push("--mode".to_string());
    v.push(args.mode.clone());

    if args.consumers != 1 {
        v.push("--consumers".to_string());
        v.push(args.consumers.to_string());
    }

    if let Some(rate) = args.target_rate {
        v.push("--target-rate".to_string());
        v.push(rate.to_string());
    }

    if let Some(events) = args.events {
        v.push("--events".to_string());
        v.push(events.to_string());
    }

    if let Some(size) = args.size {
        v.push("--size".to_string());
        v.push(size.to_string());
    }

    if args.num_messages != 100000 {
        v.push("--num-messages".to_string());
        v.push(args.num_messages.to_string());
    }

    if args.warmup != 10000 {
        v.push("--warmup".to_string());
        v.push(args.warmup.to_string());
    }

    append_output_args(&mut v, args);
    v
}

/// Build args for framed broadcast scenarios.
///
/// These use `FramedSelection::parse` which expects:
///   --mode, --json, --tree, --json-out
fn build_framed_args(args: &Args) -> Vec<String> {
    let mut v = vec!["perf-bench-broadcast".to_string()];

    v.push("--mode".to_string());
    v.push(args.mode.clone());

    append_output_args(&mut v, args);
    v
}

/// Build args for codec broadcast scenarios.
///
/// These use `CodecSelection::parse` which expects:
///   --mode, --codec, --json, --tree, --json-out
fn build_codec_args(args: &Args) -> Vec<String> {
    let mut v = vec!["perf-bench-broadcast".to_string()];

    v.push("--mode".to_string());
    v.push(args.mode.clone());

    if let Some(ref codec) = args.codec {
        v.push("--codec".to_string());
        v.push(codec.clone());
    }

    append_output_args(&mut v, args);
    v
}

/// Build args for sweep scenarios.
///
/// These use `BasicSweepSelection::parse` or `ReportOutputArgs::from_args` which expect:
///   --mode, --size, --json, --tree, --json-out
fn build_sweep_args(args: &Args) -> Vec<String> {
    let mut v = vec!["perf-bench-broadcast".to_string()];

    v.push("--mode".to_string());
    v.push(args.mode.clone());

    if let Some(size) = args.size {
        v.push("--size".to_string());
        v.push(size.to_string());
    }

    append_output_args(&mut v, args);
    v
}

// ---------------------------------------------------------------------------
// Orchestrator dispatch functions
// ---------------------------------------------------------------------------

fn run_raw_ring(args: &Args) -> Result<(), Box<dyn Error>> {
    use perf_bench::infra::BenchHarness;

    if args.timeout != 300 {
        std::env::set_var("PERF_BENCH_TIMEOUT", args.timeout.to_string());
    }

    let synthetic = build_raw_ring_args(args);

    match args.backend.as_str() {
        "shm" => {
            let bench = perf_bench::layers::raw::disruptor_mp::broadcast_shm::RawRingShmBench;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        "mmap" => {
            let bench = perf_bench::layers::raw::disruptor_mp::broadcast_mmap::RawRingMmapBench;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        _ => Err(format!("unknown backend: {}", args.backend).into()),
    }
}

fn run_framed(args: &Args) -> Result<(), Box<dyn Error>> {
    use perf_bench::infra::BenchHarness;

    if args.timeout != 300 {
        std::env::set_var("PERF_BENCH_TIMEOUT", args.timeout.to_string());
    }

    let synthetic = build_framed_args(args);

    match args.backend.as_str() {
        "shm" => {
            std::env::set_var("PERF_BENCH_BROADCAST_HARNESS", "framed_shm");
            let bench = perf_bench::layers::framed_myelon::frag::broadcast_shm::FramedShmBench;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        "mmap" => {
            std::env::set_var("PERF_BENCH_BROADCAST_HARNESS", "framed_mmap");
            let bench = perf_bench::layers::framed_myelon::frag::broadcast_mmap::FramedMmapBench;
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
            std::env::set_var("PERF_BENCH_BROADCAST_HARNESS", "codec_shm");
            let bench = perf_bench::layers::framed_myelon::codec::shm::CodecE2eShmBench;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        "mmap" => {
            std::env::set_var("PERF_BENCH_BROADCAST_HARNESS", "codec_mmap");
            let bench = perf_bench::layers::framed_myelon::codec::mmap::CodecE2eMmapBench;
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
            std::env::set_var("PERF_BENCH_BROADCAST_HARNESS", "nofrag_shm");
            let bench = perf_bench::layers::framed_myelon::codec::nofrag_shm::CodecNoFragShmBench;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        "mmap" => {
            std::env::set_var("PERF_BENCH_BROADCAST_HARNESS", "nofrag_mmap");
            let bench = perf_bench::layers::framed_myelon::codec::nofrag_mmap::CodecNoFragMmapBench;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        _ => Err(format!("unknown backend: {}", args.backend).into()),
    }
}

fn run_wait_strategy(args: &Args) -> Result<(), Box<dyn Error>> {
    use perf_bench::infra::BenchHarness;

    if args.timeout != 300 {
        std::env::set_var("PERF_BENCH_TIMEOUT", args.timeout.to_string());
    }

    // wait_strategy benches parse args via env vars + ReportOutputArgs::from_args
    let mut synthetic = vec!["perf-bench-broadcast".to_string()];
    append_output_args(&mut synthetic, args);

    match args.backend.as_str() {
        "shm" => {
            let bench =
                perf_bench::layers::raw::disruptor_mp::wait_strategy_shm::WaitStrategyShmBench;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        "mmap" => {
            let bench =
                perf_bench::layers::raw::disruptor_mp::wait_strategy_mmap::WaitStrategyMmapBench;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        _ => Err(format!("unknown backend: {}", args.backend).into()),
    }
}

fn run_myelon_layers(args: &Args) -> Result<(), Box<dyn Error>> {
    use perf_bench::infra::BenchHarness;

    if args.timeout != 300 {
        std::env::set_var("PERF_BENCH_TIMEOUT", args.timeout.to_string());
    }

    let synthetic = build_sweep_args(args);
    let bench = perf_bench::layers::sweeps::myelon_layers::MyelonLayersBench;
    bench.run_orchestrator(&synthetic)?;
    Ok(())
}

fn run_monster_sweep(args: &Args) -> Result<(), Box<dyn Error>> {
    use perf_bench::infra::BenchHarness;

    if args.timeout != 300 {
        std::env::set_var("PERF_BENCH_TIMEOUT", args.timeout.to_string());
    }

    let synthetic = build_sweep_args(args);

    match args.backend.as_str() {
        "shm" => {
            let bench = perf_bench::layers::sweeps::monster_sweep_shm::MonsterSweepShm;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        "mmap" => {
            let bench = perf_bench::layers::sweeps::monster_sweep_mmap::MonsterSweepMmap;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        _ => Err(format!("unknown backend: {}", args.backend).into()),
    }
}

fn run_framed_sweep(args: &Args) -> Result<(), Box<dyn Error>> {
    use perf_bench::infra::BenchHarness;

    if args.timeout != 300 {
        std::env::set_var("PERF_BENCH_TIMEOUT", args.timeout.to_string());
    }

    let synthetic = build_sweep_args(args);
    let bench = perf_bench::layers::sweeps::myelon_framed_sweep::MyelonFramedSweep;
    bench.run_orchestrator(&synthetic)?;
    Ok(())
}

fn run_typed_zc_sweep(args: &Args) -> Result<(), Box<dyn Error>> {
    use perf_bench::infra::BenchHarness;

    if args.timeout != 300 {
        std::env::set_var("PERF_BENCH_TIMEOUT", args.timeout.to_string());
    }

    let synthetic = build_sweep_args(args);
    let bench = perf_bench::layers::sweeps::typed_zero_copy_sweep::TypedZeroCopySweep;
    bench.run_orchestrator(&synthetic)?;
    Ok(())
}

fn run_nofrag(args: &Args) -> Result<(), Box<dyn Error>> {
    use perf_bench::infra::BenchHarness;

    if args.timeout != 300 {
        std::env::set_var("PERF_BENCH_TIMEOUT", args.timeout.to_string());
    }

    let synthetic = build_sweep_args(args);
    let bench = perf_bench::layers::sweeps::nofrag_all::NofragAllBench;
    bench.run_orchestrator(&synthetic)?;
    Ok(())
}

fn run_layout(_args: &Args) -> Result<(), Box<dyn Error>> {
    perf_bench::layers::layout::run_main();
    Ok(())
}
