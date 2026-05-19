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
    #[arg(long, value_parser = ["raw_ring", "raw_myelon", "framed", "codec",
                                 "typed_zc", "wait_strategy", "myelon_layers", "monster_sweep",
                                 "framed_sweep", "typed_zc_sweep", "nofrag", "layout"])]
    layer: String,

    /// Backend transport
    #[arg(long, value_parser = ["shm", "mmap"], default_value = "shm")]
    backend: String,

    /// Message size in bytes (logical event size including 16B header).
    /// For `raw_ring`: overrides the default 144B message events (e.g. --size 2048).
    /// Physical slot bytes may round up to the raw event's 64B alignment.
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

    /// Quick-mode hint for sweep families that expose a reduced subset.
    #[arg(long)]
    quick: bool,

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
        "raw_myelon" => run_raw_myelon(&args),
        "framed" => run_framed(&args),
        "codec" => run_codec(&args),
        "typed_zc" => run_typed_zc_sweep(&args),
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
        "raw_myelon_shm" => {
            Some(&perf_bench::layers::raw::myelon::broadcast_shm::RawMyelonShmBench)
        }
        "raw_myelon_mmap" => {
            Some(&perf_bench::layers::raw::myelon::broadcast_mmap::RawMyelonMmapBench)
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
        &perf_bench::layers::raw::myelon::broadcast_shm::RawMyelonShmBench,
        &perf_bench::layers::raw::myelon::broadcast_mmap::RawMyelonMmapBench,
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
    if args.quick {
        v.push("--quick".to_string());
    }
}

fn sweep_size_tag(size: usize) -> Option<&'static str> {
    match size {
        1024 => Some("1KB"),
        4096 => Some("4KB"),
        16384 => Some("16KB"),
        65536 => Some("64KB"),
        131072 => Some("128KB"),
        262144 => Some("256KB"),
        524288 => Some("512KB"),
        1048576 => Some("1MB"),
        _ => None,
    }
}

fn framed_payload_tag(size: usize) -> Option<&'static str> {
    match size {
        1024 => Some("1K"),
        32768 => Some("32K"),
        65536 => Some("64K"),
        131072 => Some("128K"),
        _ => None,
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

    v.push("--consumers".to_string());
    v.push(args.consumers.to_string());

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
///   --mode, --payload, --consumers, --num-messages, --json, --tree, --json-out
fn build_framed_args(args: &Args) -> Vec<String> {
    let mut v = vec!["perf-bench-broadcast".to_string()];

    v.push("--mode".to_string());
    v.push(args.mode.clone());

    if let Some(size) = args.size.and_then(framed_payload_tag) {
        v.push("--payload".to_string());
        v.push(size.to_string());
    }

    v.push("--consumers".to_string());
    v.push(args.consumers.to_string());

    if args.num_messages != 100000 {
        v.push("--num-messages".to_string());
        v.push(args.num_messages.to_string());
    }

    append_output_args(&mut v, args);
    v
}

/// Build args for codec broadcast scenarios.
///
/// These use `CodecSelection::parse` which expects:
///   --mode, --codec, --consumers, --batch, --num-messages, --target-rate, --json, --tree, --json-out
fn build_codec_args(args: &Args) -> Vec<String> {
    let mut v = vec!["perf-bench-broadcast".to_string()];

    v.push("--mode".to_string());
    v.push(args.mode.clone());

    if let Some(ref codec) = args.codec {
        v.push("--codec".to_string());
        v.push(codec.clone());
    }

    v.push("--consumers".to_string());
    v.push(args.consumers.to_string());

    v.push("--batch".to_string());
    v.push(args.batch_size.to_string());

    if args.num_messages != 100000 {
        v.push("--num-messages".to_string());
        v.push(args.num_messages.to_string());
    }

    if let Some(rate) = args.target_rate {
        v.push("--target-rate".to_string());
        v.push(rate.to_string());
    }

    append_output_args(&mut v, args);
    v
}

/// Build args for sweep scenarios.
///
/// These use `BasicSweepSelection::parse` or `ReportOutputArgs::from_args` which expect:
///   --mode, --backend, --size, --consumers, --num-messages, --target-rate, --json, --tree, --json-out
fn process_cli_requested(flag: &str) -> bool {
    std::env::args().any(|arg| arg == flag)
}

fn build_sweep_args_inner(args: &Args, include_consumers_filter: bool) -> Vec<String> {
    let mut v = vec!["perf-bench-broadcast".to_string()];

    v.push("--mode".to_string());
    v.push(args.mode.clone());

    v.push("--backend".to_string());
    v.push(args.backend.clone());

    if let Some(size) = args.size.and_then(sweep_size_tag) {
        v.push("--size".to_string());
        v.push(size.to_string());
    }

    if include_consumers_filter {
        v.push("--consumers".to_string());
        v.push(args.consumers.to_string());
    }

    if args.num_messages != 100000 {
        v.push("--num-messages".to_string());
        v.push(args.num_messages.to_string());
    }

    if let Some(ref codec) = args.codec {
        v.push("--codec".to_string());
        v.push(codec.clone());
    }

    if let Some(rate) = args.target_rate {
        v.push("--target-rate".to_string());
        v.push(rate.to_string());
    }

    append_output_args(&mut v, args);
    v
}

fn build_sweep_args(args: &Args) -> Vec<String> {
    build_sweep_args_inner(args, process_cli_requested("--consumers"))
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

fn run_raw_myelon(args: &Args) -> Result<(), Box<dyn Error>> {
    use perf_bench::infra::BenchHarness;

    if args.timeout != 300 {
        std::env::set_var("PERF_BENCH_TIMEOUT", args.timeout.to_string());
    }

    let synthetic = build_raw_ring_args(args);

    match args.backend.as_str() {
        "shm" => {
            std::env::set_var("PERF_BENCH_BROADCAST_HARNESS", "raw_myelon_shm");
            let bench = perf_bench::layers::raw::myelon::broadcast_shm::RawMyelonShmBench;
            bench.run_orchestrator(&synthetic)?;
            Ok(())
        }
        "mmap" => {
            std::env::set_var("PERF_BENCH_BROADCAST_HARNESS", "raw_myelon_mmap");
            let bench = perf_bench::layers::raw::myelon::broadcast_mmap::RawMyelonMmapBench;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_args() -> Args {
        Args {
            layer: "nofrag".to_string(),
            backend: "mmap".to_string(),
            size: Some(1024),
            mode: "co".to_string(),
            target_rate: Some(20_000),
            wait_strategy: "busyspin".to_string(),
            codec: Some("rkyv".to_string()),
            consumers: 4,
            batch_size: 8,
            num_messages: 1_000,
            warmup: 100,
            timeout: 120,
            quick: true,
            class: "message".to_string(),
            events: Some(1_000),
            json: false,
            tree: true,
            json_out: Some("out.json".to_string()),
        }
    }

    #[test]
    fn build_framed_args_maps_size_and_consumers() {
        let mut args = sample_args();
        args.layer = "framed".to_string();
        args.size = Some(32 * 1024);
        let built = build_framed_args(&args);
        assert!(built.windows(2).any(|pair| pair == ["--payload", "32K"]));
        assert!(built.windows(2).any(|pair| pair == ["--consumers", "4"]));
        assert!(built
            .windows(2)
            .any(|pair| pair == ["--num-messages", "1000"]));
    }

    #[test]
    fn build_codec_args_preserves_codec_batch_and_rate() {
        let args = sample_args();
        let built = build_codec_args(&args);
        assert!(built.windows(2).any(|pair| pair == ["--codec", "rkyv"]));
        assert!(built.windows(2).any(|pair| pair == ["--batch", "8"]));
        assert!(built
            .windows(2)
            .any(|pair| pair == ["--num-messages", "1000"]));
        assert!(built
            .windows(2)
            .any(|pair| pair == ["--target-rate", "20000"]));
        assert!(built.windows(2).any(|pair| pair == ["--consumers", "4"]));
    }

    #[test]
    fn build_sweep_args_preserves_backend_size_consumers_and_quick() {
        let args = sample_args();
        let built = build_sweep_args_inner(&args, true);
        assert!(built.windows(2).any(|pair| pair == ["--backend", "mmap"]));
        assert!(built.windows(2).any(|pair| pair == ["--size", "1KB"]));
        assert!(built.windows(2).any(|pair| pair == ["--consumers", "4"]));
        assert!(built
            .windows(2)
            .any(|pair| pair == ["--num-messages", "1000"]));
        assert!(built.windows(2).any(|pair| pair == ["--codec", "rkyv"]));
        assert!(built
            .windows(2)
            .any(|pair| pair == ["--target-rate", "20000"]));
        assert!(built.iter().any(|arg| arg == "--quick"));
    }

    #[test]
    fn build_sweep_args_omits_default_consumer_filter_when_not_requested() {
        let args = sample_args();
        let built = build_sweep_args_inner(&args, false);
        assert!(!built.iter().any(|arg| arg == "--consumers"));
        assert!(!built.iter().any(|arg| arg == "4"));
    }

    #[test]
    fn typed_zero_copy_is_advertised_as_a_broadcast_layer_alias() {
        let parsed = Args::try_parse_from([
            "perf-bench-broadcast",
            "--layer",
            "typed_zc",
            "--backend",
            "mmap",
        ])
        .expect("typed_zc should map to the typed zero-copy broadcast sweep");
        assert_eq!(parsed.layer, "typed_zc");
        assert_eq!(parsed.backend, "mmap");
    }
}
