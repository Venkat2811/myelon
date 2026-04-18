//! Competitive ping-pong benchmark over the SHM backend.
//!
//! This replaces the previous wrapper with a real 1p1c ping-pong benchmark that
//! supports maximum-throughput, coordinated-omission-aware fixed-rate, and
//! low-overhead batch-timing modes.

#[allow(dead_code)]
#[path = "../../../../disruptor-mp/benches/ipc/competitive/common.rs"]
mod common;
#[path = "../../../../disruptor-mp/benches/ipc/competitive/table.rs"]
mod table;

use clap::Parser;
use common::{calculate_data_rate_gbps, format_throughput, BenchmarkEvent};
use disruptor_mp::{
    attach_shared_consumer, build_shared_single_producer, portable_shm_segment_name,
    CoordinationMode, SharedConsumer, SharedProducer,
};
use perf_bench::coordination::UnifiedCoordination;
use perf_bench::harness;
use perf_bench::latency::{self, LatencyRecorder};
use perf_bench::reporting::{self, BenchReport};
use std::env;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

const DISCOVERY_SCAN_SLEEP: Duration = Duration::from_millis(150);
const CONSUMER_PREFIX: &str = "cp";
const ECHO_CONSUMER_ID: &str = "cp_0";
const MAIN_CONSUMER_ID: &str = "cp_0";

fn discovery_scan_rounds(num_consumers: usize) -> usize {
    if num_consumers > 1 {
        8 + num_consumers
    } else {
        8
    }
}

fn warm_discovery_scans<F>(mut scan: F, rounds: usize)
where
    F: FnMut() -> i64,
{
    for _ in 0..rounds {
        let _ = scan();
        std::thread::sleep(DISCOVERY_SCAN_SLEEP);
    }
}

struct ChildProcessGuard {
    child: Option<Child>,
}

impl ChildProcessGuard {
    fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        if let Some(mut child) = self.child.take() {
            child.wait()
        } else {
            use std::os::unix::process::ExitStatusExt;
            Ok(std::process::ExitStatus::from_raw(0))
        }
    }
}

impl Drop for ChildProcessGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Message size in bytes
    #[arg(short = 's', long, default_value = "64")]
    message_size: usize,

    /// Number of messages to send after warmup
    #[arg(short = 'n', long, default_value = "100000")]
    num_messages: u64,

    /// Number of warmup messages
    #[arg(short = 'w', long, default_value = "10000")]
    warmup: u64,

    /// Wait strategy: busyspin, spinloop, sleep, block
    #[arg(long, default_value = "busyspin")]
    wait_strategy: String,

    /// Buffer size in slots
    #[arg(short = 'b', long)]
    buffer_size: Option<usize>,

    /// Emit JSON report to stdout
    #[arg(long)]
    json: bool,

    /// Emit canonical JSON report to stdout and DISRUPTOR_MP_BENCHMARK_JSON_OUT
    #[arg(long)]
    json_canonical: bool,

    /// Write JSON report to this path
    #[arg(long)]
    json_out: Option<String>,

    /// Write CSV report to this path
    #[arg(long)]
    csv_out: Option<String>,

    /// Write Markdown report to this path
    #[arg(long = "md-out")]
    markdown_out: Option<String>,

    /// Skip competitor comparison output
    #[arg(long)]
    no_compare: bool,

    /// Target rate in messages/sec for coordinated-omission-aware mode
    #[arg(long)]
    target_rate: Option<u64>,

    /// Enable low-overhead batch timing mode
    #[arg(long)]
    batch_timing: bool,

    /// Number of messages to burst before draining replies in fixed-rate mode
    #[arg(long, default_value = "1")]
    batch_size: usize,

    /// Internal child-role flag
    #[arg(long, hide = true)]
    process_two: bool,
}

fn benchmark_json_output_path() -> Option<String> {
    env::var("DISRUPTOR_MP_BENCHMARK_JSON_OUT")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn json_mode(args: &Args) -> bool {
    args.json
        || args.json_canonical
        || args.json_out.is_some()
        || args.csv_out.is_some()
        || args.markdown_out.is_some()
}

fn measurement_mode(args: &Args) -> String {
    if args.batch_timing {
        "batch_timing".to_string()
    } else if let Some(target_rate) = args.target_rate {
        format!("co_aware@{target_rate}")
    } else {
        "max_throughput".to_string()
    }
}

fn default_buffer_size(message_size: usize) -> usize {
    match message_size {
        0..=1024 => 4096,
        1025..=16384 => 2048,
        16385..=65536 => 1024,
        _ => 512,
    }
}

fn apply_wait_strategy(wait_strategy: &str) {
    match wait_strategy.to_ascii_lowercase().as_str() {
        "sleep" => std::thread::sleep(Duration::from_micros(50)),
        "block" => std::thread::sleep(Duration::from_millis(1)),
        "spinloop" => std::hint::spin_loop(),
        _ => std::hint::spin_loop(),
    }
}

fn wait_for_next_event<E: Copy + Default>(
    consumer: &mut SharedConsumer<E>,
    wait_strategy: &str,
    deadline: Instant,
    context: &str,
) -> Result<(i64, E), Box<dyn std::error::Error>> {
    loop {
        if let Some(result) = consumer.try_consume_next() {
            return Ok(result);
        }
        if SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
            return Err("shutdown requested".into());
        }
        harness::check_deadline(deadline, context);
        apply_wait_strategy(wait_strategy);
    }
}

fn attach_consumer_with_timeout<E: Copy + Default + 'static>(
    segment: &str,
    buffer_size: usize,
    consumer_id: &str,
    timeout: Duration,
) -> Result<SharedConsumer<E>, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        match attach_shared_consumer::<E>(segment, buffer_size)
            .with_consumer_id(consumer_id)
            .build_consumer()
        {
            Ok(consumer) => return Ok(consumer),
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(error) => return Err(format!("attach failed for {consumer_id}: {error}").into()),
        }
    }
}

fn wait_for_echo_attached(coordination: &UnifiedCoordination, timeout: Duration) -> bool {
    let start = Instant::now();
    while coordination.data().echo_attached.load(Ordering::Acquire) == 0 {
        if start.elapsed() > timeout {
            return false;
        }
        std::hint::spin_loop();
    }
    true
}

fn emit_report(report: &BenchReport, args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let json = serde_json::to_string_pretty(report)?;

    if args.json || args.json_canonical {
        println!("{json}");
    }
    if let Some(path) = args.json_out.as_deref() {
        report.write_json(path)?;
    }
    if let Some(path) = args.csv_out.as_deref() {
        report.write_csv(path)?;
    }
    if let Some(path) = args.markdown_out.as_deref() {
        report.write_markdown(path)?;
    }
    if args.json_canonical {
        if let Some(path) = benchmark_json_output_path() {
            std::fs::write(path, format!("{json}\n"))?;
        }
    }

    Ok(())
}

fn build_report(
    args: &Args,
    throughput: f64,
    buffer_size: usize,
    latency: Option<latency::LatencyStats>,
    verification_passed: bool,
    messages_processed: u64,
) -> BenchReport {
    let mut result = reporting::make_result(
        "competitive_shm",
        &format!(
            "competitive_pingpong_1p1c_{}",
            human_size(args.message_size)
        ),
        "shm",
        "competitive_pingpong",
        None,
        &args.wait_strategy,
        args.message_size,
        buffer_size,
        args.num_messages,
        args.warmup,
        1,
        throughput,
        throughput,
        latency,
    );
    result.measurement_mode = measurement_mode(args);
    result.results.verification_passed = verification_passed;
    result.results.messages_processed = messages_processed;
    result.results.data_rate_mbps = throughput * args.message_size as f64 / 1_000_000.0;
    result.metadata.timestamp = chrono::Utc::now().to_rfc3339();
    result.metadata.git_commit = result.metadata.git_commit.clone();

    let mut report = BenchReport::new();
    report.add(result);
    report
}

fn human_size(bytes: usize) -> String {
    match bytes {
        b if b >= 1024 * 1024 => format!("{}MB", b / (1024 * 1024)),
        b if b >= 1024 => format!("{}KB", b / 1024),
        b => format!("{b}B"),
    }
}

fn spawn_echo_process(
    exe: &Path,
    args: &Args,
    ping_segment: &str,
    pong_segment: &str,
    coordination_name: &str,
    buffer_size: usize,
    message_size: usize,
) -> Result<ChildProcessGuard, Box<dyn std::error::Error>> {
    let mut child_cmd = Command::new(exe);
    child_cmd
        .arg("--process-two")
        .env("PING_SEGMENT", ping_segment)
        .env("PONG_SEGMENT", pong_segment)
        .env("COORDINATION_SEGMENT", coordination_name)
        .env("MESSAGE_SIZE", message_size.to_string())
        .env("BUFFER_SIZE", buffer_size.to_string())
        .env("WAIT_STRATEGY", &args.wait_strategy)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());

    if json_mode(args) {
        child_cmd.env("JSON_MODE", "1");
    }

    Ok(ChildProcessGuard::new(child_cmd.spawn()?))
}

fn print_header(args: &Args, buffer_size: usize) {
    println!("=== Competitive SHM Ping-Pong ===");
    println!("Message size: {}", human_size(args.message_size));
    println!("Buffer size: {} slots", buffer_size);
    println!("Messages: {} (+ {} warmup)", args.num_messages, args.warmup);
    println!("Wait strategy: {}", args.wait_strategy);
    println!("Mode: {}", measurement_mode(args));
    if let Some(target_rate) = args.target_rate {
        println!("Target rate: {} msgs/sec", target_rate);
    }
    println!();
}

fn print_results(
    args: &Args,
    throughput: f64,
    duration: Duration,
    latency: Option<&latency::LatencyStats>,
    verification_passed: bool,
) {
    println!("=== Results ===");
    println!("Total time: {:.6} s", duration.as_secs_f64());
    println!("Throughput: {} ops/sec", format_throughput(throughput));
    println!(
        "Data rate: {:.2} GB/s",
        calculate_data_rate_gbps(throughput, args.message_size)
    );
    println!(
        "Verification: {}",
        if verification_passed {
            "PASSED"
        } else {
            "FAILED"
        }
    );

    if let Some(stats) = latency {
        println!("\nLatency: {}", stats.summary());
    } else if args.batch_timing {
        println!("\nLatency: batch timing mode (average-only result)");
    }

    if !args.no_compare && !args.batch_timing && args.target_rate.is_none() {
        if let Some(stats) = latency {
            let competitors = table::CompetitorBenchmarks::new();
            println!("\nComparison:");
            if let Some(speedup) =
                competitors.get_speedup(args.message_size, stats.p50_ns as f64, "shmipc-rs")
            {
                println!("vs shmipc-rs: {speedup}");
            }
            if let Some(speedup) =
                competitors.get_speedup(args.message_size, stats.p50_ns as f64, "shmipc-go")
            {
                println!("vs shmipc-go: {speedup}");
            }
        } else {
            println!("\nComparison: no latency histogram available for this mode");
        }
    }
}

fn batch_timing_stats(duration: Duration, messages: u64) -> Option<latency::LatencyStats> {
    if messages == 0 {
        return None;
    }
    let average_ns = (duration.as_nanos() as u64).saturating_div(messages);
    Some(latency::LatencyStats {
        count: messages,
        min_ns: average_ns,
        max_ns: average_ns,
        mean_ns: average_ns as f64,
        stdev_ns: 0.0,
        p50_ns: average_ns,
        p90_ns: average_ns,
        p95_ns: average_ns,
        p99_ns: average_ns,
        p999_ns: average_ns,
        p9999_ns: average_ns,
        p99999_ns: average_ns,
    })
}

fn run_throughput_mode<const SIZE: usize>(
    args: &Args,
    producer_ping: &mut SharedProducer<BenchmarkEvent<SIZE>>,
    pong_consumer: &mut SharedConsumer<BenchmarkEvent<SIZE>>,
    coordination: &UnifiedCoordination,
) -> Result<(f64, Duration, Option<latency::LatencyStats>, bool, u64), Box<dyn std::error::Error>> {
    let deadline = harness::spin_deadline();
    let benchmark_start = Instant::now();
    let mut recorder = LatencyRecorder::default_range();

    for i in 0..args.num_messages {
        if SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
            break;
        }

        let send_start = Instant::now();
        let mut event = BenchmarkEvent::<SIZE>::new(args.warmup + i);
        event.set_timestamp();
        producer_ping.publish(|slot| *slot = event);
        coordination
            .data()
            .events_sent
            .fetch_add(1, Ordering::Release);

        let (_sequence, response) = wait_for_next_event(
            pong_consumer,
            &args.wait_strategy,
            deadline,
            "competitive_shm throughput receive",
        )?;
        assert_eq!(response.sequence, args.warmup + i);
        recorder.record(send_start.elapsed().as_nanos() as u64);
        coordination
            .data()
            .events_echoed
            .fetch_add(1, Ordering::Release);
    }

    let duration = benchmark_start.elapsed();
    let messages_processed = coordination.data().events_echoed.load(Ordering::Relaxed) as u64;
    let throughput = messages_processed as f64 / duration.as_secs_f64();
    let verification_passed = messages_processed == args.num_messages;
    Ok((
        throughput,
        duration,
        recorder.stats(),
        verification_passed,
        messages_processed,
    ))
}

fn run_batch_timing_mode<const SIZE: usize>(
    args: &Args,
    producer_ping: &mut SharedProducer<BenchmarkEvent<SIZE>>,
    pong_consumer: &mut SharedConsumer<BenchmarkEvent<SIZE>>,
    coordination: &UnifiedCoordination,
) -> Result<(f64, Duration, Option<latency::LatencyStats>, bool, u64), Box<dyn std::error::Error>> {
    let deadline = harness::spin_deadline();
    let benchmark_start = Instant::now();

    for i in 0..args.num_messages {
        if SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
            break;
        }

        let mut event = BenchmarkEvent::<SIZE>::new(args.warmup + i);
        event.set_timestamp();
        producer_ping.publish(|slot| *slot = event);
        coordination
            .data()
            .events_sent
            .fetch_add(1, Ordering::Release);

        let (_sequence, response) = wait_for_next_event(
            pong_consumer,
            &args.wait_strategy,
            deadline,
            "competitive_shm batch timing receive",
        )?;
        assert_eq!(response.sequence, args.warmup + i);
        coordination
            .data()
            .events_echoed
            .fetch_add(1, Ordering::Release);
    }

    let duration = benchmark_start.elapsed();
    let messages_processed = coordination.data().events_echoed.load(Ordering::Relaxed) as u64;
    let throughput = messages_processed as f64 / duration.as_secs_f64();
    let verification_passed = messages_processed == args.num_messages;
    Ok((
        throughput,
        duration,
        batch_timing_stats(duration, messages_processed),
        verification_passed,
        messages_processed,
    ))
}

fn run_fixed_rate_mode<const SIZE: usize>(
    args: &Args,
    producer_ping: &mut SharedProducer<BenchmarkEvent<SIZE>>,
    pong_consumer: &mut SharedConsumer<BenchmarkEvent<SIZE>>,
    coordination: &UnifiedCoordination,
) -> Result<(f64, Duration, Option<latency::LatencyStats>, bool, u64), Box<dyn std::error::Error>> {
    let target_rate = args
        .target_rate
        .ok_or("--target-rate is required for fixed-rate mode")?;
    let deadline = harness::spin_deadline();
    let interval_nanos = 1_000_000_000u64 / target_rate;
    let interval = Duration::from_nanos(interval_nanos);
    let mut actual = LatencyRecorder::default_range();
    let mut corrected = LatencyRecorder::default_range();

    for i in 0..args.warmup {
        let mut event = BenchmarkEvent::<SIZE>::new(i);
        event.set_timestamp();
        producer_ping.publish(|slot| *slot = event);
        coordination
            .data()
            .warmup_sent
            .fetch_add(1, Ordering::Release);
        let (_sequence, response) = wait_for_next_event(
            pong_consumer,
            &args.wait_strategy,
            deadline,
            "competitive_shm fixed-rate warmup receive",
        )?;
        assert_eq!(response.sequence, i);
        coordination
            .data()
            .warmup_echoed
            .fetch_add(1, Ordering::Release);
    }

    let benchmark_start = Instant::now();
    let base = Instant::now();
    let mut next_send_time = benchmark_start;
    let mut sent = 0u64;

    while sent < args.num_messages {
        if SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
            break;
        }

        let batch_end = std::cmp::min(sent + args.batch_size as u64, args.num_messages);
        let batch_len = (batch_end - sent) as usize;

        while Instant::now() < next_send_time {
            std::hint::spin_loop();
        }

        for offset in 0..batch_len {
            let seq = args.warmup + sent + offset as u64;
            let mut event = BenchmarkEvent::<SIZE>::new(seq);
            event.intended_send_time_ns = next_send_time.duration_since(base).as_nanos() as u64;
            event.timestamp_ns = Instant::now().duration_since(base).as_nanos() as u64;
            producer_ping.publish(|slot| *slot = event);
            coordination
                .data()
                .events_sent
                .fetch_add(1, Ordering::Release);
        }

        for offset in 0..batch_len {
            let expected = args.warmup + sent + offset as u64;
            let (_sequence, response) = wait_for_next_event(
                pong_consumer,
                &args.wait_strategy,
                deadline,
                "competitive_shm fixed-rate receive",
            )?;
            assert_eq!(response.sequence, expected);

            let now_ns = Instant::now().duration_since(base).as_nanos() as u64;
            actual.record(now_ns.saturating_sub(response.timestamp_ns));
            corrected.record(now_ns.saturating_sub(response.intended_send_time_ns));
            coordination
                .data()
                .events_echoed
                .fetch_add(1, Ordering::Release);
        }

        sent = batch_end;
        next_send_time += interval * batch_len as u32;
    }

    let duration = benchmark_start.elapsed();
    let messages_processed = coordination.data().events_echoed.load(Ordering::Relaxed) as u64;
    let throughput = messages_processed as f64 / duration.as_secs_f64();
    let verification_passed = messages_processed == args.num_messages;

    let actual_stats = actual.stats();
    let corrected_stats = corrected.stats();

    if !json_mode(args) {
        if let Some(actual_stats) = actual_stats.as_ref() {
            println!("\nActual RTT latency: {}", actual_stats.summary());
        }
        if let Some(corrected_stats) = corrected_stats.as_ref() {
            println!(
                "Coordinated omission corrected latency: {}",
                corrected_stats.summary()
            );
        }
    }

    Ok((
        throughput,
        duration,
        corrected_stats,
        verification_passed,
        messages_processed,
    ))
}

fn run_benchmark<const SIZE: usize>(
    args: Args,
    buffer_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if args.batch_timing && args.target_rate.is_some() {
        return Err("--batch-timing cannot be combined with --target-rate".into());
    }
    if args.batch_size == 0 {
        return Err("--batch-size must be greater than zero".into());
    }

    if !json_mode(&args) {
        print_header(&args, buffer_size);
    }

    let timeout = harness::bench_timeout_duration(120);
    let ping_segment = portable_shm_segment_name("cpshmping");
    let pong_segment = portable_shm_segment_name("cpshmpong");
    let coordination_name = portable_shm_segment_name("cpshmcoo");

    let coordination = UnifiedCoordination::create(&coordination_name)?;
    let mut producer_ping =
        build_shared_single_producer::<BenchmarkEvent<SIZE>>(&ping_segment, buffer_size)
            .discover_consumer_with_prefix(1, CONSUMER_PREFIX)
            .with_coordination(CoordinationMode::Immediate)
            .build_producer(BenchmarkEvent::default)?;
    coordination
        .data()
        .producer_ready
        .store(1, Ordering::Release);

    let exe = env::current_exe()?;
    let mut child_guard = spawn_echo_process(
        &exe,
        &args,
        &ping_segment,
        &pong_segment,
        &coordination_name,
        buffer_size,
        SIZE,
    )?;

    if !coordination.wait_for_echo_ready(timeout) {
        return Err("timeout waiting for echo process".into());
    }
    warm_discovery_scans(
        || producer_ping.min_gating_sequence(),
        discovery_scan_rounds(1),
    );

    let mut pong_consumer = attach_consumer_with_timeout::<BenchmarkEvent<SIZE>>(
        &pong_segment,
        buffer_size,
        MAIN_CONSUMER_ID,
        timeout,
    )?;
    coordination
        .data()
        .consumer_attached
        .store(1, Ordering::Release);
    if !wait_for_echo_attached(&coordination, timeout) {
        return Err("timeout waiting for echo service readiness".into());
    }

    for i in 0..args.warmup {
        let event = BenchmarkEvent::<SIZE>::new(i);
        producer_ping.publish(|slot| *slot = event);
        coordination
            .data()
            .warmup_sent
            .fetch_add(1, Ordering::Release);
        let (_sequence, response) = wait_for_next_event(
            &mut pong_consumer,
            &args.wait_strategy,
            harness::spin_deadline_or(120),
            "competitive_shm warmup receive",
        )?;
        assert_eq!(response.sequence, i);
        coordination
            .data()
            .warmup_echoed
            .fetch_add(1, Ordering::Release);
    }

    let (throughput, duration, latency, verification_passed, messages_processed) =
        if args.target_rate.is_some() {
            run_fixed_rate_mode(&args, &mut producer_ping, &mut pong_consumer, &coordination)?
        } else if args.batch_timing {
            run_batch_timing_mode(&args, &mut producer_ping, &mut pong_consumer, &coordination)?
        } else {
            run_throughput_mode(&args, &mut producer_ping, &mut pong_consumer, &coordination)?
        };

    coordination.signal_shutdown();
    let child_status = child_guard.wait()?;
    if !child_status.success() {
        return Err(format!("echo child exited with status {child_status}").into());
    }

    let report = build_report(
        &args,
        throughput,
        buffer_size,
        latency.clone(),
        verification_passed,
        messages_processed,
    );

    if json_mode(&args) {
        emit_report(&report, &args)?;
    } else {
        print_results(
            &args,
            throughput,
            duration,
            latency.as_ref(),
            verification_passed,
        );
    }

    Ok(())
}

fn run_process_one(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let buffer_size = args
        .buffer_size
        .unwrap_or_else(|| default_buffer_size(args.message_size));

    match args.message_size {
        64 => run_benchmark::<64>(args, buffer_size),
        512 => run_benchmark::<512>(args, buffer_size),
        1024 => run_benchmark::<1024>(args, buffer_size),
        4096 => run_benchmark::<4096>(args, buffer_size),
        16384 => run_benchmark::<16384>(args, buffer_size),
        65536 => run_benchmark::<65536>(args, buffer_size),
        131072 => run_benchmark::<131072>(args, buffer_size),
        _ => Err(format!(
            "unsupported message size: {} (expected 64, 512, 1024, 4096, 16384, 65536, 131072)",
            args.message_size
        )
        .into()),
    }
}

fn run_process_two() -> Result<(), Box<dyn std::error::Error>> {
    let ping_segment = env::var("PING_SEGMENT")?;
    let pong_segment = env::var("PONG_SEGMENT")?;
    let coordination_name = env::var("COORDINATION_SEGMENT")?;
    let message_size: usize = env::var("MESSAGE_SIZE")?.parse()?;
    let buffer_size: usize = env::var("BUFFER_SIZE")?.parse()?;
    let wait_strategy = env::var("WAIT_STRATEGY").unwrap_or_else(|_| "busyspin".to_string());

    let coordination =
        UnifiedCoordination::attach_with_timeout(&coordination_name, Duration::from_secs(30))?;
    if !coordination.wait_for_producer_ready(Duration::from_secs(30)) {
        return Err("timeout waiting for producer readiness".into());
    }

    match message_size {
        64 => echo_server::<64>(
            &ping_segment,
            &pong_segment,
            &coordination,
            buffer_size,
            &wait_strategy,
        ),
        512 => echo_server::<512>(
            &ping_segment,
            &pong_segment,
            &coordination,
            buffer_size,
            &wait_strategy,
        ),
        1024 => echo_server::<1024>(
            &ping_segment,
            &pong_segment,
            &coordination,
            buffer_size,
            &wait_strategy,
        ),
        4096 => echo_server::<4096>(
            &ping_segment,
            &pong_segment,
            &coordination,
            buffer_size,
            &wait_strategy,
        ),
        16384 => echo_server::<16384>(
            &ping_segment,
            &pong_segment,
            &coordination,
            buffer_size,
            &wait_strategy,
        ),
        65536 => echo_server::<65536>(
            &ping_segment,
            &pong_segment,
            &coordination,
            buffer_size,
            &wait_strategy,
        ),
        131072 => echo_server::<131072>(
            &ping_segment,
            &pong_segment,
            &coordination,
            buffer_size,
            &wait_strategy,
        ),
        _ => Err(format!("unsupported child message size: {message_size}").into()),
    }
}

fn echo_server<const SIZE: usize>(
    ping_segment: &str,
    pong_segment: &str,
    coordination: &UnifiedCoordination,
    buffer_size: usize,
    wait_strategy: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut ping_consumer = attach_consumer_with_timeout::<BenchmarkEvent<SIZE>>(
        ping_segment,
        buffer_size,
        ECHO_CONSUMER_ID,
        Duration::from_secs(30),
    )?;
    let mut pong_producer =
        build_shared_single_producer::<BenchmarkEvent<SIZE>>(pong_segment, buffer_size)
            .discover_consumer_with_prefix(1, CONSUMER_PREFIX)
            .with_coordination(CoordinationMode::Immediate)
            .build_producer(BenchmarkEvent::default)?;

    coordination.data().echo_ready.store(1, Ordering::Release);

    if !coordination.wait_for_consumer_attached(Duration::from_secs(30)) {
        return Err("timeout waiting for pong consumer attachment".into());
    }
    warm_discovery_scans(
        || pong_producer.min_gating_sequence(),
        discovery_scan_rounds(1),
    );
    coordination
        .data()
        .echo_attached
        .store(1, Ordering::Release);

    let deadline = harness::spin_deadline_or(120);
    while !coordination.is_shutdown() {
        match ping_consumer.try_consume_next() {
            Some((_sequence, event)) => {
                pong_producer.publish(|slot| *slot = event);
            }
            None => {
                if SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
                    break;
                }
                harness::check_deadline(deadline, "competitive_shm echo loop");
                apply_wait_strategy(wait_strategy);
            }
        }
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    ctrlc::set_handler(move || {
        SHUTDOWN_REQUESTED.store(true, Ordering::Release);
    })?;

    let filtered_args: Vec<String> = env::args().filter(|arg| arg != "--bench").collect();
    let filtered_args = harness::apply_timeout_arg(&filtered_args)
        .map_err(|error| format!("competitive_shm failed: {error}"))?;
    let args = Args::parse_from(filtered_args);

    if args.process_two {
        run_process_two()
    } else {
        run_process_one(args)
    }
}
