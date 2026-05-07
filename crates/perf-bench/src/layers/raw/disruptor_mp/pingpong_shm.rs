//! PingPong benchmark over the SHM backend.
//!
//! This replaces the previous wrapper with a real 1p1c ping-pong benchmark that
//! supports maximum-throughput, coordinated-omission-aware fixed-rate, and
//! low-overhead batch-timing modes.

use crate::bench_support::{common, table};
use crate::cli::pingpong::{self, PingPongArgs as Args, PingPongBackend};
use crate::infra;
use crate::infra::coordination::UnifiedCoordination;
use crate::infra::latency::{self, LatencyRecorder};
use crate::infra::liveness::{liveness_config, liveness_enabled};
use crate::infra::output::reporting::{self, BenchReport};
use clap::Parser;
use common::{calculate_data_rate_gbps, format_throughput, BenchmarkEvent};
use disruptor_mp::{
    attach_shared_consumer, build_shared_single_producer, portable_shm_segment_name,
    CoordinationMode, SharedConsumer, SharedProducer,
};
use std::env;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);
type PingPongRunResult =
    Result<(f64, Duration, Option<latency::LatencyStats>, bool, u64), Box<dyn std::error::Error>>;

/// Dispatch a publish through `publish_managed` when
/// `PERF_BENCH_LIVENESS=on` is set, or through plain `publish`
/// otherwise. The cached flag check is one atomic load (the
/// `OnceLock` in `infra::liveness`), and the branch resolves the
/// same way every iteration of a hot loop, so the predictor
/// collapses it to ~zero overhead. The `?` bubbles
/// `RequiredConsumerError` up through the surrounding `Result`
/// return type — every call site here is in a function that
/// already returns `Result<…, Box<dyn Error>>`.
macro_rules! publish_or_managed {
    ($producer:expr, $body:expr) => {{
        if $crate::infra::liveness::liveness_enabled() {
            $producer.publish_managed($body)?;
        } else {
            $producer.publish($body);
        }
    }};
}

const DISCOVERY_SCAN_SLEEP: Duration = Duration::from_millis(150);
const CONSUMER_PREFIX: &str = "cp";
const ECHO_CONSUMER_ID: &str = "cp_0";
const MAIN_CONSUMER_ID: &str = "cp_0";

/// Env var the parent sets on the spawned echo child when
/// `--enable-counters` was passed, so the child enables the same
/// counters wiring. Read by `run_process_two`. Opt-in only — absent
/// or any value other than "1" leaves the hot path counter-free.
const COUNTERS_ENV_VAR: &str = "PERF_BENCH_PINGPONG_COUNTERS";

/// Attach an RFC-0040 counters file to a `SharedProducer` when
/// `enabled` is true. Allocates a private, leaked, cache-line-aligned
/// region per call — the bench process keeps it for its full lifetime,
/// so leaking is the simplest way to satisfy the `&'static CountersFile`
/// borrow `attach_counters` needs. Off by default so the standard bench
/// path stays bit-identical to pre-RFC-0040 behavior.
fn maybe_attach_counters_producer<E: Copy + Default + 'static>(
    producer: &mut SharedProducer<E>,
    enabled: bool,
) {
    if !enabled {
        return;
    }
    use disruptor_mp::observability::{CountersFile, COUNTERS_FILE_RESERVED_BYTES};
    use std::ptr::NonNull;
    #[repr(C, align(64))]
    struct AlignedRegion([u8; COUNTERS_FILE_RESERVED_BYTES]);
    let leaked: &'static mut AlignedRegion =
        Box::leak(Box::new(AlignedRegion([0u8; COUNTERS_FILE_RESERVED_BYTES])));
    let ptr = NonNull::new(leaked.0.as_mut_ptr()).expect("Box::leak yields non-null");
    let file = unsafe { CountersFile::init(ptr) };
    let leaked_file: &'static CountersFile = Box::leak(Box::new(file));
    producer.attach_counters(leaked_file);
}

/// Sibling of `maybe_attach_counters_producer` for `SharedConsumer`.
fn maybe_attach_counters_consumer<E: Copy + Default + 'static>(
    consumer: &mut SharedConsumer<E>,
    enabled: bool,
) {
    if !enabled {
        return;
    }
    use disruptor_mp::observability::{CountersFile, COUNTERS_FILE_RESERVED_BYTES};
    use std::ptr::NonNull;
    #[repr(C, align(64))]
    struct AlignedRegion([u8; COUNTERS_FILE_RESERVED_BYTES]);
    let leaked: &'static mut AlignedRegion =
        Box::leak(Box::new(AlignedRegion([0u8; COUNTERS_FILE_RESERVED_BYTES])));
    let ptr = NonNull::new(leaked.0.as_mut_ptr()).expect("Box::leak yields non-null");
    let file = unsafe { CountersFile::init(ptr) };
    let leaked_file: &'static CountersFile = Box::leak(Box::new(file));
    consumer.attach_counters(leaked_file);
}

fn child_counters_enabled() -> bool {
    env::var(COUNTERS_ENV_VAR).ok().as_deref() == Some("1")
}

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

struct ShmLane<const SIZE: usize> {
    producer_ping: SharedProducer<BenchmarkEvent<SIZE>>,
    pong_consumer: SharedConsumer<BenchmarkEvent<SIZE>>,
    coordination: UnifiedCoordination,
}

fn write_event<const SIZE: usize>(
    slot: &mut BenchmarkEvent<SIZE>,
    sequence: u64,
    timestamp_ns: u64,
    intended_send_time_ns: u64,
) {
    slot.sequence = sequence;
    slot.timestamp_ns = timestamp_ns;
    slot.intended_send_time_ns = intended_send_time_ns;
}

fn wait_for_next_event<E: Copy + Default, R, F: FnOnce(&E) -> R>(
    consumer: &mut SharedConsumer<E>,
    wait_strategy: &str,
    deadline: Instant,
    context: &str,
    project: F,
) -> Result<R, Box<dyn std::error::Error>> {
    loop {
        if let Some(result) = consumer.try_consume_next_leased() {
            return Ok(project(&result));
        }
        if SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
            return Err("shutdown requested".into());
        }
        infra::check_deadline(deadline, context);
        pingpong::apply_wait_strategy(wait_strategy);
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

fn build_report(
    args: &Args,
    throughput: f64,
    buffer_size: usize,
    latency: Option<latency::LatencyStats>,
    verification_passed: bool,
    messages_processed: u64,
) -> BenchReport {
    pingpong::build_report(
        PingPongBackend::Shm,
        args,
        throughput,
        buffer_size,
        latency,
        verification_passed,
        messages_processed,
    )
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

    if pingpong::json_mode(args) {
        child_cmd.env("JSON_MODE", "1");
    }

    if args.enable_counters {
        child_cmd.env(COUNTERS_ENV_VAR, "1");
    }

    Ok(ChildProcessGuard::new(child_cmd.spawn()?))
}

fn print_results(
    args: &Args,
    throughput: f64,
    duration: Duration,
    latency: Option<&latency::LatencyStats>,
    verification_passed: bool,
    messages_processed: u64,
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
        if let Some(avg_rtt_ns) = pingpong::average_rtt_ns(duration, messages_processed) {
            println!(
                "\nAverage RTT: {:.0} ns (batch timing mode; no histogram)",
                avg_rtt_ns
            );
        } else {
            println!("\nLatency: batch timing mode (no samples recorded)");
        }
    }

    if !args.no_compare && !args.batch_timing && args.target_rate.is_none() && args.consumers == 1 {
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
    } else if !args.no_compare && args.consumers > 1 {
        println!("\nComparison: no external competitor data for multi-consumer mode");
    }
}

fn run_warmup<const SIZE: usize>(
    args: &Args,
    lanes: &mut [ShmLane<SIZE>],
    context: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = infra::spin_deadline();
    let lane_count = lanes.len();
    let mut warmed = 0u64;

    while warmed < args.warmup {
        if SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
            return Err("shutdown requested during warmup".into());
        }

        let round = std::cmp::min((args.warmup - warmed) as usize, lane_count);
        let mut pending = Vec::with_capacity(round);

        for offset in 0..round {
            let lane_idx = (warmed as usize + offset) % lane_count;
            let seq = warmed + offset as u64;
            publish_or_managed!(lanes[lane_idx].producer_ping, |slot| write_event(
                slot,
                seq,
                common::nanos_now(),
                0
            ));
            lanes[lane_idx]
                .coordination
                .data()
                .warmup_sent
                .fetch_add(1, Ordering::Release);
            pending.push((lane_idx, seq));
        }

        for (lane_idx, expected) in pending {
            let response_sequence = wait_for_next_event(
                &mut lanes[lane_idx].pong_consumer,
                &args.wait_strategy,
                deadline,
                context,
                |response| response.sequence,
            )?;
            assert_eq!(response_sequence, expected);
            lanes[lane_idx]
                .coordination
                .data()
                .warmup_echoed
                .fetch_add(1, Ordering::Release);
        }

        warmed += round as u64;
    }

    Ok(())
}

fn run_throughput_mode<const SIZE: usize>(
    args: &Args,
    lanes: &mut [ShmLane<SIZE>],
) -> PingPongRunResult {
    let deadline = infra::spin_deadline();
    let benchmark_start = Instant::now();
    let mut recorder = LatencyRecorder::default_range();
    let lane_count = lanes.len();
    let mut messages_processed = 0u64;

    while messages_processed < args.num_messages {
        if SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
            break;
        }

        let round = std::cmp::min(
            (args.num_messages - messages_processed) as usize,
            lane_count,
        );
        let mut pending = Vec::with_capacity(round);

        for offset in 0..round {
            let lane_idx = (messages_processed as usize + offset) % lane_count;
            let seq = args.warmup + messages_processed + offset as u64;
            let send_start = Instant::now();
            publish_or_managed!(lanes[lane_idx].producer_ping, |slot| write_event(
                slot,
                seq,
                common::nanos_now(),
                0
            ));
            lanes[lane_idx]
                .coordination
                .data()
                .events_sent
                .fetch_add(1, Ordering::Release);
            pending.push((lane_idx, seq, send_start));
        }

        for (lane_idx, expected, send_start) in pending {
            let response_sequence = wait_for_next_event(
                &mut lanes[lane_idx].pong_consumer,
                &args.wait_strategy,
                deadline,
                "pingpong_shm throughput receive",
                |response| response.sequence,
            )?;
            assert_eq!(response_sequence, expected);
            recorder.record(send_start.elapsed().as_nanos() as u64);
            lanes[lane_idx]
                .coordination
                .data()
                .events_echoed
                .fetch_add(1, Ordering::Release);
            messages_processed += 1;
        }
    }

    let duration = benchmark_start.elapsed();
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
    lanes: &mut [ShmLane<SIZE>],
) -> PingPongRunResult {
    let deadline = infra::spin_deadline();
    let benchmark_start = Instant::now();
    let lane_count = lanes.len();
    let mut messages_processed = 0u64;
    let mut recorder = LatencyRecorder::default_range();

    while messages_processed < args.num_messages {
        if SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
            break;
        }

        let round = std::cmp::min(
            (args.num_messages - messages_processed) as usize,
            lane_count,
        );
        let mut pending = Vec::with_capacity(round);

        for offset in 0..round {
            let lane_idx = (messages_processed as usize + offset) % lane_count;
            let seq = args.warmup + messages_processed + offset as u64;
            let send_start = Instant::now();
            publish_or_managed!(lanes[lane_idx].producer_ping, |slot| write_event(
                slot,
                seq,
                common::nanos_now(),
                0
            ));
            lanes[lane_idx]
                .coordination
                .data()
                .events_sent
                .fetch_add(1, Ordering::Release);
            pending.push((lane_idx, seq, send_start));
        }

        for (lane_idx, expected, send_start) in pending {
            let response_sequence = wait_for_next_event(
                &mut lanes[lane_idx].pong_consumer,
                &args.wait_strategy,
                deadline,
                "pingpong_shm batch timing receive",
                |response| response.sequence,
            )?;
            assert_eq!(response_sequence, expected);
            recorder.record(send_start.elapsed().as_nanos() as u64);
            lanes[lane_idx]
                .coordination
                .data()
                .events_echoed
                .fetch_add(1, Ordering::Release);
            messages_processed += 1;
        }
    }

    let duration = benchmark_start.elapsed();
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

fn run_fixed_rate_mode<const SIZE: usize>(
    args: &Args,
    lanes: &mut [ShmLane<SIZE>],
) -> PingPongRunResult {
    let target_rate = args
        .target_rate
        .ok_or("--target-rate is required for fixed-rate mode")?;
    let deadline = infra::spin_deadline();
    let interval_nanos = 1_000_000_000u64 / target_rate;
    let interval = Duration::from_nanos(interval_nanos);
    let mut actual = LatencyRecorder::default_range();
    let mut corrected = LatencyRecorder::default_range();
    let lane_count = lanes.len();

    run_warmup(args, lanes, "pingpong_shm fixed-rate warmup receive")?;

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

        let mut pending = Vec::with_capacity(batch_len);
        for offset in 0..batch_len {
            let lane_idx = (sent as usize + offset) % lane_count;
            let seq = args.warmup + sent + offset as u64;
            let intended_send_time_ns = next_send_time.duration_since(base).as_nanos() as u64;
            let timestamp_ns = Instant::now().duration_since(base).as_nanos() as u64;
            publish_or_managed!(lanes[lane_idx].producer_ping, |slot| write_event(
                slot,
                seq,
                timestamp_ns,
                intended_send_time_ns
            ));
            lanes[lane_idx]
                .coordination
                .data()
                .events_sent
                .fetch_add(1, Ordering::Release);
            pending.push((lane_idx, seq));
        }

        for (lane_idx, expected) in pending {
            let (response_sequence, response_timestamp_ns, response_intended_send_time_ns) =
                wait_for_next_event(
                    &mut lanes[lane_idx].pong_consumer,
                    &args.wait_strategy,
                    deadline,
                    "pingpong_shm fixed-rate receive",
                    |response| {
                        (
                            response.sequence,
                            response.timestamp_ns,
                            response.intended_send_time_ns,
                        )
                    },
                )?;
            assert_eq!(response_sequence, expected);

            let now_ns = Instant::now().duration_since(base).as_nanos() as u64;
            actual.record(now_ns.saturating_sub(response_timestamp_ns));
            corrected.record(now_ns.saturating_sub(response_intended_send_time_ns));
            lanes[lane_idx]
                .coordination
                .data()
                .events_echoed
                .fetch_add(1, Ordering::Release);
        }

        sent = batch_end;
        next_send_time += interval * batch_len as u32;
    }

    let duration = benchmark_start.elapsed();
    let messages_processed = sent;
    let throughput = messages_processed as f64 / duration.as_secs_f64();
    let verification_passed = messages_processed == args.num_messages;

    let actual_stats = actual.stats();
    let corrected_stats = corrected.stats();

    if !pingpong::json_mode(args) {
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
    pingpong::validate_args(&args)?;

    if !pingpong::json_mode(&args) {
        pingpong::print_header(PingPongBackend::Shm, &args, buffer_size);
    }

    let timeout = infra::bench_timeout_duration(120);
    let exe = env::current_exe()?;
    let mut lanes = Vec::with_capacity(args.consumers);
    let mut child_guards = Vec::with_capacity(args.consumers);

    for lane_idx in 0..args.consumers {
        let ping_segment = portable_shm_segment_name(&format!("cpshmpg{lane_idx}"));
        let pong_segment = portable_shm_segment_name(&format!("cpshmpo{lane_idx}"));
        let coordination_name = portable_shm_segment_name(&format!("cpshmc{lane_idx}"));

        let coordination = UnifiedCoordination::create(&coordination_name)?;
        let mut producer_ping =
            build_shared_single_producer::<BenchmarkEvent<SIZE>>(&ping_segment, buffer_size)
                .discover_consumer_with_prefix(1, CONSUMER_PREFIX)
                .with_coordination(CoordinationMode::Immediate)
                .build_producer(BenchmarkEvent::default)?;
        maybe_attach_counters_producer(&mut producer_ping, args.enable_counters);
        // RFC-0017.5 wiring. The pingpong binary's `--liveness on`
        // flag forwards `PERF_BENCH_LIVENESS=on` into the env;
        // when set, attach the policy here so the publish loops
        // below can route through `publish_managed`. Required
        // consumer ID matches what the echo side registers as
        // (see `ECHO_CONSUMER_ID = "cp_0"`). Off by default — the
        // unmanaged publish path stays the baseline.
        if liveness_enabled() {
            producer_ping.enable_required_consumer_liveness(liveness_config(&[ECHO_CONSUMER_ID]));
        }
        coordination
            .data()
            .producer_ready
            .store(1, Ordering::Release);

        let child_guard = spawn_echo_process(
            &exe,
            &args,
            &ping_segment,
            &pong_segment,
            &coordination_name,
            buffer_size,
            SIZE,
        )?;

        if !coordination.wait_for_echo_ready(timeout) {
            return Err(format!("timeout waiting for echo process on lane {lane_idx}").into());
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
        maybe_attach_counters_consumer(&mut pong_consumer, args.enable_counters);
        coordination
            .data()
            .consumer_attached
            .store(1, Ordering::Release);
        if !wait_for_echo_attached(&coordination, timeout) {
            return Err(
                format!("timeout waiting for echo service readiness on lane {lane_idx}").into(),
            );
        }

        lanes.push(ShmLane {
            producer_ping,
            pong_consumer,
            coordination,
        });
        child_guards.push(child_guard);
    }

    if args.target_rate.is_none() {
        run_warmup(&args, &mut lanes, "pingpong_shm warmup receive")?;
    }

    let (throughput, duration, latency, verification_passed, messages_processed) =
        if args.target_rate.is_some() {
            run_fixed_rate_mode(&args, &mut lanes)?
        } else if args.batch_timing {
            run_batch_timing_mode(&args, &mut lanes)?
        } else {
            run_throughput_mode(&args, &mut lanes)?
        };

    for lane in &lanes {
        lane.coordination.signal_shutdown();
    }
    for mut child_guard in child_guards {
        let child_status = child_guard.wait()?;
        if !child_status.success() {
            return Err(format!("echo child exited with status {child_status}").into());
        }
    }

    let report = build_report(
        &args,
        throughput,
        buffer_size,
        latency.clone(),
        verification_passed,
        messages_processed,
    );

    if pingpong::should_emit_report(&args) {
        let output_args = pingpong::report_output_args(&args);
        let canonical_json_out = pingpong::benchmark_json_output_path();
        reporting::emit_report_with_extra_json(
            &report,
            &output_args,
            None,
            None,
            None,
            canonical_json_out.as_deref(),
        );
    } else {
        print_results(
            &args,
            throughput,
            duration,
            latency.as_ref(),
            verification_passed,
            messages_processed,
        );
    }

    Ok(())
}

fn run_process_one(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let buffer_size = args
        .buffer_size
        .unwrap_or_else(|| pingpong::default_buffer_size(args.message_size));

    pingpong::run_with_large_stack_if_needed(
        args.message_size,
        "pingpong-shm-process-one",
        move || {
            match args.message_size {
            32 => run_benchmark::<32>(args, buffer_size),
            64 => run_benchmark::<64>(args, buffer_size),
            128 => run_benchmark::<128>(args, buffer_size),
            512 => run_benchmark::<512>(args, buffer_size),
            1024 => run_benchmark::<1024>(args, buffer_size),
            2048 => run_benchmark::<2048>(args, buffer_size),
            4096 => run_benchmark::<4096>(args, buffer_size),
            16384 => run_benchmark::<16384>(args, buffer_size),
            32768 => run_benchmark::<32768>(args, buffer_size),
            65536 => run_benchmark::<65536>(args, buffer_size),
            131072 => run_benchmark::<131072>(args, buffer_size),
            524288 => run_benchmark::<524288>(args, buffer_size),
            1048576 => run_benchmark::<1048576>(args, buffer_size),
            2097152 => run_benchmark::<2097152>(args, buffer_size),
            8388608 => run_benchmark::<8388608>(args, buffer_size),
            16777216 => run_benchmark::<16777216>(args, buffer_size),
            33554432 => run_benchmark::<33554432>(args, buffer_size),
            67108864 => run_benchmark::<67108864>(args, buffer_size),
            _ => Err(format!(
                "unsupported message size: {} (expected 32, 64, 128, 512, 1024, 2048, 4096, 16384, 32768, 65536, 131072, 524288, 1048576, 2097152, 8388608, 16777216, 33554432, 67108864)",
                args.message_size
            )
            .into()),
        }
        },
    )
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

    pingpong::run_with_large_stack_if_needed(message_size, "pingpong-shm-process-two", move || {
        match message_size {
            32 => echo_server::<32>(
                &ping_segment,
                &pong_segment,
                &coordination,
                buffer_size,
                &wait_strategy,
            ),
            64 => echo_server::<64>(
                &ping_segment,
                &pong_segment,
                &coordination,
                buffer_size,
                &wait_strategy,
            ),
            128 => echo_server::<128>(
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
            2048 => echo_server::<2048>(
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
            32768 => echo_server::<32768>(
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
            524288 => echo_server::<524288>(
                &ping_segment,
                &pong_segment,
                &coordination,
                buffer_size,
                &wait_strategy,
            ),
            1048576 => echo_server::<1048576>(
                &ping_segment,
                &pong_segment,
                &coordination,
                buffer_size,
                &wait_strategy,
            ),
            2097152 => echo_server::<2097152>(
                &ping_segment,
                &pong_segment,
                &coordination,
                buffer_size,
                &wait_strategy,
            ),
            8388608 => echo_server::<8388608>(
                &ping_segment,
                &pong_segment,
                &coordination,
                buffer_size,
                &wait_strategy,
            ),
            16777216 => echo_server::<16777216>(
                &ping_segment,
                &pong_segment,
                &coordination,
                buffer_size,
                &wait_strategy,
            ),
            33554432 => echo_server::<33554432>(
                &ping_segment,
                &pong_segment,
                &coordination,
                buffer_size,
                &wait_strategy,
            ),
            67108864 => echo_server::<67108864>(
                &ping_segment,
                &pong_segment,
                &coordination,
                buffer_size,
                &wait_strategy,
            ),
            _ => Err(format!("unsupported child message size: {message_size}").into()),
        }
    })
}

fn echo_server<const SIZE: usize>(
    ping_segment: &str,
    pong_segment: &str,
    coordination: &UnifiedCoordination,
    buffer_size: usize,
    wait_strategy: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let enable_counters = child_counters_enabled();
    let mut ping_consumer = attach_consumer_with_timeout::<BenchmarkEvent<SIZE>>(
        ping_segment,
        buffer_size,
        ECHO_CONSUMER_ID,
        Duration::from_secs(30),
    )?;
    maybe_attach_counters_consumer(&mut ping_consumer, enable_counters);
    let mut pong_producer =
        build_shared_single_producer::<BenchmarkEvent<SIZE>>(pong_segment, buffer_size)
            .discover_consumer_with_prefix(1, CONSUMER_PREFIX)
            .with_coordination(CoordinationMode::Immediate)
            .build_producer(BenchmarkEvent::default)?;
    maybe_attach_counters_producer(&mut pong_producer, enable_counters);
    // The echo side's pong_producer publishes back to the parent's
    // main consumer (`MAIN_CONSUMER_ID = "cp_0"`). Liveness is
    // wired symmetrically: when the env flag is on (inherited from
    // parent), the policy is attached here and the publish below
    // routes through `publish_managed`.
    if liveness_enabled() {
        pong_producer.enable_required_consumer_liveness(liveness_config(&[MAIN_CONSUMER_ID]));
    }

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

    let deadline = infra::spin_deadline_or(120);
    while !coordination.is_shutdown() {
        match ping_consumer.try_consume_next_leased() {
            Some(event) => {
                publish_or_managed!(pong_producer, |slot| *slot = *event);
            }
            None => {
                if SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
                    break;
                }
                infra::check_deadline(deadline, "pingpong_shm echo loop");
                pingpong::apply_wait_strategy(wait_strategy);
            }
        }
    }

    Ok(())
}

pub fn run_main() -> Result<(), Box<dyn std::error::Error>> {
    ctrlc::set_handler(move || {
        SHUTDOWN_REQUESTED.store(true, Ordering::Release);
    })?;

    let filtered_args: Vec<String> = env::args().filter(|arg| arg != "--bench").collect();
    let filtered_args = infra::apply_timeout_arg(&filtered_args)
        .map_err(|error| format!("pingpong_shm failed: {error}"))?;
    let args = Args::parse_from(filtered_args);

    if args.process_two {
        run_process_two()
    } else {
        run_process_one(args)
    }
}

/// Entry point accepting pre-built CLI args (used by the consolidated binary).
pub fn run_main_with_args(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    ctrlc::set_handler(move || {
        SHUTDOWN_REQUESTED.store(true, Ordering::Release);
    })
    .ok(); // ignore if already set

    let filtered_args =
        infra::apply_timeout_arg(&args).map_err(|error| format!("pingpong_shm failed: {error}"))?;
    let parsed = Args::parse_from(filtered_args);

    if parsed.process_two {
        run_process_two()
    } else {
        run_process_one(parsed)
    }
}
