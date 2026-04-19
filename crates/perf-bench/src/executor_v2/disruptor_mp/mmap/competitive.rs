//! Competitive ping-pong benchmark over the mmap backend.
//!
//! This replaces the previous stub with a real 1p1c ping-pong benchmark that
//! supports maximum-throughput, coordinated-omission-aware fixed-rate, and
//! low-overhead batch-timing modes.

#[allow(dead_code)]
#[path = "../../../../../disruptor-mp/benches/ipc/competitive/common.rs"]
mod common;

use clap::Parser;
use common::{calculate_data_rate_gbps, format_throughput, BenchmarkEvent};
use disruptor_mp::{portable_shm_segment_name, MmapConsumer, MmapProducer, MmapTransportLayout};
use crate::coordination::UnifiedCoordination;
use crate::harness;
use crate::latency::{self, LatencyRecorder};
use crate::reporting::{self, BenchReport};
use crate::scenario_v2::competitive::{self, CompetitiveArgs as Args, CompetitiveBackend};
use std::env;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

struct RootCleanupGuard {
    root: PathBuf,
}

impl RootCleanupGuard {
    fn new(root: PathBuf) -> Self {
        Self { root }
    }
}

impl Drop for RootCleanupGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
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

struct MmapLane<const SIZE: usize> {
    producer_ping: MmapProducer<BenchmarkEvent<SIZE>>,
    pong_consumer: MmapConsumer<BenchmarkEvent<SIZE>>,
    coordination: UnifiedCoordination,
}

fn wait_for_next_event<E: Copy + Default>(
    consumer: &mut MmapConsumer<E>,
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
        competitive::apply_wait_strategy(wait_strategy);
    }
}

fn attach_consumer_with_timeout<E: Copy + Default>(
    layout: MmapTransportLayout,
    buffer_size: usize,
    consumer_id: &str,
    timeout: Duration,
) -> Result<MmapConsumer<E>, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        match MmapConsumer::<E>::attach(layout.clone(), buffer_size, consumer_id) {
            Ok(consumer) => return Ok(consumer),
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(error) => return Err(format!("attach failed for {consumer_id}: {error}").into()),
        }
    }
}

fn build_report(
    args: &Args,
    throughput: f64,
    buffer_size: usize,
    latency: Option<latency::LatencyStats>,
    verification_passed: bool,
    messages_processed: u64,
) -> BenchReport {
    competitive::build_report(
        CompetitiveBackend::Mmap,
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
    root: &Path,
    ping_segment: &str,
    pong_segment: &str,
    coordination_name: &str,
    buffer_size: usize,
    message_size: usize,
) -> Result<ChildProcessGuard, Box<dyn std::error::Error>> {
    let mut child_cmd = Command::new(exe);
    child_cmd
        .arg("--process-two")
        .env("MMAP_ROOT", root)
        .env("PING_SEGMENT", ping_segment)
        .env("PONG_SEGMENT", pong_segment)
        .env("COORDINATION_SEGMENT", coordination_name)
        .env("MESSAGE_SIZE", message_size.to_string())
        .env("BUFFER_SIZE", buffer_size.to_string())
        .env("WAIT_STRATEGY", &args.wait_strategy)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());

    if competitive::json_mode(args) {
        child_cmd.env("JSON_MODE", "1");
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
        if let Some(avg_rtt_ns) = competitive::average_rtt_ns(duration, messages_processed) {
            println!(
                "\nAverage RTT: {:.0} ns (batch timing mode; no histogram)",
                avg_rtt_ns
            );
        } else {
            println!("\nLatency: batch timing mode (no samples recorded)");
        }
    }

    if !args.no_compare {
        println!("\nComparison: self-comparison only for mmap backend");
    }
}

fn run_warmup<const SIZE: usize>(
    args: &Args,
    lanes: &mut [MmapLane<SIZE>],
    context: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = harness::spin_deadline();
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
            let mut event = BenchmarkEvent::<SIZE>::new(seq);
            event.set_timestamp();
            lanes[lane_idx].producer_ping.publish(|slot| *slot = event);
            lanes[lane_idx]
                .coordination
                .data()
                .warmup_sent
                .fetch_add(1, Ordering::Release);
            pending.push((lane_idx, seq));
        }

        for (lane_idx, expected) in pending {
            let (_sequence, response) = wait_for_next_event(
                &mut lanes[lane_idx].pong_consumer,
                &args.wait_strategy,
                deadline,
                context,
            )?;
            assert_eq!(response.sequence, expected);
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
    lanes: &mut [MmapLane<SIZE>],
) -> Result<(f64, Duration, Option<latency::LatencyStats>, bool, u64), Box<dyn std::error::Error>> {
    let deadline = harness::spin_deadline();
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
            let mut event = BenchmarkEvent::<SIZE>::new(seq);
            event.set_timestamp();
            lanes[lane_idx].producer_ping.publish(|slot| *slot = event);
            lanes[lane_idx]
                .coordination
                .data()
                .events_sent
                .fetch_add(1, Ordering::Release);
            pending.push((lane_idx, seq, send_start));
        }

        for (lane_idx, expected, send_start) in pending {
            let (_sequence, response) = wait_for_next_event(
                &mut lanes[lane_idx].pong_consumer,
                &args.wait_strategy,
                deadline,
                "competitive_mmap throughput receive",
            )?;
            assert_eq!(response.sequence, expected);
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
    lanes: &mut [MmapLane<SIZE>],
) -> Result<(f64, Duration, Option<latency::LatencyStats>, bool, u64), Box<dyn std::error::Error>> {
    let deadline = harness::spin_deadline();
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
            let mut event = BenchmarkEvent::<SIZE>::new(seq);
            event.set_timestamp();
            lanes[lane_idx].producer_ping.publish(|slot| *slot = event);
            lanes[lane_idx]
                .coordination
                .data()
                .events_sent
                .fetch_add(1, Ordering::Release);
            pending.push((lane_idx, seq, send_start));
        }

        for (lane_idx, expected, send_start) in pending {
            let (_sequence, response) = wait_for_next_event(
                &mut lanes[lane_idx].pong_consumer,
                &args.wait_strategy,
                deadline,
                "competitive_mmap batch timing receive",
            )?;
            assert_eq!(response.sequence, expected);
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
    lanes: &mut [MmapLane<SIZE>],
) -> Result<(f64, Duration, Option<latency::LatencyStats>, bool, u64), Box<dyn std::error::Error>> {
    let target_rate = args
        .target_rate
        .ok_or("--target-rate is required for fixed-rate mode")?;
    let deadline = harness::spin_deadline();
    let interval_nanos = 1_000_000_000u64 / target_rate;
    let interval = Duration::from_nanos(interval_nanos);
    let mut actual = LatencyRecorder::default_range();
    let mut corrected = LatencyRecorder::default_range();
    let lane_count = lanes.len();

    run_warmup(args, lanes, "competitive_mmap fixed-rate warmup receive")?;

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
            let mut event = BenchmarkEvent::<SIZE>::new(seq);
            event.intended_send_time_ns = next_send_time.duration_since(base).as_nanos() as u64;
            event.timestamp_ns = Instant::now().duration_since(base).as_nanos() as u64;
            lanes[lane_idx].producer_ping.publish(|slot| *slot = event);
            lanes[lane_idx]
                .coordination
                .data()
                .events_sent
                .fetch_add(1, Ordering::Release);
            pending.push((lane_idx, seq));
        }

        for (lane_idx, expected) in pending {
            let (_sequence, response) = wait_for_next_event(
                &mut lanes[lane_idx].pong_consumer,
                &args.wait_strategy,
                deadline,
                "competitive_mmap fixed-rate receive",
            )?;
            assert_eq!(response.sequence, expected);

            let now_ns = Instant::now().duration_since(base).as_nanos() as u64;
            actual.record(now_ns.saturating_sub(response.timestamp_ns));
            corrected.record(now_ns.saturating_sub(response.intended_send_time_ns));
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

    if !competitive::json_mode(args) {
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
    competitive::validate_args(&args)?;

    if !competitive::json_mode(&args) {
        competitive::print_header(CompetitiveBackend::Mmap, &args, buffer_size);
    }

    let timeout = harness::bench_timeout_duration(120);
    let root = harness::unique_mmap_root("competitive_mmap");
    std::fs::create_dir_all(&root)?;
    let _cleanup = RootCleanupGuard::new(root.clone());

    let exe = env::current_exe()?;
    let mut lanes = Vec::with_capacity(args.consumers);
    let mut child_guards = Vec::with_capacity(args.consumers);

    for lane_idx in 0..args.consumers {
        let ping_segment = harness::unique_mmap_segment(&format!("cmp_ping_{lane_idx}"));
        let pong_segment = harness::unique_mmap_segment(&format!("cmp_pong_{lane_idx}"));
        let coordination_name = portable_shm_segment_name(&format!("cmpm{lane_idx}"));

        let coordination = UnifiedCoordination::create(&coordination_name)?;

        let ping_layout = MmapTransportLayout::new(root.clone(), ping_segment.clone())?;
        ping_layout.ensure_directories()?;
        let pong_layout = MmapTransportLayout::new(root.clone(), pong_segment.clone())?;
        pong_layout.ensure_directories()?;

        let producer_ping = MmapProducer::<BenchmarkEvent<SIZE>>::create(
            ping_layout,
            buffer_size,
            BenchmarkEvent::default,
        )?;
        coordination
            .data()
            .producer_ready
            .store(1, Ordering::Release);

        let child_guard = spawn_echo_process(
            &exe,
            &args,
            &root,
            &ping_segment,
            &pong_segment,
            &coordination_name,
            buffer_size,
            SIZE,
        )?;

        if !coordination.wait_for_echo_ready(timeout) {
            return Err(format!("timeout waiting for echo process on lane {lane_idx}").into());
        }
        if !producer_ping.wait_for_consumers_ready(1, timeout) {
            return Err(
                format!("timeout waiting for ping consumer readiness on lane {lane_idx}").into(),
            );
        }

        let pong_consumer = attach_consumer_with_timeout::<BenchmarkEvent<SIZE>>(
            pong_layout,
            buffer_size,
            "main",
            timeout,
        )?;
        coordination
            .data()
            .consumer_attached
            .store(1, Ordering::Release);

        lanes.push(MmapLane {
            producer_ping,
            pong_consumer,
            coordination,
        });
        child_guards.push(child_guard);
    }

    if args.target_rate.is_none() {
        run_warmup(&args, &mut lanes, "competitive_mmap warmup receive")?;
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

    if competitive::should_emit_report(&args) {
        let output_args = competitive::report_output_args(&args);
        let canonical_json_out = competitive::benchmark_json_output_path();
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
        .unwrap_or_else(|| competitive::default_buffer_size(args.message_size));

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
    let root = PathBuf::from(env::var("MMAP_ROOT")?);
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
            root,
            &ping_segment,
            &pong_segment,
            &coordination,
            buffer_size,
            &wait_strategy,
        ),
        512 => echo_server::<512>(
            root,
            &ping_segment,
            &pong_segment,
            &coordination,
            buffer_size,
            &wait_strategy,
        ),
        1024 => echo_server::<1024>(
            root,
            &ping_segment,
            &pong_segment,
            &coordination,
            buffer_size,
            &wait_strategy,
        ),
        4096 => echo_server::<4096>(
            root,
            &ping_segment,
            &pong_segment,
            &coordination,
            buffer_size,
            &wait_strategy,
        ),
        16384 => echo_server::<16384>(
            root,
            &ping_segment,
            &pong_segment,
            &coordination,
            buffer_size,
            &wait_strategy,
        ),
        65536 => echo_server::<65536>(
            root,
            &ping_segment,
            &pong_segment,
            &coordination,
            buffer_size,
            &wait_strategy,
        ),
        131072 => echo_server::<131072>(
            root,
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
    root: PathBuf,
    ping_segment: &str,
    pong_segment: &str,
    coordination: &UnifiedCoordination,
    buffer_size: usize,
    wait_strategy: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let ping_layout = MmapTransportLayout::new(root.clone(), ping_segment.to_string())?;
    let pong_layout = MmapTransportLayout::new(root, pong_segment.to_string())?;
    pong_layout.ensure_directories()?;

    let mut ping_consumer = attach_consumer_with_timeout::<BenchmarkEvent<SIZE>>(
        ping_layout,
        buffer_size,
        "echo",
        Duration::from_secs(30),
    )?;
    let mut pong_producer = MmapProducer::<BenchmarkEvent<SIZE>>::create(
        pong_layout,
        buffer_size,
        BenchmarkEvent::default,
    )?;

    coordination.data().echo_ready.store(1, Ordering::Release);

    if !coordination.wait_for_consumer_attached(Duration::from_secs(30)) {
        return Err("timeout waiting for pong consumer attachment".into());
    }
    if !pong_producer.wait_for_consumers_ready(1, Duration::from_secs(30)) {
        return Err("timeout waiting for pong consumer readiness".into());
    }

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
                harness::check_deadline(deadline, "competitive_mmap echo loop");
                competitive::apply_wait_strategy(wait_strategy);
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
    let filtered_args = harness::apply_timeout_arg(&filtered_args)
        .map_err(|error| format!("competitive_mmap failed: {error}"))?;
    let args = Args::parse_from(filtered_args);

    if args.process_two {
        run_process_two()
    } else {
        run_process_one(args)
    }
}
