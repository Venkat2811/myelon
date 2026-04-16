//! Raw disruptor-mp ring benchmark over SHM backend.
//!
//! Two benchmark classes:
//!   --class message  : 144B event, full 128B payload fill + timestamp (matches original ipc_shm)
//!   --class signal   : 64B event, 16B data only (head-to-head vs Alvarez Rosa V5 305M ops/s)
//!   (default)        : runs both
//!
//! Run:   cargo bench -p myelon-bench --bench raw_ring_shm
//! Signal only: cargo bench -p myelon-bench --bench raw_ring_shm -- --class signal

use disruptor_mp::{
    build_shared_single_producer, CoordinationMode, SharedDisruptorBuilder, SharedMemoryConfig,
};
use myelon_bench::coordination::BenchmarkCoordination;
use myelon_bench::events::{format_throughput, nanos_now};
use myelon_bench::latency::LatencyRecorder;
use myelon_bench::reporting::{self, BenchReport, BenchResult};
use std::env;
use std::io::Read as _;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

// ============================================================
// Event types
// ============================================================

/// Message-class event: matches original ipc_shm.rs BenchmarkEvent.
/// 144 bytes: 8 (id) + 8 (timestamp) + 128 (payload).
#[repr(C)]
#[derive(Clone, Copy)]
struct MessageEvent {
    id: u64,
    timestamp: u64,
    payload: [u8; 128],
}

impl Default for MessageEvent {
    fn default() -> Self {
        Self { id: 0, timestamp: 0, payload: [0u8; 128] }
    }
}

/// Signal-class event: cache-line aligned, 16 bytes of data.
/// Head-to-head with Alvarez Rosa V5 (305M) and Intel (452M) articles.
/// 64 bytes total (16 data + 48 padding from alignment).
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct SignalEvent {
    sequence: u64,
    data: u64,
}

impl Default for SignalEvent {
    fn default() -> Self {
        Self { sequence: 0, data: 0 }
    }
}

// ============================================================
// Shared helpers
// ============================================================

fn get_segment_name() -> String {
    if let Ok(name) = env::var("BENCHMARK_SEGMENT_NAME") { return name; }
    disruptor_mp::portable_shm_segment_name("mbshm")
}

fn spawn_child(exe: &std::path::Path, role: &str, segment: &str) -> Child {
    spawn_child_with_env(exe, role, segment, &[])
}

fn spawn_child_with_env(
    exe: &std::path::Path,
    role: &str,
    segment: &str,
    extra_env: &[(&str, String)],
) -> Child {
    let mut cmd = Command::new(exe);
    cmd.arg(role)
        .env("BENCHMARK_SEGMENT_NAME", segment)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (key, val) in extra_env {
        cmd.env(key, val);
    }
    cmd.spawn()
        .unwrap_or_else(|e| panic!("spawn {role}: {e}"))
}

fn read_env_usize(key: &str, default: usize) -> usize {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn read_env_u64(key: &str, default: u64) -> u64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn wait_with_output_timeout(mut child: Child, timeout: Duration) -> Result<Output, String> {
    fn collect(child: &mut Child, status: std::process::ExitStatus) -> Output {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        if let Some(mut o) = child.stdout.take() { let _ = o.read_to_end(&mut stdout); }
        if let Some(mut e) = child.stderr.take() { let _ = e.read_to_end(&mut stderr); }
        Output { status, stdout, stderr }
    }
    let start = Instant::now();
    loop {
        if let Some(s) = child.try_wait().map_err(|e| e.to_string())? { return Ok(collect(&mut child, s)); }
        if start.elapsed() >= timeout {
            let _ = child.kill();
            let s = child.wait().map_err(|e| e.to_string())?;
            return Err(format!("timeout; stderr: {}", String::from_utf8_lossy(&collect(&mut child, s).stderr)));
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn extract_value(output: &str, key: &str) -> f64 {
    for line in output.lines() {
        if let Some(rest) = line.strip_prefix(&format!("{key}: ")) {
            if let Some(n) = rest.split_whitespace().next() { return n.parse().unwrap_or(0.0); }
        }
    }
    0.0
}

// ============================================================
// Message-class: producer (matches original ipc_shm exactly)
// ============================================================

fn message_producer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = get_segment_name();
    const BUFFER: usize = 1024;
    const EVENTS: u64 = 100_000;
    const WARMUP: u64 = 1_000;

    // Use discovery + external coordination for proper backpressure.
    // Pattern from disruptor-mp/tests/true_multiprocess.rs:
    //   discover_consumer_with_prefix_and_interval → wait_for_consumers → warmup scans → publish
    let mut producer = build_shared_single_producer::<MessageEvent>(&segment, BUFFER)
        .enable_discovery(1)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(MessageEvent::default)?;

    let coord = BenchmarkCoordination::create(&segment)?;

    if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
        return Err("timeout waiting for consumer".into());
    }

    // Warmup discovery scans so the barrier finds the consumer cursor
    // before the first timed publish (avoids initial overwrite window).
    for _ in 0..20 {
        let _ = producer.min_gating_sequence();
        std::thread::sleep(Duration::from_millis(2));
    }

    // Warmup — full payload fill, matching original
    for i in 0..WARMUP {
        producer.publish(|event| {
            event.id = i;
            event.timestamp = 0;
            event.payload = [(i % 256) as u8; 128];
        });
    }

    // Measured — full 128B payload fill + absolute timestamp on every event
    let start = Instant::now();
    for i in 0..EVENTS {
        producer.publish(|event| {
            event.id = WARMUP + i;
            event.timestamp = nanos_now(); // absolute timestamp for cross-process latency
            event.payload = [((WARMUP + i) % 256) as u8; 128];
        });
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} events/sec", EVENTS as f64 / elapsed.as_secs_f64());
    println!("Time: {:.3} seconds", elapsed.as_secs_f64());

    coord.signal_producer_done(EVENTS as i64);
    coord.wait_for_consumers_done(1, Duration::from_secs(30));
    Ok(())
}

fn message_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = get_segment_name();
    const BUFFER: usize = 1024;
    const WARMUP: u64 = 1_000;

    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;

    let config = SharedMemoryConfig {
        name: segment.clone(),
        buffer_size: BUFFER,
        element_size: std::mem::size_of::<MessageEvent>(),
        create: false,
    };
    let mut consumer = SharedDisruptorBuilder::<MessageEvent>::new(config).build_consumer()?;
    coord.signal_consumer_ready();

    // Warmup
    let mut warmup = 0u64;
    while warmup < WARMUP {
        consumer.process_available(|_e, _s| { warmup += 1; });
        if warmup < WARMUP { std::hint::spin_loop(); }
    }

    // Measured — with real per-event HDR latency
    let mut latency = LatencyRecorder::default_range();
    let start = Instant::now();
    let mut consumed = 0u64;
    loop {
        consumer.process_available(|event, _seq| {
            consumed += 1;
            if event.timestamp > 0 {
                latency.record_delta(event.timestamp, nanos_now());
            }
        });
        if coord.is_producer_done() && consumed >= coord.events_produced() as u64 { break; }
        if consumed < coord.events_produced() as u64 { std::hint::spin_loop(); }
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} events/sec", consumed as f64 / elapsed.as_secs_f64());
    println!("Events: {}", consumed);
    if let Some(stats) = latency.stats() {
        println!("Latency: {}", stats.summary());
        println!("LatencyJSON: {}", serde_json::to_string(&stats).unwrap_or_default());
    }
    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

// ============================================================
// Signal-class: producer (head-to-head vs Alvarez Rosa V5)
// ============================================================

fn signal_producer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = get_segment_name();
    const BUFFER: usize = 65_536; // 64K slots — large ring to avoid wraps, matching articles
    const EVENTS: u64 = 10_000_000; // 10M events — run for seconds, not milliseconds
    const WARMUP: u64 = 100_000;

    let mut producer = build_shared_single_producer::<SignalEvent>(&segment, BUFFER)
        .enable_discovery(1)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(SignalEvent::default)?;

    let coord = BenchmarkCoordination::create(&segment)?;

    if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
        return Err("timeout waiting for consumer".into());
    }

    // Warmup discovery scans
    for _ in 0..20 {
        let _ = producer.min_gating_sequence();
        std::thread::sleep(Duration::from_millis(2));
    }

    // Warmup events
    for i in 0..WARMUP {
        producer.publish(|slot| { slot.sequence = i; slot.data = i.wrapping_mul(0x9E3779B97F4A7C15); });
    }

    // Measured — minimal work: write 16 bytes per event
    let start = Instant::now();
    for i in 0..EVENTS {
        producer.publish(|slot| {
            slot.sequence = WARMUP + i;
            slot.data = (WARMUP + i).wrapping_mul(0x9E3779B97F4A7C15);
        });
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} events/sec", EVENTS as f64 / elapsed.as_secs_f64());
    println!("Time: {:.3} seconds", elapsed.as_secs_f64());

    coord.signal_producer_done(EVENTS as i64);
    coord.wait_for_consumers_done(1, Duration::from_secs(30));
    Ok(())
}

fn signal_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = get_segment_name();
    const BUFFER: usize = 65_536;
    const WARMUP: u64 = 100_000;

    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;

    let config = SharedMemoryConfig {
        name: segment.clone(),
        buffer_size: BUFFER,
        element_size: std::mem::size_of::<SignalEvent>(),
        create: false,
    };
    let mut consumer = SharedDisruptorBuilder::<SignalEvent>::new(config).build_consumer()?;
    coord.signal_consumer_ready();

    // Warmup
    let mut warmup = 0u64;
    while warmup < WARMUP {
        consumer.process_available(|_e, _s| { warmup += 1; });
        if warmup < WARMUP { std::hint::spin_loop(); }
    }

    // Measured
    let start = Instant::now();
    let mut consumed = 0u64;
    loop {
        consumer.process_available(|_e, _s| { consumed += 1; });
        if coord.is_producer_done() && consumed >= coord.events_produced() as u64 { break; }
        if consumed < coord.events_produced() as u64 { std::hint::spin_loop(); }
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} events/sec", consumed as f64 / elapsed.as_secs_f64());
    println!("Events: {}", consumed);
    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

// ============================================================
// Multi-consumer message class (1p3c, 1p8c, 1p12c)
// ============================================================

fn multi_message_producer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = get_segment_name();
    let num_consumers = read_env_usize("BENCH_NUM_CONSUMERS", 3);
    let buffer = read_env_usize("BENCH_BUFFER", 4096);
    let events = read_env_u64("BENCH_EVENTS", 100_000);
    let warmup = read_env_u64("BENCH_WARMUP", 1_000);

    let mut producer = build_shared_single_producer::<MessageEvent>(&segment, buffer)
        .enable_discovery(num_consumers)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(MessageEvent::default)?;

    let coord = BenchmarkCoordination::create(&segment)?;

    if !coord.wait_for_consumers(num_consumers, Duration::from_secs(60)) {
        return Err(format!("timeout waiting for {num_consumers} consumers").into());
    }

    // Warmup discovery scans — more iterations for more consumers
    let scan_rounds = 20 + num_consumers * 5;
    for _ in 0..scan_rounds {
        let _ = producer.min_gating_sequence();
        std::thread::sleep(Duration::from_millis(2));
    }

    for i in 0..warmup {
        producer.publish(|event| {
            event.id = i;
            event.timestamp = 0;
            event.payload = [(i % 256) as u8; 128];
        });
    }

    let start = Instant::now();
    for i in 0..events {
        producer.publish(|event| {
            event.id = warmup + i;
            event.timestamp = nanos_now();
            event.payload = [((warmup + i) % 256) as u8; 128];
        });
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} events/sec", events as f64 / elapsed.as_secs_f64());
    println!("Time: {:.3} seconds", elapsed.as_secs_f64());

    coord.signal_producer_done(events as i64);
    coord.wait_for_consumers_done(num_consumers, Duration::from_secs(90));
    Ok(())
}

fn multi_message_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = get_segment_name();
    let buffer = read_env_usize("BENCH_BUFFER", 4096);
    let warmup = read_env_u64("BENCH_WARMUP", 1_000);
    let record_latency = read_env_usize("BENCH_RECORD_LATENCY", 0) == 1;

    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;

    let config = SharedMemoryConfig {
        name: segment.clone(),
        buffer_size: buffer,
        element_size: std::mem::size_of::<MessageEvent>(),
        create: false,
    };
    let mut consumer = SharedDisruptorBuilder::<MessageEvent>::new(config).build_consumer()?;
    coord.signal_consumer_ready();

    // Warmup
    let mut warmup_count = 0u64;
    while warmup_count < warmup {
        consumer.process_available(|_e, _s| { warmup_count += 1; });
        if warmup_count < warmup { std::hint::spin_loop(); }
    }

    // Measured
    let mut latency = if record_latency {
        Some(LatencyRecorder::default_range())
    } else {
        None
    };
    let start = Instant::now();
    let mut consumed = 0u64;
    loop {
        consumer.process_available(|event, _seq| {
            consumed += 1;
            if let Some(ref mut lat) = latency {
                if event.timestamp > 0 {
                    lat.record_delta(event.timestamp, nanos_now());
                }
            }
        });
        if coord.is_producer_done() && consumed >= coord.events_produced() as u64 { break; }
        if consumed < coord.events_produced() as u64 { std::hint::spin_loop(); }
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} events/sec", consumed as f64 / elapsed.as_secs_f64());
    println!("Events: {}", consumed);
    if let Some(lat) = latency {
        if let Some(stats) = lat.stats() {
            println!("Latency: {}", stats.summary());
            println!("LatencyJSON: {}", serde_json::to_string(&stats).unwrap_or_default());
        }
    }
    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

// ============================================================
// Orchestrator
// ============================================================

fn extract_latency_json(output: &str) -> Option<myelon_bench::latency::LatencyStats> {
    for line in output.lines() {
        if let Some(json) = line.strip_prefix("LatencyJSON: ") {
            return serde_json::from_str(json).ok();
        }
    }
    None
}

fn run_class(class: &str, prod_role: &str, cons_role: &str, event_bytes: usize, events: u64, buffer: usize, warmup: u64) -> BenchResult {
    let segment = get_segment_name();
    let exe = env::current_exe().expect("current_exe");

    let producer = spawn_child(&exe, prod_role, &segment);
    let consumer = spawn_child(&exe, cons_role, &segment);

    let timeout = Duration::from_secs(120);
    let prod_result = wait_with_output_timeout(producer, timeout);
    let cons_result = wait_with_output_timeout(consumer, timeout);

    let prod_out = match prod_result {
        Ok(o) => { if !o.stderr.is_empty() { eprintln!("[{class} prod stderr] {}", String::from_utf8_lossy(&o.stderr)); } String::from_utf8_lossy(&o.stdout).to_string() }
        Err(e) => { eprintln!("[{class} prod] {e}"); String::new() }
    };
    let cons_out = match cons_result {
        Ok(o) => { if !o.stderr.is_empty() { eprintln!("[{class} cons stderr] {}", String::from_utf8_lossy(&o.stderr)); } String::from_utf8_lossy(&o.stdout).to_string() }
        Err(e) => { eprintln!("[{class} cons] {e}"); String::new() }
    };

    let prod_tp = extract_value(&prod_out, "Throughput");
    let cons_tp = extract_value(&cons_out, "Throughput");
    let latency = extract_latency_json(&cons_out);

    let lat_str = latency.as_ref()
        .map(|l| l.summary())
        .unwrap_or_else(|| "-".to_string());

    println!(
        "  {:<25} producer: {:>10} ops/s  consumer: {:>10} ops/s  {}",
        class, format_throughput(prod_tp), format_throughput(cons_tp), lat_str,
    );

    reporting::make_result(
        "raw_ring_shm", class, "shm", "raw_ring",
        None, "BusySpin", event_bytes, buffer, events, warmup, 1,
        prod_tp, cons_tp, latency,
    )
}

fn run_multi_message(num_consumers: usize, buffer: usize, events: u64, warmup: u64) -> BenchResult {
    let segment = get_segment_name();
    let exe = env::current_exe().expect("current_exe");
    let class = format!("message_1p{}c_144B", num_consumers);
    // Record latency for <=3 consumers (meaningful), skip for 8+ (noisy)
    let record_latency = num_consumers <= 3;

    let env_common: Vec<(&str, String)> = vec![
        ("BENCH_NUM_CONSUMERS", num_consumers.to_string()),
        ("BENCH_BUFFER", buffer.to_string()),
        ("BENCH_EVENTS", events.to_string()),
        ("BENCH_WARMUP", warmup.to_string()),
    ];

    let producer = spawn_child_with_env(&exe, "msg_multi_producer", &segment, &env_common);

    let consumers: Vec<Child> = (0..num_consumers)
        .map(|i| {
            let mut env = env_common.clone();
            if record_latency {
                env.push(("BENCH_RECORD_LATENCY", "1".to_string()));
            }
            let _ = i; // consumer_id not needed — each gets a unique PID for discovery
            spawn_child_with_env(&exe, "msg_multi_consumer", &segment, &env)
        })
        .collect();

    let timeout = Duration::from_secs(180);

    // Collect consumer outputs first (they finish after consuming all events)
    let consumer_outputs: Vec<_> = consumers
        .into_iter()
        .map(|c| wait_with_output_timeout(c, timeout))
        .collect();
    let prod_result = wait_with_output_timeout(producer, timeout);

    let prod_out = match prod_result {
        Ok(o) => {
            if !o.stderr.is_empty() {
                eprintln!("[{class} prod stderr] {}", String::from_utf8_lossy(&o.stderr));
            }
            String::from_utf8_lossy(&o.stdout).to_string()
        }
        Err(e) => { eprintln!("[{class} prod] {e}"); String::new() }
    };

    let prod_tp = extract_value(&prod_out, "Throughput");

    let mut total_cons_tp = 0.0;
    let mut latency: Option<myelon_bench::latency::LatencyStats> = None;
    for (i, result) in consumer_outputs.into_iter().enumerate() {
        match result {
            Ok(o) => {
                if !o.stderr.is_empty() {
                    eprintln!("[{class} cons{i} stderr] {}", String::from_utf8_lossy(&o.stderr));
                }
                let stdout = String::from_utf8_lossy(&o.stdout).to_string();
                total_cons_tp += extract_value(&stdout, "Throughput");
                // Take worst-case (last) consumer latency for the report
                if let Some(lat) = extract_latency_json(&stdout) {
                    latency = Some(lat);
                }
            }
            Err(e) => eprintln!("[{class} cons{i}] {e}"),
        }
    }
    let avg_cons_tp = if num_consumers > 0 { total_cons_tp / num_consumers as f64 } else { 0.0 };

    let lat_str = latency.as_ref()
        .map(|l| l.summary())
        .unwrap_or_else(|| "-".to_string());

    println!(
        "  {:<25} producer: {:>10} ops/s  avg consumer: {:>10} ops/s  {}",
        class, format_throughput(prod_tp), format_throughput(avg_cons_tp), lat_str,
    );

    reporting::make_result(
        "raw_ring_shm", &class, "shm", "raw_ring",
        None, "BusySpin", std::mem::size_of::<MessageEvent>(), buffer, events, warmup, num_consumers,
        prod_tp, avg_cons_tp, latency,
    )
}

// ============================================================
// main
// ============================================================

fn main() {
    let args: Vec<String> = env::args().collect();

    // Child process dispatch
    if args.len() > 1 {
        let role = &args[1];
        if role.starts_with("--") { /* fall through to orchestrator */ } else {
            let result = match role.as_str() {
                "msg_producer" => message_producer(),
                "msg_consumer" => message_consumer(),
                "sig_producer" => signal_producer(),
                "sig_consumer" => signal_consumer(),
                "msg_multi_producer" => multi_message_producer(),
                "msg_multi_consumer" => multi_message_consumer(),
                _ => Ok(()),
            };
            if let Err(e) = result { eprintln!("{role} failed: {e}"); std::process::exit(1); }
            return;
        }
    }

    // Parse --class arg
    let class = args.windows(2)
        .find(|w| w[0] == "--class")
        .map(|w| w[1].as_str())
        .unwrap_or("all");

    // Parse --consumers arg (for multi-consumer message class)
    let consumers_arg = args.windows(2)
        .find(|w| w[0] == "--consumers")
        .map(|w| w[1].as_str())
        .unwrap_or("all");

    let json_mode = args.iter().any(|a| a == "--json");

    let run_message = class == "all" || class == "message";
    let run_signal = class == "all" || class == "signal";

    if !json_mode {
        println!("=== Raw Ring SHM Benchmark ===");
        println!("Backend: POSIX shared memory");
        if run_message {
            println!("Message class: {}B event, full 128B payload fill + timestamp",
                std::mem::size_of::<MessageEvent>());
        }
        if run_signal {
            println!("Signal class:  {}B event ({}B data), 10M events, 64K buffer — vs Alvarez Rosa V5 (305M ops/s)",
                std::mem::size_of::<SignalEvent>(), 16);
        }
        println!();
    }

    let mut report = BenchReport::new();

    if run_message {
        // 1p1c always runs
        if consumers_arg == "all" || consumers_arg == "1" {
            report.add(run_class(
                "message_1p1c_144B",
                "msg_producer", "msg_consumer",
                std::mem::size_of::<MessageEvent>(),
                100_000, 1024, 1_000,
            ));
        }

        // Multi-consumer scenarios
        // 1p3c: 144B, 100K events, 1K buffer, HDR latency
        if consumers_arg == "all" || consumers_arg == "3" {
            report.add(run_multi_message(3, 1024, 100_000, 1_000));
        }
        // 1p8c: 144B, 100K events, 4K buffer, throughput only
        if consumers_arg == "all" || consumers_arg == "8" {
            report.add(run_multi_message(8, 4096, 100_000, 1_000));
        }
        // 1p12c: 144B, 100K events, 4K buffer, throughput only (matches ipc_shm_high_load)
        if consumers_arg == "all" || consumers_arg == "12" {
            report.add(run_multi_message(12, 4096, 100_000, 1_000));
        }
    }

    if run_signal {
        report.add(run_class(
            "signal_1p1c_64B",
            "sig_producer", "sig_consumer",
            std::mem::size_of::<SignalEvent>(),
            10_000_000, 65_536, 100_000,
        ));
    }

    if json_mode {
        println!("{}", serde_json::to_string_pretty(&report).expect("serialize"));
    } else {
        report.print_summary();
    }

    if let Some(path) = env::var("MYELON_BENCH_JSON_OUT").ok() {
        report.write_json(&path).expect("write JSON");
    }
}
