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
use myelon_bench::events::{data_rate_mbps, format_throughput};
use myelon_bench::reporting::{BenchReport, BenchResult};
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
    Command::new(exe)
        .arg(role)
        .env("BENCHMARK_SEGMENT_NAME", segment)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {role}: {e}"))
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

    let mut producer = build_shared_single_producer::<MessageEvent>(&segment, BUFFER)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(MessageEvent::default)?;

    let coord = BenchmarkCoordination::create(&segment)?;

    if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
        return Err("timeout waiting for consumer".into());
    }

    // Warmup — full payload fill, matching original
    for i in 0..WARMUP {
        producer.publish(|event| {
            event.id = i;
            event.timestamp = 0;
            event.payload = [(i % 256) as u8; 128];
        });
    }

    // Measured — full 128B payload fill + timestamp on every event
    let start = Instant::now();
    for i in 0..EVENTS {
        producer.publish(|event| {
            event.id = WARMUP + i;
            event.timestamp = start.elapsed().as_nanos() as u64;
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
// Signal-class: producer (head-to-head vs Alvarez Rosa V5)
// ============================================================

fn signal_producer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = get_segment_name();
    const BUFFER: usize = 65_536; // 64K slots — large ring to avoid wraps, matching articles
    const EVENTS: u64 = 10_000_000; // 10M events — run for seconds, not milliseconds
    const WARMUP: u64 = 100_000;

    let mut producer = build_shared_single_producer::<SignalEvent>(&segment, BUFFER)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(SignalEvent::default)?;

    let coord = BenchmarkCoordination::create(&segment)?;

    if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
        return Err("timeout waiting for consumer".into());
    }

    // Warmup
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
// Orchestrator
// ============================================================

fn run_class(class: &str, prod_role: &str, cons_role: &str, event_bytes: usize, events: u64) -> BenchResult {
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

    println!(
        "  {:<25} producer: {:>10} ops/s  consumer: {:>10} ops/s",
        class, format_throughput(prod_tp), format_throughput(cons_tp),
    );

    BenchResult {
        scenario: class.to_string(),
        backend: "shm".to_string(),
        layer: "raw_ring".to_string(),
        codec: None,
        wait_strategy: "BusySpin".to_string(),
        num_consumers: 1,
        payload_bytes: event_bytes,
        events: events as usize,
        producer_ops_sec: prod_tp,
        consumer_ops_sec: cons_tp,
        data_rate_mbps: data_rate_mbps(prod_tp, event_bytes),
        latency: None,
    }
}

// ============================================================
// main
// ============================================================

fn main() {
    let args: Vec<String> = env::args().collect();

    // Child process dispatch
    if args.len() > 1 {
        let role = &args[1];
        if role == "--bench" || role == "--class" { /* fall through */ } else {
            let result = match role.as_str() {
                "msg_producer" => message_producer(),
                "msg_consumer" => message_consumer(),
                "sig_producer" => signal_producer(),
                "sig_consumer" => signal_consumer(),
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

    let run_message = class == "all" || class == "message";
    let run_signal = class == "all" || class == "signal";

    println!("=== Raw Ring SHM Benchmark ===");
    println!("Backend: POSIX shared memory");
    if run_message {
        println!("Message class: {}B event, full 128B payload fill + timestamp, 100K events, 1024 buffer",
            std::mem::size_of::<MessageEvent>());
    }
    if run_signal {
        println!("Signal class:  {}B event ({}B data), 10M events, 64K buffer — vs Alvarez Rosa V5 (305M ops/s)",
            std::mem::size_of::<SignalEvent>(), 16);
    }
    println!();

    let mut report = BenchReport::new();

    if run_message {
        report.add(run_class(
            "message_1p1c_144B",
            "msg_producer", "msg_consumer",
            std::mem::size_of::<MessageEvent>(),
            100_000,
        ));
    }

    if run_signal {
        report.add(run_class(
            "signal_1p1c_64B",
            "sig_producer", "sig_consumer",
            std::mem::size_of::<SignalEvent>(),
            10_000_000,
        ));
    }

    report.print_summary();

    if let Some(path) = env::var("MYELON_BENCH_JSON_OUT").ok() {
        report.write_json(&path).expect("write JSON");
    }
}
