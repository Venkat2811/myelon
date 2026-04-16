//! Raw disruptor-mp ring benchmark over mmap (file-backed) backend.
//!
//! Two benchmark classes (matching raw_ring_shm exactly):
//!   --class message  : 144B event, full 128B payload fill + timestamp
//!   --class signal   : 64B event, 16B data only
//!   (default)        : runs both
//!
//! Run:   cargo bench -p myelon-bench --bench raw_ring_mmap
//! Signal only: cargo bench -p myelon-bench --bench raw_ring_mmap -- --class signal

use disruptor_mp::{AutoWaitStrategy, MmapConsumer, MmapProducer, MmapTransportLayout};
use myelon_bench::events::{data_rate_mbps, format_throughput};
use myelon_bench::reporting::{BenchReport, BenchResult};
use std::env;
use std::io::Read as _;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ============================================================
// Event types (identical to raw_ring_shm)
// ============================================================

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
// Helpers
// ============================================================

fn unique_root(label: &str) -> PathBuf {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    env::temp_dir().join(format!("mb_mmap_{label}_{}_{}", std::process::id(), ts))
}

fn unique_segment(label: &str) -> String {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("mb_{label}_{}_{}", std::process::id() % 10000, ts % 100000)
}

fn child_layout() -> MmapTransportLayout {
    let root = env::var("MMAP_ROOT").expect("MMAP_ROOT");
    let segment = env::var("MMAP_SEGMENT").expect("MMAP_SEGMENT");
    MmapTransportLayout::new(PathBuf::from(root), segment).expect("layout")
}

fn spawn_child(exe: &std::path::Path, role: &str, root: &str, segment: &str, events: u64, buffer: usize) -> Child {
    Command::new(exe)
        .arg(role)
        .env("MMAP_ROOT", root)
        .env("MMAP_SEGMENT", segment)
        .env("MMAP_BUFFER_SIZE", buffer.to_string())
        .env("MMAP_EVENTS", events.to_string())
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
// Message-class producer (full 128B fill + timestamp — matches SHM message class)
// ============================================================

fn message_producer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    const WARMUP: u64 = 1_000;

    layout.ensure_directories()?;
    let mut producer = MmapProducer::<MessageEvent>::create(layout, buffer_size, MessageEvent::default)?;

    if !producer.wait_for_consumers_ready(1, Duration::from_secs(30)) {
        return Err("Timeout waiting for consumer".into());
    }

    // Warmup — full payload fill
    for i in 0..WARMUP {
        producer.publish(|e| { e.id = i; e.timestamp = 0; e.payload = [(i % 256) as u8; 128]; });
    }

    // Measured — identical work to SHM message class
    let start = Instant::now();
    for i in 0..num_events {
        producer.publish(|e| {
            e.id = WARMUP + i;
            e.timestamp = start.elapsed().as_nanos() as u64;
            e.payload = [((WARMUP + i) % 256) as u8; 128];
        });
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} events/sec", num_events as f64 / elapsed.as_secs_f64());
    println!("Time: {:.3} seconds", elapsed.as_secs_f64());

    let last_seq = (WARMUP + num_events - 1) as i64;
    producer.wait_until_consumed_with_strategy(last_seq, Duration::from_secs(30), AutoWaitStrategy::BusySpin);
    Ok(())
}

fn message_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    let consumer_id = format!("c{}", std::process::id());
    const WARMUP: u64 = 1_000;

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapConsumer::<MessageEvent>::attach(layout.clone(), buffer_size, &consumer_id) {
            Ok(c) => break c,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(e) => return Err(format!("attach failed: {e}").into()),
        }
    };

    // Warmup
    let mut warmup = 0u64;
    while warmup < WARMUP {
        if consumer.try_consume_next().is_some() { warmup += 1; } else { std::hint::spin_loop(); }
    }

    // Measured
    let start = Instant::now();
    let mut consumed = 0u64;
    while consumed < num_events {
        if consumer.try_consume_next().is_some() { consumed += 1; } else { std::hint::spin_loop(); }
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} events/sec", consumed as f64 / elapsed.as_secs_f64());
    println!("Events: {}", consumed);
    Ok(())
}

// ============================================================
// Signal-class producer (64B, 16B data — matches SHM signal class)
// ============================================================

fn signal_producer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    const WARMUP: u64 = 100_000;

    layout.ensure_directories()?;
    let mut producer = MmapProducer::<SignalEvent>::create(layout, buffer_size, SignalEvent::default)?;

    if !producer.wait_for_consumers_ready(1, Duration::from_secs(30)) {
        return Err("Timeout waiting for consumer".into());
    }

    for i in 0..WARMUP {
        producer.publish(|s| { s.sequence = i; s.data = i.wrapping_mul(0x9E3779B97F4A7C15); });
    }

    let start = Instant::now();
    for i in 0..num_events {
        producer.publish(|s| {
            s.sequence = WARMUP + i;
            s.data = (WARMUP + i).wrapping_mul(0x9E3779B97F4A7C15);
        });
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} events/sec", num_events as f64 / elapsed.as_secs_f64());
    println!("Time: {:.3} seconds", elapsed.as_secs_f64());

    let last_seq = (WARMUP + num_events - 1) as i64;
    producer.wait_until_consumed_with_strategy(last_seq, Duration::from_secs(30), AutoWaitStrategy::BusySpin);
    Ok(())
}

fn signal_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    let consumer_id = format!("c{}", std::process::id());
    const WARMUP: u64 = 100_000;

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapConsumer::<SignalEvent>::attach(layout.clone(), buffer_size, &consumer_id) {
            Ok(c) => break c,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(e) => return Err(format!("attach failed: {e}").into()),
        }
    };

    let mut warmup = 0u64;
    while warmup < WARMUP {
        if consumer.try_consume_next().is_some() { warmup += 1; } else { std::hint::spin_loop(); }
    }

    let start = Instant::now();
    let mut consumed = 0u64;
    while consumed < num_events {
        if consumer.try_consume_next().is_some() { consumed += 1; } else { std::hint::spin_loop(); }
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} events/sec", consumed as f64 / elapsed.as_secs_f64());
    println!("Events: {}", consumed);
    Ok(())
}

// ============================================================
// Orchestrator
// ============================================================

fn run_class(class: &str, prod_role: &str, cons_role: &str, events: u64, buffer: usize, event_bytes: usize) -> BenchResult {
    let root = unique_root(class);
    let segment = unique_segment(class);
    let exe = env::current_exe().expect("current_exe");
    let root_str = root.display().to_string();

    let producer = spawn_child(&exe, prod_role, &root_str, &segment, events, buffer);
    let consumer = spawn_child(&exe, cons_role, &root_str, &segment, events, buffer);

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

    let _ = std::fs::remove_dir_all(&root);

    println!("  {:<25} producer: {:>10} ops/s  consumer: {:>10} ops/s",
        class, format_throughput(prod_tp), format_throughput(cons_tp));

    BenchResult {
        scenario: class.to_string(),
        backend: "mmap".to_string(),
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

fn main() {
    let args: Vec<String> = env::args().collect();

    // Child dispatch
    if args.len() > 1 {
        let role = &args[1];
        if role == "--bench" || role == "--class" { /* fall through */ } else {
            let result = match role.as_str() {
                "mmap_msg_producer" => message_producer(),
                "mmap_msg_consumer" => message_consumer(),
                "mmap_sig_producer" => signal_producer(),
                "mmap_sig_consumer" => signal_consumer(),
                _ => Ok(()),
            };
            if let Err(e) = result { eprintln!("{role} failed: {e}"); std::process::exit(1); }
            return;
        }
    }

    let class = args.windows(2)
        .find(|w| w[0] == "--class")
        .map(|w| w[1].as_str())
        .unwrap_or("all");

    let run_message = class == "all" || class == "message";
    let run_signal = class == "all" || class == "signal";

    println!("=== Raw Ring MMAP Benchmark ===");
    println!("Backend: file-backed mmap");
    if run_message {
        println!("Message class: {}B event, full 128B payload fill + timestamp, 100K events, 1024 buffer",
            std::mem::size_of::<MessageEvent>());
    }
    if run_signal {
        println!("Signal class:  {}B event ({}B data), 10M events, 64K buffer",
            std::mem::size_of::<SignalEvent>(), 16);
    }
    println!();

    let mut report = BenchReport::new();

    if run_message {
        report.add(run_class(
            "message_1p1c_144B",
            "mmap_msg_producer", "mmap_msg_consumer",
            100_000, 1024,
            std::mem::size_of::<MessageEvent>(),
        ));
    }

    if run_signal {
        report.add(run_class(
            "signal_1p1c_64B",
            "mmap_sig_producer", "mmap_sig_consumer",
            10_000_000, 65_536,
            std::mem::size_of::<SignalEvent>(),
        ));
    }

    report.print_summary();

    if let Some(path) = env::var("MYELON_BENCH_JSON_OUT").ok() {
        report.write_json(&path).expect("write JSON");
    }
}
