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
use perf_bench::events::{format_throughput, nanos_now};
use perf_bench::latency::LatencyRecorder;
use perf_bench::reporting::{self, BenchReport, BenchResult};
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
    spawn_child_with_env(exe, role, root, segment, events, buffer, &[])
}

fn spawn_child_with_env(
    exe: &std::path::Path,
    role: &str,
    root: &str,
    segment: &str,
    events: u64,
    buffer: usize,
    extra_env: &[(&str, String)],
) -> Child {
    let mut cmd = Command::new(exe);
    cmd.arg(role)
        .env("MMAP_ROOT", root)
        .env("MMAP_SEGMENT", segment)
        .env("MMAP_BUFFER_SIZE", buffer.to_string())
        .env("MMAP_EVENTS", events.to_string())
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

    // Measured — identical work to SHM message class, absolute timestamps
    let start = Instant::now();
    for i in 0..num_events {
        producer.publish(|e| {
            e.id = WARMUP + i;
            e.timestamp = nanos_now();
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

    // Measured — with real per-event HDR latency
    let mut latency = LatencyRecorder::default_range();
    let start = Instant::now();
    let mut consumed = 0u64;
    while consumed < num_events {
        if let Some((_seq, event)) = consumer.try_consume_next() {
            consumed += 1;
            if event.timestamp > 0 {
                latency.record_delta(event.timestamp, nanos_now());
            }
        } else {
            std::hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} events/sec", consumed as f64 / elapsed.as_secs_f64());
    println!("Events: {}", consumed);
    if let Some(stats) = latency.stats() {
        println!("Latency: {}", stats.summary());
        println!("LatencyJSON: {}", serde_json::to_string(&stats).unwrap_or_default());
    }
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
// Multi-consumer message class (1p3c, 1p8c, 1p12c)
// ============================================================

fn multi_message_producer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    let num_consumers = read_env_usize("MMAP_NUM_CONSUMERS", 3);
    const WARMUP: u64 = 1_000;

    layout.ensure_directories()?;
    let mut producer = MmapProducer::<MessageEvent>::create(layout, buffer_size, MessageEvent::default)?;

    if !producer.wait_for_consumers_ready(num_consumers as i64, Duration::from_secs(60)) {
        return Err(format!("Timeout waiting for {num_consumers} consumers").into());
    }

    for i in 0..WARMUP {
        producer.publish(|e| { e.id = i; e.timestamp = 0; e.payload = [(i % 256) as u8; 128]; });
    }

    let start = Instant::now();
    for i in 0..num_events {
        producer.publish(|e| {
            e.id = WARMUP + i;
            e.timestamp = nanos_now();
            e.payload = [((WARMUP + i) % 256) as u8; 128];
        });
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} events/sec", num_events as f64 / elapsed.as_secs_f64());
    println!("Time: {:.3} seconds", elapsed.as_secs_f64());

    let last_seq = (WARMUP + num_events - 1) as i64;
    producer.wait_until_consumed_with_strategy(last_seq, Duration::from_secs(90), AutoWaitStrategy::BusySpin);
    Ok(())
}

fn multi_message_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    let record_latency = read_env_usize("MMAP_RECORD_LATENCY", 0) == 1;
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

    let mut warmup_count = 0u64;
    while warmup_count < WARMUP {
        if consumer.try_consume_next().is_some() { warmup_count += 1; } else { std::hint::spin_loop(); }
    }

    let mut latency = if record_latency {
        Some(LatencyRecorder::default_range())
    } else {
        None
    };
    let start = Instant::now();
    let mut consumed = 0u64;
    while consumed < num_events {
        if let Some((_seq, event)) = consumer.try_consume_next() {
            consumed += 1;
            if let Some(ref mut lat) = latency {
                if event.timestamp > 0 {
                    lat.record_delta(event.timestamp, nanos_now());
                }
            }
        } else {
            std::hint::spin_loop();
        }
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
    Ok(())
}

// ============================================================
// Orchestrator
// ============================================================

fn extract_latency_json(output: &str) -> Option<perf_bench::latency::LatencyStats> {
    for line in output.lines() {
        if let Some(json) = line.strip_prefix("LatencyJSON: ") {
            return serde_json::from_str(json).ok();
        }
    }
    None
}

fn run_class(class: &str, prod_role: &str, cons_role: &str, events: u64, buffer: usize, warmup: u64, event_bytes: usize) -> BenchResult {
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
    let latency = extract_latency_json(&cons_out);

    let _ = std::fs::remove_dir_all(&root);

    let lat_str = latency.as_ref()
        .map(|l| l.summary())
        .unwrap_or_else(|| "-".to_string());

    println!("  {:<25} producer: {:>10} ops/s  consumer: {:>10} ops/s  {}",
        class, format_throughput(prod_tp), format_throughput(cons_tp), lat_str);

    reporting::make_result(
        "raw_ring_mmap", class, "mmap", "raw_ring",
        None, "BusySpin", event_bytes, buffer, events, warmup, 1,
        prod_tp, cons_tp, latency,
    )
}

fn run_multi_message(num_consumers: usize, buffer: usize, events: u64, warmup: u64) -> BenchResult {
    let root = unique_root(&format!("multi{}c", num_consumers));
    let segment = unique_segment(&format!("multi{}c", num_consumers));
    let exe = env::current_exe().expect("current_exe");
    let root_str = root.display().to_string();
    let class = format!("message_1p{}c_144B", num_consumers);
    let record_latency = num_consumers <= 3;

    let extra_env: Vec<(&str, String)> = vec![
        ("MMAP_NUM_CONSUMERS", num_consumers.to_string()),
    ];

    let producer = spawn_child_with_env(
        &exe, "mmap_multi_msg_producer", &root_str, &segment, events, buffer, &extra_env,
    );

    let consumers: Vec<Child> = (0..num_consumers)
        .map(|_| {
            let mut env = extra_env.clone();
            if record_latency {
                env.push(("MMAP_RECORD_LATENCY", "1".to_string()));
            }
            spawn_child_with_env(
                &exe, "mmap_multi_msg_consumer", &root_str, &segment, events, buffer, &env,
            )
        })
        .collect();

    let timeout = Duration::from_secs(180);
    let consumer_outputs: Vec<_> = consumers
        .into_iter()
        .map(|c| wait_with_output_timeout(c, timeout))
        .collect();
    let prod_result = wait_with_output_timeout(producer, timeout);

    let prod_out = match prod_result {
        Ok(o) => {
            if !o.stderr.is_empty() { eprintln!("[{class} prod stderr] {}", String::from_utf8_lossy(&o.stderr)); }
            String::from_utf8_lossy(&o.stdout).to_string()
        }
        Err(e) => { eprintln!("[{class} prod] {e}"); String::new() }
    };

    let prod_tp = extract_value(&prod_out, "Throughput");

    let mut total_cons_tp = 0.0;
    let mut latency: Option<perf_bench::latency::LatencyStats> = None;
    for (i, result) in consumer_outputs.into_iter().enumerate() {
        match result {
            Ok(o) => {
                if !o.stderr.is_empty() { eprintln!("[{class} cons{i} stderr] {}", String::from_utf8_lossy(&o.stderr)); }
                let stdout = String::from_utf8_lossy(&o.stdout).to_string();
                total_cons_tp += extract_value(&stdout, "Throughput");
                if let Some(lat) = extract_latency_json(&stdout) { latency = Some(lat); }
            }
            Err(e) => eprintln!("[{class} cons{i}] {e}"),
        }
    }
    let avg_cons_tp = if num_consumers > 0 { total_cons_tp / num_consumers as f64 } else { 0.0 };

    let _ = std::fs::remove_dir_all(&root);

    let lat_str = latency.as_ref().map(|l| l.summary()).unwrap_or_else(|| "-".to_string());
    println!(
        "  {:<25} producer: {:>10} ops/s  avg consumer: {:>10} ops/s  {}",
        class, format_throughput(prod_tp), format_throughput(avg_cons_tp), lat_str,
    );

    reporting::make_result(
        "raw_ring_mmap", &class, "mmap", "raw_ring",
        None, "BusySpin", std::mem::size_of::<MessageEvent>(), buffer, events, warmup, num_consumers,
        prod_tp, avg_cons_tp, latency,
    )
}

fn main() {
    let args: Vec<String> = env::args().collect();

    // Child dispatch
    if args.len() > 1 {
        let role = &args[1];
        if role.starts_with("--") { /* fall through */ } else {
            let result = match role.as_str() {
                "mmap_msg_producer" => message_producer(),
                "mmap_msg_consumer" => message_consumer(),
                "mmap_sig_producer" => signal_producer(),
                "mmap_sig_consumer" => signal_consumer(),
                "mmap_multi_msg_producer" => multi_message_producer(),
                "mmap_multi_msg_consumer" => multi_message_consumer(),
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

    let consumers_arg = args.windows(2)
        .find(|w| w[0] == "--consumers")
        .map(|w| w[1].as_str())
        .unwrap_or("all");

    let json_mode = args.iter().any(|a| a == "--json");

    let run_message = class == "all" || class == "message";
    let run_signal = class == "all" || class == "signal";

    if !json_mode {
        println!("=== Raw Ring MMAP Benchmark ===");
        println!("Backend: file-backed mmap");
        if run_message {
            println!("Message class: {}B event, full 128B payload fill + timestamp",
                std::mem::size_of::<MessageEvent>());
        }
        if run_signal {
            println!("Signal class:  {}B event ({}B data), 10M events, 64K buffer",
                std::mem::size_of::<SignalEvent>(), 16);
        }
        println!();
    }

    let mut report = BenchReport::new();

    if run_message {
        if consumers_arg == "all" || consumers_arg == "1" {
            report.add(run_class(
                "message_1p1c_144B",
                "mmap_msg_producer", "mmap_msg_consumer",
                100_000, 1024, 1_000,
                std::mem::size_of::<MessageEvent>(),
            ));
        }
        if consumers_arg == "all" || consumers_arg == "3" {
            report.add(run_multi_message(3, 1024, 100_000, 1_000));
        }
        if consumers_arg == "all" || consumers_arg == "8" {
            report.add(run_multi_message(8, 4096, 100_000, 1_000));
        }
        if consumers_arg == "all" || consumers_arg == "12" {
            report.add(run_multi_message(12, 4096, 100_000, 1_000));
        }
    }

    if run_signal {
        report.add(run_class(
            "signal_1p1c_64B",
            "mmap_sig_producer", "mmap_sig_consumer",
            10_000_000, 65_536, 100_000,
            std::mem::size_of::<SignalEvent>(),
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
