//! Raw disruptor-mp ring benchmark over mmap (file-backed) backend.
//!
//! Same measurement as raw_ring_shm but uses MmapProducer/MmapConsumer
//! instead of SharedProducer/SharedConsumer.
//!
//! The mmap backend has built-in coordination via readiness cursors —
//! no separate BenchmarkCoordination needed.
//!
//! Run:   cargo bench -p myelon-bench --bench raw_ring_mmap
//! Quick: cargo bench -p myelon-bench --bench raw_ring_mmap -- --quick

use disruptor_mp::{AutoWaitStrategy, MmapConsumer, MmapProducer, MmapTransportLayout};
use myelon_bench::events::{data_rate_mbps, format_throughput, BenchEvent};
use myelon_bench::reporting::{BenchReport, BenchResult};
use std::env;
use std::io::Read as _;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const BUFFER_SIZE: usize = 1024;
const NUM_EVENTS: u64 = 100_000;
const WARMUP_EVENTS: u64 = 1_000;

type Event = BenchEvent<128>;

// --- Helpers ---

fn unique_root() -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    env::temp_dir().join(format!("myelon_bench_mmap_{}_{}", std::process::id(), ts))
}

fn unique_segment() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("mbbm_{}_{}", std::process::id(), ts)
}

fn spawn_child(exe: &std::path::Path, role: &str, root: &str, segment: &str) -> Child {
    Command::new(exe)
        .arg(role)
        .env("MMAP_ROOT", root)
        .env("MMAP_SEGMENT", segment)
        .env("MMAP_BUFFER_SIZE", BUFFER_SIZE.to_string())
        .env("MMAP_EVENTS", NUM_EVENTS.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {role}: {e}"))
}

fn wait_with_output_timeout(mut child: Child, timeout: Duration) -> Result<Output, String> {
    fn collect(child: &mut Child, status: std::process::ExitStatus) -> Output {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        if let Some(mut out) = child.stdout.take() { let _ = out.read_to_end(&mut stdout); }
        if let Some(mut err) = child.stderr.take() { let _ = err.read_to_end(&mut stderr); }
        Output { status, stdout, stderr }
    }
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
            return Ok(collect(&mut child, status));
        }
        if start.elapsed() >= timeout {
            let _ = child.kill();
            let status = child.wait().map_err(|e| e.to_string())?;
            let output = collect(&mut child, status);
            return Err(format!(
                "timeout after {:?}; stderr: {}",
                timeout,
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn extract_value(output: &str, key: &str) -> f64 {
    for line in output.lines() {
        if let Some(rest) = line.strip_prefix(&format!("{key}: ")) {
            if let Some(num_str) = rest.split_whitespace().next() {
                return num_str.parse().unwrap_or(0.0);
            }
        }
    }
    0.0
}

fn child_layout() -> MmapTransportLayout {
    let root = env::var("MMAP_ROOT").expect("MMAP_ROOT");
    let segment = env::var("MMAP_SEGMENT").expect("MMAP_SEGMENT");
    MmapTransportLayout::new(PathBuf::from(root), segment).expect("layout")
}

// ============================================================
// Producer child process
// ============================================================

fn producer_process() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;

    layout.ensure_directories()?;

    let mut producer = MmapProducer::<Event>::create(layout, buffer_size, Event::default)?;

    // Wait for consumer using built-in mmap coordination
    if !producer.wait_for_consumers_ready(1, Duration::from_secs(30)) {
        return Err("Timeout waiting for consumer".into());
    }

    // Warmup
    for i in 0..WARMUP_EVENTS {
        producer.publish(|slot| {
            slot.sequence = i;
            slot.timestamp_ns = 0;
        });
    }

    // Measured
    let start = Instant::now();
    for i in 0..num_events {
        producer.publish(|slot| {
            slot.sequence = WARMUP_EVENTS + i;
            slot.timestamp_ns = 0;
        });
    }
    let elapsed = start.elapsed();

    let throughput = num_events as f64 / elapsed.as_secs_f64();
    println!("Throughput: {:.0} events/sec", throughput);
    println!("Time: {:.3} seconds", elapsed.as_secs_f64());

    // Wait for consumer to finish consuming
    let last_seq = (WARMUP_EVENTS + num_events - 1) as i64;
    producer.wait_until_consumed_with_strategy(last_seq, Duration::from_secs(30), AutoWaitStrategy::BusySpin);

    Ok(())
}

// ============================================================
// Consumer child process
// ============================================================

fn consumer_process() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    let consumer_id = format!("c{}", std::process::id());

    // Retry attach until producer has created the layout
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapConsumer::<Event>::attach(layout.clone(), buffer_size, &consumer_id) {
            Ok(c) => break c,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(e) => return Err(format!("consumer attach failed: {e}").into()),
        }
    };

    // Warmup
    let mut warmup_consumed = 0u64;
    while warmup_consumed < WARMUP_EVENTS {
        if consumer.try_consume_next().is_some() {
            warmup_consumed += 1;
        } else {
            std::hint::spin_loop();
        }
    }

    // Measured
    let start = Instant::now();
    let mut events_consumed = 0u64;
    while events_consumed < num_events {
        if consumer.try_consume_next().is_some() {
            events_consumed += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();

    let throughput = events_consumed as f64 / elapsed.as_secs_f64();
    println!("Throughput: {:.0} events/sec", throughput);
    println!("Time: {:.3} seconds", elapsed.as_secs_f64());
    println!("Events: {}", events_consumed);

    Ok(())
}

// ============================================================
// Orchestrator
// ============================================================

fn run_benchmark() -> BenchResult {
    let root = unique_root();
    let segment = unique_segment();
    let exe = env::current_exe().expect("current_exe");

    let root_str = root.display().to_string();

    let producer_child = spawn_child(&exe, "mmap_producer", &root_str, &segment);
    let consumer_child = spawn_child(&exe, "mmap_consumer", &root_str, &segment);

    let timeout = Duration::from_secs(120);
    let producer_result = wait_with_output_timeout(producer_child, timeout);
    let consumer_result = wait_with_output_timeout(consumer_child, timeout);

    let prod_output = match producer_result {
        Ok(o) => {
            if !o.stderr.is_empty() {
                eprintln!("Producer stderr: {}", String::from_utf8_lossy(&o.stderr));
            }
            String::from_utf8_lossy(&o.stdout).to_string()
        }
        Err(e) => { eprintln!("Producer error: {e}"); String::new() }
    };

    let cons_output = match consumer_result {
        Ok(o) => {
            if !o.stderr.is_empty() {
                eprintln!("Consumer stderr: {}", String::from_utf8_lossy(&o.stderr));
            }
            String::from_utf8_lossy(&o.stdout).to_string()
        }
        Err(e) => { eprintln!("Consumer error: {e}"); String::new() }
    };

    let prod_throughput = extract_value(&prod_output, "Throughput");
    let cons_throughput = extract_value(&cons_output, "Throughput");
    let payload_bytes = std::mem::size_of::<Event>();

    // Cleanup
    let _ = std::fs::remove_dir_all(&root);

    println!(
        "  1p1c_{}B  producer: {} ops/s  consumer: {} ops/s",
        payload_bytes,
        format_throughput(prod_throughput),
        format_throughput(cons_throughput),
    );

    BenchResult {
        scenario: format!("1p1c_{}B", payload_bytes),
        backend: "mmap".to_string(),
        layer: "raw_ring".to_string(),
        codec: None,
        wait_strategy: "BusySpin".to_string(),
        num_consumers: 1,
        payload_bytes,
        events: NUM_EVENTS as usize,
        producer_ops_sec: prod_throughput,
        consumer_ops_sec: cons_throughput,
        data_rate_mbps: data_rate_mbps(prod_throughput, payload_bytes),
        latency: None,
    }
}

// ============================================================
// main — role dispatch
// ============================================================

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 {
        let role = &args[1];
        if role == "--bench" {
            // cargo bench mode — fall through
        } else {
            match role.as_str() {
                "mmap_producer" => {
                    if let Err(e) = producer_process() {
                        eprintln!("Producer failed: {e}");
                        std::process::exit(1);
                    }
                    return;
                }
                "mmap_consumer" => {
                    if let Err(e) = consumer_process() {
                        eprintln!("Consumer failed: {e}");
                        std::process::exit(1);
                    }
                    return;
                }
                _ => {}
            }
        }
    }

    println!("=== Raw Ring MMAP Benchmark ===");
    println!("Backend: file-backed mmap");
    println!("Event size: {} bytes", std::mem::size_of::<Event>());
    println!("Buffer: {} slots", BUFFER_SIZE);
    println!("Events: {}", NUM_EVENTS);
    println!();

    let mut report = BenchReport::new();
    report.add(run_benchmark());
    report.print_summary();

    if let Some(path) = env::var("MYELON_BENCH_JSON_OUT").ok() {
        report.write_json(&path).expect("write JSON");
        println!("JSON: {path}");
    }
}
