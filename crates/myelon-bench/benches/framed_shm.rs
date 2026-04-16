//! FramedTransport benchmark over SHM backend.
//!
//! Measures the overhead of myelon's FramedTransportProducer/Consumer
//! (frame headers, message reassembly) vs raw disruptor-mp ring.
//!
//! Run: cargo bench -p myelon-bench --bench framed_shm

use myelon_bench::coordination::BenchmarkCoordination;
use myelon_bench::events::{data_rate_mbps, format_throughput};
use myelon_bench::reporting::{BenchReport, BenchResult};
use myelon::transport::{
    FixedFrame, FramedTransportConsumer, FramedTransportProducer, MyelonWaitStrategy,
};
use std::env;
use std::io::Read as _;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

// 64KB frame (matching competitor-rs RPC frame size)
const FRAME_DATA_BYTES: usize = 64 * 1024 - 12;
type Frame = FixedFrame<FRAME_DATA_BYTES>;

const BUFFER_DEPTH: usize = 1024;
const NUM_MESSAGES: u64 = 50_000;

fn get_segment_name() -> String {
    if let Ok(name) = env::var("BENCHMARK_SEGMENT_NAME") {
        return name;
    }
    disruptor_mp::portable_shm_segment_name("frshm")
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
        if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
            return Ok(collect(&mut child, status));
        }
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
            if let Some(n) = rest.split_whitespace().next() {
                return n.parse().unwrap_or(0.0);
            }
        }
    }
    0.0
}

// ============================================================
// Producer child
// ============================================================

fn producer_process() -> Result<(), Box<dyn std::error::Error>> {
    let segment = get_segment_name();

    // Create framed transport producer (creates underlying ring)
    let mut producer = FramedTransportProducer::<Frame>::create(&segment, BUFFER_DEPTH)?;

    // Create coordination after ring exists
    let coord = BenchmarkCoordination::create(&segment)?;

    // Wait for consumer
    if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
        return Err("timeout waiting for consumer".into());
    }

    // Trigger consumer discovery for backpressure
    producer.discover_consumers(Duration::from_secs(3));

    // Generate a realistic payload (1KB — fits in one 64KB frame)
    let payload = vec![42u8; 1024];

    // Measured phase
    let start = Instant::now();
    for i in 0..NUM_MESSAGES {
        producer.publish(&payload, (i % 256) as u8);
    }
    let elapsed = start.elapsed();

    let throughput = NUM_MESSAGES as f64 / elapsed.as_secs_f64();
    println!("Throughput: {:.0} msgs/sec", throughput);
    println!("Time: {:.3} seconds", elapsed.as_secs_f64());

    coord.signal_producer_done(NUM_MESSAGES as i64);
    coord.wait_for_consumers_done(1, Duration::from_secs(30));

    Ok(())
}

// ============================================================
// Consumer child
// ============================================================

fn consumer_process() -> Result<(), Box<dyn std::error::Error>> {
    let segment = get_segment_name();

    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;

    let mut consumer =
        FramedTransportConsumer::<Frame>::attach(&segment, BUFFER_DEPTH, MyelonWaitStrategy::BusySpin)?;

    coord.signal_consumer_ready();

    let start = Instant::now();
    let mut consumed = 0u64;

    loop {
        let (_kind, _data) = consumer.recv_message_blocking();
        consumed += 1;

        if coord.is_producer_done() && consumed >= coord.events_produced() as u64 {
            break;
        }
    }
    let elapsed = start.elapsed();

    let throughput = consumed as f64 / elapsed.as_secs_f64();
    println!("Throughput: {:.0} msgs/sec", throughput);
    println!("Time: {:.3} seconds", elapsed.as_secs_f64());
    println!("Events: {}", consumed);

    coord.signal_consumer_done(consumed as i64);

    Ok(())
}

// ============================================================
// Orchestrator
// ============================================================

fn run_benchmark(payload_label: &str) -> BenchResult {
    let segment = get_segment_name();
    let exe = env::current_exe().expect("current_exe");

    let producer_child = spawn_child(&exe, "framed_producer", &segment);
    let consumer_child = spawn_child(&exe, "framed_consumer", &segment);

    let timeout = Duration::from_secs(120);
    let prod_result = wait_with_output_timeout(producer_child, timeout);
    let cons_result = wait_with_output_timeout(consumer_child, timeout);

    let prod_out = match prod_result {
        Ok(o) => { if !o.stderr.is_empty() { eprintln!("Prod stderr: {}", String::from_utf8_lossy(&o.stderr)); } String::from_utf8_lossy(&o.stdout).to_string() }
        Err(e) => { eprintln!("Prod error: {e}"); String::new() }
    };
    let cons_out = match cons_result {
        Ok(o) => { if !o.stderr.is_empty() { eprintln!("Cons stderr: {}", String::from_utf8_lossy(&o.stderr)); } String::from_utf8_lossy(&o.stdout).to_string() }
        Err(e) => { eprintln!("Cons error: {e}"); String::new() }
    };

    let prod_tp = extract_value(&prod_out, "Throughput");
    let cons_tp = extract_value(&cons_out, "Throughput");

    println!(
        "  framed_1p1c_{} producer: {} msgs/s  consumer: {} msgs/s",
        payload_label,
        format_throughput(prod_tp),
        format_throughput(cons_tp),
    );

    BenchResult {
        scenario: format!("framed_1p1c_{payload_label}"),
        backend: "shm".to_string(),
        layer: "framed".to_string(),
        codec: None,
        wait_strategy: "BusySpin".to_string(),
        num_consumers: 1,
        payload_bytes: 1024,
        events: NUM_MESSAGES as usize,
        producer_ops_sec: prod_tp,
        consumer_ops_sec: cons_tp,
        data_rate_mbps: data_rate_mbps(prod_tp, 1024),
        latency: None,
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 {
        let role = &args[1];
        if role == "--bench" {
            // fall through
        } else {
            match role.as_str() {
                "framed_producer" => {
                    if let Err(e) = producer_process() { eprintln!("Producer failed: {e}"); std::process::exit(1); }
                    return;
                }
                "framed_consumer" => {
                    if let Err(e) = consumer_process() { eprintln!("Consumer failed: {e}"); std::process::exit(1); }
                    return;
                }
                _ => {}
            }
        }
    }

    println!("=== Framed Transport SHM Benchmark ===");
    println!("Frame: {}KB data capacity", FRAME_DATA_BYTES / 1024);
    println!("Payload: 1KB per message");
    println!("Buffer: {} frames", BUFFER_DEPTH);
    println!("Messages: {}", NUM_MESSAGES);
    println!();

    let mut report = BenchReport::new();
    report.add(run_benchmark("1KB"));
    report.print_summary();

    if let Some(path) = env::var("MYELON_BENCH_JSON_OUT").ok() {
        report.write_json(&path).expect("write JSON");
    }
}
