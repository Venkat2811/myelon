//! FramedTransport benchmark over SHM backend.
//!
//! Measures the overhead of myelon's FramedTransportProducer/Consumer
//! (frame headers, message reassembly) vs raw disruptor-mp ring.
//!
//! Payload sizes: 1KB (single-frame), 32KB, 64KB (max single-frame), 128KB (fragmented)
//!
//! Run: cargo bench -p myelon-bench --bench framed_shm
//! Single payload: cargo bench -p myelon-bench --bench framed_shm -- --payload 128K

use perf_bench::coordination::BenchmarkCoordination;
use perf_bench::events::format_throughput;
use perf_bench::reporting::{self, BenchReport, BenchResult};
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

fn get_segment_name() -> String {
    if let Ok(name) = env::var("BENCHMARK_SEGMENT_NAME") {
        return name;
    }
    disruptor_mp::portable_shm_segment_name("frshm")
}

fn read_env_usize(key: &str, default: usize) -> usize {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn read_env_u64(key: &str, default: u64) -> u64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
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
// Producer child (configurable payload size + message count)
// ============================================================

fn producer_process() -> Result<(), Box<dyn std::error::Error>> {
    let segment = get_segment_name();
    let payload_size = read_env_usize("BENCH_PAYLOAD_SIZE", 1024);
    let num_messages = read_env_u64("BENCH_NUM_MESSAGES", 50_000);
    let buffer_depth = read_env_usize("BENCH_BUFFER_DEPTH", 1024);
    let num_consumers = read_env_usize("BENCH_NUM_CONSUMERS", 1);

    let mut producer = FramedTransportProducer::<Frame>::create(&segment, buffer_depth)?;
    let coord = BenchmarkCoordination::create(&segment)?;

    if !coord.wait_for_consumers(num_consumers, Duration::from_secs(30)) {
        return Err(format!("timeout waiting for {num_consumers} consumers").into());
    }

    producer.discover_consumers(Duration::from_secs(3));

    let payload = vec![42u8; payload_size];

    let start = Instant::now();
    for i in 0..num_messages {
        producer.publish(&payload, (i % 256) as u8);
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} msgs/sec", num_messages as f64 / elapsed.as_secs_f64());
    println!("Time: {:.3} seconds", elapsed.as_secs_f64());

    coord.signal_producer_done(num_messages as i64);
    coord.wait_for_consumers_done(num_consumers, Duration::from_secs(60));
    Ok(())
}

// ============================================================
// Consumer child (configurable)
// ============================================================

fn consumer_process() -> Result<(), Box<dyn std::error::Error>> {
    let segment = get_segment_name();
    let buffer_depth = read_env_usize("BENCH_BUFFER_DEPTH", 1024);
    let num_messages = read_env_u64("BENCH_NUM_MESSAGES", 50_000);

    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;

    let mut consumer =
        FramedTransportConsumer::<Frame>::attach(&segment, buffer_depth, MyelonWaitStrategy::BusySpin)?;

    coord.signal_consumer_ready();

    let start = Instant::now();
    let mut consumed = 0u64;

    // Consume exactly num_messages — avoids deadlock from calling
    // recv_message_blocking after all messages are consumed.
    while consumed < num_messages {
        let (_kind, _data) = consumer.recv_message_blocking();
        consumed += 1;
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} msgs/sec", consumed as f64 / elapsed.as_secs_f64());
    println!("Events: {}", consumed);

    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

// ============================================================
// Orchestrator
// ============================================================

fn unique_segment(label: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let name = format!("fr_{}_{}_{}", label, std::process::id() % 10000, ts % 100000);
    disruptor_mp::portable_shm_segment_name(&name)
}

fn run_benchmark(
    label: &str,
    payload_size: usize,
    num_messages: u64,
    buffer_depth: usize,
    num_consumers: usize,
) -> BenchResult {
    let segment = unique_segment(label);
    let exe = env::current_exe().expect("current_exe");

    let env_common: Vec<(&str, String)> = vec![
        ("BENCH_PAYLOAD_SIZE", payload_size.to_string()),
        ("BENCH_NUM_MESSAGES", num_messages.to_string()),
        ("BENCH_BUFFER_DEPTH", buffer_depth.to_string()),
        ("BENCH_NUM_CONSUMERS", num_consumers.to_string()),
    ];

    let producer = spawn_child_with_env(&exe, "framed_producer", &segment, &env_common);
    let consumers: Vec<Child> = (0..num_consumers)
        .map(|_| spawn_child_with_env(&exe, "framed_consumer", &segment, &env_common))
        .collect();

    let timeout = Duration::from_secs(180);
    let consumer_outputs: Vec<_> = consumers
        .into_iter()
        .map(|c| wait_with_output_timeout(c, timeout))
        .collect();
    let prod_result = wait_with_output_timeout(producer, timeout);

    let prod_out = match prod_result {
        Ok(o) => {
            if !o.stderr.is_empty() { eprintln!("[{label} prod stderr] {}", String::from_utf8_lossy(&o.stderr)); }
            String::from_utf8_lossy(&o.stdout).to_string()
        }
        Err(e) => { eprintln!("[{label} prod] {e}"); String::new() }
    };

    let prod_tp = extract_value(&prod_out, "Throughput");

    let mut total_cons_tp = 0.0;
    for (i, result) in consumer_outputs.into_iter().enumerate() {
        match result {
            Ok(o) => {
                if !o.stderr.is_empty() { eprintln!("[{label} cons{i} stderr] {}", String::from_utf8_lossy(&o.stderr)); }
                let stdout = String::from_utf8_lossy(&o.stdout).to_string();
                total_cons_tp += extract_value(&stdout, "Throughput");
            }
            Err(e) => eprintln!("[{label} cons{i}] {e}"),
        }
    }
    let avg_cons_tp = if num_consumers > 0 { total_cons_tp / num_consumers as f64 } else { 0.0 };

    let cons_label = if num_consumers == 1 { "consumer" } else { "avg cons" };
    println!(
        "  {:<30} producer: {:>10} msgs/s  {}: {:>10} msgs/s",
        label, format_throughput(prod_tp), cons_label, format_throughput(avg_cons_tp),
    );

    reporting::make_result(
        "framed_shm", label, "shm", "framed", None, "BusySpin",
        payload_size, buffer_depth, num_messages, 0, num_consumers,
        prod_tp, avg_cons_tp, None,
    )
}

// ============================================================
// main
// ============================================================

fn main() {
    let args: Vec<String> = env::args().collect();

    // Child dispatch
    if args.len() > 1 {
        let role = &args[1];
        if role.starts_with("--") { /* fall through */ } else {
            let _log = perf_bench::bench_log::BenchLog::default_capacity(role);
            let result = match role.as_str() {
                "framed_producer" => producer_process(),
                "framed_consumer" => consumer_process(),
                _ => Ok(()),
            };
            if let Err(e) = result { eprintln!("{role} failed: {e}"); std::process::exit(1); }
            return;
        }
    }

    let payload_arg = args.windows(2)
        .find(|w| w[0] == "--payload")
        .map(|w| w[1].as_str())
        .unwrap_or("all");

    let json_mode = args.iter().any(|a| a == "--json");

    if !json_mode {
        println!("=== Framed Transport SHM Benchmark ===");
        println!("Frame: {}KB data capacity", FRAME_DATA_BYTES / 1024);
        println!("Buffer: 1024 frames (2048 for fragmented)");
        println!();
    }

    // Scenarios from RFC 0013 Section 4.2
    struct Scenario {
        label: &'static str,
        payload: usize,
        messages: u64,
        buffer: usize,
        consumers: usize,
        tag: &'static str,
    }

    let scenarios = [
        Scenario { label: "framed_1p1c_1KB",     payload: 1_024,   messages: 100_000, buffer: 1024, consumers: 1, tag: "1K"   },
        Scenario { label: "framed_1p1c_32KB",    payload: 32_768,  messages: 50_000,  buffer: 1024, consumers: 1, tag: "32K"  },
        Scenario { label: "framed_1p1c_64KB",    payload: 65_524,  messages: 50_000,  buffer: 1024, consumers: 1, tag: "64K"  },
        Scenario { label: "framed_1p1c_128KB",   payload: 131_072, messages: 10_000,  buffer: 2048, consumers: 1, tag: "128K" },
        Scenario { label: "framed_1p3c_32KB",    payload: 32_768,  messages: 50_000,  buffer: 1024, consumers: 3, tag: "32K_3c" },
    ];

    let mut report = BenchReport::new();
    for s in &scenarios {
        if payload_arg == "all" || payload_arg == s.tag {
            report.add(run_benchmark(s.label, s.payload, s.messages, s.buffer, s.consumers));
        }
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
