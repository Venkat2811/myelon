//! Wait strategy benchmark over SHM — own multiprocess implementation.
//!
//! 4 strategies x 7 consumer counts (1, 2, 4, 6, 8, 10, 12).
//! Modes: quick (1p1c BusySpin), full (28 combos).
//!
//! Run: cargo bench -p myelon-bench --bench wait_strategy_shm
//! Full: cargo bench -p myelon-bench --bench wait_strategy_shm -- full

use disruptor_mp::{build_shared_single_producer, CoordinationMode, SharedDisruptorBuilder, SharedMemoryConfig};
use perf_bench::coordination::BenchmarkCoordination;
use perf_bench::events::format_throughput;
use perf_bench::reporting::{self, BenchReport, BenchResult};
use std::env;
use std::io::Read as _;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const BUFFER_SIZE: usize = 64 * 1024;
const NUM_EVENTS: u64 = 100_000;
const ELEMENT_SIZE: usize = 128;

#[repr(C)]
#[derive(Clone, Copy)]
struct Event {
    sequence: u64,
    timestamp_ns: u64,
    payload: [u8; 112],
}

impl Default for Event {
    fn default() -> Self {
        Self { sequence: 0, timestamp_ns: 0, payload: [0u8; 112] }
    }
}

fn get_segment() -> String {
    if let Ok(name) = env::var("BENCHMARK_SEGMENT_NAME") { return name; }
    disruptor_mp::portable_shm_segment_name("wshm")
}

fn read_env_usize(key: &str, default: usize) -> usize {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn spawn_child(exe: &std::path::Path, role: &str, segment: &str, num_consumers: usize, wait_strategy: &str) -> Child {
    Command::new(exe)
        .arg(role)
        .env("BENCHMARK_SEGMENT_NAME", segment)
        .env("NUM_CONSUMERS", num_consumers.to_string())
        .env("WAIT_STRATEGY", wait_strategy)
        .stdout(Stdio::piped()).stderr(Stdio::piped())
        .spawn().unwrap_or_else(|e| panic!("spawn {role}: {e}"))
}

fn wait_timeout(mut child: Child, timeout: Duration) -> Result<Output, String> {
    fn collect(child: &mut Child, status: std::process::ExitStatus) -> Output {
        let mut out = Vec::new(); let mut err = Vec::new();
        if let Some(mut o) = child.stdout.take() { let _ = o.read_to_end(&mut out); }
        if let Some(mut e) = child.stderr.take() { let _ = e.read_to_end(&mut err); }
        Output { status, stdout: out, stderr: err }
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

fn unique_segment(strategy: &str, consumers: usize) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    disruptor_mp::portable_shm_segment_name(&format!("ws_{}_{}_{}_{}", strategy, consumers, std::process::id() % 10000, ts % 100000))
}

// ============================================================
// Producer
// ============================================================

fn producer_process() -> Result<(), Box<dyn std::error::Error>> {
    let segment = get_segment();
    let num_consumers = read_env_usize("NUM_CONSUMERS", 1);

    let mut producer = build_shared_single_producer::<Event>(&segment, BUFFER_SIZE)
        .enable_discovery(num_consumers)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(|| Event::default())?;

    let coord = BenchmarkCoordination::create(&segment)?;
    if !coord.wait_for_consumers(num_consumers, Duration::from_secs(60)) {
        return Err(format!("timeout waiting for {num_consumers} consumers").into());
    }

    // Discovery warmup
    let scans = 20 + num_consumers * 5;
    for _ in 0..scans {
        let _ = producer.min_gating_sequence();
        std::thread::sleep(Duration::from_millis(2));
    }

    let start = Instant::now();
    for i in 0..NUM_EVENTS {
        producer.publish(|e| {
            e.sequence = i;
            e.payload = [(i % 256) as u8; 112];
        });
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} events/sec", NUM_EVENTS as f64 / elapsed.as_secs_f64());
    coord.signal_producer_done(NUM_EVENTS as i64);
    coord.wait_for_consumers_done(num_consumers, Duration::from_secs(90));
    Ok(())
}

// ============================================================
// Consumer
// ============================================================

fn consumer_process() -> Result<(), Box<dyn std::error::Error>> {
    let segment = get_segment();
    let wait_strategy = env::var("WAIT_STRATEGY").unwrap_or_else(|_| "BusySpin".into());

    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
    let config = SharedMemoryConfig {
        name: segment, buffer_size: BUFFER_SIZE,
        element_size: std::mem::size_of::<Event>(), create: false,
    };
    let mut consumer = SharedDisruptorBuilder::<Event>::new(config).build_consumer()?;
    coord.signal_consumer_ready();

    let start = Instant::now();
    let mut consumed = 0u64;

    while consumed < NUM_EVENTS {
        consumer.process_available(|_e, _s| { consumed += 1; });
        if consumed < NUM_EVENTS {
            match wait_strategy.as_str() {
                "BusySpin" => {},
                "BusySpinWithSpinLoopHint" => std::hint::spin_loop(),
                "Sleep" => std::thread::sleep(Duration::from_micros(1)),
                "Block" => std::thread::sleep(Duration::from_millis(1)),
                _ => std::hint::spin_loop(),
            }
        }
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} events/sec", consumed as f64 / elapsed.as_secs_f64());
    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

// ============================================================
// Orchestrator
// ============================================================

fn run_test(num_consumers: usize, wait_strategy: &str) -> BenchResult {
    let segment = unique_segment(wait_strategy, num_consumers);
    let exe = env::current_exe().expect("current_exe");

    let producer = spawn_child(&exe, "shm_wait_producer", &segment, num_consumers, wait_strategy);
    let consumers: Vec<Child> = (0..num_consumers)
        .map(|_| spawn_child(&exe, "shm_wait_consumer", &segment, num_consumers, wait_strategy))
        .collect();

    let timeout = Duration::from_secs(180);
    let consumer_outputs: Vec<_> = consumers.into_iter()
        .map(|c| wait_timeout(c, timeout))
        .collect();
    let prod_out = wait_timeout(producer, timeout);

    let prod_str = match prod_out {
        Ok(o) => { if !o.stderr.is_empty() { eprintln!("[wait prod stderr] {}", String::from_utf8_lossy(&o.stderr)); } String::from_utf8_lossy(&o.stdout).to_string() }
        Err(e) => { eprintln!("[wait prod] {e}"); String::new() }
    };

    let prod_tp = extract_value(&prod_str, "Throughput");
    let mut total_cons_tp = 0.0;
    for (i, result) in consumer_outputs.into_iter().enumerate() {
        match result {
            Ok(o) => {
                if !o.stderr.is_empty() { eprintln!("[wait cons{i} stderr] {}", String::from_utf8_lossy(&o.stderr)); }
                total_cons_tp += extract_value(&String::from_utf8_lossy(&o.stdout), "Throughput");
            }
            Err(e) => eprintln!("[wait cons{i}] {e}"),
        }
    }
    let avg_cons_tp = if num_consumers > 0 { total_cons_tp / num_consumers as f64 } else { 0.0 };

    println!(
        "  {:<6} {:<24} producer: {:>10} ops/s  avg consumer: {:>10} ops/s",
        format!("1p{}c", num_consumers), wait_strategy,
        format_throughput(prod_tp), format_throughput(avg_cons_tp),
    );

    reporting::make_result(
        "wait_strategy_shm", &format!("1p{}c_{}", num_consumers, wait_strategy),
        "shm", "wait_strategy", None, wait_strategy,
        ELEMENT_SIZE, BUFFER_SIZE, NUM_EVENTS, 0, num_consumers,
        prod_tp, avg_cons_tp, None,
    )
}

// ============================================================
// main
// ============================================================

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 {
        let role = &args[1];
        if role.starts_with("--") { /* fall through */ } else {
            let _log = perf_bench::bench_log::BenchLog::default_capacity(role);
            let result = match role.as_str() {
                "shm_wait_producer" => producer_process(),
                "shm_wait_consumer" => consumer_process(),
                _ => Ok(()),
            };
            if let Err(e) = result { eprintln!("{role} failed: {e}"); std::process::exit(1); }
            return;
        }
    }

    let json_mode = args.iter().any(|a| a == "--json");
    let mode_owned = env::var("BENCH_MODE").ok()
        .or_else(|| args.iter().skip(1).find(|a| !a.starts_with("--")).cloned())
        .unwrap_or_else(|| "quick".into());
    let mode = mode_owned.as_str();

    let scenarios: Vec<(usize, &'static str)> = match mode {
        "quick" => vec![(1, "BusySpin")],
        "full" | "comprehensive" => {
            let counts = [1usize, 2, 4, 6, 8, 10, 12];
            let strategies = ["BusySpin", "Block", "Sleep", "BusySpinWithSpinLoopHint"];
            counts.into_iter()
                .flat_map(|c| strategies.into_iter().map(move |s| (c, s)))
                .collect()
        }
        other => { eprintln!("Unknown mode '{other}', expected quick|full"); std::process::exit(1); }
    };

    if !json_mode {
        println!("=== Wait Strategy SHM Benchmark ===");
        println!("Event size: {} bytes", ELEMENT_SIZE);
        println!("Buffer size: {} slots ({}MB)", BUFFER_SIZE, BUFFER_SIZE * ELEMENT_SIZE / (1024 * 1024));
        println!("Events per test: {}", NUM_EVENTS);
        println!("Mode: {} ({} scenarios)", mode, scenarios.len());
        println!();
    }

    let mut report = BenchReport::new();
    for (num_consumers, wait_strategy) in scenarios {
        report.add(run_test(num_consumers, wait_strategy));
    }

    if json_mode {
        println!("{}", serde_json::to_string_pretty(&report).expect("serialize"));
    } else {
        report.print_summary();
    }

    if let Some(path) = args.windows(2).find(|w| w[0] == "--json-out").map(|w| w[1].clone()) {
        report.write_json(&path).expect("write JSON");
    }
    if let Some(path) = args.windows(2).find(|w| w[0] == "--csv-out").map(|w| w[1].clone()) {
        report.write_csv(&path).expect("write CSV");
    }
    if let Some(path) = args.windows(2).find(|w| w[0] == "--md-out").map(|w| w[1].clone()) {
        report.write_markdown(&path).expect("write markdown");
    }
    if let Some(path) = env::var("MYELON_BENCH_JSON_OUT").ok() {
        report.write_json(&path).expect("write JSON");
    }
}
