//! Raw disruptor-mp ring benchmark over SHM backend.
//!
//! Directly follows the proven pattern from disruptor-mp/benches/ipc/ipc_shm.rs:
//! - Orchestrator spawns producer + consumer(s) as child processes
//! - Children detect role via first CLI arg (not --bench)
//! - Segment name passed via BENCHMARK_SEGMENT_NAME env var
//! - Producer creates ring then coordination; consumer attaches with retry
//! - Consumer uses process_available() (non-blocking batch), not consume_next()
//!
//! Run:   cargo bench -p myelon-bench --bench raw_ring_shm
//! Quick: cargo bench -p myelon-bench --bench raw_ring_shm -- --quick

use disruptor_mp::{
    build_shared_single_producer, CoordinationMode, SharedDisruptorBuilder, SharedMemoryConfig,
};
use myelon_bench::coordination::BenchmarkCoordination;
use myelon_bench::events::{data_rate_mbps, format_throughput, BenchEvent};
use myelon_bench::reporting::{BenchReport, BenchResult};
use std::env;
use std::io::Read as _;
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

// --- Constants ---

const BUFFER_SIZE: usize = 1024;
const NUM_EVENTS: u64 = 100_000;
const WARMUP_EVENTS: u64 = 1_000;

type Event = BenchEvent<128>; // 144 bytes (8+8+128)

// --- Helpers (copied from disruptor-mp benches common.rs) ---

fn get_segment_name(label: &str) -> String {
    if let Ok(name) = env::var("BENCHMARK_SEGMENT_NAME") {
        return name;
    }
    disruptor_mp::portable_shm_segment_name(label)
}

fn spawn_child(
    exe: &std::path::Path,
    role: &str,
    segment_name: &str,
) -> Child {
    Command::new(exe)
        .arg(role)
        .env("BENCHMARK_SEGMENT_NAME", segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {role}: {e}"))
}

fn wait_with_output_timeout(
    mut child: Child,
    timeout: Duration,
) -> Result<Output, String> {
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

// ============================================================
// Producer child process
// ============================================================

fn producer_process() -> Result<(), Box<dyn std::error::Error>> {
    let segment_name = get_segment_name("myelon_bench");

    // Step 1: Create ring buffer
    let mut producer = build_shared_single_producer::<Event>(&segment_name, BUFFER_SIZE)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(Event::default)?;

    // Step 2: Create coordination AFTER ring exists
    let coordination = BenchmarkCoordination::create(&segment_name)?;

    // Step 3: Wait for consumer(s) to signal ready
    if !coordination.wait_for_consumers(1, Duration::from_secs(30)) {
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
    for i in 0..NUM_EVENTS {
        producer.publish(|slot| {
            slot.sequence = WARMUP_EVENTS + i;
            slot.timestamp_ns = 0;
        });
    }
    let elapsed = start.elapsed();

    let throughput = NUM_EVENTS as f64 / elapsed.as_secs_f64();
    println!("Throughput: {:.0} events/sec", throughput);
    println!("Time: {:.3} seconds", elapsed.as_secs_f64());

    // Signal done
    coordination.signal_producer_done(NUM_EVENTS as i64);
    coordination.wait_for_consumers_done(1, Duration::from_secs(30));

    Ok(())
}

// ============================================================
// Consumer child process
// ============================================================

fn consumer_process() -> Result<(), Box<dyn std::error::Error>> {
    let segment_name = get_segment_name("myelon_bench");

    // Step 1: Attach coordination with retry
    let coordination =
        BenchmarkCoordination::attach_with_timeout(&segment_name, Duration::from_secs(30))?;

    // Step 2: Attach to ring buffer
    let config = SharedMemoryConfig {
        name: segment_name.clone(),
        buffer_size: BUFFER_SIZE,
        element_size: std::mem::size_of::<Event>(),
        create: false,
    };
    let builder: SharedDisruptorBuilder<Event> = SharedDisruptorBuilder::new(config);
    let mut consumer = builder.build_consumer()?;

    // Step 3: Signal ready
    coordination.signal_consumer_ready();

    // Warmup
    let mut warmup_count = 0u64;
    while warmup_count < WARMUP_EVENTS {
        let processed = consumer.process_available(|_event, _seq| {
            warmup_count += 1;
        });
        if processed == 0 {
            std::hint::spin_loop();
        }
    }

    // Measured
    let start = Instant::now();
    let mut events_consumed = 0u64;

    loop {
        let processed = consumer.process_available(|_event, _seq| {
            events_consumed += 1;
        });

        if coordination.is_producer_done() {
            let expected = coordination.events_produced() as u64;
            if events_consumed >= expected {
                break;
            }
        }

        if processed == 0 {
            std::hint::spin_loop();
        }
    }

    let elapsed = start.elapsed();
    let throughput = events_consumed as f64 / elapsed.as_secs_f64();

    println!("Throughput: {:.0} events/sec", throughput);
    println!("Time: {:.3} seconds", elapsed.as_secs_f64());
    println!("Events: {}", events_consumed);

    coordination.signal_consumer_done(events_consumed as i64);

    Ok(())
}

// ============================================================
// Orchestrator
// ============================================================

fn run_benchmark(quick: bool) -> BenchResult {
    let segment_name = get_segment_name("myelon_bench");
    let exe = env::current_exe().expect("current_exe");

    let producer_child = spawn_child(&exe, "mb_producer", &segment_name);
    let consumer_child = spawn_child(&exe, "mb_consumer", &segment_name);

    let timeout = Duration::from_secs(120);
    // Wait for producer first — it finishes before consumers.
    // Consumer waits for producer_done signal, so producer exits first.
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

    println!(
        "  1p1c_{}B  producer: {} ops/s  consumer: {} ops/s",
        payload_bytes,
        format_throughput(prod_throughput),
        format_throughput(cons_throughput),
    );

    BenchResult {
        scenario: format!("1p1c_{}B", payload_bytes),
        backend: "shm".to_string(),
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
// main — role dispatch (matches ipc_shm.rs pattern)
// ============================================================

fn main() {
    let args: Vec<String> = env::args().collect();

    // Child process dispatch via first arg
    if args.len() > 1 {
        let role = &args[1];
        if role == "--bench" {
            // cargo bench mode — fall through to orchestrator
        } else {
            match role.as_str() {
                "mb_producer" => {
                    if let Err(e) = producer_process() {
                        eprintln!("Producer failed: {e}");
                        std::process::exit(1);
                    }
                    return;
                }
                "mb_consumer" => {
                    if let Err(e) = consumer_process() {
                        eprintln!("Consumer failed: {e}");
                        std::process::exit(1);
                    }
                    return;
                }
                _ => {} // unknown arg — fall through to orchestrator
            }
        }
    }

    // Orchestrator
    let quick = args.iter().any(|a| a == "--quick");

    println!("=== Raw Ring SHM Benchmark ===");
    println!("Backend: POSIX shared memory");
    println!("Event size: {} bytes", std::mem::size_of::<Event>());
    println!("Buffer: {} slots", BUFFER_SIZE);
    println!("Events: {}", NUM_EVENTS);
    println!();

    let mut report = BenchReport::new();
    report.add(run_benchmark(quick));
    report.print_summary();

    if let Some(path) = env::var("MYELON_BENCH_JSON_OUT").ok() {
        report.write_json(&path).expect("write JSON");
        println!("JSON: {path}");
    }
}
