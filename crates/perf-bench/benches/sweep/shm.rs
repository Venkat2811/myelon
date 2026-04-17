//! Monster Sweep Benchmark — SHM backend.
//!
//! Sweeps payload size from signal (16B touch) through 1MB (full fill) across
//! the raw disruptor ring with 1, 3, and 12 consumers.
//!
//! Three classes:
//!   signal: 64B slot, 16B data, no payload fill — disruptor signaling ceiling (~200M ops/s)
//!   data:   64B-1MB slots, full payload fill + consumer checksum — real bandwidth
//!   multi:  data class with 3 or 12 consumers
//!
//! Run:   cargo bench -p myelon-bench --bench monster_sweep_shm
//! Quick: cargo bench -p myelon-bench --bench monster_sweep_shm -- --quick
//! Size:  cargo bench -p myelon-bench --bench monster_sweep_shm -- --size 64K

use disruptor_mp::{
    build_shared_single_producer, CoordinationMode, SharedDisruptorBuilder, SharedMemoryConfig,
};
use perf_bench::coordination::BenchmarkCoordination;
use perf_bench::events::{format_throughput, nanos_now, BenchEvent};
use perf_bench::latency::{self, LatencyRecorder};
use perf_bench::reporting::{self, BenchReport, BenchResult};
use std::env;
use std::hint::black_box;
use std::io::Read as _;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

// ============================================================
// Event types
// ============================================================

/// Signal event: 64B cache-line-aligned, only 16B of data written.
/// This is the disruptor signaling ceiling — same as raw_ring_shm signal class.
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct SignalEvent {
    sequence: u64,
    data: u64,
}
impl Default for SignalEvent {
    fn default() -> Self { Self { sequence: 0, data: 0 } }
}

type Ev64 = BenchEvent<48>;
type Ev512 = BenchEvent<496>;
type Ev1K = BenchEvent<1008>;
type Ev4K = BenchEvent<4080>;
type Ev16K = BenchEvent<{ 16 * 1024 - 16 }>;
type Ev64K = BenchEvent<{ 64 * 1024 - 16 }>;
type Ev256K = BenchEvent<{ 256 * 1024 - 16 }>;
type Ev1M = BenchEvent<{ 1024 * 1024 - 16 }>;

// ============================================================
// Helpers
// ============================================================

fn read_env_usize(key: &str, default: usize) -> usize {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn read_env_u64(key: &str, default: u64) -> u64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn unique_segment(label: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    disruptor_mp::portable_shm_segment_name(&format!(
        "sw_{}_{}_{}", label, std::process::id() % 10000, ts % 100000
    ))
}

fn spawn_child(exe: &std::path::Path, role: &str, segment: &str, buffer: usize, events: u64, consumers: usize, target_rate: u64) -> Child {
    let mut cmd = Command::new(exe);
    cmd.arg(role)
        .env("SWEEP_SEGMENT", segment)
        .env("SWEEP_BUFFER", buffer.to_string())
        .env("SWEEP_EVENTS", events.to_string())
        .env("SWEEP_CONSUMERS", consumers.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if target_rate > 0 {
        cmd.env("SWEEP_TARGET_RATE", target_rate.to_string());
    }
    cmd.spawn().unwrap_or_else(|e| panic!("spawn {role}: {e}"))
}

fn wait_timeout(mut child: Child, timeout: Duration) -> Result<Output, String> {
    fn collect(child: &mut Child, status: std::process::ExitStatus) -> Output {
        let mut out = Vec::new();
        let mut err = Vec::new();
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

fn extract_latency_json(output: &str) -> Option<perf_bench::latency::LatencyStats> {
    for line in output.lines() {
        if let Some(json) = line.strip_prefix("LatencyJSON: ") { return serde_json::from_str(json).ok(); }
    }
    None
}

// ============================================================
// Signal class — disruptor signaling ceiling (16B write, no payload)
// ============================================================

fn signal_producer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = env::var("SWEEP_SEGMENT").expect("SWEEP_SEGMENT");
    let buffer = read_env_usize("SWEEP_BUFFER", 65536);
    let events = read_env_u64("SWEEP_EVENTS", 10_000_000);
    let warmup = 100_000u64;

    let mut producer = build_shared_single_producer::<SignalEvent>(&segment, buffer)
        .enable_discovery(1)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(|| SignalEvent::default())?;

    let coord = BenchmarkCoordination::create(&segment)?;
    if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
        return Err("timeout waiting for consumer".into());
    }
    for _ in 0..20 { let _ = producer.min_gating_sequence(); std::thread::sleep(Duration::from_millis(2)); }

    for i in 0..warmup {
        producer.publish(|s| { s.sequence = i; s.data = i.wrapping_mul(0x9E3779B97F4A7C15); });
    }

    let start = Instant::now();
    for i in 0..events {
        producer.publish(|s| {
            s.sequence = warmup + i;
            s.data = (warmup + i).wrapping_mul(0x9E3779B97F4A7C15);
        });
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} events/sec", events as f64 / elapsed.as_secs_f64());
    coord.signal_producer_done(events as i64);
    coord.wait_for_consumers_done(1, Duration::from_secs(60));
    Ok(())
}

fn signal_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = env::var("SWEEP_SEGMENT").expect("SWEEP_SEGMENT");
    let buffer = read_env_usize("SWEEP_BUFFER", 65536);
    let events = read_env_u64("SWEEP_EVENTS", 10_000_000);
    let warmup = 100_000u64;

    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
    let config = SharedMemoryConfig {
        name: segment, buffer_size: buffer,
        element_size: std::mem::size_of::<SignalEvent>(), create: false,
    };
    let mut consumer = SharedDisruptorBuilder::<SignalEvent>::new(config).build_consumer()?;
    coord.signal_consumer_ready();

    let mut wc = 0u64;
    while wc < warmup { consumer.process_available(|_e, _s| { wc += 1; }); if wc < warmup { std::hint::spin_loop(); } }

    let start = Instant::now();
    let mut consumed = 0u64;
    while consumed < events {
        consumer.process_available(|_e, _s| { consumed += 1; });
        if consumed < events { std::hint::spin_loop(); }
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} events/sec", consumed as f64 / elapsed.as_secs_f64());
    println!("Events: {}", consumed);
    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

// ============================================================
// Data class macro — full payload fill + consumer checksum, N consumers
// ============================================================

macro_rules! sweep_impl {
    ($ev_type:ty, $prod_fn:ident, $cons_fn:ident) => {
        fn $prod_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = env::var("SWEEP_SEGMENT").expect("SWEEP_SEGMENT");
            let buffer = read_env_usize("SWEEP_BUFFER", 4096);
            let events = read_env_u64("SWEEP_EVENTS", 100_000);
            let num_consumers = read_env_usize("SWEEP_CONSUMERS", 1);
            let target_rate = read_env_u64("SWEEP_TARGET_RATE", 0); // 0 = throughput mode
            let warmup = events / 100;

            let mut producer = build_shared_single_producer::<$ev_type>(&segment, buffer)
                .enable_discovery(num_consumers)
                .with_coordination(CoordinationMode::Immediate)
                .build_producer(|| <$ev_type>::default())?;

            let coord = BenchmarkCoordination::create(&segment)?;
            if !coord.wait_for_consumers(num_consumers, Duration::from_secs(60)) {
                return Err(format!("timeout waiting for {num_consumers} consumers").into());
            }

            let scan_rounds = 20 + num_consumers * 5;
            for _ in 0..scan_rounds {
                let _ = producer.min_gating_sequence();
                std::thread::sleep(Duration::from_millis(2));
            }

            for i in 0..warmup {
                producer.publish(|slot| { slot.sequence = i; slot.timestamp_ns = 0; });
            }

            if target_rate > 0 {
                // CO-aware mode: pace at target_rate, record intended_send_time
                let interval_ns = 1_000_000_000u64 / target_rate;
                let start = Instant::now();
                let base_ns = nanos_now();
                for i in 0..events {
                    let intended_ns = base_ns + i * interval_ns;
                    // Busy-wait until intended time
                    while nanos_now() < intended_ns { std::hint::spin_loop(); }
                    producer.publish(|slot| {
                        slot.sequence = warmup + i;
                        // Store INTENDED send time, not actual — this is the CO key insight.
                        // If the system stalls and we publish late, the consumer still
                        // measures from when we SHOULD have sent, capturing the stall.
                        slot.timestamp_ns = intended_ns;
                        slot.payload.fill(((warmup + i) & 0xFF) as u8);
                    });
                }
                let elapsed = start.elapsed();
                println!("Throughput: {:.0} events/sec", events as f64 / elapsed.as_secs_f64());
                println!("TargetRate: {} ops/sec", target_rate);
            } else {
                // Throughput mode: publish as fast as possible
                let start = Instant::now();
                for i in 0..events {
                    producer.publish(|slot| {
                        slot.sequence = warmup + i;
                        slot.timestamp_ns = nanos_now();
                        slot.payload.fill(((warmup + i) & 0xFF) as u8);
                    });
                }
                let elapsed = start.elapsed();
                println!("Throughput: {:.0} events/sec", events as f64 / elapsed.as_secs_f64());
            }

            coord.signal_producer_done(events as i64);
            coord.wait_for_consumers_done(num_consumers, Duration::from_secs(120));
            Ok(())
        }

        fn $cons_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = env::var("SWEEP_SEGMENT").expect("SWEEP_SEGMENT");
            let buffer = read_env_usize("SWEEP_BUFFER", 4096);
            let events = read_env_u64("SWEEP_EVENTS", 100_000);
            let warmup = events / 100;

            let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
            let config = SharedMemoryConfig {
                name: segment, buffer_size: buffer,
                element_size: std::mem::size_of::<$ev_type>(), create: false,
            };
            let mut consumer = SharedDisruptorBuilder::<$ev_type>::new(config).build_consumer()?;
            coord.signal_consumer_ready();

            let mut wc = 0u64;
            while wc < warmup { consumer.process_available(|_e, _s| { wc += 1; }); if wc < warmup { std::hint::spin_loop(); } }

            let mut latency = LatencyRecorder::default_range();
            let start = Instant::now();
            let mut consumed = 0u64;

            while consumed < events {
                consumer.process_available(|slot, _seq| {
                    if slot.timestamp_ns > 0 {
                        latency.record_delta(slot.timestamp_ns, nanos_now());
                    }
                    black_box(slot.payload.iter().fold(0u8, |acc, &b| acc.wrapping_add(b)));
                    consumed += 1;
                });
                if consumed < events { std::hint::spin_loop(); }
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
    };
}

sweep_impl!(Ev64,   prod_64b,  cons_64b);
sweep_impl!(Ev512,  prod_512b, cons_512b);
sweep_impl!(Ev1K,   prod_1k,   cons_1k);
sweep_impl!(Ev4K,   prod_4k,   cons_4k);
sweep_impl!(Ev16K,  prod_16k,  cons_16k);
sweep_impl!(Ev64K,  prod_64k,  cons_64k);
sweep_impl!(Ev256K, prod_256k, cons_256k);
sweep_impl!(Ev1M,   prod_1m,   cons_1m);

// ============================================================
// Orchestrator
// ============================================================

struct SweepPoint {
    label: &'static str,
    size_bytes: usize,
    events: u64,
    buffer: usize,
    consumers: usize,
    target_rate: u64, // 0 = throughput mode, >0 = CO-aware at this ops/sec
    prod_role: &'static str,
    cons_role: &'static str,
    tag: &'static str,
}

fn run_sweep_point(sp: &SweepPoint) -> BenchResult {
    let segment = unique_segment(sp.tag);
    let exe = env::current_exe().expect("current_exe");

    let producer = spawn_child(&exe, sp.prod_role, &segment, sp.buffer, sp.events, sp.consumers, sp.target_rate);
    let consumers: Vec<Child> = (0..sp.consumers)
        .map(|_| spawn_child(&exe, sp.cons_role, &segment, sp.buffer, sp.events, sp.consumers, sp.target_rate))
        .collect();

    let timeout = Duration::from_secs(300);
    let consumer_outputs: Vec<_> = consumers.into_iter()
        .map(|c| wait_timeout(c, timeout))
        .collect();
    let prod_out = wait_timeout(producer, timeout);

    let prod_str = match prod_out {
        Ok(o) => {
            if !o.stderr.is_empty() { eprintln!("[{} prod stderr] {}", sp.label, String::from_utf8_lossy(&o.stderr)); }
            String::from_utf8_lossy(&o.stdout).to_string()
        }
        Err(e) => { eprintln!("[{} prod] {e}", sp.label); String::new() }
    };

    let prod_tp = extract_value(&prod_str, "Throughput");
    let mut total_cons_tp = 0.0;
    let mut latency: Option<perf_bench::latency::LatencyStats> = None;
    for (i, result) in consumer_outputs.into_iter().enumerate() {
        match result {
            Ok(o) => {
                if !o.stderr.is_empty() { eprintln!("[{} cons{i} stderr] {}", sp.label, String::from_utf8_lossy(&o.stderr)); }
                let stdout = String::from_utf8_lossy(&o.stdout).to_string();
                total_cons_tp += extract_value(&stdout, "Throughput");
                if let Some(l) = extract_latency_json(&stdout) { latency = Some(l); }
            }
            Err(e) => eprintln!("[{} cons{i}] {e}", sp.label),
        }
    }
    let avg_cons_tp = if sp.consumers > 0 { total_cons_tp / sp.consumers as f64 } else { 0.0 };

    let ring_mb = (sp.size_bytes as u64 * sp.buffer as u64) / (1024 * 1024);
    let bw_gbs = avg_cons_tp * sp.size_bytes as f64 / 1e9;
    let lat_str = latency.as_ref().map(|l| l.summary()).unwrap_or_else(|| "-".to_string());
    let cons_label = if sp.consumers == 1 { "cons" } else { "avg_c" };
    let mode_str = if sp.target_rate > 0 { format!("CO@{}K", sp.target_rate / 1000) } else { "tput".to_string() };

    println!(
        "  {:<12} {:>5} 1p{:<2}c  ring={:>5}MB  {:>8} events  prod: {:>10}  {}: {:>10}  {:>6.1} GB/s  {}",
        sp.label, mode_str, sp.consumers, ring_mb, sp.events,
        format_throughput(prod_tp), cons_label, format_throughput(avg_cons_tp), bw_gbs, lat_str,
    );

    let mut result = reporting::make_result(
        "monster_sweep_shm", &format!("sweep_{}_{}", sp.tag, sp.consumers), "shm", "raw_ring",
        None, "BusySpin", sp.size_bytes, sp.buffer, sp.events, sp.events / 100, sp.consumers,
        prod_tp, avg_cons_tp, latency,
    );
    if sp.target_rate > 0 {
        result.measurement_mode = format!("co_aware@{}", sp.target_rate);
    }
    result
}

// ============================================================
// World-class display
// ============================================================

const HW_BW_GBS: f64 = 300.0; // M3 Max 14-core memory bandwidth

fn print_sweep_report(report: &BenchReport) {
    use tabled::{Table, Tabled, settings::Style};

    println!();
    println!("{}", "=".repeat(100));
    println!("        DISRUPTOR-MP MONSTER SWEEP -- SHM BACKEND");
    println!("{}", "=".repeat(100));
    println!();
    println!("System: {} | {}", report.metadata.cpu, report.metadata.platform);
    println!("Memory: 96GB unified | {} GB/s bandwidth", HW_BW_GBS as u64);
    if let Some(ref c) = report.metadata.git_commit { println!("Git:    {} (perf_bench)", c); }
    println!("Time:   {}", report.metadata.timestamp);
    println!("Config: Full payload fill (producer) + checksum (consumer)");
    println!();

    // --- Throughput 1p1c ---
    {
        #[derive(Tabled)]
        struct Row {
            #[tabled(rename = "Payload")]
            payload: String,
            #[tabled(rename = "Total\nMemory")]
            total_mem: String,
            #[tabled(rename = "Events")]
            events: String,
            #[tabled(rename = "Producer\n(ops/s)")]
            prod: String,
            #[tabled(rename = "Consumer\n(ops/s)")]
            cons: String,
            #[tabled(rename = "Data Rate\n(MB/s)")]
            data_rate: String,
            #[tabled(rename = "Bandwidth\n(GB/s)")]
            bw: String,
            #[tabled(rename = "% of HW\nLimit")]
            pct: String,
            #[tabled(rename = "P50")]
            p50: String,
            #[tabled(rename = "P99")]
            p99: String,
            #[tabled(rename = " ")]
            status: String,
        }

        let rows: Vec<Row> = report.results.iter()
            .filter(|r| r.config.num_consumers == 1 && !r.measurement_mode.starts_with("co_aware"))
            .map(|r| {
                let sz = r.config.message_size_bytes;
                let ring_bytes = sz as u64 * r.config.buffer_depth as u64;
                let ring_label = if ring_bytes >= 1024 * 1024 * 1024 { format!("{}GB", ring_bytes / (1024*1024*1024)) }
                    else { format!("{}MB", ring_bytes / (1024*1024)) };
                let bw = r.results.consumer_throughput_ops_sec * sz as f64 / 1e9;
                let data_rate_mbs = r.results.consumer_throughput_ops_sec * sz as f64 / 1e6;
                let pct = bw / HW_BW_GBS * 100.0;
                let is_signal = r.scenario.contains("SIG");
                let status = if is_signal || pct > 10.0 { "✓".to_string() } else if pct > 1.0 { "△".to_string() } else { "✗".to_string() };
                Row {
                    payload: if is_signal { "signal".to_string() } else { format_size(sz) },
                    total_mem: ring_label,
                    events: format_events(r.config.num_messages),
                    prod: format_throughput(r.results.producer_throughput_ops_sec),
                    cons: format_throughput(r.results.consumer_throughput_ops_sec),
                    data_rate: format!("{:.0}", data_rate_mbs),
                    bw: format!("{:.1}", bw),
                    pct: if is_signal { "seq-ctr".into() } else { format!("{:.1}%", pct) },
                    p50: r.latency.as_ref().map(|l| latency::format_ns(l.p50_ns)).unwrap_or("-".into()),
                    p99: r.latency.as_ref().map(|l| latency::format_ns(l.p99_ns)).unwrap_or("-".into()),
                    status,
                }
            }).collect();

        if !rows.is_empty() {
            println!("{}", "-".repeat(100));
            println!("  THROUGHPUT SWEEP (1p1c)");
            println!("{}", "-".repeat(100));
            println!("{}", Table::new(rows).with(Style::modern()));
            println!("Legend: ✓ = >10% BW efficiency | △ = >1% | ✗ = <1%");
            println!();
        }
    }

    // --- Consumer Scaling ---
    {
        #[derive(Tabled)]
        struct Row {
            #[tabled(rename = "Payload")]
            payload: String,
            #[tabled(rename = "1p1c")]
            c1: String,
            #[tabled(rename = "1p2c")]
            c2: String,
            #[tabled(rename = "1p4c")]
            c4: String,
            #[tabled(rename = "1p6c")]
            c6: String,
            #[tabled(rename = "1p8c")]
            c8: String,
            #[tabled(rename = "1p10c")]
            c10: String,
            #[tabled(rename = "1p12c")]
            c12: String,
        }

        let sizes: Vec<usize> = report.results.iter()
            .filter(|r| !r.measurement_mode.starts_with("co_aware") && !r.scenario.contains("SIG"))
            .map(|r| r.config.message_size_bytes)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter().collect();

        let mut rows = Vec::new();
        for sz in &sizes {
            let find = |nc: usize| -> Option<f64> {
                report.results.iter()
                    .find(|r| r.config.message_size_bytes == *sz && r.config.num_consumers == nc && !r.measurement_mode.starts_with("co_aware"))
                    .map(|r| r.results.consumer_throughput_ops_sec)
            };
            let c1 = find(1);
            let has_multi = [2,4,6,8,10,12].iter().any(|&nc| find(nc).is_some());
            if has_multi {
                let c1v = c1.unwrap_or(1.0);
                let fmt = |nc: usize| -> String {
                    find(nc).map(|v| format!("{} ({:.0}%)", format_throughput(v), v / c1v * 100.0)).unwrap_or("-".into())
                };
                rows.push(Row {
                    payload: format_size(*sz),
                    c1: c1.map(|v| format_throughput(v)).unwrap_or("-".into()),
                    c2: fmt(2), c4: fmt(4), c6: fmt(6), c8: fmt(8), c10: fmt(10), c12: fmt(12),
                });
            }
        }

        if !rows.is_empty() {
            println!("{}", "-".repeat(100));
            println!("  CONSUMER SCALING");
            println!("{}", "-".repeat(100));
            println!("{}", Table::new(rows).with(Style::modern()));
            println!();
        }
    }

    // --- CO Latency Matrix (spanned columns) ---
    if let Some(matrix) = reporting::build_co_matrix(&report.results, false) {
        println!("{}", "-".repeat(100));
        println!("  COORDINATED OMISSION LATENCY MATRIX");
        println!("{}", "-".repeat(100));
        println!("{matrix}");
        println!("Legend: ✓ P99 CO < 1us (Excellent) | △ P99 CO < 10ms (Good) | ✗ P99 CO >= 10ms (Saturated)");
        println!("All latencies are Coordinated Omission corrected, showing true user-experienced delays");
        println!();
    }

    // --- Key Findings ---
    {
        println!("{}", "=".repeat(100));
        println!("  KEY FINDINGS");
        println!("{}", "=".repeat(100));

        let signal = report.results.iter().find(|r| r.scenario.contains("SIG"));
        if let Some(s) = signal {
            println!("  Signal ceiling:    {} ops/s (sequence-counter bound)", format_throughput(s.results.consumer_throughput_ops_sec));
        }

        // Peak bandwidth
        let peak_bw = report.results.iter()
            .filter(|r| r.config.num_consumers == 1 && r.measurement_mode == "max_throughput")
            .map(|r| (r.results.consumer_throughput_ops_sec * r.config.message_size_bytes as f64 / 1e9, r.config.message_size_bytes))
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        if let Some((bw, sz)) = peak_bw {
            println!("  Peak bandwidth:    {:.1} GB/s @ {} ({:.1}% of {} GB/s HW limit)", bw, format_size(sz), bw / HW_BW_GBS * 100.0, HW_BW_GBS as u64);
        }

        // Best CO P99
        let best_co = report.results.iter()
            .filter(|r| r.measurement_mode.starts_with("co_aware") && r.latency.is_some())
            .min_by_key(|r| r.latency.as_ref().unwrap().p99_ns);
        if let Some(co) = best_co {
            let l = co.latency.as_ref().unwrap();
            println!("  Best CO P99:       {} @ {} ({}K ops/s sustained)",
                latency::format_ns(l.p99_ns), format_size(co.config.message_size_bytes),
                co.results.producer_throughput_ops_sec as u64 / 1000);
        }

        let worst_co = report.results.iter()
            .filter(|r| r.measurement_mode.starts_with("co_aware") && r.latency.is_some())
            .max_by_key(|r| r.latency.as_ref().unwrap().p99_ns);
        if let Some(co) = worst_co {
            let l = co.latency.as_ref().unwrap();
            if l.p99_ns > 10_000_000 {
                println!("  Worst CO P99:      {} @ {} ({}K/s -- above throughput ceiling)",
                    latency::format_ns(l.p99_ns), format_size(co.config.message_size_bytes),
                    co.results.producer_throughput_ops_sec as u64 / 1000);
            }
        }

        println!("{}", "=".repeat(100));
        println!();
    }
}

fn write_sweep_markdown(report: &BenchReport, path: &str) -> std::io::Result<()> {
    use tabled::{Table, Tabled, settings::Style};

    let mut md = String::new();

    // Header
    md.push_str("# Disruptor-MP Monster Sweep — SHM Backend\n\n");
    md.push_str(&format!("- **CPU**: {}\n", report.metadata.cpu));
    md.push_str(&format!("- **Platform**: {}\n", report.metadata.platform));
    md.push_str("- **Memory**: 96GB unified, 300 GB/s bandwidth\n");
    if let Some(ref c) = report.metadata.git_commit { md.push_str(&format!("- **Git**: `{}`\n", c)); }
    md.push_str(&format!("- **Timestamp**: {}\n", report.metadata.timestamp));
    md.push_str("- **Config**: Full payload fill (producer) + checksum (consumer)\n\n");

    // --- Throughput 1p1c ---
    {
        #[derive(Tabled)]
        struct TputRow {
            #[tabled(rename = "Payload")] payload: String,
            #[tabled(rename = "Total\nMemory")] total_mem: String,
            #[tabled(rename = "Events")] events: String,
            #[tabled(rename = "Producer\n(ops/s)")] prod: String,
            #[tabled(rename = "Consumer\n(ops/s)")] cons: String,
            #[tabled(rename = "Data Rate\n(MB/s)")] data_rate: String,
            #[tabled(rename = "Bandwidth\n(GB/s)")] bw: String,
            #[tabled(rename = "% of HW\nLimit")] pct: String,
            #[tabled(rename = "P50")] p50: String,
            #[tabled(rename = "P99")] p99: String,
            #[tabled(rename = " ")] status: String,
        }

        let rows: Vec<TputRow> = report.results.iter()
            .filter(|r| r.config.num_consumers == 1 && !r.measurement_mode.starts_with("co_aware"))
            .map(|r| {
                let sz = r.config.message_size_bytes;
                let ring_bytes = sz as u64 * r.config.buffer_depth as u64;
                let ring_label = if ring_bytes >= 1024 * 1024 * 1024 { format!("{}GB", ring_bytes / (1024*1024*1024)) }
                    else { format!("{}MB", ring_bytes / (1024*1024)) };
                let bw = r.results.consumer_throughput_ops_sec * sz as f64 / 1e9;
                let data_rate_mbs = r.results.consumer_throughput_ops_sec * sz as f64 / 1e6;
                let pct = bw / HW_BW_GBS * 100.0;
                let is_signal = r.scenario.contains("SIG");
                TputRow {
                    payload: if is_signal { "signal".into() } else { format_size(sz) },
                    total_mem: ring_label,
                    events: format_events(r.config.num_messages),
                    prod: format_throughput(r.results.producer_throughput_ops_sec),
                    cons: format_throughput(r.results.consumer_throughput_ops_sec),
                    data_rate: format!("{:.0}", data_rate_mbs),
                    bw: format!("{:.1}", bw),
                    pct: if is_signal { "seq-ctr".into() } else { format!("{:.1}%", pct) },
                    p50: r.latency.as_ref().map(|l| latency::format_ns(l.p50_ns)).unwrap_or("-".into()),
                    p99: r.latency.as_ref().map(|l| latency::format_ns(l.p99_ns)).unwrap_or("-".into()),
                    status: if is_signal || pct > 10.0 { "✓".into() } else if pct > 1.0 { "△".into() } else { "✗".into() },
                }
            }).collect();

        md.push_str("## Throughput Sweep (1p1c)\n\n");
        md.push_str(&Table::new(rows).with(Style::markdown()).to_string());
        md.push_str("\n\nLegend: ✓ = >10% BW efficiency | △ = >1% | ✗ = <1%\n\n");
    }

    // --- Consumer Scaling (one tabled table per payload size) ---
    {
        #[derive(Tabled)]
        struct ScaleRow {
            #[tabled(rename = "Consumers")] consumers: String,
            #[tabled(rename = "Consumer (ops/s)")] cons: String,
            #[tabled(rename = "% of 1p1c")] pct: String,
            #[tabled(rename = "BW (GB/s)")] bw: String,
        }

        let scaling_sizes: Vec<usize> = report.results.iter()
            .filter(|r| !r.measurement_mode.starts_with("co_aware") && !r.scenario.contains("SIG") && r.config.num_consumers > 1)
            .map(|r| r.config.message_size_bytes)
            .collect::<std::collections::BTreeSet<_>>().into_iter().collect();

        if !scaling_sizes.is_empty() {
            md.push_str("## Consumer Scaling\n\n");
            for sz in &scaling_sizes {
                let find = |nc: usize| -> Option<f64> {
                    report.results.iter()
                        .find(|r| r.config.message_size_bytes == *sz && r.config.num_consumers == nc && !r.measurement_mode.starts_with("co_aware"))
                        .map(|r| r.results.consumer_throughput_ops_sec)
                };
                let c1 = find(1).unwrap_or(1.0);
                let rows: Vec<ScaleRow> = [1,2,4,6,8,10,12].iter().filter_map(|&nc| {
                    find(nc).map(|v| ScaleRow {
                        consumers: format!("1p{}c", nc),
                        cons: format_throughput(v),
                        pct: format!("{:.0}%", v / c1 * 100.0),
                        bw: format!("{:.1}", v * *sz as f64 / 1e9),
                    })
                }).collect();
                if !rows.is_empty() {
                    md.push_str(&format!("### {} payload\n\n", format_size(*sz)));
                    md.push_str(&Table::new(rows).with(Style::markdown()).to_string());
                    md.push_str("\n\n");
                }
            }
        }
    }

    // --- CO Latency Matrix ---
    if let Some(matrix) = reporting::build_co_matrix(&report.results, true) {
        md.push_str("## Coordinated Omission Latency Matrix\n\n");
        md.push_str(&matrix);
        md.push_str("\n\nLegend: ✓ P99 < 1us (Excellent) | △ P99 < 10ms (Good) | ✗ P99 >= 10ms (Saturated)\n\n");
        md.push_str("All latencies are Coordinated Omission corrected, showing true user-experienced delays.\n\n");
    }

    // --- Performance Summary ---
    md.push_str("## Performance Summary\n\n");

    if let Some(s) = report.results.iter().find(|r| r.scenario.contains("SIG")) {
        md.push_str(&format!("- **Signal ceiling**: {} ops/s (sequence-counter bound)\n", format_throughput(s.results.consumer_throughput_ops_sec)));
    }

    let peak_bw = report.results.iter()
        .filter(|r| r.config.num_consumers == 1 && !r.measurement_mode.starts_with("co_aware"))
        .map(|r| (r.results.consumer_throughput_ops_sec * r.config.message_size_bytes as f64 / 1e9, r.config.message_size_bytes))
        .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    if let Some((bw, sz)) = peak_bw {
        md.push_str(&format!("- **Peak bandwidth**: {:.1} GB/s @ {} ({:.1}% of {} GB/s HW limit)\n", bw, format_size(sz), bw / HW_BW_GBS * 100.0, HW_BW_GBS as u64));
    }

    let best_co = report.results.iter()
        .filter(|r| r.measurement_mode.starts_with("co_aware") && r.latency.is_some())
        .min_by_key(|r| r.latency.as_ref().unwrap().p99_ns);
    if let Some(co) = best_co {
        let l = co.latency.as_ref().unwrap();
        let rate = co.measurement_mode.strip_prefix("co_aware@").and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
        md.push_str(&format!("- **Best CO P99**: {} @ {} ({}K ops/s sustained)\n", latency::format_ns(l.p99_ns), format_size(co.config.message_size_bytes), rate / 1000));
    }

    let sustainable: Vec<String> = report.results.iter()
        .filter(|r| r.measurement_mode.starts_with("co_aware") && r.latency.as_ref().map(|l| l.p99_ns < 10_000_000).unwrap_or(false))
        .map(|r| {
            let rate = r.measurement_mode.strip_prefix("co_aware@").and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
            format!("{}@{}K/s", format_size(r.config.message_size_bytes), rate / 1000)
        }).collect();
    if !sustainable.is_empty() {
        md.push_str(&format!("- **Sustainable (P99 < 10ms)**: {}\n", sustainable.join(", ")));
    }

    md.push_str("\n## Legend\n\n");
    md.push_str("- ✓ = P99 CO < 1us (Excellent -- true low-latency performance)\n");
    md.push_str("- △ = P99 CO < 10ms (Good -- acceptable for most applications)\n");
    md.push_str("- ✗ = P99 CO >= 10ms (Poor -- significant queueing delays)\n");

    std::fs::write(path, md)
}

fn format_size(bytes: usize) -> String {
    if bytes >= 1_048_576 { format!("{}MB", bytes / 1_048_576) }
    else if bytes >= 1024 { format!("{}KB", bytes / 1024) }
    else { format!("{}B", bytes) }
}

fn format_events(n: u64) -> String {
    if n >= 1_000_000 { format!("{}M", n / 1_000_000) }
    else if n >= 1_000 { format!("{}K", n / 1_000) }
    else { format!("{}", n) }
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
            // Child processes get their own log
            let mut _log = perf_bench::bench_log::BenchLog::default_capacity(role);
            let result = match role.as_str() {
                "sig_prod"   => signal_producer(),
                "sig_cons"   => signal_consumer(),
                "prod_64b"   => prod_64b(),
                "cons_64b"   => cons_64b(),
                "prod_512b"  => prod_512b(),
                "cons_512b"  => cons_512b(),
                "prod_1k"    => prod_1k(),
                "cons_1k"    => cons_1k(),
                "prod_4k"    => prod_4k(),
                "cons_4k"    => cons_4k(),
                "prod_16k"   => prod_16k(),
                "cons_16k"   => cons_16k(),
                "prod_64k"   => prod_64k(),
                "cons_64k"   => cons_64k(),
                "prod_256k"  => prod_256k(),
                "cons_256k"  => cons_256k(),
                "prod_1m"    => prod_1m(),
                "cons_1m"    => cons_1m(),
                _ => Ok(()),
            };
            if let Err(e) = result { eprintln!("{role} failed: {e}"); std::process::exit(1); }
            return;
        }
    }

    // Orchestrator log — tracks full benchmark lifecycle
    let mut bench_log = perf_bench::bench_log::BenchLog::default_capacity("monster_sweep_shm");
    bench_log.event("orchestrator_start");

    let size_arg = args.windows(2)
        .find(|w| w[0] == "--size")
        .map(|w| w[1].as_str())
        .unwrap_or("all");

    let json_mode = args.iter().any(|a| a == "--json");
    let quick_mode = args.iter().any(|a| a == "--quick");

    let mode_arg = args.windows(2)
        .find(|w| w[0] == "--mode")
        .map(|w| w[1].as_str())
        .unwrap_or("all");

    let run_throughput = mode_arg == "all" || mode_arg == "throughput";
    let run_co = mode_arg == "all" || mode_arg == "co";

    // Helper: map tag to role names
    fn roles(tag: &str) -> (&'static str, &'static str) {
        match tag {
            "64B" => ("prod_64b", "cons_64b"), "512B" => ("prod_512b", "cons_512b"),
            "1K" | "1K_3c" | "1K_12c" | "1K_CO" => ("prod_1k", "cons_1k"),
            "4K" | "4K_3c" | "4K_12c" | "4K_CO" => ("prod_4k", "cons_4k"),
            "16K" | "16K_CO" => ("prod_16k", "cons_16k"),
            "64K" | "64K_3c" | "64K_12c" | "64K_CO" => ("prod_64k", "cons_64k"),
            "256K" | "256K_CO" => ("prod_256k", "cons_256k"),
            "1M" | "1M_3c" | "1M_CO" => ("prod_1m", "cons_1m"),
            _ => ("prod_64b", "cons_64b"),
        }
    }

    // Build scenario list
    let scenarios: Vec<SweepPoint> = {
        let mut v = Vec::new();

        // === THROUGHPUT MODE ===
        if run_throughput {
            // Signal class
            v.push(SweepPoint { label: "signal", size_bytes: 64, events: 10_000_000, buffer: 65_536, consumers: 1, target_rate: 0, prod_role: "sig_prod", cons_role: "sig_cons", tag: "SIG" });

            // Data 1p1c sweep
            for (label, size, events, buffer, tag) in [
                ("64B",   64usize,      10_000_000u64, 65_536usize,  "64B"),
                ("512B",  512,          5_000_000,     131_072,       "512B"),
                ("1KB",   1_024,        2_000_000,     131_072,       "1K"),
                ("4KB",   4_096,        1_000_000,     65_536,        "4K"),
                ("16KB",  16_384,       500_000,       32_768,        "16K"),
                ("64KB",  65_536,       200_000,       16_384,        "64K"),
                ("256KB", 262_144,      100_000,       8_192,         "256K"),
                ("1MB",   1_048_576,    50_000,        4_096,         "1M"),
            ] {
                let (p, c) = roles(tag);
                v.push(SweepPoint { label, size_bytes: size, events, buffer, consumers: 1, target_rate: 0, prod_role: p, cons_role: c, tag });
            }

            // Multi-consumer scaling: 1p2c, 1p4c, 1p6c, 1p8c, 1p10c, 1p12c
            // Tested at 1KB and 4KB (representative small/medium payloads)
            for nc in [2usize, 4, 6, 8, 10, 12] {
                for (size, events, buffer, base_tag) in [
                    (1_024usize, 200_000u64, 131_072usize, "1K"),
                    (4_096,      100_000,    65_536,       "4K"),
                ] {
                    let tag_str = format!("{base_tag}_{nc}c");
                    // Leak the tag string so it lives for 'static lifetime in SweepPoint
                    let tag: &'static str = Box::leak(tag_str.into_boxed_str());
                    let label_str = format!("{}x{}c", format_size(size), nc);
                    let label: &'static str = Box::leak(label_str.into_boxed_str());
                    let (p, c) = roles(base_tag);
                    v.push(SweepPoint { label, size_bytes: size, events, buffer, consumers: nc, target_rate: 0, prod_role: p, cons_role: c, tag });
                }
            }
        }

        // === CO-AWARE MODE ===
        // Fixed-rate send, measure from intended_send_time. Captures system stalls.
        // Small payloads: 100K-1.2M ops/sec. Large payloads: 10K-100K ops/sec.
        if run_co {
            // CO events: 100K per rate to get enough samples for P99.999
            for (label, size, buffer, rate, tag) in [
                ("1KB@100K",  1_024,     131_072, 100_000u64,   "1K_CO"),
                ("1KB@500K",  1_024,     131_072, 500_000,      "1K_CO"),
                ("1KB@1M",    1_024,     131_072, 1_000_000,    "1K_CO"),
                ("4KB@100K",  4_096,     65_536,  100_000,      "4K_CO"),
                ("4KB@500K",  4_096,     65_536,  500_000,      "4K_CO"),
                ("16KB@100K", 16_384,    32_768,  100_000,      "16K_CO"),
                ("64KB@50K",  65_536,    16_384,  50_000,       "64K_CO"),
                ("64KB@100K", 65_536,    16_384,  100_000,      "64K_CO"),
                ("256KB@10K", 262_144,   8_192,   10_000,       "256K_CO"),
                ("256KB@50K", 262_144,   8_192,   50_000,       "256K_CO"),
                ("1MB@10K",   1_048_576, 4_096,   10_000,       "1M_CO"),
                ("1MB@30K",   1_048_576, 4_096,   30_000,       "1M_CO"),
            ] {
                let (p, c) = roles(tag);
                let events = 100_000u64; // Fixed 100K events per CO scenario
                v.push(SweepPoint { label, size_bytes: size, events, buffer, consumers: 1, target_rate: rate, prod_role: p, cons_role: c, tag });
            }
        }

        v
    };

    if !json_mode {
        println!("=== Monster Sweep SHM Benchmark ===");
        println!("Backend: POSIX shared memory (M3 Max 14-core, 96GB, 300 GB/s BW)");
        println!("Modes: throughput (max rate) + CO-aware (fixed rate, intended_send_time)");
        println!("Signal: 64B slot, 16B data — disruptor ceiling");
        println!("Data:   full payload fill + consumer checksum — real bandwidth");
        println!("CO:     P50-P99.999 at calibrated rates (100K-1.2M small, 10K-100K large)");
        println!();
    }

    let mut report = BenchReport::new();

    for sp in &scenarios {
        let run = match size_arg {
            "all" => {
                if quick_mode {
                    // Quick: signal + 4 data + 3 consumer counts + 2 CO
                    let is_quick_tput = matches!(sp.tag, "SIG" | "64B" | "1K" | "64K" | "1M");
                    let is_quick_multi = sp.tag.starts_with("1K_") && matches!(sp.consumers, 2 | 6 | 12);
                    let is_quick_co = (sp.tag == "1K_CO" && sp.target_rate == 500_000)
                        || (sp.tag == "64K_CO" && sp.target_rate == 50_000);
                    (is_quick_tput && sp.consumers == 1) || is_quick_multi || is_quick_co
                } else {
                    true
                }
            }
            tag => sp.tag == tag,
        };
        if run {
            bench_log.event(&format!("scenario_start: {}", sp.label));
            report.add(run_sweep_point(sp));
            bench_log.event(&format!("scenario_done: {}", sp.label));
        }
    }
    bench_log.event_val("scenarios_completed", report.results.len() as u64);

    if json_mode {
        println!("{}", serde_json::to_string_pretty(&report).expect("serialize"));
    } else {
        print_sweep_report(&report);
    }

    // File output: --json-out, --csv-out, --md-out
    if let Some(path) = args.windows(2).find(|w| w[0] == "--json-out").map(|w| w[1].clone()) {
        report.write_json(&path).expect("write JSON");
        eprintln!("JSON written to {path}");
    }
    if let Some(path) = args.windows(2).find(|w| w[0] == "--csv-out").map(|w| w[1].clone()) {
        report.write_csv(&path).expect("write CSV");
        eprintln!("CSV written to {path}");
    }
    if let Some(path) = args.windows(2).find(|w| w[0] == "--md-out").map(|w| w[1].clone()) {
        write_sweep_markdown(&report, &path).expect("write markdown");
        eprintln!("Markdown written to {path}");
    }
    if let Some(path) = env::var("MYELON_BENCH_JSON_OUT").ok() {
        report.write_json(&path).expect("write JSON");
    }
}
