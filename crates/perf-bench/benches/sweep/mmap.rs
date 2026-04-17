//! Monster Sweep Benchmark — mmap backend.
//!
//! Same sweep as monster_sweep_shm over file-backed mmap.
//! Compare results for SHM vs mmap crossover analysis.
//!
//! Run: cargo bench -p myelon-bench --bench monster_sweep_mmap
//! Quick: cargo bench -p myelon-bench --bench monster_sweep_mmap -- --quick

use disruptor_mp::{AutoWaitStrategy, MmapConsumer, MmapProducer, MmapTransportLayout};
use perf_bench::events::{format_throughput, nanos_now, BenchEvent};
use perf_bench::latency;
use perf_bench::latency::LatencyRecorder;
use perf_bench::reporting::{self, BenchReport, BenchResult};
use std::env;
use std::hint::black_box;
use std::io::Read as _;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ============================================================
// Event types — same as SHM sweep
// ============================================================

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct SignalEvent { sequence: u64, data: u64 }
impl Default for SignalEvent { fn default() -> Self { Self { sequence: 0, data: 0 } } }

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
fn unique_root(label: &str) -> PathBuf {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    env::temp_dir().join(format!("sweep_mmap_{label}_{}_{}", std::process::id(), ts))
}
fn unique_segment(label: &str) -> String {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("swm_{label}_{}_{}", std::process::id() % 10000, ts % 100000)
}

fn child_layout() -> MmapTransportLayout {
    let root = env::var("SWEEP_ROOT").expect("SWEEP_ROOT");
    let segment = env::var("SWEEP_SEGMENT").expect("SWEEP_SEGMENT");
    MmapTransportLayout::new(PathBuf::from(root), segment).expect("layout")
}

fn spawn_child(exe: &std::path::Path, role: &str, root: &str, segment: &str, events: u64, buffer: usize, target_rate: u64) -> Child {
    let mut cmd = Command::new(exe);
    cmd.arg(role)
        .env("SWEEP_ROOT", root)
        .env("SWEEP_SEGMENT", segment)
        .env("SWEEP_BUFFER", buffer.to_string())
        .env("SWEEP_EVENTS", events.to_string())
        .stdout(Stdio::piped()).stderr(Stdio::piped());
    if target_rate > 0 { cmd.env("SWEEP_TARGET_RATE", target_rate.to_string()); }
    cmd.spawn().unwrap_or_else(|e| panic!("spawn {role}: {e}"))
}

fn read_env_u64(key: &str, default: u64) -> u64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
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

fn extract_latency_json(output: &str) -> Option<perf_bench::latency::LatencyStats> {
    for line in output.lines() {
        if let Some(json) = line.strip_prefix("LatencyJSON: ") { return serde_json::from_str(json).ok(); }
    }
    None
}

// ============================================================
// Signal class
// ============================================================

fn signal_producer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer = read_env_usize("SWEEP_BUFFER", 65536);
    let events = read_env_u64("SWEEP_EVENTS", 10_000_000);
    let warmup = 100_000u64;
    layout.ensure_directories()?;
    let mut producer = MmapProducer::<SignalEvent>::create(layout, buffer, || SignalEvent::default())?;
    if !producer.wait_for_consumers_ready(1, Duration::from_secs(30)) { return Err("timeout".into()); }
    for i in 0..warmup { producer.publish(|s| { s.sequence = i; s.data = i.wrapping_mul(0x9E3779B97F4A7C15); }); }
    let start = Instant::now();
    for i in 0..events {
        producer.publish(|s| { s.sequence = warmup + i; s.data = (warmup + i).wrapping_mul(0x9E3779B97F4A7C15); });
    }
    let elapsed = start.elapsed();
    println!("Throughput: {:.0} events/sec", events as f64 / elapsed.as_secs_f64());
    let last = (warmup + events - 1) as i64;
    producer.wait_until_consumed_with_strategy(last, Duration::from_secs(60), AutoWaitStrategy::BusySpin);
    Ok(())
}

fn signal_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer = read_env_usize("SWEEP_BUFFER", 65536);
    let events = read_env_u64("SWEEP_EVENTS", 10_000_000);
    let warmup = 100_000u64;
    let cid = format!("c{}", std::process::id());
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapConsumer::<SignalEvent>::attach(layout.clone(), buffer, &cid) {
            Ok(c) => break c,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(e) => return Err(format!("attach: {e}").into()),
        }
    };
    let mut wc = 0u64;
    while wc < warmup { if consumer.try_consume_next().is_some() { wc += 1; } else { std::hint::spin_loop(); } }
    let start = Instant::now();
    let mut consumed = 0u64;
    while consumed < events { if consumer.try_consume_next().is_some() { consumed += 1; } else { std::hint::spin_loop(); } }
    let elapsed = start.elapsed();
    println!("Throughput: {:.0} events/sec", consumed as f64 / elapsed.as_secs_f64());
    println!("Events: {}", consumed);
    Ok(())
}

// ============================================================
// Data class macro — full payload fill + consumer checksum
// ============================================================

macro_rules! sweep_impl {
    ($ev_type:ty, $prod_fn:ident, $cons_fn:ident) => {
        fn $prod_fn() -> Result<(), Box<dyn std::error::Error>> {
            let layout = child_layout();
            let buffer = read_env_usize("SWEEP_BUFFER", 4096);
            let events = read_env_u64("SWEEP_EVENTS", 100_000);
            let target_rate = read_env_u64("SWEEP_TARGET_RATE", 0);
            let warmup = events / 100;
            layout.ensure_directories()?;
            let mut producer = MmapProducer::<$ev_type>::create(layout, buffer, || <$ev_type>::default())?;
            if !producer.wait_for_consumers_ready(1, Duration::from_secs(30)) { return Err("timeout".into()); }
            for i in 0..warmup { producer.publish(|slot| { slot.sequence = i; slot.timestamp_ns = 0; }); }

            if target_rate > 0 {
                let interval_ns = 1_000_000_000u64 / target_rate;
                let start = Instant::now();
                let base_ns = nanos_now();
                for i in 0..events {
                    let intended_ns = base_ns + i * interval_ns;
                    while nanos_now() < intended_ns { std::hint::spin_loop(); }
                    producer.publish(|slot| {
                        slot.sequence = warmup + i;
                        slot.timestamp_ns = intended_ns;
                        slot.payload.fill(((warmup + i) & 0xFF) as u8);
                    });
                }
                let elapsed = start.elapsed();
                println!("Throughput: {:.0} events/sec", events as f64 / elapsed.as_secs_f64());
                println!("TargetRate: {} ops/sec", target_rate);
            } else {
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

            let last = (warmup + events - 1) as i64;
            producer.wait_until_consumed_with_strategy(last, Duration::from_secs(120), AutoWaitStrategy::BusySpin);
            Ok(())
        }

        fn $cons_fn() -> Result<(), Box<dyn std::error::Error>> {
            let layout = child_layout();
            let buffer = read_env_usize("SWEEP_BUFFER", 4096);
            let events = read_env_u64("SWEEP_EVENTS", 100_000);
            let warmup = events / 100;
            let cid = format!("c{}", std::process::id());
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut consumer = loop {
                match MmapConsumer::<$ev_type>::attach(layout.clone(), buffer, &cid) {
                    Ok(c) => break c,
                    Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
                    Err(e) => return Err(format!("attach: {e}").into()),
                }
            };
            let mut wc = 0u64;
            while wc < warmup { if consumer.try_consume_next().is_some() { wc += 1; } else { std::hint::spin_loop(); } }
            let mut latency = LatencyRecorder::default_range();
            let start = Instant::now();
            let mut consumed = 0u64;
            while consumed < events {
                if let Some((_seq, event)) = consumer.try_consume_next() {
                    if event.timestamp_ns > 0 { latency.record_delta(event.timestamp_ns, nanos_now()); }
                    black_box(event.payload.iter().fold(0u8, |acc, &b| acc.wrapping_add(b)));
                    consumed += 1;
                } else { std::hint::spin_loop(); }
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
    label: &'static str, size_bytes: usize, events: u64, buffer: usize,
    target_rate: u64,
    prod_role: &'static str, cons_role: &'static str, tag: &'static str,
}

fn run_sweep_point(sp: &SweepPoint) -> BenchResult {
    let root = unique_root(sp.tag);
    let segment = unique_segment(sp.tag);
    let root_str = root.display().to_string();
    let exe = env::current_exe().expect("current_exe");

    let producer = spawn_child(&exe, sp.prod_role, &root_str, &segment, sp.events, sp.buffer, sp.target_rate);
    let consumer = spawn_child(&exe, sp.cons_role, &root_str, &segment, sp.events, sp.buffer, sp.target_rate);

    let timeout = Duration::from_secs(600);
    let cons_out = wait_timeout(consumer, timeout);
    let prod_out = wait_timeout(producer, timeout);

    let prod_str = match prod_out {
        Ok(o) => { if !o.stderr.is_empty() { eprintln!("[{} prod] {}", sp.label, String::from_utf8_lossy(&o.stderr)); } String::from_utf8_lossy(&o.stdout).to_string() }
        Err(e) => { eprintln!("[{} prod] {e}", sp.label); String::new() }
    };
    let cons_str = match cons_out {
        Ok(o) => { if !o.stderr.is_empty() { eprintln!("[{} cons] {}", sp.label, String::from_utf8_lossy(&o.stderr)); } String::from_utf8_lossy(&o.stdout).to_string() }
        Err(e) => { eprintln!("[{} cons] {e}", sp.label); String::new() }
    };

    let prod_tp = extract_value(&prod_str, "Throughput");
    let cons_tp = extract_value(&cons_str, "Throughput");
    let latency = extract_latency_json(&cons_str);
    let _ = std::fs::remove_dir_all(&root);

    let ring_mb = (sp.size_bytes as u64 * sp.buffer as u64) / (1024 * 1024);
    let bw_gbs = cons_tp * sp.size_bytes as f64 / 1e9;
    let lat_str = latency.as_ref().map(|l| l.summary()).unwrap_or_else(|| "-".to_string());
    let mode_str = if sp.target_rate > 0 { format!("CO@{}K", sp.target_rate / 1000) } else { "tput".to_string() };

    println!(
        "  {:<12} {:>5}  ring={:>5}MB  {:>8} events  prod: {:>10}  cons: {:>10}  {:>6.1} GB/s  {}",
        sp.label, mode_str, ring_mb, sp.events,
        format_throughput(prod_tp), format_throughput(cons_tp), bw_gbs, lat_str,
    );

    let mut result = reporting::make_result(
        "monster_sweep_mmap", &format!("sweep_{}", sp.tag), "mmap", "raw_ring",
        None, "BusySpin", sp.size_bytes, sp.buffer, sp.events, sp.events / 100, 1,
        prod_tp, cons_tp, latency,
    );
    if sp.target_rate > 0 {
        result.measurement_mode = format!("co_aware@{}", sp.target_rate);
    }
    result
}

fn write_sweep_markdown(report: &reporting::BenchReport, path: &str) -> std::io::Result<()> {
    use tabled::{Table, Tabled, settings::Style};

    let hw_bw: f64 = 300.0;
    let mut md = String::new();

    md.push_str("# Disruptor-MP Monster Sweep — MMAP Backend\n\n");
    md.push_str(&format!("- **CPU**: {}\n", report.metadata.cpu));
    md.push_str(&format!("- **Platform**: {}\n", report.metadata.platform));
    md.push_str("- **Memory**: 96GB unified, 300 GB/s bandwidth\n");
    if let Some(ref c) = report.metadata.git_commit { md.push_str(&format!("- **Git**: `{}`\n", c)); }
    md.push_str(&format!("- **Timestamp**: {}\n", report.metadata.timestamp));
    md.push_str("- **Config**: Full payload fill (producer) + checksum (consumer)\n\n");

    // Throughput
    {
        #[derive(Tabled)]
        struct TputRow {
            #[tabled(rename = "Payload")] payload: String,
            #[tabled(rename = "Total Memory")] total_mem: String,
            #[tabled(rename = "Events")] events: String,
            #[tabled(rename = "Producer (ops/s)")] prod: String,
            #[tabled(rename = "Consumer (ops/s)")] cons: String,
            #[tabled(rename = "Data Rate (MB/s)")] data_rate: String,
            #[tabled(rename = "BW (GB/s)")] bw: String,
            #[tabled(rename = "% HW Limit")] pct: String,
            #[tabled(rename = "P50")] p50: String,
            #[tabled(rename = "P99")] p99: String,
            #[tabled(rename = " ")] status: String,
        }
        let rows: Vec<TputRow> = report.results.iter()
            .filter(|r| !r.measurement_mode.starts_with("co_aware"))
            .map(|r| {
                let sz = r.config.message_size_bytes;
                let rb = sz as u64 * r.config.buffer_depth as u64;
                let rl = if rb >= 1024*1024*1024 { format!("{}GB", rb/(1024*1024*1024)) } else { format!("{}MB", rb/(1024*1024)) };
                let bw = r.results.consumer_throughput_ops_sec * sz as f64 / 1e9;
                let dr = r.results.consumer_throughput_ops_sec * sz as f64 / 1e6;
                let pct = bw / hw_bw * 100.0;
                let is_sig = r.scenario.contains("SIG");
                TputRow {
                    payload: if is_sig { "signal".into() } else { format_size(sz) },
                    total_mem: rl, events: format_events(r.config.num_messages),
                    prod: format_throughput(r.results.producer_throughput_ops_sec),
                    cons: format_throughput(r.results.consumer_throughput_ops_sec),
                    data_rate: format!("{:.0}", dr), bw: format!("{:.1}", bw),
                    pct: if is_sig { "seq-ctr".into() } else { format!("{:.1}%", pct) },
                    p50: r.latency.as_ref().map(|l| latency::format_ns(l.p50_ns)).unwrap_or("-".into()),
                    p99: r.latency.as_ref().map(|l| latency::format_ns(l.p99_ns)).unwrap_or("-".into()),
                    status: if is_sig || pct > 10.0 { "✓".into() } else if pct > 1.0 { "△".into() } else { "✗".into() },
                }
            }).collect();
        md.push_str("## Throughput Sweep (1p1c)\n\n");
        md.push_str(&Table::new(rows).with(Style::markdown()).to_string());
        md.push_str("\n\nLegend: ✓ = >10% BW efficiency | △ = >1% | ✗ = <1%\n\n");
    }

    // CO Latency Matrix
    if let Some(matrix) = reporting::build_co_matrix(&report.results, true) {
        md.push_str("## Coordinated Omission Latency Matrix\n\n");
        md.push_str(&matrix);
        md.push_str("\n\nLegend: ✓ P99 < 1us (Excellent) | △ P99 < 10ms (Good) | ✗ P99 >= 10ms (Saturated)\n\n");
    }

    // Summary
    md.push_str("## Performance Summary\n\n");
    if let Some(s) = report.results.iter().find(|r| r.scenario.contains("SIG")) {
        md.push_str(&format!("- **Signal ceiling**: {} ops/s\n", format_throughput(s.results.consumer_throughput_ops_sec)));
    }
    let peak = report.results.iter()
        .filter(|r| !r.measurement_mode.starts_with("co_aware"))
        .map(|r| (r.results.consumer_throughput_ops_sec * r.config.message_size_bytes as f64 / 1e9, r.config.message_size_bytes))
        .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    if let Some((bw, sz)) = peak {
        md.push_str(&format!("- **Peak bandwidth**: {:.1} GB/s @ {} ({:.1}% of {} GB/s HW limit)\n", bw, format_size(sz), bw/hw_bw*100.0, hw_bw as u64));
    }
    let best_co = report.results.iter()
        .filter(|r| r.measurement_mode.starts_with("co_aware") && r.latency.is_some())
        .min_by_key(|r| r.latency.as_ref().unwrap().p99_ns);
    if let Some(co) = best_co {
        let l = co.latency.as_ref().unwrap();
        let rate = co.measurement_mode.strip_prefix("co_aware@").and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
        md.push_str(&format!("- **Best CO P99**: {} @ {} ({}K ops/s)\n", latency::format_ns(l.p99_ns), format_size(co.config.message_size_bytes), rate/1000));
    }
    md.push_str("\n## Legend\n\n");
    md.push_str("- ✓ = P99 < 1us (Excellent)\n- △ = P99 < 10ms (Good)\n- ✗ = P99 >= 10ms (Saturated)\n");

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

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 {
        let role = &args[1];
        if role.starts_with("--") { /* fall through */ } else {
            let _log = perf_bench::bench_log::BenchLog::default_capacity(role);
            let result = match role.as_str() {
                "sig_prod"   => signal_producer(),
                "sig_cons"   => signal_consumer(),
                "prod_64b"   => prod_64b(),  "cons_64b"   => cons_64b(),
                "prod_512b"  => prod_512b(), "cons_512b"  => cons_512b(),
                "prod_1k"    => prod_1k(),   "cons_1k"    => cons_1k(),
                "prod_4k"    => prod_4k(),   "cons_4k"    => cons_4k(),
                "prod_16k"   => prod_16k(),  "cons_16k"   => cons_16k(),
                "prod_64k"   => prod_64k(),  "cons_64k"   => cons_64k(),
                "prod_256k"  => prod_256k(), "cons_256k"  => cons_256k(),
                "prod_1m"    => prod_1m(),   "cons_1m"    => cons_1m(),
                _ => Ok(()),
            };
            if let Err(e) = result { eprintln!("{role} failed: {e}"); std::process::exit(1); }
            return;
        }
    }

    let size_arg = args.windows(2).find(|w| w[0] == "--size").map(|w| w[1].as_str()).unwrap_or("all");
    let mode_arg = args.windows(2).find(|w| w[0] == "--mode").map(|w| w[1].as_str()).unwrap_or("all");
    let json_mode = args.iter().any(|a| a == "--json");
    let quick_mode = args.iter().any(|a| a == "--quick");

    let run_throughput = mode_arg == "all" || mode_arg == "throughput";
    let run_co = mode_arg == "all" || mode_arg == "co";

    fn roles(tag: &str) -> (&'static str, &'static str) {
        match tag {
            "64B" => ("prod_64b", "cons_64b"), "512B" => ("prod_512b", "cons_512b"),
            t if t.starts_with("1K") => ("prod_1k", "cons_1k"),
            t if t.starts_with("4K") => ("prod_4k", "cons_4k"),
            t if t.starts_with("16K") => ("prod_16k", "cons_16k"),
            t if t.starts_with("64K") => ("prod_64k", "cons_64k"),
            t if t.starts_with("256K") => ("prod_256k", "cons_256k"),
            t if t.starts_with("1M") => ("prod_1m", "cons_1m"),
            _ => ("prod_64b", "cons_64b"),
        }
    }

    let scenarios: Vec<SweepPoint> = {
        let mut v = Vec::new();

        if run_throughput {
            v.push(SweepPoint { label: "signal", size_bytes: 64, events: 10_000_000, buffer: 65_536, target_rate: 0, prod_role: "sig_prod", cons_role: "sig_cons", tag: "SIG" });
            for (label, size, events, buffer, tag) in [
                ("64B", 64usize, 10_000_000u64, 65_536usize, "64B"), ("512B", 512, 5_000_000, 131_072, "512B"),
                ("1KB", 1_024, 2_000_000, 131_072, "1K"), ("4KB", 4_096, 1_000_000, 65_536, "4K"),
                ("16KB", 16_384, 500_000, 32_768, "16K"), ("64KB", 65_536, 200_000, 16_384, "64K"),
                ("256KB", 262_144, 100_000, 8_192, "256K"), ("1MB", 1_048_576, 50_000, 4_096, "1M"),
            ] {
                let (p, c) = roles(tag);
                v.push(SweepPoint { label, size_bytes: size, events, buffer, target_rate: 0, prod_role: p, cons_role: c, tag });
            }
        }

        if run_co {
            for (label, size, buffer, rate, tag) in [
                ("1KB@100K",  1_024,     131_072, 100_000u64, "1K_CO"),
                ("1KB@500K",  1_024,     131_072, 500_000,    "1K_CO"),
                ("4KB@100K",  4_096,     65_536,  100_000,    "4K_CO"),
                ("64KB@50K",  65_536,    16_384,  50_000,     "64K_CO"),
                ("64KB@100K", 65_536,    16_384,  100_000,    "64K_CO"),
                ("256KB@10K", 262_144,   8_192,   10_000,     "256K_CO"),
                ("1MB@10K",   1_048_576, 4_096,   10_000,     "1M_CO"),
            ] {
                let (p, c) = roles(tag);
                v.push(SweepPoint { label, size_bytes: size, events: 100_000, buffer, target_rate: rate, prod_role: p, cons_role: c, tag });
            }
        }

        v
    };

    if !json_mode {
        println!("=== Monster Sweep MMAP Benchmark ===");
        println!("Backend: file-backed mmap (M3 Max 14-core, 96GB, 300 GB/s BW)");
        println!("Modes: throughput + CO-aware (fixed rate, intended_send_time)");
        println!("Full payload fill (producer) + checksum (consumer)");
        println!();
    }

    let mut report = BenchReport::new();
    for sp in &scenarios {
        let run = match size_arg {
            "all" => {
                if quick_mode {
                    matches!(sp.tag, "SIG" | "64B" | "1K" | "64K" | "1M" | "1K_CO" | "64K_CO")
                        && (sp.target_rate == 0 || sp.target_rate == 500_000 || sp.target_rate == 50_000)
                } else { true }
            }
            tag => sp.tag == tag,
        };
        if run { report.add(run_sweep_point(sp)); }
    }

    if json_mode { println!("{}", serde_json::to_string_pretty(&report).expect("serialize")); }
    else { report.print_summary(); }

    if let Some(path) = args.windows(2).find(|w| w[0] == "--json-out").map(|w| w[1].clone()) {
        report.write_json(&path).expect("write JSON"); eprintln!("JSON written to {path}");
    }
    if let Some(path) = args.windows(2).find(|w| w[0] == "--csv-out").map(|w| w[1].clone()) {
        report.write_csv(&path).expect("write CSV"); eprintln!("CSV written to {path}");
    }
    if let Some(path) = args.windows(2).find(|w| w[0] == "--md-out").map(|w| w[1].clone()) {
        write_sweep_markdown(&report, &path).expect("write markdown"); eprintln!("Markdown written to {path}");
    }
    if let Some(path) = env::var("MYELON_BENCH_JSON_OUT").ok() { report.write_json(&path).expect("write JSON"); }
}
