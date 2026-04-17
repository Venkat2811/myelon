//! FramedTransport benchmark over mmap backend.

use perf_bench::events::format_throughput;
use perf_bench::reporting::{self, BenchReport, BenchResult};
use myelon::transport::{
    FixedFrame, MmapFramedTransportConsumer, MmapFramedTransportProducer, MyelonWaitStrategy,
};
use std::env;
use std::io::Read as _;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const FRAME_DATA_BYTES: usize = 64 * 1024 - 12;
type Frame = FixedFrame<FRAME_DATA_BYTES>;
const BUFFER_DEPTH: usize = 1024;

#[derive(Clone, Copy)]
struct Scenario {
    label: &'static str,
    payload_bytes: usize,
    messages: u64,
    consumers: usize,
    tag: &'static str,
}

const SCENARIOS: [Scenario; 5] = [
    Scenario {
        label: "1KB",
        payload_bytes: 1024,
        messages: 100_000,
        consumers: 1,
        tag: "1K",
    },
    Scenario {
        label: "32KB",
        payload_bytes: 32 * 1024,
        messages: 50_000,
        consumers: 1,
        tag: "32K",
    },
    Scenario {
        label: "64KB",
        payload_bytes: 65_524,
        messages: 50_000,
        consumers: 1,
        tag: "64K",
    },
    Scenario {
        label: "128KB-frag",
        payload_bytes: 128 * 1024,
        messages: 10_000,
        consumers: 1,
        tag: "128K",
    },
    Scenario {
        label: "32KB_1p3c",
        payload_bytes: 32 * 1024,
        messages: 50_000,
        consumers: 3,
        tag: "32K_3c",
    },
];

fn unique_root() -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    env::temp_dir().join(format!("myelon_framed_mmap_{}_{}", std::process::id(), ts))
}

fn unique_segment() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("framed_{}_{}", std::process::id(), ts)
}

fn spawn_child(
    exe: &std::path::Path,
    role: &str,
    root: &str,
    segment: &str,
    payload_bytes: usize,
    messages: u64,
    num_consumers: usize,
) -> Child {
    Command::new(exe)
        .arg(role)
        .env("MMAP_ROOT", root)
        .env("MMAP_SEGMENT", segment)
        .env("BENCH_PAYLOAD_BYTES", payload_bytes.to_string())
        .env("BENCH_MESSAGES", messages.to_string())
        .env("BENCH_NUM_CONSUMERS", num_consumers.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {role}: {e}"))
}

fn wait_with_output_timeout(mut child: Child, timeout: Duration) -> Result<Output, String> {
    fn collect(child: &mut Child, status: std::process::ExitStatus) -> Output {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        if let Some(mut o) = child.stdout.take() {
            let _ = o.read_to_end(&mut stdout);
        }
        if let Some(mut e) = child.stderr.take() {
            let _ = e.read_to_end(&mut stderr);
        }
        Output {
            status,
            stdout,
            stderr,
        }
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
                "timeout; stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
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

fn read_env_usize(key: &str, default: usize) -> usize {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn read_env() -> (disruptor_mp::MmapTransportLayout, usize, u64) {
    let root = env::var("MMAP_ROOT").expect("MMAP_ROOT");
    let segment = env::var("MMAP_SEGMENT").expect("MMAP_SEGMENT");
    let payload_bytes = env::var("BENCH_PAYLOAD_BYTES")
        .expect("BENCH_PAYLOAD_BYTES")
        .parse()
        .expect("payload bytes");
    let messages = env::var("BENCH_MESSAGES")
        .expect("BENCH_MESSAGES")
        .parse()
        .expect("message count");
    let layout = disruptor_mp::MmapTransportLayout::new(PathBuf::from(root), segment)
        .expect("mmap layout");
    (layout, payload_bytes, messages)
}

fn producer_process() -> Result<(), Box<dyn std::error::Error>> {
    let (layout, payload_bytes, messages) = read_env();
    let num_consumers = read_env_usize("BENCH_NUM_CONSUMERS", 1);
    let payload = vec![42u8; payload_bytes];
    let mut producer = MmapFramedTransportProducer::<Frame>::create(layout, BUFFER_DEPTH)?;

    if !producer.wait_for_consumers_ready(num_consumers as i64, Duration::from_secs(60)) {
        return Err(format!("timeout waiting for {num_consumers} consumers").into());
    }

    let start = Instant::now();
    for i in 0..messages {
        producer.publish(&payload, (i % 256) as u8);
    }
    let elapsed = start.elapsed();
    println!("Throughput: {:.0} msgs/sec", messages as f64 / elapsed.as_secs_f64());
    println!("Time: {:.3} seconds", elapsed.as_secs_f64());

    let last_sequence = producer.raw().last_published_sequence();
    if !producer.wait_until_consumed(
        last_sequence,
        Duration::from_secs(30),
        disruptor_mp::AutoWaitStrategy::BusySpin,
    ) {
        return Err("timeout waiting for consumers to drain".into());
    }
    Ok(())
}

fn consumer_process() -> Result<(), Box<dyn std::error::Error>> {
    let (layout, _payload_bytes, messages) = read_env();
    let consumer_id = format!("c{}", std::process::id());

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapFramedTransportConsumer::<Frame>::attach(
            layout.clone(),
            BUFFER_DEPTH,
            &consumer_id,
            MyelonWaitStrategy::BusySpin,
        ) {
            Ok(consumer) => break consumer,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(error) => return Err(format!("consumer attach failed: {error}").into()),
        }
    };

    let start = Instant::now();
    let mut consumed = 0u64;
    while consumed < messages {
        let (_kind, data) = consumer.recv_message_blocking();
        consumed += 1;
        std::hint::black_box(data.len());
    }
    let elapsed = start.elapsed();

    println!("Throughput: {:.0} msgs/sec", consumed as f64 / elapsed.as_secs_f64());
    println!("Events: {}", consumed);
    Ok(())
}

fn run_benchmark(scenario: Scenario) -> BenchResult {
    let root = unique_root();
    let segment = unique_segment();
    let root_str = root.display().to_string();
    let exe = env::current_exe().expect("current_exe");
    let nc = scenario.consumers;

    let producer = spawn_child(
        &exe, "framed_mmap_producer", &root_str, &segment,
        scenario.payload_bytes, scenario.messages, nc,
    );
    let consumers: Vec<Child> = (0..nc)
        .map(|_| spawn_child(
            &exe, "framed_mmap_consumer", &root_str, &segment,
            scenario.payload_bytes, scenario.messages, nc,
        ))
        .collect();

    let timeout = Duration::from_secs(180);
    let consumer_outputs: Vec<_> = consumers
        .into_iter()
        .map(|c| wait_with_output_timeout(c, timeout))
        .collect();
    let prod_out = wait_with_output_timeout(producer, timeout);

    let prod_str = match prod_out {
        Ok(o) => {
            if !o.stderr.is_empty() { eprintln!("[{} prod stderr] {}", scenario.label, String::from_utf8_lossy(&o.stderr)); }
            String::from_utf8_lossy(&o.stdout).to_string()
        }
        Err(e) => { eprintln!("[{} prod] {e}", scenario.label); String::new() }
    };

    let prod_tp = extract_value(&prod_str, "Throughput");
    let mut total_cons_tp = 0.0;
    for (i, result) in consumer_outputs.into_iter().enumerate() {
        match result {
            Ok(o) => {
                if !o.stderr.is_empty() { eprintln!("[{} cons{i} stderr] {}", scenario.label, String::from_utf8_lossy(&o.stderr)); }
                total_cons_tp += extract_value(&String::from_utf8_lossy(&o.stdout), "Throughput");
            }
            Err(e) => eprintln!("[{} cons{i}] {e}", scenario.label),
        }
    }
    let avg_cons_tp = if nc > 0 { total_cons_tp / nc as f64 } else { 0.0 };

    let _ = std::fs::remove_dir_all(&root);

    let label_prefix = if nc == 1 { "framed_1p1c" } else { &format!("framed_1p{}c", nc) };
    let cons_label = if nc == 1 { "consumer" } else { "avg cons" };
    println!(
        "  {}_{:<10} producer: {:>10} msgs/s  {}: {:>10} msgs/s",
        label_prefix, scenario.label, format_throughput(prod_tp), cons_label, format_throughput(avg_cons_tp),
    );

    reporting::make_result(
        "framed_mmap",
        &format!("{}_{}", label_prefix, scenario.label),
        "mmap", "framed", None, "BusySpin",
        scenario.payload_bytes, BUFFER_DEPTH, scenario.messages, 0, nc,
        prod_tp, avg_cons_tp, None,
    )
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() > 1 {
        let role = &args[1];
        if role.starts_with("--") { /* fall through */ } else {
            let result = match role.as_str() {
                "framed_mmap_producer" => producer_process(),
                "framed_mmap_consumer" => consumer_process(),
                _ => Ok(()),
            };
            if let Err(e) = result {
                eprintln!("{role} failed: {e}");
                std::process::exit(1);
            }
            return;
        }
    }

    let payload_arg = args.windows(2)
        .find(|w| w[0] == "--payload")
        .map(|w| w[1].as_str())
        .unwrap_or("all");

    let json_mode = args.iter().any(|a| a == "--json");

    if !json_mode {
        println!("=== Framed Transport MMAP Benchmark ===");
        println!("Frame: {}KB data capacity", FRAME_DATA_BYTES / 1024);
        println!("Transport: file-backed mmap");
        println!();
    }

    let mut report = BenchReport::new();
    for scenario in SCENARIOS {
        if payload_arg == "all" || payload_arg == scenario.tag {
            report.add(run_benchmark(scenario));
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
