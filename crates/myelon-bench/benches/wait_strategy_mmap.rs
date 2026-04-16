//! mmap wait strategy benchmark modeled after the battle-tested SHM matrix.

use myelon_bench::events::format_throughput;
use myelon_bench::reporting::{BenchReport, BenchResult};
use std::env;
use std::io::Read as _;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use disruptor_mp::{MmapConsumer, MmapProducer, MmapTransportLayout};

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
        let mut payload = [0u8; 112];
        for (index, item) in payload.iter_mut().enumerate() {
            *item = (index % 256) as u8;
        }
        Self {
            sequence: 0,
            timestamp_ns: 0,
            payload,
        }
    }
}

fn unique_root() -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    env::temp_dir().join(format!("wait_mmap_{}_{}", std::process::id(), ts))
}

fn unique_segment() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("wait_{}_{}", std::process::id(), ts)
}

fn spawn_child(
    exe: &std::path::Path,
    role: &str,
    root: &str,
    segment: &str,
    num_consumers: usize,
    wait_strategy: &str,
    consumer_id: Option<usize>,
) -> Child {
    let mut command = Command::new(exe);
    command
        .arg(role)
        .env("MMAP_ROOT", root)
        .env("MMAP_SEGMENT", segment)
        .env("NUM_CONSUMERS", num_consumers.to_string())
        .env("WAIT_STRATEGY", wait_strategy)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(consumer_id) = consumer_id {
        command.env("CONSUMER_ID", consumer_id.to_string());
    }

    command
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {role}: {e}"))
}

fn wait_with_output_timeout(mut child: Child, timeout: Duration) -> Result<Output, String> {
    fn collect(child: &mut Child, status: std::process::ExitStatus) -> Output {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        if let Some(mut pipe) = child.stdout.take() {
            let _ = pipe.read_to_end(&mut stdout);
        }
        if let Some(mut pipe) = child.stderr.take() {
            let _ = pipe.read_to_end(&mut stderr);
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
            if let Some(value) = rest.split_whitespace().next() {
                return value.parse().unwrap_or(0.0);
            }
        }
    }
    0.0
}

fn read_env() -> (MmapTransportLayout, usize, String, String) {
    let root = env::var("MMAP_ROOT").expect("MMAP_ROOT");
    let segment = env::var("MMAP_SEGMENT").expect("MMAP_SEGMENT");
    let num_consumers = env::var("NUM_CONSUMERS")
        .expect("NUM_CONSUMERS")
        .parse()
        .expect("num consumers");
    let wait_strategy = env::var("WAIT_STRATEGY").expect("WAIT_STRATEGY");
    let layout =
        MmapTransportLayout::new(PathBuf::from(root), segment.clone()).expect("mmap layout");
    (layout, num_consumers, wait_strategy, segment)
}

fn apply_wait_strategy(wait_strategy: &str) {
    match wait_strategy {
        "BusySpin" | "BusySpinWithSpinLoopHint" => std::hint::spin_loop(),
        "Sleep" => std::thread::sleep(Duration::from_micros(1)),
        "Block" => std::thread::sleep(Duration::from_millis(1)),
        _ => std::thread::sleep(Duration::from_millis(1)),
    }
}

fn producer_process() -> Result<(), Box<dyn std::error::Error>> {
    let (layout, num_consumers, wait_strategy, _segment) = read_env();
    layout.ensure_directories()?;

    let mut producer = MmapProducer::<Event>::create(layout, BUFFER_SIZE, Event::default)?;
    if !producer.wait_for_consumers_ready(num_consumers as i64, Duration::from_secs(30)) {
        return Err("timeout waiting for consumers".into());
    }

    let start = Instant::now();
    for i in 0..NUM_EVENTS {
        producer.publish(|event| {
            event.sequence = i;
            event.timestamp_ns = start.elapsed().as_nanos() as u64;
        });
    }
    let elapsed = start.elapsed();
    let throughput = NUM_EVENTS as f64 / elapsed.as_secs_f64();
    println!("Throughput: {throughput}");
    println!("Elapsed: {}", elapsed.as_secs_f64());

    let last_sequence = producer.last_published_sequence();
    let strategy = match wait_strategy.as_str() {
        "BusySpin" => disruptor_mp::AutoWaitStrategy::BusySpin,
        "BusySpinWithSpinLoopHint" => disruptor_mp::AutoWaitStrategy::BusySpinWithSpinLoopHint,
        "Sleep" => disruptor_mp::AutoWaitStrategy::Sleep(Duration::from_micros(1)),
        "Block" => disruptor_mp::AutoWaitStrategy::Block,
        _ => disruptor_mp::AutoWaitStrategy::Block,
    };
    let _ = producer.wait_until_consumed_with_strategy(last_sequence, Duration::from_secs(60), strategy);
    Ok(())
}

fn consumer_process() -> Result<(), Box<dyn std::error::Error>> {
    let (layout, _num_consumers, wait_strategy, _segment) = read_env();
    let consumer_id = format!(
        "c{}",
        env::var("CONSUMER_ID").unwrap_or_else(|_| "0".to_string())
    );

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapConsumer::<Event>::attach(layout.clone(), BUFFER_SIZE, &consumer_id) {
            Ok(consumer) => break consumer,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(error) => return Err(format!("consumer attach failed: {error}").into()),
        }
    };

    let start = Instant::now();
    let mut consumed = 0u64;
    while consumed < NUM_EVENTS {
        if consumer.try_consume_next().is_some() {
            consumed += 1;
        } else {
            apply_wait_strategy(&wait_strategy);
        }
    }
    let elapsed = start.elapsed();
    let throughput = consumed as f64 / elapsed.as_secs_f64();
    println!("Throughput: {throughput}");
    println!("Elapsed: {}", elapsed.as_secs_f64());
    Ok(())
}

fn run_test(num_consumers: usize, wait_strategy: &str) -> BenchResult {
    let root = unique_root();
    let segment = unique_segment();
    let root_str = root.display().to_string();
    let exe = env::current_exe().expect("current_exe");

    let producer = spawn_child(
        &exe,
        "mmap_wait_producer",
        &root_str,
        &segment,
        num_consumers,
        wait_strategy,
        None,
    );
    let consumers: Vec<_> = (0..num_consumers)
        .map(|index| {
            spawn_child(
                &exe,
                "mmap_wait_consumer",
                &root_str,
                &segment,
                num_consumers,
                wait_strategy,
                Some(index),
            )
        })
        .collect();

    let timeout = Duration::from_secs(120);
    let producer_output = wait_with_output_timeout(producer, timeout);
    let consumer_outputs: Vec<_> = consumers
        .into_iter()
        .map(|child| wait_with_output_timeout(child, timeout))
        .collect();

    let producer_stdout = match producer_output {
        Ok(output) => {
            if !output.stderr.is_empty() {
                eprintln!(
                    "[mmap wait producer stderr] {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            String::from_utf8_lossy(&output.stdout).to_string()
        }
        Err(error) => {
            eprintln!("[mmap wait producer] {error}");
            String::new()
        }
    };

    let mut total_consumer_throughput = 0.0;
    for output in consumer_outputs {
        match output {
            Ok(output) => {
                if !output.stderr.is_empty() {
                    eprintln!(
                        "[mmap wait consumer stderr] {}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
                let stdout = String::from_utf8_lossy(&output.stdout);
                total_consumer_throughput += extract_value(&stdout, "Throughput");
            }
            Err(error) => eprintln!("[mmap wait consumer] {error}"),
        }
    }

    let producer_throughput = extract_value(&producer_stdout, "Throughput");
    let avg_consumer_throughput = if num_consumers > 0 {
        total_consumer_throughput / num_consumers as f64
    } else {
        0.0
    };

    let _ = std::fs::remove_dir_all(&root);

    println!(
        "  {:<6} {:<24} producer: {:>10} events/s  avg consumer: {:>10} events/s",
        format!("1p{}c", num_consumers),
        wait_strategy,
        format_throughput(producer_throughput),
        format_throughput(avg_consumer_throughput),
    );

    BenchResult {
        scenario: format!("1p{}c", num_consumers),
        backend: "mmap".to_string(),
        layer: "wait_strategy".to_string(),
        codec: None,
        wait_strategy: wait_strategy.to_string(),
        num_consumers,
        payload_bytes: ELEMENT_SIZE,
        events: NUM_EVENTS as usize,
        producer_ops_sec: producer_throughput,
        consumer_ops_sec: avg_consumer_throughput,
        data_rate_mbps: (producer_throughput * ELEMENT_SIZE as f64) / (1024.0 * 1024.0),
        latency: None,
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 {
        match args[1].as_str() {
            "mmap_wait_producer" => {
                if let Err(error) = producer_process() {
                    eprintln!("mmap_wait_producer failed: {error}");
                    std::process::exit(1);
                }
                return;
            }
            "mmap_wait_consumer" => {
                if let Err(error) = consumer_process() {
                    eprintln!("mmap_wait_consumer failed: {error}");
                    std::process::exit(1);
                }
                return;
            }
            _ => {}
        }
    }

    let json_mode = args.iter().any(|arg| arg == "--json");
    let mode = args
        .iter()
        .skip(1)
        .find(|arg| arg.as_str() != "--json")
        .map(String::as_str)
        .unwrap_or("quick");

    let scenarios: Vec<(usize, &'static str)> = match mode {
        "quick" => vec![(1, "BusySpin")],
        "full" | "comprehensive" => {
            let consumer_counts = [1usize, 2, 4, 6, 8, 10, 12];
            let wait_strategies = [
                "BusySpin",
                "Block",
                "Sleep",
                "BusySpinWithSpinLoopHint",
            ];
            consumer_counts
                .into_iter()
                .flat_map(|count| wait_strategies.into_iter().map(move |strategy| (count, strategy)))
                .collect()
        }
        other => {
            eprintln!("Unknown mode '{other}', expected quick|full|comprehensive");
            std::process::exit(1);
        }
    };

    if !json_mode {
        println!("=== Wait Strategy MMAP Benchmark ===");
        println!("Event size: {} bytes", ELEMENT_SIZE);
        println!("Buffer size: {} slots", BUFFER_SIZE);
        println!("Events per test: {}", NUM_EVENTS);
        println!();
    }

    let mut report = BenchReport::new();
    for (num_consumers, wait_strategy) in scenarios {
        report.add(run_test(num_consumers, wait_strategy));
    }

    if json_mode {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).expect("serialize json report")
        );
    } else {
        report.print_summary();
    }

    if let Some(path) = env::var("MYELON_BENCH_JSON_OUT").ok() {
        report.write_json(&path).expect("write json report");
    }
}
