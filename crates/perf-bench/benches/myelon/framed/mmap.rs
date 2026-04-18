//! FramedTransport benchmark over mmap backend.

use myelon::transport::{
    FixedFrame, MmapFramedTransportConsumer, MmapFramedTransportProducer, MyelonWaitStrategy,
};
use perf_bench::harness::{
    self, mmap_layout_from_env, read_env_usize, spawn_child, unique_mmap_root, unique_mmap_segment,
    ConsumerOutput, IpcBenchmark, ProducerOutput, ScenarioChildren,
};
use perf_bench::reporting::{self, BenchReport};
use std::env;
use std::time::{Duration, Instant};

const FRAME_DATA_BYTES: usize = 64 * 1024 - 12;
type Frame = FixedFrame<FRAME_DATA_BYTES>;

#[derive(Clone, Copy)]
struct Scenario {
    payload_label: &'static str,
    payload_tag: &'static str,
    payload_bytes: usize,
    messages: u64,
    buffer: usize,
    consumers: usize,
}

#[derive(Clone, Copy)]
struct PayloadConfig {
    label: &'static str,
    tag: &'static str,
    payload_bytes: usize,
    messages: u64,
    base_buffer: usize,
}

const PAYLOADS: [PayloadConfig; 4] = [
    PayloadConfig {
        label: "1KB",
        tag: "1K",
        payload_bytes: 1024,
        messages: 100_000,
        base_buffer: 1024,
    },
    PayloadConfig {
        label: "32KB",
        tag: "32K",
        payload_bytes: 32 * 1024,
        messages: 50_000,
        base_buffer: 1024,
    },
    PayloadConfig {
        label: "64KB",
        tag: "64K",
        payload_bytes: 65_524,
        messages: 50_000,
        base_buffer: 1024,
    },
    PayloadConfig {
        label: "128KB-frag",
        tag: "128K",
        payload_bytes: 128 * 1024,
        messages: 10_000,
        base_buffer: 2048,
    },
];

const TARGET_CONSUMERS: [usize; 6] = [1, 2, 4, 6, 8, 12];

impl Scenario {
    fn selector(&self) -> String {
        format!("{}_{}c", self.payload_tag, self.consumers)
    }
}

fn scaled_buffer(base_buffer: usize, consumers: usize) -> usize {
    base_buffer
        .max(consumers.next_power_of_two() * 256)
        .next_power_of_two()
}

fn scenarios() -> Vec<Scenario> {
    let mut scenarios = Vec::new();
    for payload in PAYLOADS {
        for consumers in TARGET_CONSUMERS {
            scenarios.push(Scenario {
                payload_label: payload.label,
                payload_tag: payload.tag,
                payload_bytes: payload.payload_bytes,
                messages: payload.messages,
                buffer: scaled_buffer(payload.base_buffer, consumers),
                consumers,
            });
        }
    }

    // Preserve the legacy 1p3c 32KB anchor scenario for comparison with older runs.
    scenarios.push(Scenario {
        payload_label: "32KB",
        payload_tag: "32K",
        payload_bytes: 32 * 1024,
        messages: 50_000,
        buffer: scaled_buffer(1024, 3),
        consumers: 3,
    });
    scenarios
}

fn read_env() -> (disruptor_mp::MmapTransportLayout, usize, u64, usize) {
    let layout = mmap_layout_from_env("MMAP_ROOT", "MMAP_SEGMENT");
    let payload_bytes: usize = env::var("BENCH_PAYLOAD_BYTES")
        .expect("BENCH_PAYLOAD_BYTES")
        .parse()
        .expect("payload bytes");
    let messages: u64 = env::var("BENCH_MESSAGES")
        .expect("BENCH_MESSAGES")
        .parse()
        .expect("message count");
    let buffer_depth = read_env_usize("BENCH_BUFFER_DEPTH", 1024);
    (layout, payload_bytes, messages, buffer_depth)
}

fn producer_process() -> Result<(), Box<dyn std::error::Error>> {
    let (layout, payload_bytes, messages, buffer_depth) = read_env();
    let num_consumers = read_env_usize("BENCH_NUM_CONSUMERS", 1);
    let payload = vec![42u8; payload_bytes];
    let mut producer = MmapFramedTransportProducer::<Frame>::create(layout, buffer_depth)?;

    if !producer.wait_for_consumers_ready(num_consumers as i64, Duration::from_secs(60)) {
        return Err(format!("timeout waiting for {num_consumers} consumers").into());
    }

    let start = Instant::now();
    for i in 0..messages {
        producer.publish(&payload, (i % 256) as u8);
    }
    let elapsed = start.elapsed();
    let output = ProducerOutput::from_elapsed(messages, elapsed, payload_bytes);
    println!("{}", serde_json::to_string(&output)?);

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
    let (layout, payload_bytes, messages, buffer_depth) = read_env();
    let consumer_id = read_env_usize("CONSUMER_ID", 0);
    let consumer_name = format!("c{}", consumer_id);

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapFramedTransportConsumer::<Frame>::attach(
            layout.clone(),
            buffer_depth,
            &consumer_name,
            MyelonWaitStrategy::BusySpin,
        ) {
            Ok(consumer) => break consumer,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(error) => return Err(format!("consumer attach failed: {error}").into()),
        }
    };

    let mut start: Option<Instant> = None;
    let mut consumed = 0u64;
    let mut checksum = 0u64;
    while consumed < messages {
        let (_kind, data) = consumer.recv_message_blocking();
        if start.is_none() {
            start = Some(Instant::now());
        }
        let payload_sum = data.iter().fold(0u8, |a, &b| a.wrapping_add(b)) as u64;
        std::hint::black_box(payload_sum);
        checksum = checksum.wrapping_add(payload_sum);
        consumed += 1;
    }
    let elapsed = start.expect("consumer never received a frame").elapsed();

    let output =
        ConsumerOutput::from_elapsed(consumer_id, consumed, elapsed, payload_bytes, checksum);
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

impl IpcBenchmark for Scenario {
    fn bench_name(&self) -> &str {
        "framed_mmap"
    }

    fn scenario_name(&self) -> String {
        format!("framed_1p{}c_{}", self.consumers, self.payload_label)
    }

    fn backend(&self) -> &str {
        "mmap"
    }

    fn layer(&self) -> &str {
        "framed"
    }

    fn message_size_bytes(&self) -> usize {
        self.payload_bytes
    }

    fn buffer_depth(&self) -> usize {
        self.buffer
    }

    fn num_messages(&self) -> u64 {
        self.messages
    }

    fn num_consumers(&self) -> usize {
        self.consumers
    }

    fn throughput_unit(&self) -> &str {
        "msgs/s"
    }

    fn consumer_summary_label(&self) -> &str {
        if self.consumers == 1 {
            "consumer"
        } else {
            "avg cons"
        }
    }

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, harness::BenchError> {
        let root = unique_mmap_root("myelon_framed_mmap");
        let segment = unique_mmap_segment("framed");
        let root_str = root.display().to_string();
        let env_common: Vec<(&str, String)> = vec![
            ("MMAP_ROOT", root_str),
            ("MMAP_SEGMENT", segment),
            ("BENCH_PAYLOAD_BYTES", self.payload_bytes.to_string()),
            ("BENCH_MESSAGES", self.messages.to_string()),
            ("BENCH_BUFFER_DEPTH", self.buffer.to_string()),
            ("BENCH_NUM_CONSUMERS", self.consumers.to_string()),
        ];

        let producer = spawn_child(exe, "framed_mmap_producer", &env_common);
        let consumers = (0..self.consumers)
            .map(|consumer_id| {
                let mut consumer_envs = env_common.clone();
                consumer_envs.push(("CONSUMER_ID", consumer_id.to_string()));
                spawn_child(exe, "framed_mmap_consumer", &consumer_envs)
            })
            .collect();

        Ok(ScenarioChildren::new(producer, consumers).with_cleanup_path(root))
    }
}

const CHILD_ROLES: &[harness::ChildRole] = &[
    harness::ChildRole::new("framed_mmap_producer", producer_process),
    harness::ChildRole::new("framed_mmap_consumer", consumer_process),
];

struct FramedMmapBench;

impl harness::BenchHarness for FramedMmapBench {
    fn bench_name(&self) -> &'static str {
        "framed_mmap"
    }

    fn child_roles(&self) -> &'static [harness::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> harness::BenchRunResult {
        let payload_arg = args
            .windows(2)
            .find(|w| w[0] == "--payload")
            .map(|w| w[1].as_str())
            .unwrap_or("all");
        let output_args = reporting::ReportOutputArgs::from_args(args);

        if !output_args.json_mode {
            println!("=== Framed Transport MMAP Benchmark ===");
            println!("Frame: {}KB data capacity", FRAME_DATA_BYTES / 1024);
            println!("Transport: file-backed mmap");
            println!();
        }

        let mut report = BenchReport::new();
        for scenario in scenarios() {
            let selector = scenario.selector();
            if payload_arg == "all"
                || payload_arg == scenario.payload_tag
                || payload_arg == selector
            {
                report.add(scenario.run_benchmark()?);
            }
        }

        reporting::emit_report(
            &report,
            &output_args,
            Some(reporting::ReportView::Summary),
            None,
            None,
        );
        Ok(())
    }
}

perf_bench::myelon_bench_main!(FramedMmapBench);
