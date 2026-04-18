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

fn read_env() -> (disruptor_mp::MmapTransportLayout, usize, u64) {
    let layout = mmap_layout_from_env("MMAP_ROOT", "MMAP_SEGMENT");
    let payload_bytes: usize = env::var("BENCH_PAYLOAD_BYTES")
        .expect("BENCH_PAYLOAD_BYTES")
        .parse()
        .expect("payload bytes");
    let messages: u64 = env::var("BENCH_MESSAGES")
        .expect("BENCH_MESSAGES")
        .parse()
        .expect("message count");
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
    let (layout, payload_bytes, messages) = read_env();
    let consumer_id = read_env_usize("CONSUMER_ID", 0);
    let consumer_name = format!("c{}", consumer_id);

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapFramedTransportConsumer::<Frame>::attach(
            layout.clone(),
            BUFFER_DEPTH,
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
        let payload_sum = data
            .iter()
            .fold(0u64, |acc, &byte| acc.wrapping_add(byte as u64));
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
        let prefix = if self.consumers == 1 {
            "framed_1p1c".to_string()
        } else {
            format!("framed_1p{}c", self.consumers)
        };
        format!("{}_{}", prefix, self.label)
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
        BUFFER_DEPTH
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
        for scenario in SCENARIOS {
            if payload_arg == "all" || payload_arg == scenario.tag {
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
