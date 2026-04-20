//! FramedTransport benchmark over SHM backend.
//!
//! Measures the overhead of myelon's FramedTransportProducer/Consumer
//! (frame headers, message reassembly) vs raw disruptor-mp ring.
//!
//! Payload sizes: 1KB (single-frame), 32KB, 64KB (max single-frame), 128KB (fragmented)
//!
//! Run: cargo bench -p myelon-bench --bench framed_shm
//! Single payload: cargo bench -p myelon-bench --bench framed_shm -- --payload 128K

use crate::coordination::BenchmarkCoordination;
use crate::harness::{
    self, read_env_u64, read_env_usize, segment_from_env, spawn_child, unique_shm_segment,
    ConsumerOutput, IpcBenchmark, ProducerOutput, ScenarioChildren,
};
use crate::report_v2::BackendKind;
use crate::reporting::{self, BenchReport};
use crate::scenario_v2::framed::{FramedScenarioSpec, FramedSelection};
use myelon::transport::{
    FixedFrame, FramedTransportConsumer, FramedTransportProducer, MyelonWaitStrategy,
};
use std::time::{Duration, Instant};

// 64KB frame (matching competitor-rs RPC frame size)
const FRAME_DATA_BYTES: usize = 64 * 1024 - 12;
type Frame = FixedFrame<FRAME_DATA_BYTES>;

// ============================================================
// Producer child (configurable payload size + message count)
// ============================================================

fn producer_process() -> Result<(), Box<dyn std::error::Error>> {
    let segment = segment_from_env("BENCHMARK_SEGMENT_NAME");
    let payload_size = read_env_usize("BENCH_PAYLOAD_SIZE", 1024);
    let num_messages = read_env_u64("BENCH_NUM_MESSAGES", 50_000);
    let buffer_depth = read_env_usize("BENCH_BUFFER_DEPTH", 1024);
    let num_consumers = read_env_usize("BENCH_NUM_CONSUMERS", 1);

    let mut producer = FramedTransportProducer::<Frame>::create_with_consumers(
        &segment,
        buffer_depth,
        num_consumers,
    )?;
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

    let output = ProducerOutput::from_elapsed(num_messages, elapsed, payload_size);
    println!("{}", serde_json::to_string(&output)?);

    coord.signal_producer_done(num_messages as i64);
    coord.wait_for_consumers_done(num_consumers, Duration::from_secs(60));
    Ok(())
}

// ============================================================
// Consumer child (configurable)
// ============================================================

fn consumer_process() -> Result<(), Box<dyn std::error::Error>> {
    let segment = segment_from_env("BENCHMARK_SEGMENT_NAME");
    let consumer_id = read_env_usize("CONSUMER_ID", 0);
    let buffer_depth = read_env_usize("BENCH_BUFFER_DEPTH", 1024);
    let num_messages = read_env_u64("BENCH_NUM_MESSAGES", 50_000);

    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;

    let mut consumer = FramedTransportConsumer::<Frame>::attach(
        &segment,
        buffer_depth,
        MyelonWaitStrategy::BusySpin,
    )?;

    coord.signal_consumer_ready();

    let mut start: Option<Instant> = None;
    let mut consumed = 0u64;
    let mut checksum = 0u64;

    // Consume exactly num_messages — avoids deadlock from calling
    // recv_message_blocking after all messages are consumed.
    while consumed < num_messages {
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

    let output = ConsumerOutput::from_elapsed(
        consumer_id,
        consumed,
        elapsed,
        read_env_usize("BENCH_PAYLOAD_SIZE", 1024),
        checksum,
    );
    println!("{}", serde_json::to_string(&output)?);

    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

// ============================================================
// Orchestrator
// ============================================================

#[derive(Clone, Copy)]
struct Scenario {
    payload_label: &'static str,
    payload: usize,
    messages: u64,
    buffer: usize,
    consumers: usize,
    producer_role: &'static str,
    consumer_role: &'static str,
}

impl IpcBenchmark for Scenario {
    fn bench_name(&self) -> &str {
        "framed_shm"
    }

    fn scenario_name(&self) -> String {
        format!("framed_1p{}c_{}", self.consumers, self.payload_label)
    }

    fn backend(&self) -> &str {
        "shm"
    }

    fn layer(&self) -> &str {
        "framed"
    }

    fn transport_metadata(&self) -> reporting::BenchTransportSpec {
        reporting::BenchTransportSpec::benchmark_shm(self.consumers)
            .with_zero_copy(false)
            .with_framing("fixed_64k")
    }

    fn message_size_bytes(&self) -> usize {
        self.payload
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
        let segment = unique_shm_segment(&format!("fr_{}", self.scenario_name()));
        let env_common: Vec<(&str, String)> = vec![
            ("BENCHMARK_SEGMENT_NAME", segment.clone()),
            ("BENCH_PAYLOAD_SIZE", self.payload.to_string()),
            ("BENCH_NUM_MESSAGES", self.messages.to_string()),
            ("BENCH_BUFFER_DEPTH", self.buffer.to_string()),
            ("BENCH_NUM_CONSUMERS", self.consumers.to_string()),
        ];

        let producer = spawn_child(exe, self.producer_role, &env_common);
        let consumers = (0..self.consumers)
            .map(|consumer_id| {
                let mut consumer_envs = env_common.clone();
                consumer_envs.push(("CONSUMER_ID", consumer_id.to_string()));
                spawn_child(exe, self.consumer_role, &consumer_envs)
            })
            .collect();

        Ok(ScenarioChildren::new(producer, consumers))
    }
}

impl Scenario {
    fn from_spec(spec: &FramedScenarioSpec) -> Self {
        Self {
            payload_label: spec.payload_label,
            payload: spec.payload_bytes,
            messages: spec.messages,
            buffer: spec.buffer_depth(),
            consumers: spec.consumers,
            producer_role: spec.producer_role,
            consumer_role: spec.consumer_role,
        }
    }
}

const CHILD_ROLES: &[harness::ChildRole] = &[
    harness::ChildRole::new("framed_producer", producer_process),
    harness::ChildRole::new("framed_consumer", consumer_process),
];

pub struct FramedShmBench;

impl harness::BenchHarness for FramedShmBench {
    fn bench_name(&self) -> &'static str {
        "framed_shm"
    }

    fn child_roles(&self) -> &'static [harness::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> harness::BenchRunResult {
        let selection = FramedSelection::parse(args);

        if !selection.output_args.json_mode {
            println!("=== Framed Transport SHM Benchmark ===");
            println!("Frame: {}KB data capacity", FRAME_DATA_BYTES / 1024);
            println!("Buffer: 1024 frames (2048 for fragmented)");
            println!();
        }

        let mut report = BenchReport::new();
        for spec in selection.scenario_specs(BackendKind::Shm) {
            report.add(Scenario::from_spec(&spec).run_benchmark()?);
        }

        reporting::emit_report(
            &report,
            &selection.output_args,
            Some(reporting::ReportView::Summary),
            None,
            None,
        );
        Ok(())
    }
}
