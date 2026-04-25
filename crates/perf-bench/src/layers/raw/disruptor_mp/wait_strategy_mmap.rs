//! mmap wait strategy benchmark modeled after the battle-tested SHM matrix.

use crate::cli::wait_strategy::{WaitStrategyScenarioSpec, WaitStrategySelection};
use crate::infra::events::format_throughput;
use crate::infra::output::report::BackendKind;
use crate::infra::output::reporting::{self, BenchReport};
use crate::infra::{
    self, mmap_layout_from_env, read_env_string, spawn_child, unique_mmap_root,
    unique_mmap_segment, ConsumerOutput, IpcBenchmark, ProducerOutput, ScenarioChildren,
};
use std::env;
use std::time::{Duration, Instant};

use disruptor_mp::{MmapConsumer, MmapProducer};

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

fn apply_wait_strategy(wait_strategy: &str) {
    match wait_strategy {
        "BusySpin" | "BusySpinWithSpinLoopHint" => std::hint::spin_loop(),
        "Sleep" => std::thread::sleep(Duration::from_micros(1)),
        "Block" => std::thread::sleep(Duration::from_millis(1)),
        _ => std::thread::sleep(Duration::from_millis(1)),
    }
}

fn producer_process() -> Result<(), Box<dyn std::error::Error>> {
    let layout = mmap_layout_from_env("MMAP_ROOT", "MMAP_SEGMENT");
    let num_consumers: usize = read_env_string("NUM_CONSUMERS", "1").parse()?;
    let wait_strategy = read_env_string("WAIT_STRATEGY", "Block");
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
    let output = ProducerOutput::from_elapsed(NUM_EVENTS, elapsed, ELEMENT_SIZE);
    println!("{}", serde_json::to_string(&output)?);

    let last_sequence = producer.last_published_sequence();
    let strategy = match wait_strategy.as_str() {
        "BusySpin" => disruptor_mp::AutoWaitStrategy::BusySpin,
        "BusySpinWithSpinLoopHint" => disruptor_mp::AutoWaitStrategy::BusySpinWithSpinLoopHint,
        "Sleep" => disruptor_mp::AutoWaitStrategy::Sleep(Duration::from_micros(1)),
        "Block" => disruptor_mp::AutoWaitStrategy::Block,
        _ => disruptor_mp::AutoWaitStrategy::Block,
    };
    let _ = producer.wait_until_consumed_with_strategy(
        last_sequence,
        Duration::from_secs(60),
        strategy,
    );
    Ok(())
}

fn consumer_process() -> Result<(), Box<dyn std::error::Error>> {
    let layout = mmap_layout_from_env("MMAP_ROOT", "MMAP_SEGMENT");
    let wait_strategy = read_env_string("WAIT_STRATEGY", "Block");
    let consumer_id = read_env_string("CONSUMER_ID", "0").parse::<usize>()?;
    let consumer_name = format!("c{}", consumer_id);

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapConsumer::<Event>::attach(layout.clone(), BUFFER_SIZE, &consumer_name) {
            Ok(consumer) => break consumer,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(error) => return Err(format!("consumer attach failed: {error}").into()),
        }
    };

    let mut start: Option<Instant> = None;
    let mut consumed = 0u64;
    let mut checksum = 0u64;
    let deadline = infra::spin_deadline();
    while consumed < NUM_EVENTS {
        if let Some((_seq, event)) = consumer.try_consume_next() {
            if start.is_none() {
                start = Some(Instant::now());
            }
            let payload_sum = event.payload.iter().fold(0u8, |a, &b| a.wrapping_add(b));
            std::hint::black_box(payload_sum);
            checksum = checksum.wrapping_add(payload_sum as u64);
            consumed += 1;
        } else {
            infra::check_deadline(deadline, "wait_strategy_mmap consumer_process");
            apply_wait_strategy(&wait_strategy);
        }
    }
    let elapsed = start.expect("consumer never received an event").elapsed();
    let output =
        ConsumerOutput::from_elapsed(consumer_id, consumed, elapsed, ELEMENT_SIZE, checksum);
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

#[derive(Clone, Copy)]
struct Scenario {
    consumers: usize,
    wait_strategy: &'static str,
    producer_role: &'static str,
    consumer_role: &'static str,
    throughput_unit: &'static str,
}

impl IpcBenchmark for Scenario {
    fn bench_name(&self) -> &str {
        "wait_strategy_mmap"
    }

    fn scenario_name(&self) -> String {
        format!("1p{}c_{}", self.consumers, self.wait_strategy)
    }

    fn backend(&self) -> &str {
        "mmap"
    }

    fn layer(&self) -> &str {
        "wait_strategy"
    }

    fn transport_metadata(&self) -> reporting::BenchTransportSpec {
        reporting::BenchTransportSpec::mmap_builtin()
            .with_zero_copy(false)
            .with_framing("none")
    }

    fn wait_strategy(&self) -> &str {
        self.wait_strategy
    }

    fn message_size_bytes(&self) -> usize {
        ELEMENT_SIZE
    }

    fn buffer_depth(&self) -> usize {
        BUFFER_SIZE
    }

    fn num_messages(&self) -> u64 {
        NUM_EVENTS
    }

    fn num_consumers(&self) -> usize {
        self.consumers
    }

    fn throughput_unit(&self) -> &str {
        self.throughput_unit
    }

    fn print_summary_with_metrics(
        &self,
        producer: &infra::ProducerOutput,
        consumers: &[infra::ConsumerOutput],
        _latency: Option<&crate::infra::latency::LatencyStats>,
    ) {
        println!(
            "  {:<6} {:<24} producer: {:>10} {}  avg consumer: {:>10} {}",
            format!("1p{}c", self.consumers),
            self.wait_strategy,
            format_throughput(producer.throughput_ops_sec),
            self.throughput_unit,
            format_throughput(self.average_consumer_ops(consumers)),
            self.throughput_unit,
        );
    }

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, infra::BenchError> {
        let root = unique_mmap_root("wait_mmap");
        let segment = unique_mmap_segment("wait");
        let root_str = root.display().to_string();
        let envs: Vec<(&str, String)> = vec![
            ("MMAP_ROOT", root_str.clone()),
            ("MMAP_SEGMENT", segment.clone()),
            ("NUM_CONSUMERS", self.consumers.to_string()),
            ("WAIT_STRATEGY", self.wait_strategy.to_string()),
        ];

        let producer = spawn_child(exe, self.producer_role, &envs);
        let consumers = (0..self.consumers)
            .map(|index| {
                let mut consumer_envs = envs.clone();
                consumer_envs.push(("CONSUMER_ID", index.to_string()));
                spawn_child(exe, self.consumer_role, &consumer_envs)
            })
            .collect();

        Ok(ScenarioChildren::new(producer, consumers).with_cleanup_path(root))
    }
}

const CHILD_ROLES: &[infra::ChildRole] = &[
    infra::ChildRole::new("mmap_wait_producer", producer_process),
    infra::ChildRole::new("mmap_wait_consumer", consumer_process),
];

pub struct WaitStrategyMmapBench;

impl infra::BenchHarness for WaitStrategyMmapBench {
    fn bench_name(&self) -> &'static str {
        "wait_strategy_mmap"
    }

    fn child_roles(&self) -> &'static [infra::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> infra::BenchRunResult {
        let mode_owned = env::var("BENCH_MODE")
            .ok()
            .or_else(|| {
                args.iter()
                    .skip(1)
                    .find(|arg| !arg.starts_with("--") && arg.as_str() != "wait_strategy_mmap")
                    .cloned()
            })
            .unwrap_or_else(|| "quick".into());
        let selection = WaitStrategySelection::parse(args, &mode_owned)?;

        let scenarios: Vec<Scenario> = selection
            .scenario_specs(BackendKind::Mmap)
            .into_iter()
            .map(Scenario::from_spec)
            .collect();

        if !selection.output_args.json_mode {
            println!("=== Wait Strategy MMAP Benchmark ===");
            println!("Event size: {} bytes", ELEMENT_SIZE);
            println!("Buffer size: {} slots", BUFFER_SIZE);
            println!("Events per test: {}", NUM_EVENTS);
            println!("Mode: {}", selection.mode_name());
            println!();
        }

        let mut report = BenchReport::new();
        for scenario in scenarios {
            report.add(scenario.run_benchmark()?);
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

impl Scenario {
    fn from_spec(spec: WaitStrategyScenarioSpec) -> Self {
        Self {
            consumers: spec.consumers,
            wait_strategy: spec.wait_strategy,
            producer_role: spec.producer_role,
            consumer_role: spec.consumer_role,
            throughput_unit: spec.throughput_unit,
        }
    }
}
