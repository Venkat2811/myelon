//! Wait strategy benchmark over SHM — 4 strategies x 7 consumer counts.
//!
//! Modes: quick (1p1c `BusySpin`), full (28 combos).
//!
//! Run: cargo bench -p perf-bench --bench `wait_strategy_shm`
//! Full: `BENCH_MODE=full` cargo bench -p perf-bench --bench `wait_strategy_shm`

use crate::cli::wait_strategy::{WaitStrategyScenarioSpec, WaitStrategySelection};
use crate::infra::coordination::BenchmarkCoordination;
use crate::infra::events::format_throughput;
use crate::infra::output::report::BackendKind;
use crate::infra::output::reporting::{self, BenchReport};
use crate::infra::{self, read_env_usize, IpcBenchmark, ScenarioChildren};
use disruptor_mp::{
    build_shared_single_producer, CoordinationMode, SharedDisruptorBuilder, SharedMemoryConfig,
};
use std::time::{Duration, Instant};

const BUFFER_SIZE: usize = 64 * 1024;
const NUM_EVENTS: u64 = 100_000;

fn num_events() -> u64 {
    std::env::var("PERF_BENCH_WAIT_NUM_EVENTS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(NUM_EVENTS)
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Event {
    sequence: u64,
    timestamp_ns: u64,
    payload: [u8; 112],
}
impl Default for Event {
    fn default() -> Self {
        Self {
            sequence: 0,
            timestamp_ns: 0,
            payload: [0u8; 112],
        }
    }
}

// ============================================================
// Producer
// ============================================================

fn producer_process() -> Result<(), Box<dyn std::error::Error>> {
    let segment = infra::segment_from_env("BENCHMARK_SEGMENT_NAME");
    let num_consumers = read_env_usize("NUM_CONSUMERS", 1);

    let mut producer = build_shared_single_producer::<Event>(&segment, BUFFER_SIZE)
        .enable_discovery(num_consumers)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(Event::default)?;

    let coord = BenchmarkCoordination::create(&segment)?;
    if !coord.wait_for_consumers(num_consumers, Duration::from_secs(60)) {
        return Err(format!("timeout waiting for {num_consumers} consumers").into());
    }

    let scans = 20 + num_consumers * 5;
    for _ in 0..scans {
        let _ = producer.min_gating_sequence();
        std::thread::sleep(Duration::from_millis(2));
    }

    let start = Instant::now();
    let num_events = num_events();
    for i in 0..num_events {
        producer.publish(|e| {
            e.sequence = i;
            e.payload.fill((i % 256) as u8);
        });
    }
    let elapsed = start.elapsed();

    let output =
        infra::ProducerOutput::from_elapsed(num_events, elapsed, std::mem::size_of::<Event>());
    println!("{}", serde_json::to_string(&output)?);
    coord.signal_producer_done(num_events as i64);
    coord.wait_for_consumers_done(num_consumers, Duration::from_secs(90));
    Ok(())
}

// ============================================================
// Consumer
// ============================================================

fn consumer_process() -> Result<(), Box<dyn std::error::Error>> {
    let segment = infra::segment_from_env("BENCHMARK_SEGMENT_NAME");
    let consumer_id = read_env_usize("CONSUMER_ID", 0);
    let wait_strategy = infra::read_env_string("WAIT_STRATEGY", "BusySpin");

    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
    let config = SharedMemoryConfig {
        name: segment,
        buffer_size: BUFFER_SIZE,
        element_size: std::mem::size_of::<Event>(),
        create: false,
    };
    let mut consumer = SharedDisruptorBuilder::<Event>::new(config).build_consumer()?;
    coord.signal_consumer_ready();

    let mut consumed = 0u64;
    let mut start: Option<Instant> = None;
    let mut checksum = 0u64;
    let deadline = infra::spin_deadline();
    let num_events = num_events();

    while consumed < num_events {
        consumer.process_available(|e, _s| {
            if start.is_none() {
                start = Some(Instant::now());
            }
            let payload_sum = e.payload.iter().fold(0u8, |a, &b| a.wrapping_add(b));
            std::hint::black_box(payload_sum);
            checksum = checksum.wrapping_add(payload_sum as u64);
            consumed += 1;
        });
        if consumed < num_events {
            infra::check_deadline(deadline, "wait_strategy_shm consumer_process");
            match wait_strategy.as_str() {
                "BusySpin" => {}
                "BusySpinWithSpinLoopHint" => std::hint::spin_loop(),
                "Sleep" => disruptor_mp::perform_default_consume_sleep_wait(),
                "Block" => disruptor_mp::perform_default_block_wait(),
                _ => std::hint::spin_loop(),
            }
        }
    }
    let elapsed = start.unwrap().elapsed();

    let output = infra::ConsumerOutput::from_elapsed(
        consumer_id,
        consumed,
        elapsed,
        std::mem::size_of::<Event>(),
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
    consumers: usize,
    wait_strategy: &'static str,
    producer_role: &'static str,
    consumer_role: &'static str,
    throughput_unit: &'static str,
}

impl IpcBenchmark for Scenario {
    fn bench_name(&self) -> &str {
        "wait_strategy_shm"
    }

    fn scenario_name(&self) -> String {
        format!("1p{}c_{}", self.consumers, self.wait_strategy)
    }

    fn backend(&self) -> &str {
        "shm"
    }

    fn layer(&self) -> &str {
        "wait_strategy"
    }

    fn transport_metadata(&self) -> reporting::BenchTransportSpec {
        reporting::BenchTransportSpec::benchmark_shm(self.consumers)
            .with_zero_copy(false)
            .with_framing("none")
    }

    fn wait_strategy(&self) -> &str {
        self.wait_strategy
    }

    fn message_size_bytes(&self) -> usize {
        std::mem::size_of::<Event>()
    }

    fn buffer_depth(&self) -> usize {
        BUFFER_SIZE
    }

    fn num_messages(&self) -> u64 {
        num_events()
    }

    fn num_consumers(&self) -> usize {
        self.consumers
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
        let segment =
            infra::unique_shm_segment(&format!("ws_{}_{}c", self.wait_strategy, self.consumers));
        let envs: Vec<(&str, String)> = vec![
            ("BENCHMARK_SEGMENT_NAME", segment.clone()),
            ("NUM_CONSUMERS", self.consumers.to_string()),
            ("WAIT_STRATEGY", self.wait_strategy.to_string()),
        ];

        let producer = infra::spawn_child(exe, self.producer_role, &envs);
        let consumers = (0..self.consumers)
            .map(|consumer_id| {
                let mut consumer_envs = envs.clone();
                consumer_envs.push(("CONSUMER_ID", consumer_id.to_string()));
                infra::spawn_child(exe, self.consumer_role, &consumer_envs)
            })
            .collect();

        Ok(ScenarioChildren::new(producer, consumers))
    }
}

const CHILD_ROLES: &[infra::ChildRole] = &[
    infra::ChildRole::new("shm_wait_producer", producer_process),
    infra::ChildRole::new("shm_wait_consumer", consumer_process),
];

pub struct WaitStrategyShmBench;

impl infra::BenchHarness for WaitStrategyShmBench {
    fn bench_name(&self) -> &'static str {
        "wait_strategy_shm"
    }

    fn child_roles(&self) -> &'static [infra::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> infra::BenchRunResult {
        let mode = infra::read_env_string(
            "BENCH_MODE",
            args.iter()
                .skip(1)
                .find(|a| !a.starts_with("--"))
                .map(|s| s.as_str())
                .unwrap_or("quick"),
        );
        let selection = WaitStrategySelection::parse(args, &mode)?;

        let scenarios: Vec<Scenario> = selection
            .scenario_specs(BackendKind::Shm)
            .into_iter()
            .map(Scenario::from_spec)
            .collect();

        if !selection.output_args.json_mode {
            println!("=== Wait Strategy SHM Benchmark ===");
            println!(
                "Event: {} bytes, Buffer: {} slots ({}MB), Events: {}, Mode: {} ({} scenarios)",
                std::mem::size_of::<Event>(),
                BUFFER_SIZE,
                BUFFER_SIZE * std::mem::size_of::<Event>() / (1024 * 1024),
                num_events(),
                selection.mode_name(),
                scenarios.len()
            );
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
