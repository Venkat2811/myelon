//! Wait strategy benchmark over SHM — 4 strategies x 7 consumer counts.
//!
//! Modes: quick (1p1c BusySpin), full (28 combos).
//!
//! Run: cargo bench -p perf-bench --bench wait_strategy_shm
//! Full: BENCH_MODE=full cargo bench -p perf-bench --bench wait_strategy_shm

use disruptor_mp::{
    build_shared_single_producer, CoordinationMode, SharedDisruptorBuilder, SharedMemoryConfig,
};
use perf_bench::coordination::BenchmarkCoordination;
use perf_bench::events::format_throughput;
use perf_bench::harness::{self, read_env_usize, IpcBenchmark, ScenarioChildren};
use perf_bench::reporting::{self, BenchReport};
use std::time::{Duration, Instant};

const BUFFER_SIZE: usize = 64 * 1024;
const NUM_EVENTS: u64 = 100_000;

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
    let segment = harness::segment_from_env("BENCHMARK_SEGMENT_NAME");
    let num_consumers = read_env_usize("NUM_CONSUMERS", 1);

    let mut producer = build_shared_single_producer::<Event>(&segment, BUFFER_SIZE)
        .enable_discovery(num_consumers)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(|| Event::default())?;

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
    for i in 0..NUM_EVENTS {
        producer.publish(|e| {
            e.sequence = i;
            e.payload = [(i % 256) as u8; 112];
        });
    }
    let elapsed = start.elapsed();

    let output =
        harness::ProducerOutput::from_elapsed(NUM_EVENTS, elapsed, std::mem::size_of::<Event>());
    println!("{}", serde_json::to_string(&output)?);
    coord.signal_producer_done(NUM_EVENTS as i64);
    coord.wait_for_consumers_done(num_consumers, Duration::from_secs(90));
    Ok(())
}

// ============================================================
// Consumer
// ============================================================

fn consumer_process() -> Result<(), Box<dyn std::error::Error>> {
    let segment = harness::segment_from_env("BENCHMARK_SEGMENT_NAME");
    let consumer_id = read_env_usize("CONSUMER_ID", 0);
    let wait_strategy = harness::read_env_string("WAIT_STRATEGY", "BusySpin");

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

    while consumed < NUM_EVENTS {
        consumer.process_available(|e, _s| {
            if start.is_none() {
                start = Some(Instant::now());
            }
            let payload_sum = e.payload.iter().fold(0u8, |a, &b| a.wrapping_add(b));
            std::hint::black_box(payload_sum);
            checksum = checksum.wrapping_add(payload_sum as u64);
            consumed += 1;
        });
        if consumed < NUM_EVENTS {
            match wait_strategy.as_str() {
                "BusySpin" => {}
                "BusySpinWithSpinLoopHint" => std::hint::spin_loop(),
                "Sleep" => std::thread::sleep(Duration::from_micros(1)),
                "Block" => std::thread::sleep(Duration::from_millis(1)),
                _ => std::hint::spin_loop(),
            }
        }
    }
    let elapsed = start.unwrap().elapsed();

    let output = harness::ConsumerOutput::from_elapsed(
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
        NUM_EVENTS
    }

    fn num_consumers(&self) -> usize {
        self.consumers
    }

    fn print_summary_with_metrics(
        &self,
        producer: &harness::ProducerOutput,
        consumers: &[harness::ConsumerOutput],
        _latency: Option<&perf_bench::latency::LatencyStats>,
    ) {
        println!(
            "  {:<6} {:<24} producer: {:>10} ops/s  avg consumer: {:>10} ops/s",
            format!("1p{}c", self.consumers),
            self.wait_strategy,
            format_throughput(producer.throughput_ops_sec),
            format_throughput(self.average_consumer_ops(consumers)),
        );
    }

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, harness::BenchError> {
        let segment =
            harness::unique_shm_segment(&format!("ws_{}_{}c", self.wait_strategy, self.consumers));
        let envs: Vec<(&str, String)> = vec![
            ("BENCHMARK_SEGMENT_NAME", segment.clone()),
            ("NUM_CONSUMERS", self.consumers.to_string()),
            ("WAIT_STRATEGY", self.wait_strategy.to_string()),
        ];

        let producer = harness::spawn_child(exe, "shm_wait_producer", &envs);
        let consumers = (0..self.consumers)
            .map(|consumer_id| {
                let mut consumer_envs = envs.clone();
                consumer_envs.push(("CONSUMER_ID", consumer_id.to_string()));
                harness::spawn_child(exe, "shm_wait_consumer", &consumer_envs)
            })
            .collect();

        Ok(ScenarioChildren::new(producer, consumers))
    }
}

const CHILD_ROLES: &[harness::ChildRole] = &[
    harness::ChildRole::new("shm_wait_producer", producer_process),
    harness::ChildRole::new("shm_wait_consumer", consumer_process),
];

struct WaitStrategyShmBench;

impl harness::BenchHarness for WaitStrategyShmBench {
    fn bench_name(&self) -> &'static str {
        "wait_strategy_shm"
    }

    fn child_roles(&self) -> &'static [harness::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> harness::BenchRunResult {
        let output_args = reporting::ReportOutputArgs::from_args(args);
        let mode = harness::read_env_string(
            "BENCH_MODE",
            args.iter()
                .skip(1)
                .find(|a| !a.starts_with("--"))
                .map(|s| s.as_str())
                .unwrap_or("quick"),
        );

        let scenarios: Vec<Scenario> = match mode.as_str() {
            "quick" => vec![Scenario {
                consumers: 1,
                wait_strategy: "BusySpin",
            }],
            "full" | "comprehensive" => {
                let counts = [1usize, 2, 4, 6, 8, 10, 12];
                let strategies = ["BusySpin", "Block", "Sleep", "BusySpinWithSpinLoopHint"];
                counts
                    .into_iter()
                    .flat_map(|consumers| {
                        strategies.into_iter().map(move |wait_strategy| Scenario {
                            consumers,
                            wait_strategy,
                        })
                    })
                    .collect()
            }
            other => {
                return Err(format!("Unknown mode '{other}', expected quick|full").into());
            }
        };

        if !output_args.json_mode {
            println!("=== Wait Strategy SHM Benchmark ===");
            println!(
                "Event: {} bytes, Buffer: {} slots ({}MB), Events: {}, Mode: {} ({} scenarios)",
                std::mem::size_of::<Event>(),
                BUFFER_SIZE,
                BUFFER_SIZE * std::mem::size_of::<Event>() / (1024 * 1024),
                NUM_EVENTS,
                mode,
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
            &output_args,
            Some(reporting::ReportView::Summary),
            None,
            None,
        );
        Ok(())
    }
}

perf_bench::myelon_bench_main!(WaitStrategyShmBench);
