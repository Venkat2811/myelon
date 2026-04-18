//! mmap wait strategy benchmark modeled after the battle-tested SHM matrix.

use perf_bench::events::format_throughput;
use perf_bench::harness::{
    self, mmap_layout_from_env, read_env_string, spawn_child, unique_mmap_root,
    unique_mmap_segment, ConsumerOutput, IpcBenchmark, ProducerOutput, ScenarioChildren,
};
use perf_bench::reporting::{self, BenchReport};
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
        "events/s"
    }

    fn print_summary_with_metrics(
        &self,
        producer: &harness::ProducerOutput,
        consumers: &[harness::ConsumerOutput],
        _latency: Option<&perf_bench::latency::LatencyStats>,
    ) {
        println!(
            "  {:<6} {:<24} producer: {:>10} events/s  avg consumer: {:>10} events/s",
            format!("1p{}c", self.consumers),
            self.wait_strategy,
            format_throughput(producer.throughput_ops_sec),
            format_throughput(self.average_consumer_ops(consumers)),
        );
    }

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, harness::BenchError> {
        let root = unique_mmap_root("wait_mmap");
        let segment = unique_mmap_segment("wait");
        let root_str = root.display().to_string();
        let envs: Vec<(&str, String)> = vec![
            ("MMAP_ROOT", root_str.clone()),
            ("MMAP_SEGMENT", segment.clone()),
            ("NUM_CONSUMERS", self.consumers.to_string()),
            ("WAIT_STRATEGY", self.wait_strategy.to_string()),
        ];

        let producer = spawn_child(exe, "mmap_wait_producer", &envs);
        let consumers = (0..self.consumers)
            .map(|index| {
                let mut consumer_envs = envs.clone();
                consumer_envs.push(("CONSUMER_ID", index.to_string()));
                spawn_child(exe, "mmap_wait_consumer", &consumer_envs)
            })
            .collect();

        Ok(ScenarioChildren::new(producer, consumers).with_cleanup_path(root))
    }
}

const CHILD_ROLES: &[harness::ChildRole] = &[
    harness::ChildRole::new("mmap_wait_producer", producer_process),
    harness::ChildRole::new("mmap_wait_consumer", consumer_process),
];

struct WaitStrategyMmapBench;

impl harness::BenchHarness for WaitStrategyMmapBench {
    fn bench_name(&self) -> &'static str {
        "wait_strategy_mmap"
    }

    fn child_roles(&self) -> &'static [harness::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> harness::BenchRunResult {
        let output_args = reporting::ReportOutputArgs::from_args(args);
        let mode_owned = env::var("BENCH_MODE")
            .ok()
            .or_else(|| {
                args.iter()
                    .skip(1)
                    .find(|arg| !arg.starts_with("--") && arg.as_str() != "wait_strategy_mmap")
                    .cloned()
            })
            .unwrap_or_else(|| "quick".into());
        let mode = mode_owned.as_str();

        let scenarios: Vec<Scenario> = match mode {
            "quick" => vec![Scenario {
                consumers: 1,
                wait_strategy: "BusySpin",
            }],
            "full" | "comprehensive" => {
                let consumer_counts = [1usize, 2, 4, 6, 8, 10, 12];
                let wait_strategies = ["BusySpin", "Block", "Sleep", "BusySpinWithSpinLoopHint"];
                consumer_counts
                    .into_iter()
                    .flat_map(|consumers| {
                        wait_strategies
                            .into_iter()
                            .map(move |wait_strategy| Scenario {
                                consumers,
                                wait_strategy,
                            })
                    })
                    .collect()
            }
            other => {
                return Err(
                    format!("Unknown mode '{other}', expected quick|full|comprehensive").into(),
                );
            }
        };

        if !output_args.json_mode {
            println!("=== Wait Strategy MMAP Benchmark ===");
            println!("Event size: {} bytes", ELEMENT_SIZE);
            println!("Buffer size: {} slots", BUFFER_SIZE);
            println!("Events per test: {}", NUM_EVENTS);
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

perf_bench::myelon_bench_main!(WaitStrategyMmapBench);
