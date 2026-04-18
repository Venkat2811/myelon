//! Raw disruptor-mp ring benchmark over SHM backend.
//!
//! Two benchmark classes:
//!   --class message  : 144B event, full 128B payload fill + timestamp (matches original ipc_shm)
//!   --class signal   : 64B event, 16B data only (head-to-head vs Alvarez Rosa V5 305M ops/s)
//!   (default)        : runs both
//!
//! Run:   cargo bench -p myelon-bench --bench raw_ring_shm
//! Signal only: cargo bench -p myelon-bench --bench raw_ring_shm -- --class signal

use disruptor_mp::{
    build_shared_single_producer, CoordinationMode, SharedDisruptorBuilder, SharedMemoryConfig,
};
use perf_bench::coordination::BenchmarkCoordination;
use perf_bench::events::nanos_now;
use perf_bench::harness::{self, IpcBenchmark, ScenarioChildren};
use perf_bench::latency::LatencyRecorder;
use perf_bench::reporting::{self, BenchReport};
use std::time::{Duration, Instant};

// ============================================================
// Event types
// ============================================================

/// Message-class event: matches original ipc_shm.rs BenchmarkEvent.
/// 144 bytes: 8 (id) + 8 (timestamp) + 128 (payload).
#[repr(C)]
#[derive(Clone, Copy)]
struct MessageEvent {
    id: u64,
    timestamp: u64,
    payload: [u8; 128],
}

impl Default for MessageEvent {
    fn default() -> Self {
        Self {
            id: 0,
            timestamp: 0,
            payload: [0u8; 128],
        }
    }
}

/// Signal-class event: cache-line aligned, 16 bytes of data.
/// Head-to-head with Alvarez Rosa V5 (305M) and Intel (452M) articles.
/// 64 bytes total (16 data + 48 padding from alignment).
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct SignalEvent {
    sequence: u64,
    data: u64,
}

impl Default for SignalEvent {
    fn default() -> Self {
        Self {
            sequence: 0,
            data: 0,
        }
    }
}

// ============================================================
// Message-class: producer (matches original ipc_shm exactly)
// ============================================================

fn message_producer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = harness::segment_from_env("BENCHMARK_SEGMENT_NAME");
    const BUFFER: usize = 1024;
    const EVENTS: u64 = 100_000;
    const WARMUP: u64 = 1_000;

    // Use discovery + external coordination for proper backpressure.
    // Pattern from disruptor-mp/tests/true_multiprocess.rs:
    //   discover_consumer_with_prefix_and_interval → wait_for_consumers → warmup scans → publish
    let mut producer = build_shared_single_producer::<MessageEvent>(&segment, BUFFER)
        .enable_discovery(1)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(MessageEvent::default)?;

    let coord = BenchmarkCoordination::create(&segment)?;

    if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
        return Err("timeout waiting for consumer".into());
    }

    // Warmup discovery scans so the barrier finds the consumer cursor
    // before the first timed publish (avoids initial overwrite window).
    for _ in 0..20 {
        let _ = producer.min_gating_sequence();
        std::thread::sleep(Duration::from_millis(2));
    }

    // Warmup — full payload fill, matching original
    for i in 0..WARMUP {
        producer.publish(|event| {
            event.id = i;
            event.timestamp = 0;
            event.payload = [(i % 256) as u8; 128];
        });
    }

    // Measured — full 128B payload fill + absolute timestamp on every event
    let start = Instant::now();
    for i in 0..EVENTS {
        producer.publish(|event| {
            event.id = WARMUP + i;
            event.timestamp = nanos_now(); // absolute timestamp for cross-process latency
            event.payload = [((WARMUP + i) % 256) as u8; 128];
        });
    }
    let elapsed = start.elapsed();

    let output =
        harness::ProducerOutput::from_elapsed(EVENTS, elapsed, std::mem::size_of::<MessageEvent>());
    println!("{}", serde_json::to_string(&output)?);

    coord.signal_producer_done(EVENTS as i64);
    coord.wait_for_consumers_done(1, Duration::from_secs(30));
    Ok(())
}

fn message_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = harness::segment_from_env("BENCHMARK_SEGMENT_NAME");
    let consumer_id = harness::read_env_usize("BENCH_CONSUMER_ID", 0);
    const BUFFER: usize = 1024;
    const WARMUP: u64 = 1_000;

    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;

    let config = SharedMemoryConfig {
        name: segment.clone(),
        buffer_size: BUFFER,
        element_size: std::mem::size_of::<MessageEvent>(),
        create: false,
    };
    let mut consumer = SharedDisruptorBuilder::<MessageEvent>::new(config).build_consumer()?;
    coord.signal_consumer_ready();

    // Warmup
    let mut warmup = 0u64;
    while warmup < WARMUP {
        consumer.process_available(|_e, _s| {
            warmup += 1;
        });
        if warmup < WARMUP {
            std::hint::spin_loop();
        }
    }

    // Measured — with real per-event HDR latency
    let mut latency = LatencyRecorder::default_range();
    let start = Instant::now();
    let mut consumed = 0u64;
    let mut checksum = 0u64;
    loop {
        consumer.process_available(|event, _seq| {
            consumed += 1;
            checksum = checksum.wrapping_add(event.id);
            if event.timestamp > 0 {
                latency.record_delta(event.timestamp, nanos_now());
            }
        });
        if coord.is_producer_done() && consumed >= coord.events_produced() as u64 {
            break;
        }
        if consumed < coord.events_produced() as u64 {
            std::hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();

    let output = if let Some(stats) = latency.stats() {
        harness::ConsumerOutput::from_elapsed(
            consumer_id,
            consumed,
            elapsed,
            std::mem::size_of::<MessageEvent>(),
            checksum,
        )
        .with_latency(stats)
    } else {
        harness::ConsumerOutput::from_elapsed(
            consumer_id,
            consumed,
            elapsed,
            std::mem::size_of::<MessageEvent>(),
            checksum,
        )
    };
    println!("{}", serde_json::to_string(&output)?);
    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

// ============================================================
// Signal-class: producer (head-to-head vs Alvarez Rosa V5)
// ============================================================

fn signal_producer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = harness::segment_from_env("BENCHMARK_SEGMENT_NAME");
    const BUFFER: usize = 65_536; // 64K slots — large ring to avoid wraps, matching articles
    const EVENTS: u64 = 10_000_000; // 10M events — run for seconds, not milliseconds
    const WARMUP: u64 = 100_000;

    let mut producer = build_shared_single_producer::<SignalEvent>(&segment, BUFFER)
        .enable_discovery(1)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(SignalEvent::default)?;

    let coord = BenchmarkCoordination::create(&segment)?;

    if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
        return Err("timeout waiting for consumer".into());
    }

    // Warmup discovery scans
    for _ in 0..20 {
        let _ = producer.min_gating_sequence();
        std::thread::sleep(Duration::from_millis(2));
    }

    // Warmup events
    for i in 0..WARMUP {
        producer.publish(|slot| {
            slot.sequence = i;
            slot.data = i.wrapping_mul(0x9E3779B97F4A7C15);
        });
    }

    // Measured — minimal work: write 16 bytes per event
    let start = Instant::now();
    for i in 0..EVENTS {
        producer.publish(|slot| {
            slot.sequence = WARMUP + i;
            slot.data = (WARMUP + i).wrapping_mul(0x9E3779B97F4A7C15);
        });
    }
    let elapsed = start.elapsed();

    let output =
        harness::ProducerOutput::from_elapsed(EVENTS, elapsed, std::mem::size_of::<SignalEvent>());
    println!("{}", serde_json::to_string(&output)?);

    coord.signal_producer_done(EVENTS as i64);
    coord.wait_for_consumers_done(1, Duration::from_secs(30));
    Ok(())
}

fn signal_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = harness::segment_from_env("BENCHMARK_SEGMENT_NAME");
    let consumer_id = harness::read_env_usize("BENCH_CONSUMER_ID", 0);
    const BUFFER: usize = 65_536;
    const WARMUP: u64 = 100_000;

    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;

    let config = SharedMemoryConfig {
        name: segment.clone(),
        buffer_size: BUFFER,
        element_size: std::mem::size_of::<SignalEvent>(),
        create: false,
    };
    let mut consumer = SharedDisruptorBuilder::<SignalEvent>::new(config).build_consumer()?;
    coord.signal_consumer_ready();

    // Warmup
    let mut warmup = 0u64;
    while warmup < WARMUP {
        consumer.process_available(|_e, _s| {
            warmup += 1;
        });
        if warmup < WARMUP {
            std::hint::spin_loop();
        }
    }

    // Measured
    let start = Instant::now();
    let mut consumed = 0u64;
    let mut checksum = 0u64;
    loop {
        consumer.process_available(|event, _s| {
            checksum = checksum.wrapping_add(event.data);
            consumed += 1;
        });
        if coord.is_producer_done() && consumed >= coord.events_produced() as u64 {
            break;
        }
        if consumed < coord.events_produced() as u64 {
            std::hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();

    let output = harness::ConsumerOutput::from_elapsed(
        consumer_id,
        consumed,
        elapsed,
        std::mem::size_of::<SignalEvent>(),
        checksum,
    );
    println!("{}", serde_json::to_string(&output)?);
    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

// ============================================================
// Multi-consumer message class (1p3c, 1p8c, 1p12c)
// ============================================================

fn multi_message_producer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = harness::segment_from_env("BENCHMARK_SEGMENT_NAME");
    let num_consumers = harness::read_env_usize("BENCH_NUM_CONSUMERS", 3);
    let buffer = harness::read_env_usize("BENCH_BUFFER", 4096);
    let events = harness::read_env_u64("BENCH_EVENTS", 100_000);
    let warmup = harness::read_env_u64("BENCH_WARMUP", 1_000);

    let mut producer = build_shared_single_producer::<MessageEvent>(&segment, buffer)
        .enable_discovery(num_consumers)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(MessageEvent::default)?;

    let coord = BenchmarkCoordination::create(&segment)?;

    if !coord.wait_for_consumers(num_consumers, Duration::from_secs(60)) {
        return Err(format!("timeout waiting for {num_consumers} consumers").into());
    }

    // Warmup discovery scans — more iterations for more consumers
    let scan_rounds = 20 + num_consumers * 5;
    for _ in 0..scan_rounds {
        let _ = producer.min_gating_sequence();
        std::thread::sleep(Duration::from_millis(2));
    }

    for i in 0..warmup {
        producer.publish(|event| {
            event.id = i;
            event.timestamp = 0;
            event.payload = [(i % 256) as u8; 128];
        });
    }

    let start = Instant::now();
    for i in 0..events {
        producer.publish(|event| {
            event.id = warmup + i;
            event.timestamp = nanos_now();
            event.payload = [((warmup + i) % 256) as u8; 128];
        });
    }
    let elapsed = start.elapsed();

    let output =
        harness::ProducerOutput::from_elapsed(events, elapsed, std::mem::size_of::<MessageEvent>());
    println!("{}", serde_json::to_string(&output)?);

    coord.signal_producer_done(events as i64);
    coord.wait_for_consumers_done(num_consumers, Duration::from_secs(90));
    Ok(())
}

fn multi_message_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = harness::segment_from_env("BENCHMARK_SEGMENT_NAME");
    let consumer_id = harness::read_env_usize("BENCH_CONSUMER_ID", 0);
    let buffer = harness::read_env_usize("BENCH_BUFFER", 4096);
    let warmup = harness::read_env_u64("BENCH_WARMUP", 1_000);
    let record_latency = harness::read_env_usize("BENCH_RECORD_LATENCY", 0) == 1;

    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;

    let config = SharedMemoryConfig {
        name: segment.clone(),
        buffer_size: buffer,
        element_size: std::mem::size_of::<MessageEvent>(),
        create: false,
    };
    let mut consumer = SharedDisruptorBuilder::<MessageEvent>::new(config).build_consumer()?;
    coord.signal_consumer_ready();

    // Warmup
    let mut warmup_count = 0u64;
    while warmup_count < warmup {
        consumer.process_available(|_e, _s| {
            warmup_count += 1;
        });
        if warmup_count < warmup {
            std::hint::spin_loop();
        }
    }

    // Measured
    let mut latency = if record_latency {
        Some(LatencyRecorder::default_range())
    } else {
        None
    };
    let start = Instant::now();
    let mut consumed = 0u64;
    let mut checksum = 0u64;
    loop {
        consumer.process_available(|event, _seq| {
            consumed += 1;
            checksum = checksum.wrapping_add(event.id);
            if let Some(ref mut lat) = latency {
                if event.timestamp > 0 {
                    lat.record_delta(event.timestamp, nanos_now());
                }
            }
        });
        if coord.is_producer_done() && consumed >= coord.events_produced() as u64 {
            break;
        }
        if consumed < coord.events_produced() as u64 {
            std::hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();

    let latency_stats = latency.as_ref().and_then(|recorder| recorder.stats());
    let output = if let Some(stats) = latency_stats {
        harness::ConsumerOutput::from_elapsed(
            consumer_id,
            consumed,
            elapsed,
            std::mem::size_of::<MessageEvent>(),
            checksum,
        )
        .with_latency(stats)
    } else {
        harness::ConsumerOutput::from_elapsed(
            consumer_id,
            consumed,
            elapsed,
            std::mem::size_of::<MessageEvent>(),
            checksum,
        )
    };
    println!("{}", serde_json::to_string(&output)?);
    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

// ============================================================
// Orchestrator
// ============================================================

#[derive(Clone, Copy)]
struct Scenario {
    label: &'static str,
    producer_role: &'static str,
    consumer_role: &'static str,
    event_bytes: usize,
    events: u64,
    buffer: usize,
    warmup: u64,
    consumers: usize,
    record_latency: bool,
}

impl IpcBenchmark for Scenario {
    fn bench_name(&self) -> &str {
        "raw_ring_shm"
    }

    fn scenario_name(&self) -> String {
        self.label.to_string()
    }

    fn backend(&self) -> &str {
        "shm"
    }

    fn layer(&self) -> &str {
        "raw_ring"
    }

    fn message_size_bytes(&self) -> usize {
        self.event_bytes
    }

    fn buffer_depth(&self) -> usize {
        self.buffer
    }

    fn num_messages(&self) -> u64 {
        self.events
    }

    fn warmup_messages(&self) -> u64 {
        self.warmup
    }

    fn num_consumers(&self) -> usize {
        self.consumers
    }

    fn timeout(&self) -> Duration {
        if self.consumers > 1 {
            Duration::from_secs(180)
        } else {
            Duration::from_secs(120)
        }
    }

    fn consumer_summary_label(&self) -> &str {
        if self.consumers == 1 {
            "consumer"
        } else {
            "avg consumer"
        }
    }

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, harness::BenchError> {
        let prefix = if self.consumers > 1 {
            "raw_ring_multi"
        } else {
            "raw_ring"
        };
        let segment = harness::unique_shm_segment(prefix);
        let envs: Vec<(&str, String)> = vec![
            ("BENCHMARK_SEGMENT_NAME", segment.clone()),
            ("BENCH_NUM_CONSUMERS", self.consumers.to_string()),
            ("BENCH_BUFFER", self.buffer.to_string()),
            ("BENCH_EVENTS", self.events.to_string()),
            ("BENCH_WARMUP", self.warmup.to_string()),
        ];

        let producer = harness::spawn_child(exe, self.producer_role, &envs);
        let consumers = (0..self.consumers)
            .map(|consumer_id| {
                let mut consumer_envs = envs.clone();
                if self.record_latency {
                    consumer_envs.push(("BENCH_RECORD_LATENCY", "1".to_string()));
                }
                consumer_envs.push(("BENCH_CONSUMER_ID", consumer_id.to_string()));
                harness::spawn_child(exe, self.consumer_role, &consumer_envs)
            })
            .collect();

        Ok(ScenarioChildren::new(producer, consumers))
    }
}

// ============================================================
// main
// ============================================================

const CHILD_ROLES: &[harness::ChildRole] = &[
    harness::ChildRole::new("msg_producer", message_producer),
    harness::ChildRole::new("msg_consumer", message_consumer),
    harness::ChildRole::new("sig_producer", signal_producer),
    harness::ChildRole::new("sig_consumer", signal_consumer),
    harness::ChildRole::new("msg_multi_producer", multi_message_producer),
    harness::ChildRole::new("msg_multi_consumer", multi_message_consumer),
];

struct RawRingShmBench;

impl harness::BenchHarness for RawRingShmBench {
    fn bench_name(&self) -> &'static str {
        "raw_ring_shm"
    }

    fn child_roles(&self) -> &'static [harness::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> harness::BenchRunResult {
        let class = args
            .windows(2)
            .find(|w| w[0] == "--class")
            .map(|w| w[1].as_str())
            .unwrap_or("all");
        let consumers_arg = args
            .windows(2)
            .find(|w| w[0] == "--consumers")
            .map(|w| w[1].as_str())
            .unwrap_or("all");
        let output_args = reporting::ReportOutputArgs::from_args(args);

        let run_message = class == "all" || class == "message";
        let run_signal = class == "all" || class == "signal";

        if !output_args.json_mode {
            println!("=== Raw Ring SHM Benchmark ===");
            println!("Backend: POSIX shared memory");
            if run_message {
                println!(
                    "Message class: {}B event, full 128B payload fill + timestamp",
                    std::mem::size_of::<MessageEvent>()
                );
            }
            if run_signal {
                println!("Signal class:  {}B event ({}B data), 10M events, 64K buffer — vs Alvarez Rosa V5 (305M ops/s)",
                    std::mem::size_of::<SignalEvent>(), 16);
            }
            println!();
        }

        let scenarios = [
            Scenario {
                label: "message_1p1c_144B",
                producer_role: "msg_producer",
                consumer_role: "msg_consumer",
                event_bytes: std::mem::size_of::<MessageEvent>(),
                events: 100_000,
                buffer: 1024,
                warmup: 1_000,
                consumers: 1,
                record_latency: true,
            },
            Scenario {
                label: "message_1p3c_144B",
                producer_role: "msg_multi_producer",
                consumer_role: "msg_multi_consumer",
                event_bytes: std::mem::size_of::<MessageEvent>(),
                events: 100_000,
                buffer: 1024,
                warmup: 1_000,
                consumers: 3,
                record_latency: true,
            },
            Scenario {
                label: "message_1p8c_144B",
                producer_role: "msg_multi_producer",
                consumer_role: "msg_multi_consumer",
                event_bytes: std::mem::size_of::<MessageEvent>(),
                events: 100_000,
                buffer: 4096,
                warmup: 1_000,
                consumers: 8,
                record_latency: false,
            },
            Scenario {
                label: "message_1p12c_144B",
                producer_role: "msg_multi_producer",
                consumer_role: "msg_multi_consumer",
                event_bytes: std::mem::size_of::<MessageEvent>(),
                events: 100_000,
                buffer: 4096,
                warmup: 1_000,
                consumers: 12,
                record_latency: false,
            },
            Scenario {
                label: "signal_1p1c_64B",
                producer_role: "sig_producer",
                consumer_role: "sig_consumer",
                event_bytes: std::mem::size_of::<SignalEvent>(),
                events: 10_000_000,
                buffer: 65_536,
                warmup: 100_000,
                consumers: 1,
                record_latency: false,
            },
        ];

        let mut report = BenchReport::new();
        for scenario in scenarios {
            let is_message = scenario.label.starts_with("message_");
            let should_run = (is_message && run_message) || (!is_message && run_signal);
            let consumer_matches = consumers_arg == "all"
                || consumers_arg.parse::<usize>().ok() == Some(scenario.consumers);
            if should_run && (scenario.consumers == 1 || consumer_matches) {
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

perf_bench::myelon_bench_main!(RawRingShmBench);
