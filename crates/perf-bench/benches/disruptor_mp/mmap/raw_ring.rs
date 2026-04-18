//! Raw disruptor-mp ring benchmark over mmap (file-backed) backend.
//!
//! Two benchmark classes (matching raw_ring_shm exactly):
//!   --class message  : 144B event, full 128B payload fill + timestamp
//!   --class signal   : 64B event, 16B data only
//!   (default)        : runs both
//!
//! Run:   cargo bench -p myelon-bench --bench raw_ring_mmap
//! Signal only: cargo bench -p myelon-bench --bench raw_ring_mmap -- --class signal

use disruptor_mp::{AutoWaitStrategy, MmapConsumer, MmapProducer, MmapTransportLayout};
use perf_bench::events::nanos_now;
use perf_bench::harness::{self, IpcBenchmark, ScenarioChildren};
use perf_bench::latency::LatencyRecorder;
use perf_bench::reporting::{self, BenchReport};
use std::env;
use std::process::Child;
use std::time::{Duration, Instant};

// ============================================================
// Event types (identical to raw_ring_shm)
// ============================================================

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
// Helpers
// ============================================================

fn child_layout() -> MmapTransportLayout {
    harness::mmap_layout_from_env("MMAP_ROOT", "MMAP_SEGMENT")
}

fn spawn_mmap_child_with_env(
    exe: &std::path::Path,
    role: &str,
    root: &str,
    segment: &str,
    events: u64,
    buffer: usize,
    extra_env: &[(&str, String)],
) -> Child {
    let mut envs = vec![
        ("MMAP_ROOT", root.to_string()),
        ("MMAP_SEGMENT", segment.to_string()),
        ("MMAP_BUFFER_SIZE", buffer.to_string()),
        ("MMAP_EVENTS", events.to_string()),
    ];
    envs.extend(extra_env.iter().map(|(key, value)| (*key, value.clone())));
    harness::spawn_child(exe, role, &envs)
}

// ============================================================
// Message-class producer (full 128B fill + timestamp — matches SHM message class)
// ============================================================

fn message_producer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    const WARMUP: u64 = 1_000;

    layout.ensure_directories()?;
    let mut producer =
        MmapProducer::<MessageEvent>::create(layout, buffer_size, MessageEvent::default)?;

    if !producer.wait_for_consumers_ready(1, Duration::from_secs(30)) {
        return Err("Timeout waiting for consumer".into());
    }

    // Warmup — full payload fill
    for i in 0..WARMUP {
        producer.publish(|e| {
            e.id = i;
            e.timestamp = 0;
            e.payload = [(i % 256) as u8; 128];
        });
    }

    // Measured — identical work to SHM message class, absolute timestamps
    let start = Instant::now();
    for i in 0..num_events {
        producer.publish(|e| {
            e.id = WARMUP + i;
            e.timestamp = nanos_now();
            e.payload = [((WARMUP + i) % 256) as u8; 128];
        });
    }
    let elapsed = start.elapsed();

    let output = harness::ProducerOutput::from_elapsed(
        num_events,
        elapsed,
        std::mem::size_of::<MessageEvent>(),
    );
    println!("{}", serde_json::to_string(&output)?);

    let last_seq = (WARMUP + num_events - 1) as i64;
    producer.wait_until_consumed_with_strategy(
        last_seq,
        Duration::from_secs(30),
        AutoWaitStrategy::BusySpin,
    );
    Ok(())
}

fn message_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    let consumer_id = harness::read_env_usize("MMAP_CONSUMER_ID", 0);
    let consumer_name = format!("c{}", consumer_id);
    const WARMUP: u64 = 1_000;

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapConsumer::<MessageEvent>::attach(layout.clone(), buffer_size, &consumer_name) {
            Ok(c) => break c,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(e) => return Err(format!("attach failed: {e}").into()),
        }
    };

    // Warmup
    let mut warmup = 0u64;
    while warmup < WARMUP {
        if consumer.try_consume_next().is_some() {
            warmup += 1;
        } else {
            std::hint::spin_loop();
        }
    }

    // Measured — with real per-event HDR latency
    let mut latency = LatencyRecorder::default_range();
    let start = Instant::now();
    let mut consumed = 0u64;
    let mut checksum = 0u64;
    while consumed < num_events {
        if let Some((_seq, event)) = consumer.try_consume_next() {
            consumed += 1;
            checksum = checksum.wrapping_add(event.id);
            if event.timestamp > 0 {
                latency.record_delta(event.timestamp, nanos_now());
            }
        } else {
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
    Ok(())
}

// ============================================================
// Signal-class producer (64B, 16B data — matches SHM signal class)
// ============================================================

fn signal_producer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    const WARMUP: u64 = 100_000;

    layout.ensure_directories()?;
    let mut producer =
        MmapProducer::<SignalEvent>::create(layout, buffer_size, SignalEvent::default)?;

    if !producer.wait_for_consumers_ready(1, Duration::from_secs(30)) {
        return Err("Timeout waiting for consumer".into());
    }

    for i in 0..WARMUP {
        producer.publish(|s| {
            s.sequence = i;
            s.data = i.wrapping_mul(0x9E3779B97F4A7C15);
        });
    }

    let start = Instant::now();
    for i in 0..num_events {
        producer.publish(|s| {
            s.sequence = WARMUP + i;
            s.data = (WARMUP + i).wrapping_mul(0x9E3779B97F4A7C15);
        });
    }
    let elapsed = start.elapsed();

    let output = harness::ProducerOutput::from_elapsed(
        num_events,
        elapsed,
        std::mem::size_of::<SignalEvent>(),
    );
    println!("{}", serde_json::to_string(&output)?);

    let last_seq = (WARMUP + num_events - 1) as i64;
    producer.wait_until_consumed_with_strategy(
        last_seq,
        Duration::from_secs(30),
        AutoWaitStrategy::BusySpin,
    );
    Ok(())
}

fn signal_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    let consumer_id = harness::read_env_usize("MMAP_CONSUMER_ID", 0);
    let consumer_name = format!("c{}", consumer_id);
    const WARMUP: u64 = 100_000;

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapConsumer::<SignalEvent>::attach(layout.clone(), buffer_size, &consumer_name) {
            Ok(c) => break c,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(e) => return Err(format!("attach failed: {e}").into()),
        }
    };

    let mut warmup = 0u64;
    while warmup < WARMUP {
        if consumer.try_consume_next().is_some() {
            warmup += 1;
        } else {
            std::hint::spin_loop();
        }
    }

    let start = Instant::now();
    let mut consumed = 0u64;
    let mut checksum = 0u64;
    while consumed < num_events {
        if let Some((_seq, event)) = consumer.try_consume_next() {
            checksum = checksum.wrapping_add(event.data);
            consumed += 1;
        } else {
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
    Ok(())
}

// ============================================================
// Multi-consumer message class (1p3c, 1p8c, 1p12c)
// ============================================================

fn multi_message_producer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    let num_consumers = harness::read_env_usize("MMAP_NUM_CONSUMERS", 3);
    const WARMUP: u64 = 1_000;

    layout.ensure_directories()?;
    let mut producer =
        MmapProducer::<MessageEvent>::create(layout, buffer_size, MessageEvent::default)?;

    if !producer.wait_for_consumers_ready(num_consumers as i64, Duration::from_secs(60)) {
        return Err(format!("Timeout waiting for {num_consumers} consumers").into());
    }

    for i in 0..WARMUP {
        producer.publish(|e| {
            e.id = i;
            e.timestamp = 0;
            e.payload = [(i % 256) as u8; 128];
        });
    }

    let start = Instant::now();
    for i in 0..num_events {
        producer.publish(|e| {
            e.id = WARMUP + i;
            e.timestamp = nanos_now();
            e.payload = [((WARMUP + i) % 256) as u8; 128];
        });
    }
    let elapsed = start.elapsed();

    let output = harness::ProducerOutput::from_elapsed(
        num_events,
        elapsed,
        std::mem::size_of::<MessageEvent>(),
    );
    println!("{}", serde_json::to_string(&output)?);

    let last_seq = (WARMUP + num_events - 1) as i64;
    producer.wait_until_consumed_with_strategy(
        last_seq,
        Duration::from_secs(90),
        AutoWaitStrategy::BusySpin,
    );
    Ok(())
}

fn multi_message_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    let record_latency = harness::read_env_usize("MMAP_RECORD_LATENCY", 0) == 1;
    let consumer_id = harness::read_env_usize("MMAP_CONSUMER_ID", 0);
    let consumer_name = format!("c{}", consumer_id);
    const WARMUP: u64 = 1_000;

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapConsumer::<MessageEvent>::attach(layout.clone(), buffer_size, &consumer_name) {
            Ok(c) => break c,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(e) => return Err(format!("attach failed: {e}").into()),
        }
    };

    let mut warmup_count = 0u64;
    while warmup_count < WARMUP {
        if consumer.try_consume_next().is_some() {
            warmup_count += 1;
        } else {
            std::hint::spin_loop();
        }
    }

    let mut latency = if record_latency {
        Some(LatencyRecorder::default_range())
    } else {
        None
    };
    let start = Instant::now();
    let mut consumed = 0u64;
    let mut checksum = 0u64;
    while consumed < num_events {
        if let Some((_seq, event)) = consumer.try_consume_next() {
            consumed += 1;
            checksum = checksum.wrapping_add(event.id);
            if let Some(ref mut lat) = latency {
                if event.timestamp > 0 {
                    lat.record_delta(event.timestamp, nanos_now());
                }
            }
        } else {
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
        "raw_ring_mmap"
    }

    fn scenario_name(&self) -> String {
        self.label.to_string()
    }

    fn backend(&self) -> &str {
        "mmap"
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
        let root = if self.consumers > 1 {
            harness::unique_mmap_root(&format!("multi{}c", self.consumers))
        } else {
            harness::unique_mmap_root(self.label)
        };
        let segment = if self.consumers > 1 {
            harness::unique_mmap_segment(&format!("multi{}c", self.consumers))
        } else {
            harness::unique_mmap_segment(self.label)
        };
        let root_str = root.display().to_string();
        let extra_env: Vec<(&str, String)> =
            vec![("MMAP_NUM_CONSUMERS", self.consumers.to_string())];

        let producer = spawn_mmap_child_with_env(
            exe,
            self.producer_role,
            &root_str,
            &segment,
            self.events,
            self.buffer,
            &extra_env,
        );

        let consumers = (0..self.consumers)
            .map(|consumer_id| {
                let mut env = extra_env.clone();
                if self.record_latency {
                    env.push(("MMAP_RECORD_LATENCY", "1".to_string()));
                }
                env.push(("MMAP_CONSUMER_ID", consumer_id.to_string()));
                spawn_mmap_child_with_env(
                    exe,
                    self.consumer_role,
                    &root_str,
                    &segment,
                    self.events,
                    self.buffer,
                    &env,
                )
            })
            .collect();

        Ok(ScenarioChildren::new(producer, consumers).with_cleanup_path(root))
    }
}

const CHILD_ROLES: &[harness::ChildRole] = &[
    harness::ChildRole::new("mmap_msg_producer", message_producer),
    harness::ChildRole::new("mmap_msg_consumer", message_consumer),
    harness::ChildRole::new("mmap_sig_producer", signal_producer),
    harness::ChildRole::new("mmap_sig_consumer", signal_consumer),
    harness::ChildRole::new("mmap_multi_msg_producer", multi_message_producer),
    harness::ChildRole::new("mmap_multi_msg_consumer", multi_message_consumer),
];

struct RawRingMmapBench;

impl harness::BenchHarness for RawRingMmapBench {
    fn bench_name(&self) -> &'static str {
        "raw_ring_mmap"
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
            println!("=== Raw Ring MMAP Benchmark ===");
            println!("Backend: file-backed mmap");
            if run_message {
                println!(
                    "Message class: {}B event, full 128B payload fill + timestamp",
                    std::mem::size_of::<MessageEvent>()
                );
            }
            if run_signal {
                println!(
                    "Signal class:  {}B event ({}B data), 10M events, 64K buffer",
                    std::mem::size_of::<SignalEvent>(),
                    16
                );
            }
            println!();
        }

        let scenarios = [
            Scenario {
                label: "message_1p1c_144B",
                producer_role: "mmap_msg_producer",
                consumer_role: "mmap_msg_consumer",
                event_bytes: std::mem::size_of::<MessageEvent>(),
                events: 100_000,
                buffer: 1024,
                warmup: 1_000,
                consumers: 1,
                record_latency: true,
            },
            Scenario {
                label: "message_1p3c_144B",
                producer_role: "mmap_multi_msg_producer",
                consumer_role: "mmap_multi_msg_consumer",
                event_bytes: std::mem::size_of::<MessageEvent>(),
                events: 100_000,
                buffer: 1024,
                warmup: 1_000,
                consumers: 3,
                record_latency: true,
            },
            Scenario {
                label: "message_1p8c_144B",
                producer_role: "mmap_multi_msg_producer",
                consumer_role: "mmap_multi_msg_consumer",
                event_bytes: std::mem::size_of::<MessageEvent>(),
                events: 100_000,
                buffer: 4096,
                warmup: 1_000,
                consumers: 8,
                record_latency: false,
            },
            Scenario {
                label: "message_1p12c_144B",
                producer_role: "mmap_multi_msg_producer",
                consumer_role: "mmap_multi_msg_consumer",
                event_bytes: std::mem::size_of::<MessageEvent>(),
                events: 100_000,
                buffer: 4096,
                warmup: 1_000,
                consumers: 12,
                record_latency: false,
            },
            Scenario {
                label: "signal_1p1c_64B",
                producer_role: "mmap_sig_producer",
                consumer_role: "mmap_sig_consumer",
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

perf_bench::myelon_bench_main!(RawRingMmapBench);
