//! Raw disruptor-mp ring benchmark over SHM backend.
//!
//! Two benchmark classes:
//!   --class message  : 144B event, full 128B payload fill + timestamp (matches original `ipc_shm`)
//!   --class signal   : 64B event, 16B data only (head-to-head vs Alvarez Rosa V5 305M ops/s)
//!   (default)        : runs both
//!
//! Run:   cargo bench -p myelon-bench --bench `raw_ring_shm`
//! Signal only: cargo bench -p myelon-bench --bench `raw_ring_shm` -- --class signal

use crate::cli::raw_ring::{RawRingScenarioSpec, RawRingSelection};
use crate::infra::coordination::BenchmarkCoordination;
use crate::infra::events::nanos_now;
use crate::infra::latency::LatencyRecorder;
use crate::infra::output::report::BackendKind;
use crate::infra::output::reporting::{self, BenchReport};
use crate::infra::{self, IpcBenchmark, ScenarioChildren};
use disruptor_mp::{
    attach_shared_consumer, build_shared_single_producer, AutoWaitStrategy, CoordinationMode,
    SharedConsumer, SharedDisruptorBuilder, SharedMemoryConfig,
};
use std::time::{Duration, Instant};

const DISCOVERY_SCAN_SLEEP: Duration = Duration::from_millis(150);
const MULTI_CONSUMER_PREFIX: &str = "rrc";

fn discovery_scan_rounds(num_consumers: usize) -> usize {
    if num_consumers > 1 {
        8 + num_consumers
    } else {
        8
    }
}

fn warm_discovery_scans<F>(mut scan: F, rounds: usize)
where
    F: FnMut() -> i64,
{
    for _ in 0..rounds {
        let _ = scan();
        std::thread::sleep(DISCOVERY_SCAN_SLEEP);
    }
}

fn multi_consumer_id(consumer_id: usize) -> String {
    format!("{MULTI_CONSUMER_PREFIX}_{consumer_id}")
}

fn attach_consumer_with_timeout<E: Copy + Default + 'static>(
    segment: &str,
    buffer: usize,
    consumer_name: &str,
    timeout: Duration,
) -> Result<SharedConsumer<E>, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        match attach_shared_consumer::<E>(segment, buffer)
            .with_consumer_id(consumer_name)
            .build_consumer()
        {
            Ok(consumer) => return Ok(consumer),
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(error) => {
                return Err(format!("attach failed for {consumer_name}: {error}").into());
            }
        }
    }
}

// ============================================================
// Event types
// ============================================================

use crate::infra::events::BenchEvent;

/// Signal-class event: cache-line aligned, 16 bytes of data.
/// Head-to-head with Alvarez Rosa V5 (305M) and Intel (452M) articles.
/// 64 bytes total (16 data + 48 padding from alignment).
#[repr(C, align(64))]
#[derive(Clone, Copy, Default)]
struct SignalEvent {
    sequence: u64,
    data: u64,
}

// ============================================================
// Message-class: producer (matches original ipc_shm exactly)
// ============================================================

fn message_producer_sized<const SIZE: usize>() -> Result<(), Box<dyn std::error::Error>> {
    let segment = infra::segment_from_env("BENCHMARK_SEGMENT_NAME");
    let target_rate = infra::read_env_u64("BENCH_TARGET_RATE", 0);
    let buffer = infra::read_env_usize("BENCH_BUFFER", 1024);
    let events = infra::read_env_u64("BENCH_EVENTS", 100_000);
    let warmup = infra::read_env_u64("BENCH_WARMUP", 1_000);

    let mut producer = build_shared_single_producer::<BenchEvent<SIZE>>(&segment, buffer)
        .enable_discovery(1)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(BenchEvent::<SIZE>::default)?;

    let coord = BenchmarkCoordination::create(&segment)?;

    if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
        return Err("timeout waiting for consumer".into());
    }

    warm_discovery_scans(|| producer.min_gating_sequence(), discovery_scan_rounds(1));

    for i in 0..warmup {
        producer.publish(|event| {
            event.sequence = i;
            event.timestamp_ns = 0;
            event.payload = [(i % 256) as u8; SIZE];
        });
    }

    let start = Instant::now();
    if target_rate > 0 {
        let interval_ns = 1_000_000_000u64 / target_rate;
        let base_ns = nanos_now();
        for i in 0..events {
            let intended_ns = base_ns.saturating_add(i.saturating_mul(interval_ns));
            while nanos_now() < intended_ns {
                std::hint::spin_loop();
            }
            producer.publish(|event| {
                event.sequence = warmup + i;
                event.timestamp_ns = intended_ns;
                event.payload = [((warmup + i) % 256) as u8; SIZE];
            });
        }
    } else {
        for i in 0..events {
            producer.publish(|event| {
                event.sequence = warmup + i;
                event.timestamp_ns = nanos_now();
                event.payload = [((warmup + i) % 256) as u8; SIZE];
            });
        }
    }
    let elapsed = start.elapsed();

    let output = infra::ProducerOutput::from_elapsed(
        events,
        elapsed,
        std::mem::size_of::<BenchEvent<SIZE>>(),
    );
    println!("{}", serde_json::to_string(&output)?);

    coord.signal_producer_done(events as i64);
    coord.wait_for_consumers_done(1, Duration::from_secs(30));
    Ok(())
}

fn message_producer() -> Result<(), Box<dyn std::error::Error>> {
    let event_bytes = infra::read_env_usize("BENCH_EVENT_SIZE", 144);
    crate::dispatch_bench_event!(event_bytes, |<SIZE>| {
        message_producer_sized::<SIZE>()
    })
}

fn message_consumer_sized<const SIZE: usize>() -> Result<(), Box<dyn std::error::Error>> {
    let segment = infra::segment_from_env("BENCHMARK_SEGMENT_NAME");
    let consumer_id = infra::read_env_usize("BENCH_CONSUMER_ID", 0);
    let buffer = infra::read_env_usize("BENCH_BUFFER", 1024);
    let warmup_target = infra::read_env_u64("BENCH_WARMUP", 1_000);

    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;

    let config = SharedMemoryConfig {
        name: segment.clone(),
        buffer_size: buffer,
        element_size: std::mem::size_of::<BenchEvent<SIZE>>(),
        create: false,
    };
    let mut consumer = SharedDisruptorBuilder::<BenchEvent<SIZE>>::new(config).build_consumer()?;
    coord.signal_consumer_ready();

    // Warmup
    let warmup_deadline = infra::spin_deadline();
    let mut warmup = 0u64;
    while warmup < warmup_target {
        let before = warmup;
        consumer.process_available(|_e, _s| {
            warmup += 1;
        });
        if warmup == before {
            infra::check_deadline(warmup_deadline, "raw_ring_shm message_consumer warmup");
            std::hint::spin_loop();
        }
    }

    // Measured
    let measure_deadline = infra::spin_deadline();
    let mut latency = LatencyRecorder::default_range();
    let start = Instant::now();
    let mut consumed = 0u64;
    let mut checksum = 0u64;
    loop {
        let before = consumed;
        consumer.process_available(|event, _seq| {
            consumed += 1;
            checksum = checksum.wrapping_add(event.sequence);
            if event.timestamp_ns > 0 {
                latency.record_delta(event.timestamp_ns, nanos_now());
            }
        });
        if coord.is_producer_done() && consumed >= coord.events_produced() as u64 {
            break;
        }
        if consumed == before {
            infra::check_deadline(measure_deadline, "raw_ring_shm message_consumer measured");
            std::hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();

    let output = if let Some(stats) = latency.stats() {
        infra::ConsumerOutput::from_elapsed(
            consumer_id,
            consumed,
            elapsed,
            std::mem::size_of::<BenchEvent<SIZE>>(),
            checksum,
        )
        .with_latency(stats)
    } else {
        infra::ConsumerOutput::from_elapsed(
            consumer_id,
            consumed,
            elapsed,
            std::mem::size_of::<BenchEvent<SIZE>>(),
            checksum,
        )
    };
    println!("{}", serde_json::to_string(&output)?);
    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

fn message_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let event_bytes = infra::read_env_usize("BENCH_EVENT_SIZE", 144);
    crate::dispatch_bench_event!(event_bytes, |<SIZE>| {
        message_consumer_sized::<SIZE>()
    })
}

// ============================================================
// Signal-class: producer (head-to-head vs Alvarez Rosa V5)
// ============================================================

fn signal_producer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = infra::segment_from_env("BENCHMARK_SEGMENT_NAME");
    let buffer = infra::read_env_usize("BENCH_BUFFER", 65_536);
    let events = infra::read_env_u64("BENCH_EVENTS", 10_000_000);
    let warmup = infra::read_env_u64("BENCH_WARMUP", 100_000);

    let mut producer = build_shared_single_producer::<SignalEvent>(&segment, buffer)
        .enable_discovery(1)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(SignalEvent::default)?;

    let coord = BenchmarkCoordination::create(&segment)?;

    if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
        return Err("timeout waiting for consumer".into());
    }

    // Warmup discovery scans
    warm_discovery_scans(|| producer.min_gating_sequence(), discovery_scan_rounds(1));

    // Warmup events
    for i in 0..warmup {
        producer.publish(|slot| {
            slot.sequence = i;
            slot.data = i.wrapping_mul(0x9E3779B97F4A7C15);
        });
    }

    // Measured — minimal work: write 16 bytes per event
    let start = Instant::now();
    for i in 0..events {
        producer.publish(|slot| {
            slot.sequence = warmup + i;
            slot.data = (warmup + i).wrapping_mul(0x9E3779B97F4A7C15);
        });
    }
    let elapsed = start.elapsed();

    let output =
        infra::ProducerOutput::from_elapsed(events, elapsed, std::mem::size_of::<SignalEvent>());
    println!("{}", serde_json::to_string(&output)?);

    coord.signal_producer_done(events as i64);
    coord.wait_for_consumers_done(1, Duration::from_secs(30));
    Ok(())
}

fn signal_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = infra::segment_from_env("BENCHMARK_SEGMENT_NAME");
    let consumer_id = infra::read_env_usize("BENCH_CONSUMER_ID", 0);
    let buffer = infra::read_env_usize("BENCH_BUFFER", 65_536);
    let warmup_target = infra::read_env_u64("BENCH_WARMUP", 100_000);

    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;

    let config = SharedMemoryConfig {
        name: segment.clone(),
        buffer_size: buffer,
        element_size: std::mem::size_of::<SignalEvent>(),
        create: false,
    };
    let mut consumer = SharedDisruptorBuilder::<SignalEvent>::new(config).build_consumer()?;
    coord.signal_consumer_ready();

    // Warmup
    let warmup_deadline = infra::spin_deadline();
    let mut warmup = 0u64;
    while warmup < warmup_target {
        let before = warmup;
        consumer.process_available(|_e, _s| {
            warmup += 1;
        });
        if warmup == before {
            infra::check_deadline(warmup_deadline, "raw_ring_shm signal_consumer warmup");
            std::hint::spin_loop();
        }
    }

    // Measured
    let measure_deadline = infra::spin_deadline();
    let start = Instant::now();
    let mut consumed = 0u64;
    let checksum = 0u64; // Signal class: no payload work — measures pure disruptor ceiling
    loop {
        let before = consumed;
        consumer.process_available(|_event, _s| {
            consumed += 1;
        });
        if coord.is_producer_done() && consumed >= coord.events_produced() as u64 {
            break;
        }
        if consumed == before {
            infra::check_deadline(measure_deadline, "raw_ring_shm signal_consumer measured");
            std::hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();

    let output = infra::ConsumerOutput::from_elapsed(
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

fn multi_signal_producer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = infra::segment_from_env("BENCHMARK_SEGMENT_NAME");
    let num_consumers = infra::read_env_usize("BENCH_NUM_CONSUMERS", 2);
    let buffer = infra::read_env_usize("BENCH_BUFFER", 65_536);
    let events = infra::read_env_u64("BENCH_EVENTS", 10_000_000);
    let warmup = infra::read_env_u64("BENCH_WARMUP", 100_000);

    let mut producer = build_shared_single_producer::<SignalEvent>(&segment, buffer)
        .discover_consumer_with_prefix_and_interval(
            num_consumers,
            MULTI_CONSUMER_PREFIX,
            DISCOVERY_SCAN_SLEEP,
        )
        .wait_for_consumers(num_consumers as i64, Duration::from_secs(60))
        .build_producer(SignalEvent::default)?;

    warm_discovery_scans(
        || producer.min_gating_sequence(),
        discovery_scan_rounds(num_consumers),
    );

    for i in 0..warmup {
        producer.publish(|slot| {
            slot.sequence = i;
            slot.data = i.wrapping_mul(0x9E3779B97F4A7C15);
        });
    }

    let start = Instant::now();
    for i in 0..events {
        producer.publish(|slot| {
            slot.sequence = warmup + i;
            slot.data = (warmup + i).wrapping_mul(0x9E3779B97F4A7C15);
        });
    }
    let elapsed = start.elapsed();

    let output =
        infra::ProducerOutput::from_elapsed(events, elapsed, std::mem::size_of::<SignalEvent>());
    println!("{}", serde_json::to_string(&output)?);

    let last_seq = (warmup + events - 1) as i64;
    producer.wait_until_consumed_with_strategy(
        last_seq,
        Duration::from_secs(90),
        AutoWaitStrategy::BusySpin,
    );
    Ok(())
}

fn multi_signal_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = infra::segment_from_env("BENCHMARK_SEGMENT_NAME");
    let consumer_id = infra::read_env_usize("BENCH_CONSUMER_ID", 0);
    let buffer = infra::read_env_usize("BENCH_BUFFER", 65_536);
    let events = infra::read_env_u64("BENCH_EVENTS", 10_000_000);
    let warmup = infra::read_env_u64("BENCH_WARMUP", 100_000);

    let consumer_name = multi_consumer_id(consumer_id);
    let mut consumer = attach_consumer_with_timeout::<SignalEvent>(
        &segment,
        buffer,
        &consumer_name,
        Duration::from_secs(15),
    )?;

    let warmup_deadline = infra::spin_deadline();
    let mut warmup_count = 0u64;
    let mut consumed = 0u64;
    let mut start = None;
    loop {
        let before_progress = warmup_count + consumed;
        consumer.process_available(|_event, _seq| {
            if _event.sequence < warmup {
                warmup_count += 1;
            } else {
                if start.is_none() {
                    start = Some(Instant::now());
                }
                consumed += 1;
            }
        });
        if consumed > 0 || warmup_count >= warmup {
            break;
        }
        if warmup_count + consumed == before_progress {
            infra::check_deadline(warmup_deadline, "raw_ring_shm multi_signal_consumer warmup");
            std::hint::spin_loop();
        }
    }

    let measure_deadline = infra::spin_deadline();
    let start = start.unwrap_or_else(Instant::now);
    let checksum = 0u64;
    while consumed < events {
        let before = consumed;
        consumer.process_available(|event, _seq| {
            if event.sequence >= warmup {
                consumed += 1;
            }
        });
        if consumed == before {
            infra::check_deadline(
                measure_deadline,
                "raw_ring_shm multi_signal_consumer measured",
            );
            std::hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();

    let output = infra::ConsumerOutput::from_elapsed(
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

fn multi_message_producer_sized<const SIZE: usize>() -> Result<(), Box<dyn std::error::Error>> {
    let segment = infra::segment_from_env("BENCHMARK_SEGMENT_NAME");
    let num_consumers = infra::read_env_usize("BENCH_NUM_CONSUMERS", 3);
    let buffer = infra::read_env_usize("BENCH_BUFFER", 4096);
    let events = infra::read_env_u64("BENCH_EVENTS", 100_000);
    let warmup = infra::read_env_u64("BENCH_WARMUP", 1_000);
    let target_rate = infra::read_env_u64("BENCH_TARGET_RATE", 0);

    let mut producer = build_shared_single_producer::<BenchEvent<SIZE>>(&segment, buffer)
        .discover_consumer_with_prefix_and_interval(
            num_consumers,
            MULTI_CONSUMER_PREFIX,
            DISCOVERY_SCAN_SLEEP,
        )
        .wait_for_consumers(num_consumers as i64, Duration::from_secs(60))
        .build_producer(BenchEvent::<SIZE>::default)?;

    warm_discovery_scans(
        || producer.min_gating_sequence(),
        discovery_scan_rounds(num_consumers),
    );

    for i in 0..warmup {
        producer.publish(|event| {
            event.sequence = i;
            event.timestamp_ns = 0;
            event.payload = [(i % 256) as u8; SIZE];
        });
    }

    let start = Instant::now();
    if target_rate > 0 {
        let interval_ns = 1_000_000_000u64 / target_rate;
        let base_ns = nanos_now();
        for i in 0..events {
            let intended_ns = base_ns.saturating_add(i.saturating_mul(interval_ns));
            while nanos_now() < intended_ns {
                std::hint::spin_loop();
            }
            producer.publish(|event| {
                event.sequence = warmup + i;
                event.timestamp_ns = intended_ns;
                event.payload = [((warmup + i) % 256) as u8; SIZE];
            });
        }
    } else {
        for i in 0..events {
            producer.publish(|event| {
                event.sequence = warmup + i;
                event.timestamp_ns = nanos_now();
                event.payload = [((warmup + i) % 256) as u8; SIZE];
            });
        }
    }
    let elapsed = start.elapsed();

    let output = infra::ProducerOutput::from_elapsed(
        events,
        elapsed,
        std::mem::size_of::<BenchEvent<SIZE>>(),
    );
    println!("{}", serde_json::to_string(&output)?);

    let last_seq = (warmup + events - 1) as i64;
    producer.wait_until_consumed_with_strategy(
        last_seq,
        Duration::from_secs(90),
        AutoWaitStrategy::BusySpin,
    );
    Ok(())
}

fn multi_message_producer() -> Result<(), Box<dyn std::error::Error>> {
    let event_bytes = infra::read_env_usize("BENCH_EVENT_SIZE", 144);
    crate::dispatch_bench_event!(event_bytes, |<SIZE>| {
        multi_message_producer_sized::<SIZE>()
    })
}

fn multi_message_consumer_sized<const SIZE: usize>() -> Result<(), Box<dyn std::error::Error>> {
    let segment = infra::segment_from_env("BENCHMARK_SEGMENT_NAME");
    let consumer_id = infra::read_env_usize("BENCH_CONSUMER_ID", 0);
    let buffer = infra::read_env_usize("BENCH_BUFFER", 4096);
    let events = infra::read_env_u64("BENCH_EVENTS", 100_000);
    let warmup = infra::read_env_u64("BENCH_WARMUP", 1_000);
    let record_latency = infra::read_env_usize("BENCH_RECORD_LATENCY", 0) == 1;

    let consumer_name = multi_consumer_id(consumer_id);
    let mut consumer = attach_consumer_with_timeout::<BenchEvent<SIZE>>(
        &segment,
        buffer,
        &consumer_name,
        Duration::from_secs(15),
    )?;

    // Warmup
    let warmup_deadline = infra::spin_deadline();
    let mut warmup_count = 0u64;
    let mut consumed = 0u64;
    let mut checksum = 0u64;
    let mut latency = if record_latency {
        Some(LatencyRecorder::default_range())
    } else {
        None
    };
    let mut start = None;
    loop {
        let before_progress = warmup_count + consumed;
        consumer.process_available(|event, _seq| {
            if event.sequence < warmup {
                warmup_count += 1;
            } else {
                if start.is_none() {
                    start = Some(Instant::now());
                }
                consumed += 1;
                checksum = checksum.wrapping_add(event.sequence);
                if let Some(ref mut lat) = latency {
                    if event.timestamp_ns > 0 {
                        lat.record_delta(event.timestamp_ns, nanos_now());
                    }
                }
            }
        });
        if consumed > 0 || warmup_count >= warmup {
            break;
        }
        if warmup_count + consumed == before_progress {
            infra::check_deadline(
                warmup_deadline,
                "raw_ring_shm multi_message_consumer warmup",
            );
            std::hint::spin_loop();
        }
    }

    // Measured
    let measure_deadline = infra::spin_deadline();
    let start = start.unwrap_or_else(Instant::now);
    while consumed < events {
        let before = consumed;
        consumer.process_available(|event, _seq| {
            if event.sequence >= warmup {
                consumed += 1;
                checksum = checksum.wrapping_add(event.sequence);
                if let Some(ref mut lat) = latency {
                    if event.timestamp_ns > 0 {
                        lat.record_delta(event.timestamp_ns, nanos_now());
                    }
                }
            }
        });
        if consumed == before {
            infra::check_deadline(
                measure_deadline,
                "raw_ring_shm multi_message_consumer measured",
            );
            std::hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();

    let latency_stats = latency.as_ref().and_then(|recorder| recorder.stats());
    let output = if let Some(stats) = latency_stats {
        infra::ConsumerOutput::from_elapsed(
            consumer_id,
            consumed,
            elapsed,
            std::mem::size_of::<BenchEvent<SIZE>>(),
            checksum,
        )
        .with_latency(stats)
    } else {
        infra::ConsumerOutput::from_elapsed(
            consumer_id,
            consumed,
            elapsed,
            std::mem::size_of::<BenchEvent<SIZE>>(),
            checksum,
        )
    };
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

fn multi_message_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let event_bytes = infra::read_env_usize("BENCH_EVENT_SIZE", 144);
    crate::dispatch_bench_event!(event_bytes, |<SIZE>| {
        multi_message_consumer_sized::<SIZE>()
    })
}

// ============================================================
// Orchestrator
// ============================================================

#[derive(Clone)]
struct Scenario {
    label: String,
    producer_role: &'static str,
    consumer_role: &'static str,
    event_bytes: usize,
    events: u64,
    buffer: usize,
    warmup: u64,
    consumers: usize,
    record_latency: bool,
    target_rate: u64,
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

    fn transport_metadata(&self) -> reporting::BenchTransportSpec {
        reporting::BenchTransportSpec::benchmark_shm(self.consumers)
            .with_zero_copy(false)
            .with_framing("none")
    }

    fn measurement_mode(&self) -> String {
        if self.target_rate > 0 {
            format!("co_aware@{}", self.target_rate)
        } else {
            "max_throughput".to_string()
        }
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

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, infra::BenchError> {
        let prefix = if self.consumers > 1 {
            "raw_ring_multi"
        } else {
            "raw_ring"
        };
        let segment = infra::unique_shm_segment(prefix);
        let mut envs: Vec<(&str, String)> = vec![
            ("BENCHMARK_SEGMENT_NAME", segment.clone()),
            ("BENCH_NUM_CONSUMERS", self.consumers.to_string()),
            ("BENCH_BUFFER", self.buffer.to_string()),
            ("BENCH_EVENTS", self.events.to_string()),
            ("BENCH_WARMUP", self.warmup.to_string()),
            ("BENCH_EVENT_SIZE", self.event_bytes.to_string()),
        ];
        if self.target_rate > 0 {
            envs.push(("BENCH_TARGET_RATE", self.target_rate.to_string()));
        }

        let producer = infra::spawn_child(exe, self.producer_role, &envs);
        let consumers = (0..self.consumers)
            .map(|consumer_id| {
                let mut consumer_envs = envs.clone();
                if self.record_latency || self.target_rate > 0 {
                    consumer_envs.push(("BENCH_RECORD_LATENCY", "1".to_string()));
                }
                consumer_envs.push(("BENCH_CONSUMER_ID", consumer_id.to_string()));
                infra::spawn_child(exe, self.consumer_role, &consumer_envs)
            })
            .collect();

        Ok(ScenarioChildren::new(producer, consumers))
    }

    fn aggregate_latency(
        &self,
        consumers: &[infra::ConsumerOutput],
    ) -> Option<crate::infra::latency::LatencyStats> {
        consumers
            .iter()
            .filter_map(|entry| entry.latency.clone())
            .max_by_key(|stats| stats.p99_ns)
    }
}

// ============================================================
// main
// ============================================================

const CHILD_ROLES: &[infra::ChildRole] = &[
    infra::ChildRole::new("msg_producer", message_producer),
    infra::ChildRole::new("msg_consumer", message_consumer),
    infra::ChildRole::new("sig_producer", signal_producer),
    infra::ChildRole::new("sig_consumer", signal_consumer),
    infra::ChildRole::new("sig_multi_producer", multi_signal_producer),
    infra::ChildRole::new("sig_multi_consumer", multi_signal_consumer),
    infra::ChildRole::new("msg_multi_producer", multi_message_producer),
    infra::ChildRole::new("msg_multi_consumer", multi_message_consumer),
];

pub struct RawRingShmBench;

impl infra::BenchHarness for RawRingShmBench {
    fn bench_name(&self) -> &'static str {
        "raw_ring_shm"
    }

    fn child_roles(&self) -> &'static [infra::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> infra::BenchRunResult {
        let selection = RawRingSelection::parse(args)?;
        let target_rate = selection.target_rate();

        if !selection.output_args.json_mode {
            println!("=== Raw Ring SHM Benchmark ===");
            println!("Backend: POSIX shared memory");
            println!(
                "Mode: {}",
                if target_rate > 0 {
                    "co_aware"
                } else {
                    "throughput"
                }
            );
            if target_rate > 0 {
                println!("Target rate: {} ops/s", target_rate);
                if selection.run_message() && selection.run_signal() {
                    println!("CO mode applies only to message-class scenarios; signal scenarios are skipped.");
                }
            }
            if selection.run_message() {
                let eb = selection.event_bytes();
                println!(
                    "Message class: {eb}B event, full {}B payload fill + timestamp",
                    eb.saturating_sub(16),
                );
            }
            if selection.run_signal() {
                println!(
                    "Signal class:  {}B event ({}B data), {} events, 64K buffer — vs Alvarez Rosa V5 (305M ops/s)",
                    std::mem::size_of::<SignalEvent>(),
                    16,
                    selection.signal_events(10_000_000)
                );
            }
            println!();
        }

        let mut report = BenchReport::new();
        for spec in selection.scenario_specs(BackendKind::Shm, 10_000_000) {
            let scenario = Scenario::from_spec(&spec);
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
    fn from_spec(spec: &RawRingScenarioSpec) -> Self {
        Self {
            label: spec.label.clone(),
            producer_role: spec.producer_role,
            consumer_role: spec.consumer_role,
            event_bytes: spec.event_bytes,
            events: spec.events,
            buffer: spec.buffer,
            warmup: spec.warmup,
            consumers: spec.consumers,
            record_latency: spec.record_latency,
            target_rate: spec.target_rate,
        }
    }
}
