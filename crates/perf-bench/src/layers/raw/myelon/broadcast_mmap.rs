//! Raw myelon re-export ring benchmark over mmap (file-backed) backend.
//!
//! Identical to `disruptor_mp/broadcast_mmap.rs` but uses myelon re-exports.
//! The point: prove myelon re-export has identical perf to raw disruptor.
//!
//! Two benchmark classes (matching `raw_myelon_shm` exactly):
//!   --class message  : 144B logical event request, 192B physical slot, full 128B payload fill + timestamp
//!   --class signal   : 64B event, 16B data only
//!   (default)        : runs both
//!
//! Run:   cargo bench -p perf-bench --bench `raw_myelon_mmap`
//! Signal only: cargo bench -p perf-bench --bench `raw_myelon_mmap` -- --class signal

use crate::cli::raw_ring::{
    aligned_slot_bytes, raw_payload_bytes, RawRingScenarioSpec, RawRingSelection,
};
use crate::infra::events::nanos_now;
use crate::infra::events::BenchEvent;
use crate::infra::latency::LatencyRecorder;
use crate::infra::output::report::BackendKind;
use crate::infra::output::reporting::{self, BenchReport};
use crate::infra::{self, IpcBenchmark, ScenarioChildren};
use myelon::{AutoWaitStrategy, MmapConsumer, MmapProducer, MmapTransportLayout};
use std::env;
use std::process::Child;
use std::time::{Duration, Instant};

const SIGNAL_MULTI_EVENTS: u64 = 1_000_000;

// ============================================================
// Event types (identical to raw_myelon_shm)
// ============================================================

#[repr(C, align(64))]
#[derive(Clone, Copy, Default)]
struct SignalEvent {
    sequence: u64,
    data: u64,
}

// ============================================================
// Helpers
// ============================================================

fn child_layout() -> MmapTransportLayout {
    infra::mmap_layout_from_env("MMAP_ROOT", "MMAP_SEGMENT")
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
    infra::spawn_child(exe, role, &envs)
}

// ============================================================
// Message-class producer (full 128B fill + timestamp — matches SHM message class)
// ============================================================

fn message_producer_sized<const SIZE: usize>() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    let target_rate = infra::read_env_u64("MMAP_TARGET_RATE", 0);
    let warmup: u64 = infra::read_env_u64("MMAP_WARMUP", 1_000);

    layout.ensure_directories()?;
    let mut producer =
        MmapProducer::<BenchEvent<SIZE>>::create(layout, buffer_size, BenchEvent::<SIZE>::default)?;

    if !producer.wait_for_consumers_ready(1, Duration::from_secs(30)) {
        return Err("Timeout waiting for consumer".into());
    }

    // Warmup — full payload fill
    for i in 0..warmup {
        producer.publish(|e| {
            e.sequence = i;
            e.timestamp_ns = 0;
            e.payload.fill((i % 256) as u8);
        });
    }

    // Measured — identical work to SHM message class, absolute timestamps
    let start = Instant::now();
    if let Some(interval_ns) = crate::infra::co_interval_ns(target_rate) {
        let base_ns = nanos_now();
        for i in 0..num_events {
            let intended_ns = base_ns.saturating_add(i.saturating_mul(interval_ns));
            while nanos_now() < intended_ns {
                std::hint::spin_loop();
            }
            producer.publish(|e| {
                e.sequence = warmup + i;
                e.timestamp_ns = intended_ns;
                e.payload.fill(((warmup + i) % 256) as u8);
            });
        }
    } else {
        for i in 0..num_events {
            producer.publish(|e| {
                e.sequence = warmup + i;
                e.timestamp_ns = nanos_now();
                e.payload.fill(((warmup + i) % 256) as u8);
            });
        }
    }
    let elapsed = start.elapsed();

    let output = infra::ProducerOutput::from_elapsed(
        num_events,
        elapsed,
        std::mem::size_of::<BenchEvent<SIZE>>(),
    );
    println!("{}", serde_json::to_string(&output)?);

    let last_seq = (warmup + num_events - 1) as i64;
    producer.wait_until_consumed_with_strategy(
        last_seq,
        Duration::from_secs(30),
        AutoWaitStrategy::BusySpin,
    );
    Ok(())
}

fn message_producer() -> Result<(), Box<dyn std::error::Error>> {
    let event_bytes = infra::read_env_usize("MMAP_EVENT_SIZE", 144);
    crate::dispatch_bench_event!(event_bytes, |<SIZE>| message_producer_sized::<SIZE>())
}

fn message_consumer_sized<const SIZE: usize>() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    let consumer_id = infra::read_env_usize("MMAP_CONSUMER_ID", 0);
    let consumer_name = format!("c{}", consumer_id);
    let warmup_target: u64 = infra::read_env_u64("MMAP_WARMUP", 1_000);

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapConsumer::<BenchEvent<SIZE>>::attach(layout.clone(), buffer_size, &consumer_name)
        {
            Ok(c) => break c,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(e) => return Err(format!("attach failed: {e}").into()),
        }
    };

    // Warmup
    let warmup_deadline = infra::spin_deadline();
    let mut warmup = 0u64;
    while warmup < warmup_target {
        if consumer.try_consume_next_leased().is_some() {
            warmup += 1;
        } else {
            infra::check_deadline(warmup_deadline, "raw_myelon_mmap message_consumer warmup");
            std::hint::spin_loop();
        }
    }

    // Measured — with real per-event HDR latency
    let measure_deadline = infra::spin_deadline();
    let mut latency = LatencyRecorder::default_range();
    let start = Instant::now();
    let mut consumed = 0u64;
    let mut checksum = 0u64;
    while consumed < num_events {
        if let Some(event) = consumer.try_consume_next_leased() {
            consumed += 1;
            checksum = checksum.wrapping_add(event.sequence);
            if event.timestamp_ns > 0 {
                latency.record_delta(event.timestamp_ns, nanos_now());
            }
        } else {
            infra::check_deadline(
                measure_deadline,
                "raw_myelon_mmap message_consumer measured",
            );
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
    Ok(())
}

fn message_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let event_bytes = infra::read_env_usize("MMAP_EVENT_SIZE", 144);
    crate::dispatch_bench_event!(event_bytes, |<SIZE>| message_consumer_sized::<SIZE>())
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

    let output = infra::ProducerOutput::from_elapsed(
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
    let consumer_id = infra::read_env_usize("MMAP_CONSUMER_ID", 0);
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

    let warmup_deadline = infra::spin_deadline();
    let mut warmup = 0u64;
    while warmup < WARMUP {
        if consumer.try_consume_next_leased().is_some() {
            warmup += 1;
        } else {
            infra::check_deadline(warmup_deadline, "raw_myelon_mmap signal_consumer warmup");
            std::hint::spin_loop();
        }
    }

    let measure_deadline = infra::spin_deadline();
    let start = Instant::now();
    let mut consumed = 0u64;
    let checksum = 0u64; // Signal class: no payload work — measures pure disruptor ceiling
    while consumed < num_events {
        if consumer.try_consume_next_leased().is_some() {
            consumed += 1;
        } else {
            infra::check_deadline(measure_deadline, "raw_myelon_mmap signal_consumer measured");
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

fn multi_signal_producer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    let num_consumers = infra::read_env_usize("MMAP_NUM_CONSUMERS", 2);
    const WARMUP: u64 = 100_000;

    layout.ensure_directories()?;
    let mut producer =
        MmapProducer::<SignalEvent>::create(layout, buffer_size, SignalEvent::default)?;

    if !producer.wait_for_consumers_ready(num_consumers as i64, Duration::from_secs(60)) {
        return Err(format!("Timeout waiting for {num_consumers} consumers").into());
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

    let output = infra::ProducerOutput::from_elapsed(
        num_events,
        elapsed,
        std::mem::size_of::<SignalEvent>(),
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

fn multi_signal_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    let consumer_id = infra::read_env_usize("MMAP_CONSUMER_ID", 0);
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

    let warmup_deadline = infra::spin_deadline();
    let mut warmup = 0u64;
    while warmup < WARMUP {
        if consumer.try_consume_next_leased().is_some() {
            warmup += 1;
        } else {
            infra::check_deadline(
                warmup_deadline,
                "raw_myelon_mmap multi_signal_consumer warmup",
            );
            std::hint::spin_loop();
        }
    }

    let measure_deadline = infra::spin_deadline();
    let start = Instant::now();
    let mut consumed = 0u64;
    let checksum = 0u64;
    while consumed < num_events {
        if consumer.try_consume_next_leased().is_some() {
            consumed += 1;
        } else {
            infra::check_deadline(
                measure_deadline,
                "raw_myelon_mmap multi_signal_consumer measured",
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
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    let num_consumers = infra::read_env_usize("MMAP_NUM_CONSUMERS", 3);
    let target_rate = infra::read_env_u64("MMAP_TARGET_RATE", 0);
    let warmup: u64 = infra::read_env_u64("MMAP_WARMUP", 1_000);

    layout.ensure_directories()?;
    let mut producer =
        MmapProducer::<BenchEvent<SIZE>>::create(layout, buffer_size, BenchEvent::<SIZE>::default)?;

    if !producer.wait_for_consumers_ready(num_consumers as i64, Duration::from_secs(60)) {
        return Err(format!("Timeout waiting for {num_consumers} consumers").into());
    }

    for i in 0..warmup {
        producer.publish(|e| {
            e.sequence = i;
            e.timestamp_ns = 0;
            e.payload.fill((i % 256) as u8);
        });
    }

    let start = Instant::now();
    if let Some(interval_ns) = crate::infra::co_interval_ns(target_rate) {
        let base_ns = nanos_now();
        for i in 0..num_events {
            let intended_ns = base_ns.saturating_add(i.saturating_mul(interval_ns));
            while nanos_now() < intended_ns {
                std::hint::spin_loop();
            }
            producer.publish(|e| {
                e.sequence = warmup + i;
                e.timestamp_ns = intended_ns;
                e.payload.fill(((warmup + i) % 256) as u8);
            });
        }
    } else {
        for i in 0..num_events {
            producer.publish(|e| {
                e.sequence = warmup + i;
                e.timestamp_ns = nanos_now();
                e.payload.fill(((warmup + i) % 256) as u8);
            });
        }
    }
    let elapsed = start.elapsed();

    let output = infra::ProducerOutput::from_elapsed(
        num_events,
        elapsed,
        std::mem::size_of::<BenchEvent<SIZE>>(),
    );
    println!("{}", serde_json::to_string(&output)?);

    let last_seq = (warmup + num_events - 1) as i64;
    producer.wait_until_consumed_with_strategy(
        last_seq,
        Duration::from_secs(90),
        AutoWaitStrategy::BusySpin,
    );
    Ok(())
}

fn multi_message_producer() -> Result<(), Box<dyn std::error::Error>> {
    let event_bytes = infra::read_env_usize("MMAP_EVENT_SIZE", 144);
    crate::dispatch_bench_event!(event_bytes, |<SIZE>| multi_message_producer_sized::<SIZE>())
}

fn multi_message_consumer_sized<const SIZE: usize>() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    let record_latency = infra::read_env_usize("MMAP_RECORD_LATENCY", 0) == 1;
    let consumer_id = infra::read_env_usize("MMAP_CONSUMER_ID", 0);
    let consumer_name = format!("c{}", consumer_id);
    let warmup_target: u64 = infra::read_env_u64("MMAP_WARMUP", 1_000);

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapConsumer::<BenchEvent<SIZE>>::attach(layout.clone(), buffer_size, &consumer_name)
        {
            Ok(c) => break c,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(e) => return Err(format!("attach failed: {e}").into()),
        }
    };

    let warmup_deadline = infra::spin_deadline();
    let mut warmup_count = 0u64;
    while warmup_count < warmup_target {
        if consumer.try_consume_next_leased().is_some() {
            warmup_count += 1;
        } else {
            infra::check_deadline(
                warmup_deadline,
                "raw_myelon_mmap multi_message_consumer warmup",
            );
            std::hint::spin_loop();
        }
    }

    let measure_deadline = infra::spin_deadline();
    let mut latency = if record_latency {
        Some(LatencyRecorder::default_range())
    } else {
        None
    };
    let start = Instant::now();
    let mut consumed = 0u64;
    let mut checksum = 0u64;
    while consumed < num_events {
        if let Some(event) = consumer.try_consume_next_leased() {
            consumed += 1;
            checksum = checksum.wrapping_add(event.sequence);
            if let Some(ref mut lat) = latency {
                if event.timestamp_ns > 0 {
                    lat.record_delta(event.timestamp_ns, nanos_now());
                }
            }
        } else {
            infra::check_deadline(
                measure_deadline,
                "raw_myelon_mmap multi_message_consumer measured",
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
    let event_bytes = infra::read_env_usize("MMAP_EVENT_SIZE", 144);
    crate::dispatch_bench_event!(event_bytes, |<SIZE>| multi_message_consumer_sized::<SIZE>())
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
        "raw_myelon_mmap"
    }

    fn scenario_name(&self) -> String {
        self.label.to_string()
    }

    fn backend(&self) -> &str {
        "mmap"
    }

    fn layer(&self) -> &str {
        "raw_myelon"
    }

    fn transport_metadata(&self) -> reporting::BenchTransportSpec {
        reporting::BenchTransportSpec::mmap_builtin()
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
        aligned_slot_bytes(self.event_bytes)
    }

    fn payload_bytes(&self) -> usize {
        raw_payload_bytes(self.event_bytes)
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
        let root = if self.consumers > 1 {
            infra::unique_mmap_root(&format!("myelon_multi{}c", self.consumers))
        } else {
            infra::unique_mmap_root(&self.label)
        };
        let segment = if self.consumers > 1 {
            infra::unique_mmap_segment(&format!("myelon_multi{}c", self.consumers))
        } else {
            infra::unique_mmap_segment(&self.label)
        };
        let root_str = root.display().to_string();
        let extra_env: Vec<(&str, String)> = {
            let mut envs = vec![
                ("MMAP_NUM_CONSUMERS", self.consumers.to_string()),
                ("MMAP_EVENT_SIZE", self.event_bytes.to_string()),
                ("MMAP_WARMUP", self.warmup.to_string()),
            ];
            if self.target_rate > 0 {
                envs.push(("MMAP_TARGET_RATE", self.target_rate.to_string()));
            }
            envs
        };

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
                if self.record_latency || self.target_rate > 0 {
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

const CHILD_ROLES: &[infra::ChildRole] = &[
    infra::ChildRole::new("my_mmap_msg_producer", message_producer),
    infra::ChildRole::new("my_mmap_msg_consumer", message_consumer),
    infra::ChildRole::new("my_mmap_sig_producer", signal_producer),
    infra::ChildRole::new("my_mmap_sig_consumer", signal_consumer),
    infra::ChildRole::new("my_mmap_multi_sig_producer", multi_signal_producer),
    infra::ChildRole::new("my_mmap_multi_sig_consumer", multi_signal_consumer),
    infra::ChildRole::new("my_mmap_multi_msg_producer", multi_message_producer),
    infra::ChildRole::new("my_mmap_multi_msg_consumer", multi_message_consumer),
];

pub struct RawMyelonMmapBench;

impl infra::BenchHarness for RawMyelonMmapBench {
    fn bench_name(&self) -> &'static str {
        "raw_myelon_mmap"
    }

    fn child_roles(&self) -> &'static [infra::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> infra::BenchRunResult {
        let selection = RawRingSelection::parse(args)?;
        let target_rate = selection.target_rate();

        if !selection.output_args.json_mode {
            println!("=== Raw Myelon MMAP Benchmark ===");
            println!("Backend: file-backed mmap (via myelon re-exports)");
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
                let slot = aligned_slot_bytes(eb);
                let payload = raw_payload_bytes(eb);
                println!(
                    "Message class: request {eb}B, physical slot {slot}B, full {payload}B payload fill + timestamp",
                );
            }
            if selection.run_signal() {
                println!(
                    "Signal class:  {}B event ({}B data), {} events, 64K buffer",
                    std::mem::size_of::<SignalEvent>(),
                    16,
                    selection.signal_events(10_000_000)
                );
            }
            println!();
        }

        let mut report = BenchReport::new();
        for spec in selection.scenario_specs(BackendKind::Mmap, SIGNAL_MULTI_EVENTS) {
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
        let (producer_role, consumer_role) = match (spec.kind, spec.consumers > 1) {
            (crate::cli::raw_ring::RawRingScenarioKind::Message, true) => {
                ("my_mmap_multi_msg_producer", "my_mmap_multi_msg_consumer")
            }
            (crate::cli::raw_ring::RawRingScenarioKind::Message, false) => {
                ("my_mmap_msg_producer", "my_mmap_msg_consumer")
            }
            (crate::cli::raw_ring::RawRingScenarioKind::Signal, true) => {
                ("my_mmap_multi_sig_producer", "my_mmap_multi_sig_consumer")
            }
            (crate::cli::raw_ring::RawRingScenarioKind::Signal, false) => {
                ("my_mmap_sig_producer", "my_mmap_sig_consumer")
            }
        };
        Self {
            label: spec.label.clone(),
            producer_role,
            consumer_role,
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
