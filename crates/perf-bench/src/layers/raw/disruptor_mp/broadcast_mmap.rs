//! Raw disruptor-mp ring benchmark over mmap (file-backed) backend.
//!
//! Two benchmark classes (matching `raw_ring_shm` exactly):
//!   --class message  : 144B logical event request, 192B physical slot, full 128B payload fill + timestamp
//!   --class signal   : 64B event, 16B data only
//!   (default)        : runs both
//!
//! Run:   cargo bench -p myelon-bench --bench `raw_ring_mmap`
//! Signal only: cargo bench -p myelon-bench --bench `raw_ring_mmap` -- --class signal

use crate::cli::raw_ring::{
    aligned_slot_bytes, raw_payload_bytes, RawRingScenarioSpec, RawRingSelection,
};
use crate::infra::events::nanos_now;
use crate::infra::latency::LatencyRecorder;
use crate::infra::output::report::BackendKind;
use crate::infra::output::reporting::{self, BenchReport};
use crate::infra::signal_latency::{
    mmap_sidecar_path_from_env, read_stamp, sample_capacity, sample_every_from_env, sampled_delta,
    should_sample, MmapTimestampSidecar, OfflineSignalSamples, SignalEvent, SignalLatencyMode,
    TimestampSidecar, TscCalibration,
};
use crate::infra::{self, IpcBenchmark, ScenarioChildren};
use disruptor_mp::{AutoWaitStrategy, MmapConsumer, MmapProducer, MmapTransportLayout};
use std::env;
use std::process::Child;
use std::time::{Duration, Instant};

const SIGNAL_MULTI_EVENTS: u64 = 1_000_000;

use crate::infra::events::BenchEvent;

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

fn mmap_signal_mode() -> Result<SignalLatencyMode, Box<dyn std::error::Error>> {
    SignalLatencyMode::from_env().map_err(Into::into)
}

#[inline]
fn record_signal_latency(
    recorder: &mut LatencyRecorder,
    mode: SignalLatencyMode,
    send_stamp: u64,
    tsc: Option<&TscCalibration>,
) {
    if send_stamp == 0 {
        return;
    }
    let recv_stamp = read_stamp(mode);
    if let Some(raw_delta) = sampled_delta(mode, send_stamp, recv_stamp) {
        if mode.uses_rdtsc() {
            if let Some(calibration) = tsc {
                recorder.record(calibration.delta_to_ns(0, raw_delta));
            }
        } else {
            recorder.record(raw_delta);
        }
    }
}

#[inline]
fn collect_signal_latency_offline(
    samples: &mut OfflineSignalSamples,
    mode: SignalLatencyMode,
    send_stamp: u64,
) {
    if send_stamp == 0 {
        return;
    }
    let recv_stamp = read_stamp(mode);
    if let Some(raw_delta) = sampled_delta(mode, send_stamp, recv_stamp) {
        samples.record(raw_delta);
    }
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

    for i in 0..warmup {
        producer.publish(|e| {
            e.sequence = i;
            e.timestamp_ns = 0;
            e.payload.fill((i % 256) as u8);
        });
    }

    let start = Instant::now();
    if target_rate > 0 {
        let interval_ns = 1_000_000_000u64 / target_rate;
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
            infra::check_deadline(warmup_deadline, "raw_ring_mmap message_consumer warmup");
            std::hint::spin_loop();
        }
    }

    // Measured
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
            infra::check_deadline(measure_deadline, "raw_ring_mmap message_consumer measured");
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
    let warmup: u64 = infra::read_env_u64("MMAP_WARMUP", 100_000);
    let target_rate = infra::read_env_u64("PERF_BENCH_SIGNAL_TARGET_RATE", 0);
    let latency_mode = mmap_signal_mode()?;

    if matches!(latency_mode, SignalLatencyMode::None) {
        layout.ensure_directories()?;
        let mut producer =
            MmapProducer::<SignalEvent>::create(layout, buffer_size, SignalEvent::default)?;

        if !producer.wait_for_consumers_ready(1, Duration::from_secs(30)) {
            return Err("Timeout waiting for consumer".into());
        }

        for i in 0..warmup {
            producer.publish(|s| {
                s.sequence = i;
                s.data = i.wrapping_mul(0x9E3779B97F4A7C15);
            });
        }

        let start = Instant::now();
        if target_rate > 0 {
            let interval_ns = 1_000_000_000u64 / target_rate;
            let base_ns = nanos_now();
            for i in 0..num_events {
                let intended_ns = base_ns.saturating_add(i.saturating_mul(interval_ns));
                while nanos_now() < intended_ns {
                    std::hint::spin_loop();
                }
                let sequence = warmup + i;
                producer.publish(|s| {
                    s.sequence = sequence;
                    s.data = sequence.wrapping_mul(0x9E3779B97F4A7C15);
                });
            }
        } else {
            for i in 0..num_events {
                let sequence = warmup + i;
                producer.publish(|s| {
                    s.sequence = sequence;
                    s.data = sequence.wrapping_mul(0x9E3779B97F4A7C15);
                });
            }
        }
        let elapsed = start.elapsed();

        let output = infra::ProducerOutput::from_elapsed(
            num_events,
            elapsed,
            std::mem::size_of::<SignalEvent>(),
        );
        println!("{}", serde_json::to_string(&output)?);

        let last_seq = (warmup + num_events - 1) as i64;
        producer.wait_until_consumed_with_strategy(
            last_seq,
            Duration::from_secs(30),
            AutoWaitStrategy::BusySpin,
        );
        return Ok(());
    }

    let sample_every = sample_every_from_env();

    layout.ensure_directories()?;
    let mut producer =
        MmapProducer::<SignalEvent>::create(layout, buffer_size, SignalEvent::default)?;
    let mut sidecar = if latency_mode.uses_sidecar() {
        Some(MmapTimestampSidecar::create(
            &mmap_sidecar_path_from_env()?,
            buffer_size,
        )?)
    } else {
        None
    };

    if !producer.wait_for_consumers_ready(1, Duration::from_secs(30)) {
        return Err("Timeout waiting for consumer".into());
    }

    for i in 0..warmup {
        producer.publish(|s| {
            s.sequence = i;
            s.data = i.wrapping_mul(0x9E3779B97F4A7C15);
            s.stamp = 0;
        });
    }

    let start = Instant::now();
    if target_rate > 0 {
        let interval_ns = 1_000_000_000u64 / target_rate;
        let base_ns = nanos_now();
        for i in 0..num_events {
            let intended_ns = base_ns.saturating_add(i.saturating_mul(interval_ns));
            while nanos_now() < intended_ns {
                std::hint::spin_loop();
            }
            let sequence = warmup + i;
            let stamp = if latency_mode.records_latency() && should_sample(sequence, sample_every) {
                read_stamp(latency_mode)
            } else {
                0
            };
            if let Some(sidecar) = sidecar.as_mut() {
                sidecar.store(sequence, stamp);
            }
            producer.publish(|s| {
                s.sequence = sequence;
                s.data = sequence.wrapping_mul(0x9E3779B97F4A7C15);
                s.stamp = if latency_mode.is_inline() { stamp } else { 0 };
            });
        }
    } else {
        for i in 0..num_events {
            let sequence = warmup + i;
            let stamp = if latency_mode.records_latency() && should_sample(sequence, sample_every) {
                read_stamp(latency_mode)
            } else {
                0
            };
            if let Some(sidecar) = sidecar.as_mut() {
                sidecar.store(sequence, stamp);
            }
            producer.publish(|s| {
                s.sequence = sequence;
                s.data = sequence.wrapping_mul(0x9E3779B97F4A7C15);
                s.stamp = if latency_mode.is_inline() { stamp } else { 0 };
            });
        }
    }
    let elapsed = start.elapsed();

    let output = infra::ProducerOutput::from_elapsed(
        num_events,
        elapsed,
        std::mem::size_of::<SignalEvent>(),
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

fn signal_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    let consumer_id = infra::read_env_usize("MMAP_CONSUMER_ID", 0);
    let consumer_name = format!("c{}", consumer_id);
    let warmup_target: u64 = infra::read_env_u64("MMAP_WARMUP", 100_000);
    let latency_mode = mmap_signal_mode()?;
    let sidecar = if latency_mode.uses_sidecar() {
        Some(MmapTimestampSidecar::open_with_timeout(
            &mmap_sidecar_path_from_env()?,
            buffer_size,
            Duration::from_secs(15),
        )?)
    } else {
        None
    };
    let tsc = if latency_mode.uses_rdtsc() {
        TscCalibration::calibrate()
    } else {
        None
    };

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapConsumer::<SignalEvent>::attach(layout.clone(), buffer_size, &consumer_name) {
            Ok(c) => break c,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(e) => return Err(format!("attach failed: {e}").into()),
        }
    };

    if matches!(latency_mode, SignalLatencyMode::None) {
        let warmup_deadline = infra::spin_deadline();
        let mut warmup_count = 0u64;
        let mut consumed = 0u64;
        let mut start = None;
        loop {
            if let Some(event) = consumer.try_consume_next_leased() {
                if event.sequence < warmup_target {
                    warmup_count += 1;
                } else {
                    start = Some(Instant::now());
                    consumed = 1;
                    break;
                }
            } else {
                infra::check_deadline(warmup_deadline, "raw_ring_mmap signal_consumer warmup");
                std::hint::spin_loop();
            }
            if warmup_count >= warmup_target {
                break;
            }
        }

        let measure_deadline = infra::spin_deadline();
        let start = start.unwrap_or_else(Instant::now);
        let checksum = 0u64;
        while consumed < num_events {
            if consumer.try_consume_next_leased().is_some() {
                consumed += 1;
            } else {
                infra::check_deadline(measure_deadline, "raw_ring_mmap signal_consumer measured");
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
        return Ok(());
    }

    let mut latency = if latency_mode.records_latency() && !latency_mode.uses_offline_samples() {
        Some(LatencyRecorder::default_range())
    } else {
        None
    };
    let mut offline_samples = if latency_mode.uses_offline_samples() {
        Some(OfflineSignalSamples::new(sample_capacity(
            num_events,
            sample_every_from_env(),
        )))
    } else {
        None
    };
    let warmup_deadline = infra::spin_deadline();
    let mut warmup_count = 0u64;
    let mut consumed = 0u64;
    let mut start = None;
    loop {
        let before_progress = warmup_count + consumed;
        if let Some(event) = consumer.try_consume_next_leased() {
            if event.sequence < warmup_target {
                warmup_count += 1;
            } else {
                if start.is_none() {
                    start = Some(Instant::now());
                }
                consumed += 1;
                let send_stamp = if latency_mode.uses_sidecar() {
                    sidecar
                        .as_ref()
                        .map(|timestamps| timestamps.load(event.sequence))
                        .unwrap_or(0)
                } else {
                    event.stamp
                };
                if let Some(recorder) = latency.as_mut() {
                    record_signal_latency(recorder, latency_mode, send_stamp, tsc.as_ref());
                } else if let Some(samples) = offline_samples.as_mut() {
                    collect_signal_latency_offline(samples, latency_mode, send_stamp);
                }
            }
        } else {
            infra::check_deadline(warmup_deadline, "raw_ring_mmap signal_consumer warmup");
            std::hint::spin_loop();
        }
        if consumed > 0 || warmup_count >= warmup_target {
            break;
        }
        if warmup_count + consumed == before_progress {
            std::hint::spin_loop();
        }
    }

    let measure_deadline = infra::spin_deadline();
    let start = start.unwrap_or_else(Instant::now);
    let checksum = 0u64; // Signal class: no payload work — measures pure disruptor ceiling
    while consumed < num_events {
        if let Some(event) = consumer.try_consume_next_leased() {
            if event.sequence >= warmup_target {
                consumed += 1;
                let send_stamp = if latency_mode.uses_sidecar() {
                    sidecar
                        .as_ref()
                        .map(|timestamps| timestamps.load(event.sequence))
                        .unwrap_or(0)
                } else {
                    event.stamp
                };
                if let Some(recorder) = latency.as_mut() {
                    record_signal_latency(recorder, latency_mode, send_stamp, tsc.as_ref());
                } else if let Some(samples) = offline_samples.as_mut() {
                    collect_signal_latency_offline(samples, latency_mode, send_stamp);
                }
            }
        } else {
            infra::check_deadline(measure_deadline, "raw_ring_mmap signal_consumer measured");
            std::hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();

    let latency_stats = if let Some(samples) = offline_samples {
        samples.into_latency_stats(latency_mode, tsc.as_ref())
    } else {
        latency.and_then(|recorder| recorder.stats())
    };
    let output = if let Some(stats) = latency_stats {
        infra::ConsumerOutput::from_elapsed(
            consumer_id,
            consumed,
            elapsed,
            std::mem::size_of::<SignalEvent>(),
            checksum,
        )
        .with_latency(stats)
    } else {
        infra::ConsumerOutput::from_elapsed(
            consumer_id,
            consumed,
            elapsed,
            std::mem::size_of::<SignalEvent>(),
            checksum,
        )
    };
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

fn multi_signal_producer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    let num_consumers = infra::read_env_usize("MMAP_NUM_CONSUMERS", 2);
    let warmup: u64 = infra::read_env_u64("MMAP_WARMUP", 100_000);
    let target_rate = infra::read_env_u64("PERF_BENCH_SIGNAL_TARGET_RATE", 0);
    let latency_mode = mmap_signal_mode()?;

    if matches!(latency_mode, SignalLatencyMode::None) {
        layout.ensure_directories()?;
        let mut producer =
            MmapProducer::<SignalEvent>::create(layout, buffer_size, SignalEvent::default)?;

        if !producer.wait_for_consumers_ready(num_consumers as i64, Duration::from_secs(60)) {
            return Err(format!("Timeout waiting for {num_consumers} consumers").into());
        }

        for i in 0..warmup {
            producer.publish(|s| {
                s.sequence = i;
                s.data = i.wrapping_mul(0x9E3779B97F4A7C15);
            });
        }

        let start = Instant::now();
        if target_rate > 0 {
            let interval_ns = 1_000_000_000u64 / target_rate;
            let base_ns = nanos_now();
            for i in 0..num_events {
                let intended_ns = base_ns.saturating_add(i.saturating_mul(interval_ns));
                while nanos_now() < intended_ns {
                    std::hint::spin_loop();
                }
                let sequence = warmup + i;
                producer.publish(|s| {
                    s.sequence = sequence;
                    s.data = sequence.wrapping_mul(0x9E3779B97F4A7C15);
                });
            }
        } else {
            for i in 0..num_events {
                let sequence = warmup + i;
                producer.publish(|s| {
                    s.sequence = sequence;
                    s.data = sequence.wrapping_mul(0x9E3779B97F4A7C15);
                });
            }
        }
        let elapsed = start.elapsed();

        let output = infra::ProducerOutput::from_elapsed(
            num_events,
            elapsed,
            std::mem::size_of::<SignalEvent>(),
        );
        println!("{}", serde_json::to_string(&output)?);

        let last_seq = (warmup + num_events - 1) as i64;
        producer.wait_until_consumed_with_strategy(
            last_seq,
            Duration::from_secs(90),
            AutoWaitStrategy::BusySpin,
        );
        return Ok(());
    }

    let sample_every = sample_every_from_env();

    layout.ensure_directories()?;
    let mut producer =
        MmapProducer::<SignalEvent>::create(layout, buffer_size, SignalEvent::default)?;
    let mut sidecar = if latency_mode.uses_sidecar() {
        Some(MmapTimestampSidecar::create(
            &mmap_sidecar_path_from_env()?,
            buffer_size,
        )?)
    } else {
        None
    };

    if !producer.wait_for_consumers_ready(num_consumers as i64, Duration::from_secs(60)) {
        return Err(format!("Timeout waiting for {num_consumers} consumers").into());
    }

    for i in 0..warmup {
        producer.publish(|s| {
            s.sequence = i;
            s.data = i.wrapping_mul(0x9E3779B97F4A7C15);
            s.stamp = 0;
        });
    }

    let start = Instant::now();
    if target_rate > 0 {
        let interval_ns = 1_000_000_000u64 / target_rate;
        let base_ns = nanos_now();
        for i in 0..num_events {
            let intended_ns = base_ns.saturating_add(i.saturating_mul(interval_ns));
            while nanos_now() < intended_ns {
                std::hint::spin_loop();
            }
            let sequence = warmup + i;
            let stamp = if latency_mode.records_latency() && should_sample(sequence, sample_every) {
                read_stamp(latency_mode)
            } else {
                0
            };
            if let Some(sidecar) = sidecar.as_mut() {
                sidecar.store(sequence, stamp);
            }
            producer.publish(|s| {
                s.sequence = sequence;
                s.data = sequence.wrapping_mul(0x9E3779B97F4A7C15);
                s.stamp = if latency_mode.is_inline() { stamp } else { 0 };
            });
        }
    } else {
        for i in 0..num_events {
            let sequence = warmup + i;
            let stamp = if latency_mode.records_latency() && should_sample(sequence, sample_every) {
                read_stamp(latency_mode)
            } else {
                0
            };
            if let Some(sidecar) = sidecar.as_mut() {
                sidecar.store(sequence, stamp);
            }
            producer.publish(|s| {
                s.sequence = sequence;
                s.data = sequence.wrapping_mul(0x9E3779B97F4A7C15);
                s.stamp = if latency_mode.is_inline() { stamp } else { 0 };
            });
        }
    }
    let elapsed = start.elapsed();

    let output = infra::ProducerOutput::from_elapsed(
        num_events,
        elapsed,
        std::mem::size_of::<SignalEvent>(),
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

fn multi_signal_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer_size: usize = env::var("MMAP_BUFFER_SIZE")?.parse()?;
    let num_events: u64 = env::var("MMAP_EVENTS")?.parse()?;
    let consumer_id = infra::read_env_usize("MMAP_CONSUMER_ID", 0);
    let consumer_name = format!("c{}", consumer_id);
    let warmup: u64 = infra::read_env_u64("MMAP_WARMUP", 100_000);
    let latency_mode = mmap_signal_mode()?;
    let sidecar = if latency_mode.uses_sidecar() {
        Some(MmapTimestampSidecar::open_with_timeout(
            &mmap_sidecar_path_from_env()?,
            buffer_size,
            Duration::from_secs(15),
        )?)
    } else {
        None
    };
    let tsc = if latency_mode.uses_rdtsc() {
        TscCalibration::calibrate()
    } else {
        None
    };

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapConsumer::<SignalEvent>::attach(layout.clone(), buffer_size, &consumer_name) {
            Ok(c) => break c,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(e) => return Err(format!("attach failed: {e}").into()),
        }
    };

    if matches!(latency_mode, SignalLatencyMode::None) {
        let mut warmup_count = 0u64;
        let mut consumed = 0u64;
        let mut start = None;
        let warmup_deadline = infra::spin_deadline();
        loop {
            if let Some(event) = consumer.try_consume_next_leased() {
                if event.sequence < warmup {
                    warmup_count += 1;
                } else {
                    start = Some(Instant::now());
                    consumed = 1;
                    break;
                }
            } else {
                infra::check_deadline(
                    warmup_deadline,
                    "raw_ring_mmap multi_signal_consumer warmup",
                );
                std::hint::spin_loop();
            }
            if warmup_count >= warmup {
                break;
            }
        }
        let measure_deadline = infra::spin_deadline();
        let start = start.unwrap_or_else(Instant::now);
        let checksum = 0u64;
        while consumed < num_events {
            if consumer.try_consume_next_leased().is_some() {
                consumed += 1;
            } else {
                infra::check_deadline(
                    measure_deadline,
                    "raw_ring_mmap multi_signal_consumer measured",
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
        return Ok(());
    }

    let mut latency = if latency_mode.records_latency() && !latency_mode.uses_offline_samples() {
        Some(LatencyRecorder::default_range())
    } else {
        None
    };
    let mut offline_samples = if latency_mode.uses_offline_samples() {
        Some(OfflineSignalSamples::new(sample_capacity(
            num_events,
            sample_every_from_env(),
        )))
    } else {
        None
    };
    let mut warmup_count = 0u64;
    let mut consumed = 0u64;
    let mut start = None;
    let warmup_deadline = infra::spin_deadline();
    loop {
        let before_progress = warmup_count + consumed;
        if let Some(event) = consumer.try_consume_next_leased() {
            if event.sequence < warmup {
                warmup_count += 1;
            } else {
                if start.is_none() {
                    start = Some(Instant::now());
                }
                consumed += 1;
                let send_stamp = if latency_mode.uses_sidecar() {
                    sidecar
                        .as_ref()
                        .map(|timestamps| timestamps.load(event.sequence))
                        .unwrap_or(0)
                } else {
                    event.stamp
                };
                if let Some(recorder) = latency.as_mut() {
                    record_signal_latency(recorder, latency_mode, send_stamp, tsc.as_ref());
                } else if let Some(samples) = offline_samples.as_mut() {
                    collect_signal_latency_offline(samples, latency_mode, send_stamp);
                }
            }
        } else {
            infra::check_deadline(
                warmup_deadline,
                "raw_ring_mmap multi_signal_consumer warmup",
            );
            std::hint::spin_loop();
        }
        if consumed > 0 || warmup_count >= warmup {
            break;
        }
        if warmup_count + consumed == before_progress {
            std::hint::spin_loop();
        }
    }
    let measure_deadline = infra::spin_deadline();
    let start = start.unwrap_or_else(Instant::now);
    let checksum = 0u64;
    while consumed < num_events {
        if let Some(event) = consumer.try_consume_next_leased() {
            if event.sequence >= warmup {
                consumed += 1;
                let send_stamp = if latency_mode.uses_sidecar() {
                    sidecar
                        .as_ref()
                        .map(|timestamps| timestamps.load(event.sequence))
                        .unwrap_or(0)
                } else {
                    event.stamp
                };
                if let Some(recorder) = latency.as_mut() {
                    record_signal_latency(recorder, latency_mode, send_stamp, tsc.as_ref());
                } else if let Some(samples) = offline_samples.as_mut() {
                    collect_signal_latency_offline(samples, latency_mode, send_stamp);
                }
            }
        } else {
            infra::check_deadline(
                measure_deadline,
                "raw_ring_mmap multi_signal_consumer measured",
            );
            std::hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();

    let latency_stats = if let Some(samples) = offline_samples {
        samples.into_latency_stats(latency_mode, tsc.as_ref())
    } else {
        latency.and_then(|recorder| recorder.stats())
    };
    let output = if let Some(stats) = latency_stats {
        infra::ConsumerOutput::from_elapsed(
            consumer_id,
            consumed,
            elapsed,
            std::mem::size_of::<SignalEvent>(),
            checksum,
        )
        .with_latency(stats)
    } else {
        infra::ConsumerOutput::from_elapsed(
            consumer_id,
            consumed,
            elapsed,
            std::mem::size_of::<SignalEvent>(),
            checksum,
        )
    };
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
    if target_rate > 0 {
        let interval_ns = 1_000_000_000u64 / target_rate;
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
                "raw_ring_mmap multi_message_consumer warmup",
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
                "raw_ring_mmap multi_message_consumer measured",
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
            infra::unique_mmap_root(&format!("multi{}c", self.consumers))
        } else {
            infra::unique_mmap_root(&self.label)
        };
        let segment = if self.consumers > 1 {
            infra::unique_mmap_segment(&format!("multi{}c", self.consumers))
        } else {
            infra::unique_mmap_segment(&self.label)
        };
        let root_str = root.display().to_string();
        let signal_mode = SignalLatencyMode::from_env()?;
        let signal_sample_every = sample_every_from_env();
        let extra_env: Vec<(&str, String)> = {
            let mut envs = vec![
                ("MMAP_NUM_CONSUMERS", self.consumers.to_string()),
                ("MMAP_EVENT_SIZE", self.event_bytes.to_string()),
                ("MMAP_WARMUP", self.warmup.to_string()),
            ];
            if matches!(self.label.as_str(), label if label.starts_with("signal_"))
                && signal_mode.uses_sidecar()
            {
                let sidecar_path = root.join(format!("{segment}.signal_timestamps"));
                envs.push((
                    "PERF_BENCH_SIGNAL_SIDECAR_MMAP_PATH",
                    sidecar_path.display().to_string(),
                ));
            }
            envs.push((
                "PERF_BENCH_SIGNAL_LATENCY_MODE",
                signal_mode.as_str().to_string(),
            ));
            envs.push((
                "PERF_BENCH_SIGNAL_SAMPLE_EVERY",
                signal_sample_every.to_string(),
            ));
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
    infra::ChildRole::new("mmap_msg_producer", message_producer),
    infra::ChildRole::new("mmap_msg_consumer", message_consumer),
    infra::ChildRole::new("mmap_sig_producer", signal_producer),
    infra::ChildRole::new("mmap_sig_consumer", signal_consumer),
    infra::ChildRole::new("mmap_multi_sig_producer", multi_signal_producer),
    infra::ChildRole::new("mmap_multi_sig_consumer", multi_signal_consumer),
    infra::ChildRole::new("mmap_multi_msg_producer", multi_message_producer),
    infra::ChildRole::new("mmap_multi_msg_consumer", multi_message_consumer),
];

pub struct RawRingMmapBench;

impl infra::BenchHarness for RawRingMmapBench {
    fn bench_name(&self) -> &'static str {
        "raw_ring_mmap"
    }

    fn child_roles(&self) -> &'static [infra::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> infra::BenchRunResult {
        let selection = RawRingSelection::parse(args)?;
        let target_rate = selection.target_rate();

        if !selection.output_args.json_mode {
            println!("=== Raw Ring MMAP Benchmark ===");
            println!("Backend: file-backed mmap");
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
