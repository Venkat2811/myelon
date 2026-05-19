//! No-frag zero-copy sweep: rkyv + flatbuf over SHM + mmap.
//!
//! All layers use right-sized ring slots (slot = encoded payload).
//! Measures the TRUE zero-copy path: serialize -> ring -> access in-place.
//!
//! Run: cargo bench -p perf-bench --bench `nofrag_all`

use crate::cli::sweeps::{self as sweep_specs, BasicSweepSelection};
use crate::infra::coordination::BenchmarkCoordination;
use crate::infra::events::{format_throughput, nanos_now, BenchEvent};
use crate::infra::latency::LatencyRecorder;
use crate::infra::output::report::ReportBundleCompat;
use crate::infra::output::reporting;
use crate::infra::{
    self, launch_mmap_group, launch_shm_group, read_env_u64, read_env_usize, ConsumerOutput,
    IpcBenchmark, MultiConsumerSpawn, ProducerOutput, ScenarioChildren,
};
use crate::layers::framed_myelon::codec::payloads::{
    access_flatbuf, access_raw, access_rkyv, encode_flatbuf, encode_rkyv, make_payloads,
};
use disruptor_mp::{
    build_shared_single_producer, CoordinationMode, MmapConsumer, MmapProducer,
    MmapTransportLayout, SharedDisruptorBuilder, SharedMemoryConfig,
};
use std::env;
use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const DISCOVERY_SCAN_SLEEP: Duration = Duration::from_millis(150);

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

fn scaled_buffer_depth(base_buffer: usize, consumers: usize) -> usize {
    let min_depth = consumers.next_power_of_two().max(1) * 256;
    base_buffer.max(min_depth).next_power_of_two()
}

// ============================================================
// Slot types (right-sized, no framing)
// ============================================================

#[repr(C)]
#[derive(Clone, Copy)]
struct Slot2K {
    len: u32,
    _pad: u32,
    ts_ns: u64,
    data: [u8; 2048 - 16],
}
impl Default for Slot2K {
    fn default() -> Self {
        Self {
            len: 0,
            _pad: 0,
            ts_ns: 0,
            data: [0; 2048 - 16],
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Slot8K {
    len: u32,
    _pad: u32,
    ts_ns: u64,
    data: [u8; 8192 - 16],
}
impl Default for Slot8K {
    fn default() -> Self {
        Self {
            len: 0,
            _pad: 0,
            ts_ns: 0,
            data: [0; 8192 - 16],
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Slot32K {
    len: u32,
    _pad: u32,
    ts_ns: u64,
    data: [u8; 32768 - 16],
}
impl Default for Slot32K {
    fn default() -> Self {
        Self {
            len: 0,
            _pad: 0,
            ts_ns: 0,
            data: [0; 32768 - 16],
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Slot128K {
    len: u32,
    _pad: u32,
    ts_ns: u64,
    data: [u8; 131072 - 16],
}
impl Default for Slot128K {
    fn default() -> Self {
        Self {
            len: 0,
            _pad: 0,
            ts_ns: 0,
            data: [0; 131072 - 16],
        }
    }
}

// Raw ring baselines at each size
type Ev1K = BenchEvent<1008>;
type Ev4K = BenchEvent<4080>;
type Ev16K = BenchEvent<{ 16 * 1024 - 16 }>;
type Ev64K = BenchEvent<{ 64 * 1024 - 16 }>;

// ============================================================
// SHM producers/consumers
// ============================================================

macro_rules! shm_nofrag_impl {
    ($slot:ty, $encode_fn:ident, $access_fn:expr, $prod:ident, $cons:ident) => {
        fn $prod() -> Result<(), Box<dyn std::error::Error>> {
            let seg = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
            let buf = read_env_usize("BENCH_BUFFER", 16384);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let batch = read_env_usize("BENCH_BATCH_SIZE", 8);
            let num_consumers = read_env_usize("BENCH_CONSUMERS", 1);
            let target_rate = read_env_u64("BENCH_TARGET_RATE", 0);
            let payloads = make_payloads(batch);
            let mut producer = build_shared_single_producer::<$slot>(&seg, buf)
                .enable_discovery(num_consumers)
                .with_coordination(CoordinationMode::Immediate)
                .build_producer(|| <$slot>::default())?;
            let coord = BenchmarkCoordination::create(&seg)?;
            if !coord.wait_for_consumers(num_consumers, Duration::from_secs(30)) {
                return Err(format!("timeout waiting for {num_consumers} consumers").into());
            }
            warm_discovery_scans(
                || producer.min_gating_sequence(),
                discovery_scan_rounds(num_consumers),
            );
            let start = Instant::now();
            if target_rate > 0 {
                let interval_ns = 1_000_000_000u64 / target_rate;
                let base_ns = nanos_now();
                for i in 0..events {
                    let intended_ns = base_ns + i * interval_ns;
                    while nanos_now() < intended_ns {
                        std::hint::spin_loop();
                    }
                    let enc = $encode_fn(&payloads);
                    producer.publish(|slot| {
                        slot.ts_ns = intended_ns;
                        slot.len = enc.len() as u32;
                        slot.data[..enc.len()].copy_from_slice(enc.as_ref());
                    });
                }
            } else {
                for _ in 0..events {
                    let enc = $encode_fn(&payloads);
                    producer.publish(|slot| {
                        slot.ts_ns = nanos_now();
                        slot.len = enc.len() as u32;
                        slot.data[..enc.len()].copy_from_slice(enc.as_ref());
                    });
                }
            }
            let elapsed = start.elapsed();
            let output =
                ProducerOutput::from_elapsed(events, elapsed, std::mem::size_of::<$slot>());
            println!("{}", serde_json::to_string(&output)?);
            coord.signal_producer_done(events as i64);
            coord.wait_for_consumers_done(num_consumers, Duration::from_secs(60));
            Ok(())
        }

        fn $cons() -> Result<(), Box<dyn std::error::Error>> {
            let seg = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
            let consumer_id = read_env_usize("BENCH_CONSUMER_ID", 0);
            let buf = read_env_usize("BENCH_BUFFER", 16384);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let coord = BenchmarkCoordination::attach_with_timeout(&seg, Duration::from_secs(30))?;
            let config = SharedMemoryConfig {
                name: seg,
                buffer_size: buf,
                element_size: std::mem::size_of::<$slot>(),
                create: false,
            };
            let mut consumer = SharedDisruptorBuilder::<$slot>::new(config).build_consumer()?;
            coord.signal_consumer_ready();
            let deadline = infra::spin_deadline();
            let mut consumed = 0u64;
            let mut start: Option<Instant> = None;
            let mut latency = LatencyRecorder::default_range();
            let mut checksum = 0u64;
            while consumed < events {
                consumer.process_available(|slot, _| {
                    if start.is_none() {
                        start = Some(Instant::now());
                    }
                    let payload_sum = ($access_fn)(&slot.data[..slot.len as usize]);
                    black_box(payload_sum);
                    checksum = checksum.wrapping_add(payload_sum);
                    latency.record_delta(slot.ts_ns, nanos_now());
                    consumed += 1;
                });
                if consumed < events {
                    infra::check_deadline(deadline, concat!(stringify!($cons), " measured"));
                    std::hint::spin_loop();
                }
            }
            let elapsed = start.expect("consumer never received a slot").elapsed();
            let output = if let Some(stats) = latency.stats() {
                ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    std::mem::size_of::<$slot>(),
                    checksum,
                )
                .with_latency(stats)
            } else {
                ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    std::mem::size_of::<$slot>(),
                    checksum,
                )
            };
            println!("{}", serde_json::to_string(&output)?);
            coord.signal_consumer_done(consumed as i64);
            Ok(())
        }
    };
}

// SHM: rkyv at all sizes
shm_nofrag_impl!(
    Slot2K,
    encode_rkyv,
    access_rkyv,
    rkyv_shm_prod_2k,
    rkyv_shm_cons_2k
);
shm_nofrag_impl!(
    Slot8K,
    encode_rkyv,
    access_rkyv,
    rkyv_shm_prod_8k,
    rkyv_shm_cons_8k
);
shm_nofrag_impl!(
    Slot32K,
    encode_rkyv,
    access_rkyv,
    rkyv_shm_prod_32k,
    rkyv_shm_cons_32k
);
shm_nofrag_impl!(
    Slot128K,
    encode_rkyv,
    access_rkyv,
    rkyv_shm_prod_128k,
    rkyv_shm_cons_128k
);
// SHM: flatbuf at all sizes
shm_nofrag_impl!(
    Slot2K,
    encode_flatbuf,
    access_flatbuf,
    fb_shm_prod_2k,
    fb_shm_cons_2k
);
shm_nofrag_impl!(
    Slot8K,
    encode_flatbuf,
    access_flatbuf,
    fb_shm_prod_8k,
    fb_shm_cons_8k
);
shm_nofrag_impl!(
    Slot32K,
    encode_flatbuf,
    access_flatbuf,
    fb_shm_prod_32k,
    fb_shm_cons_32k
);
shm_nofrag_impl!(
    Slot128K,
    encode_flatbuf,
    access_flatbuf,
    fb_shm_prod_128k,
    fb_shm_cons_128k
);
// SHM: raw ring at all sizes
macro_rules! raw_shm_impl {
    ($ev:ty, $prod:ident, $cons:ident) => {
        fn $prod() -> Result<(), Box<dyn std::error::Error>> {
            let seg = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
            let buf = read_env_usize("BENCH_BUFFER", 16384);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let num_consumers = read_env_usize("BENCH_CONSUMERS", 1);
            let target_rate = read_env_u64("BENCH_TARGET_RATE", 0);
            let mut producer = build_shared_single_producer::<$ev>(&seg, buf)
                .enable_discovery(num_consumers)
                .with_coordination(CoordinationMode::Immediate)
                .build_producer(|| <$ev>::default())?;
            let coord = BenchmarkCoordination::create(&seg)?;
            if !coord.wait_for_consumers(num_consumers, Duration::from_secs(30)) {
                return Err(format!("timeout waiting for {num_consumers} consumers").into());
            }
            warm_discovery_scans(
                || producer.min_gating_sequence(),
                discovery_scan_rounds(num_consumers),
            );
            let start = Instant::now();
            if target_rate > 0 {
                let interval_ns = 1_000_000_000u64 / target_rate;
                let base_ns = nanos_now();
                for i in 0..events {
                    let intended_ns = base_ns + i * interval_ns;
                    while nanos_now() < intended_ns {
                        std::hint::spin_loop();
                    }
                    producer.publish(|s| {
                        s.sequence = i;
                        s.timestamp_ns = intended_ns;
                        s.payload.fill((i & 0xFF) as u8);
                    });
                }
            } else {
                for i in 0..events {
                    producer.publish(|s| {
                        s.sequence = i;
                        s.timestamp_ns = nanos_now();
                        s.payload.fill((i & 0xFF) as u8);
                    });
                }
            }
            let elapsed = start.elapsed();
            let output = ProducerOutput::from_elapsed(events, elapsed, std::mem::size_of::<$ev>());
            println!("{}", serde_json::to_string(&output)?);
            coord.signal_producer_done(events as i64);
            coord.wait_for_consumers_done(num_consumers, Duration::from_secs(60));
            Ok(())
        }
        fn $cons() -> Result<(), Box<dyn std::error::Error>> {
            let seg = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
            let consumer_id = read_env_usize("BENCH_CONSUMER_ID", 0);
            let buf = read_env_usize("BENCH_BUFFER", 16384);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let coord = BenchmarkCoordination::attach_with_timeout(&seg, Duration::from_secs(30))?;
            let config = SharedMemoryConfig {
                name: seg,
                buffer_size: buf,
                element_size: std::mem::size_of::<$ev>(),
                create: false,
            };
            let mut consumer = SharedDisruptorBuilder::<$ev>::new(config).build_consumer()?;
            coord.signal_consumer_ready();
            let deadline = infra::spin_deadline();
            let mut consumed = 0u64;
            let mut start: Option<Instant> = None;
            let mut latency = LatencyRecorder::default_range();
            let mut checksum = 0u64;
            while consumed < events {
                consumer.process_available(|s, _| {
                    if start.is_none() {
                        start = Some(Instant::now());
                    }
                    let payload_sum = access_raw(&s.payload);
                    black_box(payload_sum);
                    checksum = checksum.wrapping_add(payload_sum);
                    latency.record_delta(s.timestamp_ns, nanos_now());
                    consumed += 1;
                });
                if consumed < events {
                    infra::check_deadline(deadline, concat!(stringify!($cons), " measured"));
                    std::hint::spin_loop();
                }
            }
            let elapsed = start.expect("consumer never received an event").elapsed();
            let output = if let Some(stats) = latency.stats() {
                ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    std::mem::size_of::<$ev>(),
                    checksum,
                )
                .with_latency(stats)
            } else {
                ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    std::mem::size_of::<$ev>(),
                    checksum,
                )
            };
            println!("{}", serde_json::to_string(&output)?);
            coord.signal_consumer_done(consumed as i64);
            Ok(())
        }
    };
}
raw_shm_impl!(Ev1K, raw_shm_prod_1k, raw_shm_cons_1k);
raw_shm_impl!(Ev4K, raw_shm_prod_4k, raw_shm_cons_4k);
raw_shm_impl!(Ev16K, raw_shm_prod_16k, raw_shm_cons_16k);
raw_shm_impl!(Ev64K, raw_shm_prod_64k, raw_shm_cons_64k);

// Raw ring over mmap
macro_rules! raw_mmap_impl {
    ($ev:ty, $prod:ident, $cons:ident) => {
        fn $prod() -> Result<(), Box<dyn std::error::Error>> {
            let root = env::var("BENCH_ROOT").expect("BENCH_ROOT");
            let seg = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
            let buf = read_env_usize("BENCH_BUFFER", 16384);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let num_consumers = read_env_usize("BENCH_CONSUMERS", 1);
            let target_rate = read_env_u64("BENCH_TARGET_RATE", 0);
            let layout = MmapTransportLayout::new(PathBuf::from(&root), seg).expect("layout");
            layout.ensure_directories().expect("dirs");
            let mut producer = MmapProducer::<$ev>::create(layout, buf, || <$ev>::default())?;
            if !producer.wait_for_consumers_ready(num_consumers as i64, Duration::from_secs(30)) {
                return Err(format!("timeout waiting for {num_consumers} consumers").into());
            }
            let start = Instant::now();
            if target_rate > 0 {
                let interval_ns = 1_000_000_000u64 / target_rate;
                let base_ns = nanos_now();
                for i in 0..events {
                    let intended_ns = base_ns + i * interval_ns;
                    while nanos_now() < intended_ns {
                        std::hint::spin_loop();
                    }
                    producer.publish(|s| {
                        s.sequence = i;
                        s.timestamp_ns = intended_ns;
                        s.payload.fill((i & 0xFF) as u8);
                    });
                }
            } else {
                for i in 0..events {
                    producer.publish(|s| {
                        s.sequence = i;
                        s.timestamp_ns = nanos_now();
                        s.payload.fill((i & 0xFF) as u8);
                    });
                }
            }
            let elapsed = start.elapsed();
            let output = ProducerOutput::from_elapsed(events, elapsed, std::mem::size_of::<$ev>());
            println!("{}", serde_json::to_string(&output)?);
            let last = (events - 1) as i64;
            producer.wait_until_consumed_with_strategy(
                last,
                Duration::from_secs(60),
                disruptor_mp::AutoWaitStrategy::BusySpin,
            );
            Ok(())
        }
        fn $cons() -> Result<(), Box<dyn std::error::Error>> {
            let root = env::var("BENCH_ROOT").expect("BENCH_ROOT");
            let seg = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
            let consumer_id = read_env_usize("BENCH_CONSUMER_ID", 0);
            let buf = read_env_usize("BENCH_BUFFER", 16384);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let layout = MmapTransportLayout::new(PathBuf::from(&root), seg).expect("layout");
            let cid = format!("c{consumer_id}_{}", std::process::id());
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut consumer = loop {
                match MmapConsumer::<$ev>::attach(layout.clone(), buf, &cid) {
                    Ok(c) => break c,
                    Err(_) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(25))
                    }
                    Err(e) => return Err(format!("attach: {e}").into()),
                }
            };
            let deadline = infra::spin_deadline();
            let mut consumed = 0u64;
            let mut start: Option<Instant> = None;
            let mut latency = LatencyRecorder::default_range();
            let mut checksum = 0u64;
            while consumed < events {
                if let Some(s) = consumer.try_consume_next_leased() {
                    if start.is_none() {
                        start = Some(Instant::now());
                    }
                    let payload_sum = access_raw(&s.payload);
                    black_box(payload_sum);
                    checksum = checksum.wrapping_add(payload_sum);
                    latency.record_delta(s.timestamp_ns, nanos_now());
                    consumed += 1;
                } else {
                    infra::check_deadline(deadline, concat!(stringify!($cons), " measured"));
                    std::hint::spin_loop();
                }
            }
            let elapsed = start.expect("consumer never received an event").elapsed();
            let output = if let Some(stats) = latency.stats() {
                ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    std::mem::size_of::<$ev>(),
                    checksum,
                )
                .with_latency(stats)
            } else {
                ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    std::mem::size_of::<$ev>(),
                    checksum,
                )
            };
            println!("{}", serde_json::to_string(&output)?);
            Ok(())
        }
    };
}
raw_mmap_impl!(Ev1K, raw_mmap_prod_1k, raw_mmap_cons_1k);
raw_mmap_impl!(Ev4K, raw_mmap_prod_4k, raw_mmap_cons_4k);
raw_mmap_impl!(Ev16K, raw_mmap_prod_16k, raw_mmap_cons_16k);
raw_mmap_impl!(Ev64K, raw_mmap_prod_64k, raw_mmap_cons_64k);

// ============================================================
// MMAP producers/consumers
// ============================================================

macro_rules! mmap_nofrag_impl {
    ($slot:ty, $encode_fn:ident, $access_fn:expr, $prod:ident, $cons:ident) => {
        fn $prod() -> Result<(), Box<dyn std::error::Error>> {
            let root = env::var("BENCH_ROOT").expect("BENCH_ROOT");
            let seg = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
            let buf = read_env_usize("BENCH_BUFFER", 16384);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let batch = read_env_usize("BENCH_BATCH_SIZE", 8);
            let num_consumers = read_env_usize("BENCH_CONSUMERS", 1);
            let target_rate = read_env_u64("BENCH_TARGET_RATE", 0);
            let payloads = make_payloads(batch);
            let layout = MmapTransportLayout::new(PathBuf::from(&root), seg).expect("layout");
            layout.ensure_directories().expect("dirs");
            let mut producer = MmapProducer::<$slot>::create(layout, buf, || <$slot>::default())?;
            if !producer.wait_for_consumers_ready(num_consumers as i64, Duration::from_secs(30)) {
                return Err(format!("timeout waiting for {num_consumers} consumers").into());
            }
            let start = Instant::now();
            if target_rate > 0 {
                let interval_ns = 1_000_000_000u64 / target_rate;
                let base_ns = nanos_now();
                for i in 0..events {
                    let intended_ns = base_ns + i * interval_ns;
                    while nanos_now() < intended_ns {
                        std::hint::spin_loop();
                    }
                    let enc = $encode_fn(&payloads);
                    producer.publish(|slot| {
                        slot.ts_ns = intended_ns;
                        slot.len = enc.len() as u32;
                        slot.data[..enc.len()].copy_from_slice(enc.as_ref());
                    });
                }
            } else {
                for _ in 0..events {
                    let enc = $encode_fn(&payloads);
                    producer.publish(|slot| {
                        slot.ts_ns = nanos_now();
                        slot.len = enc.len() as u32;
                        slot.data[..enc.len()].copy_from_slice(enc.as_ref());
                    });
                }
            }
            let elapsed = start.elapsed();
            let output =
                ProducerOutput::from_elapsed(events, elapsed, std::mem::size_of::<$slot>());
            println!("{}", serde_json::to_string(&output)?);
            let last = (events - 1) as i64;
            producer.wait_until_consumed_with_strategy(
                last,
                Duration::from_secs(60),
                disruptor_mp::AutoWaitStrategy::BusySpin,
            );
            Ok(())
        }

        fn $cons() -> Result<(), Box<dyn std::error::Error>> {
            let root = env::var("BENCH_ROOT").expect("BENCH_ROOT");
            let seg = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
            let consumer_id = read_env_usize("BENCH_CONSUMER_ID", 0);
            let buf = read_env_usize("BENCH_BUFFER", 16384);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let layout = MmapTransportLayout::new(PathBuf::from(&root), seg).expect("layout");
            let cid = format!("c{consumer_id}_{}", std::process::id());
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut consumer = loop {
                match MmapConsumer::<$slot>::attach(layout.clone(), buf, &cid) {
                    Ok(c) => break c,
                    Err(_) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(25))
                    }
                    Err(e) => return Err(format!("attach: {e}").into()),
                }
            };
            let deadline = infra::spin_deadline();
            let mut consumed = 0u64;
            let mut start: Option<Instant> = None;
            let mut latency = LatencyRecorder::default_range();
            let mut checksum = 0u64;
            while consumed < events {
                if let Some(slot) = consumer.try_consume_next_leased() {
                    if start.is_none() {
                        start = Some(Instant::now());
                    }
                    let payload_sum = ($access_fn)(&slot.data[..slot.len as usize]);
                    black_box(payload_sum);
                    checksum = checksum.wrapping_add(payload_sum);
                    latency.record_delta(slot.ts_ns, nanos_now());
                    consumed += 1;
                } else {
                    infra::check_deadline(deadline, concat!(stringify!($cons), " measured"));
                    std::hint::spin_loop();
                }
            }
            let elapsed = start.expect("consumer never received a slot").elapsed();
            let output = if let Some(stats) = latency.stats() {
                ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    std::mem::size_of::<$slot>(),
                    checksum,
                )
                .with_latency(stats)
            } else {
                ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    std::mem::size_of::<$slot>(),
                    checksum,
                )
            };
            println!("{}", serde_json::to_string(&output)?);
            Ok(())
        }
    };
}

mmap_nofrag_impl!(
    Slot2K,
    encode_rkyv,
    access_rkyv,
    rkyv_mmap_prod_2k,
    rkyv_mmap_cons_2k
);
mmap_nofrag_impl!(
    Slot8K,
    encode_rkyv,
    access_rkyv,
    rkyv_mmap_prod_8k,
    rkyv_mmap_cons_8k
);
mmap_nofrag_impl!(
    Slot32K,
    encode_rkyv,
    access_rkyv,
    rkyv_mmap_prod_32k,
    rkyv_mmap_cons_32k
);
mmap_nofrag_impl!(
    Slot128K,
    encode_rkyv,
    access_rkyv,
    rkyv_mmap_prod_128k,
    rkyv_mmap_cons_128k
);
mmap_nofrag_impl!(
    Slot2K,
    encode_flatbuf,
    access_flatbuf,
    fb_mmap_prod_2k,
    fb_mmap_cons_2k
);
mmap_nofrag_impl!(
    Slot8K,
    encode_flatbuf,
    access_flatbuf,
    fb_mmap_prod_8k,
    fb_mmap_cons_8k
);
mmap_nofrag_impl!(
    Slot32K,
    encode_flatbuf,
    access_flatbuf,
    fb_mmap_prod_32k,
    fb_mmap_cons_32k
);
mmap_nofrag_impl!(
    Slot128K,
    encode_flatbuf,
    access_flatbuf,
    fb_mmap_prod_128k,
    fb_mmap_cons_128k
);

// ============================================================
// Orchestrator
// ============================================================

struct Scenario {
    layer: &'static str,
    backend: &'static str,
    prod_role: &'static str,
    cons_role: &'static str,
    events: u64,
    buffer: usize,
    batch: usize,
    size_tag: &'static str,
    payload_bytes: usize,
    slot_size: usize,
    consumers: usize,
    target_rate: u64,
}

impl IpcBenchmark for Scenario {
    fn bench_name(&self) -> &str {
        "nofrag_all"
    }

    fn scenario_name(&self) -> String {
        let base = format!(
            "{}_{}_{}_1p{}c",
            self.layer, self.backend, self.size_tag, self.consumers
        );
        if self.target_rate > 0 {
            format!("{base}_co_{}rps", self.target_rate)
        } else {
            base
        }
    }

    fn backend(&self) -> &str {
        self.backend
    }

    fn layer(&self) -> &str {
        self.layer
    }

    fn transport_metadata(&self) -> reporting::BenchTransportSpec {
        let base = if self.backend == "shm" {
            reporting::BenchTransportSpec::benchmark_shm(self.consumers)
        } else {
            reporting::BenchTransportSpec::mmap_builtin()
        };
        base.with_zero_copy(matches!(
            self.layer,
            "rkyv_nofrag" | "flatbuf_nf" | "flatbuf_nofrag"
        ))
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
        self.slot_size
    }

    fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    fn buffer_depth(&self) -> usize {
        self.buffer
    }

    fn num_messages(&self) -> u64 {
        self.events
    }

    fn num_consumers(&self) -> usize {
        self.consumers
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

    fn timeout(&self) -> Duration {
        Duration::from_secs(300)
    }

    fn print_summary_with_metrics(
        &self,
        _producer: &infra::ProducerOutput,
        _consumers: &[infra::ConsumerOutput],
        _latency: Option<&crate::infra::latency::LatencyStats>,
    ) {
    }

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, infra::BenchError> {
        if self.backend == "shm" {
            let mut envs: Vec<(&str, String)> = vec![
                ("BENCH_EVENTS", self.events.to_string()),
                ("BENCH_BUFFER", self.buffer.to_string()),
                ("BENCH_BATCH_SIZE", self.batch.to_string()),
                ("BENCH_CONSUMERS", self.consumers.to_string()),
            ];
            if self.target_rate > 0 {
                envs.push(("BENCH_TARGET_RATE", self.target_rate.to_string()));
            }
            launch_shm_group(
                exe,
                &format!("nfa_{}", self.layer),
                "BENCH_SEGMENT",
                MultiConsumerSpawn {
                    producer_role: self.prod_role,
                    consumer_role: self.cons_role,
                    consumers: self.consumers,
                    consumer_id_env: "BENCH_CONSUMER_ID",
                    base_envs: envs,
                },
            )
        } else {
            let mut envs: Vec<(&str, String)> = vec![
                ("BENCH_EVENTS", self.events.to_string()),
                ("BENCH_BUFFER", self.buffer.to_string()),
                ("BENCH_BATCH_SIZE", self.batch.to_string()),
                ("BENCH_CONSUMERS", self.consumers.to_string()),
            ];
            if self.target_rate > 0 {
                envs.push(("BENCH_TARGET_RATE", self.target_rate.to_string()));
            }
            launch_mmap_group(
                exe,
                &format!("nfa_mmap_{}", self.layer),
                &format!("nfa_{}", self.layer),
                "BENCH_ROOT",
                "BENCH_SEGMENT",
                MultiConsumerSpawn {
                    producer_role: self.prod_role,
                    consumer_role: self.cons_role,
                    consumers: self.consumers,
                    consumer_id_env: "BENCH_CONSUMER_ID",
                    base_envs: envs,
                },
            )
        }
    }
}

impl Scenario {
    fn run(&self) -> Result<reporting::BenchResult, infra::BenchError> {
        self.run_benchmark()
    }
}

// ============================================================
// main
// ============================================================

const CHILD_ROLES: &[infra::ChildRole] = &[
    infra::ChildRole::new("raw_shm_prod_1k", raw_shm_prod_1k),
    infra::ChildRole::new("raw_shm_cons_1k", raw_shm_cons_1k),
    infra::ChildRole::new("raw_shm_prod_4k", raw_shm_prod_4k),
    infra::ChildRole::new("raw_shm_cons_4k", raw_shm_cons_4k),
    infra::ChildRole::new("raw_shm_prod_16k", raw_shm_prod_16k),
    infra::ChildRole::new("raw_shm_cons_16k", raw_shm_cons_16k),
    infra::ChildRole::new("raw_shm_prod_64k", raw_shm_prod_64k),
    infra::ChildRole::new("raw_shm_cons_64k", raw_shm_cons_64k),
    infra::ChildRole::new("raw_mmap_prod_1k", raw_mmap_prod_1k),
    infra::ChildRole::new("raw_mmap_cons_1k", raw_mmap_cons_1k),
    infra::ChildRole::new("raw_mmap_prod_4k", raw_mmap_prod_4k),
    infra::ChildRole::new("raw_mmap_cons_4k", raw_mmap_cons_4k),
    infra::ChildRole::new("raw_mmap_prod_16k", raw_mmap_prod_16k),
    infra::ChildRole::new("raw_mmap_cons_16k", raw_mmap_cons_16k),
    infra::ChildRole::new("raw_mmap_prod_64k", raw_mmap_prod_64k),
    infra::ChildRole::new("raw_mmap_cons_64k", raw_mmap_cons_64k),
    infra::ChildRole::new("rkyv_shm_prod_2k", rkyv_shm_prod_2k),
    infra::ChildRole::new("rkyv_shm_cons_2k", rkyv_shm_cons_2k),
    infra::ChildRole::new("rkyv_shm_prod_8k", rkyv_shm_prod_8k),
    infra::ChildRole::new("rkyv_shm_cons_8k", rkyv_shm_cons_8k),
    infra::ChildRole::new("rkyv_shm_prod_32k", rkyv_shm_prod_32k),
    infra::ChildRole::new("rkyv_shm_cons_32k", rkyv_shm_cons_32k),
    infra::ChildRole::new("rkyv_shm_prod_128k", rkyv_shm_prod_128k),
    infra::ChildRole::new("rkyv_shm_cons_128k", rkyv_shm_cons_128k),
    infra::ChildRole::new("fb_shm_prod_2k", fb_shm_prod_2k),
    infra::ChildRole::new("fb_shm_cons_2k", fb_shm_cons_2k),
    infra::ChildRole::new("fb_shm_prod_8k", fb_shm_prod_8k),
    infra::ChildRole::new("fb_shm_cons_8k", fb_shm_cons_8k),
    infra::ChildRole::new("fb_shm_prod_32k", fb_shm_prod_32k),
    infra::ChildRole::new("fb_shm_cons_32k", fb_shm_cons_32k),
    infra::ChildRole::new("fb_shm_prod_128k", fb_shm_prod_128k),
    infra::ChildRole::new("fb_shm_cons_128k", fb_shm_cons_128k),
    infra::ChildRole::new("rkyv_mmap_prod_2k", rkyv_mmap_prod_2k),
    infra::ChildRole::new("rkyv_mmap_cons_2k", rkyv_mmap_cons_2k),
    infra::ChildRole::new("rkyv_mmap_prod_8k", rkyv_mmap_prod_8k),
    infra::ChildRole::new("rkyv_mmap_cons_8k", rkyv_mmap_cons_8k),
    infra::ChildRole::new("rkyv_mmap_prod_32k", rkyv_mmap_prod_32k),
    infra::ChildRole::new("rkyv_mmap_cons_32k", rkyv_mmap_cons_32k),
    infra::ChildRole::new("rkyv_mmap_prod_128k", rkyv_mmap_prod_128k),
    infra::ChildRole::new("rkyv_mmap_cons_128k", rkyv_mmap_cons_128k),
    infra::ChildRole::new("fb_mmap_prod_2k", fb_mmap_prod_2k),
    infra::ChildRole::new("fb_mmap_cons_2k", fb_mmap_cons_2k),
    infra::ChildRole::new("fb_mmap_prod_8k", fb_mmap_prod_8k),
    infra::ChildRole::new("fb_mmap_cons_8k", fb_mmap_cons_8k),
    infra::ChildRole::new("fb_mmap_prod_32k", fb_mmap_prod_32k),
    infra::ChildRole::new("fb_mmap_cons_32k", fb_mmap_cons_32k),
    infra::ChildRole::new("fb_mmap_prod_128k", fb_mmap_prod_128k),
    infra::ChildRole::new("fb_mmap_cons_128k", fb_mmap_cons_128k),
];

pub struct NofragAllBench;

impl infra::BenchHarness for NofragAllBench {
    fn bench_name(&self) -> &'static str {
        "nofrag_all"
    }

    fn child_roles(&self) -> &'static [infra::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> infra::BenchRunResult {
        let _log = crate::infra::output::log::BenchLog::default_capacity("nofrag_all");
        let selection = BasicSweepSelection::parse(args)?;
        let backend_arg = args
            .windows(2)
            .find(|window| window[0] == "--backend")
            .map(|window| window[1].as_str())
            .unwrap_or("all");
        let mode_arg = args
            .windows(2)
            .find(|window| window[0] == "--mode")
            .map(|window| window[1].as_str())
            .unwrap_or("throughput");
        let target_rate_arg = args
            .windows(2)
            .find(|window| window[0] == "--target-rate")
            .and_then(|window| window[1].parse::<u64>().ok())
            .unwrap_or(0);
        let run_throughput = mode_arg == "throughput" || mode_arg == "all";
        let run_co = mode_arg == "co" || mode_arg == "all";

        if !selection.output_args.json_mode {
            println!("=== No-Frag Zero-Copy Complete Matrix ===");
            println!(
                "3 layers (raw_ring, rkyv, flatbuf) x 2 backends (SHM, mmap) x 4 sizes x 6 consumer counts"
            );
            println!(
                "Mode: {}",
                match mode_arg {
                    "all" => "throughput + co_aware",
                    "co" => "co_aware only",
                    _ => "throughput only",
                }
            );
            println!("All consumers read ALL field data (fair comparison).");
            println!();
        }

        let mut report = reporting::BenchReport::new();

        macro_rules! run_entry {
            ($scenario:expr, $label:expr, $backend:expr) => {{
                let bench_result = ($scenario).run()?;
                if !selection.output_args.json_mode {
                    println!(
                        "  {:<12} {:<5} {:<8} prod={:>10} cons={:>10} p99={:>8}",
                        $label,
                        $backend,
                        bench_result.measurement_mode.replace("co_aware@", "CO@"),
                        format_throughput(bench_result.results.producer_throughput_ops_sec),
                        format_throughput(bench_result.results.consumer_throughput_ops_sec),
                        if bench_result.latency.is_none() {
                            "-".to_string()
                        } else {
                            crate::infra::latency::format_ns(
                                bench_result.latency.as_ref().expect("latency").p99_ns,
                            )
                        }
                    );
                }
                report.add(bench_result);
            }};
        }

        for spec in sweep_specs::payload_sweep_specs() {
            if !selection.matches_size(spec.tag) {
                continue;
            }
            let events = selection.events_for(spec.events);
            if !selection.output_args.json_mode {
                println!("--- {} (batch={}) ---", spec.tag, spec.batch_size);
            }

            for consumers in sweep_specs::TARGET_CONSUMERS {
                if !selection.matches_consumers(consumers) {
                    continue;
                }

                let co_target_rate = if target_rate_arg > 0 {
                    target_rate_arg
                } else {
                    sweep_specs::default_co_target_rate(spec.tag)
                };

                for target_rate in [0u64, co_target_rate] {
                    if (target_rate == 0 && !run_throughput) || (target_rate > 0 && !run_co) {
                        continue;
                    }

                    for variant in sweep_specs::nofrag_variant_specs(spec.tag) {
                        if !selection.matches_layer(variant.layer) {
                            continue;
                        }
                        if backend_arg != "all" && backend_arg != variant.backend.slug() {
                            continue;
                        }
                        run_entry!(
                            Scenario {
                                layer: variant.layer,
                                backend: variant.backend.slug(),
                                prod_role: variant.prod_role,
                                cons_role: variant.cons_role,
                                events,
                                buffer: scaled_buffer_depth(spec.buffer_depth, consumers),
                                batch: spec.batch_size,
                                size_tag: spec.tag,
                                payload_bytes: spec.raw_slot_bytes,
                                slot_size: if variant.uses_codec_slot {
                                    spec.codec_slot_bytes
                                } else {
                                    spec.raw_slot_bytes
                                },
                                consumers,
                                target_rate,
                            },
                            variant.summary_label,
                            variant.backend.display_label()
                        );
                    }
                }
            }
        }

        if !selection.output_args.json_mode {
            report.to_report().print_nofrag_matrix();
        }
        reporting::emit_report(&report, &selection.output_args, None, None, None);
        Ok(())
    }
}
