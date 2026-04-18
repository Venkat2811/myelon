//! No-frag zero-copy sweep: rkyv + flatbuf over SHM + mmap.
//!
//! All layers use right-sized ring slots (slot = encoded payload).
//! Measures the TRUE zero-copy path: serialize -> ring -> access in-place.
//!
//! Run: cargo bench -p perf-bench --bench nofrag_all

use disruptor_mp::{
    build_shared_single_producer, CoordinationMode, MmapConsumer, MmapProducer,
    MmapTransportLayout, SharedDisruptorBuilder, SharedMemoryConfig,
};
use perf_bench::codec_payloads::{
    access_flatbuf, access_raw, access_rkyv, encode_flatbuf, encode_rkyv, make_payloads,
};
use perf_bench::coordination::BenchmarkCoordination;
use perf_bench::events::{format_throughput, nanos_now, BenchEvent};
use perf_bench::harness::{
    self, read_env_u64, read_env_usize, spawn_child, unique_mmap_root, unique_shm_segment,
    ConsumerOutput, ProducerOutput,
};
use perf_bench::latency::LatencyRecorder;
use std::env;
use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tabled::{settings::Style, Table, Tabled};

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
            let payloads = make_payloads(batch);
            let mut producer = build_shared_single_producer::<$slot>(&seg, buf)
                .enable_discovery(1)
                .with_coordination(CoordinationMode::Immediate)
                .build_producer(|| <$slot>::default())?;
            let coord = BenchmarkCoordination::create(&seg)?;
            if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
                return Err("timeout".into());
            }
            for _ in 0..20 {
                let _ = producer.min_gating_sequence();
                std::thread::sleep(Duration::from_millis(2));
            }
            let start = Instant::now();
            for _ in 0..events {
                let enc = $encode_fn(&payloads);
                producer.publish(|slot| {
                    slot.ts_ns = nanos_now();
                    slot.len = enc.len() as u32;
                    slot.data[..enc.len()].copy_from_slice(enc.as_ref());
                });
            }
            let elapsed = start.elapsed();
            let output =
                ProducerOutput::from_elapsed(events, elapsed, std::mem::size_of::<$slot>());
            println!("{}", serde_json::to_string(&output)?);
            coord.signal_producer_done(events as i64);
            coord.wait_for_consumers_done(1, Duration::from_secs(60));
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
            let mut producer = build_shared_single_producer::<$ev>(&seg, buf)
                .enable_discovery(1)
                .with_coordination(CoordinationMode::Immediate)
                .build_producer(|| <$ev>::default())?;
            let coord = BenchmarkCoordination::create(&seg)?;
            if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
                return Err("timeout".into());
            }
            for _ in 0..20 {
                let _ = producer.min_gating_sequence();
                std::thread::sleep(Duration::from_millis(2));
            }
            let start = Instant::now();
            for i in 0..events {
                producer.publish(|s| {
                    s.sequence = i;
                    s.timestamp_ns = nanos_now();
                    s.payload.fill((i & 0xFF) as u8);
                });
            }
            let elapsed = start.elapsed();
            let output = ProducerOutput::from_elapsed(events, elapsed, std::mem::size_of::<$ev>());
            println!("{}", serde_json::to_string(&output)?);
            coord.signal_producer_done(events as i64);
            coord.wait_for_consumers_done(1, Duration::from_secs(60));
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
            let layout = MmapTransportLayout::new(PathBuf::from(&root), seg).expect("layout");
            layout.ensure_directories().expect("dirs");
            let mut producer = MmapProducer::<$ev>::create(layout, buf, || <$ev>::default())?;
            if !producer.wait_for_consumers_ready(1, Duration::from_secs(30)) {
                return Err("timeout".into());
            }
            let start = Instant::now();
            for i in 0..events {
                producer.publish(|s| {
                    s.sequence = i;
                    s.timestamp_ns = nanos_now();
                    s.payload.fill((i & 0xFF) as u8);
                });
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
            let mut consumed = 0u64;
            let mut start: Option<Instant> = None;
            let mut latency = LatencyRecorder::default_range();
            let mut checksum = 0u64;
            while consumed < events {
                if let Some((_, s)) = consumer.try_consume_next() {
                    if start.is_none() {
                        start = Some(Instant::now());
                    }
                    let payload_sum = access_raw(&s.payload);
                    black_box(payload_sum);
                    checksum = checksum.wrapping_add(payload_sum);
                    latency.record_delta(s.timestamp_ns, nanos_now());
                    consumed += 1;
                } else {
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
            let payloads = make_payloads(batch);
            let layout = MmapTransportLayout::new(PathBuf::from(&root), seg).expect("layout");
            layout.ensure_directories().expect("dirs");
            let mut producer = MmapProducer::<$slot>::create(layout, buf, || <$slot>::default())?;
            if !producer.wait_for_consumers_ready(1, Duration::from_secs(30)) {
                return Err("timeout".into());
            }
            let start = Instant::now();
            for _ in 0..events {
                let enc = $encode_fn(&payloads);
                producer.publish(|slot| {
                    slot.ts_ns = nanos_now();
                    slot.len = enc.len() as u32;
                    slot.data[..enc.len()].copy_from_slice(enc.as_ref());
                });
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
            let mut consumed = 0u64;
            let mut start: Option<Instant> = None;
            let mut latency = LatencyRecorder::default_range();
            let mut checksum = 0u64;
            while consumed < events {
                if let Some((_, slot)) = consumer.try_consume_next() {
                    if start.is_none() {
                        start = Some(Instant::now());
                    }
                    let payload_sum = ($access_fn)(&slot.data[..slot.len as usize]);
                    black_box(payload_sum);
                    checksum = checksum.wrapping_add(payload_sum);
                    latency.record_delta(slot.ts_ns, nanos_now());
                    consumed += 1;
                } else {
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

struct Result2 {
    layer: &'static str,
    backend: &'static str,
    payload_label: String,
    slot_size: usize,
    ring_depth: usize,
    producers: usize,
    consumers: usize,
    prod_ops: f64,
    cons_ops: f64,
    p50_ns: u64,
    p99_ns: u64,
}

fn run_shm(
    layer: &'static str,
    prod_role: &str,
    cons_role: &str,
    events: u64,
    buffer: usize,
    batch: usize,
    size_tag: &str,
    slot_size: usize,
) -> Result2 {
    let seg = unique_shm_segment(&format!("nfa_{layer}"));
    let exe = env::current_exe().expect("exe");
    let envs: Vec<(&str, String)> = vec![
        ("BENCH_SEGMENT", seg.clone()),
        ("BENCH_EVENTS", events.to_string()),
        ("BENCH_BUFFER", buffer.to_string()),
        ("BENCH_BATCH_SIZE", batch.to_string()),
    ];
    let producer = spawn_child(&exe, prod_role, &envs);
    let mut consumer_envs = envs.clone();
    consumer_envs.push(("BENCH_CONSUMER_ID", "0".to_string()));
    let consumer = spawn_child(&exe, cons_role, &consumer_envs);
    let timeout = Duration::from_secs(300);
    let cons = harness::collect_child_output("cons", consumer, timeout);
    let prod = harness::collect_child_output("prod", producer, timeout);
    let prod_metrics: ProducerOutput = harness::parse_child_metrics("producer", &prod);
    let cons_metrics: ConsumerOutput = harness::parse_child_metrics("consumer", &cons);
    Result2 {
        layer,
        backend: "shm",
        payload_label: size_tag.to_string(),
        slot_size,
        ring_depth: buffer,
        producers: 1,
        consumers: 1,
        prod_ops: prod_metrics.throughput_ops_sec,
        cons_ops: cons_metrics.throughput_ops_sec,
        p50_ns: cons_metrics
            .latency
            .as_ref()
            .map(|stats| stats.p50_ns)
            .unwrap_or(0),
        p99_ns: cons_metrics
            .latency
            .as_ref()
            .map(|stats| stats.p99_ns)
            .unwrap_or(0),
    }
}

fn run_mmap(
    layer: &'static str,
    prod_role: &str,
    cons_role: &str,
    events: u64,
    buffer: usize,
    batch: usize,
    size_tag: &str,
    slot_size: usize,
) -> Result2 {
    let root = unique_mmap_root(&format!("nfa_mmap_{layer}"));
    let seg = format!("nfa_{}", std::process::id() % 10000);
    let root_str = root.display().to_string();
    let exe = env::current_exe().expect("exe");
    let envs: Vec<(&str, String)> = vec![
        ("BENCH_ROOT", root_str.clone()),
        ("BENCH_SEGMENT", seg.clone()),
        ("BENCH_EVENTS", events.to_string()),
        ("BENCH_BUFFER", buffer.to_string()),
        ("BENCH_BATCH_SIZE", batch.to_string()),
    ];
    let producer = spawn_child(&exe, prod_role, &envs);
    let mut consumer_envs = envs.clone();
    consumer_envs.push(("BENCH_CONSUMER_ID", "0".to_string()));
    let consumer = spawn_child(&exe, cons_role, &consumer_envs);
    let timeout = Duration::from_secs(300);
    let cons = harness::collect_child_output("cons", consumer, timeout);
    let prod = harness::collect_child_output("prod", producer, timeout);
    let _ = std::fs::remove_dir_all(&root);
    let prod_metrics: ProducerOutput = harness::parse_child_metrics("producer", &prod);
    let cons_metrics: ConsumerOutput = harness::parse_child_metrics("consumer", &cons);
    Result2 {
        layer,
        backend: "mmap",
        payload_label: size_tag.to_string(),
        slot_size,
        ring_depth: buffer,
        producers: 1,
        consumers: 1,
        prod_ops: prod_metrics.throughput_ops_sec,
        cons_ops: cons_metrics.throughput_ops_sec,
        p50_ns: cons_metrics
            .latency
            .as_ref()
            .map(|stats| stats.p50_ns)
            .unwrap_or(0),
        p99_ns: cons_metrics
            .latency
            .as_ref()
            .map(|stats| stats.p99_ns)
            .unwrap_or(0),
    }
}

// ============================================================
// main
// ============================================================

const CHILD_ROLES: &[harness::ChildRole] = &[
    harness::ChildRole::new("raw_shm_prod_1k", raw_shm_prod_1k),
    harness::ChildRole::new("raw_shm_cons_1k", raw_shm_cons_1k),
    harness::ChildRole::new("raw_shm_prod_4k", raw_shm_prod_4k),
    harness::ChildRole::new("raw_shm_cons_4k", raw_shm_cons_4k),
    harness::ChildRole::new("raw_shm_prod_16k", raw_shm_prod_16k),
    harness::ChildRole::new("raw_shm_cons_16k", raw_shm_cons_16k),
    harness::ChildRole::new("raw_shm_prod_64k", raw_shm_prod_64k),
    harness::ChildRole::new("raw_shm_cons_64k", raw_shm_cons_64k),
    harness::ChildRole::new("raw_mmap_prod_1k", raw_mmap_prod_1k),
    harness::ChildRole::new("raw_mmap_cons_1k", raw_mmap_cons_1k),
    harness::ChildRole::new("raw_mmap_prod_4k", raw_mmap_prod_4k),
    harness::ChildRole::new("raw_mmap_cons_4k", raw_mmap_cons_4k),
    harness::ChildRole::new("raw_mmap_prod_16k", raw_mmap_prod_16k),
    harness::ChildRole::new("raw_mmap_cons_16k", raw_mmap_cons_16k),
    harness::ChildRole::new("raw_mmap_prod_64k", raw_mmap_prod_64k),
    harness::ChildRole::new("raw_mmap_cons_64k", raw_mmap_cons_64k),
    harness::ChildRole::new("rkyv_shm_prod_2k", rkyv_shm_prod_2k),
    harness::ChildRole::new("rkyv_shm_cons_2k", rkyv_shm_cons_2k),
    harness::ChildRole::new("rkyv_shm_prod_8k", rkyv_shm_prod_8k),
    harness::ChildRole::new("rkyv_shm_cons_8k", rkyv_shm_cons_8k),
    harness::ChildRole::new("rkyv_shm_prod_32k", rkyv_shm_prod_32k),
    harness::ChildRole::new("rkyv_shm_cons_32k", rkyv_shm_cons_32k),
    harness::ChildRole::new("rkyv_shm_prod_128k", rkyv_shm_prod_128k),
    harness::ChildRole::new("rkyv_shm_cons_128k", rkyv_shm_cons_128k),
    harness::ChildRole::new("fb_shm_prod_2k", fb_shm_prod_2k),
    harness::ChildRole::new("fb_shm_cons_2k", fb_shm_cons_2k),
    harness::ChildRole::new("fb_shm_prod_8k", fb_shm_prod_8k),
    harness::ChildRole::new("fb_shm_cons_8k", fb_shm_cons_8k),
    harness::ChildRole::new("fb_shm_prod_32k", fb_shm_prod_32k),
    harness::ChildRole::new("fb_shm_cons_32k", fb_shm_cons_32k),
    harness::ChildRole::new("fb_shm_prod_128k", fb_shm_prod_128k),
    harness::ChildRole::new("fb_shm_cons_128k", fb_shm_cons_128k),
    harness::ChildRole::new("rkyv_mmap_prod_2k", rkyv_mmap_prod_2k),
    harness::ChildRole::new("rkyv_mmap_cons_2k", rkyv_mmap_cons_2k),
    harness::ChildRole::new("rkyv_mmap_prod_8k", rkyv_mmap_prod_8k),
    harness::ChildRole::new("rkyv_mmap_cons_8k", rkyv_mmap_cons_8k),
    harness::ChildRole::new("rkyv_mmap_prod_32k", rkyv_mmap_prod_32k),
    harness::ChildRole::new("rkyv_mmap_cons_32k", rkyv_mmap_cons_32k),
    harness::ChildRole::new("rkyv_mmap_prod_128k", rkyv_mmap_prod_128k),
    harness::ChildRole::new("rkyv_mmap_cons_128k", rkyv_mmap_cons_128k),
    harness::ChildRole::new("fb_mmap_prod_2k", fb_mmap_prod_2k),
    harness::ChildRole::new("fb_mmap_cons_2k", fb_mmap_cons_2k),
    harness::ChildRole::new("fb_mmap_prod_8k", fb_mmap_prod_8k),
    harness::ChildRole::new("fb_mmap_cons_8k", fb_mmap_cons_8k),
    harness::ChildRole::new("fb_mmap_prod_32k", fb_mmap_prod_32k),
    harness::ChildRole::new("fb_mmap_cons_32k", fb_mmap_cons_32k),
    harness::ChildRole::new("fb_mmap_prod_128k", fb_mmap_prod_128k),
    harness::ChildRole::new("fb_mmap_cons_128k", fb_mmap_cons_128k),
];

struct NofragAllBench;

impl harness::BenchHarness for NofragAllBench {
    fn bench_name(&self) -> &'static str {
        "nofrag_all"
    }

    fn child_roles(&self) -> &'static [harness::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, _args: &[String]) -> harness::BenchRunResult {
        let _log = perf_bench::bench_log::BenchLog::default_capacity("nofrag_all");

        struct SizeConfig {
            tag: &'static str,
            batch: usize,
            events: u64,
            buffer: usize,
            raw_slot: usize,
            codec_slot: usize,
            raw_shm_prod: &'static str,
            raw_shm_cons: &'static str,
            raw_mmap_prod: &'static str,
            raw_mmap_cons: &'static str,
            rkyv_shm_prod: &'static str,
            rkyv_shm_cons: &'static str,
            fb_shm_prod: &'static str,
            fb_shm_cons: &'static str,
            rkyv_mmap_prod: &'static str,
            rkyv_mmap_cons: &'static str,
            fb_mmap_prod: &'static str,
            fb_mmap_cons: &'static str,
        }

        let sizes = [
            SizeConfig {
                tag: "1KB",
                batch: 2,
                events: 200_000,
                buffer: 16_384,
                raw_slot: 1024,
                codec_slot: 2048,
                raw_shm_prod: "raw_shm_prod_1k",
                raw_shm_cons: "raw_shm_cons_1k",
                raw_mmap_prod: "raw_mmap_prod_1k",
                raw_mmap_cons: "raw_mmap_cons_1k",
                rkyv_shm_prod: "rkyv_shm_prod_2k",
                rkyv_shm_cons: "rkyv_shm_cons_2k",
                fb_shm_prod: "fb_shm_prod_2k",
                fb_shm_cons: "fb_shm_cons_2k",
                rkyv_mmap_prod: "rkyv_mmap_prod_2k",
                rkyv_mmap_cons: "rkyv_mmap_cons_2k",
                fb_mmap_prod: "fb_mmap_prod_2k",
                fb_mmap_cons: "fb_mmap_cons_2k",
            },
            SizeConfig {
                tag: "4KB",
                batch: 8,
                events: 100_000,
                buffer: 16_384,
                raw_slot: 4096,
                codec_slot: 8192,
                raw_shm_prod: "raw_shm_prod_4k",
                raw_shm_cons: "raw_shm_cons_4k",
                raw_mmap_prod: "raw_mmap_prod_4k",
                raw_mmap_cons: "raw_mmap_cons_4k",
                rkyv_shm_prod: "rkyv_shm_prod_8k",
                rkyv_shm_cons: "rkyv_shm_cons_8k",
                fb_shm_prod: "fb_shm_prod_8k",
                fb_shm_cons: "fb_shm_cons_8k",
                rkyv_mmap_prod: "rkyv_mmap_prod_8k",
                rkyv_mmap_cons: "rkyv_mmap_cons_8k",
                fb_mmap_prod: "fb_mmap_prod_8k",
                fb_mmap_cons: "fb_mmap_cons_8k",
            },
            SizeConfig {
                tag: "16KB",
                batch: 28,
                events: 50_000,
                buffer: 16_384,
                raw_slot: 16384,
                codec_slot: 32768,
                raw_shm_prod: "raw_shm_prod_16k",
                raw_shm_cons: "raw_shm_cons_16k",
                raw_mmap_prod: "raw_mmap_prod_16k",
                raw_mmap_cons: "raw_mmap_cons_16k",
                rkyv_shm_prod: "rkyv_shm_prod_32k",
                rkyv_shm_cons: "rkyv_shm_cons_32k",
                fb_shm_prod: "fb_shm_prod_32k",
                fb_shm_cons: "fb_shm_cons_32k",
                rkyv_mmap_prod: "rkyv_mmap_prod_32k",
                rkyv_mmap_cons: "rkyv_mmap_cons_32k",
                fb_mmap_prod: "fb_mmap_prod_32k",
                fb_mmap_cons: "fb_mmap_cons_32k",
            },
            SizeConfig {
                tag: "64KB",
                batch: 110,
                events: 20_000,
                buffer: 16_384,
                raw_slot: 65536,
                codec_slot: 131072,
                raw_shm_prod: "raw_shm_prod_64k",
                raw_shm_cons: "raw_shm_cons_64k",
                raw_mmap_prod: "raw_mmap_prod_64k",
                raw_mmap_cons: "raw_mmap_cons_64k",
                rkyv_shm_prod: "rkyv_shm_prod_128k",
                rkyv_shm_cons: "rkyv_shm_cons_128k",
                fb_shm_prod: "fb_shm_prod_128k",
                fb_shm_cons: "fb_shm_cons_128k",
                rkyv_mmap_prod: "rkyv_mmap_prod_128k",
                rkyv_mmap_cons: "rkyv_mmap_cons_128k",
                fb_mmap_prod: "fb_mmap_prod_128k",
                fb_mmap_cons: "fb_mmap_cons_128k",
            },
        ];

        println!("=== No-Frag Zero-Copy Complete Matrix ===");
        println!(
            "3 layers (raw_ring, rkyv, flatbuf) x 2 backends (SHM, mmap) x 4 sizes = 24 scenarios"
        );
        println!("All consumers read ALL field data (fair comparison)");
        println!();

        let mut results = Vec::new();
        let mut raw_baselines: std::collections::HashMap<String, f64> =
            std::collections::HashMap::new();

        for s in &sizes {
            println!("--- {} (batch={}) ---", s.tag, s.batch);

            let r = run_shm(
                "raw_ring",
                s.raw_shm_prod,
                s.raw_shm_cons,
                s.events,
                s.buffer,
                s.batch,
                s.tag,
                s.raw_slot,
            );
            println!(
                "  raw_ring     SHM   prod={:>10} cons={:>10}",
                format_throughput(r.prod_ops),
                format_throughput(r.cons_ops)
            );
            raw_baselines.insert(format!("{}_shm", s.tag), r.cons_ops);
            results.push(r);

            let r = run_mmap(
                "raw_ring",
                s.raw_mmap_prod,
                s.raw_mmap_cons,
                s.events,
                s.buffer,
                s.batch,
                s.tag,
                s.raw_slot,
            );
            println!(
                "  raw_ring     mmap  prod={:>10} cons={:>10}",
                format_throughput(r.prod_ops),
                format_throughput(r.cons_ops)
            );
            raw_baselines.insert(format!("{}_mmap", s.tag), r.cons_ops);
            results.push(r);

            let r = run_shm(
                "rkyv_nofrag",
                s.rkyv_shm_prod,
                s.rkyv_shm_cons,
                s.events,
                s.buffer,
                s.batch,
                s.tag,
                s.codec_slot,
            );
            println!(
                "  rkyv_nf      SHM   prod={:>10} cons={:>10}",
                format_throughput(r.prod_ops),
                format_throughput(r.cons_ops)
            );
            results.push(r);

            let r = run_shm(
                "flatbuf_nf",
                s.fb_shm_prod,
                s.fb_shm_cons,
                s.events,
                s.buffer,
                s.batch,
                s.tag,
                s.codec_slot,
            );
            println!(
                "  flatbuf_nf   SHM   prod={:>10} cons={:>10}",
                format_throughput(r.prod_ops),
                format_throughput(r.cons_ops)
            );
            results.push(r);

            let r = run_mmap(
                "rkyv_nofrag",
                s.rkyv_mmap_prod,
                s.rkyv_mmap_cons,
                s.events,
                s.buffer,
                s.batch,
                s.tag,
                s.codec_slot,
            );
            println!(
                "  rkyv_nf      mmap  prod={:>10} cons={:>10}",
                format_throughput(r.prod_ops),
                format_throughput(r.cons_ops)
            );
            results.push(r);

            let r = run_mmap(
                "flatbuf_nf",
                s.fb_mmap_prod,
                s.fb_mmap_cons,
                s.events,
                s.buffer,
                s.batch,
                s.tag,
                s.codec_slot,
            );
            println!(
                "  flatbuf_nf   mmap  prod={:>10} cons={:>10}",
                format_throughput(r.prod_ops),
                format_throughput(r.cons_ops)
            );
            results.push(r);
        }

        // Summary table
        fn fmt_bytes(b: usize) -> String {
            if b >= 1024 * 1024 {
                format!("{}MB", b / (1024 * 1024))
            } else if b >= 1024 {
                format!("{}KB", b / 1024)
            } else {
                format!("{}B", b)
            }
        }
        fn fmt_ring_size(slot: usize, depth: usize) -> String {
            let total = slot * depth;
            if total >= 1024 * 1024 * 1024 {
                format!("{:.1}GB", total as f64 / (1024.0 * 1024.0 * 1024.0))
            } else if total >= 1024 * 1024 {
                format!("{}MB", total / (1024 * 1024))
            } else {
                format!("{}KB", total / 1024)
            }
        }

        fn fmt_latency(ns: u64) -> String {
            if ns == 0 {
                return "-".to_string();
            }
            if ns < 1_000 {
                format!("{}ns", ns)
            } else if ns < 1_000_000 {
                format!("{:.1}us", ns as f64 / 1_000.0)
            } else {
                format!("{:.2}ms", ns as f64 / 1_000_000.0)
            }
        }

        #[derive(Tabled)]
        struct Row {
            #[tabled(rename = "Payload")]
            size: String,
            #[tabled(rename = "Layer")]
            layer: String,
            #[tabled(rename = "Backend")]
            backend: String,
            #[tabled(rename = "Slot")]
            slot: String,
            #[tabled(rename = "Depth")]
            depth: String,
            #[tabled(rename = "Ring")]
            ring_size: String,
            #[tabled(rename = "P")]
            producers: String,
            #[tabled(rename = "C")]
            consumers: String,
            #[tabled(rename = "Producer\n(ops/s)")]
            prod: String,
            #[tabled(rename = "Consumer\n(ops/s)")]
            cons: String,
            #[tabled(rename = "P50")]
            p50: String,
            #[tabled(rename = "P99")]
            p99: String,
            #[tabled(rename = "% of\nRaw")]
            pct: String,
        }

        let rows: Vec<Row> = results
            .iter()
            .map(|r| {
                let size = &r.payload_label;
                let baseline_key = format!("{}_{}", size, r.backend);
                let baseline = raw_baselines.get(&baseline_key).copied().unwrap_or(1.0);
                Row {
                    size: size.clone(),
                    layer: r.layer.to_string(),
                    backend: r.backend.to_string(),
                    slot: fmt_bytes(r.slot_size),
                    depth: format!("{}", r.ring_depth),
                    ring_size: fmt_ring_size(r.slot_size, r.ring_depth),
                    producers: format!("{}", r.producers),
                    consumers: format!("{}", r.consumers),
                    prod: format_throughput(r.prod_ops),
                    cons: format_throughput(r.cons_ops),
                    p50: fmt_latency(r.p50_ns),
                    p99: fmt_latency(r.p99_ns),
                    pct: format!("{:.0}%", r.cons_ops / baseline * 100.0),
                }
            })
            .collect();

        println!("\n{}", Table::new(rows).with(Style::modern()));
        Ok(())
    }
}

perf_bench::myelon_bench_main!(NofragAllBench);
