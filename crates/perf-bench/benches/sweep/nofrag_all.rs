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
    ConsumerOutput, IpcBenchmark, ProducerOutput, ScenarioChildren,
};
use perf_bench::latency::LatencyRecorder;
use perf_bench::reporting::{self, NofragMatrixEntry};
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

fn find_flag_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.windows(2)
        .find(|window| window[0] == flag)
        .map(|window| window[1].as_str())
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
            let deadline = harness::spin_deadline();
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
                    harness::check_deadline(deadline, concat!(stringify!($cons), " measured"));
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
            let deadline = harness::spin_deadline();
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
                    harness::check_deadline(deadline, concat!(stringify!($cons), " measured"));
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
            let layout = MmapTransportLayout::new(PathBuf::from(&root), seg).expect("layout");
            layout.ensure_directories().expect("dirs");
            let mut producer = MmapProducer::<$ev>::create(layout, buf, || <$ev>::default())?;
            if !producer.wait_for_consumers_ready(num_consumers as i64, Duration::from_secs(30)) {
                return Err(format!("timeout waiting for {num_consumers} consumers").into());
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
            let deadline = harness::spin_deadline();
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
                    harness::check_deadline(deadline, concat!(stringify!($cons), " measured"));
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
            let payloads = make_payloads(batch);
            let layout = MmapTransportLayout::new(PathBuf::from(&root), seg).expect("layout");
            layout.ensure_directories().expect("dirs");
            let mut producer = MmapProducer::<$slot>::create(layout, buf, || <$slot>::default())?;
            if !producer.wait_for_consumers_ready(num_consumers as i64, Duration::from_secs(30)) {
                return Err(format!("timeout waiting for {num_consumers} consumers").into());
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
            let deadline = harness::spin_deadline();
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
                    harness::check_deadline(deadline, concat!(stringify!($cons), " measured"));
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
    slot_size: usize,
    consumers: usize,
}

impl IpcBenchmark for Scenario {
    fn bench_name(&self) -> &str {
        "nofrag_all"
    }

    fn scenario_name(&self) -> String {
        format!(
            "{}_{}_{}_1p{}c",
            self.layer, self.backend, self.size_tag, self.consumers
        )
    }

    fn backend(&self) -> &str {
        self.backend
    }

    fn layer(&self) -> &str {
        self.layer
    }

    fn message_size_bytes(&self) -> usize {
        self.slot_size
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

    fn timeout(&self) -> Duration {
        harness::bench_timeout_duration(300)
    }

    fn print_summary_with_metrics(
        &self,
        _producer: &harness::ProducerOutput,
        _consumers: &[harness::ConsumerOutput],
        _latency: Option<&perf_bench::latency::LatencyStats>,
    ) {
    }

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, harness::BenchError> {
        if self.backend == "shm" {
            let seg = unique_shm_segment(&format!("nfa_{}", self.layer));
            let envs: Vec<(&str, String)> = vec![
                ("BENCH_SEGMENT", seg.clone()),
                ("BENCH_EVENTS", self.events.to_string()),
                ("BENCH_BUFFER", self.buffer.to_string()),
                ("BENCH_BATCH_SIZE", self.batch.to_string()),
                ("BENCH_CONSUMERS", self.consumers.to_string()),
            ];
            let producer = spawn_child(exe, self.prod_role, &envs);
            let consumers = (0..self.consumers)
                .map(|consumer_id| {
                    let mut consumer_envs = envs.clone();
                    consumer_envs.push(("BENCH_CONSUMER_ID", consumer_id.to_string()));
                    spawn_child(exe, self.cons_role, &consumer_envs)
                })
                .collect();
            Ok(ScenarioChildren::new(producer, consumers))
        } else {
            let root = unique_mmap_root(&format!("nfa_mmap_{}", self.layer));
            let seg = format!("nfa_{}", std::process::id() % 10000);
            let envs: Vec<(&str, String)> = vec![
                ("BENCH_ROOT", root.display().to_string()),
                ("BENCH_SEGMENT", seg),
                ("BENCH_EVENTS", self.events.to_string()),
                ("BENCH_BUFFER", self.buffer.to_string()),
                ("BENCH_BATCH_SIZE", self.batch.to_string()),
                ("BENCH_CONSUMERS", self.consumers.to_string()),
            ];
            let producer = spawn_child(exe, self.prod_role, &envs);
            let consumers = (0..self.consumers)
                .map(|consumer_id| {
                    let mut consumer_envs = envs.clone();
                    consumer_envs.push(("BENCH_CONSUMER_ID", consumer_id.to_string()));
                    spawn_child(exe, self.cons_role, &consumer_envs)
                })
                .collect();
            Ok(ScenarioChildren::new(producer, consumers).with_cleanup_path(root))
        }
    }
}

impl Scenario {
    fn run(&self) -> Result<(reporting::BenchResult, NofragMatrixEntry), harness::BenchError> {
        let result = self.run_benchmark()?;
        let entry = NofragMatrixEntry {
            layer: self.layer.to_string(),
            backend: self.backend.to_string(),
            payload_label: self.size_tag.to_string(),
            slot_size: self.slot_size,
            ring_depth: self.buffer,
            producers: 1,
            consumers: self.consumers,
            prod_ops: result.results.producer_throughput_ops_sec,
            cons_ops: result.results.consumer_throughput_ops_sec,
            p50_ns: result
                .latency
                .as_ref()
                .map(|stats| stats.p50_ns)
                .unwrap_or(0),
            p99_ns: result
                .latency
                .as_ref()
                .map(|stats| stats.p99_ns)
                .unwrap_or(0),
        };
        Ok((result, entry))
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

    fn run_orchestrator(&self, args: &[String]) -> harness::BenchRunResult {
        let _log = perf_bench::bench_log::BenchLog::default_capacity("nofrag_all");
        let output_args = reporting::ReportOutputArgs::from_args(args);
        let consumers_arg = find_flag_value(args, "--consumers").unwrap_or("all");
        let backend_arg = find_flag_value(args, "--backend").unwrap_or("all");
        let layer_arg = find_flag_value(args, "--layer").unwrap_or("all");
        let size_arg = find_flag_value(args, "--size").unwrap_or("all");

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
        let consumer_counts = [1usize, 2, 4, 6, 8, 12];

        if !output_args.json_mode {
            println!("=== No-Frag Zero-Copy Complete Matrix ===");
            println!(
                "3 layers (raw_ring, rkyv, flatbuf) x 2 backends (SHM, mmap) x 4 sizes x 6 consumer counts = 144 scenarios"
            );
            println!("All consumers read ALL field data (fair comparison)");
            println!();
        }

        let mut results = Vec::new();
        let mut report = reporting::BenchReport::new();

        macro_rules! run_entry {
            ($scenario:expr, $label:expr, $backend:expr) => {{
                let (bench_result, entry) = ($scenario).run()?;
                if !output_args.json_mode {
                    println!(
                        "  {:<12} {:<5} prod={:>10} cons={:>10}",
                        $label,
                        $backend,
                        format_throughput(entry.prod_ops),
                        format_throughput(entry.cons_ops)
                    );
                }
                report.add(bench_result);
                results.push(entry);
            }};
        }

        for s in &sizes {
            let size_matches = size_arg == "all" || size_arg == s.tag;
            if !size_matches {
                continue;
            }
            if !output_args.json_mode {
                println!("--- {} (batch={}) ---", s.tag, s.batch);
            }

            for consumers in consumer_counts {
                let consumers_match = consumers_arg == "all"
                    || consumers_arg.parse::<usize>().ok() == Some(consumers);
                if !consumers_match {
                    continue;
                }

                if (layer_arg == "all" || layer_arg == "raw_ring")
                    && (backend_arg == "all" || backend_arg == "shm")
                {
                    run_entry!(
                        Scenario {
                            layer: "raw_ring",
                            backend: "shm",
                            prod_role: s.raw_shm_prod,
                            cons_role: s.raw_shm_cons,
                            events: s.events,
                            buffer: scaled_buffer_depth(s.buffer, consumers),
                            batch: s.batch,
                            size_tag: s.tag,
                            slot_size: s.raw_slot,
                            consumers,
                        },
                        "raw_ring",
                        "SHM"
                    );
                }

                if (layer_arg == "all" || layer_arg == "raw_ring")
                    && (backend_arg == "all" || backend_arg == "mmap")
                {
                    run_entry!(
                        Scenario {
                            layer: "raw_ring",
                            backend: "mmap",
                            prod_role: s.raw_mmap_prod,
                            cons_role: s.raw_mmap_cons,
                            events: s.events,
                            buffer: scaled_buffer_depth(s.buffer, consumers),
                            batch: s.batch,
                            size_tag: s.tag,
                            slot_size: s.raw_slot,
                            consumers,
                        },
                        "raw_ring",
                        "mmap"
                    );
                }

                if (layer_arg == "all" || layer_arg == "rkyv_nofrag")
                    && (backend_arg == "all" || backend_arg == "shm")
                {
                    run_entry!(
                        Scenario {
                            layer: "rkyv_nofrag",
                            backend: "shm",
                            prod_role: s.rkyv_shm_prod,
                            cons_role: s.rkyv_shm_cons,
                            events: s.events,
                            buffer: scaled_buffer_depth(s.buffer, consumers),
                            batch: s.batch,
                            size_tag: s.tag,
                            slot_size: s.codec_slot,
                            consumers,
                        },
                        "rkyv_nf",
                        "SHM"
                    );
                }

                if (layer_arg == "all" || layer_arg == "flatbuf_nf")
                    && (backend_arg == "all" || backend_arg == "shm")
                {
                    run_entry!(
                        Scenario {
                            layer: "flatbuf_nf",
                            backend: "shm",
                            prod_role: s.fb_shm_prod,
                            cons_role: s.fb_shm_cons,
                            events: s.events,
                            buffer: scaled_buffer_depth(s.buffer, consumers),
                            batch: s.batch,
                            size_tag: s.tag,
                            slot_size: s.codec_slot,
                            consumers,
                        },
                        "flatbuf_nf",
                        "SHM"
                    );
                }

                if (layer_arg == "all" || layer_arg == "rkyv_nofrag")
                    && (backend_arg == "all" || backend_arg == "mmap")
                {
                    run_entry!(
                        Scenario {
                            layer: "rkyv_nofrag",
                            backend: "mmap",
                            prod_role: s.rkyv_mmap_prod,
                            cons_role: s.rkyv_mmap_cons,
                            events: s.events,
                            buffer: scaled_buffer_depth(s.buffer, consumers),
                            batch: s.batch,
                            size_tag: s.tag,
                            slot_size: s.codec_slot,
                            consumers,
                        },
                        "rkyv_nf",
                        "mmap"
                    );
                }

                if (layer_arg == "all" || layer_arg == "flatbuf_nf")
                    && (backend_arg == "all" || backend_arg == "mmap")
                {
                    run_entry!(
                        Scenario {
                            layer: "flatbuf_nf",
                            backend: "mmap",
                            prod_role: s.fb_mmap_prod,
                            cons_role: s.fb_mmap_cons,
                            events: s.events,
                            buffer: scaled_buffer_depth(s.buffer, consumers),
                            batch: s.batch,
                            size_tag: s.tag,
                            slot_size: s.codec_slot,
                            consumers,
                        },
                        "flatbuf_nf",
                        "mmap"
                    );
                }
            }
        }

        if !output_args.json_mode {
            reporting::print_nofrag_matrix(&results);
        }
        reporting::emit_report(&report, &output_args, None, None, None);
        Ok(())
    }
}

perf_bench::myelon_bench_main!(NofragAllBench);
