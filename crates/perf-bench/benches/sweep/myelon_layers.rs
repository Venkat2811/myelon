//! Myelon Layer Overhead Sweep — raw ring vs FramedTransport vs rkyv_nofrag.
//!
//! Answers: "How much overhead does each myelon abstraction layer add vs raw disruptor?"
//!
//! At each payload size, measures:
//!   raw_ring:     BenchEvent<SIZE> directly on disruptor ring
//!   framed:       FramedTransport publish(&[u8]) over SHM ring (64KB frame)
//!   framed_batch: FramedTransport batch recv (process_available_messages)
//!   framed_right: FramedTransport with right-sized frame (no bandwidth waste)
//!   rkyv_nofrag:  Zero-copy rkyv with right-sized ring slots (no framing)
//!
//! Run: cargo bench -p perf-bench --bench myelon_layers

use disruptor_mp::{
    build_shared_single_producer, CoordinationMode, SharedDisruptorBuilder, SharedMemoryConfig,
};
use myelon::transport::{
    FixedFrame, FramedTransportConsumer, FramedTransportProducer, MyelonWaitStrategy,
};
use perf_bench::codec_payloads::{access_raw, access_rkyv, encode_rkyv, make_payloads};
use perf_bench::coordination::BenchmarkCoordination;
use perf_bench::events::{format_throughput, nanos_now, BenchEvent};
use perf_bench::harness::{
    self, read_env_u64, read_env_usize, segment_from_env, spawn_child, unique_shm_segment,
    ConsumerOutput, ProducerOutput,
};
use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};
use tabled::{settings::Style, Table, Tabled};

const FRAME_DATA_BYTES: usize = 64 * 1024 - 12;
type Frame = FixedFrame<FRAME_DATA_BYTES>;

// Right-sized frames: slot matches payload to eliminate bandwidth waste
type Frame2K = FixedFrame<{ 2 * 1024 - 12 }>; // for 1KB payloads
type Frame8K = FixedFrame<{ 8 * 1024 - 12 }>; // for 4KB payloads
type Frame32K = FixedFrame<{ 32 * 1024 - 12 }>; // for 16KB payloads

// ============================================================
// Event types for raw ring
// ============================================================

type Ev1K = BenchEvent<1008>;
type Ev4K = BenchEvent<4080>;
type Ev16K = BenchEvent<{ 16 * 1024 - 16 }>;
type Ev64K = BenchEvent<{ 64 * 1024 - 16 }>;

// Slots sized for rkyv encoded output (no framing, slot = message)
// batch=2 ≈ 1.2KB, batch=8 ≈ 4.7KB, batch=28 ≈ 16.4KB, batch=110 ≈ 64KB
#[repr(C)]
#[derive(Clone, Copy)]
struct RkyvSlot2K {
    len: u32,
    _pad: u32,
    data: [u8; 2048 - 8],
}
impl Default for RkyvSlot2K {
    fn default() -> Self {
        Self {
            len: 0,
            _pad: 0,
            data: [0; 2048 - 8],
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RkyvSlot8K {
    len: u32,
    _pad: u32,
    data: [u8; 8192 - 8],
}
impl Default for RkyvSlot8K {
    fn default() -> Self {
        Self {
            len: 0,
            _pad: 0,
            data: [0; 8192 - 8],
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RkyvSlot32K {
    len: u32,
    _pad: u32,
    data: [u8; 32768 - 8],
}
impl Default for RkyvSlot32K {
    fn default() -> Self {
        Self {
            len: 0,
            _pad: 0,
            data: [0; 32768 - 8],
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RkyvSlot128K {
    len: u32,
    _pad: u32,
    data: [u8; 131072 - 8],
}
impl Default for RkyvSlot128K {
    fn default() -> Self {
        Self {
            len: 0,
            _pad: 0,
            data: [0; 131072 - 8],
        }
    }
}

// ============================================================
// rkyv nofrag: raw ring slot = encoded message, no framing
// ============================================================

macro_rules! rkyv_nofrag_impl {
    ($slot:ty, $data_len:expr, $prod_fn:ident, $cons_fn:ident) => {
        fn $prod_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = segment_from_env("BENCHMARK_SEGMENT_NAME");
            let buffer = read_env_usize("BENCH_BUFFER", 16384);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let batch_size = read_env_usize("BENCH_BATCH_SIZE", 8);
            let payloads = make_payloads(batch_size);

            let mut producer = build_shared_single_producer::<$slot>(&segment, buffer)
                .enable_discovery(1)
                .with_coordination(CoordinationMode::Immediate)
                .build_producer(|| <$slot>::default())?;
            let coord = BenchmarkCoordination::create(&segment)?;
            if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
                return Err("timeout".into());
            }
            for _ in 0..20 {
                let _ = producer.min_gating_sequence();
                std::thread::sleep(Duration::from_millis(2));
            }

            let start = Instant::now();
            for _i in 0..events {
                let enc = encode_rkyv(&payloads);
                let enc_bytes = enc.as_ref();
                producer.publish(|slot| {
                    slot.len = enc_bytes.len() as u32;
                    slot.data[..enc_bytes.len()].copy_from_slice(enc_bytes);
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

        fn $cons_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = segment_from_env("BENCHMARK_SEGMENT_NAME");
            let consumer_id = read_env_usize("BENCH_CONSUMER_ID", 0);
            let buffer = read_env_usize("BENCH_BUFFER", 16384);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let coord =
                BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
            let config = SharedMemoryConfig {
                name: segment,
                buffer_size: buffer,
                element_size: std::mem::size_of::<$slot>(),
                create: false,
            };
            let mut consumer = SharedDisruptorBuilder::<$slot>::new(config).build_consumer()?;
            coord.signal_consumer_ready();
            let mut consumed = 0u64;
            let mut start: Option<Instant> = None;
            let mut checksum = 0u64;
            while consumed < events {
                consumer.process_available(|slot, _seq| {
                    if start.is_none() {
                        start = Some(Instant::now());
                    }
                    let len = slot.len as usize;
                    let bytes = &slot.data[..len];
                    let sum = access_rkyv(bytes);
                    black_box(sum);
                    checksum = checksum.wrapping_add(sum);
                    consumed += 1;
                });
                if consumed < events {
                    std::hint::spin_loop();
                }
            }
            let elapsed = start.expect("consumer never received a payload").elapsed();
            let output = ConsumerOutput::from_elapsed(
                consumer_id,
                consumed,
                elapsed,
                std::mem::size_of::<$slot>(),
                checksum,
            );
            println!("{}", serde_json::to_string(&output)?);
            coord.signal_consumer_done(consumed as i64);
            Ok(())
        }
    };
}

rkyv_nofrag_impl!(RkyvSlot2K, 2040, rkyv_nf_prod_2k, rkyv_nf_cons_2k);
rkyv_nofrag_impl!(RkyvSlot8K, 8184, rkyv_nf_prod_8k, rkyv_nf_cons_8k);
rkyv_nofrag_impl!(RkyvSlot32K, 32760, rkyv_nf_prod_32k, rkyv_nf_cons_32k);
rkyv_nofrag_impl!(RkyvSlot128K, 131064, rkyv_nf_prod_128k, rkyv_nf_cons_128k);

// ============================================================
// Raw ring producer/consumer (macro for different sizes)
// ============================================================

macro_rules! raw_impl {
    ($ev:ty, $prod_fn:ident, $cons_fn:ident) => {
        fn $prod_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = segment_from_env("BENCHMARK_SEGMENT_NAME");
            let buffer = read_env_usize("BENCH_BUFFER", 4096);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let mut producer = build_shared_single_producer::<$ev>(&segment, buffer)
                .enable_discovery(1)
                .with_coordination(CoordinationMode::Immediate)
                .build_producer(|| <$ev>::default())?;
            let coord = BenchmarkCoordination::create(&segment)?;
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
        fn $cons_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = segment_from_env("BENCHMARK_SEGMENT_NAME");
            let consumer_id = read_env_usize("BENCH_CONSUMER_ID", 0);
            let buffer = read_env_usize("BENCH_BUFFER", 4096);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let coord =
                BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
            let config = SharedMemoryConfig {
                name: segment,
                buffer_size: buffer,
                element_size: std::mem::size_of::<$ev>(),
                create: false,
            };
            let mut consumer = SharedDisruptorBuilder::<$ev>::new(config).build_consumer()?;
            coord.signal_consumer_ready();
            let mut consumed = 0u64;
            let mut start: Option<Instant> = None;
            let mut checksum = 0u64;
            while consumed < events {
                consumer.process_available(|s, _| {
                    if start.is_none() {
                        start = Some(Instant::now());
                    }
                    let payload_sum = access_raw(&s.payload);
                    black_box(payload_sum);
                    checksum = checksum.wrapping_add(payload_sum);
                    consumed += 1;
                });
                if consumed < events {
                    std::hint::spin_loop();
                }
            }
            let elapsed = start.expect("consumer never received an event").elapsed();
            let output = ConsumerOutput::from_elapsed(
                consumer_id,
                consumed,
                elapsed,
                std::mem::size_of::<$ev>(),
                checksum,
            );
            println!("{}", serde_json::to_string(&output)?);
            coord.signal_consumer_done(consumed as i64);
            Ok(())
        }
    };
}

raw_impl!(Ev1K, raw_prod_1k, raw_cons_1k);
raw_impl!(Ev4K, raw_prod_4k, raw_cons_4k);
raw_impl!(Ev16K, raw_prod_16k, raw_cons_16k);
raw_impl!(Ev64K, raw_prod_64k, raw_cons_64k);

// ============================================================
// Framed producer/consumer
// ============================================================

fn framed_producer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = segment_from_env("BENCHMARK_SEGMENT_NAME");
    let buffer = read_env_usize("BENCH_BUFFER", 1024);
    let events = read_env_u64("BENCH_EVENTS", 50_000);
    let payload_size = read_env_usize("BENCH_PAYLOAD_SIZE", 1024);
    let mut producer = FramedTransportProducer::<Frame>::create(&segment, buffer)?;
    let coord = BenchmarkCoordination::create(&segment)?;
    if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
        return Err("timeout".into());
    }
    producer.discover_consumers(Duration::from_secs(3));
    let payload = vec![42u8; payload_size];
    let start = Instant::now();
    for i in 0..events {
        producer.publish(&payload, (i % 256) as u8);
    }
    let elapsed = start.elapsed();
    let output = ProducerOutput::from_elapsed(events, elapsed, payload_size);
    println!("{}", serde_json::to_string(&output)?);
    coord.signal_producer_done(events as i64);
    coord.wait_for_consumers_done(1, Duration::from_secs(60));
    Ok(())
}

fn framed_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = segment_from_env("BENCHMARK_SEGMENT_NAME");
    let consumer_id = read_env_usize("BENCH_CONSUMER_ID", 0);
    let buffer = read_env_usize("BENCH_BUFFER", 1024);
    let events = read_env_u64("BENCH_EVENTS", 50_000);
    let payload_size = read_env_usize("BENCH_PAYLOAD_SIZE", 1024);
    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
    let mut consumer =
        FramedTransportConsumer::<Frame>::attach(&segment, buffer, MyelonWaitStrategy::BusySpin)?;
    coord.signal_consumer_ready();
    let mut start: Option<Instant> = None;
    let mut consumed = 0u64;
    let mut checksum = 0u64;
    while consumed < events {
        let (_, data) = consumer.recv_message_blocking();
        if start.is_none() {
            start = Some(Instant::now());
        }
        let payload_sum = access_raw(&data);
        black_box(payload_sum);
        checksum = checksum.wrapping_add(payload_sum);
        consumed += 1;
    }
    let elapsed = start.expect("consumer never received a frame").elapsed();
    let output =
        ConsumerOutput::from_elapsed(consumer_id, consumed, elapsed, payload_size, checksum);
    println!("{}", serde_json::to_string(&output)?);
    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

// ============================================================
// Right-sized framed producer/consumer (frame fits payload)
// ============================================================

macro_rules! rightsized_framed_impl {
    ($frame:ty, $prod_fn:ident, $cons_fn:ident) => {
        fn $prod_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = segment_from_env("BENCHMARK_SEGMENT_NAME");
            let buffer = read_env_usize("BENCH_BUFFER", 4096);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let payload_size = read_env_usize("BENCH_PAYLOAD_SIZE", 1024);
            let mut producer = FramedTransportProducer::<$frame>::create(&segment, buffer)?;
            let coord = BenchmarkCoordination::create(&segment)?;
            if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
                return Err("timeout".into());
            }
            producer.discover_consumers(Duration::from_secs(3));
            let payload = vec![42u8; payload_size];
            let start = Instant::now();
            for i in 0..events {
                producer.publish(&payload, (i % 256) as u8);
            }
            let elapsed = start.elapsed();
            let output = ProducerOutput::from_elapsed(events, elapsed, payload_size);
            println!("{}", serde_json::to_string(&output)?);
            coord.signal_producer_done(events as i64);
            coord.wait_for_consumers_done(1, Duration::from_secs(60));
            Ok(())
        }
        fn $cons_fn() -> Result<(), Box<dyn std::error::Error>> {
            use myelon::transport::ReassemblyBuffer;
            let segment = segment_from_env("BENCHMARK_SEGMENT_NAME");
            let consumer_id = read_env_usize("BENCH_CONSUMER_ID", 0);
            let buffer = read_env_usize("BENCH_BUFFER", 4096);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let payload_size = read_env_usize("BENCH_PAYLOAD_SIZE", 1024);
            let coord =
                BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
            let mut consumer = FramedTransportConsumer::<$frame>::attach(
                &segment,
                buffer,
                MyelonWaitStrategy::BusySpin,
            )?;
            let mut reassembly = ReassemblyBuffer::new(256 * 1024);
            coord.signal_consumer_ready();
            let mut consumed = 0u64;
            let mut start: Option<Instant> = None;
            let mut checksum = 0u64;
            while consumed < events {
                consumer.process_available_messages(&mut reassembly, |_kind, data| {
                    if start.is_none() {
                        start = Some(Instant::now());
                    }
                    let payload_sum = access_raw(data);
                    black_box(payload_sum);
                    checksum = checksum.wrapping_add(payload_sum);
                    consumed += 1;
                });
                if consumed < events {
                    std::hint::spin_loop();
                }
            }
            let elapsed = start.expect("consumer never received a message").elapsed();
            let output = ConsumerOutput::from_elapsed(
                consumer_id,
                consumed,
                elapsed,
                payload_size,
                checksum,
            );
            println!("{}", serde_json::to_string(&output)?);
            coord.signal_consumer_done(consumed as i64);
            Ok(())
        }
    };
}

rightsized_framed_impl!(Frame2K, rs_framed_prod_2k, rs_framed_cons_2k);
rightsized_framed_impl!(Frame8K, rs_framed_prod_8k, rs_framed_cons_8k);
rightsized_framed_impl!(Frame32K, rs_framed_prod_32k, rs_framed_cons_32k);

// ============================================================
// Framed BATCH consumer (uses process_available_messages)
// ============================================================

fn framed_batch_consumer() -> Result<(), Box<dyn std::error::Error>> {
    use myelon::transport::ReassemblyBuffer;

    let segment = segment_from_env("BENCHMARK_SEGMENT_NAME");
    let consumer_id = read_env_usize("BENCH_CONSUMER_ID", 0);
    let buffer = read_env_usize("BENCH_BUFFER", 1024);
    let events = read_env_u64("BENCH_EVENTS", 50_000);
    let payload_size = read_env_usize("BENCH_PAYLOAD_SIZE", 1024);
    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
    let mut consumer =
        FramedTransportConsumer::<Frame>::attach(&segment, buffer, MyelonWaitStrategy::BusySpin)?;
    let mut reassembly = ReassemblyBuffer::new(256 * 1024);
    coord.signal_consumer_ready();
    let mut consumed = 0u64;
    let mut start: Option<Instant> = None;
    let mut checksum = 0u64;
    while consumed < events {
        consumer.process_available_messages(&mut reassembly, |_kind, data| {
            if start.is_none() {
                start = Some(Instant::now());
            }
            let payload_sum = access_raw(data);
            black_box(payload_sum);
            checksum = checksum.wrapping_add(payload_sum);
            consumed += 1;
        });
        if consumed < events {
            std::hint::spin_loop();
        }
    }
    let elapsed = start.expect("consumer never received a batch").elapsed();
    let output =
        ConsumerOutput::from_elapsed(consumer_id, consumed, elapsed, payload_size, checksum);
    println!("{}", serde_json::to_string(&output)?);
    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

// ============================================================
// Orchestrator
// ============================================================

struct LayerResult {
    layer: &'static str,
    payload_label: String,
    prod_ops: f64,
    cons_ops: f64,
}

fn run_layer_pair(
    layer: &'static str,
    segment_prefix: &str,
    size_tag: &str,
    prod_role: &str,
    cons_role: &str,
    mut envs: Vec<(&str, String)>,
) -> LayerResult {
    let segment = unique_shm_segment(&format!("{segment_prefix}_{size_tag}"));
    envs.push(("BENCHMARK_SEGMENT_NAME", segment));

    let exe = env::current_exe().expect("exe");
    let producer = spawn_child(&exe, prod_role, &envs);

    let mut consumer_envs = envs.clone();
    consumer_envs.push(("BENCH_CONSUMER_ID", "0".to_string()));
    let consumer = spawn_child(&exe, cons_role, &consumer_envs);

    let timeout = Duration::from_secs(300);
    let cons = harness::collect_child_output("cons", consumer, timeout);
    let prod = harness::collect_child_output("prod", producer, timeout);
    let prod_metrics: ProducerOutput = harness::parse_child_metrics("producer", &prod);
    let cons_metrics: ConsumerOutput = harness::parse_child_metrics("consumer", &cons);

    LayerResult {
        layer,
        payload_label: size_tag.to_string(),
        prod_ops: prod_metrics.throughput_ops_sec,
        cons_ops: cons_metrics.throughput_ops_sec,
    }
}

fn run_raw(
    size_tag: &str,
    prod_role: &str,
    cons_role: &str,
    events: u64,
    buffer: usize,
) -> LayerResult {
    run_layer_pair(
        "raw_ring",
        "ml_raw",
        size_tag,
        prod_role,
        cons_role,
        vec![
            ("BENCH_EVENTS", events.to_string()),
            ("BENCH_BUFFER", buffer.to_string()),
        ],
    )
}

fn run_framed(size_tag: &str, payload_size: usize, events: u64, buffer: usize) -> LayerResult {
    run_layer_pair(
        "framed",
        "ml_frm",
        size_tag,
        "framed_prod",
        "framed_cons",
        vec![
            ("BENCH_EVENTS", events.to_string()),
            ("BENCH_BUFFER", buffer.to_string()),
            ("BENCH_PAYLOAD_SIZE", payload_size.to_string()),
        ],
    )
}

fn run_framed_batch(
    size_tag: &str,
    payload_size: usize,
    events: u64,
    buffer: usize,
) -> LayerResult {
    run_layer_pair(
        "framed_batch",
        "ml_frmb",
        size_tag,
        "framed_prod",
        "framed_batch_cons",
        vec![
            ("BENCH_EVENTS", events.to_string()),
            ("BENCH_BUFFER", buffer.to_string()),
            ("BENCH_PAYLOAD_SIZE", payload_size.to_string()),
        ],
    )
}

fn run_rightsized_framed(
    size_tag: &str,
    payload_size: usize,
    events: u64,
    buffer: usize,
    prod_role: &str,
    cons_role: &str,
) -> LayerResult {
    run_layer_pair(
        "framed_right",
        "ml_rsf",
        size_tag,
        prod_role,
        cons_role,
        vec![
            ("BENCH_EVENTS", events.to_string()),
            ("BENCH_BUFFER", buffer.to_string()),
            ("BENCH_PAYLOAD_SIZE", payload_size.to_string()),
        ],
    )
}

// ============================================================
// Display
// ============================================================

fn print_layer_comparison(results: &[LayerResult]) {
    #[derive(Tabled)]
    struct Row {
        #[tabled(rename = "Payload")]
        payload: String,
        #[tabled(rename = "Layer")]
        layer: String,
        #[tabled(rename = "Producer\n(ops/s)")]
        prod: String,
        #[tabled(rename = "Consumer\n(ops/s)")]
        cons: String,
        #[tabled(rename = "% of Raw\nRing")]
        pct: String,
    }

    // Find raw ring baseline per payload size
    let raw_baseline = |tag: &str| -> f64 {
        results
            .iter()
            .find(|r| r.layer == "raw_ring" && r.payload_label == tag)
            .map(|r| r.cons_ops)
            .unwrap_or(1.0)
    };

    let rows: Vec<Row> = results
        .iter()
        .map(|r| {
            let baseline = raw_baseline(&r.payload_label);
            let pct = r.cons_ops / baseline * 100.0;
            Row {
                payload: r.payload_label.clone(),
                layer: r.layer.to_string(),
                prod: format_throughput(r.prod_ops),
                cons: format_throughput(r.cons_ops),
                pct: if r.layer == "raw_ring" {
                    "100%".into()
                } else {
                    format!("{:.0}%", pct)
                },
            }
        })
        .collect();

    println!("\n{}", Table::new(rows).with(Style::modern()));
}

// ============================================================
// main
// ============================================================

const CHILD_ROLES: &[harness::ChildRole] = &[
    harness::ChildRole::new("raw_prod_1k", raw_prod_1k),
    harness::ChildRole::new("raw_cons_1k", raw_cons_1k),
    harness::ChildRole::new("raw_prod_4k", raw_prod_4k),
    harness::ChildRole::new("raw_cons_4k", raw_cons_4k),
    harness::ChildRole::new("raw_prod_16k", raw_prod_16k),
    harness::ChildRole::new("raw_cons_16k", raw_cons_16k),
    harness::ChildRole::new("raw_prod_64k", raw_prod_64k),
    harness::ChildRole::new("raw_cons_64k", raw_cons_64k),
    harness::ChildRole::new("framed_prod", framed_producer),
    harness::ChildRole::new("framed_cons", framed_consumer),
    harness::ChildRole::new("framed_batch_cons", framed_batch_consumer),
    harness::ChildRole::new("rs_framed_prod_2k", rs_framed_prod_2k),
    harness::ChildRole::new("rs_framed_cons_2k", rs_framed_cons_2k),
    harness::ChildRole::new("rs_framed_prod_8k", rs_framed_prod_8k),
    harness::ChildRole::new("rs_framed_cons_8k", rs_framed_cons_8k),
    harness::ChildRole::new("rs_framed_prod_32k", rs_framed_prod_32k),
    harness::ChildRole::new("rs_framed_cons_32k", rs_framed_cons_32k),
    harness::ChildRole::new("rkyv_nf_prod_2k", rkyv_nf_prod_2k),
    harness::ChildRole::new("rkyv_nf_cons_2k", rkyv_nf_cons_2k),
    harness::ChildRole::new("rkyv_nf_prod_8k", rkyv_nf_prod_8k),
    harness::ChildRole::new("rkyv_nf_cons_8k", rkyv_nf_cons_8k),
    harness::ChildRole::new("rkyv_nf_prod_32k", rkyv_nf_prod_32k),
    harness::ChildRole::new("rkyv_nf_cons_32k", rkyv_nf_cons_32k),
    harness::ChildRole::new("rkyv_nf_prod_128k", rkyv_nf_prod_128k),
    harness::ChildRole::new("rkyv_nf_cons_128k", rkyv_nf_cons_128k),
];

struct MyelonLayersBench;

impl harness::BenchHarness for MyelonLayersBench {
    fn bench_name(&self) -> &'static str {
        "myelon_layers"
    }

    fn child_roles(&self) -> &'static [harness::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, _args: &[String]) -> harness::BenchRunResult {
        let mut bench_log = perf_bench::bench_log::BenchLog::default_capacity("myelon_layers");
        bench_log.event("start");

        println!("=== Myelon Layer Overhead Sweep ===");
        println!("Compares raw disruptor ring vs FramedTransport vs TypedTransport+codec");
        println!("Full payload fill + consumer read. SHM backend.");
        println!();

        let mut all_results = Vec::new();

        struct SizeConfig {
            tag: &'static str,
            payload: usize,
            events: u64,
            buffer: usize,
            raw_prod: &'static str,
            raw_cons: &'static str,
            batch: usize,
        }

        let sizes = [
            SizeConfig {
                tag: "1KB",
                payload: 1024,
                events: 200_000,
                buffer: 16_384,
                raw_prod: "raw_prod_1k",
                raw_cons: "raw_cons_1k",
                batch: 2,
            },
            SizeConfig {
                tag: "4KB",
                payload: 4096,
                events: 100_000,
                buffer: 16_384,
                raw_prod: "raw_prod_4k",
                raw_cons: "raw_cons_4k",
                batch: 8,
            },
            SizeConfig {
                tag: "16KB",
                payload: 16384,
                events: 50_000,
                buffer: 16_384,
                raw_prod: "raw_prod_16k",
                raw_cons: "raw_cons_16k",
                batch: 28,
            },
            SizeConfig {
                tag: "64KB",
                payload: 65536,
                events: 20_000,
                buffer: 16_384,
                raw_prod: "raw_prod_64k",
                raw_cons: "raw_cons_64k",
                batch: 110,
            },
        ];

        for s in &sizes {
            bench_log.event(&format!("size_start: {}", s.tag));
            println!("--- {} ---", s.tag);

            let raw = run_raw(s.tag, s.raw_prod, s.raw_cons, s.events, s.buffer);
            println!(
                "  raw_ring:  prod={} cons={}",
                format_throughput(raw.prod_ops),
                format_throughput(raw.cons_ops)
            );
            all_results.push(raw);

            let framed = run_framed(s.tag, s.payload, s.events, s.buffer);
            println!(
                "  framed:       prod={} cons={}",
                format_throughput(framed.prod_ops),
                format_throughput(framed.cons_ops)
            );
            all_results.push(framed);

            let framed_batch = run_framed_batch(s.tag, s.payload, s.events, s.buffer);
            println!(
                "  framed_batch: prod={} cons={}",
                format_throughput(framed_batch.prod_ops),
                format_throughput(framed_batch.cons_ops)
            );
            all_results.push(framed_batch);

            let (rs_prod, rs_cons, rs_buf) = match s.tag {
                "1KB" => ("rs_framed_prod_2k", "rs_framed_cons_2k", 65_536usize),
                "4KB" => ("rs_framed_prod_8k", "rs_framed_cons_8k", 32_768),
                "16KB" => ("rs_framed_prod_32k", "rs_framed_cons_32k", 16_384),
                _ => ("rs_framed_prod_2k", "rs_framed_cons_2k", 65_536),
            };
            if s.tag != "64KB" {
                let rs_framed =
                    run_rightsized_framed(s.tag, s.payload, s.events, rs_buf, rs_prod, rs_cons);
                println!(
                    "  framed_right: prod={} cons={}",
                    format_throughput(rs_framed.prod_ops),
                    format_throughput(rs_framed.cons_ops)
                );
                all_results.push(rs_framed);
            }

            let (nf_prod, nf_cons) = match s.tag {
                "1KB" => ("rkyv_nf_prod_2k", "rkyv_nf_cons_2k"),
                "4KB" => ("rkyv_nf_prod_8k", "rkyv_nf_cons_8k"),
                "16KB" => ("rkyv_nf_prod_32k", "rkyv_nf_cons_32k"),
                "64KB" => ("rkyv_nf_prod_128k", "rkyv_nf_cons_128k"),
                _ => ("rkyv_nf_prod_8k", "rkyv_nf_cons_8k"),
            };
            let rkyv_nf = run_layer_pair(
                "rkyv_nofrag",
                "ml_rkyv_nf",
                s.tag,
                nf_prod,
                nf_cons,
                vec![
                    ("BENCH_EVENTS", s.events.to_string()),
                    ("BENCH_BUFFER", s.buffer.to_string()),
                    ("BENCH_BATCH_SIZE", s.batch.to_string()),
                ],
            );
            println!(
                "  rkyv_nofrag:  prod={} cons={}",
                format_throughput(rkyv_nf.prod_ops),
                format_throughput(rkyv_nf.cons_ops)
            );
            all_results.push(rkyv_nf);

            bench_log.event(&format!("size_done: {}", s.tag));
        }

        print_layer_comparison(&all_results);
        bench_log.event_val("total_scenarios", all_results.len() as u64);
        Ok(())
    }
}

perf_bench::myelon_bench_main!(MyelonLayersBench);
