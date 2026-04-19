//! Myelon Layer Overhead Sweep — raw ring vs FramedTransport vs typed/ring zero-copy paths.
//!
//! Answers: "How much overhead does each myelon abstraction layer add vs raw disruptor?"
//!
//! At each payload size, measures:
//!   raw_ring:     BenchEvent<SIZE> directly on disruptor ring
//!   framed:       FramedTransport publish(&[u8]) over SHM ring (64KB frame)
//!   framed_batch: FramedTransport batch recv (process_available_messages)
//!   framed_right: FramedTransport with right-sized frame (no bandwidth waste)
//!   rkyv_nofrag:  Zero-copy rkyv with right-sized ring slots (no framing)
//!   typed_zero_copy: TypedTransport + ZeroCopyCodec batch receive over framed transport (rkyv)
//!   typed_zero_copy_flatbuf: same typed zero-copy path using FlatBuffers root access
//!
//! Run: cargo bench -p perf-bench --bench myelon_layers

use crate::codec_payloads::{
    access_raw, access_rkyv, checksum_archived_rkyv, checksum_flatbuf_root, encode_rkyv,
    make_payloads, FlatbufBatch, RkyvBatch,
};
use crate::coordination::BenchmarkCoordination;
use crate::events::{format_throughput, nanos_now, BenchEvent};
use crate::harness::{
    self, launch_shm_group, read_env_u64, read_env_usize, segment_from_env, ConsumerOutput,
    IpcBenchmark, MultiConsumerSpawn, ProducerOutput, ScenarioChildren,
};
use crate::report_v2::ReportBundleCompat;
use crate::reporting;
use crate::scenario_v2::sweeps::{self as sweep_specs, BasicSweepSelection};
use disruptor_mp::{
    build_shared_single_producer, CoordinationMode, SharedDisruptorBuilder, SharedMemoryConfig,
};
use myelon::transport::{
    FixedFrame, FrameMeta, FramedTransportConsumer, FramedTransportFrame, FramedTransportProducer,
    MyelonWaitStrategy, ReassemblyBuffer,
};
use myelon::typed_transport::{TypedConsumer, TypedProducer};
use std::hint::black_box;
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

const FRAME_DATA_BYTES: usize = 64 * 1024 - 12;
type Frame = FixedFrame<FRAME_DATA_BYTES>;

const ZC_FRAME_HEADER_BYTES: usize = std::mem::size_of::<ZeroCopyFrameHeader>();

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct ZeroCopyFrameHeader {
    len: u32,
    kind: u8,
    flags: u8,
    msg_id: u32,
    _aligned_header: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct ZeroCopyFrame<const DATA_BYTES: usize> {
    len: u32,
    kind: u8,
    flags: u8,
    msg_id: u32,
    _aligned_header: u64,
    data: [u8; DATA_BYTES],
}

impl<const DATA_BYTES: usize> Default for ZeroCopyFrame<DATA_BYTES> {
    fn default() -> Self {
        Self {
            len: 0,
            kind: 0,
            flags: 0,
            msg_id: 0,
            _aligned_header: 0,
            data: [0u8; DATA_BYTES],
        }
    }
}

impl<const DATA_BYTES: usize> FramedTransportFrame for ZeroCopyFrame<DATA_BYTES> {
    fn payload_capacity() -> usize {
        DATA_BYTES
    }

    fn frame_meta(&self) -> FrameMeta<'_> {
        FrameMeta {
            len: self.len as usize,
            kind: self.kind,
            flags: self.flags,
            msg_id: self.msg_id,
            timestamp_ns: None,
            data: &self.data[..self.len as usize],
        }
    }

    fn write_frame(&mut self, payload: &[u8], kind: u8, msg_id: u32, flags: u8) {
        assert!(
            payload.len() <= DATA_BYTES,
            "payload len {} exceeds zero-copy frame capacity {}",
            payload.len(),
            DATA_BYTES
        );
        self.len = payload.len() as u32;
        self.kind = kind;
        self.flags = flags;
        self.msg_id = msg_id;
        self.data[..payload.len()].copy_from_slice(payload);
    }
}

type ZcFrame = ZeroCopyFrame<{ 64 * 1024 - ZC_FRAME_HEADER_BYTES }>;

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
            let num_consumers = read_env_usize("BENCH_CONSUMERS", 1);
            let payloads = make_payloads(batch_size);

            let mut producer = build_shared_single_producer::<$slot>(&segment, buffer)
                .enable_discovery(num_consumers)
                .with_coordination(CoordinationMode::Immediate)
                .build_producer(|| <$slot>::default())?;
            let coord = BenchmarkCoordination::create(&segment)?;
            if !coord.wait_for_consumers(num_consumers, Duration::from_secs(30)) {
                return Err(format!("timeout waiting for {num_consumers} consumers").into());
            }
            warm_discovery_scans(
                || producer.min_gating_sequence(),
                discovery_scan_rounds(num_consumers),
            );

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
            coord.wait_for_consumers_done(num_consumers, Duration::from_secs(60));
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
            let deadline = harness::spin_deadline();
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
                    harness::check_deadline(deadline, concat!(stringify!($cons_fn), " measured"));
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
            let num_consumers = read_env_usize("BENCH_CONSUMERS", 1);
            let mut producer = build_shared_single_producer::<$ev>(&segment, buffer)
                .enable_discovery(num_consumers)
                .with_coordination(CoordinationMode::Immediate)
                .build_producer(|| <$ev>::default())?;
            let coord = BenchmarkCoordination::create(&segment)?;
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
    let num_consumers = read_env_usize("BENCH_CONSUMERS", 1);
    let mut producer =
        FramedTransportProducer::<Frame>::create_with_consumers(&segment, buffer, num_consumers)?;
    let coord = BenchmarkCoordination::create(&segment)?;
    if !coord.wait_for_consumers(num_consumers, Duration::from_secs(30)) {
        return Err(format!("timeout waiting for {num_consumers} consumers").into());
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
    coord.wait_for_consumers_done(num_consumers, Duration::from_secs(60));
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
            let num_consumers = read_env_usize("BENCH_CONSUMERS", 1);
            let mut producer = FramedTransportProducer::<$frame>::create_with_consumers(
                &segment,
                buffer,
                num_consumers,
            )?;
            let coord = BenchmarkCoordination::create(&segment)?;
            if !coord.wait_for_consumers(num_consumers, Duration::from_secs(30)) {
                return Err(format!("timeout waiting for {num_consumers} consumers").into());
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
            coord.wait_for_consumers_done(num_consumers, Duration::from_secs(60));
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
            let deadline = harness::spin_deadline();
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
                    harness::check_deadline(deadline, concat!(stringify!($cons_fn), " measured"));
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
    let deadline = harness::spin_deadline();
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
            harness::check_deadline(deadline, "framed_batch_consumer measured");
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
// Typed rkyv zero-copy producer/consumer
// ============================================================

fn typed_zc_producer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = segment_from_env("BENCHMARK_SEGMENT_NAME");
    let buffer = read_env_usize("BENCH_BUFFER", 1024);
    let events = read_env_u64("BENCH_EVENTS", 50_000);
    let batch_size = read_env_usize("BENCH_BATCH_SIZE", 8);
    let payload_size = read_env_usize("BENCH_PAYLOAD_SIZE", 1024);
    let codec = std::env::var("BENCH_CODEC").unwrap_or_else(|_| "rkyv".to_string());
    let num_consumers = read_env_usize("BENCH_CONSUMERS", 1);
    let payloads = make_payloads(batch_size);

    let mut producer =
        TypedProducer::<ZcFrame>::create_with_consumers(&segment, buffer, num_consumers)?;
    let coord = BenchmarkCoordination::create(&segment)?;
    if !coord.wait_for_consumers(num_consumers, Duration::from_secs(30)) {
        return Err(format!("timeout waiting for {num_consumers} consumers").into());
    }
    producer.discover_consumers(Duration::from_secs(3));

    let start = Instant::now();
    match codec.as_str() {
        "rkyv" => {
            let payload = RkyvBatch(payloads);
            for i in 0..events {
                producer.publish(&payload, (i % 256) as u8)?;
            }
        }
        "flatbuf" => {
            let payload = FlatbufBatch(payloads);
            for i in 0..events {
                producer.publish(&payload, (i % 256) as u8)?;
            }
        }
        other => return Err(format!("unsupported BENCH_CODEC '{other}'").into()),
    }
    let elapsed = start.elapsed();
    let output = ProducerOutput::from_elapsed(events, elapsed, payload_size);
    println!("{}", serde_json::to_string(&output)?);
    coord.signal_producer_done(events as i64);
    coord.wait_for_consumers_done(num_consumers, Duration::from_secs(60));
    Ok(())
}

fn typed_zc_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = segment_from_env("BENCHMARK_SEGMENT_NAME");
    let consumer_id = read_env_usize("BENCH_CONSUMER_ID", 0);
    let buffer = read_env_usize("BENCH_BUFFER", 1024);
    let events = read_env_u64("BENCH_EVENTS", 50_000);
    let payload_size = read_env_usize("BENCH_PAYLOAD_SIZE", 1024);
    let codec = std::env::var("BENCH_CODEC").unwrap_or_else(|_| "rkyv".to_string());
    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
    let mut consumer =
        TypedConsumer::<ZcFrame>::attach(&segment, buffer, MyelonWaitStrategy::BusySpin)?;
    let mut reassembly = ReassemblyBuffer::new(256 * 1024);
    coord.signal_consumer_ready();
    let deadline = harness::spin_deadline();
    let mut consumed = 0u64;
    let mut start: Option<Instant> = None;
    let mut checksum = 0u64;

    while consumed < events {
        let delivered = match codec.as_str() {
            "rkyv" => consumer.process_available_zero_copy::<RkyvBatch, _>(
                &mut reassembly,
                |_kind, archived| {
                    if start.is_none() {
                        start = Some(Instant::now());
                    }
                    let payload_sum = checksum_archived_rkyv(archived);
                    black_box(payload_sum);
                    checksum = checksum.wrapping_add(payload_sum);
                    consumed += 1;
                },
            ),
            "flatbuf" => consumer.process_available_zero_copy::<FlatbufBatch, _>(
                &mut reassembly,
                |_kind, archived| {
                    if start.is_none() {
                        start = Some(Instant::now());
                    }
                    let payload_sum = checksum_flatbuf_root(archived);
                    black_box(payload_sum);
                    checksum = checksum.wrapping_add(payload_sum);
                    consumed += 1;
                },
            ),
            other => return Err(format!("unsupported BENCH_CODEC '{other}'").into()),
        };
        if consumed < events {
            harness::check_deadline(deadline, "typed_zc_consumer measured");
            if delivered == 0 {
                std::hint::spin_loop();
            }
        }
    }

    let elapsed = start
        .expect("consumer never received a zero-copy payload")
        .elapsed();
    let output =
        ConsumerOutput::from_elapsed(consumer_id, consumed, elapsed, payload_size, checksum);
    println!("{}", serde_json::to_string(&output)?);
    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

// ============================================================
// Orchestrator
// ============================================================

struct LayerScenario {
    layer: &'static str,
    codec: Option<&'static str>,
    segment_prefix: &'static str,
    size_tag: &'static str,
    payload_size: usize,
    events: u64,
    buffer: usize,
    consumers: usize,
    prod_role: &'static str,
    cons_role: &'static str,
    envs: Vec<(&'static str, String)>,
}

impl IpcBenchmark for LayerScenario {
    fn bench_name(&self) -> &str {
        "myelon_layers"
    }

    fn scenario_name(&self) -> String {
        format!("{}_{}_1p{}c", self.layer, self.size_tag, self.consumers)
    }

    fn backend(&self) -> &str {
        "shm"
    }

    fn layer(&self) -> &str {
        self.layer
    }

    fn transport_metadata(&self) -> reporting::BenchTransportSpec {
        match self.layer {
            "raw_ring" => reporting::BenchTransportSpec::benchmark_shm(self.consumers)
                .with_zero_copy(false)
                .with_framing("none"),
            "framed" => reporting::BenchTransportSpec::benchmark_shm(self.consumers)
                .with_zero_copy(false)
                .with_framing("fixed_64k"),
            "framed_right" => reporting::BenchTransportSpec::benchmark_shm(self.consumers)
                .with_zero_copy(false)
                .with_framing("right_sized"),
            "typed" => reporting::BenchTransportSpec::benchmark_shm(self.consumers)
                .with_zero_copy(false)
                .with_framing("fixed_64k"),
            "typed_zero_copy" | "typed_zero_copy_flatbuf" => {
                reporting::BenchTransportSpec::benchmark_shm(self.consumers)
                    .with_zero_copy(true)
                    .with_framing("fixed_64k")
            }
            _ => reporting::BenchTransportSpec::benchmark_shm(self.consumers),
        }
    }

    fn codec(&self) -> Option<&str> {
        self.codec
    }

    fn message_size_bytes(&self) -> usize {
        self.payload_size
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
        Duration::from_secs(300)
    }

    fn print_summary_with_metrics(
        &self,
        _producer: &harness::ProducerOutput,
        _consumers: &[harness::ConsumerOutput],
        _latency: Option<&crate::latency::LatencyStats>,
    ) {
    }

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, harness::BenchError> {
        let mut base_envs = self.envs.clone();
        base_envs.push(("BENCH_CONSUMERS", self.consumers.to_string()));
        launch_shm_group(
            exe,
            &format!("{}_{}", self.segment_prefix, self.size_tag),
            "BENCHMARK_SEGMENT_NAME",
            MultiConsumerSpawn {
                producer_role: self.prod_role,
                consumer_role: self.cons_role,
                consumers: self.consumers,
                consumer_id_env: "BENCH_CONSUMER_ID",
                base_envs,
            },
        )
    }
}

impl LayerScenario {
    fn run(&self) -> Result<reporting::BenchResult, harness::BenchError> {
        self.run_benchmark()
    }
}

fn raw_scenario(
    size_tag: &'static str,
    payload_size: usize,
    prod_role: &'static str,
    cons_role: &'static str,
    events: u64,
    buffer: usize,
) -> LayerScenario {
    LayerScenario {
        layer: "raw_ring",
        codec: None,
        segment_prefix: "ml_raw",
        size_tag,
        payload_size,
        events,
        buffer,
        consumers: 1,
        prod_role,
        cons_role,
        envs: vec![
            ("BENCH_EVENTS", events.to_string()),
            ("BENCH_BUFFER", buffer.to_string()),
        ],
    }
}

fn framed_scenario(
    layer: &'static str,
    segment_prefix: &'static str,
    size_tag: &'static str,
    payload_size: usize,
    events: u64,
    buffer: usize,
    prod_role: &'static str,
    cons_role: &'static str,
) -> LayerScenario {
    LayerScenario {
        layer,
        codec: None,
        segment_prefix,
        size_tag,
        payload_size,
        events,
        buffer,
        consumers: 1,
        prod_role,
        cons_role,
        envs: vec![
            ("BENCH_EVENTS", events.to_string()),
            ("BENCH_BUFFER", buffer.to_string()),
            ("BENCH_PAYLOAD_SIZE", payload_size.to_string()),
        ],
    }
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
    harness::ChildRole::new("typed_zc_prod", typed_zc_producer),
    harness::ChildRole::new("typed_zc_cons", typed_zc_consumer),
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

pub struct MyelonLayersBench;

impl harness::BenchHarness for MyelonLayersBench {
    fn bench_name(&self) -> &'static str {
        "myelon_layers"
    }

    fn child_roles(&self) -> &'static [harness::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> harness::BenchRunResult {
        let mut bench_log = crate::bench_log::BenchLog::default_capacity("myelon_layers");
        bench_log.event("start");
        let selection = BasicSweepSelection::parse(args)?;

        if !selection.output_args.json_mode {
            println!("=== Myelon Layer Overhead Sweep ===");
            println!("Compares raw disruptor ring vs FramedTransport vs TypedTransport+codec");
            println!("Full payload fill + consumer read. SHM backend.");
            println!();
        }

        let mut report = reporting::BenchReport::new();

        macro_rules! run_layer {
            ($scenario:expr, $label:expr, $consumers:expr) => {{
                let bench_result = ($scenario).run()?;
                if !selection.output_args.json_mode {
                    println!(
                        "  {:<14} 1p{}c prod={} cons={}",
                        $label,
                        $consumers,
                        format_throughput(bench_result.results.producer_throughput_ops_sec),
                        format_throughput(bench_result.results.consumer_throughput_ops_sec)
                    );
                }
                report.add(bench_result);
            }};
        }

        for spec in sweep_specs::payload_sweep_specs() {
            if !selection.matches_size(spec.tag) {
                continue;
            }
            bench_log.event(&format!("size_start: {}", spec.tag));
            if !selection.output_args.json_mode {
                println!("--- {} ---", spec.tag);
            }

            for consumers in sweep_specs::TARGET_CONSUMERS {
                if !selection.matches_consumers(consumers) {
                    continue;
                }

                for variant in sweep_specs::myelon_layer_variant_specs(spec.tag) {
                    if !selection.matches_layer(variant.layer) {
                        continue;
                    }

                    let buffer = variant.buffer_override.unwrap_or(spec.buffer_depth);
                    let mut scenario = match variant.kind {
                        sweep_specs::MyelonLayerVariantKind::Raw => raw_scenario(
                            spec.tag,
                            spec.payload_bytes,
                            variant.prod_role,
                            variant.cons_role,
                            spec.events,
                            buffer,
                        ),
                        sweep_specs::MyelonLayerVariantKind::Framed => framed_scenario(
                            variant.layer,
                            variant.segment_prefix,
                            spec.tag,
                            spec.payload_bytes,
                            spec.events,
                            buffer,
                            variant.prod_role,
                            variant.cons_role,
                        ),
                        sweep_specs::MyelonLayerVariantKind::LayerScenario {
                            codec_env,
                            include_payload_size,
                        } => {
                            let mut envs = vec![
                                ("BENCH_EVENTS", spec.events.to_string()),
                                ("BENCH_BUFFER", buffer.to_string()),
                                ("BENCH_BATCH_SIZE", spec.batch_size.to_string()),
                            ];
                            if let Some(codec) = codec_env {
                                envs.push(("BENCH_CODEC", codec.to_string()));
                            }
                            if include_payload_size {
                                envs.push(("BENCH_PAYLOAD_SIZE", spec.payload_bytes.to_string()));
                            }
                            LayerScenario {
                                layer: variant.layer,
                                codec: variant.codec,
                                segment_prefix: variant.segment_prefix,
                                size_tag: spec.tag,
                                payload_size: spec.payload_bytes,
                                events: spec.events,
                                buffer,
                                consumers,
                                prod_role: variant.prod_role,
                                cons_role: variant.cons_role,
                                envs,
                            }
                        }
                    };
                    scenario.consumers = consumers;
                    run_layer!(scenario, variant.summary_label, consumers);
                }
            }

            bench_log.event(&format!("size_done: {}", spec.tag));
        }

        if !selection.output_args.json_mode {
            report.to_report_v2().print_layer_comparison();
        }
        reporting::emit_report(&report, &selection.output_args, None, None, None);
        bench_log.event_val("total_scenarios", report.results.len() as u64);
        Ok(())
    }
}
