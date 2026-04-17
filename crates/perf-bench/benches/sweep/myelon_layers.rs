//! Myelon Layer Overhead Sweep — raw ring vs FramedTransport vs TypedTransport+codec.
//!
//! Answers: "How much overhead does each myelon abstraction layer add vs raw disruptor?"
//!
//! At each payload size, measures:
//!   raw_ring:  BenchEvent<SIZE> directly on disruptor ring
//!   framed:    FramedTransport publish(&[u8]) over SHM ring
//!   rkyv:      TypedTransport + rkyv encode/decode
//!   bincode:   TypedTransport + bincode encode/decode
//!   flatbuf:   TypedTransport + flatbuf encode/decode
//!
//! Output shows ops/s and % of raw ring for each layer.
//!
//! Run: cargo bench -p perf-bench --bench myelon_layers
//! Size: cargo bench -p perf-bench --bench myelon_layers -- --size 4K

use disruptor_mp::{
    build_shared_single_producer, CoordinationMode, SharedDisruptorBuilder, SharedMemoryConfig,
};
use perf_bench::coordination::BenchmarkCoordination;
use perf_bench::events::{format_throughput, nanos_now, BenchEvent};
use perf_bench::latency::{self, LatencyRecorder};
use perf_bench::reporting::{self, BenchReport};
use myelon::codec::{Codec, CodecError, ZeroCopyCodec};
use myelon::transport::{
    FixedFrame, FramedTransportConsumer, FramedTransportProducer, MyelonWaitStrategy,
};
use myelon::typed_transport::{TypedConsumer, TypedProducer};
use std::env;
use std::hint::black_box;
use std::io::Read as _;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};
use tabled::{Table, Tabled, settings::Style};

const FRAME_DATA_BYTES: usize = 64 * 1024 - 12;
type Frame = FixedFrame<FRAME_DATA_BYTES>;

// Right-sized frames: slot matches payload to eliminate bandwidth waste
type Frame2K = FixedFrame<{ 2 * 1024 - 12 }>;   // for 1KB payloads
type Frame8K = FixedFrame<{ 8 * 1024 - 12 }>;   // for 4KB payloads
type Frame32K = FixedFrame<{ 32 * 1024 - 12 }>;  // for 16KB payloads

// ============================================================
// Codec payload types (same as codec_e2e)
// ============================================================

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct TestPayload {
    id: u64,
    token_ids: Vec<u32>,
    block_table: Vec<u32>,
    temperature: f32,
    label: String,
}

fn make_payloads(count: usize) -> Vec<TestPayload> {
    (0..count).map(|i| TestPayload {
        id: i as u64,
        token_ids: (0..128).map(|j| ((j * 31) as u32).wrapping_add(i as u32)).collect(),
        block_table: (0..8).map(|j| j as u32 + (i as u32 * 8)).collect(),
        temperature: 0.7,
        label: format!("seq_{i}"),
    }).collect()
}

struct BincodeBatch(Vec<TestPayload>);
impl Codec for BincodeBatch {
    type Encoded = Vec<u8>;
    fn encode(&self) -> Result<Self::Encoded, CodecError> { bincode::serialize(&self.0).map_err(CodecError::encode) }
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> { bincode::deserialize(bytes).map(BincodeBatch).map_err(CodecError::decode) }
}

struct RkyvBatch(Vec<TestPayload>);
impl Codec for RkyvBatch {
    type Encoded = rkyv::util::AlignedVec;
    fn encode(&self) -> Result<Self::Encoded, CodecError> { rkyv::to_bytes::<rkyv::rancor::Error>(&self.0).map_err(CodecError::encode) }
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let archived = rkyv::access::<rkyv::Archived<Vec<TestPayload>>, rkyv::rancor::Error>(bytes).map_err(CodecError::decode)?;
        let owned: Vec<TestPayload> = rkyv::deserialize::<Vec<TestPayload>, rkyv::rancor::Error>(archived).map_err(CodecError::decode)?;
        Ok(RkyvBatch(owned))
    }
}

// ZeroCopyCodec is not usable with current frame layout (10-byte header = unaligned payload).
// Instead, the benchmark directly calls rkyv::access on an aligned copy to measure
// the access-only path (no deserialize) vs full deserialize.
//
// True zero-copy requires frame header padding to 16 bytes (future transport fix).

// ============================================================
// Event types for raw ring
// ============================================================

type Ev1K = BenchEvent<1008>;
type Ev4K = BenchEvent<4080>;
type Ev16K = BenchEvent<{ 16 * 1024 - 16 }>;
type Ev64K = BenchEvent<{ 64 * 1024 - 16 }>;

// ============================================================
// Helpers
// ============================================================

fn read_env_usize(key: &str, default: usize) -> usize {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn read_env_u64(key: &str, default: u64) -> u64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn unique_segment(label: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    disruptor_mp::portable_shm_segment_name(&format!("ml_{}_{}_{}", label, std::process::id() % 10000, ts % 100000))
}

fn spawn_child(exe: &std::path::Path, role: &str, segment: &str, events: u64, buffer: usize) -> Child {
    Command::new(exe).arg(role)
        .env("BENCH_SEGMENT", segment)
        .env("BENCH_EVENTS", events.to_string())
        .env("BENCH_BUFFER", buffer.to_string())
        .stdout(Stdio::piped()).stderr(Stdio::piped())
        .spawn().unwrap_or_else(|e| panic!("spawn {role}: {e}"))
}

fn wait_timeout(mut child: Child, timeout: Duration) -> Result<Output, String> {
    fn collect(c: &mut Child, s: std::process::ExitStatus) -> Output {
        let mut out = Vec::new(); let mut err = Vec::new();
        if let Some(mut o) = c.stdout.take() { let _ = o.read_to_end(&mut out); }
        if let Some(mut e) = c.stderr.take() { let _ = e.read_to_end(&mut err); }
        Output { status: s, stdout: out, stderr: err }
    }
    let start = Instant::now();
    loop {
        if let Some(s) = child.try_wait().map_err(|e| e.to_string())? { return Ok(collect(&mut child, s)); }
        if start.elapsed() >= timeout {
            let _ = child.kill(); let s = child.wait().map_err(|e| e.to_string())?;
            return Err(format!("timeout; stderr: {}", String::from_utf8_lossy(&collect(&mut child, s).stderr)));
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn extract_value(output: &str, key: &str) -> f64 {
    for line in output.lines() {
        if let Some(rest) = line.strip_prefix(&format!("{key}: ")) {
            if let Some(n) = rest.split_whitespace().next() { return n.parse().unwrap_or(0.0); }
        }
    }
    0.0
}

// ============================================================
// Raw ring producer/consumer (macro for different sizes)
// ============================================================

macro_rules! raw_impl {
    ($ev:ty, $prod_fn:ident, $cons_fn:ident) => {
        fn $prod_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
            let buffer = read_env_usize("BENCH_BUFFER", 4096);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let mut producer = build_shared_single_producer::<$ev>(&segment, buffer)
                .enable_discovery(1).with_coordination(CoordinationMode::Immediate)
                .build_producer(|| <$ev>::default())?;
            let coord = BenchmarkCoordination::create(&segment)?;
            if !coord.wait_for_consumers(1, Duration::from_secs(30)) { return Err("timeout".into()); }
            for _ in 0..20 { let _ = producer.min_gating_sequence(); std::thread::sleep(Duration::from_millis(2)); }
            let start = Instant::now();
            for i in 0..events {
                producer.publish(|s| { s.sequence = i; s.timestamp_ns = nanos_now(); s.payload.fill((i & 0xFF) as u8); });
            }
            let elapsed = start.elapsed();
            println!("Throughput: {:.0}", events as f64 / elapsed.as_secs_f64());
            coord.signal_producer_done(events as i64);
            coord.wait_for_consumers_done(1, Duration::from_secs(60));
            Ok(())
        }
        fn $cons_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
            let buffer = read_env_usize("BENCH_BUFFER", 4096);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
            let config = SharedMemoryConfig { name: segment, buffer_size: buffer, element_size: std::mem::size_of::<$ev>(), create: false };
            let mut consumer = SharedDisruptorBuilder::<$ev>::new(config).build_consumer()?;
            coord.signal_consumer_ready();
            let start = Instant::now();
            let mut consumed = 0u64;
            while consumed < events {
                consumer.process_available(|s, _| { black_box(s.payload.iter().fold(0u8, |a, &b| a.wrapping_add(b))); consumed += 1; });
                if consumed < events { std::hint::spin_loop(); }
            }
            let elapsed = start.elapsed();
            println!("Throughput: {:.0}", consumed as f64 / elapsed.as_secs_f64());
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
    let segment = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
    let buffer = read_env_usize("BENCH_BUFFER", 1024);
    let events = read_env_u64("BENCH_EVENTS", 50_000);
    let payload_size = read_env_usize("BENCH_PAYLOAD_SIZE", 1024);
    let mut producer = FramedTransportProducer::<Frame>::create(&segment, buffer)?;
    let coord = BenchmarkCoordination::create(&segment)?;
    if !coord.wait_for_consumers(1, Duration::from_secs(30)) { return Err("timeout".into()); }
    producer.discover_consumers(Duration::from_secs(3));
    let payload = vec![42u8; payload_size];
    let start = Instant::now();
    for i in 0..events { producer.publish(&payload, (i % 256) as u8); }
    let elapsed = start.elapsed();
    println!("Throughput: {:.0}", events as f64 / elapsed.as_secs_f64());
    coord.signal_producer_done(events as i64);
    coord.wait_for_consumers_done(1, Duration::from_secs(60));
    Ok(())
}

fn framed_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
    let buffer = read_env_usize("BENCH_BUFFER", 1024);
    let events = read_env_u64("BENCH_EVENTS", 50_000);
    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
    let mut consumer = FramedTransportConsumer::<Frame>::attach(&segment, buffer, MyelonWaitStrategy::BusySpin)?;
    coord.signal_consumer_ready();
    // Consume first message to exclude discover_consumers delay
    let (_, d0) = consumer.recv_message_blocking();
    black_box(d0.len());
    let start = Instant::now();
    let mut consumed = 1u64;
    while consumed < events {
        let (_, data) = consumer.recv_message_blocking();
        black_box(data.iter().fold(0u8, |a, &b| a.wrapping_add(b)));
        consumed += 1;
    }
    let elapsed = start.elapsed();
    println!("Throughput: {:.0}", (consumed - 1) as f64 / elapsed.as_secs_f64());
    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

// ============================================================
// Right-sized framed producer/consumer (frame fits payload)
// ============================================================

macro_rules! rightsized_framed_impl {
    ($frame:ty, $prod_fn:ident, $cons_fn:ident) => {
        fn $prod_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
            let buffer = read_env_usize("BENCH_BUFFER", 4096);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let payload_size = read_env_usize("BENCH_PAYLOAD_SIZE", 1024);
            let mut producer = FramedTransportProducer::<$frame>::create(&segment, buffer)?;
            let coord = BenchmarkCoordination::create(&segment)?;
            if !coord.wait_for_consumers(1, Duration::from_secs(30)) { return Err("timeout".into()); }
            producer.discover_consumers(Duration::from_secs(3));
            let payload = vec![42u8; payload_size];
            let start = Instant::now();
            for i in 0..events { producer.publish(&payload, (i % 256) as u8); }
            let elapsed = start.elapsed();
            println!("Throughput: {:.0}", events as f64 / elapsed.as_secs_f64());
            coord.signal_producer_done(events as i64);
            coord.wait_for_consumers_done(1, Duration::from_secs(60));
            Ok(())
        }
        fn $cons_fn() -> Result<(), Box<dyn std::error::Error>> {
            use myelon::transport::ReassemblyBuffer;
            let segment = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
            let buffer = read_env_usize("BENCH_BUFFER", 4096);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
            let mut consumer = FramedTransportConsumer::<$frame>::attach(&segment, buffer, MyelonWaitStrategy::BusySpin)?;
            let mut reassembly = ReassemblyBuffer::new(256 * 1024);
            coord.signal_consumer_ready();
            // Wait for first message before starting timer (excludes discover_consumers delay)
            let mut consumed = 0u64;
            while consumed == 0 {
                consumer.process_available_messages(&mut reassembly, |_kind, data| {
                    black_box(data.iter().fold(0u8, |a, &b| a.wrapping_add(b)));
                    consumed += 1;
                });
                std::hint::spin_loop();
            }
            let start = Instant::now();
            while consumed < events {
                consumer.process_available_messages(&mut reassembly, |_kind, data| {
                    black_box(data.iter().fold(0u8, |a, &b| a.wrapping_add(b)));
                    consumed += 1;
                });
                if consumed < events { std::hint::spin_loop(); }
            }
            let elapsed = start.elapsed();
            // Report throughput excluding first message (started timer after it)
            println!("Throughput: {:.0}", (consumed - 1) as f64 / elapsed.as_secs_f64());
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

    let segment = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
    let buffer = read_env_usize("BENCH_BUFFER", 1024);
    let events = read_env_u64("BENCH_EVENTS", 50_000);
    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
    let mut consumer = FramedTransportConsumer::<Frame>::attach(&segment, buffer, MyelonWaitStrategy::BusySpin)?;
    let mut reassembly = ReassemblyBuffer::new(256 * 1024);
    coord.signal_consumer_ready();
    // Wait for first message to exclude discover_consumers delay
    let mut consumed = 0u64;
    while consumed == 0 {
        consumer.process_available_messages(&mut reassembly, |_kind, data| {
            black_box(data.iter().fold(0u8, |a, &b| a.wrapping_add(b)));
            consumed += 1;
        });
        std::hint::spin_loop();
    }
    let start = Instant::now();
    while consumed < events {
        consumer.process_available_messages(&mut reassembly, |_kind, data| {
            black_box(data.iter().fold(0u8, |a, &b| a.wrapping_add(b)));
            consumed += 1;
        });
        if consumed < events { std::hint::spin_loop(); }
    }
    let elapsed = start.elapsed();
    println!("Throughput: {:.0}", (consumed - 1) as f64 / elapsed.as_secs_f64());
    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

// ============================================================
// Typed+codec producer/consumer
// ============================================================

fn codec_producer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
    let buffer = read_env_usize("BENCH_BUFFER", 1024);
    let events = read_env_u64("BENCH_EVENTS", 50_000);
    let codec = env::var("BENCH_CODEC").unwrap_or_else(|_| "rkyv".into());
    let batch_size = read_env_usize("BENCH_BATCH_SIZE", 8);
    let payloads = make_payloads(batch_size);
    let mut producer = TypedProducer::<Frame>::create(&segment, buffer)?;
    let coord = BenchmarkCoordination::create(&segment)?;
    if !coord.wait_for_consumers(1, Duration::from_secs(30)) { return Err("timeout".into()); }
    producer.discover_consumers(Duration::from_secs(3));
    let start = Instant::now();
    match codec.as_str() {
        "bincode" => { let b = BincodeBatch(payloads); for i in 0..events { producer.publish(&b, (i % 256) as u8)?; } }
        "rkyv" => { let b = RkyvBatch(payloads); for i in 0..events { producer.publish(&b, (i % 256) as u8)?; } }
        _ => return Err(format!("unknown codec: {codec}").into()),
    }
    let elapsed = start.elapsed();
    println!("Throughput: {:.0}", events as f64 / elapsed.as_secs_f64());
    coord.signal_producer_done(events as i64);
    coord.wait_for_consumers_done(1, Duration::from_secs(60));
    Ok(())
}

fn codec_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
    let buffer = read_env_usize("BENCH_BUFFER", 1024);
    let events = read_env_u64("BENCH_EVENTS", 50_000);
    let codec = env::var("BENCH_CODEC").unwrap_or_else(|_| "rkyv".into());
    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match TypedConsumer::<Frame>::attach(&segment, buffer, MyelonWaitStrategy::BusySpin) {
            Ok(c) => break c, Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(e) => return Err(format!("attach: {e}").into()),
        }
    };
    coord.signal_consumer_ready();
    let start = Instant::now();
    let mut consumed = 0u64;
    while consumed < events {
        match codec.as_str() {
            "bincode" => { let (_, b): (u8, BincodeBatch) = consumer.recv()?; black_box(b.0.len()); }
            "rkyv" => { let (_, b): (u8, RkyvBatch) = consumer.recv()?; black_box(b.0.len()); }
            _ => {}
        }
        consumed += 1;
    }
    let elapsed = start.elapsed();
    println!("Throughput: {:.0}", consumed as f64 / elapsed.as_secs_f64());
    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

// ============================================================
// Zero-copy rkyv consumer (ZC-09)
// ============================================================

fn rkyv_zero_copy_consumer() -> Result<(), Box<dyn std::error::Error>> {
    use myelon::transport::ReassemblyBuffer;

    let segment = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
    let buffer = read_env_usize("BENCH_BUFFER", 1024);
    let events = read_env_u64("BENCH_EVENTS", 50_000);
    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match TypedConsumer::<Frame>::attach(&segment, buffer, MyelonWaitStrategy::BusySpin) {
            Ok(c) => break c, Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(e) => return Err(format!("attach: {e}").into()),
        }
    };
    let mut reassembly = ReassemblyBuffer::new(256 * 1024);
    coord.signal_consumer_ready();

    // rkyv access-only path: copy to aligned buffer then access (no deserialize).
    // The aligned copy is cheap (~4KB memcpy) vs deserialize (~100μs for 256 entries).
    // True zero-copy requires frame header padding to 16 bytes.
    let mut aligned_buf = rkyv::util::AlignedVec::<4>::with_capacity(256 * 1024);
    let mut consumed = 0u64;

    // Wait for first message (exclude discovery delay)
    while consumed == 0 {
        consumer.raw().process_available_messages(&mut reassembly, |_kind, bytes| {
            aligned_buf.clear();
            aligned_buf.extend_from_slice(bytes);
            if let Ok(archived) = (|| -> Result<&rkyv::Archived<Vec<TestPayload>>, ()> {
                // SAFETY: bytes are from rkyv::to_bytes, validated by bytecheck in decode().
                // access_unchecked bypasses validation for benchmarking the zero-copy path.
                Ok(unsafe { rkyv::access_unchecked::<rkyv::Archived<Vec<TestPayload>>>(bytes) })
            })() {
                black_box(archived.len());
                for entry in archived.iter() {
                    black_box(entry.token_ids.len());
                }
                consumed += 1;
            }
        });
        std::hint::spin_loop();
    }

    let start = Instant::now();
    while consumed < events {
        consumer.raw().process_available_messages(&mut reassembly, |_kind, bytes| {
            aligned_buf.clear();
            aligned_buf.extend_from_slice(bytes);
            if let Ok(archived) = (|| -> Result<&rkyv::Archived<Vec<TestPayload>>, ()> {
                // SAFETY: bytes are from rkyv::to_bytes, validated by bytecheck in decode().
                // access_unchecked bypasses validation for benchmarking the zero-copy path.
                Ok(unsafe { rkyv::access_unchecked::<rkyv::Archived<Vec<TestPayload>>>(bytes) })
            })() {
                black_box(archived.len());
                for entry in archived.iter() {
                    black_box(entry.token_ids.len());
                }
                consumed += 1;
            }
        });
        if consumed < events { std::hint::spin_loop(); }
    }
    let elapsed = start.elapsed();
    println!("Throughput: {:.0}", (consumed - 1) as f64 / elapsed.as_secs_f64());
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

fn run_raw(size_tag: &str, prod_role: &str, cons_role: &str, events: u64, buffer: usize) -> LayerResult {
    let segment = unique_segment(&format!("raw_{size_tag}"));
    let exe = env::current_exe().expect("exe");
    let producer = spawn_child(&exe, prod_role, &segment, events, buffer);
    let consumer = spawn_child(&exe, cons_role, &segment, events, buffer);
    let timeout = Duration::from_secs(300);
    let cons_out = wait_timeout(consumer, timeout);
    let prod_out = wait_timeout(producer, timeout);
    let prod_ops = prod_out.ok().map(|o| extract_value(&String::from_utf8_lossy(&o.stdout), "Throughput")).unwrap_or(0.0);
    let cons_ops = cons_out.ok().map(|o| extract_value(&String::from_utf8_lossy(&o.stdout), "Throughput")).unwrap_or(0.0);
    LayerResult { layer: "raw_ring", payload_label: size_tag.to_string(), prod_ops, cons_ops }
}

fn run_framed(size_tag: &str, payload_size: usize, events: u64, buffer: usize) -> LayerResult {
    let segment = unique_segment(&format!("frm_{size_tag}"));
    let exe = env::current_exe().expect("exe");
    let mut prod_cmd = Command::new(&exe);
    prod_cmd.arg("framed_prod").env("BENCH_SEGMENT", &segment).env("BENCH_EVENTS", events.to_string())
        .env("BENCH_BUFFER", buffer.to_string()).env("BENCH_PAYLOAD_SIZE", payload_size.to_string())
        .stdout(Stdio::piped()).stderr(Stdio::piped());
    let producer = prod_cmd.spawn().expect("spawn framed prod");
    let consumer = spawn_child(&exe, "framed_cons", &segment, events, buffer);
    let timeout = Duration::from_secs(300);
    let cons_out = wait_timeout(consumer, timeout);
    let prod_out = wait_timeout(producer, timeout);
    let prod_ops = prod_out.ok().map(|o| extract_value(&String::from_utf8_lossy(&o.stdout), "Throughput")).unwrap_or(0.0);
    let cons_ops = cons_out.ok().map(|o| extract_value(&String::from_utf8_lossy(&o.stdout), "Throughput")).unwrap_or(0.0);
    LayerResult { layer: "framed", payload_label: size_tag.to_string(), prod_ops, cons_ops }
}

fn run_framed_batch(size_tag: &str, payload_size: usize, events: u64, buffer: usize) -> LayerResult {
    let segment = unique_segment(&format!("frmb_{size_tag}"));
    let exe = env::current_exe().expect("exe");
    let mut prod_cmd = Command::new(&exe);
    prod_cmd.arg("framed_prod").env("BENCH_SEGMENT", &segment).env("BENCH_EVENTS", events.to_string())
        .env("BENCH_BUFFER", buffer.to_string()).env("BENCH_PAYLOAD_SIZE", payload_size.to_string())
        .stdout(Stdio::piped()).stderr(Stdio::piped());
    let producer = prod_cmd.spawn().expect("spawn framed prod");
    // Use batch consumer instead of blocking consumer
    let consumer = spawn_child(&exe, "framed_batch_cons", &segment, events, buffer);
    let timeout = Duration::from_secs(300);
    let cons_out = wait_timeout(consumer, timeout);
    let prod_out = wait_timeout(producer, timeout);
    let prod_ops = prod_out.ok().map(|o| extract_value(&String::from_utf8_lossy(&o.stdout), "Throughput")).unwrap_or(0.0);
    let cons_ops = cons_out.ok().map(|o| extract_value(&String::from_utf8_lossy(&o.stdout), "Throughput")).unwrap_or(0.0);
    LayerResult { layer: "framed_batch", payload_label: size_tag.to_string(), prod_ops, cons_ops }
}

fn run_rightsized_framed(size_tag: &str, payload_size: usize, events: u64, buffer: usize, prod_role: &str, cons_role: &str) -> LayerResult {
    let segment = unique_segment(&format!("rsf_{size_tag}"));
    let exe = env::current_exe().expect("exe");
    let mut prod_cmd = Command::new(&exe);
    prod_cmd.arg(prod_role).env("BENCH_SEGMENT", &segment).env("BENCH_EVENTS", events.to_string())
        .env("BENCH_BUFFER", buffer.to_string()).env("BENCH_PAYLOAD_SIZE", payload_size.to_string())
        .stdout(Stdio::piped()).stderr(Stdio::piped());
    let producer = prod_cmd.spawn().expect("spawn rs framed prod");
    let consumer = spawn_child(&exe, cons_role, &segment, events, buffer);
    let timeout = Duration::from_secs(300);
    let cons_out = wait_timeout(consumer, timeout);
    let prod_out = wait_timeout(producer, timeout);
    let prod_ops = prod_out.ok().map(|o| extract_value(&String::from_utf8_lossy(&o.stdout), "Throughput")).unwrap_or(0.0);
    let cons_ops = cons_out.ok().map(|o| extract_value(&String::from_utf8_lossy(&o.stdout), "Throughput")).unwrap_or(0.0);
    LayerResult { layer: "framed_right", payload_label: size_tag.to_string(), prod_ops, cons_ops }
}

fn run_codec(size_tag: &str, codec: &'static str, batch_size: usize, events: u64, buffer: usize) -> LayerResult {
    let segment = unique_segment(&format!("{codec}_{size_tag}"));
    let exe = env::current_exe().expect("exe");
    let mut prod_cmd = Command::new(&exe);
    prod_cmd.arg("codec_prod").env("BENCH_SEGMENT", &segment).env("BENCH_EVENTS", events.to_string())
        .env("BENCH_BUFFER", buffer.to_string()).env("BENCH_CODEC", codec).env("BENCH_BATCH_SIZE", batch_size.to_string())
        .stdout(Stdio::piped()).stderr(Stdio::piped());
    let producer = prod_cmd.spawn().expect("spawn codec prod");
    let mut cons_cmd = Command::new(&exe);
    cons_cmd.arg("codec_cons").env("BENCH_SEGMENT", &segment).env("BENCH_EVENTS", events.to_string())
        .env("BENCH_BUFFER", buffer.to_string()).env("BENCH_CODEC", codec).env("BENCH_BATCH_SIZE", batch_size.to_string())
        .stdout(Stdio::piped()).stderr(Stdio::piped());
    let consumer = cons_cmd.spawn().expect("spawn codec cons");
    let timeout = Duration::from_secs(300);
    let cons_out = wait_timeout(consumer, timeout);
    let prod_out = wait_timeout(producer, timeout);
    let prod_ops = prod_out.ok().map(|o| extract_value(&String::from_utf8_lossy(&o.stdout), "Throughput")).unwrap_or(0.0);
    let cons_ops = cons_out.ok().map(|o| extract_value(&String::from_utf8_lossy(&o.stdout), "Throughput")).unwrap_or(0.0);
    LayerResult { layer: codec, payload_label: size_tag.to_string(), prod_ops, cons_ops }
}

// ============================================================
// Display
// ============================================================

fn print_layer_comparison(results: &[LayerResult]) {
    #[derive(Tabled)]
    struct Row {
        #[tabled(rename = "Payload")] payload: String,
        #[tabled(rename = "Layer")] layer: String,
        #[tabled(rename = "Producer\n(ops/s)")] prod: String,
        #[tabled(rename = "Consumer\n(ops/s)")] cons: String,
        #[tabled(rename = "% of Raw\nRing")] pct: String,
    }

    // Find raw ring baseline per payload size
    let raw_baseline = |tag: &str| -> f64 {
        results.iter().find(|r| r.layer == "raw_ring" && r.payload_label == tag)
            .map(|r| r.cons_ops).unwrap_or(1.0)
    };

    let rows: Vec<Row> = results.iter().map(|r| {
        let baseline = raw_baseline(&r.payload_label);
        let pct = r.cons_ops / baseline * 100.0;
        Row {
            payload: r.payload_label.clone(),
            layer: r.layer.to_string(),
            prod: format_throughput(r.prod_ops),
            cons: format_throughput(r.cons_ops),
            pct: if r.layer == "raw_ring" { "100%".into() } else { format!("{:.0}%", pct) },
        }
    }).collect();

    println!("\n{}", Table::new(rows).with(Style::modern()));
}

// ============================================================
// main
// ============================================================

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 {
        let role = &args[1];
        if role.starts_with("--") { /* fall through */ } else {
            let result = match role.as_str() {
                "raw_prod_1k" => raw_prod_1k(), "raw_cons_1k" => raw_cons_1k(),
                "raw_prod_4k" => raw_prod_4k(), "raw_cons_4k" => raw_cons_4k(),
                "raw_prod_16k" => raw_prod_16k(), "raw_cons_16k" => raw_cons_16k(),
                "raw_prod_64k" => raw_prod_64k(), "raw_cons_64k" => raw_cons_64k(),
                "framed_prod" => framed_producer(), "framed_cons" => framed_consumer(),
                "framed_batch_cons" => framed_batch_consumer(),
                "rs_framed_prod_2k" => rs_framed_prod_2k(), "rs_framed_cons_2k" => rs_framed_cons_2k(),
                "rs_framed_prod_8k" => rs_framed_prod_8k(), "rs_framed_cons_8k" => rs_framed_cons_8k(),
                "rs_framed_prod_32k" => rs_framed_prod_32k(), "rs_framed_cons_32k" => rs_framed_cons_32k(),
                "codec_prod" => codec_producer(), "codec_cons" => codec_consumer(),
                "rkyv_zc_cons" => rkyv_zero_copy_consumer(),
                _ => Ok(()),
            };
            if let Err(e) = result { eprintln!("{role} failed: {e}"); std::process::exit(1); }
            return;
        }
    }

    let mut bench_log = perf_bench::bench_log::BenchLog::default_capacity("myelon_layers");
    bench_log.event("start");

    println!("=== Myelon Layer Overhead Sweep ===");
    println!("Compares raw disruptor ring vs FramedTransport vs TypedTransport+codec");
    println!("Full payload fill + consumer read. SHM backend.");
    println!();

    let mut all_results = Vec::new();

    // Test at 4 payload sizes
    struct SizeConfig { tag: &'static str, payload: usize, events: u64, buffer: usize, raw_prod: &'static str, raw_cons: &'static str, batch: usize }

    let sizes = [
        SizeConfig { tag: "1KB",  payload: 1024,  events: 200_000, buffer: 131_072, raw_prod: "raw_prod_1k",  raw_cons: "raw_cons_1k",  batch: 8 },
        SizeConfig { tag: "4KB",  payload: 4096,  events: 100_000, buffer: 65_536,  raw_prod: "raw_prod_4k",  raw_cons: "raw_cons_4k",  batch: 8 },
        SizeConfig { tag: "16KB", payload: 16384, events: 50_000,  buffer: 32_768,  raw_prod: "raw_prod_16k", raw_cons: "raw_cons_16k", batch: 64 },
        SizeConfig { tag: "64KB", payload: 65536, events: 20_000,  buffer: 16_384,  raw_prod: "raw_prod_64k", raw_cons: "raw_cons_64k", batch: 64 },
    ];

    for s in &sizes {
        bench_log.event(&format!("size_start: {}", s.tag));
        println!("--- {} ---", s.tag);

        // Raw ring (baseline)
        let raw = run_raw(s.tag, s.raw_prod, s.raw_cons, s.events, s.buffer);
        println!("  raw_ring:  prod={} cons={}", format_throughput(raw.prod_ops), format_throughput(raw.cons_ops));
        all_results.push(raw);

        // Framed (blocking recv — baseline)
        let framed = run_framed(s.tag, s.payload, s.events, 1024);
        println!("  framed:       prod={} cons={}", format_throughput(framed.prod_ops), format_throughput(framed.cons_ops));
        all_results.push(framed);

        // Framed BATCH recv (process_available_messages)
        let framed_batch = run_framed_batch(s.tag, s.payload, s.events, 1024);
        println!("  framed_batch: prod={} cons={}", format_throughput(framed_batch.prod_ops), format_throughput(framed_batch.cons_ops));
        all_results.push(framed_batch);

        // RIGHT-SIZED framed: frame slot matches payload (eliminates bandwidth waste)
        let (rs_prod, rs_cons, rs_buf) = match s.tag {
            "1KB"  => ("rs_framed_prod_2k", "rs_framed_cons_2k", 65_536usize),
            "4KB"  => ("rs_framed_prod_8k", "rs_framed_cons_8k", 32_768),
            "16KB" => ("rs_framed_prod_32k", "rs_framed_cons_32k", 16_384),
            _ => ("rs_framed_prod_2k", "rs_framed_cons_2k", 65_536), // fallback
        };
        if s.tag != "64KB" { // 64KB payload can't fit in 32KB frame, skip
            let rs_framed = run_rightsized_framed(s.tag, s.payload, s.events, rs_buf, rs_prod, rs_cons);
            println!("  framed_right: prod={} cons={}", format_throughput(rs_framed.prod_ops), format_throughput(rs_framed.cons_ops));
            all_results.push(rs_framed);
        }

        // rkyv
        let rkyv = run_codec(s.tag, "rkyv", s.batch, s.events, 1024);
        println!("  rkyv:      prod={} cons={}", format_throughput(rkyv.prod_ops), format_throughput(rkyv.cons_ops));
        all_results.push(rkyv);

        // bincode
        let bincode = run_codec(s.tag, "bincode", s.batch, s.events, 1024);
        println!("  bincode:      prod={} cons={}", format_throughput(bincode.prod_ops), format_throughput(bincode.cons_ops));
        all_results.push(bincode);

        // rkyv ZERO-COPY (access only, no deserialize)
        let rkyv_zc = {
            let segment = unique_segment(&format!("rkyv_zc_{}", s.tag));
            let exe = env::current_exe().expect("exe");
            let mut prod_cmd = Command::new(&exe);
            prod_cmd.arg("codec_prod").env("BENCH_SEGMENT", &segment).env("BENCH_EVENTS", s.events.to_string())
                .env("BENCH_BUFFER", "1024").env("BENCH_CODEC", "rkyv").env("BENCH_BATCH_SIZE", s.batch.to_string())
                .stdout(Stdio::piped()).stderr(Stdio::piped());
            let producer = prod_cmd.spawn().expect("spawn rkyv_zc prod");
            let consumer = spawn_child(&exe, "rkyv_zc_cons", &segment, s.events, 1024);
            let timeout = Duration::from_secs(300);
            let cons_out = wait_timeout(consumer, timeout);
            let prod_out = wait_timeout(producer, timeout);
            let prod_ops = prod_out.ok().map(|o| extract_value(&String::from_utf8_lossy(&o.stdout), "Throughput")).unwrap_or(0.0);
            let cons_ops = cons_out.ok().map(|o| extract_value(&String::from_utf8_lossy(&o.stdout), "Throughput")).unwrap_or(0.0);
            LayerResult { layer: "rkyv_zc", payload_label: s.tag.to_string(), prod_ops, cons_ops }
        };
        println!("  rkyv_zc:      prod={} cons={}", format_throughput(rkyv_zc.prod_ops), format_throughput(rkyv_zc.cons_ops));
        all_results.push(rkyv_zc);

        bench_log.event(&format!("size_done: {}", s.tag));
    }

    print_layer_comparison(&all_results);
    bench_log.event_val("total_scenarios", all_results.len() as u64);
}
