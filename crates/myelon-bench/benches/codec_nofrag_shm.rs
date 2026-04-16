//! Codec benchmark WITHOUT fragmentation — ring slot sized to fit encoded payload.
//!
//! This is the production-representative number: raw disruptor ring with slot
//! size >= encoded payload, no FramedTransport, no fragmentation overhead.
//! Isolates pure codec overhead + raw ring transit.
//!
//! Slot sizes: 8KB (batch=8), 64KB (batch=64), 256KB (batch=256)
//!
//! Run: cargo bench -p myelon-bench --bench codec_nofrag_shm
//! Single batch: cargo bench -p myelon-bench --bench codec_nofrag_shm -- --batch 256

use disruptor_mp::{build_shared_single_producer, CoordinationMode, SharedDisruptorBuilder, SharedMemoryConfig};
use myelon_bench::coordination::BenchmarkCoordination;
use myelon_bench::events::{format_throughput, nanos_now};
use myelon_bench::generated::bench_payload_generated::myelon::bench as flatbench;
use myelon_bench::latency::LatencyRecorder;
use myelon_bench::reporting::{self, BenchReport, BenchResult};
use myelon::codec::{Codec, CodecError};
use std::env;
use std::hint::black_box;
use std::io::Read as _;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

// ============================================================
// Slot types — each sized to fit the encoded payload with margin
// ============================================================

/// 8KB slot for batch=8 (~4.5KB encoded)
#[repr(C)]
#[derive(Clone, Copy)]
struct Slot8K {
    len: u32,
    timestamp: u64,
    data: [u8; 8192 - 12],
}
impl Default for Slot8K { fn default() -> Self { Self { len: 0, timestamp: 0, data: [0; 8192 - 12] } } }

/// 64KB slot for batch=64 (~37KB encoded)
#[repr(C)]
#[derive(Clone, Copy)]
struct Slot64K {
    len: u32,
    timestamp: u64,
    data: [u8; 65536 - 12],
}
impl Default for Slot64K { fn default() -> Self { Self { len: 0, timestamp: 0, data: [0; 65536 - 12] } } }

/// 256KB slot for batch=256 (~145KB encoded)
#[repr(C)]
#[derive(Clone, Copy)]
struct Slot256K {
    len: u32,
    timestamp: u64,
    data: [u8; 262144 - 12],
}
impl Default for Slot256K { fn default() -> Self { Self { len: 0, timestamp: 0, data: [0; 262144 - 12] } } }

// ============================================================
// Payload + codec types (same as codec_e2e_shm)
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

struct FlatbufBatch(Vec<TestPayload>);
impl Codec for FlatbufBatch {
    type Encoded = Vec<u8>;
    fn encode(&self) -> Result<Self::Encoded, CodecError> {
        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(16 * 1024);
        let mut entries = Vec::with_capacity(self.0.len());
        for payload in &self.0 {
            let token_ids = builder.create_vector(&payload.token_ids);
            let block_table = builder.create_vector(&payload.block_table);
            let label = builder.create_string(&payload.label);
            entries.push(flatbench::TestPayload::create(&mut builder, &flatbench::TestPayloadArgs {
                id: payload.id, token_ids: Some(token_ids), block_table: Some(block_table),
                temperature: payload.temperature, label: Some(label),
            }));
        }
        let entries = builder.create_vector(&entries);
        let root = flatbench::PayloadBatch::create(&mut builder, &flatbench::PayloadBatchArgs { entries: Some(entries) });
        builder.finish(root, None);
        Ok(builder.finished_data().to_vec())
    }
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let root = flatbuffers::root::<flatbench::PayloadBatch>(bytes).map_err(CodecError::decode)?;
        let entries = root.entries().ok_or_else(|| CodecError::decode("missing entries"))?;
        let mut decoded = Vec::with_capacity(entries.len());
        for entry in entries.iter() {
            decoded.push(TestPayload {
                id: entry.id(),
                token_ids: entry.token_ids().map(|v| v.iter().collect()).unwrap_or_default(),
                block_table: entry.block_table().map(|v| v.iter().collect()).unwrap_or_default(),
                temperature: entry.temperature(),
                label: entry.label().unwrap_or_default().to_string(),
            });
        }
        Ok(FlatbufBatch(decoded))
    }
}

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
    disruptor_mp::portable_shm_segment_name(&format!("nf_{}_{}_{}", label, std::process::id() % 10000, ts % 100000))
}

fn spawn_child(exe: &std::path::Path, role: &str, segment: &str, codec: &str, batch: usize, messages: u64) -> Child {
    Command::new(exe)
        .arg(role)
        .env("BENCHMARK_SEGMENT_NAME", segment)
        .env("BENCH_CODEC", codec)
        .env("BENCH_BATCH_SIZE", batch.to_string())
        .env("BENCH_MESSAGES", messages.to_string())
        .stdout(Stdio::piped()).stderr(Stdio::piped())
        .spawn().unwrap_or_else(|e| panic!("spawn {role}: {e}"))
}

fn wait_timeout(mut child: Child, timeout: Duration) -> Result<Output, String> {
    fn collect(child: &mut Child, status: std::process::ExitStatus) -> Output {
        let mut out = Vec::new(); let mut err = Vec::new();
        if let Some(mut o) = child.stdout.take() { let _ = o.read_to_end(&mut out); }
        if let Some(mut e) = child.stderr.take() { let _ = e.read_to_end(&mut err); }
        Output { status, stdout: out, stderr: err }
    }
    let start = Instant::now();
    loop {
        if let Some(s) = child.try_wait().map_err(|e| e.to_string())? { return Ok(collect(&mut child, s)); }
        if start.elapsed() >= timeout {
            let _ = child.kill();
            let s = child.wait().map_err(|e| e.to_string())?;
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

fn extract_latency_json(output: &str) -> Option<myelon_bench::latency::LatencyStats> {
    for line in output.lines() {
        if let Some(json) = line.strip_prefix("LatencyJSON: ") { return serde_json::from_str(json).ok(); }
    }
    None
}

// ============================================================
// Generic producer/consumer using macro to handle different slot sizes
// ============================================================

macro_rules! impl_nofrag_bench {
    ($slot_type:ty, $slot_data_len:expr, $producer_fn:ident, $consumer_fn:ident) => {
        fn $producer_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = env::var("BENCHMARK_SEGMENT_NAME").expect("BENCHMARK_SEGMENT_NAME");
            let codec = env::var("BENCH_CODEC").expect("BENCH_CODEC");
            let batch_size = read_env_usize("BENCH_BATCH_SIZE", 8);
            let messages = read_env_u64("BENCH_MESSAGES", 50_000);
            let buffer_depth = read_env_usize("BENCH_BUFFER_DEPTH", 4096);

            let payloads = make_payloads(batch_size);

            let mut producer = build_shared_single_producer::<$slot_type>(&segment, buffer_depth)
                .enable_discovery(1)
                .with_coordination(CoordinationMode::Immediate)
                .build_producer(|| <$slot_type>::default())?;

            let coord = BenchmarkCoordination::create(&segment)?;
            if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
                return Err("timeout waiting for consumer".into());
            }
            for _ in 0..20 { let _ = producer.min_gating_sequence(); std::thread::sleep(Duration::from_millis(2)); }

            let mut encode_ns = 0u64;
            let mut transport_ns = 0u64;

            macro_rules! run_loop {
                ($batch:expr) => {
                    for i in 0..messages {
                        let t0 = Instant::now();
                        let encoded = $batch.encode()?;
                        let encoded_bytes: &[u8] = encoded.as_ref();
                        let t1 = Instant::now();
                        let len = encoded_bytes.len();
                        assert!(len <= $slot_data_len, "payload {}B exceeds slot {}B", len, $slot_data_len);
                        producer.publish(|slot| {
                            slot.len = len as u32;
                            slot.timestamp = nanos_now();
                            slot.data[..len].copy_from_slice(encoded_bytes);
                        });
                        let t2 = Instant::now();
                        encode_ns += (t1 - t0).as_nanos() as u64;
                        transport_ns += (t2 - t1).as_nanos() as u64;
                    }
                };
            }

            let start = Instant::now();
            match codec.as_str() {
                "bincode" => { let b = BincodeBatch(payloads); run_loop!(b); }
                "rkyv" => { let b = RkyvBatch(payloads); run_loop!(b); }
                "flatbuf" => { let b = FlatbufBatch(payloads); run_loop!(b); }
                other => return Err(format!("unknown codec: {other}").into()),
            }
            let elapsed = start.elapsed();

            println!("Throughput: {:.0} msgs/sec", messages as f64 / elapsed.as_secs_f64());
            println!("EncodeAvgUs: {:.1}", encode_ns as f64 / messages as f64 / 1000.0);
            println!("TransportWriteAvgUs: {:.1}", transport_ns as f64 / messages as f64 / 1000.0);
            coord.signal_producer_done(messages as i64);
            coord.wait_for_consumers_done(1, Duration::from_secs(60));
            Ok(())
        }

        fn $consumer_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = env::var("BENCHMARK_SEGMENT_NAME").expect("BENCHMARK_SEGMENT_NAME");
            let codec = env::var("BENCH_CODEC").expect("BENCH_CODEC");
            let messages = read_env_u64("BENCH_MESSAGES", 50_000);
            let buffer_depth = read_env_usize("BENCH_BUFFER_DEPTH", 4096);

            let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
            let config = SharedMemoryConfig {
                name: segment, buffer_size: buffer_depth,
                element_size: std::mem::size_of::<$slot_type>(), create: false,
            };
            let mut consumer = SharedDisruptorBuilder::<$slot_type>::new(config).build_consumer()?;
            coord.signal_consumer_ready();

            let mut latency = LatencyRecorder::default_range();
            let mut decode_ns = 0u64;
            let start = Instant::now();
            let mut consumed = 0u64;

            while consumed < messages {
                consumer.process_available(|slot, _seq| {
                    let now = nanos_now();
                    if slot.timestamp > 0 { latency.record_delta(slot.timestamp, now); }
                    let len = slot.len as usize;
                    let bytes = &slot.data[..len];
                    let t0 = Instant::now();
                    match codec.as_str() {
                        "bincode" => { black_box(BincodeBatch::decode(bytes).unwrap()); }
                        "rkyv" => { black_box(RkyvBatch::decode(bytes).unwrap()); }
                        "flatbuf" => { black_box(FlatbufBatch::decode(bytes).unwrap()); }
                        _ => {}
                    }
                    let t1 = Instant::now();
                    decode_ns += (t1 - t0).as_nanos() as u64;
                    consumed += 1;
                });
                if consumed < messages { std::hint::spin_loop(); }
            }
            let elapsed = start.elapsed();

            println!("Throughput: {:.0} msgs/sec", consumed as f64 / elapsed.as_secs_f64());
            println!("Events: {}", consumed);
            println!("DecodeAvgUs: {:.1}", decode_ns as f64 / consumed as f64 / 1000.0);
            if let Some(stats) = latency.stats() {
                println!("Latency: {}", stats.summary());
                println!("LatencyJSON: {}", serde_json::to_string(&stats).unwrap_or_default());
            }
            coord.signal_consumer_done(consumed as i64);
            Ok(())
        }
    };
}

impl_nofrag_bench!(Slot8K, 8180, producer_8k, consumer_8k);
impl_nofrag_bench!(Slot64K, 65524, producer_64k, consumer_64k);
impl_nofrag_bench!(Slot256K, 262132, producer_256k, consumer_256k);

// ============================================================
// Orchestrator
// ============================================================

fn run_nofrag(codec: &str, batch_size: usize, messages: u64, buffer_depth: usize) -> BenchResult {
    let (prod_role, cons_role) = match batch_size {
        8 => ("nf_prod_8k", "nf_cons_8k"),
        64 => ("nf_prod_64k", "nf_cons_64k"),
        256 => ("nf_prod_256k", "nf_cons_256k"),
        _ => panic!("unsupported batch size: {batch_size}"),
    };
    let slot_bytes = match batch_size { 8 => 8192, 64 => 65536, _ => 262144 };
    let segment = unique_segment(&format!("{codec}_{batch_size}"));
    let exe = env::current_exe().expect("current_exe");

    let mut prod_cmd = Command::new(&exe);
    prod_cmd.arg(prod_role)
        .env("BENCHMARK_SEGMENT_NAME", &segment)
        .env("BENCH_CODEC", codec)
        .env("BENCH_BATCH_SIZE", batch_size.to_string())
        .env("BENCH_MESSAGES", messages.to_string())
        .env("BENCH_BUFFER_DEPTH", buffer_depth.to_string())
        .stdout(Stdio::piped()).stderr(Stdio::piped());
    let producer = prod_cmd.spawn().expect("spawn producer");

    let mut cons_cmd = Command::new(&exe);
    cons_cmd.arg(cons_role)
        .env("BENCHMARK_SEGMENT_NAME", &segment)
        .env("BENCH_CODEC", codec)
        .env("BENCH_BATCH_SIZE", batch_size.to_string())
        .env("BENCH_MESSAGES", messages.to_string())
        .env("BENCH_BUFFER_DEPTH", buffer_depth.to_string())
        .stdout(Stdio::piped()).stderr(Stdio::piped());
    let consumer = cons_cmd.spawn().expect("spawn consumer");

    let timeout = Duration::from_secs(180);
    let cons_out = wait_timeout(consumer, timeout);
    let prod_out = wait_timeout(producer, timeout);

    let prod_str = match prod_out {
        Ok(o) => { if !o.stderr.is_empty() { eprintln!("[{codec}/{batch_size} prod] {}", String::from_utf8_lossy(&o.stderr)); } String::from_utf8_lossy(&o.stdout).to_string() }
        Err(e) => { eprintln!("[{codec}/{batch_size} prod] {e}"); String::new() }
    };
    let cons_str = match cons_out {
        Ok(o) => { if !o.stderr.is_empty() { eprintln!("[{codec}/{batch_size} cons] {}", String::from_utf8_lossy(&o.stderr)); } String::from_utf8_lossy(&o.stdout).to_string() }
        Err(e) => { eprintln!("[{codec}/{batch_size} cons] {e}"); String::new() }
    };

    let prod_tp = extract_value(&prod_str, "Throughput");
    let cons_tp = extract_value(&cons_str, "Throughput");
    let encode_us = extract_value(&prod_str, "EncodeAvgUs");
    let write_us = extract_value(&prod_str, "TransportWriteAvgUs");
    let decode_us = extract_value(&cons_str, "DecodeAvgUs");
    let latency = extract_latency_json(&cons_str);

    let lat_str = latency.as_ref().map(|l| l.summary()).unwrap_or_else(|| "-".to_string());

    println!(
        "  {:<8} batch={:<3} slot={:>4}KB  enc: {:>6.1}μs  write: {:>6.1}μs  dec: {:>6.1}μs  | prod: {:>8}  cons: {:>8}  {}",
        codec, batch_size, slot_bytes / 1024, encode_us, write_us, decode_us,
        format_throughput(prod_tp), format_throughput(cons_tp), lat_str,
    );

    reporting::make_result(
        "codec_nofrag_shm",
        &format!("nofrag_{batch_size}seq_{codec}"),
        "shm", "raw_ring+codec",
        Some(codec), "BusySpin",
        slot_bytes, buffer_depth, messages, 0, 1,
        prod_tp, cons_tp, latency,
    )
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 {
        let role = &args[1];
        if role.starts_with("--") { /* fall through */ } else {
            let result = match role.as_str() {
                "nf_prod_8k" => producer_8k(),
                "nf_cons_8k" => consumer_8k(),
                "nf_prod_64k" => producer_64k(),
                "nf_cons_64k" => consumer_64k(),
                "nf_prod_256k" => producer_256k(),
                "nf_cons_256k" => consumer_256k(),
                _ => Ok(()),
            };
            if let Err(e) = result { eprintln!("{role} failed: {e}"); std::process::exit(1); }
            return;
        }
    }

    let batch_arg = args.windows(2)
        .find(|w| w[0] == "--batch")
        .map(|w| w[1].as_str())
        .unwrap_or("all");

    let json_mode = args.iter().any(|a| a == "--json");

    if !json_mode {
        println!("=== Codec No-Frag SHM Benchmark ===");
        println!("Transport: raw disruptor ring (slot sized to payload, ZERO fragmentation)");
        println!("This is the production-representative number.");
        println!();
    }

    // Buffer depths: larger for smaller slots to maintain good ring utilization
    let scenarios = [
        (8usize, 100_000u64, 16384usize),   // 8KB × 16K = 128MB ring
        (64, 50_000, 4096),                  // 64KB × 4K = 256MB ring
        (256, 20_000, 2048),                 // 256KB × 2K = 512MB ring
    ];

    let mut report = BenchReport::new();
    for (batch_size, messages, buffer_depth) in scenarios {
        if batch_arg == "all" || batch_arg == &batch_size.to_string() {
            for codec in ["bincode", "rkyv", "flatbuf"] {
                report.add(run_nofrag(codec, batch_size, messages, buffer_depth));
            }
        }
    }

    if json_mode {
        println!("{}", serde_json::to_string_pretty(&report).expect("serialize"));
    } else {
        report.print_summary();
    }

    if let Some(path) = env::var("MYELON_BENCH_JSON_OUT").ok() {
        report.write_json(&path).expect("write JSON");
    }
}
