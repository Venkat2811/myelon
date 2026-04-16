//! End-to-end Codec benchmark over mmap typed transport.

use myelon_bench::events::format_throughput;
use myelon_bench::generated::bench_payload_generated::myelon::bench as flatbench;
use myelon_bench::reporting::{self, BenchReport, BenchResult};
use myelon::codec::{Codec, CodecError};
use myelon::typed_transport::{MmapTypedConsumer, MmapTypedProducer};
use myelon::transport::{FixedFrame, MmapFramedTransportConsumer, MmapFramedTransportProducer, MyelonWaitStrategy};
use std::env;
use std::hint::black_box;
use std::io::Read as _;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const FRAME_DATA_BYTES: usize = 64 * 1024 - 12;
type Frame = FixedFrame<FRAME_DATA_BYTES>;
const BUFFER_DEPTH: usize = 1024;

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
    (0..count)
        .map(|i| TestPayload {
            id: i as u64,
            token_ids: (0..128)
                .map(|j| ((j * 31) as u32).wrapping_add(i as u32))
                .collect(),
            block_table: (0..8).map(|j| j as u32 + (i as u32 * 8)).collect(),
            temperature: 0.7,
            label: format!("seq_{i}"),
        })
        .collect()
}

struct BincodeBatch(Vec<TestPayload>);

impl Codec for BincodeBatch {
    type Encoded = Vec<u8>;

    fn encode(&self) -> Result<Self::Encoded, CodecError> {
        bincode::serialize(&self.0).map_err(CodecError::encode)
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        bincode::deserialize(bytes)
            .map(BincodeBatch)
            .map_err(CodecError::decode)
    }
}

struct RkyvBatch(Vec<TestPayload>);

impl Codec for RkyvBatch {
    type Encoded = rkyv::util::AlignedVec;

    fn encode(&self) -> Result<Self::Encoded, CodecError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(&self.0).map_err(CodecError::encode)
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let archived =
            rkyv::access::<rkyv::Archived<Vec<TestPayload>>, rkyv::rancor::Error>(bytes)
                .map_err(CodecError::decode)?;
        let owned: Vec<TestPayload> =
            rkyv::deserialize::<Vec<TestPayload>, rkyv::rancor::Error>(archived)
                .map_err(CodecError::decode)?;
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
            let args = flatbench::TestPayloadArgs {
                id: payload.id,
                token_ids: Some(token_ids),
                block_table: Some(block_table),
                temperature: payload.temperature,
                label: Some(label),
            };
            entries.push(flatbench::TestPayload::create(&mut builder, &args));
        }

        let entries = builder.create_vector(&entries);
        let root = flatbench::PayloadBatch::create(
            &mut builder,
            &flatbench::PayloadBatchArgs {
                entries: Some(entries),
            },
        );
        builder.finish(root, None);
        Ok(builder.finished_data().to_vec())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let root =
            flatbuffers::root::<flatbench::PayloadBatch>(bytes).map_err(CodecError::decode)?;
        let entries = root
            .entries()
            .ok_or_else(|| CodecError::decode("missing entries vector"))?;
        let mut decoded = Vec::with_capacity(entries.len());
        for entry in entries.iter() {
            let token_ids = entry
                .token_ids()
                .map(|items| items.iter().collect())
                .unwrap_or_default();
            let block_table = entry
                .block_table()
                .map(|items| items.iter().collect())
                .unwrap_or_default();
            decoded.push(TestPayload {
                id: entry.id(),
                token_ids,
                block_table,
                temperature: entry.temperature(),
                label: entry.label().unwrap_or_default().to_string(),
            });
        }
        Ok(FlatbufBatch(decoded))
    }
}

fn unique_root() -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    env::temp_dir().join(format!("myelon_codec_mmap_{}_{}", std::process::id(), ts))
}

fn unique_segment() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("codec_{}_{}", std::process::id(), ts)
}

fn spawn_child(
    exe: &std::path::Path,
    role: &str,
    root: &str,
    segment: &str,
    codec: &str,
    batch_size: usize,
    messages: u64,
    phase_timing: bool,
) -> Child {
    let mut cmd = Command::new(exe);
    cmd.arg(role)
        .env("MMAP_ROOT", root)
        .env("MMAP_SEGMENT", segment)
        .env("BENCH_CODEC", codec)
        .env("BENCH_BATCH_SIZE", batch_size.to_string())
        .env("BENCH_MESSAGES", messages.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if phase_timing {
        cmd.env("BENCH_PHASE_TIMING", "1");
    }
    cmd.spawn()
        .unwrap_or_else(|e| panic!("spawn {role}: {e}"))
}

fn wait_timeout(mut child: Child, timeout: Duration) -> Result<Output, String> {
    fn collect(child: &mut Child, status: std::process::ExitStatus) -> Output {
        let mut out = Vec::new();
        let mut err = Vec::new();
        if let Some(mut o) = child.stdout.take() {
            let _ = o.read_to_end(&mut out);
        }
        if let Some(mut e) = child.stderr.take() {
            let _ = e.read_to_end(&mut err);
        }
        Output {
            status,
            stdout: out,
            stderr: err,
        }
    }

    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
            return Ok(collect(&mut child, status));
        }
        if start.elapsed() >= timeout {
            let _ = child.kill();
            let status = child.wait().map_err(|e| e.to_string())?;
            let output = collect(&mut child, status);
            return Err(format!(
                "timeout; stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn extract_value(output: &str, key: &str) -> f64 {
    for line in output.lines() {
        if let Some(rest) = line.strip_prefix(&format!("{key}: ")) {
            if let Some(n) = rest.split_whitespace().next() {
                return n.parse().unwrap_or(0.0);
            }
        }
    }
    0.0
}

fn extract_events(output: &str) -> u64 {
    for line in output.lines() {
        if let Some(rest) = line.strip_prefix("Events: ") {
            return rest.parse().unwrap_or(0);
        }
    }
    0
}

fn read_env() -> (String, usize, u64, disruptor_mp::MmapTransportLayout, String) {
    let codec = env::var("BENCH_CODEC").expect("BENCH_CODEC");
    let batch_size = env::var("BENCH_BATCH_SIZE")
        .expect("BENCH_BATCH_SIZE")
        .parse()
        .expect("batch size");
    let messages = env::var("BENCH_MESSAGES")
        .expect("BENCH_MESSAGES")
        .parse()
        .expect("message count");
    let root = env::var("MMAP_ROOT").expect("MMAP_ROOT");
    let segment = env::var("MMAP_SEGMENT").expect("MMAP_SEGMENT");
    let layout = disruptor_mp::MmapTransportLayout::new(PathBuf::from(root), segment.clone())
        .expect("mmap layout");
    (codec, batch_size, messages, layout, segment)
}

fn payload_bytes(codec: &str, payloads: &[TestPayload]) -> usize {
    match codec {
        "bincode" => BincodeBatch(payloads.to_vec())
            .encode()
            .expect("bincode encode")
            .len(),
        "rkyv" => RkyvBatch(payloads.to_vec())
            .encode()
            .expect("rkyv encode")
            .as_ref()
            .len(),
        "flatbuf" => FlatbufBatch(payloads.to_vec())
            .encode()
            .expect("flatbuf encode")
            .len(),
        _ => 0,
    }
}

fn producer_process() -> Result<(), Box<dyn std::error::Error>> {
    let (codec, batch_size, messages, layout, _segment) = read_env();
    let phase_timing = env::var("BENCH_PHASE_TIMING").ok().map_or(false, |v| v == "1");
    let payloads = make_payloads(batch_size);

    if phase_timing {
        let mut producer = MmapFramedTransportProducer::<Frame>::create(layout, BUFFER_DEPTH)?;
        if !producer.raw().wait_for_consumers_ready(1, Duration::from_secs(30)) {
            return Err("timeout waiting for consumer".into());
        }

        let mut encode_ns = 0u64;
        let mut transport_ns = 0u64;
        let start = Instant::now();

        macro_rules! phase_loop {
            ($batch:expr) => {
                for i in 0..messages {
                    let t0 = Instant::now();
                    let encoded = $batch.encode()?;
                    let t1 = Instant::now();
                    producer.publish(encoded.as_ref(), (i % 256) as u8);
                    let t2 = Instant::now();
                    encode_ns += (t1 - t0).as_nanos() as u64;
                    transport_ns += (t2 - t1).as_nanos() as u64;
                }
            };
        }

        match codec.as_str() {
            "bincode" => { let b = BincodeBatch(payloads); phase_loop!(b); }
            "rkyv" => { let b = RkyvBatch(payloads); phase_loop!(b); }
            "flatbuf" => { let b = FlatbufBatch(payloads); phase_loop!(b); }
            other => return Err(format!("unknown codec: {other}").into()),
        }

        let elapsed = start.elapsed();
        println!("Throughput: {:.0} msgs/sec", messages as f64 / elapsed.as_secs_f64());
        println!("EncodeAvgUs: {:.1}", encode_ns as f64 / messages as f64 / 1000.0);
        println!("TransportWriteAvgUs: {:.1}", transport_ns as f64 / messages as f64 / 1000.0);

        let last_sequence = producer.raw().last_published_sequence();
        producer.wait_until_consumed(last_sequence, Duration::from_secs(30), disruptor_mp::AutoWaitStrategy::BusySpin);
    } else {
        let mut producer = MmapTypedProducer::<Frame>::create(layout, BUFFER_DEPTH)?;
        if !producer.raw().wait_for_consumers_ready(1, Duration::from_secs(30)) {
            return Err("timeout waiting for consumer".into());
        }

        let start = Instant::now();
        match codec.as_str() {
            "bincode" => { let p = BincodeBatch(payloads); for i in 0..messages { producer.publish(&p, (i % 256) as u8)?; } }
            "rkyv" => { let p = RkyvBatch(payloads); for i in 0..messages { producer.publish(&p, (i % 256) as u8)?; } }
            "flatbuf" => { let p = FlatbufBatch(payloads); for i in 0..messages { producer.publish(&p, (i % 256) as u8)?; } }
            other => return Err(format!("unknown codec: {other}").into()),
        }
        let elapsed = start.elapsed();
        println!("Throughput: {:.0} msgs/sec", messages as f64 / elapsed.as_secs_f64());

        let last_sequence = producer.raw().raw().last_published_sequence();
        producer.raw().wait_until_consumed(last_sequence, Duration::from_secs(30), disruptor_mp::AutoWaitStrategy::BusySpin);
    }
    Ok(())
}

fn consumer_process() -> Result<(), Box<dyn std::error::Error>> {
    let (codec, _batch_size, messages, layout, _segment) = read_env();
    let phase_timing = env::var("BENCH_PHASE_TIMING").ok().map_or(false, |v| v == "1");
    let consumer_id = format!("c{}", std::process::id());

    if phase_timing {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut consumer = loop {
            match MmapFramedTransportConsumer::<Frame>::attach(layout.clone(), BUFFER_DEPTH, &consumer_id, MyelonWaitStrategy::BusySpin) {
                Ok(c) => break c,
                Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
                Err(e) => return Err(format!("consumer attach failed: {e}").into()),
            }
        };

        let mut recv_ns = 0u64;
        let mut decode_ns = 0u64;
        let start = Instant::now();
        let mut consumed = 0u64;
        while consumed < messages {
            let t0 = Instant::now();
            let (_kind, raw_bytes) = consumer.recv_message_blocking();
            let t1 = Instant::now();
            match codec.as_str() {
                "bincode" => { black_box(BincodeBatch::decode(&raw_bytes)?); }
                "rkyv" => { black_box(RkyvBatch::decode(&raw_bytes)?); }
                "flatbuf" => { black_box(FlatbufBatch::decode(&raw_bytes)?); }
                other => return Err(format!("unknown codec: {other}").into()),
            }
            let t2 = Instant::now();
            recv_ns += (t1 - t0).as_nanos() as u64;
            decode_ns += (t2 - t1).as_nanos() as u64;
            consumed += 1;
        }
        let elapsed = start.elapsed();
        println!("Throughput: {:.0} msgs/sec", consumed as f64 / elapsed.as_secs_f64());
        println!("Events: {}", consumed);
        println!("TransportReadAvgUs: {:.1}", recv_ns as f64 / consumed as f64 / 1000.0);
        println!("DecodeAvgUs: {:.1}", decode_ns as f64 / consumed as f64 / 1000.0);
    } else {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut consumer = loop {
            match MmapTypedConsumer::<Frame>::attach(layout.clone(), BUFFER_DEPTH, &consumer_id, MyelonWaitStrategy::BusySpin) {
                Ok(c) => break c,
                Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
                Err(e) => return Err(format!("consumer attach failed: {e}").into()),
            }
        };

        let start = Instant::now();
        let mut consumed = 0u64;
        while consumed < messages {
            match codec.as_str() {
                "bincode" => { let (_, b): (u8, BincodeBatch) = consumer.recv()?; black_box(b.0.len()); }
                "rkyv" => { let (_, b): (u8, RkyvBatch) = consumer.recv()?; black_box(b.0.len()); }
                "flatbuf" => { let (_, b): (u8, FlatbufBatch) = consumer.recv()?; black_box(b.0.len()); }
                other => return Err(format!("unknown codec: {other}").into()),
            }
            consumed += 1;
        }
        let elapsed = start.elapsed();
        println!("Throughput: {:.0} msgs/sec", consumed as f64 / elapsed.as_secs_f64());
        println!("Events: {}", consumed);
    }
    Ok(())
}

fn run_codec_bench(codec: &str, batch_size: usize, messages: u64, phase_timing: bool) -> BenchResult {
    let root = unique_root();
    let segment = unique_segment();
    let root_str = root.display().to_string();
    let exe = env::current_exe().expect("current_exe");
    let payloads = make_payloads(batch_size);
    let encoded_bytes = payload_bytes(codec, &payloads);

    let producer =
        spawn_child(&exe, "codec_producer", &root_str, &segment, codec, batch_size, messages, phase_timing);
    let consumer =
        spawn_child(&exe, "codec_consumer", &root_str, &segment, codec, batch_size, messages, phase_timing);

    let timeout = Duration::from_secs(180);
    let prod_out = wait_timeout(producer, timeout);
    let cons_out = wait_timeout(consumer, timeout);

    let prod_str = match prod_out {
        Ok(o) => {
            if !o.stderr.is_empty() {
                eprintln!(
                    "Prod stderr [{codec}/{batch_size}]: {}",
                    String::from_utf8_lossy(&o.stderr)
                );
            }
            String::from_utf8_lossy(&o.stdout).to_string()
        }
        Err(e) => {
            eprintln!("Prod error [{codec}/{batch_size}]: {e}");
            String::new()
        }
    };
    let cons_str = match cons_out {
        Ok(o) => {
            if !o.stderr.is_empty() {
                eprintln!(
                    "Cons stderr [{codec}/{batch_size}]: {}",
                    String::from_utf8_lossy(&o.stderr)
                );
            }
            String::from_utf8_lossy(&o.stdout).to_string()
        }
        Err(e) => {
            eprintln!("Cons error [{codec}/{batch_size}]: {e}");
            String::new()
        }
    };

    let prod_tp = extract_value(&prod_str, "Throughput");
    let cons_tp = extract_value(&cons_str, "Throughput");
    let consumed = extract_events(&cons_str);

    let _ = std::fs::remove_dir_all(&root);

    if phase_timing {
        let encode_us = extract_value(&prod_str, "EncodeAvgUs");
        let write_us = extract_value(&prod_str, "TransportWriteAvgUs");
        let read_us = extract_value(&cons_str, "TransportReadAvgUs");
        let decode_us = extract_value(&cons_str, "DecodeAvgUs");
        println!(
            "  codec={:<8} batch={:<3}  encode: {:>8.1}μs  write: {:>8.1}μs  read: {:>8.1}μs  decode: {:>8.1}μs  | prod: {:>8} cons: {:>8}",
            codec, batch_size, encode_us, write_us, read_us, decode_us,
            format_throughput(prod_tp), format_throughput(cons_tp),
        );
    } else {
        println!(
            "  codec={:<8} batch={:<3} producer: {:>10} msgs/s  consumer: {:>10} msgs/s",
            codec, batch_size, format_throughput(prod_tp), format_throughput(cons_tp),
        );
    }

    reporting::make_result(
        "codec_e2e_mmap",
        &format!("codec_e2e_{batch_size}seq_{codec}"),
        "mmap",
        "typed",
        Some(codec),
        "BusySpin",
        encoded_bytes,
        BUFFER_DEPTH,
        consumed,
        0,
        1,
        prod_tp,
        cons_tp,
        None,
    )
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 {
        let role = &args[1];
        if role.starts_with("--") {
            // fall through to orchestrator mode
        } else {
            let result = match role.as_str() {
                "codec_producer" => producer_process(),
                "codec_consumer" => consumer_process(),
                _ => Ok(()),
            };
            if let Err(e) = result {
                eprintln!("{role} failed: {e}");
                std::process::exit(1);
            }
            return;
        }
    }

    let mode = args.windows(2)
        .find(|w| w[0] == "--mode")
        .map(|w| w[1].as_str())
        .unwrap_or("throughput");
    let phase_timing = mode == "phase_timing";
    let json_mode = args.iter().any(|a| a == "--json");

    if !json_mode {
        println!("=== Codec E2E MMAP Benchmark ===");
        println!("Transport: TypedTransport over file-backed mmap");
        println!("Mode: {}", if phase_timing { "phase_timing (encode/transport/decode)" } else { "throughput" });
        println!("Payload: Vec<TestPayload> with Sequence-like Vec fields");
        println!();
    }

    let scenarios = [(8usize, 50_000u64), (64, 20_000), (256, 10_000)];
    let mut report = BenchReport::new();
    for (batch_size, messages) in scenarios {
        for codec in ["bincode", "rkyv", "flatbuf"] {
            report.add(run_codec_bench(codec, batch_size, messages, phase_timing));
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
