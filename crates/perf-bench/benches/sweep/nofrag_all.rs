//! No-frag zero-copy sweep: rkyv + flatbuf over SHM + mmap.
//!
//! All layers use right-sized ring slots (slot = encoded payload).
//! Measures the TRUE zero-copy path: serialize → ring → access in-place.
//!
//! Run: cargo bench -p perf-bench --bench nofrag_all

use disruptor_mp::{
    build_shared_single_producer, CoordinationMode, SharedDisruptorBuilder, SharedMemoryConfig,
    MmapConsumer, MmapProducer, MmapTransportLayout,
};
use perf_bench::coordination::BenchmarkCoordination;
use perf_bench::events::{format_throughput, BenchEvent};
use perf_bench::generated::bench_payload_generated::myelon::bench as flatbench;
use std::env;
use std::hint::black_box;
use std::io::Read as _;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tabled::{Table, Tabled, settings::Style};

// ============================================================
// Payload types
// ============================================================

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct TestPayload { id: u64, token_ids: Vec<u32>, block_table: Vec<u32>, temperature: f32, label: String }

fn make_payloads(count: usize) -> Vec<TestPayload> {
    (0..count).map(|i| TestPayload {
        id: i as u64,
        token_ids: (0..128).map(|j| ((j * 31) as u32).wrapping_add(i as u32)).collect(),
        block_table: (0..8).map(|j| j as u32 + (i as u32 * 8)).collect(),
        temperature: 0.7, label: format!("seq_{i}"),
    }).collect()
}

fn encode_rkyv(payloads: &[TestPayload]) -> Vec<u8> {
    let v = payloads.to_vec();
    rkyv::to_bytes::<rkyv::rancor::Error>(&v).unwrap().to_vec()
}

fn encode_flatbuf(payloads: &[TestPayload]) -> Vec<u8> {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(64 * 1024);
    let mut entries = Vec::with_capacity(payloads.len());
    for p in payloads {
        let tids = builder.create_vector(&p.token_ids);
        let bt = builder.create_vector(&p.block_table);
        let label = builder.create_string(&p.label);
        entries.push(flatbench::TestPayload::create(&mut builder, &flatbench::TestPayloadArgs {
            id: p.id, token_ids: Some(tids), block_table: Some(bt),
            temperature: p.temperature, label: Some(label),
        }));
    }
    let entries = builder.create_vector(&entries);
    let root = flatbench::PayloadBatch::create(&mut builder, &flatbench::PayloadBatchArgs { entries: Some(entries) });
    builder.finish(root, None);
    builder.finished_data().to_vec()
}

// ============================================================
// Slot types (right-sized, no framing)
// ============================================================

#[repr(C)] #[derive(Clone, Copy)]
struct Slot2K { len: u32, _pad: u32, data: [u8; 2048 - 8] }
impl Default for Slot2K { fn default() -> Self { Self { len: 0, _pad: 0, data: [0; 2048 - 8] } } }

#[repr(C)] #[derive(Clone, Copy)]
struct Slot8K { len: u32, _pad: u32, data: [u8; 8192 - 8] }
impl Default for Slot8K { fn default() -> Self { Self { len: 0, _pad: 0, data: [0; 8192 - 8] } } }

#[repr(C)] #[derive(Clone, Copy)]
struct Slot32K { len: u32, _pad: u32, data: [u8; 32768 - 8] }
impl Default for Slot32K { fn default() -> Self { Self { len: 0, _pad: 0, data: [0; 32768 - 8] } } }

#[repr(C)] #[derive(Clone, Copy)]
struct Slot128K { len: u32, _pad: u32, data: [u8; 131072 - 8] }
impl Default for Slot128K { fn default() -> Self { Self { len: 0, _pad: 0, data: [0; 131072 - 8] } } }

// Raw ring baselines at each size
type Ev1K = BenchEvent<1008>;
type Ev4K = BenchEvent<4080>;
type Ev16K = BenchEvent<{ 16 * 1024 - 16 }>;
type Ev64K = BenchEvent<{ 64 * 1024 - 16 }>;

// ============================================================
// Helpers
// ============================================================

fn read_env_usize(key: &str, default: usize) -> usize { env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default) }
fn read_env_u64(key: &str, default: u64) -> u64 { env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default) }

fn unique_segment(label: &str) -> String {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    disruptor_mp::portable_shm_segment_name(&format!("nfa_{}_{}_{}", label, std::process::id() % 10000, ts % 100000))
}
fn unique_root(label: &str) -> PathBuf {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    env::temp_dir().join(format!("nfa_mmap_{label}_{}_{}", std::process::id(), ts))
}

fn spawn_child(exe: &std::path::Path, role: &str, envs: &[(&str, String)]) -> Child {
    let mut cmd = Command::new(exe);
    cmd.arg(role).stdout(Stdio::piped()).stderr(Stdio::piped());
    for (k, v) in envs { cmd.env(k, v); }
    cmd.spawn().unwrap_or_else(|e| panic!("spawn {role}: {e}"))
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
        if start.elapsed() >= timeout { let _ = child.kill(); let s = child.wait().map_err(|e| e.to_string())?; return Err(format!("timeout; stderr: {}", String::from_utf8_lossy(&collect(&mut child, s).stderr))); }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn extract_value(output: &str, key: &str) -> f64 {
    for line in output.lines() { if let Some(rest) = line.strip_prefix(&format!("{key}: ")) { if let Some(n) = rest.split_whitespace().next() { return n.parse().unwrap_or(0.0); } } }
    0.0
}

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
                .enable_discovery(1).with_coordination(CoordinationMode::Immediate)
                .build_producer(|| <$slot>::default())?;
            let coord = BenchmarkCoordination::create(&seg)?;
            if !coord.wait_for_consumers(1, Duration::from_secs(30)) { return Err("timeout".into()); }
            for _ in 0..20 { let _ = producer.min_gating_sequence(); std::thread::sleep(Duration::from_millis(2)); }
            let start = Instant::now();
            for _ in 0..events {
                let enc = $encode_fn(&payloads);
                producer.publish(|slot| { slot.len = enc.len() as u32; slot.data[..enc.len()].copy_from_slice(&enc); });
            }
            let elapsed = start.elapsed();
            println!("Throughput: {:.0}", events as f64 / elapsed.as_secs_f64());
            coord.signal_producer_done(events as i64);
            coord.wait_for_consumers_done(1, Duration::from_secs(60));
            Ok(())
        }

        fn $cons() -> Result<(), Box<dyn std::error::Error>> {
            let seg = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
            let buf = read_env_usize("BENCH_BUFFER", 16384);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let coord = BenchmarkCoordination::attach_with_timeout(&seg, Duration::from_secs(30))?;
            let config = SharedMemoryConfig { name: seg, buffer_size: buf, element_size: std::mem::size_of::<$slot>(), create: false };
            let mut consumer = SharedDisruptorBuilder::<$slot>::new(config).build_consumer()?;
            coord.signal_consumer_ready();
            let mut consumed = 0u64;
            let mut start: Option<Instant> = None;
            while consumed < events {
                consumer.process_available(|slot, _| {
                    if start.is_none() { start = Some(Instant::now()); }
                    ($access_fn)(&slot.data[..slot.len as usize]);
                    consumed += 1;
                });
                if consumed < events { std::hint::spin_loop(); }
            }
            let elapsed = start.unwrap().elapsed();
            println!("Throughput: {:.0}", consumed as f64 / elapsed.as_secs_f64());
            coord.signal_consumer_done(consumed as i64);
            Ok(())
        }
    };
}

fn access_rkyv(bytes: &[u8]) {
    let archived = unsafe { rkyv::access_unchecked::<rkyv::Archived<Vec<TestPayload>>>(bytes) };
    let mut sum = 0u64;
    for e in archived.iter() { sum = sum.wrapping_add(e.id.into()); for t in e.token_ids.iter() { sum = sum.wrapping_add(u32::from(*t) as u64); } }
    black_box(sum);
}

fn access_flatbuf(bytes: &[u8]) {
    let root = flatbuffers::root::<flatbench::PayloadBatch>(bytes).unwrap();
    let entries = root.entries().unwrap();
    let mut sum = 0u64;
    for e in entries.iter() { sum = sum.wrapping_add(e.id()); if let Some(tids) = e.token_ids() { for t in tids.iter() { sum = sum.wrapping_add(t as u64); } } }
    black_box(sum);
}

fn access_raw(bytes: &[u8]) {
    black_box(bytes.iter().fold(0u8, |a, &b| a.wrapping_add(b)));
}

// SHM: rkyv at all sizes
shm_nofrag_impl!(Slot2K, encode_rkyv, access_rkyv, rkyv_shm_prod_2k, rkyv_shm_cons_2k);
shm_nofrag_impl!(Slot8K, encode_rkyv, access_rkyv, rkyv_shm_prod_8k, rkyv_shm_cons_8k);
shm_nofrag_impl!(Slot32K, encode_rkyv, access_rkyv, rkyv_shm_prod_32k, rkyv_shm_cons_32k);
shm_nofrag_impl!(Slot128K, encode_rkyv, access_rkyv, rkyv_shm_prod_128k, rkyv_shm_cons_128k);
// SHM: flatbuf at all sizes
shm_nofrag_impl!(Slot2K, encode_flatbuf, access_flatbuf, fb_shm_prod_2k, fb_shm_cons_2k);
shm_nofrag_impl!(Slot8K, encode_flatbuf, access_flatbuf, fb_shm_prod_8k, fb_shm_cons_8k);
shm_nofrag_impl!(Slot32K, encode_flatbuf, access_flatbuf, fb_shm_prod_32k, fb_shm_cons_32k);
shm_nofrag_impl!(Slot128K, encode_flatbuf, access_flatbuf, fb_shm_prod_128k, fb_shm_cons_128k);
// SHM: raw ring at all sizes
macro_rules! raw_shm_impl {
    ($ev:ty, $prod:ident, $cons:ident) => {
        fn $prod() -> Result<(), Box<dyn std::error::Error>> {
            let seg = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
            let buf = read_env_usize("BENCH_BUFFER", 16384);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let mut producer = build_shared_single_producer::<$ev>(&seg, buf)
                .enable_discovery(1).with_coordination(CoordinationMode::Immediate)
                .build_producer(|| <$ev>::default())?;
            let coord = BenchmarkCoordination::create(&seg)?;
            if !coord.wait_for_consumers(1, Duration::from_secs(30)) { return Err("timeout".into()); }
            for _ in 0..20 { let _ = producer.min_gating_sequence(); std::thread::sleep(Duration::from_millis(2)); }
            let start = Instant::now();
            for i in 0..events { producer.publish(|s| { s.sequence = i; s.payload.fill((i & 0xFF) as u8); }); }
            let elapsed = start.elapsed();
            println!("Throughput: {:.0}", events as f64 / elapsed.as_secs_f64());
            coord.signal_producer_done(events as i64);
            coord.wait_for_consumers_done(1, Duration::from_secs(60));
            Ok(())
        }
        fn $cons() -> Result<(), Box<dyn std::error::Error>> {
            let seg = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
            let buf = read_env_usize("BENCH_BUFFER", 16384);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let coord = BenchmarkCoordination::attach_with_timeout(&seg, Duration::from_secs(30))?;
            let config = SharedMemoryConfig { name: seg, buffer_size: buf, element_size: std::mem::size_of::<$ev>(), create: false };
            let mut consumer = SharedDisruptorBuilder::<$ev>::new(config).build_consumer()?;
            coord.signal_consumer_ready();
            let mut consumed = 0u64;
            let mut start: Option<Instant> = None;
            while consumed < events {
                consumer.process_available(|s, _| {
                    if start.is_none() { start = Some(Instant::now()); }
                    access_raw(&s.payload);
                    consumed += 1;
                });
                if consumed < events { std::hint::spin_loop(); }
            }
            let elapsed = start.unwrap().elapsed();
            println!("Throughput: {:.0}", consumed as f64 / elapsed.as_secs_f64());
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
            if !producer.wait_for_consumers_ready(1, Duration::from_secs(30)) { return Err("timeout".into()); }
            let start = Instant::now();
            for i in 0..events { producer.publish(|s| { s.sequence = i; s.payload.fill((i & 0xFF) as u8); }); }
            let elapsed = start.elapsed();
            println!("Throughput: {:.0}", events as f64 / elapsed.as_secs_f64());
            let last = (events - 1) as i64;
            producer.wait_until_consumed_with_strategy(last, Duration::from_secs(60), disruptor_mp::AutoWaitStrategy::BusySpin);
            Ok(())
        }
        fn $cons() -> Result<(), Box<dyn std::error::Error>> {
            let root = env::var("BENCH_ROOT").expect("BENCH_ROOT");
            let seg = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
            let buf = read_env_usize("BENCH_BUFFER", 16384);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let layout = MmapTransportLayout::new(PathBuf::from(&root), seg).expect("layout");
            let cid = format!("c{}", std::process::id());
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut consumer = loop {
                match MmapConsumer::<$ev>::attach(layout.clone(), buf, &cid) {
                    Ok(c) => break c, Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
                    Err(e) => return Err(format!("attach: {e}").into()),
                }
            };
            let mut consumed = 0u64;
            let mut start: Option<Instant> = None;
            while consumed < events {
                if let Some((_, s)) = consumer.try_consume_next() {
                    if start.is_none() { start = Some(Instant::now()); }
                    access_raw(&s.payload);
                    consumed += 1;
                } else { std::hint::spin_loop(); }
            }
            let elapsed = start.unwrap().elapsed();
            println!("Throughput: {:.0}", consumed as f64 / elapsed.as_secs_f64());
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
            if !producer.wait_for_consumers_ready(1, Duration::from_secs(30)) { return Err("timeout".into()); }
            let start = Instant::now();
            for _ in 0..events {
                let enc = $encode_fn(&payloads);
                producer.publish(|slot| { slot.len = enc.len() as u32; slot.data[..enc.len()].copy_from_slice(&enc); });
            }
            let elapsed = start.elapsed();
            println!("Throughput: {:.0}", events as f64 / elapsed.as_secs_f64());
            let last = (events - 1) as i64;
            producer.wait_until_consumed_with_strategy(last, Duration::from_secs(60), disruptor_mp::AutoWaitStrategy::BusySpin);
            Ok(())
        }

        fn $cons() -> Result<(), Box<dyn std::error::Error>> {
            let root = env::var("BENCH_ROOT").expect("BENCH_ROOT");
            let seg = env::var("BENCH_SEGMENT").expect("BENCH_SEGMENT");
            let buf = read_env_usize("BENCH_BUFFER", 16384);
            let events = read_env_u64("BENCH_EVENTS", 100_000);
            let layout = MmapTransportLayout::new(PathBuf::from(&root), seg).expect("layout");
            let cid = format!("c{}", std::process::id());
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut consumer = loop {
                match MmapConsumer::<$slot>::attach(layout.clone(), buf, &cid) {
                    Ok(c) => break c, Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
                    Err(e) => return Err(format!("attach: {e}").into()),
                }
            };
            let mut consumed = 0u64;
            let mut start: Option<Instant> = None;
            while consumed < events {
                if let Some((_, slot)) = consumer.try_consume_next() {
                    if start.is_none() { start = Some(Instant::now()); }
                    ($access_fn)(&slot.data[..slot.len as usize]);
                    consumed += 1;
                } else { std::hint::spin_loop(); }
            }
            let elapsed = start.unwrap().elapsed();
            println!("Throughput: {:.0}", consumed as f64 / elapsed.as_secs_f64());
            Ok(())
        }
    };
}

mmap_nofrag_impl!(Slot2K, encode_rkyv, access_rkyv, rkyv_mmap_prod_2k, rkyv_mmap_cons_2k);
mmap_nofrag_impl!(Slot8K, encode_rkyv, access_rkyv, rkyv_mmap_prod_8k, rkyv_mmap_cons_8k);
mmap_nofrag_impl!(Slot32K, encode_rkyv, access_rkyv, rkyv_mmap_prod_32k, rkyv_mmap_cons_32k);
mmap_nofrag_impl!(Slot128K, encode_rkyv, access_rkyv, rkyv_mmap_prod_128k, rkyv_mmap_cons_128k);
mmap_nofrag_impl!(Slot2K, encode_flatbuf, access_flatbuf, fb_mmap_prod_2k, fb_mmap_cons_2k);
mmap_nofrag_impl!(Slot8K, encode_flatbuf, access_flatbuf, fb_mmap_prod_8k, fb_mmap_cons_8k);
mmap_nofrag_impl!(Slot32K, encode_flatbuf, access_flatbuf, fb_mmap_prod_32k, fb_mmap_cons_32k);
mmap_nofrag_impl!(Slot128K, encode_flatbuf, access_flatbuf, fb_mmap_prod_128k, fb_mmap_cons_128k);

// ============================================================
// Orchestrator
// ============================================================

struct Result2 { layer: &'static str, backend: &'static str, payload_label: String, prod_ops: f64, cons_ops: f64 }

fn run_shm(layer: &'static str, prod_role: &str, cons_role: &str, events: u64, buffer: usize, batch: usize, size_tag: &str) -> Result2 {
    let seg = unique_segment(layer);
    let exe = env::current_exe().expect("exe");
    let envs: Vec<(&str, String)> = vec![("BENCH_SEGMENT", seg.clone()), ("BENCH_EVENTS", events.to_string()), ("BENCH_BUFFER", buffer.to_string()), ("BENCH_BATCH_SIZE", batch.to_string())];
    let producer = spawn_child(&exe, prod_role, &envs);
    let consumer = spawn_child(&exe, cons_role, &envs);
    let timeout = Duration::from_secs(300);
    let cons_out = wait_timeout(consumer, timeout);
    let prod_out = wait_timeout(producer, timeout);
    let prod_ops = prod_out.ok().map(|o| extract_value(&String::from_utf8_lossy(&o.stdout), "Throughput")).unwrap_or(0.0);
    let cons_ops = cons_out.ok().map(|o| extract_value(&String::from_utf8_lossy(&o.stdout), "Throughput")).unwrap_or(0.0);
    Result2 { layer, backend: "shm", payload_label: size_tag.to_string(), prod_ops, cons_ops }
}

fn run_mmap(layer: &'static str, prod_role: &str, cons_role: &str, events: u64, buffer: usize, batch: usize, size_tag: &str) -> Result2 {
    let root = unique_root(layer);
    let seg = format!("nfa_{}", std::process::id() % 10000);
    let root_str = root.display().to_string();
    let exe = env::current_exe().expect("exe");
    let envs: Vec<(&str, String)> = vec![("BENCH_ROOT", root_str.clone()), ("BENCH_SEGMENT", seg.clone()), ("BENCH_EVENTS", events.to_string()), ("BENCH_BUFFER", buffer.to_string()), ("BENCH_BATCH_SIZE", batch.to_string())];
    let producer = spawn_child(&exe, prod_role, &envs);
    let consumer = spawn_child(&exe, cons_role, &envs);
    let timeout = Duration::from_secs(300);
    let cons_out = wait_timeout(consumer, timeout);
    let prod_out = wait_timeout(producer, timeout);
    let _ = std::fs::remove_dir_all(&root);
    let prod_ops = prod_out.ok().map(|o| extract_value(&String::from_utf8_lossy(&o.stdout), "Throughput")).unwrap_or(0.0);
    let cons_ops = cons_out.ok().map(|o| extract_value(&String::from_utf8_lossy(&o.stdout), "Throughput")).unwrap_or(0.0);
    Result2 { layer, backend: "mmap", payload_label: size_tag.to_string(), prod_ops, cons_ops }
}

// ============================================================
// main
// ============================================================

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 {
        let role = &args[1];
        if role.starts_with("--") { /* fall through */ } else {
            let _log = perf_bench::bench_log::BenchLog::default_capacity(role);
            let result = match role.as_str() {
                // SHM raw ring
                "raw_shm_prod_1k" => raw_shm_prod_1k(), "raw_shm_cons_1k" => raw_shm_cons_1k(),
                "raw_shm_prod_4k" => raw_shm_prod_4k(), "raw_shm_cons_4k" => raw_shm_cons_4k(),
                "raw_shm_prod_16k" => raw_shm_prod_16k(), "raw_shm_cons_16k" => raw_shm_cons_16k(),
                "raw_shm_prod_64k" => raw_shm_prod_64k(), "raw_shm_cons_64k" => raw_shm_cons_64k(),
                // mmap raw ring
                "raw_mmap_prod_1k" => raw_mmap_prod_1k(), "raw_mmap_cons_1k" => raw_mmap_cons_1k(),
                "raw_mmap_prod_4k" => raw_mmap_prod_4k(), "raw_mmap_cons_4k" => raw_mmap_cons_4k(),
                "raw_mmap_prod_16k" => raw_mmap_prod_16k(), "raw_mmap_cons_16k" => raw_mmap_cons_16k(),
                "raw_mmap_prod_64k" => raw_mmap_prod_64k(), "raw_mmap_cons_64k" => raw_mmap_cons_64k(),
                // SHM rkyv
                "rkyv_shm_prod_2k" => rkyv_shm_prod_2k(), "rkyv_shm_cons_2k" => rkyv_shm_cons_2k(),
                "rkyv_shm_prod_8k" => rkyv_shm_prod_8k(), "rkyv_shm_cons_8k" => rkyv_shm_cons_8k(),
                "rkyv_shm_prod_32k" => rkyv_shm_prod_32k(), "rkyv_shm_cons_32k" => rkyv_shm_cons_32k(),
                "rkyv_shm_prod_128k" => rkyv_shm_prod_128k(), "rkyv_shm_cons_128k" => rkyv_shm_cons_128k(),
                // SHM flatbuf
                "fb_shm_prod_2k" => fb_shm_prod_2k(), "fb_shm_cons_2k" => fb_shm_cons_2k(),
                "fb_shm_prod_8k" => fb_shm_prod_8k(), "fb_shm_cons_8k" => fb_shm_cons_8k(),
                "fb_shm_prod_32k" => fb_shm_prod_32k(), "fb_shm_cons_32k" => fb_shm_cons_32k(),
                "fb_shm_prod_128k" => fb_shm_prod_128k(), "fb_shm_cons_128k" => fb_shm_cons_128k(),
                // mmap rkyv
                "rkyv_mmap_prod_2k" => rkyv_mmap_prod_2k(), "rkyv_mmap_cons_2k" => rkyv_mmap_cons_2k(),
                "rkyv_mmap_prod_8k" => rkyv_mmap_prod_8k(), "rkyv_mmap_cons_8k" => rkyv_mmap_cons_8k(),
                "rkyv_mmap_prod_32k" => rkyv_mmap_prod_32k(), "rkyv_mmap_cons_32k" => rkyv_mmap_cons_32k(),
                "rkyv_mmap_prod_128k" => rkyv_mmap_prod_128k(), "rkyv_mmap_cons_128k" => rkyv_mmap_cons_128k(),
                // mmap flatbuf
                "fb_mmap_prod_2k" => fb_mmap_prod_2k(), "fb_mmap_cons_2k" => fb_mmap_cons_2k(),
                "fb_mmap_prod_8k" => fb_mmap_prod_8k(), "fb_mmap_cons_8k" => fb_mmap_cons_8k(),
                "fb_mmap_prod_32k" => fb_mmap_prod_32k(), "fb_mmap_cons_32k" => fb_mmap_cons_32k(),
                "fb_mmap_prod_128k" => fb_mmap_prod_128k(), "fb_mmap_cons_128k" => fb_mmap_cons_128k(),
                _ => Ok(()),
            };
            if let Err(e) = result { eprintln!("{role} failed: {e}"); std::process::exit(1); }
            return;
        }
    }

    let _log = perf_bench::bench_log::BenchLog::default_capacity("nofrag_all");

    struct SizeConfig {
        tag: &'static str, batch: usize, events: u64, buffer: usize,
        raw_shm_prod: &'static str, raw_shm_cons: &'static str,
        raw_mmap_prod: &'static str, raw_mmap_cons: &'static str,
        rkyv_shm_prod: &'static str, rkyv_shm_cons: &'static str,
        fb_shm_prod: &'static str, fb_shm_cons: &'static str,
        rkyv_mmap_prod: &'static str, rkyv_mmap_cons: &'static str,
        fb_mmap_prod: &'static str, fb_mmap_cons: &'static str,
    }

    let sizes = [
        SizeConfig { tag: "1KB", batch: 2, events: 200_000, buffer: 16_384,
            raw_shm_prod: "raw_shm_prod_1k", raw_shm_cons: "raw_shm_cons_1k",
            raw_mmap_prod: "raw_mmap_prod_1k", raw_mmap_cons: "raw_mmap_cons_1k",
            rkyv_shm_prod: "rkyv_shm_prod_2k", rkyv_shm_cons: "rkyv_shm_cons_2k",
            fb_shm_prod: "fb_shm_prod_2k", fb_shm_cons: "fb_shm_cons_2k",
            rkyv_mmap_prod: "rkyv_mmap_prod_2k", rkyv_mmap_cons: "rkyv_mmap_cons_2k",
            fb_mmap_prod: "fb_mmap_prod_2k", fb_mmap_cons: "fb_mmap_cons_2k",
        },
        SizeConfig { tag: "4KB", batch: 8, events: 100_000, buffer: 16_384,
            raw_shm_prod: "raw_shm_prod_4k", raw_shm_cons: "raw_shm_cons_4k",
            raw_mmap_prod: "raw_mmap_prod_4k", raw_mmap_cons: "raw_mmap_cons_4k",
            rkyv_shm_prod: "rkyv_shm_prod_8k", rkyv_shm_cons: "rkyv_shm_cons_8k",
            fb_shm_prod: "fb_shm_prod_8k", fb_shm_cons: "fb_shm_cons_8k",
            rkyv_mmap_prod: "rkyv_mmap_prod_8k", rkyv_mmap_cons: "rkyv_mmap_cons_8k",
            fb_mmap_prod: "fb_mmap_prod_8k", fb_mmap_cons: "fb_mmap_cons_8k",
        },
        SizeConfig { tag: "16KB", batch: 28, events: 50_000, buffer: 16_384,
            raw_shm_prod: "raw_shm_prod_16k", raw_shm_cons: "raw_shm_cons_16k",
            raw_mmap_prod: "raw_mmap_prod_16k", raw_mmap_cons: "raw_mmap_cons_16k",
            rkyv_shm_prod: "rkyv_shm_prod_32k", rkyv_shm_cons: "rkyv_shm_cons_32k",
            fb_shm_prod: "fb_shm_prod_32k", fb_shm_cons: "fb_shm_cons_32k",
            rkyv_mmap_prod: "rkyv_mmap_prod_32k", rkyv_mmap_cons: "rkyv_mmap_cons_32k",
            fb_mmap_prod: "fb_mmap_prod_32k", fb_mmap_cons: "fb_mmap_cons_32k",
        },
        SizeConfig { tag: "64KB", batch: 110, events: 20_000, buffer: 16_384,
            raw_shm_prod: "raw_shm_prod_64k", raw_shm_cons: "raw_shm_cons_64k",
            raw_mmap_prod: "raw_mmap_prod_64k", raw_mmap_cons: "raw_mmap_cons_64k",
            rkyv_shm_prod: "rkyv_shm_prod_128k", rkyv_shm_cons: "rkyv_shm_cons_128k",
            fb_shm_prod: "fb_shm_prod_128k", fb_shm_cons: "fb_shm_cons_128k",
            rkyv_mmap_prod: "rkyv_mmap_prod_128k", rkyv_mmap_cons: "rkyv_mmap_cons_128k",
            fb_mmap_prod: "fb_mmap_prod_128k", fb_mmap_cons: "fb_mmap_cons_128k",
        },
    ];

    println!("=== No-Frag Zero-Copy Complete Matrix ===");
    println!("3 layers (raw_ring, rkyv, flatbuf) x 2 backends (SHM, mmap) x 4 sizes = 24 scenarios");
    println!("All consumers read ALL field data (fair comparison)");
    println!();

    let mut results = Vec::new();
    let mut raw_baselines: std::collections::HashMap<String, f64> = std::collections::HashMap::new();

    for s in &sizes {
        println!("--- {} (batch={}) ---", s.tag, s.batch);

        let r = run_shm("raw_ring", s.raw_shm_prod, s.raw_shm_cons, s.events, s.buffer, s.batch, s.tag);
        println!("  raw_ring     SHM   prod={:>10} cons={:>10}", format_throughput(r.prod_ops), format_throughput(r.cons_ops));
        raw_baselines.insert(format!("{}_shm", s.tag), r.cons_ops);
        results.push(r);

        let r = run_mmap("raw_ring", s.raw_mmap_prod, s.raw_mmap_cons, s.events, s.buffer, s.batch, s.tag);
        println!("  raw_ring     mmap  prod={:>10} cons={:>10}", format_throughput(r.prod_ops), format_throughput(r.cons_ops));
        raw_baselines.insert(format!("{}_mmap", s.tag), r.cons_ops);
        results.push(r);

        let r = run_shm("rkyv_nofrag", s.rkyv_shm_prod, s.rkyv_shm_cons, s.events, s.buffer, s.batch, s.tag);
        println!("  rkyv_nf      SHM   prod={:>10} cons={:>10}", format_throughput(r.prod_ops), format_throughput(r.cons_ops));
        results.push(r);

        let r = run_shm("flatbuf_nf", s.fb_shm_prod, s.fb_shm_cons, s.events, s.buffer, s.batch, s.tag);
        println!("  flatbuf_nf   SHM   prod={:>10} cons={:>10}", format_throughput(r.prod_ops), format_throughput(r.cons_ops));
        results.push(r);

        let r = run_mmap("rkyv_nofrag", s.rkyv_mmap_prod, s.rkyv_mmap_cons, s.events, s.buffer, s.batch, s.tag);
        println!("  rkyv_nf      mmap  prod={:>10} cons={:>10}", format_throughput(r.prod_ops), format_throughput(r.cons_ops));
        results.push(r);

        let r = run_mmap("flatbuf_nf", s.fb_mmap_prod, s.fb_mmap_cons, s.events, s.buffer, s.batch, s.tag);
        println!("  flatbuf_nf   mmap  prod={:>10} cons={:>10}", format_throughput(r.prod_ops), format_throughput(r.cons_ops));
        results.push(r);
    }

    // Summary table
    #[derive(Tabled)]
    struct Row {
        #[tabled(rename = "Size")] size: String,
        #[tabled(rename = "Layer")] layer: String,
        #[tabled(rename = "Backend")] backend: String,
        #[tabled(rename = "Producer\n(ops/s)")] prod: String,
        #[tabled(rename = "Consumer\n(ops/s)")] cons: String,
        #[tabled(rename = "% of Raw\nRing")] pct: String,
    }

    let rows: Vec<Row> = results.iter().map(|r| {
        let size = &r.payload_label;
        let baseline_key = format!("{}_{}", size, r.backend);
        let baseline = raw_baselines.get(&baseline_key).copied().unwrap_or(1.0);
        Row {
            size: size.clone(), layer: r.layer.to_string(), backend: r.backend.to_string(),
            prod: format_throughput(r.prod_ops), cons: format_throughput(r.cons_ops),
            pct: format!("{:.0}%", r.cons_ops / baseline * 100.0),
        }
    }).collect();

    println!("\n{}", Table::new(rows).with(Style::modern()));
}
