//! DST-style real multiprocess typed transport checks for codec and zero-copy paths.

use dst_fixtures::dst_buggify::ScopedBuggify;
use dst_runner::{BackendKind, CodecKind, DstConfig};
use myelon::codec::{Codec, CodecError};
use myelon::transport::{FixedFrame, MyelonWaitStrategy, ReassemblyBuffer};
use myelon::{MmapTypedConsumer, MmapTypedProducer, TypedConsumer, TypedProducer};
use serde::{Deserialize, Serialize};
use std::env;
use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const CHILD_TEST_NAME: &str = "dst_codec_child";
const DEPTH: usize = 256;
const EQ_FRAME_DATA: usize = 8 * 1024;
const FRAG_FRAME_DATA: usize = 256;
static MMAP_COUNTER: AtomicUsize = AtomicUsize::new(0);

type EqFrame = FixedFrame<EQ_FRAME_DATA>;
type FragFrame = FixedFrame<FRAG_FRAME_DATA>;

#[cfg(feature = "flatbuffers")]
#[allow(
    clippy::all,
    elided_lifetimes_in_paths,
    explicit_outlives_requirements,
    unused_extern_crates
)]
#[path = "../../perf-bench/src/generated/bench_payload_generated.rs"]
mod bench_payload_generated;

#[cfg(feature = "flatbuffers")]
use bench_payload_generated::myelon::bench as flatbench;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
struct TestPayload {
    id: u64,
    token_ids: Vec<u32>,
    block_table: Vec<u32>,
    temperature: f32,
    label: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct CodecChildResult {
    owned_checksum: u64,
    leased_checksum: Option<u64>,
    messages: usize,
}

fn segment_name(tag: &str) -> String {
    if let Ok(name) = env::var("DST_CODEC_SEGMENT") {
        return name;
    }
    disruptor_mp::portable_shm_segment_name(tag)
}

fn mmap_case(tag: &str) -> (PathBuf, String, disruptor_mp::MmapTransportLayout) {
    let pid = std::process::id();
    let seq = MMAP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!("dst_codec_mmap_{pid}_{seq}"));
    let segment = format!("{tag}_{pid}_{seq}");
    let layout = disruptor_mp::MmapTransportLayout::new(root.clone(), segment.clone())
        .expect("valid mmap transport layout");
    (root, segment, layout)
}

fn make_payloads(seed: u64, count: usize, token_count: usize) -> Vec<TestPayload> {
    (0..count)
        .map(|index| TestPayload {
            id: seed.wrapping_mul(10_000).wrapping_add(index as u64),
            token_ids: (0..token_count)
                .map(|token| {
                    ((token as u32).wrapping_mul(31))
                        .wrapping_add(index as u32)
                        .wrapping_add(seed as u32)
                })
                .collect(),
            block_table: (0..8)
                .map(|slot| seed as u32 + (index as u32 * 8) + slot as u32)
                .collect(),
            temperature: 0.25 + ((seed % 10) as f32 * 0.05),
            label: format!("dst_seq_{seed}_{index}"),
        })
        .collect()
}

fn checksum_payloads(payloads: &[TestPayload]) -> u64 {
    payloads.iter().fold(0u64, |sum, payload| {
        let token_sum = payload
            .token_ids
            .iter()
            .fold(0u64, |acc, value| acc.wrapping_add(*value as u64));
        let block_sum = payload
            .block_table
            .iter()
            .fold(0u64, |acc, value| acc.wrapping_add(*value as u64));
        let label_sum = payload
            .label
            .bytes()
            .fold(0u64, |acc, value| acc.wrapping_add(value as u64));
        sum.wrapping_add(payload.id)
            .wrapping_add(token_sum)
            .wrapping_add(block_sum)
            .wrapping_add(payload.temperature.to_bits() as u64)
            .wrapping_add(label_sum)
    })
}

fn assert_child_success(output: &Output, label: &str) -> CodecChildResult {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "{label} failed with status {:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status.code()
    );

    let json_line = stdout
        .lines()
        .find(|line| line.starts_with("DST_CODEC_OK "))
        .unwrap_or_else(|| {
            panic!("{label} missing success marker\nstdout:\n{stdout}\nstderr:\n{stderr}")
        });
    serde_json::from_str(json_line.trim_start_matches("DST_CODEC_OK ")).unwrap_or_else(|err| {
        panic!("{label} invalid child json: {err}\nstdout:\n{stdout}\nstderr:\n{stderr}")
    })
}

fn assert_child_alive(child: &mut std::process::Child, label: &str) {
    if let Some(status) = child.try_wait().expect("poll codec child") {
        let mut stdout = String::new();
        let mut stderr = String::new();
        if let Some(mut pipe) = child.stdout.take() {
            let _ = pipe.read_to_string(&mut stdout);
        }
        if let Some(mut pipe) = child.stderr.take() {
            let _ = pipe.read_to_string(&mut stderr);
        }
        panic!(
            "{label} exited early with status {:?}\nstdout:\n{}\nstderr:\n{}",
            status.code(),
            stdout,
            stderr
        );
    }
}

fn spawn_codec_child(
    case: &str,
    codec: &str,
    segment: &str,
    batch_count: usize,
) -> std::process::Child {
    spawn_codec_child_custom(
        case,
        codec,
        "shm",
        None,
        segment,
        None,
        batch_count,
        None,
        0,
    )
}

fn spawn_codec_child_mmap(
    case: &str,
    codec: &str,
    root: &std::path::Path,
    segment: &str,
    consumer_id: &str,
    batch_count: usize,
) -> std::process::Child {
    spawn_codec_child_custom(
        case,
        codec,
        "mmap",
        Some(root),
        segment,
        Some(consumer_id),
        batch_count,
        None,
        0,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "DST codec test helper keeps child-process knobs explicit"
)]
fn spawn_codec_child_custom(
    case: &str,
    codec: &str,
    backend: &str,
    root: Option<&std::path::Path>,
    segment: &str,
    consumer_id: Option<&str>,
    batch_count: usize,
    seed_base: Option<u64>,
    start_sequence: usize,
) -> std::process::Child {
    let mut cmd = Command::new(env::current_exe().expect("exe"));
    cmd.arg("--exact")
        .arg(CHILD_TEST_NAME)
        .arg("--ignored")
        .arg("--nocapture")
        .env("DST_CODEC_CASE", case)
        .env("DST_CODEC_CODEC", codec)
        .env("DST_CODEC_BACKEND", backend)
        .env("DST_CODEC_SEGMENT", segment)
        .env("DST_CODEC_DEPTH", DEPTH.to_string())
        .env("DST_CODEC_BATCH_COUNT", batch_count.to_string())
        .env("DST_CODEC_START_SEQUENCE", start_sequence.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(root) = root {
        cmd.env("DST_CODEC_ROOT", root);
    }
    if let Some(consumer_id) = consumer_id {
        cmd.env("DST_CODEC_CONSUMER_ID", consumer_id);
    }
    if let Some(seed_base) = seed_base {
        cmd.env("DST_CODEC_SEED_BASE", seed_base.to_string());
    }
    cmd.spawn().expect("spawn dst codec child")
}

fn run_codec_fuzz_seed(seed: u64, nightly: bool) {
    let _buggify = ScopedBuggify::new(seed);
    let config = DstConfig::from_seed(seed);
    let codec = config.codec.unwrap_or(CodecKind::Bincode);
    let batch_count = (config.message_count.min(if nightly { 8 } else { 4 }) as usize).max(2);
    let seed_base = 0x4000 + seed * 16;

    match (config.backend, codec, config.zero_copy) {
        #[cfg(feature = "rkyv")]
        (BackendKind::Shm, CodecKind::Rkyv, true) => {
            let segment = segment_name("dzfzr");
            let payloads = make_payloads(seed_base, 4, 32);
            let expected_checksum = checksum_payloads(&payloads);
            let mut producer =
                TypedProducer::<EqFrame>::create(&segment, DEPTH).expect("create producer");
            let child = spawn_codec_child_custom(
                "equivalence",
                "rkyv",
                "shm",
                None,
                &segment,
                None,
                payloads.len(),
                Some(seed_base),
                0,
            );
            std::thread::sleep(Duration::from_millis(200));
            producer.discover_consumers(Duration::from_secs(3));
            producer
                .publish(&RkyvBatch(payloads.clone()), 11)
                .expect("publish owned");
            producer
                .publish(&RkyvBatch(payloads), 12)
                .expect("publish leased");
            let output = child.wait_with_output().expect("wait rkyv shm fuzz child");
            let result = assert_child_success(&output, "rkyv shm fuzz child");
            assert_eq!(result.owned_checksum, expected_checksum);
            assert_eq!(result.leased_checksum, Some(expected_checksum));
        }
        #[cfg(feature = "rkyv")]
        (BackendKind::Mmap, CodecKind::Rkyv, true) => {
            let (_root, segment, layout) = mmap_case("dzfzrm");
            let payloads = make_payloads(seed_base, 4, 32);
            let expected_checksum = checksum_payloads(&payloads);
            let mut producer = MmapTypedProducer::<EqFrame>::create(layout.clone(), DEPTH)
                .expect("create mmap producer");
            let child = spawn_codec_child_custom(
                "equivalence",
                "rkyv",
                "mmap",
                Some(layout.root_dir()),
                &segment,
                Some("dst_codec_fuzz_rkyv"),
                payloads.len(),
                Some(seed_base),
                0,
            );
            assert!(
                producer
                    .raw()
                    .wait_for_consumers_ready(1, Duration::from_secs(3)),
                "mmap rkyv fuzz producer timed out waiting for consumer"
            );
            producer
                .publish(&RkyvBatch(payloads.clone()), 11)
                .expect("publish owned");
            producer
                .publish(&RkyvBatch(payloads), 12)
                .expect("publish leased");
            let output = child.wait_with_output().expect("wait rkyv mmap fuzz child");
            let result = assert_child_success(&output, "rkyv mmap fuzz child");
            assert_eq!(result.owned_checksum, expected_checksum);
            assert_eq!(result.leased_checksum, Some(expected_checksum));
        }
        #[cfg(feature = "flatbuffers")]
        (BackendKind::Shm, CodecKind::Flatbuf, true) => {
            let segment = segment_name("dzfzf");
            let payloads = make_payloads(seed_base, 4, 32);
            let expected_checksum = checksum_payloads(&payloads);
            let mut producer =
                TypedProducer::<EqFrame>::create(&segment, DEPTH).expect("create producer");
            let child = spawn_codec_child_custom(
                "equivalence",
                "flatbuf",
                "shm",
                None,
                &segment,
                None,
                payloads.len(),
                Some(seed_base),
                0,
            );
            std::thread::sleep(Duration::from_millis(200));
            producer.discover_consumers(Duration::from_secs(3));
            producer
                .publish(&FlatbufBatch(payloads.clone()), 21)
                .expect("publish owned");
            producer
                .publish(&FlatbufBatch(payloads), 22)
                .expect("publish leased");
            let output = child
                .wait_with_output()
                .expect("wait flatbuf shm fuzz child");
            let result = assert_child_success(&output, "flatbuf shm fuzz child");
            assert_eq!(result.owned_checksum, expected_checksum);
            assert_eq!(result.leased_checksum, Some(expected_checksum));
        }
        #[cfg(feature = "flatbuffers")]
        (BackendKind::Mmap, CodecKind::Flatbuf, true) => {
            let (_root, segment, layout) = mmap_case("dzfzfm");
            let payloads = make_payloads(seed_base, 4, 32);
            let expected_checksum = checksum_payloads(&payloads);
            let mut producer = MmapTypedProducer::<EqFrame>::create(layout.clone(), DEPTH)
                .expect("create mmap producer");
            let child = spawn_codec_child_custom(
                "equivalence",
                "flatbuf",
                "mmap",
                Some(layout.root_dir()),
                &segment,
                Some("dst_codec_fuzz_flatbuf"),
                payloads.len(),
                Some(seed_base),
                0,
            );
            assert!(
                producer
                    .raw()
                    .wait_for_consumers_ready(1, Duration::from_secs(3)),
                "mmap flatbuf fuzz producer timed out waiting for consumer"
            );
            producer
                .publish(&FlatbufBatch(payloads.clone()), 21)
                .expect("publish owned");
            producer
                .publish(&FlatbufBatch(payloads), 22)
                .expect("publish leased");
            let output = child
                .wait_with_output()
                .expect("wait flatbuf mmap fuzz child");
            let result = assert_child_success(&output, "flatbuf mmap fuzz child");
            assert_eq!(result.owned_checksum, expected_checksum);
            assert_eq!(result.leased_checksum, Some(expected_checksum));
        }
        (BackendKind::Shm, codec_kind, _) => {
            run_codec_roundtrip_fuzz_shm(codec_kind, batch_count, seed_base);
        }
        (BackendKind::Mmap, codec_kind, _) => {
            run_codec_roundtrip_fuzz_mmap(codec_kind, batch_count, seed_base);
        }
    }
}

fn run_codec_roundtrip_fuzz_shm(codec: CodecKind, batch_count: usize, seed_base: u64) {
    let segment = segment_name("dczf");
    match codec {
        #[cfg(feature = "rkyv")]
        CodecKind::Rkyv => {
            let mut producer =
                TypedProducer::<FragFrame>::create(&segment, DEPTH).expect("create producer");
            let child = spawn_codec_child_custom(
                "roundtrip",
                "rkyv",
                "shm",
                None,
                &segment,
                None,
                batch_count,
                Some(seed_base),
                0,
            );
            std::thread::sleep(Duration::from_millis(200));
            producer.discover_consumers(Duration::from_secs(3));
            for sequence in 0..batch_count as u64 {
                producer
                    .publish(
                        &RkyvBatch(make_payloads(seed_base + sequence, 6, 96)),
                        (sequence % 251) as u8,
                    )
                    .expect("publish rkyv");
            }
            let output = child
                .wait_with_output()
                .expect("wait rkyv shm roundtrip fuzz child");
            let result = assert_child_success(&output, "rkyv shm roundtrip fuzz child");
            assert_eq!(result.messages, batch_count);
        }
        #[cfg(feature = "flatbuffers")]
        CodecKind::Flatbuf => {
            let mut producer =
                TypedProducer::<FragFrame>::create(&segment, DEPTH).expect("create producer");
            let child = spawn_codec_child_custom(
                "roundtrip",
                "flatbuf",
                "shm",
                None,
                &segment,
                None,
                batch_count,
                Some(seed_base),
                0,
            );
            std::thread::sleep(Duration::from_millis(200));
            producer.discover_consumers(Duration::from_secs(3));
            for sequence in 0..batch_count as u64 {
                producer
                    .publish(
                        &FlatbufBatch(make_payloads(seed_base + sequence, 6, 96)),
                        (sequence % 251) as u8,
                    )
                    .expect("publish flatbuf");
            }
            let output = child
                .wait_with_output()
                .expect("wait flatbuf shm roundtrip fuzz child");
            let result = assert_child_success(&output, "flatbuf shm roundtrip fuzz child");
            assert_eq!(result.messages, batch_count);
        }
        CodecKind::Bincode => {
            let mut producer =
                TypedProducer::<FragFrame>::create(&segment, DEPTH).expect("create producer");
            let child = spawn_codec_child_custom(
                "roundtrip",
                "bincode",
                "shm",
                None,
                &segment,
                None,
                batch_count,
                Some(seed_base),
                0,
            );
            std::thread::sleep(Duration::from_millis(200));
            producer.discover_consumers(Duration::from_secs(3));
            for sequence in 0..batch_count as u64 {
                producer
                    .publish(
                        &BincodeBatch(make_payloads(seed_base + sequence, 6, 96)),
                        (sequence % 251) as u8,
                    )
                    .expect("publish bincode");
            }
            let output = child
                .wait_with_output()
                .expect("wait bincode shm roundtrip fuzz child");
            let result = assert_child_success(&output, "bincode shm roundtrip fuzz child");
            assert_eq!(result.messages, batch_count);
        }
        #[cfg(not(all(feature = "rkyv", feature = "flatbuffers")))]
        other => {
            panic!("codec {other:?} requires its corresponding cargo feature (rkyv / flatbuffers)")
        }
    }
}

fn run_codec_roundtrip_fuzz_mmap(codec: CodecKind, batch_count: usize, seed_base: u64) {
    let (_root, segment, layout) = mmap_case("dczfm");
    match codec {
        #[cfg(feature = "rkyv")]
        CodecKind::Rkyv => {
            let mut producer = MmapTypedProducer::<FragFrame>::create(layout.clone(), DEPTH)
                .expect("create mmap producer");
            let child = spawn_codec_child_custom(
                "roundtrip",
                "rkyv",
                "mmap",
                Some(layout.root_dir()),
                &segment,
                Some("dst_codec_fuzz_rkyv_rt"),
                batch_count,
                Some(seed_base),
                0,
            );
            assert!(
                producer
                    .raw()
                    .wait_for_consumers_ready(1, Duration::from_secs(3)),
                "mmap rkyv roundtrip fuzz producer timed out waiting for consumer"
            );
            for sequence in 0..batch_count as u64 {
                producer
                    .publish(
                        &RkyvBatch(make_payloads(seed_base + sequence, 6, 96)),
                        (sequence % 251) as u8,
                    )
                    .expect("publish rkyv");
            }
            let output = child
                .wait_with_output()
                .expect("wait rkyv mmap roundtrip fuzz child");
            let result = assert_child_success(&output, "rkyv mmap roundtrip fuzz child");
            assert_eq!(result.messages, batch_count);
        }
        #[cfg(feature = "flatbuffers")]
        CodecKind::Flatbuf => {
            let mut producer = MmapTypedProducer::<FragFrame>::create(layout.clone(), DEPTH)
                .expect("create mmap producer");
            let child = spawn_codec_child_custom(
                "roundtrip",
                "flatbuf",
                "mmap",
                Some(layout.root_dir()),
                &segment,
                Some("dst_codec_fuzz_flatbuf_rt"),
                batch_count,
                Some(seed_base),
                0,
            );
            assert!(
                producer
                    .raw()
                    .wait_for_consumers_ready(1, Duration::from_secs(3)),
                "mmap flatbuf roundtrip fuzz producer timed out waiting for consumer"
            );
            for sequence in 0..batch_count as u64 {
                producer
                    .publish(
                        &FlatbufBatch(make_payloads(seed_base + sequence, 6, 96)),
                        (sequence % 251) as u8,
                    )
                    .expect("publish flatbuf");
            }
            let output = child
                .wait_with_output()
                .expect("wait flatbuf mmap roundtrip fuzz child");
            let result = assert_child_success(&output, "flatbuf mmap roundtrip fuzz child");
            assert_eq!(result.messages, batch_count);
        }
        CodecKind::Bincode => {
            let mut producer = MmapTypedProducer::<FragFrame>::create(layout.clone(), DEPTH)
                .expect("create mmap producer");
            let child = spawn_codec_child_custom(
                "roundtrip",
                "bincode",
                "mmap",
                Some(layout.root_dir()),
                &segment,
                Some("dst_codec_fuzz_bincode_rt"),
                batch_count,
                Some(seed_base),
                0,
            );
            assert!(
                producer
                    .raw()
                    .wait_for_consumers_ready(1, Duration::from_secs(3)),
                "mmap bincode roundtrip fuzz producer timed out waiting for consumer"
            );
            for sequence in 0..batch_count as u64 {
                producer
                    .publish(
                        &BincodeBatch(make_payloads(seed_base + sequence, 6, 96)),
                        (sequence % 251) as u8,
                    )
                    .expect("publish bincode");
            }
            let output = child
                .wait_with_output()
                .expect("wait bincode mmap roundtrip fuzz child");
            let result = assert_child_success(&output, "bincode mmap roundtrip fuzz child");
            assert_eq!(result.messages, batch_count);
        }
        #[cfg(not(all(feature = "rkyv", feature = "flatbuffers")))]
        other => {
            panic!("codec {other:?} requires its corresponding cargo feature (rkyv / flatbuffers)")
        }
    }
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

#[cfg(feature = "rkyv")]
type ArchivedPayloadBatch = rkyv::Archived<Vec<TestPayload>>;

#[cfg(feature = "rkyv")]
struct RkyvBatch(Vec<TestPayload>);

#[cfg(feature = "rkyv")]
impl Codec for RkyvBatch {
    type Encoded = rkyv::util::AlignedVec;

    fn encode(&self) -> Result<Self::Encoded, CodecError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(&self.0).map_err(CodecError::encode)
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let archived = rkyv::access::<ArchivedPayloadBatch, rkyv::rancor::Error>(bytes)
            .map_err(CodecError::decode)?;
        let owned = rkyv::deserialize::<Vec<TestPayload>, rkyv::rancor::Error>(archived)
            .map_err(CodecError::decode)?;
        Ok(RkyvBatch(owned))
    }
}

#[cfg(feature = "rkyv")]
impl myelon::codec::ZeroCopyCodec for RkyvBatch {
    type Archived<'a> = &'a ArchivedPayloadBatch;

    fn access<'a>(bytes: &'a [u8]) -> Result<Self::Archived<'a>, CodecError> {
        rkyv::access::<ArchivedPayloadBatch, rkyv::rancor::Error>(bytes).map_err(CodecError::decode)
    }
}

#[cfg(feature = "rkyv")]
fn checksum_archived_rkyv(batch: &ArchivedPayloadBatch) -> u64 {
    let mut sum = 0u64;
    for payload in batch.iter() {
        sum = sum.wrapping_add(payload.id.into());
        for token in payload.token_ids.iter() {
            sum = sum.wrapping_add(u32::from(*token) as u64);
        }
        for block in payload.block_table.iter() {
            sum = sum.wrapping_add(u32::from(*block) as u64);
        }
        let temperature: f32 = payload.temperature.into();
        sum = sum.wrapping_add(temperature.to_bits() as u64);
        for byte in payload.label.as_bytes().iter() {
            sum = sum.wrapping_add((*byte) as u64);
        }
    }
    sum
}

#[cfg(feature = "flatbuffers")]
struct FlatbufBatch(Vec<TestPayload>);

#[cfg(feature = "flatbuffers")]
impl Codec for FlatbufBatch {
    type Encoded = Vec<u8>;

    fn encode(&self) -> Result<Self::Encoded, CodecError> {
        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(32 * 1024);
        let mut entries = Vec::with_capacity(self.0.len());
        for payload in &self.0 {
            let token_ids = builder.create_vector(&payload.token_ids);
            let block_table = builder.create_vector(&payload.block_table);
            let label = builder.create_string(&payload.label);
            entries.push(flatbench::TestPayload::create(
                &mut builder,
                &flatbench::TestPayloadArgs {
                    id: payload.id,
                    token_ids: Some(token_ids),
                    block_table: Some(block_table),
                    temperature: payload.temperature,
                    label: Some(label),
                },
            ));
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
            flatbuffers::root::<flatbench::PayloadBatch<'_>>(bytes).map_err(CodecError::decode)?;
        let entries = root
            .entries()
            .ok_or_else(|| CodecError::decode("missing flatbuffer entries"))?;
        let mut decoded = Vec::with_capacity(entries.len());
        for entry in entries.iter() {
            decoded.push(TestPayload {
                id: entry.id(),
                token_ids: entry
                    .token_ids()
                    .map(|items| items.iter().collect())
                    .unwrap_or_default(),
                block_table: entry
                    .block_table()
                    .map(|items| items.iter().collect())
                    .unwrap_or_default(),
                temperature: entry.temperature(),
                label: entry.label().unwrap_or_default().to_string(),
            });
        }
        Ok(FlatbufBatch(decoded))
    }
}

#[cfg(feature = "flatbuffers")]
impl myelon::codec::ZeroCopyCodec for FlatbufBatch {
    type Archived<'a> = flatbench::PayloadBatch<'a>;

    fn access<'a>(bytes: &'a [u8]) -> Result<Self::Archived<'a>, CodecError> {
        flatbench::root_as_payload_batch(bytes).map_err(CodecError::decode)
    }
}

#[cfg(feature = "flatbuffers")]
fn checksum_flatbuf_root(root: flatbench::PayloadBatch<'_>) -> u64 {
    let mut sum = 0u64;
    let entries = root.entries().expect("flatbuffer entries");
    for payload in entries.iter() {
        sum = sum.wrapping_add(payload.id());
        if let Some(token_ids) = payload.token_ids() {
            for token in token_ids.iter() {
                sum = sum.wrapping_add(token as u64);
            }
        }
        if let Some(block_table) = payload.block_table() {
            for block in block_table.iter() {
                sum = sum.wrapping_add(block as u64);
            }
        }
        sum = sum.wrapping_add(payload.temperature().to_bits() as u64);
        if let Some(label) = payload.label() {
            for byte in label.bytes() {
                sum = sum.wrapping_add(byte as u64);
            }
        }
    }
    sum
}

#[cfg(feature = "rkyv")]
#[test]
fn dst_zero_copy_equivalence_rkyv_shm() {
    let segment = segment_name("dzcr");
    let payloads = make_payloads(0x2201, 4, 32);
    let expected_checksum = checksum_payloads(&payloads);
    let mut producer = TypedProducer::<EqFrame>::create(&segment, DEPTH).expect("create producer");
    let child = spawn_codec_child("equivalence", "rkyv", &segment, payloads.len());

    std::thread::sleep(Duration::from_millis(400));
    producer.discover_consumers(Duration::from_secs(3));
    producer
        .publish(&RkyvBatch(payloads.clone()), 11)
        .expect("publish owned");
    producer
        .publish(&RkyvBatch(payloads.clone()), 12)
        .expect("publish leased");

    let output = child.wait_with_output().expect("wait for codec child");
    let result = assert_child_success(&output, "dst rkyv equivalence child");
    assert_eq!(result.messages, payloads.len());
    assert_eq!(result.owned_checksum, expected_checksum);
    assert_eq!(result.leased_checksum, Some(expected_checksum));
}

#[cfg(feature = "rkyv")]
#[test]
fn dst_zero_copy_equivalence_rkyv_mmap() {
    let (_root, segment, layout) = mmap_case("dzcrm");
    let consumer_id = "dst_codec_eq_rkyv";
    let payloads = make_payloads(0x2203, 4, 32);
    let expected_checksum = checksum_payloads(&payloads);
    let mut producer =
        MmapTypedProducer::<EqFrame>::create(layout.clone(), DEPTH).expect("create mmap producer");
    let child = spawn_codec_child_mmap(
        "equivalence",
        "rkyv",
        layout.root_dir(),
        &segment,
        consumer_id,
        payloads.len(),
    );

    assert!(
        producer
            .raw()
            .wait_for_consumers_ready(1, Duration::from_secs(3)),
        "mmap producer timed out waiting for rkyv consumer"
    );
    producer
        .publish(&RkyvBatch(payloads.clone()), 31)
        .expect("publish mmap owned");
    producer
        .publish(&RkyvBatch(payloads.clone()), 32)
        .expect("publish mmap leased");

    let output = child.wait_with_output().expect("wait for mmap codec child");
    let result = assert_child_success(&output, "dst mmap rkyv equivalence child");
    assert_eq!(result.messages, payloads.len());
    assert_eq!(result.owned_checksum, expected_checksum);
    assert_eq!(result.leased_checksum, Some(expected_checksum));
}

#[cfg(feature = "flatbuffers")]
#[test]
fn dst_zero_copy_equivalence_flatbuf_shm() {
    let segment = segment_name("dzcf");
    let payloads = make_payloads(0x2202, 4, 32);
    let expected_checksum = checksum_payloads(&payloads);
    let mut producer = TypedProducer::<EqFrame>::create(&segment, DEPTH).expect("create producer");
    let child = spawn_codec_child("equivalence", "flatbuf", &segment, payloads.len());

    std::thread::sleep(Duration::from_millis(400));
    producer.discover_consumers(Duration::from_secs(3));
    producer
        .publish(&FlatbufBatch(payloads.clone()), 21)
        .expect("publish owned");
    producer
        .publish(&FlatbufBatch(payloads.clone()), 22)
        .expect("publish leased");

    let output = child.wait_with_output().expect("wait for codec child");
    let result = assert_child_success(&output, "dst flatbuf equivalence child");
    assert_eq!(result.messages, payloads.len());
    assert_eq!(result.owned_checksum, expected_checksum);
    assert_eq!(result.leased_checksum, Some(expected_checksum));
}

#[cfg(feature = "flatbuffers")]
#[test]
fn dst_zero_copy_equivalence_flatbuf_mmap() {
    let (_root, segment, layout) = mmap_case("dzcfm");
    let consumer_id = "dst_codec_eq_flatbuf";
    let payloads = make_payloads(0x2204, 4, 32);
    let expected_checksum = checksum_payloads(&payloads);
    let mut producer =
        MmapTypedProducer::<EqFrame>::create(layout.clone(), DEPTH).expect("create mmap producer");
    let child = spawn_codec_child_mmap(
        "equivalence",
        "flatbuf",
        layout.root_dir(),
        &segment,
        consumer_id,
        payloads.len(),
    );

    assert!(
        producer
            .raw()
            .wait_for_consumers_ready(1, Duration::from_secs(3)),
        "mmap producer timed out waiting for flatbuf consumer"
    );
    producer
        .publish(&FlatbufBatch(payloads.clone()), 41)
        .expect("publish mmap owned");
    producer
        .publish(&FlatbufBatch(payloads.clone()), 42)
        .expect("publish mmap leased");

    let output = child.wait_with_output().expect("wait for mmap codec child");
    let result = assert_child_success(&output, "dst mmap flatbuf equivalence child");
    assert_eq!(result.messages, payloads.len());
    assert_eq!(result.owned_checksum, expected_checksum);
    assert_eq!(result.leased_checksum, Some(expected_checksum));
}

#[cfg(feature = "rkyv")]
#[test]
fn dst_codec_roundtrip_fragmented_rkyv_shm() {
    let segment = segment_name("dcrr");
    let mut producer =
        TypedProducer::<FragFrame>::create(&segment, DEPTH).expect("create producer");
    let child = spawn_codec_child("roundtrip", "rkyv", &segment, 12);

    std::thread::sleep(Duration::from_millis(400));
    producer.discover_consumers(Duration::from_secs(3));
    for sequence in 0..12u64 {
        producer
            .publish(
                &RkyvBatch(make_payloads(0x3300 + sequence, 6, 96)),
                (sequence % 251) as u8,
            )
            .expect("publish fragmented rkyv");
    }

    let output = child.wait_with_output().expect("wait for codec child");
    let result = assert_child_success(&output, "dst rkyv roundtrip child");
    assert_eq!(result.messages, 12);
    assert!(result.owned_checksum > 0);
}

#[cfg(feature = "rkyv")]
#[test]
fn dst_codec_roundtrip_fragmented_rkyv_mmap() {
    let (_root, segment, layout) = mmap_case("dcrrm");
    let consumer_id = "dst_codec_rt_rkyv";
    let mut producer = MmapTypedProducer::<FragFrame>::create(layout.clone(), DEPTH)
        .expect("create mmap producer");
    let child = spawn_codec_child_mmap(
        "roundtrip",
        "rkyv",
        layout.root_dir(),
        &segment,
        consumer_id,
        12,
    );

    assert!(
        producer
            .raw()
            .wait_for_consumers_ready(1, Duration::from_secs(3)),
        "mmap producer timed out waiting for rkyv roundtrip consumer"
    );
    for sequence in 0..12u64 {
        producer
            .publish(
                &RkyvBatch(make_payloads(0x3600 + sequence, 6, 96)),
                (sequence % 251) as u8,
            )
            .expect("publish fragmented mmap rkyv");
    }

    let output = child.wait_with_output().expect("wait for mmap codec child");
    let result = assert_child_success(&output, "dst mmap rkyv roundtrip child");
    assert_eq!(result.messages, 12);
    assert!(result.owned_checksum > 0);
}

#[cfg(feature = "flatbuffers")]
#[test]
fn dst_codec_roundtrip_fragmented_flatbuf_shm() {
    let segment = segment_name("dcrf");
    let mut producer =
        TypedProducer::<FragFrame>::create(&segment, DEPTH).expect("create producer");
    let child = spawn_codec_child("roundtrip", "flatbuf", &segment, 12);

    std::thread::sleep(Duration::from_millis(400));
    producer.discover_consumers(Duration::from_secs(3));
    for sequence in 0..12u64 {
        producer
            .publish(
                &FlatbufBatch(make_payloads(0x3400 + sequence, 6, 96)),
                (sequence % 251) as u8,
            )
            .expect("publish fragmented flatbuf");
    }

    let output = child.wait_with_output().expect("wait for codec child");
    let result = assert_child_success(&output, "dst flatbuf roundtrip child");
    assert_eq!(result.messages, 12);
    assert!(result.owned_checksum > 0);
}

#[cfg(feature = "flatbuffers")]
#[test]
fn dst_codec_roundtrip_fragmented_flatbuf_mmap() {
    let (_root, segment, layout) = mmap_case("dcrfm");
    let consumer_id = "dst_codec_rt_flatbuf";
    let mut producer = MmapTypedProducer::<FragFrame>::create(layout.clone(), DEPTH)
        .expect("create mmap producer");
    let child = spawn_codec_child_mmap(
        "roundtrip",
        "flatbuf",
        layout.root_dir(),
        &segment,
        consumer_id,
        12,
    );

    assert!(
        producer
            .raw()
            .wait_for_consumers_ready(1, Duration::from_secs(3)),
        "mmap producer timed out waiting for flatbuf roundtrip consumer"
    );
    for sequence in 0..12u64 {
        producer
            .publish(
                &FlatbufBatch(make_payloads(0x3700 + sequence, 6, 96)),
                (sequence % 251) as u8,
            )
            .expect("publish fragmented mmap flatbuf");
    }

    let output = child.wait_with_output().expect("wait for mmap codec child");
    let result = assert_child_success(&output, "dst mmap flatbuf roundtrip child");
    assert_eq!(result.messages, 12);
    assert!(result.owned_checksum > 0);
}

#[test]
fn dst_codec_roundtrip_fragmented_bincode_shm() {
    let segment = segment_name("dcrb");
    let mut producer =
        TypedProducer::<FragFrame>::create(&segment, DEPTH).expect("create producer");
    let child = spawn_codec_child("roundtrip", "bincode", &segment, 12);

    std::thread::sleep(Duration::from_millis(400));
    producer.discover_consumers(Duration::from_secs(3));
    for sequence in 0..12u64 {
        producer
            .publish(
                &BincodeBatch(make_payloads(0x3500 + sequence, 6, 96)),
                (sequence % 251) as u8,
            )
            .expect("publish fragmented bincode");
    }

    let output = child.wait_with_output().expect("wait for codec child");
    let result = assert_child_success(&output, "dst bincode roundtrip child");
    assert_eq!(result.messages, 12);
    assert!(result.owned_checksum > 0);
    assert_eq!(result.leased_checksum, None);
}

#[test]
fn dst_codec_roundtrip_fragmented_bincode_mmap() {
    let (_root, segment, layout) = mmap_case("dcrbm");
    let consumer_id = "dst_codec_rt_bincode";
    let mut producer = MmapTypedProducer::<FragFrame>::create(layout.clone(), DEPTH)
        .expect("create mmap producer");
    let child = spawn_codec_child_mmap(
        "roundtrip",
        "bincode",
        layout.root_dir(),
        &segment,
        consumer_id,
        12,
    );

    assert!(
        producer
            .raw()
            .wait_for_consumers_ready(1, Duration::from_secs(3)),
        "mmap producer timed out waiting for bincode roundtrip consumer"
    );
    for sequence in 0..12u64 {
        producer
            .publish(
                &BincodeBatch(make_payloads(0x3800 + sequence, 6, 96)),
                (sequence % 251) as u8,
            )
            .expect("publish fragmented mmap bincode");
    }

    let output = child.wait_with_output().expect("wait for mmap codec child");
    let result = assert_child_success(&output, "dst mmap bincode roundtrip child");
    assert_eq!(result.messages, 12);
    assert!(result.owned_checksum > 0);
    assert_eq!(result.leased_checksum, None);
}

fn run_codec_consumer_restart_shm(codec: &str, seed_base: u64) {
    let segment = segment_name("dcrs");
    let batch_count = 64usize;
    let restart_after = 12usize;
    let offline_backlog = 6usize;
    let consumer_id = match codec {
        "rkyv" => "dcrr",
        "flatbuf" => "dcrf",
        "bincode" => "dcrb",
        other => panic!("unsupported shm restart codec: {other}"),
    };
    let mut producer =
        TypedProducer::<FragFrame>::create(&segment, DEPTH).expect("create producer");
    let child = spawn_codec_child_custom(
        "restart_roundtrip",
        codec,
        "shm",
        None,
        &segment,
        Some(consumer_id),
        restart_after,
        Some(seed_base),
        0,
    );

    std::thread::sleep(Duration::from_millis(250));
    assert!(
        producer.discover_consumer_id(consumer_id, Duration::from_secs(3)),
        "shm producer timed out waiting for initial restart codec consumer"
    );

    for sequence in 0..restart_after {
        publish_codec_batch_shm(codec, &mut producer, seed_base, sequence);
        std::thread::sleep(Duration::from_micros(250));
    }

    let output = child
        .wait_with_output()
        .expect("wait for initial codec shm child");
    let result = assert_child_success(&output, "initial codec shm child");
    assert_eq!(result.messages, restart_after);

    for sequence in restart_after..restart_after + offline_backlog {
        publish_codec_batch_shm(codec, &mut producer, seed_base, sequence);
    }

    let mut child = spawn_codec_child_custom(
        "restart_roundtrip",
        codec,
        "shm",
        None,
        &segment,
        Some(consumer_id),
        batch_count - restart_after,
        Some(seed_base),
        restart_after,
    );
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        producer.discover_consumer_id(consumer_id, Duration::from_secs(3)),
        "shm producer timed out waiting for restarted codec consumer"
    );

    for sequence in restart_after + offline_backlog..batch_count {
        assert_child_alive(&mut child, "codec shm restarted child");
        publish_codec_batch_shm(codec, &mut producer, seed_base, sequence);
        std::thread::sleep(Duration::from_micros(250));
    }

    let output = child
        .wait_with_output()
        .expect("wait for restarted codec shm child");
    let result = assert_child_success(&output, "restarted codec shm child");
    assert_eq!(result.messages, batch_count - restart_after);
}

fn run_codec_consumer_restart_mmap(codec: &str, seed_base: u64) {
    let (_root, segment, layout) = mmap_case("dcrsm");
    let batch_count = 64usize;
    let restart_after = 12usize;
    let offline_backlog = 6usize;
    let consumer_id = format!("dst_codec_restart_mmap_{codec}");
    let mut producer = MmapTypedProducer::<FragFrame>::create(layout.clone(), DEPTH)
        .expect("create mmap producer");
    let child = spawn_codec_child_custom(
        "restart_roundtrip",
        codec,
        "mmap",
        Some(layout.root_dir()),
        &segment,
        Some(&consumer_id),
        restart_after,
        Some(seed_base),
        0,
    );

    assert!(
        producer
            .raw()
            .wait_for_consumers_ready(1, Duration::from_secs(3)),
        "mmap producer timed out waiting for restart codec consumer"
    );

    for sequence in 0..restart_after {
        publish_codec_batch_mmap(codec, &mut producer, seed_base, sequence);
        std::thread::sleep(Duration::from_micros(250));
    }

    let output = child
        .wait_with_output()
        .expect("wait for initial codec mmap child");
    let result = assert_child_success(&output, "initial codec mmap child");
    assert_eq!(result.messages, restart_after);

    for sequence in restart_after..restart_after + offline_backlog {
        publish_codec_batch_mmap(codec, &mut producer, seed_base, sequence);
    }

    let mut child = spawn_codec_child_custom(
        "restart_roundtrip",
        codec,
        "mmap",
        Some(layout.root_dir()),
        &segment,
        Some(&consumer_id),
        batch_count - restart_after,
        Some(seed_base),
        restart_after,
    );
    assert!(
        producer
            .raw()
            .wait_for_consumers_ready(1, Duration::from_secs(3)),
        "mmap producer timed out waiting for restarted codec consumer"
    );

    for sequence in restart_after + offline_backlog..batch_count {
        assert_child_alive(&mut child, "codec mmap restarted child");
        publish_codec_batch_mmap(codec, &mut producer, seed_base, sequence);
        std::thread::sleep(Duration::from_micros(250));
    }

    let output = child
        .wait_with_output()
        .expect("wait for restarted codec mmap child");
    let result = assert_child_success(&output, "restarted codec mmap child");
    assert_eq!(result.messages, batch_count - restart_after);
}

#[cfg(feature = "rkyv")]
#[test]
fn dst_codec_consumer_restart_rkyv_shm() {
    run_codec_consumer_restart_shm("rkyv", 0x5100);
}

#[cfg(feature = "rkyv")]
#[test]
fn dst_codec_consumer_restart_rkyv_mmap() {
    run_codec_consumer_restart_mmap("rkyv", 0x5200);
}

#[cfg(feature = "flatbuffers")]
#[test]
fn dst_codec_consumer_restart_flatbuf_shm() {
    run_codec_consumer_restart_shm("flatbuf", 0x5300);
}

#[cfg(feature = "flatbuffers")]
#[test]
fn dst_codec_consumer_restart_flatbuf_mmap() {
    run_codec_consumer_restart_mmap("flatbuf", 0x5400);
}

#[test]
fn dst_codec_consumer_restart_bincode_shm() {
    run_codec_consumer_restart_shm("bincode", 0x5500);
}

#[test]
fn dst_codec_consumer_restart_bincode_mmap() {
    run_codec_consumer_restart_mmap("bincode", 0x5600);
}

#[test]
#[ignore]
fn dst_fuzz_codec_ci_seed_matrix() {
    for seed in 0x2600..0x2664 {
        if seed % 10 == 0 {
            eprintln!("codec ci fuzz seed={seed:#x}");
        }
        run_codec_fuzz_seed(seed, false);
    }
}

#[test]
#[ignore]
fn dst_fuzz_codec_nightly_seed_matrix() {
    for seed in 0x2700..0x2ae8 {
        if seed % 50 == 0 {
            eprintln!("codec nightly fuzz seed={seed:#x}");
        }
        run_codec_fuzz_seed(seed, true);
    }
}

#[test]
#[ignore]
fn dst_codec_child() {
    let case = env::var("DST_CODEC_CASE").expect("DST_CODEC_CASE should be set");
    let codec = env::var("DST_CODEC_CODEC").expect("DST_CODEC_CODEC should be set");
    let backend = env::var("DST_CODEC_BACKEND").unwrap_or_else(|_| "shm".to_string());
    let segment = env::var("DST_CODEC_SEGMENT").expect("DST_CODEC_SEGMENT should be set");
    let depth: usize = env::var("DST_CODEC_DEPTH")
        .expect("DST_CODEC_DEPTH should be set")
        .parse()
        .expect("depth should parse");
    let batch_count: usize = env::var("DST_CODEC_BATCH_COUNT")
        .expect("DST_CODEC_BATCH_COUNT should be set")
        .parse()
        .expect("batch count should parse");
    let start_sequence: usize = env::var("DST_CODEC_START_SEQUENCE")
        .ok()
        .map(|raw| raw.parse().expect("DST_CODEC_START_SEQUENCE should parse"))
        .unwrap_or(0);
    let explicit_seed_base = env::var("DST_CODEC_SEED_BASE").ok().map(|raw| {
        raw.parse::<u64>()
            .expect("DST_CODEC_SEED_BASE should parse")
    });
    let deadline = Instant::now() + Duration::from_secs(15);
    match case.as_str() {
        "equivalence" => match codec.as_str() {
            #[cfg(feature = "rkyv")]
            "rkyv" => {
                let mut consumer = attach_eq_consumer(&backend, &segment, depth, deadline);
                let (_, owned): (u8, RkyvBatch) = match &mut consumer {
                    EqCodecConsumer::Shm(consumer) => {
                        consumer.recv_owned().expect("recv_owned rkyv")
                    }
                    EqCodecConsumer::Mmap(consumer) => {
                        consumer.recv_owned().expect("recv_owned mmap rkyv")
                    }
                };
                let owned_checksum = checksum_payloads(&owned.0);
                let mut reassembly = ReassemblyBuffer::new(128 * 1024);
                let leased_checksum = match &mut consumer {
                    EqCodecConsumer::Shm(consumer) => consumer
                        .recv_leased::<RkyvBatch, _, _>(&mut reassembly, |_kind, archived| {
                            checksum_archived_rkyv(archived)
                        })
                        .expect("recv_leased rkyv"),
                    EqCodecConsumer::Mmap(consumer) => consumer
                        .recv_leased::<RkyvBatch, _, _>(&mut reassembly, |_kind, archived| {
                            checksum_archived_rkyv(archived)
                        })
                        .expect("recv_leased mmap rkyv"),
                };
                println!(
                    "DST_CODEC_OK {}",
                    serde_json::to_string(&CodecChildResult {
                        owned_checksum,
                        leased_checksum: Some(leased_checksum),
                        messages: batch_count,
                    })
                    .expect("serialize child result")
                );
            }
            #[cfg(feature = "flatbuffers")]
            "flatbuf" => {
                let mut consumer = attach_eq_consumer(&backend, &segment, depth, deadline);
                let (_, owned): (u8, FlatbufBatch) = match &mut consumer {
                    EqCodecConsumer::Shm(consumer) => {
                        consumer.recv_owned().expect("recv_owned flatbuf")
                    }
                    EqCodecConsumer::Mmap(consumer) => {
                        consumer.recv_owned().expect("recv_owned mmap flatbuf")
                    }
                };
                let owned_checksum = checksum_payloads(&owned.0);
                let mut reassembly = ReassemblyBuffer::new(128 * 1024);
                let leased_checksum = match &mut consumer {
                    EqCodecConsumer::Shm(consumer) => consumer
                        .recv_leased::<FlatbufBatch, _, _>(&mut reassembly, |_kind, archived| {
                            checksum_flatbuf_root(archived)
                        })
                        .expect("recv_leased flatbuf"),
                    EqCodecConsumer::Mmap(consumer) => consumer
                        .recv_leased::<FlatbufBatch, _, _>(&mut reassembly, |_kind, archived| {
                            checksum_flatbuf_root(archived)
                        })
                        .expect("recv_leased mmap flatbuf"),
                };
                println!(
                    "DST_CODEC_OK {}",
                    serde_json::to_string(&CodecChildResult {
                        owned_checksum,
                        leased_checksum: Some(leased_checksum),
                        messages: batch_count,
                    })
                    .expect("serialize child result")
                );
            }
            other => panic!("unsupported equivalence codec: {other}"),
        },
        "roundtrip" => match codec.as_str() {
            #[cfg(feature = "rkyv")]
            "rkyv" => {
                let mut consumer = attach_frag_consumer(&backend, &segment, depth, deadline);
                let seed_base =
                    explicit_seed_base.unwrap_or(if backend == "mmap" { 0x3600 } else { 0x3300 });
                let checksum = roundtrip_owned::<RkyvBatch>(&mut consumer, batch_count, seed_base);
                println!(
                    "DST_CODEC_OK {}",
                    serde_json::to_string(&CodecChildResult {
                        owned_checksum: checksum,
                        leased_checksum: None,
                        messages: batch_count,
                    })
                    .expect("serialize child result")
                );
            }
            #[cfg(feature = "flatbuffers")]
            "flatbuf" => {
                let mut consumer = attach_frag_consumer(&backend, &segment, depth, deadline);
                let seed_base =
                    explicit_seed_base.unwrap_or(if backend == "mmap" { 0x3700 } else { 0x3400 });
                let checksum =
                    roundtrip_owned::<FlatbufBatch>(&mut consumer, batch_count, seed_base);
                println!(
                    "DST_CODEC_OK {}",
                    serde_json::to_string(&CodecChildResult {
                        owned_checksum: checksum,
                        leased_checksum: None,
                        messages: batch_count,
                    })
                    .expect("serialize child result")
                );
            }
            "bincode" => {
                let mut consumer = attach_frag_consumer(&backend, &segment, depth, deadline);
                let seed_base =
                    explicit_seed_base.unwrap_or(if backend == "mmap" { 0x3800 } else { 0x3500 });
                let checksum =
                    roundtrip_owned::<BincodeBatch>(&mut consumer, batch_count, seed_base);
                println!(
                    "DST_CODEC_OK {}",
                    serde_json::to_string(&CodecChildResult {
                        owned_checksum: checksum,
                        leased_checksum: None,
                        messages: batch_count,
                    })
                    .expect("serialize child result")
                );
            }
            other => panic!("unsupported roundtrip codec: {other}"),
        },
        "restart_roundtrip" => match codec.as_str() {
            #[cfg(feature = "rkyv")]
            "rkyv" => {
                let mut consumer = attach_frag_consumer(&backend, &segment, depth, deadline);
                let seed_base =
                    explicit_seed_base.expect("restart roundtrip rkyv requires seed base");
                let checksum = roundtrip_owned_from::<RkyvBatch>(
                    &mut consumer,
                    batch_count,
                    seed_base,
                    start_sequence,
                );
                println!(
                    "DST_CODEC_OK {}",
                    serde_json::to_string(&CodecChildResult {
                        owned_checksum: checksum,
                        leased_checksum: None,
                        messages: batch_count,
                    })
                    .expect("serialize child result")
                );
            }
            #[cfg(feature = "flatbuffers")]
            "flatbuf" => {
                let mut consumer = attach_frag_consumer(&backend, &segment, depth, deadline);
                let seed_base =
                    explicit_seed_base.expect("restart roundtrip flatbuf requires seed base");
                let checksum = roundtrip_owned_from::<FlatbufBatch>(
                    &mut consumer,
                    batch_count,
                    seed_base,
                    start_sequence,
                );
                println!(
                    "DST_CODEC_OK {}",
                    serde_json::to_string(&CodecChildResult {
                        owned_checksum: checksum,
                        leased_checksum: None,
                        messages: batch_count,
                    })
                    .expect("serialize child result")
                );
            }
            "bincode" => {
                let mut consumer = attach_frag_consumer(&backend, &segment, depth, deadline);
                let seed_base =
                    explicit_seed_base.expect("restart roundtrip bincode requires seed base");
                let checksum = roundtrip_owned_from::<BincodeBatch>(
                    &mut consumer,
                    batch_count,
                    seed_base,
                    start_sequence,
                );
                println!(
                    "DST_CODEC_OK {}",
                    serde_json::to_string(&CodecChildResult {
                        owned_checksum: checksum,
                        leased_checksum: None,
                        messages: batch_count,
                    })
                    .expect("serialize child result")
                );
            }
            other => panic!("unsupported restart roundtrip codec: {other}"),
        },
        other => panic!("unsupported codec dst case: {other}"),
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "test-only enum keeps backend-specific typed consumers direct for simpler assertions"
)]
enum EqCodecConsumer {
    Shm(TypedConsumer<EqFrame>),
    Mmap(MmapTypedConsumer<EqFrame>),
}

#[expect(
    clippy::large_enum_variant,
    reason = "test-only enum keeps backend-specific typed consumers direct for simpler assertions"
)]
enum FragCodecConsumer {
    Shm(TypedConsumer<FragFrame>),
    Mmap(MmapTypedConsumer<FragFrame>),
}

fn attach_eq_consumer(
    backend: &str,
    segment: &str,
    depth: usize,
    deadline: Instant,
) -> EqCodecConsumer {
    loop {
        match backend {
            "shm" => {
                match TypedConsumer::<EqFrame>::attach(segment, depth, MyelonWaitStrategy::BusySpin)
                {
                    Ok(consumer) => return EqCodecConsumer::Shm(consumer),
                    Err(_) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(25))
                    }
                    Err(err) => panic!("attach eq consumer: {err}"),
                }
            }
            "mmap" => {
                let root = env::var("DST_CODEC_ROOT").expect("DST_CODEC_ROOT should be set");
                let consumer_id =
                    env::var("DST_CODEC_CONSUMER_ID").expect("DST_CODEC_CONSUMER_ID should be set");
                let layout = disruptor_mp::MmapTransportLayout::new(root, segment)
                    .expect("valid dst codec mmap layout");
                match MmapTypedConsumer::<EqFrame>::attach(
                    layout,
                    depth,
                    &consumer_id,
                    MyelonWaitStrategy::BusySpin,
                ) {
                    Ok(consumer) => return EqCodecConsumer::Mmap(consumer),
                    Err(_) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(25))
                    }
                    Err(err) => panic!("attach mmap eq consumer: {err}"),
                }
            }
            other => panic!("unsupported codec backend: {other}"),
        }
    }
}

fn attach_frag_consumer(
    backend: &str,
    segment: &str,
    depth: usize,
    deadline: Instant,
) -> FragCodecConsumer {
    loop {
        match backend {
            "shm" => {
                let attach = if let Ok(consumer_id) = env::var("DST_CODEC_CONSUMER_ID") {
                    TypedConsumer::<FragFrame>::attach_with_consumer_id(
                        segment,
                        depth,
                        &consumer_id,
                        MyelonWaitStrategy::BusySpin,
                    )
                } else {
                    TypedConsumer::<FragFrame>::attach(segment, depth, MyelonWaitStrategy::BusySpin)
                };
                match attach {
                    Ok(consumer) => return FragCodecConsumer::Shm(consumer),
                    Err(_) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(25))
                    }
                    Err(err) => panic!("attach frag consumer: {err}"),
                }
            }
            "mmap" => {
                let root = env::var("DST_CODEC_ROOT").expect("DST_CODEC_ROOT should be set");
                let consumer_id =
                    env::var("DST_CODEC_CONSUMER_ID").expect("DST_CODEC_CONSUMER_ID should be set");
                let layout = disruptor_mp::MmapTransportLayout::new(root, segment)
                    .expect("valid dst codec mmap layout");
                match MmapTypedConsumer::<FragFrame>::attach(
                    layout,
                    depth,
                    &consumer_id,
                    MyelonWaitStrategy::BusySpin,
                ) {
                    Ok(consumer) => return FragCodecConsumer::Mmap(consumer),
                    Err(_) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(25))
                    }
                    Err(err) => panic!("attach mmap frag consumer: {err}"),
                }
            }
            other => panic!("unsupported codec backend: {other}"),
        }
    }
}

fn roundtrip_owned<T>(consumer: &mut FragCodecConsumer, batch_count: usize, seed_base: u64) -> u64
where
    T: Codec + From<Vec<TestPayload>> + Into<Vec<TestPayload>>,
{
    roundtrip_owned_from::<T>(consumer, batch_count, seed_base, 0)
}

fn roundtrip_owned_from<T>(
    consumer: &mut FragCodecConsumer,
    batch_count: usize,
    seed_base: u64,
    start_sequence: usize,
) -> u64
where
    T: Codec + From<Vec<TestPayload>> + Into<Vec<TestPayload>>,
{
    let mut checksum = 0u64;
    for offset in 0..batch_count {
        let sequence = start_sequence + offset;
        let (kind, batch): (u8, T) = match consumer {
            FragCodecConsumer::Shm(consumer) => consumer.recv_owned().expect("recv shm owned"),
            FragCodecConsumer::Mmap(consumer) => consumer.recv_owned().expect("recv mmap owned"),
        };
        assert_eq!(kind, (sequence as u8) % 251, "kind mismatch at {sequence}");
        let decoded: Vec<TestPayload> = batch.into();
        let expected = make_payloads(seed_base + sequence as u64, 6, 96);
        assert_eq!(decoded, expected, "payload mismatch at {sequence}");
        checksum = checksum.wrapping_add(checksum_payloads(&decoded));
    }
    checksum
}

fn publish_codec_batch_shm(
    codec: &str,
    producer: &mut TypedProducer<FragFrame>,
    seed_base: u64,
    sequence: usize,
) {
    let kind = (sequence as u8) % 251;
    match codec {
        #[cfg(feature = "rkyv")]
        "rkyv" => producer
            .publish(
                &RkyvBatch(make_payloads(seed_base + sequence as u64, 6, 96)),
                kind,
            )
            .expect("publish restart rkyv shm"),
        #[cfg(feature = "flatbuffers")]
        "flatbuf" => producer
            .publish(
                &FlatbufBatch(make_payloads(seed_base + sequence as u64, 6, 96)),
                kind,
            )
            .expect("publish restart flatbuf shm"),
        "bincode" => producer
            .publish(
                &BincodeBatch(make_payloads(seed_base + sequence as u64, 6, 96)),
                kind,
            )
            .expect("publish restart bincode shm"),
        other => panic!("unsupported restart codec: {other}"),
    }
}

fn publish_codec_batch_mmap(
    codec: &str,
    producer: &mut MmapTypedProducer<FragFrame>,
    seed_base: u64,
    sequence: usize,
) {
    let kind = (sequence as u8) % 251;
    match codec {
        #[cfg(feature = "rkyv")]
        "rkyv" => producer
            .publish(
                &RkyvBatch(make_payloads(seed_base + sequence as u64, 6, 96)),
                kind,
            )
            .expect("publish restart rkyv mmap"),
        #[cfg(feature = "flatbuffers")]
        "flatbuf" => producer
            .publish(
                &FlatbufBatch(make_payloads(seed_base + sequence as u64, 6, 96)),
                kind,
            )
            .expect("publish restart flatbuf mmap"),
        "bincode" => producer
            .publish(
                &BincodeBatch(make_payloads(seed_base + sequence as u64, 6, 96)),
                kind,
            )
            .expect("publish restart bincode mmap"),
        other => panic!("unsupported restart codec: {other}"),
    }
}

impl From<Vec<TestPayload>> for BincodeBatch {
    fn from(value: Vec<TestPayload>) -> Self {
        Self(value)
    }
}

impl From<BincodeBatch> for Vec<TestPayload> {
    fn from(value: BincodeBatch) -> Self {
        value.0
    }
}

#[cfg(feature = "rkyv")]
impl From<Vec<TestPayload>> for RkyvBatch {
    fn from(value: Vec<TestPayload>) -> Self {
        Self(value)
    }
}

#[cfg(feature = "rkyv")]
impl From<RkyvBatch> for Vec<TestPayload> {
    fn from(value: RkyvBatch) -> Self {
        value.0
    }
}

#[cfg(feature = "flatbuffers")]
impl From<Vec<TestPayload>> for FlatbufBatch {
    fn from(value: Vec<TestPayload>) -> Self {
        Self(value)
    }
}

#[cfg(feature = "flatbuffers")]
impl From<FlatbufBatch> for Vec<TestPayload> {
    fn from(value: FlatbufBatch) -> Self {
        value.0
    }
}
