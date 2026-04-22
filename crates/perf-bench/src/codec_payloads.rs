//! Shared codec payload types for benchmark scenarios.
//!
//! Eliminates 5 copies of TestPayload + make_payloads + encode/access functions
//! from codec/shm, codec/mmap, codec/nofrag_shm, sweep/myelon_layers, sweep/nofrag_all.

use crate::allocation::measure_allocations;
use myelon::codec::{Codec, CodecError, ZeroCopyCodec};
use std::hint::black_box;
use std::time::Instant;

/// Common benchmark payload — mimics a Competitor Sequence with token_ids and block_table.
#[derive(
    Clone,
    Debug,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct TestPayload {
    pub id: u64,
    pub token_ids: Vec<u32>,
    pub block_table: Vec<u32>,
    pub temperature: f32,
    pub label: String,
}

/// Generate `count` test payloads with deterministic data.
/// Each payload is ~585 bytes encoded via rkyv.
pub fn make_payloads(count: usize) -> Vec<TestPayload> {
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

// --- rkyv encode/access ---

/// Encode payloads via rkyv. Returns AlignedVec (zero extra copies).
pub fn encode_rkyv(payloads: &Vec<TestPayload>) -> rkyv::util::AlignedVec {
    rkyv::to_bytes::<rkyv::rancor::Error>(payloads).unwrap()
}

/// Zero-copy access of rkyv-archived payloads. Returns checksum of all fields.
pub fn access_rkyv(bytes: &[u8]) -> u64 {
    let archived = unsafe { rkyv::access_unchecked::<ArchivedPayloadBatch>(bytes) };
    checksum_archived_rkyv(archived)
}

pub type ArchivedPayloadBatch = rkyv::Archived<Vec<TestPayload>>;

pub fn checksum_archived_rkyv(archived: &ArchivedPayloadBatch) -> u64 {
    let mut sum = 0u64;
    for e in archived.iter() {
        sum = sum.wrapping_add(e.id.into());
        for t in e.token_ids.iter() {
            sum = sum.wrapping_add(u32::from(*t) as u64);
        }
        for b in e.block_table.iter() {
            sum = sum.wrapping_add(u32::from(*b) as u64);
        }
        let temperature: f32 = e.temperature.into();
        sum = sum.wrapping_add(temperature.to_bits() as u64);
        for byte in e.label.as_bytes().iter() {
            sum = sum.wrapping_add((*byte) as u64);
        }
    }
    black_box(sum);
    sum
}

// --- FlatBuffers encode/access ---

use crate::generated::bench_payload_generated::myelon::bench as flatbench;

/// Encode payloads via FlatBuffers. Returns `Vec<u8>`.
pub fn encode_flatbuf(payloads: &[TestPayload]) -> Vec<u8> {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(64 * 1024);
    let mut entries = Vec::with_capacity(payloads.len());
    for p in payloads {
        let tids = builder.create_vector(&p.token_ids);
        let bt = builder.create_vector(&p.block_table);
        let label = builder.create_string(&p.label);
        entries.push(flatbench::TestPayload::create(
            &mut builder,
            &flatbench::TestPayloadArgs {
                id: p.id,
                token_ids: Some(tids),
                block_table: Some(bt),
                temperature: p.temperature,
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
    builder.finished_data().to_vec()
}

/// Zero-copy access of FlatBuffers-encoded payloads. Returns checksum of all fields.
pub fn access_flatbuf(bytes: &[u8]) -> u64 {
    let root = flatbench::root_as_payload_batch(bytes).unwrap();
    checksum_flatbuf_root(root)
}

pub fn checksum_flatbuf_root(root: flatbench::PayloadBatch<'_>) -> u64 {
    let entries = root.entries().unwrap();
    let mut sum = 0u64;
    for e in entries.iter() {
        sum = sum.wrapping_add(e.id());
        if let Some(tids) = e.token_ids() {
            for t in tids.iter() {
                sum = sum.wrapping_add(t as u64);
            }
        }
        if let Some(bt) = e.block_table() {
            for b in bt.iter() {
                sum = sum.wrapping_add(b as u64);
            }
        }
        sum = sum.wrapping_add(e.temperature().to_bits() as u64);
        if let Some(label) = e.label() {
            for byte in label.bytes() {
                sum = sum.wrapping_add(byte as u64);
            }
        }
    }
    black_box(sum);
    sum
}

/// Raw bytes checksum — used for raw ring baseline comparison.
pub fn access_raw(bytes: &[u8]) -> u64 {
    let sum = bytes
        .iter()
        .fold(0u64, |acc, &value| acc.wrapping_add(value as u64));
    black_box(sum);
    sum
}

// --- Bincode encode/decode ---

/// Encode payloads via bincode. Returns `Vec<u8>`.
pub fn encode_bincode(payloads: &[TestPayload]) -> Vec<u8> {
    bincode::serialize(payloads).unwrap()
}

pub fn checksum_payloads(payloads: &[TestPayload]) -> u64 {
    payloads.iter().fold(0u64, |acc, payload| {
        let token_sum = payload
            .token_ids
            .iter()
            .fold(0u64, |sum, &value| sum.wrapping_add(value as u64));
        let block_sum = payload
            .block_table
            .iter()
            .fold(0u64, |sum, &value| sum.wrapping_add(value as u64));
        let label_sum = payload
            .label
            .bytes()
            .fold(0u64, |sum, value| sum.wrapping_add(value as u64));

        acc.wrapping_add(payload.id)
            .wrapping_add(token_sum)
            .wrapping_add(block_sum)
            .wrapping_add(payload.temperature.to_bits() as u64)
            .wrapping_add(label_sum)
    })
}

/// Decode payloads via bincode. Returns checksum.
pub fn decode_bincode(bytes: &[u8]) -> u64 {
    let payloads: Vec<TestPayload> = bincode::deserialize(bytes).unwrap();
    let sum = checksum_payloads(&payloads);
    black_box(sum);
    sum
}

pub struct BincodeBatch(pub Vec<TestPayload>);

impl Codec for BincodeBatch {
    type Encoded = Vec<u8>;

    fn encode(&self) -> Result<Self::Encoded, CodecError> {
        Ok(encode_bincode(&self.0))
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        bincode::deserialize(bytes)
            .map(BincodeBatch)
            .map_err(CodecError::decode)
    }
}

pub struct RkyvBatch(pub Vec<TestPayload>);

impl Codec for RkyvBatch {
    type Encoded = rkyv::util::AlignedVec;

    fn encode(&self) -> Result<Self::Encoded, CodecError> {
        Ok(encode_rkyv(&self.0))
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let archived = rkyv::access::<rkyv::Archived<Vec<TestPayload>>, rkyv::rancor::Error>(bytes)
            .map_err(CodecError::decode)?;
        let owned: Vec<TestPayload> =
            rkyv::deserialize::<Vec<TestPayload>, rkyv::rancor::Error>(archived)
                .map_err(CodecError::decode)?;
        Ok(RkyvBatch(owned))
    }
}

impl ZeroCopyCodec for RkyvBatch {
    type Archived<'a> = &'a ArchivedPayloadBatch;

    fn access<'a>(bytes: &'a [u8]) -> Result<Self::Archived<'a>, CodecError> {
        // Bench payloads are produced entirely by our own encode path, and RFC 0014's
        // alignment/reassembly hardening plus the regression tests below already prove
        // the framed/fragmented typed transport delivers valid aligned bytes here.
        // Using the unchecked accessor keeps the benchmark telemetry honest about the
        // steady-state zero-copy fast path instead of measuring validator allocations.
        Ok(unsafe { rkyv::access_unchecked::<ArchivedPayloadBatch>(bytes) })
    }
}

pub struct FlatbufBatch(pub Vec<TestPayload>);

impl Codec for FlatbufBatch {
    type Encoded = Vec<u8>;

    fn encode(&self) -> Result<Self::Encoded, CodecError> {
        Ok(encode_flatbuf(&self.0))
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

impl ZeroCopyCodec for FlatbufBatch {
    type Archived<'a> = flatbench::PayloadBatch<'a>;

    fn access<'a>(bytes: &'a [u8]) -> Result<Self::Archived<'a>, CodecError> {
        flatbench::root_as_payload_batch(bytes).map_err(CodecError::decode)
    }
}

pub fn encoded_len(codec: &str, payloads: &[TestPayload]) -> usize {
    match codec {
        "bincode" => BincodeBatch(payloads.to_vec())
            .encode()
            .expect("bincode encode")
            .len(),
        "rkyv" => encode_rkyv(&payloads.to_vec()).len(),
        "flatbuf" => FlatbufBatch(payloads.to_vec())
            .encode()
            .expect("flatbuf encode")
            .len(),
        _ => 0,
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AccessTelemetry {
    pub access_avg_ns: f64,
    pub decode_avg_ns: f64,
    pub access_vs_decode_speedup: f64,
    pub access_alloc_count: u64,
    pub access_alloc_bytes: u64,
    pub decode_alloc_count: u64,
    pub decode_alloc_bytes: u64,
}

pub fn measure_zero_copy_telemetry(codec: &str, payloads: &[TestPayload]) -> AccessTelemetry {
    match codec {
        "rkyv" => measure_rkyv_zero_copy_telemetry(payloads),
        "flatbuf" => measure_flatbuf_zero_copy_telemetry(payloads),
        other => panic!("unsupported zero-copy telemetry codec '{other}'"),
    }
}

fn timing_iterations(encoded_len: usize) -> usize {
    if encoded_len <= 4 * 1024 {
        50_000
    } else if encoded_len <= 64 * 1024 {
        10_000
    } else if encoded_len <= 256 * 1024 {
        2_000
    } else {
        500
    }
}

fn warmup_iterations(iterations: usize) -> usize {
    (iterations / 10).clamp(100, 5_000)
}

fn measure_rkyv_zero_copy_telemetry(payloads: &[TestPayload]) -> AccessTelemetry {
    let encoded = RkyvBatch(payloads.to_vec()).encode().expect("rkyv encode");
    let bytes = encoded.as_slice();
    let iterations = timing_iterations(bytes.len());
    let warmup = warmup_iterations(iterations);

    for _ in 0..warmup {
        let archived = RkyvBatch::access(bytes).expect("rkyv access");
        black_box(checksum_archived_rkyv(archived));
        let decoded = RkyvBatch::decode(bytes).expect("rkyv decode");
        black_box(checksum_payloads(&decoded.0));
    }

    let (_, access_alloc) = measure_allocations(|| {
        let archived = RkyvBatch::access(bytes).expect("rkyv access");
        black_box(checksum_archived_rkyv(archived));
    });
    let (_, decode_alloc) = measure_allocations(|| {
        let decoded = RkyvBatch::decode(bytes).expect("rkyv decode");
        black_box(checksum_payloads(&decoded.0));
    });

    let start = Instant::now();
    for _ in 0..iterations {
        let archived = RkyvBatch::access(bytes).expect("rkyv access");
        black_box(checksum_archived_rkyv(archived));
    }
    let access_avg_ns = start.elapsed().as_nanos() as f64 / iterations as f64;

    let start = Instant::now();
    for _ in 0..iterations {
        let decoded = RkyvBatch::decode(bytes).expect("rkyv decode");
        black_box(checksum_payloads(&decoded.0));
    }
    let decode_avg_ns = start.elapsed().as_nanos() as f64 / iterations as f64;

    AccessTelemetry {
        access_avg_ns,
        decode_avg_ns,
        access_vs_decode_speedup: decode_avg_ns / access_avg_ns,
        access_alloc_count: access_alloc.alloc_count,
        access_alloc_bytes: access_alloc.alloc_bytes,
        decode_alloc_count: decode_alloc.alloc_count,
        decode_alloc_bytes: decode_alloc.alloc_bytes,
    }
}

fn measure_flatbuf_zero_copy_telemetry(payloads: &[TestPayload]) -> AccessTelemetry {
    let encoded = FlatbufBatch(payloads.to_vec())
        .encode()
        .expect("flatbuf encode");
    let bytes = encoded.as_slice();
    let iterations = timing_iterations(bytes.len());
    let warmup = warmup_iterations(iterations);

    for _ in 0..warmup {
        let archived = FlatbufBatch::access(bytes).expect("flatbuf access");
        black_box(checksum_flatbuf_root(archived));
        let decoded = FlatbufBatch::decode(bytes).expect("flatbuf decode");
        black_box(checksum_payloads(&decoded.0));
    }

    let (_, access_alloc) = measure_allocations(|| {
        let archived = FlatbufBatch::access(bytes).expect("flatbuf access");
        black_box(checksum_flatbuf_root(archived));
    });
    let (_, decode_alloc) = measure_allocations(|| {
        let decoded = FlatbufBatch::decode(bytes).expect("flatbuf decode");
        black_box(checksum_payloads(&decoded.0));
    });

    let start = Instant::now();
    for _ in 0..iterations {
        let archived = FlatbufBatch::access(bytes).expect("flatbuf access");
        black_box(checksum_flatbuf_root(archived));
    }
    let access_avg_ns = start.elapsed().as_nanos() as f64 / iterations as f64;

    let start = Instant::now();
    for _ in 0..iterations {
        let decoded = FlatbufBatch::decode(bytes).expect("flatbuf decode");
        black_box(checksum_payloads(&decoded.0));
    }
    let decode_avg_ns = start.elapsed().as_nanos() as f64 / iterations as f64;

    AccessTelemetry {
        access_avg_ns,
        decode_avg_ns,
        access_vs_decode_speedup: decode_avg_ns / access_avg_ns,
        access_alloc_count: access_alloc.alloc_count,
        access_alloc_bytes: access_alloc.alloc_bytes,
        decode_alloc_count: decode_alloc.alloc_count,
        decode_alloc_bytes: decode_alloc.alloc_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use myelon::transport::{
        FixedFrame, FramedTransportFrame, MyelonWaitStrategy, ReassemblyBuffer,
    };
    use myelon::typed_transport::{TypedConsumer, TypedProducer};

    #[repr(C)]
    #[derive(Clone, Copy, Debug)]
    struct AlignedFrame<const DATA_BYTES: usize> {
        len: u32,
        kind: u8,
        flags: u8,
        msg_id: u32,
        _aligned_header: u64,
        data: [u8; DATA_BYTES],
    }

    impl<const DATA_BYTES: usize> Default for AlignedFrame<DATA_BYTES> {
        fn default() -> Self {
            Self {
                len: 0,
                kind: 0,
                flags: 0,
                msg_id: 0,
                _aligned_header: 0,
                data: [0; DATA_BYTES],
            }
        }
    }

    impl<const DATA_BYTES: usize> FramedTransportFrame for AlignedFrame<DATA_BYTES> {
        fn payload_capacity() -> usize {
            DATA_BYTES
        }

        fn frame_meta(&self) -> myelon::transport::FrameMeta<'_> {
            myelon::transport::FrameMeta {
                len: self.len as usize,
                kind: self.kind,
                flags: self.flags,
                msg_id: self.msg_id,
                timestamp_ns: None,
                data: &self.data[..self.len as usize],
            }
        }

        fn write_frame(&mut self, payload: &[u8], kind: u8, msg_id: u32, flags: u8) {
            assert!(payload.len() <= DATA_BYTES);
            self.len = payload.len() as u32;
            self.kind = kind;
            self.flags = flags;
            self.msg_id = msg_id;
            self.data[..payload.len()].copy_from_slice(payload);
        }
    }

    #[test]
    fn test_make_payloads() {
        let payloads = make_payloads(8);
        assert_eq!(payloads.len(), 8);
        assert_eq!(payloads[0].token_ids.len(), 128);
        assert_eq!(payloads[0].block_table.len(), 8);
    }

    #[test]
    fn zero_copy_telemetry_reports_speedup_for_rkyv() {
        let telemetry = measure_zero_copy_telemetry("rkyv", &make_payloads(8));
        assert!(telemetry.access_avg_ns > 0.0);
        assert!(telemetry.decode_avg_ns > 0.0);
        assert!(telemetry.access_vs_decode_speedup > 1.0);
        assert_eq!(telemetry.access_alloc_count, 0);
        assert_eq!(telemetry.access_alloc_bytes, 0);
        assert!(telemetry.decode_alloc_count > 0);
        assert!(telemetry.decode_alloc_bytes > 0);
    }

    #[test]
    fn zero_copy_telemetry_reports_speedup_for_flatbuf() {
        let telemetry = measure_zero_copy_telemetry("flatbuf", &make_payloads(8));
        assert!(telemetry.access_avg_ns > 0.0);
        assert!(telemetry.decode_avg_ns > 0.0);
        assert!(telemetry.access_vs_decode_speedup > 1.0);
        assert_eq!(telemetry.access_alloc_count, 0);
        assert_eq!(telemetry.access_alloc_bytes, 0);
        assert!(telemetry.decode_alloc_count > 0);
        assert!(telemetry.decode_alloc_bytes > 0);
    }

    #[test]
    fn test_rkyv_roundtrip() {
        let payloads = make_payloads(4);
        let encoded = encode_rkyv(&payloads);
        let checksum = access_rkyv(&encoded);
        assert!(checksum > 0);
    }

    #[test]
    fn test_flatbuf_roundtrip() {
        let payloads = make_payloads(4);
        let encoded = encode_flatbuf(&payloads);
        let checksum = access_flatbuf(&encoded);
        assert!(checksum > 0);
    }

    #[test]
    fn test_bincode_roundtrip() {
        let payloads = make_payloads(4);
        let encoded = encode_bincode(&payloads);
        let checksum = decode_bincode(&encoded);
        assert!(checksum > 0);
    }

    #[test]
    fn test_rkyv_flatbuf_checksums_match() {
        let payloads = make_payloads(4);
        let rkyv_sum = access_rkyv(&encode_rkyv(&payloads));
        let fb_sum = access_flatbuf(&encode_flatbuf(&payloads));
        assert_eq!(
            rkyv_sum, fb_sum,
            "rkyv and flatbuf should produce same checksum"
        );
    }

    #[test]
    fn test_encoded_sizes() {
        let p2 = make_payloads(2);
        let p8 = make_payloads(8);
        let r2 = encode_rkyv(&p2);
        let r8 = encode_rkyv(&p8);
        // batch=2 should be ~1.2KB, batch=8 should be ~4.7KB
        assert!(
            r2.len() > 1000 && r2.len() < 2000,
            "batch=2 rkyv size: {}",
            r2.len()
        );
        assert!(
            r8.len() > 4000 && r8.len() < 6000,
            "batch=8 rkyv size: {}",
            r8.len()
        );
    }

    #[test]
    fn test_rkyv_batch_zero_copy_access_works_from_framed_slot_bytes() {
        type Fixed = FixedFrame<{ 64 * 1024 - 12 }>;
        type Aligned = AlignedFrame<{ 64 * 1024 - 24 }>;

        let payloads = make_payloads(8);
        let encoded = encode_rkyv(&payloads);
        let mut fixed = Fixed::default();
        fixed.write_frame(encoded.as_ref(), 7, 42, 0b11);
        let fixed_bytes = fixed.frame_meta().data;
        assert!(
            rkyv::access::<ArchivedPayloadBatch, rkyv::rancor::Error>(fixed_bytes).is_err(),
            "checked rkyv access unexpectedly succeeded for framed slot bytes"
        );

        let mut aligned = Aligned::default();
        aligned.write_frame(encoded.as_ref(), 7, 42, 0b11);
        let archived =
            RkyvBatch::access(aligned.frame_meta().data).expect("typed zero-copy access");
        let checksum = checksum_archived_rkyv(archived);
        assert_eq!(checksum, access_rkyv(encoded.as_ref()));
    }

    #[test]
    fn test_rkyv_batch_zero_copy_fragmented_typed_transport() {
        use std::thread;
        use std::time::{Duration, Instant};

        type FragFrame = AlignedFrame<256>;

        let ring_name = format!("rkyv_zc_frag_{}", std::process::id());
        let depth = 32;
        let payloads = make_payloads(8);
        let expected_checksum = access_rkyv(encode_rkyv(&payloads).as_ref());
        let payload = RkyvBatch(payloads);

        let mut producer: TypedProducer<FragFrame> =
            TypedProducer::create(&ring_name, depth).expect("create producer");

        let consumer_name = ring_name.clone();
        let handle = thread::spawn(move || {
            let mut consumer: TypedConsumer<FragFrame> =
                TypedConsumer::attach(&consumer_name, depth, MyelonWaitStrategy::BusySpin)
                    .expect("attach consumer");
            let mut reassembly = ReassemblyBuffer::new(8 * 1024);
            let deadline = Instant::now() + Duration::from_secs(30);
            let mut result = None;
            while result.is_none() {
                consumer.process_available_zero_copy::<RkyvBatch, _>(
                    &mut reassembly,
                    |kind, archived| {
                        result = Some((kind, checksum_archived_rkyv(archived)));
                    },
                );
                assert!(
                    Instant::now() <= deadline,
                    "timed out waiting for fragmented zero-copy payload"
                );
                std::hint::spin_loop();
            }
            result.expect("result")
        });

        thread::sleep(Duration::from_millis(50));
        producer.discover_consumers(Duration::from_secs(3));
        producer.publish(&payload, 7).expect("publish payload");

        let (kind, checksum) = handle.join().expect("join");
        assert_eq!(kind, 7);
        assert_eq!(checksum, expected_checksum);
    }

    #[test]
    fn test_flatbuf_batch_zero_copy_access_from_fixed_frame_bytes() {
        type Frame = FixedFrame<{ 64 * 1024 - 12 }>;

        let payloads = make_payloads(8);
        let encoded = encode_flatbuf(&payloads);
        let mut frame = Frame::default();
        frame.write_frame(encoded.as_ref(), 7, 42, 0b11);

        let root = FlatbufBatch::access(frame.frame_meta().data).expect("flatbuf zero-copy access");
        let checksum = checksum_flatbuf_root(root);
        assert_eq!(checksum, access_flatbuf(encoded.as_ref()));
    }

    #[test]
    fn test_flatbuf_batch_zero_copy_fragmented_typed_transport() {
        use std::thread;
        use std::time::{Duration, Instant};

        type FragFrame = FixedFrame<256>;

        let ring_name = format!("fzcf_{}", std::process::id());
        let depth = 32;
        let payloads = make_payloads(8);
        let expected_checksum = access_flatbuf(encode_flatbuf(&payloads).as_ref());
        let payload = FlatbufBatch(payloads);

        let mut producer: TypedProducer<FragFrame> =
            TypedProducer::create(&ring_name, depth).expect("create producer");

        let consumer_name = ring_name.clone();
        let handle = thread::spawn(move || {
            let mut consumer: TypedConsumer<FragFrame> =
                TypedConsumer::attach(&consumer_name, depth, MyelonWaitStrategy::BusySpin)
                    .expect("attach consumer");
            let mut reassembly = ReassemblyBuffer::new(8 * 1024);
            let deadline = Instant::now() + Duration::from_secs(30);
            let mut result = None;
            while result.is_none() {
                consumer.process_available_zero_copy::<FlatbufBatch, _>(
                    &mut reassembly,
                    |kind, archived| {
                        result = Some((kind, checksum_flatbuf_root(archived)));
                    },
                );
                assert!(
                    Instant::now() <= deadline,
                    "timed out waiting for fragmented flatbuf zero-copy payload"
                );
                std::hint::spin_loop();
            }
            result.expect("result")
        });

        thread::sleep(Duration::from_millis(50));
        producer.discover_consumers(Duration::from_secs(3));
        producer.publish(&payload, 9).expect("publish payload");

        let (kind, checksum) = handle.join().expect("join");
        assert_eq!(kind, 9);
        assert_eq!(checksum, expected_checksum);
    }
}
