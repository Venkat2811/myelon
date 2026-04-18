//! Shared codec payload types for benchmark scenarios.
//!
//! Eliminates 5 copies of TestPayload + make_payloads + encode/access functions
//! from codec/shm, codec/mmap, codec/nofrag_shm, sweep/myelon_layers, sweep/nofrag_all.

use myelon::codec::{Codec, CodecError};
use std::hint::black_box;

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
    let archived = unsafe { rkyv::access_unchecked::<rkyv::Archived<Vec<TestPayload>>>(bytes) };
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
            sum = sum.wrapping_add(u8::from(*byte) as u64);
        }
    }
    black_box(sum);
    sum
}

// --- FlatBuffers encode/access ---

use crate::generated::bench_payload_generated::myelon::bench as flatbench;

/// Encode payloads via FlatBuffers. Returns Vec<u8>.
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
    let root = flatbuffers::root::<flatbench::PayloadBatch>(bytes).unwrap();
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

/// Encode payloads via bincode. Returns Vec<u8>.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_payloads() {
        let payloads = make_payloads(8);
        assert_eq!(payloads.len(), 8);
        assert_eq!(payloads[0].token_ids.len(), 128);
        assert_eq!(payloads[0].block_table.len(), 8);
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
}
