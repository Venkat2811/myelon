use crate::codec_payloads::{
    make_payloads, measure_zero_copy_telemetry, AccessTelemetry, TestPayload,
};
use myelon::transport::{FrameMeta, FramedTransportFrame};

pub(super) fn payloads_for(batch_size: usize) -> Vec<TestPayload> {
    make_payloads(batch_size)
}

pub(super) fn access_telemetry(codec: &str, batch_size: usize) -> AccessTelemetry {
    measure_zero_copy_telemetry(codec, &make_payloads(batch_size))
}

pub(super) fn reassembly_capacity(encoded_bytes: usize) -> usize {
    encoded_bytes.max(256 * 1024)
}

pub(super) fn zero_copy_layer(codec: &str) -> &'static str {
    match codec {
        "flatbuf" => "typed_zero_copy_flatbuf",
        _ => "typed_zero_copy",
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(super) struct ZeroCopyFrameHeader {
    len: u32,
    kind: u8,
    flags: u8,
    msg_id: u32,
    _aligned_header: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(super) struct ZeroCopyFrame<const DATA_BYTES: usize> {
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
            data: [0; DATA_BYTES],
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

pub(super) type ZcFrame = ZeroCopyFrame<{ 64 * 1024 - std::mem::size_of::<ZeroCopyFrameHeader>() }>;
