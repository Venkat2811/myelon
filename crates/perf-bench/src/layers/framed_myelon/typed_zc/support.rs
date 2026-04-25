use crate::layers::framed_myelon::codec::payloads::{
    make_payloads, measure_zero_copy_telemetry, AccessTelemetry, TestPayload,
};
use myelon::AlignedFixedFrame;

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

/// Aligned zero-copy frame sized for a 64KB ring slot.
/// Header is 16 bytes, leaving 65520 bytes for payload data.
/// Payload data starts at a 16-byte aligned offset, suitable for
/// direct rkyv/flatbuf archived access without alignment-fix copies.
pub(super) type ZcFrame = AlignedFixedFrame<{ 64 * 1024 - 16 }>;
