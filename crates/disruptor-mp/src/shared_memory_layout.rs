//! Shared-memory layout header contract (`LayoutV1`) shared by all multiprocess segments.
//!
//! Every shared-memory segment created by `disruptor-mp` begins with this fixed header.
//! The validator runs before payload access and fails fast on mismatch.

use crate::{MultiProcessError, MultiProcessResult};
use shared_memory::Shmem;
use std::{mem, ptr, slice};

const LAYOUT_MAGIC: [u8; 8] = *b"DSMPTV01";
const CURRENT_LAYOUT_VERSION: u16 = 1;
const HEADER_ALIGNMENT_MIN: usize = 8;
const MIGRATION_HINT: &str =
    "MIGRATION: create a new ring/segment name for LayoutV2 and perform a rolling restart.";

#[repr(C)]
#[derive(Clone, Copy)]
struct LayoutHeader {
    magic: [u8; 8],
    version: u16,
    kind: u16,
    header_size: u16,
    alignment: u16,
    payload_size: u64,
    element_size: u64,
    capacity: u64,
    reserved: u64,
    checksum: u32,
    reserved2: u32,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum SegmentKind {
    RingBuffer = 1,
    Cursor = 2,
}

impl SegmentKind {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::RingBuffer => "ring_buffer",
            Self::Cursor => "cursor",
        }
    }
}

fn segment_kind_from_u16(kind: u16) -> MultiProcessResult<SegmentKind> {
    match kind {
        1 => Ok(SegmentKind::RingBuffer),
        2 => Ok(SegmentKind::Cursor),
        _ => Err(MultiProcessError::IncompatibleLayout(format!(
            "segment kind unknown: {}",
            kind
        ))),
    }
}

#[derive(Debug)]
pub(crate) struct LayoutContract {
    pub payload_offset: usize,
    pub total_size: usize,
}

fn align_up(value: usize, align: usize) -> MultiProcessResult<usize> {
    if align == 0 || !align.is_power_of_two() {
        return Err(MultiProcessError::IncompatibleLayout(
            "layout alignment must be power-of-two".to_string(),
        ));
    }
    Ok((value + align - 1) & !(align - 1))
}

fn checksum32(bytes: &[u8]) -> u32 {
    let mut hash = 0x811C_9DC5u32;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

fn header_size() -> usize {
    mem::size_of::<LayoutHeader>()
}

fn bytes_of_header(header: &LayoutHeader) -> &[u8] {
    // SAFETY: LayoutHeader is plain-old-data and can be hashed byte-wise.
    unsafe {
        slice::from_raw_parts(
            (header as *const LayoutHeader).cast::<u8>(),
            mem::size_of::<LayoutHeader>(),
        )
    }
}

fn is_valid_checksum(header: &LayoutHeader) -> bool {
    let mut copy = *header;
    copy.checksum = 0;
    checksum32(bytes_of_header(&copy)) == header.checksum
}

fn read_header(base: *const u8) -> MultiProcessResult<LayoutHeader> {
    if base.is_null() {
        return Err(MultiProcessError::IncompatibleLayout(
            "shared layout pointer is null".to_string(),
        ));
    }

    let header = unsafe { ptr::read_unaligned(base.cast::<LayoutHeader>()) };
    if header.magic != LAYOUT_MAGIC {
        return Err(MultiProcessError::IncompatibleLayout(format!(
            "layout magic mismatch (expected {:?}, got {:?})",
            LAYOUT_MAGIC, header.magic
        )));
    }
    if header.version != CURRENT_LAYOUT_VERSION {
        return Err(MultiProcessError::IncompatibleLayout(format!(
            "layout version mismatch: got {}, expected {}. {}",
            header.version, CURRENT_LAYOUT_VERSION, MIGRATION_HINT
        )));
    }
    if !is_valid_checksum(&header) {
        return Err(MultiProcessError::IncompatibleLayout(
            "layout checksum failed".to_string(),
        ));
    }
    if usize::from(header.header_size) != header_size() {
        return Err(MultiProcessError::IncompatibleLayout(format!(
            "layout header size mismatch: got {}, expected {}",
            header.header_size,
            header_size()
        )));
    }
    let alignment = usize::from(header.alignment);
    if alignment == 0 || !alignment.is_power_of_two() {
        return Err(MultiProcessError::IncompatibleLayout(
            "layout alignment is invalid".to_string(),
        ));
    }
    Ok(header)
}

fn validate_kind(header_kind: u16, expected: SegmentKind) -> MultiProcessResult<()> {
    let observed_kind = segment_kind_from_u16(header_kind)?;
    if observed_kind != expected {
        return Err(MultiProcessError::IncompatibleLayout(format!(
            "segment kind mismatch: got {}, expected {}",
            observed_kind.name(),
            expected.name()
        )));
    }
    Ok(())
}

fn payload_offset(payload_alignment: usize) -> MultiProcessResult<usize> {
    let align = payload_alignment
        .max(mem::align_of::<LayoutHeader>())
        .max(HEADER_ALIGNMENT_MIN);
    align_up(header_size(), align)
}

fn make_layout_header(
    payload_size: usize,
    element_size: usize,
    capacity: usize,
    payload_alignment: usize,
    kind: SegmentKind,
) -> MultiProcessResult<LayoutHeader> {
    let payload_alignment = payload_alignment
        .max(mem::align_of::<LayoutHeader>())
        .max(HEADER_ALIGNMENT_MIN);

    let mut header = LayoutHeader {
        magic: LAYOUT_MAGIC,
        version: CURRENT_LAYOUT_VERSION,
        kind: kind as u16,
        header_size: u16::try_from(header_size()).map_err(|_| {
            MultiProcessError::IncompatibleLayout("layout header size does not fit u16".to_string())
        })?,
        alignment: u16::try_from(payload_alignment).map_err(|_| {
            MultiProcessError::IncompatibleLayout("layout alignment does not fit u16".to_string())
        })?,
        payload_size: u64::try_from(payload_size).map_err(|_| {
            MultiProcessError::IncompatibleLayout("payload size does not fit u64".to_string())
        })?,
        element_size: u64::try_from(element_size).map_err(|_| {
            MultiProcessError::IncompatibleLayout("element size does not fit u64".to_string())
        })?,
        capacity: u64::try_from(capacity).map_err(|_| {
            MultiProcessError::IncompatibleLayout("capacity does not fit u64".to_string())
        })?,
        reserved: 0,
        checksum: 0,
        reserved2: 0,
    };

    header.checksum = checksum32(bytes_of_header(&header));
    Ok(header)
}

fn make_contract(
    payload_size: usize,
    payload_alignment: usize,
) -> MultiProcessResult<LayoutContract> {
    let payload_offset = payload_offset(payload_alignment)?;
    let total_size = payload_size.checked_add(payload_offset).ok_or_else(|| {
        MultiProcessError::SharedMemoryError("shared memory size overflow".to_string())
    })?;
    Ok(LayoutContract {
        payload_offset,
        total_size,
    })
}

pub(crate) fn required_layout_size(
    payload_size: usize,
    payload_alignment: usize,
) -> MultiProcessResult<usize> {
    make_contract(payload_size, payload_alignment).map(|contract| contract.total_size)
}

pub(crate) fn write_layout(
    shmem: &Shmem,
    payload_size: usize,
    element_size: usize,
    capacity: usize,
    payload_alignment: usize,
    kind: SegmentKind,
) -> MultiProcessResult<LayoutContract> {
    write_layout_bytes(
        shmem.as_ptr().cast::<u8>(),
        shmem.len(),
        payload_size,
        element_size,
        capacity,
        payload_alignment,
        kind,
    )
}

pub(crate) fn write_layout_bytes(
    base: *mut u8,
    region_len: usize,
    payload_size: usize,
    element_size: usize,
    capacity: usize,
    payload_alignment: usize,
    kind: SegmentKind,
) -> MultiProcessResult<LayoutContract> {
    let contract = make_contract(payload_size, payload_alignment)?;
    if region_len < contract.total_size {
        return Err(MultiProcessError::SharedMemoryError(
            "shared segment too small for layout".to_string(),
        ));
    }

    let header = make_layout_header(
        payload_size,
        element_size,
        capacity,
        payload_alignment,
        kind,
    )?;

    let bytes = bytes_of_header(&header);
    unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr(), base, bytes.len());
    }
    Ok(contract)
}

pub(crate) fn validate_layout(
    shmem: &Shmem,
    payload_size: usize,
    element_size: usize,
    capacity: usize,
    payload_alignment: usize,
    kind: SegmentKind,
) -> MultiProcessResult<LayoutContract> {
    validate_layout_bytes(
        shmem.as_ptr().cast::<u8>(),
        shmem.len(),
        payload_size,
        element_size,
        capacity,
        payload_alignment,
        kind,
    )
}

pub(crate) fn validate_layout_bytes(
    base: *const u8,
    region_len: usize,
    payload_size: usize,
    element_size: usize,
    capacity: usize,
    payload_alignment: usize,
    kind: SegmentKind,
) -> MultiProcessResult<LayoutContract> {
    let header = read_header(base)?;
    validate_kind(header.kind, kind)?;

    let expected_payload_size = u64::try_from(payload_size).map_err(|_| {
        MultiProcessError::IncompatibleLayout("payload size does not fit u64".to_string())
    })?;
    if header.payload_size != expected_payload_size {
        return Err(MultiProcessError::IncompatibleLayout(format!(
            "payload size mismatch for {} segment: got {}, expected {}",
            kind.name(),
            header.payload_size,
            expected_payload_size
        )));
    }

    let expected_element_size = u64::try_from(element_size).map_err(|_| {
        MultiProcessError::IncompatibleLayout("element size does not fit u64".to_string())
    })?;
    if header.element_size != expected_element_size {
        return Err(MultiProcessError::IncompatibleLayout(format!(
            "element size mismatch for {} segment: got {}, expected {}",
            kind.name(),
            header.element_size,
            expected_element_size
        )));
    }

    let expected_capacity = u64::try_from(capacity).map_err(|_| {
        MultiProcessError::IncompatibleLayout("capacity does not fit u64".to_string())
    })?;
    if header.capacity != expected_capacity {
        return Err(MultiProcessError::IncompatibleLayout(format!(
            "capacity mismatch for {} segment: got {}, expected {}",
            kind.name(),
            header.capacity,
            expected_capacity
        )));
    }

    let contract = make_contract(
        payload_size,
        payload_alignment.max(usize::from(header.alignment)),
    )?;
    if region_len < contract.total_size {
        return Err(MultiProcessError::IncompatibleLayout(format!(
            "shared segment too small for layout: got {} bytes, expected at least {}",
            region_len, contract.total_size
        )));
    }

    Ok(contract)
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared_memory::ShmemConf;

    fn write_header(shmem: &shared_memory::Shmem, header: &LayoutHeader) {
        let bytes = bytes_of_header(header);
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), shmem.as_ptr().cast::<u8>(), bytes.len());
        }
    }

    fn make_test_header(
        payload_size: usize,
        element_size: usize,
        capacity: usize,
        payload_alignment: usize,
        kind: SegmentKind,
    ) -> (LayoutHeader, LayoutContract) {
        let mut header = make_layout_header(
            payload_size,
            element_size,
            capacity,
            payload_alignment,
            kind,
        )
        .expect("header should be constructible in test");
        let contract = make_contract(
            payload_size,
            payload_alignment
                .max(mem::align_of::<LayoutHeader>())
                .max(HEADER_ALIGNMENT_MIN),
        )
        .expect("contract should be constructible in test");
        header.checksum = 0;
        header.checksum = checksum32(bytes_of_header(&header));
        (header, contract)
    }

    #[test]
    fn unknown_segment_kind_is_rejected() {
        let result = segment_kind_from_u16(3);
        assert!(result.is_err());
        if let Err(MultiProcessError::IncompatibleLayout(message)) = result {
            assert!(message.contains("segment kind unknown"));
        } else {
            panic!("expected IncompatibleLayout");
        }
    }

    #[test]
    fn known_segment_kind_mapping_roundtrip() {
        match segment_kind_from_u16(1) {
            Ok(value) => assert_eq!(value, SegmentKind::RingBuffer),
            Err(_) => panic!("expected known kind"),
        }
        match segment_kind_from_u16(2) {
            Ok(value) => assert_eq!(value, SegmentKind::Cursor),
            Err(_) => panic!("expected known kind"),
        }
    }

    #[test]
    fn validate_layout_rejects_version_mismatch() {
        let payload_size = size_of::<u64>();
        let payload_alignment = align_of::<u64>();
        let element_size = size_of::<u64>();
        let capacity = 8usize;

        let segment_size = required_layout_size(payload_size, payload_alignment)
            .expect("shared memory size should be computable");
        let shmem = ShmemConf::new().size(segment_size).create().unwrap();
        let (mut header, _) = make_test_header(
            payload_size,
            element_size,
            capacity,
            payload_alignment,
            SegmentKind::RingBuffer,
        );
        header.version = 2;
        header.checksum = checksum32(bytes_of_header(&header));
        write_header(&shmem, &header);

        let err = validate_layout(
            &shmem,
            payload_size,
            element_size,
            capacity,
            payload_alignment,
            SegmentKind::RingBuffer,
        )
        .expect_err("version mismatch should fail");
        assert!(matches!(
            err,
            MultiProcessError::IncompatibleLayout(msg) if msg.contains("version")
        ));
    }

    #[test]
    fn validate_layout_rejects_legacy_version_with_migration_hint() {
        let payload_size = size_of::<u64>();
        let payload_alignment = align_of::<u64>();
        let element_size = size_of::<u64>();
        let capacity = 4usize;

        let segment_size = required_layout_size(payload_size, payload_alignment)
            .expect("shared memory size should be computable");
        let shmem = ShmemConf::new().size(segment_size).create().unwrap();
        let (mut header, _) = make_test_header(
            payload_size,
            element_size,
            capacity,
            payload_alignment,
            SegmentKind::RingBuffer,
        );
        header.version = 0;
        header.checksum = checksum32(bytes_of_header(&header));
        write_header(&shmem, &header);

        let error = match validate_layout(
            &shmem,
            payload_size,
            element_size,
            capacity,
            payload_alignment,
            SegmentKind::RingBuffer,
        ) {
            Ok(_) => panic!("legacy version should fail"),
            Err(err) => err,
        };
        match error {
            MultiProcessError::IncompatibleLayout(msg) => {
                assert!(msg.contains("rolling restart"));
            }
            _ => panic!("expected IncompatibleLayout"),
        }
    }

    #[test]
    fn validate_layout_rejects_unknown_kind_mismatch() {
        let payload_size = size_of::<u64>();
        let payload_alignment = align_of::<u64>();
        let element_size = size_of::<u64>();
        let capacity = 8usize;

        let segment_size = required_layout_size(payload_size, payload_alignment)
            .expect("shared memory size should be computable");
        let shmem = ShmemConf::new().size(segment_size).create().unwrap();
        let (mut header, _) = make_test_header(
            payload_size,
            element_size,
            capacity,
            payload_alignment,
            SegmentKind::RingBuffer,
        );
        header.kind = 3;
        header.checksum = checksum32(bytes_of_header(&header));
        write_header(&shmem, &header);

        // Sanity-check that the header write we injected is visible on read-back.
        let observed_kind =
            unsafe { ptr::read_unaligned(shmem.as_ptr().cast::<LayoutHeader>()).kind };
        assert_eq!(observed_kind, 3);

        let err = validate_layout(
            &shmem,
            payload_size,
            element_size,
            capacity,
            payload_alignment,
            SegmentKind::RingBuffer,
        )
        .expect_err("unknown kind should fail");
        if let MultiProcessError::IncompatibleLayout(msg) = err {
            assert!(
                msg.contains("segment kind") || msg.contains("checksum"),
                "unexpected error message: {msg}"
            );
        } else {
            panic!("expected IncompatibleLayout");
        }
    }
}
