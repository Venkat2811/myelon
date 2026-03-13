use disruptor_mp::{MultiProcessError, SharedCursor, SharedMemoryConfig, SharedRingBuffer};
use shared_memory::ShmemConf;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_name(prefix: &str) -> String {
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be valid")
        .as_nanos();
    format!("{prefix}_{pid}_{nanos}")
}

#[test]
fn ring_buffer_layout_rejects_element_size_mismatch() {
    let name = unique_name("layout_ring_sz");
    let create_config = SharedMemoryConfig {
        name: name.clone(),
        buffer_size: 8,
        element_size: std::mem::size_of::<u64>(),
        create: true,
    };

    let _producer = SharedRingBuffer::<u64>::new(create_config.clone(), || 0u64).unwrap();

    let attach_bad = match SharedRingBuffer::<u64>::attach(SharedMemoryConfig {
        name,
        buffer_size: 8,
        element_size: std::mem::size_of::<u32>(),
        create: false,
    }) {
        Ok(_) => panic!("attach must reject mismatched element_size"),
        Err(err) => err,
    };

    match attach_bad {
        MultiProcessError::IncompatibleLayout(msg) => {
            assert!(msg.contains("element size mismatch"));
        }
        _ => panic!("expected IncompatibleLayout, got {attach_bad:?}"),
    }
}

#[test]
fn ring_buffer_layout_rejects_corrupted_magic() {
    let name = unique_name("layout_ring_magic");
    let create_config = SharedMemoryConfig {
        name: name.clone(),
        buffer_size: 8,
        element_size: std::mem::size_of::<u64>(),
        create: true,
    };

    let _ring = SharedRingBuffer::<u64>::new(create_config.clone(), || 0u64).unwrap();

    let shmem = ShmemConf::new()
        .os_id(&name)
        .open()
        .expect("shared segment should exist");

    // Corrupt header magic so future attach validates against layout failure.
    unsafe {
        *shmem.as_ptr().cast::<u8>() = 0x00;
        *shmem.as_ptr().cast::<u8>().add(1) = 0x00;
    }

    let attach_err = match SharedRingBuffer::<u64>::attach(create_config) {
        Ok(_) => panic!("attach should fail"),
        Err(err) => err,
    };
    match attach_err {
        MultiProcessError::IncompatibleLayout(msg) => {
            assert!(msg.contains("magic"));
        }
        _ => panic!("expected IncompatibleLayout, got {attach_err:?}"),
    }
}

#[test]
fn cursor_and_ring_layout_kind_isolated() {
    let name = unique_name("layout_kind_cross");
    let create_config = SharedMemoryConfig {
        name: name.clone(),
        buffer_size: 8,
        element_size: std::mem::size_of::<u64>(),
        create: true,
    };
    let _ring = SharedRingBuffer::<u64>::new(create_config, || 0u64).unwrap();

    let cursor_err = match SharedCursor::attach(&name) {
        Ok(_) => panic!("cursor attach must reject ring layout"),
        Err(err) => err,
    };
    match cursor_err {
        MultiProcessError::IncompatibleLayout(msg) => assert!(msg.contains("segment kind")),
        _ => panic!("expected IncompatibleLayout, got {cursor_err:?}"),
    }
}

#[test]
fn ring_buffer_layout_rejects_version_regression() {
    let name = unique_name("layout_ring_version");
    let create_config = SharedMemoryConfig {
        name: name.clone(),
        buffer_size: 4,
        element_size: std::mem::size_of::<u64>(),
        create: true,
    };

    let _producer = SharedRingBuffer::<u64>::new(create_config.clone(), || 0u64).unwrap();
    let shmem = ShmemConf::new()
        .os_id(&name)
        .open()
        .expect("shared segment should exist");

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct RawLayoutHeader {
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

    fn checksum32(bytes: &[u8]) -> u32 {
        let mut hash = 0x811C_9DC5u32;
        for byte in bytes {
            hash ^= u32::from(*byte);
            hash = hash.wrapping_mul(0x0100_0193);
        }
        hash
    }

    fn header_bytes(header: &RawLayoutHeader) -> &[u8] {
        // SAFETY: `RawLayoutHeader` is plain-old-data and plain-byte layout is stable under `repr(C)`.
        unsafe {
            std::slice::from_raw_parts(
                (header as *const RawLayoutHeader).cast::<u8>(),
                std::mem::size_of::<RawLayoutHeader>(),
            )
        }
    }

    // Flip layout version while keeping checksum valid so version mismatch path is exercised.
    unsafe {
        let header_ptr = shmem.as_ptr().cast::<RawLayoutHeader>();
        let mut header = header_ptr.read_unaligned();
        header.version = 0;
        header.checksum = 0;
        header.checksum = checksum32(header_bytes(&header));
        header_ptr.write_unaligned(header);
    }

    let attach_err = match SharedRingBuffer::<u64>::attach(create_config) {
        Ok(_) => panic!("attach must reject stale layout version"),
        Err(err) => err,
    };
    match attach_err {
        MultiProcessError::IncompatibleLayout(msg) => {
            assert!(msg.contains("version"));
            assert!(msg.contains("rolling restart"));
        }
        _ => panic!("expected IncompatibleLayout, got {attach_err:?}"),
    }
}
