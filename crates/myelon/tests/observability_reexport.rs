//! Verify the `myelon::observability` re-export exposes the
//! `disruptor-mp` counters API without forcing downstream users to take
//! a direct `disruptor-mp` dependency. RFC 0040 §Public API.

use myelon::observability::{
    ids, CountersFile, COUNTERS_FILE_RESERVED_BYTES, COUNTER_FLAG_PRODUCER,
};
use std::ptr::NonNull;

#[repr(C, align(64))]
struct AlignedRegion([u8; COUNTERS_FILE_RESERVED_BYTES]);

#[test]
fn reexport_init_register_inc_snapshot_roundtrip() {
    let mut buf = Box::new(AlignedRegion([0u8; COUNTERS_FILE_RESERVED_BYTES]));
    let raw = NonNull::new(buf.0.as_mut_ptr()).unwrap();
    let file = unsafe { CountersFile::init(raw) };

    let h = file
        .register(
            ids::EVENTS_PUBLISHED,
            COUNTER_FLAG_PRODUCER,
            "events_published",
        )
        .expect("register");
    for _ in 0..42 {
        h.inc();
    }
    let snap = file.snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].id, ids::EVENTS_PUBLISHED);
    assert_eq!(snap[0].value, 42);
    assert_eq!(snap[0].label, "events_published");
}
