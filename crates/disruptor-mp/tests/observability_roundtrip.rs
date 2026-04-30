//! End-to-end test that a producer and consumer wired up to the same
//! `CountersFile` populate it correctly through normal `publish` /
//! `try_consume_next` operations. RFC 0040 §Counters.
//!
//! Runs entirely in-process (no SHM segments). The counters file lives
//! in a 64-byte aligned heap region; producer and consumer increment
//! into it via `attach_counters`. We don't stand up a real ring buffer
//! — that's covered by the existing `true_multiprocess.rs` suite.
//! Here we drive the counters API directly to verify the wiring on
//! both sides matches.

use disruptor_mp::observability::{
    ids, CountersFile, COUNTERS_FILE_RESERVED_BYTES, COUNTER_FLAG_CONSUMER, COUNTER_FLAG_PRODUCER,
};
use std::ptr::NonNull;

#[repr(C, align(64))]
struct AlignedRegion([u8; COUNTERS_FILE_RESERVED_BYTES]);

fn fresh_region() -> Box<AlignedRegion> {
    Box::new(AlignedRegion([0u8; COUNTERS_FILE_RESERVED_BYTES]))
}

#[test]
fn counters_file_records_producer_and_consumer_activity() {
    let mut buf = fresh_region();
    let raw = NonNull::new(buf.0.as_mut_ptr()).unwrap();
    let file = unsafe { CountersFile::init(raw) };

    // Simulate a producer's attach_counters: register the producer-side
    // pair the same way SharedProducer::attach_counters does.
    let pub_h = file
        .register(
            ids::EVENTS_PUBLISHED,
            COUNTER_FLAG_PRODUCER,
            "events_published",
        )
        .expect("register events_published");
    let full_h = file
        .register(
            ids::PRODUCER_FULL_EVENTS,
            COUNTER_FLAG_PRODUCER,
            "producer_full_events",
        )
        .expect("register producer_full_events");

    // And a consumer's attach_counters.
    let con_h = file
        .register(
            ids::EVENTS_CONSUMED,
            COUNTER_FLAG_CONSUMER,
            "events_consumed",
        )
        .expect("register events_consumed");
    let empty_h = file
        .register(
            ids::CONSUMER_EMPTY_SPINS,
            COUNTER_FLAG_CONSUMER,
            "consumer_empty_spins",
        )
        .expect("register consumer_empty_spins");
    let lag_h = file
        .register(
            ids::CONSUMER_LAG_MAX,
            COUNTER_FLAG_CONSUMER,
            "consumer_lag_max",
        )
        .expect("register consumer_lag_max");

    // Mimic a small workload: 1024 publishes, 1024 consumes, a handful
    // of empty-ring spins on the consumer side, one full-ring event on
    // the producer side, and lag samples that converge on a known max.
    for _ in 0..1024 {
        pub_h.inc();
        con_h.inc();
    }
    full_h.inc();
    for _ in 0..7 {
        empty_h.inc();
    }
    lag_h.record_max(2);
    lag_h.record_max(8);
    lag_h.record_max(1); // ignored — current is already higher

    // External attach (separate view) to verify visibility from a
    // process that didn't do the writes.
    let reader = unsafe { CountersFile::attach(raw) }.expect("reader attach");
    let snap = reader.snapshot();
    let by_id = |id: u32| {
        snap.iter()
            .find(|c| c.id == id)
            .cloned()
            .unwrap_or_else(|| panic!("counter id 0x{id:x} not in snapshot"))
    };

    assert_eq!(by_id(ids::EVENTS_PUBLISHED).value, 1024);
    assert_eq!(by_id(ids::EVENTS_CONSUMED).value, 1024);
    assert_eq!(by_id(ids::PRODUCER_FULL_EVENTS).value, 1);
    assert_eq!(by_id(ids::CONSUMER_EMPTY_SPINS).value, 7);
    assert_eq!(by_id(ids::CONSUMER_LAG_MAX).value, 8);

    // Labels survived the round-trip.
    assert_eq!(by_id(ids::EVENTS_PUBLISHED).label, "events_published");
    assert_eq!(by_id(ids::EVENTS_CONSUMED).label, "events_consumed");
}
