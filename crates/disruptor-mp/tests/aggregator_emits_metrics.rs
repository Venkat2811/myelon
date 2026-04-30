//! End-to-end test: aggregator pumps `CountersFile` into the
//! `metrics`-rs facade. Uses `metrics_util::debugging::DebuggingRecorder`
//! as the test backend so we can assert exactly what was emitted.
//!
//! RFC 0040 §L3.

#![cfg(feature = "metrics")]

use disruptor_mp::observability::{
    ids, AggregatorConfig, AggregatorHandle, CountersFile, COUNTERS_FILE_RESERVED_BYTES,
    COUNTER_FLAG_PRODUCER,
};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use std::ptr::NonNull;
use std::time::Duration;

#[repr(C, align(64))]
struct AlignedRegion([u8; COUNTERS_FILE_RESERVED_BYTES]);

#[test]
fn aggregator_publishes_counter_values_via_metrics_facade() {
    // Install a `DebuggingRecorder` as the global metrics recorder
    // for this process. `install` may return Err on later runs in the
    // same process (cargo test may share a process across tests) — in
    // that case we still get a usable snapshotter from a fresh
    // recorder, since the global one is already configured.
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _ = recorder.install();

    // Build a fresh counters file.
    let mut buf = Box::new(AlignedRegion([0u8; COUNTERS_FILE_RESERVED_BYTES]));
    let raw = NonNull::new(buf.0.as_mut_ptr()).unwrap();
    let file = unsafe { CountersFile::init(raw) };

    // Register a couple of counters and prime them.
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
    for _ in 0..50 {
        pub_h.inc();
    }
    full_h.add(3);

    // Spawn the aggregator with a tight interval so the test doesn't
    // sleep too long.
    let aggregator = unsafe {
        AggregatorHandle::spawn(
            &file,
            AggregatorConfig {
                interval: Duration::from_millis(10),
                metric_suffix: None,
            },
        )
    };

    // Give it a few ticks.
    std::thread::sleep(Duration::from_millis(80));

    // Stop + join so the final flush runs.
    drop(aggregator);

    let snap = snapshotter.snapshot().into_hashmap();
    let mut found_published = None;
    let mut found_full = None;
    for (key, (_unit, _desc, val)) in snap.into_iter() {
        let name = key.key().name().to_string();
        if let DebugValue::Counter(v) = val {
            if name == "disruptor_mp_events_published" {
                found_published = Some(v);
            } else if name == "disruptor_mp_producer_full_events" {
                found_full = Some(v);
            }
        }
    }

    assert_eq!(found_published, Some(50), "events_published not emitted");
    assert_eq!(found_full, Some(3), "producer_full_events not emitted");
}
