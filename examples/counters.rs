//! End-to-end observability example: producer + consumer in one
//! process, both attached to the same RFC-0040 counters file.
//!
//! Demonstrates:
//!
//! - [`myelon::observability`] — the re-export of
//!   `disruptor_mp::observability` so a `myelon`-only dependency is
//!   sufficient.
//! - [`myelon::SharedProducer::attach_counters`] /
//!   [`myelon::SharedConsumer::attach_counters`] — the per-process
//!   counters wiring that lights up
//!   `events_published`, `events_consumed`, `producer_full_events`,
//!   `consumer_empty_spins`, and `consumer_lag_max` on the hot path.
//!
//! For brevity the example runs producer and consumer threads inside
//! one process. The counters file would normally live in a SHM
//! segment shared by separate producer / consumer processes — see the
//! `crates/perf-bench/src/layers/raw/disruptor_mp/pingpong_shm.rs`
//! `--enable-counters` path for the multiprocess wiring.
//!
//! Run:
//!
//! ```bash
//! cargo run --release -p myelon --example counters
//! ```

use disruptor_mp::portable_shm_segment_name;
use myelon::observability::{
    ids, AttachError, CountersFile, COUNTERS_FILE_RESERVED_BYTES, COUNTER_FLAG_CONSUMER,
    COUNTER_FLAG_PRODUCER,
};
use myelon::producer::CoordinationMode;
use myelon::{attach_shared_consumer, build_shared_single_producer};
use std::ptr::NonNull;
use std::thread;
use std::time::Duration;

const RING_SLOTS: usize = 4096;
const N_EVENTS: u64 = 250_000;

#[derive(Copy, Clone, Default)]
#[repr(C)]
struct Tick {
    sequence: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Allocate a counters region. In a real multiprocess setup
    //    this would be backed by a SHM segment shared with the
    //    consumer process; here we leak a heap-aligned buffer so the
    //    pointer satisfies `CountersFile::init`'s 'static-lifetime
    //    contract for the duration of the demo.
    let counters_file = leak_static_counters_file();

    // 2. Build the producer and attach the producer-side counters.
    //    `portable_shm_segment_name` derives a macOS-safe segment name
    //    (PSHMNAMLEN ≤ 31) from the user-supplied label.
    let segment = portable_shm_segment_name("counters_demo");
    let mut producer = build_shared_single_producer::<Tick>(&segment, RING_SLOTS)
        .discover_consumer_with_prefix(1, "cp")
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(Tick::default)?;
    producer.attach_counters(counters_file);

    // 3. Spawn the consumer (in a real setup this would be a separate
    //    process attaching the same SHM segment + counters file).
    let consumer_segment = segment.clone();
    let consumer_handle = thread::spawn(move || {
        let mut consumer = attach_shared_consumer::<Tick>(&consumer_segment, RING_SLOTS)
            .with_consumer_id("cp_0")
            .build_consumer()
            .expect("attach consumer");
        consumer.attach_counters(counters_file);

        let mut last_seq = 0u64;
        let mut delivered = 0u64;
        while delivered < N_EVENTS {
            if let Some(tick) = consumer.try_consume_next_leased() {
                last_seq = tick.sequence;
                delivered += 1;
            } else {
                std::hint::spin_loop();
            }
        }
        last_seq
    });

    // 4. Publish.
    for sequence in 1..=N_EVENTS {
        producer.publish(|slot| slot.sequence = sequence);
    }

    let last_seen = consumer_handle.join().expect("consumer thread");
    assert_eq!(last_seen, N_EVENTS);

    // 5. Snapshot the counters file (the reader-side, attach view).
    //    A separate `myelon-stat`-style reader process would do this
    //    against the SHM segment without touching the producer or
    //    consumer code paths.
    let snapshot = counters_file.snapshot();
    println!("RFC-0040 counters after {} events:", N_EVENTS);
    println!("{:<28}  {:>10}  flags", "label", "value");
    for slot in snapshot {
        let mut flag_str = String::new();
        if slot.flags & COUNTER_FLAG_PRODUCER != 0 {
            flag_str.push_str("producer");
        }
        if slot.flags & COUNTER_FLAG_CONSUMER != 0 {
            if !flag_str.is_empty() {
                flag_str.push('|');
            }
            flag_str.push_str("consumer");
        }
        println!("{:<28}  {:>10}  {}", slot.label, slot.value, flag_str);
    }

    // Spot-check: a counter id we know was registered.
    let _ = ids::EVENTS_PUBLISHED;
    let _ = ids::EVENTS_CONSUMED;

    // Reattach demo: a separate reader could open the same memory
    // and walk the registered slots. Here we exercise the safe path.
    let _attach_demo: Result<CountersFile, AttachError> =
        unsafe { Ok(CountersFile::attach(counters_file_ptr())?) };

    // Brief pause so any async aggregator (when wired via the
    // `metrics-rs` facade) gets a tick.
    thread::sleep(Duration::from_millis(50));
    Ok(())
}

// ---------------------------------------------------------------------------
// Allocation helpers
// ---------------------------------------------------------------------------

#[repr(C, align(64))]
struct AlignedRegion([u8; COUNTERS_FILE_RESERVED_BYTES]);

static mut REGION_PTR: Option<NonNull<u8>> = None;

/// Allocate a cache-line-aligned region big enough for a counters
/// file, leak it for the process's lifetime, and return a
/// `'static CountersFile` view over it. This pattern matches what
/// `perf-bench --enable-counters` does at bench startup.
fn leak_static_counters_file() -> &'static CountersFile {
    let leaked: &'static mut AlignedRegion =
        Box::leak(Box::new(AlignedRegion([0u8; COUNTERS_FILE_RESERVED_BYTES])));
    let ptr = NonNull::new(leaked.0.as_mut_ptr()).expect("Box::leak yields non-null");
    unsafe {
        REGION_PTR = Some(ptr);
    }
    let file = unsafe { CountersFile::init(ptr) };
    Box::leak(Box::new(file))
}

fn counters_file_ptr() -> NonNull<u8> {
    unsafe { REGION_PTR.expect("counters file initialised") }
}
