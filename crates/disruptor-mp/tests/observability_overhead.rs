//! Lightweight overhead probe for the counters file (RFC 0040 §Cost
//! model).
//!
//! Times two single-process hot loops:
//!
//!   1. baseline — empty atomic counter increment, no observability
//!   2. with_inc — `CounterHandle::inc()` (relaxed atomic + label
//!      indirection through the counters file)
//!
//! Prints both timings and the ratio. Asserts only a generous upper
//! bound (`with_inc < 5× baseline`) so the test isn't CI-flaky on
//! contended machines but still catches a regression that turns the
//! ~1 ns increment into something pathological.
//!
//! Kept as a `#[test]` rather than a criterion benchmark so it runs
//! in normal `cargo test` without extra setup. Single-process only —
//! the production hot path is also single-writer-per-counter.

use disruptor_mp::observability::{
    ids, CountersFile, COUNTERS_FILE_RESERVED_BYTES, COUNTER_FLAG_PRODUCER,
};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const ITERATIONS: u64 = 2_000_000;

#[repr(C, align(64))]
struct AlignedRegion([u8; COUNTERS_FILE_RESERVED_BYTES]);

#[test]
fn counter_handle_inc_is_within_5x_of_a_plain_atomic_increment() {
    // Baseline: a plain `AtomicU64::fetch_add(Relaxed)` in a tight loop.
    // Represents the lower bound — what an inlined inc costs without
    // any indirection through the counters file.
    let baseline_atomic = AtomicU64::new(0);
    let baseline_start = Instant::now();
    for _ in 0..ITERATIONS {
        baseline_atomic.fetch_add(1, Ordering::Relaxed);
    }
    let baseline = baseline_start.elapsed();
    assert_eq!(baseline_atomic.load(Ordering::Relaxed), ITERATIONS);

    // Subject: `CounterHandle::inc()` — what a producer / consumer hot
    // path actually pays for an attached counter.
    let mut buf = Box::new(AlignedRegion([0u8; COUNTERS_FILE_RESERVED_BYTES]));
    let raw = NonNull::new(buf.0.as_mut_ptr()).unwrap();
    let file = unsafe { CountersFile::init(raw) };
    let handle = file
        .register(
            ids::EVENTS_PUBLISHED,
            COUNTER_FLAG_PRODUCER,
            "events_published",
        )
        .expect("register");

    let inc_start = Instant::now();
    for _ in 0..ITERATIONS {
        handle.inc();
    }
    let with_inc = inc_start.elapsed();
    assert_eq!(handle.get(), ITERATIONS);

    let baseline_ns_per_op = baseline.as_nanos() as f64 / ITERATIONS as f64;
    let inc_ns_per_op = with_inc.as_nanos() as f64 / ITERATIONS as f64;
    let ratio = inc_ns_per_op / baseline_ns_per_op.max(0.001);

    eprintln!(
        "observability_overhead: baseline={:.2}ns/op  CounterHandle::inc={:.2}ns/op  ratio={:.2}x",
        baseline_ns_per_op, inc_ns_per_op, ratio
    );

    // Generous upper bound to avoid CI flakiness while still catching
    // a pathological regression. RFC 0040's claimed cost is ~1 ns
    // relaxed atomic; the indirection through the slot pointer adds
    // a single `*mut CounterSlot` deref, which on real hardware is
    // negligible. Allow up to 5× before failing.
    assert!(
        ratio < 5.0,
        "CounterHandle::inc {:.2}ns/op is more than 5× the baseline {:.2}ns/op",
        inc_ns_per_op,
        baseline_ns_per_op
    );
}
