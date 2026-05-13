#![cfg(dst)]
//! DST-style coverage for the counters file (RFC 0040).
//!
//! These tests exercise the counters file under deterministic fault
//! injection (`dst_buggify`) to verify two invariants:
//!
//! 1. **Deterministic totals.** Given a fixed seed, multiple concurrent
//!    writers reach the same final counter values across runs — buggify
//!    yields don't introduce lost or duplicated increments.
//!
//! 2. **Reader sees a consistent view.** A separate "process" attaching
//!    via `CountersFile::attach` reads only valid values (no torn
//!    reads, no garbage labels) regardless of what the writer is doing.
//!
//! Both invariants are checked under `DST_BUGGIFY=1` with a stable
//! `DST_BUGGIFY_SEED` so reruns are identical.
//!
//! The tests are gated behind `#[cfg(dst)]` to align with
//! the rest of the DST test suite.

#![cfg(dst)]

use disruptor_mp::observability::{
    ids, AttachError, CountersFile, COUNTERS_FILE_RESERVED_BYTES, COUNTER_FLAG_CONSUMER,
    COUNTER_FLAG_PRODUCER,
};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

const ITERATIONS: usize = 4096;
const WRITER_THREADS: usize = 4;

#[repr(C, align(64))]
struct AlignedRegion([u8; COUNTERS_FILE_RESERVED_BYTES]);

fn fresh_aligned_region() -> Box<AlignedRegion> {
    Box::new(AlignedRegion([0u8; COUNTERS_FILE_RESERVED_BYTES]))
}

/// Address of the aligned region encoded as `usize` so it crosses
/// `Send` boundaries cleanly. The tests own the `Box<AlignedRegion>`
/// for the full lifetime of all writers; threads reconstruct
/// `NonNull<u8>` from the address inside the worker closure.
#[derive(Copy, Clone)]
struct RegionPtr(usize);

impl RegionPtr {
    fn into_nonnull(self) -> NonNull<u8> {
        // SAFETY: address came from `Box::as_mut_ptr`; the tests
        // ensure the box outlives every thread that uses this value.
        unsafe { NonNull::new_unchecked(self.0 as *mut u8) }
    }
}

/// Buggify-perturbed concurrent writers must not lose increments —
/// final counter totals match the deterministic expected sums.
#[test]
fn counters_file_totals_are_deterministic_under_buggify() {
    let mut buf = fresh_aligned_region();
    let region = RegionPtr(buf.0.as_mut_ptr() as usize);
    let _file = unsafe { CountersFile::init(region.into_nonnull()) };

    // Single shared register: each writer thread re-attaches to the
    // same counters file (mirrors how a real producer/consumer process
    // pair would use it) and registers its own per-thread tag for
    // independent verification.
    let writer_done = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for tid in 0..WRITER_THREADS {
        let writer_done = Arc::clone(&writer_done);
        handles.push(thread::spawn(move || {
            let view =
                unsafe { CountersFile::attach(region.into_nonnull()) }.expect("writer attach");
            let pub_h = view
                .register(
                    ids::EVENTS_PUBLISHED + tid as u32,
                    COUNTER_FLAG_PRODUCER,
                    &format!("events_published_t{tid}"),
                )
                .expect("register events_published_t");
            let con_h = view
                .register(
                    ids::EVENTS_CONSUMED + tid as u32,
                    COUNTER_FLAG_CONSUMER,
                    &format!("events_consumed_t{tid}"),
                )
                .expect("register events_consumed_t");
            for i in 0..ITERATIONS {
                pub_h.inc();
                con_h.inc();
                // Buggify-driven yields exercise scheduling-induced
                // interleavings between threads; counter discipline
                // (relaxed atomics) must absorb them losslessly.
                if disruptor_mp::dst::buggify::buggify(file!(), line!()) {
                    thread::yield_now();
                }
                if i % 64 == 0 {
                    // Mid-loop snapshot from the writer thread itself —
                    // the values it observes for its own counters must
                    // be monotonic.
                    let v = pub_h.get();
                    assert!(v <= (i + 1) as u64);
                }
            }
            writer_done.fetch_add(1, Ordering::Release);
        }));
    }

    for h in handles {
        h.join().expect("writer thread join");
    }
    assert_eq!(writer_done.load(Ordering::Acquire), WRITER_THREADS);

    // External attach reads — no race here because all writers are
    // joined; counter values are stable monotonic totals.
    let reader = unsafe { CountersFile::attach(region.into_nonnull()) }.expect("reader attach");
    let snap = reader.snapshot();

    // Each writer registered two slots; we expect 2*WRITER_THREADS in
    // the snapshot.
    let mut producer_slots = 0;
    let mut consumer_slots = 0;
    for c in &snap {
        if c.flags & COUNTER_FLAG_PRODUCER != 0 {
            producer_slots += 1;
            assert_eq!(
                c.value, ITERATIONS as u64,
                "{} = {} (expected {ITERATIONS})",
                c.label, c.value
            );
        }
        if c.flags & COUNTER_FLAG_CONSUMER != 0 {
            consumer_slots += 1;
            assert_eq!(
                c.value, ITERATIONS as u64,
                "{} = {} (expected {ITERATIONS})",
                c.label, c.value
            );
        }
    }
    assert_eq!(producer_slots, WRITER_THREADS);
    assert_eq!(consumer_slots, WRITER_THREADS);
}

/// While writers are mid-flight, an external reader's `snapshot()` must
/// return only valid records — no torn reads, no garbled labels.
#[test]
fn external_reader_sees_consistent_snapshot_during_writes() {
    let mut buf = fresh_aligned_region();
    let region = RegionPtr(buf.0.as_mut_ptr() as usize);
    let _file = unsafe { CountersFile::init(region.into_nonnull()) };

    // Spawn writers that keep incrementing.
    let stop = Arc::new(AtomicUsize::new(0));
    let mut writers = Vec::new();
    for tid in 0..WRITER_THREADS {
        let stop = Arc::clone(&stop);
        writers.push(thread::spawn(move || {
            let view =
                unsafe { CountersFile::attach(region.into_nonnull()) }.expect("writer attach");
            let h = view
                .register(
                    ids::EVENTS_PUBLISHED + tid as u32,
                    COUNTER_FLAG_PRODUCER,
                    &format!("hot_t{tid}"),
                )
                .expect("register");
            while stop.load(Ordering::Relaxed) == 0 {
                h.inc();
                if disruptor_mp::dst::buggify::buggify(file!(), line!()) {
                    thread::yield_now();
                }
            }
        }));
    }

    // Reader thread: take repeated snapshots while writers are running.
    // Every observed counter must have a sane label (valid UTF-8, the
    // expected `hot_t<n>` shape) and a non-decreasing value.
    let reader_region = region;
    let stop_for_reader = Arc::clone(&stop);
    let reader = thread::spawn(move || {
        let view =
            unsafe { CountersFile::attach(reader_region.into_nonnull()) }.expect("reader attach");
        let mut last_seen: std::collections::HashMap<u32, u64> = Default::default();
        for _ in 0..256 {
            let snap = view.snapshot();
            for c in snap {
                assert!(
                    c.label.starts_with("hot_t"),
                    "label corruption: {:?}",
                    c.label
                );
                let prev = last_seen.entry(c.id).or_insert(0);
                assert!(
                    c.value >= *prev,
                    "monotonic violation: {} < {}",
                    c.value,
                    *prev
                );
                *prev = c.value;
            }
            if disruptor_mp::dst::buggify::buggify(file!(), line!()) {
                thread::yield_now();
            }
        }
        stop_for_reader.store(1, Ordering::Release);
    });

    reader.join().expect("reader thread join");
    for w in writers {
        w.join().expect("writer thread join");
    }
}

/// Magic-word validation rejects an uninitialised region — useful
/// for catching DST scenarios where a process attaches before the
/// owner has zeroed and initialised the segment.
#[test]
fn attach_before_init_returns_bad_magic() {
    let mut buf = fresh_aligned_region();
    let region = RegionPtr(buf.0.as_mut_ptr() as usize);
    let err = unsafe { CountersFile::attach(region.into_nonnull()) }.unwrap_err();
    match err {
        AttachError::BadMagic(0) => {}
        other => panic!("expected BadMagic(0) before init, got {other:?}"),
    }
}
