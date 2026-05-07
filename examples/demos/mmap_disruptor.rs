//! Layer 0 multiprocess quick start over a memory-mapped file.
//!
//! Mirror of [`shm_disruptor`](./shm_disruptor.rs) but backed by
//! [`MmapTransportLayout`] / [`MmapProducer`] / [`MmapConsumer`]
//! instead of a POSIX shared-memory segment. The wire-level
//! semantics (broadcast, gating, sequence cursors) are identical;
//! only the backing storage differs.
//!
//! Two real OS processes:
//!
//! - **producer** (parent) creates the mmap layout, publishes a
//!   stream of fixed-size events.
//! - **consumer** (child) attaches under a stable consumer ID,
//!   drains every event.
//!
//! Run:
//!
//! ```bash
//! cargo run --release -p demos --example mmap_disruptor
//! ```
//!
//! ## Why mmap over SHM
//!
//! - The region is a regular file, so it survives reboots and is
//!   inspectable / movable / archivable.
//! - Naming uses filesystem paths, not the macOS PSHMNAMLEN budget.
//!
//! ## Pattern
//!
//! Mirrors `crates/perf-bench/src/layers/raw/disruptor_mp/pingpong_mmap.rs`:
//! the producer uses [`MmapProducer::wait_for_consumers_ready`] as
//! its rendezvous primitive (mmap producers expose this directly,
//! unlike the SHM side which leans on
//! `discover_consumer_with_prefix` + a warmup scan).
//! [`ChildProcessGuard`] keeps cleanup honest.

use demos::{child_role, child_segment, spawn_self, ChildProcessGuard};
use myelon::{MmapConsumer, MmapProducer, MmapTransportLayout};
use std::env;
use std::time::Duration;

const N_EVENTS: u64 = 100_000;
const RING_SLOTS: usize = 4096;
const READY_TIMEOUT: Duration = Duration::from_secs(10);

const CONSUMER_ID: &str = "cp_0";

#[derive(Copy, Clone, Default)]
#[repr(C)]
struct Tick {
    sequence: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(role) = child_role() {
        return run_child(&role);
    }
    run_parent()
}

fn build_layout(label: &str) -> MmapTransportLayout {
    let root = env::temp_dir().join("myelon-mmap-example");
    MmapTransportLayout::new(root, label.to_string()).expect("build mmap layout")
}

fn run_parent() -> Result<(), Box<dyn std::error::Error>> {
    let label = format!("mmapdemo_{}", std::process::id());
    println!("[parent] mmap label = {label}; publishing {N_EVENTS} events");

    // Build the producer first so the mmap region exists before the
    // child attempts to attach.
    let layout = build_layout(&label);
    layout.ensure_directories()?;
    let mut producer = MmapProducer::<Tick>::create(layout, RING_SLOTS, Tick::default)?;

    // Spawn child under a guard so a panic below doesn't orphan it.
    let mut consumer_child = ChildProcessGuard::new(spawn_self("consumer", &label)?);

    // Block until the consumer registers with the producer's gating
    // barrier. Returns `false` on timeout.
    if !producer.wait_for_consumers_ready(1, READY_TIMEOUT) {
        return Err("consumer did not attach within 10s".into());
    }

    for sequence in 1..=N_EVENTS {
        producer.publish(|slot| slot.sequence = sequence);
    }
    println!("[parent] published; waiting for consumer to drain and exit");

    let status = consumer_child.wait()?;
    if !status.success() {
        return Err(format!("consumer exited with {status}").into());
    }
    println!("[parent] done");
    Ok(())
}

fn run_child(role: &str) -> Result<(), Box<dyn std::error::Error>> {
    match role {
        "consumer" => run_consumer(),
        other => Err(format!("unknown child role: {other}").into()),
    }
}

fn run_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let label = child_segment();
    println!("[consumer] attaching to mmap label {label} as {CONSUMER_ID}");

    let layout = build_layout(&label);

    // The mmap consumer's `attach` waits internally for the producer
    // to finish creating the layout, so explicit retry-on-attach
    // (used by the SHM example) is not needed here.
    let mut consumer = MmapConsumer::<Tick>::attach(layout, RING_SLOTS, CONSUMER_ID)?;

    let mut delivered = 0u64;
    let mut last = 0u64;
    while delivered < N_EVENTS {
        if let Some(tick) = consumer.try_consume_next_leased() {
            last = tick.sequence;
            delivered += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    println!("[consumer] drained {delivered} events, last sequence = {last}");
    Ok(())
}
