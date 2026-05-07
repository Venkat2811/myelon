//! Layer 0 multiprocess quick start over a memory-mapped file,
//! **using `disruptor-mp` as a direct dependency** (not via the
//! `myelon` façade).
//!
//! Same shape as [`mmap_disruptor`](./mmap_disruptor.rs) — one
//! producer + one consumer in two real OS processes, mmap-backed
//! ring — but every Layer 0 type comes from `disruptor_mp::*`
//! rather than `myelon::*`.
//!
//! # When to pick this dependency profile
//!
//! Pick `disruptor-mp` directly when:
//!
//! - You only need the raw cross-process ring buffer plus its
//!   coordination, discovery, liveness, and observability primitives,
//!   and you don't want the framing / codec / typed-zero-copy /
//!   topology surface compiled into your binary.
//! - You're publishing your own wire-format crate on top of the
//!   substrate and want a small, stable dependency surface.
//!
//! Pick `myelon` (see [`mmap_disruptor.rs`](./mmap_disruptor.rs))
//! otherwise — `myelon` re-exports every type used here, plus adds
//! the higher layers, so most users only need that one dependency.
//!
//! Run:
//!
//! ```bash
//! cargo run --release -p demos --example disruptor_mp_mmap
//! ```

use demos::{child_role, child_segment, spawn_self, ChildProcessGuard};
use disruptor_mp::{MmapConsumer, MmapProducer, MmapTransportLayout};
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
    let label = format!("dmpmmap_{}", std::process::id());
    println!("[parent] (disruptor-mp direct) mmap label = {label}; publishing {N_EVENTS} events");

    let layout = build_layout(&label);
    layout.ensure_directories()?;
    let mut producer = MmapProducer::<Tick>::create(layout, RING_SLOTS, Tick::default)?;

    let mut consumer_child = ChildProcessGuard::new(spawn_self("consumer", &label)?);

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
    println!("[consumer] (disruptor-mp direct) attaching to mmap label {label} as {CONSUMER_ID}");

    let layout = build_layout(&label);
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
