//! Layer 0 multiprocess quick start over a POSIX shared-memory
//! segment.
//!
//! Two real OS processes:
//!
//! - **producer** (parent) creates the SHM segment, publishes a
//!   stream of fixed-size events.
//! - **consumer** (child) attaches under a stable consumer ID,
//!   drains every event, prints what it saw.
//!
//! Run:
//!
//! ```bash
//! cargo run --release -p examples --example shm_disruptor
//! ```
//!
//! ## What this exercises (and what perf-bench does similarly)
//!
//! - [`portable_shm_segment_name`] for macOS-safe segment naming
//!   (PSHMNAMLEN ≤ 31).
//! - [`build_shared_single_producer`] with
//!   [`CoordinationMode::Immediate`] + `discover_consumer_with_prefix`,
//!   then a discovery-scan warmup before publishing — same
//!   sequence used by
//!   `crates/perf-bench/src/layers/raw/disruptor_mp/pingpong_shm.rs`.
//! - [`attach_shared_consumer_with_retry`] on the consumer side to
//!   tolerate the unavoidable producer-creates-segment / consumer-
//!   attaches race window.
//! - [`ChildProcessGuard`] so the child is reaped even if the parent
//!   panics.
//! - `try_consume_next_leased()` + `spin_loop()` on the consumer hot
//!   path — fastest available wait strategy. perf-bench parameterises
//!   this via `--wait-strategy`; for an example we keep it static.

use disruptor_mp::{build_shared_single_producer, portable_shm_segment_name, CoordinationMode};
use examples::{
    attach_shared_consumer_with_retry, child_role, child_segment, spawn_self,
    warm_shared_producer_discovery, ChildProcessGuard, DISCOVERY_SCAN_ROUNDS_1P1C,
};
use std::time::Duration;

const N_EVENTS: u64 = 100_000;
const RING_SLOTS: usize = 4096;
const ATTACH_TIMEOUT: Duration = Duration::from_secs(10);

const CONSUMER_PREFIX: &str = "cp";
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

fn run_parent() -> Result<(), Box<dyn std::error::Error>> {
    let segment = portable_shm_segment_name("shmdemo");
    println!("[parent] segment = {segment}; publishing {N_EVENTS} events to one consumer");

    // Build the producer first so the SHM segment exists before the
    // child tries to attach. `Immediate` returns immediately; the
    // discovery-with-prefix barrier finds the consumer asynchronously
    // once it attaches.
    let mut producer = build_shared_single_producer::<Tick>(&segment, RING_SLOTS)
        .discover_consumer_with_prefix(1, CONSUMER_PREFIX)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(Tick::default)?;

    // Spawn the consumer; guard so a panic below doesn't orphan it.
    let mut consumer_child = ChildProcessGuard::new(spawn_self("consumer", &segment)?);

    // Drive the discovery barrier through a few scan ticks so it
    // registers the just-attached consumer before we publish. Without
    // this, a fast producer can race past an unregistered consumer
    // and collapse backpressure.
    warm_shared_producer_discovery(
        || producer.min_gating_sequence(),
        DISCOVERY_SCAN_ROUNDS_1P1C,
    );

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
    let segment = child_segment();
    println!("[consumer] attaching to {segment} as {CONSUMER_ID}");

    let mut consumer = attach_shared_consumer_with_retry::<Tick>(
        &segment,
        RING_SLOTS,
        CONSUMER_ID,
        ATTACH_TIMEOUT,
    )?;

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
