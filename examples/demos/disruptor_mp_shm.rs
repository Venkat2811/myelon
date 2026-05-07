//! Layer 0 multiprocess quick start over SHM, **using `disruptor-mp`
//! as a direct dependency** (not via the `myelon` façade).
//!
//! Same shape as [`shm_disruptor`](./shm_disruptor.rs) — one
//! producer + one consumer in two real OS processes — but every
//! Layer 0 type comes from `disruptor_mp::*` rather than
//! `myelon::*`.
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
//! Pick `myelon` (see [`shm_disruptor.rs`](./shm_disruptor.rs))
//! otherwise — `myelon` re-exports every type used here, plus adds
//! the higher layers, so most users only need that one dependency.
//!
//! Run:
//!
//! ```bash
//! cargo run --release -p demos --example disruptor_mp_shm
//! ```

use demos::{
    attach_shared_consumer_with_retry, child_role, child_segment, spawn_self,
    warm_shared_producer_discovery, ChildProcessGuard, DISCOVERY_SCAN_ROUNDS_1P1C,
};
use disruptor_mp::{build_shared_single_producer, portable_shm_segment_name, CoordinationMode};
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
    let segment = portable_shm_segment_name("dmpshm");
    println!(
        "[parent] (disruptor-mp direct) segment = {segment}; \
         publishing {N_EVENTS} events to one consumer"
    );

    let mut producer = build_shared_single_producer::<Tick>(&segment, RING_SLOTS)
        .discover_consumer_with_prefix(1, CONSUMER_PREFIX)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(Tick::default)?;

    let mut consumer_child = ChildProcessGuard::new(spawn_self("consumer", &segment)?);

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
    println!("[consumer] (disruptor-mp direct) attaching to {segment} as {CONSUMER_ID}");

    // `attach_shared_consumer_with_retry` returns a `SharedConsumer<Tick>`
    // — the same type whether reached via `disruptor_mp::SharedConsumer`
    // or `myelon::SharedConsumer`. The type identity is the reason the
    // helper crate compiles unchanged for both dependency profiles.
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
