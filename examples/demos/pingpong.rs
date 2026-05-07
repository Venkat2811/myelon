//! Multiprocess request/response RTT example.
//!
//! Two real OS processes connected by two SHM ring buffers, one per
//! direction:
//!
//! - **parent**       publishes pings on the `ping` ring, consumes
//!   pongs from the `pong` ring, measures round-trip time.
//! - **echo (child)** consumes pings, echoes them back as pongs on
//!   the matching ring.
//!
//! Run:
//!
//! ```bash
//! cargo run --release -p demos --example pingpong
//! ```
//!
//! ## Pattern
//!
//! Mirrors the rendezvous sequence from
//! `crates/perf-bench/src/layers/raw/disruptor_mp/pingpong_shm.rs` —
//! the bench-grade reference implementation — without bringing in
//! `UnifiedCoordination` or `LatencyRecorder`:
//!
//! 1. Parent creates the ping producer (segment exists).
//! 2. Parent spawns the echo child under [`ChildProcessGuard`].
//! 3. Parent warms its discovery barrier
//!    ([`warm_shared_producer_discovery`]) so the just-attached child
//!    consumer is registered before publish begins.
//! 4. Parent attaches as the pong consumer with bounded retry
//!    ([`attach_shared_consumer_with_retry`]).
//! 5. Child does the symmetric setup on its half of the rings.
//!
//! Two SHM segments are pre-rendered in the parent via
//! [`portable_shm_segment_name`] (which adds a per-call salt) and
//! passed verbatim to the child through the segment env so both
//! sides agree on the exact strings.

use demos::{
    attach_shared_consumer_with_retry, child_role, child_segment, spawn_self,
    warm_shared_producer_discovery, ChildProcessGuard, DISCOVERY_SCAN_ROUNDS_1P1C,
};
use myelon::{build_shared_single_producer, portable_shm_segment_name, CoordinationMode};
use std::time::{Duration, Instant};

const N_ROUND_TRIPS: u64 = 10_000;
const WARMUP_ROUND_TRIPS: u64 = 1_000;
const RING_SLOTS: usize = 4096;
const ATTACH_TIMEOUT: Duration = Duration::from_secs(10);

const CONSUMER_PREFIX: &str = "cp";
const CONSUMER_ID: &str = "cp_0";

#[derive(Copy, Clone, Default)]
#[repr(C)]
struct PingPongEvent {
    /// Monotonic sequence number, matched on the way back.
    sequence: u64,
    /// Sender-side wall clock at publish, used for RTT measurement
    /// once the matching pong returns.
    sent_ns: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(role) = child_role() {
        return run_child(&role);
    }
    run_parent()
}

fn now_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos() as u64
}

fn split_segments(joined: &str) -> (String, String) {
    let mut parts = joined.split('|');
    let ping = parts.next().expect("ping segment").to_string();
    let pong = parts.next().expect("pong segment").to_string();
    (ping, pong)
}

fn run_parent() -> Result<(), Box<dyn std::error::Error>> {
    // Render both segment names once in the parent and pass them to
    // the child verbatim. `portable_shm_segment_name` adds a per-call
    // salt, so re-deriving in the child would yield different names.
    let ping_seg = portable_shm_segment_name("pppi");
    let pong_seg = portable_shm_segment_name("pppo");
    let joined = format!("{ping_seg}|{pong_seg}");
    println!("[parent] ping={ping_seg} pong={pong_seg}");

    // 1. Build the ping producer first so the segment exists.
    let mut ping_producer = build_shared_single_producer::<PingPongEvent>(&ping_seg, RING_SLOTS)
        .discover_consumer_with_prefix(1, CONSUMER_PREFIX)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(PingPongEvent::default)?;

    // 2. Spawn the echo child under a guard.
    let mut echo_child = ChildProcessGuard::new(spawn_self("echo", &joined)?);

    // 3. Warm the discovery barrier so the child's ping_consumer
    //    registers before we start publishing.
    warm_shared_producer_discovery(
        || ping_producer.min_gating_sequence(),
        DISCOVERY_SCAN_ROUNDS_1P1C,
    );

    // 4. Attach as the pong consumer (the child has by now created
    //    the pong segment and is warming its own discovery barrier).
    let mut pong_consumer = attach_shared_consumer_with_retry::<PingPongEvent>(
        &pong_seg,
        RING_SLOTS,
        CONSUMER_ID,
        ATTACH_TIMEOUT,
    )?;

    // Warmup so we don't measure first-message page faults / cold
    // cache lines.
    for sequence in 1..=WARMUP_ROUND_TRIPS {
        ping_producer.publish(|slot| {
            slot.sequence = sequence;
            slot.sent_ns = now_ns();
        });
        loop {
            if let Some(pong) = pong_consumer.try_consume_next_leased() {
                debug_assert_eq!(pong.sequence, sequence);
                break;
            }
            std::hint::spin_loop();
        }
    }
    println!("[parent] warmup complete; measuring {N_ROUND_TRIPS} round trips");

    let mut total_rtt_ns: u128 = 0;
    let start = Instant::now();
    for sequence in 1..=N_ROUND_TRIPS {
        let send_ns = now_ns();
        ping_producer.publish(|slot| {
            slot.sequence = WARMUP_ROUND_TRIPS + sequence;
            slot.sent_ns = send_ns;
        });
        let recv_ns = loop {
            if let Some(pong) = pong_consumer.try_consume_next_leased() {
                debug_assert_eq!(pong.sequence, WARMUP_ROUND_TRIPS + sequence);
                break now_ns();
            }
            std::hint::spin_loop();
        };
        total_rtt_ns += u128::from(recv_ns - send_ns);
    }
    let elapsed = start.elapsed();

    let avg_rtt_ns = (total_rtt_ns / u128::from(N_ROUND_TRIPS)) as u64;
    println!(
        "[parent] {N_ROUND_TRIPS} round trips in {elapsed:?}: \
         avg RTT = {avg_rtt_ns} ns ({:.2} M round trips / s)",
        N_ROUND_TRIPS as f64 / elapsed.as_secs_f64() / 1e6,
    );

    let status = echo_child.wait()?;
    if !status.success() {
        return Err(format!("echo process exited with {status}").into());
    }
    Ok(())
}

fn run_child(role: &str) -> Result<(), Box<dyn std::error::Error>> {
    match role {
        "echo" => run_echo(),
        other => Err(format!("unknown child role: {other}").into()),
    }
}

fn run_echo() -> Result<(), Box<dyn std::error::Error>> {
    let joined = child_segment();
    let (ping_seg, pong_seg) = split_segments(&joined);
    println!("[echo] attaching ping={ping_seg} pong={pong_seg}");

    // 1. Attach as the ping consumer (parent has already created the
    //    ping segment) with bounded retry to absorb the attach race.
    let mut ping_consumer = attach_shared_consumer_with_retry::<PingPongEvent>(
        &ping_seg,
        RING_SLOTS,
        CONSUMER_ID,
        ATTACH_TIMEOUT,
    )?;

    // 2. Build the pong producer (creates the pong segment).
    let mut pong_producer = build_shared_single_producer::<PingPongEvent>(&pong_seg, RING_SLOTS)
        .discover_consumer_with_prefix(1, CONSUMER_PREFIX)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(PingPongEvent::default)?;

    // 3. Warm the pong producer's discovery barrier so the parent's
    //    pong_consumer is registered before the first echo.
    warm_shared_producer_discovery(
        || pong_producer.min_gating_sequence(),
        DISCOVERY_SCAN_ROUNDS_1P1C,
    );

    let total_round_trips = WARMUP_ROUND_TRIPS + N_ROUND_TRIPS;
    let mut delivered = 0u64;
    while delivered < total_round_trips {
        if let Some(ping) = ping_consumer.try_consume_next_leased() {
            let echoed = *ping;
            pong_producer.publish(|slot| *slot = echoed);
            delivered += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    println!("[echo] echoed {delivered} round trips; exiting");
    Ok(())
}
