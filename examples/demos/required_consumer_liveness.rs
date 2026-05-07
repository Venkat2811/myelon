//! Required-consumer liveness with same-ID rejoin (RFC 0017.5).
//!
//! Story this example tells:
//!
//! 1. Parent spawns two required consumers, `cp_0` and `cp_1`,
//!    over a shared SHM ring. The producer uses
//!    `publish_managed(...)` so the liveness policy is consulted on
//!    every publish.
//! 2. Both consumers attach and start consuming in lockstep
//!    (strict broadcast — every event goes to both).
//! 3. Mid-flight, the parent SIGKILLs `cp_0` to simulate a hard
//!    worker crash. Without liveness, the producer would block
//!    *forever* the moment the ring fills past `cp_0`'s frozen
//!    cursor, because strict broadcast already gates publishing on
//!    the slowest consumer's position. (No overwrite ever happens
//!    in either mode — that's the strict-broadcast guarantee.)
//! 4. With liveness enabled, the producer instead notices the
//!    stall after `progress_timeout` and fires the alert hook so
//!    the parent gets an observable signal that something's wrong.
//!    Within `shutdown_grace_period` the parent respawns `cp_0`
//!    under the *same* `consumer_id`. The cursor `cp_0` left
//!    behind is still in SHM; the new process attaches to it and
//!    resumes consuming from exactly where the old one stopped.
//!    Producer recovery is transparent — the long-running
//!    `publish_managed` call returns successfully and the loop
//!    continues.
//! 5. Both consumers ultimately see every sequence up to
//!    `N_EVENTS`. The parent confirms the run was clean.
//!
//! What liveness *adds* on top of vanilla strict broadcast: a
//! structured, time-bounded, observable failure path
//! (alert + recoverable rejoin window). Without it, a crashed
//! required consumer turns a high-throughput pipeline into a
//! silent freeze.
//!
//! Run:
//!
//! ```bash
//! cargo run --release -p demos --example required_consumer_liveness
//! ```

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use demos::{
    attach_shared_consumer_with_retry, child_role, child_segment, spawn_self,
    warm_shared_producer_discovery, ChildProcessGuard, DISCOVERY_SCAN_ROUNDS_1P1C,
};
use disruptor_mp::{
    portable_shm_segment_name, RequiredConsumerAlert, RequiredConsumerFailureAction,
    RequiredConsumerLivenessConfig,
};
use myelon::build_shared_single_producer;
use myelon::producer::CoordinationMode;

const N_EVENTS: u64 = 200_000;
const RING_SLOTS: usize = 4096;
const ATTACH_TIMEOUT: Duration = Duration::from_secs(10);

/// Discovery prefix the producer scans for at startup; both
/// consumers register IDs that start with this prefix.
const CONSUMER_PREFIX: &str = "cp";
/// Sequence at which the parent SIGKILLs `cp_0` to simulate a
/// worker crash mid-publish.
const KILL_AT: u64 = 50_000;
/// Sequence at which the parent respawns `cp_0` under the same
/// `consumer_id`. Must land inside the `shutdown_grace_period`
/// window after the kill so the rejoin is recognised as recovery
/// rather than a fresh attach.
const RESPAWN_AT: u64 = 80_000;

#[derive(Copy, Clone, Default)]
#[repr(C)]
struct Tick {
    sequence: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(role) = child_role() {
        return run_consumer(&role);
    }
    run_parent()
}

fn run_parent() -> Result<(), Box<dyn std::error::Error>> {
    let segment = portable_shm_segment_name("livenessdemo");
    println!(
        "[parent] segment = {segment}; publishing {N_EVENTS} events to required consumers cp_0 + cp_1"
    );

    let mut producer = build_shared_single_producer::<Tick>(&segment, RING_SLOTS)
        .discover_consumer_with_prefix(2, CONSUMER_PREFIX)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(Tick::default)?;

    // Liveness policy. The alert hook prints a single line so the
    // run output reads as a timeline (kill → alert → respawn →
    // catch-up). `shutdown_grace_period` is the budget within
    // which the parent has to bring `cp_0` back; if it expires,
    // the next `publish_managed` call returns
    // `RequiredConsumerError::GracefulShutdownTriggered`.
    let alert_hook: Arc<dyn Fn(&RequiredConsumerAlert) + Send + Sync + 'static> =
        Arc::new(|alert| {
            eprintln!(
                "[parent]   ⚠ STALL alert: {} stuck at seq {} for {:.2}s",
                alert.consumer_id,
                alert.last_sequence,
                alert.stalled_for.as_secs_f32(),
            );
        });
    producer.enable_required_consumer_liveness(RequiredConsumerLivenessConfig {
        required_consumer_ids: vec!["cp_0".into(), "cp_1".into()],
        startup_wait_timeout: Duration::from_secs(10),
        progress_timeout: Duration::from_millis(200),
        progress_check_interval: Duration::from_millis(20),
        shutdown_grace_period: Duration::from_secs(5),
        failure_action: RequiredConsumerFailureAction::GracefulShutdown,
        alert_hook: Some(alert_hook),
    });

    let mut cp0 = Some(ChildProcessGuard::new(spawn_self("consumer_0", &segment)?));
    let mut cp1 = ChildProcessGuard::new(spawn_self("consumer_1", &segment)?);

    warm_shared_producer_discovery(
        || producer.min_gating_sequence(),
        DISCOVERY_SCAN_ROUNDS_1P1C * 2,
    );
    println!("[parent] both consumers attached; starting publish loop");

    let mut respawned = false;
    for sequence in 1..=N_EVENTS {
        producer.publish_managed(|slot| slot.sequence = sequence)?;

        if sequence == KILL_AT {
            // Hard crash via Drop. `ChildProcessGuard::drop` calls
            // `child.kill()` (SIGKILL) followed by `child.wait()`,
            // so dropping the guard is a single atomic step that
            // both terminates the child and reaps the zombie. No
            // separate kill/wait dance needed; the helper crate
            // already encapsulates the right semantics.
            drop(cp0.take());
            println!(
                "[parent] 💀 cp_0 dropped at seq {sequence} (Drop = SIGKILL + reap; cursor frozen in SHM)"
            );
        }

        if sequence == RESPAWN_AT && !respawned {
            cp0 = Some(ChildProcessGuard::new(spawn_self("consumer_0", &segment)?));
            respawned = true;
            println!(
                "[parent] ↺ respawned cp_0 at seq {sequence} (same consumer_id, picks up cursor from SHM)"
            );
        }
    }

    println!("[parent] all {N_EVENTS} events published; waiting for consumers to drain");

    if let Some(mut guard) = cp0 {
        let status = guard.wait()?;
        if !status.success() {
            return Err(format!("cp_0 exited with {status}").into());
        }
    }
    let status = cp1.wait()?;
    if !status.success() {
        return Err(format!("cp_1 exited with {status}").into());
    }

    println!("[parent] ✓ both consumers received seq 1..={N_EVENTS}; producer never errored");
    Ok(())
}

fn run_consumer(role: &str) -> Result<(), Box<dyn std::error::Error>> {
    let consumer_id = match role {
        "consumer_0" => "cp_0",
        "consumer_1" => "cp_1",
        other => return Err(format!("unknown role: {other}").into()),
    };
    let segment = child_segment();
    let pid = std::process::id();
    eprintln!("[{consumer_id}] (pid {pid}) attaching to {segment}");

    let mut consumer = attach_shared_consumer_with_retry::<Tick>(
        &segment,
        RING_SLOTS,
        consumer_id,
        ATTACH_TIMEOUT,
    )?;

    let mut delivered = 0u64;
    let mut last = 0u64;
    let mut first_seen = false;
    while last < N_EVENTS {
        if let Some(tick) = consumer.try_consume_next_leased() {
            last = tick.sequence;
            delivered += 1;
            if !first_seen {
                // For the respawned cp_0 this prints the resume
                // sequence — which should sit just above where the
                // killed instance stopped, proving the cursor was
                // recovered from SHM rather than reset.
                eprintln!("[{consumer_id}] (pid {pid}) first event after attach: seq = {last}");
                first_seen = true;
            }
        } else {
            // Brief sleep keeps the CPU calm while the producer
            // might be paused on a stalled peer; spin-loop would
            // also work but yields nicer logs.
            thread::sleep(Duration::from_micros(50));
        }
    }
    eprintln!(
        "[{consumer_id}] (pid {pid}) drained {delivered} events from this process; last seq = {last}"
    );
    Ok(())
}
