//! Workspace-level runnable examples for `disruptor-mp` and `myelon`.
//!
//! Every example in this crate is **multiprocess by construction** —
//! that's the whole point of `disruptor-mp` (mp = multiprocess) and of
//! `myelon` on top of it. Each example spawns its peer(s) as real OS
//! child processes via [`spawn_self`] and dispatches them to their
//! role through [`child_role`] at startup.
//!
//! Run any example with:
//!
//! ```bash
//! cargo run --release -p examples --example shm_disruptor
//! cargo run --release -p examples --example mmap_disruptor
//! cargo run --release -p examples --example pingpong
//! cargo run --release -p examples --example counters
//! cargo run --release -p examples --example fixed_inference_topology
//! ```
//!
//! ## Patterns shared with `crates/perf-bench`
//!
//! Examples deliberately stay smaller than `perf-bench` (no
//! benchmark-tier reporting, no `UnifiedCoordination`, no
//! `LatencyRecorder`), but they use the same safety and correctness
//! primitives so the demos are not racy:
//!
//! - **`ChildProcessGuard`** — drop-kills the child if the parent
//!   panics, so we never leak orphaned processes.
//! - **`attach_shared_consumer_with_retry`** — consumer side
//!   tolerates the producer-creates-segment / consumer-attaches race
//!   by retrying for a bounded duration.
//! - **`warm_shared_producer_discovery`** — after the consumer
//!   attaches, the producer's discovery barrier is given a few scan
//!   ticks before publishing so it actually finds the new consumer.

use disruptor_mp::{attach_shared_consumer, SharedConsumer};
use std::env;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Env var name used to dispatch a re-entered process to its role.
pub const ROLE_ENV: &str = "MYELON_EXAMPLE_ROLE";

/// Env var name used to pass the SHM segment label / mmap layout
/// label / coordination tag to the child.
pub const SEGMENT_ENV: &str = "MYELON_EXAMPLE_SEGMENT";

/// Returns `Some(role)` when this process was spawned by an example
/// parent. Returns `None` for the original parent process.
#[must_use]
pub fn child_role() -> Option<String> {
    env::var(ROLE_ENV).ok()
}

/// Read the shared segment / coordination label set by the parent.
///
/// # Panics
///
/// Panics if [`SEGMENT_ENV`] is not set; only call this from a child
/// path gated on [`child_role`] returning `Some`.
#[must_use]
pub fn child_segment() -> String {
    env::var(SEGMENT_ENV).expect("MYELON_EXAMPLE_SEGMENT not set in child")
}

/// Spawn this binary again as a child process, asking it to run
/// `role` against the shared `segment` label. Both stdout and stderr
/// are inherited so the user sees one unified log.
///
/// # Errors
///
/// Forwards [`std::io::Error`] from `current_exe()` or `spawn()`.
pub fn spawn_self(role: &str, segment: &str) -> std::io::Result<Child> {
    let exe = env::current_exe()?;
    Command::new(exe)
        .env(ROLE_ENV, role)
        .env(SEGMENT_ENV, segment)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
}

/// RAII wrapper around a child process.
///
/// On drop, kills + reaps the child if it has not already been
/// waited on. Mirrors the safety pattern in
/// `crates/perf-bench/src/layers/raw/disruptor_mp/pingpong_shm.rs`
/// — the parent should never leave an orphaned child behind, even
/// if it panics partway through setup.
pub struct ChildProcessGuard {
    child: Option<Child>,
}

impl ChildProcessGuard {
    /// Wrap an already-spawned child.
    #[must_use]
    pub fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    /// Wait for the child to exit normally. Idempotent — calling
    /// again returns `Ok(0)` without re-waiting.
    ///
    /// # Errors
    ///
    /// Forwards [`std::io::Error`] from `Child::wait`.
    pub fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        if let Some(mut child) = self.child.take() {
            child.wait()
        } else {
            use std::os::unix::process::ExitStatusExt;
            Ok(std::process::ExitStatus::from_raw(0))
        }
    }
}

impl Drop for ChildProcessGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Attach to a SHM ring buffer with bounded retries.
///
/// The producer-creates-segment / consumer-attaches race means the
/// first `attach_shared_consumer().build_consumer()` call from the
/// child can lose to a still-initialising producer; retry every
/// 25 ms until `timeout` elapses. Mirrors `attach_consumer_with_timeout`
/// in `crates/perf-bench/src/layers/raw/disruptor_mp/pingpong_shm.rs`.
///
/// # Errors
///
/// Returns the underlying error if attach is still failing at
/// `timeout`.
pub fn attach_shared_consumer_with_retry<E: Copy + Default + 'static>(
    segment: &str,
    buffer_size: usize,
    consumer_id: &str,
    timeout: Duration,
) -> Result<SharedConsumer<E>, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        match attach_shared_consumer::<E>(segment, buffer_size)
            .with_consumer_id(consumer_id)
            .build_consumer()
        {
            Ok(consumer) => return Ok(consumer),
            Err(_) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(error) => {
                return Err(format!("attach failed for {consumer_id}: {error}").into());
            }
        }
    }
}

/// Drive the producer's consumer-discovery barrier through a few
/// scan cycles after the consumer attaches but before publishing
/// starts.
///
/// Each call to `min_gating_sequence` triggers an internal discovery
/// scan if the scan interval has elapsed; sleeping between calls
/// gives the kernel time to publish the new consumer's cursor to the
/// producer's barrier. Without this warmup, a fast producer can
/// publish its first event before the barrier sees the consumer,
/// which collapses backpressure.
///
/// `scan` is invoked `rounds` times with a 150 ms sleep between
/// calls (matching `crates/perf-bench/.../pingpong_shm.rs`).
pub fn warm_shared_producer_discovery<F: FnMut() -> i64>(mut scan: F, rounds: usize) {
    for _ in 0..rounds {
        let _ = scan();
        std::thread::sleep(Duration::from_millis(150));
    }
}

/// Default scan rounds for a single-consumer topology. Matches
/// `discovery_scan_rounds(1)` in perf-bench: 8 scans × 150 ms = 1.2 s
/// budget for the discovery barrier to register the consumer.
pub const DISCOVERY_SCAN_ROUNDS_1P1C: usize = 8;
