//! Multiprocess coordination for benchmarks.
//!
//! Uses shared-memory atomic cursors for lock-free synchronization between
//! producer and consumer processes. Coordination always uses SHM cursors
//! regardless of the benchmark's transport backend.

use disruptor_mp::lock_free::SharedCursor;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// Default timeout for waiting on coordination signals.
pub const DEFAULT_COORDINATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Atomic coordination state shared between benchmark processes.
///
/// Each cursor is a separate shared-memory segment backed by a `SharedCursor`.
/// All fields are cache-line-isolated by the cursor implementation.
pub struct BenchmarkCoordination {
    pub consumers_ready: SharedCursor,
    pub producer_done: SharedCursor,
    pub events_produced: SharedCursor,
    pub consumer_done: SharedCursor,
    pub events_consumed: SharedCursor,
}

impl BenchmarkCoordination {
    /// Create coordination cursors (producer side).
    /// Names are kept short for macOS POSIX shm 31-char limit.
    pub fn create(prefix: &str) -> Result<Self, disruptor_mp::MultiProcessError> {
        Ok(Self {
            consumers_ready: SharedCursor::new(&format!("{prefix}cr"), 0)?,
            producer_done: SharedCursor::new(&format!("{prefix}pd"), 0)?,
            events_produced: SharedCursor::new(&format!("{prefix}ep"), 0)?,
            consumer_done: SharedCursor::new(&format!("{prefix}cd"), 0)?,
            events_consumed: SharedCursor::new(&format!("{prefix}ec"), 0)?,
        })
    }

    /// Attach to existing coordination cursors (consumer side).
    /// Fails immediately if cursors don't exist yet.
    pub fn attach(prefix: &str) -> Result<Self, disruptor_mp::MultiProcessError> {
        Ok(Self {
            consumers_ready: SharedCursor::attach(&format!("{prefix}cr"))?,
            producer_done: SharedCursor::attach(&format!("{prefix}pd"))?,
            events_produced: SharedCursor::attach(&format!("{prefix}ep"))?,
            consumer_done: SharedCursor::attach(&format!("{prefix}cd"))?,
            events_consumed: SharedCursor::attach(&format!("{prefix}ec"))?,
        })
    }

    /// Attach with bounded timeout retry (for child processes that may start
    /// before the parent has finished creating coordination cursors).
    pub fn attach_with_timeout(
        prefix: &str,
        timeout: Duration,
    ) -> Result<Self, String> {
        let start = Instant::now();
        let sleep = Duration::from_millis(25);
        let mut attempt = 0u32;

        loop {
            match Self::attach(prefix) {
                Ok(coord) => return Ok(coord),
                Err(_) => {
                    attempt += 1;
                    if start.elapsed() >= timeout {
                        return Err(format!(
                            "Failed to attach coordination '{}' after {} retries ({:?})",
                            prefix, attempt, timeout
                        ));
                    }
                    std::thread::sleep(sleep);
                }
            }
        }
    }

    /// Signal that a consumer is ready.
    pub fn signal_consumer_ready(&self) {
        self.consumers_ready.fetch_add(1, Ordering::Release);
    }

    /// Wait for `n` consumers to be ready. Returns false on timeout.
    pub fn wait_for_consumers(&self, n: usize, timeout: Duration) -> bool {
        let start = Instant::now();
        loop {
            if self.consumers_ready.load(Ordering::Acquire) >= n as i64 {
                return true;
            }
            if start.elapsed() > timeout {
                return false;
            }
            std::hint::spin_loop();
        }
    }

    /// Signal that the producer is done and store event count.
    pub fn signal_producer_done(&self, events: i64) {
        self.events_produced.store(events, Ordering::Release);
        self.producer_done.store(1, Ordering::Release);
    }

    /// Check if the producer is done.
    pub fn is_producer_done(&self) -> bool {
        self.producer_done.load(Ordering::Acquire) > 0
    }

    /// Wait for the producer to be done. Returns false on timeout.
    pub fn wait_for_producer_done(&self, timeout: Duration) -> bool {
        let start = Instant::now();
        loop {
            if self.is_producer_done() {
                return true;
            }
            if start.elapsed() > timeout {
                return false;
            }
            std::hint::spin_loop();
        }
    }

    /// Signal that a consumer is done and add to event count.
    pub fn signal_consumer_done(&self, events: i64) {
        self.events_consumed.fetch_add(events, Ordering::Release);
        self.consumer_done.fetch_add(1, Ordering::Release);
    }

    /// Wait for `n` consumers to be done. Returns false on timeout.
    pub fn wait_for_consumers_done(&self, n: usize, timeout: Duration) -> bool {
        let start = Instant::now();
        loop {
            if self.consumer_done.load(Ordering::Acquire) >= n as i64 {
                return true;
            }
            if start.elapsed() > timeout {
                return false;
            }
            std::hint::spin_loop();
        }
    }

    /// Get the total events produced.
    pub fn events_produced(&self) -> i64 {
        self.events_produced.load(Ordering::Acquire)
    }

    /// Get the total events consumed across all consumers.
    pub fn events_consumed(&self) -> i64 {
        self.events_consumed.load(Ordering::Acquire)
    }
}

/// Generate a unique benchmark prefix incorporating the PID to avoid collisions.
pub fn bench_prefix(label: &str) -> String {
    format!("mb_{label}_{}", std::process::id() % 10000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coordination_create_attach_signal() {
        let prefix = bench_prefix("coord_test");
        let owner = BenchmarkCoordination::create(&prefix).expect("create");
        let attached = BenchmarkCoordination::attach(&prefix).expect("attach");

        // Signal ready
        attached.signal_consumer_ready();
        assert!(owner.wait_for_consumers(1, Duration::from_secs(1)));

        // Signal done
        owner.signal_producer_done(42i64);
        assert!(attached.wait_for_producer_done(Duration::from_secs(1)));
        assert_eq!(attached.events_produced(), 42i64);

        // Consumer done
        attached.signal_consumer_done(42i64);
        assert!(owner.wait_for_consumers_done(1, Duration::from_secs(1)));
        assert_eq!(owner.events_consumed(), 42i64);
    }
}
