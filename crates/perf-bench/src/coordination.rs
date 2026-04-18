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
    pub fn attach_with_timeout(prefix: &str, timeout: Duration) -> Result<Self, String> {
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

// ============================================================
// UnifiedCoordination — single SHM segment for ping-pong benchmarks
// ============================================================

const CACHE_LINE: usize = 64;

/// Cache-line-padded atomic for false-sharing isolation between processes.
#[repr(C, align(64))]
pub struct PaddedAtomicI64 {
    value: std::sync::atomic::AtomicI64,
    _pad: [u8; CACHE_LINE - std::mem::size_of::<std::sync::atomic::AtomicI64>()],
}

impl PaddedAtomicI64 {
    pub const fn new(v: i64) -> Self {
        Self {
            value: std::sync::atomic::AtomicI64::new(v),
            _pad: [0; CACHE_LINE - std::mem::size_of::<std::sync::atomic::AtomicI64>()],
        }
    }

    #[inline]
    pub fn load(&self, ord: Ordering) -> i64 {
        self.value.load(ord)
    }

    #[inline]
    pub fn store(&self, v: i64, ord: Ordering) {
        self.value.store(v, ord);
    }

    #[inline]
    pub fn fetch_add(&self, v: i64, ord: Ordering) -> i64 {
        self.value.fetch_add(v, ord)
    }
}

/// All ping-pong coordination state in one contiguous SHM segment.
/// 9 atomics × 64 bytes = 576 bytes. Each on its own cache line.
#[repr(C)]
pub struct CoordinationData {
    pub producer_ready: PaddedAtomicI64,
    pub echo_ready: PaddedAtomicI64,
    pub shutdown: PaddedAtomicI64,
    pub events_sent: PaddedAtomicI64,
    pub events_echoed: PaddedAtomicI64,
    pub echo_attached: PaddedAtomicI64,
    pub consumer_attached: PaddedAtomicI64,
    pub warmup_sent: PaddedAtomicI64,
    pub warmup_echoed: PaddedAtomicI64,
}

impl Default for CoordinationData {
    fn default() -> Self {
        Self {
            producer_ready: PaddedAtomicI64::new(0),
            echo_ready: PaddedAtomicI64::new(0),
            shutdown: PaddedAtomicI64::new(0),
            events_sent: PaddedAtomicI64::new(0),
            events_echoed: PaddedAtomicI64::new(0),
            echo_attached: PaddedAtomicI64::new(0),
            consumer_attached: PaddedAtomicI64::new(0),
            warmup_sent: PaddedAtomicI64::new(0),
            warmup_echoed: PaddedAtomicI64::new(0),
        }
    }
}

// Compile-time layout assertions
const _: [(); 9 * CACHE_LINE] = [(); std::mem::size_of::<CoordinationData>()];

/// Single-segment coordination for bidirectional (ping-pong) benchmarks.
///
/// Uses the `shared_memory` crate directly for a single contiguous segment
/// instead of 5+ separate SharedCursor segments. This matches the pattern
/// from `disruptor-mp/benches/ipc/competitive/coordination.rs`.
pub struct UnifiedCoordination {
    _shmem: shared_memory::Shmem,
    data: *mut CoordinationData,
    is_owner: bool,
}

unsafe impl Send for UnifiedCoordination {}
unsafe impl Sync for UnifiedCoordination {}

impl Drop for UnifiedCoordination {
    fn drop(&mut self) {
        if self.is_owner {
            Self::force_unlink(&self._shmem.get_os_id().to_string());
        }
    }
}

impl UnifiedCoordination {
    fn force_unlink(name: &str) {
        unsafe {
            if let Ok(c_str) = std::ffi::CString::new(name) {
                libc::shm_unlink(c_str.as_ptr());
            }
        }
    }

    /// Create a new coordination segment (owner).
    pub fn create(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Self::force_unlink(name);

        let shmem = shared_memory::ShmemConf::new()
            .size(std::mem::size_of::<CoordinationData>())
            .os_id(name)
            .create()?;

        let data = shmem.as_ptr() as *mut CoordinationData;
        unsafe {
            std::ptr::write(data, CoordinationData::default());
        }

        Ok(Self {
            _shmem: shmem,
            data,
            is_owner: true,
        })
    }

    /// Attach to existing coordination segment (non-owner).
    pub fn attach(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let shmem = shared_memory::ShmemConf::new().os_id(name).open()?;
        let data = shmem.as_ptr() as *mut CoordinationData;
        Ok(Self {
            _shmem: shmem,
            data,
            is_owner: false,
        })
    }

    /// Attach with retry (for child processes).
    pub fn attach_with_timeout(
        name: &str,
        timeout: Duration,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let deadline = Instant::now() + timeout;
        loop {
            match Self::attach(name) {
                Ok(c) => return Ok(c),
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(25))
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub fn data(&self) -> &CoordinationData {
        unsafe { &*self.data }
    }

    pub fn wait_for_producer_ready(&self, timeout: Duration) -> bool {
        let start = Instant::now();
        while self.data().producer_ready.load(Ordering::Acquire) == 0 {
            if start.elapsed() > timeout {
                return false;
            }
            std::hint::spin_loop();
        }
        true
    }

    pub fn wait_for_echo_ready(&self, timeout: Duration) -> bool {
        let start = Instant::now();
        while self.data().echo_ready.load(Ordering::Acquire) == 0 {
            if start.elapsed() > timeout {
                return false;
            }
            std::hint::spin_loop();
        }
        true
    }

    pub fn wait_for_consumer_attached(&self, timeout: Duration) -> bool {
        let start = Instant::now();
        while self.data().consumer_attached.load(Ordering::Acquire) == 0 {
            if start.elapsed() > timeout {
                return false;
            }
            std::hint::spin_loop();
        }
        true
    }

    pub fn signal_shutdown(&self) {
        self.data().shutdown.store(1, Ordering::Release);
    }

    pub fn is_shutdown(&self) -> bool {
        self.data().shutdown.load(Ordering::Acquire) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coordination_create_attach_signal() {
        let prefix = bench_prefix("coord_test");
        let owner = BenchmarkCoordination::create(&prefix).expect("create");
        let attached = BenchmarkCoordination::attach(&prefix).expect("attach");

        attached.signal_consumer_ready();
        assert!(owner.wait_for_consumers(1, Duration::from_secs(1)));

        owner.signal_producer_done(42i64);
        assert!(attached.wait_for_producer_done(Duration::from_secs(1)));
        assert_eq!(attached.events_produced(), 42i64);

        attached.signal_consumer_done(42i64);
        assert!(owner.wait_for_consumers_done(1, Duration::from_secs(1)));
        assert_eq!(owner.events_consumed(), 42i64);
    }

    #[test]
    fn test_unified_coordination_create_attach() {
        let name = format!("uc_test_{}", std::process::id() % 10000);
        let owner = UnifiedCoordination::create(&name).expect("create");

        // Signal producer ready
        owner.data().producer_ready.store(1, Ordering::Release);

        let attached = UnifiedCoordination::attach(&name).expect("attach");
        assert!(attached.wait_for_producer_ready(Duration::from_secs(1)));

        // Signal echo ready
        attached.data().echo_ready.store(1, Ordering::Release);
        assert!(owner.wait_for_echo_ready(Duration::from_secs(1)));

        // Shutdown
        owner.signal_shutdown();
        assert!(attached.is_shutdown());
    }

    #[test]
    fn test_padded_atomic_size() {
        assert_eq!(std::mem::size_of::<PaddedAtomicI64>(), 64);
        assert_eq!(std::mem::align_of::<PaddedAtomicI64>(), 64);
    }

    #[test]
    fn test_coordination_data_size() {
        assert_eq!(std::mem::size_of::<CoordinationData>(), 9 * 64);
    }
}
