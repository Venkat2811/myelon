use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

#[allow(dead_code)]
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
            Self::force_unlink(self._shmem.get_os_id());
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
