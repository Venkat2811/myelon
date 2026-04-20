use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocationMetrics {
    pub alloc_count: u64,
    pub alloc_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct AllocationSnapshot {
    alloc_count: u64,
    alloc_bytes: u64,
}

struct TrackingAllocator;

static TRACKING_DEPTH: AtomicUsize = AtomicUsize::new(0);
static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static GLOBAL_ALLOCATOR: TrackingAllocator = TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() && tracking_enabled() {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let out = System.realloc(ptr, layout, new_size);
        if !out.is_null() && tracking_enabled() {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        out
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() && tracking_enabled() {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }
}

pub fn measure_allocations<R>(f: impl FnOnce() -> R) -> (R, AllocationMetrics) {
    TRACKING_DEPTH.fetch_add(1, Ordering::SeqCst);
    let before = snapshot();
    let result = f();
    let after = snapshot();
    TRACKING_DEPTH.fetch_sub(1, Ordering::SeqCst);
    (
        result,
        AllocationMetrics {
            alloc_count: after.alloc_count.saturating_sub(before.alloc_count),
            alloc_bytes: after.alloc_bytes.saturating_sub(before.alloc_bytes),
        },
    )
}

fn snapshot() -> AllocationSnapshot {
    AllocationSnapshot {
        alloc_count: ALLOC_COUNT.load(Ordering::SeqCst),
        alloc_bytes: ALLOC_BYTES.load(Ordering::SeqCst),
    }
}

fn tracking_enabled() -> bool {
    TRACKING_DEPTH.load(Ordering::Relaxed) > 0
}

#[cfg(test)]
mod tests {
    use super::measure_allocations;

    #[test]
    fn tracks_owned_allocations() {
        let (_, metrics) = measure_allocations(|| vec![0u8; 128]);
        assert!(metrics.alloc_count >= 1);
        assert!(metrics.alloc_bytes >= 128);
    }

    #[test]
    fn reports_zero_for_non_allocating_work() {
        let (_, metrics) = measure_allocations(|| 2usize + 2);
        assert_eq!(metrics.alloc_count, 0);
        assert_eq!(metrics.alloc_bytes, 0);
    }
}
