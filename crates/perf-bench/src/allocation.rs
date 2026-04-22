use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::thread_local;

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

thread_local! {
    static TRACKING_DEPTH: Cell<usize> = const { Cell::new(0) };
    static ALLOC_COUNT: Cell<u64> = const { Cell::new(0) };
    static ALLOC_BYTES: Cell<u64> = const { Cell::new(0) };
}

#[global_allocator]
static GLOBAL_ALLOCATOR: TrackingAllocator = TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() && tracking_enabled() {
            ALLOC_COUNT.with(|count| count.set(count.get() + 1));
            ALLOC_BYTES.with(|bytes| bytes.set(bytes.get() + layout.size() as u64));
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let out = System.realloc(ptr, layout, new_size);
        if !out.is_null() && tracking_enabled() {
            ALLOC_COUNT.with(|count| count.set(count.get() + 1));
            ALLOC_BYTES.with(|bytes| bytes.set(bytes.get() + new_size as u64));
        }
        out
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() && tracking_enabled() {
            ALLOC_COUNT.with(|count| count.set(count.get() + 1));
            ALLOC_BYTES.with(|bytes| bytes.set(bytes.get() + layout.size() as u64));
        }
        ptr
    }
}

pub fn measure_allocations<R>(f: impl FnOnce() -> R) -> (R, AllocationMetrics) {
    TRACKING_DEPTH.with(|depth| depth.set(depth.get() + 1));
    let before = snapshot();
    let result = f();
    let after = snapshot();
    TRACKING_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
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
        alloc_count: ALLOC_COUNT.with(|count| count.get()),
        alloc_bytes: ALLOC_BYTES.with(|bytes| bytes.get()),
    }
}

fn tracking_enabled() -> bool {
    TRACKING_DEPTH.with(|depth| depth.get() > 0)
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
