//! Process-safe cursor operations using shared memory.
//!
//! This module provides a [`SharedCursor`] that implements the disruptor pattern's
//! cursor concept for multi-process scenarios using shared memory. Cursors track
//! sequence positions and coordinate between producers and consumers across processes
//! with cache-line padding to prevent false sharing.

use crate::{
    shared_memory_layout::{required_layout_size, validate_layout, write_layout, SegmentKind},
    MultiProcessError, MultiProcessResult,
};
use shared_memory::{Shmem, ShmemConf};
use std::mem::align_of;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicI64, Ordering};

/// Cache line size for padding (64 bytes on most modern CPUs)
const CACHE_LINE_SIZE: usize = 64;

/// Padded atomic to prevent false sharing
#[repr(align(64))]
struct PaddedAtomicI64 {
    atomic: AtomicI64,
    _padding: [u8; CACHE_LINE_SIZE - std::mem::size_of::<AtomicI64>()],
}

impl PaddedAtomicI64 {
    fn new(value: i64) -> Self {
        Self {
            atomic: AtomicI64::new(value),
            _padding: [0; CACHE_LINE_SIZE - std::mem::size_of::<AtomicI64>()],
        }
    }
}

/// A shared cursor that can be accessed across processes
///
/// This implements the disruptor pattern's cursor concept for multi-process scenarios.
/// Each cursor tracks a sequence position and can be safely shared between processes
/// using shared memory with cache-line padding to prevent false sharing.
pub struct SharedCursor {
    _shmem: Shmem,
    cursor_ptr: NonNull<PaddedAtomicI64>,
    is_owner: bool,
}

unsafe impl Send for SharedCursor {}
unsafe impl Sync for SharedCursor {}

impl Clone for SharedCursor {
    fn clone(&self) -> Self {
        // Clone by creating a new mapping to the same shared memory
        // Get the OS ID from the existing shared memory
        let os_id = self._shmem.get_os_id();

        // Attach to the same shared memory segment
        // Cloned instances are not owners
        Self::attach(os_id)
            .expect("Failed to clone SharedCursor - shared memory segment should exist")
    }
}

impl Drop for SharedCursor {
    fn drop(&mut self) {
        if self.is_owner {
            // Cleanup ownership is centralized in shared_memory::Shmem drop.
            // Avoid explicit unlink here to prevent double-unlink races and
            // cross-platform backend mismatches.
        }
    }
}

impl SharedCursor {
    fn ensure_name(name: &str) {
        assert!(!name.is_empty(), "shared cursor name must not be empty");
    }

    #[cfg(unix)]
    fn unlink_shared_segment(name: &str) {
        use std::ffi::CString;
        if let Ok(c_str) = CString::new(name) {
            // Best-effort explicit unlink for crash-recovery workflows.
            // Ignore errors because segment may not exist.
            unsafe {
                libc::shm_unlink(c_str.as_ptr());
            }
        }
    }

    #[cfg(not(unix))]
    fn unlink_shared_segment(_name: &str) {
        // Best-effort no-op on non-Unix platforms.
    }

    /// Create a new shared cursor with automatic naming
    pub fn new_auto(initial_value: i64) -> MultiProcessResult<(Self, String)> {
        let payload_size = std::mem::size_of::<PaddedAtomicI64>();
        let payload_alignment = align_of::<PaddedAtomicI64>();
        // Let shared_memory crate generate the name automatically
        let shmem = ShmemConf::new()
            .size(required_layout_size(payload_size, payload_alignment)?)
            .create() // No .os_id() = automatic naming
            .map_err(|e| MultiProcessError::SharedMemoryError(e.to_string()))?;

        let generated_name = shmem.get_os_id().to_string();

        let contract = write_layout(
            &shmem,
            payload_size,
            payload_size,
            1,
            payload_alignment,
            SegmentKind::Cursor,
        )?;
        // Map the shared memory
        let ptr = unsafe {
            shmem.as_ptr().cast::<u8>().add(contract.payload_offset) as *mut PaddedAtomicI64
        };
        let cursor_ptr = NonNull::new(ptr)
            .ok_or_else(|| MultiProcessError::MemoryMapError("Null pointer".to_string()))?;

        // Initialize the padded atomic value
        unsafe {
            std::ptr::write(ptr, PaddedAtomicI64::new(initial_value));
        }

        let cursor = Self {
            _shmem: shmem,
            cursor_ptr,
            is_owner: true, // Creator is the owner
        };

        Ok((cursor, generated_name))
    }

    /// Create a new shared cursor in a new shared memory segment (legacy method)
    pub fn new(name: &str, initial_value: i64) -> MultiProcessResult<Self> {
        Self::ensure_name(name);
        // Do not unlink preemptively: that can replace a live segment and break
        // existing attachers. Callers should use unique names or explicit cleanup.
        let shmem = ShmemConf::new()
            .size(required_layout_size(
                std::mem::size_of::<PaddedAtomicI64>(),
                align_of::<PaddedAtomicI64>(),
            )?)
            .os_id(name)
            .create()
            .map_err(|e| MultiProcessError::SharedMemoryError(e.to_string()))?;

        let contract = write_layout(
            &shmem,
            std::mem::size_of::<PaddedAtomicI64>(),
            std::mem::size_of::<PaddedAtomicI64>(),
            1,
            align_of::<PaddedAtomicI64>(),
            SegmentKind::Cursor,
        )?;
        // Map the shared memory
        let ptr = unsafe {
            shmem.as_ptr().cast::<u8>().add(contract.payload_offset) as *mut PaddedAtomicI64
        };
        let cursor_ptr = NonNull::new(ptr)
            .ok_or_else(|| MultiProcessError::MemoryMapError("Null pointer".to_string()))?;

        // Initialize the padded atomic value
        unsafe {
            std::ptr::write(ptr, PaddedAtomicI64::new(initial_value));
        }

        Ok(Self {
            _shmem: shmem,
            cursor_ptr,
            is_owner: true, // Creator is the owner
        })
    }

    /// Explicitly recreate a shared cursor by unlinking and creating a fresh segment.
    ///
    /// This is intended for crash-restart recovery paths when a stale segment may
    /// remain after an unclean shutdown. This operation is destructive for any
    /// currently attached process using the same name.
    pub fn recreate(name: &str, initial_value: i64) -> MultiProcessResult<Self> {
        Self::ensure_name(name);
        Self::unlink_shared_segment(name);
        Self::new(name, initial_value)
    }

    /// Attach to an existing shared cursor in an existing shared memory segment
    pub fn attach(name: &str) -> MultiProcessResult<Self> {
        Self::ensure_name(name);
        let payload_size = std::mem::size_of::<PaddedAtomicI64>();
        let payload_alignment = align_of::<PaddedAtomicI64>();
        let shmem = ShmemConf::new()
            .os_id(name)
            .open()
            .map_err(|e| MultiProcessError::SegmentNotFound(e.to_string()))?;

        let contract = validate_layout(
            &shmem,
            payload_size,
            payload_size,
            1,
            payload_alignment,
            SegmentKind::Cursor,
        )?;
        let ptr = unsafe {
            shmem.as_ptr().cast::<u8>().add(contract.payload_offset) as *mut PaddedAtomicI64
        };
        let cursor_ptr = NonNull::new(ptr)
            .ok_or_else(|| MultiProcessError::MemoryMapError("Null pointer".to_string()))?;

        Ok(Self {
            _shmem: shmem,
            cursor_ptr,
            is_owner: false, // Attacher is not the owner
        })
    }

    /// Create a new shared cursor, or attach to existing one if it already exists
    pub fn new_or_attach(name: &str, initial_value: i64) -> MultiProcessResult<Self> {
        // First try to create
        match Self::new(name, initial_value) {
            Ok(cursor) => Ok(cursor),
            Err(MultiProcessError::SharedMemoryError(ref msg))
                if msg.contains("already exists") || msg.contains("File exists") =>
            {
                // If creation fails because segment already exists, try to attach
                Self::attach(name)
            }
            Err(e) => Err(e),
        }
    }

    /// Return whether this mapping currently owns unlink responsibility.
    pub fn is_owner(&self) -> bool {
        self.is_owner
    }

    /// Transfer or release ownership of the underlying shared-memory name.
    ///
    /// Consumer sequence cursors use this to become persistent restart anchors:
    /// the first attaching consumer may create the cursor segment, but it must not
    /// unlink that name when the process exits, or subsequent restarts would reset
    /// the logical consumer position back to the initial value.
    pub fn set_owner(&mut self, is_owner: bool) -> bool {
        let previous = self.is_owner;
        self._shmem.set_owner(is_owner);
        self.is_owner = is_owner;
        previous
    }

    /// Load the current value
    pub fn load(&self, ordering: Ordering) -> i64 {
        // Caller-supplied ordering allows callers in producer/consumer layers
        // to enforce the required synchronizes-with relation in each hot path.
        unsafe { self.cursor_ptr.as_ref().atomic.load(ordering) }
    }

    /// Store a new value
    pub fn store(&self, value: i64, ordering: Ordering) {
        // Store ordering is selected by the caller because this type is shared
        // across producers/consumers with different release/acquire needs.
        unsafe { self.cursor_ptr.as_ref().atomic.store(value, ordering) }
    }

    /// Compare and swap
    pub fn compare_exchange(
        &self,
        current: i64,
        new: i64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<i64, i64> {
        unsafe {
            self.cursor_ptr
                .as_ref()
                .atomic
                .compare_exchange(current, new, success, failure)
        }
    }

    /// Compare and swap (weak version, may fail spuriously)
    pub fn compare_exchange_weak(
        &self,
        current: i64,
        new: i64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<i64, i64> {
        unsafe {
            self.cursor_ptr
                .as_ref()
                .atomic
                .compare_exchange_weak(current, new, success, failure)
        }
    }

    /// Fetch and add
    pub fn fetch_add(&self, val: i64, ordering: Ordering) -> i64 {
        // Caller controls ordering for backpressure and publication fences
        // at the ring-buffer protocol level.
        unsafe { self.cursor_ptr.as_ref().atomic.fetch_add(val, ordering) }
    }

    /// Atomic exchange
    pub fn swap(&self, val: i64, ordering: Ordering) -> i64 {
        unsafe { self.cursor_ptr.as_ref().atomic.swap(val, ordering) }
    }
}

/// Generic shared cursor trait
pub trait SharedCursorTrait<T> {
    /// Load the current value with the given memory ordering
    fn load(&self, ordering: Ordering) -> T;

    /// Store a new value with the given memory ordering
    fn store(&self, value: T, ordering: Ordering);

    /// Compare and exchange operation with success and failure memory orderings
    fn compare_exchange(
        &self,
        current: T,
        new: T,
        success: Ordering,
        failure: Ordering,
    ) -> Result<T, T>;
}

impl SharedCursorTrait<i64> for SharedCursor {
    fn load(&self, ordering: Ordering) -> i64 {
        self.load(ordering)
    }

    fn store(&self, value: i64, ordering: Ordering) {
        self.store(value, ordering)
    }

    fn compare_exchange(
        &self,
        current: i64,
        new: i64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<i64, i64> {
        self.compare_exchange(current, new, success, failure)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn test_shared_cursor_basic_operations() {
        let cursor = SharedCursor::new("test_cursor", 42).unwrap();

        assert_eq!(cursor.load(Ordering::Relaxed), 42);

        cursor.store(100, Ordering::Relaxed);
        assert_eq!(cursor.load(Ordering::Relaxed), 100);

        let old = cursor.fetch_add(5, Ordering::Relaxed);
        assert_eq!(old, 100);
        assert_eq!(cursor.load(Ordering::Relaxed), 105);
    }

    #[test]
    #[should_panic(expected = "shared cursor name must not be empty")]
    fn test_new_cursor_rejects_empty_name() {
        let _ = SharedCursor::new("", 0).unwrap();
    }

    #[test]
    #[should_panic(expected = "shared cursor name must not be empty")]
    fn test_attach_cursor_rejects_empty_name() {
        let _ = SharedCursor::attach("").unwrap();
    }

    #[test]
    #[should_panic(expected = "shared cursor name must not be empty")]
    fn test_recreate_cursor_rejects_empty_name() {
        let _ = SharedCursor::recreate("", 0).unwrap();
    }
}
