//! Shared memory ring buffer for multi-process communication.
//!
//! This module provides the [`SharedRingBuffer`] type for storing events in shared memory
//! that can be accessed by multiple processes. The ring buffer provides lock-free,
//! wait-free access patterns with power-of-2 sizing for efficient indexing.

use crate::{MultiProcessError, MultiProcessResult, SharedMemoryConfig};
use disruptor_core::Sequence;
use shared_memory::{Shmem, ShmemConf};
use std::cell::UnsafeCell;
use std::mem;
use std::ptr::NonNull;

/// Ring buffer stored in shared memory for multi-process access
pub struct SharedRingBuffer<E> {
    _shmem: Shmem,
    slots_ptr: NonNull<UnsafeCell<E>>,
    index_mask: i64,
    size: usize,
    is_owner: bool,
}

unsafe impl<E> Send for SharedRingBuffer<E> {}
unsafe impl<E> Sync for SharedRingBuffer<E> {}

impl<E> Drop for SharedRingBuffer<E> {
    fn drop(&mut self) {
        if self.is_owner {
            // Cleanup ownership is centralized in shared_memory::Shmem drop.
            // Avoid explicit unlink here to prevent double-unlink races and
            // cross-platform backend mismatches.
        }
    }
}

fn is_pow_of_2(num: usize) -> bool {
    num != 0 && (num & (num - 1) == 0)
}

impl<E> SharedRingBuffer<E>
where
    E: Copy + Default,
{
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

    /// Create a new shared ring buffer with automatic naming
    pub fn new_auto<F>(
        buffer_size: usize,
        mut event_factory: F,
    ) -> MultiProcessResult<(Self, String)>
    where
        F: FnMut() -> E,
    {
        if !is_pow_of_2(buffer_size) {
            return Err(MultiProcessError::IncompatibleLayout);
        }

        let size = buffer_size;
        let element_size = mem::size_of::<UnsafeCell<E>>();
        let total_size = element_size * size;

        // Let shared_memory crate generate the name automatically
        let shmem = ShmemConf::new()
            .size(total_size)
            .create() // No .os_id() = automatic naming
            .map_err(|e| MultiProcessError::SharedMemoryError(e.to_string()))?;

        let generated_name = shmem.get_os_id().to_string();

        let ptr = shmem.as_ptr() as *mut UnsafeCell<E>;
        let slots_ptr = NonNull::new(ptr)
            .ok_or_else(|| MultiProcessError::MemoryMapError("Null pointer".to_string()))?;

        // Initialize the ring buffer with default values
        unsafe {
            for i in 0..size {
                let slot_ptr = ptr.add(i);
                std::ptr::write(slot_ptr, UnsafeCell::new(event_factory()));
            }
        }

        let index_mask = (size - 1) as i64;

        let ring_buffer = SharedRingBuffer {
            _shmem: shmem,
            slots_ptr,
            index_mask,
            size,
            is_owner: true, // Creator is the owner
        };

        Ok((ring_buffer, generated_name))
    }

    /// Create a new shared ring buffer (legacy method with explicit naming)
    pub fn new<F>(config: SharedMemoryConfig, mut event_factory: F) -> MultiProcessResult<Self>
    where
        F: FnMut() -> E,
    {
        if !is_pow_of_2(config.buffer_size) {
            return Err(MultiProcessError::IncompatibleLayout);
        }

        let size = config.buffer_size;
        let element_size = mem::size_of::<UnsafeCell<E>>();
        let total_size = element_size * size;

        let shmem = if config.create {
            // Producer creates the shared memory.
            // Do not unlink preemptively: that can replace a live segment and break
            // existing attachers. Callers should use unique names or explicit cleanup.
            ShmemConf::new()
                .size(total_size)
                .os_id(&config.name)
                .create()
                .map_err(|e| MultiProcessError::SharedMemoryError(e.to_string()))?
        } else {
            // Consumer attaches to existing shared memory
            ShmemConf::new()
                .os_id(&config.name)
                .open()
                .map_err(|e| MultiProcessError::SegmentNotFound(e.to_string()))?
        };

        let ptr = shmem.as_ptr() as *mut UnsafeCell<E>;
        let slots_ptr = NonNull::new(ptr)
            .ok_or_else(|| MultiProcessError::MemoryMapError("Null pointer".to_string()))?;

        if config.create {
            // Initialize the ring buffer with default values
            unsafe {
                for i in 0..size {
                    let slot_ptr = ptr.add(i);
                    std::ptr::write(slot_ptr, UnsafeCell::new(event_factory()));
                }
            }
        }

        let index_mask = (size - 1) as i64;

        Ok(SharedRingBuffer {
            _shmem: shmem,
            slots_ptr,
            index_mask,
            size,
            is_owner: config.create, // Creator is the owner
        })
    }

    /// Explicitly recreate a shared ring buffer by unlinking and creating a fresh segment.
    ///
    /// This is intended for crash-restart recovery paths when a stale segment may
    /// remain after an unclean shutdown. This operation is destructive for any
    /// currently attached process using the same name.
    pub fn recreate<F>(config: SharedMemoryConfig, event_factory: F) -> MultiProcessResult<Self>
    where
        F: FnMut() -> E,
    {
        if !config.create {
            return Err(MultiProcessError::SharedMemoryError(
                "SharedRingBuffer::recreate requires config.create = true".to_string(),
            ));
        }

        Self::unlink_shared_segment(&config.name);
        Self::new(config, event_factory)
    }

    /// Attach to an existing shared ring buffer
    pub fn attach(config: SharedMemoryConfig) -> MultiProcessResult<Self> {
        if !is_pow_of_2(config.buffer_size) {
            return Err(MultiProcessError::IncompatibleLayout);
        }

        let size = config.buffer_size;
        let element_size = mem::size_of::<UnsafeCell<E>>();
        let expected_size = element_size * size;

        let shmem = ShmemConf::new()
            .os_id(&config.name)
            .open()
            .map_err(|e| MultiProcessError::SegmentNotFound(e.to_string()))?;

        // Verify size is at least as large as expected
        if shmem.len() < expected_size {
            return Err(MultiProcessError::IncompatibleLayout);
        }

        let ptr = shmem.as_ptr() as *mut UnsafeCell<E>;
        let slots_ptr = NonNull::new(ptr)
            .ok_or_else(|| MultiProcessError::MemoryMapError("Null pointer".to_string()))?;

        let index_mask = (size - 1) as i64;

        Ok(SharedRingBuffer {
            _shmem: shmem,
            slots_ptr,
            index_mask,
            size,
            is_owner: false, // Attacher is not the owner
        })
    }

    #[inline]
    fn wrap_point(&self, sequence: Sequence) -> Sequence {
        // This matches the non-multiprocess implementation
        sequence - self.size() as i64
    }

    #[inline]
    pub(crate) fn free_slots(
        &self,
        producer: Sequence,
        highest_read_by_consumers: Sequence,
    ) -> i64 {
        // Use the same calculation as non-multiprocess version
        let wrap_point = self.wrap_point(producer);
        highest_read_by_consumers - wrap_point
    }

    /// Get a mutable pointer to the element at the given sequence
    ///
    /// # Safety
    /// Callers must ensure that only a single mutable reference or multiple immutable references
    /// exist at any point in time for the same sequence.
    #[inline]
    pub fn get(&self, sequence: Sequence) -> *mut E {
        let index = (sequence & self.index_mask) as usize;
        unsafe {
            let slot_ptr = self.slots_ptr.as_ptr().add(index);
            (*slot_ptr).get()
        }
    }

    #[inline]
    pub(crate) fn size(&self) -> usize {
        self.size
    }

    /// Get the underlying shared memory ID for this buffer
    pub fn shared_memory_id(&self) -> String {
        self._shmem.get_os_id().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_ring_buffer_creation() {
        let config = SharedMemoryConfig {
            name: "test_ring".to_string(),
            buffer_size: 8,
            element_size: std::mem::size_of::<i32>(),
            create: true,
        };

        let ring_buffer = SharedRingBuffer::new(config, || 0i32).unwrap();
        assert_eq!(ring_buffer.size(), 8);
    }

    #[test]
    fn test_free_slots() {
        let config = SharedMemoryConfig {
            name: "test_ring_slots".to_string(),
            buffer_size: 8,
            element_size: std::mem::size_of::<i32>(),
            create: true,
        };

        let ring_buffer = SharedRingBuffer::new(config, || 0i32).unwrap();

        // Test similar to the original ring buffer tests
        assert_eq!(1, ring_buffer.free_slots(7, 0));
        assert_eq!(0, ring_buffer.free_slots(8, 0));
        assert_eq!(8, ring_buffer.free_slots(0, 0));
        assert_eq!(4, ring_buffer.free_slots(3, -1));
    }
}
