//! Shared memory ring buffer for multi-process communication.
//!
//! This module provides the [`SharedRingBuffer`] type for storing events in shared memory
//! that can be accessed by multiple processes. The ring buffer provides lock-free,
//! wait-free access patterns with power-of-2 sizing for efficient indexing.

use crate::{
    shared_memory_layout::{required_layout_size, validate_layout, write_layout, SegmentKind},
    MultiProcessError, MultiProcessResult, SharedMemoryConfig,
};
use disruptor_core::Sequence;
use shared_memory::{Shmem, ShmemConf};
use std::cell::UnsafeCell;
use std::mem::{align_of, size_of};
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

fn validate_event_config(name: &str, expected: usize, configured: usize) -> MultiProcessResult<()> {
    if expected != configured {
        return Err(MultiProcessError::IncompatibleLayout(format!(
            "{} event element size mismatch: expected {}, configured {}",
            name, expected, configured
        )));
    }
    Ok(())
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

    /// Create a new shared ring buffer with automatic naming.
    pub fn new_auto<F>(
        buffer_size: usize,
        mut event_factory: F,
    ) -> MultiProcessResult<(Self, String)>
    where
        F: FnMut() -> E,
    {
        assert!(
            buffer_size > 0,
            "ring buffer size must be greater than zero"
        );
        if !is_pow_of_2(buffer_size) {
            return Err(MultiProcessError::IncompatibleLayout(
                "ring buffer size must be power-of-two".to_string(),
            ));
        }

        let element_size = size_of::<UnsafeCell<E>>();
        let size = buffer_size;
        let payload_size = element_size.checked_mul(size).ok_or_else(|| {
            MultiProcessError::SharedMemoryError("shared memory size overflow".to_string())
        })?;
        let payload_alignment = align_of::<UnsafeCell<E>>();
        let required_size = required_layout_size(payload_size, payload_alignment)?;

        // Let shared_memory crate generate the name automatically
        let shmem = ShmemConf::new()
            .size(required_size)
            .create()
            .map_err(|e| MultiProcessError::SharedMemoryError(e.to_string()))?;

        let generated_name = shmem.get_os_id().to_string();
        let contract = write_layout(
            &shmem,
            payload_size,
            element_size,
            size,
            payload_alignment,
            SegmentKind::RingBuffer,
        )?;

        let ptr = unsafe {
            shmem.as_ptr().cast::<u8>().add(contract.payload_offset) as *mut UnsafeCell<E>
        };
        let slots_ptr = NonNull::new(ptr)
            .ok_or_else(|| MultiProcessError::MemoryMapError("Null pointer".to_string()))?;

        // Initialize the ring buffer with default values
        unsafe {
            for i in 0..size {
                std::ptr::write(ptr.add(i), UnsafeCell::new(event_factory()));
            }
        }

        let index_mask = (size - 1) as i64;
        let ring_buffer = SharedRingBuffer {
            _shmem: shmem,
            slots_ptr,
            index_mask,
            size,
            is_owner: true,
        };

        Ok((ring_buffer, generated_name))
    }

    /// Create a new shared ring buffer (legacy method with explicit naming).
    pub fn new<F>(config: SharedMemoryConfig, mut event_factory: F) -> MultiProcessResult<Self>
    where
        F: FnMut() -> E,
    {
        assert!(
            !config.name.is_empty(),
            "shared ring buffer name must not be empty"
        );
        assert!(
            config.buffer_size > 0,
            "ring buffer size must be greater than zero"
        );
        if !is_pow_of_2(config.buffer_size) {
            return Err(MultiProcessError::IncompatibleLayout(
                "ring buffer size must be power-of-two".to_string(),
            ));
        }

        validate_event_config("ring", size_of::<E>(), config.element_size)?;

        let size = config.buffer_size;
        let element_size = size_of::<UnsafeCell<E>>();
        let payload_size = element_size.checked_mul(size).ok_or_else(|| {
            MultiProcessError::SharedMemoryError("shared memory size overflow".to_string())
        })?;
        let payload_alignment = align_of::<UnsafeCell<E>>();

        let shmem = if config.create {
            let required_size = required_layout_size(payload_size, payload_alignment)?;
            ShmemConf::new()
                .size(required_size)
                .os_id(&config.name)
                .create()
                .map_err(|e| MultiProcessError::SharedMemoryError(e.to_string()))?
        } else {
            ShmemConf::new()
                .os_id(&config.name)
                .open()
                .map_err(|e| MultiProcessError::SegmentNotFound(e.to_string()))?
        };

        let payload_offset = if config.create {
            write_layout(
                &shmem,
                payload_size,
                element_size,
                size,
                payload_alignment,
                SegmentKind::RingBuffer,
            )?
            .payload_offset
        } else {
            validate_layout(
                &shmem,
                payload_size,
                element_size,
                size,
                payload_alignment,
                SegmentKind::RingBuffer,
            )?
            .payload_offset
        };

        let ptr = unsafe { shmem.as_ptr().cast::<u8>().add(payload_offset) as *mut UnsafeCell<E> };
        let slots_ptr = NonNull::new(ptr)
            .ok_or_else(|| MultiProcessError::MemoryMapError("Null pointer".to_string()))?;

        if config.create {
            // Initialize the ring buffer with default values
            unsafe {
                for i in 0..size {
                    std::ptr::write(ptr.add(i), UnsafeCell::new(event_factory()));
                }
            }
        }

        let index_mask = (size - 1) as i64;
        Ok(SharedRingBuffer {
            _shmem: shmem,
            slots_ptr,
            index_mask,
            size,
            is_owner: config.create,
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
        assert!(
            config.create,
            "SharedRingBuffer::recreate requires create = true"
        );
        if !config.create {
            return Err(MultiProcessError::SharedMemoryError(
                "SharedRingBuffer::recreate requires config.create = true".to_string(),
            ));
        }
        validate_event_config("ring", size_of::<E>(), config.element_size)?;
        Self::unlink_shared_segment(&config.name);
        Self::new(config, event_factory)
    }

    /// Attach to an existing shared ring buffer.
    pub fn attach(config: SharedMemoryConfig) -> MultiProcessResult<Self> {
        assert!(
            !config.name.is_empty(),
            "shared ring buffer name must not be empty"
        );
        assert!(
            config.buffer_size > 0,
            "ring buffer size must be greater than zero"
        );
        if !is_pow_of_2(config.buffer_size) {
            return Err(MultiProcessError::IncompatibleLayout(
                "ring buffer size must be power-of-two".to_string(),
            ));
        }
        validate_event_config("ring", size_of::<E>(), config.element_size)?;

        let size = config.buffer_size;
        let element_size = size_of::<UnsafeCell<E>>();
        let payload_size = element_size.checked_mul(size).ok_or_else(|| {
            MultiProcessError::SharedMemoryError("shared memory size overflow".to_string())
        })?;
        let payload_alignment = align_of::<UnsafeCell<E>>();

        let shmem = ShmemConf::new()
            .os_id(&config.name)
            .open()
            .map_err(|e| MultiProcessError::SegmentNotFound(e.to_string()))?;
        let payload_offset = validate_layout(
            &shmem,
            payload_size,
            element_size,
            size,
            payload_alignment,
            SegmentKind::RingBuffer,
        )?
        .payload_offset;

        let ptr = unsafe { shmem.as_ptr().cast::<u8>().add(payload_offset) as *mut UnsafeCell<E> };
        let slots_ptr = NonNull::new(ptr)
            .ok_or_else(|| MultiProcessError::MemoryMapError("Null pointer".to_string()))?;

        let index_mask = (size - 1) as i64;
        Ok(SharedRingBuffer {
            _shmem: shmem,
            slots_ptr,
            index_mask,
            size,
            is_owner: false,
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
        assert!(sequence >= 0, "ring buffer sequence cannot be negative");

        let sequence =
            usize::try_from(sequence).expect("validated non-negative sequence before indexing");
        let index_mask =
            usize::try_from(self.index_mask).expect("ring index mask must be non-negative");
        let index = sequence & index_mask;
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

    #[test]
    #[should_panic(expected = "ring buffer sequence cannot be negative")]
    fn test_get_panics_on_negative_sequence() {
        let config = SharedMemoryConfig {
            name: "test_negative_get".to_string(),
            buffer_size: 8,
            element_size: std::mem::size_of::<i32>(),
            create: true,
        };

        let ring_buffer = SharedRingBuffer::new(config, || 0i32).unwrap();
        let _ = ring_buffer.get(-1);
    }
}
