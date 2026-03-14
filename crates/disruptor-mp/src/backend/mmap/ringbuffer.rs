//! File-backed mmap ring buffer for multi-process communication.
//!
//! This backend mirrors the shared-memory ring buffer layout but uses a file
//! mapping as the transport. It is the first building block for the Python mmap
//! data-plane tracked by `N03`.

use crate::{
    shared_memory_layout::{
        required_layout_size, validate_layout_bytes, write_layout_bytes, SegmentKind,
    },
    MmapFileConfig, MultiProcessError, MultiProcessResult,
};
use disruptor_core::Sequence;
use memmap2::{MmapMut, MmapOptions};
use std::cell::UnsafeCell;
use std::fs::OpenOptions;
use std::mem::{align_of, size_of};
use std::path::Path;
use std::ptr::NonNull;

/// Ring buffer stored in a file-backed mmap region for multi-process access.
pub struct MmapRingBuffer<E> {
    _mmap: MmapMut,
    slots_ptr: NonNull<UnsafeCell<E>>,
    index_mask: i64,
    size: usize,
    path: std::path::PathBuf,
    is_owner: bool,
}

unsafe impl<E> Send for MmapRingBuffer<E> {}
unsafe impl<E> Sync for MmapRingBuffer<E> {}

impl<E> Drop for MmapRingBuffer<E> {
    fn drop(&mut self) {
        if self.is_owner {
            // File lifecycle is intentionally explicit for now so attachers can
            // outlive the creator while N03 is still stabilizing.
        }
    }
}

fn is_pow_of_2(num: usize) -> bool {
    num != 0 && (num & (num - 1) == 0)
}

fn validate_event_config(expected: usize, configured: usize) -> MultiProcessResult<()> {
    if expected != configured {
        return Err(MultiProcessError::IncompatibleLayout(format!(
            "mmap event element size mismatch: expected {}, configured {}",
            expected, configured
        )));
    }
    Ok(())
}

impl<E> MmapRingBuffer<E>
where
    E: Copy + Default,
{
    /// Create or attach a file-backed mmap ring buffer.
    pub fn new<F>(config: MmapFileConfig, mut event_factory: F) -> MultiProcessResult<Self>
    where
        F: FnMut() -> E,
    {
        assert!(
            !config.path.as_os_str().is_empty(),
            "mmap ring buffer path must not be empty"
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

        validate_event_config(size_of::<E>(), config.element_size)?;

        let size = config.buffer_size;
        let element_size = size_of::<UnsafeCell<E>>();
        let payload_size = element_size.checked_mul(size).ok_or_else(|| {
            MultiProcessError::SharedMemoryError("mmap layout size overflow".to_string())
        })?;
        let payload_alignment = align_of::<UnsafeCell<E>>();

        ensure_parent_dir(&config.path)?;

        let file = if config.create {
            let required_size = required_layout_size(payload_size, payload_alignment)?;
            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&config.path)
                .map_err(|error| MultiProcessError::SharedMemoryError(error.to_string()))?;
            file.set_len(required_size as u64)
                .map_err(|error| MultiProcessError::SharedMemoryError(error.to_string()))?;
            file
        } else {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&config.path)
                .map_err(|error| MultiProcessError::SegmentNotFound(error.to_string()))?
        };

        let mut mmap = unsafe {
            MmapOptions::new()
                .map_mut(&file)
                .map_err(|error| MultiProcessError::MemoryMapError(error.to_string()))?
        };

        let payload_offset = if config.create {
            write_layout_bytes(
                mmap.as_mut_ptr(),
                mmap.len(),
                payload_size,
                element_size,
                size,
                payload_alignment,
                SegmentKind::RingBuffer,
            )?
            .payload_offset
        } else {
            validate_layout_bytes(
                mmap.as_ptr(),
                mmap.len(),
                payload_size,
                element_size,
                size,
                payload_alignment,
                SegmentKind::RingBuffer,
            )?
            .payload_offset
        };

        let ptr = unsafe { mmap.as_mut_ptr().add(payload_offset) as *mut UnsafeCell<E> };
        let slots_ptr = NonNull::new(ptr)
            .ok_or_else(|| MultiProcessError::MemoryMapError("Null mmap pointer".to_string()))?;

        if config.create {
            unsafe {
                for index in 0..size {
                    std::ptr::write(ptr.add(index), UnsafeCell::new(event_factory()));
                }
            }
        }

        let index_mask = (size - 1) as i64;
        Ok(Self {
            _mmap: mmap,
            slots_ptr,
            index_mask,
            size,
            path: config.path,
            is_owner: config.create,
        })
    }

    /// Attach to an existing file-backed mmap ring buffer.
    pub fn attach(config: MmapFileConfig) -> MultiProcessResult<Self> {
        assert!(
            !config.create,
            "MmapRingBuffer::attach requires create = false"
        );
        Self::new(config, E::default)
    }

    #[inline]
    fn wrap_point(&self, sequence: Sequence) -> Sequence {
        sequence - self.size() as i64
    }

    #[inline]
    /// Compute the currently available free slots for the producer.
    pub fn free_slots(&self, producer: Sequence, highest_read_by_consumers: Sequence) -> i64 {
        let wrap_point = self.wrap_point(producer);
        highest_read_by_consumers - wrap_point
    }

    /// Get a mutable pointer to the element at the given sequence.
    ///
    /// # Safety
    /// Callers must ensure the returned slot is accessed according to the
    /// single-writer / coordinated-reader contract for the ring buffer.
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
    /// Return the configured ring capacity.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Return the backing path for this mmap ring.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn ensure_parent_dir(path: &Path) -> MultiProcessResult<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }

    std::fs::create_dir_all(parent)
        .map_err(|error| MultiProcessError::SharedMemoryError(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_test_path(prefix: &str) -> std::path::PathBuf {
        let pid = std::process::id();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be valid")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}_{pid}_{nanos}.mmap"))
    }

    #[test]
    fn create_and_attach_share_data() {
        let path = unique_test_path("mmap_ring");
        let config_create = MmapFileConfig {
            path: path.clone(),
            buffer_size: 8,
            element_size: size_of::<u64>(),
            create: true,
        };
        let config_attach = MmapFileConfig {
            path: path.clone(),
            buffer_size: 8,
            element_size: size_of::<u64>(),
            create: false,
        };

        {
            let owner = MmapRingBuffer::<u64>::new(config_create, || 0u64).unwrap();
            let attached = MmapRingBuffer::<u64>::attach(config_attach).unwrap();

            unsafe {
                *owner.get(0) = 41;
                *attached.get(1) = 99;
            }

            let first = unsafe { *attached.get(0) };
            let second = unsafe { *owner.get(1) };
            assert_eq!(first, 41);
            assert_eq!(second, 99);
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn attach_rejects_mismatched_layout() {
        let path = unique_test_path("mmap_ring_mismatch");
        let config_create = MmapFileConfig {
            path: path.clone(),
            buffer_size: 8,
            element_size: size_of::<u64>(),
            create: true,
        };
        let config_attach = MmapFileConfig {
            path: path.clone(),
            buffer_size: 8,
            element_size: size_of::<u32>(),
            create: false,
        };

        let _owner = MmapRingBuffer::new(config_create, || 0u64).unwrap();
        let error = match MmapRingBuffer::<u64>::attach(config_attach) {
            Ok(_) => panic!("expected mismatched layout to fail"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            MultiProcessError::IncompatibleLayout(message) if message.contains("element size mismatch")
        ));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn free_slots_matches_shared_memory_math() {
        let path = unique_test_path("mmap_ring_slots");
        let config = MmapFileConfig {
            path: path.clone(),
            buffer_size: 8,
            element_size: size_of::<u64>(),
            create: true,
        };

        let ring_buffer = MmapRingBuffer::<u64>::new(config, || 0u64).unwrap();
        assert_eq!(1, ring_buffer.free_slots(7, 0));
        assert_eq!(0, ring_buffer.free_slots(8, 0));
        assert_eq!(8, ring_buffer.free_slots(0, 0));
        assert_eq!(4, ring_buffer.free_slots(3, -1));

        drop(ring_buffer);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    #[should_panic(expected = "ring buffer sequence cannot be negative")]
    fn get_panics_on_negative_sequence() {
        let path = unique_test_path("mmap_ring_negative");
        let config = MmapFileConfig {
            path: path.clone(),
            buffer_size: 8,
            element_size: size_of::<u64>(),
            create: true,
        };

        let ring_buffer = MmapRingBuffer::<u64>::new(config, || 0u64).unwrap();
        let _ = ring_buffer.get(-1);
    }
}
