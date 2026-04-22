//! File-backed mmap cursor for multi-process coordination.
//!
//! This mirrors [`crate::SharedCursor`] but uses a file-backed mapping so the
//! mmap backend can share the same atomic coordination model as the shared
//! memory backend.

use crate::{
    shared_memory_layout::{
        required_layout_size, validate_layout_bytes, write_layout_bytes, SegmentKind,
    },
    MmapCursorConfig, MultiProcessError, MultiProcessResult,
};
use memmap2::{MmapMut, MmapOptions};
use std::fs::OpenOptions;
use std::mem::align_of;
use std::path::Path;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicI64, Ordering};

/// Cache line size for padding (64 bytes on most modern CPUs).
const CACHE_LINE_SIZE: usize = 64;

/// Padded atomic to prevent false sharing.
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

/// File-backed mmap cursor with cache-line padding.
pub struct MmapCursor {
    _mmap: MmapMut,
    cursor_ptr: NonNull<PaddedAtomicI64>,
    path: std::path::PathBuf,
    is_owner: bool,
}

unsafe impl Send for MmapCursor {}
unsafe impl Sync for MmapCursor {}

impl Clone for MmapCursor {
    fn clone(&self) -> Self {
        Self::attach(MmapCursorConfig {
            path: self.path.clone(),
            create: false,
        })
        .expect("mmap cursor clone must attach to existing backing file")
    }
}

impl Drop for MmapCursor {
    fn drop(&mut self) {
        if self.is_owner {
            // File lifecycle remains explicit while the mmap backend is still
            // stabilizing and attachers may outlive the creator.
        }
    }
}

impl MmapCursor {
    /// Create or attach a file-backed mmap cursor.
    pub fn new(config: MmapCursorConfig, initial_value: i64) -> MultiProcessResult<Self> {
        assert!(
            !config.path.as_os_str().is_empty(),
            "mmap cursor path must not be empty"
        );

        ensure_parent_dir(&config.path)?;

        let payload_size = std::mem::size_of::<PaddedAtomicI64>();
        let payload_alignment = align_of::<PaddedAtomicI64>();
        let required_size = required_layout_size(payload_size, payload_alignment)?;

        let file = if config.create {
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
                payload_size,
                1,
                payload_alignment,
                SegmentKind::Cursor,
            )?
            .payload_offset
        } else {
            validate_layout_bytes(
                mmap.as_ptr(),
                mmap.len(),
                payload_size,
                payload_size,
                1,
                payload_alignment,
                SegmentKind::Cursor,
            )?
            .payload_offset
        };

        let ptr = unsafe { mmap.as_mut_ptr().add(payload_offset) as *mut PaddedAtomicI64 };
        let cursor_ptr = NonNull::new(ptr).ok_or_else(|| {
            MultiProcessError::MemoryMapError("Null mmap cursor pointer".to_string())
        })?;

        if config.create {
            unsafe {
                std::ptr::write(ptr, PaddedAtomicI64::new(initial_value));
            }
        }

        Ok(Self {
            _mmap: mmap,
            cursor_ptr,
            path: config.path,
            is_owner: config.create,
        })
    }

    /// Attach to an existing file-backed mmap cursor.
    pub fn attach(config: MmapCursorConfig) -> MultiProcessResult<Self> {
        assert!(!config.create, "MmapCursor::attach requires create = false");
        Self::new(config, 0)
    }

    /// Create a new cursor if it does not exist, or attach to the existing one.
    ///
    /// This is the mmap equivalent of `SharedCursor::new_or_attach` and is required
    /// for restart paths where a logical consumer must reattach to its existing
    /// sequence cursor instead of truncating it back to the initial value.
    pub fn new_or_attach(
        mut config: MmapCursorConfig,
        initial_value: i64,
    ) -> MultiProcessResult<Self> {
        if config.path.exists() {
            config.create = false;
            return Self::attach(config);
        }
        Self::new(config, initial_value)
    }

    /// Return whether this mapping created the backing file.
    pub fn is_owner(&self) -> bool {
        self.is_owner
    }

    /// Load the current value.
    pub fn load(&self, ordering: Ordering) -> i64 {
        unsafe { self.cursor_ptr.as_ref().atomic.load(ordering) }
    }

    /// Store a new value.
    pub fn store(&self, value: i64, ordering: Ordering) {
        unsafe { self.cursor_ptr.as_ref().atomic.store(value, ordering) }
    }

    /// Compare and swap.
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

    /// Fetch and add.
    pub fn fetch_add(&self, value: i64, ordering: Ordering) -> i64 {
        unsafe { self.cursor_ptr.as_ref().atomic.fetch_add(value, ordering) }
    }

    /// Atomic swap.
    pub fn swap(&self, value: i64, ordering: Ordering) -> i64 {
        unsafe { self.cursor_ptr.as_ref().atomic.swap(value, ordering) }
    }

    /// Return the backing path for this mmap cursor.
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
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_test_path(prefix: &str) -> std::path::PathBuf {
        let pid = std::process::id();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be valid")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}_{pid}_{nanos}.cursor"))
    }

    fn truncate_file(path: &Path, len: u64) {
        let file = OpenOptions::new()
            .write(true)
            .open(path)
            .expect("test file should be reopenable for truncation");
        file.set_len(len)
            .expect("test file truncation should succeed");
    }

    #[test]
    fn create_and_attach_share_state() {
        let path = unique_test_path("mmap_cursor");
        let config_create = MmapCursorConfig {
            path: path.clone(),
            create: true,
        };
        let config_attach = MmapCursorConfig {
            path: path.clone(),
            create: false,
        };

        {
            let owner = MmapCursor::new(config_create, 7).unwrap();
            let attached = MmapCursor::attach(config_attach).unwrap();

            assert_eq!(attached.load(Ordering::Acquire), 7);
            owner.store(19, Ordering::Release);
            assert_eq!(attached.load(Ordering::Acquire), 19);
            assert_eq!(attached.fetch_add(2, Ordering::AcqRel), 19);
            assert_eq!(owner.load(Ordering::Acquire), 21);
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn compare_exchange_round_trip() {
        let path = unique_test_path("mmap_cursor_cas");
        let config = MmapCursorConfig {
            path: path.clone(),
            create: true,
        };

        let cursor = MmapCursor::new(config, 3).unwrap();
        assert_eq!(
            cursor.compare_exchange(3, 8, Ordering::AcqRel, Ordering::Acquire),
            Ok(3)
        );
        assert_eq!(cursor.load(Ordering::Acquire), 8);
        assert_eq!(
            cursor.compare_exchange(3, 9, Ordering::AcqRel, Ordering::Acquire),
            Err(8)
        );

        drop(cursor);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn attach_rejects_truncated_layout_header() {
        let path = unique_test_path("mmap_cursor_truncated_header");
        let config_create = MmapCursorConfig {
            path: path.clone(),
            create: true,
        };
        let config_attach = MmapCursorConfig {
            path: path.clone(),
            create: false,
        };

        {
            let owner = MmapCursor::new(config_create, 11).unwrap();
            drop(owner);
        }

        truncate_file(&path, 8);
        let error = match MmapCursor::attach(config_attach) {
            Ok(_) => panic!("expected truncated header attach to fail"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            MultiProcessError::IncompatibleLayout(message)
                if message.contains("layout header")
        ));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn attach_rejects_truncated_payload_region() {
        let path = unique_test_path("mmap_cursor_truncated_payload");
        let config_create = MmapCursorConfig {
            path: path.clone(),
            create: true,
        };
        let config_attach = MmapCursorConfig {
            path: path.clone(),
            create: false,
        };

        {
            let owner = MmapCursor::new(config_create, 17).unwrap();
            drop(owner);
        }

        let file_len = std::fs::metadata(&path)
            .expect("cursor file metadata should exist")
            .len();
        truncate_file(&path, file_len - 1);
        let error = match MmapCursor::attach(config_attach) {
            Ok(_) => panic!("expected truncated payload attach to fail"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            MultiProcessError::IncompatibleLayout(message)
                if message.contains("shared segment too small for layout")
        ));

        let _ = std::fs::remove_file(path);
    }
}
