//! Canonical file layout for the mmap multiprocess transport.
//!
//! This groups the ring and cursor files under one validated naming contract so
//! later mmap producer/consumer wiring can reuse the same control-plane handle.

use crate::{MmapCursorConfig, MmapFileConfig, MultiProcessError, MultiProcessResult};
use std::path::{Path, PathBuf};

const RING_SUFFIX: &str = ".ring";
const PRODUCER_CURSOR_SUFFIX: &str = ".producer.cursor";
const READINESS_CURSOR_SUFFIX: &str = ".ready.cursor";
const CONSUMERS_DIR_SUFFIX: &str = ".consumers";
const CONSUMER_CURSOR_SUFFIX: &str = ".cursor";

/// Stable file layout for one mmap-backed disruptor transport instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MmapTransportLayout {
    root_dir: PathBuf,
    segment_name: String,
    ring_path: PathBuf,
    producer_cursor_path: PathBuf,
    readiness_cursor_path: PathBuf,
    consumers_dir: PathBuf,
}

impl MmapTransportLayout {
    /// Create a new validated mmap transport layout rooted under `root_dir`.
    pub fn new(
        root_dir: impl Into<PathBuf>,
        segment_name: impl Into<String>,
    ) -> MultiProcessResult<Self> {
        let root_dir = root_dir.into();
        if root_dir.as_os_str().is_empty() {
            return Err(MultiProcessError::SharedMemoryError(
                "mmap root directory must not be empty".to_string(),
            ));
        }

        let segment_name = segment_name.into();
        validate_component("segment_name", &segment_name)?;

        let ring_path = root_dir.join(format!("{segment_name}{RING_SUFFIX}"));
        let producer_cursor_path = root_dir.join(format!("{segment_name}{PRODUCER_CURSOR_SUFFIX}"));
        let readiness_cursor_path =
            root_dir.join(format!("{segment_name}{READINESS_CURSOR_SUFFIX}"));
        let consumers_dir = root_dir.join(format!("{segment_name}{CONSUMERS_DIR_SUFFIX}"));

        Ok(Self {
            root_dir,
            segment_name,
            ring_path,
            producer_cursor_path,
            readiness_cursor_path,
            consumers_dir,
        })
    }

    /// Ensure the root and consumer directories exist.
    pub fn ensure_directories(&self) -> MultiProcessResult<()> {
        std::fs::create_dir_all(&self.root_dir)
            .map_err(|error| MultiProcessError::SharedMemoryError(error.to_string()))?;
        std::fs::create_dir_all(&self.consumers_dir)
            .map_err(|error| MultiProcessError::SharedMemoryError(error.to_string()))?;
        Ok(())
    }

    /// Build the ring buffer config for this transport.
    pub fn ring_config(
        &self,
        buffer_size: usize,
        element_size: usize,
        create: bool,
    ) -> MmapFileConfig {
        MmapFileConfig {
            path: self.ring_path.clone(),
            buffer_size,
            element_size,
            create,
        }
    }

    /// Build the producer cursor config for this transport.
    pub fn producer_cursor_config(&self, create: bool) -> MmapCursorConfig {
        MmapCursorConfig {
            path: self.producer_cursor_path.clone(),
            create,
        }
    }

    /// Build the readiness cursor config for this transport.
    pub fn readiness_cursor_config(&self, create: bool) -> MmapCursorConfig {
        MmapCursorConfig {
            path: self.readiness_cursor_path.clone(),
            create,
        }
    }

    /// Build a consumer cursor config for the given consumer id.
    pub fn consumer_cursor_config(
        &self,
        consumer_id: &str,
        create: bool,
    ) -> MultiProcessResult<MmapCursorConfig> {
        Ok(MmapCursorConfig {
            path: self.consumer_cursor_path(consumer_id)?,
            create,
        })
    }

    /// Return the consumer cursor path for a specific consumer id.
    pub fn consumer_cursor_path(&self, consumer_id: &str) -> MultiProcessResult<PathBuf> {
        validate_component("consumer_id", consumer_id)?;
        Ok(self
            .consumers_dir
            .join(format!("{consumer_id}{CONSUMER_CURSOR_SUFFIX}")))
    }

    /// Return the layout root directory.
    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    /// Return the logical segment name for this layout.
    pub fn segment_name(&self) -> &str {
        &self.segment_name
    }

    /// Return the ring mapping path.
    pub fn ring_path(&self) -> &Path {
        &self.ring_path
    }

    /// Return the producer cursor mapping path.
    pub fn producer_cursor_path(&self) -> &Path {
        &self.producer_cursor_path
    }

    /// Return the readiness cursor mapping path.
    pub fn readiness_cursor_path(&self) -> &Path {
        &self.readiness_cursor_path
    }

    /// Return the directory holding per-consumer cursor files.
    pub fn consumers_dir(&self) -> &Path {
        &self.consumers_dir
    }
}

fn validate_component(label: &str, value: &str) -> MultiProcessResult<()> {
    if value.is_empty() {
        return Err(MultiProcessError::SharedMemoryError(format!(
            "{label} must not be empty"
        )));
    }
    if value
        .bytes()
        .any(|byte| byte == b'/' || byte == b'\\' || byte == 0)
    {
        return Err(MultiProcessError::SharedMemoryError(format!(
            "{label} must not contain path separators or NUL bytes"
        )));
    }
    if value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Ok(());
    }

    Err(MultiProcessError::SharedMemoryError(format!(
        "{label} must use only ASCII letters, digits, '_' or '-'"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_root(prefix: &str) -> PathBuf {
        let pid = std::process::id();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be valid")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}_{pid}_{nanos}"))
    }

    #[test]
    fn builds_canonical_paths_and_configs() {
        let root = unique_root("mmap_transport");
        let layout = MmapTransportLayout::new(root.clone(), "queue01").unwrap();

        assert_eq!(layout.root_dir(), root.as_path());
        assert_eq!(layout.segment_name(), "queue01");
        assert_eq!(layout.ring_path(), root.join("queue01.ring").as_path());
        assert_eq!(
            layout.producer_cursor_path(),
            root.join("queue01.producer.cursor").as_path()
        );
        assert_eq!(
            layout.readiness_cursor_path(),
            root.join("queue01.ready.cursor").as_path()
        );
        assert_eq!(
            layout.consumers_dir(),
            root.join("queue01.consumers").as_path()
        );

        let ring = layout.ring_config(1024, 256, true);
        assert_eq!(ring.path, root.join("queue01.ring"));
        assert_eq!(ring.buffer_size, 1024);
        assert_eq!(ring.element_size, 256);
        assert!(ring.create);

        let producer_cursor = layout.producer_cursor_config(false);
        assert_eq!(producer_cursor.path, root.join("queue01.producer.cursor"));
        assert!(!producer_cursor.create);

        let readiness_cursor = layout.readiness_cursor_config(true);
        assert_eq!(readiness_cursor.path, root.join("queue01.ready.cursor"));
        assert!(readiness_cursor.create);

        let consumer_cursor = layout.consumer_cursor_config("c0001", true).unwrap();
        assert_eq!(
            consumer_cursor.path,
            root.join("queue01.consumers").join("c0001.cursor")
        );
        assert!(consumer_cursor.create);
    }

    #[test]
    fn rejects_invalid_segment_and_consumer_ids() {
        let root = unique_root("mmap_transport_invalid");

        let error = MmapTransportLayout::new(root.clone(), "queue/01").unwrap_err();
        assert!(error
            .to_string()
            .contains("segment_name must not contain path separators"));

        let layout = MmapTransportLayout::new(root, "queue01").unwrap();
        let error = layout.consumer_cursor_path("bad/id").unwrap_err();
        assert!(error
            .to_string()
            .contains("consumer_id must not contain path separators"));

        let error = layout.consumer_cursor_path("bad.id").unwrap_err();
        assert!(error
            .to_string()
            .contains("consumer_id must use only ASCII letters"));
    }

    #[test]
    fn ensure_directories_creates_root_and_consumer_directory() {
        let root = unique_root("mmap_transport_dirs");
        let layout = MmapTransportLayout::new(root.clone(), "queue01").unwrap();

        assert!(!root.exists());
        layout.ensure_directories().unwrap();

        assert!(root.is_dir());
        assert!(layout.consumers_dir().is_dir());

        let _ = std::fs::remove_dir_all(root);
    }
}
