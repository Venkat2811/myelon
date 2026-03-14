//! Consumer discovery and readiness coordination for the mmap transport.
//!
//! This mirrors the shared-memory consumer barrier at the backend level:
//! per-consumer progress is tracked via one cursor file per consumer and
//! startup coordination uses a separate readiness cursor.

use crate::{MmapCursor, MmapTransportLayout, MultiProcessResult};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// Consumer barrier for the mmap transport.
pub struct MmapConsumerBarrier {
    layout: MmapTransportLayout,
    consumer_cursors: HashMap<String, MmapCursor>,
    producer_cursor: Option<MmapCursor>,
    readiness_cursor: Option<MmapCursor>,
}

impl MmapConsumerBarrier {
    /// Create a barrier that attaches to any existing transport files.
    pub fn attach(layout: MmapTransportLayout) -> Self {
        let producer_cursor = MmapCursor::attach(layout.producer_cursor_config(false)).ok();
        let readiness_cursor = MmapCursor::attach(layout.readiness_cursor_config(false)).ok();

        Self {
            layout,
            consumer_cursors: HashMap::new(),
            producer_cursor,
            readiness_cursor,
        }
    }

    /// Create a barrier and own the readiness cursor for startup coordination.
    pub fn new_with_coordination(layout: MmapTransportLayout) -> MultiProcessResult<Self> {
        layout.ensure_directories()?;
        let readiness_cursor = MmapCursor::new(layout.readiness_cursor_config(true), 0)?;
        let producer_cursor = MmapCursor::attach(layout.producer_cursor_config(false)).ok();

        Ok(Self {
            layout,
            consumer_cursors: HashMap::new(),
            producer_cursor,
            readiness_cursor: Some(readiness_cursor),
        })
    }

    /// Set the producer cursor used when no consumer cursors are yet visible.
    pub fn set_producer_cursor(&mut self, producer_cursor: MmapCursor) {
        self.producer_cursor = Some(producer_cursor);
    }

    /// Return the current readiness count if coordination is configured.
    pub fn readiness_count(&self) -> Option<i64> {
        self.readiness_cursor
            .as_ref()
            .map(|cursor| cursor.load(Ordering::Acquire))
    }

    /// Wait until at least `min_consumers` have signaled readiness.
    pub fn wait_for_consumers_ready(&self, min_consumers: i64, timeout: Duration) -> bool {
        assert!(min_consumers > 0, "min_consumers must be greater than zero");
        assert!(timeout > Duration::ZERO, "timeout must be positive");

        let Some(readiness_cursor) = &self.readiness_cursor else {
            return false;
        };

        let start = Instant::now();
        while start.elapsed() < timeout {
            if readiness_cursor.load(Ordering::Acquire) >= min_consumers {
                return true;
            }
            std::sync::atomic::fence(Ordering::Acquire);
            std::hint::spin_loop();
        }

        false
    }

    /// Discover consumer cursor files present under the layout consumer directory.
    pub fn discover_consumers(&mut self) -> MultiProcessResult<usize> {
        let mut discovered = 0usize;

        let read_dir = match std::fs::read_dir(self.layout.consumers_dir()) {
            Ok(read_dir) => read_dir,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => {
                return Err(crate::MultiProcessError::SharedMemoryError(
                    error.to_string(),
                ))
            }
        };

        for entry in read_dir {
            let entry = entry
                .map_err(|error| crate::MultiProcessError::SharedMemoryError(error.to_string()))?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("cursor") {
                continue;
            }

            let Some(consumer_id) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            if self.consumer_cursors.contains_key(consumer_id) {
                continue;
            }

            let cursor =
                match MmapCursor::attach(self.layout.consumer_cursor_config(consumer_id, false)?) {
                    Ok(cursor) => cursor,
                    Err(_) => continue,
                };

            self.consumer_cursors
                .insert(consumer_id.to_string(), cursor);
            discovered += 1;
        }

        Ok(discovered)
    }

    /// Return the minimum visible consumer sequence after a discovery pass.
    pub fn min_consumer_sequence(&mut self) -> MultiProcessResult<i64> {
        self.discover_consumers()?;

        if self.consumer_cursors.is_empty() {
            return Ok(self.fallback_sequence());
        }

        let mut minimum = i64::MAX;
        for cursor in self.consumer_cursors.values() {
            minimum = minimum.min(cursor.load(Ordering::Acquire));
        }
        Ok(minimum)
    }

    /// Return the minimum consumer sequence, ignoring transient filesystem
    /// discovery failures and falling back to the producer sequence when needed.
    pub fn best_effort_min_consumer_sequence(&mut self) -> i64 {
        self.min_consumer_sequence()
            .unwrap_or_else(|_| self.fallback_sequence())
    }

    /// Discover consumers and return the current count, ignoring transient
    /// discovery failures in the hot path.
    pub fn best_effort_consumer_count(&mut self) -> usize {
        let _ = self.discover_consumers();
        self.consumer_cursors.len()
    }

    /// Return the number of discovered consumer cursors.
    pub fn discovered_consumer_count(&self) -> usize {
        self.consumer_cursors.len()
    }

    fn fallback_sequence(&self) -> i64 {
        self.producer_cursor
            .as_ref()
            .map(|cursor| cursor.load(Ordering::Acquire))
            .unwrap_or(-1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
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
    fn discovers_created_consumer_cursor_files() {
        let root = unique_root("mmap_barrier_discovery");
        let layout = MmapTransportLayout::new(root.clone(), "queue01").unwrap();
        layout.ensure_directories().unwrap();

        let _consumer_a =
            MmapCursor::new(layout.consumer_cursor_config("c0001", true).unwrap(), 7).unwrap();
        let _consumer_b =
            MmapCursor::new(layout.consumer_cursor_config("c0002", true).unwrap(), 3).unwrap();

        let mut barrier = MmapConsumerBarrier::attach(layout.clone());
        assert_eq!(barrier.discover_consumers().unwrap(), 2);
        assert_eq!(barrier.discovered_consumer_count(), 2);
        assert_eq!(barrier.min_consumer_sequence().unwrap(), 3);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn falls_back_to_producer_cursor_when_no_consumers_exist() {
        let root = unique_root("mmap_barrier_fallback");
        let layout = MmapTransportLayout::new(root.clone(), "queue01").unwrap();
        layout.ensure_directories().unwrap();

        let producer_cursor = MmapCursor::new(layout.producer_cursor_config(true), 19).unwrap();

        let mut barrier = MmapConsumerBarrier::attach(layout);
        barrier.set_producer_cursor(producer_cursor);

        assert_eq!(barrier.min_consumer_sequence().unwrap(), 19);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn waits_for_readiness_cursor_threshold() {
        let root = unique_root("mmap_barrier_ready");
        let layout = MmapTransportLayout::new(root.clone(), "queue01").unwrap();
        let barrier = MmapConsumerBarrier::new_with_coordination(layout.clone()).unwrap();
        let readiness = MmapCursor::attach(layout.readiness_cursor_config(false)).unwrap();

        readiness.store(2, Ordering::Release);
        assert!(barrier.wait_for_consumers_ready(2, Duration::from_millis(20)));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_count_is_none_without_coordination() {
        let root = unique_root("mmap_barrier_no_ready");
        let layout = MmapTransportLayout::new(root.clone(), "queue01").unwrap();
        let barrier = MmapConsumerBarrier::attach(layout);

        assert_eq!(barrier.readiness_count(), None);
    }
}
