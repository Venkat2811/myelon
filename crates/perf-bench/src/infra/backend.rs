//! Backend abstraction for SHM vs mmap transport.
//!
//! The `RingBackend` trait abstracts the differences between shared-memory
//! and memory-mapped file backends so benchmark logic can be written once
//! and parameterized over the backend.
//!
//! Both backends expose identical producer/consumer APIs:
//! - `Producer::publish(|slot| { ... })`
//! - `Consumer::try_consume_next_leased() -> Option<(Sequence, &E)>`
//!
//! The only differences are creation, naming, and cleanup — which this
//! trait encapsulates.

use disruptor_mp::{
    attach_shared_consumer, build_shared_single_producer, CoordinationMode, MmapConsumer,
    MmapProducer, MmapTransportLayout, MultiProcessResult, SharedConsumer, SharedProducer,
};
use std::path::PathBuf;

/// Opaque handle to a segment (SHM name or mmap root+segment pair).
#[derive(Debug, Clone)]
pub enum SegmentHandle {
    Shm { name: String },
    Mmap { root: PathBuf, segment: String },
}

impl SegmentHandle {
    /// Create an SHM segment handle from a portable name.
    pub fn shm(name: impl Into<String>) -> Self {
        Self::Shm { name: name.into() }
    }

    /// Create an mmap segment handle from a root directory and segment name.
    pub fn mmap(root: impl Into<PathBuf>, segment: impl Into<String>) -> Self {
        Self::Mmap {
            root: root.into(),
            segment: segment.into(),
        }
    }

    /// Get the display name for reporting.
    pub fn display_name(&self) -> &str {
        match self {
            Self::Shm { name } => name,
            Self::Mmap { segment, .. } => segment,
        }
    }

    /// Get the backend label ("shm" or "mmap").
    pub fn backend_label(&self) -> &'static str {
        match self {
            Self::Shm { .. } => "shm",
            Self::Mmap { .. } => "mmap",
        }
    }
}

/// Create a producer for the given segment.
///
/// SHM: uses `build_shared_single_producer` with discovery.
/// Mmap: uses `MmapProducer::create`.
/// Create a producer for the given segment with immediate coordination.
///
/// SHM: uses `build_shared_single_producer` with prefix-based discovery.
/// Mmap: uses `MmapProducer::create`.
pub fn create_producer<E: Copy + Default + Send + 'static>(
    handle: &SegmentHandle,
    buffer_size: usize,
) -> MultiProcessResult<ProducerHandle<E>> {
    match handle {
        SegmentHandle::Shm { name } => {
            let producer = build_shared_single_producer::<E>(name, buffer_size)
                .with_coordination(CoordinationMode::Immediate)
                .build_producer(E::default)?;
            Ok(ProducerHandle::Shm(producer))
        }
        SegmentHandle::Mmap { root, segment } => {
            let layout = MmapTransportLayout::new(root.clone(), segment.clone())?;
            let producer = MmapProducer::<E>::create(layout, buffer_size, || E::default())?;
            Ok(ProducerHandle::Mmap(producer))
        }
    }
}

/// Attach a consumer to the given segment.
///
/// SHM: uses `attach_shared_consumer` with consumer ID.
/// Mmap: uses `MmapConsumer::attach`.
pub fn attach_consumer<E: Copy + Default + Send + 'static>(
    handle: &SegmentHandle,
    buffer_size: usize,
    consumer_id: &str,
) -> MultiProcessResult<ConsumerHandle<E>> {
    match handle {
        SegmentHandle::Shm { name } => {
            let consumer = attach_shared_consumer::<E>(name, buffer_size)
                .with_consumer_id(consumer_id)
                .build_consumer()?;
            Ok(ConsumerHandle::Shm(consumer))
        }
        SegmentHandle::Mmap { root, segment } => {
            let layout = MmapTransportLayout::new(root.clone(), segment.clone())?;
            let consumer = MmapConsumer::<E>::attach(layout, buffer_size, consumer_id)?;
            Ok(ConsumerHandle::Mmap(consumer))
        }
    }
}

/// Clean up segment resources after a benchmark run.
pub fn cleanup(handle: &SegmentHandle) {
    match handle {
        SegmentHandle::Shm { .. } => {
            // SHM segments are cleaned up automatically on last close
        }
        SegmentHandle::Mmap { root, .. } => {
            let _ = std::fs::remove_dir_all(root);
        }
    }
}

/// Type-erased producer handle (SHM or mmap).
#[allow(clippy::large_enum_variant)]
pub enum ProducerHandle<E: Copy + Default + Send + 'static> {
    Shm(SharedProducer<E>),
    Mmap(MmapProducer<E>),
}

impl<E: Copy + Default + Send + 'static> ProducerHandle<E> {
    /// Publish an event using the factory pattern.
    #[inline]
    pub fn publish(&mut self, factory: impl FnOnce(&mut E)) {
        match self {
            Self::Shm(p) => p.publish(factory),
            Self::Mmap(p) => p.publish(factory),
        }
    }
}

/// Type-erased consumer handle (SHM or mmap).
#[allow(clippy::large_enum_variant)]
pub enum ConsumerHandle<E: Copy + Default + Send + 'static> {
    Shm(SharedConsumer<E>),
    Mmap(MmapConsumer<E>),
}

impl<E: Copy + Default + Send + 'static> ConsumerHandle<E> {
    /// Try to consume the next event (copied).
    ///
    /// Uses copy-based consume for backend uniformity. The copy cost is
    /// negligible compared to the IPC transit being measured.
    #[inline]
    pub fn try_consume_next(&mut self) -> Option<(disruptor_mp::Sequence, E)> {
        match self {
            Self::Shm(c) => c.try_consume_next(),
            Self::Mmap(c) => c.try_consume_next(),
        }
    }

    /// Signal readiness to the producer.
    pub fn signal_readiness(&self) {
        match self {
            Self::Shm(c) => c.signal_readiness(),
            Self::Mmap(c) => c.signal_readiness(),
        }
    }

    /// Access the underlying SHM consumer (for leased access in hot paths).
    pub fn as_shm(&mut self) -> Option<&mut SharedConsumer<E>> {
        match self {
            Self::Shm(c) => Some(c),
            Self::Mmap(_) => None,
        }
    }

    /// Access the underlying mmap consumer (for leased access in hot paths).
    pub fn as_mmap(&mut self) -> Option<&mut MmapConsumer<E>> {
        match self {
            Self::Shm(_) => None,
            Self::Mmap(c) => Some(c),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shm_handle_labels() {
        let h = SegmentHandle::shm("test_seg");
        assert_eq!(h.backend_label(), "shm");
        assert_eq!(h.display_name(), "test_seg");
    }

    #[test]
    fn mmap_handle_labels() {
        let h = SegmentHandle::mmap("/tmp/bench", "ring_0");
        assert_eq!(h.backend_label(), "mmap");
        assert_eq!(h.display_name(), "ring_0");
    }
}
