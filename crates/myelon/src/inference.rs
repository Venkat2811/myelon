//! Fixed-topology inference helpers over the curated `disruptor-mp` façade.
//!
//! This module keeps the first myelon domain API intentionally small:
//! a scheduler-facing producer and a fixed set of worker consumers.
//! Worker counts are constrained to 2-8 via [`WorkerCount`] so topology
//! semantics stay explicit in the type system.
//!
//! ```rust,no_run
//! use myelon::inference::{FixedTopology, WorkerCount};
//! use std::time::Duration;
//!
//! #[derive(Copy, Clone, Default)]
//! struct InferenceEvent {
//!     token_id: u32,
//!     worker_id: u16,
//!     end_of_batch: bool,
//! }
//!
//! let topology = FixedTopology::new("infer_demo", 1024, WorkerCount::Three)
//!     .with_coordination_timeout(Duration::from_secs(5));
//!
//! let _scheduler_builder = topology.scheduler_builder::<InferenceEvent>();
//! for worker_index in topology.worker_indices() {
//!     let _worker_builder = topology.worker_builder::<InferenceEvent>(worker_index).unwrap();
//! }
//! ```

use crate::{
    attach_shared_consumer, build_shared_single_producer, MultiProcessError, SharedConsumer,
    SharedDisruptorBuilder, SharedProducer,
};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::time::Duration;

const DEFAULT_COORDINATION_TIMEOUT: Duration = Duration::from_secs(15);

/// Constrained worker count for the fixed inference topology. Limited
/// to the 2..=8 range so a topology shape stays expressible in the type
/// system rather than as a free `usize`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum WorkerCount {
    /// Two workers.
    Two,
    /// Three workers.
    Three,
    /// Four workers.
    Four,
    /// Five workers.
    Five,
    /// Six workers.
    Six,
    /// Seven workers.
    Seven,
    /// Eight workers.
    Eight,
}

impl WorkerCount {
    /// Numeric worker count.
    pub const fn as_usize(self) -> usize {
        match self {
            Self::Two => 2,
            Self::Three => 3,
            Self::Four => 4,
            Self::Five => 5,
            Self::Six => 6,
            Self::Seven => 7,
            Self::Eight => 8,
        }
    }

    /// Build a [`WorkerCount`] from a numeric value, returning `None`
    /// if the value is outside the supported 2..=8 range.
    pub const fn from_usize(value: usize) -> Option<Self> {
        match value {
            2 => Some(Self::Two),
            3 => Some(Self::Three),
            4 => Some(Self::Four),
            5 => Some(Self::Five),
            6 => Some(Self::Six),
            7 => Some(Self::Seven),
            8 => Some(Self::Eight),
            _ => None,
        }
    }
}

/// Errors returned by topology construction and worker attachment.
#[derive(Debug)]
pub enum InferenceTopologyError {
    /// The supplied worker index is outside `0..worker_count`.
    InvalidWorkerIndex {
        /// Worker index that was supplied.
        index: usize,
        /// Configured worker count for the topology.
        worker_count: usize,
    },
    /// A failure surfaced from the underlying multiprocess transport.
    MultiProcess(MultiProcessError),
}

impl Display for InferenceTopologyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidWorkerIndex { index, worker_count } => write!(
                f,
                "worker index {index} is out of range for fixed topology with {worker_count} workers"
            ),
            Self::MultiProcess(error) => Display::fmt(error, f),
        }
    }
}

impl Error for InferenceTopologyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidWorkerIndex { .. } => None,
            Self::MultiProcess(error) => Some(error),
        }
    }
}

impl From<MultiProcessError> for InferenceTopologyError {
    fn from(error: MultiProcessError) -> Self {
        Self::MultiProcess(error)
    }
}

/// Result alias for fallible topology / worker operations.
pub type InferenceTopologyResult<T> = Result<T, InferenceTopologyError>;

/// Single-scheduler / N-worker topology over a SHM ring buffer.
/// Scheduler publishes to a ring named `segment_name`; workers attach
/// as consumers under stable, prefix-derived consumer IDs.
#[derive(Clone, Debug)]
pub struct FixedTopology {
    segment_name: String,
    buffer_size: usize,
    worker_count: WorkerCount,
    coordination_timeout: Duration,
}

impl FixedTopology {
    /// Construct a topology with the given segment name, ring depth
    /// (`buffer_size`), and worker count. Coordination timeout
    /// defaults to 15 seconds; override with
    /// [`Self::with_coordination_timeout`].
    pub fn new(
        segment_name: impl Into<String>,
        buffer_size: usize,
        worker_count: WorkerCount,
    ) -> Self {
        Self {
            segment_name: segment_name.into(),
            buffer_size,
            worker_count,
            coordination_timeout: DEFAULT_COORDINATION_TIMEOUT,
        }
    }

    /// Configured shared-memory segment name.
    pub fn segment_name(&self) -> &str {
        &self.segment_name
    }

    /// Configured ring depth in slots.
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Configured worker count.
    pub const fn worker_count(&self) -> WorkerCount {
        self.worker_count
    }

    /// Configured coordination timeout for scheduler/worker rendezvous.
    pub const fn coordination_timeout(&self) -> Duration {
        self.coordination_timeout
    }

    /// Iterator-friendly range of valid worker indices.
    pub fn worker_indices(&self) -> std::ops::Range<usize> {
        0..self.worker_count.as_usize()
    }

    /// Replace the coordination timeout, returning `self` for chaining.
    pub fn with_coordination_timeout(mut self, timeout: Duration) -> Self {
        self.coordination_timeout = timeout;
        self
    }

    /// Stable consumer ID for the given worker index.
    ///
    /// # Errors
    ///
    /// Returns [`InferenceTopologyError::InvalidWorkerIndex`] when
    /// `worker_index` is outside `0..self.worker_count()`.
    pub fn worker_consumer_id(&self, worker_index: usize) -> InferenceTopologyResult<String> {
        self.validate_worker_index(worker_index)?;
        Ok(format!("{}_{}", self.worker_prefix(), worker_index))
    }

    /// Pre-configured [`SharedDisruptorBuilder`] for the scheduler
    /// side. Discovery is wired up to expect exactly
    /// `self.worker_count()` consumers under the topology's worker
    /// prefix.
    pub fn scheduler_builder<E>(&self) -> SharedDisruptorBuilder<E>
    where
        E: Copy + Default + 'static,
    {
        let worker_count = self.worker_count.as_usize();
        build_shared_single_producer::<E>(&self.segment_name, self.buffer_size)
            .discover_consumer_with_prefix(worker_count, &self.worker_prefix())
            .wait_for_consumers(worker_count as i64, self.coordination_timeout)
    }

    /// Construct the scheduler [`SharedProducer<E>`] from
    /// [`Self::scheduler_builder`] and a slot-default factory.
    ///
    /// # Errors
    ///
    /// Forwards [`InferenceTopologyError::MultiProcess`] from the
    /// underlying SHM build.
    pub fn build_scheduler<E, F>(
        &self,
        default_event_fn: F,
    ) -> InferenceTopologyResult<SharedProducer<E>>
    where
        E: Copy + Default + 'static,
        F: FnMut() -> E,
    {
        self.scheduler_builder()
            .build_producer(default_event_fn)
            .map_err(Into::into)
    }

    /// Pre-configured [`SharedDisruptorBuilder`] for the worker at
    /// `worker_index`. Attaches under [`Self::worker_consumer_id`].
    ///
    /// # Errors
    ///
    /// Returns [`InferenceTopologyError::InvalidWorkerIndex`] when
    /// `worker_index` is out of range.
    pub fn worker_builder<E>(
        &self,
        worker_index: usize,
    ) -> InferenceTopologyResult<SharedDisruptorBuilder<E>>
    where
        E: Copy + Default + 'static,
    {
        let consumer_id = self.worker_consumer_id(worker_index)?;
        Ok(
            attach_shared_consumer::<E>(&self.segment_name, self.buffer_size)
                .with_consumer_id(&consumer_id),
        )
    }

    /// Attach the worker at `worker_index` and return its
    /// [`SharedConsumer<E>`].
    ///
    /// # Errors
    ///
    /// Forwards [`InferenceTopologyError::InvalidWorkerIndex`] or
    /// [`InferenceTopologyError::MultiProcess`].
    pub fn attach_worker<E>(
        &self,
        worker_index: usize,
    ) -> InferenceTopologyResult<SharedConsumer<E>>
    where
        E: Copy + Default + 'static,
    {
        self.worker_builder(worker_index)?
            .build_consumer()
            .map_err(Into::into)
    }

    fn validate_worker_index(&self, worker_index: usize) -> InferenceTopologyResult<()> {
        let worker_count = self.worker_count.as_usize();
        if worker_index < worker_count {
            Ok(())
        } else {
            Err(InferenceTopologyError::InvalidWorkerIndex {
                index: worker_index,
                worker_count,
            })
        }
    }

    fn worker_prefix(&self) -> String {
        format!("{}_wk", self.segment_name)
    }
}
