//! Multi-process producer implementation.
//!
//! This module provides the [`SharedProducer`] type for publishing events to a shared memory
//! ring buffer that can be consumed by multiple processes. It supports various coordination
//! and discovery modes for different deployment scenarios.

use super::consumer_barrier::{DiscoveryMode, SharedConsumerBarrier};
use crate::required_consumer::{
    RequiredConsumerError, RequiredConsumerLivenessConfig, RequiredConsumerLivenessState,
};
use crate::{SharedCursor, SharedRingBuffer};
use disruptor_core::{MissingFreeSlots, RingBufferFull, Sequence};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// Coordination mode for multiprocess producer startup
#[derive(Debug, Clone, Default)]
pub enum CoordinationMode {
    /// Start producing immediately (for external coordination - benchmark baseline only)
    #[default]
    Immediate,
    /// Wait for at least N consumers to be ready before starting production
    WaitForConsumers {
        /// Minimum number of consumers to wait for
        min_consumers: i64,
        /// Maximum time to wait for consumers
        timeout: Duration,
    },
    /// Buffer initial events until consumers attach (future enhancement)
    BufferUntilConsumers {
        /// Maximum number of events to buffer
        max_buffer: usize,
    },
}

impl CoordinationMode {
    /// Create a coordination mode that waits for consumers
    pub fn wait_for_consumers(min_consumers: i64, timeout: Duration) -> Self {
        assert!(min_consumers > 0, "min_consumers must be greater than zero");
        CoordinationMode::WaitForConsumers {
            min_consumers,
            timeout,
        }
    }

    /// Create a coordination mode for single consumer scenarios
    pub fn wait_for_single_consumer(timeout: Duration) -> Self {
        assert!(!timeout.is_zero(), "timeout must be greater than zero");
        CoordinationMode::WaitForConsumers {
            min_consumers: 1,
            timeout,
        }
    }
}

/// Multi-process producer for publishing events to shared memory ring buffer.
///
/// The producer maintains coordination with multiple consumers across processes,
/// supports various startup coordination modes, and provides both blocking and
/// non-blocking publish operations. It uses a consumer barrier to track all
/// attached consumers and ensure proper backpressure handling.
pub struct SharedProducer<E> {
    ring_buffer: SharedRingBuffer<E>,
    producer_sequence: SharedCursor,
    /// Consumer barrier for tracking all consumers (replaces `min_consumer_sequence`)
    pub(crate) consumer_barrier: SharedConsumerBarrier,
    /// Next sequence to be published
    sequence: Sequence,
    /// Highest sequence available for publication because consumers are behind
    sequence_clear_of_consumers: Sequence,
    /// Whether we've completed initial coordination
    pub(crate) coordination_completed: bool,
    /// Optional producer-side liveness policy for a required consumer set.
    required_consumer_liveness: Option<RequiredConsumerLivenessState>,
    /// Aeron-style hot-path counters (RFC 0040). All-`None` while no
    /// counters file is attached; `attach_counters` populates them.
    counters: ProducerCounters,
}

/// Producer-side observability handles. Each field is a relaxed-atomic
/// counter sitting in a shared `CountersFile`. Hot-path increments
/// branch on `Option::Some` once per `publish`/`try_publish`; the branch
/// is amortised to ~1 ns under prediction. RFC 0040 §Counters defines
/// the canonical IDs.
#[derive(Default, Debug)]
pub struct ProducerCounters {
    /// `events_published` — successful publishes.
    pub events_published: Option<crate::observability::CounterHandle>,
    /// `producer_full_events` — `try_publish` saw ring full.
    pub producer_full_events: Option<crate::observability::CounterHandle>,
}

/// Select which producer-side counters are attached to a shared counters file.
///
/// The full set is appropriate for normal observability. Targeted microbenchmarks can
/// deliberately narrow the set to reduce hot-path perturbation while still exposing the
/// counters required for a specific experiment.
#[derive(Clone, Copy, Debug)]
pub struct ProducerCounterSelection {
    /// Attach the monotonic `events_published` counter.
    pub events_published: bool,
    /// Attach the `producer_full_events` backpressure counter.
    pub producer_full_events: bool,
}

impl ProducerCounterSelection {
    /// Full RFC-0040 producer counter set.
    pub const FULL: Self = Self {
        events_published: true,
        producer_full_events: true,
    };

    /// Minimal producer counter set for experiments that only need publish progress.
    pub const LITE: Self = Self {
        events_published: true,
        producer_full_events: false,
    };
}

impl<E> SharedProducer<E>
where
    E: Copy + Default,
{
    pub(crate) fn new_with_coordination_and_discovery(
        ring_buffer: SharedRingBuffer<E>,
        producer_sequence: SharedCursor,
        base_name: String,
        coordination_mode: CoordinationMode,
        discovery_mode: DiscoveryMode,
        consumer_registration: Option<SharedCursor>,
    ) -> Self {
        // Initialize producer sequence to -1 (no events published yet).
        // Release publishing makes the initial state visible before consumers start publishing.
        producer_sequence.store(-1, std::sync::atomic::Ordering::Release);

        // Create consumer barrier with coordination support only when needed
        let mut consumer_barrier = match &coordination_mode {
            CoordinationMode::WaitForConsumers { .. } => {
                // Only create coordination structures when actually needed
                SharedConsumerBarrier::new_with_coordination_and_discovery(
                    base_name.clone(),
                    discovery_mode.clone(),
                )
                .unwrap_or_else(|_| {
                    SharedConsumerBarrier::new_with_discovery(base_name, discovery_mode)
                })
            }
            _ => {
                // Use simple barrier without coordination overhead
                SharedConsumerBarrier::new_with_discovery(base_name, discovery_mode)
            }
        };

        // Set the producer sequence reference so the barrier can handle no-consumer case
        if let Some(consumer_registration) = consumer_registration {
            consumer_barrier.set_consumer_registration(consumer_registration);
        }
        consumer_barrier.set_producer_sequence(producer_sequence.clone());

        // Start with all slots available since no consumer has consumed anything
        let sequence_clear_of_consumers = ring_buffer.size() as i64 - 1;
        Self {
            ring_buffer,
            producer_sequence,
            consumer_barrier,
            sequence: 0,
            sequence_clear_of_consumers,
            coordination_completed: false,
            required_consumer_liveness: None,
            counters: ProducerCounters::default(),
        }
    }

    /// Register producer counters in the supplied counters file and
    /// store handles on this producer. After this call, hot-path
    /// `publish` / `try_publish` operations record into the file with
    /// one relaxed atomic increment per event. RFC 0040 §Counters.
    pub fn attach_counters(&mut self, file: &crate::observability::CountersFile) {
        self.attach_counters_selected(file, ProducerCounterSelection::FULL);
    }

    /// Register a selected subset of producer counters in the supplied counters file.
    pub fn attach_counters_selected(
        &mut self,
        file: &crate::observability::CountersFile,
        selection: ProducerCounterSelection,
    ) {
        use crate::observability::{ids, COUNTER_FLAG_PRODUCER};
        self.counters.events_published = if selection.events_published {
            file.register(
                ids::EVENTS_PUBLISHED,
                COUNTER_FLAG_PRODUCER,
                "events_published",
            )
        } else {
            None
        };
        self.counters.producer_full_events = if selection.producer_full_events {
            file.register(
                ids::PRODUCER_FULL_EVENTS,
                COUNTER_FLAG_PRODUCER,
                "producer_full_events",
            )
        } else {
            None
        };
    }

    /// Read-only access to the producer's attached counters. Useful for
    /// tests and aggregator threads.
    pub fn counters(&self) -> &ProducerCounters {
        &self.counters
    }

    /// Record a publish-side latency sample (nanoseconds) through the
    /// `metrics`-rs facade under the histogram name
    /// `disruptor_mp_publish_latency_ns`. The downstream recorder
    /// (Prometheus / OTLP / Debugging) decides how to aggregate the
    /// distribution.
    ///
    /// Cost: one `metrics::histogram!` call (per-process recorder
    /// dispatch). When no recorder is installed the call is a no-op.
    /// When the `metrics` feature is off this method compiles to a
    /// no-op; it stays in the API so call-sites don't need to be
    /// `cfg`-gated.
    #[inline]
    pub fn record_publish_latency_ns(&self, ns: u64) {
        #[cfg(feature = "metrics")]
        metrics::histogram!("disruptor_mp_publish_latency_ns").record(ns as f64);
        #[cfg(not(feature = "metrics"))]
        let _ = ns;
    }

    /// Check if we have enough free slots for publishing n events
    #[inline]
    fn next_sequences(&mut self, n: usize) -> Result<Sequence, MissingFreeSlots> {
        // Skip coordination check - should be completed during creation
        // self.ensure_coordination_completed(); // Removed per-operation overhead

        let n = i64::try_from(n).map_err(|_| MissingFreeSlots(u64::MAX))?;
        assert!(n > 0, "batch size must be greater than zero");

        let n_next = self.last_reserved_sequence(n)?;
        self.update_min_consumer_clearance_window(n, n_next)?;
        Ok(n_next)
    }

    fn last_reserved_sequence(&self, n: i64) -> Result<Sequence, MissingFreeSlots> {
        self.sequence
            .checked_sub(1)
            .and_then(|current| current.checked_add(n))
            .ok_or(MissingFreeSlots(u64::MAX))
    }

    fn update_min_consumer_clearance_window(
        &mut self,
        n: i64,
        n_next: Sequence,
    ) -> Result<(), MissingFreeSlots> {
        if self.sequence_clear_of_consumers >= n_next {
            return Ok(());
        }

        // PERFORMANCE OPTIMIZATION: Reduce expensive barrier checks by batching them
        // Check where consumers are to avoid overwriting unread slots.
        let last_published = self.sequence - 1;
        let rear_sequence_read = self.consumer_barrier.get_min_consumer_sequence();
        let free_slots = self
            .ring_buffer
            .free_slots(last_published, rear_sequence_read);

        if free_slots < n {
            #[cfg(dst)]
            crate::dst::assert_sometimes(
                true,
                "producer blocked",
                format!("free_slots={free_slots} requested={n}"),
            );
            return Err(MissingFreeSlots((n - free_slots) as u64));
        }

        // PERFORMANCE OPTIMIZATION: Cache more aggressively to reduce barrier calls
        // Use all available free slots for better batching (safe because we checked
        // rear_sequence_read for the current cursor snapshot).
        self.sequence_clear_of_consumers = last_published + free_slots;
        Ok(())
    }

    /// Apply update to a single event and publish it
    #[inline]
    fn apply_update<F>(&mut self, update: F) -> Sequence
    where
        F: FnOnce(&mut E),
    {
        assert!(
            self.sequence >= 0,
            "producer sequence must be non-negative while active"
        );

        let sequence = self.sequence;

        // Get mutable access to the event at this sequence
        let event_ptr = self.ring_buffer.get(sequence);
        let event = unsafe { &mut *event_ptr };

        // Apply the update
        update(event);

        // Publish sequence with release ordering so consumer-visible event writes happen-before sequence update.
        self.producer_sequence.store(sequence, Ordering::Release);

        #[cfg(dst)]
        if sequence > 0 && sequence % self.ring_buffer.size() as i64 == 0 {
            crate::dst::assert_sometimes(
                true,
                "ring buffer wraps around",
                format!("sequence={sequence} size={}", self.ring_buffer.size()),
            );
        }

        // Move to next sequence
        self.sequence += 1;

        sequence
    }

    /// Apply updates to a batch of events and publish them.
    #[inline]
    fn apply_batch_updates<F>(&mut self, n: usize, update_fn: F) -> Sequence
    where
        F: Fn(&mut E, usize), // Function that takes event and index
    {
        assert!(
            n > 0,
            "batch publish requires a non-zero number of events to update"
        );

        let n = i64::try_from(n).expect("batch size must fit in Sequence");
        let lower = self.sequence;
        let upper_offset = n.checked_sub(1).expect("batch size is positive");
        let upper = lower
            .checked_add(upper_offset)
            .expect("sequence arithmetic must not overflow");

        // Apply updates to each event in the batch
        for (i, seq) in (lower..=upper).enumerate() {
            let event_ptr = self.ring_buffer.get(seq);
            let event = unsafe { &mut *event_ptr };
            update_fn(event, i);
        }

        // Publish the entire batch by publishing the upper sequence with release ordering.
        self.producer_sequence.store(upper, Ordering::Release);

        // Move sequence forward
        self.sequence += n;

        upper
    }
}

impl<E> SharedProducer<E>
where
    E: Copy + Default,
{
    /// Attempt to publish a single event.
    pub fn try_publish<F>(&mut self, update: F) -> Result<Sequence, RingBufferFull>
    where
        F: FnOnce(&mut E),
    {
        if self.next_sequences(1).is_err() {
            // Record backpressure before bubbling the error out.
            if let Some(h) = &self.counters.producer_full_events {
                h.inc();
            }
            return Err(RingBufferFull);
        }
        let sequence = self.apply_update(update);
        if let Some(h) = &self.counters.events_published {
            h.inc();
        }
        Ok(sequence)
    }

    /// Publish a single event, spinning until a slot is available.
    pub fn publish<F>(&mut self, update: F)
    where
        F: FnOnce(&mut E),
    {
        let mut spun = false;
        while self.next_sequences(1).is_err() {
            if !spun {
                if let Some(h) = &self.counters.producer_full_events {
                    h.inc();
                }
                spun = true;
            }
            std::hint::spin_loop();
        }
        #[cfg(dst)]
        if crate::dst::buggify(file!(), line!()) {
            std::thread::yield_now();
        }
        self.apply_update(update);
        if let Some(h) = &self.counters.events_published {
            h.inc();
        }
    }

    /// Attempt to publish a batch of events using an indexed closure.
    ///
    /// The closure receives `(&mut event, batch_index)`.
    /// Batch size zero is a documented no-op for compatibility with upstream
    /// `Producer` API semantics and returns the previous published sequence.
    pub fn try_batch_publish<F>(
        &mut self,
        n: usize,
        update_fn: F,
    ) -> Result<Sequence, MissingFreeSlots>
    where
        F: Fn(&mut E, usize),
    {
        if n == 0 {
            return Ok(self.sequence - 1);
        }
        self.next_sequences(n)?;
        let sequence = self.apply_batch_updates(n, update_fn);
        Ok(sequence)
    }

    /// Publish a batch of events, spinning until enough slots are available.
    ///
    /// The closure receives `(&mut event, batch_index)`.
    ///
    /// This returns a `Result` so callers can fail fast when capacity is unavailable.
    /// There is no implicit blocking wait in this method.
    pub fn batch_publish<F>(&mut self, n: usize, update_fn: F) -> Result<Sequence, MissingFreeSlots>
    where
        F: Fn(&mut E, usize),
    {
        self.try_batch_publish(n, update_fn)
    }

    /// Compatibility wrapper for the legacy explicit method name.
    pub fn simple_batch_publish<F>(
        &mut self,
        n: usize,
        update_fn: F,
    ) -> Result<Sequence, MissingFreeSlots>
    where
        F: Fn(&mut E, usize),
    {
        self.try_batch_publish(n, update_fn)
    }
}

impl<E> Drop for SharedProducer<E> {
    fn drop(&mut self) {
        // Clean up shared memory segments created by this producer
        // Note: The SharedRingBuffer and SharedCursors have their own Drop implementations
        // that will handle their respective cleanup
    }
}

/// Errors that can occur during publish operations with timeout.
#[derive(Debug, thiserror::Error)]
pub enum PublishTimeoutError {
    /// The publish operation timed out waiting for available slots
    #[error("Publish operation timed out")]
    Timeout,
}

impl<E> SharedProducer<E>
where
    E: Copy + Default,
{
    /// Enable producer-side liveness enforcement for a required consumer set.
    pub fn enable_required_consumer_liveness(
        &mut self,
        config: RequiredConsumerLivenessConfig,
    ) -> &mut Self {
        self.required_consumer_liveness = Some(RequiredConsumerLivenessState::new(config));
        self
    }

    #[cold]
    #[inline(never)]
    fn ensure_required_consumers_ready(&mut self) -> Result<(), RequiredConsumerError> {
        let Some(mut state) = self.required_consumer_liveness.take() else {
            return Ok(());
        };
        if state.startup_completed() {
            self.required_consumer_liveness = Some(state);
            return Ok(());
        }

        let deadline = Instant::now()
            .checked_add(state.startup_wait_timeout())
            .expect("startup_wait_timeout does not fit in Instant");

        loop {
            let missing = state
                .missing_required_consumers(|consumer_id| self.discover_consumer_id(consumer_id));
            if missing.is_empty() {
                let now = Instant::now();
                state.seed_progress(now, |consumer_id| self.consumer_sequence(consumer_id));
                state.mark_startup_completed(now);
                self.required_consumer_liveness = Some(state);
                return Ok(());
            }

            if Instant::now() >= deadline {
                let error = RequiredConsumerError::StartupTimeout { missing };
                self.required_consumer_liveness = Some(state);
                return Err(error);
            }

            super::wait::perform_default_discovery_poll_wait();
        }
    }

    #[cold]
    #[inline(never)]
    fn check_required_consumer_liveness(&mut self) -> Result<(), RequiredConsumerError> {
        let Some(mut state) = self.required_consumer_liveness.take() else {
            return Ok(());
        };

        let now = Instant::now();
        let producer_sequence = self.last_published_sequence();
        let failure = state.evaluate_blocked(now, producer_sequence, |consumer_id| {
            self.consumer_sequence(consumer_id)
        });
        self.required_consumer_liveness = Some(state);

        if let Some(error) = failure {
            return Err(error);
        }

        Ok(())
    }

    /// Attempt to publish an event but give up after `timeout`.
    /// Returns `Ok(sequence)` on success or `Err(PublishTimeoutError::Timeout)` if the timeout expired.
    pub fn publish_with_timeout<F>(
        &mut self,
        timeout: Duration,
        update: F,
    ) -> Result<Sequence, PublishTimeoutError>
    where
        F: FnOnce(&mut E),
    {
        assert!(
            timeout > Duration::ZERO,
            "timeout must be greater than zero"
        );

        let deadline = Instant::now()
            .checked_add(timeout)
            .expect("timeout duration does not fit in Instant");

        // Wait for available slots with timeout
        while self.next_sequences(1).is_err() {
            if Instant::now() >= deadline {
                return Err(PublishTimeoutError::Timeout);
            }
            std::hint::spin_loop();
        }

        // Publish the event
        let sequence = self.apply_update(update);
        Ok(sequence)
    }

    /// Publish one event with required-consumer liveness enforcement.
    ///
    /// Existing `publish()` semantics remain unchanged. This managed path is opt-in and returns a
    /// structured error when startup or recovery requirements are not satisfied.
    pub fn publish_managed<F>(&mut self, update: F) -> Result<Sequence, RequiredConsumerError>
    where
        F: FnOnce(&mut E),
    {
        self.ensure_required_consumers_ready()?;

        let mut update = Some(update);
        loop {
            if self.next_sequences(1).is_ok() {
                #[cfg(dst)]
                if crate::dst::buggify(file!(), line!()) {
                    std::thread::yield_now();
                }
                let sequence =
                    self.apply_update(update.take().expect("managed update is consumed once"));
                return Ok(sequence);
            }

            self.check_required_consumer_liveness()?;
            std::thread::yield_now();
        }
    }

    /// Publish a batch with required-consumer liveness enforcement.
    ///
    /// Batch size zero preserves the existing producer semantics and returns the previously
    /// published sequence without performing liveness checks.
    pub fn publish_batch_managed<F>(
        &mut self,
        n: usize,
        update_fn: F,
    ) -> Result<Sequence, RequiredConsumerError>
    where
        F: Fn(&mut E, usize),
    {
        if n == 0 {
            return Ok(self.sequence - 1);
        }

        self.ensure_required_consumers_ready()?;

        loop {
            if self.next_sequences(n).is_ok() {
                return Ok(self.apply_batch_updates(n, &update_fn));
            }

            self.check_required_consumer_liveness()?;
            std::thread::yield_now();
        }
    }
}

// ----------------------------------------------------------------------------
// Native gating helpers for producers (public API)
// ----------------------------------------------------------------------------
impl<E> SharedProducer<E>
where
    E: Copy + Default,
{
    /// Return the last published sequence observed by consumers.
    /// Uses Acquire ordering to ensure memory visibility of published data.
    pub fn last_published_sequence(&self) -> Sequence {
        self.producer_sequence
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Return the minimum gating sequence across all discovered consumers.
    /// This may discover consumers based on configuration and scan interval.
    pub fn min_gating_sequence(&mut self) -> Sequence {
        #[cfg(dst)]
        if crate::dst::buggify(file!(), line!()) {
            return self.last_published_sequence();
        }
        self.consumer_barrier.get_min_consumer_sequence()
    }

    /// Check if the given sequence has been consumed by all known consumers.
    pub fn is_consumed(&mut self, seq: Sequence) -> bool {
        // Barrier comparison uses current consumer state loaded with producer-side
        // synchronization semantics.
        //
        // This is intentionally acquired on every check so the producer observes
        // the latest consumer progress before advancing lifecycle assumptions.
        self.consumer_barrier.get_min_consumer_sequence() >= seq
    }

    /// Attach a known consumer cursor by explicit consumer id.
    pub fn discover_consumer_id(&mut self, consumer_id: &str) -> bool {
        self.consumer_barrier.discover_consumer_id(consumer_id)
    }

    /// Wait for a known consumer cursor to appear by explicit consumer id.
    pub fn wait_for_consumer_id(&mut self, consumer_id: &str, timeout: Duration) -> bool {
        assert!(timeout > Duration::ZERO, "timeout must be positive");
        let deadline = Instant::now()
            .checked_add(timeout)
            .expect("timeout duration does not fit in Instant");

        loop {
            if self.discover_consumer_id(consumer_id) {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            super::wait::perform_default_discovery_poll_wait();
        }
    }

    /// Return the latest visible sequence for a specific consumer id.
    pub fn consumer_sequence(&mut self, consumer_id: &str) -> Option<Sequence> {
        self.consumer_barrier.consumer_sequence(consumer_id)
    }

    /// Return the number of currently discovered consumers.
    pub fn get_consumer_count(&mut self) -> usize {
        self.consumer_barrier.best_effort_consumer_count()
    }

    /// Wait until the provided sequence is consumed by all known consumers or timeout.
    ///
    /// The waiting behavior is controlled by the provided `AutoWaitStrategy`:
    /// - `BusySpin` / `BusySpinWithSpinLoopHint`: busy spin using `spin_loop`
    /// - Block: sleep using block strategy duration from wait config
    /// - Sleep(d): sleep for the specified duration
    pub fn wait_until_consumed_with_strategy(
        &mut self,
        seq: Sequence,
        timeout: Duration,
        strategy: super::builder::AutoWaitStrategy,
    ) -> bool {
        use std::time::Instant;

        assert!(timeout >= Duration::ZERO, "timeout must be non-negative");

        let deadline = Instant::now()
            .checked_add(timeout)
            .expect("timeout duration does not fit in Instant");

        while Instant::now() < deadline {
            if self.is_consumed(seq) {
                return true;
            }
            Self::apply_wait_strategy(&strategy);
        }

        false
    }

    fn apply_wait_strategy(strategy: &super::builder::AutoWaitStrategy) {
        use super::builder::AutoWaitStrategy as WS;

        match strategy {
            WS::BusySpin | WS::BusySpinWithSpinLoopHint => {
                // Hot-path spin keeps this method low-latency for small bounded waits.
                std::hint::spin_loop();
            }
            WS::SpinThenYield { spins } => {
                for _ in 0..*spins {
                    std::hint::spin_loop();
                }
                // Yield after bounded spin to allow other runnable threads.
                std::thread::yield_now();
            }
            WS::Block => {
                super::wait::perform_default_block_wait();
            }
            WS::Sleep(d) => {
                // Explicit sleep strategy lets the platform scheduler absorb queueing jitter.
                super::wait::sleep_or_yield(*d);
            }
        }
    }
}
