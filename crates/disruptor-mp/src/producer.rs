//! Multi-process producer implementation.
//!
//! This module provides the [`SharedProducer`] type for publishing events to a shared memory
//! ring buffer that can be consumed by multiple processes. It supports various coordination
//! and discovery modes for different deployment scenarios.

use super::consumer_barrier::{DiscoveryMode, SharedConsumerBarrier};
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
        CoordinationMode::WaitForConsumers {
            min_consumers,
            timeout,
        }
    }

    /// Create a coordination mode for single consumer scenarios
    pub fn wait_for_single_consumer(timeout: Duration) -> Self {
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
    /// Consumer barrier for tracking all consumers (replaces min_consumer_sequence)
    pub(crate) consumer_barrier: SharedConsumerBarrier,
    /// Next sequence to be published
    sequence: Sequence,
    /// Highest sequence available for publication because consumers are behind
    sequence_clear_of_consumers: Sequence,
    /// Whether we've completed initial coordination
    pub(crate) coordination_completed: bool,
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
    ) -> Self {
        // Initialize producer sequence to -1 (no events published yet)
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
        }
    }

    /// Check if we have enough free slots for publishing n events
    #[inline]
    fn next_sequences(&mut self, n: usize) -> Result<Sequence, MissingFreeSlots> {
        // Skip coordination check - should be completed during creation
        // self.ensure_coordination_completed(); // Removed per-operation overhead

        let n = n as i64;
        let n_next = self.sequence - 1 + n;

        if self.sequence_clear_of_consumers < n_next {
            // PERFORMANCE OPTIMIZATION: Reduce expensive barrier checks by batching them
            // Check where consumers are to avoid overwriting unread slots
            let last_published = self.sequence - 1;
            let rear_sequence_read = self.consumer_barrier.get_min_consumer_sequence();
            let free_slots = self
                .ring_buffer
                .free_slots(last_published, rear_sequence_read);

            if free_slots < n {
                return Err(MissingFreeSlots((n - free_slots) as u64));
            }

            // PERFORMANCE OPTIMIZATION: Cache more aggressively to reduce barrier calls
            // Use all available free slots for better batching (safe because we check rear_sequence_read)
            self.sequence_clear_of_consumers = last_published + free_slots;
        }

        Ok(n_next)
    }

    /// Apply update to a single event and publish it
    #[inline]
    fn apply_update<F>(&mut self, update: F) -> Sequence
    where
        F: FnOnce(&mut E),
    {
        let sequence = self.sequence;

        // Get mutable access to the event at this sequence
        let event_ptr = self.ring_buffer.get(sequence);
        let event = unsafe { &mut *event_ptr };

        // Apply the update
        update(event);

        // Publish the sequence (make it available to consumers)
        self.producer_sequence.store(sequence, Ordering::Release);

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
        let n = n as i64;
        let lower = self.sequence;
        let upper = lower + n - 1;

        // Apply updates to each event in the batch
        for (i, seq) in (lower..=upper).enumerate() {
            let event_ptr = self.ring_buffer.get(seq);
            let event = unsafe { &mut *event_ptr };
            update_fn(event, i);
        }

        // Publish the entire batch by publishing the upper sequence
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
        self.next_sequences(1).map_err(|_| RingBufferFull)?;
        let sequence = self.apply_update(update);
        Ok(sequence)
    }

    /// Publish a single event, spinning until a slot is available.
    pub fn publish<F>(&mut self, update: F)
    where
        F: FnOnce(&mut E),
    {
        while self.next_sequences(1).is_err() {
            std::hint::spin_loop();
        }
        self.apply_update(update);
    }

    /// Attempt to publish a batch of events using an indexed closure.
    ///
    /// The closure receives `(&mut event, batch_index)`.
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
    pub fn batch_publish<F>(&mut self, n: usize, update_fn: F)
    where
        F: Fn(&mut E, usize),
    {
        if n == 0 {
            return;
        }
        while self.next_sequences(n).is_err() {
            std::hint::spin_loop();
        }
        self.apply_batch_updates(n, update_fn);
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
    /// Attempt to publish an event but give up after `timeout`.
    /// Returns Ok(sequence) on success or Err(PublishTimeoutError::Timeout) if the timeout expired.
    pub fn publish_with_timeout<F>(
        &mut self,
        timeout: Duration,
        update: F,
    ) -> Result<Sequence, PublishTimeoutError>
    where
        F: FnOnce(&mut E),
    {
        let deadline = Instant::now() + timeout;

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
        self.consumer_barrier.get_min_consumer_sequence()
    }

    /// Check if the given sequence has been consumed by all known consumers.
    pub fn is_consumed(&mut self, seq: Sequence) -> bool {
        self.consumer_barrier.get_min_consumer_sequence() >= seq
    }

    /// Wait until the provided sequence is consumed by all known consumers or timeout.
    ///
    /// The waiting behavior is controlled by the provided `AutoWaitStrategy`:
    /// - BusySpin / BusySpinWithSpinLoopHint: busy spin using `spin_loop`
    /// - Block: sleep using block strategy duration from wait config
    /// - Sleep(d): sleep for the specified duration
    pub fn wait_until_consumed_with_strategy(
        &mut self,
        seq: Sequence,
        timeout: Duration,
        strategy: crate::builder::AutoWaitStrategy,
    ) -> bool {
        use crate::builder::AutoWaitStrategy as WS;
        use std::time::Instant;

        let deadline = Instant::now() + timeout;

        loop {
            if self.is_consumed(seq) {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }

            match strategy {
                WS::BusySpin | WS::BusySpinWithSpinLoopHint => {
                    std::hint::spin_loop();
                }
                WS::SpinThenYield { spins } => {
                    for _ in 0..spins {
                        std::hint::spin_loop();
                    }
                    std::thread::yield_now();
                }
                WS::Block => {
                    std::thread::sleep(super::wait::SLEEP_CONFIG.block_strategy_duration());
                }
                WS::Sleep(d) => {
                    std::thread::sleep(d);
                }
            }
        }
    }
}
