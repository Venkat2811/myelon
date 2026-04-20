//! Multi-process consumer implementation.
//!
//! This module provides the [`SharedConsumer`] type for consuming events from a shared memory
//! ring buffer created by a producer in another process. Each consumer maintains its own
//! sequence tracking and supports both manual and automatic event processing modes.

use crate::{SharedCursor, SharedRingBuffer};
use disruptor_core::Sequence;
use std::ops::Deref;
use std::sync::atomic::Ordering;

/// Consumer for multi-process disruptor with broadcast semantics
/// Each consumer maintains its own sequence and sees all events
pub struct SharedConsumer<E> {
    ring_buffer: SharedRingBuffer<E>,
    producer_sequence: SharedCursor,
    /// This consumer's own sequence (last event it processed)
    consumer_sequence: SharedCursor,
    /// Consumer ID for this instance
    consumer_id: String,
    /// Last sequence processed by THIS consumer
    last_processed_sequence: Sequence,
    /// Consumer readiness counter for internal coordination (optional)
    consumers_ready: Option<SharedCursor>,
}

pub struct SharedConsumerLease<'a, E>
where
    E: Copy + Default,
{
    consumer: &'a mut SharedConsumer<E>,
    sequence: Sequence,
    event_ptr: *const E,
}

impl<E> SharedConsumerLease<'_, E>
where
    E: Copy + Default,
{
    pub fn sequence(&self) -> Sequence {
        self.sequence
    }
}

impl<E> Deref for SharedConsumerLease<'_, E>
where
    E: Copy + Default,
{
    type Target = E;

    fn deref(&self) -> &Self::Target {
        // Safety: the lease keeps the consumer sequence unpublished until drop,
        // so the producer cannot reuse the slot backing `event_ptr`.
        unsafe { &*self.event_ptr }
    }
}

impl<E> Drop for SharedConsumerLease<'_, E>
where
    E: Copy + Default,
{
    fn drop(&mut self) {
        self.consumer.publish_consumed_sequence(self.sequence);
    }
}

impl<E> SharedConsumer<E>
where
    E: Copy + Default,
{
    pub(crate) fn new_with_coordination(
        ring_buffer: SharedRingBuffer<E>,
        producer_sequence: SharedCursor,
        consumer_sequence: SharedCursor,
        consumer_id: String,
        base_name: Option<String>,
    ) -> Self {
        assert!(!consumer_id.is_empty(), "consumer_id must not be empty");

        // Try to attach to coordination structure if base_name is provided
        let consumers_ready = base_name.as_ref().and_then(|name| {
            // Use shorter name for macOS compatibility (error 63 = name too long)
            let coordination_name = format!("{}_cr", name);
            SharedCursor::attach(&coordination_name).ok()
        });

        let mut consumer = Self {
            ring_buffer,
            producer_sequence,
            consumer_sequence,
            consumer_id,
            last_processed_sequence: -1,
            consumers_ready,
        };

        consumer.last_processed_sequence = consumer.consumer_sequence.load(Ordering::Acquire);

        // Automatically signal readiness if coordination is available
        consumer.signal_readiness();

        consumer
    }

    /// Signal consumer readiness (internal coordination)
    /// This is called automatically when the consumer is created
    pub fn signal_readiness(&self) {
        if let Some(consumers_ready) = &self.consumers_ready {
            // AcqRel preserves readiness-count monotonicity across processes.
            consumers_ready.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Try to attach to coordination structure (retry mechanism for timing issues)
    pub fn try_attach_coordination(&mut self, base_name: &str) -> bool {
        assert!(!base_name.is_empty(), "base_name must not be empty");

        if self.consumers_ready.is_some() {
            return true; // Already attached
        }

        // Use shorter name for macOS compatibility (error 63 = name too long)
        let coordination_name = format!("{}_cr", base_name);
        if let Ok(cursor) = SharedCursor::attach(&coordination_name) {
            self.consumers_ready = Some(cursor);
            self.signal_readiness(); // Signal readiness now that we're attached
            return true;
        }
        false
    }

    /// Check if this consumer has coordination support
    pub fn has_coordination_support(&self) -> bool {
        self.consumers_ready.is_some()
    }

    /// Try to consume the next available event for this consumer
    /// Returns None if no new events are available
    pub fn try_consume_next(&mut self) -> Option<(Sequence, E)> {
        // Acquire load enforces visibility for producer progress before deciding
        // whether the next slot is safe to consume.
        let (next_sequence, _) = self.available_batch_bounds()?;

        let event_ptr = self.ring_buffer.get(next_sequence);
        let event = unsafe { *event_ptr }; // Copy the event

        self.publish_consumed_sequence(next_sequence);
        Some((next_sequence, event))
    }

    /// Try to lease the next available event without copying it out of the ring slot.
    ///
    /// The returned lease publishes consumer progress only when dropped, which keeps
    /// the backing slot valid for the lease lifetime.
    pub fn try_consume_next_leased(&mut self) -> Option<SharedConsumerLease<'_, E>> {
        let (next_sequence, _) = self.available_batch_bounds()?;
        let event_ptr = self.ring_buffer.get(next_sequence) as *const E;
        Some(SharedConsumerLease {
            consumer: self,
            sequence: next_sequence,
            event_ptr,
        })
    }

    #[inline]
    fn available_batch_bounds(&self) -> Option<(Sequence, Sequence)> {
        assert!(
            self.last_processed_sequence >= -1,
            "consumer sequence must not be lower than -1"
        );

        let producer_seq = self.producer_sequence.load(Ordering::Acquire);
        let next_sequence = self.last_processed_sequence + 1;

        if next_sequence > producer_seq {
            return None;
        }

        Some((next_sequence, producer_seq))
    }

    #[inline]
    fn publish_consumed_sequence(&mut self, sequence: Sequence) {
        // Publish consumer progress once per consumed snapshot batch. Keeping the
        // shared cursor slightly behind while callbacks run is safe because it can
        // only make the producer more conservative; it cannot permit overwrite of
        // unread slots.
        self.consumer_sequence.store(sequence, Ordering::Release);
        self.last_processed_sequence = sequence;
    }

    #[inline]
    fn is_end_of_batch(&self) -> bool {
        self.last_processed_sequence >= self.producer_sequence.load(Ordering::Acquire)
    }

    #[inline]
    fn process_snapshot_batch<F>(
        &mut self,
        lower: Sequence,
        upper: Sequence,
        processor: &mut F,
    ) -> usize
    where
        F: FnMut(&E, Sequence),
    {
        let mut processed = 0usize;

        for sequence in lower..=upper {
            let event_ptr = self.ring_buffer.get(sequence);
            let event = unsafe { &*event_ptr };
            processor(event, sequence);
            processed += 1;
        }

        self.publish_consumed_sequence(upper);
        processed
    }

    #[inline]
    fn process_snapshot_batch_with_eob<F>(
        &mut self,
        lower: Sequence,
        upper: Sequence,
        processor: &mut F,
    ) -> usize
    where
        F: FnMut(&E, Sequence, bool),
    {
        let mut processed = 0usize;

        for sequence in lower..=upper {
            let event_ptr = self.ring_buffer.get(sequence);
            let event = unsafe { &*event_ptr };
            let end_of_batch = sequence == upper;
            processor(event, sequence, end_of_batch);
            processed += 1;
        }

        self.publish_consumed_sequence(upper);
        processed
    }

    /// Wait for and consume the next event (blocking)
    /// Returns the sequence and event data
    pub fn consume_next(&mut self) -> (Sequence, E) {
        loop {
            if let Some((seq, event)) = self.try_consume_next() {
                return (seq, event);
            }
            #[cfg(feature = "dst")]
            if dst_fixtures::dst_buggify::buggify(file!(), line!()) {
                std::thread::yield_now();
            }
            // High performance: Use spin_loop for maximum throughput
            // This matches the performance of manual polling approach
            std::hint::spin_loop();
        }
    }

    /// Wait for and lease the next event (blocking).
    pub fn consume_next_leased(&mut self) -> SharedConsumerLease<'_, E> {
        loop {
            if let Some((next_sequence, _)) = self.available_batch_bounds() {
                let event_ptr = self.ring_buffer.get(next_sequence) as *const E;
                return SharedConsumerLease {
                    consumer: self,
                    sequence: next_sequence,
                    event_ptr,
                };
            }
            std::hint::spin_loop();
        }
    }

    /// Wait for and consume the next event (blocking with sleep for CPU efficiency)
    /// Returns the sequence and event data
    /// Use this when you want better CPU efficiency at the cost of throughput
    pub fn consume_next_with_sleep(&mut self) -> (Sequence, E) {
        loop {
            if let Some((seq, event)) = self.try_consume_next() {
                return (seq, event);
            }
            // CPU efficient: Use sleep to reduce CPU usage (lower throughput)
            // TODO: Implement proper blocking with futex/condition variables
            std::thread::sleep(super::wait::SLEEP_CONFIG.consume_sleep_duration());
        }
    }

    /// Wait for and lease the next event (blocking with sleep for CPU efficiency).
    pub fn consume_next_leased_with_sleep(&mut self) -> SharedConsumerLease<'_, E> {
        loop {
            if let Some((next_sequence, _)) = self.available_batch_bounds() {
                let event_ptr = self.ring_buffer.get(next_sequence) as *const E;
                return SharedConsumerLease {
                    consumer: self,
                    sequence: next_sequence,
                    event_ptr,
                };
            }
            std::thread::sleep(super::wait::SLEEP_CONFIG.consume_sleep_duration());
        }
    }

    /// Process next available event with blocking semantics (HIGH PERFORMANCE)
    /// This method blocks until an event is available, then processes it
    /// Uses spin_loop() for maximum throughput (CPU intensive)
    /// Returns the sequence and whether this was the end of a batch
    pub fn process_next_blocking<F>(&mut self, mut processor: F) -> (Sequence, bool)
    where
        F: FnMut(&E, Sequence, bool),
    {
        // Block until an event is available (high performance - uses spin_loop)
        let (sequence, event) = self.consume_next();

        // Check if more events are immediately available (end_of_batch detection)
        let end_of_batch = self.is_end_of_batch();

        // Process the event with end_of_batch information
        processor(&event, sequence, end_of_batch);

        (sequence, end_of_batch)
    }

    /// Process next available event with blocking semantics (CPU EFFICIENT)
    /// This method blocks until an event is available, then processes it
    /// Uses sleep() for better CPU efficiency (lower throughput)
    /// Returns the sequence and whether this was the end of a batch
    pub fn process_next_blocking_with_sleep<F>(&mut self, mut processor: F) -> (Sequence, bool)
    where
        F: FnMut(&E, Sequence, bool),
    {
        // Block until an event is available (CPU efficient - uses sleep)
        let (sequence, event) = self.consume_next_with_sleep();

        // Check if more events are immediately available (end_of_batch detection)
        let end_of_batch = self.is_end_of_batch();

        // Process the event with end_of_batch information
        processor(&event, sequence, end_of_batch);

        (sequence, end_of_batch)
    }

    /// Process available events with blocking semantics + batch processing (HIGH PERFORMANCE)
    /// This method blocks until at least one event is available, then processes ALL available events
    /// This matches the performance characteristics of manual polling by processing in batches
    /// Returns the number of events processed
    pub fn process_available_blocking<F>(&mut self, mut processor: F) -> usize
    where
        F: FnMut(&E, Sequence, bool),
    {
        let mut processed = 0usize;

        loop {
            if let Some((lower, upper)) = self.available_batch_bounds() {
                processed += self.process_snapshot_batch_with_eob(lower, upper, &mut processor);
                break;
            }

            // High performance blocking wait for the first batch.
            std::hint::spin_loop();
        }

        while let Some((lower, upper)) = self.available_batch_bounds() {
            processed += self.process_snapshot_batch_with_eob(lower, upper, &mut processor);
        }

        processed
    }

    /// Process available events with a callback function
    /// Returns the number of events processed by this consumer
    pub fn process_available<F>(&mut self, mut processor: F) -> usize
    where
        F: FnMut(&E, Sequence),
    {
        #[cfg(feature = "dst")]
        if dst_fixtures::dst_buggify::buggify(file!(), line!()) {
            return 0;
        }

        let mut processed = 0usize;

        while let Some((lower, upper)) = self.available_batch_bounds() {
            processed += self.process_snapshot_batch(lower, upper, &mut processor);
        }

        processed
    }

    /// Get the last sequence processed by this consumer
    pub fn current_sequence(&self) -> Sequence {
        self.last_processed_sequence
    }

    /// Get the current producer sequence (for debugging)
    pub fn producer_sequence(&self) -> Sequence {
        // Acquire load gives a coherent producer cursor for diagnostics.
        self.producer_sequence.load(Ordering::Acquire)
    }

    /// Get this consumer's sequence (for debugging)
    pub fn consumer_sequence(&self) -> Sequence {
        // Acquire load keeps debug output in the same ordering domain as runtime reads.
        self.consumer_sequence.load(Ordering::Acquire)
    }

    /// Get debug information about sequences
    pub fn debug_sequences(&self) -> (Sequence, Sequence, Sequence) {
        let producer_seq = self.producer_sequence.load(Ordering::Acquire);
        let consumer_seq = self.consumer_sequence.load(Ordering::Acquire);
        (self.last_processed_sequence, producer_seq, consumer_seq)
    }

    /// Get consumer ID
    pub fn consumer_id(&self) -> &str {
        &self.consumer_id
    }
}

// Note: SharedConsumer doesn't need a Drop implementation because:
// 1. The SharedRingBuffer it holds is not owned (attached, not created)
// 2. The SharedCursors (producer_sequence, consumer_sequence, consumers_ready) have their own Drop impl
// 3. Consumers don't create shared memory segments, they only attach to existing ones
