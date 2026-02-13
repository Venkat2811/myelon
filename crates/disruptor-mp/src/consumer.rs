//! Multi-process consumer implementation.
//!
//! This module provides the [`SharedConsumer`] type for consuming events from a shared memory
//! ring buffer created by a producer in another process. Each consumer maintains its own
//! sequence tracking and supports both manual and automatic event processing modes.

use crate::{SharedCursor, SharedRingBuffer};
use disruptor_core::Sequence;
use std::sync::atomic::Ordering;
use std::time::Duration;

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

        // Automatically signal readiness if coordination is available
        consumer.signal_readiness();

        // If we couldn't attach initially, try a few more times with delays
        // This handles the case where the producer creates the coordination structure
        // after the consumer starts
        if let (true, Some(name)) = (consumer.consumers_ready.is_none(), base_name.as_ref()) {
            for attempt in 1..=5 {
                std::thread::sleep(Duration::from_millis(attempt * 100));
                if consumer.try_attach_coordination(name) {
                    break;
                }
            }
        }

        consumer
    }

    /// Signal consumer readiness (internal coordination)
    /// This is called automatically when the consumer is created
    pub fn signal_readiness(&self) {
        if let Some(consumers_ready) = &self.consumers_ready {
            consumers_ready.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Try to attach to coordination structure (retry mechanism for timing issues)
    pub fn try_attach_coordination(&mut self, base_name: &str) -> bool {
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
        let producer_seq = self.producer_sequence.load(Ordering::Acquire);
        let current_consumer_seq = self.consumer_sequence.load(Ordering::Acquire);
        let next_sequence = current_consumer_seq + 1;

        // Check if there are new events available
        if next_sequence > producer_seq {
            return None; // No new events available
        }

        // Get the event (this consumer gets to see it)
        let event_ptr = self.ring_buffer.get(next_sequence);
        let event = unsafe { *event_ptr }; // Copy the event

        // Advance this consumer's sequence
        self.consumer_sequence
            .store(next_sequence, Ordering::Release);

        // P0 FIX: Each consumer only updates its own sequence
        // The producer will discover and track individual consumer sequences
        // This eliminates the race condition where fast consumers could advance
        // the global minimum past slow consumers

        self.last_processed_sequence = next_sequence;
        Some((next_sequence, event))
    }

    /// Wait for and consume the next event (blocking)
    /// Returns the sequence and event data
    pub fn consume_next(&mut self) -> (Sequence, E) {
        loop {
            if let Some((seq, event)) = self.try_consume_next() {
                return (seq, event);
            }
            // High performance: Use spin_loop for maximum throughput
            // This matches the performance of manual polling approach
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
        let producer_seq = self.producer_sequence.load(Ordering::Acquire);
        let consumer_seq = self.consumer_sequence.load(Ordering::Acquire);
        let end_of_batch = consumer_seq >= producer_seq;

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
        let producer_seq = self.producer_sequence.load(Ordering::Acquire);
        let consumer_seq = self.consumer_sequence.load(Ordering::Acquire);
        let end_of_batch = consumer_seq >= producer_seq;

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
        // Block until at least one event is available (high performance)
        let (first_sequence, first_event) = self.consume_next();

        // Process the first event
        let mut processed = 1;

        // Check if more events are immediately available for batch processing
        let producer_seq = self.producer_sequence.load(Ordering::Acquire);
        let consumer_seq = self.consumer_sequence.load(Ordering::Acquire);
        let first_end_of_batch = consumer_seq >= producer_seq;

        processor(&first_event, first_sequence, first_end_of_batch);

        // Now process any additional available events in batch (non-blocking)
        while let Some((seq, event)) = self.try_consume_next() {
            processed += 1;

            // Check if this is the end of the batch
            let producer_seq = self.producer_sequence.load(Ordering::Acquire);
            let consumer_seq = self.consumer_sequence.load(Ordering::Acquire);
            let end_of_batch = consumer_seq >= producer_seq;

            processor(&event, seq, end_of_batch);
        }

        processed
    }

    /// Process available events with a callback function
    /// Returns the number of events processed by this consumer
    pub fn process_available<F>(&mut self, mut processor: F) -> usize
    where
        F: FnMut(&E, Sequence),
    {
        let mut processed = 0;

        // Keep processing events until we're caught up
        while let Some((seq, event)) = self.try_consume_next() {
            processor(&event, seq);
            processed += 1;
        }

        processed
    }

    /// Get the last sequence processed by this consumer
    pub fn current_sequence(&self) -> Sequence {
        self.last_processed_sequence
    }

    /// Get the current producer sequence (for debugging)
    pub fn producer_sequence(&self) -> Sequence {
        self.producer_sequence.load(Ordering::Acquire)
    }

    /// Get this consumer's sequence (for debugging)
    pub fn consumer_sequence(&self) -> Sequence {
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
