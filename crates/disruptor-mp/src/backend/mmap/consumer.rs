//! Mmap-backed consumer implementation.

use crate::{MmapCursor, MmapRingBuffer, MmapTransportLayout, MultiProcessResult};
use disruptor_core::Sequence;
use std::ops::Deref;
use std::sync::atomic::Ordering;

/// Consumer for the mmap transport.
pub struct MmapConsumer<E> {
    ring_buffer: MmapRingBuffer<E>,
    producer_sequence: MmapCursor,
    consumer_sequence: MmapCursor,
    consumer_id: String,
    last_processed_sequence: Sequence,
    readiness_cursor: Option<MmapCursor>,
}

pub struct MmapConsumerLease<'a, E>
where
    E: Copy + Default,
{
    consumer: &'a mut MmapConsumer<E>,
    sequence: Sequence,
    event_ptr: *const E,
}

impl<E> MmapConsumerLease<'_, E>
where
    E: Copy + Default,
{
    pub fn sequence(&self) -> Sequence {
        self.sequence
    }
}

impl<E> Deref for MmapConsumerLease<'_, E>
where
    E: Copy + Default,
{
    type Target = E;

    fn deref(&self) -> &Self::Target {
        // Safety: the lease keeps the consumer sequence unpublished until drop,
        // so the producer cannot reuse the backing ring slot.
        unsafe { &*self.event_ptr }
    }
}

impl<E> Drop for MmapConsumerLease<'_, E>
where
    E: Copy + Default,
{
    fn drop(&mut self) {
        self.consumer
            .consumer_sequence
            .store(self.sequence, Ordering::Release);
        self.consumer.last_processed_sequence = self.sequence;
    }
}

impl<E> MmapConsumer<E>
where
    E: Copy + Default,
{
    /// Attach a consumer to an existing mmap transport.
    pub fn attach(
        layout: MmapTransportLayout,
        buffer_size: usize,
        consumer_id: &str,
    ) -> MultiProcessResult<Self> {
        let ring_buffer = MmapRingBuffer::attach(layout.ring_config(
            buffer_size,
            std::mem::size_of::<E>(),
            false,
        ))?;
        let producer_sequence = MmapCursor::attach(layout.producer_cursor_config(false))?;
        let consumer_sequence =
            MmapCursor::new_or_attach(layout.consumer_cursor_config(consumer_id, true)?, -1)?;
        let is_new_consumer = consumer_sequence.is_owner();
        let readiness_cursor = MmapCursor::attach(layout.readiness_cursor_config(false)).ok();

        let mut consumer = Self {
            ring_buffer,
            producer_sequence,
            consumer_sequence,
            consumer_id: consumer_id.to_string(),
            last_processed_sequence: -1,
            readiness_cursor,
        };

        consumer.last_processed_sequence = consumer.consumer_sequence.load(Ordering::Acquire);
        if is_new_consumer {
            consumer.signal_readiness();
        }
        Ok(consumer)
    }

    /// Signal that this consumer is ready.
    pub fn signal_readiness(&self) {
        if let Some(readiness_cursor) = &self.readiness_cursor {
            readiness_cursor.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Return whether this consumer has a readiness cursor attached.
    pub fn has_coordination_support(&self) -> bool {
        self.readiness_cursor.is_some()
    }

    /// Try to consume the next available event.
    pub fn try_consume_next(&mut self) -> Option<(Sequence, E)> {
        let producer_sequence = self.producer_sequence.load(Ordering::Acquire);
        let next_sequence = self.last_processed_sequence + 1;
        if next_sequence > producer_sequence {
            return None;
        }

        let event_ptr = self.ring_buffer.get(next_sequence);
        let event = unsafe { *event_ptr };

        self.consumer_sequence
            .store(next_sequence, Ordering::Release);
        self.last_processed_sequence = next_sequence;
        Some((next_sequence, event))
    }

    /// Try to lease the next available event without copying it out of the ring slot.
    pub fn try_consume_next_leased(&mut self) -> Option<MmapConsumerLease<'_, E>> {
        let producer_sequence = self.producer_sequence.load(Ordering::Acquire);
        let next_sequence = self.last_processed_sequence + 1;
        if next_sequence > producer_sequence {
            return None;
        }

        let event_ptr = self.ring_buffer.get(next_sequence) as *const E;
        Some(MmapConsumerLease {
            consumer: self,
            sequence: next_sequence,
            event_ptr,
        })
    }

    /// Process all currently available events.
    pub fn process_available<F>(&mut self, mut processor: F) -> usize
    where
        F: FnMut(&E, Sequence),
    {
        let mut processed = 0usize;
        while let Some((sequence, event)) = self.try_consume_next() {
            processor(&event, sequence);
            processed += 1;
        }
        processed
    }

    /// Block until one event is available, then consume it.
    pub fn consume_next(&mut self) -> (Sequence, E) {
        loop {
            if let Some(result) = self.try_consume_next() {
                return result;
            }
            std::hint::spin_loop();
        }
    }

    /// Block until one event is available, then lease it.
    pub fn consume_next_leased(&mut self) -> MmapConsumerLease<'_, E> {
        loop {
            let producer_sequence = self.producer_sequence.load(Ordering::Acquire);
            let next_sequence = self.last_processed_sequence + 1;
            if next_sequence <= producer_sequence {
                let event_ptr = self.ring_buffer.get(next_sequence) as *const E;
                return MmapConsumerLease {
                    consumer: self,
                    sequence: next_sequence,
                    event_ptr,
                };
            }
            std::hint::spin_loop();
        }
    }

    /// Return the last processed sequence.
    pub fn current_sequence(&self) -> Sequence {
        self.last_processed_sequence
    }

    /// Return the logical consumer id.
    pub fn consumer_id(&self) -> &str {
        &self.consumer_id
    }
}
