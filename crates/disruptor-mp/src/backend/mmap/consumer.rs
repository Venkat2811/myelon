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
    #[inline]
    fn available_batch_bounds(&self) -> Option<(Sequence, Sequence)> {
        let producer_sequence = self.producer_sequence.load(Ordering::Acquire);
        let next_sequence = self.last_processed_sequence + 1;
        if next_sequence > producer_sequence {
            return None;
        }
        Some((next_sequence, producer_sequence))
    }

    #[inline]
    fn publish_consumed_sequence(&mut self, sequence: Sequence) {
        self.consumer_sequence.store(sequence, Ordering::Release);
        self.last_processed_sequence = sequence;
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
        let (next_sequence, _upper) = self.available_batch_bounds()?;

        let event_ptr = self.ring_buffer.get(next_sequence);
        let event = unsafe { *event_ptr };

        self.publish_consumed_sequence(next_sequence);
        Some((next_sequence, event))
    }

    /// Try to lease the next available event without copying it out of the ring slot.
    pub fn try_consume_next_leased(&mut self) -> Option<MmapConsumerLease<'_, E>> {
        let (next_sequence, _upper) = self.available_batch_bounds()?;

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
        let Some((lower, upper)) = self.available_batch_bounds() else {
            return 0;
        };
        self.process_snapshot_batch(lower, upper, &mut processor)
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

    /// Block until one event is available, then consume it with the configured sleep policy.
    pub fn consume_next_with_sleep(&mut self) -> (Sequence, E) {
        loop {
            if let Some(result) = self.try_consume_next() {
                return result;
            }
            crate::perform_default_consume_sleep_wait();
        }
    }

    /// Block until one event is available, then lease it.
    pub fn consume_next_leased(&mut self) -> MmapConsumerLease<'_, E> {
        loop {
            if let Some((next_sequence, _upper)) = self.available_batch_bounds() {
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

    /// Block until one event is available, then lease it with the configured sleep policy.
    pub fn consume_next_leased_with_sleep(&mut self) -> MmapConsumerLease<'_, E> {
        loop {
            if let Some((next_sequence, _upper)) = self.available_batch_bounds() {
                let event_ptr = self.ring_buffer.get(next_sequence) as *const E;
                return MmapConsumerLease {
                    consumer: self,
                    sequence: next_sequence,
                    event_ptr,
                };
            }
            crate::perform_default_consume_sleep_wait();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MmapProducer;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Debug, Copy, Clone, Default, PartialEq)]
    struct TestEvent {
        sequence: i64,
        data: i64,
    }

    fn unique_layout(prefix: &str) -> MmapTransportLayout {
        let pid = std::process::id();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be valid")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("{prefix}_{pid}_{nanos}"));
        MmapTransportLayout::new(root, "queue01").unwrap()
    }

    #[test]
    fn process_available_borrows_ring_slots_instead_of_stack_copies() {
        let layout = unique_layout("mmap_process_available_ref");
        let mut producer =
            MmapProducer::<TestEvent>::create(layout.clone(), 8, TestEvent::default).unwrap();
        let mut consumer = MmapConsumer::<TestEvent>::attach(layout.clone(), 8, "c0001").unwrap();

        for i in 0..4 {
            producer.publish(|event| {
                event.sequence = i;
                event.data = i * 10;
            });
        }

        let expected_ptrs: Vec<usize> = (0..4)
            .map(|seq| consumer.ring_buffer.get(seq) as *const TestEvent as usize)
            .collect();
        let mut seen_ptrs = Vec::new();
        let processed = consumer.process_available(|event, _seq| {
            seen_ptrs.push(event as *const TestEvent as usize);
        });

        assert_eq!(processed, 4);
        assert_eq!(seen_ptrs, expected_ptrs);

        let _ = std::fs::remove_dir_all(layout.root_dir());
    }

    #[test]
    fn process_available_publishes_consumer_sequence_after_batch_completion() {
        let layout = unique_layout("mmap_process_available_cursor");
        let mut producer =
            MmapProducer::<TestEvent>::create(layout.clone(), 8, TestEvent::default).unwrap();
        let mut consumer = MmapConsumer::<TestEvent>::attach(layout.clone(), 8, "c0001").unwrap();

        for i in 0..6 {
            producer.publish(|event| {
                event.sequence = i;
                event.data = i * 100;
            });
        }

        let initial_sequence = producer
            .consumer_sequence("c0001")
            .expect("consumer cursor should be discoverable");
        let mut seen = Vec::new();
        let processed = consumer.process_available(|event, seq| {
            let observed = producer
                .consumer_sequence("c0001")
                .expect("consumer cursor should stay readable during callback");
            assert_eq!(observed, initial_sequence);
            seen.push((seq, event.sequence, event.data));
        });

        assert_eq!(processed, 6);
        assert_eq!(seen.len(), 6);
        assert_eq!(
            producer.consumer_sequence("c0001"),
            Some((processed - 1) as i64)
        );
        assert_eq!(consumer.current_sequence(), (processed - 1) as i64);

        let _ = std::fs::remove_dir_all(layout.root_dir());
    }
}
