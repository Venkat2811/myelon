//! Mmap-backed producer implementation.

use crate::{
    AutoWaitStrategy, MmapConsumerBarrier, MmapCursor, MmapRingBuffer, MmapTransportLayout,
    MultiProcessResult,
};
use disruptor_core::{MissingFreeSlots, RingBufferFull, Sequence};
use std::sync::atomic::Ordering;
use std::time::Duration;

/// Producer for the mmap transport.
pub struct MmapProducer<E> {
    ring_buffer: MmapRingBuffer<E>,
    producer_sequence: MmapCursor,
    consumer_barrier: MmapConsumerBarrier,
    sequence: Sequence,
    sequence_clear_of_consumers: Sequence,
}

impl<E> MmapProducer<E>
where
    E: Copy + Default,
{
    /// Create a new mmap-backed producer.
    pub fn create<F>(
        layout: MmapTransportLayout,
        buffer_size: usize,
        event_factory: F,
    ) -> MultiProcessResult<Self>
    where
        F: FnMut() -> E,
    {
        let ring_buffer = MmapRingBuffer::new(
            layout.ring_config(buffer_size, std::mem::size_of::<E>(), true),
            event_factory,
        )?;
        let producer_sequence = MmapCursor::new(layout.producer_cursor_config(true), -1)?;
        let mut consumer_barrier = MmapConsumerBarrier::new_with_coordination(layout)?;
        consumer_barrier.set_producer_cursor(producer_sequence.clone());

        Ok(Self {
            ring_buffer,
            producer_sequence,
            consumer_barrier,
            sequence: 0,
            sequence_clear_of_consumers: buffer_size as i64 - 1,
        })
    }

    #[inline]
    fn next_sequences(&mut self, n: usize) -> Result<Sequence, MissingFreeSlots> {
        let n = i64::try_from(n).map_err(|_| MissingFreeSlots(u64::MAX))?;
        assert!(n > 0, "batch size must be greater than zero");

        let n_next = self
            .sequence
            .checked_sub(1)
            .and_then(|current| current.checked_add(n))
            .ok_or(MissingFreeSlots(u64::MAX))?;

        if self.sequence_clear_of_consumers < n_next {
            let last_published = self.sequence - 1;
            let rear_sequence_read = self.consumer_barrier.best_effort_min_consumer_sequence();
            let free_slots = self
                .ring_buffer
                .free_slots(last_published, rear_sequence_read);

            if free_slots < n {
                return Err(MissingFreeSlots((n - free_slots) as u64));
            }

            self.sequence_clear_of_consumers = last_published + free_slots;
        }

        Ok(n_next)
    }

    #[inline]
    fn apply_update<F>(&mut self, update: F) -> Sequence
    where
        F: FnOnce(&mut E),
    {
        let sequence = self.sequence;
        let event_ptr = self.ring_buffer.get(sequence);
        let event = unsafe { &mut *event_ptr };
        update(event);
        self.producer_sequence.store(sequence, Ordering::Release);
        self.sequence += 1;
        sequence
    }

    #[inline]
    fn apply_batch_updates<F>(&mut self, n: usize, update_fn: F) -> Sequence
    where
        F: Fn(&mut E, usize),
    {
        let n = i64::try_from(n).expect("batch size must fit in Sequence");
        let lower = self.sequence;
        let upper = lower
            .checked_add(n - 1)
            .expect("sequence arithmetic must not overflow");

        for (index, sequence) in (lower..=upper).enumerate() {
            let event_ptr = self.ring_buffer.get(sequence);
            let event = unsafe { &mut *event_ptr };
            update_fn(event, index);
        }

        self.producer_sequence.store(upper, Ordering::Release);
        self.sequence += n;
        upper
    }

    /// Attempt to publish one event without blocking.
    pub fn try_publish<F>(&mut self, update: F) -> Result<Sequence, RingBufferFull>
    where
        F: FnOnce(&mut E),
    {
        self.next_sequences(1).map_err(|_| RingBufferFull)?;
        Ok(self.apply_update(update))
    }

    /// Publish one event, spinning until capacity is available.
    pub fn publish<F>(&mut self, update: F)
    where
        F: FnOnce(&mut E),
    {
        while self.next_sequences(1).is_err() {
            std::hint::spin_loop();
        }
        let _ = self.apply_update(update);
    }

    /// Attempt to publish a batch without blocking.
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
        Ok(self.apply_batch_updates(n, update_fn))
    }

    /// Return the last published sequence.
    pub fn last_published_sequence(&self) -> Sequence {
        self.producer_sequence.load(Ordering::Acquire)
    }

    /// Return the minimum visible consumer sequence.
    pub fn min_gating_sequence(&mut self) -> Sequence {
        self.consumer_barrier.best_effort_min_consumer_sequence()
    }

    /// Return true when all consumers have advanced past `sequence`.
    pub fn is_consumed(&mut self, sequence: Sequence) -> bool {
        self.min_gating_sequence() >= sequence
    }

    /// Wait until the provided sequence is consumed by all known consumers or timeout.
    pub fn wait_until_consumed_with_strategy(
        &mut self,
        sequence: Sequence,
        timeout: Duration,
        strategy: AutoWaitStrategy,
    ) -> bool {
        let deadline = std::time::Instant::now()
            .checked_add(timeout)
            .expect("timeout duration does not fit in Instant");

        while std::time::Instant::now() < deadline {
            if self.is_consumed(sequence) {
                return true;
            }
            Self::apply_wait_strategy(&strategy);
        }

        false
    }

    /// Wait until at least `min_consumers` have signaled readiness.
    pub fn wait_for_consumers_ready(&self, min_consumers: i64, timeout: Duration) -> bool {
        self.consumer_barrier
            .wait_for_consumers_ready(min_consumers, timeout)
    }

    /// Return the number of discovered consumers.
    pub fn get_consumer_count(&mut self) -> usize {
        self.consumer_barrier.best_effort_consumer_count()
    }

    fn apply_wait_strategy(strategy: &AutoWaitStrategy) {
        match strategy {
            AutoWaitStrategy::BusySpin | AutoWaitStrategy::BusySpinWithSpinLoopHint => {
                std::hint::spin_loop();
            }
            AutoWaitStrategy::SpinThenYield { spins } => {
                for _ in 0..*spins {
                    std::hint::spin_loop();
                }
                std::thread::yield_now();
            }
            AutoWaitStrategy::Block => {
                std::thread::sleep(Duration::from_millis(1));
            }
            AutoWaitStrategy::Sleep(duration) => {
                std::thread::sleep(*duration);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MmapConsumer;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[derive(Debug, Copy, Clone, Default, PartialEq)]
    struct TestEvent {
        sequence: i64,
        data: i64,
    }

    #[derive(Debug, Copy, Clone, PartialEq)]
    struct PayloadEvent {
        len: u32,
        bytes: [u8; 64],
    }

    impl Default for PayloadEvent {
        fn default() -> Self {
            Self {
                len: 0,
                bytes: [0; 64],
            }
        }
    }

    impl PayloadEvent {
        fn write_from(&mut self, data: &[u8]) {
            assert!(data.len() <= self.bytes.len(), "payload exceeds slot size");
            self.len = data.len() as u32;
            self.bytes[..data.len()].copy_from_slice(data);
        }

        fn as_slice(&self) -> &[u8] {
            &self.bytes[..self.len as usize]
        }
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
    fn spsc_publish_consume_round_trip() {
        let layout = unique_layout("mmap_spsc");
        let mut producer =
            MmapProducer::<TestEvent>::create(layout.clone(), 8, TestEvent::default).unwrap();
        let mut consumer = MmapConsumer::<TestEvent>::attach(layout.clone(), 8, "c0001").unwrap();

        producer.publish(|event| {
            event.sequence = 0;
            event.data = 42;
        });

        let (sequence, event) = consumer.try_consume_next().unwrap();
        assert_eq!(sequence, 0);
        assert_eq!(
            event,
            TestEvent {
                sequence: 0,
                data: 42,
            }
        );
        assert!(producer.wait_for_consumers_ready(1, Duration::from_millis(20)));

        let _ = std::fs::remove_dir_all(layout.root_dir());
    }

    #[test]
    fn discovered_consumer_count_tracks_attached_consumers() {
        let layout = unique_layout("mmap_count");
        let mut producer =
            MmapProducer::<TestEvent>::create(layout.clone(), 8, TestEvent::default).unwrap();
        let _consumer_a = MmapConsumer::<TestEvent>::attach(layout.clone(), 8, "c0001").unwrap();
        let _consumer_b = MmapConsumer::<TestEvent>::attach(layout.clone(), 8, "c0002").unwrap();

        assert_eq!(producer.get_consumer_count(), 2);

        let _ = std::fs::remove_dir_all(layout.root_dir());
    }

    #[test]
    fn consumer_attach_fails_cleanly_after_transport_directory_removal() {
        let layout = unique_layout("mmap_stale");
        let _producer =
            MmapProducer::<TestEvent>::create(layout.clone(), 8, TestEvent::default).unwrap();

        std::fs::remove_dir_all(layout.root_dir()).unwrap();

        let error = match MmapConsumer::<TestEvent>::attach(layout.clone(), 8, "c0001") {
            Ok(_) => panic!("expected stale transport attach to fail"),
            Err(error) => error,
        };
        let message = error.to_string().to_lowercase();
        assert!(message.contains("not found") || message.contains("no such file"));
    }

    #[test]
    fn transport_can_be_recreated_after_directory_removal() {
        let layout = unique_layout("mmap_recreate");

        let first_producer =
            MmapProducer::<TestEvent>::create(layout.clone(), 8, TestEvent::default).unwrap();
        drop(first_producer);

        std::fs::remove_dir_all(layout.root_dir()).unwrap();

        let mut producer =
            MmapProducer::<TestEvent>::create(layout.clone(), 8, TestEvent::default).unwrap();
        let mut consumer = MmapConsumer::<TestEvent>::attach(layout.clone(), 8, "c0001").unwrap();

        producer.publish(|event| {
            event.sequence = 0;
            event.data = 77;
        });

        let (sequence, event) = consumer.consume_next();
        assert_eq!(sequence, 0);
        assert_eq!(
            event,
            TestEvent {
                sequence: 0,
                data: 77,
            }
        );

        let _ = std::fs::remove_dir_all(layout.root_dir());
    }

    #[test]
    fn slot_reuse_preserves_short_payload_length_after_exact_fit_publish() {
        let layout = unique_layout("mmap_payload_reuse");
        let mut producer =
            MmapProducer::<PayloadEvent>::create(layout.clone(), 1, PayloadEvent::default).unwrap();
        let mut consumer =
            MmapConsumer::<PayloadEvent>::attach(layout.clone(), 1, "c0001").unwrap();

        assert!(producer.wait_for_consumers_ready(1, Duration::from_millis(20)));

        let exact_payload = [b'A'; 64];
        let short_payload = b"short";

        producer.publish(|event| event.write_from(&exact_payload));
        let (first_sequence, first_event) = consumer.consume_next();
        assert_eq!(first_sequence, 0);
        assert_eq!(first_event.as_slice(), exact_payload.as_slice());

        producer.publish(|event| event.write_from(short_payload));
        let (second_sequence, second_event) = consumer.consume_next();
        assert_eq!(second_sequence, 1);
        assert_eq!(second_event.as_slice(), short_payload);

        let _ = std::fs::remove_dir_all(layout.root_dir());
    }
}
