//! Mmap-backed producer implementation.

use crate::{
    required_consumer::{
        RequiredConsumerError, RequiredConsumerLivenessConfig, RequiredConsumerLivenessState,
    },
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
    required_consumer_liveness: Option<RequiredConsumerLivenessState>,
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
        // Create coordination first so consumers never attach without a
        // readiness cursor and silently miss startup registration.
        let mut consumer_barrier = MmapConsumerBarrier::new_with_coordination(layout.clone())?;
        let ring_buffer = MmapRingBuffer::new(
            layout.ring_config(buffer_size, std::mem::size_of::<E>(), true),
            event_factory,
        )?;
        let producer_sequence = MmapCursor::new(layout.producer_cursor_config(true), -1)?;
        consumer_barrier.set_producer_cursor(producer_sequence.clone());

        Ok(Self {
            ring_buffer,
            producer_sequence,
            consumer_barrier,
            sequence: 0,
            sequence_clear_of_consumers: buffer_size as i64 - 1,
            required_consumer_liveness: None,
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

        let deadline = std::time::Instant::now()
            .checked_add(state.startup_wait_timeout())
            .expect("startup_wait_timeout does not fit in Instant");

        loop {
            let missing = state
                .missing_required_consumers(|consumer_id| self.discover_consumer_id(consumer_id));
            if missing.is_empty() {
                let now = std::time::Instant::now();
                state.seed_progress(now, |consumer_id| self.consumer_sequence(consumer_id));
                state.mark_startup_completed(now);
                self.required_consumer_liveness = Some(state);
                return Ok(());
            }

            if std::time::Instant::now() >= deadline {
                let error = RequiredConsumerError::StartupTimeout { missing };
                self.required_consumer_liveness = Some(state);
                return Err(error);
            }

            std::thread::sleep(Duration::from_millis(25));
        }
    }

    #[cold]
    #[inline(never)]
    fn check_required_consumer_liveness(&mut self) -> Result<(), RequiredConsumerError> {
        let Some(mut state) = self.required_consumer_liveness.take() else {
            return Ok(());
        };

        let now = std::time::Instant::now();
        let failure = state.evaluate_blocked(now, self.last_published_sequence(), |consumer_id| {
            self.consumer_sequence(consumer_id)
        });
        self.required_consumer_liveness = Some(state);

        if let Some(error) = failure {
            return Err(error);
        }

        Ok(())
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

    /// Publish one event with required-consumer liveness enforcement.
    pub fn publish_managed<F>(&mut self, update: F) -> Result<Sequence, RequiredConsumerError>
    where
        F: FnOnce(&mut E),
    {
        self.ensure_required_consumers_ready()?;

        let mut update = Some(update);
        loop {
            if self.next_sequences(1).is_ok() {
                return Ok(self.apply_update(
                    update
                        .take()
                        .expect("managed mmap producer update is consumed once"),
                ));
            }

            self.check_required_consumer_liveness()?;
            std::thread::yield_now();
        }
    }

    /// Publish a batch with required-consumer liveness enforcement.
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

    /// Attach a known consumer cursor by explicit consumer id.
    pub fn discover_consumer_id(&mut self, consumer_id: &str) -> bool {
        self.consumer_barrier.discover_consumer_id(consumer_id)
    }

    /// Wait for a known consumer cursor to appear by explicit consumer id.
    pub fn wait_for_consumer_id(&mut self, consumer_id: &str, timeout: Duration) -> bool {
        assert!(timeout > Duration::ZERO, "timeout must be positive");
        let deadline = std::time::Instant::now()
            .checked_add(timeout)
            .expect("timeout duration does not fit in Instant");

        loop {
            if self.discover_consumer_id(consumer_id) {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    /// Return the latest visible sequence for a specific consumer id.
    pub fn consumer_sequence(&mut self, consumer_id: &str) -> Option<Sequence> {
        self.consumer_barrier.consumer_sequence(consumer_id)
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
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

    fn test_liveness_config() -> RequiredConsumerLivenessConfig {
        RequiredConsumerLivenessConfig::new(vec!["c1".into(), "c2".into()])
            .with_startup_wait_timeout(Duration::from_millis(100))
            .with_progress_timeout(Duration::from_millis(20))
            .with_progress_check_interval(Duration::from_millis(1))
            .with_shutdown_grace_period(Duration::from_millis(200))
    }

    #[test]
    fn managed_publish_reports_missing_required_consumer_at_startup() {
        let layout = unique_layout("mmap_req_start");
        let mut producer =
            MmapProducer::<TestEvent>::create(layout.clone(), 4, TestEvent::default).unwrap();
        producer.enable_required_consumer_liveness(test_liveness_config());

        let _consumer1 = MmapConsumer::<TestEvent>::attach(layout.clone(), 4, "c1").unwrap();

        let error = producer
            .publish_managed(|event| {
                event.sequence = 0;
                event.data = 11;
            })
            .expect_err("missing c2 should trip startup timeout");

        assert!(matches!(
            error,
            RequiredConsumerError::StartupTimeout { missing } if missing == vec!["c2".to_string()]
        ));

        let _ = std::fs::remove_dir_all(layout.root_dir());
    }

    #[test]
    fn managed_batch_publish_reports_missing_required_consumer_at_startup() {
        let layout = unique_layout("mmap_req_batch_start");
        let mut producer =
            MmapProducer::<TestEvent>::create(layout.clone(), 4, TestEvent::default).unwrap();
        producer.enable_required_consumer_liveness(test_liveness_config());

        let _consumer1 = MmapConsumer::<TestEvent>::attach(layout.clone(), 4, "c1").unwrap();

        let error = producer
            .publish_batch_managed(2, |event, index| {
                event.sequence = index as i64;
                event.data = (index as i64) * 10;
            })
            .expect_err("missing c2 should trip startup timeout on batch path");

        assert!(matches!(
            error,
            RequiredConsumerError::StartupTimeout { missing } if missing == vec!["c2".to_string()]
        ));

        let _ = std::fs::remove_dir_all(layout.root_dir());
    }

    #[test]
    fn managed_publish_shuts_down_when_required_consumer_stalls() {
        let layout = unique_layout("mmap_req_stall");
        let mut producer =
            MmapProducer::<TestEvent>::create(layout.clone(), 4, TestEvent::default).unwrap();
        producer.enable_required_consumer_liveness(test_liveness_config());

        let stop_consumer1 = Arc::new(AtomicBool::new(false));
        let stop_consumer1_thread = Arc::clone(&stop_consumer1);
        let layout_for_thread = layout.clone();
        let consumer1_thread = std::thread::spawn(move || {
            let mut consumer1 =
                MmapConsumer::<TestEvent>::attach(layout_for_thread, 4, "c1").unwrap();
            while !stop_consumer1_thread.load(Ordering::Acquire) {
                if consumer1.try_consume_next().is_none() {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        });

        let mut consumer2 = MmapConsumer::<TestEvent>::attach(layout.clone(), 4, "c2").unwrap();

        producer
            .publish_managed(|event| {
                event.sequence = 0;
                event.data = 0;
            })
            .unwrap();
        let _ = consumer2.consume_next();
        drop(consumer2);

        for i in 1..=4 {
            producer
                .publish_managed(|event| {
                    event.sequence = i as i64;
                    event.data = (i as i64) * 10;
                })
                .unwrap();
        }

        let error = producer
            .publish_managed(|event| {
                event.sequence = 99;
                event.data = 990;
            })
            .expect_err("managed mmap publish should fail once c2 stops advancing");

        stop_consumer1.store(true, Ordering::Release);
        consumer1_thread.join().unwrap();

        match error {
            RequiredConsumerError::GracefulShutdownTriggered { consumer_id, .. } => {
                assert_eq!(consumer_id, "c2");
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let _ = std::fs::remove_dir_all(layout.root_dir());
    }

    #[test]
    fn managed_publish_recovers_when_same_consumer_id_rejoins() {
        let layout = unique_layout("mmap_req_rejn");
        let mut producer =
            MmapProducer::<TestEvent>::create(layout.clone(), 4, TestEvent::default).unwrap();
        producer.enable_required_consumer_liveness(test_liveness_config());

        let stop_consumer1 = Arc::new(AtomicBool::new(false));
        let stop_consumer1_thread = Arc::clone(&stop_consumer1);
        let layout_for_thread = layout.clone();
        let consumer1_thread = std::thread::spawn(move || {
            let mut consumer1 =
                MmapConsumer::<TestEvent>::attach(layout_for_thread, 4, "c1").unwrap();
            while !stop_consumer1_thread.load(Ordering::Acquire) {
                if consumer1.try_consume_next().is_none() {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        });

        let mut consumer2 = MmapConsumer::<TestEvent>::attach(layout.clone(), 4, "c2").unwrap();

        producer
            .publish_managed(|event| {
                event.sequence = 0;
                event.data = 0;
            })
            .unwrap();
        let _ = consumer2.consume_next();
        drop(consumer2);

        for i in 1..=4 {
            producer
                .publish_managed(|event| {
                    event.sequence = i as i64;
                    event.data = (i as i64) * 10;
                })
                .unwrap();
        }

        let layout_for_rejoin = layout.clone();
        let rejoin_thread = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(40));
            let mut rejoined =
                MmapConsumer::<TestEvent>::attach(layout_for_rejoin, 4, "c2").unwrap();
            let deadline = Instant::now() + Duration::from_millis(500);
            let mut consumed = 0usize;
            while Instant::now() < deadline && consumed < 6 {
                if rejoined.try_consume_next().is_some() {
                    consumed += 1;
                } else {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            consumed
        });

        let sequence = producer
            .publish_managed(|event| {
                event.sequence = 99;
                event.data = 990;
            })
            .expect("same-id mmap rejoin should recover before shutdown");

        stop_consumer1.store(true, Ordering::Release);
        consumer1_thread.join().unwrap();
        let rejoined_consumed = rejoin_thread.join().unwrap();

        assert!(sequence >= 4);
        assert!(rejoined_consumed > 0, "rejoined c2 should consume backlog");

        let _ = std::fs::remove_dir_all(layout.root_dir());
    }
}
