use myelon::{
    self,
    consumer::SharedConsumer,
    inference::{FixedTopology, WorkerCount},
    lock_free::{self, ConsumerBarrier},
    producer, shared_memory, transport,
};
use myelon::{attach_shared_consumer, build_shared_single_producer};
use myelon::{MultiProcessError, MultiProcessResult};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Copy, Clone, Default, Debug, PartialEq)]
struct MpEvent {
    sequence: u64,
    payload: u64,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
struct ApiFrame {
    len: usize,
    kind: u8,
    flags: u8,
    msg_id: u32,
    data: [u8; 8],
}

impl transport::FramedTransportFrame for ApiFrame {
    fn payload_capacity() -> usize {
        8
    }

    fn frame_meta(&self) -> transport::FrameMeta<'_> {
        transport::FrameMeta {
            len: self.len,
            kind: self.kind,
            flags: self.flags,
            msg_id: self.msg_id,
            timestamp_ns: None,
            data: &self.data[..self.len],
        }
    }

    fn write_frame(&mut self, payload: &[u8], kind: u8, msg_id: u32, flags: u8) {
        self.len = payload.len();
        self.kind = kind;
        self.flags = flags;
        self.msg_id = msg_id;
        self.data[..payload.len()].copy_from_slice(payload);
    }
}

fn unique_name(prefix: &str) -> String {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let prefix: String = prefix
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .take(4)
        .collect();
    let pid = std::process::id() % 10_000;
    let suffix = COUNTER.fetch_add(1, Ordering::Relaxed) % 100;
    let name = format!("{prefix}{pid:04}{suffix:02}");
    assert!(
        name.len() <= 14,
        "segment name exceeds macOS budget: {name}"
    );
    name
}

#[test]
fn api_boundary_smoke_test() {
    let _ = std::mem::size_of::<shared_memory::SharedMemoryConfig>();
    let _ = std::mem::size_of::<shared_memory::ShmRingBuffer<MpEvent>>();
    let _ = std::mem::size_of::<lock_free::SharedCursor>();
    let _ = std::mem::size_of::<lock_free::ConsumerBarrier>();
    let _ = std::mem::size_of::<lock_free::ProducerBarrier>();
    let _ = std::mem::size_of::<producer::CoordinationMode>();
    let _ = std::mem::size_of::<myelon::shared_memory::SharedRingBuffer<MpEvent>>();
    let _ = std::mem::size_of::<myelon::backend::shared_memory::ShmRingBuffer<MpEvent>>();
    let _ = std::mem::size_of::<myelon::prelude::SharedProducer<MpEvent>>();
    let _ = std::mem::size_of::<myelon::prelude::SharedConsumer<MpEvent>>();
    let _ = std::mem::size_of::<ConsumerBarrier>();
    let _ = std::mem::size_of::<FixedTopology>();
    let _ = std::mem::size_of::<WorkerCount>();
    let _ = std::mem::size_of::<MultiProcessError>();
    let _ = std::mem::size_of::<MultiProcessResult<MpEvent>>();
    let _ = std::mem::size_of::<transport::MyelonTransportConfig>();
    let _ = std::mem::size_of::<transport::MyelonTransportLayout>();
    let _ = std::mem::size_of::<transport::RunnerMyelonTransportConfig>();
    let _ = std::mem::size_of::<transport::MyelonWaitStrategy>();
    let _ = std::mem::size_of::<transport::FrameMeta<'static>>();
    let _ = std::mem::size_of::<transport::FixedFrame<8>>();
    let _ = std::mem::size_of::<transport::FramedTransportProducer<ApiFrame>>();
    let _ = std::mem::size_of::<transport::FramedTransportConsumer<ApiFrame>>();
    let _ = std::mem::size_of::<myelon::MmapProducer<MpEvent>>();
    let _ = std::mem::size_of::<myelon::MmapConsumer<MpEvent>>();
    let _ = std::mem::size_of::<myelon::MmapTransportLayout>();

    let segment = unique_name("boundary");
    let buffer_size = 8usize;
    let events_to_publish = 4u64;

    let mut producer = build_shared_single_producer::<MpEvent>(&segment, buffer_size)
        .enable_discovery(1)
        .build_producer(MpEvent::default)
        .expect("producer creation should succeed");

    let _consumer: SharedConsumer<MpEvent> =
        attach_shared_consumer::<MpEvent>(&segment, buffer_size)
            .enable_discovery(1)
            .with_coordination(producer::CoordinationMode::Immediate)
            .build_consumer()
            .expect("consumer creation should succeed");

    for idx in 0..events_to_publish {
        producer
            .try_publish(|event| {
                event.sequence = idx;
                event.payload = idx.saturating_mul(2);
            })
            .expect("publish should succeed");
    }

    assert_eq!(
        producer.last_published_sequence(),
        (events_to_publish as i64) - 1
    );

    let topology = FixedTopology::new(segment.clone(), buffer_size, WorkerCount::Two);
    assert_eq!(topology.worker_count().as_usize(), 2);
    assert_eq!(
        topology
            .worker_consumer_id(1)
            .expect("worker 1 id should be valid"),
        format!("{segment}_wk_1")
    );

    let transport_layout =
        transport::MyelonTransportLayout::for_session_with_prefix("vmy", &segment, 2, 16, 8)
            .expect("transport layout should be valid");
    let transport_config = transport::MyelonTransportConfig::resolve(None, None, None)
        .expect("transport config should be valid");
    assert_eq!(transport_layout.transport_prefix(), "vmy");
    assert_eq!(transport_layout.runner_count(), 2);
    assert_eq!(
        transport_config.rpc_depth,
        transport::DEFAULT_MYELON_RPC_DEPTH
    );
    assert_eq!(
        transport_config.response_depth,
        transport::DEFAULT_MYELON_RESPONSE_DEPTH
    );
    assert!(transport::is_single_frame(transport::frame_flags(
        true, true
    )));
    assert!(transport::is_last_frame(transport::frame_flags(
        false, true
    )));
    assert_eq!(
        transport::RunnerMyelonTransportConfig::for_rank(
            &transport_layout,
            1,
            transport::MyelonWaitStrategy::Block
        )
        .expect("ranked config should be valid")
        .response_ring_name,
        transport_layout
            .response_ring_name(1)
            .expect("response ring should exist")
    );
}
