use myelon::backend;
use myelon::consumer;
use myelon::inference::{FixedTopology, WorkerCount};
use myelon::lock_free::{ConsumerBarrier, ProducerBarrier, SharedCursor};
use myelon::producer::{CoordinationMode, SharedProducer};
use myelon::shared_memory::{SharedMemoryConfig, SharedRingBuffer, ShmRingBuffer};
use myelon::transport::{
    frame_flags, is_last_frame, is_single_frame, FixedFrame, FrameMeta, MyelonTransportConfig,
    MyelonTransportLayout, MyelonWaitStrategy, RunnerMyelonTransportConfig,
};
use myelon::{
    attach_shared_consumer, build_shared_single_producer, AutoWaitStrategy, MultiProcessError,
    MmapConsumer, MmapProducer, MmapTransportLayout, MultiProcessResult, Sequence,
};
use myelon::SharedDisruptorBuilder;

#[derive(Copy, Clone, Default)]
struct ApiFrame {
    len: usize,
    kind: u8,
    flags: u8,
    msg_id: u32,
    data: [u8; 8],
}

impl myelon::transport::FramedTransportFrame for ApiFrame {
    fn payload_capacity() -> usize {
        8
    }

    fn frame_meta(&self) -> FrameMeta<'_> {
        FrameMeta {
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

fn main() {
    let _builder_fn = build_shared_single_producer::<u64>;
    let _consumer_builder_fn = attach_shared_consumer::<u64>;

    let _ = std::mem::size_of::<backend::shared_memory::ShmRingBuffer<u64>>();
    let _ = std::mem::size_of::<consumer::SharedConsumer<u64>>();
    let _ = std::mem::size_of::<ConsumerBarrier>();
    let _ = std::mem::size_of::<ProducerBarrier>();
    let _ = std::mem::size_of::<SharedCursor>();
    let _ = std::mem::size_of::<CoordinationMode>();
    let _ = std::mem::size_of::<SharedProducer<u64>>();
    let _ = std::mem::size_of::<SharedDisruptorBuilder<u64>>();
    let _ = std::mem::size_of::<SharedMemoryConfig>();
    let _ = std::mem::size_of::<ShmRingBuffer::<u64>>();
    let _ = std::mem::size_of::<SharedRingBuffer::<u64>>();
    let _ = std::mem::size_of::<AutoWaitStrategy>();
    let _ = std::mem::size_of::<Sequence>();
    let _ = std::mem::size_of::<FixedTopology>();
    let _ = std::mem::size_of::<WorkerCount>();
    let _ = std::mem::size_of::<MultiProcessResult::<()>>();
    let _ = std::mem::size_of::<Result<(), MultiProcessError>>();
    let _ = std::mem::size_of::<MyelonTransportConfig>();
    let _ = std::mem::size_of::<MyelonTransportLayout>();
    let _ = std::mem::size_of::<RunnerMyelonTransportConfig>();
    let _ = std::mem::size_of::<MyelonWaitStrategy>();
    let _ = std::mem::size_of::<FrameMeta<'static>>();
    let _ = std::mem::size_of::<FixedFrame<8>>();
    let _ = std::mem::size_of::<myelon::transport::FramedTransportProducer<ApiFrame>>();
    let _ = std::mem::size_of::<myelon::transport::FramedTransportConsumer<ApiFrame>>();
    let _ = std::mem::size_of::<MmapProducer<u64>>();
    let _ = std::mem::size_of::<MmapConsumer<u64>>();
    let _ = std::mem::size_of::<MmapTransportLayout>();
    let _ = frame_flags(true, true);
    let _ = is_single_frame(0b11);
    let _ = is_last_frame(0b10);
}
