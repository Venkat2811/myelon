use disruptor_mp::{
    backend,
    consumer::SharedConsumer,
    lock_free::{ConsumerBarrier, DiscoveryMode, ProducerBarrier, SharedCursor},
    producer::CoordinationMode,
    shared_memory::{SharedMemoryConfig, SharedRingBuffer, ShmRingBuffer},
    SharedProducer,
};

#[test]
fn public_namespace_smoke_test() {
    let _ = std::mem::size_of::<SharedMemoryConfig>();
    let _ = std::mem::size_of::<SharedCursor>();
    let _ = std::mem::size_of::<ConsumerBarrier>();
    let _ = std::mem::size_of::<ProducerBarrier>();

    let _shared_ring: Option<SharedRingBuffer<u64>> = None;
    let _shm_ring: Option<ShmRingBuffer<u64>> = None;
    let _backend_shm_ring: Option<backend::shared_memory::ShmRingBuffer<u64>> = None;
    let _producer: Option<SharedProducer<u64>> = None;
    let _consumer: Option<SharedConsumer<u64>> = None;

    let _mode = CoordinationMode::Immediate;
    let _discovery = DiscoveryMode::Disabled;
}
