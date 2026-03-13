use disruptor_mp::builder::build_shared_single_producer;
use disruptor_mp::cursor::SharedCursor;
use disruptor_mp::producer::SharedProducer;
use disruptor_mp::ringbuffer::SharedRingBuffer;
use disruptor_mp::wait::SLEEP_CONFIG;

fn main() {
    let _ = std::mem::size_of::<SharedCursor>();
    let _ = std::mem::size_of::<SharedRingBuffer<u64>>();
    let _ = build_shared_single_producer::<u64>;
    let _ = std::mem::size_of::<SharedProducer<u64>>();
    let _ = SLEEP_CONFIG.shutdown_grace_ms;
}
