use disruptor_mp::cursor::SharedCursor;
use disruptor_mp::ringbuffer::SharedRingBuffer;
use disruptor_mp::wait::SLEEP_CONFIG;

fn main() {
    let _ = std::mem::size_of::<SharedCursor>();
    let _ = std::mem::size_of::<SharedRingBuffer<u64>>();
    let _ = SLEEP_CONFIG.shutdown_grace_ms;
}
