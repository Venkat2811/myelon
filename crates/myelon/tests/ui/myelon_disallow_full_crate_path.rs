use myelon::disruptor_mp;

fn main() {
    let _ = std::mem::size_of::<disruptor_mp::ringbuffer::SharedRingBuffer<u64>>();
}
