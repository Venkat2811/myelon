use disruptor_mp::producer::{ConsumerBarrier, DiscoveryMode};

fn main() {
    let _ = std::mem::size_of::<ConsumerBarrier>();
    let _ = DiscoveryMode::Disabled;
}
