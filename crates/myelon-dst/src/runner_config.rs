use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackendKind {
    Shm,
    Mmap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WaitStrategyKind {
    BusySpin,
    Sleep,
    Block,
    SpinLoopHint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CodecKind {
    Rkyv,
    Flatbuf,
    Bincode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CoordinationKind {
    External,
    SharedMemoryDiscovery,
    MmapBuiltin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DstConfig {
    pub seed: u64,
    pub backend: BackendKind,
    pub ring_depth: usize,
    pub payload_size: usize,
    pub frame_size: usize,
    pub consumer_count: usize,
    pub message_count: u64,
    pub wait_strategy: WaitStrategyKind,
    pub codec: Option<CodecKind>,
    pub zero_copy: bool,
    pub coordination: CoordinationKind,
}

#[derive(Debug, Clone)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        let init = if seed == 0 {
            0x9E37_79B1_85EB_CA87
        } else {
            seed
        };
        Self { state: init }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 7;
        x ^= x >> 9;
        x ^= x << 8;
        self.state = x;
        x
    }

    fn pick<T: Copy>(&mut self, values: &[T]) -> T {
        let index = (self.next_u64() as usize) % values.len();
        values[index]
    }
}

const RING_DEPTHS: &[usize] = &[
    256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072,
];
const FRAME_SIZES: &[usize] = &[1024, 2048, 4096, 8192, 16384, 32768, 65536];
const RAW_RING_PAYLOAD_SIZES: &[usize] = &[64, 128, 256, 512, 1024];
const FULL_PAYLOAD_SIZES: &[usize] =
    &[64, 128, 256, 512, 1024, 4096, 16384, 65536, 262144, 1048576];
const CONSUMER_COUNTS: &[usize] = &[1, 2, 4, 6, 8, 12];
const MESSAGE_COUNTS: &[u64] = &[1000, 2000, 5000, 10000, 20000, 50000, 100000];
const WAIT_STRATEGIES: &[WaitStrategyKind] = &[
    WaitStrategyKind::BusySpin,
    WaitStrategyKind::Sleep,
    WaitStrategyKind::Block,
    WaitStrategyKind::SpinLoopHint,
];
const CODECS: &[Option<CodecKind>] = &[
    None,
    Some(CodecKind::Rkyv),
    Some(CodecKind::Flatbuf),
    Some(CodecKind::Bincode),
];

impl DstConfig {
    pub fn from_seed(seed: u64) -> Self {
        let mut rng = XorShift64::new(seed);
        let backend = rng.pick(&[BackendKind::Shm, BackendKind::Mmap]);
        let codec = rng.pick(CODECS);

        Self {
            seed,
            backend,
            ring_depth: rng.pick(RING_DEPTHS),
            payload_size: rng.pick(FULL_PAYLOAD_SIZES),
            frame_size: rng.pick(FRAME_SIZES),
            consumer_count: rng.pick(CONSUMER_COUNTS),
            message_count: rng.pick(MESSAGE_COUNTS),
            wait_strategy: rng.pick(WAIT_STRATEGIES),
            codec,
            zero_copy: matches!(codec, Some(CodecKind::Rkyv | CodecKind::Flatbuf))
                && (rng.next_u64() & 1) == 0,
            coordination: match backend {
                BackendKind::Shm => CoordinationKind::SharedMemoryDiscovery,
                BackendKind::Mmap => CoordinationKind::MmapBuiltin,
            },
        }
    }

    pub fn raw_ring_from_seed(seed: u64) -> Self {
        let mut config = Self::from_seed(seed);
        let mut rng = XorShift64::new(seed ^ 0xD57D_A11E_5EED);
        config.codec = None;
        config.zero_copy = false;
        config.frame_size = 1024;
        config.payload_size = rng.pick(RAW_RING_PAYLOAD_SIZES);
        config.consumer_count = rng.pick(CONSUMER_COUNTS);
        config.message_count = rng.pick(MESSAGE_COUNTS);
        config
    }

    pub fn with_backend(mut self, backend: BackendKind) -> Self {
        self.backend = backend;
        self.coordination = match backend {
            BackendKind::Shm => CoordinationKind::SharedMemoryDiscovery,
            BackendKind::Mmap => CoordinationKind::MmapBuiltin,
        };
        self
    }

    pub fn with_ring_depth(mut self, ring_depth: usize) -> Self {
        self.ring_depth = ring_depth;
        self
    }

    pub fn with_payload_size(mut self, payload_size: usize) -> Self {
        self.payload_size = payload_size;
        self
    }

    pub fn with_consumer_count(mut self, consumer_count: usize) -> Self {
        self.consumer_count = consumer_count;
        self
    }

    pub fn with_message_count(mut self, message_count: u64) -> Self {
        self.message_count = message_count;
        self
    }

    pub fn with_wait_strategy(mut self, wait_strategy: WaitStrategyKind) -> Self {
        self.wait_strategy = wait_strategy;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeded_configs_are_deterministic() {
        assert_eq!(DstConfig::from_seed(42), DstConfig::from_seed(42));
        assert_eq!(
            DstConfig::raw_ring_from_seed(77),
            DstConfig::raw_ring_from_seed(77)
        );
    }

    #[test]
    fn raw_ring_config_stays_in_supported_range() {
        let cfg = DstConfig::raw_ring_from_seed(99);
        assert!(RAW_RING_PAYLOAD_SIZES.contains(&cfg.payload_size));
        assert_eq!(cfg.codec, None);
        assert!(!cfg.zero_copy);
    }
}
