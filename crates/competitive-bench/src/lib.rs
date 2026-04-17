//! Competitive IPC benchmark suite.
//!
//! Bidirectional ping-pong benchmarks across multiple IPC libraries:
//! - disruptor-mp (our baseline)
//! - crossbar (zero-copy pub/sub over mmap)
//! - photon-ring (per-slot seqlock ring buffer)
//! - shmipc-rs (CloudWeGo shared memory)
//! - Rusteron/Aeron IPC
//! - ZeroMQ IPC (Unix domain socket baseline)
//!
//! Each adapter implements the same ping-pong protocol:
//! 1. Producer sends a message with timestamp
//! 2. Echo process receives and sends back
//! 3. Producer records round-trip latency
//!
//! Measurement modes:
//! - max_throughput: send as fast as possible, HDR latency
//! - co_aware: fixed send rate, measure from intended_send_time
//! - batch_timing: no per-event instrumentation
//!
//! Message sizes: 64B, 512B, 1KB, 2KB, 4KB, 8KB, 16KB, 32KB, 64KB, 128KB

pub mod adapter;

/// Common trait for IPC adapters.
pub trait PingPongAdapter {
    /// Name of the IPC library.
    fn name(&self) -> &str;

    /// Set up the bidirectional channel.
    fn setup(&mut self, message_size: usize) -> Result<(), Box<dyn std::error::Error>>;

    /// Send a message (producer side).
    fn send(&mut self, payload: &[u8]) -> Result<(), Box<dyn std::error::Error>>;

    /// Receive a message (echo/consumer side).
    fn recv(&mut self) -> Result<Vec<u8>, Box<dyn std::error::Error>>;

    /// Clean up resources.
    fn teardown(&mut self);
}
