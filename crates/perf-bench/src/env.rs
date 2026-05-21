//! Environment variable contract for the `perf-bench` harness.
//!
//! `bench::*` carries the env keys consumed by perf-bench binaries and
//! re-exported through `competitive-bench` via the workspace path dep.
//! Parsers live in [`disruptor_mp::env::read`].

macro_rules! env_keys {
    ($( $(#[$meta:meta])* $name:ident = $value:literal; )+ $(,)?) => {
        $(
            $(#[$meta])*
            pub const $name: &str = $value;
        )+
    };
}

/// Benchmark env keys shared by `perf-bench` and `competitive-bench`.
pub mod bench {
    env_keys! {
        /// Batch size used by layered message benchmarks.
        BATCH_SIZE = "MYELON_BENCH_BATCH_SIZE";
        /// Broadcast harness selector for child dispatch.
        BROADCAST_HARNESS = "MYELON_BENCH_BROADCAST_HARNESS";
        /// Ring buffer capacity or benchmark buffer size.
        BUFFER = "MYELON_BENCH_BUFFER";
        /// Buffer depth for sweep-driven benchmarks.
        BUFFER_DEPTH = "MYELON_BENCH_BUFFER_DEPTH";
        /// Buffer size, usually in ring slots.
        BUFFER_SIZE = "MYELON_BENCH_BUFFER_SIZE";
        /// Codec selector for framed and typed benchmarks.
        CODEC = "MYELON_BENCH_CODEC";
        /// Consumer count for ad-hoc launch paths.
        CONSUMERS = "MYELON_BENCH_CONSUMERS";
        /// Consumer identifier passed to a child process.
        CONSUMER_ID = "MYELON_BENCH_CONSUMER_ID";
        /// Coordination segment name for ping-pong style runs.
        COORDINATION_SEGMENT = "MYELON_BENCH_COORDINATION_SEGMENT";
        /// Child dispatch selector for the ping-pong binary.
        DISPATCH = "MYELON_BENCH_DISPATCH";
        /// Encoded payload size used by codec sweeps.
        ENCODED_BYTES = "MYELON_BENCH_ENCODED_BYTES";
        /// Event count for signal and raw ring runs.
        EVENTS = "MYELON_BENCH_EVENTS";
        /// Event size for low-level ring benchmarks.
        EVENT_SIZE = "MYELON_BENCH_EVENT_SIZE";
        /// Fragment segment name for framed fragmentation runs.
        FRAG_SEGMENT = "MYELON_BENCH_FRAG_SEGMENT";
        /// Selects JSON emission mode for bench children.
        JSON_MODE = "MYELON_BENCH_JSON_MODE";
        /// Canonical JSON output path for a benchmark run.
        JSON_OUT = "MYELON_BENCH_JSON_OUT";
        /// Iteration count for layout and structural sweeps.
        LAYOUT_ITERATIONS = "MYELON_BENCH_LAYOUT_ITERATIONS";
        /// Enables liveness instrumentation in selected benches.
        LIVENESS = "MYELON_BENCH_LIVENESS";
        /// Enables bench-local debug logging.
        LOG = "MYELON_BENCH_LOG";
        /// Directory for benchmark log files.
        LOG_DIR = "MYELON_BENCH_LOG_DIR";
        /// Message count for message-oriented benchmarks.
        MESSAGES = "MYELON_BENCH_MESSAGES";
        /// Requested message size in bytes.
        MESSAGE_SIZE = "MYELON_BENCH_MESSAGE_SIZE";
        /// mmap buffer size override for file-backed runs.
        MMAP_BUFFER_SIZE = "MYELON_BENCH_MMAP_BUFFER_SIZE";
        /// mmap event count override for file-backed runs.
        MMAP_EVENTS = "MYELON_BENCH_MMAP_EVENTS";
        /// mmap root directory for file-backed runs.
        MMAP_ROOT = "MYELON_BENCH_MMAP_ROOT";
        /// mmap segment name for file-backed runs.
        MMAP_SEGMENT = "MYELON_BENCH_MMAP_SEGMENT";
        /// Scenario mode selector for sweep-style benches.
        MODE = "MYELON_BENCH_MODE";
        /// Consumer count for multi-consumer runners.
        NUM_CONSUMERS = "MYELON_BENCH_NUM_CONSUMERS";
        /// Message count for raw harness children.
        NUM_MESSAGES = "MYELON_BENCH_NUM_MESSAGES";
        /// Output directory for benchmark artifacts.
        OUT_DIR = "MYELON_BENCH_OUT_DIR";
        /// Payload byte count for framed and typed benchmarks.
        PAYLOAD_BYTES = "MYELON_BENCH_PAYLOAD_BYTES";
        /// Payload size selector for sweeps and helpers.
        PAYLOAD_SIZE = "MYELON_BENCH_PAYLOAD_SIZE";
        /// Enables phase-timing instrumentation.
        PHASE_TIMING = "MYELON_BENCH_PHASE_TIMING";
        /// Enables ping-pong counters instrumentation.
        PINGPONG_COUNTERS = "MYELON_BENCH_PINGPONG_COUNTERS";
        /// Ping segment name for ping-pong scenarios.
        PING_SEGMENT = "MYELON_BENCH_PING_SEGMENT";
        /// Pong segment name for ping-pong scenarios.
        PONG_SEGMENT = "MYELON_BENCH_PONG_SEGMENT";
        /// Enables latency recording on selected surfaces.
        RECORD_LATENCY = "MYELON_BENCH_RECORD_LATENCY";
        /// Generic root path for benchmark artifacts or mmap runs.
        ROOT = "MYELON_BENCH_ROOT";
        /// Generic segment name for benchmark children.
        SEGMENT = "MYELON_BENCH_SEGMENT";
        /// Shared-memory segment name used by launch helpers.
        SEGMENT_NAME = "MYELON_BENCH_SEGMENT_NAME";
        /// Signal counters mode selector.
        SIGNAL_COUNTERS_MODE = "MYELON_BENCH_SIGNAL_COUNTERS_MODE";
        /// Shared-memory identifier for signal counters.
        SIGNAL_COUNTERS_SHM_ID = "MYELON_BENCH_SIGNAL_COUNTERS_SHM_ID";
        /// Signal latency instrumentation mode selector.
        SIGNAL_LATENCY_MODE = "MYELON_BENCH_SIGNAL_LATENCY_MODE";
        /// Enables recording of signal latency histograms.
        SIGNAL_RECORD_LATENCY = "MYELON_BENCH_SIGNAL_RECORD_LATENCY";
        /// Signal latency sampling divisor.
        SIGNAL_SAMPLE_EVERY = "MYELON_BENCH_SIGNAL_SAMPLE_EVERY";
        /// mmap path for the signal latency sidecar buffer.
        SIGNAL_SIDECAR_MMAP_PATH = "MYELON_BENCH_SIGNAL_SIDECAR_MMAP_PATH";
        /// Shared-memory identifier for the signal latency sidecar buffer.
        SIGNAL_SIDECAR_SHM_ID = "MYELON_BENCH_SIGNAL_SIDECAR_SHM_ID";
        /// Fixed offered rate for signal CO-style experiments.
        SIGNAL_TARGET_RATE = "MYELON_BENCH_SIGNAL_TARGET_RATE";
        /// Fixed offered rate for message CO benchmarks.
        TARGET_RATE = "MYELON_BENCH_TARGET_RATE";
        /// Default benchmark timeout, in seconds.
        TIMEOUT = "MYELON_BENCH_TIMEOUT";
        /// Explicit timeout override parsed from CLI flags.
        TIMEOUT_OVERRIDE = "MYELON_BENCH_TIMEOUT_OVERRIDE";
        /// Event count for wait-strategy microbenchmarks.
        WAIT_NUM_EVENTS = "MYELON_BENCH_WAIT_NUM_EVENTS";
        /// Wait strategy selector for a child benchmark.
        WAIT_STRATEGY = "MYELON_BENCH_WAIT_STRATEGY";
        /// Warmup count or warmup event budget.
        WARMUP = "MYELON_BENCH_WARMUP";
        /// Enables additional benchmark debug output.
        DEBUG = "MYELON_BENCH_DEBUG";
        /// Attach timeout used by competitive-bench internal adapters.
        ATTACH_TIMEOUT_MS = "MYELON_BENCH_ATTACH_TIMEOUT_MS";
        /// Coordination timeout used by competitive-bench internal adapters.
        COORD_TIMEOUT_MS = "MYELON_BENCH_COORD_TIMEOUT_MS";
    }
}
