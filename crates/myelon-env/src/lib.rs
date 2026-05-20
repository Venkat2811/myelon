#![warn(missing_docs)]
//! Shared environment key definitions and parsing helpers.
//!
//! This crate keeps benchmark, runtime, and DST env contracts in one place so
//! workspace crates stop scattering string literals and ad-hoc parsers.

use std::str::FromStr;

macro_rules! env_keys {
    ($( $(#[$meta:meta])* $name:ident = $value:literal; )+ $(,)?) => {
        $(
            $(#[$meta])*
            pub const $name: &str = $value;
        )+
    };
}

/// Generic environment parsing helpers.
pub mod read {
    use super::FromStr;

    /// Read a required string environment variable.
    pub fn required(key: &str) -> String {
        std::env::var(key).unwrap_or_else(|_| panic!("missing env var: {key}"))
    }

    /// Read an optional string environment variable.
    pub fn optional(key: &str) -> Option<String> {
        std::env::var(key).ok()
    }

    /// Read a string environment variable with a default fallback.
    pub fn string_or(key: &str, default: &str) -> String {
        std::env::var(key).unwrap_or_else(|_| default.to_string())
    }

    /// Parse an optional environment variable.
    pub fn parse<T>(key: &str) -> Option<T>
    where
        T: FromStr,
    {
        std::env::var(key).ok().and_then(|raw| raw.parse().ok())
    }

    /// Parse a required environment variable.
    pub fn parse_required<T>(key: &str) -> T
    where
        T: FromStr,
        <T as FromStr>::Err: std::fmt::Display,
    {
        let raw = required(key);
        raw.parse()
            .unwrap_or_else(|err| panic!("invalid {key}='{raw}': {err}"))
    }

    /// Parse an environment variable with a fallback default.
    pub fn parse_or<T>(key: &str, default: T) -> T
    where
        T: FromStr,
    {
        parse(key).unwrap_or(default)
    }

    /// Return true when the environment variable is set to a truthy value.
    pub fn flag(key: &str) -> bool {
        matches!(
            std::env::var(key).ok().as_deref(),
            Some("1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON")
        )
    }
}

/// Runtime knobs used by `disruptor-mp` and surfaced via `myelon`.
pub mod runtime {
    env_keys! {
        /// Nanosecond backoff override for `AutoWaitStrategy`.
        AUTO_WAIT_DELAY_NS = "MYELON_AUTO_WAIT_DELAY_NS";
        /// Microsecond backoff override for `AutoWaitStrategy`.
        AUTO_WAIT_DELAY_US = "MYELON_AUTO_WAIT_DELAY_US";
        /// Preferred CPU core for the auto consumer thread.
        AUTO_CONSUMER_CORE = "MYELON_AUTO_CONSUMER_CORE";
        /// Preferred CPU core for the producer process or thread.
        PRODUCER_CORE = "MYELON_PRODUCER_CORE";
        /// Preferred CPU core for the consumer process or thread.
        CONSUMER_CORE = "MYELON_CONSUMER_CORE";
        /// Fallback CPU core when no role-specific affinity is set.
        PROCESS_CORE = "MYELON_PROCESS_CORE";
        /// Grace period for shutdown waits, in milliseconds.
        SHUTDOWN_GRACE_MS = "MYELON_SHUTDOWN_GRACE_MS";
        /// Blocking wait sleep quantum, in microseconds.
        BLOCK_STRATEGY_US = "MYELON_BLOCK_STRATEGY_US";
        /// Blocking wait sleep quantum, in milliseconds.
        BLOCK_STRATEGY_MS = "MYELON_BLOCK_STRATEGY_MS";
        /// Discovery and startup polling interval, in milliseconds.
        DISCOVERY_POLL_MS = "MYELON_DISCOVERY_POLL_MS";
        /// Consumer sleep duration for sleep-based waits, in microseconds.
        CONSUME_SLEEP_US = "MYELON_CONSUME_SLEEP_US";
        /// Yield threshold before escalating to sleep, in microseconds.
        SLEEP_YIELD_THRESHOLD_US = "MYELON_SLEEP_YIELD_THRESHOLD_US";
        /// Busy-wait guard duration for consumers, in microseconds.
        CONSUMER_BUSY_WAIT_US = "MYELON_CONSUMER_BUSY_WAIT_US";
    }
}

/// Deterministic simulation and DST harness env keys.
pub mod dst {
    /// Shared BUGGIFY controls.
    pub mod buggify {
        env_keys! {
            /// Enables BUGGIFY fault injection.
            ENABLED = "MYELON_DST_BUGGIFY";
            /// Sets the deterministic BUGGIFY seed.
            SEED = "MYELON_DST_BUGGIFY_SEED";
            /// Sets the percentage of BUGGIFY sites that activate.
            ACTIVATION_PERCENT = "MYELON_DST_BUGGIFY_ACTIVATION_PERCENT";
            /// Sets the percentage of activated BUGGIFY sites that actually fire.
            FIRE_PERCENT = "MYELON_DST_BUGGIFY_FIRE_PERCENT";
        }
    }

    /// Child-runner and harness controls used by `myelon-dst`.
    pub mod runner {
        env_keys! {
            /// Selects the transport family the child runner executes.
            CHILD_TRANSPORT = "MYELON_DST_CHILD_TRANSPORT";
            /// Selects the backend the child runner executes.
            CHILD_BACKEND = "MYELON_DST_CHILD_BACKEND";
            /// Selects the child mode or scenario.
            CHILD_MODE = "MYELON_DST_CHILD_MODE";
            /// Printed by the child runner to confirm successful startup.
            CHILD_OK = "MYELON_DST_CHILD_OK";
            /// Root directory for run artifacts.
            RUN_ROOT = "MYELON_DST_RUN_ROOT";
            /// Shared memory or logical segment name for the scenario.
            SEGMENT = "MYELON_DST_SEGMENT";
            /// Deterministic seed for a scenario run.
            SEED = "MYELON_DST_SEED";
            /// Ring depth for the exercised transport.
            RING_DEPTH = "MYELON_DST_RING_DEPTH";
            /// Total message count for the scenario.
            MESSAGE_COUNT = "MYELON_DST_MESSAGE_COUNT";
            /// Payload size in bytes for the scenario.
            PAYLOAD_SIZE = "MYELON_DST_PAYLOAD_SIZE";
            /// Number of consumers to attach to the scenario.
            CONSUMER_COUNT = "MYELON_DST_CONSUMER_COUNT";
            /// Wait strategy slug for the scenario.
            WAIT_STRATEGY = "MYELON_DST_WAIT_STRATEGY";
            /// Producer post-publish hold interval, in milliseconds.
            POST_PUBLISH_HOLD_MS = "MYELON_DST_POST_PUBLISH_HOLD_MS";
            /// Frequency for injected publish pauses.
            PUBLISH_PAUSE_EVERY = "MYELON_DST_PUBLISH_PAUSE_EVERY";
            /// Length of injected publish pauses, in microseconds.
            PUBLISH_PAUSE_MICROS = "MYELON_DST_PUBLISH_PAUSE_MICROS";
            /// Forces the producer to wait for consumers to advertise readiness.
            WAIT_FOR_CONSUMERS_READY = "MYELON_DST_WAIT_FOR_CONSUMERS_READY";
            /// Prefix used when synthesizing consumer identifiers.
            CONSUMER_PREFIX = "MYELON_DST_CONSUMER_PREFIX";
            /// Path to the producer-side report file.
            PRODUCER_REPORT_PATH = "MYELON_DST_PRODUCER_REPORT_PATH";
            /// Path to the checkpoint file.
            CHECKPOINT_PATH = "MYELON_DST_CHECKPOINT_PATH";
            /// Path to the scenario report file.
            REPORT_PATH = "MYELON_DST_REPORT_PATH";
            /// Comma-separated required consumer identifiers.
            REQUIRED_CONSUMER_IDS = "MYELON_DST_REQUIRED_CONSUMER_IDS";
            /// Required-consumer startup deadline, in milliseconds.
            REQUIRED_STARTUP_WAIT_MS = "MYELON_DST_REQUIRED_STARTUP_WAIT_MS";
            /// Required-consumer progress timeout, in milliseconds.
            REQUIRED_PROGRESS_TIMEOUT_MS = "MYELON_DST_REQUIRED_PROGRESS_TIMEOUT_MS";
            /// Required-consumer progress polling interval, in milliseconds.
            REQUIRED_PROGRESS_CHECK_INTERVAL_MS = "MYELON_DST_REQUIRED_PROGRESS_CHECK_INTERVAL_MS";
            /// Required-consumer shutdown grace period, in milliseconds.
            REQUIRED_SHUTDOWN_GRACE_MS = "MYELON_DST_REQUIRED_SHUTDOWN_GRACE_MS";
            /// Override message count for the producer child.
            PRODUCER_MESSAGE_COUNT = "MYELON_DST_PRODUCER_MESSAGE_COUNT";
            /// Override message count for the consumer child.
            CONSUMER_MESSAGE_COUNT = "MYELON_DST_CONSUMER_MESSAGE_COUNT";
            /// Starting sequence number for corruption and checkpoint tests.
            SEQUENCE_START = "MYELON_DST_SEQUENCE_START";
            /// Checkpoint frequency in messages.
            CHECKPOINT_EVERY = "MYELON_DST_CHECKPOINT_EVERY";
            /// Sequence number at which corruption is injected.
            CORRUPT_AT_SEQUENCE = "MYELON_DST_CORRUPT_AT_SEQUENCE";
            /// Allows validation to continue after intentional corruption.
            ALLOW_CORRUPTION_VALIDATION = "MYELON_DST_ALLOW_CORRUPTION_VALIDATION";
            /// Zero-based consumer index used by child processes.
            CONSUMER_INDEX = "MYELON_DST_CONSUMER_INDEX";
            /// Concrete consumer identifier used by child processes.
            CONSUMER_ID = "MYELON_DST_CONSUMER_ID";
            /// Discovery polling interval for DST child processes, in milliseconds.
            DISCOVERY_POLL_MS = "MYELON_DST_DISCOVERY_POLL_MS";
            /// Grace period after producer completion, in milliseconds.
            PRODUCER_DONE_GRACE_MS = "MYELON_DST_PRODUCER_DONE_GRACE_MS";
        }
    }
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
