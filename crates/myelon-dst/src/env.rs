//! Environment variable contract for the DST runner harness.
//!
//! `runner::*` carries the env keys consumed by the DST child runner and its
//! orchestrator. Parsers live in [`disruptor_mp::env::read`]; BUGGIFY env keys
//! live in [`disruptor_mp::env::dst::buggify`].

macro_rules! env_keys {
    ($( $(#[$meta:meta])* $name:ident = $value:literal; )+ $(,)?) => {
        $(
            $(#[$meta])*
            pub const $name: &str = $value;
        )+
    };
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
