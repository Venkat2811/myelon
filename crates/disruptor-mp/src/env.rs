//! Environment variable contract for the multiprocess substrate.
//!
//! Centralises env-key string constants and parsing helpers so that callers do
//! not scatter ad-hoc literals or duplicated parsers. The `read` parsers are
//! also used by workspace siblings (`myelon-dst`, `perf-bench`,
//! `competitive-bench`) that depend on this crate.
//!
//! Public surface:
//!
//! - [`read`] — generic env-var parsing helpers.
//! - [`runtime`] — env-key string constants for substrate tuning knobs.
//! - `dst::buggify` — DST BUGGIFY env-key constants, gated by `#[cfg(dst)]`.

use std::str::FromStr;

macro_rules! env_keys {
    ($( $(#[$meta:meta])* $name:ident = $value:literal; )+ $(,)?) => {
        $(
            $(#[$meta])*
            pub const $name: &str = $value;
        )+
    };
}

/// Generic environment-variable parsing helpers.
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

/// Deterministic-simulation env keys. Gated by `--cfg dst` because the rest of
/// the DST surface in this crate is gated the same way.
#[cfg(dst)]
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
}
