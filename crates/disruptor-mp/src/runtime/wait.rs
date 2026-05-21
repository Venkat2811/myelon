//! Sleep configuration for multiprocess operations
//!
//! This module provides configurable sleep durations for various waiting operations
//! in the multiprocess implementation. Values can be configured via environment
//! variables with sensible defaults.

use crate::env::{read, runtime as runtime_env};
use once_cell::sync::Lazy;
use std::time::Duration;

/// Sleep configuration for multiprocess operations
#[derive(Debug, Clone)]
pub struct SleepConfig {
    /// Grace period for thread shutdown (milliseconds)
    pub shutdown_grace_ms: u64,
    /// Block wait strategy sleep duration (milliseconds)
    pub block_strategy_ms: f64,
    /// Consumer discovery polling interval (milliseconds)
    pub discovery_poll_ms: u64,
    /// `consume_next_with_sleep` duration (microseconds)
    pub consume_sleep_us: u64,
    /// Durations below this threshold yield instead of calling into the OS scheduler.
    pub sleep_yield_threshold_us: u64,
    /// Python consumer busy wait prevention (microseconds)
    pub consumer_busy_wait_us: u64,
}

impl Default for SleepConfig {
    fn default() -> Self {
        Self {
            shutdown_grace_ms: 10,
            // Keep the default block backoff in the low-microsecond range. The current
            // implementation is still sleep-based rather than a true futex/condvar block,
            // so millisecond-scale defaults make the library look arbitrarily slow.
            block_strategy_ms: 0.01,
            discovery_poll_ms: 10,
            consume_sleep_us: 1,
            sleep_yield_threshold_us: 50,
            consumer_busy_wait_us: 10,
        }
    }
}

impl SleepConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();

        // Parse shutdown grace period
        if let Some(ms) = read::parse::<u64>(runtime_env::SHUTDOWN_GRACE_MS) {
            config.shutdown_grace_ms = ms;
        }

        // Parse block strategy sleep (microsecond override first for sane low-latency tuning).
        if let Some(us) = read::parse::<u64>(runtime_env::BLOCK_STRATEGY_US) {
            config.block_strategy_ms = us as f64 / 1000.0;
        } else if let Some(ms) = read::parse::<f64>(runtime_env::BLOCK_STRATEGY_MS) {
            if ms >= 0.0 {
                config.block_strategy_ms = ms;
            }
        }

        // Parse discovery poll interval
        if let Some(ms) = read::parse::<u64>(runtime_env::DISCOVERY_POLL_MS) {
            config.discovery_poll_ms = ms;
        }

        // Parse consume sleep
        if let Some(us) = read::parse::<u64>(runtime_env::CONSUME_SLEEP_US) {
            config.consume_sleep_us = us;
        }

        if let Some(us) = read::parse::<u64>(runtime_env::SLEEP_YIELD_THRESHOLD_US) {
            config.sleep_yield_threshold_us = us;
        }

        // Parse consumer busy wait
        if let Some(us) = read::parse::<u64>(runtime_env::CONSUMER_BUSY_WAIT_US) {
            config.consumer_busy_wait_us = us;
        }

        config
    }

    /// Get shutdown grace period as Duration
    pub fn shutdown_grace_duration(&self) -> Duration {
        Duration::from_millis(self.shutdown_grace_ms)
    }

    /// Get block strategy duration
    pub fn block_strategy_duration(&self) -> Duration {
        let millis = (self.block_strategy_ms * 1000.0) as u64;
        let nanos = ((self.block_strategy_ms * 1_000_000.0) as u64) % 1_000_000;
        Duration::from_millis(millis / 1000) + Duration::from_nanos(nanos)
    }

    /// Get discovery poll duration
    pub fn discovery_poll_duration(&self) -> Duration {
        Duration::from_millis(self.discovery_poll_ms)
    }

    /// Get consume sleep duration
    pub fn consume_sleep_duration(&self) -> Duration {
        Duration::from_micros(self.consume_sleep_us)
    }

    /// Get consumer busy wait duration
    #[cfg(test)]
    pub fn consumer_busy_wait_duration(&self) -> Duration {
        Duration::from_micros(self.consumer_busy_wait_us)
    }
}

/// Global sleep configuration loaded once at startup
pub static SLEEP_CONFIG: Lazy<SleepConfig> = Lazy::new(SleepConfig::from_env);

/// Return the configured backoff used by `AutoWaitStrategy::Block`.
#[inline]
pub fn default_block_strategy_duration() -> Duration {
    SLEEP_CONFIG.block_strategy_duration()
}

/// Return the configured sleep used by `consume_next_with_sleep` and similar helpers.
#[inline]
pub fn default_consume_sleep_duration() -> Duration {
    SLEEP_CONFIG.consume_sleep_duration()
}

/// Return the configured poll interval used by discovery/startup coordination loops.
#[inline]
pub fn default_discovery_poll_duration() -> Duration {
    SLEEP_CONFIG.discovery_poll_duration()
}

/// For very short waits, yield instead of sleeping so scheduler granularity does not
/// turn microsecond-scale wait strategies into tens-of-microseconds stalls.
#[inline]
pub fn sleep_or_yield(duration: Duration) {
    if duration <= Duration::from_micros(SLEEP_CONFIG.sleep_yield_threshold_us) {
        std::thread::yield_now();
    } else {
        std::thread::sleep(duration);
    }
}

/// Apply the configured `AutoWaitStrategy::Block` wait policy.
#[inline]
pub fn perform_default_block_wait() {
    sleep_or_yield(default_block_strategy_duration());
}

/// Apply the configured consumer sleep wait policy.
#[inline]
pub fn perform_default_consume_sleep_wait() {
    sleep_or_yield(default_consume_sleep_duration());
}

/// Apply the configured discovery/startup polling wait policy.
#[inline]
pub fn perform_default_discovery_poll_wait() {
    sleep_or_yield(default_discovery_poll_duration());
}

/// Apply the explicit `AutoWaitStrategy::Sleep(duration)` wait policy.
#[inline]
pub fn perform_sleep_wait(duration: Duration) {
    sleep_or_yield(duration);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = SleepConfig::default();
        assert_eq!(config.shutdown_grace_ms, 10);
        assert_eq!(config.block_strategy_ms, 0.01);
        assert_eq!(config.discovery_poll_ms, 10);
        assert_eq!(config.consume_sleep_us, 1);
        assert_eq!(config.sleep_yield_threshold_us, 50);
        assert_eq!(config.consumer_busy_wait_us, 10);
    }

    #[test]
    fn test_duration_conversions() {
        let config = SleepConfig::default();

        assert_eq!(config.shutdown_grace_duration(), Duration::from_millis(10));
        assert_eq!(config.discovery_poll_duration(), Duration::from_millis(10));
        assert_eq!(config.consume_sleep_duration(), Duration::from_micros(1));
        assert_eq!(config.block_strategy_duration(), Duration::from_micros(10));
        assert_eq!(
            config.consumer_busy_wait_duration(),
            Duration::from_micros(10)
        );
    }

    #[test]
    fn test_block_strategy_fractional() {
        let config = SleepConfig {
            block_strategy_ms: 0.5,
            ..SleepConfig::default()
        };

        // 0.5ms = 500μs
        assert_eq!(config.block_strategy_duration(), Duration::from_micros(500));
    }
}
