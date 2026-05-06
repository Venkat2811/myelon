//! Sleep configuration for multiprocess operations
//!
//! This module provides configurable sleep durations for various waiting operations
//! in the multiprocess implementation. Values can be configured via environment
//! variables with sensible defaults.

use once_cell::sync::Lazy;
use std::env;
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
    /// Python consumer busy wait prevention (microseconds)
    pub consumer_busy_wait_us: u64,
}

impl Default for SleepConfig {
    fn default() -> Self {
        Self {
            shutdown_grace_ms: 10,
            block_strategy_ms: 1.0,
            discovery_poll_ms: 10,
            consume_sleep_us: 1,
            consumer_busy_wait_us: 10,
        }
    }
}

impl SleepConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();

        // Parse shutdown grace period
        if let Ok(val) = env::var("DISRUPTOR_SHUTDOWN_GRACE_MS") {
            if let Ok(ms) = val.parse::<u64>() {
                config.shutdown_grace_ms = ms;
            }
        }

        // Parse block strategy sleep
        if let Ok(val) = env::var("DISRUPTOR_BLOCK_STRATEGY_MS") {
            if let Ok(ms) = val.parse::<f64>() {
                if ms >= 0.0 {
                    config.block_strategy_ms = ms;
                }
            }
        }

        // Parse discovery poll interval
        if let Ok(val) = env::var("DISRUPTOR_DISCOVERY_POLL_MS") {
            if let Ok(ms) = val.parse::<u64>() {
                config.discovery_poll_ms = ms;
            }
        }

        // Parse consume sleep
        if let Ok(val) = env::var("DISRUPTOR_CONSUME_SLEEP_US") {
            if let Ok(us) = val.parse::<u64>() {
                config.consume_sleep_us = us;
            }
        }

        // Parse consumer busy wait
        if let Ok(val) = env::var("DISRUPTOR_CONSUMER_BUSY_WAIT_US") {
            if let Ok(us) = val.parse::<u64>() {
                config.consumer_busy_wait_us = us;
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = SleepConfig::default();
        assert_eq!(config.shutdown_grace_ms, 10);
        assert_eq!(config.block_strategy_ms, 1.0);
        assert_eq!(config.discovery_poll_ms, 10);
        assert_eq!(config.consume_sleep_us, 1);
        assert_eq!(config.consumer_busy_wait_us, 10);
    }

    #[test]
    fn test_duration_conversions() {
        let config = SleepConfig::default();

        assert_eq!(config.shutdown_grace_duration(), Duration::from_millis(10));
        assert_eq!(config.discovery_poll_duration(), Duration::from_millis(10));
        assert_eq!(config.consume_sleep_duration(), Duration::from_micros(1));
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
