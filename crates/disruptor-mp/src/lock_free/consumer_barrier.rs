//! Lock-free consumer barrier and discovery for multiprocess coordination.
//!
//! These primitives track consumer progress and provide discovery/coordination
//! across processes without introducing locks in the hot path.

use crate::SharedCursor;
use disruptor_core::Sequence;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// Discovery mode for consumer detection.
#[derive(Debug, Clone, Default)]
pub enum DiscoveryMode {
    /// No consumer discovery - externally coordinated scenarios.
    #[default]
    Disabled,
    /// Automatic discovery with optimized scanning for fixed topologies.
    Enabled {
        /// Maximum number of consumers expected.
        max_consumers: usize,
        /// Optional consumer name prefix for targeted discovery.
        consumer_prefix: Option<String>,
        /// How often to scan for new consumers.
        scan_interval: Duration,
    },
}

impl DiscoveryMode {
    /// Create enabled discovery mode with default scan interval.
    pub fn enabled(max_consumers: usize) -> Self {
        DiscoveryMode::Enabled {
            max_consumers,
            consumer_prefix: None,
            scan_interval: Duration::from_millis(100),
        }
    }

    /// Create enabled discovery mode with consumer prefix and default scan interval.
    pub fn with_consumer_prefix(max_consumers: usize, prefix: String) -> Self {
        DiscoveryMode::Enabled {
            max_consumers,
            consumer_prefix: Some(prefix),
            scan_interval: Duration::from_millis(100),
        }
    }

    /// Create enabled discovery mode with custom scan interval.
    pub fn with_scan_interval(max_consumers: usize, scan_interval: Duration) -> Self {
        DiscoveryMode::Enabled {
            max_consumers,
            consumer_prefix: None,
            scan_interval,
        }
    }

    /// Create enabled discovery mode with consumer prefix and custom scan interval.
    pub fn with_consumer_prefix_and_interval(
        max_consumers: usize,
        prefix: String,
        scan_interval: Duration,
    ) -> Self {
        DiscoveryMode::Enabled {
            max_consumers,
            consumer_prefix: Some(prefix),
            scan_interval,
        }
    }
}

/// Barrier for tracking consumers in multiprocess shared-memory topologies.
pub struct SharedConsumerBarrier {
    /// Map of consumer ID to sequence cursor.
    consumer_cursors: HashMap<String, SharedCursor>,
    /// Base name for discovery.
    base_name: String,
    /// Last scan timestamp.
    last_scan: Instant,
    /// Consumer readiness counter for startup coordination.
    consumers_ready: Option<SharedCursor>,
    /// Discovery configuration.
    discovery_mode: DiscoveryMode,
    /// True when all expected consumers have been discovered.
    discovery_completed: bool,
    /// Producer sequence for no-consumer fallback behavior.
    producer_sequence: Option<SharedCursor>,
}

/// Concise alias for the shared consumer barrier type.
pub type ConsumerBarrier = SharedConsumerBarrier;

impl SharedConsumerBarrier {
    /// Create a barrier with default discovery mode.
    pub fn new(base_name: String) -> Self {
        Self::new_with_discovery(base_name, DiscoveryMode::default())
    }

    /// Create a barrier with explicit discovery mode.
    pub fn new_with_discovery(base_name: String, discovery_mode: DiscoveryMode) -> Self {
        Self {
            consumer_cursors: HashMap::new(),
            base_name,
            last_scan: Instant::now(),
            consumers_ready: None,
            discovery_mode,
            discovery_completed: false,
            producer_sequence: None,
        }
    }

    /// Create a barrier with internal readiness coordination.
    pub fn new_with_coordination(base_name: String) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_coordination_and_discovery(base_name, DiscoveryMode::default())
    }

    /// Create a barrier with coordination and explicit discovery mode.
    pub fn new_with_coordination_and_discovery(
        base_name: String,
        discovery_mode: DiscoveryMode,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let consumers_ready_name = format!("{}_cr", base_name);
        let consumers_ready = Some(SharedCursor::new(&consumers_ready_name, 0)?);

        Ok(Self {
            consumer_cursors: HashMap::new(),
            base_name,
            last_scan: Instant::now(),
            consumers_ready,
            discovery_mode,
            discovery_completed: false,
            producer_sequence: None,
        })
    }

    /// Set producer sequence reference for no-consumer fallback.
    pub fn set_producer_sequence(&mut self, producer_sequence: SharedCursor) {
        self.producer_sequence = Some(producer_sequence);
    }

    /// Access readiness counter used during startup coordination.
    pub fn get_consumer_readiness_counter(&self) -> Option<&SharedCursor> {
        self.consumers_ready.as_ref()
    }

    /// Wait for at least `min_consumers` to be ready.
    pub fn wait_for_consumers_ready(&self, min_consumers: i64, timeout: Duration) -> bool {
        if let Some(consumers_ready) = &self.consumers_ready {
            let start = Instant::now();

            let coordination_strategy = match min_consumers {
                1 => Duration::from_millis(200),
                2 => Duration::from_millis(1200),
                3..=4 => Duration::from_millis(800),
                5..=8 => Duration::from_millis(400),
                _ => Duration::from_millis(100),
            };

            let effective_timeout = timeout.min(coordination_strategy);

            while start.elapsed() < effective_timeout {
                let ready_count = consumers_ready.load(Ordering::Acquire);
                if ready_count >= min_consumers {
                    return true;
                }
                std::sync::atomic::fence(Ordering::Acquire);
                std::hint::spin_loop();
            }

            if effective_timeout < timeout {
                while start.elapsed() < timeout {
                    let ready_count = consumers_ready.load(Ordering::Acquire);
                    if ready_count >= min_consumers {
                        return true;
                    }
                    std::hint::spin_loop();
                }
            }

            false
        } else {
            let start = Instant::now();
            while start.elapsed() < timeout {
                let mut barrier = self.clone();
                barrier.discover_consumers();
                if barrier.consumer_cursors.len() >= min_consumers as usize {
                    return true;
                }
                std::thread::sleep(super::wait::SLEEP_CONFIG.discovery_poll_duration());
            }
            false
        }
    }

    /// Discover and track new consumer sequences.
    pub fn discover_consumers(&mut self) {
        let now = Instant::now();

        let (should_scan, max_consumers, consumer_prefix) = match &self.discovery_mode {
            DiscoveryMode::Disabled => (false, 0, None),
            DiscoveryMode::Enabled {
                scan_interval,
                max_consumers,
                consumer_prefix,
            } => {
                if now.duration_since(self.last_scan) < *scan_interval {
                    return;
                }

                if self.discovery_completed || self.consumer_cursors.len() >= *max_consumers {
                    self.discovery_completed = true;
                    return;
                }

                (true, *max_consumers, consumer_prefix.clone())
            }
        };

        if !should_scan {
            return;
        }

        self.last_scan = now;

        if let Some(prefix) = consumer_prefix {
            self.discover_with_consumer_prefix(&prefix);
        } else {
            self.discover_with_pid_based_scanning();
        }

        if self.consumer_cursors.len() >= max_consumers {
            self.discovery_completed = true;
            println!(
                "Discovery completed: found all {} expected consumers",
                max_consumers
            );
        }
    }

    /// Optimized discovery using name prefix conventions.
    fn discover_with_consumer_prefix(&mut self, prefix: &str) {
        let max_consumers = match &self.discovery_mode {
            DiscoveryMode::Enabled { max_consumers, .. } => *max_consumers,
            _ => 16,
        };

        for counter in 0..max_consumers {
            let consumer_name = format!("{}_{}", prefix, counter);
            let sequence_name = format!("{}_{}_seq", self.base_name, consumer_name);

            if self.consumer_cursors.contains_key(&consumer_name) {
                continue;
            }

            if let Ok(cursor) = SharedCursor::attach(&sequence_name) {
                self.consumer_cursors.insert(consumer_name, cursor);
            }
        }
    }

    /// PID-based discovery fallback.
    fn discover_with_pid_based_scanning(&mut self) {
        let current_pid = std::process::id();

        let max_consumers = match &self.discovery_mode {
            DiscoveryMode::Enabled { max_consumers, .. } => (*max_consumers).min(8),
            _ => 8,
        };

        let pid_ranges = [(current_pid.saturating_sub(20), current_pid + 20)];

        for (start_pid, end_pid) in pid_ranges {
            for pid in start_pid..=end_pid {
                for counter in 0..max_consumers {
                    let consumer_name = format!("c{}_{}", pid % 10000, counter);
                    let sequence_name = format!("{}_{}_seq", self.base_name, consumer_name);

                    if self.consumer_cursors.contains_key(&consumer_name) {
                        continue;
                    }

                    if let Ok(cursor) = SharedCursor::attach(&sequence_name) {
                        self.consumer_cursors.insert(consumer_name, cursor);
                    }
                }
            }
        }
    }

    /// Return minimum sequence across discovered consumers.
    pub fn get_min_consumer_sequence(&mut self) -> Sequence {
        match &self.discovery_mode {
            DiscoveryMode::Disabled => {}
            DiscoveryMode::Enabled { .. } => {
                if !self.discovery_completed {
                    self.discover_consumers();
                }
            }
        }

        let mut min_sequence = i64::MAX;

        for cursor in self.consumer_cursors.values() {
            let sequence = cursor.load(Ordering::Acquire);

            if sequence >= -1 {
                min_sequence = std::cmp::min(min_sequence, sequence);
            }
        }

        if min_sequence == i64::MAX {
            if let Some(ref producer_seq) = self.producer_sequence {
                producer_seq.load(Ordering::Acquire)
            } else {
                -1
            }
        } else {
            min_sequence
        }
    }
}

impl Clone for SharedConsumerBarrier {
    fn clone(&self) -> Self {
        Self {
            consumer_cursors: HashMap::new(),
            base_name: self.base_name.clone(),
            last_scan: Instant::now(),
            consumers_ready: self.consumers_ready.as_ref().map(|cursor| {
                SharedCursor::attach(&format!("{}_cr", self.base_name))
                    .unwrap_or_else(|_| cursor.clone())
            }),
            discovery_mode: self.discovery_mode.clone(),
            discovery_completed: false,
            producer_sequence: self.producer_sequence.clone(),
        }
    }
}
