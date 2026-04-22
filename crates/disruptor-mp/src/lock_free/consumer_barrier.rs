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
        assert!(max_consumers > 0, "max_consumers must be greater than zero");
        DiscoveryMode::Enabled {
            max_consumers,
            consumer_prefix: None,
            scan_interval: Duration::from_millis(100),
        }
    }

    /// Create enabled discovery mode with consumer prefix and default scan interval.
    pub fn with_consumer_prefix(max_consumers: usize, prefix: String) -> Self {
        assert!(max_consumers > 0, "max_consumers must be greater than zero");
        DiscoveryMode::Enabled {
            max_consumers,
            consumer_prefix: Some(prefix),
            scan_interval: Duration::from_millis(100),
        }
    }

    /// Create enabled discovery mode with custom scan interval.
    pub fn with_scan_interval(max_consumers: usize, scan_interval: Duration) -> Self {
        assert!(
            !scan_interval.is_zero(),
            "scan_interval must be greater than zero"
        );
        assert!(max_consumers > 0, "max_consumers must be greater than zero");
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
        assert!(
            !scan_interval.is_zero(),
            "scan_interval must be greater than zero"
        );
        assert!(max_consumers > 0, "max_consumers must be greater than zero");
        DiscoveryMode::Enabled {
            max_consumers,
            consumer_prefix: Some(prefix),
            scan_interval,
        }
    }
}

const CONSUMER_READINESS_SUFFIX: &str = "_cr";
const CONSUMER_REGISTRATION_SUFFIX: &str = "_ci";
const AUTO_CONSUMER_PREFIX: &str = "ad";

pub(crate) fn consumer_readiness_cursor_name(base_name: &str) -> String {
    format!("{base_name}{CONSUMER_READINESS_SUFFIX}")
}

pub(crate) fn consumer_registration_cursor_name(base_name: &str) -> String {
    format!("{base_name}{CONSUMER_REGISTRATION_SUFFIX}")
}

pub(crate) fn auto_consumer_id(slot: usize) -> String {
    format!("{AUTO_CONSUMER_PREFIX}_{slot}")
}

fn uses_registered_auto_ids(discovery_mode: &DiscoveryMode) -> bool {
    matches!(
        discovery_mode,
        DiscoveryMode::Enabled {
            consumer_prefix: None,
            ..
        }
    )
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
    /// Consumer registration counter for deterministic auto IDs.
    consumer_registration: Option<SharedCursor>,
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
        assert!(!base_name.is_empty(), "base_name must not be empty");

        let mut barrier = Self {
            consumer_cursors: HashMap::new(),
            consumer_registration: None,
            base_name,
            last_scan: Instant::now(),
            consumers_ready: None,
            discovery_mode,
            discovery_completed: false,
            producer_sequence: None,
        };
        if !matches!(barrier.discovery_mode, DiscoveryMode::Disabled) {
            barrier.discover_consumers();
        }
        barrier
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
        assert!(!base_name.is_empty(), "base_name must not be empty");

        let consumers_ready_name = consumer_readiness_cursor_name(&base_name);
        let consumers_ready = Some(SharedCursor::new(&consumers_ready_name, 0)?);

        let mut barrier = Self {
            consumer_cursors: HashMap::new(),
            consumer_registration: None,
            base_name,
            last_scan: Instant::now(),
            consumers_ready,
            discovery_mode,
            discovery_completed: false,
            producer_sequence: None,
        };
        barrier.discover_consumers();
        Ok(barrier)
    }

    /// Set producer sequence reference for no-consumer fallback.
    pub fn set_producer_sequence(&mut self, producer_sequence: SharedCursor) {
        self.producer_sequence = Some(producer_sequence);
    }

    pub(crate) fn set_consumer_registration(&mut self, consumer_registration: SharedCursor) {
        self.consumer_registration = Some(consumer_registration);
    }

    /// Attach a known consumer cursor by explicit consumer id.
    ///
    /// This is required for restart tests and long-lived named consumers where the producer
    /// cannot infer the cursor name from auto-ID registration or PID-based discovery.
    pub fn discover_consumer_id(&mut self, consumer_id: &str) -> bool {
        if self.consumer_cursors.contains_key(consumer_id) {
            return true;
        }

        let sequence_name = format!("{}_{}_seq", self.base_name, consumer_id);
        match SharedCursor::attach(&sequence_name) {
            Ok(cursor) => {
                self.consumer_cursors
                    .insert(consumer_id.to_string(), cursor);
                true
            }
            Err(_) => false,
        }
    }

    /// Return the latest visible sequence for a known consumer id.
    pub fn consumer_sequence(&mut self, consumer_id: &str) -> Option<Sequence> {
        if !self.consumer_cursors.contains_key(consumer_id)
            && !self.discover_consumer_id(consumer_id)
        {
            return None;
        }
        self.consumer_cursors
            .get(consumer_id)
            .map(|cursor| cursor.load(Ordering::Acquire))
    }

    /// Access readiness counter used during startup coordination.
    pub fn get_consumer_readiness_counter(&self) -> Option<&SharedCursor> {
        self.consumers_ready.as_ref()
    }

    /// Wait for at least `min_consumers` to be ready.
    pub fn wait_for_consumers_ready(&self, min_consumers: i64, timeout: Duration) -> bool {
        assert!(timeout > Duration::ZERO, "timeout must be positive");
        assert!(min_consumers > 0, "min_consumers must be greater than zero");
        let min_consumers_usize =
            usize::try_from(min_consumers).expect("min_consumers conversion to usize");
        let min_consumers =
            i64::try_from(min_consumers_usize).expect("min_consumers conversion to i64");
        let start = Instant::now();

        if let Some(consumers_ready) = &self.consumers_ready {
            let coordination_strategy = self.coordination_timeout_for(min_consumers);
            return self.wait_for_coordination_minimum(
                consumers_ready,
                min_consumers,
                start,
                timeout,
                coordination_strategy,
            );
        }

        self.wait_for_discovery_minimum(min_consumers_usize, start, timeout)
    }

    fn coordination_timeout_for(&self, min_consumers: i64) -> Duration {
        match min_consumers {
            1 => Duration::from_millis(200),
            2 => Duration::from_millis(1200),
            3..=4 => Duration::from_millis(800),
            5..=8 => Duration::from_millis(400),
            _ => Duration::from_millis(100),
        }
    }

    fn wait_for_coordination_minimum(
        &self,
        consumers_ready: &SharedCursor,
        min_consumers: i64,
        start: Instant,
        timeout: Duration,
        coordination_timeout: Duration,
    ) -> bool {
        let effective_timeout = timeout.min(coordination_timeout);

        if self.spin_until_ready_count(consumers_ready, min_consumers, start, effective_timeout) {
            return true;
        }

        if effective_timeout < timeout {
            // After coordination timeout we still allow additional waiting within the caller
            // timeout so startup ordering can recover in slower environments.
            self.spin_until_ready_count(consumers_ready, min_consumers, start, timeout)
        } else {
            false
        }
    }

    fn spin_until_ready_count(
        &self,
        consumers_ready: &SharedCursor,
        min_consumers: i64,
        start: Instant,
        timeout: Duration,
    ) -> bool {
        while start.elapsed() < timeout {
            // Acquire load pairs with release increments from `signal_readiness`.
            let ready_count = consumers_ready.load(Ordering::Acquire);
            if ready_count >= min_consumers {
                return true;
            }
            // Keep a local acquire fence before spinning to prevent compiler
            // reordering around ready counter loads and keep observed state bounded.
            std::sync::atomic::fence(Ordering::Acquire);
            std::hint::spin_loop();
        }
        false
    }

    fn wait_for_discovery_minimum(
        &self,
        min_consumers_usize: usize,
        start: Instant,
        timeout: Duration,
    ) -> bool {
        while start.elapsed() < timeout {
            // Use a cloned barrier per attempt to avoid mutating caller-owned state
            // while probing for newly discovered consumers.
            let mut discovery_probe = self.clone();
            discovery_probe.discover_consumers();
            if discovery_probe.consumer_cursors.len() >= min_consumers_usize {
                return true;
            }
            std::thread::sleep(super::wait::SLEEP_CONFIG.discovery_poll_duration());
        }
        false
    }

    /// Discover and track new consumer sequences.
    pub fn discover_consumers(&mut self) {
        let now = Instant::now();
        let (should_scan, max_consumers, consumer_prefix) = self.discovery_scan_plan(now);

        if !should_scan {
            return;
        }

        self.last_scan = now;

        if let Some(prefix) = consumer_prefix {
            self.discover_with_consumer_prefix(&prefix);
        } else {
            let registered_slots = self.discover_with_registered_slots(max_consumers);
            if registered_slots == 0 || self.consumer_cursors.len() < registered_slots {
                self.discover_with_pid_based_scanning();
            }
        }

        if self.consumer_cursors.len() >= max_consumers {
            self.discovery_completed = true;
            println!(
                "Discovery completed: found all {} expected consumers",
                max_consumers
            );
        }
    }

    fn discovery_scan_plan(&self, now: Instant) -> (bool, usize, Option<String>) {
        match &self.discovery_mode {
            DiscoveryMode::Disabled => (false, 0, None),
            DiscoveryMode::Enabled {
                scan_interval,
                max_consumers,
                consumer_prefix,
            } => {
                if self.discovery_completed {
                    return (false, *max_consumers, None);
                }
                if self.consumer_cursors.len() >= *max_consumers {
                    return (false, *max_consumers, None);
                }

                let recently_scanned = now.duration_since(self.last_scan) < *scan_interval;
                let has_discovered_consumers = !self.consumer_cursors.is_empty();
                if recently_scanned && has_discovered_consumers {
                    return (false, *max_consumers, None);
                }

                (true, *max_consumers, consumer_prefix.clone())
            }
        }
    }

    /// Optimized discovery using name prefix conventions.
    fn discover_with_consumer_prefix(&mut self, prefix: &str) {
        #[cfg(feature = "dst")]
        if dst_fixtures::dst_buggify::buggify(file!(), line!()) {
            std::thread::sleep(Duration::from_millis(50));
        }

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
                #[cfg(feature = "dst")]
                dst_fixtures::dst_assertions::assert_sometimes(
                    true,
                    "consumer discovered",
                    format!("prefix={prefix} consumer={counter}"),
                );
            }
        }
    }

    /// Deterministic discovery for coordinated auto-generated consumer IDs.
    fn discover_with_registered_slots(&mut self, max_consumers: usize) -> usize {
        #[cfg(feature = "dst")]
        if dst_fixtures::dst_buggify::buggify(file!(), line!()) {
            std::thread::sleep(Duration::from_millis(50));
        }

        if self.consumer_registration.is_none() && uses_registered_auto_ids(&self.discovery_mode) {
            self.consumer_registration =
                SharedCursor::attach(&consumer_registration_cursor_name(&self.base_name)).ok();
        }

        let Some(consumer_registration) = &self.consumer_registration else {
            return 0;
        };

        let registered = consumer_registration.load(Ordering::Acquire);
        let registered = registered.clamp(0, max_consumers as i64) as usize;

        for slot in 0..registered {
            let consumer_name = auto_consumer_id(slot);
            let sequence_name = format!("{}_{}_seq", self.base_name, consumer_name);

            if self.consumer_cursors.contains_key(&consumer_name) {
                continue;
            }

            if let Ok(cursor) = SharedCursor::attach(&sequence_name) {
                self.consumer_cursors.insert(consumer_name, cursor);
                #[cfg(feature = "dst")]
                dst_fixtures::dst_assertions::assert_sometimes(
                    true,
                    "consumer discovered",
                    format!("registered-slot={slot}"),
                );
            }
        }

        registered
    }

    /// PID-based discovery fallback.
    fn discover_with_pid_based_scanning(&mut self) {
        #[cfg(feature = "dst")]
        if dst_fixtures::dst_buggify::buggify(file!(), line!()) {
            std::thread::sleep(Duration::from_millis(50));
        }

        let current_pid = std::process::id();

        let max_consumers = match &self.discovery_mode {
            DiscoveryMode::Enabled { max_consumers, .. } => (*max_consumers).clamp(32, 128),
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
                        #[cfg(feature = "dst")]
                        dst_fixtures::dst_assertions::assert_sometimes(
                            true,
                            "consumer discovered",
                            format!("pid={pid} counter={counter}"),
                        );
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

        let discovered_min = self.discovered_min_sequence();

        if let Some(min_sequence) = discovered_min {
            return min_sequence;
        }

        self.min_sequence_fallback()
    }

    fn discovered_min_sequence(&self) -> Option<Sequence> {
        let mut min_sequence = i64::MAX;

        for cursor in self.consumer_cursors.values() {
            // Acquire ensures we observe cursor progress that is visible after each
            // producer-consumer publication boundary.
            let sequence = cursor.load(Ordering::Acquire);

            if sequence >= -1 {
                min_sequence = std::cmp::min(min_sequence, sequence);
            }
        }

        if min_sequence == i64::MAX {
            None
        } else {
            Some(min_sequence)
        }
    }

    fn min_sequence_fallback(&self) -> Sequence {
        // In discovery mode, if no consumers are discovered yet, return conservative
        // floor to preserve backpressure rather than assuming producer is the sole
        // consumer (which would permit overwriting unread data).
        match &self.discovery_mode {
            DiscoveryMode::Enabled { .. } => -1,
            _ => {
                if let Some(ref producer_seq) = self.producer_sequence {
                    // Acquire load pairs with Release writes from producer
                    // to avoid reading stale sequence values during no-discovery mode.
                    producer_seq.load(Ordering::Acquire)
                } else {
                    -1
                }
            }
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
                SharedCursor::attach(&consumer_readiness_cursor_name(&self.base_name))
                    .unwrap_or_else(|_| cursor.clone())
            }),
            consumer_registration: self.consumer_registration.as_ref().map(|cursor| {
                SharedCursor::attach(&consumer_registration_cursor_name(&self.base_name))
                    .unwrap_or_else(|_| cursor.clone())
            }),
            discovery_mode: self.discovery_mode.clone(),
            discovery_completed: false,
            producer_sequence: self.producer_sequence.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    #[should_panic(expected = "max_consumers must be greater than zero")]
    fn discovery_mode_rejects_zero_max_consumers() {
        let _ = DiscoveryMode::enabled(0);
    }

    #[test]
    #[should_panic(expected = "max_consumers must be greater than zero")]
    fn discovery_mode_with_prefix_rejects_zero_max_consumers() {
        let _ = DiscoveryMode::with_consumer_prefix(0, "test".to_string());
    }

    #[test]
    #[should_panic(expected = "min_consumers must be greater than zero")]
    fn wait_for_consumers_ready_rejects_non_positive_min_consumers() {
        let barrier = SharedConsumerBarrier::new("test_barrier".to_string());
        let _ = barrier.wait_for_consumers_ready(0, Duration::from_millis(1));
    }

    #[test]
    #[should_panic(expected = "timeout must be positive")]
    fn wait_for_consumers_ready_rejects_zero_timeout() {
        let barrier = SharedConsumerBarrier::new_with_discovery(
            "test_barrier_timeout".to_string(),
            DiscoveryMode::enabled(1),
        );
        let _ = barrier.wait_for_consumers_ready(1, Duration::ZERO);
    }

    #[test]
    #[should_panic(expected = "base_name must not be empty")]
    fn new_with_discovery_rejects_empty_base_name() {
        let _ =
            SharedConsumerBarrier::new_with_discovery("".to_string(), DiscoveryMode::enabled(1));
    }

    #[test]
    #[should_panic(expected = "scan_interval must be greater than zero")]
    fn discovery_mode_with_zero_scan_interval_rejects() {
        let _ = DiscoveryMode::with_scan_interval(1, Duration::ZERO);
    }
}
