//! Builder for multi-process disruptor.
//!
//! This module provides the [`SharedDisruptorBuilder`] for constructing multi-process
//! disruptors with various coordination, discovery, and event handling options.
//! It supports both manual consumer management and automatic event handlers with
//! built-in resource management.
//!
//! # Key Features
//!
//! - **Automatic Event Handlers**: Background thread processing via `handle_events_with()`
//! - **Consumer Discovery**: Automatic detection of consumers using PID or prefix matching
//! - **Coordination Modes**: Immediate, wait-for-consumers, and discovery-based startup
//! - **Resource Management**: Automatic cleanup via `Drop` trait implementation
//! - **Wait Strategies**: Configurable performance vs CPU usage trade-offs
//!
//! # Usage Patterns
//!
//! ## Automatic Event Processing (Recommended)
//!
//! ```rust,no_run
//! use disruptor_mp::*;
//!
//! #[derive(Copy, Clone, Default)]
//! struct Event { data: i64 }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Producer with automatic coordination
//! let mut producer = build_shared_single_producer::<Event>("my_ring", 1024)
//!     .enable_discovery(1)  // Discover 1 consumer automatically
//!     .build_producer(Event::default)?;
//!
//! // Consumer with automatic event handling
//! let _consumer = attach_shared_consumer::<Event>("my_ring", 1024)
//!     .handle_events_with(|event, sequence, end_of_batch| {
//!         // Process events automatically in background thread
//!         println!("Processing event: {}", event.data);
//!     })?;
//! // Consumer automatically cleans up when dropped
//! # Ok(())
//! # }
//! ```
//!
//! ## Manual Consumer Management
//!
//! ```rust,no_run
//! use disruptor_mp::*;
//!
//! #[derive(Copy, Clone, Default)]
//! struct Event { data: i64 }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Manual consumer for custom polling patterns
//! let mut consumer = attach_shared_consumer::<Event>("my_ring", 1024)
//!     .build_consumer()?;
//!
//! // Custom polling loop
//! loop {
//!     let processed = consumer.process_available(|event, sequence| {
//!         println!("Processing: {}", event.data);
//!     });
//!     if processed == 0 {
//!         std::thread::sleep(std::time::Duration::from_millis(1));
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use super::consumer::SharedConsumer;
use super::consumer_barrier::{auto_consumer_id, consumer_registration_cursor_name, DiscoveryMode};
use super::producer::{CoordinationMode, SharedProducer};
use crate::{MultiProcessResult, SharedCursor, SharedMemoryConfig, SharedRingBuffer};
use disruptor_core::Sequence;
use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Global counter for generating unique consumer IDs within a process
static CONSUMER_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn uses_registered_auto_ids(discovery_mode: &DiscoveryMode) -> bool {
    matches!(
        discovery_mode,
        DiscoveryMode::Enabled {
            consumer_prefix: None,
            ..
        }
    )
}

fn create_consumer_registration_cursor(
    base_name: &str,
    discovery_mode: &DiscoveryMode,
) -> MultiProcessResult<Option<SharedCursor>> {
    if !uses_registered_auto_ids(discovery_mode) {
        return Ok(None);
    }

    let registration_name = consumer_registration_cursor_name(base_name);
    let cursor = SharedCursor::new_or_attach(&registration_name, 0)?;
    Ok(Some(cursor))
}

fn allocate_registered_consumer_id(base_name: &str) -> Option<String> {
    let registration_name = consumer_registration_cursor_name(base_name);
    let registration = SharedCursor::attach(&registration_name).ok()?;
    let slot = registration.fetch_add(1, Ordering::AcqRel);
    if slot < 0 {
        return None;
    }

    Some(auto_consumer_id(slot as usize))
}

fn default_consumer_id(base_name: &str) -> String {
    if let Some(consumer_id) = allocate_registered_consumer_id(base_name) {
        return consumer_id;
    }

    let process_id = std::process::id();
    let consumer_counter = CONSUMER_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("c{}_{}", process_id % 10000, consumer_counter)
}

/// Wait strategy for automatic event handlers
#[derive(Debug, Clone, Default)]
pub enum AutoWaitStrategy {
    /// Maximum performance busy spinning (100% CPU usage) - true busy spin, no hints
    BusySpin,
    /// High performance busy spinning with spin loop hints (slightly lower CPU usage)
    BusySpinWithSpinLoopHint,
    /// Hybrid: spin N iterations, then yield the thread
    SpinThenYield {
        /// Number of spin-loop iterations before yielding once.
        spins: usize,
    },
    /// CPU efficient with configurable sleep duration
    Sleep(Duration),
    /// Blocking with efficient waiting (balanced performance/CPU)
    #[default]
    Block,
}

impl AutoWaitStrategy {
    /// Create a high performance wait strategy (true busy spin)
    pub fn high_performance() -> Self {
        AutoWaitStrategy::BusySpin
    }

    /// Create a high performance wait strategy with spin loop hints
    pub fn high_performance_with_hints() -> Self {
        AutoWaitStrategy::BusySpinWithSpinLoopHint
    }

    /// Create a hybrid strategy that spins N times then yields
    pub fn spin_then_yield(spins: usize) -> Self {
        AutoWaitStrategy::SpinThenYield { spins }
    }

    /// Create a CPU efficient wait strategy
    pub fn cpu_efficient() -> Self {
        AutoWaitStrategy::Sleep(Duration::from_micros(1))
    }

    /// Create a custom sleep-based wait strategy
    pub fn sleep(duration: Duration) -> Self {
        AutoWaitStrategy::Sleep(duration)
    }

    /// Create a sleep-based wait strategy with nanosecond precision
    ///
    /// # Special Values
    /// - `0` = Use spin_loop() instead of sleep (high performance)
    /// - `1..` = Sleep for the specified nanoseconds (lower performance, CPU efficient)
    ///
    /// Note: Actual sleep precision depends on the operating system.
    /// Very small durations (< 1000ns) may not sleep at all on some systems.
    pub fn sleep_nanos(nanos: u64) -> Self {
        if nanos == 0 {
            AutoWaitStrategy::BusySpinWithSpinLoopHint
        } else {
            AutoWaitStrategy::Sleep(Duration::from_nanos(nanos))
        }
    }

    /// Create a sleep-based wait strategy with microsecond precision
    ///
    /// # Special Values
    /// - `0` = Use spin_loop() instead of sleep (high performance)
    /// - `1..` = Sleep for the specified microseconds (lower performance, CPU efficient)
    pub fn sleep_micros(micros: u64) -> Self {
        if micros == 0 {
            AutoWaitStrategy::BusySpinWithSpinLoopHint
        } else {
            AutoWaitStrategy::Sleep(Duration::from_micros(micros))
        }
    }

    /// Create wait strategy from environment variables with fallback
    ///
    /// Checks these environment variables in order:
    /// 1. `AUTO_WAIT_DELAY_NS` - nanosecond precision (0 = spin_loop)
    /// 2. `AUTO_WAIT_DELAY_US` - microsecond precision (0 = spin_loop)
    /// 3. Falls back to the provided default
    ///
    /// # Examples
    /// ```bash
    /// # Use spin_loop (maximum performance)
    /// export AUTO_WAIT_DELAY_NS=0
    ///
    /// # Sleep for 100 nanoseconds
    /// export AUTO_WAIT_DELAY_NS=100
    ///
    /// # Sleep for 10 microseconds
    /// export AUTO_WAIT_DELAY_US=10
    /// ```
    pub fn from_env_or(default: AutoWaitStrategy) -> Self {
        // Check for nanosecond precision first
        if let Ok(nanos_str) = std::env::var("AUTO_WAIT_DELAY_NS") {
            if let Ok(nanos) = nanos_str.parse::<u64>() {
                return Self::sleep_nanos(nanos);
            }
        }

        // Check for microsecond precision
        if let Ok(micros_str) = std::env::var("AUTO_WAIT_DELAY_US") {
            if let Ok(micros) = micros_str.parse::<u64>() {
                return Self::sleep_micros(micros);
            }
        }

        // Fall back to default
        default
    }
}

/// Automatic consumer that runs in a background thread.
///
/// This provides a handle to a consumer running automatic event processing
/// in a separate thread. It supports graceful shutdown and cleanup operations.
pub struct AutoConsumer {
    join_handle: Option<JoinHandle<()>>,
    shutdown_signal: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl AutoConsumer {
    fn new(
        join_handle: JoinHandle<()>,
        shutdown_signal: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self {
            join_handle: Some(join_handle),
            shutdown_signal,
        }
    }

    /// Signal the consumer to shutdown
    pub fn shutdown(&self) {
        self.shutdown_signal
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Wait for the consumer thread to finish
    pub fn join(&mut self) {
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
    }

    /// Shutdown and wait for the consumer thread to finish
    pub fn shutdown_and_join(&mut self) {
        self.shutdown();
        self.join();
    }

    /// Check if the consumer thread is still running
    pub fn is_running(&self) -> bool {
        self.join_handle.is_some()
            && !self
                .shutdown_signal
                .load(std::sync::atomic::Ordering::Acquire)
    }
}

/// Automatic resource management for AutoConsumer
///
/// This ensures proper cleanup when the consumer goes out of scope,
/// which is essential for Competitor integration where Python's garbage
/// collection expects automatic resource management.
impl Drop for AutoConsumer {
    fn drop(&mut self) {
        // Signal shutdown and wait for thread to finish
        self.shutdown_signal
            .store(true, std::sync::atomic::Ordering::Release);

        if let Some(handle) = self.join_handle.take() {
            // Give the thread a moment to see the shutdown signal
            std::thread::sleep(super::wait::SLEEP_CONFIG.shutdown_grace_duration());

            // Wait for clean shutdown (with timeout for safety)
            match handle.join() {
                Ok(_) => {
                    // Clean shutdown successful
                }
                Err(_) => {
                    // Thread panicked - this is unexpected but not fatal
                    eprintln!(
                        "Warning: AutoConsumer thread terminated unexpectedly during cleanup"
                    );
                }
            }
        }
    }
}

/// Builder for creating multi-process disruptors.
///
/// This builder provides a fluent API for configuring and creating shared memory
/// disruptors with various coordination modes, discovery options, and event handling
/// strategies. It supports both producer and consumer creation with automatic
/// shared memory management.
///
/// ## Key Features for Competitor Integration
///
/// - **Automatic Event Delivery**: `handle_events_with()` creates background processing
/// - **Automatic Resource Management**: Proper cleanup via `Drop` implementations
/// - **Configurable Timeouts**: Customizable coordination and discovery timeouts
/// - **Error Handling**: Clear error messages and robust failure handling
///
/// ## Usage Patterns
///
/// ```rust,no_run
/// use disruptor_mp::*;
/// use std::time::Duration;
///
/// #[derive(Copy, Clone, Default)]
/// struct Event { data: i64 }
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Producer with automatic coordination
/// let mut producer = build_shared_single_producer::<Event>("test", 1024)
///     .enable_discovery(2)
///     .with_coordination_timeout(Duration::from_secs(30))
///     .build_producer(Event::default)?;
///
/// // Consumer with automatic event delivery  
/// let consumer = attach_shared_consumer::<Event>("test", 1024)
///     .handle_events_with(|event, seq, eob| {
///         // Events delivered automatically
///     })?;
/// # Ok(())
/// # }
/// ```
pub struct SharedDisruptorBuilder<E> {
    config: SharedMemoryConfig,
    coordination_mode: Option<CoordinationMode>,
    discovery_mode: Option<DiscoveryMode>,
    consumer_id: Option<String>,
    process_core: Option<usize>,
    consumer_core: Option<usize>,
    coordination_timeout: Option<Duration>,
    _phantom: std::marker::PhantomData<E>,
}

#[derive(Copy, Clone)]
enum ProcessRole {
    Producer,
    Consumer,
}

impl ProcessRole {
    fn env_var(self) -> &'static str {
        match self {
            ProcessRole::Producer => "DISRUPTOR_MP_PRODUCER_CORE",
            ProcessRole::Consumer => "DISRUPTOR_MP_CONSUMER_CORE",
        }
    }

    fn label(self) -> &'static str {
        match self {
            ProcessRole::Producer => "producer process",
            ProcessRole::Consumer => "consumer process",
        }
    }
}

impl<E> SharedDisruptorBuilder<E>
where
    E: Copy + Default + 'static,
{
    /// Create a new builder with the given configuration
    pub fn new(config: SharedMemoryConfig) -> Self {
        Self {
            config,
            coordination_mode: None,
            discovery_mode: None,
            consumer_id: None,
            process_core: None,
            consumer_core: None,
            coordination_timeout: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Set a custom coordination timeout
    ///
    /// This overrides the default adaptive timeout behavior for consumer coordination.
    /// Useful for environments with specific timing requirements.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use std::time::Duration;
    /// let builder = builder.with_coordination_timeout(Duration::from_secs(60));
    /// ```
    pub fn with_coordination_timeout(mut self, timeout: Duration) -> Self {
        self.coordination_timeout = Some(timeout);
        self
    }

    /// Set the coordination mode for the producer
    ///
    /// This enables internal coordination to eliminate the need for external coordination structures.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use std::time::Duration;
    /// use disruptor_mp::producer::CoordinationMode;
    ///
    /// // Wait for single consumer with 30 second timeout
    /// let builder = builder.with_coordination(
    ///     CoordinationMode::wait_for_single_consumer(Duration::from_secs(30))
    /// );
    ///
    /// // Wait for multiple consumers with 45 second timeout
    /// let builder = builder.with_coordination(
    ///     CoordinationMode::wait_for_consumers(3, Duration::from_secs(45))
    /// );
    /// ```
    pub fn with_coordination(mut self, coordination_mode: CoordinationMode) -> Self {
        self.coordination_mode = Some(coordination_mode);
        self
    }

    /// Configure consumer discovery mode
    ///
    /// # Discovery Modes (Default: Disabled)
    /// - `DiscoveryMode::Disabled` - No discovery (default), maximum performance for externally coordinated scenarios
    /// - `DiscoveryMode::Enabled { consumer_prefix: None }` - Deterministic slot-based discovery
    ///   when coordination is present, with PID scanning as legacy fallback
    /// - `DiscoveryMode::Enabled { consumer_prefix: Some("DIS_SM") }` - Optimized prefix-based discovery
    ///
    /// # Examples
    /// ```rust,ignore
    /// use disruptor_mp::lock_free::DiscoveryMode;
    ///
    /// // Enable basic discovery for 2 consumers (fixed topology)
    /// let builder = builder.enable_discovery(2);
    ///
    /// // Use consumer naming convention for efficient discovery of 5 consumers
    /// let builder = builder.discover_consumer_with_prefix(5, "DIS_SM");
    /// ```
    pub fn with_discovery(mut self, discovery_mode: DiscoveryMode) -> Self {
        self.discovery_mode = Some(discovery_mode);
        self
    }

    /// Disable consumer discovery (default behavior)
    ///
    /// This is ideal for externally coordinated scenarios like benchmarks
    /// where consumer coordination is handled outside the disruptor.
    /// Discovery is disabled by default for maximum performance.
    pub fn disable_discovery(self) -> Self {
        self.with_discovery(DiscoveryMode::Disabled)
    }

    /// Enable basic consumer discovery for fixed topology (convenience method)
    ///
    /// When startup coordination is present, consumers are assigned deterministic
    /// slot-based IDs (`ad_0`, `ad_1`, ...) so discovery does not rely on PID
    /// proximity. Without coordination, discovery falls back to PID scanning with
    /// the legacy naming pattern `c{pid}_{counter}`.
    /// Stops scanning once the expected number of consumers are discovered.
    /// Default scan interval: 100ms.
    pub fn enable_discovery(self, max_consumers: usize) -> Self {
        self.with_discovery(DiscoveryMode::enabled(max_consumers))
    }

    /// Enable optimized discovery with consumer naming convention for fixed topology (convenience method)
    ///
    /// This uses consumer name prefixes for much faster consumer discovery
    /// compared to PID scanning. Consumers will be discovered using names like: "DIS_SM_1", "DIS_SM_2", etc.
    /// The prefix identifies the consumer group/application, not the OS process name.
    /// Stops scanning once the expected number of consumers are discovered, saving CPU cycles.
    /// Default scan interval: 100ms.
    pub fn discover_consumer_with_prefix(self, max_consumers: usize, prefix: &str) -> Self {
        self.with_discovery(DiscoveryMode::with_consumer_prefix(
            max_consumers,
            prefix.to_string(),
        ))
    }

    /// Enable discovery with custom scan interval for fixed topology (convenience method)
    ///
    /// This configures how often the producer scans for new consumers.
    /// Stops scanning once the expected number of consumers are discovered, saving CPU cycles.
    /// Shorter intervals provide faster consumer detection but use more CPU.
    /// Longer intervals reduce CPU usage but may delay consumer detection.
    pub fn with_discovery_interval(self, max_consumers: usize, scan_interval: Duration) -> Self {
        self.with_discovery(DiscoveryMode::with_scan_interval(
            max_consumers,
            scan_interval,
        ))
    }

    /// Enable discovery with consumer prefix and custom scan interval for fixed topology (convenience method)
    ///
    /// Combines optimized consumer prefix discovery with configurable scan timing.
    /// Stops scanning once the expected number of consumers are discovered, saving CPU cycles.
    pub fn discover_consumer_with_prefix_and_interval(
        self,
        max_consumers: usize,
        prefix: &str,
        scan_interval: Duration,
    ) -> Self {
        self.with_discovery(DiscoveryMode::with_consumer_prefix_and_interval(
            max_consumers,
            prefix.to_string(),
            scan_interval,
        ))
    }

    /// Enable coordination for single consumer scenarios (convenience method)
    ///
    /// This is equivalent to:
    /// ```rust,ignore
    /// use std::time::Duration;
    /// use disruptor_mp::producer::CoordinationMode;
    /// builder.with_coordination(CoordinationMode::wait_for_single_consumer(timeout))
    /// ```
    pub fn wait_for_single_consumer(self, timeout: Duration) -> Self {
        self.with_coordination(CoordinationMode::wait_for_single_consumer(timeout))
    }

    /// Enable coordination for multiple consumer scenarios (convenience method)
    pub fn wait_for_consumers(self, min_consumers: i64, timeout: Duration) -> Self {
        self.with_coordination(CoordinationMode::wait_for_consumers(min_consumers, timeout))
    }

    /// Set a custom consumer ID for this consumer instance
    ///
    /// This is useful for prefix-based consumer discovery where consumers need
    /// specific naming patterns. If not set, a default ID will be generated
    /// using the pattern "c{pid}_{counter}".
    ///
    /// # Example
    /// ```rust,ignore
    /// let consumer = builder
    ///     .with_consumer_id("TEST_CONSUMER_1")
    ///     .build_consumer()?;
    /// ```
    pub fn with_consumer_id(mut self, consumer_id: &str) -> Self {
        self.consumer_id = Some(consumer_id.to_string());
        self
    }

    /// Bind automatic consumer thread to a specific CPU core.
    ///
    /// Uses Linux core affinity on Linux builds. On macOS and other non-Linux
    /// platforms this configuration is accepted, ignored, and logged as a
    /// best-effort no-op so consumer bring-up still succeeds.
    ///
    /// The builder resolves the effective affinity order:
    /// 1. Explicit builder value.
    /// 2. `DISRUPTOR_MP_AUTO_CONSUMER_CORE` environment variable (if set).
    pub fn with_consumer_core(mut self, core_id: usize) -> Self {
        self.consumer_core = Some(core_id);
        self
    }

    /// Bind this process (producer or consumer) to a specific CPU core.
    ///
    /// On Linux this pins the current thread before ring setup. For the normal
    /// single-threaded producer/consumer entrypoints this acts as process-role
    /// affinity, and newly spawned auto-consumer threads inherit the mask unless
    /// they are pinned separately with `with_consumer_core()`. On macOS and
    /// other non-Linux platforms this request is accepted, ignored, and logged
    /// as a best-effort no-op so producer/consumer bring-up still succeeds.
    ///
    /// Resolution order:
    /// 1. Explicit builder value via `with_process_core()`
    /// 2. Role specific env var (`DISRUPTOR_MP_PRODUCER_CORE`
    ///    or `DISRUPTOR_MP_CONSUMER_CORE`)
    /// 3. Generic env var `DISRUPTOR_MP_PROCESS_CORE`
    pub fn with_process_core(mut self, core_id: usize) -> Self {
        self.process_core = Some(core_id);
        self
    }

    fn maybe_pin_process_to_core(&self, role: ProcessRole) {
        if let Some(core_id) = resolve_process_core(self.process_core, role) {
            pin_current_thread_to_core(core_id, role.label());
        }
    }

    /// Build a consumer with automatic batch event handling
    ///
    /// This provides automatic event delivery with configurable wait strategies.
    /// Events are processed in batches for optimal performance.
    ///
    /// # Wait Strategies
    /// - `AutoWaitStrategy::BusySpin` - Maximum performance (100% CPU, true busy spin)
    /// - `AutoWaitStrategy::BusySpinWithSpinLoopHint` - High performance with CPU hints
    /// - `AutoWaitStrategy::Block` - Balanced performance/CPU (default)
    /// - `AutoWaitStrategy::Sleep(duration)` - CPU efficient with custom sleep
    ///
    /// # Examples
    /// ```rust,ignore
    /// use std::time::Duration;
    /// use disruptor_mp::builder::AutoWaitStrategy;
    ///
    /// // Maximum performance for AI inference (true busy spin)
    /// consumer.handle_events_batch(handler, AutoWaitStrategy::high_performance())
    ///
    /// // High performance with CPU hints (slightly more efficient)
    /// consumer.handle_events_batch(handler, AutoWaitStrategy::high_performance_with_hints())
    ///
    /// // CPU efficient
    /// consumer.handle_events_batch(handler, AutoWaitStrategy::cpu_efficient())
    ///
    /// // Custom sleep duration
    /// consumer.handle_events_batch(handler, AutoWaitStrategy::sleep(Duration::from_micros(10)))
    ///
    /// // Balanced (default)
    /// consumer.handle_events_batch(handler, AutoWaitStrategy::default())
    /// ```
    pub fn handle_events_batch<EH>(
        self,
        mut event_handler: EH,
        wait_strategy: AutoWaitStrategy,
    ) -> MultiProcessResult<AutoConsumer>
    where
        EH: 'static + Send + FnMut(&E, Sequence, bool),
    {
        self.maybe_pin_process_to_core(ProcessRole::Consumer);

        let ring_buffer: SharedRingBuffer<E> = SharedRingBuffer::attach(self.config.clone())?;

        // Attach to existing shared atomics
        let producer_sequence_name = format!("{}_producer_seq", self.config.name);
        let producer_sequence = SharedCursor::attach(&producer_sequence_name)?;

        // Use custom consumer ID if provided, otherwise generate a deterministic
        // slot-based ID when coordinated discovery is available.
        let consumer_id = if let Some(custom_id) = self.consumer_id {
            custom_id
        } else {
            default_consumer_id(&self.config.name)
        };

        // Create this consumer's own sequence tracker
        let consumer_sequence_name = format!("{}_{}_seq", self.config.name, consumer_id);
        let mut consumer_sequence = SharedCursor::new_or_attach(&consumer_sequence_name, -1)?;
        if consumer_sequence.is_owner() {
            consumer_sequence.set_owner(false);
        }

        // Create the consumer with coordination support
        let mut consumer = SharedConsumer::new_with_coordination(
            ring_buffer,
            producer_sequence,
            consumer_sequence,
            consumer_id,
            Some(self.config.name.clone()),
        );

        // Create shutdown signal
        let shutdown_signal = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let shutdown_signal_clone = std::sync::Arc::clone(&shutdown_signal);
        let consumer_core = resolve_auto_consumer_core(self.consumer_core);
        let thread_name = format!("multiprocess-consumer-{}", std::process::id());

        // Spawn background thread for automatic event processing
        let join_handle = thread::Builder::new()
            .name(thread_name.clone())
            .spawn(move || {
                if let Some(core_id) = consumer_core {
                    pin_current_thread_to_core(core_id, &thread_name);
                }

                loop {
                    let processed = match wait_strategy {
                        AutoWaitStrategy::BusySpin => {
                            // Maximum performance: True busy spin
                            // TRUE BUSY SPIN: Do nothing when no events (like single-process BusySpin)
                            // This achieves maximum throughput at 100% CPU usage
                            consumer.process_available(|event, seq| {
                                // Approximate end_of_batch as false for maximum performance
                                event_handler(event, seq, false);
                            })
                        }
                        AutoWaitStrategy::BusySpinWithSpinLoopHint => {
                            // High performance: Busy spin with hints
                            let processed = consumer.process_available(|event, seq| {
                                // Approximate end_of_batch as false for maximum performance
                                event_handler(event, seq, false);
                            });

                            if processed == 0 {
                                std::hint::spin_loop();
                            }
                            processed
                        }
                        AutoWaitStrategy::SpinThenYield { spins } => {
                            // Hybrid: spin N times then yield to reduce tail latencies
                            let processed = consumer.process_available(|event, seq| {
                                event_handler(event, seq, false);
                            });

                            if processed == 0 {
                                // Spin N iterations using spin_loop hint
                                for _ in 0..spins {
                                    std::hint::spin_loop();
                                }
                                // Then yield the thread to reduce scheduler starvation
                                std::thread::yield_now();
                            }
                            processed
                        }
                        AutoWaitStrategy::Block => {
                            // ⚖️ BALANCED: Batch processing with non-blocking check first
                            // Check if events are immediately available
                            let processed = consumer.process_available(|event, seq| {
                                // Approximate end_of_batch as false for simplicity
                                event_handler(event, seq, false);
                            });

                            if processed == 0 {
                                // No events available, wait a bit before trying again
                                // This prevents the infinite spin in consume_next()
                                std::thread::sleep(
                                    super::wait::SLEEP_CONFIG.block_strategy_duration(),
                                );
                            }

                            processed
                        }
                        AutoWaitStrategy::Sleep(duration) => {
                            // CPU efficient: Batch processing with configurable sleep
                            let processed = consumer.process_available(|event, seq| {
                                // Approximate end_of_batch as false for simplicity
                                event_handler(event, seq, false);
                            });

                            if processed == 0 {
                                std::thread::sleep(duration);
                            }
                            processed
                        }
                    };

                    // Continue processing - the wait strategy handles timing

                    // Performance optimization: Only check shutdown when idle (no events processed)
                    // This avoids atomic loads in the hot path when processing events
                    if processed == 0
                        && shutdown_signal_clone.load(std::sync::atomic::Ordering::Acquire)
                    {
                        break;
                    }
                }
            })
            .expect("Should spawn consumer thread");

        Ok(AutoConsumer::new(join_handle, shutdown_signal))
    }

    /// Build a consumer with automatic event handling (convenience method)
    ///
    /// Uses the default balanced wait strategy (Block).
    /// For performance tuning, use `handle_events_batch()` with explicit wait strategy.
    pub fn handle_events_with<EH>(self, event_handler: EH) -> MultiProcessResult<AutoConsumer>
    where
        EH: 'static + Send + FnMut(&E, Sequence, bool),
    {
        self.handle_events_batch(event_handler, AutoWaitStrategy::default())
    }

    /// Build a producer (creates shared memory segments)
    ///
    /// Note: Only one producer can exist per shared memory segment.
    /// For multi-process scenarios, use Single Producer Multiple Consumer (SPMC) pattern.
    pub fn build_producer<F>(self, event_factory: F) -> MultiProcessResult<SharedProducer<E>>
    where
        F: FnMut() -> E,
    {
        #[cfg(feature = "dst")]
        if dst_fixtures::dst_buggify::buggify(file!(), line!()) {
            std::thread::sleep(Duration::from_millis(100));
        }

        self.maybe_pin_process_to_core(ProcessRole::Producer);

        let discovery_mode = self.discovery_mode.unwrap_or_default();
        let consumer_registration =
            create_consumer_registration_cursor(&self.config.name, &discovery_mode)?;

        let ring_buffer: SharedRingBuffer<E> =
            SharedRingBuffer::new(self.config.clone(), event_factory)?;

        // Create shared atomic for producer sequence
        let producer_sequence_name = format!("{}_producer_seq", self.config.name);
        let producer_sequence = SharedCursor::new(&producer_sequence_name, -1)?;

        // Automatic coordination when discovery is enabled
        // This eliminates the need for users to manually configure coordination
        let coordination_mode = self.coordination_mode.unwrap_or_else(|| {
            // Auto-enable coordination when discovery is enabled
            match &discovery_mode {
                DiscoveryMode::Enabled { max_consumers, .. } => {
                    // Use custom timeout if provided, otherwise use adaptive timeout
                    let timeout = self.coordination_timeout.unwrap_or_else(|| {
                        // FRAMEWORK INTELLIGENCE: Adaptive timeout based on consumer count
                        match *max_consumers {
                            1 => Duration::from_secs(15),     // SPSC: Quick startup
                            2 => Duration::from_secs(20),     // SPMC-2: Proven sweet spot
                            3..=4 => Duration::from_secs(25), // SPMC-3/4: Moderate
                            5..=8 => Duration::from_secs(30), // SPMC-5+: More time for coordination
                            _ => Duration::from_secs(45),     // SPMC-9+: Maximum timeout
                        }
                    });

                    CoordinationMode::wait_for_consumers(*max_consumers as i64, timeout)
                }
                _ => CoordinationMode::default(), // No discovery = immediate start
            }
        });

        let mut producer = SharedProducer::new_with_coordination_and_discovery(
            ring_buffer,
            producer_sequence,
            self.config.name.clone(), // Pass base name for consumer discovery
            coordination_mode.clone(),
            discovery_mode,
            consumer_registration,
        );

        // Handle coordination during producer creation, not first publish
        match coordination_mode {
            CoordinationMode::Immediate => {
                // No coordination - for external coordination benchmarks
                producer.coordination_completed = true;
            }
            CoordinationMode::WaitForConsumers {
                min_consumers,
                timeout,
            } => {
                println!(
                    "Framework coordinating startup: waiting for {} consumers (timeout: {:?})...",
                    min_consumers, timeout
                );
                let coordination_ready = producer
                    .consumer_barrier
                    .wait_for_consumers_ready(min_consumers, timeout);
                if !coordination_ready {
                    eprintln!("Warning: Timed out waiting for {} consumers after {:?}. Producer created anyway.",
                        min_consumers, timeout);
                    println!(
                        "Framework coordination incomplete - producer continuing without {} ready consumers",
                        min_consumers
                    );
                } else {
                    println!(
                        "Framework coordination completed - {} consumers ready",
                        min_consumers
                    );
                }
                // Mark coordination as completed
                producer.coordination_completed = true;
            }
            CoordinationMode::BufferUntilConsumers { .. } => {
                // Future enhancement - for now, treat as completed
                producer.coordination_completed = true;
            }
        }

        Ok(producer)
    }

    /// Build a consumer (attaches to existing shared memory segments)
    pub fn build_consumer(self) -> MultiProcessResult<SharedConsumer<E>> {
        #[cfg(feature = "dst")]
        if dst_fixtures::dst_buggify::buggify(file!(), line!()) {
            std::thread::sleep(Duration::from_millis(100));
        }

        self.maybe_pin_process_to_core(ProcessRole::Consumer);

        let ring_buffer: SharedRingBuffer<E> = SharedRingBuffer::attach(self.config.clone())?;

        // Attach to existing shared atomics
        let producer_sequence_name = format!("{}_producer_seq", self.config.name);
        let producer_sequence = SharedCursor::attach(&producer_sequence_name)?;

        // Use custom consumer ID if provided, otherwise generate a deterministic
        // slot-based ID when coordinated discovery is available.
        let consumer_id = if let Some(custom_id) = self.consumer_id {
            custom_id
        } else {
            default_consumer_id(&self.config.name)
        };

        // Create this consumer's own sequence tracker
        // Note: Uses new_or_attach because multiple consumers might start simultaneously
        // and try to create the same sequence name (different from coordination structures)
        let consumer_sequence_name = format!("{}_{}_seq", self.config.name, consumer_id);
        let mut consumer_sequence = SharedCursor::new_or_attach(&consumer_sequence_name, -1)?;
        if consumer_sequence.is_owner() {
            consumer_sequence.set_owner(false);
        }

        Ok(SharedConsumer::new_with_coordination(
            ring_buffer,
            producer_sequence,
            consumer_sequence,
            consumer_id,
            Some(self.config.name.clone()),
        ))
    }
}

fn resolve_auto_consumer_core(consumer_core: Option<usize>) -> Option<usize> {
    if let Some(core_id) = consumer_core {
        return Some(core_id);
    }

    resolve_core_from_env_vars(&["DISRUPTOR_MP_AUTO_CONSUMER_CORE"])
}

fn resolve_process_core(process_core: Option<usize>, role: ProcessRole) -> Option<usize> {
    if let Some(core_id) = process_core {
        return Some(core_id);
    }

    resolve_core_from_env_vars(&[role.env_var(), "DISRUPTOR_MP_PROCESS_CORE"])
}

fn resolve_core_from_env_vars(env_vars: &[&str]) -> Option<usize> {
    for env_var in env_vars {
        match env::var(env_var) {
            Ok(raw) => match raw.parse::<usize>() {
                Ok(core_id) => return Some(core_id),
                Err(_) => {
                    eprintln!(
                        "Invalid {}='{}'. Expected a non-negative integer.",
                        env_var, raw
                    );
                    return None;
                }
            },
            Err(env::VarError::NotPresent) => {}
            Err(env::VarError::NotUnicode(_)) => {
                eprintln!("Invalid {}: value is not valid Unicode.", env_var);
                return None;
            }
        }
    }

    None
}

#[cfg(target_os = "linux")]
fn pin_current_thread_to_core(core_id: usize, thread_name: &str) {
    let available_cores = core_affinity::get_core_ids().unwrap_or_default();
    let core = core_affinity::CoreId { id: core_id };

    if !available_cores
        .iter()
        .any(|candidate| candidate.id == core_id)
    {
        eprintln!(
            "Could not pin {} to core {}: core not available.",
            thread_name, core_id
        );
        return;
    }

    if !core_affinity::set_for_current(core) {
        eprintln!("Could not pin {} to core {}.", thread_name, core_id);
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread_to_core(core_id: usize, thread_name: &str) {
    eprintln!(
        "CPU affinity support is Linux-only in disruptor-mp. Requested pinning {} -> core {} ignored; bring-up continues without pinning on this platform.",
        thread_name, core_id
    );
}

/// Create a shared single producer for multi-process communication
///
/// This creates a Single Producer Multiple Consumer (SPMC) setup where:
/// - One process creates and owns the producer
/// - Multiple processes can attach as consumers (each sees all events)
///
/// Note: SharedProducer cannot be cloned across processes. Each shared memory
/// segment supports exactly one producer process.
///
/// For automatic coordination, use the builder pattern:
/// ```rust,ignore
/// use std::time::Duration;
/// use disruptor_mp::build_shared_single_producer;
///
/// let producer = build_shared_single_producer::<Event>("test", 1024)
///     .wait_for_single_consumer(Duration::from_secs(30))
///     .build_producer(Event::default)?;
/// ```
/// Create a builder for a shared single producer with the given name and size.
///
/// This is a convenience function that creates a [`SharedDisruptorBuilder`] configured
/// for producer creation with the specified shared memory segment name and buffer size.
///
/// # Arguments
/// * `name` - Shared memory segment name (keep under 10 characters for cross-platform compatibility)
/// * `size` - Ring buffer size (must be power of 2)
///
/// # Examples
/// ```rust
/// use disruptor_mp::build_shared_single_producer;
///
/// #[derive(Copy, Clone, Default)]
/// struct Event { data: i64 }
///
/// let builder = build_shared_single_producer::<Event>("ring123", 1024);
/// let producer = builder.build_producer(|| Event::default())?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn build_shared_single_producer<E: Copy + Default + 'static>(
    name: &str,
    size: usize,
) -> SharedDisruptorBuilder<E> {
    let config = SharedMemoryConfig {
        name: name.to_string(),
        buffer_size: size,
        element_size: std::mem::size_of::<E>(),
        create: true,
    };

    SharedDisruptorBuilder::new(config)
}

/// Attach to an existing shared disruptor as a consumer
///
/// This allows multiple consumer processes to attach to a shared memory segment
/// created by a producer process. Each consumer will see all events (broadcast semantics).
/// Create a builder for attaching to an existing shared consumer.
///
/// This is a convenience function that creates a [`SharedDisruptorBuilder`] configured
/// for consumer attachment to an existing shared memory segment created by a producer.
///
/// # Arguments
/// * `name` - Shared memory segment name (must match the producer's name)
/// * `size` - Ring buffer size (must match the producer's size)
///
/// # Examples
/// ```rust,no_run
/// use disruptor_mp::attach_shared_consumer;
///
/// #[derive(Copy, Clone, Default)]
/// struct Event { data: i64 }
///
/// let builder = attach_shared_consumer::<Event>("ring123", 1024);
/// let consumer = builder.build_consumer()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn attach_shared_consumer<E: Copy + Default + 'static>(
    name: &str,
    size: usize,
) -> SharedDisruptorBuilder<E> {
    let config = SharedMemoryConfig {
        name: name.to_string(),
        buffer_size: size,
        element_size: std::mem::size_of::<E>(),
        create: false,
    };

    SharedDisruptorBuilder::new(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::sync::{Mutex, OnceLock};
    use std::time::{Duration, Instant};

    #[derive(Copy, Clone, Default)]
    struct TestEvent {
        value: i64,
    }

    fn env_lock() -> &'static Mutex<()> {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    fn with_env_vars<const N: usize, F, R>(
        vars: [(&'static str, Option<&'static str>); N],
        f: F,
    ) -> R
    where
        F: FnOnce() -> R,
    {
        let _guard = env_lock().lock().expect("environment lock poisoned");
        let previous: [(&'static str, Option<OsString>); N] =
            vars.map(|(key, _)| (key, std::env::var_os(key)));

        for (key, value) in vars {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }

        let result = f();

        for (key, value) in previous {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }

        result
    }

    /// Test that Block wait strategy doesn't hang when no events are available
    #[test]
    fn test_block_wait_strategy_no_hang() {
        let segment_name = format!("test_blk_{}", std::process::id() % 10000);
        let buffer_size = 128;

        // Create producer
        let mut producer = build_shared_single_producer::<TestEvent>(&segment_name, buffer_size)
            .build_producer(TestEvent::default)
            .expect("Failed to create producer");

        // Create consumer with Block wait strategy
        let events_received = Arc::new(AtomicUsize::new(0));
        let events_clone = Arc::clone(&events_received);

        let consumer = attach_shared_consumer::<TestEvent>(&segment_name, buffer_size)
            .handle_events_batch(
                move |_event: &TestEvent, _seq, _eob| {
                    events_clone.fetch_add(1, Ordering::Relaxed);
                },
                AutoWaitStrategy::Block,
            )
            .expect("Failed to create consumer");

        // Wait a bit to ensure consumer doesn't hang
        std::thread::sleep(Duration::from_millis(100));

        // Publish an event
        producer.publish(|event| {
            event.value = 42;
        });

        // Wait for event to be processed
        std::thread::sleep(Duration::from_millis(50));

        // Verify event was received
        assert_eq!(events_received.load(Ordering::Relaxed), 1);

        // Shutdown consumer
        drop(consumer);
    }

    /// Test that BusySpin wait strategy works correctly
    #[test]
    fn test_busy_spin_wait_strategy() {
        let segment_name = format!("test_spn_{}", std::process::id() % 10000);
        let buffer_size = 128;

        // Create producer
        let mut producer = build_shared_single_producer::<TestEvent>(&segment_name, buffer_size)
            .build_producer(TestEvent::default)
            .expect("Failed to create producer");

        // Create consumer with BusySpin wait strategy
        let events_received = Arc::new(AtomicUsize::new(0));
        let events_clone = Arc::clone(&events_received);

        let consumer = attach_shared_consumer::<TestEvent>(&segment_name, buffer_size)
            .handle_events_batch(
                move |_event: &TestEvent, _seq, _eob| {
                    events_clone.fetch_add(1, Ordering::Relaxed);
                },
                AutoWaitStrategy::BusySpin,
            )
            .expect("Failed to create consumer");

        // Publish multiple events
        for i in 0..10 {
            producer.publish(|event| {
                event.value = i;
            });
        }

        // Wait for events to be processed
        std::thread::sleep(Duration::from_millis(50));

        // Verify all events were received
        assert_eq!(events_received.load(Ordering::Relaxed), 10);

        // Shutdown consumer
        drop(consumer);
    }

    /// Test that Sleep wait strategy works correctly
    #[test]
    fn test_sleep_wait_strategy() {
        let segment_name = format!("test_slp_{}", std::process::id() % 10000);
        let buffer_size = 128;

        // Create producer
        let mut producer = build_shared_single_producer::<TestEvent>(&segment_name, buffer_size)
            .build_producer(TestEvent::default)
            .expect("Failed to create producer");

        // Create consumer with Sleep wait strategy
        let events_received = Arc::new(AtomicUsize::new(0));
        let events_clone = Arc::clone(&events_received);

        let consumer = attach_shared_consumer::<TestEvent>(&segment_name, buffer_size)
            .handle_events_batch(
                move |_event: &TestEvent, _seq, _eob| {
                    events_clone.fetch_add(1, Ordering::Relaxed);
                },
                AutoWaitStrategy::Sleep(Duration::from_millis(5)),
            )
            .expect("Failed to create consumer");

        // Publish events
        for i in 0..5 {
            producer.publish(|event| {
                event.value = i;
            });
            std::thread::sleep(Duration::from_millis(10));
        }

        // Wait for events to be processed
        std::thread::sleep(Duration::from_millis(100));

        // Verify all events were received
        assert_eq!(events_received.load(Ordering::Relaxed), 5);

        // Shutdown consumer
        drop(consumer);
    }

    /// Test AutoConsumer shutdown mechanism
    #[test]
    fn test_auto_consumer_shutdown() {
        let segment_name = format!("test_sht_{}", std::process::id() % 10000);
        let buffer_size = 128;

        // Create producer
        let mut producer = build_shared_single_producer::<TestEvent>(&segment_name, buffer_size)
            .build_producer(TestEvent::default)
            .expect("Failed to create producer");

        // Create consumer
        let events_received = Arc::new(AtomicUsize::new(0));
        let events_clone = Arc::clone(&events_received);

        let mut consumer = attach_shared_consumer::<TestEvent>(&segment_name, buffer_size)
            .handle_events_batch(
                move |_event: &TestEvent, _seq, _eob| {
                    events_clone.fetch_add(1, Ordering::Relaxed);
                },
                AutoWaitStrategy::Block,
            )
            .expect("Failed to create consumer");

        // Publish some events
        for i in 0..5 {
            producer.publish(|event| {
                event.value = i;
            });
        }

        // Wait for events to be processed
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(events_received.load(Ordering::Relaxed), 5);

        // Shutdown consumer
        consumer.shutdown();

        // Publish more events
        for i in 5..10 {
            producer.publish(|event| {
                event.value = i;
            });
        }

        // Wait and verify no new events are processed
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(events_received.load(Ordering::Relaxed), 5);

        // Clean up
        consumer.join();
    }

    /// Test that AutoConsumer processes events correctly
    /// Note: The batch tracking with end_of_batch flag is not reliable in the current
    /// implementation as it's approximated for performance reasons
    #[test]
    fn test_auto_consumer_batch_processing() {
        let segment_name = format!("test_bat_{}", std::process::id() % 10000);
        let buffer_size = 1024;

        // Track events processed (simpler test)
        let events_processed = Arc::new(AtomicUsize::new(0));
        let events_clone = Arc::clone(&events_processed);

        // Create producer WITHOUT discovery (since buffer wrapping is now fixed)
        let mut producer = build_shared_single_producer::<TestEvent>(&segment_name, buffer_size)
            .build_producer(TestEvent::default)
            .expect("Failed to create producer");

        // Create consumer after producer (now safe with buffer wrapping fix)
        let consumer = attach_shared_consumer::<TestEvent>(&segment_name, buffer_size)
            .handle_events_batch(
                move |_event: &TestEvent, _seq, _end_of_batch| {
                    events_clone.fetch_add(1, Ordering::Relaxed);
                },
                AutoWaitStrategy::Block,
            )
            .expect("Failed to create consumer");

        // Publish events in bursts
        for burst in 0..3 {
            for i in 0..10 {
                producer.publish(|event| {
                    event.value = burst * 10 + i;
                });
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        // Wait for processing
        std::thread::sleep(Duration::from_millis(200)); // Slightly longer wait

        // Verify all events were processed
        let total_events = events_processed.load(Ordering::Relaxed);
        assert_eq!(total_events, 30, "Should have processed all 30 events");

        // Shutdown consumer
        drop(consumer);
    }

    /// Test performance characteristics of different wait strategies
    #[test]
    #[ignore] // Ignore by default as this is a performance test
    fn test_wait_strategy_performance() {
        let buffer_size = 8192;
        let num_events = 100_000;

        // Test each wait strategy
        let strategies = vec![
            ("BusySpin", AutoWaitStrategy::BusySpin),
            (
                "BusySpinWithHint",
                AutoWaitStrategy::BusySpinWithSpinLoopHint,
            ),
            ("Block", AutoWaitStrategy::Block),
            (
                "Sleep_1us",
                AutoWaitStrategy::Sleep(Duration::from_micros(1)),
            ),
            (
                "Sleep_100us",
                AutoWaitStrategy::Sleep(Duration::from_micros(100)),
            ),
        ];

        for (name, strategy) in strategies {
            let segment_name = format!("tst_p_{}_{}", name, std::process::id() % 10000);

            // Create producer
            let mut producer =
                build_shared_single_producer::<TestEvent>(&segment_name, buffer_size)
                    .build_producer(TestEvent::default)
                    .expect("Failed to create producer");

            // Create consumer
            let events_received = Arc::new(AtomicUsize::new(0));
            let events_clone = Arc::clone(&events_received);
            let start = Instant::now();

            let consumer = attach_shared_consumer::<TestEvent>(&segment_name, buffer_size)
                .handle_events_batch(
                    move |_event: &TestEvent, _seq, _eob| {
                        events_clone.fetch_add(1, Ordering::Relaxed);
                    },
                    strategy,
                )
                .expect("Failed to create consumer");

            // Publish events
            for i in 0..num_events {
                producer.publish(|event| {
                    event.value = i;
                });
            }

            // Wait for all events to be processed
            while events_received.load(Ordering::Relaxed) < num_events as usize {
                std::thread::sleep(Duration::from_millis(1));
            }

            let elapsed = start.elapsed();
            let events_per_sec = num_events as f64 / elapsed.as_secs_f64();

            println!("{} strategy: {:.0} events/sec", name, events_per_sec);

            // Shutdown consumer
            drop(consumer);
        }
    }

    #[test]
    fn test_consumer_core_override_is_preserved() {
        let segment_name = format!("test_cpu_{}", std::process::id() % 10000);
        let buffer_size = 128;
        let builder =
            attach_shared_consumer::<TestEvent>(&segment_name, buffer_size).with_consumer_core(3);

        assert_eq!(builder.consumer_core, Some(3));
    }

    #[test]
    fn test_consumer_core_resolve_from_builder_overrides_env() {
        let override_core = 7usize;
        let resolved = resolve_auto_consumer_core(Some(override_core));
        assert_eq!(resolved, Some(override_core));
    }

    #[test]
    fn test_consumer_core_resolve_from_env_var() {
        with_env_vars([("DISRUPTOR_MP_AUTO_CONSUMER_CORE", Some("9"))], || {
            let resolved = resolve_auto_consumer_core(None);
            assert_eq!(resolved, Some(9));
        });
    }

    #[test]
    fn test_consumer_core_resolve_from_invalid_env_var() {
        with_env_vars(
            [("DISRUPTOR_MP_AUTO_CONSUMER_CORE", Some("invalid"))],
            || {
                let resolved = resolve_auto_consumer_core(None);
                assert_eq!(resolved, None);
            },
        );
    }

    #[test]
    fn test_process_core_override_is_preserved() {
        let segment_name = format!("test_prc_{}", std::process::id() % 10000);
        let buffer_size = 128;
        let builder = build_shared_single_producer::<TestEvent>(&segment_name, buffer_size)
            .with_process_core(5);

        assert_eq!(builder.process_core, Some(5));
    }

    #[test]
    fn test_process_core_resolve_from_builder_overrides_env() {
        with_env_vars(
            [
                ("DISRUPTOR_MP_PRODUCER_CORE", Some("7")),
                ("DISRUPTOR_MP_PROCESS_CORE", Some("9")),
            ],
            || {
                let resolved = resolve_process_core(Some(11), ProcessRole::Producer);
                assert_eq!(resolved, Some(11));
            },
        );
    }

    #[test]
    fn test_process_core_resolve_from_role_specific_env_var() {
        with_env_vars(
            [
                ("DISRUPTOR_MP_PRODUCER_CORE", Some("6")),
                ("DISRUPTOR_MP_PROCESS_CORE", Some("8")),
            ],
            || {
                let resolved = resolve_process_core(None, ProcessRole::Producer);
                assert_eq!(resolved, Some(6));
            },
        );
    }

    #[test]
    fn test_process_core_resolve_from_generic_env_var() {
        with_env_vars(
            [
                ("DISRUPTOR_MP_PRODUCER_CORE", None),
                ("DISRUPTOR_MP_PROCESS_CORE", Some("10")),
            ],
            || {
                let resolved = resolve_process_core(None, ProcessRole::Producer);
                assert_eq!(resolved, Some(10));
            },
        );
    }

    #[test]
    fn test_process_core_resolve_from_invalid_role_specific_env_var() {
        with_env_vars(
            [
                ("DISRUPTOR_MP_CONSUMER_CORE", Some("invalid")),
                ("DISRUPTOR_MP_PROCESS_CORE", Some("12")),
            ],
            || {
                let resolved = resolve_process_core(None, ProcessRole::Consumer);
                assert_eq!(resolved, None);
            },
        );
    }
}
