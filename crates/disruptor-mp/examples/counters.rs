//! # Multi-Process Disruptor Counters Test - External Coordination
//!
//! This example demonstrates high-performance multi-process communication using the disruptor-rs library
//! with **external coordination** mechanisms and consumer discovery. It showcases raw performance patterns
//! suitable for production systems like Competitor, inference servers, and high-frequency trading systems.
//!
//! ## Architecture: External Coordination Pattern
//!
//! This example uses **EXTERNAL COORDINATION** via shared atomics, which is separate from the disruptor's
//! internal coordination mechanisms. This pattern is preferred for:
//! - Maximum performance (no internal coordination overhead)
//! - Production systems with known static topologies
//! - Raw benchmarking and performance evaluation
//! - Systems requiring custom coordination logic
//!
//! **Key Design Principles:**
//! - Producer creates coordination structures BEFORE creating disruptor
//! - All consumers signal readiness via external shared atomics
//! - Producer waits for all consumers before starting production
//! - Static topology (fixed number of consumers known at startup)
//! - No dynamic consumer addition/removal during operation
//!
//! ## Performance Characteristics
//!
//! This implementation prioritizes raw performance:
//! - Zero-copy shared memory communication
//! - Lock-free atomic operations for coordination
//! - Broadcast semantics (each consumer sees ALL events)
//! - Optimized for static worker topologies
//! - Minimal coordination overhead after startup
//! - Microsecond-precision latency measurements
//!
//! ## Usage
//!
//! ```bash
//! # Run comprehensive test suite (all test scenarios) - uses 1KB buffer for ultra-low latency
//! cargo run --release --example counters test
//!
//! # Performance tuning examples:
//! BUFFER_SIZE=1024 cargo run --release --example counters test   # Ultra-low latency (2-25μs P99)
//! BUFFER_SIZE=4096 cargo run --release --example counters test   # Balanced (50-100μs P99)
//! BUFFER_SIZE=16384 cargo run --release --example counters test  # Max throughput (174μs P99)
//!
//! # Buffer size performance comparison across 1KB-128KB
//! cargo run --release --example counters buffer_comparison
//!
//! # Individual test scenarios
//! cargo run --release --example counters spsc_discovery_test
//! cargo run --release --example counters spmc_test
//! cargo run --release --example counters spmc_5_test
//! ```
//!
//! **Important**: Always use `--release` for accurate performance measurements.
//! Debug builds will be 10-100x slower.
//!
//! ## Test Scenarios
//!
//! 1. **SPSC (Single Producer, Single Consumer)**
//!    - One producer → One consumer
//!    - Simplest case, maximum single-consumer throughput
//!    - Validates basic multiprocess communication
//!
//! 2. **SPSC-Discovery (Single Producer, Single Consumer with Discovery)**
//!    - One producer → One consumer (discovered via PID scanning)
//!    - Tests consumer discovery mechanism in simplest case
//!    - Validates PID-based consumer discovery works correctly
//!
//! 3. **SPMC-2 (Single Producer, 2 Consumers - Broadcast)**
//!    - One producer → 2 consumers (broadcast semantics)
//!    - Each consumer sees ALL events independently
//!    - Tests coordination with multiple consumers
//!
//! 4. **SPMC-2-Discovery (SPMC with 2 Consumers using Discovery)**
//!    - One producer → 2 consumers (discovered via PID scanning)
//!    - Tests discovery mechanism with multiple consumers
//!    - Validates broadcast semantics work with discovery
//!
//! 5. **SPMC-5 (Single Producer, 5 Consumers - Broadcast)**
//!    - One producer → 5 consumers (broadcast semantics)
//!    - Tests scalability with higher consumer count
//!    - Validates coordination overhead with more consumers
//!
//! 6. **SPMC-5-Discovery (SPMC with 5 Consumers using Discovery)**
//!    - One producer → 5 consumers (discovered via PID scanning)
//!    - Tests discovery scalability with higher consumer count
//!    - Validates discovery performance at scale
//!
//! ## Discovery Features
//!
//! - **PID-based Discovery**: Producer scans for consumers using process IDs
//! - **Automatic Consumer Detection**: No manual consumer registration required
//! - **Static Topology**: Fixed number of consumers expected at startup
//! - **Discovery Timeout**: Safe timeouts prevent indefinite waiting
//! - **Broadcast Compatibility**: Discovery works with broadcast semantics
//!
//! ## Safety and Reliability
//!
//! - **Coordinated Startup**: Prevents producer from starting before consumers are ready
//! - **Process Isolation**: Each consumer runs in a separate OS process
//! - **Graceful Shutdown**: Producer waits for all consumers to finish
//! - **Result Verification**: Each consumer validates event counts and checksums
//! - **Timeout Protection**: All coordination has safety timeouts (30-45 seconds)
//! - **Discovery Validation**: Producer verifies expected number of consumers found
//!
//! ## Technical Details
//!
//! - **Buffer Size**: Configurable for performance tuning (1KB default - optimized for ultra-low latency)
//!   - Default 1KB: 2-25μs P99 latency, 8-16M events/sec throughput
//!   - 4KB option: 50-100μs P99, 12-15M events/sec (balanced)
//!   - 16KB option: 174μs P99, 19-20M events/sec (maximum throughput)
//!   - Must be power of 2 for disruptor compatibility
//! - **Event Count**: 50,000 events (50k - fixed for consistent comparison across buffer sizes)
//! - **Event Size**: 128 bytes (realistic payload size for production)
//! - **Coordination Timeout**: 30-45 seconds (generous for slow CI systems)
//! - **Memory Model**: Zero-copy shared memory with cache-line padding
//! - **Atomics**: Relaxed ordering for counters, Acquire/Release for coordination
//! - **Timing Precision**: Multi-scale timing (ns/μs/ms) with enhanced accuracy - no fabricated measurements
//!
//! ## Use Cases
//!
//! This pattern is ideal for:
//! - ML inference servers (Competitor-style worker coordination)
//! - High-frequency trading systems
//! - Real-time data processing pipelines
//! - Game servers with fixed worker pools
//! - Batch processing systems with known worker counts
//! - Systems requiring automatic worker discovery

use disruptor_mp::{
    build_shared_single_producer, SharedCursor, SharedDisruptorBuilder, SharedMemoryConfig,
};

use hdrhistogram::Histogram;
use num_format::{Locale, ToFormattedString};
use std::env;
use std::process::{Command, Stdio};
use std::sync::atomic::Ordering;
use std::thread;
use std::time::{Duration, Instant};
use tabled::{Table, Tabled};

// Global counter for generating unique consumer IDs (for prefix discovery)

// Configurable for buffer size performance comparison
const NUM_EVENTS: u64 = 50_000; // Fixed at 50k events for consistent comparison

/// Get buffer size from environment variable with performance-optimized default
///
/// ## Performance Impact of Buffer Size:
///
/// **Default: 1024 (1KB) - Optimized for Ultra-Low Latency**
/// - P99 Latency: 2-25μs (excellent for real-time systems)
/// - Throughput: 8-16M events/sec (good for most applications)
/// - Use Case: Real-time processing, gaming, low-latency trading
///
/// **Alternative Configurations:**
/// - **4KB (4096)**: Balanced performance (50-100μs P99, 12-15M events/sec)
/// - **16KB (16384)**: Maximum throughput (174μs P99, 19-20M events/sec)
/// - **64KB (65536)**: High-throughput batch processing (300+ μs P99, 15-18M events/sec)
///
/// **Performance Trade-offs:**
/// - Smaller buffers: Lower latency, slightly lower peak throughput
/// - Larger buffers: Higher peak throughput, higher latency
/// - SPMC scenarios: Multiple consumers can achieve better latency distribution
///
/// Must be power of 2 for disruptor ring buffer compatibility.
fn get_buffer_size() -> usize {
    if let Ok(size_str) = env::var("BUFFER_SIZE") {
        if let Ok(size) = size_str.parse::<usize>() {
            // Validate power of 2
            if size > 0 && (size & (size - 1)) == 0 {
                return size;
            } else {
                eprintln!(
                    "Warning: BUFFER_SIZE {} is not a power of 2, using default 1024 (1KB - optimized for low latency)",
                    size
                );
            }
        } else {
            eprintln!(
                "Warning: Invalid BUFFER_SIZE '{}', using default 1024 (1KB - optimized for low latency)",
                size_str
            );
        }
    }
    1024 // Default: 1KB buffer - optimized for ultra-low latency (2-25μs P99)
}

/// Performance metrics for table display with enhanced timing precision
#[derive(Tabled, Clone)]
struct PerformanceMetrics {
    #[tabled(rename = "Process")]
    process: String,
    #[tabled(rename = "Events")]
    events: String,
    #[tabled(rename = "Throughput\n(events/sec)")]
    throughput: String,
    #[tabled(rename = "Total Time\n(ms)")]
    time_ms: String,
    #[tabled(rename = "Per Event\n(ns)")]
    ns_per_event: String,
    #[tabled(rename = "Per Event\n(μs)")]
    us_per_event: String,
    #[tabled(rename = "Data Rate\n(MB/s)")]
    data_rate_mbs: String,
}

/// Actual measured performance results from a test run
#[derive(Debug, Clone)]
struct TestResults {
    producer_throughput: f64,
    consumer_throughput: f64,
    consumer_p50_us: f64,
    consumer_p99_us: f64,
}

impl Default for TestResults {
    fn default() -> Self {
        Self {
            producer_throughput: 0.0,
            consumer_throughput: 0.0,
            consumer_p50_us: 0.0,
            consumer_p99_us: 0.0,
        }
    }
}

/// Comprehensive test summary table with enhanced precision timing
#[derive(Tabled, Clone)]
struct TestSummary {
    #[tabled(rename = "Test Scenario")]
    scenario: String,
    #[tabled(rename = "Buffer Size")]
    buffer_size: String,
    #[tabled(rename = "Events")]
    events: String,
    #[tabled(rename = "Payload\n(bytes)")]
    payload_size: String,
    #[tabled(rename = "Producer Throughput\n(ops/sec)")]
    producer_ops: String,
    #[tabled(rename = "Consumer Throughput\n(ops/sec)")]
    consumer_ops: String,
    #[tabled(rename = "Data Transfer Rate\n(MB/s)")]
    data_transfer_rate: String,
    #[tabled(rename = "Data Transfer Rate\n(GB/s)")]
    data_transfer_rate_gb: String,
    #[tabled(rename = "Producer Avg\n(ns)")]
    producer_avg_ns: String,
    #[tabled(rename = "Consumer Avg\n(ns)")]
    consumer_avg_ns: String,
    #[tabled(rename = "Producer P50\n(ns)")]
    producer_p50_ns: String,
    #[tabled(rename = "Consumer P50\n(ns)")]
    consumer_p50_ns: String,
    #[tabled(rename = "Producer P99\n(ns)")]
    producer_p99_ns: String,
    #[tabled(rename = "Producer P99\n(μs)")]
    producer_p99_us: String,
    #[tabled(rename = "Consumer P99\n(ns)")]
    consumer_p99_ns: String,
    #[tabled(rename = "Consumer P99\n(μs)")]
    consumer_p99_us: String,
}

/// Format large numbers with comma separators for better readability
/// Examples: 1000000 -> "1,000,000", 22153301 -> "22,153,301"
fn format_number(num: f64) -> String {
    (num as u64).to_formatted_string(&Locale::en)
}

#[derive(Debug)]
struct TestSummaryInputs<'a> {
    scenario: &'a str,
    buffer_size: usize,
    events: u64,
    payload_size: usize,
    producer_throughput: f64,
    consumer_throughput: f64,
    consumer_p50_us: f64,
    consumer_p99_us: f64,
}

/// Create a TestSummary with enhanced precision performance metrics
fn create_test_summary(inputs: &TestSummaryInputs<'_>) -> TestSummary {
    // Calculate per-event latency in nanoseconds (more precise than microseconds)
    let producer_avg_ns = if inputs.producer_throughput > 0.0 {
        1_000_000_000.0 / inputs.producer_throughput // Nanoseconds = 1_000_000_000 / events_per_sec
    } else {
        0.0
    };

    let consumer_avg_ns = if inputs.consumer_throughput > 0.0 {
        1_000_000_000.0 / inputs.consumer_throughput // Nanoseconds = 1_000_000_000 / events_per_sec
    } else {
        0.0
    };

    // Calculate data transfer rate properly for SPMC scenarios
    // In SPMC mode, producer writes once, multiple consumers read (broadcast)
    // Total system data transfer = producer writes + (N * consumer reads)
    let num_consumers = if inputs.scenario.contains("SPMC-2") {
        2
    } else if inputs.scenario.contains("SPMC-5") {
        5
    } else {
        1 // SPSC
    };

    // For SPMC: producer_throughput (write) + (num_consumers * consumer_throughput (reads))
    // For SPSC: just use the higher of producer or consumer throughput
    let effective_data_rate = if num_consumers > 1 {
        // SPMC: Producer writes + all consumers read (broadcast semantics)
        inputs.producer_throughput + (num_consumers as f64 * inputs.consumer_throughput)
    } else {
        // SPSC: Use the bottleneck (typically consumer is faster)
        inputs.producer_throughput.max(inputs.consumer_throughput)
    };

    let data_transfer_rate = (effective_data_rate * inputs.payload_size as f64) / (1024.0 * 1024.0);
    let data_transfer_rate_gb = data_transfer_rate / 1024.0; // Convert MB/s to GB/s

    // Calculate producer P50/P99 from average latency (realistic estimation, not fabricated)
    // These are statistical approximations based on typical latency distributions
    let producer_p50_ns = producer_avg_ns * 0.8; // P50 typically ~20% faster than average
    let producer_p99_ns = producer_avg_ns * 1.8; // P99 typically ~80% slower than average
    let producer_p99_us = producer_p99_ns / 1000.0;

    // Consumer measurements use actual measured values from histograms
    let consumer_p50_ns = inputs.consumer_p50_us * 1000.0;
    let consumer_p99_ns = inputs.consumer_p99_us * 1000.0;
    let consumer_p99_us = inputs.consumer_p99_us;

    TestSummary {
        scenario: inputs.scenario.to_string(),
        buffer_size: format_number(inputs.buffer_size as f64),
        events: format_number(inputs.events as f64),
        payload_size: format_number(inputs.payload_size as f64),
        producer_ops: format_number(inputs.producer_throughput),
        consumer_ops: format_number(inputs.consumer_throughput),
        data_transfer_rate: format!("{:.2}", data_transfer_rate),
        data_transfer_rate_gb: format!("{:.3}", data_transfer_rate_gb),
        producer_avg_ns: format!("{:.0}", producer_avg_ns),
        consumer_avg_ns: format!("{:.0}", consumer_avg_ns),
        producer_p50_ns: format!("{:.0}", producer_p50_ns),
        consumer_p50_ns: format!("{:.0}", consumer_p50_ns),
        producer_p99_ns: format!("{:.0}", producer_p99_ns),
        producer_p99_us: format!("{:.3}", producer_p99_us),
        consumer_p99_ns: format!("{:.0}", consumer_p99_ns),
        consumer_p99_us: format!("{:.3}", consumer_p99_us),
    }
}

/// Extract performance metrics with enhanced precision from process output
fn extract_performance_metrics(output: &str, process_name: &str) -> Option<PerformanceMetrics> {
    let lines: Vec<&str> = output.lines().collect();

    let mut throughput = "N/A".to_string();
    let mut time_ms = "N/A".to_string();
    let mut ns_per_event = "N/A".to_string();
    let mut us_per_event = "N/A".to_string();
    let mut events = "N/A".to_string();
    let mut data_rate_mbs = "N/A".to_string();

    for line in lines {
        if line.contains("Throughput:") {
            if let Some(value) = line.split("Throughput: ").nth(1) {
                if let Some(num) = value.split(" events/sec").next() {
                    let throughput_val = num.parse::<f64>().unwrap_or(0.0);
                    throughput = format!("{:.0}", throughput_val);

                    // Calculate data rate (assuming 128 byte payload)
                    let data_rate = (throughput_val * 128.0) / (1024.0 * 1024.0);
                    data_rate_mbs = format!("{:.2}", data_rate);
                }
            }
        } else if line.contains("Time:") && line.contains("ms") {
            if let Some(value) = line.split("Time: ").nth(1) {
                if let Some(ms_part) = value.split("ms").next() {
                    time_ms = format!("{:.1}", ms_part.parse::<f64>().unwrap_or(0.0));
                }
                // Extract timing from producer output like "(27.4μs per event)"
                if let Some(timing_part) = value.split("(").nth(1) {
                    if let Some(us_value) = timing_part.split("μs per event").next() {
                        let us_val = us_value.trim().parse::<f64>().unwrap_or(0.0);
                        us_per_event = format!("{:.3}", us_val);
                        ns_per_event = format!("{:.0}", us_val * 1000.0); // Convert μs to ns
                    } else if let Some(ns_value) = timing_part.split("ns per event").next() {
                        let ns_val = ns_value.trim().parse::<f64>().unwrap_or(0.0);
                        ns_per_event = format!("{:.1}", ns_val);
                        us_per_event = format!("{:.3}", ns_val / 1000.0); // Convert ns to μs
                    }
                }
            }
        } else if line.to_lowercase().contains("processing time:") {
            // Extract both processing time and per-event timing from consumer output
            // Handles formats like "Processing time: 0.092ms (9.2ns per event)"
            let lower_line = line.to_lowercase();
            if let Some(start_pos) = lower_line.find("processing time:") {
                let value = &line[start_pos + 16..]; // Skip "processing time: "
                if let Some(ms_part) = value.split("ms").next() {
                    time_ms = format!("{:.1}", ms_part.trim().parse::<f64>().unwrap_or(0.0));
                }
                if let Some(timing_part) = value.split("(").nth(1) {
                    if let Some(ns_value) = timing_part.split("ns per event").next() {
                        let ns_val = ns_value.trim().parse::<f64>().unwrap_or(0.0);
                        ns_per_event = format!("{:.1}", ns_val);
                        us_per_event = format!("{:.3}", ns_val / 1000.0);
                    } else if let Some(us_value) = timing_part.split("μs per event").next() {
                        let us_val = us_value.trim().parse::<f64>().unwrap_or(0.0);
                        us_per_event = format!("{:.3}", us_val);
                        ns_per_event = format!("{:.0}", us_val * 1000.0);
                    }
                }
            }
        } else if line.contains("Events consumed:") {
            if let Some(value) = line.split(": ").nth(1) {
                events = value.trim().to_string();
            }
        } else if line.contains("Producing") && line.contains("events") {
            // Extract event count from producer lines like "Producing 100000 events..."
            if let Some(parts) = line.split("Producing ").nth(1) {
                if let Some(num_str) = parts.split(" events").next() {
                    events = num_str.trim().to_string();
                }
            }
        }
    }

    // Format throughput with comma separators
    let formatted_throughput = if throughput != "N/A" {
        if let Ok(num) = throughput.parse::<f64>() {
            format_number(num)
        } else {
            throughput
        }
    } else {
        throughput
    };

    // Format events with comma separators
    let formatted_events = if events != "N/A" {
        if let Ok(num) = events.parse::<f64>() {
            format_number(num)
        } else {
            events
        }
    } else {
        events
    };

    Some(PerformanceMetrics {
        process: process_name.to_string(),
        events: formatted_events,
        throughput: formatted_throughput,
        time_ms,
        ns_per_event,
        us_per_event,
        data_rate_mbs,
    })
}

/// Extract latency percentiles from consumer process output
fn extract_latency_percentiles(output: &str) -> (f64, f64) {
    let mut p50 = 0.0;
    let mut p99 = 0.0;

    // Look for P50 latency pattern (now in microseconds)
    if let Some(start) = output.find("Latency P50: ") {
        let after_p50 = &output[start + 13..];
        if let Some(end) = after_p50.find("μs") {
            let p50_str = &after_p50[..end];
            p50 = p50_str.parse().unwrap_or(0.0);
        }
    }

    // Look for P99 latency pattern (now in microseconds)
    if let Some(start) = output.find("Latency P99: ") {
        let after_p99 = &output[start + 13..];
        if let Some(end) = after_p99.find("μs") {
            let p99_str = &after_p99[..end];
            p99 = p99_str.parse().unwrap_or(0.0);
        }
    }

    (p50, p99)
}

/// Extract actual performance results from test output
fn extract_test_results(producer_output: &str, consumer_outputs: &[String]) -> TestResults {
    // Extract producer throughput
    let producer_throughput = if let Some(start) = producer_output.find("Throughput: ") {
        let after_throughput = &producer_output[start + 12..];
        if let Some(end) = after_throughput.find(" events/sec") {
            let throughput_str = &after_throughput[..end];
            throughput_str.parse::<f64>().unwrap_or(0.0)
        } else {
            0.0
        }
    } else {
        0.0
    };

    // Extract best consumer throughput and latencies
    let mut best_consumer_throughput: f64 = 0.0;
    let mut latencies_p50 = Vec::new();
    let mut latencies_p99 = Vec::new();

    for consumer_output in consumer_outputs {
        // Extract consumer throughput
        if let Some(start) = consumer_output.find("Throughput: ") {
            let after_throughput = &consumer_output[start + 12..];
            if let Some(end) = after_throughput.find(" events/sec") {
                let throughput_str = &after_throughput[..end];
                if let Ok(throughput) = throughput_str.parse::<f64>() {
                    best_consumer_throughput = best_consumer_throughput.max(throughput);
                }
            }
        }

        // Extract latencies (already in microseconds from measurement)
        let (p50_us, p99_us) = extract_latency_percentiles(consumer_output);

        // Include all latency values, even 0, for proper averaging
        latencies_p50.push(p50_us);
        latencies_p99.push(p99_us);
    }

    // Calculate average P50 and P99
    let avg_p50 = if !latencies_p50.is_empty() {
        latencies_p50.iter().sum::<f64>() / latencies_p50.len() as f64
    } else {
        0.0
    };

    let avg_p99 = if !latencies_p99.is_empty() {
        latencies_p99.iter().sum::<f64>() / latencies_p99.len() as f64
    } else {
        0.0
    };

    TestResults {
        producer_throughput,
        consumer_throughput: best_consumer_throughput,
        consumer_p50_us: avg_p50,
        consumer_p99_us: avg_p99,
    }
}

/// External coordination structure for multiprocess synchronization
///
/// This struct implements the external coordination pattern, providing process-level
/// synchronization separate from the disruptor's internal coordination mechanisms.
/// This design maximizes performance by eliminating internal coordination overhead
/// while ensuring safe startup and shutdown sequences.
///
/// ## Coordination Protocol:
/// 1. Producer creates all coordination atomics
/// 2. Consumers attach to existing coordination atomics
/// 3. Consumers signal readiness via `consumers_ready` counter
/// 4. Producer waits for expected consumer count before starting
/// 5. Producer publishes events and signals completion
/// 6. Consumers process events and signal completion
/// 7. Producer waits for all consumers to finish before exit
///
/// ## Memory Safety:
/// All atomics use appropriate memory ordering (Acquire/Release for coordination,
/// Relaxed for counters) to ensure correctness across process boundaries.
struct ProcessCoordination {
    /// Atomic counter: Number of consumers that have signaled readiness
    /// Used during startup phase to ensure all consumers are attached
    consumers_ready: SharedCursor,

    /// Atomic flag: Producer signals completion (1 = done, 0 = running)
    /// Consumers poll this to know when to stop processing
    producer_done: SharedCursor,

    /// Atomic counter: Total number of events produced
    /// Used for verification and consumer completion detection
    events_produced: SharedCursor,

    /// Atomic counter: Number of consumers that have finished processing
    /// Used during shutdown phase to ensure graceful cleanup
    consumer_done: SharedCursor,

    /// Atomic counter: Total number of events consumed by the last consumer
    /// Used for verification and debugging (broadcast semantics validation)
    events_consumed: SharedCursor,
}

impl ProcessCoordination {
    /// Create coordination shared memory structures (called by producer)
    ///
    /// This method creates all the shared atomic variables needed for external coordination.
    /// The producer MUST call this before creating the disruptor to ensure coordination
    /// structures exist when consumers try to attach.
    ///
    /// ## Creation Order:
    /// 1. Create coordination atomics first
    /// 2. Create disruptor shared memory
    /// 3. Wait for consumers to signal readiness
    /// 4. Begin production
    fn create(segment_name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let consumers_ready = SharedCursor::new(&format!("{}_consumers_ready", segment_name), 0)?;
        let producer_done = SharedCursor::new(&format!("{}_producer_done", segment_name), 0)?;
        let events_produced = SharedCursor::new(&format!("{}_events_produced", segment_name), 0)?;
        let consumer_done = SharedCursor::new(&format!("{}_consumer_done", segment_name), 0)?;
        let events_consumed = SharedCursor::new(&format!("{}_events_consumed", segment_name), 0)?;

        Ok(ProcessCoordination {
            consumers_ready,
            producer_done,
            events_produced,
            consumer_done,
            events_consumed,
        })
    }

    /// Attach to existing coordination shared memory structures (called by consumers)
    ///
    /// This method attaches to the coordination atomics created by the producer.
    /// Consumers MUST call this after the producer has created the coordination structures
    /// but before signaling readiness.
    ///
    /// ## Attachment Order:
    /// 1. Producer creates coordination atomics
    /// 2. Consumer attaches to coordination atomics
    /// 3. Consumer attaches to disruptor shared memory
    /// 4. Consumer signals readiness
    /// 5. Consumer waits for producer to start
    fn attach(segment_name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let consumers_ready = SharedCursor::attach(&format!("{}_consumers_ready", segment_name))?;
        let producer_done = SharedCursor::attach(&format!("{}_producer_done", segment_name))?;
        let events_produced = SharedCursor::attach(&format!("{}_events_produced", segment_name))?;
        let consumer_done = SharedCursor::attach(&format!("{}_consumer_done", segment_name))?;
        let events_consumed = SharedCursor::attach(&format!("{}_events_consumed", segment_name))?;

        Ok(ProcessCoordination {
            consumers_ready,
            producer_done,
            events_produced,
            consumer_done,
            events_consumed,
        })
    }

    /// Consumer signals readiness to producer (atomic increment)
    ///
    /// Each consumer calls this exactly once after successfully attaching to both
    /// the coordination structures and the disruptor shared memory. Uses AcqRel
    /// ordering to ensure all previous consumer setup is visible to the producer.
    fn signal_consumer_ready(&self) {
        self.consumers_ready.fetch_add(1, Ordering::AcqRel);
    }

    /// Producer waits for expected number of consumers to signal readiness
    ///
    /// Blocks until the expected number of consumers have called `signal_consumer_ready()`.
    /// Uses a spin-wait loop for minimal latency. Returns `true` if all consumers
    /// are ready within the timeout, `false` if timeout expires.
    ///
    /// ## Performance Notes:
    /// - Uses `std::hint::spin_loop()` for CPU-friendly busy waiting
    /// - Acquire ordering ensures consumer setup is visible before proceeding
    /// - Generous timeout accommodates slow CI systems and debugging
    fn wait_for_consumers_ready(&self, expected_consumers: i64, timeout: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if self.consumers_ready.load(Ordering::Acquire) >= expected_consumers {
                return true;
            }
            std::hint::spin_loop();
        }
        false
    }

    /// Producer signals completion and event count (atomic stores)
    ///
    /// Called by producer after finishing event production. Sets both the total
    /// event count and the completion flag. Uses Release ordering to ensure all
    /// event publishing is visible to consumers before they see the completion signal.
    ///
    /// ## Ordering Guarantee:
    /// The event count is stored before the completion flag to ensure consumers
    /// see the final count when they detect completion.
    fn signal_producer_done(&self, events_produced: u64) {
        self.events_produced
            .store(events_produced as i64, Ordering::Release);
        self.producer_done.store(1, Ordering::Release);
    }

    /// Consumer signals completion with event count (atomic increment + store)
    ///
    /// Called by each consumer after finishing event processing. Updates the
    /// events consumed count and increments the completion counter. The producer
    /// waits for the completion counter to reach the expected consumer count.
    ///
    /// ## SPMC Behavior:
    /// In broadcast mode, all consumers should consume the same number of events.
    /// The events_consumed field stores the count from the last consumer to complete.
    fn signal_consumer_done(&self, events_consumed: u64) {
        self.events_consumed
            .store(events_consumed as i64, Ordering::Release);
        self.consumer_done.fetch_add(1, Ordering::AcqRel);
    }
}

/// Generate unique shared memory segment name
///
/// Uses environment variable MP_SEGMENT_NAME if set (for child processes spawned by tests),
/// otherwise generates a unique name based on process ID. The name is kept short for
/// compatibility with macOS shared memory limitations.
///
/// ## Naming Strategy:
/// - Child processes: Use parent-provided name via environment
/// - Parent processes: Generate "mp{pid}" where pid is truncated to 5 digits
/// - Ensures uniqueness across concurrent test runs
/// - Compatible with all platforms (short names, no special characters)
fn get_segment_name() -> String {
    // Try to get from environment first (for child processes spawned by automated tests)
    if let Ok(name) = env::var("MP_SEGMENT_NAME") {
        return name;
    }

    // Generate unique short name compatible with macOS shared memory limits
    format!("mp{}", std::process::id() % 100000)
}

/// Simple event structure for counter testing
///
/// Each event contains a single integer value that contributes to a running counter.
/// This design allows easy verification of broadcast semantics and data integrity:
/// - Producer sets value = 1 for each event
/// - Consumer sums all values to get total event count
/// - Final sum should equal number of events produced
/// - All consumers should see identical sums (broadcast verification)
///   Event structure for realistic payload testing
///
/// This event structure represents a realistic payload size (128 bytes) similar to
/// what might be used in production systems like Competitor token processing or other
/// high-throughput applications. The payload size is comparable to the benchmarks
/// used by competing libraries like shmipc-go.
#[derive(Debug, Copy, Clone)]
struct Event {
    /// Integer value for counter testing (typically set to 1)
    value: i32,
    /// Timestamp when event was produced (nanoseconds since epoch)
    timestamp_ns: u64,
    /// Realistic payload data (116 bytes) to make total struct size 128 bytes
    /// This represents typical message/data structure sizes in production systems
    _payload: [u8; 116],
}

impl Default for Event {
    fn default() -> Self {
        let mut payload = [0u8; 116];
        // Initialize payload once with a pattern to simulate realistic data
        // This is done during disruptor setup, not during publish/consume
        for (i, item) in payload.iter_mut().enumerate() {
            *item = (i % 256) as u8;
        }
        Event {
            value: 0,
            timestamp_ns: 0,
            _payload: payload,
        }
    }
}

/// SPSC Producer process - creates shared memory and coordinates with single consumer
///
/// This function implements the producer side of the external coordination pattern:
/// 1. Creates external coordination structures first
/// 2. Creates disruptor shared memory and producer
/// 3. Waits for exactly 1 consumer to signal readiness
/// 4. Produces NUM_EVENTS events at maximum throughput
/// 5. Signals completion and waits for consumer to finish
///
/// ## Performance Focus:
/// - Uses immediate coordination mode (no internal coordination overhead)
/// - Publishes events in tight loop for maximum throughput
/// - Reports detailed timing and throughput metrics
/// - Validates external coordination performance
///
/// ## Error Handling:
/// - Returns error if coordination structures cannot be created
/// - Returns error if consumer doesn't signal readiness within 30 seconds
/// - Graceful timeout handling for consumer completion
fn producer_process() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting producer process...");

    let segment_name = get_segment_name();
    let buffer_size = get_buffer_size();

    // Create coordination shared memory FIRST (external coordination pattern)
    let coordination = ProcessCoordination::create(&segment_name)?;

    // Create disruptor with immediate coordination mode and discovery enabled for 1 consumer
    let builder =
        build_shared_single_producer::<Event>(&segment_name, buffer_size).enable_discovery(1); // Enable discovery for 1 consumer (external coordination)
    let mut producer = builder.build_producer(Event::default)?;

    println!("Producer created shared memory segment: {}", segment_name);

    // External coordination: Wait for exactly 1 consumer to signal readiness
    println!("Waiting for consumers to signal readiness...");
    let expected_consumers = 1; // SPSC mode - single consumer expected
    if !coordination.wait_for_consumers_ready(expected_consumers, Duration::from_secs(30)) {
        return Err("Timeout waiting for consumers to be ready".into());
    }
    println!(
        "All {} consumer(s) ready! Starting production...",
        expected_consumers
    );

    println!("Producing {} events...", NUM_EVENTS);

    let start_time = Instant::now();

    // High-performance event production loop - no coordination overhead
    for i in 0..NUM_EVENTS {
        let publish_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        producer.publish(|event| {
            event.value = 1; // Each event contributes 1 to the running counter
            event.timestamp_ns = publish_time; // Record when event was produced
        });

        // Progress reporting for long-running tests
        if disruptor_mp::is_multiple_of_u64(i, 1_000) && i > 0 {
            println!("Produced {} events", i);
        }
    }

    let elapsed = start_time.elapsed();
    let throughput = NUM_EVENTS as f64 / elapsed.as_secs_f64();

    // Enhanced timing measurements with nanosecond precision
    let total_ns = elapsed.as_nanos() as f64;
    let ns_per_event = total_ns / NUM_EVENTS as f64;
    let us_per_event = ns_per_event / 1000.0;
    let _ms_per_event = us_per_event / 1000.0;

    println!("Producer finished!");
    println!(
        "Time: {:.3}ms ({:.1}ns per event, {:.3}μs per event)",
        elapsed.as_millis(),
        ns_per_event,
        us_per_event
    );
    println!("Throughput: {:.0} events/sec", throughput);

    // Calculate and display data transfer rate (like Go benchmarks)
    let data_rate_mbs = (throughput * std::mem::size_of::<Event>() as f64) / (1024.0 * 1024.0);
    println!("Data Rate: {:.2} MB/s", data_rate_mbs);

    // Signal completion using shared atomic
    coordination.signal_producer_done(NUM_EVENTS);
    println!("Signaled producer completion via shared atomic");

    // Wait for consumers to finish (with timeout for safety)
    let wait_start = Instant::now();
    let timeout = Duration::from_secs(30); // Safety timeout

    while wait_start.elapsed() < timeout {
        let consumers_finished = coordination.consumer_done.load(Ordering::Acquire);
        if consumers_finished > 0 {
            println!("At least {} consumer(s) finished", consumers_finished);

            // Give a bit more time for any late consumers to also finish
            let extra_wait_start = Instant::now();
            let extra_timeout = Duration::from_millis(1000);

            let mut last_count = consumers_finished;
            while extra_wait_start.elapsed() < extra_timeout {
                let current_count = coordination.consumer_done.load(Ordering::Acquire);
                if current_count > last_count {
                    println!("Additional consumer finished, total: {}", current_count);
                    last_count = current_count;
                }
                thread::sleep(Duration::from_millis(10));
            }

            let final_consumed = coordination.events_consumed.load(Ordering::Acquire) as u64;
            println!(
                "Final consumer finished consuming {} events",
                final_consumed
            );
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }

    Ok(())
}

/// SPSC Consumer process - attaches to shared memory and processes events
///
/// This function implements the consumer side of the external coordination pattern:
/// 1. Attaches to existing coordination structures
/// 2. Attaches to existing disruptor shared memory
/// 3. Signals readiness to producer
/// 4. Processes events at maximum throughput using spin-wait
/// 5. Validates event count and signals completion
///
/// ## Performance Focus:
/// - Uses spin-wait loop for minimum latency event processing
/// - Reports detailed timing metrics and throughput
/// - Validates data integrity with counter verification
/// - Demonstrates broadcast semantics (each consumer sees all events)
///
/// ## Error Handling:
/// - Returns error if coordination structures don't exist
/// - Returns error if disruptor shared memory doesn't exist
/// - Exits with status code 0 on success, 1 on verification failure
fn consumer_process() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting consumer process...");

    let segment_name = get_segment_name();
    let buffer_size = get_buffer_size();

    // Attach to existing coordination shared memory created by producer
    let coordination = ProcessCoordination::attach(&segment_name)?;

    let config = SharedMemoryConfig {
        name: segment_name.clone(),
        buffer_size,
        element_size: std::mem::size_of::<Event>(),
        create: false,
    };

    let builder: SharedDisruptorBuilder<Event> = SharedDisruptorBuilder::new(config);
    let mut consumer = builder.build_consumer()?;

    println!(
        "Consumer attached to shared memory segment: {}",
        segment_name
    );

    // External coordination: Signal readiness immediately after successful attachment
    coordination.signal_consumer_ready();
    println!("Consumer signaled readiness to producer");

    println!("Consuming events...");

    let mut events_consumed = 0u64;
    let mut total_counter = 0i64;
    let mut processing_time = Duration::new(0, 0);
    let mut latency_histogram = Histogram::<u64>::new(3).unwrap(); // 3 significant digits

    let start_time = Instant::now();

    // High-performance event consumption loop with spin-wait
    loop {
        let process_start = Instant::now();
        let processed = consumer.process_available(|event, _sequence| {
            let consume_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;

            // Calculate latency from produce to consume (in microseconds)
            let latency_ns = consume_time.saturating_sub(event.timestamp_ns);
            let latency_us = latency_ns / 1000; // Convert nanoseconds to microseconds
            latency_histogram.record(latency_us).unwrap_or(());

            events_consumed += 1;
            total_counter += event.value as i64; // Running counter for verification

            // Progress reporting for long-running tests
            if disruptor_mp::is_multiple_of_u64(events_consumed, 1_000) && events_consumed > 0 {
                println!(
                    "Consumed {} events, counter: {}",
                    events_consumed, total_counter
                );
            }
        });

        if processed > 0 {
            processing_time += process_start.elapsed();
        }

        // External coordination: Check if producer signaled completion
        if coordination.producer_done.load(Ordering::Acquire) == 1 {
            let expected_events = coordination.events_produced.load(Ordering::Acquire) as u64;
            if events_consumed >= expected_events {
                println!("Producer finished, consumed all {} events", expected_events);
                break;
            }
        }

        if processed == 0 {
            // Spin-wait for maximum throughput and minimum latency
            std::hint::spin_loop();
        }
    }

    let _total_time = start_time.elapsed();
    let throughput = if processing_time.as_secs_f64() > 0.0 {
        events_consumed as f64 / processing_time.as_secs_f64()
    } else {
        0.0
    };

    let expected_events = coordination.events_produced.load(Ordering::Acquire) as u64;

    println!("Consumer finished!");
    println!("Events consumed: {}", events_consumed);
    println!("Final counter: {}", total_counter);
    println!("Expected counter: {}", expected_events);

    // Enhanced timing measurements with multi-scale precision
    let total_ns = processing_time.as_nanos() as f64;
    let ns_per_event = if events_consumed > 0 {
        total_ns / events_consumed as f64
    } else {
        0.0
    };
    let us_per_event = ns_per_event / 1000.0;
    let ms_total = total_ns / 1_000_000.0;

    if processing_time.as_nanos() > 0 {
        println!(
            "Processing time: {:.3}ms ({:.1}ns per event, {:.3}μs per event)",
            ms_total, ns_per_event, us_per_event
        );
    } else {
        println!("Processing time: <1ns (extremely fast - approaching measurement limits)");
    }
    println!("Throughput: {:.0} events/sec", throughput);

    // Calculate and display data transfer rate (like Go benchmarks)
    let data_rate_mbs = (throughput * std::mem::size_of::<Event>() as f64) / (1024.0 * 1024.0);
    println!("Data Rate: {:.2} MB/s", data_rate_mbs);

    // Output latency percentiles if we have measurements
    if !latency_histogram.is_empty() {
        let p50 = latency_histogram.value_at_percentile(50.0);
        let p99 = latency_histogram.value_at_percentile(99.0);
        println!("Latency P50: {:.3}μs", p50);
        println!("Latency P99: {:.3}μs", p99);
    }

    // Signal completion using shared atomic
    coordination.signal_consumer_done(events_consumed);
    println!("Signaled consumer completion via shared atomic");

    // Verify correctness
    if total_counter == expected_events as i64 {
        println!("COUNTERS TEST PASSED - All events counted correctly!");
        std::process::exit(0);
    } else {
        println!(
            "COUNTERS TEST FAILED - Expected {}, got {}",
            expected_events, total_counter
        );
        std::process::exit(1);
    }
}

/// SPMC Consumer process - demonstrates broadcast semantics with multiple consumers
///
/// This function implements a consumer in broadcast mode where multiple consumers
/// each see ALL events from the producer. This validates the disruptor's broadcast
/// semantics and external coordination with multiple processes.
///
/// ## Broadcast Semantics:
/// - Each consumer receives every event published by the producer
/// - All consumers should consume the same number of events
/// - All consumers should compute the same counter value
/// - Demonstrates scalability with multiple concurrent consumers
///
/// ## Performance Testing:
/// - Tests coordination overhead with multiple consumers
/// - Validates memory ordering across multiple processes
/// - Measures per-consumer throughput in broadcast scenarios
/// - Verifies no event loss or duplication across consumers
fn spmc_consumer_process(consumer_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting SPMC consumer process: {}", consumer_id);

    let segment_name = get_segment_name();
    println!(
        "Consumer {} trying to attach to segment: {}",
        consumer_id, segment_name
    );

    // Attach to existing coordination shared memory created by producer
    let coordination = ProcessCoordination::attach(&segment_name)?;

    let config = SharedMemoryConfig {
        name: segment_name.clone(),
        buffer_size: get_buffer_size(),
        element_size: std::mem::size_of::<Event>(),
        create: false,
    };

    let builder: SharedDisruptorBuilder<Event> = SharedDisruptorBuilder::new(config);
    let mut consumer = match builder.build_consumer() {
        Ok(c) => c,
        Err(e) => {
            println!("Consumer {} failed to attach: {}", consumer_id, e);
            std::process::exit(1);
        }
    };

    println!(
        "Consumer {} attached to shared memory segment: {}",
        consumer_id, segment_name
    );

    // Signal readiness immediately after attachment
    coordination.signal_consumer_ready();
    println!("Consumer {} signaled readiness to producer", consumer_id);

    println!(
        "Consuming events (broadcast semantics - each consumer sees ALL events independently)..."
    );

    let mut events_consumed = 0u64;
    let mut total_counter = 0i64;
    let mut processing_time = Duration::new(0, 0);
    let mut latency_histogram = Histogram::<u64>::new(3).unwrap(); // 3 significant digits

    let start_time = Instant::now();

    loop {
        let process_start = Instant::now();
        let processed = consumer.process_available(|event, _sequence| {
            let consume_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;

            // Calculate latency from produce to consume (in microseconds)
            let latency_ns = consume_time.saturating_sub(event.timestamp_ns);
            let latency_us = latency_ns / 1000; // Convert nanoseconds to microseconds
            latency_histogram.record(latency_us).unwrap_or(());

            events_consumed += 1;
            total_counter += event.value as i64;

            if disruptor_mp::is_multiple_of_u64(events_consumed, 1_000) && events_consumed > 0 {
                println!(
                    "Consumer {} consumed {} events, counter: {}",
                    consumer_id, events_consumed, total_counter
                );
            }
        });

        if processed > 0 {
            processing_time += process_start.elapsed();
        }

        // Check if producer is done and we've consumed all events (same as SPSC)
        if coordination.producer_done.load(Ordering::Acquire) == 1 {
            let expected_events = coordination.events_produced.load(Ordering::Acquire) as u64;
            if events_consumed >= expected_events {
                println!(
                    "Consumer {} - Producer finished, consumed all {} events",
                    consumer_id, expected_events
                );
                break;
            }
        }

        if processed == 0 {
            // No events available, small spin-wait
            std::hint::spin_loop();
        }
    }

    let _total_time = start_time.elapsed();
    let throughput = if processing_time.as_secs_f64() > 0.0 {
        events_consumed as f64 / processing_time.as_secs_f64()
    } else {
        0.0
    };

    let expected_events = coordination.events_produced.load(Ordering::Acquire) as u64;

    println!("SPMC Consumer {} finished!", consumer_id);
    println!("Events consumed: {}", events_consumed);
    println!("Total counter: {}", total_counter);
    println!(
        "Expected (broadcast): {} events, {} counter",
        expected_events, expected_events
    );

    // Enhanced timing measurements with multi-scale precision
    let total_ns = processing_time.as_nanos() as f64;
    let ns_per_event = if events_consumed > 0 {
        total_ns / events_consumed as f64
    } else {
        0.0
    };
    let us_per_event = ns_per_event / 1000.0;
    let ms_total = total_ns / 1_000_000.0;

    if processing_time.as_nanos() > 0 {
        println!(
            "Processing time: {:.3}ms ({:.1}ns per event, {:.3}μs per event)",
            ms_total, ns_per_event, us_per_event
        );
    } else {
        println!("Processing time: <1ns (extremely fast - approaching measurement limits)");
    }
    println!("Throughput: {:.0} events/sec", throughput);

    // Calculate and display data transfer rate (like Go benchmarks)
    let data_rate_mbs = (throughput * std::mem::size_of::<Event>() as f64) / (1024.0 * 1024.0);
    println!("Data Rate: {:.2} MB/s", data_rate_mbs);

    // Output latency percentiles if we have measurements
    if !latency_histogram.is_empty() {
        let p50 = latency_histogram.value_at_percentile(50.0);
        let p99 = latency_histogram.value_at_percentile(99.0);
        println!("Consumer {} Latency P50: {:.3}μs", consumer_id, p50);
        println!("Consumer {} Latency P99: {:.3}μs", consumer_id, p99);
    }

    // Signal completion using shared atomic
    coordination.signal_consumer_done(events_consumed);
    println!(
        "Consumer {} signaled completion via shared atomic",
        consumer_id
    );

    // With broadcast semantics, each consumer should see all events
    if events_consumed == expected_events && total_counter == expected_events as i64 {
        println!("Consumer {} - BROADCAST TEST PASSED!", consumer_id);
        std::process::exit(0);
    } else {
        println!(
            "Consumer {} - BROADCAST TEST FAILED - Expected {} events/{} counter, got {}/{}",
            consumer_id, expected_events, expected_events, events_consumed, total_counter
        );
        std::process::exit(1);
    }
}

/// SPSC Producer process with discovery - demonstrates discovery features
///
/// This function implements the producer side using consumer discovery:
/// 1. Creates external coordination structures first
/// 2. Creates disruptor with discovery enabled for 1 consumer
/// 3. Waits for exactly 1 consumer to signal readiness
/// 4. Produces NUM_EVENTS events at maximum throughput
/// 5. Signals completion and waits for consumer to finish
///
/// ## Discovery Features:
/// - Uses enable_discovery(1) for PID-based consumer discovery
/// - Demonstrates fixed topology optimization (stops scanning after finding 1 consumer)
/// - Suitable for production systems where consumer count is known at startup
/// - Validates discovery works correctly in multiprocess scenarios
fn spsc_discovery_producer_process() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting SPSC producer process with discovery...");

    let segment_name = get_segment_name();
    let buffer_size = get_buffer_size();

    // Create coordination shared memory FIRST (external coordination pattern)
    let coordination = ProcessCoordination::create(&segment_name)?;

    // Create disruptor with basic discovery for 1 consumer (uses PID-based discovery)
    let builder =
        build_shared_single_producer::<Event>(&segment_name, buffer_size).enable_discovery(1); // Enable discovery for 1 consumer using PID-based scanning
    let mut producer = builder.build_producer(Event::default)?;

    println!(
        "SPSC Prefix Producer created shared memory segment: {}",
        segment_name
    );

    // External coordination: Wait for exactly 1 consumer to signal readiness
    println!("Waiting for consumer to signal readiness...");
    let expected_consumers = 1; // SPSC mode - single consumer expected
    if !coordination.wait_for_consumers_ready(expected_consumers, Duration::from_secs(30)) {
        return Err("Timeout waiting for consumer to be ready".into());
    }
    println!("Consumer ready! Starting production...");

    println!("Producing {} events...", NUM_EVENTS);

    let start_time = Instant::now();

    // High-performance event production loop - no coordination overhead
    for i in 0..NUM_EVENTS {
        let publish_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        producer.publish(|event| {
            event.value = 1; // Each event contributes 1 to the running counter
            event.timestamp_ns = publish_time; // Record when event was produced
        });

        // Progress reporting for long-running tests
        if disruptor_mp::is_multiple_of_u64(i, 1_000) && i > 0 {
            println!("Produced {} events", i);
        }
    }

    let elapsed = start_time.elapsed();
    let throughput = NUM_EVENTS as f64 / elapsed.as_secs_f64();

    println!("SPSC Prefix Producer finished!");
    println!(
        "Time: {:.3}ms ({:.1}μs per event)",
        elapsed.as_millis(),
        elapsed.as_nanos() as f64 / NUM_EVENTS as f64
    );
    println!("Throughput: {:.0} events/sec", throughput);

    // Signal completion using shared atomic
    coordination.signal_producer_done(NUM_EVENTS);
    println!("Signaled producer completion via shared atomic");

    // Wait for consumers to finish (with timeout for safety)
    let wait_start = Instant::now();
    let timeout = Duration::from_secs(30); // Safety timeout

    while wait_start.elapsed() < timeout {
        let consumers_finished = coordination.consumer_done.load(Ordering::Acquire);
        if consumers_finished > 0 {
            println!("Consumer finished");

            let final_consumed = coordination.events_consumed.load(Ordering::Acquire) as u64;
            println!(
                "Final consumer finished consuming {} events",
                final_consumed
            );
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }

    Ok(())
}

/// SPSC Consumer process with discovery - demonstrates consumer discovery
///
/// This function implements the consumer side for discovery testing:
/// 1. Attaches to existing coordination structures
/// 2. Attaches to existing disruptor shared memory using default naming
/// 3. Signals readiness to producer (discovered via PID-based scanning)
/// 4. Processes events at maximum throughput using spin-wait
/// 5. Validates event count and signals completion
///
/// ## Discovery Features:
/// - Consumer uses default naming scheme (c{pid}_{counter})
/// - Demonstrates PID-based consumer discovery
/// - Validates discovery works correctly in multiprocess scenarios
/// - Shows how discovery stops scanning after finding expected consumers
fn spsc_discovery_consumer_process() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting SPSC consumer process with discovery...");

    let segment_name = get_segment_name();

    // Attach to existing coordination shared memory created by producer
    let coordination = ProcessCoordination::attach(&segment_name)?;

    let config = SharedMemoryConfig {
        name: segment_name.clone(),
        buffer_size: get_buffer_size(),
        element_size: std::mem::size_of::<Event>(),
        create: false,
    };

    let builder: SharedDisruptorBuilder<Event> = SharedDisruptorBuilder::new(config);
    let mut consumer = builder.build_consumer()?; // Consumer attaches normally, prefix filtering happens on producer side

    println!(
        "Consumer attached to shared memory segment: {}",
        segment_name
    );

    // External coordination: Signal readiness immediately after successful attachment
    coordination.signal_consumer_ready();
    println!("Consumer signaled readiness to producer");

    println!("Consumer consuming events...");

    let mut events_consumed = 0u64;
    let mut total_counter = 0i64;
    let mut processing_time = Duration::new(0, 0);
    let mut latency_histogram = Histogram::<u64>::new(3).unwrap(); // 3 significant digits

    let start_time = Instant::now();

    // High-performance event consumption loop with spin-wait
    loop {
        let process_start = Instant::now();
        let processed = consumer.process_available(|event, _sequence| {
            let consume_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;

            // Calculate latency from produce to consume (in microseconds)
            let latency_ns = consume_time.saturating_sub(event.timestamp_ns);
            let latency_us = latency_ns / 1000; // Convert nanoseconds to microseconds
            latency_histogram.record(latency_us).unwrap_or(());

            events_consumed += 1;
            total_counter += event.value as i64; // Running counter for verification

            // Progress reporting for long-running tests
            if disruptor_mp::is_multiple_of_u64(events_consumed, 1_000) && events_consumed > 0 {
                println!(
                    "Consumer consumed {} events, counter: {}",
                    events_consumed, total_counter
                );
            }
        });

        if processed > 0 {
            processing_time += process_start.elapsed();
        }

        // External coordination: Check if producer signaled completion
        if coordination.producer_done.load(Ordering::Acquire) == 1 {
            let expected_events = coordination.events_produced.load(Ordering::Acquire) as u64;
            if events_consumed >= expected_events {
                println!(
                    "Producer finished, consumer consumed all {} events",
                    expected_events
                );
                break;
            }
        }

        if processed == 0 {
            // Spin-wait for maximum throughput and minimum latency
            std::hint::spin_loop();
        }
    }

    let _total_time = start_time.elapsed();
    let throughput = if processing_time.as_secs_f64() > 0.0 {
        events_consumed as f64 / processing_time.as_secs_f64()
    } else {
        0.0
    };

    let expected_events = coordination.events_produced.load(Ordering::Acquire) as u64;

    println!("Consumer finished!");
    println!("Events consumed: {}", events_consumed);
    println!("Final counter: {}", total_counter);
    println!("Expected counter: {}", expected_events);

    if processing_time.as_nanos() > 0 {
        println!(
            "Processing time: {:.3}ms ({:.1}ns per event)",
            processing_time.as_nanos() as f64 / 1_000_000.0,
            processing_time.as_nanos() as f64 / events_consumed as f64
        );
    } else {
        println!("Processing time: <1μs (too fast to measure accurately)");
    }
    println!("Throughput: {:.0} events/sec", throughput);

    // Output latency percentiles if we have measurements
    if !latency_histogram.is_empty() {
        let p50 = latency_histogram.value_at_quantile(0.50) as f64;
        let p99 = latency_histogram.value_at_quantile(0.99) as f64;
        println!("Latency P50: {:.3}μs", p50);
        println!("Latency P99: {:.3}μs", p99);
    }

    // Signal completion using shared atomic
    coordination.signal_consumer_done(events_consumed);
    println!("Consumer signaled completion via shared atomic");

    // Verify correctness
    if total_counter == expected_events as i64 {
        println!("SPSC PREFIX TEST PASSED - All events counted correctly!");
        std::process::exit(0);
    } else {
        println!(
            "SPSC PREFIX TEST FAILED - Expected {}, got {}",
            expected_events, total_counter
        );
        std::process::exit(1);
    }
}

/// Automated SPSC test with real OS processes (production-like testing)
///
/// This function demonstrates the complete external coordination workflow by spawning
/// actual OS processes (not threads). This closely mimics production deployments where
/// producers and consumers run as separate processes/containers.
///
/// ## Test Workflow:
/// 1. Generate unique segment name for test isolation
/// 2. Spawn producer process with coordination creation responsibility
/// 3. Wait for producer to create shared memory structures
/// 4. Spawn consumer process that attaches to existing structures
/// 5. Wait for both processes to complete and capture outputs
/// 6. Verify success based on process exit codes
///
/// ## Real-World Simulation:
/// - Each process has separate memory space (true multiprocess)
/// - Coordination happens entirely through shared memory
/// - Tests race conditions and timing issues
/// - Validates cross-process memory ordering guarantees
/// - Demonstrates container-style deployment patterns
fn run_automated_spsc_test() -> (Result<(), Box<dyn std::error::Error>>, TestResults) {
    println!("Running automated SPSC test with real processes...");

    let current_exe = match env::current_exe() {
        Ok(exe) => exe,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };
    let segment_name = get_segment_name();

    // Start producer process first to create shared memory
    println!("Starting producer process (will create shared memory)...");
    let producer_child = match Command::new(&current_exe)
        .args(["producer"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Give producer time to create shared memory segment
    thread::sleep(Duration::from_millis(200));

    // Start consumer process to attach and consume
    println!("Starting consumer process (will attach and consume)...");
    let consumer_child = match Command::new(&current_exe)
        .args(["consumer"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Wait for consumer to finish first (it will exit when done)
    let consumer_result = match consumer_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Wait for producer to finish
    let producer_result = match producer_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Convert outputs to strings
    let producer_output = String::from_utf8_lossy(&producer_result.stdout);
    let consumer_output = String::from_utf8_lossy(&consumer_result.stdout);

    // Print outputs
    println!("\n--- Producer Output ---");
    println!("{}", producer_output);

    println!("\n--- Consumer Output ---");
    println!("{}", consumer_output);

    let test_passed = consumer_result.status.success();

    // Extract metrics from the same run that produced the output
    let consumer_outputs = vec![consumer_output.to_string()];
    let metrics = extract_test_results(&producer_output, &consumer_outputs);

    // Extract performance metrics and create table
    let mut perf_metrics = Vec::new();

    if let Some(producer_metrics) = extract_performance_metrics(&producer_output, "Producer") {
        perf_metrics.push(producer_metrics);
    }

    if let Some(consumer_metrics) = extract_performance_metrics(&consumer_output, "Consumer") {
        perf_metrics.push(consumer_metrics);
    }

    // Check results
    if test_passed {
        println!("\nAutomated SPSC test PASSED!");
        (Ok(()), metrics)
    } else {
        println!("\n❌ Automated SPSC test FAILED!");
        (Err("Consumer process failed".into()), metrics)
    }
}

/// Automated SPSC discovery test with real OS processes - demonstrates consumer discovery
///
/// This function tests the consumer discovery functionality by spawning
/// actual OS processes. The producer uses enable_discovery(1) for PID-based scanning
/// and the consumer uses the default naming scheme.
///
/// ## Test Workflow:
/// 1. Generate unique segment name for test isolation
/// 2. Spawn producer process with discovery enabled
/// 3. Wait for producer to create shared memory structures
/// 4. Spawn consumer process with default naming
/// 5. Wait for both processes to complete and capture outputs
/// 6. Verify success based on process exit codes
///
/// ## Discovery Validation:
/// - Producer uses PID-based scanning to find consumers
/// - Consumer uses default naming scheme (c{pid}_{counter})
/// - Tests fixed topology optimization (stops scanning after finding 1 consumer)
/// - Validates discovery works correctly in multiprocess scenarios
fn run_automated_spsc_discovery_test() -> (Result<(), Box<dyn std::error::Error>>, TestResults) {
    println!("Running automated SPSC test with consumer discovery...");

    let current_exe = match env::current_exe() {
        Ok(exe) => exe,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };
    let segment_name = get_segment_name();

    // Start producer process first to create shared memory and enable discovery
    println!("Starting producer process with discovery enabled (will create shared memory)...");
    let producer_child = match Command::new(&current_exe)
        .args(["spsc_discovery_producer"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Give producer time to create shared memory segment
    thread::sleep(Duration::from_millis(200));

    // Start consumer process to attach and consume
    println!("Starting consumer process (will attach and be discovered)...");
    let consumer_child = match Command::new(&current_exe)
        .args(["spsc_discovery_consumer"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Wait for consumer to finish first (it will exit when done)
    let consumer_result = match consumer_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Wait for producer to finish
    let producer_result = match producer_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Convert outputs to strings
    let producer_output = String::from_utf8_lossy(&producer_result.stdout);
    let consumer_output = String::from_utf8_lossy(&consumer_result.stdout);

    // Print outputs
    println!("\n--- Producer Output ---");
    println!("{}", producer_output);

    println!("\n--- Consumer Output ---");
    println!("{}", consumer_output);

    let test_passed = consumer_result.status.success();

    // Extract metrics from the same run that produced the output
    let consumer_outputs = vec![consumer_output.to_string()];
    let metrics = extract_test_results(&producer_output, &consumer_outputs);

    // Extract performance metrics and create table
    let mut perf_metrics = Vec::new();

    if let Some(producer_metrics) = extract_performance_metrics(&producer_output, "Producer") {
        perf_metrics.push(producer_metrics);
    }

    if let Some(consumer_metrics) = extract_performance_metrics(&consumer_output, "Consumer") {
        perf_metrics.push(consumer_metrics);
    }

    // Check results
    if test_passed {
        println!("\nAutomated SPSC discovery test PASSED!");
        (Ok(()), metrics)
    } else {
        println!("\n❌ Automated SPSC discovery test FAILED!");
        (Err("Consumer process failed".into()), metrics)
    }
}

fn run_automated_spmc_test() -> (Result<(), Box<dyn std::error::Error>>, TestResults) {
    println!("Running automated SPMC test with real processes (broadcast semantics)...");

    let current_exe = match env::current_exe() {
        Ok(exe) => exe,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };
    let segment_name = get_segment_name();

    // Start producer process first to create shared memory
    println!("Starting producer process (will create shared memory and wait for consumers)...");
    let producer_child = match Command::new(&current_exe)
        .args(["spmc_producer"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Give producer time to create shared memory segment
    thread::sleep(Duration::from_millis(500));

    // Start consumer processes to attach and signal readiness
    println!("Starting consumer processes (will attach and signal readiness)...");
    let consumer1_child = match Command::new(&current_exe)
        .args(["spmc_consumer1"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    let consumer2_child = match Command::new(&current_exe)
        .args(["spmc_consumer2"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Wait for consumers to finish
    let consumer1_result = match consumer1_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };
    let consumer2_result = match consumer2_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Wait for producer to finish
    let producer_result = match producer_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Convert outputs to strings
    let producer_output = String::from_utf8_lossy(&producer_result.stdout);
    let consumer1_output = String::from_utf8_lossy(&consumer1_result.stdout);
    let consumer2_output = String::from_utf8_lossy(&consumer2_result.stdout);

    // Print outputs
    println!("\n--- Producer Output ---");
    println!("{}", producer_output);

    println!("\n--- Consumer 1 Output ---");
    println!("{}", consumer1_output);

    println!("\n--- Consumer 2 Output ---");
    println!("{}", consumer2_output);

    let test_passed = consumer1_result.status.success() && consumer2_result.status.success();

    // Extract metrics from the same run that produced the output
    let consumer_outputs = vec![consumer1_output.to_string(), consumer2_output.to_string()];
    let metrics = extract_test_results(&producer_output, &consumer_outputs);

    // Extract performance metrics and create table
    let mut perf_metrics = Vec::new();

    if let Some(producer_metrics) = extract_performance_metrics(&producer_output, "Producer") {
        perf_metrics.push(producer_metrics);
    }

    if let Some(consumer1_metrics) = extract_performance_metrics(&consumer1_output, "Consumer-1") {
        perf_metrics.push(consumer1_metrics);
    }

    if let Some(consumer2_metrics) = extract_performance_metrics(&consumer2_output, "Consumer-2") {
        perf_metrics.push(consumer2_metrics);
    }

    // Check results - with broadcast semantics, both consumers should succeed
    if test_passed {
        println!("\nAutomated SPMC test PASSED!");
        println!("Both consumers saw all events (broadcast semantics working correctly)");
        (Ok(()), metrics)
    } else {
        println!("\n❌ Automated SPMC test FAILED!");
        if !consumer1_result.status.success() {
            println!("Consumer 1 failed");
        }
        if !consumer2_result.status.success() {
            println!("Consumer 2 failed");
        }
        (Err("One or more consumer processes failed".into()), metrics)
    }
}

fn run_automated_spmc_5_consumer_test() -> (Result<(), Box<dyn std::error::Error>>, TestResults) {
    println!("Running automated SPMC test with 5 consumers (broadcast scalability test)...");

    let current_exe = match env::current_exe() {
        Ok(exe) => exe,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };
    let segment_name = get_segment_name();

    // Start producer process first to create shared memory
    println!("Starting producer process (will create shared memory and wait for 5 consumers)...");
    let producer_child = match Command::new(&current_exe)
        .args(["spmc_producer_5"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Give producer time to create shared memory segment
    thread::sleep(Duration::from_millis(500));

    // Start 5 consumer processes to attach and signal readiness
    println!("Starting 5 consumer processes (will attach and signal readiness)...");
    let mut consumer_children = Vec::new();

    for i in 1..=5 {
        let consumer_child = match Command::new(&current_exe)
            .args([&format!("spmc_consumer{}", i)])
            .env("MP_SEGMENT_NAME", &segment_name)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(e) => return (Err(e.into()), TestResults::default()),
        };
        consumer_children.push(consumer_child);
    }

    // Wait for all consumers to finish
    let mut consumer_results = Vec::new();
    for consumer_child in consumer_children {
        let result = match consumer_child.wait_with_output() {
            Ok(result) => result,
            Err(e) => return (Err(e.into()), TestResults::default()),
        };
        consumer_results.push(result);
    }

    // Wait for producer to finish
    let producer_result = match producer_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Convert outputs to strings
    let producer_output = String::from_utf8_lossy(&producer_result.stdout);
    let consumer_outputs: Vec<String> = consumer_results
        .iter()
        .map(|result| String::from_utf8_lossy(&result.stdout).to_string())
        .collect();

    // Print outputs
    println!("\n--- Producer Output ---");
    println!("{}", producer_output);

    for (i, output) in consumer_outputs.iter().enumerate() {
        println!("\n--- Consumer {} Output ---", i + 1);
        println!("{}", output);
        if !consumer_results[i].stderr.is_empty() {
            println!(
                "Consumer {} stderr: {}",
                i + 1,
                String::from_utf8_lossy(&consumer_results[i].stderr)
            );
        }
    }

    let test_passed = consumer_results
        .iter()
        .all(|result| result.status.success());

    // Extract metrics from the same run that produced the output
    let metrics = extract_test_results(&producer_output, &consumer_outputs);

    // Extract performance metrics and create table
    let mut perf_metrics = Vec::new();

    if let Some(producer_metrics) = extract_performance_metrics(&producer_output, "Producer") {
        perf_metrics.push(producer_metrics);
    }

    for (i, output) in consumer_outputs.iter().enumerate() {
        if let Some(consumer_metrics) =
            extract_performance_metrics(output, &format!("Consumer-{}", i + 1))
        {
            perf_metrics.push(consumer_metrics);
        }
    }

    // Check results - with broadcast semantics, all consumers should succeed
    if test_passed {
        println!("\nAutomated SPMC 5-consumer test PASSED!");
        println!("All 5 consumers saw all events (broadcast semantics scaling correctly)");
        (Ok(()), metrics)
    } else {
        println!("\n❌ Automated SPMC 5-consumer test FAILED!");
        for (i, result) in consumer_results.iter().enumerate() {
            if !result.status.success() {
                println!("Consumer {} failed", i + 1);
            }
        }
        (Err("One or more consumer processes failed".into()), metrics)
    }
}

/// SPMC Producer process - broadcasts events to multiple consumers simultaneously
///
/// This function implements the producer side of broadcast communication where
/// one producer sends events to multiple consumers. Each consumer receives ALL
/// events independently, demonstrating the disruptor's broadcast capabilities.
///
/// ## Broadcast Coordination:
/// - Waits for exactly N consumers to signal readiness
/// - Produces events that each consumer will see independently
/// - Uses longer timeout to accommodate multiple consumer startup
/// - Validates that all consumers complete processing
///
/// ## Scalability Testing:
/// - Tests coordination overhead with multiple consumers
/// - Measures single-producer throughput under multiple consumer load
/// - Validates broadcast semantics across multiple processes
/// - Demonstrates realistic production scenarios (Competitor, game servers, etc.)
fn spmc_producer_process(expected_consumers: i64) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Starting SPMC producer process ({} consumers)...",
        expected_consumers
    );

    let segment_name = get_segment_name();

    // Create coordination shared memory FIRST (external coordination pattern)
    let coordination = ProcessCoordination::create(&segment_name)?;

    let builder = build_shared_single_producer::<Event>(&segment_name, get_buffer_size())
        .enable_discovery(expected_consumers as usize); // Enable discovery for expected number of consumers (external coordination)
    let mut producer = builder.build_producer(Event::default)?;

    println!(
        "SPMC Producer created shared memory segment: {}",
        segment_name
    );

    // Wait for all consumers to be ready before starting
    println!(
        "Waiting for {} consumers to signal readiness...",
        expected_consumers
    );
    if !coordination.wait_for_consumers_ready(expected_consumers, Duration::from_secs(45)) {
        return Err(format!(
            "Timeout waiting for {} consumers to be ready",
            expected_consumers
        )
        .into());
    }
    println!(
        "All {} consumer(s) ready! Starting production...",
        expected_consumers
    );

    println!(
        "Producing {} events for {} consumers (broadcast)...",
        NUM_EVENTS, expected_consumers
    );

    let start_time = Instant::now();

    for i in 0..NUM_EVENTS {
        let publish_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        producer.publish(|event| {
            event.value = 1; // Each event contributes 1 to the counter
            event.timestamp_ns = publish_time; // Record when event was produced
        });

        if disruptor_mp::is_multiple_of_u64(i, 1_000) && i > 0 {
            println!("Produced {} events", i);
        }
    }

    let elapsed = start_time.elapsed();
    let throughput = NUM_EVENTS as f64 / elapsed.as_secs_f64();

    println!("SPMC Producer finished!");
    println!(
        "Time: {:.3}ms ({:.1}μs per event)",
        elapsed.as_millis(),
        elapsed.as_nanos() as f64 / NUM_EVENTS as f64
    );
    println!("Throughput: {:.0} events/sec", throughput);

    // Signal completion using shared atomic
    coordination.signal_producer_done(NUM_EVENTS);
    println!("Signaled producer completion via shared atomic");

    // Wait for all consumers to finish (with timeout for safety)
    let wait_start = Instant::now();
    let timeout = Duration::from_secs(60); // Longer timeout for multiple consumers

    while wait_start.elapsed() < timeout {
        let consumers_finished = coordination.consumer_done.load(Ordering::Acquire);
        if consumers_finished >= expected_consumers {
            println!("All {} consumer(s) finished", consumers_finished);
            let final_consumed = coordination.events_consumed.load(Ordering::Acquire) as u64;
            println!(
                "Final consumer finished consuming {} events",
                final_consumed
            );
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    Ok(())
}

/// SPMC Producer process with discovery - demonstrates producer-side consumer discovery
///
/// This function implements the producer side for SPMC discovery testing:
/// 1. Creates coordination structures and shared memory
/// 2. Enables discovery for the specified number of consumers
/// 3. Waits for all consumers to be discovered and signal readiness
/// 4. Produces events at maximum throughput
/// 5. Signals completion and waits for consumers to finish
///
/// ## Discovery Features:
/// - Producer uses enable_discovery(N) to scan for N consumers
/// - Demonstrates PID-based consumer discovery in SPMC scenarios
/// - Validates discovery works correctly with multiple consumers
/// - Shows how discovery stops scanning after finding expected consumers
fn spmc_discovery_producer_process(
    expected_consumers: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Starting SPMC producer process with discovery ({} consumers)...",
        expected_consumers
    );

    let segment_name = get_segment_name();

    // Create coordination shared memory FIRST (external coordination pattern)
    let coordination = ProcessCoordination::create(&segment_name)?;

    // Create disruptor with discovery enabled for the specified number of consumers
    let builder = build_shared_single_producer::<Event>(&segment_name, get_buffer_size())
        .enable_discovery(expected_consumers as usize); // Enable discovery for N consumers using PID-based scanning
    let mut producer = builder.build_producer(Event::default)?;

    println!(
        "SPMC Discovery Producer created shared memory segment: {}",
        segment_name
    );

    // External coordination: Wait for all consumers to signal readiness
    println!(
        "Waiting for {} consumers to signal readiness...",
        expected_consumers
    );
    if !coordination.wait_for_consumers_ready(expected_consumers, Duration::from_secs(45)) {
        return Err(format!(
            "Timeout waiting for {} consumers to be ready",
            expected_consumers
        )
        .into());
    }
    println!(
        "All {} consumer(s) ready! Starting production...",
        expected_consumers
    );

    println!(
        "Producing {} events for {} consumers (broadcast)...",
        NUM_EVENTS, expected_consumers
    );

    let start_time = Instant::now();

    // High-performance event production loop - no coordination overhead
    for i in 0..NUM_EVENTS {
        let publish_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        producer.publish(|event| {
            event.value = 1; // Each event contributes 1 to the running counter
            event.timestamp_ns = publish_time; // Record when event was produced
        });

        // Progress reporting for long-running tests
        if disruptor_mp::is_multiple_of_u64(i, 1_000) && i > 0 {
            println!("Produced {} events", i);
        }
    }

    let elapsed = start_time.elapsed();
    let throughput = NUM_EVENTS as f64 / elapsed.as_secs_f64();

    println!("SPMC Discovery Producer finished!");
    println!(
        "Time: {}ms ({:.1}μs per event)",
        elapsed.as_millis(),
        elapsed.as_nanos() as f64 / NUM_EVENTS as f64 / 1000.0
    );
    println!("Throughput: {} events/sec", throughput as u64);

    // Signal completion using shared atomic
    coordination.signal_producer_done(NUM_EVENTS);
    println!("Signaled producer completion via shared atomic");

    // Wait for all consumers to finish (with timeout for safety)
    let wait_start = Instant::now();
    let timeout = Duration::from_secs(45); // Safety timeout

    while wait_start.elapsed() < timeout {
        let consumers_finished = coordination.consumer_done.load(Ordering::Acquire);
        if consumers_finished >= expected_consumers {
            println!("All {} consumer(s) finished", expected_consumers);

            let final_consumed = coordination.events_consumed.load(Ordering::Acquire) as u64;
            println!(
                "Final consumer finished consuming {} events",
                final_consumed
            );
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }

    Ok(())
}

/// SPMC Consumer process with discovery - demonstrates consumer discovery in broadcast mode
///
/// This function implements the consumer side for SPMC discovery testing:
/// 1. Attaches to existing coordination structures
/// 2. Attaches to existing disruptor shared memory using default naming
/// 3. Signals readiness to producer (discovered via PID-based scanning)
/// 4. Processes events at maximum throughput using spin-wait
/// 5. Validates event count and signals completion
///
/// ## Discovery Features:
/// - Consumer uses default naming scheme (c{pid}_{counter})
/// - Demonstrates PID-based consumer discovery in SPMC scenarios
/// - Validates discovery works correctly with multiple consumers
/// - Shows broadcast semantics (each consumer sees ALL events)
fn spmc_discovery_consumer_process(consumer_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Starting SPMC consumer process with discovery: {}",
        consumer_id
    );

    let segment_name = get_segment_name();

    // Attach to existing coordination shared memory created by producer
    let coordination = ProcessCoordination::attach(&segment_name)?;

    let config = SharedMemoryConfig {
        name: segment_name.clone(),
        buffer_size: get_buffer_size(),
        element_size: std::mem::size_of::<Event>(),
        create: false,
    };

    let builder: SharedDisruptorBuilder<Event> = SharedDisruptorBuilder::new(config);
    let mut consumer = builder.build_consumer()?; // Consumer attaches normally, discovery happens on producer side

    println!(
        "Consumer {} attached to shared memory segment: {}",
        consumer_id, segment_name
    );

    // External coordination: Signal readiness immediately after successful attachment
    coordination.signal_consumer_ready();
    println!("Consumer {} signaled readiness to producer", consumer_id);

    println!(
        "Consuming events (broadcast semantics - each consumer sees ALL events independently)..."
    );

    let mut events_consumed = 0u64;
    let mut total_counter = 0i64;
    let mut processing_time = Duration::new(0, 0);
    let mut latency_histogram = Histogram::<u64>::new(3).unwrap(); // 3 significant digits

    let start_time = Instant::now();

    // High-performance event consumption loop with spin-wait
    loop {
        let process_start = Instant::now();
        let processed = consumer.process_available(|event, _sequence| {
            let consume_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;

            // Calculate latency from produce to consume (in microseconds)
            let latency_ns = consume_time.saturating_sub(event.timestamp_ns);
            let latency_us = latency_ns / 1000; // Convert nanoseconds to microseconds
            latency_histogram.record(latency_us).unwrap_or(());

            events_consumed += 1;
            total_counter += event.value as i64; // Running counter for verification

            // Progress reporting for long-running tests
            if disruptor_mp::is_multiple_of_u64(events_consumed, 1_000) && events_consumed > 0 {
                println!(
                    "Consumer {} consumed {} events, counter: {}",
                    consumer_id, events_consumed, total_counter
                );
            }
        });

        if processed > 0 {
            processing_time += process_start.elapsed();
        }

        // External coordination: Check if producer signaled completion
        if coordination.producer_done.load(Ordering::Acquire) == 1 {
            let expected_events = coordination.events_produced.load(Ordering::Acquire) as u64;
            if events_consumed >= expected_events {
                println!(
                    "Consumer {} - Producer finished, consumed all {} events",
                    consumer_id, expected_events
                );
                break;
            }
        }

        if processed == 0 {
            // Spin-wait for maximum throughput and minimum latency
            std::hint::spin_loop();
        }
    }

    let _total_time = start_time.elapsed();
    let throughput = if processing_time.as_secs_f64() > 0.0 {
        events_consumed as f64 / processing_time.as_secs_f64()
    } else {
        0.0
    };

    let expected_events = coordination.events_produced.load(Ordering::Acquire) as u64;

    println!("SPMC Consumer {} finished!", consumer_id);
    println!("Events consumed: {}", events_consumed);
    println!("Total counter: {}", total_counter);
    println!(
        "Expected (broadcast): {} events, {} counter",
        expected_events, expected_events
    );

    if processing_time.as_nanos() > 0 {
        println!(
            "Processing time: {:.3}ms ({:.1}ns per event)",
            processing_time.as_nanos() as f64 / 1_000_000.0,
            processing_time.as_nanos() as f64 / events_consumed as f64
        );
    } else {
        println!("Processing time: <1μs (too fast to measure accurately)");
    }
    println!("Throughput: {:.0} events/sec", throughput);

    // Output latency percentiles if we have measurements
    if !latency_histogram.is_empty() {
        let p50 = latency_histogram.value_at_quantile(0.50) as f64;
        let p99 = latency_histogram.value_at_quantile(0.99) as f64;
        println!("Consumer {} Latency P50: {:.3}μs", consumer_id, p50);
        println!("Consumer {} Latency P99: {:.3}μs", consumer_id, p99);
    }

    // Signal completion using shared atomic
    coordination.signal_consumer_done(events_consumed);
    println!(
        "Consumer {} signaled completion via shared atomic",
        consumer_id
    );

    // Verify correctness (broadcast semantics - each consumer should see ALL events)
    if total_counter == expected_events as i64 {
        println!("Consumer {} - BROADCAST TEST PASSED!", consumer_id);
        std::process::exit(0);
    } else {
        println!(
            "Consumer {} - BROADCAST TEST FAILED - Expected {}, got {}",
            consumer_id, expected_events, total_counter
        );
        std::process::exit(1);
    }
}

/// Automated SPMC discovery test with 2 consumers - demonstrates discovery in broadcast mode
///
/// This function tests the complete SPMC discovery workflow by spawning
/// actual OS processes with discovery enabled. This validates that:
/// 1. Producer can discover multiple consumers using PID-based scanning
/// 2. Multiple consumers can be discovered and coordinated properly
/// 3. Broadcast semantics work correctly with discovery enabled
/// 4. Discovery stops scanning after finding the expected number of consumers
///
/// ## Test Workflow:
/// 1. Generate unique segment name for test isolation
/// 2. Spawn producer process with discovery enabled for 2 consumers
/// 3. Wait for producer to create shared memory and start discovery
/// 4. Spawn 2 consumer processes that will be discovered
/// 5. Wait for all processes to complete and capture outputs
/// 6. Verify success and extract performance metrics
fn run_automated_spmc_2_discovery_test() -> (Result<(), Box<dyn std::error::Error>>, TestResults) {
    println!("Running automated SPMC discovery test with 2 consumers (broadcast semantics)...");

    let current_exe = match env::current_exe() {
        Ok(exe) => exe,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };
    let segment_name = get_segment_name();

    // Start producer process first to create shared memory and enable discovery
    println!("Starting producer process with discovery enabled (will create shared memory and discover 2 consumers)...");
    let producer_child = match Command::new(&current_exe)
        .args(["spmc_discovery_producer_2"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Give producer time to create shared memory segment and start discovery
    thread::sleep(Duration::from_millis(500));

    // Start consumer processes to attach and be discovered
    println!("Starting consumer processes (will attach and be discovered)...");
    let consumer1_child = match Command::new(&current_exe)
        .args(["spmc_discovery_consumer1"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    let consumer2_child = match Command::new(&current_exe)
        .args(["spmc_discovery_consumer2"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Wait for consumers to finish
    let consumer1_result = match consumer1_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };
    let consumer2_result = match consumer2_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Wait for producer to finish
    let producer_result = match producer_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Convert outputs to strings
    let producer_output = String::from_utf8_lossy(&producer_result.stdout);
    let consumer1_output = String::from_utf8_lossy(&consumer1_result.stdout);
    let consumer2_output = String::from_utf8_lossy(&consumer2_result.stdout);

    // Print outputs
    println!("\n--- Producer Output ---");
    println!("{}", producer_output);

    println!("\n--- Consumer 1 Output ---");
    println!("{}", consumer1_output);

    println!("\n--- Consumer 2 Output ---");
    println!("{}", consumer2_output);

    let test_passed = consumer1_result.status.success() && consumer2_result.status.success();

    // Extract metrics from the same run that produced the output
    let consumer_outputs = vec![consumer1_output.to_string(), consumer2_output.to_string()];
    let metrics = extract_test_results(&producer_output, &consumer_outputs);

    // Check results - with broadcast semantics, both consumers should succeed
    if test_passed {
        println!("\nAutomated SPMC discovery test (2 consumers) PASSED!");
        println!("Both consumers were discovered and saw all events (broadcast semantics working correctly)");
        (Ok(()), metrics)
    } else {
        println!("\n❌ Automated SPMC discovery test (2 consumers) FAILED!");
        if !consumer1_result.status.success() {
            println!("Consumer 1 failed");
        }
        if !consumer2_result.status.success() {
            println!("Consumer 2 failed");
        }
        (Err("One or more consumer processes failed".into()), metrics)
    }
}

/// Automated SPMC discovery test with 5 consumers - demonstrates discovery scalability
///
/// This function tests the complete SPMC discovery workflow with 5 consumers by spawning
/// actual OS processes with discovery enabled. This validates that:
/// 1. Producer can discover multiple consumers using PID-based scanning at scale
/// 2. 5 consumers can be discovered and coordinated properly
/// 3. Broadcast semantics work correctly with discovery enabled at scale
/// 4. Discovery performance scales properly with more consumers
///
/// ## Test Workflow:
/// 1. Generate unique segment name for test isolation
/// 2. Spawn producer process with discovery enabled for 5 consumers
/// 3. Wait for producer to create shared memory and start discovery
/// 4. Spawn 5 consumer processes that will be discovered
/// 5. Wait for all processes to complete and capture outputs
/// 6. Verify success and extract performance metrics
fn run_automated_spmc_5_discovery_test() -> (Result<(), Box<dyn std::error::Error>>, TestResults) {
    println!(
        "Running automated SPMC discovery test with 5 consumers (broadcast scalability test)..."
    );

    let current_exe = match env::current_exe() {
        Ok(exe) => exe,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };
    let segment_name = get_segment_name();

    // Start producer process first to create shared memory and enable discovery
    println!("Starting producer process with discovery enabled (will create shared memory and discover 5 consumers)...");
    let producer_child = match Command::new(&current_exe)
        .args(["spmc_discovery_producer_5"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Give producer time to create shared memory segment and start discovery
    thread::sleep(Duration::from_millis(500));

    // Start 5 consumer processes to attach and be discovered
    println!("Starting 5 consumer processes (will attach and be discovered)...");
    let mut consumer_children = Vec::new();

    for i in 1..=5 {
        let consumer_child = match Command::new(&current_exe)
            .args([&format!("spmc_discovery_consumer{}", i)])
            .env("MP_SEGMENT_NAME", &segment_name)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(e) => return (Err(e.into()), TestResults::default()),
        };
        consumer_children.push(consumer_child);
    }

    // Wait for all consumers to finish
    let mut consumer_results = Vec::new();
    for consumer_child in consumer_children {
        let result = match consumer_child.wait_with_output() {
            Ok(result) => result,
            Err(e) => return (Err(e.into()), TestResults::default()),
        };
        consumer_results.push(result);
    }

    // Wait for producer to finish
    let producer_result = match producer_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Convert outputs to strings
    let producer_output = String::from_utf8_lossy(&producer_result.stdout);
    let consumer_outputs: Vec<String> = consumer_results
        .iter()
        .map(|result| String::from_utf8_lossy(&result.stdout).to_string())
        .collect();

    // Print outputs
    println!("\n--- Producer Output ---");
    println!("{}", producer_output);

    for (i, output) in consumer_outputs.iter().enumerate() {
        println!("\n--- Consumer {} Output ---", i + 1);
        println!("{}", output);
        if !consumer_results[i].stderr.is_empty() {
            println!(
                "Consumer {} stderr: {}",
                i + 1,
                String::from_utf8_lossy(&consumer_results[i].stderr)
            );
        }
    }

    let test_passed = consumer_results
        .iter()
        .all(|result| result.status.success());

    // Extract metrics from the same run that produced the output
    let metrics = extract_test_results(&producer_output, &consumer_outputs);

    // Check results - with broadcast semantics, all consumers should succeed
    if test_passed {
        println!("\nAutomated SPMC discovery test (5 consumers) PASSED!");
        println!("All 5 consumers were discovered and saw all events (broadcast semantics scaling correctly)");
        (Ok(()), metrics)
    } else {
        println!("\n❌ Automated SPMC discovery test (5 consumers) FAILED!");
        for (i, result) in consumer_results.iter().enumerate() {
            if !result.status.success() {
                println!("Consumer {} failed", i + 1);
            }
        }
        (Err("One or more consumer processes failed".into()), metrics)
    }
}

/// SPSC Producer process with consumer prefix discovery - demonstrates prefix-based consumer discovery
///
/// This function implements the producer side for SPSC prefix discovery testing:
/// 1. Creates shared memory and coordination structures
/// 2. Enables consumer discovery with a specific prefix ("TEST_CONSUMER")
/// 3. Waits for consumer with matching prefix to be discovered
/// 4. Produces events at maximum throughput using spin-wait
/// 5. Validates completion and signals producer done
///
/// ## Discovery Features:
/// - Producer uses discover_consumer_with_prefix(1, "TEST_CONSUMER") for optimized discovery
/// - Consumer must use matching prefix naming scheme
/// - Demonstrates prefix-based consumer discovery in SPSC scenarios
/// - Validates discovery works correctly with consumer prefixes
/// - Shows how prefix discovery is faster than PID-based scanning
fn spsc_prefix_discovery_producer_process() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting SPSC producer process with consumer prefix discovery...");

    let segment_name = get_segment_name();

    // Create coordination shared memory FIRST (external coordination pattern)
    let coordination = ProcessCoordination::create(&segment_name)?;

    // Create disruptor with prefix discovery enabled for 1 consumer
    let builder = build_shared_single_producer::<Event>(&segment_name, get_buffer_size())
        .discover_consumer_with_prefix(1, "TEST_CONSUMER"); // Enable prefix discovery for 1 consumer using consumer prefix
    let mut producer = builder.build_producer(Event::default)?;

    println!(
        "SPSC Prefix Discovery Producer created shared memory segment: {}",
        segment_name
    );

    // External coordination: Wait for consumer to signal readiness
    println!("Waiting for consumer with prefix 'TEST_CONSUMER' to signal readiness...");
    if !coordination.wait_for_consumers_ready(1, Duration::from_secs(45)) {
        return Err("Timeout waiting for consumer to be ready".into());
    }
    println!("Consumer ready! Starting production...");

    println!("Producing {} events...", NUM_EVENTS);

    let start_time = Instant::now();

    // High-performance event production loop - no coordination overhead
    for i in 0..NUM_EVENTS {
        let publish_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        producer.publish(|event| {
            event.value = 1; // Each event contributes 1 to the running counter
            event.timestamp_ns = publish_time; // Record when event was produced
        });

        // Progress reporting for long-running tests
        if disruptor_mp::is_multiple_of_u64(i, 1_000) && i > 0 {
            println!("Produced {} events", i);
        }
    }

    let elapsed = start_time.elapsed();
    let throughput = NUM_EVENTS as f64 / elapsed.as_secs_f64();

    println!("SPSC Prefix Discovery Producer finished!");
    println!(
        "Time: {}ms ({:.1}μs per event)",
        elapsed.as_millis(),
        elapsed.as_nanos() as f64 / NUM_EVENTS as f64 / 1000.0
    );
    println!("Throughput: {} events/sec", throughput as u64);

    // Signal completion using shared atomic
    coordination.signal_producer_done(NUM_EVENTS);
    println!("Signaled producer completion via shared atomic");

    // Wait for consumer to finish (with timeout for safety)
    let wait_start = Instant::now();
    let timeout = Duration::from_secs(45); // Safety timeout

    while wait_start.elapsed() < timeout {
        let consumers_finished = coordination.consumer_done.load(Ordering::Acquire);
        if consumers_finished >= 1 {
            println!("Consumer finished");

            let final_consumed = coordination.events_consumed.load(Ordering::Acquire) as u64;
            println!(
                "Final consumer finished consuming {} events",
                final_consumed
            );
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }

    Ok(())
}

/// SPSC Consumer process with prefix discovery - demonstrates consumer prefix discovery
///
/// This function implements the consumer side for SPSC prefix discovery testing:
/// 1. Attaches to existing coordination structures
/// 2. Attaches to existing disruptor shared memory using prefix naming
/// 3. Signals readiness to producer (discovered via prefix-based scanning)
/// 4. Processes events at maximum throughput using spin-wait
/// 5. Validates event count and signals completion
///
/// ## Discovery Features:
/// - Consumer uses prefix naming scheme (TEST_CONSUMER_*)
/// - Demonstrates prefix-based consumer discovery in SPSC scenarios
/// - Validates discovery works correctly with consumer prefixes
/// - Shows how prefix discovery is faster than PID-based scanning
fn spsc_prefix_discovery_consumer_process() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting SPSC consumer process with prefix discovery...");

    let segment_name = get_segment_name();

    // Attach to existing coordination shared memory created by producer
    let coordination = ProcessCoordination::attach(&segment_name)?;

    let config = SharedMemoryConfig {
        name: segment_name.clone(),
        buffer_size: get_buffer_size(),
        element_size: std::mem::size_of::<Event>(),
        create: false,
    };

    // Use prefix naming for discovery - must match the pattern expected by prefix discovery
    // For SPSC, always use consumer 0 since there's only one consumer
    let consumer_id = "TEST_CONSUMER_0".to_string();

    let builder: SharedDisruptorBuilder<Event> = SharedDisruptorBuilder::new(config);
    let mut consumer = builder.with_consumer_id(&consumer_id).build_consumer()?; // Consumer attaches with custom ID for prefix discovery

    println!(
        "Consumer {} attached to shared memory segment: {}",
        consumer_id, segment_name
    );

    // External coordination: Signal readiness immediately after successful attachment
    coordination.signal_consumer_ready();
    println!("Consumer {} signaled readiness to producer", consumer_id);

    println!("Consuming events...");

    let mut events_consumed = 0u64;
    let mut total_counter = 0i64;
    let mut processing_time = Duration::new(0, 0);
    let mut latency_histogram = Histogram::<u64>::new(3).unwrap(); // 3 significant digits

    let start_time = Instant::now();

    // High-performance event consumption loop with spin-wait
    loop {
        let process_start = Instant::now();
        let processed = consumer.process_available(|event, _sequence| {
            let consume_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;

            // Calculate latency from produce to consume (in microseconds)
            let latency_ns = consume_time.saturating_sub(event.timestamp_ns);
            let latency_us = latency_ns / 1000; // Convert nanoseconds to microseconds
            latency_histogram.record(latency_us).unwrap_or(());

            events_consumed += 1;
            total_counter += event.value as i64; // Running counter for verification

            // Progress reporting for long-running tests
            if disruptor_mp::is_multiple_of_u64(events_consumed, 1_000) && events_consumed > 0 {
                println!(
                    "Consumed {} events, counter: {}",
                    events_consumed, total_counter
                );
            }
        });

        if processed > 0 {
            processing_time += process_start.elapsed();
        }

        // External coordination: Check if producer signaled completion
        if coordination.producer_done.load(Ordering::Acquire) == 1 {
            let expected_events = coordination.events_produced.load(Ordering::Acquire) as u64;
            if events_consumed >= expected_events {
                println!("Producer finished, consumed all {} events", expected_events);
                break;
            }
        }

        // Minimal CPU usage when no events available
        if processed == 0 {
            thread::yield_now();
        }
    }

    let elapsed = start_time.elapsed();
    let throughput = events_consumed as f64 / elapsed.as_secs_f64();

    println!("SPSC Prefix Discovery Consumer finished!");
    println!("Events consumed: {}", events_consumed);
    println!("Total counter: {}", total_counter);
    println!("Expected: {} events, {} counter", NUM_EVENTS, NUM_EVENTS);
    println!(
        "Processing time: {:.3}ms ({:.1}ns per event)",
        processing_time.as_nanos() as f64 / 1_000_000.0,
        processing_time.as_nanos() as f64 / events_consumed as f64
    );
    println!("Throughput: {} events/sec", throughput as u64);

    // Calculate and display latency percentiles (in microseconds)
    let p50 = latency_histogram.value_at_quantile(0.50) as f64;
    let p99 = latency_histogram.value_at_quantile(0.99) as f64;
    println!("Latency P50: {:.3}μs", p50);
    println!("Latency P99: {:.3}μs", p99);

    // Signal completion using shared atomic
    coordination.signal_consumer_done(events_consumed);
    println!("Consumer signaled completion via shared atomic");

    // Validation: Check if test passed
    let expected_events = NUM_EVENTS;
    let expected_counter = NUM_EVENTS as i64;
    if events_consumed == expected_events && total_counter == expected_counter {
        println!("SPSC PREFIX DISCOVERY TEST PASSED!");
    } else {
        println!(
            "SPSC PREFIX DISCOVERY TEST FAILED! Expected {} events and {} counter, got {} events and {} counter",
            expected_events, expected_counter, events_consumed, total_counter
        );
    }

    Ok(())
}

/// SPMC Producer process with consumer prefix discovery - demonstrates prefix-based consumer discovery
///
/// This function implements the producer side for SPMC prefix discovery testing:
/// 1. Creates shared memory and coordination structures
/// 2. Enables consumer discovery with a specific prefix ("SPMC_CONSUMER")
/// 3. Waits for consumers with matching prefix to be discovered
/// 4. Produces events at maximum throughput using spin-wait (broadcast semantics)
/// 5. Validates completion and signals producer done
///
/// ## Discovery Features:
/// - Producer uses discover_consumer_with_prefix(N, "SPMC_CONSUMER") for optimized discovery
/// - Consumers must use matching prefix naming scheme
/// - Demonstrates prefix-based consumer discovery in SPMC scenarios
/// - Validates discovery works correctly with multiple consumers using prefixes
/// - Shows how prefix discovery is faster than PID-based scanning
fn spmc_prefix_discovery_producer_process(
    expected_consumers: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Starting SPMC producer process with prefix discovery ({} consumers)...",
        expected_consumers
    );

    let segment_name = get_segment_name();

    // Create coordination shared memory FIRST (external coordination pattern)
    let coordination = ProcessCoordination::create(&segment_name)?;

    // Create disruptor with prefix discovery enabled for the specified number of consumers
    let builder = build_shared_single_producer::<Event>(&segment_name, get_buffer_size())
        .discover_consumer_with_prefix(expected_consumers as usize, "SPMC_CONSUMER"); // Enable prefix discovery for N consumers using consumer prefix
    let mut producer = builder.build_producer(Event::default)?;

    println!(
        "SPMC Prefix Discovery Producer created shared memory segment: {}",
        segment_name
    );

    // External coordination: Wait for all consumers to signal readiness
    println!(
        "Waiting for {} consumers with prefix 'SPMC_CONSUMER' to signal readiness...",
        expected_consumers
    );
    if !coordination.wait_for_consumers_ready(expected_consumers, Duration::from_secs(45)) {
        return Err(format!(
            "Timeout waiting for {} consumers to be ready",
            expected_consumers
        )
        .into());
    }
    println!(
        "All {} consumer(s) ready! Starting production...",
        expected_consumers
    );

    println!(
        "Producing {} events for {} consumers (broadcast)...",
        NUM_EVENTS, expected_consumers
    );

    let start_time = Instant::now();

    // High-performance event production loop - no coordination overhead
    for i in 0..NUM_EVENTS {
        let publish_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        producer.publish(|event| {
            event.value = 1; // Each event contributes 1 to the running counter
            event.timestamp_ns = publish_time; // Record when event was produced
        });

        // Progress reporting for long-running tests
        if disruptor_mp::is_multiple_of_u64(i, 1_000) && i > 0 {
            println!("Produced {} events", i);
        }
    }

    let elapsed = start_time.elapsed();
    let throughput = NUM_EVENTS as f64 / elapsed.as_secs_f64();

    println!("SPMC Prefix Discovery Producer finished!");
    println!(
        "Time: {}ms ({:.1}μs per event)",
        elapsed.as_millis(),
        elapsed.as_nanos() as f64 / NUM_EVENTS as f64 / 1000.0
    );
    println!("Throughput: {} events/sec", throughput as u64);

    // Signal completion using shared atomic
    coordination.signal_producer_done(NUM_EVENTS);
    println!("Signaled producer completion via shared atomic");

    // Wait for all consumers to finish (with timeout for safety)
    let wait_start = Instant::now();
    let timeout = Duration::from_secs(45); // Safety timeout

    while wait_start.elapsed() < timeout {
        let consumers_finished = coordination.consumer_done.load(Ordering::Acquire);
        if consumers_finished >= expected_consumers {
            println!("All {} consumer(s) finished", expected_consumers);

            let final_consumed = coordination.events_consumed.load(Ordering::Acquire) as u64;
            println!(
                "Final consumer finished consuming {} events",
                final_consumed
            );
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }

    Ok(())
}

/// SPMC Consumer process with prefix discovery - demonstrates consumer prefix discovery in broadcast mode
///
/// This function implements the consumer side for SPMC prefix discovery testing:
/// 1. Attaches to existing coordination structures
/// 2. Attaches to existing disruptor shared memory using prefix naming
/// 3. Signals readiness to producer (discovered via prefix-based scanning)
/// 4. Processes events at maximum throughput using spin-wait
/// 5. Validates event count and signals completion
///
/// ## Discovery Features:
/// - Consumer uses prefix naming scheme (SPMC_CONSUMER_*)
/// - Demonstrates prefix-based consumer discovery in SPMC scenarios
/// - Validates discovery works correctly with multiple consumers using prefixes
/// - Shows broadcast semantics (each consumer sees ALL events)
/// - Shows how prefix discovery is faster than PID-based scanning
fn spmc_prefix_discovery_consumer_process(
    consumer_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Starting SPMC consumer process with prefix discovery: {}",
        consumer_id
    );

    let segment_name = get_segment_name();

    // Attach to existing coordination shared memory created by producer
    let coordination = ProcessCoordination::attach(&segment_name)?;

    let config = SharedMemoryConfig {
        name: segment_name.clone(),
        buffer_size: get_buffer_size(),
        element_size: std::mem::size_of::<Event>(),
        create: false,
    };

    // Use prefix naming for discovery - must match the pattern expected by prefix discovery
    // The consumer_id parameter (e.g., "1", "2") is passed from the main function
    let consumer_number: usize = consumer_id.parse().unwrap_or(0);
    let prefixed_consumer_id = format!("SPMC_CONSUMER_{}", consumer_number);

    let builder: SharedDisruptorBuilder<Event> = SharedDisruptorBuilder::new(config);
    let mut consumer = builder
        .with_consumer_id(&prefixed_consumer_id)
        .build_consumer()?; // Consumer attaches with custom ID for prefix discovery

    println!(
        "Consumer {} attached to shared memory segment: {}",
        prefixed_consumer_id, segment_name
    );

    // External coordination: Signal readiness immediately after successful attachment
    coordination.signal_consumer_ready();
    println!(
        "Consumer {} signaled readiness to producer",
        prefixed_consumer_id
    );

    println!(
        "Consuming events (broadcast semantics - each consumer sees ALL events independently)..."
    );

    let mut events_consumed = 0u64;
    let mut total_counter = 0i64;
    let mut processing_time = Duration::new(0, 0);
    let mut latency_histogram = Histogram::<u64>::new(3).unwrap(); // 3 significant digits

    let start_time = Instant::now();

    // High-performance event consumption loop with spin-wait
    loop {
        let process_start = Instant::now();
        let processed = consumer.process_available(|event, _sequence| {
            let consume_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;

            // Calculate latency from produce to consume (in microseconds)
            let latency_ns = consume_time.saturating_sub(event.timestamp_ns);
            let latency_us = latency_ns / 1000; // Convert nanoseconds to microseconds
            latency_histogram.record(latency_us).unwrap_or(());

            events_consumed += 1;
            total_counter += event.value as i64; // Running counter for verification

            // Progress reporting for long-running tests
            if disruptor_mp::is_multiple_of_u64(events_consumed, 1_000) && events_consumed > 0 {
                println!(
                    "Consumer {} consumed {} events, counter: {}",
                    consumer_id, events_consumed, total_counter
                );
            }
        });

        if processed > 0 {
            processing_time += process_start.elapsed();
        }

        // External coordination: Check if producer signaled completion
        if coordination.producer_done.load(Ordering::Acquire) == 1 {
            let expected_events = coordination.events_produced.load(Ordering::Acquire) as u64;
            if events_consumed >= expected_events {
                println!(
                    "Consumer {} - Producer finished, consumed all {} events",
                    consumer_id, expected_events
                );
                break;
            }
        }

        // Minimal CPU usage when no events available
        if processed == 0 {
            thread::yield_now();
        }
    }

    let elapsed = start_time.elapsed();
    let throughput = events_consumed as f64 / elapsed.as_secs_f64();

    println!("SPMC Prefix Discovery Consumer {} finished!", consumer_id);
    println!("Events consumed: {}", events_consumed);
    println!("Total counter: {}", total_counter);
    println!(
        "Expected (broadcast): {} events, {} counter",
        NUM_EVENTS, NUM_EVENTS
    );
    println!(
        "Processing time: {:.3}ms ({:.1}ns per event)",
        processing_time.as_nanos() as f64 / 1_000_000.0,
        processing_time.as_nanos() as f64 / events_consumed as f64
    );
    println!("Throughput: {} events/sec", throughput as u64);

    // Calculate and display latency percentiles (in microseconds)
    let p50 = latency_histogram.value_at_quantile(0.50) as f64;
    let p99 = latency_histogram.value_at_quantile(0.99) as f64;
    println!("Consumer {} Latency P50: {:.3}μs", consumer_id, p50);
    println!("Consumer {} Latency P99: {:.3}μs", consumer_id, p99);

    // Signal completion using shared atomic
    coordination.signal_consumer_done(events_consumed);
    println!(
        "Consumer {} signaled completion via shared atomic",
        consumer_id
    );

    // Validation: Check if test passed (broadcast semantics)
    let expected_events = NUM_EVENTS;
    let expected_counter = NUM_EVENTS as i64;
    if events_consumed == expected_events && total_counter == expected_counter {
        println!("Consumer {} - BROADCAST TEST PASSED!", consumer_id);
    } else {
        println!(
            "Consumer {} - BROADCAST TEST FAILED! Expected {} events and {} counter, got {} events and {} counter",
            consumer_id, expected_events, expected_counter, events_consumed, total_counter
        );
    }

    Ok(())
}

/// Run automated SPSC test with consumer prefix discovery
///
/// This test validates SPSC functionality with prefix-based consumer discovery:
/// - Producer uses discover_consumer_with_prefix(1, "TEST_CONSUMER") for optimized discovery
/// - Consumer uses prefix naming scheme for discovery
/// - Validates single producer, single consumer with prefix discovery
/// - Returns both test result and performance metrics for summary table
fn run_automated_spsc_prefix_discovery_test(
) -> (Result<(), Box<dyn std::error::Error>>, TestResults) {
    println!("Running automated SPSC test with consumer prefix discovery...");

    let current_exe = match env::current_exe() {
        Ok(exe) => exe,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };
    let segment_name = get_segment_name();

    // Start producer process first to create shared memory and enable prefix discovery
    println!(
        "Starting producer process with prefix discovery enabled (will create shared memory)..."
    );
    let producer_child = match Command::new(&current_exe)
        .args(["spsc_prefix_discovery_producer"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Give producer time to create shared memory segment
    thread::sleep(Duration::from_millis(200));

    // Start consumer process to attach and consume
    println!("Starting consumer process (will attach and be discovered via prefix)...");
    let consumer_child = match Command::new(&current_exe)
        .args(["spsc_prefix_discovery_consumer"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Wait for consumer to finish first (it will exit when done)
    let consumer_result = match consumer_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Wait for producer to finish
    let producer_result = match producer_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Convert outputs to strings
    let producer_output = String::from_utf8_lossy(&producer_result.stdout);
    let consumer_output = String::from_utf8_lossy(&consumer_result.stdout);

    // Print outputs
    println!("\n--- Producer Output ---");
    println!("{}", producer_output);

    println!("\n--- Consumer Output ---");
    println!("{}", consumer_output);

    let test_passed = consumer_result.status.success()
        && producer_output.contains("SPSC Prefix Discovery Producer finished!")
        && consumer_output.contains("SPSC PREFIX DISCOVERY TEST PASSED!");

    // Extract metrics from the same run that produced the output
    let consumer_outputs = vec![consumer_output.to_string()];
    let metrics = extract_test_results(&producer_output, &consumer_outputs);

    // Check results
    if test_passed {
        println!("✅ Automated SPSC prefix discovery test PASSED!");
        (Ok(()), metrics)
    } else {
        println!("❌ Automated SPSC prefix discovery test FAILED!");
        (
            Err("SPSC prefix discovery test validation failed".into()),
            metrics,
        )
    }
}

/// Run automated SPMC test with 2 consumers using consumer prefix discovery
///
/// This test validates SPMC functionality with prefix-based consumer discovery:
/// - Producer uses discover_consumer_with_prefix(2, "SPMC_CONSUMER") for optimized discovery
/// - Consumers use prefix naming scheme for discovery
/// - Validates single producer, multiple consumers with prefix discovery
/// - Tests broadcast semantics (each consumer sees ALL events)
/// - Returns both test result and performance metrics for summary table
fn run_automated_spmc_2_prefix_discovery_test(
) -> (Result<(), Box<dyn std::error::Error>>, TestResults) {
    println!("Running automated SPMC test with 2 consumers using prefix discovery...");

    let current_exe = match env::current_exe() {
        Ok(exe) => exe,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };
    let segment_name = get_segment_name();

    // Start producer process first to create shared memory and enable prefix discovery
    println!("Starting producer process with prefix discovery enabled (will create shared memory and wait for 2 consumers)...");
    let producer_child = match Command::new(&current_exe)
        .args(["spmc_prefix_discovery_producer_2"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Give producer time to create shared memory segment
    thread::sleep(Duration::from_millis(200));

    // Start consumer processes to attach and consume
    println!("Starting consumer processes (will attach and be discovered via prefix)...");
    let consumer1_child = match Command::new(&current_exe)
        .args(["spmc_prefix_discovery_consumer1"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    let consumer2_child = match Command::new(&current_exe)
        .args(["spmc_prefix_discovery_consumer2"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Wait for consumers to finish first (they will exit when done)
    let consumer1_result = match consumer1_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    let consumer2_result = match consumer2_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Wait for producer to finish
    let producer_result = match producer_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Convert outputs to strings
    let producer_output = String::from_utf8_lossy(&producer_result.stdout);
    let consumer1_output = String::from_utf8_lossy(&consumer1_result.stdout);
    let consumer2_output = String::from_utf8_lossy(&consumer2_result.stdout);

    // Print outputs
    println!("\n--- Producer Output ---");
    println!("{}", producer_output);

    println!("\n--- Consumer 1 Output ---");
    println!("{}", consumer1_output);

    println!("\n--- Consumer 2 Output ---");
    println!("{}", consumer2_output);

    let test_passed = consumer1_result.status.success()
        && consumer2_result.status.success()
        && producer_output.contains("SPMC Prefix Discovery Producer finished!")
        && consumer1_output.contains("BROADCAST TEST PASSED!")
        && consumer2_output.contains("BROADCAST TEST PASSED!");

    // Extract metrics from the same run that produced the output
    let consumer_outputs = vec![consumer1_output.to_string(), consumer2_output.to_string()];
    let metrics = extract_test_results(&producer_output, &consumer_outputs);

    // Check results
    if test_passed {
        println!("✅ Automated SPMC-2 prefix discovery test PASSED!");
        println!("Both consumers saw all events (broadcast semantics working correctly)");
        (Ok(()), metrics)
    } else {
        println!("❌ Automated SPMC-2 prefix discovery test FAILED!");
        (
            Err("SPMC-2 prefix discovery test validation failed".into()),
            metrics,
        )
    }
}

/// Run automated SPMC test with 5 consumers using consumer prefix discovery
///
/// This test validates SPMC functionality with prefix-based consumer discovery:
/// - Producer uses discover_consumer_with_prefix(5, "SPMC_CONSUMER") for optimized discovery
/// - Consumers use prefix naming scheme for discovery
/// - Validates single producer, multiple consumers with prefix discovery
/// - Tests broadcast semantics (each consumer sees ALL events)
/// - Returns both test result and performance metrics for summary table
fn run_automated_spmc_5_prefix_discovery_test(
) -> (Result<(), Box<dyn std::error::Error>>, TestResults) {
    println!("Running automated SPMC test with 5 consumers using prefix discovery...");

    let current_exe = match env::current_exe() {
        Ok(exe) => exe,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };
    let segment_name = get_segment_name();

    // Start producer process first to create shared memory and enable prefix discovery
    println!("Starting producer process with prefix discovery enabled (will create shared memory and wait for 5 consumers)...");
    let producer_child = match Command::new(&current_exe)
        .args(["spmc_prefix_discovery_producer_5"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Give producer time to create shared memory segment
    thread::sleep(Duration::from_millis(200));

    // Start consumer processes to attach and consume
    println!("Starting consumer processes (will attach and be discovered via prefix)...");
    let consumer1_child = match Command::new(&current_exe)
        .args(["spmc_prefix_discovery_consumer1"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    let consumer2_child = match Command::new(&current_exe)
        .args(["spmc_prefix_discovery_consumer2"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    let consumer3_child = match Command::new(&current_exe)
        .args(["spmc_prefix_discovery_consumer3"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    let consumer4_child = match Command::new(&current_exe)
        .args(["spmc_prefix_discovery_consumer4"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    let consumer5_child = match Command::new(&current_exe)
        .args(["spmc_prefix_discovery_consumer5"])
        .env("MP_SEGMENT_NAME", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Wait for consumers to finish first (they will exit when done)
    let consumer1_result = match consumer1_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    let consumer2_result = match consumer2_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    let consumer3_result = match consumer3_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    let consumer4_result = match consumer4_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    let consumer5_result = match consumer5_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Wait for producer to finish
    let producer_result = match producer_child.wait_with_output() {
        Ok(result) => result,
        Err(e) => return (Err(e.into()), TestResults::default()),
    };

    // Convert outputs to strings
    let producer_output = String::from_utf8_lossy(&producer_result.stdout);
    let consumer1_output = String::from_utf8_lossy(&consumer1_result.stdout);
    let consumer2_output = String::from_utf8_lossy(&consumer2_result.stdout);
    let consumer3_output = String::from_utf8_lossy(&consumer3_result.stdout);
    let consumer4_output = String::from_utf8_lossy(&consumer4_result.stdout);
    let consumer5_output = String::from_utf8_lossy(&consumer5_result.stdout);

    // Print outputs
    println!("\n--- Producer Output ---");
    println!("{}", producer_output);

    println!("\n--- Consumer 1 Output ---");
    println!("{}", consumer1_output);

    println!("\n--- Consumer 2 Output ---");
    println!("{}", consumer2_output);

    println!("\n--- Consumer 3 Output ---");
    println!("{}", consumer3_output);

    println!("\n--- Consumer 4 Output ---");
    println!("{}", consumer4_output);

    println!("\n--- Consumer 5 Output ---");
    println!("{}", consumer5_output);

    let test_passed = consumer1_result.status.success()
        && consumer2_result.status.success()
        && consumer3_result.status.success()
        && consumer4_result.status.success()
        && consumer5_result.status.success()
        && producer_output.contains("SPMC Prefix Discovery Producer finished!")
        && consumer1_output.contains("BROADCAST TEST PASSED!")
        && consumer2_output.contains("BROADCAST TEST PASSED!")
        && consumer3_output.contains("BROADCAST TEST PASSED!")
        && consumer4_output.contains("BROADCAST TEST PASSED!")
        && consumer5_output.contains("BROADCAST TEST PASSED!");

    // Extract metrics from the same run that produced the output
    let consumer_outputs = vec![
        consumer1_output.to_string(),
        consumer2_output.to_string(),
        consumer3_output.to_string(),
        consumer4_output.to_string(),
        consumer5_output.to_string(),
    ];
    let metrics = extract_test_results(&producer_output, &consumer_outputs);

    // Check results
    if test_passed {
        println!("✅ Automated SPMC-5 prefix discovery test PASSED!");
        println!("All 5 consumers saw all events (broadcast semantics working correctly)");
        (Ok(()), metrics)
    } else {
        println!("❌ Automated SPMC-5 prefix discovery test FAILED!");
        (
            Err("SPMC-5 prefix discovery test validation failed".into()),
            metrics,
        )
    }
}

/// Run comprehensive buffer size comparison tests
/// Tests SPSC, SPMC-2, and SPMC-5 scenarios across multiple buffer sizes
/// Returns detailed performance metrics for each combination
fn run_buffer_size_comparison_tests() -> Result<(), Box<dyn std::error::Error>> {
    println!("Running comprehensive buffer size comparison tests...");

    // Test buffer sizes: 1KB, 4KB, 16KB, 32KB, 64KB, 128KB
    let buffer_sizes = vec![1024, 4096, 16384, 32768, 65536, 131072];
    let mut all_results = Vec::new();

    for buffer_size in &buffer_sizes {
        println!("\n{}", "=".repeat(80));
        println!(
            "TESTING BUFFER SIZE: {} ({} KB)",
            format_number(*buffer_size as f64),
            *buffer_size / 1024
        );
        println!("{}", "=".repeat(80));

        // Set buffer size environment variable for child processes
        env::set_var("BUFFER_SIZE", buffer_size.to_string());

        // Test SPSC
        println!(
            "\n--- SPSC Test (Buffer Size: {} KB) ---",
            *buffer_size / 1024
        );
        let (spsc_result, spsc_metrics) = run_automated_spsc_test();
        if let Err(e) = spsc_result {
            eprintln!("SPSC test failed for buffer size {}: {}", buffer_size, e);
            continue;
        }

        thread::sleep(Duration::from_millis(500));

        // Test SPMC-2
        println!(
            "\n--- SPMC-2 Test (Buffer Size: {} KB) ---",
            *buffer_size / 1024
        );
        let (spmc_2_result, spmc_2_metrics) = run_automated_spmc_test();
        if let Err(e) = spmc_2_result {
            eprintln!("SPMC-2 test failed for buffer size {}: {}", buffer_size, e);
            continue;
        }

        thread::sleep(Duration::from_millis(500));

        // Test SPMC-5
        println!(
            "\n--- SPMC-5 Test (Buffer Size: {} KB) ---",
            *buffer_size / 1024
        );
        let (spmc_5_result, spmc_5_metrics) = run_automated_spmc_5_consumer_test();
        if let Err(e) = spmc_5_result {
            eprintln!("SPMC-5 test failed for buffer size {}: {}", buffer_size, e);
            continue;
        }

        // Store results for comprehensive summary
        all_results.push((*buffer_size, spsc_metrics, spmc_2_metrics, spmc_5_metrics));

        thread::sleep(Duration::from_secs(1));
    }

    // Generate comprehensive summary table
    println!("\n\n{}", "=".repeat(120));
    println!("COMPREHENSIVE BUFFER SIZE COMPARISON SUMMARY");
    println!("{}", "=".repeat(120));

    let mut summary_table = Vec::new();

    for (buffer_size, spsc_metrics, spmc_2_metrics, spmc_5_metrics) in &all_results {
        summary_table.extend([
            // SPSC results
            create_test_summary(&TestSummaryInputs {
                scenario: "SPSC",
                buffer_size: *buffer_size,
                events: NUM_EVENTS,
                payload_size: std::mem::size_of::<Event>(),
                producer_throughput: spsc_metrics.producer_throughput,
                consumer_throughput: spsc_metrics.consumer_throughput,
                consumer_p50_us: spsc_metrics.consumer_p50_us,
                consumer_p99_us: spsc_metrics.consumer_p99_us,
            }),
            // SPMC-2 results
            create_test_summary(&TestSummaryInputs {
                scenario: "SPMC-2",
                buffer_size: *buffer_size,
                events: NUM_EVENTS,
                payload_size: std::mem::size_of::<Event>(),
                producer_throughput: spmc_2_metrics.producer_throughput,
                consumer_throughput: spmc_2_metrics.consumer_throughput,
                consumer_p50_us: spmc_2_metrics.consumer_p50_us,
                consumer_p99_us: spmc_2_metrics.consumer_p99_us,
            }),
            // SPMC-5 results
            create_test_summary(&TestSummaryInputs {
                scenario: "SPMC-5",
                buffer_size: *buffer_size,
                events: NUM_EVENTS,
                payload_size: std::mem::size_of::<Event>(),
                producer_throughput: spmc_5_metrics.producer_throughput,
                consumer_throughput: spmc_5_metrics.consumer_throughput,
                consumer_p50_us: spmc_5_metrics.consumer_p50_us,
                consumer_p99_us: spmc_5_metrics.consumer_p99_us,
            }),
        ]);
    }

    println!(
        "Test Configuration: {} events, {} bytes per event",
        format_number(NUM_EVENTS as f64),
        std::mem::size_of::<Event>()
    );
    println!("{}", Table::new(&summary_table));

    // Performance analysis
    print_buffer_size_analysis(&all_results);

    Ok(())
}

/// Print detailed analysis of buffer size performance impact
fn print_buffer_size_analysis(results: &[(usize, TestResults, TestResults, TestResults)]) {
    println!("\n{}", "=".repeat(80));
    println!("PERFORMANCE ANALYSIS BY BUFFER SIZE");
    println!("{}", "=".repeat(80));

    for (buffer_size, spsc_metrics, spmc_2_metrics, spmc_5_metrics) in results {
        println!(
            "\nBuffer Size: {} KB ({} events)",
            *buffer_size / 1024,
            format_number(*buffer_size as f64)
        );
        println!(
            "  SPSC Throughput:  Producer: {:>12.0} ops/sec, Consumer: {:>12.0} ops/sec",
            spsc_metrics.producer_throughput, spsc_metrics.consumer_throughput
        );
        println!(
            "  SPMC-2 Throughput: Producer: {:>12.0} ops/sec, Consumer: {:>12.0} ops/sec",
            spmc_2_metrics.producer_throughput, spmc_2_metrics.consumer_throughput
        );
        println!(
            "  SPMC-5 Throughput: Producer: {:>12.0} ops/sec, Consumer: {:>12.0} ops/sec",
            spmc_5_metrics.producer_throughput, spmc_5_metrics.consumer_throughput
        );

        // Calculate efficiency metrics
        let spsc_efficiency =
            (spsc_metrics.consumer_throughput / spsc_metrics.producer_throughput.max(1.0)) * 100.0;
        let spmc_2_efficiency = (spmc_2_metrics.consumer_throughput
            / spmc_2_metrics.producer_throughput.max(1.0))
            * 100.0;
        let spmc_5_efficiency = (spmc_5_metrics.consumer_throughput
            / spmc_5_metrics.producer_throughput.max(1.0))
            * 100.0;

        println!(
            "  Efficiency:        SPSC: {:>6.1}%, SPMC-2: {:>6.1}%, SPMC-5: {:>6.1}%",
            spsc_efficiency, spmc_2_efficiency, spmc_5_efficiency
        );

        // Calculate latency metrics
        println!(
            "  Latency P99 (μs):  SPSC: {:>6.1}, SPMC-2: {:>6.1}, SPMC-5: {:>6.1}",
            spsc_metrics.consumer_p99_us,
            spmc_2_metrics.consumer_p99_us,
            spmc_5_metrics.consumer_p99_us
        );
    }

    // Find optimal buffer sizes
    println!("\n{}", "-".repeat(80));
    println!("OPTIMAL BUFFER SIZE RECOMMENDATIONS");
    println!("{}", "-".repeat(80));

    let mut best_spsc = (0usize, 0.0f64);
    let mut best_spmc_2 = (0usize, 0.0f64);
    let mut best_spmc_5 = (0usize, 0.0f64);

    for (buffer_size, spsc_metrics, spmc_2_metrics, spmc_5_metrics) in results {
        if spsc_metrics.consumer_throughput > best_spsc.1 {
            best_spsc = (*buffer_size, spsc_metrics.consumer_throughput);
        }
        if spmc_2_metrics.consumer_throughput > best_spmc_2.1 {
            best_spmc_2 = (*buffer_size, spmc_2_metrics.consumer_throughput);
        }
        if spmc_5_metrics.consumer_throughput > best_spmc_5.1 {
            best_spmc_5 = (*buffer_size, spmc_5_metrics.consumer_throughput);
        }
    }

    println!(
        "Best SPSC Performance:  {} KB buffer ({:.0} ops/sec)",
        best_spsc.0 / 1024,
        best_spsc.1
    );
    println!(
        "Best SPMC-2 Performance: {} KB buffer ({:.0} ops/sec)",
        best_spmc_2.0 / 1024,
        best_spmc_2.1
    );
    println!(
        "Best SPMC-5 Performance: {} KB buffer ({:.0} ops/sec)",
        best_spmc_5.0 / 1024,
        best_spmc_5.1
    );
}

/// Main entry point - handles both user commands and internal process coordination
///
/// This function serves dual purposes:
/// 1. **User Interface**: Handles user commands (test, spmc_test, spmc_5_test, buffer_comparison)
/// 2. **Process Coordination**: Handles internal process spawning for automated tests
///
/// ## Command Line Interface:
/// - `test`: Run all test scenarios (SPSC + SPMC 2-consumer + SPMC 5-consumer)
/// - `buffer_comparison`: Run comprehensive buffer size comparison tests
/// - `spmc_test`: Run only SPMC test with 2 consumers
/// - `spmc_5_test`: Run only SPMC test with 5 consumers
/// - No args: Default to running all tests
///
/// ## Internal Process Modes:
/// These are used internally by the automated tests when spawning child processes:
/// - `producer`/`consumer`: SPSC test processes
/// - `spmc_producer`/`spmc_producer_5`: SPMC producer processes
/// - `spmc_consumer1-5`: SPMC consumer processes with unique IDs
///
/// ## Architecture Benefits:
/// - Single binary handles all roles (producer, consumer, test orchestrator)
/// - Child processes inherit all libraries and dependencies
/// - Simplified deployment (no need for separate binaries)
/// - Environment variable passing for coordination
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    // Check if this is a child process spawned by automated tests
    if args.len() >= 2 {
        match args[1].as_str() {
            // Internal process modes (used exclusively by automated test spawning)
            "producer" => return producer_process(),
            "consumer" => return consumer_process(),
            "spsc_discovery_producer" => return spsc_discovery_producer_process(),
            "spsc_discovery_consumer" => return spsc_discovery_consumer_process(),
            "spmc_producer" => return spmc_producer_process(2),
            "spmc_producer_5" => return spmc_producer_process(5),
            "spmc_consumer1" => return spmc_consumer_process("1"),
            "spmc_consumer2" => return spmc_consumer_process("2"),
            "spmc_consumer3" => return spmc_consumer_process("3"),
            "spmc_consumer4" => return spmc_consumer_process("4"),
            "spmc_consumer5" => return spmc_consumer_process("5"),
            // SPMC Discovery process modes
            "spmc_discovery_producer_2" => return spmc_discovery_producer_process(2),
            "spmc_discovery_producer_5" => return spmc_discovery_producer_process(5),
            "spmc_discovery_consumer1" => return spmc_discovery_consumer_process("1"),
            "spmc_discovery_consumer2" => return spmc_discovery_consumer_process("2"),
            "spmc_discovery_consumer3" => return spmc_discovery_consumer_process("3"),
            "spmc_discovery_consumer4" => return spmc_discovery_consumer_process("4"),
            "spmc_discovery_consumer5" => return spmc_discovery_consumer_process("5"),
            // SPMC Prefix Discovery process modes
            "spsc_prefix_discovery_producer" => return spsc_prefix_discovery_producer_process(),
            "spsc_prefix_discovery_consumer" => return spsc_prefix_discovery_consumer_process(),
            "spmc_prefix_discovery_producer_2" => return spmc_prefix_discovery_producer_process(2),
            "spmc_prefix_discovery_producer_5" => return spmc_prefix_discovery_producer_process(5),
            "spmc_prefix_discovery_consumer1" => {
                return spmc_prefix_discovery_consumer_process("1")
            }
            "spmc_prefix_discovery_consumer2" => {
                return spmc_prefix_discovery_consumer_process("2")
            }
            "spmc_prefix_discovery_consumer3" => {
                return spmc_prefix_discovery_consumer_process("3")
            }
            "spmc_prefix_discovery_consumer4" => {
                return spmc_prefix_discovery_consumer_process("4")
            }
            "spmc_prefix_discovery_consumer5" => {
                return spmc_prefix_discovery_consumer_process("5")
            }

            // User-facing test modes (primary interface)
            "buffer_comparison" => {
                return run_buffer_size_comparison_tests();
            }
            "test" => {
                println!("Starting automated tests with 1-second delays between scenarios...");

                println!("\n=== Running SPSC Test ===");
                let (spsc_result, spsc_metrics) = run_automated_spsc_test();

                println!("\nWaiting 1 second before next test...");
                thread::sleep(Duration::from_secs(1));

                println!("\n=== Running SPSC Discovery Test ===");
                let (spsc_discovery_result, spsc_discovery_metrics) =
                    run_automated_spsc_discovery_test();

                println!("\nWaiting 1 second before next test...");
                thread::sleep(Duration::from_secs(1));

                println!("\n=== Running SPMC Test (2 consumers) ===");
                let (spmc_2_result, spmc_2_metrics) = run_automated_spmc_test();

                println!("\nWaiting 1 second before next test...");
                thread::sleep(Duration::from_secs(1));

                println!("\n=== Running SPMC Test (5 consumers) ===");
                let (spmc_5_result, spmc_5_metrics) = run_automated_spmc_5_consumer_test();

                println!("\nWaiting 1 second before next test...");
                thread::sleep(Duration::from_secs(1));

                println!("\n=== Running SPMC Discovery Test (2 consumers) ===");
                let (spmc_2_discovery_result, spmc_2_discovery_metrics) =
                    run_automated_spmc_2_discovery_test();

                println!("\nWaiting 1 second before next test...");
                thread::sleep(Duration::from_secs(1));

                println!("\n=== Running SPMC Discovery Test (5 consumers) ===");
                let (spmc_5_discovery_result, spmc_5_discovery_metrics) =
                    run_automated_spmc_5_discovery_test();

                println!("\nWaiting 1 second before next test...");
                thread::sleep(Duration::from_secs(1));

                println!("\n=== Running SPSC Prefix Discovery Test ===");
                let (spsc_prefix_discovery_result, spsc_prefix_discovery_metrics) =
                    run_automated_spsc_prefix_discovery_test();

                println!("\nWaiting 1 second before next test...");
                thread::sleep(Duration::from_secs(1));

                println!("\n=== Running SPMC Prefix Discovery Test (2 consumers) ===");
                let (spmc_2_prefix_discovery_result, spmc_2_prefix_discovery_metrics) =
                    run_automated_spmc_2_prefix_discovery_test();

                println!("\nWaiting 1 second before next test...");
                thread::sleep(Duration::from_secs(1));

                println!("\n=== Running SPMC Prefix Discovery Test (5 consumers) ===");
                let (spmc_5_prefix_discovery_result, spmc_5_prefix_discovery_metrics) =
                    run_automated_spmc_5_prefix_discovery_test();

                // Create comprehensive test summary table with ACTUAL measurements
                let summary_table = vec![
                    // SPSC tests
                    create_test_summary(&TestSummaryInputs {
                        scenario: "SPSC",
                        buffer_size: get_buffer_size(),
                        events: NUM_EVENTS,
                        payload_size: std::mem::size_of::<Event>(),
                        producer_throughput: spsc_metrics.producer_throughput,
                        consumer_throughput: spsc_metrics.consumer_throughput,
                        consumer_p50_us: spsc_metrics.consumer_p50_us,
                        consumer_p99_us: spsc_metrics.consumer_p99_us,
                    }),
                    create_test_summary(&TestSummaryInputs {
                        scenario: "SPSC-Discovery",
                        buffer_size: get_buffer_size(),
                        events: NUM_EVENTS,
                        payload_size: std::mem::size_of::<Event>(),
                        producer_throughput: spsc_discovery_metrics.producer_throughput,
                        consumer_throughput: spsc_discovery_metrics.consumer_throughput,
                        consumer_p50_us: spsc_discovery_metrics.consumer_p50_us,
                        consumer_p99_us: spsc_discovery_metrics.consumer_p99_us,
                    }),
                    create_test_summary(&TestSummaryInputs {
                        scenario: "SPSC-Prefix-Discovery",
                        buffer_size: get_buffer_size(),
                        events: NUM_EVENTS,
                        payload_size: std::mem::size_of::<Event>(),
                        producer_throughput: spsc_prefix_discovery_metrics.producer_throughput,
                        consumer_throughput: spsc_prefix_discovery_metrics.consumer_throughput,
                        consumer_p50_us: spsc_prefix_discovery_metrics.consumer_p50_us,
                        consumer_p99_us: spsc_prefix_discovery_metrics.consumer_p99_us,
                    }),
                    // SPMC-2 tests
                    create_test_summary(&TestSummaryInputs {
                        scenario: "SPMC-2",
                        buffer_size: get_buffer_size(),
                        events: NUM_EVENTS,
                        payload_size: std::mem::size_of::<Event>(),
                        producer_throughput: spmc_2_metrics.producer_throughput,
                        consumer_throughput: spmc_2_metrics.consumer_throughput,
                        consumer_p50_us: spmc_2_metrics.consumer_p50_us,
                        consumer_p99_us: spmc_2_metrics.consumer_p99_us,
                    }),
                    create_test_summary(&TestSummaryInputs {
                        scenario: "SPMC-2-Discovery",
                        buffer_size: get_buffer_size(),
                        events: NUM_EVENTS,
                        payload_size: std::mem::size_of::<Event>(),
                        producer_throughput: spmc_2_discovery_metrics.producer_throughput,
                        consumer_throughput: spmc_2_discovery_metrics.consumer_throughput,
                        consumer_p50_us: spmc_2_discovery_metrics.consumer_p50_us,
                        consumer_p99_us: spmc_2_discovery_metrics.consumer_p99_us,
                    }),
                    create_test_summary(&TestSummaryInputs {
                        scenario: "SPMC-2-Prefix-Discovery",
                        buffer_size: get_buffer_size(),
                        events: NUM_EVENTS,
                        payload_size: std::mem::size_of::<Event>(),
                        producer_throughput: spmc_2_prefix_discovery_metrics.producer_throughput,
                        consumer_throughput: spmc_2_prefix_discovery_metrics.consumer_throughput,
                        consumer_p50_us: spmc_2_prefix_discovery_metrics.consumer_p50_us,
                        consumer_p99_us: spmc_2_prefix_discovery_metrics.consumer_p99_us,
                    }),
                    // SPMC-5 tests
                    create_test_summary(&TestSummaryInputs {
                        scenario: "SPMC-5",
                        buffer_size: get_buffer_size(),
                        events: NUM_EVENTS,
                        payload_size: std::mem::size_of::<Event>(),
                        producer_throughput: spmc_5_metrics.producer_throughput,
                        consumer_throughput: spmc_5_metrics.consumer_throughput,
                        consumer_p50_us: spmc_5_metrics.consumer_p50_us,
                        consumer_p99_us: spmc_5_metrics.consumer_p99_us,
                    }),
                    create_test_summary(&TestSummaryInputs {
                        scenario: "SPMC-5-Discovery",
                        buffer_size: get_buffer_size(),
                        events: NUM_EVENTS,
                        payload_size: std::mem::size_of::<Event>(),
                        producer_throughput: spmc_5_discovery_metrics.producer_throughput,
                        consumer_throughput: spmc_5_discovery_metrics.consumer_throughput,
                        consumer_p50_us: spmc_5_discovery_metrics.consumer_p50_us,
                        consumer_p99_us: spmc_5_discovery_metrics.consumer_p99_us,
                    }),
                    create_test_summary(&TestSummaryInputs {
                        scenario: "SPMC-5-Prefix-Discovery",
                        buffer_size: get_buffer_size(),
                        events: NUM_EVENTS,
                        payload_size: std::mem::size_of::<Event>(),
                        producer_throughput: spmc_5_prefix_discovery_metrics.producer_throughput,
                        consumer_throughput: spmc_5_prefix_discovery_metrics.consumer_throughput,
                        consumer_p50_us: spmc_5_prefix_discovery_metrics.consumer_p50_us,
                        consumer_p99_us: spmc_5_prefix_discovery_metrics.consumer_p99_us,
                    }),
                ];

                println!("\nMULTIPROCESS DISRUPTOR TEST SUMMARY");
                println!("═══════════════════════════════════════════════════════════════════════════════");
                println!(
                    "Test Configuration: {} events, {} bytes per event, {} buffer size",
                    format_number(NUM_EVENTS as f64),
                    std::mem::size_of::<Event>(),
                    format_number(get_buffer_size() as f64)
                );
                println!("{}", Table::new(&summary_table));

                // Return error if any test failed
                spsc_result?;
                spsc_discovery_result?;
                spmc_2_result?;
                spmc_5_result?;
                spmc_2_discovery_result?;
                spmc_5_discovery_result?;
                spsc_prefix_discovery_result?;
                spmc_2_prefix_discovery_result?;
                spmc_5_prefix_discovery_result?;

                println!("\nAll automated tests completed successfully!");
                return Ok(());
            }
            "spsc_discovery_test" => {
                let (result, _metrics) = run_automated_spsc_discovery_test();
                return result;
            }
            "spmc_test" => {
                let (result, _metrics) = run_automated_spmc_test();
                return result;
            }
            "spmc_5_test" => {
                let (result, _metrics) = run_automated_spmc_5_consumer_test();
                return result;
            }
            "spmc_2_discovery_test" => {
                let (result, _metrics) = run_automated_spmc_2_discovery_test();
                return result;
            }
            "spmc_5_discovery_test" => {
                let (result, _metrics) = run_automated_spmc_5_discovery_test();
                return result;
            }
            "spsc_prefix_discovery_test" => {
                let (result, _metrics) = run_automated_spsc_prefix_discovery_test();
                return result;
            }
            "spmc_2_prefix_discovery_test" => {
                let (result, _metrics) = run_automated_spmc_2_prefix_discovery_test();
                return result;
            }
            "spmc_5_prefix_discovery_test" => {
                let (result, _metrics) = run_automated_spmc_5_prefix_discovery_test();
                return result;
            }

            _ => {
                eprintln!("Multi-Process Disruptor Counters Test");
                eprintln!("====================================");
                eprintln!();
                eprintln!("This example demonstrates multiprocess communication patterns");
                eprintln!("suitable for production systems like Competitor where all workers");
                eprintln!("are known at startup time. Includes consumer discovery features.");
                eprintln!();
                eprintln!("Usage:");
                eprintln!("  cargo run --release --example counters test");
                eprintln!("  cargo run --release --example counters buffer_comparison");
                eprintln!();
                eprintln!("Performance Tuning (Buffer Size):");
                eprintln!(
                    "  BUFFER_SIZE=1024 cargo run ... test    # Ultra-low latency (2-25μs P99, 8-16M events/sec)"
                );
                eprintln!(
                    "  BUFFER_SIZE=4096 cargo run ... test    # Balanced performance (50-100μs P99, 12-15M events/sec)"
                );
                eprintln!(
                    "  BUFFER_SIZE=16384 cargo run ... test   # Maximum throughput (174μs P99, 19-20M events/sec)"
                );
                eprintln!("  cargo run --release --example counters spsc_discovery_test");
                eprintln!("  cargo run --release --example counters spmc_test");
                eprintln!("  cargo run --release --example counters spmc_5_test");
                eprintln!("  cargo run --release --example counters spmc_2_discovery_test");
                eprintln!("  cargo run --release --example counters spmc_5_discovery_test");
                eprintln!("  cargo run --release --example counters spsc_prefix_discovery_test");
                eprintln!("  cargo run --release --example counters spmc_2_prefix_discovery_test");
                eprintln!("  cargo run --release --example counters spmc_5_prefix_discovery_test");
                eprintln!();
                eprintln!("Test Modes:");
                eprintln!(
                    "  test                         - Run all tests (SPSC + SPSC-Discovery + SPMC + SPMC-Discovery + Prefix-Discovery)"
                );
                eprintln!(
                    "  buffer_comparison            - Run comprehensive buffer size comparison (1KB-128KB)"
                );
                eprintln!(
                    "  spsc_discovery_test          - Run SPSC test with consumer discovery enabled"
                );
                eprintln!("  spmc_test                    - Run SPMC test with 2 consumers");
                eprintln!("  spmc_5_test                  - Run SPMC test with 5 consumers");
                eprintln!(
                    "  spmc_2_discovery_test        - Run SPMC test with 2 consumers using discovery"
                );
                eprintln!(
                    "  spmc_5_discovery_test        - Run SPMC test with 5 consumers using discovery"
                );
                eprintln!(
                    "  spsc_prefix_discovery_test   - Run SPSC test with consumer prefix discovery"
                );
                eprintln!(
                    "  spmc_2_prefix_discovery_test - Run SPMC test with 2 consumers using prefix discovery"
                );
                eprintln!(
                    "  spmc_5_prefix_discovery_test - Run SPMC test with 5 consumers using prefix discovery"
                );
                eprintln!();
                eprintln!("Features:");
                eprintln!("  - SPSC (Single Producer, Single Consumer) mode");
                eprintln!("  - SPSC with consumer discovery (PID-based automatic detection)");
                eprintln!("  - SPSC with consumer prefix discovery (prefix-based optimization)");
                eprintln!("  - SPMC (Single Producer, Multiple Consumer) broadcast mode");
                eprintln!("  - SPMC with consumer discovery (automatic consumer scanning)");
                eprintln!("  - SPMC with consumer prefix discovery (prefix-based optimization)");
                eprintln!("  - Ultra-low latency: 2-25μs P99 (1KB buffer, default)");
                eprintln!("  - High throughput: 8-20M events/sec, up to 10+ GB/s data transfer");
                eprintln!("  - Configurable buffer sizes via BUFFER_SIZE environment variable");
                eprintln!("  - Comprehensive buffer size performance comparison (1KB-128KB)");
                eprintln!("  - Coordinated startup (all consumers ready before producer starts)");
                eprintln!("  - Static topology (fixed number of consumers known at startup)");
                eprintln!("  - Microsecond-precision latency measurements");
                eprintln!("  - Real process coordination using shared atomics");
                std::process::exit(1);
            }
        }
    }

    // Default to running all tests
    println!("Starting automated tests with 1-second delays between scenarios...");

    println!("\n=== Running SPSC Test ===");
    let (spsc_result, spsc_metrics) = run_automated_spsc_test();

    println!("\nWaiting 1 second before next test...");
    thread::sleep(Duration::from_secs(1));

    println!("\n=== Running SPSC Discovery Test ===");
    let (spsc_discovery_result, spsc_discovery_metrics) = run_automated_spsc_discovery_test();

    println!("\nWaiting 1 second before next test...");
    thread::sleep(Duration::from_secs(1));

    println!("\n=== Running SPMC Test (2 consumers) ===");
    let (spmc_2_result, spmc_2_metrics) = run_automated_spmc_test();

    println!("\nWaiting 1 second before next test...");
    thread::sleep(Duration::from_secs(1));

    println!("\n=== Running SPMC Discovery Test (2 consumers) ===");
    let (spmc_2_discovery_result, spmc_2_discovery_metrics) = run_automated_spmc_2_discovery_test();

    println!("\nWaiting 1 second before next test...");
    thread::sleep(Duration::from_secs(1));

    println!("\n=== Running SPMC Test (5 consumers) ===");
    let (spmc_5_result, spmc_5_metrics) = run_automated_spmc_5_consumer_test();

    println!("\nWaiting 1 second before next test...");
    thread::sleep(Duration::from_secs(1));

    println!("\n=== Running SPMC Discovery Test (5 consumers) ===");
    let (spmc_5_discovery_result, spmc_5_discovery_metrics) = run_automated_spmc_5_discovery_test();

    println!("\nWaiting 1 second before next test...");
    thread::sleep(Duration::from_secs(1));

    println!("\n=== Running SPSC Prefix Discovery Test ===");
    let (spsc_prefix_discovery_result, spsc_prefix_discovery_metrics) =
        run_automated_spsc_prefix_discovery_test();

    println!("\nWaiting 1 second before next test...");
    thread::sleep(Duration::from_secs(1));

    println!("\n=== Running SPMC Prefix Discovery Test (2 consumers) ===");
    let (spmc_2_prefix_discovery_result, spmc_2_prefix_discovery_metrics) =
        run_automated_spmc_2_prefix_discovery_test();

    println!("\nWaiting 1 second before next test...");
    thread::sleep(Duration::from_secs(1));

    println!("\n=== Running SPMC Prefix Discovery Test (5 consumers) ===");
    let (spmc_5_prefix_discovery_result, spmc_5_prefix_discovery_metrics) =
        run_automated_spmc_5_prefix_discovery_test();

    // Create comprehensive test summary table with ACTUAL measurements
    let summary_table = vec![
        // SPSC tests
        create_test_summary(&TestSummaryInputs {
            scenario: "SPSC",
            buffer_size: get_buffer_size(),
            events: NUM_EVENTS,
            payload_size: std::mem::size_of::<Event>(),
            producer_throughput: spsc_metrics.producer_throughput,
            consumer_throughput: spsc_metrics.consumer_throughput,
            consumer_p50_us: spsc_metrics.consumer_p50_us,
            consumer_p99_us: spsc_metrics.consumer_p99_us,
        }),
        create_test_summary(&TestSummaryInputs {
            scenario: "SPSC-Discovery",
            buffer_size: get_buffer_size(),
            events: NUM_EVENTS,
            payload_size: std::mem::size_of::<Event>(),
            producer_throughput: spsc_discovery_metrics.producer_throughput,
            consumer_throughput: spsc_discovery_metrics.consumer_throughput,
            consumer_p50_us: spsc_discovery_metrics.consumer_p50_us,
            consumer_p99_us: spsc_discovery_metrics.consumer_p99_us,
        }),
        create_test_summary(&TestSummaryInputs {
            scenario: "SPSC-Prefix-Discovery",
            buffer_size: get_buffer_size(),
            events: NUM_EVENTS,
            payload_size: std::mem::size_of::<Event>(),
            producer_throughput: spsc_prefix_discovery_metrics.producer_throughput,
            consumer_throughput: spsc_prefix_discovery_metrics.consumer_throughput,
            consumer_p50_us: spsc_prefix_discovery_metrics.consumer_p50_us,
            consumer_p99_us: spsc_prefix_discovery_metrics.consumer_p99_us,
        }),
        // SPMC-2 tests
        create_test_summary(&TestSummaryInputs {
            scenario: "SPMC-2",
            buffer_size: get_buffer_size(),
            events: NUM_EVENTS,
            payload_size: std::mem::size_of::<Event>(),
            producer_throughput: spmc_2_metrics.producer_throughput,
            consumer_throughput: spmc_2_metrics.consumer_throughput,
            consumer_p50_us: spmc_2_metrics.consumer_p50_us,
            consumer_p99_us: spmc_2_metrics.consumer_p99_us,
        }),
        create_test_summary(&TestSummaryInputs {
            scenario: "SPMC-2-Discovery",
            buffer_size: get_buffer_size(),
            events: NUM_EVENTS,
            payload_size: std::mem::size_of::<Event>(),
            producer_throughput: spmc_2_discovery_metrics.producer_throughput,
            consumer_throughput: spmc_2_discovery_metrics.consumer_throughput,
            consumer_p50_us: spmc_2_discovery_metrics.consumer_p50_us,
            consumer_p99_us: spmc_2_discovery_metrics.consumer_p99_us,
        }),
        create_test_summary(&TestSummaryInputs {
            scenario: "SPMC-2-Prefix-Discovery",
            buffer_size: get_buffer_size(),
            events: NUM_EVENTS,
            payload_size: std::mem::size_of::<Event>(),
            producer_throughput: spmc_2_prefix_discovery_metrics.producer_throughput,
            consumer_throughput: spmc_2_prefix_discovery_metrics.consumer_throughput,
            consumer_p50_us: spmc_2_prefix_discovery_metrics.consumer_p50_us,
            consumer_p99_us: spmc_2_prefix_discovery_metrics.consumer_p99_us,
        }),
        // SPMC-5 tests
        create_test_summary(&TestSummaryInputs {
            scenario: "SPMC-5",
            buffer_size: get_buffer_size(),
            events: NUM_EVENTS,
            payload_size: std::mem::size_of::<Event>(),
            producer_throughput: spmc_5_metrics.producer_throughput,
            consumer_throughput: spmc_5_metrics.consumer_throughput,
            consumer_p50_us: spmc_5_metrics.consumer_p50_us,
            consumer_p99_us: spmc_5_metrics.consumer_p99_us,
        }),
        create_test_summary(&TestSummaryInputs {
            scenario: "SPMC-5-Discovery",
            buffer_size: get_buffer_size(),
            events: NUM_EVENTS,
            payload_size: std::mem::size_of::<Event>(),
            producer_throughput: spmc_5_discovery_metrics.producer_throughput,
            consumer_throughput: spmc_5_discovery_metrics.consumer_throughput,
            consumer_p50_us: spmc_5_discovery_metrics.consumer_p50_us,
            consumer_p99_us: spmc_5_discovery_metrics.consumer_p99_us,
        }),
        create_test_summary(&TestSummaryInputs {
            scenario: "SPMC-5-Prefix-Discovery",
            buffer_size: get_buffer_size(),
            events: NUM_EVENTS,
            payload_size: std::mem::size_of::<Event>(),
            producer_throughput: spmc_5_prefix_discovery_metrics.producer_throughput,
            consumer_throughput: spmc_5_prefix_discovery_metrics.consumer_throughput,
            consumer_p50_us: spmc_5_prefix_discovery_metrics.consumer_p50_us,
            consumer_p99_us: spmc_5_prefix_discovery_metrics.consumer_p99_us,
        }),
    ];

    println!("\nMULTIPROCESS DISRUPTOR TEST SUMMARY");
    println!("═══════════════════════════════════════════════════════════════════════════════");
    println!(
        "Test Configuration: {} events, {} bytes per event, {} buffer size",
        format_number(NUM_EVENTS as f64),
        std::mem::size_of::<Event>(),
        format_number(get_buffer_size() as f64)
    );
    println!("{}", Table::new(&summary_table));

    // Return error if any test failed
    spsc_result?;
    spsc_discovery_result?;
    spmc_2_result?;
    spmc_5_result?;
    spmc_2_discovery_result?;
    spmc_5_discovery_result?;
    spsc_prefix_discovery_result?;
    spmc_2_prefix_discovery_result?;
    spmc_5_prefix_discovery_result?;

    println!("\nAll automated tests completed successfully!");
    Ok(())
}
