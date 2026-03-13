//! # Multi-process Performance Runner
//!
//! This runner provides consistent measurement of both Rust native implementations
//! and Python binding implementations using common modularized functions.
//!
//! ## Features
//!
//! - **Consistent Measurement**: Same timing methodology for Rust and Python
//! - **Modular Design**: Reusable test functions for different implementations  
//! - **Performance Comparison**: Side-by-side analysis of Rust vs Python
//! - **Comprehensive Results**: Detailed performance metrics and tables
//!
//! ## Usage
//!
//! ```bash
//! # Run both Rust and Python tests with comparison
//! cargo run --release --example performance_runner
//!
//! # Run only Rust native test
//! cargo run --release --example performance_runner -- --rust-only
//!
//! # Run only Python bindings test  
//! cargo run --release --example performance_runner -- --python-only
//! ```

use disruptor_mp::*;
use hdrhistogram::Histogram;
use num_format::{Locale, ToFormattedString};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{env, thread};
use tabled::{Table, Tabled};

// Configuration for consistent testing
const NUM_EVENTS: u64 = 1_000; // Reduced for faster testing
const DEFAULT_BUFFER_SIZE: usize = 1024;
// const ELEMENT_SIZE: usize = 128;
const TIMEOUT_SECONDS: u64 = 45;

/// Event structure matching both Rust and Python implementations
#[repr(C, packed)]
#[derive(Clone, Copy)]
struct Event {
    value: u32,         // 4 bytes
    timestamp_ns: u64,  // 8 bytes
    payload: [u8; 116], // 116 bytes
}

impl Default for Event {
    fn default() -> Self {
        let mut payload = [0u8; 116];
        for (i, item) in payload.iter_mut().enumerate() {
            *item = (i % 256) as u8;
        }
        Event {
            value: 0,
            timestamp_ns: 0,
            payload,
        }
    }
}

/// Performance results for a single test run
#[derive(Debug, Clone)]
struct PerformanceResults {
    implementation: String,
    producer_throughput: f64,
    consumer_throughput: f64,
    producer_latency_p99_ns: f64,
    consumer_latency_p99_us: f64,
    success: bool,
    duration_ms: f64,
}

impl Default for PerformanceResults {
    fn default() -> Self {
        PerformanceResults {
            implementation: String::new(),
            producer_throughput: 0.0,
            consumer_throughput: 0.0,
            producer_latency_p99_ns: 0.0,
            consumer_latency_p99_us: 0.0,
            success: false,
            duration_ms: 0.0,
        }
    }
}

/// Comprehensive test summary for comparison tables
#[derive(Tabled, Clone)]
struct ComparisonSummary {
    #[tabled(rename = "Implementation")]
    implementation: String,
    #[tabled(rename = "Producer\n(ops/sec)")]
    producer_ops: String,
    #[tabled(rename = "Consumer\n(ops/sec)")]
    consumer_ops: String,
    #[tabled(rename = "Producer\n% of Rust")]
    producer_percentage: String,
    #[tabled(rename = "Consumer\n% of Rust")]
    consumer_percentage: String,
    #[tabled(rename = "Overall\n% of Rust")]
    overall_percentage: String,
    #[tabled(rename = "Producer P99\n(μs)")]
    producer_p99_us: String,
    #[tabled(rename = "Consumer P99\n(μs)")]
    consumer_p99_us: String,
    #[tabled(rename = "Success")]
    success: String,
}

/// Get buffer size from environment variable
fn get_buffer_size() -> usize {
    env::var("BUFFER_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&size| size > 0 && (size & (size - 1)) == 0)
        .unwrap_or(DEFAULT_BUFFER_SIZE)
}

/// Get segment name for testing
fn get_segment_name() -> String {
    env::var("SEGMENT_NAME").unwrap_or_else(|_| format!("perf{}", std::process::id()))
}

/// Format large numbers with thousands separators
fn format_number(num: f64) -> String {
    (num as i64).to_formatted_string(&Locale::en)
}

/// Run Rust native SPSC test using counters_auto pattern
fn run_rust_native_test() -> PerformanceResults {
    println!("🦀 Running Rust Native SPSC Test...");

    let mut result = PerformanceResults {
        implementation: "Rust Native".to_string(),
        ..Default::default()
    };

    let segment_name = get_segment_name();
    let buffer_size = get_buffer_size();

    let test_start = Instant::now();

    // Start producer in separate thread (simulating separate process)
    let segment_name_producer = segment_name.clone();
    let producer_handle = thread::spawn(
        move || -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
            let mut producer =
                build_shared_single_producer::<Event>(&segment_name_producer, buffer_size)
                    .enable_discovery(1)
                    .build_producer(Event::default)?;

            println!(
                "Rust Producer: Starting production of {} events",
                NUM_EVENTS
            );

            let start_time = Instant::now();

            for i in 0..NUM_EVENTS {
                let publish_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;

                producer.publish(|event| {
                    event.value = 1;
                    event.timestamp_ns = publish_time;
                    event.payload[0] = (i & 0xFF) as u8;
                });

                if i.is_multiple_of(10_000) && i > 0 {
                    println!("Rust Producer: Published {} events", i);
                }
            }

            let elapsed = start_time.elapsed();
            let throughput = NUM_EVENTS as f64 / elapsed.as_secs_f64();

            println!("Rust Producer: Finished - {:.0} events/sec", throughput);
            Ok(throughput)
        },
    );

    // Give producer time to start
    thread::sleep(Duration::from_millis(100));

    // Start consumer
    let consumer_result = (|| -> Result<f64, Box<dyn std::error::Error>> {
        let events_consumed = Arc::new(AtomicU64::new(0));
        let total_counter = Arc::new(AtomicI64::new(0));
        let latency_histogram = Arc::new(std::sync::Mutex::new(Histogram::<u64>::new(3)?));

        let events_consumed_clone = Arc::clone(&events_consumed);
        let total_counter_clone = Arc::clone(&total_counter);
        let latency_histogram_clone = Arc::clone(&latency_histogram);

        let _consumer = attach_shared_consumer::<Event>(&segment_name, buffer_size)
            .handle_events_with(move |event: &Event, _sequence, _end_of_batch| {
                let consume_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;

                // Calculate latency
                let latency_ns = consume_time.saturating_sub(event.timestamp_ns);
                let latency_us = latency_ns / 1000;
                if let Ok(mut histogram) = latency_histogram_clone.lock() {
                    histogram.record(latency_us).unwrap_or(());
                }

                let consumed = events_consumed_clone.fetch_add(1, Ordering::Relaxed) + 1;
                total_counter_clone.fetch_add(
                    event.value as i64 + event.payload[0] as i64,
                    Ordering::Relaxed,
                );

                if consumed.is_multiple_of(10_000) {
                    println!("Rust Consumer: Consumed {} events", consumed);
                }
            })?;

        println!("Rust Consumer: Waiting for events...");

        // Wait for completion
        let timeout = Duration::from_secs(TIMEOUT_SECONDS);
        let wait_start = Instant::now();

        while events_consumed.load(Ordering::Relaxed) < NUM_EVENTS {
            if wait_start.elapsed() > timeout {
                return Err("Rust Consumer timeout".into());
            }
            thread::sleep(Duration::from_millis(100));
        }

        let processing_time = wait_start.elapsed();
        let throughput = NUM_EVENTS as f64 / processing_time.as_secs_f64();

        println!("Rust Consumer: Finished - {:.0} events/sec", throughput);

        Ok(throughput)
    })();

    // Wait for producer to complete
    let producer_throughput = match producer_handle.join() {
        Ok(Ok(throughput)) => throughput,
        Ok(Err(e)) => {
            println!("Rust Producer error: {}", e);
            return result;
        }
        Err(_) => {
            println!("Rust Producer thread panic");
            return result;
        }
    };

    let consumer_throughput = match consumer_result {
        Ok(throughput) => throughput,
        Err(e) => {
            println!("Rust Consumer error: {}", e);
            return result;
        }
    };

    result.producer_throughput = producer_throughput;
    result.consumer_throughput = consumer_throughput;
    result.success = true;
    result.duration_ms = test_start.elapsed().as_millis() as f64;

    result
}

/// Run Python bindings test using the equivalent implementation
fn run_python_bindings_test() -> PerformanceResults {
    println!("🐍 Running Python Bindings SPSC Test...");

    let mut result = PerformanceResults {
        implementation: "Python Bindings".to_string(),
        ..Default::default()
    };

    let test_start = Instant::now();

    // Use the automatic counters example (1:1 equivalent to counters_auto.rs).
    // Support both legacy disruptor-rs-playground and current myelon layouts.
    let python_script_candidates = [
        "bindings/python/examples/counters/py_bindings_mp_counters_auto.py",
        "../../python-surface-archive/examples/counters/py_bindings_mp_counters_auto.py",
    ];
    let python_script = match python_script_candidates
        .iter()
        .find(|candidate| std::path::Path::new(candidate).exists())
    {
        Some(path) => *path,
        None => {
            println!("❌ Python script not found. Tried:");
            for candidate in &python_script_candidates {
                println!("   - {}", candidate);
            }
            return result;
        }
    };
    println!("🐍 Using Python script: {}", python_script);

    println!("🐍 Executing Python equivalent test...");

    // Run Python test
    println!("🐍 Running Python test (this may take a moment)...");
    let output = Command::new("python3")
        .arg(python_script)
        .env("PYTHONUNBUFFERED", "1")
        .env("NUM_EVENTS", NUM_EVENTS.to_string()) // Pass reduced event count
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();

    match output {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);

            println!("🐍 Python Output:");
            println!("{}", stdout);

            if !stderr.is_empty() {
                println!("🐍 Python Errors:");
                println!("{}", stderr);
            }

            result.success = output.status.success();

            // Parse performance numbers from output
            for line in stdout.lines() {
                if line.contains("Throughput:") && line.contains("events/sec") {
                    if let Some(throughput_str) = line.split("Throughput:").nth(1) {
                        if let Some(number_str) = throughput_str.split("events/sec").next() {
                            if let Ok(throughput) = number_str.trim().parse::<f64>() {
                                if result.producer_throughput == 0.0 {
                                    result.producer_throughput = throughput;
                                } else {
                                    result.consumer_throughput = throughput;
                                }
                            }
                        }
                    }
                }
            }

            if result.success
                && result.producer_throughput > 0.0
                && result.consumer_throughput > 0.0
            {
                println!("✅ Python test completed successfully");
                println!("📊 Producer: {:.0} events/sec", result.producer_throughput);
                println!("📊 Consumer: {:.0} events/sec", result.consumer_throughput);
            } else {
                println!("❌ Python test failed or incomplete results");
                result.success = false;
            }
        }
        Err(e) => {
            println!("❌ Failed to execute Python test: {}", e);
        }
    }

    result.duration_ms = test_start.elapsed().as_millis() as f64;
    result
}

/// Create comparison summary table
fn create_comparison_summary(
    rust_result: &PerformanceResults,
    python_result: &PerformanceResults,
) -> Vec<ComparisonSummary> {
    let rust_producer_baseline = rust_result.producer_throughput;
    let rust_consumer_baseline = rust_result.consumer_throughput;

    vec![
        // Rust native row
        ComparisonSummary {
            implementation: "Rust Native".to_string(),
            producer_ops: format_number(rust_result.producer_throughput),
            consumer_ops: format_number(rust_result.consumer_throughput),
            producer_percentage: "100.0%".to_string(),
            consumer_percentage: "100.0%".to_string(),
            overall_percentage: "100.0%".to_string(),
            producer_p99_us: format!("{:.3}", rust_result.producer_latency_p99_ns / 1000.0),
            consumer_p99_us: format!("{:.3}", rust_result.consumer_latency_p99_us),
            success: if rust_result.success { "✅" } else { "❌" }.to_string(),
        },
        // Python bindings row
        ComparisonSummary {
            implementation: "Python Bindings".to_string(),
            producer_ops: format_number(python_result.producer_throughput),
            consumer_ops: format_number(python_result.consumer_throughput),
            producer_percentage: if rust_producer_baseline > 0.0 {
                format!(
                    "{:.1}%",
                    (python_result.producer_throughput / rust_producer_baseline) * 100.0
                )
            } else {
                "N/A".to_string()
            },
            consumer_percentage: if rust_consumer_baseline > 0.0 {
                format!(
                    "{:.1}%",
                    (python_result.consumer_throughput / rust_consumer_baseline) * 100.0
                )
            } else {
                "N/A".to_string()
            },
            overall_percentage: if rust_consumer_baseline > 0.0 {
                // Use consumer throughput as overall (it's typically the bottleneck)
                format!(
                    "{:.1}%",
                    (python_result.consumer_throughput / rust_consumer_baseline) * 100.0
                )
            } else {
                "N/A".to_string()
            },
            producer_p99_us: format!("{:.3}", python_result.producer_latency_p99_ns / 1000.0),
            consumer_p99_us: format!("{:.3}", python_result.consumer_latency_p99_us),
            success: if python_result.success { "✅" } else { "❌" }.to_string(),
        },
    ]
}

/// Main performance comparison runner
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Multi-process Performance Runner");
    println!("Comparing Rust native vs Python bindings performance");
    println!("{}", "=".repeat(80));

    let args: Vec<String> = env::args().collect();
    let rust_only = args.contains(&"--rust-only".to_string());
    let python_only = args.contains(&"--python-only".to_string());

    let mut results = Vec::new();

    // Run Rust native test
    if !python_only {
        let rust_result = run_rust_native_test();
        results.push(rust_result);
    }

    // Run Python bindings test
    if !rust_only {
        let python_result = run_python_bindings_test();
        results.push(python_result);
    }

    // Display results
    println!("\n{}", "=".repeat(80));
    println!("🏆 PERFORMANCE COMPARISON RESULTS");
    println!("{}", "=".repeat(80));

    if results.len() >= 2 {
        let rust_result = &results[0];
        let python_result = &results[1];

        let comparison_table = create_comparison_summary(rust_result, python_result);
        let table = Table::new(comparison_table);
        println!("{}", table);

        // Detailed analysis
        println!("\n📊 DETAILED ANALYSIS:");
        println!("{}", "─".repeat(80));

        if rust_result.success && python_result.success {
            let producer_ratio =
                (python_result.producer_throughput / rust_result.producer_throughput) * 100.0;
            let consumer_ratio =
                (python_result.consumer_throughput / rust_result.consumer_throughput) * 100.0;

            println!(
                "Producer Performance: {:.1}% of Rust baseline",
                producer_ratio
            );
            println!(
                "Consumer Performance: {:.1}% of Rust baseline",
                consumer_ratio
            );

            // Achievement analysis
            println!("\n🎯 TARGET ACHIEVEMENT:");
            println!("Target: 70% producer, 50% consumer");

            if producer_ratio >= 70.0 {
                println!("✅ Producer target ACHIEVED: {:.1}% ≥ 70%", producer_ratio);
            } else {
                println!("❌ Producer target MISSED: {:.1}% < 70%", producer_ratio);
            }

            if consumer_ratio >= 50.0 {
                println!("✅ Consumer target ACHIEVED: {:.1}% ≥ 50%", consumer_ratio);
            } else {
                println!("❌ Consumer target MISSED: {:.1}% < 50%", consumer_ratio);
            }

            // Overall assessment
            let overall_ratio = consumer_ratio.min(producer_ratio);
            println!(
                "\n🏆 OVERALL PERFORMANCE: {:.1}% of Rust baseline",
                overall_ratio
            );

            if overall_ratio >= 50.0 {
                println!("🎉 EXCELLENT: Python bindings achieve high performance!");
            } else if overall_ratio >= 30.0 {
                println!("👍 GOOD: Python bindings show solid performance");
            } else {
                println!("⚠️ NEEDS IMPROVEMENT: Python bindings need optimization");
            }
        } else {
            println!("❌ Cannot compare - one or both tests failed");
        }
    } else {
        // Single test result
        for result in &results {
            println!("Implementation: {}", result.implementation);
            println!("Success: {}", if result.success { "✅" } else { "❌" });
            println!(
                "Producer Throughput: {:.0} events/sec",
                result.producer_throughput
            );
            println!(
                "Consumer Throughput: {:.0} events/sec",
                result.consumer_throughput
            );
            println!("Duration: {:.1}ms", result.duration_ms);
        }
    }

    println!("\n{}", "=".repeat(80));
    println!("🏁 Performance Runner Complete");

    // Exit with success only if all tests passed
    let all_success = results.iter().all(|r| r.success);
    if !all_success {
        std::process::exit(1);
    }

    Ok(())
}
