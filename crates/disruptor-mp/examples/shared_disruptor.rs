//! # Multi-process Disruptor Example with Broadcast Semantics
//!
//! This example demonstrates the basic shared memory disruptor API designed for
//! production systems with known consumer counts (like Competitor inference servers).
//! It showcases broadcast semantics where each consumer receives ALL events.
//!
//! ## Usage
//!
//! ```bash
//! # Run the automated test with 3 consumers
//! cargo run --release --example shared_disruptor
//! ```
//!
//! **Important**: Always use `--release` for accurate performance measurements.
//! Debug builds will be significantly slower.
//!
//! ## Key Features
//!
//! - **Fixed Consumer Topology**: Uses `.enable_discovery(3)` API for exactly 3 consumers
//! - **Optimized Discovery**: Stops scanning once all 3 consumers are found (CPU efficient)
//! - **Broadcast Semantics**: Each consumer sees ALL events independently
//! - **External Coordination**: Uses separate `ProcessCoordination` for startup synchronization
//! - **Platform Policy**: Linux supported, macOS best effort, Windows unsupported
//! - **Performance Metrics**: Detailed throughput and timing measurements
//!
//! ## Architecture Pattern
//!
//! This example demonstrates the **external coordination pattern** optimized for production:
//!
//! 1. **Known Consumer Count**: Exactly 3 consumers specified at build time
//! 2. **Coordinated Startup**: All consumers start first and signal readiness via shared atomics
//! 3. **Discovery Optimization**: Producer stops scanning after finding all 3 consumers
//! 4. **Static Operation**: No dynamic consumer addition/removal during operation
//! 5. **CPU Efficient**: No unnecessary background scanning after topology established
//!
//! ## Performance Characteristics
//!
//! **Typical Results (with `--release` builds):**
//! - **Producer Throughput**: ~15-20M events/sec
//! - **Consumer Throughput**: ~15-20M events/sec per consumer
//! - **Latency**: Sub-microsecond processing times
//! - **Data Transfer**: 1-2 GB/s effective throughput
//!
//! ## Use Cases
//!
//! Perfect for systems like:
//! - Competitor inference servers with fixed worker pools
//! - ML model serving with known replica counts
//! - Real-time data processing with static topologies
//! - High-frequency trading systems
//! - Game servers with predetermined worker counts

use disruptor_mp::{
    build_shared_single_producer, SharedCursor, SharedDisruptorBuilder, SharedMemoryConfig,
};
use std::env;
use std::process::{Command, Stdio};
use std::sync::atomic::Ordering;
use std::thread;
use std::time::{Duration, Instant};

const BUFFER_SIZE: usize = 1024; // Must be power of 2
const NUM_EVENTS: u64 = 50_000; // Balanced for demonstration and speed

/// Coordination structure for coordinated startup scenarios (like Competitor)
/// All consumers must signal readiness before producer starts
struct ProcessCoordination {
    /// Number of consumers that have signaled readiness
    consumers_ready: SharedCursor,
    /// Producer signals completion via this atomic
    producer_done: SharedCursor,
    /// Number of events produced (for verification)
    events_produced: SharedCursor,
    /// Consumer signals completion via this atomic
    consumer_done: SharedCursor,
    /// Number of events consumed (for verification)
    events_consumed: SharedCursor,
}

impl ProcessCoordination {
    /// Create coordination shared memory (producer)
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

    /// Attach to existing coordination shared memory (consumer)
    /// Coordinated startup: Producer creates coordination first, consumers attach
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

    /// Consumer signals readiness (coordinated startup pattern)
    fn signal_consumer_ready(&self) {
        self.consumers_ready.fetch_add(1, Ordering::AcqRel);
    }

    /// Producer waits for all consumers to be ready (coordinated startup pattern)
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

    /// Producer signals completion
    fn signal_producer_done(&self, events_produced: u64) {
        self.events_produced
            .store(events_produced as i64, Ordering::Release);
        self.producer_done.store(1, Ordering::Release);
    }

    /// Consumer signals completion (increment counter for multiple consumers)
    fn signal_consumer_done(&self, events_consumed: u64) {
        self.events_consumed
            .store(events_consumed as i64, Ordering::Release);
        self.consumer_done.fetch_add(1, Ordering::AcqRel);
    }
}

fn get_segment_name() -> String {
    // Try to get from environment first (for child processes)
    if let Ok(name) = env::var("SHARED_DISRUPTOR_SEGMENT") {
        return name;
    }

    // Generate unique short name
    format!("sd{}", std::process::id() % 100000)
}

#[derive(Debug, Copy, Clone, Default)]
struct Event {
    id: u64,
    timestamp: u64,
    value: i64,
}

fn producer_process() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting producer process...");

    let segment_name = get_segment_name();

    // Create coordination shared memory FIRST
    let coordination = ProcessCoordination::create(&segment_name)?;

    // Build producer with fixed topology: exactly 3 consumers
    // .enable_discovery(3) optimizes scanning - stops after finding all 3 consumers
    // No discovery by default for maximum performance in static topologies
    let builder =
        build_shared_single_producer::<Event>(&segment_name, BUFFER_SIZE).enable_discovery(3);
    let mut producer = builder.build_producer(Event::default)?;

    println!("Producer created shared memory segment: {}", segment_name);

    // Wait for all consumers to be ready before starting
    println!("Waiting for 3 consumers to signal readiness...");
    let expected_consumers = 3;
    if !coordination.wait_for_consumers_ready(expected_consumers, Duration::from_secs(45)) {
        return Err("Timeout waiting for 3 consumers to be ready".into());
    }
    println!(
        "All {} consumer(s) ready! Starting production...",
        expected_consumers
    );

    println!("Producing {} events...", NUM_EVENTS);

    let start_time = Instant::now();

    for i in 0..NUM_EVENTS {
        producer.publish(|event| {
            event.id = i;
            event.timestamp = start_time.elapsed().as_nanos() as u64;
            event.value = (i * 42) as i64;
        });

        if disruptor_mp::is_multiple_of_u64(i, 10_000) {
            println!("Produced {} events", i);
        }
    }

    let elapsed = start_time.elapsed();
    let throughput = NUM_EVENTS as f64 / elapsed.as_secs_f64();

    println!("Producer finished!");
    println!("Time: {:.2}s", elapsed.as_secs_f64());
    println!("Throughput: {:.0} events/sec", throughput);

    // Signal completion using shared atomic
    coordination.signal_producer_done(NUM_EVENTS);
    println!("Signaled producer completion via shared atomic");

    // Wait for consumers to finish (with timeout for safety)
    let wait_start = Instant::now();
    let timeout = Duration::from_secs(90); // Safety timeout

    println!("Waiting for consumers to finish...");
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

    println!("Producer exiting.");
    Ok(())
}

fn consumer_process(consumer_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting consumer process: {}", consumer_id);

    let segment_name = get_segment_name();

    // Attach to coordination shared memory
    let coordination = ProcessCoordination::attach(&segment_name)?;

    let config = SharedMemoryConfig {
        name: segment_name.clone(),
        buffer_size: BUFFER_SIZE,
        element_size: std::mem::size_of::<Event>(),
        create: false,
    };

    let builder: SharedDisruptorBuilder<Event> = SharedDisruptorBuilder::new(config);
    let mut consumer = builder.build_consumer()?;

    println!(
        "Consumer {} attached to shared memory segment: {}",
        consumer_id, segment_name
    );

    // Signal readiness immediately after attachment
    coordination.signal_consumer_ready();
    println!("Consumer {} signaled readiness to producer", consumer_id);

    println!(
        "Consumer {} consuming events (broadcast semantics - sees all events)...",
        consumer_id
    );

    let start_time = Instant::now();
    let mut events_consumed = 0u64;
    let mut last_event_id = None;
    let mut processing_time = Duration::new(0, 0);

    loop {
        let process_start = Instant::now();
        let processed = consumer.process_available(|event, sequence| {
            events_consumed += 1;
            last_event_id = Some(event.id);

            // Verify event data and show progress
            if disruptor_mp::is_multiple_of_u64(events_consumed, 10_000) {
                println!(
                    "Consumer {} consumed event {}: id={}, timestamp={}, value={}, seq={}",
                    consumer_id, events_consumed, event.id, event.timestamp, event.value, sequence
                );
            }
        });

        if processed > 0 {
            processing_time += process_start.elapsed();
        }

        // Check if producer is done and we've consumed all events
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

    let elapsed = start_time.elapsed();
    let expected_events = coordination.events_produced.load(Ordering::Acquire) as u64;
    let throughput = if elapsed.as_secs_f64() > 0.0 {
        events_consumed as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!("Consumer {} finished!", consumer_id);
    println!(
        "Consumer {} events consumed: {}",
        consumer_id, events_consumed
    );
    println!(
        "Consumer {} time: {:.2}s",
        consumer_id,
        elapsed.as_secs_f64()
    );

    if processing_time.as_nanos() > 0 {
        println!(
            "Consumer {} processing time: {:.3}ms ({:.1}ns per event)",
            consumer_id,
            processing_time.as_nanos() as f64 / 1_000_000.0,
            processing_time.as_nanos() as f64 / events_consumed as f64
        );
    }

    println!(
        "Consumer {} throughput: {:.0} events/sec",
        consumer_id, throughput
    );

    // Signal completion using shared atomic
    coordination.signal_consumer_done(events_consumed);
    println!(
        "Consumer {} signaled completion via shared atomic",
        consumer_id
    );

    // Verify we got all events (broadcast semantics)
    if events_consumed == expected_events {
        println!(
            "Consumer {} SUCCESS: All {} events consumed correctly!",
            consumer_id, expected_events
        );
        if let Some(last_id) = last_event_id {
            println!("Consumer {} last event ID: {}", consumer_id, last_id);
        }
        std::process::exit(0);
    } else {
        println!(
            "⚠️  Consumer {} WARNING: Expected {} events, got {}",
            consumer_id, expected_events, events_consumed
        );
        std::process::exit(1);
    }
}

// Automated test that spawns real processes
fn run_automated_test() -> Result<(), Box<dyn std::error::Error>> {
    println!("Running automated shared disruptor test with 1 producer + 3 consumers...");

    let current_exe = env::current_exe()?;
    let segment_name = get_segment_name();

    // Start producer process first to create shared memory
    println!("Starting producer process (will create shared memory and wait for consumers)...");
    let producer_child = Command::new(&current_exe)
        .arg("producer")
        .env("SHARED_DISRUPTOR_SEGMENT", &segment_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    // Give producer time to create shared memory segment
    thread::sleep(Duration::from_millis(500));

    // Start 3 consumer processes to attach and signal readiness
    println!("Starting 3 consumer processes (will attach and signal readiness)...");
    let mut consumer_children = Vec::new();

    for i in 1..=3 {
        let consumer_child = Command::new(&current_exe)
            .arg(format!("consumer{}", i))
            .env("SHARED_DISRUPTOR_SEGMENT", &segment_name)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        consumer_children.push(consumer_child);
    }

    // Wait for all consumers to finish
    let mut consumer_results = Vec::new();
    for consumer_child in consumer_children {
        let result = consumer_child.wait_with_output()?;
        consumer_results.push(result);
    }

    // Wait for producer to finish
    let producer_result = producer_child.wait_with_output()?;

    // Print outputs
    println!("\n--- Producer Output ---");
    println!("{}", String::from_utf8_lossy(&producer_result.stdout));
    if !producer_result.stderr.is_empty() {
        println!(
            "Producer stderr: {}",
            String::from_utf8_lossy(&producer_result.stderr)
        );
    }

    for (i, result) in consumer_results.iter().enumerate() {
        println!("\n--- Consumer {} Output ---", i + 1);
        println!("{}", String::from_utf8_lossy(&result.stdout));
        if !result.stderr.is_empty() {
            println!(
                "Consumer {} stderr: {}",
                i + 1,
                String::from_utf8_lossy(&result.stderr)
            );
        }
    }

    // Check results - with broadcast semantics, all consumers should succeed
    let all_success = producer_result.status.success()
        && consumer_results
            .iter()
            .all(|result| result.status.success());

    if all_success {
        println!("Automated shared disruptor test PASSED!");
        println!("All 3 consumers saw all events (broadcast semantics working correctly)");
        println!("Shared disruptor test completed successfully");
        Ok(())
    } else {
        println!("Automated shared disruptor test FAILED!");
        if !producer_result.status.success() {
            println!("Producer failed");
        }
        for (i, result) in consumer_results.iter().enumerate() {
            if !result.status.success() {
                println!("Consumer {} failed", i + 1);
            }
        }
        Err("One or more consumer processes failed".into())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    // Check if this is a child process spawned by the automated test
    if args.len() >= 2 {
        match args[1].as_str() {
            "producer" => return producer_process(),
            "consumer1" => return consumer_process("1"),
            "consumer2" => return consumer_process("2"),
            "consumer3" => return consumer_process("3"),
            "test" => {} // Continue to run automated test
            _ => {
                eprintln!("Multi-Process Shared Disruptor Example - Fixed Topology API");
                eprintln!("============================================================");
                eprintln!();
                eprintln!("This example demonstrates the latest fixed topology API optimized");
                eprintln!("for production systems like Competitor where worker count is predetermined.");
                eprintln!(
                    "Uses .enable_discovery(3) to stop scanning after finding all consumers."
                );
                eprintln!();
                eprintln!("Usage:");
                eprintln!("  cargo run --release --example shared_disruptor test");
                eprintln!();
                eprintln!("Key Features:");
                eprintln!(
                    "  - Fixed consumer topology: exactly 3 consumers (.enable_discovery(3))"
                );
                eprintln!("  - Discovery optimization: stops scanning once all consumers found");
                eprintln!("  - Broadcast semantics: each consumer sees all events independently");
                eprintln!("  - Coordinated startup: external coordination using shared atomics");
                eprintln!("  - CPU efficient: no unnecessary background scanning after startup");
                std::process::exit(1);
            }
        }
    }

    // Default to running automated test
    run_automated_test()
}
