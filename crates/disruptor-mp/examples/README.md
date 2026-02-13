# Multi-Process Examples

Practical examples demonstrating high-performance multi-process communication with shared memory, optimized for production systems like Competitor.

**📖 Full documentation:** [../../README.md](../../README.md)

## Quick Start

```toml
[dependencies]
disruptor = { version = "3.4.0" }
```

## Examples

### `shared_disruptor.rs` - Basic 3-Consumer Broadcast

Demonstrates broadcast semantics with 3 consumers (each sees all events):

```bash
cargo run --release --example shared_disruptor
```

**Key Features:**

- Fixed consumer topology (exactly 3 consumers)
- External coordination using shared atomics
- Broadcast semantics validation
- Cross-platform shared memory

### `counters.rs` - External Coordination Test Suite

Comprehensive test suite using **external coordination** patterns:

```bash
# All tests (SPSC + SPMC 2-consumer + SPMC 5-consumer + Discovery)
cargo run --release --example counters test

# Individual test modes
cargo run --release --example counters spsc_test
cargo run --release --example counters spmc_test
cargo run --release --example counters spmc_5_test

# Buffer size performance comparison
cargo run --release --example counters buffer_comparison
```

**Key Features:**

- External `ProcessCoordination` for maximum performance
- Consumer discovery with PID scanning and prefix-based discovery
- Comprehensive performance metrics and latency analysis
- Buffer size optimization testing (1KB-128KB)
- Static topology patterns for production systems

### `counters_auto.rs` - Automatic Coordination Test Suite (Complete)

Production-ready test suite using **automatic coordination** built into the disruptor:

```bash
# All tests with automatic coordination (runs by default)
BUFFER_SIZE=1024 cargo run --release --example counters_auto

# Individual test modes
cargo run --release --example counters_auto spsc_test
cargo run --release --example counters_auto spmc_test
cargo run --release --example counters_auto spmc_5_test
```

**Features (Production Ready):**

- **Automatic Event Delivery**: No manual polling loops - use `.handle_events_with()` for hands-off event processing
- **Built-in Coordination**: No external `ProcessCoordination` struct needed - coordination built directly into disruptor
- **Automatic Resource Management**: `Drop` implementation handles cleanup automatically
- **Improved Error Handling**: Better timeout configuration and clearer error messages
- **Clean API**: Streamlined builder pattern for rapid development and deployment
- **Performance Parity**: Matches external coordination performance (18M+ events/sec typical)

**Key Achievements:**

- Consumer discovery with automatic coordination
- Background thread management for event handlers
- Production-ready timeout handling and graceful degradation
- Comprehensive test coverage for SPSC, SPMC-2, SPMC-5 scenarios

## Performance Characteristics

### Coordination Patterns

**External Coordination (`counters.rs`):**

- Uses separate `ProcessCoordination` struct with shared atomics
- Maximum performance for static topologies
- Manual coordination timing and consumer discovery
- Ideal for production systems with known worker counts

**Automatic Coordination (`counters_auto.rs`) - Complete:**

- Built-in coordination within disruptor framework (no external ProcessCoordination needed)
- Simplified API with automatic event delivery via `.handle_events_with()`
- **Production-ready performance** matching external coordination (18M+ events/sec)
- **Automatic resource management** with Drop implementation cleanup
- **Superior for rapid development AND production deployment**

### Performance Results

**Typical Performance (with `--release` builds):**

| Configuration | Producer Throughput | Consumer Throughput | Latency P99 |
| ------------- | ------------------: | ------------------: | ----------: |
| **SPSC**      |  ~13-18M events/sec |  ~16-20M events/sec |    ~37-78μs |
| **SPMC-2**    |  ~14-21M events/sec |  ~20-21M events/sec |     ~7-30μs |
| **SPMC-5**    |    ~7-9M events/sec |     ~17M events/sec |    ~0-106μs |

**Buffer Size Impact:**

- **1KB (default)**: Ultra-low latency (2-25μs P99, 8-16M events/sec)
- **4KB**: Balanced performance (50-100μs P99, 12-15M events/sec)
- **16KB**: Maximum throughput (174μs P99, 19-20M events/sec)

### Startup Sequence

All examples use coordinated startup for optimal performance:

1. **Producer starts first**: Creates shared memory and coordination structures
2. **Consumers attach and signal readiness**: Each consumer signals when ready
3. **Producer waits for all consumers**: Ensures coordination is complete
4. **Producer starts publishing**: Now runs at maximum speed with minimal overhead

## Shared Memory Naming

Examples use simple, unique naming:

- **counters**: `mp{process_id}` (e.g., `mp12345`)
- **counters_auto**: `mpa{process_id}` (e.g., `mpa67890`)
- **shared_disruptor**: `sd{process_id}` (e.g., `sd54321`)

See [multiprocess documentation](../../README.md) for naming constraints and best practices.

## Performance Tuning

### Buffer Size Configuration

```bash
# Ultra-low latency (2-25μs P99)
BUFFER_SIZE=1024 cargo run --release --example counters test

# Balanced performance (50-100μs P99)
BUFFER_SIZE=4096 cargo run --release --example counters test

# Maximum throughput (174μs P99)
BUFFER_SIZE=16384 cargo run --release --example counters test
```

### Discovery Modes

**PID-based Discovery:**

- Automatic consumer detection using process IDs
- Good for dynamic environments
- Slightly higher discovery overhead

**Prefix-based Discovery:**

- Optimized discovery using consumer name prefixes
- Faster discovery for known naming patterns
- Ideal for static topologies

## Use Cases

These examples are ideal for:

- **Static worker topologies** (like Competitor inference servers)
- **High-frequency publishing** (millions of events per second)
- **Predictable consumer counts** (known at startup time)
- **Broadcast scenarios** (all consumers need to see all events)
- **ML inference servers** with fixed worker pools
- **Real-time data processing** pipelines

## Troubleshooting

### Common Issues

**Permission errors**: Examples handle shared memory cleanup automatically
**Platform differences**: Examples are cross-platform compatible (Linux, macOS, Windows)
**Debug performance**: Always use `--release` builds for performance testing
**Shared memory conflicts**: Examples use unique segment names to avoid conflicts

### Debug Output

All examples include detailed logging:

- Process coordination timing
- Consumer discovery progress
- Performance metrics and latency percentiles
- Event counting validation
- Shared memory segment information

## Benchmarks

Compare performance with other IPC libraries:

```bash
# Run comprehensive IPC benchmark
cargo bench --bench ipc_shm

# Compare with specific competitors
COMPETITOR=shared-mem-queue cargo bench --bench ipc_shm
COMPETITOR=unix-socket cargo bench --bench ipc_shm
```

**Expected results**: disruptor-rs achieves 60-90ns per event vs 150-1350ns for competitors.

## Architecture Comparison

### External vs Automatic Coordination

| Aspect             | External Coordination        | Automatic Coordination   |
| ------------------ | ---------------------------- | ------------------------ |
| **Performance**    | Maximum (baseline)           | ~10-20% slower           |
| **API Complexity** | Higher (manual coordination) | Lower (built-in)         |
| **Use Case**       | Production systems           | Development/prototyping  |
| **Coordination**   | Manual `ProcessCoordination` | Built-in framework       |
| **Discovery**      | Manual implementation        | Automatic with framework |

### When to Use Each

**External Coordination (`counters.rs`):**

- Production systems requiring maximum performance
- Static topologies with known consumer counts
- Custom coordination requirements
- Benchmarking and performance analysis

**Automatic Coordination (`counters_auto.rs`):**

- Rapid development and prototyping
- Simplified integration requirements
- Applications where 10-20% performance trade-off is acceptable
- Testing and validation scenarios

```
(base) venkat:~/Documents/p/venkat-github/debug1/disruptor-rs % BUFFER_SIZE=1024 cargo run --release --example counters

MULTIPROCESS DISRUPTOR TEST SUMMARY
═══════════════════════════════════════════════════════════════════════════════
Test Configuration: 50,000 events, 128 bytes per event, 1,024 buffer size
+-------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| Test Scenario           | Buffer Size | Events | Payload | Producer Throughput | Consumer Throughput | Data Transfer Rate | Data Transfer Rate | Producer Avg | Consumer Avg | Producer P50 | Consumer P50 | Producer P99 | Producer P99 | Consumer P99 | Consumer P99 |
|                         |             |        | (bytes) | (ops/sec)           | (ops/sec)           | (MB/s)             | (GB/s)             | (ns)         | (ns)         | (ns)         | (ns)         | (ns)         | (μs)         | (ns)         | (μs)         |
+-------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| SPSC                    | 1,024       | 50,000 | 128     | 14,125,958          | 14,142,603          | 1726.39            | 1.686              | 71           | 71           | 57           | 74000        | 127          | 0.127        | 80000        | 80.000       |
+-------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| SPSC-Discovery          | 1,024       | 50,000 | 128     | 14,069,478          | 14,074,595          | 1718.09            | 1.678              | 71           | 71           | 57           | 75000        | 128          | 0.128        | 79000        | 79.000       |
+-------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| SPSC-Prefix-Discovery   | 1,024       | 50,000 | 128     | 14,141,439          | 14,080,869          | 1726.25            | 1.686              | 71           | 71           | 57           | 74000        | 127          | 0.127        | 85000        | 85.000       |
+-------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| SPMC-2                  | 1,024       | 50,000 | 128     | 19,871,827          | 20,739,307          | 7489.07            | 7.314              | 50           | 48           | 40           | 500          | 91           | 0.091        | 10000        | 10.000       |
+-------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| SPMC-2-Discovery        | 1,024       | 50,000 | 128     | 19,833,399          | 20,723,610          | 7480.54            | 7.305              | 50           | 48           | 40           | 1500         | 91           | 0.091        | 10000        | 10.000       |
+-------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| SPMC-2-Prefix-Discovery | 1,024       | 50,000 | 128     | 20,805,159          | 20,706,077          | 7594.89            | 7.417              | 48           | 48           | 38           | 3000         | 87           | 0.087        | 44500        | 44.500       |
+-------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| SPMC-5                  | 1,024       | 50,000 | 128     | 7,718,184           | 11,179,465          | 7765.57            | 7.584              | 130          | 89           | 104          | 200          | 233          | 0.233        | 112600       | 112.600      |
+-------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| SPMC-5-Discovery        | 1,024       | 50,000 | 128     | 7,574,993           | 13,787,931          | 9340.17            | 9.121              | 132          | 73           | 106          | 0            | 238          | 0.238        | 4400         | 4.400        |
+-------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| SPMC-5-Prefix-Discovery | 1,024       | 50,000 | 128     | 9,197,376           | 9,174,874           | 6722.63            | 6.565              | 109          | 109          | 87           | 0            | 196          | 0.196        | 113800       | 113.800      |
+-------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+

All automated tests completed successfully!

(base) venkat:~/Documents/p/venkat-github/debug1/disruptor-rs % BUFFER_SIZE=1024 cargo run --release --example counters_auto

AUTOMATIC COORDINATION TEST SUMMARY (Including Discovery and Prefix Discovery)
═══════════════════════════════════════════════════════════════════════════════
Test Configuration: 50,000 events, 128 bytes per event, 1,024 buffer size
+------------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| Test Scenario                | Buffer Size | Events | Payload | Producer Throughput | Consumer Throughput | Data Transfer Rate | Data Transfer Rate | Producer Avg | Consumer Avg | Producer P50 | Consumer P50 | Producer P99 | Producer P99 | Consumer P99 | Consumer P99 |
|                              |             |        | (bytes) | (ops/sec)           | (ops/sec)           | (MB/s)             | (GB/s)             | (ns)         | (ns)         | (ns)         | (ns)         | (ns)         | (μs)         | (ns)         | (μs)         |
+------------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| SPSC-Auto                    | 1,024       | 50,000 | 128     | 7,105,763           | 18,569,488          | 2266.78            | 2.214              | 141          | 54           | 113          | 133000       | 253          | 0.253        | 0            | 0.000        |
+------------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| SPSC-Discovery-Auto          | 1,024       | 50,000 | 128     | 13,187,682          | 51,044,211          | 6230.98            | 6.085              | 76           | 20           | 61           | 0            | 136          | 0.136        | 0            | 0.000        |
+------------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| SPSC-Prefix-Discovery-Auto   | 1,024       | 50,000 | 128     | 13,491,937          | 50,078,724          | 6113.13            | 5.970              | 74           | 20           | 59           | 0            | 133          | 0.133        | 0            | 0.000        |
+------------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| SPMC-2-Auto                  | 1,024       | 50,000 | 128     | 6,998,554           | 19,931,094          | 5720.31            | 5.586              | 143          | 50           | 114          | 126000       | 257          | 0.257        | 234000       | 234.000      |
+------------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| SPMC-2-Discovery-Auto        | 1,024       | 50,000 | 128     | 7,964,161           | 24,565,437          | 6969.61            | 6.806              | 126          | 41           | 100          | 0            | 226          | 0.226        | 0            | 0.000        |
+------------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| SPMC-2-Prefix-Discovery-Auto | 1,024       | 50,000 | 128     | 8,293,765           | 25,137,136          | 7149.42            | 6.982              | 121          | 40           | 96           | 0            | 217          | 0.217        | 0            | 0.000        |
+------------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| SPMC-5-Auto                  | 1,024       | 50,000 | 128     | 6,832,118           | 20,295,428          | 13221.34           | 12.911             | 146          | 49           | 117          | 2000         | 263          | 0.263        | 139000       | 139.000      |
+------------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| SPMC-5-Discovery-Auto        | 1,024       | 50,000 | 128     | 6,836,400           | 24,647,164          | 15877.96           | 15.506             | 146          | 41           | 117          | 0            | 263          | 0.263        | 0            | 0.000        |
+------------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
| SPMC-5-Prefix-Discovery-Auto | 1,024       | 50,000 | 128     | 6,058,892           | 24,506,428          | 15697.15           | 15.329             | 165          | 41           | 132          | 0            | 297          | 0.297        | 0            | 0.000        |
+------------------------------+-------------+--------+---------+---------------------+---------------------+--------------------+--------------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+--------------+
Error: "SPMC-5 prefix discovery test failed"

(base) venkat:~/Documents/p/venkat-github/debug1/disruptor-rs % BUFFER_SIZE=1024 cargo run --release --example counters_auto spmc_5_prefix_discovery_test
    Finished `release` profile [optimized] target(s) in 0.04s
     Running `target/release/examples/counters_auto spmc_5_prefix_discovery_test`
 Running automated SPMC prefix discovery test with automatic coordination (5 consumers)...
 SPMC-5 Prefix Discovery Test configuration:
  - Segment name: mpa63733
  - Buffer size: 1024 bytes (1 KB)
  - Events: 50000
  - Consumers: 5 (with automatic prefix discovery)
  - Prefix: SPMC_CONSUMER
  - Timeout: 90 seconds
  - Context: Test Suite (enhanced coordination)
 Starting SPMC prefix discovery producer process for 5 consumers...
 Starting 5 SPMC prefix discovery consumer processes...
 Waiting for all 5 consumers to be discovered by producer (test suite - extended timeout context)...

 === SPMC-5 Prefix Discovery Producer Output ===
 Starting SPMC prefix discovery producer with automatic coordination for 5 consumers...
 Prefix Discovery Producer creating shared memory segment: mpa63733
 Buffer size: 1024 bytes (1 KB)
Framework coordinating startup: waiting for 5 consumers (timeout: 30s)...
Framework coordination completed - 5 consumers ready
 Prefix Discovery Producer created - automatic consumer prefix discovery enabled for 5 consumers
 Producer will automatically discover and wait for 5 consumers with prefix 'SPMC_CONSUMER'...
 Producing 50000 events to 5 discovered consumers with prefix discovery...
 Produced 10000 events (20.0%)
 Produced 20000 events (40.0%)
 Produced 30000 events (60.0%)
 Produced 40000 events (80.0%)
 SPMC Prefix Discovery Producer finished!
  Time: 7ms (141.5ns per event, 0.141μs per event)
 Throughput: 7068470 events/sec
 Data Rate: 862.85 MB/s
 SPMC Prefix Discovery Producer completed - automatic coordination handled prefix discovery and shutdown
 Waiting for consumers to finish processing in SPMC mode...


 === SPMC Prefix Discovery Consumer 1 Output ===
 Starting SPMC prefix discovery consumer 1 with automatic event handlers...
 Prefix Discovery Consumer 1 attaching to shared memory segment: mpa63733
 Creating automatic prefix discovery consumer 1 with SPMC_CONSUMER prefix...
 Consumer 1 using prefixed ID: SPMC_CONSUMER_1 for discovery
 Automatic prefix discovery consumer 1 created - producer will discover via prefix 'SPMC_CONSUMER' and coordinate automatically
 Consumer 1 will automatically receive events when producer discovers it via prefix...
 Prefix Discovery Consumer 1 processed all 50000 events!
 Prefix Discovery Consumer 1 finished!
 Events consumed: 50000
 Final counter: 50000
 Expected counter: 50000
  Total time: 199.514ms, Processing time: 2.030ms (40.6ns per event, 0.041μs per event)
 Throughput: 24626745 events/sec
 Data Rate: 3006.19 MB/s
 Consumer P50: 0.0μs, P99: 0.1μs
 Prefix Discovery Consumer 1 automatically coordinated shutdown - producer discovered via prefix and coordinated seamlessly
 AUTOMATIC PREFIX DISCOVERY TEST PASSED for consumer 1 - All events counted correctly!


 === SPMC Prefix Discovery Consumer 2 Output ===
 Starting SPMC prefix discovery consumer 2 with automatic event handlers...
 Prefix Discovery Consumer 2 attaching to shared memory segment: mpa63733
 Creating automatic prefix discovery consumer 2 with SPMC_CONSUMER prefix...
 Consumer 2 using prefixed ID: SPMC_CONSUMER_2 for discovery
 Automatic prefix discovery consumer 2 created - producer will discover via prefix 'SPMC_CONSUMER' and coordinate automatically
 Consumer 2 will automatically receive events when producer discovers it via prefix...
 Prefix Discovery Consumer 2 processed all 50000 events!
 Prefix Discovery Consumer 2 finished!
 Events consumed: 50000
 Final counter: 50000
 Expected counter: 50000
  Total time: 149.019ms, Processing time: 2.025ms (40.5ns per event, 0.040μs per event)
 Throughput: 24696968 events/sec
 Data Rate: 3014.77 MB/s
 Consumer P50: 0.0μs, P99: 0.1μs
 Prefix Discovery Consumer 2 automatically coordinated shutdown - producer discovered via prefix and coordinated seamlessly
 AUTOMATIC PREFIX DISCOVERY TEST PASSED for consumer 2 - All events counted correctly!


 === SPMC Prefix Discovery Consumer 3 Output ===
 Starting SPMC prefix discovery consumer 3 with automatic event handlers...
 Prefix Discovery Consumer 3 attaching to shared memory segment: mpa63733
 Creating automatic prefix discovery consumer 3 with SPMC_CONSUMER prefix...
 Consumer 3 using prefixed ID: SPMC_CONSUMER_3 for discovery
 Automatic prefix discovery consumer 3 created - producer will discover via prefix 'SPMC_CONSUMER' and coordinate automatically
 Consumer 3 will automatically receive events when producer discovers it via prefix...
 Prefix Discovery Consumer 3 processed all 50000 events!
 Prefix Discovery Consumer 3 finished!
 Events consumed: 50000
 Final counter: 50000
 Expected counter: 50000
  Total time: 99.488ms, Processing time: 2.038ms (40.8ns per event, 0.041μs per event)
 Throughput: 24528609 events/sec
 Data Rate: 2994.21 MB/s
 Consumer P50: 0.0μs, P99: 0.1μs
 Prefix Discovery Consumer 3 automatically coordinated shutdown - producer discovered via prefix and coordinated seamlessly
 AUTOMATIC PREFIX DISCOVERY TEST PASSED for consumer 3 - All events counted correctly!


 === SPMC Prefix Discovery Consumer 4 Output ===
 Starting SPMC prefix discovery consumer 4 with automatic event handlers...
 Prefix Discovery Consumer 4 attaching to shared memory segment: mpa63733
 Creating automatic prefix discovery consumer 4 with SPMC_CONSUMER prefix...
 Consumer 4 using prefixed ID: SPMC_CONSUMER_4 for discovery
 Automatic prefix discovery consumer 4 created - producer will discover via prefix 'SPMC_CONSUMER' and coordinate automatically
 Consumer 4 will automatically receive events when producer discovers it via prefix...
 Prefix Discovery Consumer 4 processed all 50000 events!
 Prefix Discovery Consumer 4 finished!
 Events consumed: 50000
 Final counter: 50000
 Expected counter: 50000
  Total time: 49.697ms, Processing time: 1.894ms (37.9ns per event, 0.038μs per event)
 Throughput: 26394278 events/sec
 Data Rate: 3221.96 MB/s
 Consumer P50: 0.0μs, P99: 0.1μs
 Prefix Discovery Consumer 4 automatically coordinated shutdown - producer discovered via prefix and coordinated seamlessly
 AUTOMATIC PREFIX DISCOVERY TEST PASSED for consumer 4 - All events counted correctly!


 === SPMC Prefix Discovery Consumer 5 Output ===
 Starting SPMC prefix discovery consumer 5 with automatic event handlers...
 Prefix Discovery Consumer 5 attaching to shared memory segment: mpa63733
 Creating automatic prefix discovery consumer 5 with SPMC_CONSUMER prefix...
 Consumer 5 using prefixed ID: SPMC_CONSUMER_5 for discovery
 Automatic prefix discovery consumer 5 created - producer will discover via prefix 'SPMC_CONSUMER' and coordinate automatically
 Consumer 5 will automatically receive events when producer discovers it via prefix...
 Prefix Discovery Consumer 5 processed all 50000 events!
 Prefix Discovery Consumer 5 finished!
 Events consumed: 50000
 Final counter: 50000
 Expected counter: 50000
  Total time: 205.005ms, Processing time: 2.038ms (40.8ns per event, 0.041μs per event)
 Throughput: 24531461 events/sec
 Data Rate: 2994.56 MB/s
 Consumer P50: 0.0μs, P99: 0.1μs
 Prefix Discovery Consumer 5 automatically coordinated shutdown - producer discovered via prefix and coordinated seamlessly
 AUTOMATIC PREFIX DISCOVERY TEST PASSED for consumer 5 - All events counted correctly!

 Automated SPMC-5 prefix discovery test with automatic coordination PASSED!
All 5 consumers saw all events (broadcast semantics working correctly)
(base) venkat:~/Documents/p/venkat-github/debug1/disruptor-rs % BUFFER_SIZE=1024 cargo run --release --example counters_auto spmc_5_prefix_discovery_test
```
