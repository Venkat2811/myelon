//! Monster Sweep Benchmark — SHM backend.
//!
//! Sweeps payload size from signal (16B touch) through 1MB (full fill) across
//! the raw disruptor ring with 1, 3, and 12 consumers.
//!
//! Three classes:
//!   signal: 64B slot, 16B data, no payload fill — disruptor signaling ceiling (~200M ops/s)
//!   data:   64B-1MB slots, full payload fill + consumer checksum — real bandwidth
//!   multi:  data class with 3 or 12 consumers
//!
//! Run:   cargo bench -p myelon-bench --bench monster_sweep_shm
//! Quick: cargo bench -p myelon-bench --bench monster_sweep_shm -- --quick
//! Size:  cargo bench -p myelon-bench --bench monster_sweep_shm -- --size 64K

use disruptor_mp::{
    build_shared_single_producer, CoordinationMode, SharedDisruptorBuilder, SharedMemoryConfig,
};
use perf_bench::coordination::BenchmarkCoordination;
use perf_bench::events::{format_throughput, nanos_now, BenchEvent};
use perf_bench::harness::{self, IpcBenchmark, ScenarioChildren};
use perf_bench::latency::LatencyRecorder;
use perf_bench::reporting::{self, BenchReport, BenchResult};
use std::hint::black_box;
use std::time::{Duration, Instant};

// ============================================================
// Event types
// ============================================================

/// Signal event: 64B cache-line-aligned, only 16B of data written.
/// This is the disruptor signaling ceiling — same as raw_ring_shm signal class.
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct SignalEvent {
    sequence: u64,
    data: u64,
}
impl Default for SignalEvent {
    fn default() -> Self {
        Self {
            sequence: 0,
            data: 0,
        }
    }
}

type Ev64 = BenchEvent<48>;
type Ev512 = BenchEvent<496>;
type Ev1K = BenchEvent<1008>;
type Ev4K = BenchEvent<4080>;
type Ev16K = BenchEvent<{ 16 * 1024 - 16 }>;
type Ev64K = BenchEvent<{ 64 * 1024 - 16 }>;
type Ev256K = BenchEvent<{ 256 * 1024 - 16 }>;
type Ev1M = BenchEvent<{ 1024 * 1024 - 16 }>;

fn checksum_bytes(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .fold(0u64, |sum, &value| sum.wrapping_add(value as u64))
}

fn spawn_sweep_child(
    exe: &std::path::Path,
    role: &str,
    segment: &str,
    buffer: usize,
    events: u64,
    consumers: usize,
    target_rate: u64,
) -> std::process::Child {
    let mut envs = vec![
        ("SWEEP_SEGMENT", segment.to_string()),
        ("SWEEP_BUFFER", buffer.to_string()),
        ("SWEEP_EVENTS", events.to_string()),
        ("SWEEP_CONSUMERS", consumers.to_string()),
    ];
    if target_rate > 0 {
        envs.push(("SWEEP_TARGET_RATE", target_rate.to_string()));
    }
    harness::spawn_child(exe, role, &envs)
}

// ============================================================
// Signal class — disruptor signaling ceiling (16B write, no payload)
// ============================================================

fn signal_producer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = harness::segment_from_env("SWEEP_SEGMENT");
    let buffer = harness::read_env_usize("SWEEP_BUFFER", 65536);
    let events = harness::read_env_u64("SWEEP_EVENTS", 10_000_000);
    let warmup = 100_000u64;

    let mut producer = build_shared_single_producer::<SignalEvent>(&segment, buffer)
        .enable_discovery(1)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(|| SignalEvent::default())?;

    let coord = BenchmarkCoordination::create(&segment)?;
    if !coord.wait_for_consumers(1, Duration::from_secs(30)) {
        return Err("timeout waiting for consumer".into());
    }
    for _ in 0..20 {
        let _ = producer.min_gating_sequence();
        std::thread::sleep(Duration::from_millis(2));
    }

    for i in 0..warmup {
        producer.publish(|s| {
            s.sequence = i;
            s.data = i.wrapping_mul(0x9E3779B97F4A7C15);
        });
    }

    let start = Instant::now();
    for i in 0..events {
        producer.publish(|s| {
            s.sequence = warmup + i;
            s.data = (warmup + i).wrapping_mul(0x9E3779B97F4A7C15);
        });
    }
    let elapsed = start.elapsed();
    let output =
        harness::ProducerOutput::from_elapsed(events, elapsed, std::mem::size_of::<SignalEvent>());
    println!("{}", serde_json::to_string(&output)?);
    coord.signal_producer_done(events as i64);
    coord.wait_for_consumers_done(1, Duration::from_secs(60));
    Ok(())
}

fn signal_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = harness::segment_from_env("SWEEP_SEGMENT");
    let consumer_id = harness::read_env_usize("BENCH_CONSUMER_ID", 0);
    let buffer = harness::read_env_usize("SWEEP_BUFFER", 65536);
    let events = harness::read_env_u64("SWEEP_EVENTS", 10_000_000);
    let warmup = 100_000u64;

    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
    let config = SharedMemoryConfig {
        name: segment,
        buffer_size: buffer,
        element_size: std::mem::size_of::<SignalEvent>(),
        create: false,
    };
    let mut consumer = SharedDisruptorBuilder::<SignalEvent>::new(config).build_consumer()?;
    coord.signal_consumer_ready();

    let mut wc = 0u64;
    while wc < warmup {
        consumer.process_available(|_e, _s| {
            wc += 1;
        });
        if wc < warmup {
            std::hint::spin_loop();
        }
    }

    let start = Instant::now();
    let mut consumed = 0u64;
    let mut checksum = 0u64;
    while consumed < events {
        consumer.process_available(|event, _s| {
            checksum = checksum.wrapping_add(event.data);
            consumed += 1;
        });
        if consumed < events {
            std::hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();
    let output = harness::ConsumerOutput::from_elapsed(
        consumer_id,
        consumed,
        elapsed,
        std::mem::size_of::<SignalEvent>(),
        checksum,
    );
    println!("{}", serde_json::to_string(&output)?);
    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

// ============================================================
// Data class macro — full payload fill + consumer checksum, N consumers
// ============================================================

macro_rules! sweep_impl {
    ($ev_type:ty, $prod_fn:ident, $cons_fn:ident) => {
        fn $prod_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = harness::segment_from_env("SWEEP_SEGMENT");
            let buffer = harness::read_env_usize("SWEEP_BUFFER", 4096);
            let events = harness::read_env_u64("SWEEP_EVENTS", 100_000);
            let num_consumers = harness::read_env_usize("SWEEP_CONSUMERS", 1);
            let target_rate = harness::read_env_u64("SWEEP_TARGET_RATE", 0); // 0 = throughput mode
            let warmup = events / 100;

            let mut producer = build_shared_single_producer::<$ev_type>(&segment, buffer)
                .enable_discovery(num_consumers)
                .with_coordination(CoordinationMode::Immediate)
                .build_producer(|| <$ev_type>::default())?;

            let coord = BenchmarkCoordination::create(&segment)?;
            if !coord.wait_for_consumers(num_consumers, Duration::from_secs(60)) {
                return Err(format!("timeout waiting for {num_consumers} consumers").into());
            }

            let scan_rounds = 20 + num_consumers * 5;
            for _ in 0..scan_rounds {
                let _ = producer.min_gating_sequence();
                std::thread::sleep(Duration::from_millis(2));
            }

            for i in 0..warmup {
                producer.publish(|slot| {
                    slot.sequence = i;
                    slot.timestamp_ns = 0;
                });
            }

            if target_rate > 0 {
                // CO-aware mode: pace at target_rate, record intended_send_time
                let interval_ns = 1_000_000_000u64 / target_rate;
                let start = Instant::now();
                let base_ns = nanos_now();
                for i in 0..events {
                    let intended_ns = base_ns + i * interval_ns;
                    // Busy-wait until intended time
                    while nanos_now() < intended_ns {
                        std::hint::spin_loop();
                    }
                    producer.publish(|slot| {
                        slot.sequence = warmup + i;
                        // Store INTENDED send time, not actual — this is the CO key insight.
                        // If the system stalls and we publish late, the consumer still
                        // measures from when we SHOULD have sent, capturing the stall.
                        slot.timestamp_ns = intended_ns;
                        slot.payload.fill(((warmup + i) & 0xFF) as u8);
                    });
                }
                let elapsed = start.elapsed();
                let output = harness::ProducerOutput::from_elapsed(
                    events,
                    elapsed,
                    std::mem::size_of::<$ev_type>(),
                );
                println!("{}", serde_json::to_string(&output)?);
            } else {
                // Throughput mode: publish as fast as possible
                let start = Instant::now();
                for i in 0..events {
                    producer.publish(|slot| {
                        slot.sequence = warmup + i;
                        slot.timestamp_ns = nanos_now();
                        slot.payload.fill(((warmup + i) & 0xFF) as u8);
                    });
                }
                let elapsed = start.elapsed();
                let output = harness::ProducerOutput::from_elapsed(
                    events,
                    elapsed,
                    std::mem::size_of::<$ev_type>(),
                );
                println!("{}", serde_json::to_string(&output)?);
            }

            coord.signal_producer_done(events as i64);
            coord.wait_for_consumers_done(num_consumers, Duration::from_secs(120));
            Ok(())
        }

        fn $cons_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = harness::segment_from_env("SWEEP_SEGMENT");
            let consumer_id = harness::read_env_usize("BENCH_CONSUMER_ID", 0);
            let buffer = harness::read_env_usize("SWEEP_BUFFER", 4096);
            let events = harness::read_env_u64("SWEEP_EVENTS", 100_000);
            let warmup = events / 100;

            let coord =
                BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
            let config = SharedMemoryConfig {
                name: segment,
                buffer_size: buffer,
                element_size: std::mem::size_of::<$ev_type>(),
                create: false,
            };
            let mut consumer = SharedDisruptorBuilder::<$ev_type>::new(config).build_consumer()?;
            coord.signal_consumer_ready();

            let mut wc = 0u64;
            while wc < warmup {
                consumer.process_available(|_e, _s| {
                    wc += 1;
                });
                if wc < warmup {
                    std::hint::spin_loop();
                }
            }

            let mut latency = LatencyRecorder::default_range();
            let start = Instant::now();
            let mut consumed = 0u64;
            let mut checksum = 0u64;

            while consumed < events {
                consumer.process_available(|slot, _seq| {
                    if slot.timestamp_ns > 0 {
                        latency.record_delta(slot.timestamp_ns, nanos_now());
                    }
                    let payload_sum = checksum_bytes(&slot.payload);
                    black_box(payload_sum);
                    checksum = checksum.wrapping_add(payload_sum);
                    consumed += 1;
                });
                if consumed < events {
                    std::hint::spin_loop();
                }
            }
            let elapsed = start.elapsed();
            let output = if let Some(stats) = latency.stats() {
                harness::ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    std::mem::size_of::<$ev_type>(),
                    checksum,
                )
                .with_latency(stats)
            } else {
                harness::ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    std::mem::size_of::<$ev_type>(),
                    checksum,
                )
            };
            println!("{}", serde_json::to_string(&output)?);
            coord.signal_consumer_done(consumed as i64);
            Ok(())
        }
    };
}

sweep_impl!(Ev64, prod_64b, cons_64b);
sweep_impl!(Ev512, prod_512b, cons_512b);
sweep_impl!(Ev1K, prod_1k, cons_1k);
sweep_impl!(Ev4K, prod_4k, cons_4k);
sweep_impl!(Ev16K, prod_16k, cons_16k);
sweep_impl!(Ev64K, prod_64k, cons_64k);
sweep_impl!(Ev256K, prod_256k, cons_256k);
sweep_impl!(Ev1M, prod_1m, cons_1m);

// ============================================================
// Orchestrator
// ============================================================

struct SweepPoint {
    label: &'static str,
    size_bytes: usize,
    events: u64,
    buffer: usize,
    consumers: usize,
    target_rate: u64, // 0 = throughput mode, >0 = CO-aware at this ops/sec
    prod_role: &'static str,
    cons_role: &'static str,
    tag: &'static str,
}

impl IpcBenchmark for SweepPoint {
    fn bench_name(&self) -> &str {
        "monster_sweep_shm"
    }

    fn scenario_name(&self) -> String {
        format!("sweep_{}_{}", self.tag, self.consumers)
    }

    fn backend(&self) -> &str {
        "shm"
    }

    fn layer(&self) -> &str {
        "raw_ring"
    }

    fn message_size_bytes(&self) -> usize {
        self.size_bytes
    }

    fn buffer_depth(&self) -> usize {
        self.buffer
    }

    fn num_messages(&self) -> u64 {
        self.events
    }

    fn warmup_messages(&self) -> u64 {
        self.events / 100
    }

    fn num_consumers(&self) -> usize {
        self.consumers
    }

    fn timeout(&self) -> Duration {
        Duration::from_secs(300)
    }

    fn producer_label(&self) -> String {
        format!("{} prod", self.label)
    }

    fn consumer_label(&self, consumer_id: usize) -> String {
        format!("{} cons{consumer_id}", self.label)
    }

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, harness::BenchError> {
        let segment = harness::unique_shm_segment(&format!("sw_{}", self.tag));
        let producer = spawn_sweep_child(
            exe,
            self.prod_role,
            &segment,
            self.buffer,
            self.events,
            self.consumers,
            self.target_rate,
        );
        let consumers = (0..self.consumers)
            .map(|consumer_id| {
                let mut envs = vec![
                    ("SWEEP_SEGMENT", segment.clone()),
                    ("SWEEP_BUFFER", self.buffer.to_string()),
                    ("SWEEP_EVENTS", self.events.to_string()),
                    ("SWEEP_CONSUMERS", self.consumers.to_string()),
                    ("BENCH_CONSUMER_ID", consumer_id.to_string()),
                ];
                if self.target_rate > 0 {
                    envs.push(("SWEEP_TARGET_RATE", self.target_rate.to_string()));
                }
                harness::spawn_child(exe, self.cons_role, &envs)
            })
            .collect();

        Ok(ScenarioChildren::new(producer, consumers))
    }

    fn aggregate_latency(
        &self,
        consumers: &[harness::ConsumerOutput],
    ) -> Option<perf_bench::latency::LatencyStats> {
        consumers
            .iter()
            .filter_map(|entry| entry.latency.clone())
            .max_by_key(|stats| stats.p99_ns)
    }

    fn print_summary_with_metrics(
        &self,
        producer: &harness::ProducerOutput,
        consumers: &[harness::ConsumerOutput],
        latency: Option<&perf_bench::latency::LatencyStats>,
    ) {
        let avg_cons_tp = self.average_consumer_ops(consumers);
        let ring_mb = (self.size_bytes as u64 * self.buffer as u64) / (1024 * 1024);
        let bw_gbs = avg_cons_tp * self.size_bytes as f64 / 1e9;
        let lat_str = latency
            .map(|stats| stats.summary())
            .unwrap_or_else(|| "-".to_string());
        let cons_label = if self.consumers == 1 { "cons" } else { "avg_c" };
        let mode_str = if self.target_rate > 0 {
            format!("CO@{}K", self.target_rate / 1000)
        } else {
            "tput".to_string()
        };

        println!(
            "  {:<12} {:>5} 1p{:<2}c  ring={:>5}MB  {:>8} events  prod: {:>10}  {}: {:>10}  {:>6.1} GB/s  {}",
            self.label,
            mode_str,
            self.consumers,
            ring_mb,
            self.events,
            format_throughput(producer.throughput_ops_sec),
            cons_label,
            format_throughput(avg_cons_tp),
            bw_gbs,
            lat_str,
        );
    }

    fn build_result_with_metrics(
        &self,
        producer: &harness::ProducerOutput,
        consumers: &[harness::ConsumerOutput],
        latency: Option<perf_bench::latency::LatencyStats>,
    ) -> BenchResult {
        let mut result = reporting::make_result(
            self.bench_name(),
            &self.scenario_name(),
            self.backend(),
            self.layer(),
            self.codec(),
            self.wait_strategy(),
            self.message_size_bytes(),
            self.buffer_depth(),
            self.num_messages(),
            self.warmup_messages(),
            self.num_consumers(),
            producer.throughput_ops_sec,
            self.average_consumer_ops(consumers),
            latency,
        );
        if self.target_rate > 0 {
            result.measurement_mode = format!("co_aware@{}", self.target_rate);
        }
        result
    }
}

// ============================================================
// World-class display
// ============================================================

fn print_sweep_report(report: &BenchReport) {
    reporting::print_monster_sweep_report(report, reporting::MonsterSweepBackend::Shm);
}

fn write_sweep_markdown(report: &BenchReport, path: &str) -> std::io::Result<()> {
    reporting::write_monster_sweep_markdown(
        report,
        path,
        reporting::MonsterSweepBackend::Shm,
    )
}

fn format_size(bytes: usize) -> String {
    if bytes >= 1_048_576 {
        format!("{}MB", bytes / 1_048_576)
    } else if bytes >= 1024 {
        format!("{}KB", bytes / 1024)
    } else {
        format!("{}B", bytes)
    }
}

// ============================================================
// main
// ============================================================

const CHILD_ROLES: &[harness::ChildRole] = &[
    harness::ChildRole::new("sig_prod", signal_producer),
    harness::ChildRole::new("sig_cons", signal_consumer),
    harness::ChildRole::new("prod_64b", prod_64b),
    harness::ChildRole::new("cons_64b", cons_64b),
    harness::ChildRole::new("prod_512b", prod_512b),
    harness::ChildRole::new("cons_512b", cons_512b),
    harness::ChildRole::new("prod_1k", prod_1k),
    harness::ChildRole::new("cons_1k", cons_1k),
    harness::ChildRole::new("prod_4k", prod_4k),
    harness::ChildRole::new("cons_4k", cons_4k),
    harness::ChildRole::new("prod_16k", prod_16k),
    harness::ChildRole::new("cons_16k", cons_16k),
    harness::ChildRole::new("prod_64k", prod_64k),
    harness::ChildRole::new("cons_64k", cons_64k),
    harness::ChildRole::new("prod_256k", prod_256k),
    harness::ChildRole::new("cons_256k", cons_256k),
    harness::ChildRole::new("prod_1m", prod_1m),
    harness::ChildRole::new("cons_1m", cons_1m),
];

struct MonsterSweepShm;

impl harness::BenchHarness for MonsterSweepShm {
    fn bench_name(&self) -> &'static str {
        "monster_sweep_shm"
    }

    fn child_roles(&self) -> &'static [harness::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> harness::BenchRunResult {
        let mut bench_log = perf_bench::bench_log::BenchLog::default_capacity("monster_sweep_shm");
        bench_log.event("orchestrator_start");

        let size_arg = args
            .windows(2)
            .find(|w| w[0] == "--size")
            .map(|w| w[1].as_str())
            .unwrap_or("all");

        let output_args = reporting::ReportOutputArgs::from_args(args);
        let json_mode = output_args.json_mode;
        let quick_mode = output_args.quick_mode;

        let mode_arg = args
            .windows(2)
            .find(|w| w[0] == "--mode")
            .map(|w| w[1].as_str())
            .unwrap_or("all");

        let run_throughput = mode_arg == "all" || mode_arg == "throughput";
        let run_co = mode_arg == "all" || mode_arg == "co";

        fn roles(tag: &str) -> (&'static str, &'static str) {
            match tag {
                "64B" => ("prod_64b", "cons_64b"),
                "512B" => ("prod_512b", "cons_512b"),
                "1K" | "1K_3c" | "1K_12c" | "1K_CO" => ("prod_1k", "cons_1k"),
                "4K" | "4K_3c" | "4K_12c" | "4K_CO" => ("prod_4k", "cons_4k"),
                "16K" | "16K_CO" => ("prod_16k", "cons_16k"),
                "64K" | "64K_3c" | "64K_12c" | "64K_CO" => ("prod_64k", "cons_64k"),
                "256K" | "256K_CO" => ("prod_256k", "cons_256k"),
                "1M" | "1M_3c" | "1M_CO" => ("prod_1m", "cons_1m"),
                _ => ("prod_64b", "cons_64b"),
            }
        }

        let scenarios: Vec<SweepPoint> = {
            let mut v = Vec::new();

            if run_throughput {
                v.push(SweepPoint {
                    label: "signal",
                    size_bytes: 64,
                    events: 10_000_000,
                    buffer: 65_536,
                    consumers: 1,
                    target_rate: 0,
                    prod_role: "sig_prod",
                    cons_role: "sig_cons",
                    tag: "SIG",
                });

                for (label, size, events, buffer, tag) in [
                    ("64B", 64usize, 10_000_000u64, 65_536usize, "64B"),
                    ("512B", 512, 5_000_000, 131_072, "512B"),
                    ("1KB", 1_024, 2_000_000, 131_072, "1K"),
                    ("4KB", 4_096, 1_000_000, 65_536, "4K"),
                    ("16KB", 16_384, 500_000, 32_768, "16K"),
                    ("64KB", 65_536, 200_000, 16_384, "64K"),
                    ("256KB", 262_144, 100_000, 8_192, "256K"),
                    ("1MB", 1_048_576, 50_000, 4_096, "1M"),
                ] {
                    let (p, c) = roles(tag);
                    v.push(SweepPoint {
                        label,
                        size_bytes: size,
                        events,
                        buffer,
                        consumers: 1,
                        target_rate: 0,
                        prod_role: p,
                        cons_role: c,
                        tag,
                    });
                }

                for nc in [2usize, 4, 6, 8, 10, 12] {
                    for (size, events, buffer, base_tag) in [
                        (1_024usize, 200_000u64, 131_072usize, "1K"),
                        (4_096, 100_000, 65_536, "4K"),
                    ] {
                        let tag_str = format!("{base_tag}_{nc}c");
                        let tag: &'static str = Box::leak(tag_str.into_boxed_str());
                        let label_str = format!("{}x{}c", format_size(size), nc);
                        let label: &'static str = Box::leak(label_str.into_boxed_str());
                        let (p, c) = roles(base_tag);
                        v.push(SweepPoint {
                            label,
                            size_bytes: size,
                            events,
                            buffer,
                            consumers: nc,
                            target_rate: 0,
                            prod_role: p,
                            cons_role: c,
                            tag,
                        });
                    }
                }
            }

            if run_co {
                for (label, size, buffer, rate, tag) in [
                    ("1KB@100K", 1_024, 131_072, 100_000u64, "1K_CO"),
                    ("1KB@500K", 1_024, 131_072, 500_000, "1K_CO"),
                    ("1KB@1M", 1_024, 131_072, 1_000_000, "1K_CO"),
                    ("4KB@100K", 4_096, 65_536, 100_000, "4K_CO"),
                    ("4KB@500K", 4_096, 65_536, 500_000, "4K_CO"),
                    ("16KB@100K", 16_384, 32_768, 100_000, "16K_CO"),
                    ("64KB@50K", 65_536, 16_384, 50_000, "64K_CO"),
                    ("64KB@100K", 65_536, 16_384, 100_000, "64K_CO"),
                    ("256KB@10K", 262_144, 8_192, 10_000, "256K_CO"),
                    ("256KB@50K", 262_144, 8_192, 50_000, "256K_CO"),
                    ("1MB@10K", 1_048_576, 4_096, 10_000, "1M_CO"),
                    ("1MB@30K", 1_048_576, 4_096, 30_000, "1M_CO"),
                ] {
                    let (p, c) = roles(tag);
                    v.push(SweepPoint {
                        label,
                        size_bytes: size,
                        events: 100_000,
                        buffer,
                        consumers: 1,
                        target_rate: rate,
                        prod_role: p,
                        cons_role: c,
                        tag,
                    });
                }
            }

            v
        };

        if !json_mode {
            println!("=== Monster Sweep SHM Benchmark ===");
            println!("Backend: POSIX shared memory (M3 Max 14-core, 96GB, 300 GB/s BW)");
            println!("Modes: throughput (max rate) + CO-aware (fixed rate, intended_send_time)");
            println!("Signal: 64B slot, 16B data — disruptor ceiling");
            println!("Data:   full payload fill + consumer checksum — real bandwidth");
            println!("CO:     P50-P99.999 at calibrated rates (100K-1.2M small, 10K-100K large)");
            println!();
        }

        let mut report = BenchReport::new();

        for sp in &scenarios {
            let run = match size_arg {
                "all" => {
                    if quick_mode {
                        let is_quick_tput = matches!(sp.tag, "SIG" | "64B" | "1K" | "64K" | "1M");
                        let is_quick_multi =
                            sp.tag.starts_with("1K_") && matches!(sp.consumers, 2 | 6 | 12);
                        let is_quick_co = (sp.tag == "1K_CO" && sp.target_rate == 500_000)
                            || (sp.tag == "64K_CO" && sp.target_rate == 50_000);
                        (is_quick_tput && sp.consumers == 1) || is_quick_multi || is_quick_co
                    } else {
                        true
                    }
                }
                tag => sp.tag == tag,
            };
            if run {
                bench_log.event(&format!("scenario_start: {}", sp.label));
                report.add(sp.run_benchmark()?);
                bench_log.event(&format!("scenario_done: {}", sp.label));
            }
        }
        bench_log.event_val("scenarios_completed", report.results.len() as u64);

        if !output_args.json_mode && !output_args.quick_mode {
            print_sweep_report(&report);
        }
        reporting::emit_report(
            &report,
            &output_args,
            None,
            Some(reporting::ReportView::Tree),
            Some(write_sweep_markdown),
        );
        Ok(())
    }
}

perf_bench::myelon_bench_main!(MonsterSweepShm);
