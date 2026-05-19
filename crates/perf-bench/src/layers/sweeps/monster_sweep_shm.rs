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
//! Run:   cargo bench -p myelon-bench --bench `monster_sweep_shm`
//! Quick: cargo bench -p myelon-bench --bench `monster_sweep_shm` -- --quick
//! Size:  cargo bench -p myelon-bench --bench `monster_sweep_shm` -- --size 64K

use crate::cli::sweeps::{self as sweep_specs, SweepBackend};
use crate::infra::coordination::BenchmarkCoordination;
use crate::infra::events::{format_throughput, nanos_now, BenchEvent};
use crate::infra::latency::LatencyRecorder;
use crate::infra::output::report::{MonsterSweepBackend, ReportBundleCompat};
use crate::infra::output::reporting::{self, BenchReport};
use crate::infra::{self, IpcBenchmark, ScenarioChildren};
use disruptor_mp::{
    attach_shared_consumer, build_shared_single_producer, AutoWaitStrategy, CoordinationMode,
    SharedConsumer, SharedDisruptorBuilder, SharedMemoryConfig,
};
use std::hint::black_box;
use std::time::{Duration, Instant};

// ============================================================
// Event types
// ============================================================

/// Signal event: 64B cache-line-aligned, only 16B of data written.
/// This is the disruptor signaling ceiling — same as `raw_ring_shm` signal class.
#[repr(C, align(64))]
#[derive(Clone, Copy, Default)]
struct SignalEvent {
    sequence: u64,
    data: u64,
}

type Ev64 = BenchEvent<48>;
type Ev512 = BenchEvent<496>;
type Ev1K = BenchEvent<1008>;
type Ev4K = BenchEvent<4080>;
type Ev16K = BenchEvent<{ 16 * 1024 - 16 }>;
type Ev64K = BenchEvent<{ 64 * 1024 - 16 }>;
type Ev256K = BenchEvent<{ 256 * 1024 - 16 }>;
type Ev1M = BenchEvent<{ 1024 * 1024 - 16 }>;

const DISCOVERY_SCAN_SLEEP: Duration = Duration::from_millis(150);
const MULTI_CONSUMER_PREFIX: &str = "msc";

fn discovery_scan_rounds(num_consumers: usize) -> usize {
    if num_consumers > 1 {
        8 + num_consumers
    } else {
        8
    }
}

fn warm_discovery_scans<F>(mut scan: F, rounds: usize)
where
    F: FnMut() -> i64,
{
    for _ in 0..rounds {
        let _ = scan();
        std::thread::sleep(DISCOVERY_SCAN_SLEEP);
    }
}

fn multi_consumer_id(consumer_id: usize) -> String {
    format!("{MULTI_CONSUMER_PREFIX}_{consumer_id}")
}

fn attach_consumer_with_timeout<E: Copy + Default + 'static>(
    segment: &str,
    buffer: usize,
    consumer_name: &str,
    timeout: Duration,
) -> Result<SharedConsumer<E>, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        match attach_shared_consumer::<E>(segment, buffer)
            .with_consumer_id(consumer_name)
            .build_consumer()
        {
            Ok(consumer) => return Ok(consumer),
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(error) => {
                return Err(format!("attach failed for {consumer_name}: {error}").into());
            }
        }
    }
}

fn checksum_bytes(bytes: &[u8]) -> u64 {
    // u8 accumulator for SIMD-friendly vectorization, matching original monster_sweep.
    bytes.iter().fold(0u8, |a, &b| a.wrapping_add(b)) as u64
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
    infra::spawn_child(exe, role, &envs)
}

// ============================================================
// Signal class — disruptor signaling ceiling (16B write, no payload)
// ============================================================

fn signal_producer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = infra::segment_from_env("SWEEP_SEGMENT");
    let buffer = infra::read_env_usize("SWEEP_BUFFER", 65536);
    let events = infra::read_env_u64("SWEEP_EVENTS", 10_000_000);
    let warmup = 100_000u64;

    let mut producer = build_shared_single_producer::<SignalEvent>(&segment, buffer)
        .enable_discovery(1)
        .with_coordination(CoordinationMode::Immediate)
        .build_producer(SignalEvent::default)?;

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
        infra::ProducerOutput::from_elapsed(events, elapsed, std::mem::size_of::<SignalEvent>());
    println!("{}", serde_json::to_string(&output)?);
    coord.signal_producer_done(events as i64);
    coord.wait_for_consumers_done(1, Duration::from_secs(60));
    Ok(())
}

fn signal_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let segment = infra::segment_from_env("SWEEP_SEGMENT");
    let consumer_id = infra::read_env_usize("BENCH_CONSUMER_ID", 0);
    let buffer = infra::read_env_usize("SWEEP_BUFFER", 65536);
    let events = infra::read_env_u64("SWEEP_EVENTS", 10_000_000);
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

    let deadline = infra::spin_deadline();
    let mut wc = 0u64;
    while wc < warmup {
        consumer.process_available(|_e, _s| {
            wc += 1;
        });
        if wc < warmup {
            infra::check_deadline(deadline, "monster_sweep_shm signal_consumer warmup");
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
            infra::check_deadline(deadline, "monster_sweep_shm signal_consumer measured");
            std::hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();
    let output = infra::ConsumerOutput::from_elapsed(
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
            let segment = infra::segment_from_env("SWEEP_SEGMENT");
            let buffer = infra::read_env_usize("SWEEP_BUFFER", 4096);
            let events = infra::read_env_u64("SWEEP_EVENTS", 100_000);
            let num_consumers = infra::read_env_usize("SWEEP_CONSUMERS", 1);
            let target_rate = infra::read_env_u64("SWEEP_TARGET_RATE", 0); // 0 = throughput mode
            let warmup = events / 100;

            let mut producer = if num_consumers > 1 {
                let coord = BenchmarkCoordination::create(&segment)?;
                let producer = build_shared_single_producer::<$ev_type>(&segment, buffer)
                    .discover_consumer_with_prefix_and_interval(
                        num_consumers,
                        MULTI_CONSUMER_PREFIX,
                        DISCOVERY_SCAN_SLEEP,
                    )
                    .wait_for_consumers(num_consumers as i64, Duration::from_secs(60))
                    .build_producer(|| <$ev_type>::default())?;
                if !coord.wait_for_consumers(num_consumers, Duration::from_secs(60)) {
                    return Err(format!("timeout waiting for {num_consumers} consumers").into());
                }
                producer
            } else {
                let producer = build_shared_single_producer::<$ev_type>(&segment, buffer)
                    .enable_discovery(num_consumers)
                    .with_coordination(CoordinationMode::Immediate)
                    .build_producer(|| <$ev_type>::default())?;
                let coord = BenchmarkCoordination::create(&segment)?;
                if !coord.wait_for_consumers(num_consumers, Duration::from_secs(60)) {
                    return Err(format!("timeout waiting for {num_consumers} consumers").into());
                }
                producer
            };

            warm_discovery_scans(
                || producer.min_gating_sequence(),
                discovery_scan_rounds(num_consumers),
            );

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
                let output = infra::ProducerOutput::from_elapsed(
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
                let output = infra::ProducerOutput::from_elapsed(
                    events,
                    elapsed,
                    std::mem::size_of::<$ev_type>(),
                );
                println!("{}", serde_json::to_string(&output)?);
            }

            let last_seq = (warmup + events - 1) as i64;
            producer.wait_until_consumed_with_strategy(
                last_seq,
                Duration::from_secs(180),
                AutoWaitStrategy::BusySpin,
            );
            Ok(())
        }

        fn $cons_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = infra::segment_from_env("SWEEP_SEGMENT");
            let consumer_id = infra::read_env_usize("BENCH_CONSUMER_ID", 0);
            let buffer = infra::read_env_usize("SWEEP_BUFFER", 4096);
            let events = infra::read_env_u64("SWEEP_EVENTS", 100_000);
            let num_consumers = infra::read_env_usize("SWEEP_CONSUMERS", 1);
            let warmup = events / 100;

            let mut consumer = if num_consumers > 1 {
                let coord =
                    BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
                let consumer_name = multi_consumer_id(consumer_id);
                let consumer = attach_consumer_with_timeout::<$ev_type>(
                    &segment,
                    buffer,
                    &consumer_name,
                    Duration::from_secs(15),
                )?;
                coord.signal_consumer_ready();
                consumer
            } else {
                let coord =
                    BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
                let config = SharedMemoryConfig {
                    name: segment,
                    buffer_size: buffer,
                    element_size: std::mem::size_of::<$ev_type>(),
                    create: false,
                };
                let consumer = SharedDisruptorBuilder::<$ev_type>::new(config).build_consumer()?;
                coord.signal_consumer_ready();
                consumer
            };

            let deadline = infra::spin_deadline();
            let mut wc = 0u64;
            let mut consumed = 0u64;
            let mut latency = LatencyRecorder::default_range();
            let mut checksum = 0u64;
            let mut start = None;
            while wc < warmup {
                let before_progress = wc + consumed;
                consumer.process_available(|slot, _s| {
                    if slot.sequence < warmup {
                        wc += 1;
                    } else {
                        if start.is_none() {
                            start = Some(Instant::now());
                        }
                        if slot.timestamp_ns > 0 {
                            latency.record_delta(slot.timestamp_ns, nanos_now());
                        }
                        let payload_sum = checksum_bytes(&slot.payload);
                        black_box(payload_sum);
                        checksum = checksum.wrapping_add(payload_sum);
                        consumed += 1;
                    }
                });
                if consumed > 0 || wc >= warmup {
                    break;
                }
                if wc + consumed == before_progress {
                    infra::check_deadline(deadline, concat!(stringify!($cons_fn), " warmup"));
                    std::hint::spin_loop();
                }
            }

            let start = start.unwrap_or_else(Instant::now);

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
                    infra::check_deadline(deadline, concat!(stringify!($cons_fn), " measured"));
                    std::hint::spin_loop();
                }
            }
            let elapsed = start.elapsed();
            let output = if let Some(stats) = latency.stats() {
                infra::ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    std::mem::size_of::<$ev_type>(),
                    checksum,
                )
                .with_latency(stats)
            } else {
                infra::ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    std::mem::size_of::<$ev_type>(),
                    checksum,
                )
            };
            println!("{}", serde_json::to_string(&output)?);
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
    label: String,
    size_bytes: usize,
    events: u64,
    buffer: usize,
    consumers: usize,
    target_rate: u64, // 0 = throughput mode, >0 = CO-aware at this ops/sec
    prod_role: &'static str,
    cons_role: &'static str,
    tag: String,
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

    fn transport_metadata(&self) -> reporting::BenchTransportSpec {
        reporting::BenchTransportSpec::benchmark_shm(self.consumers)
            .with_zero_copy(false)
            .with_framing("none")
    }

    fn measurement_mode(&self) -> String {
        if self.target_rate > 0 {
            format!("co_aware@{}", self.target_rate)
        } else {
            "max_throughput".to_string()
        }
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

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, infra::BenchError> {
        let segment = infra::unique_shm_segment(&format!("sw_{}", self.tag));
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
                infra::spawn_child(exe, self.cons_role, &envs)
            })
            .collect();

        Ok(ScenarioChildren::new(producer, consumers))
    }

    fn aggregate_latency(
        &self,
        consumers: &[infra::ConsumerOutput],
    ) -> Option<crate::infra::latency::LatencyStats> {
        consumers
            .iter()
            .filter_map(|entry| entry.latency.clone())
            .max_by_key(|stats| stats.p99_ns)
    }

    fn print_summary_with_metrics(
        &self,
        producer: &infra::ProducerOutput,
        consumers: &[infra::ConsumerOutput],
        latency: Option<&crate::infra::latency::LatencyStats>,
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
}

// ============================================================
// World-class display
// ============================================================

fn print_sweep_report(report: &BenchReport) {
    report
        .to_report()
        .print_monster_sweep_report(MonsterSweepBackend::Shm);
}

fn write_sweep_markdown(report: &BenchReport, path: &str) -> std::io::Result<()> {
    report
        .to_report()
        .write_monster_sweep_markdown(path, MonsterSweepBackend::Shm)
}

// ============================================================
// main
// ============================================================

const CHILD_ROLES: &[infra::ChildRole] = &[
    infra::ChildRole::new("sig_prod", signal_producer),
    infra::ChildRole::new("sig_cons", signal_consumer),
    infra::ChildRole::new("prod_64b", prod_64b),
    infra::ChildRole::new("cons_64b", cons_64b),
    infra::ChildRole::new("prod_512b", prod_512b),
    infra::ChildRole::new("cons_512b", cons_512b),
    infra::ChildRole::new("prod_1k", prod_1k),
    infra::ChildRole::new("cons_1k", cons_1k),
    infra::ChildRole::new("prod_4k", prod_4k),
    infra::ChildRole::new("cons_4k", cons_4k),
    infra::ChildRole::new("prod_16k", prod_16k),
    infra::ChildRole::new("cons_16k", cons_16k),
    infra::ChildRole::new("prod_64k", prod_64k),
    infra::ChildRole::new("cons_64k", cons_64k),
    infra::ChildRole::new("prod_256k", prod_256k),
    infra::ChildRole::new("cons_256k", cons_256k),
    infra::ChildRole::new("prod_1m", prod_1m),
    infra::ChildRole::new("cons_1m", cons_1m),
];

pub struct MonsterSweepShm;

impl infra::BenchHarness for MonsterSweepShm {
    fn bench_name(&self) -> &'static str {
        "monster_sweep_shm"
    }

    fn child_roles(&self) -> &'static [infra::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> infra::BenchRunResult {
        let mut bench_log =
            crate::infra::output::log::BenchLog::default_capacity("monster_sweep_shm");
        bench_log.event("orchestrator_start");

        let size_arg = args
            .windows(2)
            .find(|w| w[0] == "--size")
            .map(|w| w[1].as_str())
            .unwrap_or("all");

        let output_args = reporting::ReportOutputArgs::from_args(args);
        let json_mode = output_args.json_mode;
        let quick_mode = output_args.quick_mode;
        let num_messages_override = args
            .windows(2)
            .find(|w| w[0] == "--num-messages")
            .and_then(|w| w[1].parse::<u64>().ok());

        let mode_arg = args
            .windows(2)
            .find(|w| w[0] == "--mode")
            .map(|w| w[1].as_str())
            .unwrap_or("all");

        let run_throughput = mode_arg == "all" || mode_arg == "throughput";
        let run_co = mode_arg == "all" || mode_arg == "co";
        let scenario_specs =
            sweep_specs::monster_sweep_scenarios(SweepBackend::Shm, run_throughput, run_co);

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

        for spec in &scenario_specs {
            if sweep_specs::monster_sweep_should_run(size_arg, quick_mode, SweepBackend::Shm, spec)
            {
                let (prod_role, cons_role) = sweep_specs::monster_sweep_roles(spec.role_key);
                let sp = SweepPoint {
                    label: spec.label.clone(),
                    size_bytes: spec.size_bytes,
                    events: num_messages_override.unwrap_or(spec.events),
                    buffer: spec.buffer,
                    consumers: spec.consumers,
                    target_rate: spec.target_rate,
                    prod_role,
                    cons_role,
                    tag: spec.tag.clone(),
                };
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
