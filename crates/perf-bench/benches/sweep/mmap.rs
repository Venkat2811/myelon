//! Monster Sweep Benchmark — mmap backend.
//!
//! Same sweep as monster_sweep_shm over file-backed mmap.
//! Compare results for SHM vs mmap crossover analysis.
//!
//! Run: cargo bench -p myelon-bench --bench monster_sweep_mmap
//! Quick: cargo bench -p myelon-bench --bench monster_sweep_mmap -- --quick

use disruptor_mp::{AutoWaitStrategy, MmapConsumer, MmapProducer, MmapTransportLayout};
use perf_bench::events::{format_throughput, nanos_now, BenchEvent};
use perf_bench::harness::{self, IpcBenchmark, ScenarioChildren};
use perf_bench::latency::LatencyRecorder;
use perf_bench::reporting::{self, BenchReport, BenchResult};
use std::hint::black_box;
use std::time::{Duration, Instant};

// ============================================================
// Event types — same as SHM sweep
// ============================================================

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
    // u8 accumulator for SIMD-friendly vectorization, matching original monster_sweep.
    bytes.iter().fold(0u8, |a, &b| a.wrapping_add(b)) as u64
}

fn child_layout() -> MmapTransportLayout {
    harness::mmap_layout_from_env("SWEEP_ROOT", "SWEEP_SEGMENT")
}

fn spawn_sweep_child(
    exe: &std::path::Path,
    role: &str,
    root: &str,
    segment: &str,
    events: u64,
    buffer: usize,
    consumers: usize,
    target_rate: u64,
) -> std::process::Child {
    let mut envs = vec![
        ("SWEEP_ROOT", root.to_string()),
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
// Signal class
// ============================================================

fn signal_producer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let buffer = harness::read_env_usize("SWEEP_BUFFER", 65536);
    let events = harness::read_env_u64("SWEEP_EVENTS", 10_000_000);
    let consumers = harness::read_env_usize("SWEEP_CONSUMERS", 1);
    let warmup = 100_000u64;
    layout.ensure_directories()?;
    let mut producer =
        MmapProducer::<SignalEvent>::create(layout, buffer, || SignalEvent::default())?;
    if !producer.wait_for_consumers_ready(consumers as i64, Duration::from_secs(30)) {
        return Err("timeout".into());
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
    let last = (warmup + events - 1) as i64;
    producer.wait_until_consumed_with_strategy(
        last,
        Duration::from_secs(60),
        AutoWaitStrategy::BusySpin,
    );
    Ok(())
}

fn signal_consumer() -> Result<(), Box<dyn std::error::Error>> {
    let layout = child_layout();
    let consumer_id = harness::read_env_usize("BENCH_CONSUMER_ID", 0);
    let buffer = harness::read_env_usize("SWEEP_BUFFER", 65536);
    let events = harness::read_env_u64("SWEEP_EVENTS", 10_000_000);
    let warmup = 100_000u64;
    let cid = format!("c{consumer_id}_{}", std::process::id());
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapConsumer::<SignalEvent>::attach(layout.clone(), buffer, &cid) {
            Ok(c) => break c,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(e) => return Err(format!("attach: {e}").into()),
        }
    };
    let mut wc = 0u64;
    let deadline = harness::spin_deadline();
    while wc < warmup {
        if consumer.try_consume_next().is_some() {
            wc += 1;
        } else {
            harness::check_deadline(deadline, "monster_sweep_mmap signal_consumer warmup");
            std::hint::spin_loop();
        }
    }
    let start = Instant::now();
    let mut consumed = 0u64;
    let mut checksum = 0u64;
    while consumed < events {
        if let Some((_seq, event)) = consumer.try_consume_next() {
            checksum = checksum.wrapping_add(event.data);
            consumed += 1;
        } else {
            harness::check_deadline(deadline, "monster_sweep_mmap signal_consumer measured");
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
    Ok(())
}

// ============================================================
// Data class macro — full payload fill + consumer checksum
// ============================================================

macro_rules! sweep_impl {
    ($ev_type:ty, $prod_fn:ident, $cons_fn:ident) => {
        fn $prod_fn() -> Result<(), Box<dyn std::error::Error>> {
            let layout = child_layout();
            let buffer = harness::read_env_usize("SWEEP_BUFFER", 4096);
            let events = harness::read_env_u64("SWEEP_EVENTS", 100_000);
            let consumers = harness::read_env_usize("SWEEP_CONSUMERS", 1);
            let target_rate = harness::read_env_u64("SWEEP_TARGET_RATE", 0);
            let warmup = events / 100;
            layout.ensure_directories()?;
            let mut producer =
                MmapProducer::<$ev_type>::create(layout, buffer, || <$ev_type>::default())?;
            if !producer.wait_for_consumers_ready(consumers as i64, Duration::from_secs(30)) {
                return Err("timeout".into());
            }
            for i in 0..warmup {
                producer.publish(|slot| {
                    slot.sequence = i;
                    slot.timestamp_ns = 0;
                });
            }

            if target_rate > 0 {
                let interval_ns = 1_000_000_000u64 / target_rate;
                let start = Instant::now();
                let base_ns = nanos_now();
                for i in 0..events {
                    let intended_ns = base_ns + i * interval_ns;
                    while nanos_now() < intended_ns {
                        std::hint::spin_loop();
                    }
                    producer.publish(|slot| {
                        slot.sequence = warmup + i;
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

            let last = (warmup + events - 1) as i64;
            producer.wait_until_consumed_with_strategy(
                last,
                Duration::from_secs(120),
                AutoWaitStrategy::BusySpin,
            );
            Ok(())
        }

        fn $cons_fn() -> Result<(), Box<dyn std::error::Error>> {
            let layout = child_layout();
            let consumer_id = harness::read_env_usize("BENCH_CONSUMER_ID", 0);
            let buffer = harness::read_env_usize("SWEEP_BUFFER", 4096);
            let events = harness::read_env_u64("SWEEP_EVENTS", 100_000);
            let warmup = events / 100;
            let cid = format!("c{consumer_id}_{}", std::process::id());
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut consumer = loop {
                match MmapConsumer::<$ev_type>::attach(layout.clone(), buffer, &cid) {
                    Ok(c) => break c,
                    Err(_) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(25))
                    }
                    Err(e) => return Err(format!("attach: {e}").into()),
                }
            };
            let mut wc = 0u64;
            let deadline = harness::spin_deadline();
            while wc < warmup {
                if consumer.try_consume_next().is_some() {
                    wc += 1;
                } else {
                    harness::check_deadline(deadline, concat!(stringify!($cons_fn), " warmup"));
                    std::hint::spin_loop();
                }
            }
            let mut latency = LatencyRecorder::default_range();
            let start = Instant::now();
            let mut consumed = 0u64;
            let mut checksum = 0u64;
            while consumed < events {
                if let Some((_seq, event)) = consumer.try_consume_next() {
                    if event.timestamp_ns > 0 {
                        latency.record_delta(event.timestamp_ns, nanos_now());
                    }
                    let payload_sum = checksum_bytes(&event.payload);
                    black_box(payload_sum);
                    checksum = checksum.wrapping_add(payload_sum);
                    consumed += 1;
                } else {
                    harness::check_deadline(deadline, concat!(stringify!($cons_fn), " measured"));
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
    target_rate: u64,
    prod_role: &'static str,
    cons_role: &'static str,
    tag: &'static str,
}

impl IpcBenchmark for SweepPoint {
    fn bench_name(&self) -> &str {
        "monster_sweep_mmap"
    }

    fn scenario_name(&self) -> String {
        format!("sweep_{}", self.tag)
    }

    fn backend(&self) -> &str {
        "mmap"
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
        harness::bench_timeout_duration(600)
    }

    fn producer_label(&self) -> String {
        format!("{} prod", self.label)
    }

    fn consumer_label(&self, _consumer_id: usize) -> String {
        format!("{} cons", self.label)
    }

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, harness::BenchError> {
        let root = harness::unique_mmap_root(self.tag);
        let segment = harness::unique_mmap_segment(self.tag);
        let root_str = root.display().to_string();

        let producer = spawn_sweep_child(
            exe,
            self.prod_role,
            &root_str,
            &segment,
            self.events,
            self.buffer,
            self.consumers,
            self.target_rate,
        );
        let consumers = (0..self.consumers)
            .map(|i| {
                let mut consumer_env = vec![
                    ("SWEEP_ROOT", root_str.clone()),
                    ("SWEEP_SEGMENT", segment.clone()),
                    ("SWEEP_BUFFER", self.buffer.to_string()),
                    ("SWEEP_EVENTS", self.events.to_string()),
                    ("BENCH_CONSUMER_ID", i.to_string()),
                ];
                if self.target_rate > 0 {
                    consumer_env.push(("SWEEP_TARGET_RATE", self.target_rate.to_string()));
                }
                harness::spawn_child(exe, self.cons_role, &consumer_env)
            })
            .collect();

        Ok(ScenarioChildren::new(producer, consumers).with_cleanup_path(root))
    }

    fn print_summary_with_metrics(
        &self,
        producer: &harness::ProducerOutput,
        consumers: &[harness::ConsumerOutput],
        latency: Option<&perf_bench::latency::LatencyStats>,
    ) {
        let cons_tp = self.average_consumer_ops(consumers);
        let ring_mb = (self.size_bytes as u64 * self.buffer as u64) / (1024 * 1024);
        let bw_gbs = cons_tp * self.size_bytes as f64 / 1e9;
        let lat_str = latency
            .map(|stats| stats.summary())
            .unwrap_or_else(|| "-".to_string());
        let mode_str = if self.target_rate > 0 {
            format!("CO@{}K", self.target_rate / 1000)
        } else {
            "tput".to_string()
        };

        println!(
            "  {:<12} {:>5}  ring={:>5}MB  {:>8} events  prod: {:>10}  cons: {:>10}  {:>6.1} GB/s  {}",
            self.label,
            mode_str,
            ring_mb,
            self.events,
            format_throughput(producer.throughput_ops_sec),
            format_throughput(cons_tp),
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

fn write_sweep_markdown(report: &reporting::BenchReport, path: &str) -> std::io::Result<()> {
    reporting::write_monster_sweep_markdown(report, path, reporting::MonsterSweepBackend::Mmap)
}

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

struct MonsterSweepMmap;

impl harness::BenchHarness for MonsterSweepMmap {
    fn bench_name(&self) -> &'static str {
        "monster_sweep_mmap"
    }

    fn child_roles(&self) -> &'static [harness::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> harness::BenchRunResult {
        let size_arg = args
            .windows(2)
            .find(|w| w[0] == "--size")
            .map(|w| w[1].as_str())
            .unwrap_or("all");
        let mode_arg = args
            .windows(2)
            .find(|w| w[0] == "--mode")
            .map(|w| w[1].as_str())
            .unwrap_or("all");
        let output_args = reporting::ReportOutputArgs::from_args(args);
        let json_mode = output_args.json_mode;
        let quick_mode = output_args.quick_mode;

        let run_throughput = mode_arg == "all" || mode_arg == "throughput";
        let run_co = mode_arg == "all" || mode_arg == "co";

        fn label_for_size(bytes: usize) -> &'static str {
            match bytes {
                64 => "64B",
                512 => "512B",
                1024 => "1KB",
                4096 => "4KB",
                16384 => "16KB",
                65536 => "64KB",
                262144 => "256KB",
                1048576 => "1MB",
                _ => "?",
            }
        }

        fn roles(tag: &str) -> (&'static str, &'static str) {
            match tag {
                t if t.starts_with("SIG") => ("sig_prod", "sig_cons"),
                t if t.starts_with("64B") => ("prod_64b", "cons_64b"),
                t if t.starts_with("512B") => ("prod_512b", "cons_512b"),
                t if t.starts_with("1K") => ("prod_1k", "cons_1k"),
                t if t.starts_with("4K") => ("prod_4k", "cons_4k"),
                t if t.starts_with("16K") => ("prod_16k", "cons_16k"),
                t if t.starts_with("64K") => ("prod_64k", "cons_64k"),
                t if t.starts_with("256K") => ("prod_256k", "cons_256k"),
                t if t.starts_with("1M") => ("prod_1m", "cons_1m"),
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

                for nc in [2usize, 4, 6, 8, 12] {
                    let tag_str = format!("SIG_{nc}c");
                    let tag: &'static str = Box::leak(tag_str.into_boxed_str());
                    let label_str = format!("signalx{nc}c");
                    let label: &'static str = Box::leak(label_str.into_boxed_str());
                    let (p, c) = roles("SIG");
                    v.push(SweepPoint {
                        label,
                        size_bytes: 64,
                        events: 1_000_000,
                        buffer: 65_536,
                        consumers: nc,
                        target_rate: 0,
                        prod_role: p,
                        cons_role: c,
                        tag,
                    });
                }

                // Multi-consumer throughput scenarios across the full size sweep.
                for nc in [2usize, 4, 6, 8, 12] {
                    for (size, events, buffer, base_tag) in [
                        (64usize, 1_000_000u64, 65_536usize, "64B"),
                        (512, 500_000, 131_072, "512B"),
                        (1_024usize, 200_000u64, 131_072usize, "1K"),
                        (4_096, 100_000, 65_536, "4K"),
                        (16_384, 50_000, 32_768, "16K"),
                        (65_536, 20_000, 16_384, "64K"),
                        (262_144, 10_000, 8_192, "256K"),
                        (1_048_576, 5_000, 4_096, "1M"),
                    ] {
                        let tag_str = format!("{base_tag}_{nc}c");
                        let tag: &'static str = Box::leak(tag_str.into_boxed_str());
                        let label_str = format!("{}x{}c", label_for_size(size), nc);
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
                    ("4KB@100K", 4_096, 65_536, 100_000, "4K_CO"),
                    ("64KB@50K", 65_536, 16_384, 50_000, "64K_CO"),
                    ("64KB@100K", 65_536, 16_384, 100_000, "64K_CO"),
                    ("256KB@10K", 262_144, 8_192, 10_000, "256K_CO"),
                    ("1MB@10K", 1_048_576, 4_096, 10_000, "1M_CO"),
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

                for nc in [2usize, 4, 6, 8, 12] {
                    for (size, buffer, rate, base_tag) in [
                        (1_024usize, 131_072usize, 100_000u64, "1K_CO"),
                        (1_024, 131_072, 500_000, "1K_CO"),
                        (4_096, 65_536, 100_000, "4K_CO"),
                        (65_536, 16_384, 50_000, "64K_CO"),
                        (65_536, 16_384, 100_000, "64K_CO"),
                        (262_144, 8_192, 10_000, "256K_CO"),
                        (1_048_576, 4_096, 10_000, "1M_CO"),
                    ] {
                        let tag_str = format!("{base_tag}_{nc}c");
                        let tag: &'static str = Box::leak(tag_str.into_boxed_str());
                        let label_str =
                            format!("{}@{}Kx{}c", label_for_size(size), rate / 1000, nc);
                        let label: &'static str = Box::leak(label_str.into_boxed_str());
                        let (p, c) = roles(base_tag);
                        v.push(SweepPoint {
                            label,
                            size_bytes: size,
                            events: 100_000,
                            buffer,
                            consumers: nc,
                            target_rate: rate,
                            prod_role: p,
                            cons_role: c,
                            tag,
                        });
                    }
                }
            }

            v
        };

        if !json_mode {
            println!("=== Monster Sweep MMAP Benchmark ===");
            println!("Backend: file-backed mmap (M3 Max 14-core, 96GB, 300 GB/s BW)");
            println!("Modes: throughput + CO-aware (fixed rate, intended_send_time)");
            println!("Full payload fill (producer) + checksum (consumer)");
            println!();
        }

        let mut report = BenchReport::new();
        for sp in &scenarios {
            let run = match size_arg {
                "all" => {
                    if quick_mode {
                        matches!(
                            sp.tag,
                            "SIG" | "64B" | "1K" | "64K" | "1M" | "1K_CO" | "64K_CO"
                        ) && (sp.target_rate == 0
                            || sp.target_rate == 500_000
                            || sp.target_rate == 50_000)
                    } else {
                        true
                    }
                }
                tag => sp.tag == tag,
            };
            if run {
                report.add(sp.run_benchmark()?);
            }
        }

        reporting::emit_report(
            &report,
            &output_args,
            Some(reporting::ReportView::Summary),
            Some(reporting::ReportView::Tree),
            Some(write_sweep_markdown),
        );
        Ok(())
    }
}

perf_bench::myelon_bench_main!(MonsterSweepMmap);
