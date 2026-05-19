//! Framed myelon sweep across SHM and mmap with real HDR + CO latency.
//!
//! Covers the remaining framed half of RFC 0015 T-11:
//!   - 64KB framed transport over SHM and mmap at 1KB..1MB
//!   - right-sized framed transport over SHM and mmap at 1KB..64KB
//!   - 1,2,4,6,8,12 consumers
//!   - max-throughput and explicit CO-aware modes

use crate::cli::sweeps::{self as sweep_specs, SweepBackend};
use crate::infra::coordination::BenchmarkCoordination;
use crate::infra::events::{format_throughput, nanos_now};
use crate::infra::latency::LatencyRecorder;
use crate::infra::output::reporting::{self, BenchReport, ReportOutputArgs, ReportView};
use crate::infra::{
    self, mmap_layout_from_env, read_env_u64, read_env_usize, segment_from_env, spawn_child,
    unique_mmap_root, unique_mmap_segment, unique_shm_segment, ConsumerOutput, IpcBenchmark,
    ProducerOutput, ScenarioChildren,
};
use crate::layers::framed_myelon::codec::payloads::access_raw;
use myelon::transport::{
    FrameMeta, FramedTransportConsumer, FramedTransportFrame, FramedTransportProducer,
    MmapFramedTransportConsumer, MmapFramedTransportProducer, MyelonWaitStrategy,
};
use std::cell::Cell;
use std::hint::black_box;
use std::time::{Duration, Instant};

const MMAP_BACKLOG_CHECK_INTERVAL: u64 = 32;

thread_local! {
    static INTENDED_SEND_TIMESTAMP_NS: Cell<Option<u64>> = const { Cell::new(None) };
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct TimedFrameHeader {
    len: u32,
    kind: u8,
    flags: u8,
    msg_id: u32,
    timestamp_ns: u64,
}

const TIMED_FRAME_HEADER_BYTES: usize = std::mem::size_of::<TimedFrameHeader>();

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct TimedFrame<const DATA_BYTES: usize> {
    len: u32,
    kind: u8,
    flags: u8,
    msg_id: u32,
    timestamp_ns: u64,
    data: [u8; DATA_BYTES],
}

impl<const DATA_BYTES: usize> Default for TimedFrame<DATA_BYTES> {
    fn default() -> Self {
        Self {
            len: 0,
            kind: 0,
            flags: 0,
            msg_id: 0,
            timestamp_ns: 0,
            data: [0u8; DATA_BYTES],
        }
    }
}

impl<const DATA_BYTES: usize> FramedTransportFrame for TimedFrame<DATA_BYTES> {
    fn payload_capacity() -> usize {
        DATA_BYTES
    }

    fn frame_meta(&self) -> FrameMeta<'_> {
        FrameMeta {
            len: self.len as usize,
            kind: self.kind,
            flags: self.flags,
            msg_id: self.msg_id,
            timestamp_ns: Some(self.timestamp_ns),
            data: &self.data[..self.len as usize],
        }
    }

    fn write_frame(&mut self, payload: &[u8], kind: u8, msg_id: u32, flags: u8) {
        assert!(payload.len() <= DATA_BYTES);
        self.len = payload.len() as u32;
        self.kind = kind;
        self.flags = flags;
        self.msg_id = msg_id;
        self.timestamp_ns = current_send_timestamp_ns();
        self.data[..payload.len()].copy_from_slice(payload);
    }
}

type Frame64K = TimedFrame<{ 64 * 1024 - TIMED_FRAME_HEADER_BYTES }>;
type Frame2K = TimedFrame<{ 2 * 1024 - TIMED_FRAME_HEADER_BYTES }>;
type Frame8K = TimedFrame<{ 8 * 1024 - TIMED_FRAME_HEADER_BYTES }>;
type Frame32K = TimedFrame<{ 32 * 1024 - TIMED_FRAME_HEADER_BYTES }>;
type Frame128K = TimedFrame<{ 128 * 1024 - TIMED_FRAME_HEADER_BYTES }>;

fn wall_clock_ns() -> u64 {
    nanos_now()
}

fn current_send_timestamp_ns() -> u64 {
    INTENDED_SEND_TIMESTAMP_NS
        .with(|cell| cell.get())
        .unwrap_or_else(wall_clock_ns)
}

fn set_intended_send_timestamp(timestamp_ns: Option<u64>) {
    INTENDED_SEND_TIMESTAMP_NS.with(|cell| cell.set(timestamp_ns));
}

fn scaled_buffer(base_buffer: usize, consumers: usize) -> usize {
    base_buffer
        .max(consumers.next_power_of_two() * 256)
        .next_power_of_two()
}

fn find_flag_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.windows(2)
        .find(|window| window[0] == flag)
        .map(|window| window[1].as_str())
}

fn run_publish_loop<F>(events: u64, target_rate: u64, mut publish: F)
where
    F: FnMut(u64),
{
    if let Some(interval_ns) = crate::infra::co_interval_ns(target_rate) {
        let base_ns = wall_clock_ns();
        for i in 0..events {
            let intended_ns = base_ns + i * interval_ns;
            while wall_clock_ns() < intended_ns {
                std::hint::spin_loop();
            }
            set_intended_send_timestamp(Some(intended_ns));
            publish(i);
        }
        set_intended_send_timestamp(None);
    } else {
        set_intended_send_timestamp(None);
        for i in 0..events {
            publish(i);
        }
    }
}

fn throttle_mmap_backlog<T: FramedTransportFrame>(
    producer: &mut MmapFramedTransportProducer<T>,
    buffer_depth: usize,
    num_consumers: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if num_consumers <= 1 || buffer_depth < 2 {
        return Ok(());
    }

    let backlog_window = (buffer_depth / 2) as i64;
    let last_sequence = producer.raw().last_published_sequence();
    let required_consumed = last_sequence - backlog_window;
    if required_consumed >= 0
        && !producer.wait_until_consumed(
            required_consumed,
            Duration::from_secs(30),
            disruptor_mp::AutoWaitStrategy::BusySpin,
        )
    {
        return Err(format!(
            "timeout waiting for consumers to drain framed mmap backlog at seq {required_consumed}"
        )
        .into());
    }

    Ok(())
}

macro_rules! shm_framed_impl {
    ($frame:ty, $prod_fn:ident, $cons_fn:ident) => {
        fn $prod_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = segment_from_env("BENCHMARK_SEGMENT_NAME");
            let buffer_depth = read_env_usize("BENCH_BUFFER_DEPTH", 1024);
            let num_messages = read_env_u64("BENCH_NUM_MESSAGES", 50_000);
            let payload_bytes = read_env_usize("BENCH_PAYLOAD_BYTES", 1024);
            let num_consumers = read_env_usize("BENCH_NUM_CONSUMERS", 1);
            let target_rate = read_env_u64("BENCH_TARGET_RATE", 0);
            let payload = vec![42u8; payload_bytes];

            let mut producer = FramedTransportProducer::<$frame>::create_with_consumers(
                &segment,
                buffer_depth,
                num_consumers,
            )?;
            let coord = BenchmarkCoordination::create(&segment)?;
            if !coord.wait_for_consumers(num_consumers, Duration::from_secs(30)) {
                return Err(format!("timeout waiting for {num_consumers} consumers").into());
            }
            producer.discover_consumers(Duration::from_secs(3));

            let start = Instant::now();
            run_publish_loop(num_messages, target_rate, |i| {
                producer.publish(&payload, (i % 256) as u8);
            });
            let elapsed = start.elapsed();

            let output = ProducerOutput::from_elapsed(num_messages, elapsed, payload_bytes);
            println!("{}", serde_json::to_string(&output)?);

            coord.signal_producer_done(num_messages as i64);
            coord.wait_for_consumers_done(num_consumers, Duration::from_secs(60));
            Ok(())
        }

        fn $cons_fn() -> Result<(), Box<dyn std::error::Error>> {
            let segment = segment_from_env("BENCHMARK_SEGMENT_NAME");
            let consumer_id = read_env_usize("BENCH_CONSUMER_ID", 0);
            let buffer_depth = read_env_usize("BENCH_BUFFER_DEPTH", 1024);
            let num_messages = read_env_u64("BENCH_NUM_MESSAGES", 50_000);
            let payload_bytes = read_env_usize("BENCH_PAYLOAD_BYTES", 1024);

            let coord =
                BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
            let mut consumer = FramedTransportConsumer::<$frame>::attach(
                &segment,
                buffer_depth,
                MyelonWaitStrategy::BusySpin,
            )?;
            coord.signal_consumer_ready();

            let mut start: Option<Instant> = None;
            let mut consumed = 0u64;
            let mut checksum = 0u64;
            let mut latency = LatencyRecorder::default_range();

            while consumed < num_messages {
                let (meta, data) = consumer.recv_message_blocking_with_meta();
                if start.is_none() {
                    start = Some(Instant::now());
                }
                let payload_sum = access_raw(&data);
                black_box(payload_sum);
                checksum = checksum.wrapping_add(payload_sum);
                if let Some(lat_ns) = meta.one_way_latency_ns() {
                    latency.record(lat_ns);
                }
                consumed += 1;
            }

            let elapsed = start.expect("consumer never received a frame").elapsed();
            let output = if let Some(stats) = latency.stats() {
                ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    payload_bytes,
                    checksum,
                )
                .with_latency(stats)
            } else {
                ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    payload_bytes,
                    checksum,
                )
            };
            println!("{}", serde_json::to_string(&output)?);
            coord.signal_consumer_done(consumed as i64);
            Ok(())
        }
    };
}

macro_rules! mmap_framed_impl {
    ($frame:ty, $prod_fn:ident, $cons_fn:ident) => {
        fn $prod_fn() -> Result<(), Box<dyn std::error::Error>> {
            let layout = mmap_layout_from_env("MMAP_ROOT", "MMAP_SEGMENT");
            let buffer_depth = read_env_usize("BENCH_BUFFER_DEPTH", 1024);
            let num_messages = read_env_u64("BENCH_NUM_MESSAGES", 50_000);
            let payload_bytes = read_env_usize("BENCH_PAYLOAD_BYTES", 1024);
            let num_consumers = read_env_usize("BENCH_NUM_CONSUMERS", 1);
            let target_rate = read_env_u64("BENCH_TARGET_RATE", 0);
            let payload = vec![42u8; payload_bytes];

            let mut producer = MmapFramedTransportProducer::<$frame>::create(layout, buffer_depth)?;
            if !producer.wait_for_consumers_ready(num_consumers as i64, Duration::from_secs(30)) {
                return Err(format!("timeout waiting for {num_consumers} consumers").into());
            }

            let start = Instant::now();
            let mut publish_error: Option<Box<dyn std::error::Error>> = None;
            run_publish_loop(num_messages, target_rate, |i| {
                if publish_error.is_some() {
                    return;
                }
                producer.publish(&payload, (i % 256) as u8);
                if num_consumers > 1 && i % MMAP_BACKLOG_CHECK_INTERVAL == 31 {
                    if let Err(error) =
                        throttle_mmap_backlog(&mut producer, buffer_depth, num_consumers)
                    {
                        publish_error = Some(error);
                    }
                }
            });
            if let Some(error) = publish_error {
                return Err(error);
            }
            let elapsed = start.elapsed();

            let output = ProducerOutput::from_elapsed(num_messages, elapsed, payload_bytes);
            println!("{}", serde_json::to_string(&output)?);

            let last_sequence = producer.raw().last_published_sequence();
            if !producer.wait_until_consumed(
                last_sequence,
                Duration::from_secs(60),
                disruptor_mp::AutoWaitStrategy::BusySpin,
            ) {
                return Err("timeout waiting for framed mmap consumers to drain".into());
            }
            Ok(())
        }

        fn $cons_fn() -> Result<(), Box<dyn std::error::Error>> {
            let layout = mmap_layout_from_env("MMAP_ROOT", "MMAP_SEGMENT");
            let consumer_id = read_env_usize("BENCH_CONSUMER_ID", 0);
            let buffer_depth = read_env_usize("BENCH_BUFFER_DEPTH", 1024);
            let num_messages = read_env_u64("BENCH_NUM_MESSAGES", 50_000);
            let payload_bytes = read_env_usize("BENCH_PAYLOAD_BYTES", 1024);
            let consumer_name = format!("c{consumer_id}_{}", std::process::id());

            let deadline = Instant::now() + Duration::from_secs(15);
            let mut consumer = loop {
                match MmapFramedTransportConsumer::<$frame>::attach(
                    layout.clone(),
                    buffer_depth,
                    &consumer_name,
                    MyelonWaitStrategy::BusySpin,
                ) {
                    Ok(consumer) => break consumer,
                    Err(_) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(25))
                    }
                    Err(error) => return Err(format!("consumer attach failed: {error}").into()),
                }
            };

            let mut start: Option<Instant> = None;
            let mut consumed = 0u64;
            let mut checksum = 0u64;
            let mut latency = LatencyRecorder::default_range();

            while consumed < num_messages {
                let (meta, data) = consumer.recv_message_blocking_with_meta();
                if start.is_none() {
                    start = Some(Instant::now());
                }
                let payload_sum = access_raw(&data);
                black_box(payload_sum);
                checksum = checksum.wrapping_add(payload_sum);
                if let Some(lat_ns) = meta.one_way_latency_ns() {
                    latency.record(lat_ns);
                }
                consumed += 1;
            }

            let elapsed = start.expect("consumer never received a frame").elapsed();
            let output = if let Some(stats) = latency.stats() {
                ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    payload_bytes,
                    checksum,
                )
                .with_latency(stats)
            } else {
                ConsumerOutput::from_elapsed(
                    consumer_id,
                    consumed,
                    elapsed,
                    payload_bytes,
                    checksum,
                )
            };
            println!("{}", serde_json::to_string(&output)?);
            Ok(())
        }
    };
}

shm_framed_impl!(Frame64K, shm_frame64_prod, shm_frame64_cons);
shm_framed_impl!(Frame2K, shm_frame2k_prod, shm_frame2k_cons);
shm_framed_impl!(Frame8K, shm_frame8k_prod, shm_frame8k_cons);
shm_framed_impl!(Frame32K, shm_frame32k_prod, shm_frame32k_cons);
shm_framed_impl!(Frame128K, shm_frame128k_prod, shm_frame128k_cons);

mmap_framed_impl!(Frame64K, mmap_frame64_prod, mmap_frame64_cons);
mmap_framed_impl!(Frame2K, mmap_frame2k_prod, mmap_frame2k_cons);
mmap_framed_impl!(Frame8K, mmap_frame8k_prod, mmap_frame8k_cons);
mmap_framed_impl!(Frame32K, mmap_frame32k_prod, mmap_frame32k_cons);
mmap_framed_impl!(Frame128K, mmap_frame128k_prod, mmap_frame128k_cons);

struct Scenario {
    layer: &'static str,
    backend: &'static str,
    size_tag: &'static str,
    payload_size: usize,
    events: u64,
    buffer: usize,
    consumers: usize,
    target_rate: u64,
    prod_role: &'static str,
    cons_role: &'static str,
}

impl IpcBenchmark for Scenario {
    fn bench_name(&self) -> &str {
        "myelon_framed_sweep"
    }

    fn scenario_name(&self) -> String {
        let base = format!(
            "{}_{}_{}_1p{}c",
            self.layer, self.backend, self.size_tag, self.consumers
        );
        if self.target_rate > 0 {
            format!("{base}_co_{}rps", self.target_rate)
        } else {
            base
        }
    }

    fn backend(&self) -> &str {
        self.backend
    }

    fn layer(&self) -> &str {
        self.layer
    }

    fn transport_metadata(&self) -> reporting::BenchTransportSpec {
        let base = if self.backend == "shm" {
            reporting::BenchTransportSpec::benchmark_shm(self.consumers)
        } else {
            reporting::BenchTransportSpec::mmap_builtin()
        };
        base.with_zero_copy(false)
            .with_framing(if self.layer == "framed_right" {
                "right_sized"
            } else {
                "fixed_64k"
            })
    }

    fn measurement_mode(&self) -> String {
        if self.target_rate > 0 {
            format!("co_aware@{}", self.target_rate)
        } else {
            "max_throughput".to_string()
        }
    }

    fn message_size_bytes(&self) -> usize {
        self.payload_size
    }

    fn buffer_depth(&self) -> usize {
        self.buffer
    }

    fn num_messages(&self) -> u64 {
        self.events
    }

    fn num_consumers(&self) -> usize {
        self.consumers
    }

    fn throughput_unit(&self) -> &str {
        "msgs/s"
    }

    fn timeout(&self) -> Duration {
        Duration::from_secs(300)
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
        let mode = if self.target_rate > 0 {
            format!("CO@{}", self.target_rate)
        } else {
            "tput".to_string()
        };
        let avg_consumer = self.average_consumer_ops(consumers);
        let p99 = latency
            .map(|stats| crate::infra::latency::format_ns(stats.p99_ns))
            .unwrap_or_else(|| "-".to_string());
        println!(
            "  {:<14} {:<4} {:<8} 1p{:<2}c prod={:>10} cons={:>10} p99={:>8}",
            self.layer,
            self.backend,
            mode,
            self.consumers,
            format_throughput(producer.throughput_ops_sec),
            format_throughput(avg_consumer),
            p99,
        );
    }

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, infra::BenchError> {
        if self.backend == "shm" {
            let segment = unique_shm_segment(&format!("mfs_{}_{}", self.layer, self.size_tag));
            let mut envs = vec![
                ("BENCHMARK_SEGMENT_NAME", segment.clone()),
                ("BENCH_PAYLOAD_BYTES", self.payload_size.to_string()),
                ("BENCH_NUM_MESSAGES", self.events.to_string()),
                ("BENCH_BUFFER_DEPTH", self.buffer.to_string()),
                ("BENCH_NUM_CONSUMERS", self.consumers.to_string()),
            ];
            if self.target_rate > 0 {
                envs.push(("BENCH_TARGET_RATE", self.target_rate.to_string()));
            }
            let producer = spawn_child(exe, self.prod_role, &envs);
            let consumers = (0..self.consumers)
                .map(|consumer_id| {
                    let mut consumer_envs = envs.clone();
                    consumer_envs.push(("BENCH_CONSUMER_ID", consumer_id.to_string()));
                    spawn_child(exe, self.cons_role, &consumer_envs)
                })
                .collect();
            Ok(ScenarioChildren::new(producer, consumers))
        } else {
            let root = unique_mmap_root(&format!("mfs_{}_{}", self.layer, self.size_tag));
            let segment = unique_mmap_segment("framed");
            let mut envs = vec![
                ("MMAP_ROOT", root.display().to_string()),
                ("MMAP_SEGMENT", segment),
                ("BENCH_PAYLOAD_BYTES", self.payload_size.to_string()),
                ("BENCH_NUM_MESSAGES", self.events.to_string()),
                ("BENCH_BUFFER_DEPTH", self.buffer.to_string()),
                ("BENCH_NUM_CONSUMERS", self.consumers.to_string()),
            ];
            if self.target_rate > 0 {
                envs.push(("BENCH_TARGET_RATE", self.target_rate.to_string()));
            }
            let producer = spawn_child(exe, self.prod_role, &envs);
            let consumers = (0..self.consumers)
                .map(|consumer_id| {
                    let mut consumer_envs = envs.clone();
                    consumer_envs.push(("BENCH_CONSUMER_ID", consumer_id.to_string()));
                    spawn_child(exe, self.cons_role, &consumer_envs)
                })
                .collect();
            Ok(ScenarioChildren::new(producer, consumers).with_cleanup_path(root))
        }
    }
}

const CHILD_ROLES: &[infra::ChildRole] = &[
    infra::ChildRole::new("shm_frame64_prod", shm_frame64_prod),
    infra::ChildRole::new("shm_frame64_cons", shm_frame64_cons),
    infra::ChildRole::new("shm_frame2k_prod", shm_frame2k_prod),
    infra::ChildRole::new("shm_frame2k_cons", shm_frame2k_cons),
    infra::ChildRole::new("shm_frame8k_prod", shm_frame8k_prod),
    infra::ChildRole::new("shm_frame8k_cons", shm_frame8k_cons),
    infra::ChildRole::new("shm_frame32k_prod", shm_frame32k_prod),
    infra::ChildRole::new("shm_frame32k_cons", shm_frame32k_cons),
    infra::ChildRole::new("shm_frame128k_prod", shm_frame128k_prod),
    infra::ChildRole::new("shm_frame128k_cons", shm_frame128k_cons),
    infra::ChildRole::new("mmap_frame64_prod", mmap_frame64_prod),
    infra::ChildRole::new("mmap_frame64_cons", mmap_frame64_cons),
    infra::ChildRole::new("mmap_frame2k_prod", mmap_frame2k_prod),
    infra::ChildRole::new("mmap_frame2k_cons", mmap_frame2k_cons),
    infra::ChildRole::new("mmap_frame8k_prod", mmap_frame8k_prod),
    infra::ChildRole::new("mmap_frame8k_cons", mmap_frame8k_cons),
    infra::ChildRole::new("mmap_frame32k_prod", mmap_frame32k_prod),
    infra::ChildRole::new("mmap_frame32k_cons", mmap_frame32k_cons),
    infra::ChildRole::new("mmap_frame128k_prod", mmap_frame128k_prod),
    infra::ChildRole::new("mmap_frame128k_cons", mmap_frame128k_cons),
];

pub struct MyelonFramedSweep;

impl infra::BenchHarness for MyelonFramedSweep {
    fn bench_name(&self) -> &'static str {
        "myelon_framed_sweep"
    }

    fn child_roles(&self) -> &'static [infra::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> infra::BenchRunResult {
        let output_args = ReportOutputArgs::from_args(args);
        let backend_arg = find_flag_value(args, "--backend").unwrap_or("all");
        let layer_arg = find_flag_value(args, "--layer").unwrap_or("all");
        let size_arg = find_flag_value(args, "--size").unwrap_or("all");
        let consumers_arg = find_flag_value(args, "--consumers").unwrap_or("all");
        let mode_arg = find_flag_value(args, "--mode").unwrap_or("throughput");
        let target_rate_arg = find_flag_value(args, "--target-rate")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let num_messages_override =
            find_flag_value(args, "--num-messages").and_then(|value| value.parse::<u64>().ok());
        let run_throughput = mode_arg == "throughput" || mode_arg == "all";
        let run_co = mode_arg == "co" || mode_arg == "all";

        if !output_args.json_mode {
            println!("=== Myelon Framed Sweep ===");
            println!(
                "Layers: framed (64KB transport frame) + framed_right (single-frame right-sized)"
            );
            println!("Backends: SHM + mmap");
            println!("Modes: max_throughput + explicit CO-aware latency");
            println!();
        }

        let mut report = BenchReport::new();

        for backend_kind in [SweepBackend::Shm, SweepBackend::Mmap] {
            let backend = backend_kind.slug();
            if backend_arg != "all" && backend_arg != backend {
                continue;
            }

            for size in sweep_specs::framed_sweep_specs() {
                if layer_arg != "all" && layer_arg != size.layer.slug() {
                    continue;
                }
                if size_arg != "all" && size_arg != size.tag {
                    continue;
                }

                let co_rate = if target_rate_arg > 0 {
                    target_rate_arg
                } else {
                    sweep_specs::framed_sweep_default_co_target_rate(size.tag)
                };
                let (prod_role, cons_role) =
                    sweep_specs::framed_sweep_roles(backend_kind, size.role_key);
                let target_rates: Vec<u64> = if co_rate > 0 {
                    vec![0, co_rate]
                } else {
                    vec![0]
                };

                for consumers in sweep_specs::TARGET_CONSUMERS {
                    let consumers_match = consumers_arg == "all"
                        || consumers_arg.parse::<usize>().ok() == Some(consumers);
                    if !consumers_match {
                        continue;
                    }
                    for target_rate in &target_rates {
                        let target_rate = *target_rate;
                        if (target_rate == 0 && !run_throughput) || (target_rate > 0 && !run_co) {
                            continue;
                        }
                        let scenario = Scenario {
                            layer: size.layer.slug(),
                            backend,
                            size_tag: size.tag,
                            payload_size: size.payload_bytes,
                            events: num_messages_override.unwrap_or(size.events),
                            buffer: scaled_buffer(size.base_buffer, consumers),
                            consumers,
                            target_rate,
                            prod_role,
                            cons_role,
                        };
                        report.add(scenario.run_benchmark()?);
                    }
                }
            }
        }

        reporting::emit_report(
            &report,
            &output_args,
            Some(ReportView::Summary),
            Some(ReportView::Summary),
            None,
        );
        Ok(())
    }
}
