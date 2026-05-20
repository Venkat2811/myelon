//! Dedicated typed zero-copy sweep across SHM and mmap.
//!
//! Makes typed zero-copy a first-class benchmark family instead of hiding it
//! inside `myelon_layers`.

use crate::cli::sweeps::{self as sweep_specs, BasicSweepSelection, SweepBackend};
use crate::infra::coordination::BenchmarkCoordination;
use crate::infra::events::format_throughput;
use crate::infra::output::reporting::{self, BenchReport, ReportOutputArgs};
use crate::infra::{
    self, launch_mmap_group, launch_shm_group, mmap_layout_from_env, read_env_string, read_env_u64,
    read_env_usize, segment_from_env, spin_deadline, ConsumerOutput, IpcBenchmark,
    MultiConsumerSpawn, ProducerOutput, ScenarioChildren,
};
use crate::layers::framed_myelon::codec::payloads::{
    checksum_archived_rkyv, checksum_flatbuf_root, make_payloads, measure_zero_copy_telemetry,
    AccessTelemetry, FlatbufBatch, RkyvBatch,
};
use disruptor_mp::AutoWaitStrategy;
use myelon::transport::{MyelonWaitStrategy, ReassemblyBuffer};
use myelon::typed_transport::{MmapTypedConsumer, MmapTypedProducer, TypedConsumer, TypedProducer};
use myelon::AlignedFixedFrame;
use std::collections::HashMap;
use std::hint::black_box;
use std::time::{Duration, Instant};

const MMAP_BACKLOG_CHECK_INTERVAL: u64 = 32;

/// Aligned zero-copy frame sized for a 64KB ring slot.
type ZcFrame = AlignedFixedFrame<{ 64 * 1024 - 16 }>;

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

fn typed_zc_shm_prod() -> Result<(), Box<dyn std::error::Error>> {
    let segment = segment_from_env("MYELON_BENCH_SEGMENT_NAME");
    let buffer_depth = read_env_usize("MYELON_BENCH_BUFFER_DEPTH", 1024);
    let num_messages = read_env_u64("MYELON_BENCH_NUM_MESSAGES", 50_000);
    let payload_bytes = read_env_usize("MYELON_BENCH_PAYLOAD_BYTES", 1024);
    let batch_size = read_env_usize("MYELON_BENCH_BATCH_SIZE", 8);
    let codec = read_env_string("MYELON_BENCH_CODEC", "rkyv");
    let num_consumers = read_env_usize("MYELON_BENCH_NUM_CONSUMERS", 1);
    let payloads = make_payloads(batch_size);

    let mut producer =
        TypedProducer::<ZcFrame>::create_with_consumers(&segment, buffer_depth, num_consumers)?;
    let coord = BenchmarkCoordination::create(&segment)?;
    if !coord.wait_for_consumers(num_consumers, Duration::from_secs(30)) {
        return Err(format!("timeout waiting for {num_consumers} consumers").into());
    }
    producer.discover_consumers(Duration::from_secs(3));

    let start = Instant::now();
    match codec.as_str() {
        "rkyv" => {
            let payload = RkyvBatch(payloads);
            for i in 0..num_messages {
                producer.publish(&payload, (i % 256) as u8)?;
            }
        }
        "flatbuf" => {
            let payload = FlatbufBatch(payloads);
            for i in 0..num_messages {
                producer.publish(&payload, (i % 256) as u8)?;
            }
        }
        other => return Err(format!("unsupported MYELON_BENCH_CODEC '{other}'").into()),
    }
    let elapsed = start.elapsed();

    let output = ProducerOutput::from_elapsed(num_messages, elapsed, payload_bytes);
    println!("{}", serde_json::to_string(&output)?);

    coord.signal_producer_done(num_messages as i64);
    coord.wait_for_consumers_done(num_consumers, Duration::from_secs(60));
    Ok(())
}

fn typed_zc_shm_cons() -> Result<(), Box<dyn std::error::Error>> {
    let segment = segment_from_env("MYELON_BENCH_SEGMENT_NAME");
    let consumer_id = read_env_usize("MYELON_BENCH_CONSUMER_ID", 0);
    let buffer_depth = read_env_usize("MYELON_BENCH_BUFFER_DEPTH", 1024);
    let num_messages = read_env_u64("MYELON_BENCH_NUM_MESSAGES", 50_000);
    let payload_bytes = read_env_usize("MYELON_BENCH_PAYLOAD_BYTES", 1024);
    let codec = read_env_string("MYELON_BENCH_CODEC", "rkyv");

    let coord = BenchmarkCoordination::attach_with_timeout(&segment, Duration::from_secs(30))?;
    let mut consumer =
        TypedConsumer::<ZcFrame>::attach(&segment, buffer_depth, MyelonWaitStrategy::BusySpin)?;
    let mut reassembly = ReassemblyBuffer::new(payload_bytes.max(256 * 1024));
    coord.signal_consumer_ready();

    let deadline = spin_deadline();
    let mut consumed = 0u64;
    let mut start: Option<Instant> = None;
    let mut checksum = 0u64;

    while consumed < num_messages {
        let delivered = match codec.as_str() {
            "rkyv" => consumer.process_available_zero_copy::<RkyvBatch, _>(
                &mut reassembly,
                |_kind, archived| {
                    if start.is_none() {
                        start = Some(Instant::now());
                    }
                    let payload_sum = checksum_archived_rkyv(archived);
                    black_box(payload_sum);
                    checksum = checksum.wrapping_add(payload_sum);
                    consumed += 1;
                },
            ),
            "flatbuf" => consumer.process_available_zero_copy::<FlatbufBatch, _>(
                &mut reassembly,
                |_kind, archived| {
                    if start.is_none() {
                        start = Some(Instant::now());
                    }
                    let payload_sum = checksum_flatbuf_root(archived);
                    black_box(payload_sum);
                    checksum = checksum.wrapping_add(payload_sum);
                    consumed += 1;
                },
            ),
            other => return Err(format!("unsupported MYELON_BENCH_CODEC '{other}'").into()),
        };
        if consumed < num_messages {
            infra::check_deadline(deadline, "typed_zc_shm_cons measured");
            if delivered == 0 {
                std::hint::spin_loop();
            }
        }
    }

    let elapsed = start
        .expect("consumer never received a zero-copy payload")
        .elapsed();
    let output =
        ConsumerOutput::from_elapsed(consumer_id, consumed, elapsed, payload_bytes, checksum);
    println!("{}", serde_json::to_string(&output)?);
    coord.signal_consumer_done(consumed as i64);
    Ok(())
}

fn throttle_mmap_backlog(
    producer: &mut MmapTypedProducer<ZcFrame>,
    buffer_depth: usize,
    num_consumers: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if num_consumers <= 1 || buffer_depth < 2 {
        return Ok(());
    }

    let backlog_window = (buffer_depth / 2) as i64;
    let framed = producer.raw();
    let last_sequence = framed.raw().last_published_sequence();
    let required_consumed = last_sequence - backlog_window;
    if required_consumed >= 0
        && !framed.wait_until_consumed(
            required_consumed,
            Duration::from_secs(30),
            AutoWaitStrategy::BusySpin,
        )
    {
        return Err(format!(
            "timeout waiting for consumers to drain typed mmap backlog at seq {required_consumed}"
        )
        .into());
    }

    Ok(())
}

fn typed_zc_mmap_prod() -> Result<(), Box<dyn std::error::Error>> {
    let layout = mmap_layout_from_env("MYELON_BENCH_MMAP_ROOT", "MYELON_BENCH_MMAP_SEGMENT");
    let buffer_depth = read_env_usize("MYELON_BENCH_BUFFER_DEPTH", 1024);
    let num_messages = read_env_u64("MYELON_BENCH_NUM_MESSAGES", 50_000);
    let payload_bytes = read_env_usize("MYELON_BENCH_PAYLOAD_BYTES", 1024);
    let batch_size = read_env_usize("MYELON_BENCH_BATCH_SIZE", 8);
    let codec = read_env_string("MYELON_BENCH_CODEC", "rkyv");
    let num_consumers = read_env_usize("MYELON_BENCH_NUM_CONSUMERS", 1);
    let payloads = make_payloads(batch_size);

    let mut producer = MmapTypedProducer::<ZcFrame>::create(layout, buffer_depth)?;
    if !producer
        .raw()
        .wait_for_consumers_ready(num_consumers as i64, Duration::from_secs(30))
    {
        return Err(format!("timeout waiting for {num_consumers} consumers").into());
    }

    let start = Instant::now();
    let mut publish_error: Option<Box<dyn std::error::Error>> = None;
    match codec.as_str() {
        "rkyv" => {
            let payload = RkyvBatch(payloads);
            for i in 0..num_messages {
                if let Some(error) = publish_error.take() {
                    return Err(error);
                }
                producer.publish(&payload, (i % 256) as u8)?;
                if num_consumers > 1 && i % MMAP_BACKLOG_CHECK_INTERVAL == 31 {
                    if let Err(error) =
                        throttle_mmap_backlog(&mut producer, buffer_depth, num_consumers)
                    {
                        publish_error = Some(error);
                    }
                }
            }
        }
        "flatbuf" => {
            let payload = FlatbufBatch(payloads);
            for i in 0..num_messages {
                if let Some(error) = publish_error.take() {
                    return Err(error);
                }
                producer.publish(&payload, (i % 256) as u8)?;
                if num_consumers > 1 && i % MMAP_BACKLOG_CHECK_INTERVAL == 31 {
                    if let Err(error) =
                        throttle_mmap_backlog(&mut producer, buffer_depth, num_consumers)
                    {
                        publish_error = Some(error);
                    }
                }
            }
        }
        other => return Err(format!("unsupported MYELON_BENCH_CODEC '{other}'").into()),
    }
    if let Some(error) = publish_error {
        return Err(error);
    }

    let elapsed = start.elapsed();
    let output = ProducerOutput::from_elapsed(num_messages, elapsed, payload_bytes);
    println!("{}", serde_json::to_string(&output)?);

    let framed = producer.raw();
    let last_sequence = framed.raw().last_published_sequence();
    if !framed.wait_until_consumed(
        last_sequence,
        Duration::from_secs(60),
        AutoWaitStrategy::BusySpin,
    ) {
        return Err("timeout waiting for typed mmap consumers to drain".into());
    }
    Ok(())
}

fn typed_zc_mmap_cons() -> Result<(), Box<dyn std::error::Error>> {
    let layout = mmap_layout_from_env("MYELON_BENCH_MMAP_ROOT", "MYELON_BENCH_MMAP_SEGMENT");
    let consumer_id = read_env_usize("MYELON_BENCH_CONSUMER_ID", 0);
    let buffer_depth = read_env_usize("MYELON_BENCH_BUFFER_DEPTH", 1024);
    let num_messages = read_env_u64("MYELON_BENCH_NUM_MESSAGES", 50_000);
    let payload_bytes = read_env_usize("MYELON_BENCH_PAYLOAD_BYTES", 1024);
    let codec = read_env_string("MYELON_BENCH_CODEC", "rkyv");
    let consumer_name = format!("tzc{consumer_id}_{}", std::process::id());

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapTypedConsumer::<ZcFrame>::attach(
            layout.clone(),
            buffer_depth,
            &consumer_name,
            MyelonWaitStrategy::BusySpin,
        ) {
            Ok(consumer) => break consumer,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(error) => return Err(format!("consumer attach failed: {error}").into()),
        }
    };

    let deadline = spin_deadline();
    let mut reassembly = ReassemblyBuffer::new(payload_bytes.max(256 * 1024));
    let mut consumed = 0u64;
    let mut start: Option<Instant> = None;
    let mut checksum = 0u64;

    while consumed < num_messages {
        let delivered = match codec.as_str() {
            "rkyv" => consumer.process_available_zero_copy::<RkyvBatch, _>(
                &mut reassembly,
                |_kind, archived| {
                    if start.is_none() {
                        start = Some(Instant::now());
                    }
                    let payload_sum = checksum_archived_rkyv(archived);
                    black_box(payload_sum);
                    checksum = checksum.wrapping_add(payload_sum);
                    consumed += 1;
                },
            ),
            "flatbuf" => consumer.process_available_zero_copy::<FlatbufBatch, _>(
                &mut reassembly,
                |_kind, archived| {
                    if start.is_none() {
                        start = Some(Instant::now());
                    }
                    let payload_sum = checksum_flatbuf_root(archived);
                    black_box(payload_sum);
                    checksum = checksum.wrapping_add(payload_sum);
                    consumed += 1;
                },
            ),
            other => return Err(format!("unsupported MYELON_BENCH_CODEC '{other}'").into()),
        };
        if consumed < num_messages {
            infra::check_deadline(deadline, "typed_zc_mmap_cons measured");
            if delivered == 0 {
                std::hint::spin_loop();
            }
        }
    }

    let elapsed = start
        .expect("consumer never received a zero-copy payload")
        .elapsed();
    let output =
        ConsumerOutput::from_elapsed(consumer_id, consumed, elapsed, payload_bytes, checksum);
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

struct Scenario {
    backend_kind: SweepBackend,
    codec: sweep_specs::TypedZeroCopyCodec,
    size_tag: &'static str,
    payload_size: usize,
    batch_size: usize,
    events: u64,
    buffer: usize,
    consumers: usize,
    prod_role: &'static str,
    cons_role: &'static str,
}

impl IpcBenchmark for Scenario {
    fn bench_name(&self) -> &str {
        "typed_zero_copy_sweep"
    }

    fn scenario_name(&self) -> String {
        format!(
            "typed_zero_copy_{}_{}_{}_1p{}c",
            self.codec.slug(),
            self.backend(),
            self.size_tag,
            self.consumers
        )
    }

    fn backend(&self) -> &str {
        self.backend_kind.slug()
    }

    fn layer(&self) -> &str {
        "typed_zero_copy"
    }

    fn transport_metadata(&self) -> reporting::BenchTransportSpec {
        let base = if self.backend_kind == SweepBackend::Shm {
            reporting::BenchTransportSpec::benchmark_shm(self.consumers)
        } else {
            reporting::BenchTransportSpec::mmap_builtin()
        };
        base.with_zero_copy(true).with_framing("fixed_64k")
    }

    fn codec(&self) -> Option<&str> {
        Some(self.codec.slug())
    }

    fn message_size_bytes(&self) -> usize {
        self.payload_size
    }

    fn payload_bytes(&self) -> usize {
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

    fn print_summary_with_metrics(
        &self,
        producer: &ProducerOutput,
        consumers: &[ConsumerOutput],
        _latency: Option<&crate::infra::latency::LatencyStats>,
    ) {
        let avg_consumer = self.average_consumer_ops(consumers);
        println!(
            "  {:<8} {:<5} {:<8} {:<5} prod={:>10} cons={:>10} batch={:<4} bytes={}",
            self.codec.slug(),
            self.backend(),
            self.size_tag,
            format!("1p{}c", self.consumers),
            format_throughput(producer.throughput_ops_sec),
            format_throughput(avg_consumer),
            self.batch_size,
            self.payload_size
        );
    }

    fn launch(&self, exe: &std::path::Path) -> Result<ScenarioChildren, infra::BenchError> {
        let base_envs = vec![
            ("MYELON_BENCH_PAYLOAD_BYTES", self.payload_size.to_string()),
            ("MYELON_BENCH_NUM_MESSAGES", self.events.to_string()),
            ("MYELON_BENCH_BUFFER_DEPTH", self.buffer.to_string()),
            ("MYELON_BENCH_NUM_CONSUMERS", self.consumers.to_string()),
            ("MYELON_BENCH_BATCH_SIZE", self.batch_size.to_string()),
            ("MYELON_BENCH_CODEC", self.codec.slug().to_string()),
        ];
        if self.backend_kind == SweepBackend::Shm {
            launch_shm_group(
                exe,
                &format!("tzc_{}_{}", self.codec.slug(), self.size_tag),
                "MYELON_BENCH_SEGMENT_NAME",
                MultiConsumerSpawn {
                    producer_role: self.prod_role,
                    consumer_role: self.cons_role,
                    consumers: self.consumers,
                    consumer_id_env: "MYELON_BENCH_CONSUMER_ID",
                    base_envs,
                },
            )
        } else {
            launch_mmap_group(
                exe,
                &format!("tzc_{}_{}", self.codec.slug(), self.size_tag),
                "tzc",
                "MYELON_BENCH_MMAP_ROOT",
                "MYELON_BENCH_MMAP_SEGMENT",
                MultiConsumerSpawn {
                    producer_role: self.prod_role,
                    consumer_role: self.cons_role,
                    consumers: self.consumers,
                    consumer_id_env: "MYELON_BENCH_CONSUMER_ID",
                    base_envs,
                },
            )
        }
    }
}

const CHILD_ROLES: &[infra::ChildRole] = &[
    infra::ChildRole::new("typed_zc_shm_prod", typed_zc_shm_prod),
    infra::ChildRole::new("typed_zc_shm_cons", typed_zc_shm_cons),
    infra::ChildRole::new("typed_zc_mmap_prod", typed_zc_mmap_prod),
    infra::ChildRole::new("typed_zc_mmap_cons", typed_zc_mmap_cons),
];

pub struct TypedZeroCopySweep;

impl infra::BenchHarness for TypedZeroCopySweep {
    fn bench_name(&self) -> &'static str {
        "typed_zero_copy_sweep"
    }

    fn child_roles(&self) -> &'static [infra::ChildRole] {
        CHILD_ROLES
    }

    fn run_orchestrator(&self, args: &[String]) -> infra::BenchRunResult {
        let selection = BasicSweepSelection::parse(args)?;
        let output_args = ReportOutputArgs::from_args(args);
        let backend_arg = find_flag_value(args, "--backend").unwrap_or("all");
        let codec_arg = find_flag_value(args, "--codec").unwrap_or("all");

        if !output_args.json_mode {
            println!("=== Typed Zero-Copy Sweep ===");
            println!("Dedicated typed framed zero-copy matrix over SHM and mmap");
            println!("Codecs: rkyv + FlatBuffers");
            println!();
        }

        let mut report = BenchReport::new();
        let mut access_telemetry_cache: HashMap<(String, &'static str), AccessTelemetry> =
            HashMap::new();

        for backend_kind in [SweepBackend::Shm, SweepBackend::Mmap] {
            if backend_arg != "all" && backend_arg != backend_kind.slug() {
                continue;
            }

            let (prod_role, cons_role) = sweep_specs::typed_zero_copy_roles(backend_kind);
            for codec in [
                sweep_specs::TypedZeroCopyCodec::Rkyv,
                sweep_specs::TypedZeroCopyCodec::Flatbuf,
            ] {
                if codec_arg != "all" && codec_arg != codec.slug() {
                    continue;
                }

                for spec in sweep_specs::typed_zero_copy_sweep_specs(codec) {
                    if !selection.matches_size(spec.tag) {
                        continue;
                    }
                    let events = selection.events_for(spec.events);
                    let telemetry = *access_telemetry_cache
                        .entry((codec.slug().to_string(), spec.tag))
                        .or_insert_with(|| {
                            measure_zero_copy_telemetry(
                                codec.slug(),
                                &make_payloads(spec.batch_size),
                            )
                        });
                    for consumers in sweep_specs::TARGET_CONSUMERS {
                        if !selection.matches_consumers(consumers) {
                            continue;
                        }
                        let scenario = Scenario {
                            backend_kind,
                            codec,
                            size_tag: spec.tag,
                            payload_size: spec.payload_bytes,
                            batch_size: spec.batch_size,
                            events,
                            buffer: scaled_buffer(spec.base_buffer, consumers),
                            consumers,
                            prod_role,
                            cons_role,
                        };
                        let mut result = scenario.run_benchmark()?;
                        result.results.access_avg_ns = Some(telemetry.access_avg_ns);
                        result.results.access_vs_decode_speedup =
                            Some(telemetry.access_vs_decode_speedup);
                        result.results.alloc_count = Some(telemetry.access_alloc_count);
                        result.results.alloc_bytes = Some(telemetry.access_alloc_bytes);
                        report.add(result);
                    }
                }
            }
        }

        reporting::emit_report(&report, &output_args, None, None, None);
        Ok(())
    }
}
