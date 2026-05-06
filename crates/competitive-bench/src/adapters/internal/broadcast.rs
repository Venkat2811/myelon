#[allow(dead_code)]
#[allow(clippy::duplicate_mod)]
#[path = "../../../../disruptor-mp/benches/ipc/competitive/common.rs"]
mod common;

use crate::infra::result_json::{BenchmarkConfigOut, BenchmarkResultsOut, LatencyStatsOut};
use clap::Parser;
use common::{nanos_now, BenchmarkEvent};
use disruptor_mp::{
    attach_shared_consumer as disruptor_attach_shared_consumer,
    build_shared_single_producer as disruptor_build_shared_single_producer, AutoWaitStrategy,
    MmapConsumer, MmapProducer, MmapTransportLayout, SharedConsumer, SharedProducer,
};
use myelon::{
    attach_shared_consumer as myelon_attach_shared_consumer,
    build_shared_single_producer as myelon_build_shared_single_producer,
};
use perf_bench::cli::pingpong::{
    apply_wait_strategy, default_buffer_size, run_with_large_stack_if_needed,
};
use perf_bench::infra::coordination::BenchmarkCoordination;
use perf_bench::infra::latency::LatencyRecorder;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const DISCOVERY_SCAN_SLEEP: Duration = Duration::from_millis(50);
const CONSUMER_PREFIX: &str = "bc";
const ATTACH_TIMEOUT: Duration = Duration::from_secs(60);
const COORD_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "Internal raw broadcast benchmark")]
struct Args {
    #[arg(long, value_parser = ["controller", "consumer"], default_value = "controller")]
    mode: String,

    #[arg(long, value_parser = ["disruptor-shm", "disruptor-mmap", "myelon-raw-shm", "myelon-raw-mmap"])]
    adapter: String,

    #[arg(long, default_value_t = default_base_name())]
    base: String,

    #[arg(long, short = 's', default_value_t = 64)]
    message_size: usize,

    #[arg(long, default_value_t = 10_000)]
    warmup: u64,

    #[arg(long, short = 'n', default_value_t = 100_000)]
    num_messages: u64,

    #[arg(long, default_value_t = 4)]
    consumers: usize,

    #[arg(long, default_value = "busyspin")]
    wait_strategy: String,

    #[arg(long)]
    target_rate: Option<u64>,

    #[arg(long)]
    json: bool,

    #[arg(long)]
    buffer_size: Option<usize>,

    #[arg(long, hide = true)]
    consumer_id: Option<usize>,

    #[arg(long, hide = true)]
    result_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConsumerResult {
    consumer_id: usize,
    consumed: u64,
    duration_secs: f64,
    latency_stats: LatencyStatsOut,
    verification_passed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdapterKind {
    DisruptorShm,
    DisruptorMmap,
    MyelonRawShm,
    MyelonRawMmap,
}

impl AdapterKind {
    fn parse(raw: &str) -> Self {
        match raw {
            "disruptor-shm" => Self::DisruptorShm,
            "disruptor-mmap" => Self::DisruptorMmap,
            "myelon-raw-shm" => Self::MyelonRawShm,
            "myelon-raw-mmap" => Self::MyelonRawMmap,
            _ => panic!("unsupported adapter: {raw}"),
        }
    }

    fn display(self) -> &'static str {
        match self {
            Self::DisruptorShm => "disruptor-shm",
            Self::DisruptorMmap => "disruptor-mmap",
            Self::MyelonRawShm => "myelon-raw-shm",
            Self::MyelonRawMmap => "myelon-raw-mmap",
        }
    }

    fn is_shm(self) -> bool {
        matches!(self, Self::DisruptorShm | Self::MyelonRawShm)
    }
}

fn default_base_name() -> String {
    format!("cbcast_{}", std::process::id())
}

fn encode_base36(mut value: u64, width: usize) -> String {
    const ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = vec![b'0'; width];
    for slot in out.iter_mut().rev() {
        *slot = ALPHABET[(value % 36) as usize];
        value /= 36;
    }
    String::from_utf8(out).expect("base36 output should stay ASCII")
}

fn stable_short_name(base: &str, tag: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in tag.bytes().chain(base.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{tag}{}", encode_base36(hash % 2_821_109_907_456, 8))
}

fn coordination_prefix(base: &str) -> String {
    stable_short_name(base, "bc")
}

fn cleanup_coordination(prefix: &str) {
    for suffix in ["cr", "pd", "ep", "cd", "ec"] {
        let name = format!("{prefix}{suffix}");
        if let Ok(c_name) = std::ffi::CString::new(name) {
            unsafe {
                libc::shm_unlink(c_name.as_ptr());
            }
        }
    }
}

fn shm_segment_name(base: &str) -> String {
    stable_short_name(base, "bs")
}

fn mmap_root(base: &str) -> PathBuf {
    std::env::temp_dir().join(format!("competitive-broadcast-{base}"))
}

fn mmap_segment(base: &str) -> String {
    format!("{}seg", base)
}

fn consumer_result_path(base: &str, consumer_id: usize) -> PathBuf {
    std::env::temp_dir().join(format!("{base}_broadcast_consumer_{consumer_id}.json"))
}

fn debug_enabled() -> bool {
    std::env::var("COMP_BENCH_DEBUG").ok().as_deref() == Some("1")
}

fn append_debug_line(message: &str) {
    use std::io::Write;

    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("/tmp/competitive_bench_debug.log")
    {
        let _ = writeln!(file, "{message}");
    }
}

macro_rules! debug_log {
    ($($arg:tt)*) => {
        if debug_enabled() {
            let message = format!($($arg)*);
            eprintln!("{}", message);
            append_debug_line(&message);
        }
    };
}

fn effective_buffer_size(message_size: usize, consumers: usize, num_messages: u64) -> usize {
    let base = default_buffer_size(message_size);
    let count_hint = (num_messages.min(4096) as usize).max(consumers.saturating_mul(16));
    let cap = match message_size {
        0..=131_072 => 4096,
        131_073..=1_048_576 => 512,
        1_048_577..=2_097_152 => 128,
        2_097_153..=8_388_608 => 32,
        8_388_609..=16_777_216 => 16,
        _ => 8,
    };
    base.max(count_hint.min(cap))
}

fn write_event<const SIZE: usize>(
    slot: &mut BenchmarkEvent<SIZE>,
    sequence: u64,
    timestamp_ns: u64,
    intended_send_time_ns: u64,
) {
    slot.sequence = sequence;
    slot.timestamp_ns = timestamp_ns;
    slot.intended_send_time_ns = intended_send_time_ns;
}

fn spawn_consumer(
    exe: &Path,
    args: &Args,
    consumer_id: usize,
    result_path: &Path,
) -> Result<std::process::Child, Box<dyn std::error::Error>> {
    let mut cmd = Command::new(exe);
    cmd.arg("--mode")
        .arg("consumer")
        .arg("--adapter")
        .arg(&args.adapter)
        .arg("--base")
        .arg(&args.base)
        .arg("--message-size")
        .arg(args.message_size.to_string())
        .arg("--warmup")
        .arg(args.warmup.to_string())
        .arg("--num-messages")
        .arg(args.num_messages.to_string())
        .arg("--consumers")
        .arg(args.consumers.to_string())
        .arg("--wait-strategy")
        .arg(&args.wait_strategy)
        .arg("--consumer-id")
        .arg(consumer_id.to_string())
        .arg("--result-path")
        .arg(result_path)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());

    if let Some(target_rate) = args.target_rate {
        cmd.arg("--target-rate").arg(target_rate.to_string());
    }
    if let Some(buffer_size) = args.buffer_size {
        cmd.arg("--buffer-size").arg(buffer_size.to_string());
    }

    Ok(cmd.spawn()?)
}

fn load_consumer_results(
    base: &str,
    consumers: usize,
) -> Result<Vec<ConsumerResult>, Box<dyn std::error::Error>> {
    let mut results = Vec::with_capacity(consumers);
    for consumer_id in 0..consumers {
        let path = consumer_result_path(base, consumer_id);
        let text = fs::read_to_string(&path)?;
        results.push(serde_json::from_str(&text)?);
        let _ = fs::remove_file(path);
    }
    Ok(results)
}

fn worst_consumer_latency(
    results: &[ConsumerResult],
) -> Result<LatencyStatsOut, Box<dyn std::error::Error>> {
    results
        .iter()
        .max_by_key(|result| result.latency_stats.p99)
        .map(|result| result.latency_stats.clone())
        .ok_or_else(|| "missing consumer latency stats".into())
}

fn emit_result(
    args: &Args,
    adapter: AdapterKind,
    buffer_size: usize,
    publish_duration: Duration,
    total_duration: Duration,
    latency_stats: LatencyStatsOut,
    verification_passed: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = BenchmarkResultsOut {
        adapter: adapter.display().to_string(),
        family: "broadcast".to_string(),
        config: BenchmarkConfigOut {
            message_size: args.message_size,
            num_messages: args.num_messages,
            warmup_messages: args.warmup,
            buffer_size,
            wait_strategy: args.wait_strategy.clone(),
            consumers: Some(args.consumers),
        },
        throughput: args.num_messages as f64 / publish_duration.as_secs_f64(),
        fanout_throughput: Some(
            (args.num_messages * args.consumers as u64) as f64 / total_duration.as_secs_f64(),
        ),
        messages_processed: args.num_messages * args.consumers as u64,
        duration_secs: total_duration.as_secs_f64(),
        publish_duration_secs: Some(publish_duration.as_secs_f64()),
        latency_stats,
        timestamp: chrono::Utc::now().to_rfc3339(),
        verification_passed: Some(verification_passed),
        measurement_mode: Some(if args.target_rate.is_some() {
            "fixed_rate".to_string()
        } else {
            "max_throughput".to_string()
        }),
        target_rate: args.target_rate,
        consumer_count: Some(args.consumers),
        coordinated_omission_stats: None,
    };
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

fn controller<const SIZE: usize>(
    args: Args,
    adapter: AdapterKind,
) -> Result<(), Box<dyn std::error::Error>> {
    let buffer_size = args.buffer_size.unwrap_or_else(|| {
        effective_buffer_size(args.message_size, args.consumers, args.num_messages)
    });
    let coord_prefix = coordination_prefix(&args.base);
    cleanup_coordination(&coord_prefix);
    let coord = BenchmarkCoordination::create(&coord_prefix)?;
    let exe = std::env::current_exe()?;

    let mut children = Vec::with_capacity(args.consumers);
    for consumer_id in 0..args.consumers {
        let path = consumer_result_path(&args.base, consumer_id);
        let _ = fs::remove_file(&path);
        children.push(spawn_consumer(&exe, &args, consumer_id, &path)?);
    }

    let controller_result = if adapter.is_shm() {
        run_controller_shm::<SIZE>(&args, adapter, buffer_size, &coord)
    } else {
        run_controller_mmap::<SIZE>(&args, adapter, buffer_size, &coord)
    };

    for mut child in children {
        let status = child.wait()?;
        if !status.success() {
            return Err(format!("broadcast consumer exited with status {status}").into());
        }
    }

    controller_result
}

fn build_shm_producer<const SIZE: usize>(
    adapter: AdapterKind,
    segment: &str,
    buffer_size: usize,
) -> Result<SharedProducer<BenchmarkEvent<SIZE>>, Box<dyn std::error::Error>> {
    let builder = match adapter {
        AdapterKind::DisruptorShm => {
            disruptor_build_shared_single_producer::<BenchmarkEvent<SIZE>>(segment, buffer_size)
        }
        AdapterKind::MyelonRawShm => {
            myelon_build_shared_single_producer::<BenchmarkEvent<SIZE>>(segment, buffer_size)
        }
        _ => unreachable!(),
    };

    Ok(builder.build_producer(BenchmarkEvent::default)?)
}

fn attach_shm_consumer<const SIZE: usize>(
    adapter: AdapterKind,
    segment: &str,
    buffer_size: usize,
    consumer_id: &str,
) -> Result<SharedConsumer<BenchmarkEvent<SIZE>>, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + ATTACH_TIMEOUT;
    loop {
        let attempt = match adapter {
            AdapterKind::DisruptorShm => {
                disruptor_attach_shared_consumer::<BenchmarkEvent<SIZE>>(segment, buffer_size)
                    .with_consumer_id(consumer_id)
                    .build_consumer()
            }
            AdapterKind::MyelonRawShm => {
                myelon_attach_shared_consumer::<BenchmarkEvent<SIZE>>(segment, buffer_size)
                    .with_consumer_id(consumer_id)
                    .build_consumer()
            }
            _ => unreachable!(),
        };
        match attempt {
            Ok(consumer) => return Ok(consumer),
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(error) => return Err(format!("attach failed for {consumer_id}: {error}").into()),
        }
    }
}

fn publish_with_optional_rate<const SIZE: usize, F>(args: &Args, mut publish: F) -> Duration
where
    F: FnMut(u64, u64, u64),
{
    let publish_start = Instant::now();
    if let Some(target_rate) = args.target_rate {
        let interval_ns = std::cmp::max(1, 1_000_000_000u64 / target_rate);
        let base_instant = Instant::now();
        let base_target_ns = nanos_now();
        let mut intended_instant = base_instant;
        let mut intended_target_ns = base_target_ns;
        for i in 0..args.num_messages {
            if i > 0 {
                intended_instant += Duration::from_nanos(interval_ns);
                intended_target_ns = intended_target_ns.saturating_add(interval_ns);
            }
            while Instant::now() < intended_instant {
                std::hint::spin_loop();
            }
            publish(args.warmup + i, nanos_now(), intended_target_ns);
        }
    } else {
        for i in 0..args.num_messages {
            publish(args.warmup + i, nanos_now(), 0);
        }
    }
    publish_start.elapsed()
}

fn run_controller_shm<const SIZE: usize>(
    args: &Args,
    adapter: AdapterKind,
    buffer_size: usize,
    coord: &BenchmarkCoordination,
) -> Result<(), Box<dyn std::error::Error>> {
    let segment = shm_segment_name(&args.base);
    let mut producer = build_shm_producer::<SIZE>(adapter, &segment, buffer_size)?;
    debug_log!("shm controller: producer built segment={segment} buffer={buffer_size}");

    if !coord.wait_for_consumers(args.consumers, COORD_TIMEOUT) {
        return Err("timeout waiting for broadcast coordination consumers".into());
    }
    debug_log!(
        "shm controller: coordination reports {} consumers ready",
        args.consumers
    );
    let consumer_names: Vec<String> = (0..args.consumers)
        .map(|consumer_id| format!("{CONSUMER_PREFIX}_{consumer_id}"))
        .collect();
    let deadline = Instant::now() + COORD_TIMEOUT;
    let mut discovered = vec![false; consumer_names.len()];
    while discovered.iter().any(|ready| !ready) {
        for (idx, consumer_name) in consumer_names.iter().enumerate() {
            if !discovered[idx] && producer.discover_consumer_id(consumer_name) {
                discovered[idx] = true;
            }
        }
        if discovered.iter().all(|ready| *ready) {
            break;
        }
        if Instant::now() >= deadline {
            return Err("timeout discovering SHM broadcast consumers".into());
        }
        std::thread::sleep(DISCOVERY_SCAN_SLEEP);
    }
    debug_log!(
        "shm controller: discovered all consumers: {:?}",
        consumer_names
    );

    for i in 0..args.warmup {
        producer.publish(|slot| write_event(slot, i, 0, 0));
    }
    debug_log!("shm controller: warmup complete ({})", args.warmup);

    let total_start = Instant::now();
    let publish_duration = publish_with_optional_rate::<SIZE, _>(
        args,
        |sequence, timestamp_ns, intended_send_time_ns| {
            producer
                .publish(|slot| write_event(slot, sequence, timestamp_ns, intended_send_time_ns));
        },
    );
    coord.signal_producer_done(args.num_messages as i64);
    debug_log!(
        "shm controller: measured publish complete ({})",
        args.num_messages
    );

    if !coord.wait_for_consumers_done(args.consumers, COORD_TIMEOUT) {
        return Err("timeout waiting for broadcast consumers to finish".into());
    }
    debug_log!("shm controller: all consumers done");
    let total_duration = total_start.elapsed();
    let results = load_consumer_results(&args.base, args.consumers)?;
    let verification_passed = results.iter().all(|r| r.verification_passed);
    let latency_stats = worst_consumer_latency(&results)?;

    let _ = producer.wait_until_consumed_with_strategy(
        (args.warmup + args.num_messages - 1) as i64,
        COORD_TIMEOUT,
        AutoWaitStrategy::BusySpin,
    );

    emit_result(
        args,
        adapter,
        buffer_size,
        publish_duration,
        total_duration,
        latency_stats,
        verification_passed,
    )
}

fn run_controller_mmap<const SIZE: usize>(
    args: &Args,
    adapter: AdapterKind,
    buffer_size: usize,
    coord: &BenchmarkCoordination,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = mmap_root(&args.base);
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root)?;
    let layout = MmapTransportLayout::new(root.clone(), mmap_segment(&args.base))?;
    layout.ensure_directories()?;

    let result = (|| {
        let mut producer = MmapProducer::<BenchmarkEvent<SIZE>>::create(
            layout.clone(),
            buffer_size,
            BenchmarkEvent::default,
        )?;
        if !producer.wait_for_consumers_ready(args.consumers as i64, COORD_TIMEOUT) {
            return Err::<(), Box<dyn std::error::Error>>(
                "timeout waiting for mmap broadcast consumers".into(),
            );
        }
        if !coord.wait_for_consumers(args.consumers, COORD_TIMEOUT) {
            return Err::<(), Box<dyn std::error::Error>>(
                "timeout waiting for mmap coordination consumers".into(),
            );
        }

        for i in 0..args.warmup {
            producer.publish(|slot| write_event(slot, i, 0, 0));
        }

        let total_start = Instant::now();
        let publish_duration = publish_with_optional_rate::<SIZE, _>(
            args,
            |sequence, timestamp_ns, intended_send_time_ns| {
                producer.publish(|slot| {
                    write_event(slot, sequence, timestamp_ns, intended_send_time_ns)
                });
            },
        );
        coord.signal_producer_done(args.num_messages as i64);

        if !coord.wait_for_consumers_done(args.consumers, COORD_TIMEOUT) {
            return Err::<(), Box<dyn std::error::Error>>(
                "timeout waiting for mmap broadcast consumers to finish".into(),
            );
        }
        let total_duration = total_start.elapsed();
        let results = load_consumer_results(&args.base, args.consumers)?;
        let verification_passed = results.iter().all(|r| r.verification_passed);
        let latency_stats = worst_consumer_latency(&results)?;

        let _ = producer.wait_until_consumed_with_strategy(
            (args.warmup + args.num_messages - 1) as i64,
            COORD_TIMEOUT,
            AutoWaitStrategy::BusySpin,
        );

        emit_result(
            args,
            adapter,
            buffer_size,
            publish_duration,
            total_duration,
            latency_stats,
            verification_passed,
        )
    })();

    let _ = fs::remove_dir_all(&root);
    result
}

fn consumer<const SIZE: usize>(
    args: Args,
    adapter: AdapterKind,
) -> Result<(), Box<dyn std::error::Error>> {
    let consumer_id = args.consumer_id.ok_or("--consumer-id is required")?;
    let result_path = args
        .result_path
        .clone()
        .ok_or("--result-path is required")?;
    let buffer_size = args.buffer_size.unwrap_or_else(|| {
        effective_buffer_size(args.message_size, args.consumers, args.num_messages)
    });
    let coord = BenchmarkCoordination::attach_with_timeout(
        &coordination_prefix(&args.base),
        COORD_TIMEOUT,
    )?;

    let stats = if adapter.is_shm() {
        consume_shm::<SIZE>(&args, adapter, buffer_size, consumer_id, &coord)?
    } else {
        consume_mmap::<SIZE>(&args, buffer_size, consumer_id, &coord)?
    };

    fs::write(result_path, serde_json::to_string(&stats)?)?;
    coord.signal_consumer_done(stats.consumed as i64);
    Ok(())
}

fn consume_shm<const SIZE: usize>(
    args: &Args,
    adapter: AdapterKind,
    buffer_size: usize,
    consumer_id: usize,
    coord: &BenchmarkCoordination,
) -> Result<ConsumerResult, Box<dyn std::error::Error>> {
    let consumer_name = format!("{CONSUMER_PREFIX}_{consumer_id}");
    let segment = shm_segment_name(&args.base);
    let mut consumer = attach_shm_consumer::<SIZE>(adapter, &segment, buffer_size, &consumer_name)?;
    coord.signal_consumer_ready();
    debug_log!("shm consumer {consumer_id}: attached segment={segment} name={consumer_name}");

    let mut warmup_seen = 0u64;
    while warmup_seen < args.warmup {
        if let Some(_lease) = consumer.try_consume_next_leased() {
            warmup_seen += 1;
        } else {
            apply_wait_strategy(&args.wait_strategy);
        }
    }
    debug_log!(
        "shm consumer {consumer_id}: warmup complete ({})",
        args.warmup
    );

    let mut recorder = LatencyRecorder::default_range();
    let start = Instant::now();
    let mut consumed = 0u64;
    while consumed < args.num_messages {
        if let Some(event) = consumer.try_consume_next_leased() {
            let now = nanos_now();
            if args.target_rate.is_some() {
                recorder.record(now.saturating_sub(event.intended_send_time_ns));
            } else {
                recorder.record(now.saturating_sub(event.timestamp_ns));
            }
            consumed += 1;
        } else if coord.is_producer_done() && coord.events_produced() as u64 <= consumed {
            break;
        } else {
            apply_wait_strategy(&args.wait_strategy);
        }
    }
    let elapsed = start.elapsed();
    debug_log!("shm consumer {consumer_id}: measured complete ({consumed})");
    let latency_stats = recorder
        .stats()
        .ok_or("broadcast consumer recorded no latency samples")?;

    Ok(ConsumerResult {
        consumer_id,
        consumed,
        duration_secs: elapsed.as_secs_f64(),
        latency_stats: LatencyStatsOut::from(&latency_stats),
        verification_passed: consumed == args.num_messages,
    })
}

fn consume_mmap<const SIZE: usize>(
    args: &Args,
    buffer_size: usize,
    consumer_id: usize,
    coord: &BenchmarkCoordination,
) -> Result<ConsumerResult, Box<dyn std::error::Error>> {
    let root = mmap_root(&args.base);
    let layout = MmapTransportLayout::new(root, mmap_segment(&args.base))?;
    let consumer_name = format!("{CONSUMER_PREFIX}_{consumer_id}");
    let deadline = Instant::now() + ATTACH_TIMEOUT;
    let mut consumer = loop {
        match MmapConsumer::<BenchmarkEvent<SIZE>>::attach(
            layout.clone(),
            buffer_size,
            &consumer_name,
        ) {
            Ok(consumer) => break consumer,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(error) => return Err(format!("attach failed for {consumer_name}: {error}").into()),
        }
    };
    coord.signal_consumer_ready();

    let mut warmup_seen = 0u64;
    while warmup_seen < args.warmup {
        if let Some(_lease) = consumer.try_consume_next_leased() {
            warmup_seen += 1;
        } else {
            apply_wait_strategy(&args.wait_strategy);
        }
    }

    let mut recorder = LatencyRecorder::default_range();
    let start = Instant::now();
    let mut consumed = 0u64;
    while consumed < args.num_messages {
        if let Some(event) = consumer.try_consume_next_leased() {
            let now = nanos_now();
            if args.target_rate.is_some() {
                recorder.record(now.saturating_sub(event.intended_send_time_ns));
            } else {
                recorder.record(now.saturating_sub(event.timestamp_ns));
            }
            consumed += 1;
        } else if coord.is_producer_done() && coord.events_produced() as u64 <= consumed {
            break;
        } else {
            apply_wait_strategy(&args.wait_strategy);
        }
    }
    let elapsed = start.elapsed();
    let latency_stats = recorder
        .stats()
        .ok_or("broadcast consumer recorded no latency samples")?;

    Ok(ConsumerResult {
        consumer_id,
        consumed,
        duration_secs: elapsed.as_secs_f64(),
        latency_stats: LatencyStatsOut::from(&latency_stats),
        verification_passed: consumed == args.num_messages,
    })
}

macro_rules! dispatch_by_size {
    ($size:expr, $f:ident, $args:expr, $adapter:expr) => {
        match $size {
            32 => $f::<32>($args, $adapter),
            64 => $f::<64>($args, $adapter),
            128 => $f::<128>($args, $adapter),
            512 => $f::<512>($args, $adapter),
            1024 => $f::<1024>($args, $adapter),
            2048 => $f::<2048>($args, $adapter),
            4096 => $f::<4096>($args, $adapter),
            16384 => $f::<16384>($args, $adapter),
            32768 => $f::<32768>($args, $adapter),
            65536 => $f::<65536>($args, $adapter),
            131072 => $f::<131072>($args, $adapter),
            524288 => $f::<524288>($args, $adapter),
            1048576 => $f::<1048576>($args, $adapter),
            2097152 => $f::<2097152>($args, $adapter),
            8388608 => $f::<8388608>($args, $adapter),
            16777216 => $f::<16777216>($args, $adapter),
            33554432 => $f::<33554432>($args, $adapter),
            67108864 => $f::<67108864>($args, $adapter),
            other => Err(format!("unsupported broadcast message size: {other}").into()),
        }
    };
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let adapter = AdapterKind::parse(&args.adapter);

    if args.consumers == 0 {
        return Err("--consumers must be greater than zero".into());
    }
    if args.mode == "consumer" && args.result_path.is_none() {
        return Err("--result-path is required in consumer mode".into());
    }

    if args.mode == "controller" {
        run_with_large_stack_if_needed(
            args.message_size,
            "internal-broadcast-controller",
            move || dispatch_by_size!(args.message_size, controller, args, adapter),
        )
    } else {
        run_with_large_stack_if_needed(
            args.message_size,
            "internal-broadcast-consumer",
            move || dispatch_by_size!(args.message_size, consumer, args, adapter),
        )
    }
}
