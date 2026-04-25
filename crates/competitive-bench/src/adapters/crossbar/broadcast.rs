use clap::Parser;
use crate::infra::result_json::{BenchmarkConfigOut, BenchmarkResultsOut, LatencyStatsOut};
use crossbar::{error::Error as CrossbarError, Config, Publisher, Subscriber, WaitStrategy};
use perf_bench::infra::coordination::BenchmarkCoordination;
use perf_bench::infra::latency::LatencyRecorder;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const COORD_TIMEOUT: Duration = Duration::from_secs(120);
const TOPIC_URI: &str = "/bench/broadcast";
const HEADER_BYTES: usize = 24;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "crossbar pubsub broadcast benchmark")]
pub struct Args {
    #[arg(long, value_parser = ["controller", "consumer"], default_value = "controller")]
    pub mode: String,

    #[arg(long, default_value_t = default_base_name())]
    pub base: String,

    #[arg(long, short = 's', default_value_t = 64)]
    pub message_size: usize,

    #[arg(long, default_value_t = 10_000)]
    pub warmup: u64,

    #[arg(long, short = 'n', default_value_t = 100_000)]
    pub num_messages: u64,

    #[arg(long, default_value_t = 4)]
    pub consumers: usize,

    #[arg(long, default_value = "busyspin")]
    pub wait_strategy: String,

    #[arg(long)]
    pub target_rate: Option<u64>,

    #[arg(long)]
    pub json: bool,

    #[arg(long)]
    pub ring_size: Option<usize>,

    #[arg(long, hide = true)]
    pub consumer_id: Option<usize>,

    #[arg(long, hide = true)]
    pub result_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConsumerResult {
    consumer_id: usize,
    consumed: u64,
    duration_secs: f64,
    latency_stats: LatencyStatsOut,
    verification_passed: bool,
}

pub fn parse_args() -> Args {
    Args::parse()
}

pub fn validate_args(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    if args.message_size < HEADER_BYTES {
        return Err(format!(
            "message_size {} is too small; need at least {HEADER_BYTES} bytes",
            args.message_size
        )
        .into());
    }
    if args.consumers == 0 {
        return Err("consumers must be >= 1".into());
    }
    if let Some(target_rate) = args.target_rate {
        if target_rate == 0 {
            return Err("target-rate must be > 0".into());
        }
    }
    if let Some(ring_size) = args.ring_size {
        if ring_size < 2 || !ring_size.is_power_of_two() {
            return Err(format!("ring-size must be a power of two >= 2, got {ring_size}").into());
        }
    }
    Ok(())
}

fn default_base_name() -> String {
    format!("cbbcast_{}", std::process::id())
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
    stable_short_name(base, "cb")
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

fn consumer_result_path(base: &str, consumer_id: usize) -> PathBuf {
    std::env::temp_dir().join(format!(
        "{base}_crossbar_broadcast_consumer_{consumer_id}.json"
    ))
}

fn nanos_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn parse_wait_strategy(raw: &str) -> WaitStrategy {
    match raw.to_ascii_lowercase().as_str() {
        "busyspin" | "busy_spin" => WaitStrategy::BusySpin,
        "yieldspin" | "yield_spin" => WaitStrategy::YieldSpin,
        "backoffspin" | "backoff_spin" => WaitStrategy::BackoffSpin,
        _ => WaitStrategy::default(),
    }
}

fn block_count_for(_message_size: usize, total_messages: u64, consumers: usize) -> u32 {
    let floor = consumers.saturating_mul(512).max(128);
    let desired = total_messages as usize;
    desired.max(floor).min(262_144) as u32
}

fn ring_depth_for(total_messages: u64, consumers: usize) -> u32 {
    let needed = (total_messages as usize)
        .max(consumers.saturating_mul(32))
        .max(8);
    needed.next_power_of_two().min(262_144) as u32
}

fn block_size_for(message_size: usize) -> u32 {
    let total = message_size.saturating_add(8);
    total as u32
}

fn publisher_config(args: &Args) -> Config {
    let total_messages = args.warmup.saturating_add(args.num_messages);
    Config {
        max_topics: 1,
        block_count: block_count_for(args.message_size, total_messages, args.consumers),
        block_size: block_size_for(args.message_size),
        ring_depth: args
            .ring_size
            .map(|ring| ring as u32)
            .unwrap_or_else(|| ring_depth_for(total_messages, args.consumers)),
        heartbeat_interval: Duration::from_millis(50),
        stale_timeout: Duration::from_secs(30),
    }
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

fn spawn_consumer(
    exe: &Path,
    args: &Args,
    consumer_id: usize,
    result_path: &Path,
) -> Result<std::process::Child, Box<dyn std::error::Error>> {
    let mut cmd = Command::new(exe);
    cmd.arg("--mode")
        .arg("consumer")
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
    if let Some(ring_size) = args.ring_size {
        cmd.arg("--ring-size").arg(ring_size.to_string());
    }

    Ok(cmd.spawn()?)
}

fn emit_result(
    args: &Args,
    config: Config,
    publish_duration: Duration,
    total_duration: Duration,
    latency_stats: LatencyStatsOut,
    verification_passed: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = BenchmarkResultsOut {
        adapter: "crossbar-pubsub".to_string(),
        family: "broadcast".to_string(),
        config: BenchmarkConfigOut {
            message_size: args.message_size,
            num_messages: args.num_messages,
            warmup_messages: args.warmup,
            buffer_size: config.ring_depth as usize,
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

fn encode_header(buf: &mut [u8], sequence: u64, timestamp_ns: u64, intended_send_time_ns: u64) {
    buf[..8].copy_from_slice(&sequence.to_le_bytes());
    buf[8..16].copy_from_slice(&timestamp_ns.to_le_bytes());
    buf[16..24].copy_from_slice(&intended_send_time_ns.to_le_bytes());
}

fn decode_u64(buf: &[u8], offset: usize) -> u64 {
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&buf[offset..offset + 8]);
    u64::from_le_bytes(bytes)
}

fn publish_retry(
    publisher: &mut Publisher,
    topic: &crossbar::Topic,
    payload: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        match publisher.publish(topic, payload) {
            Ok(()) => return Ok(()),
            Err(CrossbarError::PoolExhausted) => {
                let _ = publisher.heartbeat();
                std::hint::spin_loop();
            }
            Err(err) => return Err(err.into()),
        }
    }
}

fn wait_for_subscribers(
    publisher: &Publisher,
    topic: &crossbar::Topic,
    consumers: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    loop {
        let observed = publisher.subscriber_count(topic)? as usize;
        if observed >= consumers {
            return Ok(());
        }
        if start.elapsed() >= COORD_TIMEOUT {
            return Err(format!(
                "timed out waiting for {consumers} subscribers, observed {observed}"
            )
            .into());
        }
        std::hint::spin_loop();
    }
}

fn publish_with_optional_rate<F>(
    args: &Args,
    mut publish: F,
) -> Result<Duration, Box<dyn std::error::Error>>
where
    F: FnMut(u64, u64, u64) -> Result<(), Box<dyn std::error::Error>>,
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
            publish(i, nanos_now(), intended_target_ns)?;
        }
    } else {
        for i in 0..args.num_messages {
            publish(i, nanos_now(), 0)?;
        }
    }
    Ok(publish_start.elapsed())
}

pub fn controller(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let config = publisher_config(&args);
    let coord_prefix = coordination_prefix(&args.base);
    cleanup_coordination(&coord_prefix);
    let coord = BenchmarkCoordination::create(&coord_prefix)?;
    let exe = std::env::current_exe()?;
    let mut publisher = Publisher::create(&args.base, config)?;
    let topic = publisher.register(TOPIC_URI)?;

    let mut children = Vec::with_capacity(args.consumers);
    for consumer_id in 0..args.consumers {
        let path = consumer_result_path(&args.base, consumer_id);
        let _ = fs::remove_file(&path);
        children.push(spawn_consumer(&exe, &args, consumer_id, &path)?);
    }

    if !coord.wait_for_consumers(args.consumers, COORD_TIMEOUT) {
        return Err(format!(
            "timed out waiting for {} consumers to subscribe",
            args.consumers
        )
        .into());
    }
    wait_for_subscribers(&publisher, &topic, args.consumers)?;

    let mut payload = vec![0_u8; args.message_size];

    for warmup_seq in 0..args.warmup {
        encode_header(&mut payload, warmup_seq, nanos_now(), 0);
        publish_retry(&mut publisher, &topic, &payload)?;
    }

    let total_start = Instant::now();
    let publish_duration = publish_with_optional_rate(
        &args,
        |measured_idx, timestamp_ns, intended_send_time_ns| {
            let sequence = args.warmup.saturating_add(measured_idx);
            encode_header(&mut payload, sequence, timestamp_ns, intended_send_time_ns);
            publish_retry(&mut publisher, &topic, &payload)
        },
    )?;

    coord.signal_producer_done(args.num_messages as i64);

    if !coord.wait_for_consumers_done(args.consumers, COORD_TIMEOUT) {
        return Err(format!(
            "timed out waiting for {} consumers to finish",
            args.consumers
        )
        .into());
    }

    let total_duration = total_start.elapsed();
    let results = load_consumer_results(&args.base, args.consumers)?;
    let verification_passed = results.iter().all(|result| result.verification_passed);
    let latency_stats = worst_consumer_latency(&results)?;

    for mut child in children {
        let _ = child.wait();
    }

    emit_result(
        &args,
        config,
        publish_duration,
        total_duration,
        latency_stats,
        verification_passed,
    )
}

pub fn consumer(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let consumer_id = args.consumer_id.ok_or("missing consumer-id")?;
    let result_path = args.result_path.clone().ok_or("missing result-path")?;
    let coord = BenchmarkCoordination::attach_with_timeout(
        &coordination_prefix(&args.base),
        COORD_TIMEOUT,
    )?;
    let subscriber = Subscriber::connect(&args.base)?;
    let stream = subscriber.subscribe(TOPIC_URI)?;
    let wait_strategy = parse_wait_strategy(&args.wait_strategy);

    coord.signal_consumer_ready();

    let total_expected = args.warmup.saturating_add(args.num_messages);
    let mut total_seen = 0_u64;
    let mut measured_seen = 0_u64;
    let mut expected_sequence = 0_u64;
    let mut verification_passed = true;
    let mut recorder = LatencyRecorder::default_range();
    let mut measurement_start: Option<Instant> = None;
    let mut measurement_end: Option<Instant> = None;

    while total_seen < total_expected {
        let sample = stream.recv_with(wait_strategy)?;
        if sample.len() < HEADER_BYTES {
            verification_passed = false;
            continue;
        }

        let sequence = decode_u64(&sample, 0);
        let timestamp_ns = decode_u64(&sample, 8);
        let intended_send_time_ns = decode_u64(&sample, 16);

        if sequence != expected_sequence {
            verification_passed = false;
            expected_sequence = sequence.saturating_add(1);
        } else {
            expected_sequence = expected_sequence.saturating_add(1);
        }

        total_seen = total_seen.saturating_add(1);

        if sequence >= args.warmup {
            if measurement_start.is_none() {
                measurement_start = Some(Instant::now());
            }
            let now = nanos_now();
            if intended_send_time_ns != 0 {
                recorder.record(now.saturating_sub(intended_send_time_ns));
            } else {
                recorder.record(now.saturating_sub(timestamp_ns));
            }
            measurement_end = Some(Instant::now());
            measured_seen = measured_seen.saturating_add(1);
        }
    }

    let duration_secs = match (measurement_start, measurement_end) {
        (Some(start), Some(end)) => end.saturating_duration_since(start).as_secs_f64(),
        _ => 0.0,
    };

    let stats = recorder
        .stats()
        .map(|stats| LatencyStatsOut::from(&stats))
        .ok_or("missing latency stats")?;

    let result = ConsumerResult {
        consumer_id,
        consumed: measured_seen,
        duration_secs,
        latency_stats: stats,
        verification_passed,
    };

    fs::write(result_path, serde_json::to_vec(&result)?)?;
    coord.signal_consumer_done(measured_seen as i64);
    Ok(())
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();
    validate_args(&args)?;
    if args.mode == "controller" {
        controller(args)
    } else {
        consumer(args)
    }
}
