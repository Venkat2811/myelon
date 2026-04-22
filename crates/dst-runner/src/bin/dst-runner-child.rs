use disruptor_mp::{
    attach_shared_consumer, build_shared_single_producer, MmapConsumer, MmapProducer,
    MmapTransportLayout, RequiredConsumerLivenessConfig,
};
use dst_fixtures::dst_contract::ProcessRole;
use dst_runner::{payload_bytes, stable_payload_hash, BackendKind, ChildReport, OracleMessage};
use serde_json::to_string;
use std::env;
use std::fmt::Display;
use std::fs;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

const RAW_EVENT_CAPACITY: usize = 1024;

#[repr(C)]
#[derive(Clone, Copy)]
struct RawRingEvent {
    sequence: u64,
    payload_hash: u64,
    payload_len: u32,
    _reserved: u32,
    payload: [u8; RAW_EVENT_CAPACITY],
}

impl Default for RawRingEvent {
    fn default() -> Self {
        Self {
            sequence: 0,
            payload_hash: 0,
            payload_len: 0,
            _reserved: 0,
            payload: [0; RAW_EVENT_CAPACITY],
        }
    }
}

fn main() {
    if env::var("DST_CHILD_TRANSPORT").as_deref() != Ok("raw_ring") {
        panic!("DST child transport must be raw_ring for the initial RFC 0017 slice");
    }

    let mode = env::var("DST_CHILD_MODE").expect("DST_CHILD_MODE should be set");
    let backend = parse_backend();

    let report = match (backend, mode.as_str()) {
        (BackendKind::Shm, "producer") => run_shm_producer(),
        (BackendKind::Shm, "consumer") => run_shm_consumer(),
        (BackendKind::Mmap, "producer") => run_mmap_producer(),
        (BackendKind::Mmap, "consumer") => run_mmap_consumer(),
        _ => panic!("unsupported backend/mode combination: {backend:?}/{mode}"),
    };

    let report_path = env::var("DST_REPORT_PATH").expect("DST_REPORT_PATH should be set");
    fs::write(
        &report_path,
        to_string(&report).expect("child report should serialize"),
    )
    .expect("child report should write");
    println!(
        "DST_CHILD_OK role={:?} messages={} report={}",
        report.role,
        report.messages.len(),
        report_path
    );
}

fn write_checkpoint(report: &ChildReport) {
    let report_path = env::var("DST_CHECKPOINT_PATH")
        .or_else(|_| env::var("DST_REPORT_PATH"))
        .expect("DST_CHECKPOINT_PATH or DST_REPORT_PATH should be set");
    fs::write(
        &report_path,
        to_string(report).expect("child report should serialize"),
    )
    .expect("child report should write");
}

fn producer_completed() -> bool {
    let report_path =
        env::var("DST_PRODUCER_REPORT_PATH").expect("DST_PRODUCER_REPORT_PATH should be set");
    PathBuf::from(report_path).exists()
}

fn parse_env<T>(name: &str) -> T
where
    T: std::str::FromStr,
    T::Err: Display,
{
    let raw = env::var(name).unwrap_or_else(|_| panic!("missing env var: {name}"));
    raw.parse::<T>()
        .unwrap_or_else(|err| panic!("invalid env var {name}='{raw}': {err}"))
}

fn parse_env_or<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr,
    T::Err: Display,
{
    match env::var(name) {
        Ok(raw) => raw
            .parse::<T>()
            .unwrap_or_else(|err| panic!("invalid env var {name}='{raw}': {err}")),
        Err(_) => default,
    }
}

fn required_consumer_liveness_config() -> Option<RequiredConsumerLivenessConfig> {
    let raw_ids = env::var("DST_REQUIRED_CONSUMER_IDS").ok()?;
    let required_consumer_ids = raw_ids
        .split(',')
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if required_consumer_ids.is_empty() {
        return None;
    }

    Some(
        RequiredConsumerLivenessConfig::new(required_consumer_ids)
            .with_startup_wait_timeout(Duration::from_millis(parse_env_or(
                "DST_REQUIRED_STARTUP_WAIT_MS",
                100u64,
            )))
            .with_progress_timeout(Duration::from_millis(parse_env_or(
                "DST_REQUIRED_PROGRESS_TIMEOUT_MS",
                20u64,
            )))
            .with_progress_check_interval(Duration::from_millis(parse_env_or(
                "DST_REQUIRED_PROGRESS_CHECK_INTERVAL_MS",
                1u64,
            )))
            .with_shutdown_grace_period(Duration::from_millis(parse_env_or(
                "DST_REQUIRED_SHUTDOWN_GRACE_MS",
                200u64,
            ))),
    )
}

fn parse_backend() -> BackendKind {
    match env::var("DST_CHILD_BACKEND")
        .expect("DST_CHILD_BACKEND should be set")
        .as_str()
    {
        "shm" => BackendKind::Shm,
        "mmap" => BackendKind::Mmap,
        other => panic!("unsupported backend: {other}"),
    }
}

fn make_event(seed: u64, sequence: u64, payload_len: usize) -> (RawRingEvent, OracleMessage) {
    assert!(
        payload_len <= RAW_EVENT_CAPACITY,
        "payload_len {payload_len} exceeds RAW_EVENT_CAPACITY {RAW_EVENT_CAPACITY}"
    );
    let payload = payload_bytes(seed, sequence, payload_len);
    let hash = stable_payload_hash(&payload);
    let mut event = RawRingEvent {
        sequence,
        payload_hash: hash,
        payload_len: payload_len as u32,
        _reserved: 0,
        payload: [0; RAW_EVENT_CAPACITY],
    };
    event.payload[..payload_len].copy_from_slice(&payload);

    let oracle = OracleMessage {
        sequence,
        payload_hash: hash,
        payload_len,
        timestamp_ns: sequence,
    };

    (event, oracle)
}

fn validate_event(
    seed: u64,
    event: &RawRingEvent,
    allow_corruption_validation: bool,
) -> OracleMessage {
    let payload_len = event.payload_len as usize;
    let expected = payload_bytes(seed, event.sequence, payload_len);
    let actual = &event.payload[..payload_len];
    let actual_hash = stable_payload_hash(actual);
    if !allow_corruption_validation {
        assert_eq!(
            actual_hash, event.payload_hash,
            "event payload hash field mismatch for sequence {}",
            event.sequence
        );
        assert_eq!(
            actual,
            expected.as_slice(),
            "event payload bytes mismatch for sequence {}",
            event.sequence
        );
    }

    OracleMessage {
        sequence: event.sequence,
        payload_hash: actual_hash,
        payload_len,
        timestamp_ns: event.sequence,
    }
}

fn run_shm_producer() -> ChildReport {
    let segment = env::var("DST_SEGMENT").expect("DST_SEGMENT should be set");
    let ring_depth: usize = parse_env("DST_RING_DEPTH");
    let message_count: u64 =
        parse_env_or("DST_PRODUCER_MESSAGE_COUNT", parse_env("DST_MESSAGE_COUNT"));
    let sequence_start: u64 = parse_env_or("DST_SEQUENCE_START", 0u64);
    let payload_size: usize = parse_env("DST_PAYLOAD_SIZE");
    let seed: u64 = parse_env("DST_SEED");
    let consumer_count: usize = parse_env("DST_CONSUMER_COUNT");
    let wait_for_consumers_ready = env::var("DST_WAIT_FOR_CONSUMERS_READY").as_deref() == Ok("1");
    let consumer_prefix =
        env::var("DST_CONSUMER_PREFIX").expect("DST_CONSUMER_PREFIX should be set");
    let pause_every: usize = parse_env("DST_PUBLISH_PAUSE_EVERY");
    let pause_micros: u64 = parse_env("DST_PUBLISH_PAUSE_MICROS");
    let post_publish_hold_ms: u64 = parse_env("DST_POST_PUBLISH_HOLD_MS");
    let corrupt_at_sequence: Option<u64> = env::var("DST_CORRUPT_AT_SEQUENCE").ok().map(|raw| {
        raw.parse::<u64>()
            .expect("DST_CORRUPT_AT_SEQUENCE should parse")
    });

    let mut builder = build_shared_single_producer::<RawRingEvent>(&segment, ring_depth);
    if wait_for_consumers_ready {
        builder = builder
            .discover_consumer_with_prefix_and_interval(
                consumer_count,
                &consumer_prefix,
                Duration::from_millis(10),
            )
            .wait_for_consumers(consumer_count as i64, Duration::from_secs(15));
    }
    let mut producer = builder
        .build_producer(RawRingEvent::default)
        .expect("shared producer should build");
    if let Some(config) = required_consumer_liveness_config() {
        producer.enable_required_consumer_liveness(config);
    }

    let mut messages = Vec::with_capacity(message_count as usize);
    let mut checksum_total = 0u64;

    for sequence in sequence_start..sequence_start + message_count {
        let (mut event, oracle) = make_event(seed, sequence, payload_size);
        if corrupt_at_sequence == Some(sequence) && payload_size > 0 {
            event.payload[0] ^= 0x5a;
        }
        if env::var("DST_REQUIRED_CONSUMER_IDS").is_ok() {
            producer
                .publish_managed(|slot| *slot = event)
                .expect("managed shared publish should succeed");
        } else {
            producer.publish(|slot| *slot = event);
        }
        checksum_total = checksum_total.wrapping_add(oracle.payload_hash);
        messages.push(oracle);
        write_checkpoint(&ChildReport {
            role: ProcessRole::Producer,
            messages: messages.clone(),
            checksum_total,
            backpressure_events: 0,
            attached_after_ms: 0,
        });
        if pause_every > 0 && (sequence as usize + 1).is_multiple_of(pause_every) {
            thread::sleep(Duration::from_micros(pause_micros));
        }
    }

    if post_publish_hold_ms > 0 {
        thread::sleep(Duration::from_millis(post_publish_hold_ms));
    }

    let report = ChildReport {
        role: ProcessRole::Producer,
        messages,
        checksum_total,
        backpressure_events: 0,
        attached_after_ms: 0,
    };
    write_checkpoint(&report);
    report
}

fn run_shm_consumer() -> ChildReport {
    let segment = env::var("DST_SEGMENT").expect("DST_SEGMENT should be set");
    let ring_depth: usize = parse_env("DST_RING_DEPTH");
    let message_count: u64 =
        parse_env_or("DST_CONSUMER_MESSAGE_COUNT", parse_env("DST_MESSAGE_COUNT"));
    let seed: u64 = parse_env("DST_SEED");
    let index: usize = parse_env("DST_CONSUMER_INDEX");
    let consumer_id = env::var("DST_CONSUMER_ID").expect("DST_CONSUMER_ID should be set");
    let allow_corruption_validation =
        env::var("DST_ALLOW_CORRUPTION_VALIDATION").as_deref() == Ok("1");

    let start = Instant::now();
    let attach_deadline = start + Duration::from_secs(15);
    let mut consumer = loop {
        let builder = attach_shared_consumer::<RawRingEvent>(&segment, ring_depth)
            .with_consumer_id(&consumer_id);
        match builder.build_consumer() {
            Ok(consumer) => break consumer,
            Err(_) if Instant::now() < attach_deadline => thread::sleep(Duration::from_millis(20)),
            Err(err) => panic!("consumer attach failed: {err}"),
        }
    };
    let attached_after_ms = start.elapsed().as_millis() as u64;

    let mut messages = Vec::with_capacity(message_count as usize);
    let mut checksum_total = 0u64;
    let mut previous = None::<u64>;
    let consume_deadline = Instant::now() + Duration::from_secs(20);
    let mut producer_done_at = None::<Instant>;

    while messages.len() < message_count as usize {
        if let Some((sequence, event)) = consumer.try_consume_next() {
            let oracle = validate_event(seed, &event, allow_corruption_validation);
            if let Some(prev) = previous {
                assert_eq!(
                    sequence as u64,
                    prev + 1,
                    "sequence gap for consumer {consumer_id}"
                );
            }
            previous = Some(sequence as u64);
            checksum_total = checksum_total.wrapping_add(oracle.payload_hash);
            messages.push(oracle);
            producer_done_at = None;
            write_checkpoint(&ChildReport {
                role: ProcessRole::Consumer {
                    index: index as u32,
                },
                messages: messages.clone(),
                checksum_total,
                backpressure_events: 0,
                attached_after_ms,
            });
            continue;
        }

        if producer_completed() {
            let done_at = producer_done_at.get_or_insert_with(Instant::now);
            if done_at.elapsed() >= Duration::from_millis(250) {
                break;
            }
        }

        assert!(
            Instant::now() < consume_deadline,
            "consumer timed out after consuming {} messages",
            messages.len()
        );
        thread::sleep(Duration::from_millis(1));
    }

    let report = ChildReport {
        role: ProcessRole::Consumer {
            index: index as u32,
        },
        messages,
        checksum_total,
        backpressure_events: 0,
        attached_after_ms,
    };
    write_checkpoint(&report);
    report
}

fn run_mmap_producer() -> ChildReport {
    let ring_depth: usize = parse_env("DST_RING_DEPTH");
    let message_count: u64 =
        parse_env_or("DST_PRODUCER_MESSAGE_COUNT", parse_env("DST_MESSAGE_COUNT"));
    let sequence_start: u64 = parse_env_or("DST_SEQUENCE_START", 0u64);
    let payload_size: usize = parse_env("DST_PAYLOAD_SIZE");
    let seed: u64 = parse_env("DST_SEED");
    let consumer_count: usize = parse_env("DST_CONSUMER_COUNT");
    let wait_for_consumers_ready = env::var("DST_WAIT_FOR_CONSUMERS_READY").as_deref() == Ok("1");
    let pause_every: usize = parse_env("DST_PUBLISH_PAUSE_EVERY");
    let pause_micros: u64 = parse_env("DST_PUBLISH_PAUSE_MICROS");
    let post_publish_hold_ms: u64 = parse_env("DST_POST_PUBLISH_HOLD_MS");
    let corrupt_at_sequence: Option<u64> = env::var("DST_CORRUPT_AT_SEQUENCE").ok().map(|raw| {
        raw.parse::<u64>()
            .expect("DST_CORRUPT_AT_SEQUENCE should parse")
    });

    let mut producer =
        MmapProducer::<RawRingEvent>::create(child_layout(), ring_depth, RawRingEvent::default)
            .expect("mmap producer should build");
    if let Some(config) = required_consumer_liveness_config() {
        producer.enable_required_consumer_liveness(config);
    }
    if wait_for_consumers_ready {
        assert!(
            producer.wait_for_consumers_ready(consumer_count as i64, Duration::from_secs(15)),
            "mmap producer timed out waiting for {consumer_count} consumers"
        );
    }

    let mut messages = Vec::with_capacity(message_count as usize);
    let mut checksum_total = 0u64;

    for sequence in sequence_start..sequence_start + message_count {
        let (mut event, oracle) = make_event(seed, sequence, payload_size);
        if corrupt_at_sequence == Some(sequence) && payload_size > 0 {
            event.payload[0] ^= 0x5a;
        }
        if env::var("DST_REQUIRED_CONSUMER_IDS").is_ok() {
            producer
                .publish_managed(|slot| *slot = event)
                .expect("managed mmap publish should succeed");
        } else {
            producer.publish(|slot| *slot = event);
        }
        checksum_total = checksum_total.wrapping_add(oracle.payload_hash);
        messages.push(oracle);
        write_checkpoint(&ChildReport {
            role: ProcessRole::Producer,
            messages: messages.clone(),
            checksum_total,
            backpressure_events: 0,
            attached_after_ms: 0,
        });
        if pause_every > 0 && (sequence as usize + 1).is_multiple_of(pause_every) {
            thread::sleep(Duration::from_micros(pause_micros));
        }
    }

    if post_publish_hold_ms > 0 {
        thread::sleep(Duration::from_millis(post_publish_hold_ms));
    }

    let report = ChildReport {
        role: ProcessRole::Producer,
        messages,
        checksum_total,
        backpressure_events: 0,
        attached_after_ms: 0,
    };
    write_checkpoint(&report);
    report
}

fn run_mmap_consumer() -> ChildReport {
    let ring_depth: usize = parse_env("DST_RING_DEPTH");
    let message_count: u64 =
        parse_env_or("DST_CONSUMER_MESSAGE_COUNT", parse_env("DST_MESSAGE_COUNT"));
    let seed: u64 = parse_env("DST_SEED");
    let index: usize = parse_env("DST_CONSUMER_INDEX");
    let consumer_id = env::var("DST_CONSUMER_ID").expect("DST_CONSUMER_ID should be set");
    let allow_corruption_validation =
        env::var("DST_ALLOW_CORRUPTION_VALIDATION").as_deref() == Ok("1");

    let start = Instant::now();
    let attach_deadline = start + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapConsumer::<RawRingEvent>::attach(child_layout(), ring_depth, &consumer_id) {
            Ok(consumer) => break consumer,
            Err(_) if Instant::now() < attach_deadline => thread::sleep(Duration::from_millis(20)),
            Err(err) => panic!("mmap consumer attach failed: {err}"),
        }
    };
    let attached_after_ms = start.elapsed().as_millis() as u64;

    let mut messages = Vec::with_capacity(message_count as usize);
    let mut checksum_total = 0u64;
    let mut previous = None::<u64>;
    let consume_deadline = Instant::now() + Duration::from_secs(20);
    let mut producer_done_at = None::<Instant>;

    while messages.len() < message_count as usize {
        if let Some((sequence, event)) = consumer.try_consume_next() {
            let oracle = validate_event(seed, &event, allow_corruption_validation);
            if let Some(prev) = previous {
                assert_eq!(
                    sequence as u64,
                    prev + 1,
                    "sequence gap for consumer {consumer_id}"
                );
            }
            previous = Some(sequence as u64);
            checksum_total = checksum_total.wrapping_add(oracle.payload_hash);
            messages.push(oracle);
            producer_done_at = None;
            write_checkpoint(&ChildReport {
                role: ProcessRole::Consumer {
                    index: index as u32,
                },
                messages: messages.clone(),
                checksum_total,
                backpressure_events: 0,
                attached_after_ms,
            });
            continue;
        }

        if producer_completed() {
            let done_at = producer_done_at.get_or_insert_with(Instant::now);
            if done_at.elapsed() >= Duration::from_millis(250) {
                break;
            }
        }

        assert!(
            Instant::now() < consume_deadline,
            "mmap consumer timed out after consuming {} messages",
            messages.len()
        );
        thread::sleep(Duration::from_millis(1));
    }

    let report = ChildReport {
        role: ProcessRole::Consumer {
            index: index as u32,
        },
        messages,
        checksum_total,
        backpressure_events: 0,
        attached_after_ms,
    };
    write_checkpoint(&report);
    report
}

fn child_layout() -> MmapTransportLayout {
    let root = env::var("DST_RUN_ROOT").expect("DST_RUN_ROOT should be set");
    let segment = env::var("DST_SEGMENT").expect("DST_SEGMENT should be set");
    MmapTransportLayout::new(PathBuf::from(root), segment).expect("mmap layout should be valid")
}
