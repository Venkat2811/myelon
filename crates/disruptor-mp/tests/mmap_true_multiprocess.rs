use disruptor_mp::{AutoWaitStrategy, MmapConsumer, MmapProducer, MmapTransportLayout};
use std::env;
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::str::FromStr;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CHILD_TEST_NAME: &str = "mmap_mp_child_entry";

#[repr(C)]
#[derive(Clone, Copy)]
struct TestEvent {
    sequence: u64,
    checksum: u64,
    payload: [u8; 112],
}

impl Default for TestEvent {
    fn default() -> Self {
        Self {
            sequence: 0,
            checksum: 0,
            payload: [0; 112],
        }
    }
}

#[derive(Clone, Copy)]
struct CaseSpec {
    buffer_size: usize,
    events: u64,
    consumers: usize,
    slow_every: usize,
    slow_micros: u64,
    label: &'static str,
}

fn event_checksum(sequence: u64) -> u64 {
    sequence.wrapping_mul(0x9E37_79B1_85EB_CA87).rotate_left(17)
}

fn unique_root(prefix: &str) -> PathBuf {
    let timestamp_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after UNIX_EPOCH")
        .as_nanos();
    env::temp_dir().join(format!("{prefix}_{}_{}", std::process::id(), timestamp_ns))
}

fn unique_segment(prefix: &str) -> String {
    let timestamp_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after UNIX_EPOCH")
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), timestamp_ns)
}

fn spawn_child(
    mode: &str,
    root: &Path,
    segment: &str,
    case: CaseSpec,
    consumer_id: Option<&str>,
    consumer_index: Option<usize>,
) -> Child {
    let current_exe = env::current_exe().expect("failed to resolve test binary");
    let mut cmd = Command::new(current_exe);
    cmd.arg("--exact")
        .arg(CHILD_TEST_NAME)
        .arg("--ignored")
        .arg("--nocapture")
        .env("MMAP_MP_CHILD_MODE", mode)
        .env("MMAP_MP_ROOT", root.display().to_string())
        .env("MMAP_MP_SEGMENT", segment)
        .env("MMAP_MP_BUFFER_SIZE", case.buffer_size.to_string())
        .env("MMAP_MP_EVENTS", case.events.to_string())
        .env("MMAP_MP_CONSUMERS", case.consumers.to_string())
        .env("MMAP_MP_SLOW_EVERY", case.slow_every.to_string())
        .env("MMAP_MP_SLOW_MICROS", case.slow_micros.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(id) = consumer_id {
        cmd.env("MMAP_MP_CONSUMER_ID", id);
    }
    if let Some(index) = consumer_index {
        cmd.env("MMAP_MP_CONSUMER_INDEX", index.to_string());
    }

    cmd.spawn().expect("failed to spawn child process")
}

fn wait_with_timeout(mut child: Child, timeout: Duration, role: &str) -> Output {
    let start = Instant::now();
    loop {
        if child
            .try_wait()
            .expect("failed to poll child process")
            .is_some()
        {
            return child
                .wait_with_output()
                .expect("failed to read child output");
        }

        if start.elapsed() > timeout {
            let _ = child.kill();
            let output = child
                .wait_with_output()
                .expect("failed to read timed-out child output");
            panic!(
                "{role} child timed out after {:?}\nstdout:\n{}\nstderr:\n{}",
                timeout,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }

        thread::sleep(Duration::from_millis(10));
    }
}

fn assert_child_success(output: &Output, role: &str, expected: &[&str]) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "{role} child failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        stdout,
        stderr,
    );

    for marker in expected {
        assert!(
            stdout.contains(marker),
            "{role} child output missing '{marker}'\nstdout:\n{}\nstderr:\n{}",
            stdout,
            stderr,
        );
    }
}

fn parse_env<T>(name: &str) -> T
where
    T: FromStr,
    T::Err: Display,
{
    let raw = env::var(name).unwrap_or_else(|_| panic!("missing env var: {name}"));
    raw.parse::<T>()
        .unwrap_or_else(|err| panic!("invalid {name} value '{raw}': {err}"))
}

fn child_layout() -> MmapTransportLayout {
    let root = env::var("MMAP_MP_ROOT").expect("MMAP_MP_ROOT should be set");
    let segment = env::var("MMAP_MP_SEGMENT").expect("MMAP_MP_SEGMENT should be set");
    MmapTransportLayout::new(PathBuf::from(root), segment).expect("child layout should be valid")
}

fn child_case() -> CaseSpec {
    CaseSpec {
        buffer_size: parse_env("MMAP_MP_BUFFER_SIZE"),
        events: parse_env("MMAP_MP_EVENTS"),
        consumers: parse_env("MMAP_MP_CONSUMERS"),
        slow_every: parse_env("MMAP_MP_SLOW_EVERY"),
        slow_micros: parse_env("MMAP_MP_SLOW_MICROS"),
        label: "child",
    }
}

fn run_child_producer() {
    let layout = child_layout();
    let case = child_case();
    let mut producer =
        MmapProducer::<TestEvent>::create(layout, case.buffer_size, TestEvent::default)
            .expect("mmap producer build failed");

    assert!(
        producer.wait_for_consumers_ready(case.consumers as i64, Duration::from_secs(15)),
        "consumers did not signal readiness in time"
    );

    for sequence in 0..case.events {
        producer.publish(|event| {
            event.sequence = sequence;
            event.checksum = event_checksum(sequence);
            for (index, slot) in event.payload.iter_mut().enumerate() {
                *slot = ((sequence + index as u64) & 0xFF) as u8;
            }
        });
    }

    let last_sequence = case.events as i64 - 1;
    assert!(
        producer.wait_until_consumed_with_strategy(
            last_sequence,
            Duration::from_secs(20),
            AutoWaitStrategy::BusySpin,
        ),
        "producer did not observe all consumers advancing to the final sequence"
    );
    assert_eq!(
        producer.get_consumer_count(),
        case.consumers,
        "producer did not discover the expected number of consumers"
    );

    println!(
        "MMAP_MP_CHILD_OK mode=producer published={} consumers={}",
        case.events, case.consumers
    );
}

fn run_child_consumer() {
    let layout = child_layout();
    let case = child_case();
    let consumer_id =
        env::var("MMAP_MP_CONSUMER_ID").expect("MMAP_MP_CONSUMER_ID should be set for consumer");
    let consumer_index: usize = parse_env("MMAP_MP_CONSUMER_INDEX");

    let attach_deadline = Instant::now() + Duration::from_secs(15);
    let mut consumer = loop {
        match MmapConsumer::<TestEvent>::attach(layout.clone(), case.buffer_size, &consumer_id) {
            Ok(consumer) => break consumer,
            Err(_) if Instant::now() < attach_deadline => thread::sleep(Duration::from_millis(25)),
            Err(err) => panic!("consumer attach failed: {err}"),
        }
    };

    let consume_deadline = Instant::now() + Duration::from_secs(30);
    let mut consumed = 0u64;
    let mut last_sequence = None::<u64>;

    while consumed < case.events {
        if let Some((sequence, event)) = consumer.try_consume_next() {
            let sequence = sequence as u64;
            if let Some(last) = last_sequence {
                assert_eq!(
                    sequence,
                    last + 1,
                    "sequence gap for consumer {consumer_id}"
                );
            }
            last_sequence = Some(sequence);
            assert_eq!(event.sequence, sequence, "event sequence mismatch");
            assert_eq!(
                event.checksum,
                event_checksum(sequence),
                "event checksum mismatch"
            );
            for (index, slot) in event.payload.iter().enumerate() {
                assert_eq!(
                    *slot,
                    ((sequence + index as u64) & 0xFF) as u8,
                    "payload mismatch at index {index}",
                );
            }
            consumed += 1;

            if case.slow_every > 0
                && consumer_index == case.consumers.saturating_sub(1)
                && consumed.checked_rem(case.slow_every as u64) == Some(0)
            {
                thread::sleep(Duration::from_micros(case.slow_micros));
            }
            continue;
        }

        if Instant::now() > consume_deadline {
            panic!(
                "consumer timeout: consumed={} expected={} id={}",
                consumed, case.events, consumer_id
            );
        }
        thread::sleep(Duration::from_micros(100));
    }

    println!(
        "MMAP_MP_CHILD_OK mode=consumer id={} consumed={}",
        consumer_id, consumed
    );
}

fn run_case(case: CaseSpec) {
    let root = unique_root(&format!("mmap_mp_{}", case.label));
    let segment = unique_segment(case.label);
    let mut consumers = Vec::with_capacity(case.consumers);

    for index in 0..case.consumers {
        let consumer_id = format!("MMC_{index}");
        let consumer = spawn_child(
            "consumer",
            &root,
            &segment,
            case,
            Some(&consumer_id),
            Some(index),
        );
        consumers.push((consumer_id, consumer));
        thread::sleep(Duration::from_millis(30));
    }

    thread::sleep(Duration::from_millis(100));

    let producer = spawn_child("producer", &root, &segment, case, None, None);
    let producer_output = wait_with_timeout(producer, Duration::from_secs(45), "producer");
    assert_child_success(
        &producer_output,
        "producer",
        &[
            "MMAP_MP_CHILD_OK",
            &format!("published={}", case.events),
            &format!("consumers={}", case.consumers),
        ],
    );

    for (index, (consumer_id, child)) in consumers.into_iter().enumerate() {
        let output =
            wait_with_timeout(child, Duration::from_secs(45), &format!("consumer-{index}"));
        assert_child_success(
            &output,
            &format!("consumer-{index}"),
            &[
                "MMAP_MP_CHILD_OK",
                &format!("id={consumer_id}"),
                &format!("consumed={}", case.events),
            ],
        );
    }

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn true_mmap_multiprocess_spsc_wraparound() {
    run_case(CaseSpec {
        buffer_size: 32,
        events: 8_192,
        consumers: 1,
        slow_every: 0,
        slow_micros: 0,
        label: "spsc_wraparound",
    });
}

#[test]
fn true_mmap_multiprocess_spmc_slow_consumer_gating() {
    run_case(CaseSpec {
        buffer_size: 64,
        events: 4_096,
        consumers: 2,
        slow_every: 17,
        slow_micros: 150,
        label: "spmc_slow_consumer",
    });
}

#[test]
#[ignore]
fn true_mmap_multiprocess_stress_matrix() {
    let cases = [
        CaseSpec {
            buffer_size: 512,
            events: 65_537,
            consumers: 1,
            slow_every: 0,
            slow_micros: 0,
            label: "stress_spsc_boundary",
        },
        CaseSpec {
            buffer_size: 64,
            events: 32_768,
            consumers: 1,
            slow_every: 0,
            slow_micros: 0,
            label: "stress_spsc_wraparound",
        },
        CaseSpec {
            buffer_size: 128,
            events: 16_384,
            consumers: 2,
            slow_every: 31,
            slow_micros: 75,
            label: "stress_spmc_slow",
        },
    ];

    for case in cases {
        run_case(case);
    }
}

#[test]
#[ignore]
fn mmap_mp_child_entry() {
    match env::var("MMAP_MP_CHILD_MODE")
        .expect("MMAP_MP_CHILD_MODE should be set")
        .as_str()
    {
        "producer" => run_child_producer(),
        "consumer" => run_child_consumer(),
        mode => panic!("unknown MMAP_MP_CHILD_MODE: {mode}"),
    }
}
