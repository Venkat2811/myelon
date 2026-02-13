use disruptor_mp::{attach_shared_consumer, build_shared_single_producer, AutoWaitStrategy};
use std::env;
use std::process::{Child, Command, Output, Stdio};
use std::str;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CHILD_TEST_NAME: &str = "mp_child_entry";
const PREFIX: &str = "TMC";

#[repr(C)]
#[derive(Copy, Clone)]
struct MpEvent {
    sequence: u64,
    checksum: u64,
    payload: [u8; 112],
}

impl Default for MpEvent {
    fn default() -> Self {
        Self {
            sequence: 0,
            checksum: 0,
            payload: [0; 112],
        }
    }
}

fn event_checksum(sequence: u64) -> u64 {
    sequence.wrapping_mul(0x9E37_79B1_85EB_CA87).rotate_left(17)
}

fn expected_checksum(events: u64) -> u64 {
    let mut acc = 0u64;
    for sequence in 0..events {
        acc = acc.wrapping_add(event_checksum(sequence));
    }
    acc
}

fn unique_segment(prefix: &str) -> String {
    let pid = std::process::id() % 10_000;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .subsec_nanos()
        % 100_000;
    format!("{prefix}{pid:04}{nanos:05}")
}

struct ChildSpec<'a> {
    mode: &'a str,
    segment: &'a str,
    buffer_size: usize,
    events: u64,
    consumers: usize,
    consumer_id: Option<&'a str>,
    slow_every: usize,
    slow_micros: u64,
}

fn spawn_child(spec: ChildSpec<'_>) -> Child {
    let exe = env::current_exe().expect("current_exe should be available");
    let mut cmd = Command::new(exe);
    cmd.arg("--exact")
        .arg(CHILD_TEST_NAME)
        .arg("--ignored")
        .arg("--nocapture")
        .env("MP_CHILD_MODE", spec.mode)
        .env("MP_SEGMENT", spec.segment)
        .env("MP_BUFFER_SIZE", spec.buffer_size.to_string())
        .env("MP_EVENTS", spec.events.to_string())
        .env("MP_CONSUMERS", spec.consumers.to_string())
        .env("MP_PREFIX", PREFIX)
        .env("MP_SLOW_EVERY", spec.slow_every.to_string())
        .env("MP_SLOW_MICROS", spec.slow_micros.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(id) = spec.consumer_id {
        cmd.env("MP_CONSUMER_ID", id);
    }

    cmd.spawn().expect("child process should spawn")
}

fn wait_with_timeout(mut child: Child, timeout: Duration, role: &str) -> Output {
    let start = Instant::now();
    loop {
        if let Some(_status) = child.try_wait().expect("try_wait should succeed") {
            return child
                .wait_with_output()
                .expect("wait_with_output should succeed");
        }

        if start.elapsed() > timeout {
            let _ = child.kill();
            let output = child
                .wait_with_output()
                .expect("wait_with_output should succeed after kill");
            panic!(
                "{role} timed out after {:?}\nstdout:\n{}\nstderr:\n{}",
                timeout,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }

        thread::sleep(Duration::from_millis(20));
    }
}

fn assert_child_success(output: &Output, role: &str) {
    if !output.status.success() {
        panic!(
            "{role} failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn true_multiprocess_spsc_wraparound_and_startup_race() {
    let segment = unique_segment("s");
    let buffer_size = 64usize;
    let events = 8_192u64; // 128 full wraps

    let consumer = spawn_child(ChildSpec {
        mode: "consumer",
        segment: &segment,
        buffer_size,
        events,
        consumers: 1,
        consumer_id: Some("TMC_0"),
        slow_every: 0,
        slow_micros: 0,
    });

    // Ensure consumer starts first and retries attach.
    thread::sleep(Duration::from_millis(150));

    let producer = spawn_child(ChildSpec {
        mode: "producer",
        segment: &segment,
        buffer_size,
        events,
        consumers: 1,
        consumer_id: None,
        slow_every: 0,
        slow_micros: 0,
    });

    let producer_output = wait_with_timeout(producer, Duration::from_secs(40), "producer");
    let consumer_output = wait_with_timeout(consumer, Duration::from_secs(40), "consumer");

    assert_child_success(&producer_output, "producer");
    assert_child_success(&consumer_output, "consumer");

    let producer_stdout = String::from_utf8_lossy(&producer_output.stdout);
    let consumer_stdout = String::from_utf8_lossy(&consumer_output.stdout);

    assert!(
        producer_stdout.contains("MP_CHILD_OK mode=producer"),
        "producer completion marker missing\nstdout:\n{producer_stdout}"
    );
    assert!(
        consumer_stdout.contains("MP_CHILD_OK mode=consumer"),
        "consumer completion marker missing\nstdout:\n{consumer_stdout}"
    );
    assert!(
        consumer_stdout.contains("consumed=8192"),
        "consumer did not report expected event count\nstdout:\n{consumer_stdout}"
    );
    assert!(
        consumer_stdout.contains("ordered=true"),
        "consumer did not report ordered=true\nstdout:\n{consumer_stdout}"
    );
}

#[test]
fn true_multiprocess_spmc_two_consumers_with_slow_consumer() {
    let segment = unique_segment("b");
    let buffer_size = 128usize;
    let events = 12_000u64;

    let fast_consumer = spawn_child(ChildSpec {
        mode: "consumer",
        segment: &segment,
        buffer_size,
        events,
        consumers: 2,
        consumer_id: Some("TMC_0"),
        slow_every: 0,
        slow_micros: 0,
    });
    let slow_consumer = spawn_child(ChildSpec {
        mode: "consumer",
        segment: &segment,
        buffer_size,
        events,
        consumers: 2,
        consumer_id: Some("TMC_1"),
        slow_every: 64,
        slow_micros: 200,
    });

    thread::sleep(Duration::from_millis(150));

    let producer = spawn_child(ChildSpec {
        mode: "producer",
        segment: &segment,
        buffer_size,
        events,
        consumers: 2,
        consumer_id: None,
        slow_every: 0,
        slow_micros: 0,
    });

    let producer_output = wait_with_timeout(producer, Duration::from_secs(60), "producer");
    let fast_output = wait_with_timeout(fast_consumer, Duration::from_secs(60), "fast_consumer");
    let slow_output = wait_with_timeout(slow_consumer, Duration::from_secs(60), "slow_consumer");

    assert_child_success(&producer_output, "producer");
    assert_child_success(&fast_output, "fast_consumer");
    assert_child_success(&slow_output, "slow_consumer");

    let producer_stdout = String::from_utf8_lossy(&producer_output.stdout);
    let fast_stdout = String::from_utf8_lossy(&fast_output.stdout);
    let slow_stdout = String::from_utf8_lossy(&slow_output.stdout);

    assert!(
        producer_stdout.contains("MP_CHILD_OK mode=producer"),
        "producer completion marker missing\nstdout:\n{producer_stdout}"
    );
    assert!(
        fast_stdout.contains("MP_CHILD_OK mode=consumer"),
        "fast consumer completion marker missing\nstdout:\n{fast_stdout}"
    );
    assert!(
        slow_stdout.contains("MP_CHILD_OK mode=consumer"),
        "slow consumer completion marker missing\nstdout:\n{slow_stdout}"
    );
    assert!(
        fast_stdout.contains("consumed=12000"),
        "fast consumer consumed count mismatch\nstdout:\n{fast_stdout}"
    );
    assert!(
        slow_stdout.contains("consumed=12000"),
        "slow consumer consumed count mismatch\nstdout:\n{slow_stdout}"
    );
}

fn parse_env<T>(name: &str) -> T
where
    T: str::FromStr,
    <T as str::FromStr>::Err: std::fmt::Display,
{
    let raw = env::var(name).unwrap_or_else(|_| panic!("missing env var: {name}"));
    raw.parse::<T>()
        .unwrap_or_else(|e| panic!("failed to parse env var {name}={raw}: {e}"))
}

fn run_child_producer() {
    let segment = env::var("MP_SEGMENT").expect("MP_SEGMENT should be set");
    let prefix = env::var("MP_PREFIX").expect("MP_PREFIX should be set");
    let buffer_size: usize = parse_env("MP_BUFFER_SIZE");
    let events: u64 = parse_env("MP_EVENTS");
    let consumers: usize = parse_env("MP_CONSUMERS");

    let mut producer = build_shared_single_producer::<MpEvent>(&segment, buffer_size)
        .discover_consumer_with_prefix_and_interval(consumers, &prefix, Duration::from_millis(1))
        .wait_for_consumers(consumers as i64, Duration::from_secs(20))
        .build_producer(MpEvent::default)
        .expect("producer should build");

    // Warm up discovery scans before publishing to avoid early overwrite windows.
    for _ in 0..20 {
        let _ = producer.min_gating_sequence();
        thread::sleep(Duration::from_millis(2));
    }

    let mut produced_checksum = 0u64;
    for sequence in 0..events {
        let checksum = event_checksum(sequence);
        produced_checksum = produced_checksum.wrapping_add(checksum);

        producer
            .publish_with_timeout(Duration::from_secs(5), |event| {
                event.sequence = sequence;
                event.checksum = checksum;
                event.payload.fill((sequence as u8) ^ 0xA5);
            })
            .expect("publish_with_timeout should not time out");

        // Force periodic consumer-gating checks in true multiprocess mode.
        if sequence % (buffer_size as u64 / 2).max(1) == 0 {
            let _ = producer.wait_until_consumed_with_strategy(
                sequence as i64,
                Duration::from_millis(200),
                AutoWaitStrategy::Block,
            );
        }
    }

    println!(
        "MP_CHILD_OK mode=producer published={} checksum={}",
        events, produced_checksum
    );
}

fn run_child_consumer() {
    let segment = env::var("MP_SEGMENT").expect("MP_SEGMENT should be set");
    let consumer_id = env::var("MP_CONSUMER_ID").expect("MP_CONSUMER_ID should be set");
    let buffer_size: usize = parse_env("MP_BUFFER_SIZE");
    let events: u64 = parse_env("MP_EVENTS");
    let slow_every: usize = parse_env("MP_SLOW_EVERY");
    let slow_micros: u64 = parse_env("MP_SLOW_MICROS");

    let attach_deadline = Instant::now() + Duration::from_secs(20);
    let mut consumer = loop {
        match attach_shared_consumer::<MpEvent>(&segment, buffer_size)
            .with_consumer_id(&consumer_id)
            .build_consumer()
        {
            Ok(consumer) => break consumer,
            Err(_) if Instant::now() < attach_deadline => {
                thread::sleep(Duration::from_millis(25));
            }
            Err(e) => panic!("consumer attach failed after retries: {e}"),
        }
    };

    let expected = expected_checksum(events);
    let deadline = Instant::now() + Duration::from_secs(40);
    let mut consumed = 0u64;
    let mut sum = 0u64;
    let mut ordered = true;
    let mut last_sequence = None::<u64>;

    while consumed < events {
        let processed = consumer.process_available(|event, sequence| {
            let seq = sequence as u64;
            if let Some(last) = last_sequence {
                if seq != last + 1 {
                    ordered = false;
                }
            }
            last_sequence = Some(seq);
            if event.sequence != seq {
                ordered = false;
            }
            if event.checksum != event_checksum(seq) {
                ordered = false;
            }

            consumed += 1;
            sum = sum.wrapping_add(event.checksum);

            if slow_every > 0 && consumed % slow_every as u64 == 0 {
                thread::sleep(Duration::from_micros(slow_micros));
            }
        });

        if consumed >= events {
            break;
        }

        if Instant::now() > deadline {
            panic!(
                "consumer timeout: consumed={} expected={} ordered={}",
                consumed, events, ordered
            );
        }

        if processed == 0 {
            thread::sleep(Duration::from_micros(100));
        }
    }

    assert_eq!(
        sum, expected,
        "consumer checksum mismatch: got {sum}, expected {expected}"
    );
    assert!(
        ordered,
        "consumer observed out-of-order or corrupted events"
    );

    println!(
        "MP_CHILD_OK mode=consumer id={} consumed={} checksum={} ordered={}",
        consumer_id, consumed, sum, ordered
    );
}

#[test]
#[ignore]
fn mp_child_entry() {
    let mode = env::var("MP_CHILD_MODE").expect("MP_CHILD_MODE should be set");
    match mode.as_str() {
        "producer" => run_child_producer(),
        "consumer" => run_child_consumer(),
        other => panic!("unknown MP_CHILD_MODE: {other}"),
    }
}
