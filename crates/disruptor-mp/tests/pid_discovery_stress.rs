use disruptor_mp::{attach_shared_consumer, build_shared_single_producer};
use std::env;
use std::fmt::Display;
use std::process::{Child, Command, Output, Stdio};
use std::str::FromStr;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CHILD_TEST_NAME: &str = "pid_discovery_child_entry";

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
            payload: [0u8; 112],
        }
    }
}

fn event_checksum(sequence: u64) -> u64 {
    sequence.wrapping_mul(31).wrapping_add(7)
}

fn unique_segment(prefix: &str) -> String {
    let timestamp_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after UNIX_EPOCH")
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), timestamp_ns)
}

enum ChildOutcome {
    Completed(Output),
    TimedOut(Output),
}

fn spawn_child(mode: &str, segment: &str, buffer_size: usize, events: u64) -> Child {
    let current_exe = env::current_exe().expect("failed to resolve test binary");
    Command::new(current_exe)
        .arg("--exact")
        .arg(CHILD_TEST_NAME)
        .arg("--ignored")
        .arg("--nocapture")
        .env("PID_DISCOVERY_CHILD_MODE", mode)
        .env("PID_DISCOVERY_SEGMENT", segment)
        .env("PID_DISCOVERY_BUFFER_SIZE", buffer_size.to_string())
        .env("PID_DISCOVERY_EVENTS", events.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn child process")
}

fn wait_with_timeout(mut child: Child, timeout: Duration) -> ChildOutcome {
    let start = Instant::now();
    loop {
        if child
            .try_wait()
            .expect("failed to poll child process")
            .is_some()
        {
            return ChildOutcome::Completed(
                child
                    .wait_with_output()
                    .expect("failed to read child output"),
            );
        }

        if start.elapsed() > timeout {
            let _ = child.kill();
            return ChildOutcome::TimedOut(
                child
                    .wait_with_output()
                    .expect("failed to read timed-out child output"),
            );
        }

        thread::sleep(Duration::from_millis(10));
    }
}

fn describe_outcome(role: &str, timeout: Duration, outcome: &ChildOutcome) -> String {
    match outcome {
        ChildOutcome::Completed(output) => format!(
            "{role} completed with status {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        ),
        ChildOutcome::TimedOut(output) => format!(
            "{role} timed out after {:?}\nstdout:\n{}\nstderr:\n{}",
            timeout,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        ),
    }
}

fn assert_child_success(output: Output, role: &str, buffer_size: usize, events: u64) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "{role} child failed (buffer_size={}, events={})\nstdout:\n{}\nstderr:\n{}",
        buffer_size,
        events,
        stdout,
        stderr,
    );
    assert!(
        stdout.contains("PID_DISCOVERY_CHILD_OK"),
        "{role} child did not report success marker (buffer_size={}, events={})\nstdout:\n{}\nstderr:\n{}",
        buffer_size,
        events,
        stdout,
        stderr,
    );
}

fn child_succeeded(output: &Output) -> bool {
    output.status.success()
        && String::from_utf8_lossy(&output.stdout).contains("PID_DISCOVERY_CHILD_OK")
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

fn child_case() -> (String, usize, u64) {
    (
        env::var("PID_DISCOVERY_SEGMENT").expect("PID_DISCOVERY_SEGMENT should be set"),
        parse_env("PID_DISCOVERY_BUFFER_SIZE"),
        parse_env("PID_DISCOVERY_EVENTS"),
    )
}

fn run_child_producer() {
    let (segment, buffer_size, events) = child_case();
    let producer_pid = std::process::id();
    let mut producer = build_shared_single_producer::<TestEvent>(&segment, buffer_size)
        .enable_discovery(1)
        .build_producer(TestEvent::default)
        .expect("producer build failed");

    for sequence in 0..events {
        if let Err(error) = producer.publish_with_timeout(Duration::from_secs(5), |event| {
            event.sequence = sequence;
            event.checksum = event_checksum(sequence);
            for (index, slot) in event.payload.iter_mut().enumerate() {
                *slot = ((sequence + index as u64) & 0xFF) as u8;
            }
        }) {
            panic!(
                "producer publish timeout at sequence {sequence} buffer_size={} events={} min_gating_sequence={} producer_pid={} error={}",
                buffer_size,
                events,
                producer.min_gating_sequence(),
                producer_pid,
                error,
            );
        }
    }

    println!(
        "PID_DISCOVERY_CHILD_OK mode=producer published={} buffer_size={} producer_pid={}",
        events, buffer_size, producer_pid
    );
}

fn run_child_consumer() {
    let (segment, buffer_size, events) = child_case();
    let consumer_pid = std::process::id();
    let attach_deadline = Instant::now() + Duration::from_secs(20);
    let mut consumer = loop {
        match attach_shared_consumer::<TestEvent>(&segment, buffer_size).build_consumer() {
            Ok(consumer) => break consumer,
            Err(_) if Instant::now() < attach_deadline => thread::sleep(Duration::from_millis(25)),
            Err(err) => panic!("consumer attach failed: {err}"),
        }
    };
    let consumer_id = consumer.consumer_id().to_string();
    println!(
        "PID_DISCOVERY_CONSUMER_ATTACHED consumer_id={} buffer_size={} events={} consumer_pid={}",
        consumer_id, buffer_size, events, consumer_pid
    );

    let consume_deadline = Instant::now() + Duration::from_secs(40);
    let mut consumed = 0u64;
    let mut last_sequence = None::<u64>;

    while consumed < events {
        let processed = consumer.process_available(|event, sequence| {
            let sequence = sequence as u64;
            if let Some(last) = last_sequence {
                assert_eq!(sequence, last + 1, "sequence gap");
            }
            last_sequence = Some(sequence);
            assert_eq!(event.sequence, sequence, "event sequence mismatch");
            assert_eq!(
                event.checksum,
                event_checksum(sequence),
                "event checksum mismatch"
            );
            consumed += 1;
            if consumed == 1 {
                println!(
                    "PID_DISCOVERY_CONSUMER_FIRST_EVENT consumer_id={} sequence={} consumer_pid={}",
                    consumer_id, sequence, consumer_pid
                );
            }
        });

        if consumed >= events {
            break;
        }
        if Instant::now() > consume_deadline {
            let (last_processed, producer_seq, consumer_seq) = consumer.debug_sequences();
            panic!(
                "consumer timeout: consumed={} expected={} buffer_size={} last_processed={} producer_seq={} consumer_seq={} consumer_id={} consumer_pid={}",
                consumed, events, buffer_size, last_processed, producer_seq, consumer_seq, consumer_id, consumer_pid
            );
        }
        if processed == 0 {
            thread::sleep(Duration::from_micros(100));
        }
    }

    println!(
        "PID_DISCOVERY_CHILD_OK mode=consumer consumed={} buffer_size={} consumer_id={} consumer_pid={}",
        consumed, buffer_size, consumer_id, consumer_pid
    );
}

fn run_case(buffer_size: usize, events: u64) {
    let segment = unique_segment("pid_discovery");
    let consumer = spawn_child("consumer", &segment, buffer_size, events);
    thread::sleep(Duration::from_millis(100));
    let producer = spawn_child("producer", &segment, buffer_size, events);

    let producer_timeout = Duration::from_secs(45);
    let consumer_timeout = Duration::from_secs(45);
    let producer_outcome = wait_with_timeout(producer, producer_timeout);
    match producer_outcome {
        ChildOutcome::Completed(output) => {
            if child_succeeded(&output) {
                assert_child_success(output, "producer", buffer_size, events);
            } else {
                let producer_outcome = ChildOutcome::Completed(output);
                let consumer_outcome = wait_with_timeout(consumer, Duration::from_secs(5));
                panic!(
                    "PID discovery producer child failure (buffer_size={}, events={})\n{}\n{}\nsegment={}",
                    buffer_size,
                    events,
                    describe_outcome("producer", producer_timeout, &producer_outcome),
                    describe_outcome("consumer", Duration::from_secs(5), &consumer_outcome),
                    segment,
                );
            }
        }
        ChildOutcome::TimedOut(output) => {
            let producer_outcome = ChildOutcome::TimedOut(output);
            let consumer_outcome = wait_with_timeout(consumer, Duration::from_secs(5));
            panic!(
                "PID discovery producer timeout (buffer_size={}, events={})\n{}\n{}\nsegment={}",
                buffer_size,
                events,
                describe_outcome("producer", producer_timeout, &producer_outcome),
                describe_outcome("consumer", Duration::from_secs(5), &consumer_outcome),
                segment,
            );
        }
    }

    let consumer_outcome = wait_with_timeout(consumer, consumer_timeout);
    match consumer_outcome {
        ChildOutcome::Completed(output) => {
            assert_child_success(output, "consumer", buffer_size, events);
        }
        ChildOutcome::TimedOut(output) => {
            let consumer_outcome = ChildOutcome::TimedOut(output);
            panic!(
                "PID discovery consumer timeout (buffer_size={}, events={})\n{}\nsegment={}",
                buffer_size,
                events,
                describe_outcome("consumer", consumer_timeout, &consumer_outcome),
                segment,
            );
        }
    }
}

#[test]
#[ignore]
fn true_multiprocess_pid_discovery_benchmark_scale() {
    run_case(512, 100_000);
}

#[test]
#[ignore]
fn true_multiprocess_pid_discovery_boundary_sequence() {
    let cases = [
        (512usize, 65_535u64),
        (512usize, 65_536u64),
        (512usize, 65_537u64),
        (512usize, 512u64),
        (512usize, 1_024u64),
        (512usize, 100_000u64),
    ];

    for (buffer_size, events) in cases {
        run_case(buffer_size, events);
    }
}

#[test]
#[ignore]
fn pid_discovery_child_entry() {
    match env::var("PID_DISCOVERY_CHILD_MODE")
        .expect("PID_DISCOVERY_CHILD_MODE should be set")
        .as_str()
    {
        "producer" => run_child_producer(),
        "consumer" => run_child_consumer(),
        mode => panic!("unknown PID_DISCOVERY_CHILD_MODE: {mode}"),
    }
}
