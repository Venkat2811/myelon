use disruptor_mp::{attach_shared_consumer, build_shared_single_producer};
use std::env;
use std::fmt::Display;
use std::process::{Child, Command, Output, Stdio};
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const DEADLOCK_PREFIX: &str = "TDL";
static SEGMENT_COUNTER: AtomicU32 = AtomicU32::new(0);

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

#[derive(Clone, Copy)]
struct CaseSpec {
    buffer_size: usize,
    events: u64,
    label: &'static str,
}

fn event_checksum(sequence: u64) -> u64 {
    sequence.wrapping_mul(31).wrapping_add(7)
}

fn unique_segment(prefix: &str) -> String {
    let pid = std::process::id() % 10_000;
    let suffix = SEGMENT_COUNTER.fetch_add(1, Ordering::Relaxed) % 10_000;
    let compact_prefix: String = prefix
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .take(3)
        .collect();
    let compact_prefix = if compact_prefix.is_empty() {
        DEADLOCK_PREFIX.to_lowercase()
    } else {
        compact_prefix.to_lowercase()
    };
    format!("{compact_prefix}{pid:04}{suffix:04}")
}

fn spawn_child(mode: &str, segment: &str, case: CaseSpec, consumer_id: Option<&str>) -> Child {
    let current_exe = env::current_exe().expect("failed to resolve test binary");
    let mut command = Command::new(current_exe);
    command
        .arg("--exact")
        .arg("mp_deadlock_child_entry")
        .arg("--ignored")
        .arg("--nocapture")
        .env("MP_DEADLOCK_CHILD_MODE", mode)
        .env("MP_DEADLOCK_SEGMENT", segment)
        .env("MP_DEADLOCK_BUFFER_SIZE", case.buffer_size.to_string())
        .env("MP_DEADLOCK_EVENTS", case.events.to_string())
        .env("MP_DEADLOCK_PREFIX", DEADLOCK_PREFIX)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(id) = consumer_id {
        command.env("MP_DEADLOCK_CONSUMER_ID", id);
    }

    command.spawn().expect("failed to spawn child process")
}

enum ChildOutcome {
    Completed(Output),
    TimedOut(Output),
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
            let output = child
                .wait_with_output()
                .expect("failed to read timed-out child output");
            return ChildOutcome::TimedOut(output);
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

fn assert_child_success(output: Output, role: &str, case: CaseSpec) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "{role} child failed for case '{}' (buffer_size={}, events={})\nstdout:\n{}\nstderr:\n{}",
        case.label,
        case.buffer_size,
        case.events,
        stdout,
        stderr,
    );
    assert!(
        stdout.contains("MP_DEADLOCK_CHILD_OK"),
        "{role} child did not report success marker for case '{}' (buffer_size={}, events={})\nstdout:\n{}\nstderr:\n{}",
        case.label,
        case.buffer_size,
        case.events,
        stdout,
        stderr,
    );
}

fn run_case(case: CaseSpec) {
    let segment = unique_segment("mp_deadlock");
    let consumer_id = format!("{DEADLOCK_PREFIX}_0");
    let consumer = spawn_child("consumer", &segment, case, Some(&consumer_id));
    thread::sleep(Duration::from_millis(100));
    let producer = spawn_child("producer", &segment, case, None);

    let producer_timeout = Duration::from_secs(45);
    let consumer_timeout = Duration::from_secs(45);
    let producer_outcome = wait_with_timeout(producer, producer_timeout);
    match producer_outcome {
        ChildOutcome::Completed(output) => {
            assert_child_success(output, "producer", case);
        }
        ChildOutcome::TimedOut(output) => {
            let producer_outcome = ChildOutcome::TimedOut(output);
            let consumer_outcome = wait_with_timeout(consumer, Duration::from_secs(5));
            panic!(
                "producer child timed out for case '{}' (buffer_size={}, events={})\n{}\n{}\nsegment={}",
                case.label,
                case.buffer_size,
                case.events,
                describe_outcome("producer", producer_timeout, &producer_outcome),
                describe_outcome("consumer", Duration::from_secs(5), &consumer_outcome),
                segment,
            );
        }
    }

    let consumer_outcome = wait_with_timeout(consumer, consumer_timeout);
    match consumer_outcome {
        ChildOutcome::Completed(output) => {
            assert_child_success(output, "consumer", case);
        }
        ChildOutcome::TimedOut(output) => {
            let consumer_outcome = ChildOutcome::TimedOut(output);
            panic!(
                "consumer child timed out for case '{}' (buffer_size={}, events={})\n{}\nsegment={}",
                case.label,
                case.buffer_size,
                case.events,
                describe_outcome("consumer", consumer_timeout, &consumer_outcome),
                segment,
            );
        }
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

fn child_case() -> (String, CaseSpec) {
    let segment = env::var("MP_DEADLOCK_SEGMENT").expect("MP_DEADLOCK_SEGMENT should be set");
    let case = CaseSpec {
        buffer_size: parse_env("MP_DEADLOCK_BUFFER_SIZE"),
        events: parse_env("MP_DEADLOCK_EVENTS"),
        label: "child",
    };
    (segment, case)
}

fn run_child_producer() {
    let (segment, case) = child_case();
    let prefix = env::var("MP_DEADLOCK_PREFIX").expect("MP_DEADLOCK_PREFIX should be set");
    let mut producer = build_shared_single_producer::<TestEvent>(&segment, case.buffer_size)
        .discover_consumer_with_prefix_and_interval(1, &prefix, Duration::from_millis(1))
        .wait_for_consumers(1, Duration::from_secs(20))
        .build_producer(TestEvent::default)
        .expect("producer build failed");

    for _ in 0..20 {
        let _ = producer.min_gating_sequence();
        thread::sleep(Duration::from_millis(2));
    }

    let deadline = Instant::now() + Duration::from_secs(35);
    for sequence in 0..case.events {
        if Instant::now() > deadline {
            panic!(
                "producer timeout at sequence {sequence} for buffer_size={} events={}",
                case.buffer_size, case.events
            );
        }

        producer.publish(|event| {
            event.sequence = sequence;
            event.checksum = event_checksum(sequence);
            for (index, slot) in event.payload.iter_mut().enumerate() {
                *slot = ((sequence + index as u64) & 0xFF) as u8;
            }
        });
    }

    println!(
        "MP_DEADLOCK_CHILD_OK mode=producer produced={} buffer_size={}",
        case.events, case.buffer_size
    );
}

fn run_child_consumer() {
    let (segment, case) = child_case();
    let consumer_id =
        env::var("MP_DEADLOCK_CONSUMER_ID").expect("MP_DEADLOCK_CONSUMER_ID should be set");
    let attach_deadline = Instant::now() + Duration::from_secs(20);
    let mut consumer = loop {
        match attach_shared_consumer::<TestEvent>(&segment, case.buffer_size)
            .with_consumer_id(&consumer_id)
            .build_consumer()
        {
            Ok(consumer) => break consumer,
            Err(_) if Instant::now() < attach_deadline => thread::sleep(Duration::from_millis(25)),
            Err(err) => panic!("consumer attach failed: {err}"),
        }
    };

    let consume_deadline = Instant::now() + Duration::from_secs(40);
    let mut consumed = 0u64;
    let mut last_sequence = None::<u64>;

    while consumed < case.events {
        let processed = consumer.process_available(|event, sequence| {
            let sequence = sequence as u64;

            if let Some(last) = last_sequence {
                assert_eq!(
                    sequence,
                    last + 1,
                    "sequence gap: expected {}, got {}",
                    last + 1,
                    sequence
                );
            }
            last_sequence = Some(sequence);
            assert_eq!(event.sequence, sequence, "event sequence mismatch");
            assert_eq!(
                event.checksum,
                event_checksum(sequence),
                "event checksum mismatch"
            );
            consumed += 1;
        });

        if consumed >= case.events {
            break;
        }
        if Instant::now() > consume_deadline {
            panic!(
                "consumer timeout: consumed={} expected={} buffer_size={}",
                consumed, case.events, case.buffer_size
            );
        }
        if processed == 0 {
            thread::sleep(Duration::from_micros(100));
        }
    }

    println!(
        "MP_DEADLOCK_CHILD_OK mode=consumer consumed={} buffer_size={}",
        consumed, case.buffer_size
    );
}

#[test]
fn true_multiprocess_64kb_buffer_deadlock_at_65536_events() {
    run_case(CaseSpec {
        buffer_size: 512,
        events: 65_536,
        label: "64kb_exact_boundary",
    });
}

#[test]
fn true_multiprocess_deadlock_boundary_conditions() {
    let cases = [
        CaseSpec {
            buffer_size: 512,
            events: 65_535,
            label: "just_before_boundary",
        },
        CaseSpec {
            buffer_size: 512,
            events: 65_536,
            label: "exact_boundary",
        },
        CaseSpec {
            buffer_size: 512,
            events: 65_537,
            label: "just_after_boundary",
        },
        CaseSpec {
            buffer_size: 512,
            events: 512,
            label: "exact_buffer_size",
        },
        CaseSpec {
            buffer_size: 512,
            events: 1_024,
            label: "double_buffer_size",
        },
        CaseSpec {
            buffer_size: 512,
            events: 100_000,
            label: "benchmark_scale",
        },
    ];

    for case in cases {
        run_case(case);
    }
}

#[test]
fn true_multiprocess_deadlock_multiple_buffer_sizes() {
    for buffer_size in [256usize, 512, 1024, 2048, 4096] {
        run_case(CaseSpec {
            buffer_size,
            events: buffer_size as u64 * 10,
            label: "buffer_wrap_stress",
        });
    }
}

#[test]
#[ignore]
fn mp_deadlock_child_entry() {
    let mode = env::var("MP_DEADLOCK_CHILD_MODE").expect("MP_DEADLOCK_CHILD_MODE should be set");
    match mode.as_str() {
        "producer" => run_child_producer(),
        "consumer" => run_child_consumer(),
        other => panic!("unknown child mode: {other}"),
    }
}
