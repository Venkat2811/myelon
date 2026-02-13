use disruptor_mp::{attach_shared_consumer, build_shared_single_producer};
use std::env;
use std::fmt::Display;
use std::process::{Child, Command, Output, Stdio};
use std::str::FromStr;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    let timestamp_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after UNIX_EPOCH")
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), timestamp_ns)
}

fn spawn_child(mode: &str, segment: &str, case: CaseSpec) -> Child {
    let current_exe = env::current_exe().expect("failed to resolve test binary");
    Command::new(current_exe)
        .arg("--exact")
        .arg("mp_deadlock_child_entry")
        .arg("--ignored")
        .arg("--nocapture")
        .env("MP_DEADLOCK_CHILD_MODE", mode)
        .env("MP_DEADLOCK_SEGMENT", segment)
        .env("MP_DEADLOCK_BUFFER_SIZE", case.buffer_size.to_string())
        .env("MP_DEADLOCK_EVENTS", case.events.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn child process")
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
    let consumer = spawn_child("consumer", &segment, case);
    thread::sleep(Duration::from_millis(100));
    let producer = spawn_child("producer", &segment, case);

    let producer_output = wait_with_timeout(producer, Duration::from_secs(45), "producer");
    assert_child_success(producer_output, "producer", case);

    let consumer_output = wait_with_timeout(consumer, Duration::from_secs(45), "consumer");
    assert_child_success(consumer_output, "consumer", case);
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
    let mut producer = build_shared_single_producer::<TestEvent>(&segment, case.buffer_size)
        .enable_discovery(1)
        .build_producer(TestEvent::default)
        .expect("producer build failed");

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
    let attach_deadline = Instant::now() + Duration::from_secs(20);
    let mut consumer = loop {
        match attach_shared_consumer::<TestEvent>(&segment, case.buffer_size).build_consumer() {
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
