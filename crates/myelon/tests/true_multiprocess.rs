use myelon::{
    attach_shared_consumer, build_shared_single_producer,
    inference::{FixedTopology, WorkerCount},
    AutoWaitStrategy, RingBufferFull,
};
use std::env;
use std::process::{Child, Command, Output, Stdio};
use std::str;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const CHILD_TEST_NAME: &str = "mp_child_entry";
const PREFIX: &str = "TMC";
static UNIQUE_SEGMENT_COUNTER: AtomicUsize = AtomicUsize::new(0);

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
    let prefix: String = prefix
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .take(4)
        .collect();
    let pid = std::process::id() % 10_000;
    let suffix = UNIQUE_SEGMENT_COUNTER.fetch_add(1, Ordering::Relaxed) % 100;
    let name = format!("{prefix}{pid:04}{suffix:02}");
    assert!(
        name.len() <= 14,
        "segment name exceeds macOS budget: {name}"
    );
    name
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
    scenario: &'a str,
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
        .env("MP_CHILD_SCENARIO", spec.scenario)
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

fn assert_child_contains(output: &Output, role: &str, expected: &str) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(expected),
        "{role} output missing '{expected}'\nstdout:\n{stdout}"
    );
}

fn parse_marker_u64(output: &str, marker: &str) -> u64 {
    for token in output.split_whitespace() {
        let Some((key, value)) = token.split_once('=') else {
            continue;
        };
        if key == marker {
            return value
                .parse::<u64>()
                .unwrap_or_else(|err| panic!("invalid value for marker '{marker}': {err}"));
        }
    }

    panic!("output did not contain marker '{marker}': {output}");
}

#[derive(Clone, Copy)]
struct StartupLoopCase {
    run: usize,
    iteration: usize,
    events: u64,
    buffer_size: usize,
    consumers: usize,
    producer_first: bool,
    producer_scenario: &'static str,
}

fn run_startup_determinism_case(case: StartupLoopCase) {
    let segment = unique_segment(&format!("startdet_{}_{}", case.run, case.iteration));
    let mut consumers = Vec::with_capacity(case.consumers);

    let mut launch_consumer = |start_id: usize| {
        for id in 0..case.consumers {
            let consumer_id = format!("TMC_{}", start_id + id);
            let consumer = spawn_child(ChildSpec {
                mode: "consumer",
                segment: &segment,
                buffer_size: case.buffer_size,
                events: case.events,
                consumers: case.consumers,
                consumer_id: Some(&consumer_id),
                slow_every: 0,
                slow_micros: 0,
                scenario: "basic",
            });
            consumers.push(consumer);
            thread::sleep(Duration::from_millis(2));
        }
    };

    if !case.producer_first {
        launch_consumer(0);
        thread::sleep(Duration::from_millis(80));
    }

    let producer = spawn_child(ChildSpec {
        mode: "producer",
        segment: &segment,
        buffer_size: case.buffer_size,
        events: case.events,
        consumers: case.consumers,
        consumer_id: None,
        slow_every: 0,
        slow_micros: 0,
        scenario: case.producer_scenario,
    });

    if case.producer_first {
        thread::sleep(Duration::from_millis(120));
        launch_consumer(0);
    }

    let producer_output = wait_with_timeout(producer, Duration::from_secs(45), "producer");
    assert_child_success(&producer_output, "producer");
    let producer_stdout = String::from_utf8_lossy(&producer_output.stdout);
    assert_eq!(
        parse_marker_u64(&producer_stdout, "published"),
        case.events,
        "producer event count mismatch\nstdout:\n{producer_stdout}"
    );

    for (idx, child) in consumers.into_iter().enumerate() {
        let consumer_output =
            wait_with_timeout(child, Duration::from_secs(45), &format!("consumer-{idx}"));
        assert_child_success(&consumer_output, &format!("consumer-{idx}"));
        assert_child_contains(
            &consumer_output,
            &format!("consumer-{idx}"),
            &format!("id=TMC_{idx}"),
        );
        assert_child_contains(
            &consumer_output,
            &format!("consumer-{idx}"),
            &format!("consumed={}", case.events),
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
        scenario: "basic",
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
        scenario: "basic",
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
fn true_multiprocess_startup_determinism_loop() {
    let events = 3_072u64;
    let buffer_size = 128usize;

    let cases = [
        StartupLoopCase {
            run: 1,
            iteration: 0,
            events,
            buffer_size,
            consumers: 1,
            producer_first: false,
            producer_scenario: "basic",
        },
        StartupLoopCase {
            run: 1,
            iteration: 1,
            events,
            buffer_size,
            consumers: 1,
            producer_first: true,
            producer_scenario: "basic",
        },
        StartupLoopCase {
            run: 1,
            iteration: 2,
            events,
            buffer_size,
            consumers: 2,
            producer_first: false,
            producer_scenario: "basic",
        },
        StartupLoopCase {
            run: 1,
            iteration: 3,
            events,
            buffer_size,
            consumers: 2,
            producer_first: true,
            producer_scenario: "basic",
        },
        StartupLoopCase {
            run: 1,
            iteration: 4,
            events,
            buffer_size,
            consumers: 3,
            producer_first: false,
            producer_scenario: "basic",
        },
        StartupLoopCase {
            run: 1,
            iteration: 5,
            events,
            buffer_size,
            consumers: 3,
            producer_first: true,
            producer_scenario: "basic",
        },
    ];

    for case in cases {
        run_startup_determinism_case(case);
    }
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
        scenario: "basic",
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
        scenario: "basic",
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
        scenario: "basic",
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

#[test]
fn true_multiprocess_fixed_topology_two_workers() {
    run_fixed_topology_case(
        128,
        6_000,
        &[
            FixedTopologyWorkerCase {
                worker_index: 0,
                slow_every: 0,
                slow_micros: 0,
            },
            FixedTopologyWorkerCase {
                worker_index: 1,
                slow_every: 32,
                slow_micros: 150,
            },
        ],
    );
}

#[test]
fn true_multiprocess_fixed_topology_three_workers() {
    run_fixed_topology_case(
        128,
        4_096,
        &[
            FixedTopologyWorkerCase {
                worker_index: 0,
                slow_every: 0,
                slow_micros: 0,
            },
            FixedTopologyWorkerCase {
                worker_index: 1,
                slow_every: 24,
                slow_micros: 120,
            },
            FixedTopologyWorkerCase {
                worker_index: 2,
                slow_every: 40,
                slow_micros: 80,
            },
        ],
    );
}

#[test]
fn true_multiprocess_fixed_topology_four_workers() {
    run_fixed_topology_case(
        128,
        3_072,
        &[
            FixedTopologyWorkerCase {
                worker_index: 0,
                slow_every: 0,
                slow_micros: 0,
            },
            FixedTopologyWorkerCase {
                worker_index: 1,
                slow_every: 20,
                slow_micros: 100,
            },
            FixedTopologyWorkerCase {
                worker_index: 2,
                slow_every: 28,
                slow_micros: 120,
            },
            FixedTopologyWorkerCase {
                worker_index: 3,
                slow_every: 36,
                slow_micros: 140,
            },
        ],
    );
}

#[test]
fn true_multiprocess_fixed_topology_five_workers() {
    run_fixed_topology_case(
        256,
        2_560,
        &[
            FixedTopologyWorkerCase {
                worker_index: 0,
                slow_every: 0,
                slow_micros: 0,
            },
            FixedTopologyWorkerCase {
                worker_index: 1,
                slow_every: 20,
                slow_micros: 75,
            },
            FixedTopologyWorkerCase {
                worker_index: 2,
                slow_every: 24,
                slow_micros: 85,
            },
            FixedTopologyWorkerCase {
                worker_index: 3,
                slow_every: 28,
                slow_micros: 95,
            },
            FixedTopologyWorkerCase {
                worker_index: 4,
                slow_every: 32,
                slow_micros: 105,
            },
        ],
    );
}

#[test]
fn true_multiprocess_fixed_topology_six_workers() {
    run_fixed_topology_case(
        256,
        2_304,
        &[
            FixedTopologyWorkerCase {
                worker_index: 0,
                slow_every: 0,
                slow_micros: 0,
            },
            FixedTopologyWorkerCase {
                worker_index: 1,
                slow_every: 22,
                slow_micros: 80,
            },
            FixedTopologyWorkerCase {
                worker_index: 2,
                slow_every: 26,
                slow_micros: 90,
            },
            FixedTopologyWorkerCase {
                worker_index: 3,
                slow_every: 30,
                slow_micros: 100,
            },
            FixedTopologyWorkerCase {
                worker_index: 4,
                slow_every: 34,
                slow_micros: 110,
            },
            FixedTopologyWorkerCase {
                worker_index: 5,
                slow_every: 38,
                slow_micros: 120,
            },
        ],
    );
}

#[test]
fn true_multiprocess_fixed_topology_seven_workers() {
    run_fixed_topology_case(
        256,
        2_176,
        &[
            FixedTopologyWorkerCase {
                worker_index: 0,
                slow_every: 0,
                slow_micros: 0,
            },
            FixedTopologyWorkerCase {
                worker_index: 1,
                slow_every: 22,
                slow_micros: 80,
            },
            FixedTopologyWorkerCase {
                worker_index: 2,
                slow_every: 26,
                slow_micros: 90,
            },
            FixedTopologyWorkerCase {
                worker_index: 3,
                slow_every: 30,
                slow_micros: 100,
            },
            FixedTopologyWorkerCase {
                worker_index: 4,
                slow_every: 34,
                slow_micros: 110,
            },
            FixedTopologyWorkerCase {
                worker_index: 5,
                slow_every: 38,
                slow_micros: 120,
            },
            FixedTopologyWorkerCase {
                worker_index: 6,
                slow_every: 42,
                slow_micros: 130,
            },
        ],
    );
}

#[test]
fn true_multiprocess_fixed_topology_eight_workers() {
    run_fixed_topology_case(
        256,
        2_048,
        &[
            FixedTopologyWorkerCase {
                worker_index: 0,
                slow_every: 0,
                slow_micros: 0,
            },
            FixedTopologyWorkerCase {
                worker_index: 1,
                slow_every: 24,
                slow_micros: 70,
            },
            FixedTopologyWorkerCase {
                worker_index: 2,
                slow_every: 28,
                slow_micros: 80,
            },
            FixedTopologyWorkerCase {
                worker_index: 3,
                slow_every: 32,
                slow_micros: 90,
            },
            FixedTopologyWorkerCase {
                worker_index: 4,
                slow_every: 36,
                slow_micros: 100,
            },
            FixedTopologyWorkerCase {
                worker_index: 5,
                slow_every: 40,
                slow_micros: 110,
            },
            FixedTopologyWorkerCase {
                worker_index: 6,
                slow_every: 44,
                slow_micros: 120,
            },
            FixedTopologyWorkerCase {
                worker_index: 7,
                slow_every: 48,
                slow_micros: 130,
            },
        ],
    );
}

#[test]
fn true_multiprocess_concurrent_spsc() {
    let segment = unique_segment("mp_concurrent");
    let buffer_size = 64usize;
    let events = 2_000u64;

    let consumer = spawn_child(ChildSpec {
        mode: "consumer",
        segment: &segment,
        buffer_size,
        events,
        consumers: 1,
        consumer_id: Some("TMC_0"),
        slow_every: 0,
        slow_micros: 0,
        scenario: "basic",
    });

    thread::sleep(Duration::from_millis(120));

    let producer = spawn_child(ChildSpec {
        mode: "producer",
        segment: &segment,
        buffer_size,
        events,
        consumers: 1,
        consumer_id: None,
        slow_every: 0,
        slow_micros: 0,
        scenario: "basic",
    });

    let producer_output = wait_with_timeout(producer, Duration::from_secs(45), "producer");
    let consumer_output = wait_with_timeout(consumer, Duration::from_secs(45), "consumer");

    assert_child_success(&producer_output, "producer");
    assert_child_success(&consumer_output, "consumer");

    let producer_stdout = String::from_utf8_lossy(&producer_output.stdout);
    let consumer_stdout = String::from_utf8_lossy(&consumer_output.stdout);

    assert!(
        producer_stdout.contains("MP_CHILD_OK mode=producer scenario=basic"),
        "producer completion marker missing\nstdout:\n{producer_stdout}"
    );
    assert!(
        consumer_stdout.contains("MP_CHILD_OK mode=consumer"),
        "consumer completion marker missing\nstdout:\n{consumer_stdout}"
    );
    assert_eq!(
        parse_marker_u64(&consumer_stdout, "consumed"),
        events,
        "consumer consumed count mismatch\nstdout:\n{consumer_stdout}"
    );
}

#[derive(Clone, Copy)]
struct BackpressureCase {
    events: u64,
    buffer_size: usize,
    consumers: usize,
    slow_every: usize,
    slow_micros: u64,
    producer_scenario: &'static str,
}

#[derive(Clone, Copy)]
struct FixedTopologyWorkerCase {
    worker_index: usize,
    slow_every: usize,
    slow_micros: u64,
}

#[test]
fn true_multiprocess_backpressure_spsc() {
    let case = BackpressureCase {
        events: 200,
        buffer_size: 8,
        consumers: 1,
        slow_every: 4,
        slow_micros: 250,
        producer_scenario: "backpressure",
    };

    let segment = unique_segment("mp_backpressure");
    let consumer = spawn_child(ChildSpec {
        mode: "consumer",
        segment: &segment,
        buffer_size: case.buffer_size,
        events: case.events,
        consumers: case.consumers,
        consumer_id: Some("TMC_0"),
        slow_every: case.slow_every,
        slow_micros: case.slow_micros,
        scenario: "basic",
    });

    thread::sleep(Duration::from_millis(80));

    let producer = spawn_child(ChildSpec {
        mode: "producer",
        segment: &segment,
        buffer_size: case.buffer_size,
        events: case.events,
        consumers: case.consumers,
        consumer_id: None,
        slow_every: 0,
        slow_micros: 0,
        scenario: case.producer_scenario,
    });

    let producer_output = wait_with_timeout(producer, Duration::from_secs(45), "producer");
    let consumer_output = wait_with_timeout(consumer, Duration::from_secs(45), "consumer");

    assert_child_success(&producer_output, "producer");
    assert_child_success(&consumer_output, "consumer");

    let producer_stdout = String::from_utf8_lossy(&producer_output.stdout);
    let consumer_stdout = String::from_utf8_lossy(&consumer_output.stdout);

    assert!(
        producer_stdout.contains("MP_CHILD_OK mode=producer scenario=backpressure"),
        "producer completion marker missing\nstdout:\n{producer_stdout}"
    );
    assert!(
        consumer_stdout.contains("MP_CHILD_OK mode=consumer"),
        "consumer completion marker missing\nstdout:\n{consumer_stdout}"
    );
    assert!(
        parse_marker_u64(&producer_stdout, "published") == case.events,
        "producer should publish exact event count\nstdout:\n{producer_stdout}"
    );
    assert!(
        parse_marker_u64(&producer_stdout, "backpressure_events") > 0,
        "backpressure profile should observe consumer stall\nstdout:\n{producer_stdout}"
    );
    assert!(
        parse_marker_u64(&consumer_stdout, "consumed") == case.events,
        "consumer should consume all events\nstdout:\n{consumer_stdout}"
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

fn worker_count_from_usize(value: usize) -> WorkerCount {
    WorkerCount::from_usize(value).unwrap_or_else(|| {
        panic!("fixed topology requires a worker count between 2 and 8, got {value}")
    })
}

fn run_fixed_topology_case(buffer_size: usize, events: u64, workers: &[FixedTopologyWorkerCase]) {
    let segment = unique_segment("topology");
    let consumers = workers.len();
    let mut children = Vec::with_capacity(consumers);

    for worker in workers {
        let worker_id = worker.worker_index.to_string();
        let child = spawn_child(ChildSpec {
            mode: "consumer",
            segment: &segment,
            buffer_size,
            events,
            consumers,
            consumer_id: Some(&worker_id),
            slow_every: worker.slow_every,
            slow_micros: worker.slow_micros,
            scenario: "fixed_topology",
        });
        children.push((worker.worker_index, child));
    }

    thread::sleep(Duration::from_millis(150));

    let scheduler = spawn_child(ChildSpec {
        mode: "producer",
        segment: &segment,
        buffer_size,
        events,
        consumers,
        consumer_id: None,
        slow_every: 0,
        slow_micros: 0,
        scenario: "fixed_topology",
    });

    let scheduler_output = wait_with_timeout(scheduler, Duration::from_secs(60), "scheduler");
    assert_child_success(&scheduler_output, "scheduler");
    let scheduler_stdout = String::from_utf8_lossy(&scheduler_output.stdout);
    assert!(
        scheduler_stdout.contains("MP_CHILD_OK mode=producer scenario=fixed_topology"),
        "scheduler completion marker missing\nstdout:\n{scheduler_stdout}"
    );

    for (worker_index, child) in children {
        let worker_role = format!("worker-{worker_index}");
        let worker_output = wait_with_timeout(child, Duration::from_secs(60), &worker_role);
        assert_child_success(&worker_output, &worker_role);

        let worker_stdout = String::from_utf8_lossy(&worker_output.stdout);
        assert!(
            worker_stdout.contains("MP_CHILD_OK mode=consumer"),
            "{worker_role} completion marker missing\nstdout:\n{worker_stdout}"
        );
        assert!(
            worker_stdout.contains(&format!("id={worker_index}")),
            "{worker_role} id marker missing\nstdout:\n{worker_stdout}"
        );
        assert!(
            worker_stdout.contains(&format!("consumed={events}")),
            "{worker_role} consumed count mismatch\nstdout:\n{worker_stdout}"
        );
    }
}

fn run_child_producer() {
    let segment = env::var("MP_SEGMENT").expect("MP_SEGMENT should be set");
    let prefix = env::var("MP_PREFIX").expect("MP_PREFIX should be set");
    let buffer_size: usize = parse_env("MP_BUFFER_SIZE");
    let events: u64 = parse_env("MP_EVENTS");
    let consumers: usize = parse_env("MP_CONSUMERS");
    let scenario = env::var("MP_CHILD_SCENARIO").unwrap_or_else(|_| "basic".to_string());
    let expected_checksum = expected_checksum(events);

    match scenario.as_str() {
        "basic" => {
            run_child_producer_basic(
                &segment,
                &prefix,
                buffer_size,
                events,
                consumers,
                expected_checksum,
            );
        }
        "fixed_topology" => {
            run_child_producer_fixed_topology(
                &segment,
                buffer_size,
                events,
                consumers,
                expected_checksum,
            );
        }
        "backpressure" => {
            run_child_producer_backpressure(
                &segment,
                &prefix,
                buffer_size,
                events,
                consumers,
                expected_checksum,
            );
        }
        other => panic!("unknown MP_CHILD_SCENARIO: {other}"),
    }
}

fn run_child_producer_basic(
    segment: &str,
    prefix: &str,
    buffer_size: usize,
    events: u64,
    consumers: usize,
    expected_checksum: u64,
) {
    let mut producer = build_shared_single_producer::<MpEvent>(segment, buffer_size)
        .discover_consumer_with_prefix_and_interval(consumers, prefix, Duration::from_millis(1))
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

    assert_eq!(produced_checksum, expected_checksum);

    println!(
        "MP_CHILD_OK mode=producer scenario=basic published={events} checksum={produced_checksum}"
    );
}

fn run_child_producer_fixed_topology(
    segment: &str,
    buffer_size: usize,
    events: u64,
    consumers: usize,
    expected_checksum: u64,
) {
    let topology = FixedTopology::new(segment, buffer_size, worker_count_from_usize(consumers))
        .with_coordination_timeout(Duration::from_secs(20));

    let mut producer = topology
        .build_scheduler(MpEvent::default)
        .expect("fixed topology scheduler should build");

    let mut produced_checksum = 0u64;
    for sequence in 0..events {
        let checksum = event_checksum(sequence);
        produced_checksum = produced_checksum.wrapping_add(checksum);

        producer
            .publish_with_timeout(Duration::from_secs(5), |event| {
                event.sequence = sequence;
                event.checksum = checksum;
                event.payload.fill((sequence as u8) ^ 0x3C);
            })
            .expect("fixed topology publish_with_timeout should not time out");

        if sequence % (buffer_size as u64 / 2).max(1) == 0 {
            let _ = producer.wait_until_consumed_with_strategy(
                sequence as i64,
                Duration::from_millis(200),
                AutoWaitStrategy::Block,
            );
        }
    }

    assert_eq!(produced_checksum, expected_checksum);

    println!(
        "MP_CHILD_OK mode=producer scenario=fixed_topology published={events} checksum={produced_checksum}"
    );
}

fn run_child_producer_backpressure(
    segment: &str,
    prefix: &str,
    buffer_size: usize,
    events: u64,
    consumers: usize,
    expected_checksum: u64,
) {
    let mut producer = build_shared_single_producer::<MpEvent>(segment, buffer_size)
        .discover_consumer_with_prefix_and_interval(consumers, prefix, Duration::from_millis(1))
        .wait_for_consumers(consumers as i64, Duration::from_secs(20))
        .build_producer(MpEvent::default)
        .expect("producer should build");

    // Warm up discovery scans before publishing to avoid early overwrite windows.
    for _ in 0..20 {
        let _ = producer.min_gating_sequence();
        thread::sleep(Duration::from_millis(2));
    }

    let mut produced_checksum = 0u64;
    let mut published = 0u64;
    let mut attempts = 0u64;
    let mut backpressure_events = 0u64;
    let deadline = Instant::now() + Duration::from_secs(35);

    while published < events {
        attempts += 1;
        let sequence = published;
        let checksum = event_checksum(sequence);
        match producer.try_publish(|event| {
            event.sequence = sequence;
            event.checksum = checksum;
            event.payload.fill((sequence as u8) ^ 0xA5);
        }) {
            Ok(_) => {
                published += 1;
                produced_checksum = produced_checksum.wrapping_add(checksum);
            }
            Err(RingBufferFull) => {
                backpressure_events += 1;
                thread::yield_now();
            }
        }

        if published == events {
            break;
        }

        if Instant::now() > deadline {
            panic!(
                "producer timed out waiting for consumer: published={published} attempts={attempts} backpressure_events={backpressure_events}"
            );
        }
    }

    assert_eq!(produced_checksum, expected_checksum);

    println!(
        "MP_CHILD_OK mode=producer scenario=backpressure published={published} attempts={attempts} backpressure_events={backpressure_events} checksum={produced_checksum}"
    );
}

fn run_child_consumer() {
    let segment = env::var("MP_SEGMENT").expect("MP_SEGMENT should be set");
    let consumer_id = env::var("MP_CONSUMER_ID").expect("MP_CONSUMER_ID should be set");
    let buffer_size: usize = parse_env("MP_BUFFER_SIZE");
    let events: u64 = parse_env("MP_EVENTS");
    let slow_every: usize = parse_env("MP_SLOW_EVERY");
    let slow_micros: u64 = parse_env("MP_SLOW_MICROS");
    let scenario = env::var("MP_CHILD_SCENARIO").unwrap_or_else(|_| "basic".to_string());

    let attach_deadline = Instant::now() + Duration::from_secs(20);
    let mut consumer = loop {
        let attempt = match scenario.as_str() {
            "fixed_topology" => {
                let worker_index = consumer_id
                    .parse::<usize>()
                    .unwrap_or_else(|e| panic!("invalid worker index {consumer_id}: {e}"));
                FixedTopology::new(
                    &segment,
                    buffer_size,
                    worker_count_from_usize(parse_env("MP_CONSUMERS")),
                )
                .with_coordination_timeout(Duration::from_secs(20))
                .attach_worker::<MpEvent>(worker_index)
            }
            "basic" | "backpressure" => attach_shared_consumer::<MpEvent>(&segment, buffer_size)
                .with_consumer_id(&consumer_id)
                .build_consumer()
                .map_err(Into::into),
            other => panic!("unknown MP_CHILD_SCENARIO: {other}"),
        };

        match attempt {
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

            if slow_every > 0 && consumed % (slow_every as u64) == 0 {
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
        "MP_CHILD_OK mode=consumer scenario={} id={} consumed={} checksum={} ordered={}",
        scenario, consumer_id, consumed, sum, ordered
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
