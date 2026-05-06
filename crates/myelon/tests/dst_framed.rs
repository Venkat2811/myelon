//! DST-style real multiprocess FramedTransport checks.
//!
//! These tests deliberately use real OS child processes and real SHM-backed
//! framed rings so fragmentation and backpressure behavior is exercised outside
//! of the in-memory DST contract layer.

use dst_fixtures::dst_buggify::ScopedBuggify;
use dst_runner::{BackendKind, DstConfig};
use myelon::transport::{
    FixedFrame, FramedTransportConsumer, FramedTransportProducer, MmapFramedTransportConsumer,
    MmapFramedTransportProducer, MyelonWaitStrategy,
};
use std::env;
use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const CHILD_TEST_NAME: &str = "dst_framed_child";
const FRAME_DATA: usize = 1024;
type Frame = FixedFrame<FRAME_DATA>;
const DEFAULT_DEPTH: usize = 256;
static MMAP_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn segment_name(tag: &str) -> String {
    if let Ok(name) = env::var("DST_FRAMED_SEGMENT") {
        return name;
    }
    disruptor_mp::portable_shm_segment_name(tag)
}

fn mmap_case(tag: &str) -> (PathBuf, String, disruptor_mp::MmapTransportLayout) {
    let pid = std::process::id();
    let seq = MMAP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!("dst_framed_mmap_{pid}_{seq}"));
    let segment = format!("{tag}_{pid}_{seq}");
    let layout = disruptor_mp::MmapTransportLayout::new(root.clone(), segment.clone())
        .expect("valid mmap transport layout");
    (root, segment, layout)
}

fn payload_for(sequence: usize, payload_size: usize) -> Vec<u8> {
    (0..payload_size)
        .map(|index| {
            (sequence as u8)
                .wrapping_add((index as u8).wrapping_mul(17))
                .wrapping_add(11)
        })
        .collect()
}

fn assert_child_success(output: &Output, label: &str) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "{label} failed with status {:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status.code()
    );
    assert!(
        stdout.contains("DST_FRAMED_OK"),
        "{label} missing success marker\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

fn assert_child_alive(child: &mut std::process::Child, label: &str) {
    if let Some(status) = child.try_wait().expect("poll framed child") {
        let mut stdout = String::new();
        let mut stderr = String::new();
        if let Some(mut pipe) = child.stdout.take() {
            let _ = pipe.read_to_string(&mut stdout);
        }
        if let Some(mut pipe) = child.stderr.take() {
            let _ = pipe.read_to_string(&mut stderr);
        }
        panic!(
            "{label} exited early with status {:?}\nstdout:\n{}\nstderr:\n{}",
            status.code(),
            stdout,
            stderr
        );
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "DST framed test helper keeps child-process knobs explicit"
)]
fn spawn_framed_child(
    backend: &str,
    segment: &str,
    depth: usize,
    payload_size: usize,
    messages: usize,
    slow_micros: u64,
    root: Option<&std::path::Path>,
    consumer_id: Option<&str>,
) -> std::process::Child {
    spawn_framed_child_custom(
        "stream",
        backend,
        segment,
        depth,
        payload_size,
        messages,
        slow_micros,
        root,
        consumer_id,
        0,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "DST framed test helper keeps restart and transport knobs explicit"
)]
fn spawn_framed_child_custom(
    case: &str,
    backend: &str,
    segment: &str,
    depth: usize,
    payload_size: usize,
    messages: usize,
    slow_micros: u64,
    root: Option<&std::path::Path>,
    consumer_id: Option<&str>,
    start_sequence: usize,
) -> std::process::Child {
    let mut cmd = Command::new(env::current_exe().expect("exe"));
    cmd.arg("--exact")
        .arg(CHILD_TEST_NAME)
        .arg("--ignored")
        .arg("--nocapture")
        .env("DST_FRAMED_CASE", case)
        .env("DST_FRAMED_BACKEND", backend)
        .env("DST_FRAMED_SEGMENT", segment)
        .env("DST_FRAMED_DEPTH", depth.to_string())
        .env("DST_FRAMED_PAYLOAD_SIZE", payload_size.to_string())
        .env("DST_FRAMED_MESSAGES", messages.to_string())
        .env("DST_FRAMED_SLOW_MICROS", slow_micros.to_string())
        .env("DST_FRAMED_START_SEQUENCE", start_sequence.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(root) = root {
        cmd.env("DST_FRAMED_ROOT", root);
    }
    if let Some(consumer_id) = consumer_id {
        cmd.env("DST_FRAMED_CONSUMER_ID", consumer_id);
    }
    cmd.spawn().expect("spawn framed child")
}

fn run_framed_fuzz_seed(seed: u64, nightly: bool) {
    let _buggify = ScopedBuggify::new(seed);
    let config = DstConfig::from_seed(seed);
    let payload_cap = if nightly { 131_072 } else { 32_768 };
    let payload_floor = 256usize;
    let payload_size = config.payload_size.min(payload_cap).max(payload_floor);
    let byte_budget = if nightly {
        2_000_000usize
    } else {
        400_000usize
    };
    let messages = (byte_budget / payload_size.max(1)).clamp(8, if nightly { 96 } else { 32 });
    let depth = config
        .ring_depth
        .min(if nightly { 512 } else { 256 })
        .max(64);
    let slow_micros = if seed & 1 == 0 { 0 } else { 250 };

    match config.backend {
        BackendKind::Shm => {
            let segment = segment_name("ffz");
            let mut producer =
                FramedTransportProducer::<Frame>::create(&segment, depth).expect("create producer");
            let child = spawn_framed_child(
                "shm",
                &segment,
                depth,
                payload_size,
                messages,
                slow_micros,
                None,
                None,
            );

            std::thread::sleep(Duration::from_millis(200));
            producer.discover_consumers(Duration::from_secs(3));
            for sequence in 0..messages {
                producer.publish(&payload_for(sequence, payload_size), (sequence % 251) as u8);
            }
            producer.publish(&[], 255);

            let output = child
                .wait_with_output()
                .expect("wait for shm framed fuzz child");
            assert_child_success(&output, "shm framed fuzz child");
        }
        BackendKind::Mmap => {
            let (_root, segment, layout) = mmap_case("ffzm");
            let consumer_id = "dst_framed_fuzz";
            let mut producer = MmapFramedTransportProducer::<Frame>::create(layout.clone(), depth)
                .expect("create mmap producer");
            let child = spawn_framed_child(
                "mmap",
                &segment,
                depth,
                payload_size,
                messages,
                slow_micros,
                Some(layout.root_dir()),
                Some(consumer_id),
            );

            assert!(
                producer.wait_for_consumers_ready(1, Duration::from_secs(3)),
                "mmap framed fuzz producer timed out waiting for consumer (seed {seed:#x})"
            );
            for sequence in 0..messages {
                producer.publish(&payload_for(sequence, payload_size), (sequence % 251) as u8);
            }
            producer.publish(&[], 255);

            let output = child
                .wait_with_output()
                .expect("wait for mmap framed fuzz child");
            assert_child_success(&output, "mmap framed fuzz child");
        }
    }
}

#[test]
fn dst_fragmentation_correctness_shm() {
    let segment = segment_name("fdst");
    let payload_size = 4096;
    let messages = 1000usize;
    let mut producer =
        FramedTransportProducer::<Frame>::create(&segment, DEFAULT_DEPTH).expect("create producer");

    let mut child = spawn_framed_child(
        "shm",
        &segment,
        DEFAULT_DEPTH,
        payload_size,
        messages,
        0,
        None,
        None,
    );

    std::thread::sleep(Duration::from_millis(400));
    producer.discover_consumers(Duration::from_secs(3));

    for sequence in 0..messages {
        assert_child_alive(&mut child, "framed shm restart child");
        producer.publish(&payload_for(sequence, payload_size), (sequence % 251) as u8);
    }
    producer.publish(&[], 255);

    let output = child.wait_with_output().expect("wait for framed child");
    assert_child_success(&output, "fragmentation child");
}

#[test]
fn dst_backpressure_enforced_with_slow_consumer_shm() {
    let segment = segment_name("fbp");
    let depth = 64usize;
    let payload_size = 16 * 1024;
    let messages = 256usize;
    let mut producer =
        FramedTransportProducer::<Frame>::create(&segment, depth).expect("create producer");

    let mut child = spawn_framed_child(
        "shm",
        &segment,
        depth,
        payload_size,
        messages,
        1000,
        None,
        None,
    );

    std::thread::sleep(Duration::from_millis(400));
    producer.discover_consumers(Duration::from_secs(3));

    let start = Instant::now();
    for sequence in 0..messages {
        assert_child_alive(&mut child, "framed mmap restart child");
        producer.publish(&payload_for(sequence, payload_size), (sequence % 251) as u8);
    }
    producer.publish(&[], 255);
    let elapsed = start.elapsed();

    let output = child.wait_with_output().expect("wait for framed child");
    assert_child_success(&output, "slow-consumer child");

    assert!(
        elapsed >= Duration::from_millis(50),
        "producer completed too quickly for a slow-consumer backpressure scenario: {elapsed:?}"
    );
}

#[test]
fn dst_fragmentation_correctness_mmap() {
    let (_root, segment, layout) = mmap_case("fdstm");
    let payload_size = 4096;
    let messages = 1000usize;
    let consumer_id = "dst_framed_mmap_consumer";
    let mut producer = MmapFramedTransportProducer::<Frame>::create(layout.clone(), DEFAULT_DEPTH)
        .expect("create mmap producer");

    let child = spawn_framed_child(
        "mmap",
        &segment,
        DEFAULT_DEPTH,
        payload_size,
        messages,
        0,
        Some(layout.root_dir()),
        Some(consumer_id),
    );

    assert!(
        producer.wait_for_consumers_ready(1, Duration::from_secs(3)),
        "mmap framed producer timed out waiting for consumer"
    );

    for sequence in 0..messages {
        producer.publish(&payload_for(sequence, payload_size), (sequence % 251) as u8);
    }
    producer.publish(&[], 255);

    let output = child
        .wait_with_output()
        .expect("wait for mmap framed child");
    assert_child_success(&output, "mmap fragmentation child");
}

#[test]
fn dst_backpressure_enforced_with_slow_consumer_mmap() {
    let (_root, segment, layout) = mmap_case("fbpm");
    let depth = 64usize;
    let payload_size = 16 * 1024;
    let messages = 256usize;
    let consumer_id = "dst_framed_mmap_slow";
    let mut producer = MmapFramedTransportProducer::<Frame>::create(layout.clone(), depth)
        .expect("create mmap producer");

    let child = spawn_framed_child(
        "mmap",
        &segment,
        depth,
        payload_size,
        messages,
        1000,
        Some(layout.root_dir()),
        Some(consumer_id),
    );

    assert!(
        producer.wait_for_consumers_ready(1, Duration::from_secs(3)),
        "mmap framed producer timed out waiting for slow consumer"
    );

    let start = Instant::now();
    for sequence in 0..messages {
        producer.publish(&payload_for(sequence, payload_size), (sequence % 251) as u8);
    }
    producer.publish(&[], 255);
    let elapsed = start.elapsed();

    let output = child
        .wait_with_output()
        .expect("wait for mmap framed child");
    assert_child_success(&output, "mmap slow-consumer child");

    assert!(
        elapsed >= Duration::from_millis(50),
        "mmap producer completed too quickly for a slow-consumer backpressure scenario: {elapsed:?}"
    );
}

#[test]
fn dst_consumer_restart_resumes_framed_shm() {
    let segment = segment_name("frst");
    let depth = 64usize;
    let payload_size = 4 * 1024;
    let messages = 128usize;
    let restart_after = 24usize;
    let offline_backlog = 8usize;
    let consumer_id = "dfrs";
    let mut producer =
        FramedTransportProducer::<Frame>::create(&segment, depth).expect("create producer");
    let child = spawn_framed_child_custom(
        "restart",
        "shm",
        &segment,
        depth,
        payload_size,
        restart_after,
        0,
        None,
        Some(consumer_id),
        0,
    );

    std::thread::sleep(Duration::from_millis(250));
    assert!(
        producer.discover_consumer_id(consumer_id, Duration::from_secs(3)),
        "shm framed producer timed out waiting for initial restart consumer"
    );

    for sequence in 0..restart_after {
        producer.publish(&payload_for(sequence, payload_size), (sequence % 251) as u8);
        std::thread::sleep(Duration::from_micros(250));
    }

    let output = child
        .wait_with_output()
        .expect("wait for initial framed shm child");
    assert_child_success(&output, "initial framed shm child");

    for sequence in restart_after..restart_after + offline_backlog {
        producer.publish(&payload_for(sequence, payload_size), (sequence % 251) as u8);
    }

    let mut child = spawn_framed_child_custom(
        "restart",
        "shm",
        &segment,
        depth,
        payload_size,
        messages - restart_after,
        0,
        None,
        Some(consumer_id),
        restart_after,
    );
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        producer.discover_consumer_id(consumer_id, Duration::from_secs(3)),
        "shm framed producer timed out waiting for restarted consumer"
    );

    for sequence in restart_after + offline_backlog..messages {
        assert_child_alive(&mut child, "framed shm restarted child");
        producer.publish(&payload_for(sequence, payload_size), (sequence % 251) as u8);
        std::thread::sleep(Duration::from_micros(250));
    }

    let output = child
        .wait_with_output()
        .expect("wait for restarted framed shm child");
    assert_child_success(&output, "restarted framed shm child");
}

#[test]
fn dst_consumer_restart_resumes_framed_mmap() {
    let (_root, segment, layout) = mmap_case("frstm");
    let depth = 64usize;
    let payload_size = 4 * 1024;
    let messages = 128usize;
    let restart_after = 24usize;
    let offline_backlog = 8usize;
    let consumer_id = "dst_framed_restart_mmap";
    let mut producer = MmapFramedTransportProducer::<Frame>::create(layout.clone(), depth)
        .expect("create mmap producer");
    let child = spawn_framed_child_custom(
        "restart",
        "mmap",
        &segment,
        depth,
        payload_size,
        restart_after,
        0,
        Some(layout.root_dir()),
        Some(consumer_id),
        0,
    );

    assert!(
        producer.wait_for_consumers_ready(1, Duration::from_secs(3)),
        "mmap framed producer timed out waiting for restart consumer"
    );

    for sequence in 0..restart_after {
        producer.publish(&payload_for(sequence, payload_size), (sequence % 251) as u8);
        std::thread::sleep(Duration::from_micros(250));
    }

    let output = child
        .wait_with_output()
        .expect("wait for initial framed mmap child");
    assert_child_success(&output, "initial framed mmap child");

    for sequence in restart_after..restart_after + offline_backlog {
        producer.publish(&payload_for(sequence, payload_size), (sequence % 251) as u8);
    }

    let mut child = spawn_framed_child_custom(
        "restart",
        "mmap",
        &segment,
        depth,
        payload_size,
        messages - restart_after,
        0,
        Some(layout.root_dir()),
        Some(consumer_id),
        restart_after,
    );
    assert!(
        producer.wait_for_consumers_ready(1, Duration::from_secs(3)),
        "mmap framed producer timed out waiting for restarted consumer"
    );

    for sequence in restart_after + offline_backlog..messages {
        assert_child_alive(&mut child, "framed mmap restarted child");
        producer.publish(&payload_for(sequence, payload_size), (sequence % 251) as u8);
        std::thread::sleep(Duration::from_micros(250));
    }

    let output = child
        .wait_with_output()
        .expect("wait for restarted framed mmap child");
    assert_child_success(&output, "restarted framed mmap child");
}

#[test]
#[ignore]
fn dst_fuzz_framed_ci_seed_matrix() {
    for seed in 0x2100..0x2164 {
        if seed % 10 == 0 {
            eprintln!("framed ci fuzz seed={seed:#x}");
        }
        run_framed_fuzz_seed(seed, false);
    }
}

#[test]
#[ignore]
fn dst_fuzz_framed_nightly_seed_matrix() {
    for seed in 0x2200..0x25e8 {
        if seed % 50 == 0 {
            eprintln!("framed nightly fuzz seed={seed:#x}");
        }
        run_framed_fuzz_seed(seed, true);
    }
}

#[test]
#[ignore]
fn dst_framed_child() {
    let case = env::var("DST_FRAMED_CASE").unwrap_or_else(|_| "stream".to_string());
    let backend = env::var("DST_FRAMED_BACKEND").unwrap_or_else(|_| "shm".to_string());
    let segment = env::var("DST_FRAMED_SEGMENT").expect("DST_FRAMED_SEGMENT should be set");
    let payload_size: usize = env::var("DST_FRAMED_PAYLOAD_SIZE")
        .expect("DST_FRAMED_PAYLOAD_SIZE should be set")
        .parse()
        .expect("payload size should parse");
    let depth: usize = env::var("DST_FRAMED_DEPTH")
        .expect("DST_FRAMED_DEPTH should be set")
        .parse()
        .expect("depth should parse");
    let messages: usize = env::var("DST_FRAMED_MESSAGES")
        .expect("DST_FRAMED_MESSAGES should be set")
        .parse()
        .expect("message count should parse");
    let start_sequence: usize = env::var("DST_FRAMED_START_SEQUENCE")
        .ok()
        .map(|raw| raw.parse().expect("start sequence should parse"))
        .unwrap_or(0);
    let slow_micros: u64 = env::var("DST_FRAMED_SLOW_MICROS")
        .expect("DST_FRAMED_SLOW_MICROS should be set")
        .parse()
        .expect("slow micros should parse");
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut consumer = loop {
        match backend.as_str() {
            "shm" => {
                let attach = if let Ok(consumer_id) = env::var("DST_FRAMED_CONSUMER_ID") {
                    FramedTransportConsumer::<Frame>::attach_with_consumer_id(
                        &segment,
                        depth,
                        &consumer_id,
                        MyelonWaitStrategy::BusySpin,
                    )
                } else {
                    FramedTransportConsumer::<Frame>::attach(
                        &segment,
                        depth,
                        MyelonWaitStrategy::BusySpin,
                    )
                };
                match attach {
                    Ok(consumer) => break FramedConsumer::Shm(consumer),
                    Err(_) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(25))
                    }
                    Err(err) => panic!("attach framed consumer: {err}"),
                }
            }
            "mmap" => {
                let root = env::var("DST_FRAMED_ROOT").expect("DST_FRAMED_ROOT should be set");
                let consumer_id = env::var("DST_FRAMED_CONSUMER_ID")
                    .expect("DST_FRAMED_CONSUMER_ID should be set");
                let layout = disruptor_mp::MmapTransportLayout::new(root, &segment)
                    .expect("valid mmap framed layout");
                match MmapFramedTransportConsumer::<Frame>::attach(
                    layout,
                    depth,
                    &consumer_id,
                    MyelonWaitStrategy::BusySpin,
                ) {
                    Ok(consumer) => break FramedConsumer::Mmap(consumer),
                    Err(_) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(25))
                    }
                    Err(err) => panic!("attach mmap framed consumer: {err}"),
                }
            }
            other => panic!("unsupported framed backend: {other}"),
        }
    };

    for offset in 0..messages {
        let sequence = start_sequence + offset;
        let (kind, data) = match &mut consumer {
            FramedConsumer::Shm(consumer) => consumer.recv_message_blocking_owned(),
            FramedConsumer::Mmap(consumer) => consumer.recv_message_blocking_owned(),
        };
        assert_eq!(
            kind,
            (sequence % 251) as u8,
            "message kind mismatch at {sequence}"
        );
        assert_eq!(
            data,
            payload_for(sequence, payload_size),
            "payload mismatch at {sequence}"
        );
        if slow_micros > 0 {
            std::thread::sleep(Duration::from_micros(slow_micros));
        }
    }

    if case == "stream" {
        let (kind, data) = match &mut consumer {
            FramedConsumer::Shm(consumer) => consumer.recv_message_blocking_owned(),
            FramedConsumer::Mmap(consumer) => consumer.recv_message_blocking_owned(),
        };
        assert_eq!(kind, 255, "sentinel kind mismatch");
        assert!(data.is_empty(), "sentinel payload should be empty");
    }
    println!("DST_FRAMED_OK messages={messages} payload_size={payload_size}");
}

#[expect(
    clippy::large_enum_variant,
    reason = "test-only transport enum favors direct storage over extra boxing noise"
)]
enum FramedConsumer {
    Shm(FramedTransportConsumer<Frame>),
    Mmap(MmapFramedTransportConsumer<Frame>),
}
