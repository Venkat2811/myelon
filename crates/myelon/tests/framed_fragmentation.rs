//! Test that `FramedTransport` correctly handles multi-frame (fragmented)
//! payloads under sustained multiprocess load with backpressure.
//!
//! This is the regression test for the `DiscoveryMode::Disabled` bug where
//! `FramedTransportProducer` had zero backpressure and overwrote unread
//! slots when the consumer fell behind on large payloads.
//!
//! Run: cargo test -p myelon --test `framed_fragmentation` -- --nocapture

use myelon::transport::{
    FixedFrame, FramedTransportConsumer, FramedTransportProducer, MyelonWaitStrategy,
};
use std::env;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const FRAME_DATA: usize = 1024; // small frame to force fragmentation
type Frame = FixedFrame<FRAME_DATA>;
const DEPTH: usize = 256;
const PAYLOAD_SIZE: usize = 4000; // 4x frame capacity → 4 frames per message
const NUM_MESSAGES: usize = 5000;

fn segment_name() -> String {
    if let Ok(s) = env::var("FRAG_TEST_SEGMENT") {
        return s;
    }
    disruptor_mp::portable_shm_segment_name("frtst")
}

#[test]
fn fragmented_multiprocess_backpressure() {
    let segment = segment_name();
    let exe = env::current_exe().expect("exe");

    // Create producer (creates ring + enables discovery)
    let mut producer = FramedTransportProducer::<Frame>::create(&segment, DEPTH).expect("create");

    // Spawn consumer child
    let child = Command::new(&exe)
        .arg("--exact")
        .arg("frag_child")
        .arg("--ignored")
        .arg("--nocapture")
        .env("FRAG_TEST_SEGMENT", &segment)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");

    // Wait for consumer to attach, then discover it for backpressure
    std::thread::sleep(Duration::from_millis(500));
    producer.discover_consumers(Duration::from_secs(3));

    // Build payload: deterministic bytes so consumer can verify
    let payload: Vec<u8> = (0..PAYLOAD_SIZE).map(|i| (i % 251) as u8).collect();

    // Publish all messages
    for i in 0..NUM_MESSAGES {
        producer.publish(&payload, (i % 256) as u8);
    }
    // Sentinel
    producer.publish(&[], 255);

    // Wait for child
    let output = child.wait_with_output().expect("wait");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "child failed (status {:?})\nstdout: {stdout}\nstderr: {stderr}",
        output.status.code()
    );
    assert!(
        stdout.contains("VERIFIED_OK"),
        "child did not verify all messages\nstdout: {stdout}\nstderr: {stderr}"
    );
}

#[test]
#[ignore]
fn frag_child() {
    let segment = segment_name();

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut consumer = loop {
        match FramedTransportConsumer::<Frame>::attach(
            &segment,
            DEPTH,
            MyelonWaitStrategy::BusySpin,
        ) {
            Ok(c) => break c,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(e) => panic!("attach: {e}"),
        }
    };

    let expected: Vec<u8> = (0..PAYLOAD_SIZE).map(|i| (i % 251) as u8).collect();
    let mut count = 0usize;

    loop {
        let (kind, data) = consumer.recv_message_blocking();

        if kind == 255 && data.is_empty() {
            break;
        }

        count += 1;

        assert_eq!(
            data.len(),
            PAYLOAD_SIZE,
            "message {count}: got {} bytes, expected {PAYLOAD_SIZE}",
            data.len()
        );

        assert_eq!(data, expected, "message {count}: byte mismatch");
    }

    assert_eq!(
        count, NUM_MESSAGES,
        "expected {NUM_MESSAGES} messages, got {count}"
    );
    println!("VERIFIED_OK: {count} messages × {PAYLOAD_SIZE} bytes, all correct");
}
