//! Reproduction test for batch=256 codec corruption bug.
//! Sends a large payload (>64KB, requires fragmentation) through
//! SHM FramedTransport in a true multiprocess setup and verifies
//! byte-for-byte correctness.

use myelon::transport::{
    FixedFrame, FramedTransportConsumer, FramedTransportProducer, MyelonWaitStrategy,
};
use std::env;
use std::io::Read as _;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

// Use a 4KB frame (not 64KB) so fragmentation happens at smaller payloads,
// keeping the test fast while still exercising multi-frame reassembly.
const FRAME_DATA_BYTES: usize = 4 * 1024 - 12;
type Frame = FixedFrame<FRAME_DATA_BYTES>;
const BUFFER_DEPTH: usize = 512;

fn get_segment() -> String {
    if let Ok(name) = env::var("FRAG_SEGMENT") {
        return name;
    }
    disruptor_mp::portable_shm_segment_name("frag")
}

#[test]
fn test_fragmented_multiprocess_correctness() {
    let segment = get_segment();
    let exe = env::current_exe().expect("current_exe");

    // Create producer (creates ring)
    let mut producer =
        FramedTransportProducer::<Frame>::create(&segment, BUFFER_DEPTH).expect("create producer");

    // Spawn consumer child
    let child = Command::new(&exe)
        .arg("--exact")
        .arg("frag_consumer_child")
        .arg("--ignored")
        .arg("--nocapture")
        .env("FRAG_SEGMENT", &segment)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn consumer");

    // Wait for consumer to attach, then trigger discovery so the
    // producer's backpressure barrier knows about the consumer.
    // The consumer needs time to: start process, attach SHM, create cursor.
    // Then discovery needs 1+ scan intervals (100ms) to find the cursor.
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        producer.discover_consumers(Duration::from_secs(1)),
        "producer failed to discover consumer"
    );

    // Payload larger than frame capacity (4084 bytes) to force fragmentation.
    // 16KB = 4 frames per message.
    let payload_size = 16 * 1024;
    let payload: Vec<u8> = (0..payload_size).map(|i| (i % 251) as u8).collect();

    assert!(
        payload.len() > FRAME_DATA_BYTES,
        "payload {} must exceed frame capacity {} to test fragmentation",
        payload.len(),
        FRAME_DATA_BYTES
    );

    // 500 messages × 16KB = 8MB total. With 4KB frames that's 4 frames per
    // message = 2000 frames through a 512-slot ring = ~4 ring wraps.
    // Enough to catch the lapping bug while finishing in seconds.
    let num_messages = 500u64;
    for i in 0..num_messages {
        producer.publish(&payload, (i % 256) as u8);
    }

    // Send a sentinel (empty payload, kind=255) to signal done
    producer.publish(&[], 255);

    // Wait for child
    let output = child.wait_with_output().expect("wait child");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        panic!("Consumer child failed!\nstdout:\n{stdout}\nstderr:\n{stderr}");
    }

    assert!(
        stdout.contains("ALL_OK"),
        "Consumer did not report ALL_OK\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

#[test]
#[ignore]
fn frag_consumer_child() {
    let segment = get_segment();

    // Retry attach — use raw consumer for frame-level debugging
    use disruptor_mp::{SharedDisruptorBuilder, SharedMemoryConfig};
    use myelon::transport::{
        frame_flags, is_last_frame, is_single_frame, FrameMeta, FramedTransportFrame,
    };

    let config = SharedMemoryConfig {
        name: segment.clone(),
        buffer_size: BUFFER_DEPTH,
        element_size: std::mem::size_of::<Frame>(),
        create: false,
    };

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut raw_consumer = loop {
        match SharedDisruptorBuilder::<Frame>::new(config.clone()).build_consumer() {
            Ok(c) => {
                eprintln!(
                    "Consumer attached, current_sequence={}, consumer_id={}",
                    c.current_sequence(),
                    c.consumer_id()
                );
                break c;
            }
            Err(e) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(e) => panic!("attach failed: {e}"),
        }
    };

    // Manual recv_framed_message with logging
    fn recv_debug(
        raw_consumer: &mut disruptor_mp::SharedConsumer<Frame>,
        msg_count: u64,
    ) -> (u8, Vec<u8>) {
        let (_, first_frame) = raw_consumer.consume_next();
        let first = first_frame.frame_meta();

        let is_first_flag = first.flags & 0b01 != 0;
        if msg_count <= 10100 {
            eprintln!(
                "  [msg {}] frame0: flags={:#04b} kind={} msg_id={} len={}{}",
                msg_count,
                first.flags,
                first.kind,
                first.msg_id,
                first.len,
                if !is_first_flag {
                    " *** NOT FIRST ***"
                } else {
                    ""
                }
            );
        }

        if is_single_frame(first.flags) {
            return (first.kind, first.data.to_vec());
        }

        let mut payload = Vec::with_capacity(first.len.max(1024));
        payload.extend_from_slice(first.data);
        let msg_id = first.msg_id;
        let kind = first.kind;

        if is_last_frame(first.flags) {
            return (kind, payload);
        }

        let mut frame_count = 1u32;
        loop {
            let (_, frame) = raw_consumer.consume_next();
            let frame = frame.frame_meta();
            frame_count += 1;

            if frame.msg_id != msg_id {
                if msg_count < 40 {
                    eprintln!(
                        "  [msg {}] SKIPPING frame with wrong msg_id: expected={} got={} flags={:#04b} len={}",
                        msg_count, msg_id, frame.msg_id, frame.flags, frame.len
                    );
                }
                continue;
            }

            payload.extend_from_slice(frame.data);
            if is_last_frame(frame.flags) {
                return (kind, payload);
            }
        }
    }

    let expected_size = 16 * 1024;
    let expected: Vec<u8> = (0..expected_size).map(|i| (i % 251) as u8).collect();
    let mut count = 0u64;

    loop {
        let (kind, data) = recv_debug(&mut raw_consumer, count + 1);

        if kind == 255 && data.is_empty() {
            // Sentinel — done
            break;
        }

        count += 1;

        if data.len() != expected_size {
            eprintln!(
                "MESSAGE {count}: SIZE MISMATCH: got {} bytes, expected {expected_size} (diff={})",
                data.len(),
                expected_size as i64 - data.len() as i64,
            );
            // Check if data is a prefix of expected
            let check_len = data.len().min(expected_size);
            let mut first_bad = None;
            for j in 0..check_len {
                if data[j] != expected[j] {
                    first_bad = Some(j);
                    break;
                }
            }
            if let Some(j) = first_bad {
                eprintln!("  First byte mismatch at offset {j}");
            } else {
                eprintln!("  Data is a VALID PREFIX of expected (truncated, not corrupted)");
            }
            std::process::exit(1);
        }

        // Byte-for-byte verification
        for (j, (&got, &exp)) in data.iter().zip(expected.iter()).enumerate() {
            if got != exp {
                eprintln!(
                    "MESSAGE {count}: BYTE MISMATCH at offset {j}: got {got:#04x}, expected {exp:#04x}"
                );
                // Show context
                let start = j.saturating_sub(8);
                let end = (j + 8).min(data.len());
                eprintln!("  got:      {:?}", &data[start..end]);
                eprintln!("  expected: {:?}", &expected[start..end]);
                std::process::exit(1);
            }
        }
    }

    println!("ALL_OK: {count} messages verified byte-for-byte ({expected_size} bytes each)");
}
