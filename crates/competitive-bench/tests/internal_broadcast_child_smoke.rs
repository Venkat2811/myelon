#![cfg(unix)]

use std::process::Command;

fn assert_consumer_fails_gracefully(adapter: &str, size: usize) {
    let exe = env!("CARGO_BIN_EXE_internal_broadcast");
    let result_path = std::env::temp_dir().join(format!(
        "internal_broadcast_smoke_{}_{}_{}.json",
        adapter.replace('-', "_"),
        size,
        std::process::id()
    ));
    let output = Command::new(exe)
        .arg("--mode")
        .arg("consumer")
        .arg("--adapter")
        .arg(adapter)
        .arg("--base")
        .arg(format!("cbcast_smoke_{}_{}", std::process::id(), size))
        .arg("--message-size")
        .arg(size.to_string())
        .arg("--warmup")
        .arg("0")
        .arg("--num-messages")
        .arg("1")
        .arg("--consumers")
        .arg("4")
        .arg("--consumer-id")
        .arg("0")
        .arg("--result-path")
        .arg(&result_path)
        .env("COMP_BENCH_ATTACH_TIMEOUT_MS", "20")
        .env("COMP_BENCH_COORD_TIMEOUT_MS", "20")
        .output()
        .expect("spawn internal_broadcast child consumer");

    assert!(
        !output.status.success(),
        "{adapter}/{size} unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(
        output.status.code().is_some(),
        "{adapter}/{size} terminated by signal\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
    assert!(
        !stderr.contains("stack overflow") && !stderr.contains("overflowed its stack"),
        "{adapter}/{size} hit stack overflow again\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[test]
fn internal_broadcast_consumers_fail_gracefully_without_runtime_setup() {
    for adapter in [
        "disruptor-shm",
        "disruptor-mmap",
        "myelon-raw-shm",
        "myelon-raw-mmap",
    ] {
        assert_consumer_fails_gracefully(adapter, 8 * 1024 * 1024);
    }
}
