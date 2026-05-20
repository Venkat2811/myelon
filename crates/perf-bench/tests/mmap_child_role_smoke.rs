#![cfg(unix)]

use std::process::Command;

fn assert_child_fails_gracefully(harness: &str, role: &str) {
    let exe = env!("CARGO_BIN_EXE_perf-bench-broadcast");
    let output = Command::new(exe)
        .arg(role)
        .env("MYELON_BENCH_BROADCAST_HARNESS", harness)
        .output()
        .expect("spawn perf-bench-broadcast child role");

    assert!(
        !output.status.success(),
        "{harness}/{role} unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(
        output.status.code().is_some(),
        "{harness}/{role} terminated by signal\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
    assert!(
        !stderr.contains("stack overflow") && !stderr.contains("overflowed its stack"),
        "{harness}/{role} hit stack overflow again\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[test]
fn mmap_child_roles_fail_gracefully_without_runtime_setup() {
    let cases = [
        ("raw_ring_mmap", "mmap_multi_msg_consumer"),
        ("raw_myelon_mmap", "my_mmap_multi_msg_consumer"),
        ("monster_mmap", "cons_1m"),
        ("nofrag_all", "raw_mmap_cons_64k"),
        ("myelon_layers", "ml_my_raw_mmap_cons_64k"),
        ("nofrag_mmap", "nf_cons_256k"),
    ];

    for (harness, role) in cases {
        assert_child_fails_gracefully(harness, role);
    }
}
