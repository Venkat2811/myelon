use serde_json::Value;
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

fn signal_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn run_signal(backend: &str, latency: bool) -> Value {
    let _guard = signal_lock().lock().expect("signal lock");
    let exe = env!("CARGO_BIN_EXE_perf-bench-signal");
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let out_path = std::env::temp_dir().join(format!("perf-bench-signal-{backend}-{unique}.json"));
    let mut cmd = Command::new(exe);
    cmd.args([
        "--backend",
        backend,
        "--consumers",
        "1",
        "--events",
        "1000",
        "--warmup",
        "100",
        "--json-out",
        out_path.to_str().expect("temp path"),
    ]);
    if latency {
        cmd.arg("--latency");
    }
    let output = cmd.output().expect("run perf-bench-signal");
    assert!(
        output.status.success(),
        "signal run failed for backend={backend} latency={latency}\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value = serde_json::from_slice(&std::fs::read(&out_path).expect("read json report"))
        .expect("json report");
    let _ = std::fs::remove_file(out_path);
    value
}

fn first_latency(value: &Value) -> &Value {
    &value["scenarios"][0]["outcome"]["Throughput"]["latency"]
}

#[test]
fn signal_mode_none_keeps_latency_null() {
    for backend in ["shm", "mmap"] {
        let report = run_signal(backend, false);
        assert!(
            first_latency(&report).is_null(),
            "expected null latency for {backend}"
        );
    }
}

#[test]
fn signal_canonical_latency_mode_emits_latency() {
    for backend in ["shm", "mmap"] {
        let report = run_signal(backend, true);
        assert!(
            first_latency(&report).is_object(),
            "expected latency object for {backend}"
        );
    }
}

#[test]
fn signal_sequence_observer_emits_observer_payload() {
    let _guard = signal_lock().lock().expect("signal lock");
    let exe = env!("CARGO_BIN_EXE_perf-bench-signal");
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let out_path = std::env::temp_dir().join(format!("perf-bench-signal-seqobs-{unique}.json"));
    let output = Command::new(exe)
        .args([
            "--backend",
            "shm",
            "--consumers",
            "1",
            "--events",
            "1000",
            "--warmup",
            "100",
            "--observe-sequences",
            "--sequence-poll-us",
            "1000",
            "--json-out",
            out_path.to_str().expect("temp path"),
        ])
        .output()
        .expect("run perf-bench-signal seqobs");
    assert!(
        output.status.success(),
        "signal seqobs run failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&std::fs::read(&out_path).expect("read json report"))
        .expect("json report");
    let _ = std::fs::remove_file(out_path);
    assert_eq!(value["mode"], "signal_sequence_observer");
    assert!(value["producer"]["throughput_ops_sec"].is_number());
    assert!(value["consumer"]["throughput_ops_sec"].is_number());
    assert!(value["sequence_observer"]["sample_count"].is_number());
}
