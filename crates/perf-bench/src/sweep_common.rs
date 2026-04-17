//! Shared utilities for monster sweep benchmarks (SHM + mmap).
//!
//! Extracts common code to avoid duplication between sweep/shm.rs and sweep/mmap.rs.

use std::io::Read as _;
use std::process::{Child, Output};
use std::time::{Duration, Instant};

/// Read an env var as usize with default.
pub fn read_env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Read an env var as u64 with default.
pub fn read_env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Wait for a child process with timeout. Returns stdout/stderr or error.
pub fn wait_timeout(mut child: Child, timeout: Duration) -> Result<Output, String> {
    fn collect(child: &mut Child, status: std::process::ExitStatus) -> Output {
        let mut out = Vec::new();
        let mut err = Vec::new();
        if let Some(mut o) = child.stdout.take() { let _ = o.read_to_end(&mut out); }
        if let Some(mut e) = child.stderr.take() { let _ = e.read_to_end(&mut err); }
        Output { status, stdout: out, stderr: err }
    }
    let start = Instant::now();
    loop {
        if let Some(s) = child.try_wait().map_err(|e| e.to_string())? {
            return Ok(collect(&mut child, s));
        }
        if start.elapsed() >= timeout {
            let _ = child.kill();
            let s = child.wait().map_err(|e| e.to_string())?;
            return Err(format!("timeout; stderr: {}", String::from_utf8_lossy(&collect(&mut child, s).stderr)));
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Extract a numeric value from child process stdout output.
/// Parses lines like "Throughput: 1234567" or "EncodeAvgUs: 3.5".
pub fn extract_value(output: &str, key: &str) -> f64 {
    for line in output.lines() {
        if let Some(rest) = line.strip_prefix(&format!("{key}: ")) {
            if let Some(n) = rest.split_whitespace().next() {
                return n.parse().unwrap_or(0.0);
            }
        }
    }
    0.0
}

/// Extract LatencyJSON from child process stdout.
pub fn extract_latency_json(output: &str) -> Option<crate::latency::LatencyStats> {
    for line in output.lines() {
        if let Some(json) = line.strip_prefix("LatencyJSON: ") {
            return serde_json::from_str(json).ok();
        }
    }
    None
}

/// Format a byte size for display.
pub fn format_size(bytes: usize) -> String {
    if bytes >= 1_048_576 { format!("{}MB", bytes / 1_048_576) }
    else if bytes >= 1024 { format!("{}KB", bytes / 1024) }
    else { format!("{}B", bytes) }
}

/// Format event count for display.
pub fn format_events(n: u64) -> String {
    if n >= 1_000_000 { format!("{}M", n / 1_000_000) }
    else if n >= 1_000 { format!("{}K", n / 1_000) }
    else { format!("{}", n) }
}

/// M3 Max 14-core memory bandwidth constant.
pub const HW_BW_GBS: f64 = 300.0;
