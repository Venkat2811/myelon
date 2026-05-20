//! Zero-overhead benchmark lifecycle logging.
//!
//! `BenchLog` provides per-process logging that costs ~10-50ns per entry
//! during timed sections (memcpy to pre-allocated buffer, no syscalls).
//! All output is flushed after the timed section completes.
//!
//! Usage:
//! ```ignore
//! let mut log = BenchLog::new("producer", 1024 * 1024); // 1MB buffer
//!
//! log.event("ring created");
//! log.event("waiting for consumers");
//! log.event("warmup start");
//!
//! // --- timed section ---
//! for i in 0..events {
//!     // ~10ns overhead per log entry (memcpy only)
//!     if i % 10000 == 0 { log.event("progress 10K"); }
//!     producer.publish(...);
//! }
//! // --- end timed section ---
//!
//! log.event("measurement complete");
//! log.flush_to_stdout(); // actual I/O happens here
//! ```
//!
//! Log format (JSON lines):
//! ```json
//! {"ts":1713385200000000,"pid":12345,"role":"producer","msg":"ring created"}
//! ```

use std::fmt::Write;

const DEFAULT_LOG_DIR: &str = "output/logs";

/// Resolve the log output directory.
///
/// Priority:
/// 1. `MYELON_BENCH_LOG_DIR` env var (explicit override)
/// 2. `MYELON_BENCH_OUT_DIR` env var + `/logs` suffix (run output dir)
/// 3. Default: `output/logs` (relative to cwd, gitignored)
pub fn log_dir() -> String {
    if let Ok(dir) = std::env::var("MYELON_BENCH_LOG_DIR") {
        return dir;
    }
    if let Ok(out_dir) = std::env::var("MYELON_BENCH_OUT_DIR") {
        return format!("{out_dir}/logs");
    }
    DEFAULT_LOG_DIR.to_string()
}

/// Check if logging is enabled (default: yes, disable with `MYELON_BENCH_LOG=0`).
pub fn is_enabled() -> bool {
    std::env::var("MYELON_BENCH_LOG")
        .map(|v| v != "0")
        .unwrap_or(true)
}

/// Pre-allocated log buffer for zero-overhead lifecycle logging.
///
/// Each entry is a timestamp + role + message written to a `Vec<u8>` via
/// `write!()` (~10-50ns, memcpy only — no syscalls, no locks, no heap alloc).
///
/// On `Drop`, automatically writes to `output/logs/<role>_<pid>_<ts>.jsonl`
/// (or `MYELON_BENCH_LOG_DIR` / `MYELON_BENCH_OUT_DIR/logs` if set).
/// Disable entirely with `MYELON_BENCH_LOG=0`.
pub struct BenchLog {
    buf: String,
    pid: u32,
    role: String,
    count: usize,
    auto_flush: bool,
}

impl BenchLog {
    /// Create a new log buffer with the given role and capacity in bytes.
    /// Auto-flushes to `output/logs/` on Drop (configurable via env vars).
    pub fn new(role: &str, capacity: usize) -> Self {
        let enabled = is_enabled();
        let mut log = Self {
            buf: String::with_capacity(if enabled { capacity } else { 0 }),
            pid: std::process::id(),
            role: role.to_string(),
            count: 0,
            auto_flush: enabled,
        };
        if enabled {
            log.event("log_start");
        }
        log
    }

    /// Create a log with default 1MB capacity.
    pub fn default_capacity(role: &str) -> Self {
        Self::new(role, 1024 * 1024)
    }

    /// Create a no-op log (zero overhead, no buffer allocation).
    pub fn disabled() -> Self {
        Self {
            buf: String::new(),
            pid: std::process::id(),
            role: String::new(),
            count: 0,
            auto_flush: false,
        }
    }

    /// Record a log entry. ~10-50ns (memcpy to pre-allocated buffer).
    /// No-op if logging is disabled.
    ///
    /// Safe to call inside timed sections — no syscalls, no locks.
    #[inline]
    pub fn event(&mut self, msg: &str) {
        if !self.auto_flush {
            return;
        }
        let ts = crate::infra::events::nanos_now();
        let _ = writeln!(
            &mut self.buf,
            "{{\"ts\":{},\"pid\":{},\"role\":\"{}\",\"msg\":\"{}\"}}",
            ts, self.pid, self.role, msg
        );
        self.count += 1;
    }

    /// Record a log entry with a numeric value. ~10-50ns.
    /// No-op if logging is disabled.
    #[inline]
    pub fn event_val(&mut self, msg: &str, val: u64) {
        if !self.auto_flush {
            return;
        }
        let ts = crate::infra::events::nanos_now();
        let _ = writeln!(
            &mut self.buf,
            "{{\"ts\":{},\"pid\":{},\"role\":\"{}\",\"msg\":\"{}\",\"val\":{}}}",
            ts, self.pid, self.role, msg, val
        );
        self.count += 1;
    }

    /// Number of entries recorded.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Flush all entries to stdout. Call AFTER the timed section.
    pub fn flush_to_stdout(&self) {
        use std::io::Write as IoWrite;
        let _ = std::io::stdout().write_all(self.buf.as_bytes());
    }

    /// Flush all entries to stderr. Call AFTER the timed section.
    pub fn flush_to_stderr(&self) {
        use std::io::Write as IoWrite;
        let _ = std::io::stderr().write_all(self.buf.as_bytes());
    }

    /// Write all entries to a file. Call AFTER the timed section.
    pub fn write_to_file(&self, path: &str) -> std::io::Result<()> {
        std::fs::write(path, &self.buf)
    }

    /// Get the raw log content as a string.
    pub fn as_str(&self) -> &str {
        &self.buf
    }

    /// Reset the buffer for reuse.
    pub fn reset(&mut self) {
        self.buf.clear();
        self.count = 0;
    }

    /// Generate the auto-flush file path.
    fn auto_log_path(&self) -> (String, std::path::PathBuf) {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let dir = log_dir();
        let path = std::path::PathBuf::from(&dir).join(format!(
            "{}_{}_{}_{}.jsonl",
            self.role, self.pid, ts, self.count
        ));
        (dir, path)
    }
}

impl Drop for BenchLog {
    fn drop(&mut self) {
        if !self.auto_flush || self.count == 0 {
            return;
        }
        self.event("log_end");
        let (dir, path) = self.auto_log_path();
        if let Err(e) = std::fs::create_dir_all(&dir) {
            eprintln!("[BenchLog] failed to create {dir}: {e}");
            return;
        }
        match std::fs::write(&path, &self.buf) {
            Ok(_) => eprintln!("[BenchLog] {} entries -> {}", self.count, path.display()),
            Err(e) => eprintln!("[BenchLog] failed to write {}: {e}", path.display()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a test log that doesn't auto-flush (no file I/O during tests).
    fn test_log(role: &str) -> BenchLog {
        BenchLog {
            buf: String::with_capacity(4096),
            pid: std::process::id(),
            role: role.to_string(),
            count: 0,
            auto_flush: false, // disable Drop file write in tests
        }
    }

    #[test]
    fn test_basic_logging() {
        let mut log = test_log("test");
        log.auto_flush = true; // enable event recording but not Drop
        log.event("hello");
        log.event("world");
        assert_eq!(log.count(), 2);
        let content = log.as_str();
        assert!(content.contains("\"role\":\"test\""));
        assert!(content.contains("\"msg\":\"hello\""));
        assert!(content.contains("\"msg\":\"world\""));
        log.auto_flush = false; // prevent Drop write
    }

    #[test]
    fn test_event_val() {
        let mut log = test_log("producer");
        log.auto_flush = true;
        log.event_val("events_published", 100000);
        assert!(log.as_str().contains("\"val\":100000"));
        log.auto_flush = false;
    }

    #[test]
    fn test_json_lines_format() {
        let mut log = test_log("consumer");
        log.auto_flush = true;
        log.event("start");
        log.event("end");
        let lines: Vec<&str> = log.as_str().lines().collect();
        assert_eq!(lines.len(), 2);
        for line in &lines {
            let _: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
        }
        log.auto_flush = false;
    }

    #[test]
    fn test_reset() {
        let mut log = test_log("test");
        log.auto_flush = true;
        log.event("first");
        assert_eq!(log.count(), 1);
        log.reset();
        assert_eq!(log.count(), 0);
        assert!(log.as_str().is_empty());
        log.auto_flush = false;
    }

    #[test]
    fn test_overhead_is_low() {
        let mut log = test_log("perf");
        log.auto_flush = true;
        log.buf = String::with_capacity(10 * 1024 * 1024);
        let start = std::time::Instant::now();
        for i in 0..100_000 {
            log.event_val("tick", i);
        }
        let elapsed = start.elapsed();
        let ns_per_entry = elapsed.as_nanos() / 100_000;
        // This is a guardrail, not a microbenchmark. Under full-suite load on slower CI
        // or contention-heavy developer boxes, the per-entry cost can drift above the
        // tighter steady-state number while still remaining comfortably below any
        // threshold that would matter for actual benchmark logging.
        assert!(
            ns_per_entry < 1_000,
            "overhead too high: {}ns/entry",
            ns_per_entry
        );
        log.auto_flush = false;
    }

    #[test]
    fn test_disabled_is_noop() {
        let mut log = BenchLog::disabled();
        log.event("should be ignored");
        log.event_val("also ignored", 42);
        assert_eq!(log.count(), 0);
        assert!(log.as_str().is_empty());
    }
}
