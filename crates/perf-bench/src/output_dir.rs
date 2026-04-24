//! Benchmark output directory structure.
//!
//! Creates a standardized, gitignored output directory for each benchmark run
//! with timestamped subdirectories and nested topology/layer/backend paths.
//!
//! Directory layout:
//! ```text
//! crates/perf-bench/output/                      ← gitignored
//! └── 2026-04-25T14-30-00/                       ← run timestamp
//!     ├── run.json                                ← run metadata
//!     ├── logs/                                   ← BenchLog JSONL files
//!     ├── pingpong/
//!     │   ├── raw_ring/shm/64B_throughput.json
//!     │   ├── raw_ring/mmap/64B_co_400000.json
//!     │   ├── typed_zc/shm/frag/rkyv/64B_throughput.json
//!     │   └── ...
//!     ├── broadcast/
//!     │   ├── raw_ring/shm/4c/64B_throughput.json
//!     │   └── ...
//!     └── signal/
//!         ├── shm/1c/64B_throughput.json
//!         └── mmap/1c/64B_throughput.json
//! ```
//!
//! Env vars:
//! - `PERF_BENCH_OUT_DIR`: override the run directory (skips timestamp creation)
//! - BenchLog uses `PERF_BENCH_OUT_DIR/logs` automatically

use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const OUTPUT_ROOT: &str = "output";

/// Metadata about a benchmark run, written to `run.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMetadata {
    pub timestamp: String,
    pub platform: String,
    pub cpu: String,
    pub git_commit: String,
    pub tier: Option<String>,
}

impl RunMetadata {
    /// Create metadata for the current environment.
    pub fn current(tier: Option<&str>) -> Self {
        Self {
            timestamp: Utc::now().to_rfc3339(),
            platform: format!("{} {}", std::env::consts::OS, std::env::consts::ARCH),
            cpu: detect_cpu(),
            git_commit: detect_git_commit(),
            tier: tier.map(String::from),
        }
    }
}

/// Create (or resolve) the output directory for a benchmark run.
///
/// If `PERF_BENCH_OUT_DIR` is set, uses that directly.
/// Otherwise creates `output/{timestamp}/` and sets `PERF_BENCH_OUT_DIR`
/// so child processes and BenchLog pick it up.
pub fn resolve_run_dir(tier: Option<&str>) -> PathBuf {
    if let Ok(dir) = std::env::var("PERF_BENCH_OUT_DIR") {
        let path = PathBuf::from(dir);
        std::fs::create_dir_all(&path).ok();
        return path;
    }

    let ts = Utc::now().format("%Y-%m-%dT%H-%M-%S").to_string();
    let run_dir = PathBuf::from(OUTPUT_ROOT).join(&ts);
    std::fs::create_dir_all(&run_dir).expect("failed to create output run dir");

    // Write run metadata
    let meta = RunMetadata::current(tier);
    let meta_path = run_dir.join("run.json");
    if let Ok(json) = serde_json::to_string_pretty(&meta) {
        std::fs::write(&meta_path, json).ok();
    }

    // Create logs subdirectory
    std::fs::create_dir_all(run_dir.join("logs")).ok();

    // Set env so child processes and BenchLog use the same dir
    std::env::set_var("PERF_BENCH_OUT_DIR", run_dir.to_str().unwrap_or("output"));

    run_dir
}

/// Build the result file path for a benchmark scenario.
///
/// ```text
/// {run_dir}/{topology}/{layer}/{backend}/[{frag}/][{codec}/][{consumers}c/]{size}_{mode}[_{rate}].json
/// ```
pub fn result_path(
    run_dir: &Path,
    topology: &str,
    layer: &str,
    backend: &str,
    codec: Option<&str>,
    frag: Option<&str>,
    consumers: Option<usize>,
    size_bytes: usize,
    mode: &str,
    target_rate: Option<u64>,
) -> PathBuf {
    let mut path = run_dir.join(topology).join(layer).join(backend);

    if let Some(f) = frag {
        path = path.join(f);
    }
    if let Some(c) = codec {
        path = path.join(c);
    }
    if let Some(n) = consumers {
        path = path.join(format!("{n}c"));
    }

    std::fs::create_dir_all(&path).ok();

    let size_label = human_size(size_bytes);
    let filename = match target_rate {
        Some(rate) => format!("{size_label}_{mode}_{rate}.json"),
        None => format!("{size_label}_{mode}.json"),
    };
    path.join(filename)
}

fn human_size(bytes: usize) -> String {
    if bytes >= 1_048_576 {
        format!("{}MB", bytes / 1_048_576)
    } else if bytes >= 1024 {
        format!("{}KB", bytes / 1024)
    } else {
        format!("{bytes}B")
    }
}

fn detect_cpu() -> String {
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("sysctl")
            .args(["-n", "machdep.cpu.brand_string"])
            .output()
            .ok()
            .and_then(|out| String::from_utf8(out.stdout).ok())
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| "unknown".to_string())
    }
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/cpuinfo")
            .ok()
            .and_then(|info| {
                info.lines()
                    .find(|l| l.starts_with("model name"))
                    .and_then(|l| l.split(':').nth(1))
                    .map(|s| s.trim().to_string())
            })
            .unwrap_or_else(|| "unknown".to_string())
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        "unknown".to_string()
    }
}

fn detect_git_commit() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_path_builds_nested_structure() {
        let run_dir = PathBuf::from("/tmp/test_run");
        let path = result_path(
            &run_dir,
            "pingpong",
            "typed_zc",
            "shm",
            Some("rkyv"),
            Some("frag"),
            None,
            64,
            "throughput",
            None,
        );
        assert_eq!(
            path,
            PathBuf::from("/tmp/test_run/pingpong/typed_zc/shm/frag/rkyv/64B_throughput.json")
        );
    }

    #[test]
    fn result_path_with_co_rate() {
        let run_dir = PathBuf::from("/tmp/test_run");
        let path = result_path(
            &run_dir,
            "pingpong",
            "raw_ring",
            "mmap",
            None,
            None,
            None,
            1024,
            "co",
            Some(400000),
        );
        assert_eq!(
            path,
            PathBuf::from("/tmp/test_run/pingpong/raw_ring/mmap/1KB_co_400000.json")
        );
    }

    #[test]
    fn result_path_broadcast_with_consumers() {
        let run_dir = PathBuf::from("/tmp/test_run");
        let path = result_path(
            &run_dir,
            "broadcast",
            "raw_ring",
            "shm",
            None,
            None,
            Some(4),
            64,
            "throughput",
            None,
        );
        assert_eq!(
            path,
            PathBuf::from("/tmp/test_run/broadcast/raw_ring/shm/4c/64B_throughput.json")
        );
    }

    #[test]
    fn run_metadata_captures_platform() {
        let meta = RunMetadata::current(Some("quick"));
        assert!(!meta.timestamp.is_empty());
        assert!(!meta.platform.is_empty());
        assert_eq!(meta.tier.as_deref(), Some("quick"));
    }

    #[test]
    fn human_size_formats_correctly() {
        assert_eq!(human_size(64), "64B");
        assert_eq!(human_size(1024), "1KB");
        assert_eq!(human_size(65536), "64KB");
        assert_eq!(human_size(1_048_576), "1MB");
        assert_eq!(human_size(33_554_432), "32MB");
    }
}
