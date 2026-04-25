//! Unique segment and directory naming for benchmark isolation.
//!
//! Each benchmark run gets unique SHM segment names and mmap root directories
//! to prevent collision between concurrent runs or leftover segments.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

/// Generate a unique POSIX SHM segment name.
pub fn unique_shm_segment(prefix: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    disruptor_mp::portable_shm_segment_name(&format!(
        "{}_{}_{}",
        prefix,
        std::process::id() % 10000,
        ts % 100000
    ))
}

/// Generate a unique mmap root directory path.
pub fn unique_mmap_root(prefix: &str) -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("{}_{}_{}", prefix, std::process::id(), ts))
}

/// Generate a unique mmap segment name.
pub fn unique_mmap_segment(prefix: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}_{}_{}", prefix, std::process::id() % 10000, ts % 100000)
}

/// Read SHM segment name from environment (child process).
pub fn segment_from_env(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("{key} env var not set"))
}

/// Read mmap layout from environment (child process).
pub fn mmap_layout_from_env(
    root_key: &str,
    segment_key: &str,
) -> disruptor_mp::MmapTransportLayout {
    let root = std::env::var(root_key).unwrap_or_else(|_| panic!("{root_key} not set"));
    let segment = std::env::var(segment_key).unwrap_or_else(|_| panic!("{segment_key} not set"));
    disruptor_mp::MmapTransportLayout::new(PathBuf::from(root), segment).expect("mmap layout")
}
