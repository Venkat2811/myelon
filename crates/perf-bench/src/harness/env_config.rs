//! Environment variable helpers for child process configuration.
//!
//! Replaces 12 identical copies of `read_env_usize`/`read_env_u64` across bench files.

/// Read a usize from an environment variable, returning default if not set or invalid.
pub fn read_env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Read a u64 from an environment variable, returning default if not set or invalid.
pub fn read_env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Read a bool from an environment variable (accepts "1", "true", "yes").
pub fn read_env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
        .unwrap_or(default)
}

/// Read a string from an environment variable, returning default if not set.
pub fn read_env_string(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
