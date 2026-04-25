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

/// Default benchmark timeout in seconds. Override with BENCH_TIMEOUT env var.
pub fn bench_timeout_secs() -> u64 {
    read_env_u64("BENCH_TIMEOUT", 300) // 5 minutes default
}

/// Read BENCH_TIMEOUT with a bench-specific default.
pub fn bench_timeout_secs_or(default_secs: u64) -> u64 {
    read_env_u64("BENCH_TIMEOUT", default_secs)
}

/// Read an explicit timeout override parsed from CLI flags.
pub fn bench_timeout_override_secs() -> Option<u64> {
    std::env::var("BENCH_TIMEOUT_OVERRIDE")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
}

/// Construct a duration from BENCH_TIMEOUT with a bench-specific default.
pub fn bench_timeout_duration(default_secs: u64) -> std::time::Duration {
    std::time::Duration::from_secs(bench_timeout_secs_or(default_secs))
}

/// Create a deadline Instant for spin-loop safety.
/// Prevents infinite hangs from spin-loops that never receive events.
pub fn spin_deadline() -> std::time::Instant {
    std::time::Instant::now() + std::time::Duration::from_secs(bench_timeout_secs())
}

/// Create a deadline using a bench-specific default timeout.
pub fn spin_deadline_or(default_secs: u64) -> std::time::Instant {
    std::time::Instant::now() + bench_timeout_duration(default_secs)
}

/// Check if a deadline has passed. Panics with a descriptive message if so.
/// Call inside spin-loops to prevent zombie processes.
#[inline]
pub fn check_deadline(deadline: std::time::Instant, context: &str) {
    if std::time::Instant::now() > deadline {
        panic!(
            "TIMEOUT: {context} — exceeded {}s deadline. Kill with: pkill -f perf-bench",
            bench_timeout_secs()
        );
    }
}

/// Parse `--timeout <seconds>` from bench CLI args, export BENCH_TIMEOUT,
/// and return a filtered argv without the timeout flag/value pair.
pub fn apply_timeout_arg(args: &[String]) -> Result<Vec<String>, String> {
    let mut filtered = Vec::with_capacity(args.len());
    let mut index = 0;

    while index < args.len() {
        if args[index] == "--timeout" {
            let value = args
                .get(index + 1)
                .ok_or_else(|| "--timeout requires a value".to_string())?;
            let seconds = value
                .parse::<u64>()
                .map_err(|_| format!("invalid --timeout value: {value}"))?;
            if seconds == 0 {
                return Err("--timeout must be greater than zero".to_string());
            }
            std::env::set_var("BENCH_TIMEOUT", seconds.to_string());
            std::env::set_var("BENCH_TIMEOUT_OVERRIDE", seconds.to_string());
            index += 2;
            continue;
        }

        filtered.push(args[index].clone());
        index += 1;
    }

    Ok(filtered)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_timeout_arg_sets_explicit_override() {
        std::env::remove_var("BENCH_TIMEOUT");
        std::env::remove_var("BENCH_TIMEOUT_OVERRIDE");

        let args = vec![
            "bench".to_string(),
            "--timeout".to_string(),
            "45".to_string(),
            "--quick".to_string(),
        ];
        let filtered = apply_timeout_arg(&args).expect("timeout parsing should succeed");

        assert_eq!(filtered, vec!["bench".to_string(), "--quick".to_string()]);
        assert_eq!(std::env::var("BENCH_TIMEOUT").ok().as_deref(), Some("45"));
        assert_eq!(
            std::env::var("BENCH_TIMEOUT_OVERRIDE").ok().as_deref(),
            Some("45")
        );

        std::env::remove_var("BENCH_TIMEOUT");
        std::env::remove_var("BENCH_TIMEOUT_OVERRIDE");
    }
}
