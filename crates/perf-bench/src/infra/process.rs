//! Multiprocess spawning, waiting, and output collection.
//!
//! Replaces copy-pasted child spawning, waiting, and JSON-output collection
//! implementations across benchmark files.

use std::io::Read as _;
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

/// Spawn a child process with role argument and environment variables.
pub fn spawn_child(exe: &Path, role: &str, envs: &[(&str, String)]) -> Child {
    let mut cmd = Command::new(exe);
    cmd.arg(role).stdout(Stdio::piped()).stderr(Stdio::piped());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd.spawn().unwrap_or_else(|e| panic!("spawn {role}: {e}"))
}

/// Wait for a child process with timeout. Returns stdout/stderr on success,
/// or error message on timeout (after killing the child).
pub fn wait_timeout(mut child: Child, timeout: Duration) -> Result<Output, String> {
    fn collect(child: &mut Child, status: std::process::ExitStatus) -> Output {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        if let Some(mut out) = child.stdout.take() {
            let _ = out.read_to_end(&mut stdout);
        }
        if let Some(mut err) = child.stderr.take() {
            let _ = err.read_to_end(&mut stderr);
        }
        Output {
            status,
            stdout,
            stderr,
        }
    }

    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
            return Ok(collect(&mut child, status));
        }
        if start.elapsed() >= timeout {
            let _ = child.kill();
            let status = child.wait().map_err(|e| e.to_string())?;
            let output = collect(&mut child, status);
            return Err(format!(
                "timeout after {timeout:?}; stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Collected output from a child process.
pub struct ProcessOutput {
    pub stdout: String,
    pub stderr: String,
    pub success: bool,
}

/// Collect and log child process output.
pub fn collect_output(label: &str, result: Result<Output, String>) -> ProcessOutput {
    match result {
        Ok(o) => {
            if !o.stderr.is_empty() {
                eprintln!("[{label} stderr] {}", String::from_utf8_lossy(&o.stderr));
            }
            ProcessOutput {
                stdout: String::from_utf8_lossy(&o.stdout).to_string(),
                stderr: String::from_utf8_lossy(&o.stderr).to_string(),
                success: o.status.success(),
            }
        }
        Err(e) => {
            eprintln!("[{label}] {e}");
            ProcessOutput {
                stdout: String::new(),
                stderr: e,
                success: false,
            }
        }
    }
}

/// Parse structured JSON output from a child process.
/// The child prints a single JSON line to stdout.
pub fn parse_json_output<T: serde::de::DeserializeOwned>(output: &str) -> Option<T> {
    for line in output.lines() {
        if line.starts_with('{') {
            return serde_json::from_str(line).ok();
        }
    }
    None
}
