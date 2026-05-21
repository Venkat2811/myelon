use crate::infra::adapter::{parity_adapters, AdapterId, AdapterOrigin, AdapterSpec};
use crate::infra::parity::{self, SizeTuning};
use crate::runner::cli::RunnerCli;
use crate::runner::dispatch::{strategy_for, ExecutionStrategy};
use crate::runner::platform::PlatformInfo;
use std::fs;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct RunOutcome {
    pub adapter: AdapterId,
    pub display_name: String,
    pub size: usize,
    pub mode: String,
    pub output_path: PathBuf,
    pub success: bool,
}

pub fn execute_tier(cli: &RunnerCli) -> Vec<RunOutcome> {
    let cfg = parity::config();
    let platform = PlatformInfo::detect(find_target_dir(cli));
    let manifest = cli.resolve_manifest_path();

    let sizes = cli
        .parse_sizes()
        .unwrap_or_else(|| parity::sizes_for_tier(cfg, &cli.tier));

    let adapter_filter = cli.parse_adapters();
    let adapters: Vec<&AdapterSpec> = parity_adapters()
        .iter()
        .filter(|spec| match_adapter_filter(spec, &adapter_filter))
        .collect();

    fs::create_dir_all(&cli.outdir).ok();

    let mut outcomes = Vec::new();

    for adapter in &adapters {
        let strategy = strategy_for(adapter.id);

        for &size in &sizes {
            let tuning = override_tuning(cli, &parity::tune_for_size(cfg, size));

            // Skip adapters with size limits
            if adapter.id == AdapterId::Rusteron && size > 16_777_216 {
                eprintln!(
                    "  - skip {} {}B (exceeds Aeron max message size)",
                    adapter.display_name, size
                );
                continue;
            }

            // Throughput mode
            if cli.throughput {
                let outcome = execute_single(
                    adapter, &strategy, size, &tuning, None, cli, &platform, &manifest,
                );
                outcomes.push(outcome);
            }

            // Fixed-rate mode
            if cli.fixed_rate {
                for &rate in &tuning.rates {
                    let outcome = execute_single(
                        adapter,
                        &strategy,
                        size,
                        &tuning,
                        Some(rate),
                        cli,
                        &platform,
                        &manifest,
                    );
                    outcomes.push(outcome);
                }
            }
        }
    }

    // --- Broadcast topology ---
    if cli.broadcast {
        let broadcast_consumers = cfg.broadcast_consumers.clone();
        let broadcast_adapters: Vec<(&str, &str)> = vec![
            ("disruptor-shm", "internal_broadcast"),
            ("disruptor-mmap", "internal_broadcast"),
            ("myelon-raw-shm", "internal_broadcast"),
            ("myelon-raw-mmap", "internal_broadcast"),
            ("crossbar", "crossbar_broadcast"),
        ];

        for &(adapter_flag, binary_name) in &broadcast_adapters {
            // Check if this adapter is in the filter
            let adapter_spec = parity_adapters().iter().find(|s| {
                s.display_name.replace('-', "_") == adapter_flag.replace('-', "_")
                    || s.output_prefix == adapter_flag.replace('-', "_")
            });
            let Some(adapter_spec) = adapter_spec else {
                continue;
            };
            if !match_adapter_filter(adapter_spec, &adapter_filter) {
                continue;
            }

            for &size in &sizes {
                let tuning = override_tuning(cli, &parity::tune_for_size(cfg, size));
                for &consumers in &broadcast_consumers {
                    // Throughput
                    if cli.throughput {
                        let outcome = execute_broadcast(
                            adapter_flag,
                            binary_name,
                            size,
                            consumers,
                            &tuning,
                            None,
                            cli,
                            &platform,
                        );
                        outcomes.push(outcome);
                    }
                    // Fixed-rate
                    if cli.fixed_rate {
                        for &rate in &tuning.rates {
                            let outcome = execute_broadcast(
                                adapter_flag,
                                binary_name,
                                size,
                                consumers,
                                &tuning,
                                Some(rate),
                                cli,
                                &platform,
                            );
                            outcomes.push(outcome);
                        }
                    }
                }
            }
        }
    }

    outcomes
}

#[allow(clippy::too_many_arguments)]
fn execute_broadcast(
    adapter_flag: &str,
    binary_name: &str,
    size: usize,
    consumers: usize,
    tuning: &SizeTuning,
    target_rate: Option<u64>,
    cli: &RunnerCli,
    platform: &PlatformInfo,
) -> RunOutcome {
    let output_prefix = adapter_flag.replace('-', "_");
    let mode_label = target_rate
        .map(|r| format!("{size} {consumers}c @{r}/s"))
        .unwrap_or_else(|| format!("{size} {consumers}c"));

    let output_name = match target_rate {
        Some(rate) => format!("broadcast_{output_prefix}_{size}_{consumers}c_{rate}.json"),
        None => format!("broadcast_{output_prefix}_{size}_{consumers}c.json"),
    };
    let output_path = cli.outdir.join(&output_name);

    if cli.dry_run {
        eprintln!(
            "  [dry-run] broadcast {} {} {}",
            adapter_flag,
            mode_label,
            output_path.display()
        );
        return RunOutcome {
            adapter: AdapterId::DisruptorShm, // placeholder for display
            display_name: format!("broadcast-{adapter_flag}"),
            size,
            mode: mode_label,
            output_path,
            success: true,
        };
    }

    let binary = platform.binary_path(&cli.profile, binary_name);
    let base = format!(
        "broadcast_{output_prefix}_{consumers}c_{size}_{}_{}",
        fastrand_u32(),
        std::process::id()
    );

    let mut args: Vec<String> = vec![
        "--mode".into(),
        "controller".into(),
        "--base".into(),
        base.clone(),
        "--message-size".into(),
        size.to_string(),
        "--consumers".into(),
        consumers.to_string(),
        "--num-messages".into(),
        tuning.num_messages.to_string(),
        "--warmup".into(),
        tuning.warmup.to_string(),
        "--json".into(),
    ];

    // internal_broadcast needs --adapter flag, crossbar_broadcast does not
    if binary_name == "internal_broadcast" {
        args.extend(["--adapter".into(), adapter_flag.into()]);
    }

    if let Some(rate) = target_rate {
        args.extend(["--target-rate".into(), rate.to_string()]);
    }

    let success = run_and_capture(
        binary.to_str().unwrap_or("binary"),
        &args,
        &[],
        &output_path,
        cli.timeout,
    );

    let status = if success { "ok" } else { "FAIL" };
    eprintln!("  {status} broadcast {adapter_flag} {mode_label}");

    RunOutcome {
        adapter: AdapterId::DisruptorShm, // placeholder
        display_name: format!("broadcast-{adapter_flag}"),
        size,
        mode: mode_label,
        output_path,
        success,
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_single(
    adapter: &AdapterSpec,
    strategy: &ExecutionStrategy,
    size: usize,
    tuning: &SizeTuning,
    target_rate: Option<u64>,
    cli: &RunnerCli,
    platform: &PlatformInfo,
    manifest: &Path,
) -> RunOutcome {
    let mode_label = target_rate
        .map(|r| format!("{}@{r}/s", size))
        .unwrap_or_else(|| format!("{size}"));

    let output_path = output_file_path(&cli.outdir, adapter, size, target_rate);

    if cli.dry_run {
        eprintln!(
            "  [dry-run] {} {} {}",
            adapter.display_name,
            mode_label,
            output_path.display()
        );
        return RunOutcome {
            adapter: adapter.id,
            display_name: adapter.display_name.to_string(),
            size,
            mode: mode_label,
            output_path,
            success: true,
        };
    }

    let success = match strategy {
        ExecutionStrategy::InternalPingpong { layer, backend } => execute_internal(
            layer,
            backend,
            size,
            tuning,
            target_rate,
            &output_path,
            manifest,
            &cli.profile,
            cli.timeout,
        ),
        ExecutionStrategy::ExternalPingpong {
            binary_name,
            relative_to_crate,
            extra_server_args,
            extra_client_args,
            startup_delay,
            needs_aeron_env,
            cleanup_shm_base,
        } => {
            let base = generate_base_name(adapter, size);
            let env = if *needs_aeron_env {
                platform.aeron_env()
            } else {
                vec![]
            };
            let binary = if *relative_to_crate {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(binary_name)
            } else {
                platform.binary_path(&cli.profile, binary_name)
            };
            let result = execute_external(
                &binary,
                &base,
                size,
                tuning,
                target_rate,
                extra_server_args,
                extra_client_args,
                *startup_delay,
                &env,
                &output_path,
                cli.timeout,
            );
            if *cleanup_shm_base {
                let shm_path = format!("{}/{}", platform.ipc_dir(), base);
                let _ = fs::remove_dir_all(&shm_path);
            }
            result
        }
        ExecutionStrategy::Mpi {
            binary_relative,
            extra_args,
        } => execute_mpi(
            binary_relative,
            extra_args,
            size,
            tuning,
            target_rate,
            &output_path,
            cli.timeout,
        ),
        ExecutionStrategy::InternalBroadcast { .. } | ExecutionStrategy::CrossbarBroadcast => {
            eprintln!("  [skip] broadcast not yet wired in runner");
            false
        }
    };

    let status = if success { "ok" } else { "FAIL" };
    eprintln!("  {status} {} {mode_label}", adapter.display_name);

    RunOutcome {
        adapter: adapter.id,
        display_name: adapter.display_name.to_string(),
        size,
        mode: mode_label,
        output_path,
        success,
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_internal(
    layer: &str,
    backend: &str,
    size: usize,
    tuning: &SizeTuning,
    target_rate: Option<u64>,
    output_path: &Path,
    manifest: &Path,
    profile: &str,
    timeout_secs: u64,
) -> bool {
    let mut args = vec![
        "run".to_string(),
        "-p".to_string(),
        "perf-bench".to_string(),
        "--profile".to_string(),
        profile.to_string(),
        "--manifest-path".to_string(),
        manifest.display().to_string(),
        "--bin".to_string(),
        "perf-bench-pingpong".to_string(),
        "--".to_string(),
        "--layer".to_string(),
        layer.to_string(),
        "--backend".to_string(),
        backend.to_string(),
        "--size".to_string(),
        size.to_string(),
        "-n".to_string(),
        tuning.num_messages.to_string(),
        "-w".to_string(),
        tuning.warmup.to_string(),
        "--json".to_string(),
    ];
    if let Some(rate) = target_rate {
        args.extend(["--mode".to_string(), "co".to_string()]);
        args.extend(["--target-rate".to_string(), rate.to_string()]);
    }
    if timeout_secs > 0 {
        args.extend(["--timeout".to_string(), timeout_secs.to_string()]);
    }

    run_and_capture("cargo", &args, &[], output_path, timeout_secs)
}

#[allow(clippy::too_many_arguments)]
fn execute_external(
    binary: &Path,
    base: &str,
    size: usize,
    tuning: &SizeTuning,
    target_rate: Option<u64>,
    extra_server_args: &[String],
    extra_client_args: &[String],
    startup_delay: Duration,
    env: &[(String, String)],
    output_path: &Path,
    timeout_secs: u64,
) -> bool {
    // Spawn server
    let mut server_args: Vec<String> = vec![
        "--mode".into(),
        "server".into(),
        "--base".into(),
        base.into(),
        "--message-size".into(),
        size.to_string(),
    ];
    server_args.extend(extra_server_args.iter().cloned());

    let server = Command::new(binary)
        .args(&server_args)
        .envs(env.iter().cloned())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();

    let mut server = match server {
        Ok(s) => s,
        Err(e) => {
            eprintln!("    failed to spawn server: {e}");
            return false;
        }
    };

    std::thread::sleep(startup_delay);

    // Run client
    let mut client_args: Vec<String> = vec![
        "--mode".into(),
        "client".into(),
        "--base".into(),
        base.into(),
        "--message-size".into(),
        size.to_string(),
        "--num-messages".into(),
        tuning.num_messages.to_string(),
        "--warmup".into(),
        tuning.warmup.to_string(),
        "--json".into(),
    ];
    if let Some(rate) = target_rate {
        client_args.extend(["--target-rate".into(), rate.to_string()]);
    }
    client_args.extend(extra_client_args.iter().cloned());

    let success = run_and_capture(
        binary.to_str().unwrap_or("binary"),
        &client_args,
        env,
        output_path,
        timeout_secs,
    );

    // Kill server
    let _ = server.kill();
    let _ = server.wait();

    success
}

fn execute_mpi(
    binary_relative: &str,
    extra_args: &[&str],
    size: usize,
    tuning: &SizeTuning,
    target_rate: Option<u64>,
    output_path: &Path,
    timeout_secs: u64,
) -> bool {
    let binary_path = PathBuf::from(binary_relative);
    if !binary_path.exists() {
        eprintln!("    MPI binary not found: {}", binary_path.display());
        return false;
    }

    let mut args: Vec<String> = extra_args.iter().map(|s| s.to_string()).collect();
    args.push(binary_path.display().to_string());
    args.extend([
        "--message-size".into(),
        size.to_string(),
        "--num-messages".into(),
        tuning.num_messages.to_string(),
        "--warmup".into(),
        tuning.warmup.to_string(),
        "--json".into(),
    ]);
    if let Some(rate) = target_rate {
        args.extend(["--target-rate".into(), rate.to_string()]);
    }

    run_and_capture("mpirun", &args, &[], output_path, timeout_secs)
}

// --- Helpers ---

fn run_and_capture(
    program: &str,
    args: &[String],
    env: &[(String, String)],
    output_path: &Path,
    timeout_secs: u64,
) -> bool {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).ok();
    }
    let tmp_output_path = output_path.with_extension("tmp");
    let stdout_file = match File::create(&tmp_output_path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!(
                "    failed to create temp output {}: {e}",
                tmp_output_path.display()
            );
            return false;
        }
    };

    let mut cmd = Command::new(program);
    cmd.args(args)
        .envs(env.iter().cloned())
        .stderr(Stdio::inherit())
        .stdout(Stdio::from(stdout_file));

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => {
            eprintln!("    failed to spawn {program}: {e}");
            let _ = fs::remove_file(&tmp_output_path);
            return false;
        }
    };

    let status = if timeout_secs > 0 {
        match wait_for_child(&mut child, Duration::from_secs(timeout_secs)) {
            Ok(WaitOutcome::Exited(status)) => status,
            Ok(WaitOutcome::TimedOut) => {
                eprintln!("    {program} timed out after {timeout_secs}s");
                let _ = child.kill();
                let _ = child.wait();
                let _ = fs::remove_file(&tmp_output_path);
                return false;
            }
            Err(e) => {
                eprintln!("    failed to wait for {program}: {e}");
                let _ = child.kill();
                let _ = child.wait();
                let _ = fs::remove_file(&tmp_output_path);
                return false;
            }
        }
    } else {
        match child.wait() {
            Ok(status) => status,
            Err(e) => {
                eprintln!("    failed to wait for {program}: {e}");
                let _ = child.kill();
                let _ = child.wait();
                let _ = fs::remove_file(&tmp_output_path);
                return false;
            }
        }
    };

    if !status.success() {
        eprintln!("    {program} exited with {}", status.code().unwrap_or(-1));
        let _ = fs::remove_file(&tmp_output_path);
        return false;
    }

    if let Err(e) = fs::rename(&tmp_output_path, output_path) {
        eprintln!(
            "    failed to finalize {} from {}: {e}",
            output_path.display(),
            tmp_output_path.display()
        );
        let _ = fs::remove_file(&tmp_output_path);
        return false;
    }

    true
}

enum WaitOutcome {
    Exited(ExitStatus),
    TimedOut,
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> io::Result<WaitOutcome> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(WaitOutcome::Exited(status));
        }
        if Instant::now() >= deadline {
            return Ok(WaitOutcome::TimedOut);
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn output_file_path(
    outdir: &Path,
    adapter: &AdapterSpec,
    size: usize,
    target_rate: Option<u64>,
) -> PathBuf {
    let name = match target_rate {
        Some(rate) => format!("{}_{}_{}.json", adapter.output_prefix, size, rate),
        None => format!("{}_{}.json", adapter.output_prefix, size),
    };
    outdir.join(name)
}

fn generate_base_name(adapter: &AdapterSpec, size: usize) -> String {
    format!(
        "{}_{}_{}_{}",
        adapter.output_prefix,
        size,
        fastrand_u32(),
        std::process::id()
    )
}

fn fastrand_u32() -> u32 {
    // Simple non-cryptographic random for base name uniqueness
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    t ^ std::process::id()
}

fn match_adapter_filter(spec: &AdapterSpec, filter: &[String]) -> bool {
    if filter.iter().any(|f| f == "all") {
        return true;
    }
    if filter.iter().any(|f| f == "internal") && spec.origin == AdapterOrigin::Internal {
        return true;
    }
    if filter.iter().any(|f| f == "external") && spec.origin != AdapterOrigin::Internal {
        return true;
    }
    // Match by output_prefix, display_name, or partial prefix
    let normalized = |s: &str| s.replace('-', "_").to_lowercase();
    filter.iter().any(|f| {
        let f_norm = normalized(f);
        let prefix_norm = normalized(spec.output_prefix);
        let display_norm = normalized(spec.display_name);
        f_norm == prefix_norm
            || f_norm == display_norm
            || prefix_norm.starts_with(&f_norm)
            || display_norm.starts_with(&f_norm)
    })
}

fn find_target_dir(cli: &RunnerCli) -> PathBuf {
    let manifest = cli.resolve_manifest_path();
    manifest
        .parent()
        .map(|p| p.join("target"))
        .unwrap_or_else(|| PathBuf::from("target"))
}

fn override_tuning(cli: &RunnerCli, base: &SizeTuning) -> SizeTuning {
    let rates = if cli.tier == "super-tiny" {
        vec![50_000]
    } else {
        base.rates.clone()
    };
    SizeTuning {
        num_messages: cli.msgs.unwrap_or(base.num_messages),
        warmup: cli.warmup.unwrap_or(base.warmup),
        rates,
        headon_rate_smoke: base.headon_rate_smoke,
        stack_kb: base.stack_kb,
    }
}

#[cfg(test)]
mod tests {
    use super::{override_tuning, wait_for_child, WaitOutcome};
    use crate::infra::parity::SizeTuning;
    use crate::runner::cli::RunnerCli;
    use std::path::PathBuf;
    use std::process::Command;
    use std::time::Duration;

    #[test]
    fn wait_for_child_detects_successful_exit() {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("exit 0")
            .spawn()
            .expect("spawn short-lived shell");

        let outcome = wait_for_child(&mut child, Duration::from_secs(1)).expect("wait");
        assert!(matches!(outcome, WaitOutcome::Exited(status) if status.success()));
    }

    #[cfg(unix)]
    #[test]
    fn wait_for_child_reports_timeout() {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 2")
            .spawn()
            .expect("spawn sleeper");

        let outcome = wait_for_child(&mut child, Duration::from_millis(100)).expect("wait");
        assert!(matches!(outcome, WaitOutcome::TimedOut));
        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn super_tiny_collapses_fixed_rate_ladder_to_single_ci_rate() {
        let cli = RunnerCli {
            tier: "super-tiny".to_string(),
            adapters: "all".to_string(),
            outdir: PathBuf::from("output"),
            sizes: None,
            msgs: Some(1000),
            warmup: Some(100),
            throughput: true,
            fixed_rate: true,
            broadcast: true,
            timeout: 60,
            dry_run: false,
            manifest_path: None,
            profile: "competitive".to_string(),
        };
        let base = SizeTuning {
            num_messages: 123,
            warmup: 45,
            rates: vec![200_000, 400_000, 600_000],
            headon_rate_smoke: 200_000,
            stack_kb: 16_384,
        };

        let tuned = override_tuning(&cli, &base);
        assert_eq!(tuned.num_messages, 1000);
        assert_eq!(tuned.warmup, 100);
        assert_eq!(tuned.rates, vec![50_000]);
    }
}
