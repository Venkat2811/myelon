use clap::Parser;
use perf_bench::infra::repeatability;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

#[derive(Debug, Parser)]
#[command(about = "Repeat a canonical perf-bench command and compute throughput variance")]
struct Args {
    /// Number of measured runs to execute
    #[arg(long, default_value_t = 3)]
    repeat: usize,

    /// Warmup runs to execute and discard before measuring
    #[arg(long, default_value_t = 0)]
    warmup_runs: usize,

    /// Maximum allowed coefficient of variation for producer and consumer throughput
    #[arg(long, default_value_t = 10.0)]
    threshold_pct: f64,

    /// Environment variable that benchmark command uses for canonical JSON export
    #[arg(long, default_value = perf_bench::infra::env::JSON_OUT)]
    json_env: String,

    /// Write aggregated repeatability report as JSON
    #[arg(long)]
    json_out: Option<String>,

    /// Write aggregated repeatability report as Markdown
    #[arg(long)]
    md_out: Option<String>,

    /// Suppress per-run progress logs
    #[arg(long)]
    quiet: bool,

    /// Benchmark command to repeat; pass it after `--`
    #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
    command: Vec<String>,
}

fn main() {
    let args = Args::parse();
    if args.repeat == 0 {
        eprintln!("repeatability requires --repeat >= 1");
        std::process::exit(2);
    }

    let total_runs = args.warmup_runs + args.repeat;
    let mut reports = Vec::with_capacity(args.repeat);

    for run_index in 0..total_runs {
        let measured_run = run_index >= args.warmup_runs;
        let report_path = temp_report_path(run_index);
        let phase = if measured_run { "run" } else { "warmup" };
        if !args.quiet {
            eprintln!("[repeatability] {phase} {}/{}", run_index + 1, total_runs);
        }

        let status = Command::new(&args.command[0])
            .args(&args.command[1..])
            .env(&args.json_env, &report_path)
            .status()
            .expect("spawn repeatability child command");
        if !status.success() {
            eprintln!(
                "repeatability child command failed on {phase} {} with status {status}",
                run_index + 1
            );
            std::process::exit(status.code().unwrap_or(1));
        }

        let raw = fs::read_to_string(&report_path).unwrap_or_else(|error| {
            panic!("read canonical report {}: {error}", report_path.display())
        });
        let report = repeatability::parse_bench_report(&raw).unwrap_or_else(|error| {
            panic!("parse canonical report {}: {error}", report_path.display())
        });
        let _ = fs::remove_file(&report_path);

        if measured_run {
            reports.push(report);
        }
    }

    let aggregate = repeatability::aggregate_reports(&args.command, &reports, args.threshold_pct)
        .unwrap_or_else(|error| panic!("aggregate repeatability report: {error}"));

    println!("{}", repeatability::render_summary_table(&aggregate));

    if let Some(path) = args.json_out.as_deref() {
        fs::write(
            path,
            format!(
                "{}\n",
                serde_json::to_string_pretty(&aggregate).expect("serialize repeatability report")
            ),
        )
        .expect("write repeatability JSON");
        eprintln!("JSON written to {path}");
    }
    if let Some(path) = args.md_out.as_deref() {
        fs::write(path, repeatability::render_markdown(&aggregate))
            .expect("write repeatability markdown");
        eprintln!("Markdown written to {path}");
    }

    if !aggregate.overall_pass {
        std::process::exit(1);
    }
}

fn temp_report_path(run_index: usize) -> PathBuf {
    let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    std::env::temp_dir().join(format!(
        "perf_bench_repeatability_{}_{}_{}.json",
        std::process::id(),
        timestamp,
        run_index
    ))
}
