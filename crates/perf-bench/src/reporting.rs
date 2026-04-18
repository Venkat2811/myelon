//! Benchmark reporting — canonical JSON schema, tabled output, CSV.
//!
//! Every benchmark in myelon-bench produces `BenchResult` structs that
//! share a single canonical JSON schema. Latency comes from real HDR
//! histograms (via `latency::LatencyStats`) or is `None`.

use crate::events::format_throughput;
use crate::latency;
use serde::{Deserialize, Serialize};
use std::io;
use std::process::Command;

/// Canonical benchmark result. All benchmarks produce this same struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchResult {
    /// Benchmark identifier (e.g., "myelon-bench/raw_ring_shm/signal_1p1c_64B").
    pub benchmark_id: String,
    /// Scenario name (e.g., "signal_1p1c_64B").
    pub scenario: String,
    /// Transport backend: "shm" or "mmap".
    pub backend: String,
    /// Transport layer: "raw_ring", "framed", "typed".
    pub layer: String,
    /// Codec used, or null for raw transport.
    pub codec: Option<String>,
    /// Measurement mode: "max_throughput", "fixed_rate", "batch_timing".
    pub measurement_mode: String,
    /// Wait strategy used.
    pub wait_strategy: String,

    /// Configuration
    pub config: BenchConfig,

    /// Results
    pub results: BenchResults,

    /// Latency percentiles from real HDR histogram, or null if not measured.
    pub latency: Option<latency::LatencyStats>,

    /// Metadata
    pub metadata: BenchMetadata,
}

/// Benchmark configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchConfig {
    pub message_size_bytes: usize,
    pub buffer_depth: usize,
    pub num_messages: u64,
    pub warmup_messages: u64,
    pub num_producers: usize,
    pub num_consumers: usize,
}

/// Benchmark results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchResults {
    pub producer_throughput_ops_sec: f64,
    pub consumer_throughput_ops_sec: f64,
    pub data_rate_mbps: f64,
    pub messages_processed: u64,
    pub verification_passed: bool,
}

/// Benchmark metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchMetadata {
    pub timestamp: String,
    pub platform: String,
    pub cpu: String,
    pub git_commit: Option<String>,
    pub rust_version: String,
}

impl BenchMetadata {
    pub fn capture() -> Self {
        Self {
            timestamp: chrono::Utc::now().to_rfc3339(),
            platform: format!("{} {}", std::env::consts::OS, std::env::consts::ARCH),
            cpu: detect_cpu(),
            git_commit: detect_git_commit(),
            rust_version: env!("CARGO_PKG_RUST_VERSION")
                .parse()
                .unwrap_or_else(|_| "unknown".to_string()),
        }
    }
}

fn detect_cpu() -> String {
    #[cfg(target_os = "macos")]
    {
        Command::new("sysctl")
            .args(["-n", "machdep.cpu.brand_string"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| "Apple Silicon".to_string())
    }
    #[cfg(not(target_os = "macos"))]
    {
        std::fs::read_to_string("/proc/cpuinfo")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("model name"))
                    .map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string())
            })
            .unwrap_or_else(|| "unknown".to_string())
    }
}

fn detect_git_commit() -> Option<String> {
    Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
}

/// Collection of results from a benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchReport {
    pub metadata: BenchMetadata,
    pub results: Vec<BenchResult>,
}

#[derive(Debug, Clone)]
pub struct LayerComparisonEntry {
    pub payload_label: String,
    pub layer: String,
    pub consumers: usize,
    pub prod_ops: f64,
    pub cons_ops: f64,
}

#[derive(Debug, Clone)]
pub struct NofragMatrixEntry {
    pub payload_label: String,
    pub layer: String,
    pub backend: String,
    pub slot_size: usize,
    pub ring_depth: usize,
    pub producers: usize,
    pub consumers: usize,
    pub prod_ops: f64,
    pub cons_ops: f64,
    pub p50_ns: u64,
    pub p99_ns: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportView {
    Summary,
    Tree,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonsterSweepBackend {
    Shm,
    Mmap,
}

impl MonsterSweepBackend {
    fn title(self) -> &'static str {
        match self {
            Self::Shm => "SHM",
            Self::Mmap => "MMAP",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReportOutputArgs {
    pub json_mode: bool,
    pub quick_mode: bool,
    pub json_out: Option<String>,
    pub csv_out: Option<String>,
    pub markdown_out: Option<String>,
}

impl ReportOutputArgs {
    pub fn from_args(args: &[String]) -> Self {
        Self {
            json_mode: args.iter().any(|arg| arg == "--json"),
            quick_mode: args.iter().any(|arg| arg == "--quick"),
            json_out: find_arg_value(args, "--json-out"),
            csv_out: find_arg_value(args, "--csv-out"),
            markdown_out: find_arg_value(args, "--md-out"),
        }
    }
}

impl BenchReport {
    pub fn new() -> Self {
        Self {
            metadata: BenchMetadata::capture(),
            results: Vec::new(),
        }
    }

    pub fn add(&mut self, result: BenchResult) {
        self.results.push(result);
    }

    pub fn write_json(&self, path: &str) -> std::io::Result<()> {
        std::fs::write(path, serde_json::to_string_pretty(self)?)
    }

    pub fn write_csv(&self, path: &str) -> std::io::Result<()> {
        let mut csv = String::from(
            "scenario,backend,layer,codec,mode,strategy,msg_bytes,buffer,consumers,\
             prod_ops,cons_ops,mbps,p50_ns,p99_ns,p999_ns,p9999_ns,p99999_ns,verified\n",
        );
        for r in &self.results {
            let codec = r.codec.as_deref().unwrap_or("");
            let lat_field = |f: fn(&latency::LatencyStats) -> u64| -> String {
                r.latency
                    .as_ref()
                    .map(|l| f(l).to_string())
                    .unwrap_or_default()
            };
            csv.push_str(&format!(
                "{},{},{},{},{},{},{},{},{},{:.0},{:.0},{:.1},{},{},{},{},{},{}\n",
                r.scenario,
                r.backend,
                r.layer,
                codec,
                r.measurement_mode,
                r.wait_strategy,
                r.config.message_size_bytes,
                r.config.buffer_depth,
                r.config.num_consumers,
                r.results.producer_throughput_ops_sec,
                r.results.consumer_throughput_ops_sec,
                r.results.data_rate_mbps,
                lat_field(|l| l.p50_ns),
                lat_field(|l| l.p99_ns),
                lat_field(|l| l.p999_ns),
                lat_field(|l| l.p9999_ns),
                lat_field(|l| l.p99999_ns),
                r.results.verification_passed,
            ));
        }
        std::fs::write(path, csv)
    }

    /// Print a formatted summary table using `tabled`.
    pub fn print_summary(&self) {
        use tabled::{Table, Tabled};

        #[derive(Tabled)]
        struct Row {
            #[tabled(rename = "Scenario")]
            scenario: String,
            #[tabled(rename = "Bknd")]
            backend: String,
            #[tabled(rename = "Layer")]
            layer: String,
            #[tabled(rename = "Codec")]
            codec: String,
            #[tabled(rename = "Cons")]
            consumers: usize,
            #[tabled(rename = "Prod ops/s")]
            prod_ops: String,
            #[tabled(rename = "Cons ops/s")]
            cons_ops: String,
            #[tabled(rename = "P50")]
            p50: String,
            #[tabled(rename = "P99")]
            p99: String,
            #[tabled(rename = "P99.9")]
            p999: String,
            #[tabled(rename = "P99.99")]
            p9999: String,
            #[tabled(rename = "P99.999")]
            p99999: String,
        }

        let rows: Vec<Row> = self
            .results
            .iter()
            .map(|r| {
                let lat = |f: fn(&latency::LatencyStats) -> u64| {
                    r.latency
                        .as_ref()
                        .map(|l| latency::format_ns(f(l)))
                        .unwrap_or_else(|| "-".into())
                };
                Row {
                    scenario: r.scenario.clone(),
                    backend: r.backend.clone(),
                    layer: r.layer.clone(),
                    codec: r.codec.as_deref().unwrap_or("-").to_string(),
                    consumers: r.config.num_consumers,
                    prod_ops: format_throughput(r.results.producer_throughput_ops_sec),
                    cons_ops: format_throughput(r.results.consumer_throughput_ops_sec),
                    p50: lat(|l| l.p50_ns),
                    p99: lat(|l| l.p99_ns),
                    p999: lat(|l| l.p999_ns),
                    p9999: lat(|l| l.p9999_ns),
                    p99999: lat(|l| l.p99999_ns),
                }
            })
            .collect();

        println!("\n{}\n", Table::new(rows));
    }

    /// Render a divan-style tree with aligned metric columns.
    pub fn render_tree(&self) -> String {
        use std::collections::BTreeMap;

        let mut groups: BTreeMap<String, BTreeMap<String, Vec<&BenchResult>>> = BTreeMap::new();
        for result in &self.results {
            groups
                .entry(result.backend.clone())
                .or_default()
                .entry(result.layer.clone())
                .or_default()
                .push(result);
        }

        for layers in groups.values_mut() {
            for results in layers.values_mut() {
                results.sort_by(|a, b| {
                    (
                        a.config.message_size_bytes,
                        a.config.num_consumers,
                        a.measurement_mode.as_str(),
                        a.scenario.as_str(),
                    )
                        .cmp(&(
                            b.config.message_size_bytes,
                            b.config.num_consumers,
                            b.measurement_mode.as_str(),
                            b.scenario.as_str(),
                        ))
                });
            }
        }

        let title = self.infer_tree_title();
        let leaf_width = groups
            .values()
            .flat_map(|layers| layers.values())
            .flatten()
            .map(|result| compact_scenario_label(result))
            .map(|label| label.len())
            .max()
            .unwrap_or(24)
            .max(24);

        let mut out = String::new();
        out.push_str(&format!(
            "{}\n",
            format!(
                "{:<width$} payload depth  P  C   prod ops/s   cons ops/s   data rate      p50      p99   mode         wait",
                title,
                width = leaf_width + 16
            )
        ));

        let backends: Vec<_> = groups.into_iter().collect();
        for (backend_idx, (backend, layers)) in backends.iter().enumerate() {
            let backend_is_last = backend_idx + 1 == backends.len();
            out.push_str(&format!(
                "{} {}\n",
                tree_branch(&[], backend_is_last),
                backend
            ));

            let layer_items: Vec<_> = layers.iter().collect();
            for (layer_idx, (layer, results)) in layer_items.iter().enumerate() {
                let layer_is_last = layer_idx + 1 == layer_items.len();
                out.push_str(&format!(
                    "{} {}\n",
                    tree_branch(&[backend_is_last], layer_is_last),
                    layer
                ));

                for (result_idx, result) in results.iter().enumerate() {
                    let result_is_last = result_idx + 1 == results.len();
                    let label = compact_scenario_label(result);
                    let payload = human_size(result.config.message_size_bytes);
                    let depth = human_depth(result.config.buffer_depth);
                    let prod = format_throughput(result.results.producer_throughput_ops_sec);
                    let cons = format_throughput(result.results.consumer_throughput_ops_sec);
                    let data_rate = human_data_rate(
                        result.results.consumer_throughput_ops_sec,
                        result.config.message_size_bytes,
                    );
                    let p50 = result
                        .latency
                        .as_ref()
                        .map(|l| latency::format_ns(l.p50_ns))
                        .unwrap_or_else(|| "-".into());
                    let p99 = result
                        .latency
                        .as_ref()
                        .map(|l| latency::format_ns(l.p99_ns))
                        .unwrap_or_else(|| "-".into());
                    let mode = compact_mode(&result.measurement_mode);
                    let wait = compact_wait(&result.wait_strategy);

                    out.push_str(&format!(
                        "{} {:<leaf_width$} {:>7} {:>5} {:>2} {:>2} {:>12} {:>12} {:>11} {:>8} {:>8} {:>12} {:>12}\n",
                        tree_branch(&[backend_is_last, layer_is_last], result_is_last),
                        label,
                        payload,
                        depth,
                        result.config.num_producers,
                        result.config.num_consumers,
                        prod,
                        cons,
                        data_rate,
                        p50,
                        p99,
                        mode,
                        wait,
                        leaf_width = leaf_width,
                    ));
                }
            }
        }

        out
    }

    /// Print a divan-style tree with aligned metric columns.
    pub fn print_tree(&self) {
        println!("\n{}", self.render_tree());
    }

    /// Write a markdown report file using tabled with markdown style.
    pub fn write_markdown(&self, path: &str) -> std::io::Result<()> {
        use tabled::{settings::Style, Table, Tabled};

        #[derive(Tabled)]
        struct Row {
            #[tabled(rename = "Scenario")]
            scenario: String,
            #[tabled(rename = "Backend")]
            backend: String,
            #[tabled(rename = "Layer")]
            layer: String,
            #[tabled(rename = "Codec")]
            codec: String,
            #[tabled(rename = "Consumers")]
            consumers: usize,
            #[tabled(rename = "Producer ops/s")]
            prod_ops: String,
            #[tabled(rename = "Consumer ops/s")]
            cons_ops: String,
            #[tabled(rename = "P50")]
            p50: String,
            #[tabled(rename = "P99")]
            p99: String,
            #[tabled(rename = "P99.9")]
            p999: String,
            #[tabled(rename = "P99.99")]
            p9999: String,
            #[tabled(rename = "P99.999")]
            p99999: String,
        }

        let rows: Vec<Row> = self
            .results
            .iter()
            .map(|r| {
                let lat = |f: fn(&latency::LatencyStats) -> u64| -> String {
                    r.latency
                        .as_ref()
                        .map(|l| latency::format_ns(f(l)))
                        .unwrap_or_else(|| "-".into())
                };
                Row {
                    scenario: r.scenario.clone(),
                    backend: r.backend.clone(),
                    layer: r.layer.clone(),
                    codec: r.codec.as_deref().unwrap_or("-").to_string(),
                    consumers: r.config.num_consumers,
                    prod_ops: format_throughput(r.results.producer_throughput_ops_sec),
                    cons_ops: format_throughput(r.results.consumer_throughput_ops_sec),
                    p50: lat(|l| l.p50_ns),
                    p99: lat(|l| l.p99_ns),
                    p999: lat(|l| l.p999_ns),
                    p9999: lat(|l| l.p9999_ns),
                    p99999: lat(|l| l.p99999_ns),
                }
            })
            .collect();

        #[derive(Tabled)]
        struct ConfigRow {
            #[tabled(rename = "Scenario")]
            scenario: String,
            #[tabled(rename = "Payload")]
            payload: String,
            #[tabled(rename = "Buffer Depth")]
            buffer: usize,
            #[tabled(rename = "Events")]
            events: u64,
            #[tabled(rename = "Warmup")]
            warmup: u64,
        }

        let config_rows: Vec<ConfigRow> = self
            .results
            .iter()
            .map(|r| {
                let size = if r.config.message_size_bytes >= 1_048_576 {
                    format!("{}MB", r.config.message_size_bytes / 1_048_576)
                } else if r.config.message_size_bytes >= 1024 {
                    format!("{}KB", r.config.message_size_bytes / 1024)
                } else {
                    format!("{}B", r.config.message_size_bytes)
                };
                ConfigRow {
                    scenario: r.scenario.clone(),
                    payload: size,
                    buffer: r.config.buffer_depth,
                    events: r.config.num_messages,
                    warmup: r.config.warmup_messages,
                }
            })
            .collect();

        let results_table = Table::new(rows).with(Style::markdown()).to_string();
        let config_table = Table::new(config_rows).with(Style::markdown()).to_string();

        let mut md = String::new();
        md.push_str("# Benchmark Report\n\n");
        md.push_str(&format!("- **Platform**: {}\n", self.metadata.platform));
        md.push_str(&format!("- **CPU**: {}\n", self.metadata.cpu));
        md.push_str(&format!("- **Timestamp**: {}\n", self.metadata.timestamp));
        if let Some(ref commit) = self.metadata.git_commit {
            md.push_str(&format!("- **Git Commit**: `{}`\n", commit));
        }
        md.push_str("\n## Results\n\n");
        md.push_str(&results_table);
        md.push_str("\n\n## Configuration\n\n");
        md.push_str(&config_table);
        md.push('\n');

        std::fs::write(path, md)
    }
}

pub fn print_layer_comparison(entries: &[LayerComparisonEntry]) {
    use tabled::{settings::Style, Table, Tabled};

    #[derive(Tabled)]
    struct Row {
        #[tabled(rename = "Payload")]
        payload: String,
        #[tabled(rename = "Layer")]
        layer: String,
        #[tabled(rename = "C")]
        consumers: String,
        #[tabled(rename = "Producer\n(ops/s)")]
        prod: String,
        #[tabled(rename = "Consumer\n(ops/s)")]
        cons: String,
        #[tabled(rename = "% of Raw\nRing")]
        pct: String,
    }

    let raw_baseline = |tag: &str, consumers: usize| -> Option<f64> {
        entries
            .iter()
            .find(|entry| {
                entry.layer == "raw_ring"
                    && entry.payload_label == tag
                    && entry.consumers == consumers
            })
            .map(|entry| entry.cons_ops)
    };

    let rows: Vec<Row> = entries
        .iter()
        .map(|entry| {
            let pct = if entry.layer == "raw_ring" {
                "100%".to_string()
            } else if let Some(baseline) = raw_baseline(&entry.payload_label, entry.consumers) {
                format!("{:.0}%", entry.cons_ops / baseline * 100.0)
            } else {
                "-".to_string()
            };
            Row {
                payload: entry.payload_label.clone(),
                layer: entry.layer.clone(),
                consumers: entry.consumers.to_string(),
                prod: format_throughput(entry.prod_ops),
                cons: format_throughput(entry.cons_ops),
                pct,
            }
        })
        .collect();

    println!("\n{}", Table::new(rows).with(Style::modern()));
}

pub fn print_nofrag_matrix(entries: &[NofragMatrixEntry]) {
    use tabled::{settings::Style, Table, Tabled};

    #[derive(Tabled)]
    struct Row {
        #[tabled(rename = "Payload")]
        size: String,
        #[tabled(rename = "Layer")]
        layer: String,
        #[tabled(rename = "Backend")]
        backend: String,
        #[tabled(rename = "Slot")]
        slot: String,
        #[tabled(rename = "Depth")]
        depth: String,
        #[tabled(rename = "Ring")]
        ring_size: String,
        #[tabled(rename = "P")]
        producers: String,
        #[tabled(rename = "C")]
        consumers: String,
        #[tabled(rename = "Producer\n(ops/s)")]
        prod: String,
        #[tabled(rename = "Consumer\n(ops/s)")]
        cons: String,
        #[tabled(rename = "P50")]
        p50: String,
        #[tabled(rename = "P99")]
        p99: String,
        #[tabled(rename = "% of\nRaw")]
        pct: String,
    }

    let ring_size = |slot_size: usize, depth: usize| -> String {
        let total = slot_size * depth;
        if total >= 1024 * 1024 * 1024 {
            format!("{:.1}GB", total as f64 / (1024.0 * 1024.0 * 1024.0))
        } else if total >= 1024 * 1024 {
            format!("{}MB", total / (1024 * 1024))
        } else {
            format!("{}KB", total / 1024)
        }
    };

    let raw_baseline = |payload_label: &str, backend: &str, consumers: usize| -> Option<f64> {
        entries
            .iter()
            .find(|entry| {
                entry.layer == "raw_ring"
                    && entry.payload_label == payload_label
                    && entry.backend == backend
                    && entry.consumers == consumers
            })
            .map(|entry| entry.cons_ops)
    };

    let rows: Vec<Row> = entries
        .iter()
        .map(|entry| {
            let baseline = raw_baseline(&entry.payload_label, &entry.backend, entry.consumers);
            let pct = if entry.layer == "raw_ring" {
                "100%".to_string()
            } else if let Some(baseline) = baseline {
                format!("{:.0}%", entry.cons_ops / baseline * 100.0)
            } else {
                "-".to_string()
            };
            Row {
                size: entry.payload_label.clone(),
                layer: entry.layer.clone(),
                backend: entry.backend.clone(),
                slot: human_size(entry.slot_size),
                depth: entry.ring_depth.to_string(),
                ring_size: ring_size(entry.slot_size, entry.ring_depth),
                producers: entry.producers.to_string(),
                consumers: entry.consumers.to_string(),
                prod: format_throughput(entry.prod_ops),
                cons: format_throughput(entry.cons_ops),
                p50: if entry.p50_ns == 0 {
                    "-".into()
                } else {
                    latency::format_ns(entry.p50_ns)
                },
                p99: if entry.p99_ns == 0 {
                    "-".into()
                } else {
                    latency::format_ns(entry.p99_ns)
                },
                pct,
            }
        })
        .collect();

    println!("\n{}", Table::new(rows).with(Style::modern()));
}

impl Default for BenchReport {
    fn default() -> Self {
        Self::new()
    }
}

pub fn emit_report(
    report: &BenchReport,
    output_args: &ReportOutputArgs,
    default_view: Option<ReportView>,
    quick_view: Option<ReportView>,
    markdown_writer: Option<fn(&BenchReport, &str) -> io::Result<()>>,
) {
    if output_args.json_mode {
        println!(
            "{}",
            serde_json::to_string_pretty(report).expect("serialize report")
        );
    } else if output_args.quick_mode {
        match quick_view {
            Some(ReportView::Tree) => report.print_tree(),
            Some(ReportView::Summary) => report.print_summary(),
            None => {}
        }
    } else {
        match default_view {
            Some(ReportView::Tree) => report.print_tree(),
            Some(ReportView::Summary) => report.print_summary(),
            None => {}
        }
    }

    if let Some(path) = output_args.json_out.as_deref() {
        report.write_json(path).expect("write JSON");
        eprintln!("JSON written to {path}");
    }
    if let Some(path) = output_args.csv_out.as_deref() {
        report.write_csv(path).expect("write CSV");
        eprintln!("CSV written to {path}");
    }
    if let Some(path) = output_args.markdown_out.as_deref() {
        if let Some(writer) = markdown_writer {
            writer(report, path).expect("write markdown");
        } else {
            report.write_markdown(path).expect("write markdown");
        }
        eprintln!("Markdown written to {path}");
    }
    if let Ok(path) = std::env::var("MYELON_BENCH_JSON_OUT") {
        report.write_json(&path).expect("write JSON");
    }
}

fn find_arg_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|window| window[0] == flag)
        .map(|window| window[1].clone())
}

fn infer_benchmark_name(result: &BenchResult) -> Option<&str> {
    result.benchmark_id.split('/').nth(1)
}

fn compact_scenario_label(result: &BenchResult) -> String {
    result
        .scenario
        .strip_prefix("sweep_")
        .unwrap_or(&result.scenario)
        .replace('_', " ")
}

fn human_size(bytes: usize) -> String {
    if bytes >= 1_048_576 {
        format!("{}MB", bytes / 1_048_576)
    } else if bytes >= 1024 {
        format!("{}KB", bytes / 1024)
    } else {
        format!("{}B", bytes)
    }
}

fn human_depth(depth: usize) -> String {
    if depth >= 1_000_000 {
        format!("{}M", depth / 1_000_000)
    } else if depth >= 1000 {
        format!("{}K", depth / 1000)
    } else {
        depth.to_string()
    }
}

fn human_data_rate(ops_sec: f64, payload_bytes: usize) -> String {
    let bytes_per_sec = ops_sec * payload_bytes as f64;
    if bytes_per_sec >= 1e9 {
        format!("{:.1}GB/s", bytes_per_sec / 1e9)
    } else if bytes_per_sec >= 1e6 {
        format!("{:.0}MB/s", bytes_per_sec / 1e6)
    } else if bytes_per_sec >= 1e3 {
        format!("{:.0}KB/s", bytes_per_sec / 1e3)
    } else {
        format!("{:.0}B/s", bytes_per_sec)
    }
}

fn compact_mode(mode: &str) -> String {
    mode.strip_prefix("co_aware@")
        .map(|rate| format!("CO@{}", rate))
        .unwrap_or_else(|| mode.to_string())
}

fn compact_wait(wait_strategy: &str) -> String {
    match wait_strategy {
        "BusySpin" | "BusySpinWithSpinLoopHint" | "Block" | "Sleep" => wait_strategy.to_string(),
        other => other.to_string(),
    }
}

fn tree_branch(ancestor_last: &[bool], is_last: bool) -> String {
    let mut prefix = String::new();
    for last in ancestor_last {
        prefix.push_str(if *last { "   " } else { "│  " });
    }
    prefix.push_str(if is_last { "╰─" } else { "├─" });
    prefix
}

impl BenchReport {
    fn infer_tree_title(&self) -> String {
        let mut names = self
            .results
            .iter()
            .filter_map(infer_benchmark_name)
            .collect::<std::collections::BTreeSet<_>>();
        if names.len() == 1 {
            format!("perf-bench/{}", names.pop_first().unwrap_or("report"))
        } else {
            "perf-bench".to_string()
        }
    }
}

/// Build a CO latency matrix: rows = payload sizes, columns = rate groups (each spanning P50/P90/P99/P99.9).
/// Uses `tabled::Builder` + `Span::column(4)` for multi-column rate headers.
/// Returns None if no CO results exist.
pub fn build_co_matrix(results: &[BenchResult], use_markdown: bool) -> Option<String> {
    use tabled::builder::Builder;
    use tabled::settings::{object::Cell, Alignment, Span, Style};

    let co_results: Vec<&BenchResult> = results
        .iter()
        .filter(|r| r.measurement_mode.starts_with("co_aware") && r.latency.is_some())
        .collect();

    if co_results.is_empty() {
        return None;
    }

    // Collect unique rates and sizes (sorted)
    let rates: Vec<u64> = co_results
        .iter()
        .filter_map(|r| {
            r.measurement_mode
                .strip_prefix("co_aware@")
                .and_then(|s| s.parse().ok())
        })
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let sizes: Vec<usize> = co_results
        .iter()
        .map(|r| r.config.message_size_bytes)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();

    let fmt_rate = |r: u64| -> String {
        if r >= 1_000_000 {
            format!("{}M/s", r / 1_000_000)
        } else {
            format!("{}K/s", r / 1000)
        }
    };
    let fmt_size = |b: usize| -> String {
        if b >= 1_048_576 {
            format!("{}MB", b / 1_048_576)
        } else if b >= 1024 {
            format!("{}KB", b / 1024)
        } else {
            format!("{}B", b)
        }
    };

    let mut builder = Builder::default();

    // Row 0: rate group headers — each spans 4 sub-columns
    let mut header0 = vec!["Size".to_string()];
    for rate in &rates {
        header0.push(fmt_rate(*rate));
        header0.push(String::new());
        header0.push(String::new());
        header0.push(String::new());
    }
    builder.push_record(header0);

    // Row 1: percentile sub-headers
    let mut header1 = vec![String::new()];
    for _ in &rates {
        header1.push("P50".into());
        header1.push("P90".into());
        header1.push("P99".into());
        header1.push("P99.9".into());
    }
    builder.push_record(header1);

    // Data rows — one per payload size
    for sz in &sizes {
        let mut row = vec![fmt_size(*sz)];
        for rate in &rates {
            let entry = co_results.iter().find(|r| {
                r.config.message_size_bytes == *sz
                    && r.measurement_mode == format!("co_aware@{}", rate)
            });
            if let Some(r) = entry {
                let l = r.latency.as_ref().unwrap();
                let p99 = l.p99_ns;
                let indicator = if p99 < 1_000 {
                    "✓"
                } else if p99 < 10_000_000 {
                    "△"
                } else {
                    "✗"
                };
                row.push(latency::format_ns(l.p50_ns));
                row.push(latency::format_ns(l.p90_ns));
                row.push(format!("{}{}", latency::format_ns(l.p99_ns), indicator));
                row.push(latency::format_ns(l.p999_ns));
            } else {
                row.extend(["-".into(), "-".into(), "-".into(), "-".into()]);
            }
        }
        builder.push_record(row);
    }

    let mut table = builder.build();

    // Apply spans: each rate header in row 0 spans 4 columns
    for (i, _) in rates.iter().enumerate() {
        let col = 1 + i * 4;
        table.modify(Cell::new(0, col), Span::column(4));
        table.modify(Cell::new(0, col), Alignment::center());
    }

    if use_markdown {
        table.with(Style::markdown());
    } else {
        table.with(Style::modern());
    }

    Some(table.to_string())
}

pub fn print_monster_sweep_report(report: &BenchReport, backend: MonsterSweepBackend) {
    use tabled::{settings::Style, Table, Tabled};

    const HW_BW_GBS: f64 = 300.0;

    println!();
    println!("{}", "=".repeat(100));
    println!(
        "        DISRUPTOR-MP MONSTER SWEEP -- {} BACKEND",
        backend.title()
    );
    println!("{}", "=".repeat(100));
    println!();
    println!(
        "System: {} | {}",
        report.metadata.cpu, report.metadata.platform
    );
    println!("Memory: 96GB unified | {} GB/s bandwidth", HW_BW_GBS as u64);
    if let Some(ref commit) = report.metadata.git_commit {
        println!("Git:    {} (perf_bench)", commit);
    }
    println!("Time:   {}", report.metadata.timestamp);
    println!("Config: Full payload fill (producer) + checksum (consumer)");
    println!();

    #[derive(Tabled)]
    struct ThroughputRow {
        #[tabled(rename = "Payload")]
        payload: String,
        #[tabled(rename = "Total\nMemory")]
        total_mem: String,
        #[tabled(rename = "Events")]
        events: String,
        #[tabled(rename = "Producer\n(ops/s)")]
        prod: String,
        #[tabled(rename = "Consumer\n(ops/s)")]
        cons: String,
        #[tabled(rename = "Data Rate\n(MB/s)")]
        data_rate: String,
        #[tabled(rename = "Bandwidth\n(GB/s)")]
        bw: String,
        #[tabled(rename = "% of HW\nLimit")]
        pct: String,
        #[tabled(rename = "P50")]
        p50: String,
        #[tabled(rename = "P99")]
        p99: String,
        #[tabled(rename = " ")]
        status: String,
    }

    let throughput_rows: Vec<ThroughputRow> = report
        .results
        .iter()
        .filter(|r| r.config.num_consumers == 1 && !r.measurement_mode.starts_with("co_aware"))
        .map(|result| {
            let size = result.config.message_size_bytes;
            let bw = result.results.consumer_throughput_ops_sec * size as f64 / 1e9;
            let pct = bw / HW_BW_GBS * 100.0;
            let is_signal = result.scenario.contains("SIG");
            ThroughputRow {
                payload: if is_signal {
                    "signal".to_string()
                } else {
                    human_size(size)
                },
                total_mem: monster_sweep_ring_label(size, result.config.buffer_depth),
                events: human_events(result.config.num_messages),
                prod: format_throughput(result.results.producer_throughput_ops_sec),
                cons: format_throughput(result.results.consumer_throughput_ops_sec),
                data_rate: format!(
                    "{:.0}",
                    result.results.consumer_throughput_ops_sec * size as f64 / 1e6
                ),
                bw: format!("{:.1}", bw),
                pct: if is_signal {
                    "seq-ctr".into()
                } else {
                    format!("{pct:.1}%")
                },
                p50: result
                    .latency
                    .as_ref()
                    .map(|latency| latency::format_ns(latency.p50_ns))
                    .unwrap_or("-".into()),
                p99: result
                    .latency
                    .as_ref()
                    .map(|latency| latency::format_ns(latency.p99_ns))
                    .unwrap_or("-".into()),
                status: if is_signal || pct > 10.0 {
                    "✓".to_string()
                } else if pct > 1.0 {
                    "△".to_string()
                } else {
                    "✗".to_string()
                },
            }
        })
        .collect();

    if !throughput_rows.is_empty() {
        println!("{}", "-".repeat(100));
        println!("  THROUGHPUT SWEEP (1p1c)");
        println!("{}", "-".repeat(100));
        println!("{}", Table::new(throughput_rows).with(Style::modern()));
        println!("Legend: ✓ = >10% BW efficiency | △ = >1% | ✗ = <1%");
        println!();
    }

    #[derive(Tabled)]
    struct ScalingRow {
        #[tabled(rename = "Payload")]
        payload: String,
        #[tabled(rename = "1p1c")]
        c1: String,
        #[tabled(rename = "1p2c")]
        c2: String,
        #[tabled(rename = "1p4c")]
        c4: String,
        #[tabled(rename = "1p6c")]
        c6: String,
        #[tabled(rename = "1p8c")]
        c8: String,
        #[tabled(rename = "1p10c")]
        c10: String,
        #[tabled(rename = "1p12c")]
        c12: String,
    }

    let scaling_sizes: Vec<usize> = report
        .results
        .iter()
        .filter(|r| !r.measurement_mode.starts_with("co_aware") && !r.scenario.contains("SIG"))
        .map(|r| r.config.message_size_bytes)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();

    let mut scaling_rows = Vec::new();
    for size in &scaling_sizes {
        let find = |consumers: usize| -> Option<f64> {
            report
                .results
                .iter()
                .find(|r| {
                    r.config.message_size_bytes == *size
                        && r.config.num_consumers == consumers
                        && !r.measurement_mode.starts_with("co_aware")
                })
                .map(|r| r.results.consumer_throughput_ops_sec)
        };
        let c1 = find(1);
        let has_multi = [2, 4, 6, 8, 10, 12]
            .iter()
            .any(|&consumers| find(consumers).is_some());
        if has_multi {
            let baseline = c1.unwrap_or(1.0);
            let format_consumer = |consumers: usize| -> String {
                find(consumers)
                    .map(|value| {
                        format!(
                            "{} ({:.0}%)",
                            format_throughput(value),
                            value / baseline * 100.0
                        )
                    })
                    .unwrap_or("-".into())
            };
            scaling_rows.push(ScalingRow {
                payload: human_size(*size),
                c1: c1.map(format_throughput).unwrap_or("-".into()),
                c2: format_consumer(2),
                c4: format_consumer(4),
                c6: format_consumer(6),
                c8: format_consumer(8),
                c10: format_consumer(10),
                c12: format_consumer(12),
            });
        }
    }

    if !scaling_rows.is_empty() {
        println!("{}", "-".repeat(100));
        println!("  CONSUMER SCALING");
        println!("{}", "-".repeat(100));
        println!("{}", Table::new(scaling_rows).with(Style::modern()));
        println!();
    }

    if let Some(matrix) = build_co_matrix(&report.results, false) {
        println!("{}", "-".repeat(100));
        println!("  COORDINATED OMISSION LATENCY MATRIX");
        println!("{}", "-".repeat(100));
        println!("{matrix}");
        println!("Legend: ✓ P99 CO < 1us (Excellent) | △ P99 CO < 10ms (Good) | ✗ P99 CO >= 10ms (Saturated)");
        println!("All latencies are Coordinated Omission corrected, showing true user-experienced delays");
        println!();
    }

    println!("{}", "=".repeat(100));
    println!("  KEY FINDINGS");
    println!("{}", "=".repeat(100));

    if let Some(signal) = report.results.iter().find(|r| r.scenario.contains("SIG")) {
        println!(
            "  Signal ceiling:    {} ops/s (sequence-counter bound)",
            format_throughput(signal.results.consumer_throughput_ops_sec)
        );
    }

    let peak_bw = report
        .results
        .iter()
        .filter(|r| r.config.num_consumers == 1 && r.measurement_mode == "max_throughput")
        .map(|r| {
            (
                r.results.consumer_throughput_ops_sec * r.config.message_size_bytes as f64 / 1e9,
                r.config.message_size_bytes,
            )
        })
        .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    if let Some((bw, size)) = peak_bw {
        println!(
            "  Peak bandwidth:    {:.1} GB/s @ {} ({:.1}% of {} GB/s HW limit)",
            bw,
            human_size(size),
            bw / HW_BW_GBS * 100.0,
            HW_BW_GBS as u64
        );
    }

    let best_co = report
        .results
        .iter()
        .filter(|r| r.measurement_mode.starts_with("co_aware") && r.latency.is_some())
        .min_by_key(|r| r.latency.as_ref().unwrap().p99_ns);
    if let Some(result) = best_co {
        let latency = result.latency.as_ref().unwrap();
        println!(
            "  Best CO P99:       {} @ {} ({}K ops/s sustained)",
            latency::format_ns(latency.p99_ns),
            human_size(result.config.message_size_bytes),
            result.results.producer_throughput_ops_sec as u64 / 1000
        );
    }

    let worst_co = report
        .results
        .iter()
        .filter(|r| r.measurement_mode.starts_with("co_aware") && r.latency.is_some())
        .max_by_key(|r| r.latency.as_ref().unwrap().p99_ns);
    if let Some(result) = worst_co {
        let latency = result.latency.as_ref().unwrap();
        if latency.p99_ns > 10_000_000 {
            println!(
                "  Worst CO P99:      {} @ {} ({}K/s -- above throughput ceiling)",
                latency::format_ns(latency.p99_ns),
                human_size(result.config.message_size_bytes),
                result.results.producer_throughput_ops_sec as u64 / 1000
            );
        }
    }

    println!("{}", "=".repeat(100));
    println!();
}

pub fn write_monster_sweep_markdown(
    report: &BenchReport,
    path: &str,
    backend: MonsterSweepBackend,
) -> io::Result<()> {
    use tabled::{settings::Style, Table, Tabled};

    const HW_BW_GBS: f64 = 300.0;

    let mut md = String::new();
    md.push_str(&format!(
        "# Disruptor-MP Monster Sweep — {} Backend\n\n",
        backend.title()
    ));
    md.push_str(&format!("- **CPU**: {}\n", report.metadata.cpu));
    md.push_str(&format!("- **Platform**: {}\n", report.metadata.platform));
    md.push_str("- **Memory**: 96GB unified, 300 GB/s bandwidth\n");
    if let Some(ref commit) = report.metadata.git_commit {
        md.push_str(&format!("- **Git**: `{}`\n", commit));
    }
    md.push_str(&format!("- **Timestamp**: {}\n", report.metadata.timestamp));
    md.push_str("- **Config**: Full payload fill (producer) + checksum (consumer)\n\n");

    #[derive(Tabled)]
    struct ThroughputRow {
        #[tabled(rename = "Payload")]
        payload: String,
        #[tabled(rename = "Total Memory")]
        total_mem: String,
        #[tabled(rename = "Events")]
        events: String,
        #[tabled(rename = "Producer (ops/s)")]
        prod: String,
        #[tabled(rename = "Consumer (ops/s)")]
        cons: String,
        #[tabled(rename = "Data Rate (MB/s)")]
        data_rate: String,
        #[tabled(rename = "BW (GB/s)")]
        bw: String,
        #[tabled(rename = "% HW Limit")]
        pct: String,
        #[tabled(rename = "P50")]
        p50: String,
        #[tabled(rename = "P99")]
        p99: String,
        #[tabled(rename = " ")]
        status: String,
    }

    let throughput_rows: Vec<ThroughputRow> = report
        .results
        .iter()
        .filter(|r| r.config.num_consumers == 1 && !r.measurement_mode.starts_with("co_aware"))
        .map(|result| {
            let size = result.config.message_size_bytes;
            let bw = result.results.consumer_throughput_ops_sec * size as f64 / 1e9;
            let pct = bw / HW_BW_GBS * 100.0;
            let is_signal = result.scenario.contains("SIG");
            ThroughputRow {
                payload: if is_signal {
                    "signal".into()
                } else {
                    human_size(size)
                },
                total_mem: monster_sweep_ring_label(size, result.config.buffer_depth),
                events: human_events(result.config.num_messages),
                prod: format_throughput(result.results.producer_throughput_ops_sec),
                cons: format_throughput(result.results.consumer_throughput_ops_sec),
                data_rate: format!(
                    "{:.0}",
                    result.results.consumer_throughput_ops_sec * size as f64 / 1e6
                ),
                bw: format!("{:.1}", bw),
                pct: if is_signal {
                    "seq-ctr".into()
                } else {
                    format!("{pct:.1}%")
                },
                p50: result
                    .latency
                    .as_ref()
                    .map(|latency| latency::format_ns(latency.p50_ns))
                    .unwrap_or("-".into()),
                p99: result
                    .latency
                    .as_ref()
                    .map(|latency| latency::format_ns(latency.p99_ns))
                    .unwrap_or("-".into()),
                status: if is_signal || pct > 10.0 {
                    "✓".into()
                } else if pct > 1.0 {
                    "△".into()
                } else {
                    "✗".into()
                },
            }
        })
        .collect();

    md.push_str("## Throughput Sweep (1p1c)\n\n");
    md.push_str(
        &Table::new(throughput_rows)
            .with(Style::markdown())
            .to_string(),
    );
    md.push_str("\n\nLegend: ✓ = >10% BW efficiency | △ = >1% | ✗ = <1%\n\n");

    #[derive(Tabled)]
    struct ScaleRow {
        #[tabled(rename = "Consumers")]
        consumers: String,
        #[tabled(rename = "Consumer (ops/s)")]
        cons: String,
        #[tabled(rename = "% of 1p1c")]
        pct: String,
        #[tabled(rename = "BW (GB/s)")]
        bw: String,
    }

    let scaling_sizes: Vec<usize> = report
        .results
        .iter()
        .filter(|r| {
            !r.measurement_mode.starts_with("co_aware")
                && !r.scenario.contains("SIG")
                && r.config.num_consumers > 1
        })
        .map(|r| r.config.message_size_bytes)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();

    if !scaling_sizes.is_empty() {
        md.push_str("## Consumer Scaling\n\n");
        for size in &scaling_sizes {
            let find = |consumers: usize| -> Option<f64> {
                report
                    .results
                    .iter()
                    .find(|r| {
                        r.config.message_size_bytes == *size
                            && r.config.num_consumers == consumers
                            && !r.measurement_mode.starts_with("co_aware")
                    })
                    .map(|r| r.results.consumer_throughput_ops_sec)
            };
            let baseline = find(1).unwrap_or(1.0);
            let rows: Vec<ScaleRow> = [1, 2, 4, 6, 8, 10, 12]
                .iter()
                .filter_map(|&consumers| {
                    find(consumers).map(|value| ScaleRow {
                        consumers: format!("1p{consumers}c"),
                        cons: format_throughput(value),
                        pct: format!("{:.0}%", value / baseline * 100.0),
                        bw: format!("{:.1}", value * *size as f64 / 1e9),
                    })
                })
                .collect();
            if !rows.is_empty() {
                md.push_str(&format!("### {} payload\n\n", human_size(*size)));
                md.push_str(&Table::new(rows).with(Style::markdown()).to_string());
                md.push_str("\n\n");
            }
        }
    }

    if let Some(matrix) = build_co_matrix(&report.results, true) {
        md.push_str("## Coordinated Omission Latency Matrix\n\n");
        md.push_str(&matrix);
        md.push_str("\n\nLegend: ✓ P99 < 1us (Excellent) | △ P99 < 10ms (Good) | ✗ P99 >= 10ms (Saturated)\n\n");
        md.push_str("All latencies are Coordinated Omission corrected, showing true user-experienced delays.\n\n");
    }

    md.push_str("## Performance Summary\n\n");
    if let Some(signal) = report.results.iter().find(|r| r.scenario.contains("SIG")) {
        md.push_str(&format!(
            "- **Signal ceiling**: {} ops/s (sequence-counter bound)\n",
            format_throughput(signal.results.consumer_throughput_ops_sec)
        ));
    }

    let peak_bw = report
        .results
        .iter()
        .filter(|r| r.config.num_consumers == 1 && !r.measurement_mode.starts_with("co_aware"))
        .map(|r| {
            (
                r.results.consumer_throughput_ops_sec * r.config.message_size_bytes as f64 / 1e9,
                r.config.message_size_bytes,
            )
        })
        .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    if let Some((bw, size)) = peak_bw {
        md.push_str(&format!(
            "- **Peak bandwidth**: {:.1} GB/s @ {} ({:.1}% of {} GB/s HW limit)\n",
            bw,
            human_size(size),
            bw / HW_BW_GBS * 100.0,
            HW_BW_GBS as u64
        ));
    }

    let best_co = report
        .results
        .iter()
        .filter(|r| r.measurement_mode.starts_with("co_aware") && r.latency.is_some())
        .min_by_key(|r| r.latency.as_ref().unwrap().p99_ns);
    if let Some(result) = best_co {
        let latency = result.latency.as_ref().unwrap();
        let rate = co_rate(&result.measurement_mode).unwrap_or(0);
        md.push_str(&format!(
            "- **Best CO P99**: {} @ {} ({}K ops/s sustained)\n",
            latency::format_ns(latency.p99_ns),
            human_size(result.config.message_size_bytes),
            rate / 1000
        ));
    }

    let sustainable: Vec<String> = report
        .results
        .iter()
        .filter(|r| {
            r.measurement_mode.starts_with("co_aware")
                && r.latency
                    .as_ref()
                    .map(|latency| latency.p99_ns < 10_000_000)
                    .unwrap_or(false)
        })
        .map(|result| {
            format!(
                "{}@{}K/s",
                human_size(result.config.message_size_bytes),
                co_rate(&result.measurement_mode).unwrap_or(0) / 1000
            )
        })
        .collect();
    if !sustainable.is_empty() {
        md.push_str(&format!(
            "- **Sustainable (P99 < 10ms)**: {}\n",
            sustainable.join(", ")
        ));
    }

    md.push_str("\n## Legend\n\n");
    md.push_str("- ✓ = P99 CO < 1us (Excellent -- true low-latency performance)\n");
    md.push_str("- △ = P99 CO < 10ms (Good -- acceptable for most applications)\n");
    md.push_str("- ✗ = P99 CO >= 10ms (Poor -- significant queueing delays)\n");

    std::fs::write(path, md)
}

fn monster_sweep_ring_label(message_size_bytes: usize, buffer_depth: usize) -> String {
    let ring_bytes = message_size_bytes as u64 * buffer_depth as u64;
    if ring_bytes >= 1024 * 1024 * 1024 {
        format!("{}GB", ring_bytes / (1024 * 1024 * 1024))
    } else {
        format!("{}MB", ring_bytes / (1024 * 1024))
    }
}

fn human_events(num_messages: u64) -> String {
    if num_messages >= 1_000_000 {
        format!("{}M", num_messages / 1_000_000)
    } else if num_messages >= 1_000 {
        format!("{}K", num_messages / 1_000)
    } else {
        num_messages.to_string()
    }
}

fn co_rate(measurement_mode: &str) -> Option<u64> {
    measurement_mode
        .strip_prefix("co_aware@")
        .and_then(|rate| rate.parse().ok())
}

/// Helper to build a BenchResult with common defaults.
pub fn make_result(
    bench_name: &str,
    scenario: &str,
    backend: &str,
    layer: &str,
    codec: Option<&str>,
    wait_strategy: &str,
    msg_bytes: usize,
    buffer_depth: usize,
    num_messages: u64,
    warmup: u64,
    consumers: usize,
    prod_ops: f64,
    cons_ops: f64,
    latency: Option<latency::LatencyStats>,
) -> BenchResult {
    BenchResult {
        benchmark_id: format!("myelon-bench/{bench_name}/{scenario}"),
        scenario: scenario.to_string(),
        backend: backend.to_string(),
        layer: layer.to_string(),
        codec: codec.map(|s| s.to_string()),
        measurement_mode: "max_throughput".to_string(),
        wait_strategy: wait_strategy.to_string(),
        config: BenchConfig {
            message_size_bytes: msg_bytes,
            buffer_depth,
            num_messages,
            warmup_messages: warmup,
            num_producers: 1,
            num_consumers: consumers,
        },
        results: BenchResults {
            producer_throughput_ops_sec: prod_ops,
            consumer_throughput_ops_sec: cons_ops,
            data_rate_mbps: crate::events::data_rate_mbps(prod_ops, msg_bytes),
            messages_processed: num_messages,
            verification_passed: true,
        },
        latency,
        metadata: BenchMetadata::capture(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonical_json_schema() {
        let result = make_result(
            "raw_ring_shm",
            "signal_1p1c_64B",
            "shm",
            "raw_ring",
            None,
            "BusySpin",
            64,
            65536,
            10_000_000,
            100_000,
            1,
            207_000_000.0,
            207_000_000.0,
            None,
        );
        let json = serde_json::to_string_pretty(&result).unwrap();
        // Verify schema fields exist
        assert!(json.contains("benchmark_id"));
        assert!(json.contains("measurement_mode"));
        assert!(json.contains("config"));
        assert!(json.contains("message_size_bytes"));
        assert!(json.contains("results"));
        assert!(json.contains("producer_throughput_ops_sec"));
        assert!(json.contains("metadata"));
        assert!(json.contains("platform"));
        // Round-trip
        let _: BenchResult = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn test_report_csv() {
        let mut report = BenchReport::new();
        report.add(make_result(
            "test",
            "test_scenario",
            "shm",
            "raw_ring",
            None,
            "BusySpin",
            144,
            1024,
            100_000,
            1_000,
            1,
            25_000_000.0,
            25_000_000.0,
            None,
        ));
        let dir = std::env::temp_dir();
        let path = dir.join("perf_bench_test.csv");
        report.write_csv(path.to_str().unwrap()).unwrap();
        let csv = std::fs::read_to_string(&path).unwrap();
        assert!(csv.contains("test_scenario"));
        assert!(csv.contains("shm"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_metadata_capture() {
        let meta = BenchMetadata::capture();
        assert!(!meta.timestamp.is_empty());
        assert!(!meta.platform.is_empty());
    }

    #[test]
    fn test_result_with_latency() {
        let mut recorder = crate::latency::LatencyRecorder::default_range();
        for _ in 0..1000 {
            recorder.record(250);
        }
        let result = make_result(
            "test",
            "test",
            "shm",
            "raw_ring",
            None,
            "BusySpin",
            64,
            1024,
            1000,
            100,
            1,
            1_000_000.0,
            1_000_000.0,
            recorder.stats(),
        );
        assert!(result.latency.is_some());
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("p50_ns"));
        assert!(json.contains("p99_ns"));
    }

    #[test]
    fn test_tree_render_contains_hierarchy_and_metrics() {
        let mut report = BenchReport::new();
        report.add(make_result(
            "monster_sweep_shm",
            "sweep_64B_1",
            "shm",
            "raw_ring",
            None,
            "BusySpin",
            64,
            65_536,
            10_000_000,
            100_000,
            1,
            200_000_000.0,
            180_000_000.0,
            None,
        ));
        report.add(make_result(
            "monster_sweep_mmap",
            "sweep_1K",
            "mmap",
            "raw_ring",
            None,
            "BusySpin",
            1024,
            16_384,
            100_000,
            1_000,
            1,
            1_500_000.0,
            1_400_000.0,
            None,
        ));

        let tree = report.render_tree();
        assert!(tree.contains("perf-bench"));
        assert!(tree.contains("shm"));
        assert!(tree.contains("mmap"));
        assert!(tree.contains("raw_ring"));
        assert!(tree.contains("64B 1"));
        assert!(tree.contains("1K"));
        assert!(tree.contains("prod ops/s"));
        assert!(tree.contains("cons ops/s"));
    }

    #[test]
    fn test_report_output_args_parse_common_flags() {
        let args = vec![
            "bench".to_string(),
            "--quick".to_string(),
            "--json-out".to_string(),
            "/tmp/out.json".to_string(),
            "--csv-out".to_string(),
            "/tmp/out.csv".to_string(),
            "--md-out".to_string(),
            "/tmp/out.md".to_string(),
        ];

        let parsed = ReportOutputArgs::from_args(&args);
        assert!(!parsed.json_mode);
        assert!(parsed.quick_mode);
        assert_eq!(parsed.json_out.as_deref(), Some("/tmp/out.json"));
        assert_eq!(parsed.csv_out.as_deref(), Some("/tmp/out.csv"));
        assert_eq!(parsed.markdown_out.as_deref(), Some("/tmp/out.md"));
    }
}
