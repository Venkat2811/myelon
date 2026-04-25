use crate::infra::events::format_throughput;
use crate::infra::output::report::{BenchReportCompat, ReportBundle};
use crate::infra::output::reporting::{BenchConfig, BenchReport, BenchResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tabled::{settings::Style, Table, Tabled};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricStats {
    pub mean: f64,
    pub min: f64,
    pub max: f64,
    pub stddev: f64,
    pub cv_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSample {
    pub run_index: usize,
    pub producer_throughput_ops_sec: f64,
    pub consumer_throughput_ops_sec: f64,
    pub verification_passed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_p50_ns: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_p99_ns: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioRepeatability {
    pub benchmark_id: String,
    pub scenario: String,
    pub backend: String,
    pub layer: String,
    pub codec: Option<String>,
    pub measurement_mode: String,
    pub wait_strategy: String,
    pub config: BenchConfig,
    pub runs: Vec<RunSample>,
    pub producer_throughput: MetricStats,
    pub consumer_throughput: MetricStats,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_p50_ns: Option<MetricStats>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_p99_ns: Option<MetricStats>,
    pub pass: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepeatabilityReport {
    pub generated_at: String,
    pub command: Vec<String>,
    pub repeat_count: usize,
    pub threshold_pct: f64,
    pub overall_pass: bool,
    pub scenarios: Vec<ScenarioRepeatability>,
}

#[derive(Tabled)]
struct SummaryRow {
    #[tabled(rename = "Scenario")]
    scenario: String,
    #[tabled(rename = "Runs")]
    runs: usize,
    #[tabled(rename = "Prod Mean")]
    producer_mean: String,
    #[tabled(rename = "Prod CV")]
    producer_cv: String,
    #[tabled(rename = "Cons Mean")]
    consumer_mean: String,
    #[tabled(rename = "Cons CV")]
    consumer_cv: String,
    #[tabled(rename = "P99 Mean")]
    p99_mean: String,
    #[tabled(rename = "Result")]
    result: String,
}

pub fn parse_bench_report(json: &str) -> Result<BenchReport, String> {
    serde_json::from_str::<BenchReport>(json)
        .map(|report| report.finalized())
        .or_else(|_| {
            serde_json::from_str::<BenchResult>(json).map(|result| {
                let mut report = BenchReport::new();
                report.add(result);
                report.finalized()
            })
        })
        .or_else(|_| {
            serde_json::from_str::<ReportBundle>(json)
                .map(|bundle| bundle.to_report_v1_compat().finalized())
        })
        .map_err(|error| format!("failed to parse canonical bench report JSON: {error}"))
}

pub fn aggregate_reports(
    command: &[String],
    reports: &[BenchReport],
    threshold_pct: f64,
) -> Result<RepeatabilityReport, String> {
    if reports.is_empty() {
        return Err("repeatability aggregation requires at least one report".into());
    }

    let indexed_reports: Vec<BTreeMap<String, BenchResult>> = reports
        .iter()
        .map(index_report)
        .collect::<Result<Vec<_>, _>>()?;
    let expected_keys: Vec<String> = indexed_reports[0].keys().cloned().collect();

    for (index, report) in indexed_reports.iter().enumerate().skip(1) {
        let keys: Vec<String> = report.keys().cloned().collect();
        if keys != expected_keys {
            return Err(format!(
                "run {} produced a different scenario set than run 0",
                index
            ));
        }
    }

    let scenarios = expected_keys
        .iter()
        .map(|key| build_scenario_repeatability(key, &indexed_reports, threshold_pct))
        .collect::<Result<Vec<_>, _>>()?;
    let overall_pass = scenarios.iter().all(|scenario| scenario.pass);

    Ok(RepeatabilityReport {
        generated_at: chrono::Utc::now().to_rfc3339(),
        command: command.to_vec(),
        repeat_count: reports.len(),
        threshold_pct,
        overall_pass,
        scenarios,
    })
}

pub fn render_summary_table(report: &RepeatabilityReport) -> String {
    let rows: Vec<SummaryRow> = report.scenarios.iter().map(SummaryRow::from).collect();
    Table::new(rows).with(Style::modern()).to_string()
}

pub fn render_markdown(report: &RepeatabilityReport) -> String {
    let rows: Vec<SummaryRow> = report.scenarios.iter().map(SummaryRow::from).collect();
    let mut md = String::new();
    md.push_str("# Repeatability Report\n\n");
    md.push_str(&format!("- **Generated At**: {}\n", report.generated_at));
    md.push_str(&format!("- **Repeats**: {}\n", report.repeat_count));
    md.push_str(&format!(
        "- **Threshold**: {:.2}% CV\n",
        report.threshold_pct
    ));
    md.push_str(&format!("- **Overall Pass**: {}\n", report.overall_pass));
    md.push_str(&format!(
        "- **Command**: `{}`\n",
        shell_words(report.command.iter())
    ));
    md.push_str("\n## Scenarios\n\n");
    md.push_str(&Table::new(rows).with(Style::markdown()).to_string());
    md.push('\n');
    md
}

fn shell_words<'a>(parts: impl IntoIterator<Item = &'a String>) -> String {
    parts
        .into_iter()
        .map(|part| {
            if part.contains(' ') {
                format!("\"{part}\"")
            } else {
                part.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn build_scenario_repeatability(
    key: &str,
    indexed_reports: &[BTreeMap<String, BenchResult>],
    threshold_pct: f64,
) -> Result<ScenarioRepeatability, String> {
    let baseline = indexed_reports[0]
        .get(key)
        .cloned()
        .ok_or_else(|| format!("missing baseline scenario {key}"))?;
    let results = indexed_reports
        .iter()
        .map(|report| {
            report
                .get(key)
                .cloned()
                .ok_or_else(|| format!("missing scenario {key} in repeat run"))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let runs: Vec<RunSample> = results
        .iter()
        .enumerate()
        .map(|(run_index, result)| RunSample {
            run_index,
            producer_throughput_ops_sec: result.results.producer_throughput_ops_sec,
            consumer_throughput_ops_sec: result.results.consumer_throughput_ops_sec,
            verification_passed: result.results.verification_passed,
            latency_p50_ns: result.latency.as_ref().map(|latency| latency.p50_ns),
            latency_p99_ns: result.latency.as_ref().map(|latency| latency.p99_ns),
        })
        .collect();

    let producer_samples: Vec<f64> = runs
        .iter()
        .map(|sample| sample.producer_throughput_ops_sec)
        .collect();
    let consumer_samples: Vec<f64> = runs
        .iter()
        .map(|sample| sample.consumer_throughput_ops_sec)
        .collect();
    let p50_samples: Vec<Option<u64>> = runs.iter().map(|sample| sample.latency_p50_ns).collect();
    let p99_samples: Vec<Option<u64>> = runs.iter().map(|sample| sample.latency_p99_ns).collect();

    let producer_throughput = metric_stats(&producer_samples);
    let consumer_throughput = metric_stats(&consumer_samples);
    let latency_p50_ns = optional_metric_stats(&p50_samples);
    let latency_p99_ns = optional_metric_stats(&p99_samples);

    let mut failures = Vec::new();
    if runs.iter().any(|sample| !sample.verification_passed) {
        failures.push("verification failed in at least one run".to_string());
    }
    if producer_throughput.cv_pct > threshold_pct {
        failures.push(format!(
            "producer throughput CV {:.2}% exceeded {:.2}%",
            producer_throughput.cv_pct, threshold_pct
        ));
    }
    if consumer_throughput.cv_pct > threshold_pct {
        failures.push(format!(
            "consumer throughput CV {:.2}% exceeded {:.2}%",
            consumer_throughput.cv_pct, threshold_pct
        ));
    }

    Ok(ScenarioRepeatability {
        benchmark_id: baseline.benchmark_id,
        scenario: baseline.scenario,
        backend: baseline.backend,
        layer: baseline.layer,
        codec: baseline.codec,
        measurement_mode: baseline.measurement_mode,
        wait_strategy: baseline.wait_strategy,
        config: baseline.config,
        runs,
        producer_throughput,
        consumer_throughput,
        latency_p50_ns,
        latency_p99_ns,
        pass: failures.is_empty(),
        failures,
    })
}

fn index_report(report: &BenchReport) -> Result<BTreeMap<String, BenchResult>, String> {
    let mut indexed = BTreeMap::new();
    for result in &report.results {
        if indexed
            .insert(result.benchmark_id.clone(), result.clone())
            .is_some()
        {
            return Err(format!(
                "duplicate benchmark_id in repeatability input: {}",
                result.benchmark_id
            ));
        }
    }
    Ok(indexed)
}

fn metric_stats(values: &[f64]) -> MetricStats {
    let count = values.len() as f64;
    let mean = values.iter().sum::<f64>() / count;
    let min = values.iter().copied().fold(f64::INFINITY, f64::min);
    let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let variance = if values.len() > 1 {
        values
            .iter()
            .map(|value| {
                let delta = value - mean;
                delta * delta
            })
            .sum::<f64>()
            / (count - 1.0)
    } else {
        0.0
    };
    let stddev = variance.sqrt();
    let cv_pct = if mean.abs() > f64::EPSILON {
        stddev / mean * 100.0
    } else {
        0.0
    };

    MetricStats {
        mean,
        min,
        max,
        stddev,
        cv_pct,
    }
}

fn optional_metric_stats(values: &[Option<u64>]) -> Option<MetricStats> {
    values
        .iter()
        .copied()
        .collect::<Option<Vec<_>>>()
        .map(|values| {
            let values: Vec<f64> = values.into_iter().map(|value| value as f64).collect();
            metric_stats(&values)
        })
}

impl From<&ScenarioRepeatability> for SummaryRow {
    fn from(scenario: &ScenarioRepeatability) -> Self {
        Self {
            scenario: scenario.scenario.clone(),
            runs: scenario.runs.len(),
            producer_mean: format_throughput(scenario.producer_throughput.mean),
            producer_cv: format!("{:.2}%", scenario.producer_throughput.cv_pct),
            consumer_mean: format_throughput(scenario.consumer_throughput.mean),
            consumer_cv: format!("{:.2}%", scenario.consumer_throughput.cv_pct),
            p99_mean: scenario
                .latency_p99_ns
                .as_ref()
                .map(|stats| format!("{:.0}ns", stats.mean))
                .unwrap_or_else(|| "-".to_string()),
            result: if scenario.pass {
                "PASS".to_string()
            } else {
                "FAIL".to_string()
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::latency::LatencyStats;
    use crate::infra::output::report::ReportBundleCompat;
    use crate::infra::output::reporting::{BenchMetadata, BenchResults};

    fn sample_result(
        benchmark_id: &str,
        scenario: &str,
        producer: f64,
        consumer: f64,
    ) -> BenchResult {
        BenchResult {
            benchmark_id: benchmark_id.to_string(),
            scenario: scenario.to_string(),
            backend: "shm".to_string(),
            layer: "raw_ring".to_string(),
            codec: None,
            measurement_mode: "max_throughput".to_string(),
            wait_strategy: "BusySpin".to_string(),
            config: BenchConfig {
                message_size_bytes: 64,
                payload_bytes: 64,
                buffer_depth: 1024,
                num_messages: 1_000,
                warmup_messages: 100,
                num_producers: 1,
                num_consumers: 2,
                coordination: None,
                discovery_mode: None,
                zero_copy: None,
                framing: None,
            },
            results: BenchResults {
                producer_throughput_ops_sec: producer,
                consumer_throughput_ops_sec: consumer,
                data_rate_mbps: 0.0,
                messages_processed: 1_000,
                verification_passed: true,
                producer_bandwidth_bytes_sec: None,
                producer_data_rate_gbps: None,
                consumer_avg_bandwidth_bytes_sec: None,
                consumer_avg_data_rate_gbps: None,
                consumer_min_throughput_ops_sec: None,
                consumer_max_throughput_ops_sec: None,
                consumer_total_throughput_ops_sec: None,
                consumer_checksum_total: None,
                phase_timing: None,
                pct_of_raw_ring: None,
                delta_vs_raw_ring_pct: None,
                speedup_vs_bincode: None,
                delta_vs_bincode_pct: None,
                access_avg_ns: None,
                access_vs_decode_speedup: None,
                alloc_count: None,
                alloc_bytes: None,
                hw_bandwidth_limit_gbps: None,
                hw_efficiency_pct: None,
                per_consumer: Vec::new(),
                layout_validation: None,
            },
            latency: Some(LatencyStats {
                count: 1_000,
                min_ns: 90,
                max_ns: 220,
                mean_ns: 110.0,
                stdev_ns: 12.0,
                p1_ns: 90,
                p10_ns: 95,
                p25_ns: 98,
                p50_ns: 100,
                p75_ns: 80,
                p90_ns: 120,
                p95_ns: 130,
                p99_ns: 150,
                p999_ns: 200,
                p9999_ns: 210,
                p99999_ns: 220,
                p999999_ns: 220,
            }),
            metadata: BenchMetadata::capture(),
        }
    }

    #[test]
    fn aggregate_reports_computes_cv_and_pass() {
        let mut run1 = BenchReport::new();
        run1.add(sample_result(
            "bench/raw_ring/signal_1p2c_64B",
            "signal_1p2c_64B",
            100.0,
            98.0,
        ));
        let mut run2 = BenchReport::new();
        run2.add(sample_result(
            "bench/raw_ring/signal_1p2c_64B",
            "signal_1p2c_64B",
            103.0,
            97.0,
        ));
        let mut run3 = BenchReport::new();
        run3.add(sample_result(
            "bench/raw_ring/signal_1p2c_64B",
            "signal_1p2c_64B",
            101.0,
            99.0,
        ));

        let report =
            aggregate_reports(&["cargo".into(), "bench".into()], &[run1, run2, run3], 10.0)
                .expect("aggregate");

        assert!(report.overall_pass);
        assert_eq!(report.repeat_count, 3);
        assert_eq!(report.scenarios.len(), 1);
        assert!(report.scenarios[0].producer_throughput.cv_pct < 10.0);
        assert!(report.scenarios[0].latency_p99_ns.is_some());
    }

    #[test]
    fn aggregate_reports_rejects_mismatched_scenarios() {
        let mut run1 = BenchReport::new();
        run1.add(sample_result("bench/raw_ring/a", "a", 100.0, 98.0));
        let mut run2 = BenchReport::new();
        run2.add(sample_result("bench/raw_ring/b", "b", 103.0, 97.0));

        let error = aggregate_reports(&["cargo".into(), "bench".into()], &[run1, run2], 10.0)
            .expect_err("mismatch should fail");
        assert!(error.contains("different scenario set"));
    }

    #[test]
    fn parse_bench_report_accepts_report_json() {
        let mut report = BenchReport::new();
        report.add(sample_result(
            "bench/raw_ring/signal_1p2c_64B",
            "signal_1p2c_64B",
            100.0,
            98.0,
        ));

        let json = report.to_report().to_json_pretty();
        let parsed = parse_bench_report(&json).expect("parse report v2 json");
        assert_eq!(parsed.results.len(), 1);
        assert_eq!(parsed.results[0].scenario, "signal_1p2c_64B");
    }
}
