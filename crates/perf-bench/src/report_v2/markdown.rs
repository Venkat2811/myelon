use super::model::{CodecKind, ReportBundle, ScenarioOutcome};

impl ReportBundle {
    pub fn write_markdown(&self, path: &str) -> std::io::Result<()> {
        if self
            .scenarios
            .iter()
            .all(|scenario| matches!(scenario.outcome, ScenarioOutcome::Layout(_)))
        {
            return std::fs::write(path, layout_markdown(self));
        }
        std::fs::write(path, throughput_markdown(self))
    }
}

fn layout_markdown(report: &ReportBundle) -> String {
    use tabled::{settings::Style, Table, Tabled};

    #[derive(Tabled)]
    struct LayoutRow {
        #[tabled(rename = "Target")]
        target: String,
        #[tabled(rename = "Backend")]
        backend: String,
        #[tabled(rename = "Layer")]
        layer: String,
        #[tabled(rename = "Avg (ns/op)")]
        avg_ns: String,
        #[tabled(rename = "Budget (ns)")]
        budget: String,
        #[tabled(rename = "Result")]
        result: String,
    }

    let rows: Vec<LayoutRow> = report
        .scenarios
        .iter()
        .map(|scenario| {
            let ScenarioOutcome::Layout(layout) = &scenario.outcome else {
                unreachable!("layout-only markdown path");
            };
            LayoutRow {
                target: scenario.identity.scenario.clone(),
                backend: backend_label(scenario),
                layer: scenario.identity.layer.clone(),
                avg_ns: layout.avg_ns.to_string(),
                budget: layout.budget_ns.to_string(),
                result: if layout.pass {
                    "PASS".into()
                } else {
                    "FAIL".into()
                },
            }
        })
        .collect();

    let mut md = String::new();
    md.push_str("# Layout Validation Report\n\n");
    md.push_str(&format!("- **Platform**: {}\n", report.metadata.platform));
    md.push_str(&format!("- **CPU**: {}\n", report.metadata.cpu));
    md.push_str(&format!("- **Timestamp**: {}\n", report.metadata.timestamp));
    if let Some(commit) = &report.metadata.git_commit {
        md.push_str(&format!("- **Git Commit**: `{commit}`\n"));
    }
    md.push_str("\n## Results\n\n");
    md.push_str(&Table::new(rows).with(Style::markdown()).to_string());
    md.push('\n');
    md
}

fn throughput_markdown(report: &ReportBundle) -> String {
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
        #[tabled(rename = "Mode")]
        mode: String,
        #[tabled(rename = "Consumers")]
        consumers: usize,
        #[tabled(rename = "Access")]
        access_avg: String,
        #[tabled(rename = "Access x")]
        access_speedup: String,
        #[tabled(rename = "Allocs")]
        alloc_count: String,
        #[tabled(rename = "Alloc bytes")]
        alloc_bytes: String,
        #[tabled(rename = "Prod ops/s")]
        prod_ops: String,
        #[tabled(rename = "Cons ops/s")]
        cons_ops: String,
        #[tabled(rename = "P50")]
        p50: String,
        #[tabled(rename = "P99")]
        p99: String,
    }

    let rows: Vec<Row> = report
        .scenarios
        .iter()
        .map(|scenario| {
            let ScenarioOutcome::Throughput(outcome) = &scenario.outcome else {
                unreachable!("throughput markdown path");
            };
            Row {
                scenario: scenario.identity.scenario.clone(),
                backend: backend_label(scenario),
                layer: scenario.identity.layer.clone(),
                codec: scenario
                    .identity
                    .codec
                    .as_ref()
                    .map(codec_label)
                    .unwrap_or_else(|| "-".to_string()),
                mode: measurement_label(scenario),
                consumers: scenario.config.workload.num_consumers,
                access_avg: outcome
                    .derived
                    .access_avg_ns
                    .map(format_avg_ns)
                    .unwrap_or_else(|| "-".to_string()),
                access_speedup: outcome
                    .derived
                    .access_vs_decode_speedup
                    .map(|value| format!("{value:.1}x"))
                    .unwrap_or_else(|| "-".to_string()),
                alloc_count: outcome
                    .derived
                    .alloc_count
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                alloc_bytes: outcome
                    .derived
                    .alloc_bytes
                    .map(format_alloc_bytes)
                    .unwrap_or_else(|| "-".to_string()),
                prod_ops: crate::events::format_throughput(outcome.producer.throughput_ops_sec),
                cons_ops: crate::events::format_throughput(
                    outcome.consumers.average_throughput_ops_sec,
                ),
                p50: outcome
                    .latency
                    .as_ref()
                    .map(|stats| crate::latency::format_ns(stats.p50_ns))
                    .unwrap_or_else(|| "-".to_string()),
                p99: outcome
                    .latency
                    .as_ref()
                    .map(|stats| crate::latency::format_ns(stats.p99_ns))
                    .unwrap_or_else(|| "-".to_string()),
            }
        })
        .collect();

    let mut md = String::new();
    md.push_str("# perf-bench Report V2\n\n");
    md.push_str(&format!("- **Platform**: {}\n", report.metadata.platform));
    md.push_str(&format!("- **CPU**: {}\n", report.metadata.cpu));
    md.push_str(&format!("- **Timestamp**: {}\n", report.metadata.timestamp));
    if let Some(commit) = &report.metadata.git_commit {
        md.push_str(&format!("- **Git Commit**: `{commit}`\n"));
    }
    md.push_str("\n## Results\n\n");
    md.push_str(&Table::new(rows).with(Style::markdown()).to_string());
    md.push('\n');
    md
}

fn backend_label(report: &super::model::ScenarioReport) -> String {
    match &report.identity.backend {
        super::model::BackendKind::Shm => "shm".to_string(),
        super::model::BackendKind::Mmap => "mmap".to_string(),
        super::model::BackendKind::Layout => "layout".to_string(),
        super::model::BackendKind::Unknown(other) => other.clone(),
    }
}

fn codec_label(codec: &CodecKind) -> String {
    match codec {
        CodecKind::Bincode => "bincode".to_string(),
        CodecKind::Rkyv => "rkyv".to_string(),
        CodecKind::Flatbuf => "flatbuf".to_string(),
        CodecKind::Unknown(other) => other.clone(),
    }
}

fn measurement_label(scenario: &super::model::ScenarioReport) -> String {
    match &scenario.config.measurement {
        super::model::MeasurementKind::MaxThroughput => "max_throughput".to_string(),
        super::model::MeasurementKind::BatchTiming => "batch_timing".to_string(),
        super::model::MeasurementKind::LayoutValidation => "layout_validation".to_string(),
        super::model::MeasurementKind::CoAware { target_rate } => format!("co_aware@{target_rate}"),
        super::model::MeasurementKind::Unknown(other) => other.clone(),
    }
}

fn format_avg_ns(value: f64) -> String {
    if value < 1_000.0 {
        format!("{value:.1}ns")
    } else {
        crate::latency::format_ns(value.round() as u64)
    }
}

fn format_alloc_bytes(bytes: u64) -> String {
    if bytes >= 1_000_000 {
        format!("{:.1}MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.1}KB", bytes as f64 / 1_000.0)
    } else {
        format!("{bytes}B")
    }
}
