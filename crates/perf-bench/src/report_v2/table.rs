use super::model::{CodecKind, ReportBundle, ScenarioOutcome};
use crate::events::format_throughput;
use crate::latency::format_ns;

impl ReportBundle {
    pub fn render_summary_table(&self) -> String {
        if self
            .scenarios
            .iter()
            .all(|scenario| matches!(scenario.outcome, ScenarioOutcome::Layout(_)))
        {
            return render_layout_summary(self);
        }
        render_throughput_summary(self)
    }

    pub fn print_summary(&self) {
        println!("\n{}\n", self.render_summary_table());
    }
}

fn render_layout_summary(report: &ReportBundle) -> String {
    use tabled::{Table, Tabled};

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
                unreachable!("layout-only summary path");
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

    Table::new(rows).to_string()
}

fn render_throughput_summary(report: &ReportBundle) -> String {
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

    let rows: Vec<Row> = report
        .scenarios
        .iter()
        .map(|scenario| {
            let ScenarioOutcome::Throughput(outcome) = &scenario.outcome else {
                unreachable!("throughput summary path");
            };
            let latency = |f: fn(&crate::latency::LatencyStats) -> u64| {
                outcome
                    .latency
                    .as_ref()
                    .map(|stats| format_ns(f(stats)))
                    .unwrap_or_else(|| "-".to_string())
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
                consumers: scenario.config.workload.num_consumers,
                prod_ops: format_throughput(outcome.producer.throughput_ops_sec),
                cons_ops: format_throughput(outcome.consumers.average_throughput_ops_sec),
                p50: latency(|stats| stats.p50_ns),
                p99: latency(|stats| stats.p99_ns),
                p999: latency(|stats| stats.p999_ns),
                p9999: latency(|stats| stats.p9999_ns),
                p99999: latency(|stats| stats.p99999_ns),
            }
        })
        .collect();

    Table::new(rows).to_string()
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

#[cfg(test)]
mod tests {
    use crate::report_v2::{LayoutTargetMeasurement, ReportBundle};
    use crate::scenario_v2::layout;

    #[test]
    fn renders_layout_summary_table() {
        let report = ReportBundle::from_layout_targets(
            "layout_validation",
            50,
            &[LayoutTargetMeasurement::new(
                &layout::TYPED_SHM_CONSUMER_ATTACH,
                123,
            )],
        );
        let table = report.render_summary_table();
        assert!(table.contains("typed-shm-consumer-attach"));
        assert!(table.contains("PASS"));
    }
}
