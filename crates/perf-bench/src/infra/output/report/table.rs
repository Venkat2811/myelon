use super::model::{CodecKind, ReportBundle, ScenarioOutcome};
use crate::infra::events::format_throughput;
use crate::infra::latency::format_ns;

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
            let latency = |f: fn(&crate::infra::latency::LatencyStats) -> u64| {
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
                    .map(human_alloc_bytes)
                    .unwrap_or_else(|| "-".to_string()),
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

fn format_avg_ns(value: f64) -> String {
    if value < 1_000.0 {
        format!("{value:.1}ns")
    } else {
        format_ns(value.round() as u64)
    }
}

fn human_alloc_bytes(bytes: u64) -> String {
    if bytes >= 1_000_000 {
        format!("{:.1}MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.1}KB", bytes as f64 / 1_000.0)
    } else {
        format!("{bytes}B")
    }
}

#[cfg(test)]
mod tests {
    use crate::cli::layout;
    use crate::infra::output::report::{LayoutTargetMeasurement, ReportBundle, RunMetadata};

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

    #[test]
    fn renders_access_columns_in_summary_table() {
        let report = ReportBundle {
            metadata: RunMetadata::capture(),
            scenarios: vec![crate::infra::output::report::ScenarioReport {
                identity: crate::infra::output::report::ScenarioIdentity {
                    benchmark_id: "myelon-bench/typed_zero_copy_sweep/demo".into(),
                    suite: "myelon-bench".into(),
                    benchmark: "typed_zero_copy_sweep".into(),
                    family: crate::infra::output::report::ScenarioFamily::MyelonLayerSweep,
                    scenario: "typed_zero_copy_rkyv_shm_1KB_1p1c".into(),
                    backend: crate::infra::output::report::BackendKind::Shm,
                    layer: "typed_zero_copy".into(),
                    codec: Some(crate::infra::output::report::CodecKind::Rkyv),
                },
                config: crate::infra::output::report::ScenarioConfig {
                    measurement: crate::infra::output::report::MeasurementKind::MaxThroughput,
                    transport: crate::infra::output::report::model::TransportSpec {
                        wait_strategy: crate::infra::output::report::WaitStrategyKind::BusySpin,
                        coordination: None,
                        discovery: None,
                        framing: None,
                        zero_copy: Some(crate::infra::output::report::ZeroCopyKind::Enabled),
                    },
                    workload: crate::infra::output::report::model::WorkloadConfig {
                        message_size_bytes: 1024,
                        payload_bytes: 1024,
                        buffer_depth: 1024,
                        num_messages: 1000,
                        warmup_messages: 100,
                        num_producers: 1,
                        num_consumers: 1,
                        batch_size: Some(8),
                    },
                },
                outcome: crate::infra::output::report::ScenarioOutcome::Throughput(
                    crate::infra::output::report::ThroughputOutcome {
                        producer: crate::infra::output::report::ProducerMetrics {
                            throughput_ops_sec: 1000.0,
                            bandwidth_bytes_sec: None,
                            data_rate_gbps: None,
                            data_rate_mbps: 1.0,
                        },
                        consumers: crate::infra::output::report::ConsumerAggregate {
                            average_throughput_ops_sec: 1000.0,
                            min_throughput_ops_sec: None,
                            max_throughput_ops_sec: None,
                            total_throughput_ops_sec: None,
                            average_bandwidth_bytes_sec: None,
                            average_data_rate_gbps: None,
                            checksum_total: None,
                        },
                        per_consumer: Vec::new(),
                        verification: crate::infra::output::report::VerificationMetrics {
                            passed: true,
                            messages_processed: 1000,
                        },
                        latency: None,
                        phase_timing: None,
                        derived: crate::infra::output::report::DerivedMetrics {
                            access_avg_ns: Some(123.4),
                            access_vs_decode_speedup: Some(5.6),
                            alloc_count: Some(0),
                            alloc_bytes: Some(0),
                            ..Default::default()
                        },
                    },
                ),
                metadata: crate::infra::output::report::RunMetadata::capture(),
            }],
        };
        let table = report.render_summary_table();
        assert!(table.contains("Access"));
        assert!(table.contains("123.4ns"));
        assert!(table.contains("5.6x"));
        assert!(table.contains("Allocs"));
        assert!(table.contains("0B"));
    }
}
